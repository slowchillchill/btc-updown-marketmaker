"""已领取NOE记录的Ledger设备待消费缓存。

本层保留原领取顺序、交付序和首次available_at；不重新领取或推进SIM时间。
registry.observe仍归共享执行协调器。本接口绑定一个共享不可变NOE列块。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, _take


@dataclass(frozen=True)
class ClosedAdmission:
    received: torch.Tensor
    consumed: torch.Tensor
    receive_rank: torch.Tensor  # 当前块物理领取人口顺序，非业务u64身份。
    next_rank: torch.Tensor
    delivered_sequences: object
    available_at: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls, columns):
        roots, width = columns.row_refs.shape
        blank = torch.zeros((roots, width), dtype=torch.bool, device=columns.row_refs.device)
        return cls(
            blank, blank.clone(), torch.full_like(columns.row_refs, (1 << 63) - 1),
            torch.zeros(roots, dtype=torch.int64, device=columns.row_refs.device),
            ExactInt.from_i64_tensor(torch.zeros(roots * width, dtype=torch.int64, device=blank.device)),
            torch.zeros_like(columns.row_refs),
            torch.zeros(roots, dtype=torch.int32, device=columns.row_refs.device),
        )

    def accept(self, columns, closed):
        roots, width = self.received.shape
        if columns.row_refs.shape != self.received.shape:
            raise ValueError('缓存须绑定原共享NOE列块')
        if closed.record_refs.ndim != 2 or closed.record_refs.size(0) != roots or closed.record_refs.device != self.received.device:
            raise ValueError('关闭NOE引用与Ledger共享列根布局不符')
        population = closed.record_refs.size(1)
        if population == 0:
            return replace(self, fault=self.fault | closed.fault)
        if width == 0:
            error = closed.emitted.any(1).to(torch.int32)
            return replace(self, fault=self.fault | closed.fault | error)
        members = torch.cat([batch.emitted for batch in columns.projection_batches], dim=1)
        matches = (columns.row_refs[:, :, None] == closed.record_refs[:, None, :]) & members[:, :, None]
        counts = matches.to(torch.int64).sum(1)
        error = ((counts != 1) & closed.emitted).any(1).to(torch.int32)
        fault = self.fault | closed.fault | error
        actual = matches & closed.emitted[:, None, :]
        index = actual.to(torch.int64).argmax(2)
        # 同一物理引用再次传入不追加人口，也不刷新首次可见性。
        first = torch.arange(population, device=actual.device)[None, None, :] == index[:, :, None]
        fresh_incoming = (actual & first & ~self.received[:, :, None]).any(1)
        fresh_rank = fresh_incoming.to(torch.int64).cumsum(1) - 1 + self.next_rank[:, None]
        fresh = actual.any(2) & ~self.received & (fault == 0)[:, None]
        incoming_available = closed.available_at.gather(1, index)
        coordinates = (torch.arange(roots, device=index.device)[:, None] * population + index).flatten()
        delivery = _take(closed.delivered_sequences, coordinates)
        return replace(
            self, received=self.received | fresh,
            receive_rank=torch.where(fresh, fresh_rank.gather(1, index), self.receive_rank),
            next_rank=self.next_rank + fresh.to(torch.int64).sum(1),
            delivered_sequences=delivery.select(fresh.flatten(), self.delivered_sequences),
            available_at=torch.where(fresh, incoming_available, self.available_at), fault=fault,
        )

    def drain(self, admission, association_available, *, passes=None):
        """按原领取序重试；整轮无退出时设备关闭该根后续扫描。

        K个人口至多K轮可消解本批无环前驱。passes仅切分物理调度；未解引用仍保留。
        资源需求由Admission目录返回，不能把未消费统一解释为业务缺前驱。
        """
        roots, width = self.received.shape
        if association_available.shape != self.received.shape or association_available.dtype != torch.bool or association_available.device != self.received.device:
            raise TypeError('经济关联须同设备bool[roots,records]')
        if passes is None:
            passes = width
        if type(passes) is not int or passes < 0:
            raise ValueError('重试物理轮数须非负整数')
        root = torch.arange(roots, device=self.received.device)
        order = self.receive_rank.argsort(dim=1, stable=True)
        result = self
        scanning = result.fault == 0
        for _ in range(passes):
            progress = torch.zeros_like(scanning)
            for position in range(width):
                column = order[:, position]
                enabled = result.received[root, column] & ~result.consumed[root, column] & scanning
                admission, consumed = admission.apply_record(
                    column, enabled, result.available_at[root, column], association_available[root, column],
                )
                already = admission.applied[root, column]
                exited = enabled & (consumed | already) & (admission.fault == 0)
                mask = torch.zeros_like(result.consumed).scatter(1, column[:, None], exited[:, None])
                result = replace(result, consumed=result.consumed | mask, fault=result.fault | admission.fault)
                progress = progress | exited
            scanning = scanning & progress & (result.fault == 0)
        return admission, result
