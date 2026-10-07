"""已领取NOE记录的Ledger设备待消费缓存。

本层保留原领取顺序、交付序和首次available_at；不重新领取或推进SIM时间。
registry.observe仍归共享执行协调器。本接口绑定一个共享不可变NOE列块。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, _take


@dataclass(frozen=True)
class ClosedDrainCursor:
    order: torch.Tensor  # 仅本轮固定布局的原领取序；不是NOE身份。
    position: torch.Tensor
    scanning: torch.Tensor
    progress: torch.Tensor  # 完整一轮是否有记录退出；单记录等待不停止该轮。


@dataclass(frozen=True)
class ClosedAdmission:
    received: torch.Tensor
    consumed: torch.Tensor
    receive_rank: torch.Tensor  # 当前块物理领取人口顺序，非业务u64身份。
    next_rank: torch.Tensor
    delivered_sequences: object
    available_at: torch.Tensor
    fault: torch.Tensor

    def append_columns(self, incoming):
        """物理扩展待消费人口；不重领旧waiting，不刷新首次visibility。"""
        from transport import append_root_fields
        roots,width=self.received.shape
        extra=incoming.row_refs.size(1)
        blank=type(self).empty(incoming)
        return replace(self,received=torch.cat([self.received,blank.received],1),
            consumed=torch.cat([self.consumed,blank.consumed],1),
            receive_rank=torch.cat([self.receive_rank,blank.receive_rank],1),
            available_at=torch.cat([self.available_at,blank.available_at],1),
            delivered_sequences=append_root_fields(self.delivered_sequences,blank.delivered_sequences,roots,width,extra))

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

    def accept(self, columns, closed, *, source_rows=None):
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
        if source_rows is None:
            members = torch.cat([batch.emitted for batch in columns.projection_batches], dim=1)
            matches = (columns.row_refs[:, :, None] == closed.record_refs[:, None, :]) & members[:, :, None]
            counts = matches.to(torch.int64).sum(1)
            error = ((counts != 1) & closed.emitted).any(1).to(torch.int32)
            fault = self.fault | closed.fault | error
        else:
            # 与Registry/cancel共享实际源坐标；等待只接纳原匹配故障，不提前用Ledger业务结果。
            matches = source_rows.matches
            fault = self.fault | source_rows.match_fault
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

    def start_drain(self):
        """首次领取候选提交后启动扫描；不重新领取waiting或改first visibility。"""
        order = self.receive_rank.argsort(dim=1, stable=True)
        scanning = (self.fault == 0) & (self.received & ~self.consumed).any(1)
        return ClosedDrainCursor(order, torch.zeros_like(self.next_rank), scanning,
                                 torch.zeros_like(scanning))

    def drain_one(self, admission, association_available, cursor):
        """一个原领取位置的完整候选；调用方共同提交Ledger/waiting/cursor。

        只在整轮末判无进展。容量/字资源不足时，捕获消费者须保本位置及此前
        已提交前缀，从未提交输入续跑；此函数不扩池、不签发消费或公开前沿。
        """
        roots, width = self.received.shape
        if association_available.shape != self.received.shape or association_available.dtype != torch.bool or association_available.device != self.received.device:
            raise TypeError('经济关联须同设备bool[roots,records]')
        if cursor.order.shape != self.received.shape or cursor.position.shape != (roots,):
            raise ValueError('扫描游标须保持本轮原领取人口布局')
        if width == 0:
            return admission, self, replace(cursor, scanning=torch.zeros_like(cursor.scanning))
        root = torch.arange(roots, device=self.received.device)
        column = cursor.order.gather(1, cursor.position.clamp(0, width-1)[:, None])[:, 0]
        enabled = self.received[root, column] & ~self.consumed[root, column] & cursor.scanning
        admission, consumed = admission.apply_record(
            column, enabled, self.available_at[root, column], association_available[root, column])
        already = admission.applied[root, column]
        exited = enabled & (consumed | already) & (admission.fault == 0)
        mask = torch.zeros_like(self.consumed).scatter(1, column[:, None], exited[:, None])
        result = replace(self, consumed=self.consumed | mask, fault=self.fault | admission.fault)
        progress = cursor.progress | exited
        end = cursor.position + 1 == width
        remaining = (result.received & ~result.consumed).any(1)
        scanning = cursor.scanning & (result.fault == 0) & (~end | (progress & remaining))
        position = torch.where(cursor.scanning, torch.where(end, 0, cursor.position+1), cursor.position)
        cursor = replace(cursor, position=position, scanning=scanning,
                         progress=torch.where(end, False, progress))
        return admission, result, cursor

    def drain(self, admission, association_available, *, passes=None):
        """兼容现冷发射入口，单记录语义与后续设备条件消费者共用。

        passes仅切分物理调度；未解引用仍保留。无进展根设备关闭业务消费，
        本包装仍发射固定passes×width候选，不宣称物理工作已提前结束。
        """
        _, width = self.received.shape
        if association_available.shape != self.received.shape or association_available.dtype != torch.bool or association_available.device != self.received.device:
            raise TypeError('经济关联须同设备bool[roots,records]')
        if passes is None:
            passes = width
        if type(passes) is not int or passes < 0:
            raise ValueError('重试物理轮数须非负整数')
        cursor = self.start_drain()
        result = self
        for _ in range(passes * width):
            admission, result, cursor = result.drain_one(admission, association_available, cursor)
        return admission, result
