"""SIM动态反馈发射序与原时延候选，独立于NOE记录序及实际公开领取。

每根输入草稿轴保持原发射顺序。资源重算不能提交本对象或源账户。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt, Numbers, _take


@dataclass(frozen=True)
class FeedbackEmission:
    private_sequences: ExactInt
    batch_sequences: ExactInt
    received_at: torch.Tensor
    emitted: torch.Tensor
    root_fault: torch.Tensor


@dataclass(frozen=True)
class FeedbackEmitter:
    next_private_sequence: ExactInt
    next_batch_sequence: ExactInt
    fault: torch.Tensor

    @classmethod
    def open(cls, roots, *, device='cuda'):
        # 开户本身占一个非空投影批，不占private反馈序。
        return cls(ExactInt.from_values([1]*roots,device=device),
                   ExactInt.from_values([2]*roots,device=device),
                   torch.zeros(roots,dtype=torch.int32,device=device))

    def emit(self, sampler, active, direct, platform_at, effective_event_at,
             direct_received_at, source_fault, *, control_admission=None):
        """[root,draft]：life以原private序采样；直接响应沿请求已抽收到时刻。

        effective_event_at由原协调器提供实际补处理时刻与platform时刻较大者。
        保留platform_at不改，不能让隐藏预取时间充当有效处理时刻。
        """
        roots, width = active.shape
        if control_admission is None:
            control_admission=torch.zeros(roots,dtype=torch.bool,device=active.device)
        if control_admission.shape!=(roots,) or control_admission.dtype!=torch.bool or control_admission.device!=active.device:
            raise TypeError('SIM开户/市场登记标记需同设备bool[roots]')
        if roots != self.fault.numel():
            raise ValueError('SIM反馈根人口不符')
        for value in [active,direct,platform_at,effective_event_at,direct_received_at,source_fault]:
            if value.shape != active.shape or value.device != active.device:
                raise ValueError('SIM反馈草稿布局不符')
        if active.dtype != torch.bool or direct.dtype != torch.bool or source_fault.dtype != torch.int32 or any(
                value.dtype != torch.int64 for value in [platform_at,effective_event_at,direct_received_at]):
            raise TypeError('SIM反馈草稿需bool/int32/int64设备列')
        counts=active.to(torch.int64).sum(dim=1)
        prefixes=active.to(torch.int64).cumsum(dim=1)-active.to(torch.int64)
        coordinates=torch.arange(roots,device=active.device).repeat_interleave(width)
        private=_take(self.next_private_sequence,coordinates)+ExactInt.from_i64_tensor(prefixes.reshape(-1))
        next_private=self.next_private_sequence+ExactInt.from_i64_tensor(counts)
        nonempty=(counts>0)|control_admission
        next_batch=self.next_batch_sequence+ExactInt.from_i64_tensor(nonempty.to(torch.int64))
        maximum=self.next_private_sequence.constant((1<<64)-1)
        minimum=self.next_private_sequence.constant(1)
        private_bound=next_private.compare(maximum)
        batch_bound=next_batch.compare(maximum)
        private_start=self.next_private_sequence.compare(minimum)
        batch_start=self.next_batch_sequence.compare(minimum)
        fault=self.fault | self.next_private_sequence.fault | self.next_batch_sequence.fault
        fault=fault | private_bound.fault | batch_bound.fault
        fault=fault | private_start.fault | batch_start.fault
        fault=fault | ((private_bound.order>0)|(batch_bound.order>0)).to(torch.int32)
        fault=fault | ((private_start.order<0)|(batch_start.order<0)).to(torch.int32)
        sampled=sampler.sample_feedback(coordinates,private)
        arrived,arrival_fault=sampled.arrival_at(effective_event_at.reshape(-1))
        received=torch.where(direct.reshape(-1),direct_received_at.reshape(-1),arrived)
        local_fault=source_fault.reshape(-1) | private.fault
        local_fault=local_fault | torch.where(direct.reshape(-1),torch.zeros_like(arrival_fault),arrival_fault)
        # 原补处理只能推后，直接响应收到时间不能早于实际回应。
        time_invalid=torch.where(direct,direct_received_at<platform_at,effective_event_at<platform_at)
        local_fault=local_fault | time_invalid.reshape(-1).to(torch.int32)
        local_fault=torch.where(active.reshape(-1),local_fault,torch.zeros_like(local_fault))
        fault=fault | Numbers.combine_fault_rows(local_fault.reshape(roots,width))
        success=fault==0
        emitted=active & success[:,None]
        candidate=FeedbackEmitter(next_private.select(success,self.next_private_sequence),
            next_batch.select(success,self.next_batch_sequence),fault)
        sequences=ExactInt(private.words,private.sign,private.fault | fault[coordinates])
        batch=_take(self.next_batch_sequence,coordinates)
        batch=ExactInt(batch.words,batch.sign,batch.fault | fault[coordinates])
        return candidate,FeedbackEmission(sequences,batch,received.reshape(roots,width),emitted,fault)
