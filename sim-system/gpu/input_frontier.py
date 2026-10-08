"""SIM设备整根输入提交前沿；公开组、桶、时钟与请求共用原u64序。"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt


@dataclass(frozen=True)
class AppliedPublicGroup:
    positions: torch.Tensor
    input_sequence: object
    source: object  # 同一已验收PreparedPublicPage，身份/成员/进度不复制。

    def original_reference(self,root):
        """冷阶段记录复用正式PublicGroupReference字段，不重做事实归约。"""
        position=int(self.positions[root].cpu())
        columns=self.source.columns;group=columns["groups"][position]
        return dict(input_stream_id=columns["input_stream_id"],applied_prefix=columns["group_applied_prefixes"][position],
            group_position=int(group[0]),group_id=columns["group_ids"][position],closed_at_input_position=int(group[1]))


@dataclass(frozen=True)
class GroupDecisionCutoff:
    receipt: AppliedPublicGroup
    observed_at: torch.Tensor
    private_feedback_sequence: object
    run_id: str
    session_id: str


@dataclass(frozen=True)
class InputCommitFrontier:
    next_sequence: object
    run_id: str
    session_id: str
    public_receipt: AppliedPublicGroup | None = None

    @classmethod
    def open(cls,roots,run_id,session_id,device='cuda'):
        return cls(ExactInt.from_i64_tensor(torch.ones(roots,dtype=torch.int64,device=device)),run_id,session_id)

    def commit(self,enabled):
        """所属完整业务候选成功后共同提交；资源退出或重复传False。"""
        increment=ExactInt.from_i64_tensor(enabled.to(torch.int64))
        candidate=self.next_sequence+increment
        comparison=candidate.compare(candidate.constant((1<<64)-1))
        fault=candidate.fault|comparison.fault|(comparison.order>0).to(torch.int32)
        committed=enabled&(fault==0)
        return replace(self,next_sequence=candidate.select(committed,self.next_sequence)),fault

    def public_group(self,source,positions,enabled):
        sequence=self.next_sequence
        candidate,fault=self.commit(enabled)
        previous=self.public_receipt
        old_positions=torch.full_like(positions,-1) if previous is None else previous.positions
        old_sequence=sequence.constant(0) if previous is None else previous.input_sequence
        receipt=AppliedPublicGroup(torch.where(enabled&(fault==0),positions,old_positions),
            sequence.select(enabled&(fault==0),old_sequence),source)
        return replace(candidate,public_receipt=receipt),fault

    def observe(self,at,pending,waiting,enabled):
        """原cutoff引用实际按received/private领取分配的delivery前沿。"""
        private=ExactInt(pending.delivered_through.words.clone(),pending.delivered_through.sign.clone(),
            pending.delivered_through.fault.clone())
        candidate,fault=self.commit(enabled)
        receipt=self.public_receipt
        if receipt is None:raise ValueError('观察必须有实际已应用公共组')
        return candidate,GroupDecisionCutoff(AppliedPublicGroup(receipt.positions.clone(),
            receipt.input_sequence,receipt.source),at.clone(),private,self.run_id,self.session_id),fault
