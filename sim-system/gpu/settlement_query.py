"""显式Timeout／QueryReply有限场景；查询不重新判断首次发送资格。"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,Numbers


@dataclass(frozen=True)
class SettlementQueries:
    timeout_offsets: torch.Tensor
    reply_offsets: torch.Tensor
    replies: torch.Tensor  # 原QueryReply：Unknown0/AcceptedPending1/FinalSuccess2/ConfirmedNoTransaction3。
    timeout_consumed: torch.Tensor
    reply_consumed: torch.Tensor
    queries: object
    consumed_queries: object
    fault: torch.Tensor

    @classmethod
    def empty(cls,conditions,timeout_offsets,reply_offsets,replies):
        for value in (timeout_offsets,reply_offsets,replies):
            if value.shape!=conditions.shape or value.dtype!=torch.int64 or value.device!=conditions.device:
                raise TypeError('查询scenario须原根/市场目录的同设备int64列')
        zero=ExactInt.from_i64_tensor(torch.zeros(conditions.numel(),dtype=torch.int64,device=conditions.device))
        blank=torch.zeros_like(conditions,dtype=torch.bool)
        invalid=(timeout_offsets<0)|(reply_offsets<timeout_offsets)|(replies<0)|(replies>3)
        return cls(timeout_offsets,reply_offsets,replies,blank,blank.clone(),zero,zero,
            Numbers.combine_fault_rows(invalid.to(torch.int32)))

    def submit(self,backend,actions):
        roots,markets=backend.conditions.shape
        matches=(torch.arange(roots,device=backend.conditions.device)[:,None,None]==actions.roots[None,None,:]) & (
            backend.conditions[:,:,None]==actions.conditions[None,None,:])
        counts=matches.to(torch.int64).sum((0,1))
        slots=matches.to(torch.int32).argmax(2)
        requested=(matches & actions.query[None,None,:]).any(2)
        valid=backend.present & (backend.resolutions==actions.resolutions[slots])
        error=torch.where(actions.query,actions.fault | (counts!=1).to(torch.int32),0)
        fault=self.fault | backend.fault | Numbers.combine_fault_rows(torch.where(
            torch.arange(roots,device=backend.conditions.device)[:,None]==actions.roots[None,:],error[None,:],0))
        fault |= Numbers.combine_fault_rows((requested & ~valid).to(torch.int32))
        query=self.queries+ExactInt.from_i64_tensor((requested & (fault[:,None]==0)).flatten().to(torch.int64))
        maximum=query.constant((1<<64)-1)
        relation=query.compare(maximum)
        fault |= Numbers.combine_fault_rows((query.fault | relation.fault | (relation.order>0).to(torch.int32)).reshape(roots,markets))
        return replace(self,queries=query.select((fault[:,None]==0).expand_as(requested).flatten(),self.queries),fault=fault)

    def coordinated(self,original,valid):
        mask=valid[:,None]
        return replace(self,timeout_consumed=torch.where(mask,self.timeout_consumed,original.timeout_consumed),
            reply_consumed=torch.where(mask,self.reply_consumed,original.reply_consumed),
            queries=self.queries.select(mask.expand_as(self.timeout_consumed).flatten(),original.queries),
            consumed_queries=self.consumed_queries.select(mask.expand_as(self.timeout_consumed).flatten(),original.consumed_queries))

