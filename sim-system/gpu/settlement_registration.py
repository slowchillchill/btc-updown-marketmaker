"""请求式SIM首登记/Registered回执的纯候选，有限scenario不设训练默认。

完整成功、费用、查询及受控重试仍由其真实后端分支接入；本对象只安装显式
单Registered步骤，不把回执当经济到账。根已绑定原account，resolution为原ID引用。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, ExactInt
from scheduling import ScheduledTime


@dataclass(frozen=True)
class RegisteredSettlementFeedback:
    conditions: torch.Tensor
    resolutions: torch.Tensor
    at: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class SettlementRegistrations:
    conditions: torch.Tensor  # [root,market]原目录，不代表市场已初始化。
    receipt_offsets: torch.Tensor  # 显式工程scenario单Registered步骤的offset。
    present: torch.Tensor
    resolutions: torch.Tensor
    first_arrival: torch.Tensor
    received: torch.Tensor
    sends: object  # 完整原u64[roots*markets]。
    fault: torch.Tensor

    @classmethod
    def empty(cls, conditions, receipt_offsets):
        if conditions.ndim!=2 or conditions.dtype!=torch.int64 or receipt_offsets.shape!=conditions.shape or receipt_offsets.dtype!=torch.int64 or receipt_offsets.device!=conditions.device:
            raise TypeError('单Registered scenario需同设备根/市场目录及int64 offset')
        zero=ExactInt.from_i64_tensor(torch.zeros(conditions.numel(),dtype=torch.int64,device=conditions.device))
        blank=torch.zeros_like(conditions,dtype=torch.bool)
        return cls(conditions,receipt_offsets,blank,torch.full_like(conditions,-1),torch.zeros_like(conditions),
            blank.clone(),zero,Numbers.combine_fault_rows((receipt_offsets<0).to(torch.int32)))

    def register(self,actions,sampler,identity_bytes,identity_offsets,obligations,maker,columns):
        """首请求独立核后台屏障，重复保首次到达/scenario位置/计数。"""
        roots,markets=self.conditions.shape
        if obligations.reference_indices.shape!=self.conditions.shape or maker.market_result.shape!=self.conditions.shape:
            raise ValueError('后台请求须对应同根实际判定义务和Maker市场目录')
        delays=sampler.sample_requests_indexed(actions.roots,identity_bytes,identity_offsets,
            actions.resolutions,torch.zeros_like(actions.conditions))
        arrival,error=delays.arrival_at(actions.requested_at)
        member=(torch.arange(roots,device=self.conditions.device)[:,None,None]==actions.roots[None,None,:]) & (
            self.conditions[:,:,None]==actions.conditions[None,None,:])
        counts=member.to(torch.int64).sum((0,1))
        at=member.to(torch.int32).argmax(2)
        requested=(member & actions.request[None,None,:]).any(2)
        fresh=requested & ~self.present
        references=obligations.reference_indices
        safe=references.clamp(0,columns.fields.size(0)-1)
        original=columns.fields[safe,3]
        valid=(references>=0) & ~obligations.pending.any(2) & (maker.market_result>=0) & (
            original==actions.resolutions[at])
        incoming=actions.fault[at] | error[at] | (counts[at]!=1).to(torch.int32)
        fault=self.fault | obligations.fault | maker.fault | Numbers.combine_fault_rows(torch.where(
            fresh,incoming | (~valid).to(torch.int32),0))
        use=fresh & (fault[:,None]==0)
        repeated=use.flatten()
        sends=self.sends.constant(1).select(repeated,self.sends)
        return replace(self,present=self.present|use,resolutions=torch.where(use,actions.resolutions[at],self.resolutions),
            first_arrival=torch.where(use,arrival[at],self.first_arrival),sends=sends,fault=fault)

    def next_clock(self):
        overflow=self.present & (self.first_arrival > (1<<63)-1-self.receipt_offsets)
        fault=self.fault | Numbers.combine_fault_rows(overflow.to(torch.int32))
        times=self.first_arrival+self.receipt_offsets
        pending=self.present & ~self.received
        masked=torch.where(pending,times,(1<<63)-1)
        at=masked.min(1).values
        return ScheduledTime(at,pending.any(1)&(fault==0),fault)

    def advance_registered(self,at,enabled):
        scheduled=self.next_clock()
        pending=self.present & ~self.received
        due=pending & (self.first_arrival+self.receipt_offsets==at[:,None])
        # 该有限scenario同根同刻可发多市场；返回完整市场人口，不按slot重排业务ID。
        emitted=due & enabled[:,None] & scheduled.present[:,None] & (scheduled.at==at)[:,None]
        fault=scheduled.fault
        emitted &= fault[:,None]==0
        return replace(self,received=self.received|emitted,fault=fault), RegisteredSettlementFeedback(
            self.conditions,self.resolutions,at[:,None].expand_as(self.conditions),emitted,fault)
