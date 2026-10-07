"""请求式SIM首登记/Registered回执的纯候选，有限scenario不设训练默认。

当前接显式Registered、有限Timeout/QueryReply与TerminalSuccess步骤。费用及受控重试尚未接入；
Registered不当经济到账。根已绑定原account，resolution为原ID引用。
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
    succeeded: torch.Tensor
    success_offsets: torch.Tensor | None = None
    query_state: object | None = None
    terminal_consumed: torch.Tensor | None = None

    def install_queries(self,timeout_offsets,reply_offsets,replies):
        from settlement_query import SettlementQueries
        if self.query_state is not None:
            raise ValueError('查询scenario只能冷安装一次')
        queries=SettlementQueries.empty(self.conditions,timeout_offsets,reply_offsets,replies)
        if self.success_offsets is None:
            raise ValueError('当前有限查询场景需同一已安装的后续成功步骤')
        invalid=(timeout_offsets<self.receipt_offsets)|(reply_offsets>self.success_offsets)
        fault=self.fault | queries.fault | Numbers.combine_fault_rows(invalid.to(torch.int32))
        return replace(self,query_state=queries,terminal_consumed=torch.zeros_like(self.present),fault=fault)

    def submit_queries(self,actions):
        if self.query_state is None:
            raise ValueError('查询需实际安装场景与首请求')
        queries=self.query_state.submit(self,actions)
        return replace(self,query_state=queries,fault=self.fault | queries.fault)

    @classmethod
    def empty(cls, conditions, receipt_offsets,success_offsets=None):
        if conditions.ndim!=2 or conditions.dtype!=torch.int64 or receipt_offsets.shape!=conditions.shape or receipt_offsets.dtype!=torch.int64 or receipt_offsets.device!=conditions.device:
            raise TypeError('单Registered scenario需同设备根/市场目录及int64 offset')
        if success_offsets is not None and (success_offsets.shape!=conditions.shape or success_offsets.dtype!=torch.int64 or success_offsets.device!=conditions.device):
            raise TypeError('成功scenario offset需同设备原目录')
        zero=ExactInt.from_i64_tensor(torch.zeros(conditions.numel(),dtype=torch.int64,device=conditions.device))
        blank=torch.zeros_like(conditions,dtype=torch.bool)
        return cls(conditions,receipt_offsets,blank,torch.full_like(conditions,-1),torch.zeros_like(conditions),
            blank.clone(),zero,Numbers.combine_fault_rows((receipt_offsets<0).to(torch.int32)),blank.clone(),success_offsets)

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
        action_error=torch.where(actions.request,actions.fault | (counts!=1).to(torch.int32),0)
        root_error=Numbers.combine_fault_rows(torch.where(
            torch.arange(roots,device=self.conditions.device)[:,None]==actions.roots[None,:],action_error[None,:],0))
        fault=self.fault | root_error | obligations.fault | maker.fault | Numbers.combine_fault_rows(torch.where(
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
        if self.success_offsets is not None:
            terminal_done=self.succeeded if self.terminal_consumed is None else self.terminal_consumed
            success_pending=self.present & self.received & ~terminal_done
            if self.query_state is not None:
                query=self.query_state
                success_pending &= query.reply_consumed
                timeout_pending=self.present & self.received & ~query.timeout_consumed
                reply_pending=self.present & query.timeout_consumed & ~query.reply_consumed
                for mask,offset in ((timeout_pending,query.timeout_offsets),(reply_pending,query.reply_offsets)):
                    fault |= Numbers.combine_fault_rows((mask & (self.first_arrival>(1<<63)-1-offset)).to(torch.int32))
                    times=torch.minimum(torch.where(pending,times,(1<<63)-1),torch.where(mask,self.first_arrival+offset,(1<<63)-1))
                    pending |= mask
                fault |= query.fault
            success_overflow=success_pending & (self.first_arrival>(1<<63)-1-self.success_offsets)
            fault |= Numbers.combine_fault_rows(success_overflow.to(torch.int32))
            success_at=self.first_arrival+self.success_offsets
            times=torch.minimum(torch.where(pending,times,(1<<63)-1),torch.where(success_pending,success_at,(1<<63)-1))
            pending |= success_pending
        masked=torch.where(pending,times,(1<<63)-1)
        at=masked.min(1).values
        return ScheduledTime(at,pending.any(1)&(fault==0),fault)

    def coordinated(self,original,valid_roots):
        use=valid_roots[:,None]
        return replace(self,**{name:torch.where(use,getattr(self,name),getattr(original,name))
            for name in ('present','resolutions','first_arrival','received','succeeded')},
            sends=self.sends.select(use.expand_as(self.present).flatten(),original.sends),
            query_state=None if self.query_state is None else self.query_state.coordinated(original.query_state,valid_roots),
            terminal_consumed=None if self.terminal_consumed is None else torch.where(use,self.terminal_consumed,original.terminal_consumed))

    def advance_timeout(self,at,enabled):
        if self.query_state is None:
            raise ValueError('Timeout需实际查询scenario')
        query=self.query_state
        clock=self.next_clock()
        due=self.present & self.received & ~query.timeout_consumed & (self.first_arrival+query.timeout_offsets==at[:,None])
        emitted=due & enabled[:,None] & clock.present[:,None] & (clock.at==at)[:,None]
        emitted &= clock.fault[:,None]==0
        return replace(self,query_state=replace(query,timeout_consumed=query.timeout_consumed|emitted),fault=clock.fault),RegisteredSettlementFeedback(
            self.conditions,self.resolutions,at[:,None].expand_as(self.conditions),emitted,clock.fault)

    def advance_query_reply(self,at,enabled,account,sources,obligations):
        if self.query_state is None:
            raise ValueError('QueryReply需实际查询scenario')
        query=self.query_state
        due=self.present & query.timeout_consumed & ~query.reply_consumed & (self.first_arrival+query.reply_offsets==at[:,None])
        market=due.to(torch.int32).argmax(1)
        root=torch.arange(due.size(0),device=at.device)
        clock=self.next_clock()
        use=enabled & due.any(1) & clock.present & (clock.at==at)
        index=root*due.size(1)+market
        from queues import _take
        remaining=_take(query.queries,index).compare(_take(query.consumed_queries,index))
        reference=obligations.reference_indices[root,market]
        assets,tokens,payouts,error=sources.gather(reference)
        fault=self.fault | clock.fault | torch.where(use,error | remaining.fault | (remaining.order<=0).to(torch.int32),0)
        success=query.replies[root,market]==2
        original=account
        account,delta=account.redeem_outcomes(assets,payouts,use & success & ~self.succeeded[root,market] & (fault==0))
        fault |= account.fault
        account=account.coordinate_result(original,fault)
        delta=account.coordinate_delta(delta,fault==0,fault)
        consumed=use & (fault==0)
        selected=(torch.arange(due.size(1),device=at.device)[None,:]==market[:,None]) & consumed[:,None]
        counts=query.consumed_queries+ExactInt.from_i64_tensor(selected.flatten().to(torch.int64))
        queries=replace(query,reply_consumed=query.reply_consumed|selected,consumed_queries=counts)
        backend=replace(self,query_state=queries,succeeded=self.succeeded|(selected & success[:,None]),fault=fault)
        observations=torch.tensor([3,4,5,6],dtype=torch.int64,device=at.device)[query.replies[root,market].clamp(0,3)]
        return backend,account,(self.conditions[root,market],self.resolutions[root,market],assets,tokens,delta,consumed,fault,observations)

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


    def advance_success(self,at,enabled,account,sources,obligations):
        """有限Registered→TerminalSuccess；每根本轮一项，成功守卫不重复兑付。"""
        if self.success_offsets is None:
            raise ValueError('未安装实际成功scenario步骤')
        done=self.succeeded if self.terminal_consumed is None else self.terminal_consumed
        due=self.present & self.received & ~done & (self.first_arrival+self.success_offsets==at[:,None])
        if self.query_state is not None:
            due &= self.query_state.reply_consumed
        market=due.to(torch.int32).argmax(1)
        root=torch.arange(due.size(0),device=at.device)
        scheduled=self.next_clock()
        use=enabled & due.any(1) & scheduled.present & (scheduled.at==at)
        reference=obligations.reference_indices[root,market]
        assets,tokens,payouts,error=sources.gather(reference)
        fault=self.fault | torch.where(use,error,0) | scheduled.fault
        original=account
        account,delta=account.redeem_outcomes(assets,payouts,use & ~self.succeeded[root,market] & (fault==0))
        fault |= account.fault
        valid=fault==0
        account=account.coordinate_result(original,fault)
        delta=account.coordinate_delta(delta,valid,fault)
        consumed=use & valid
        selected=torch.arange(due.size(1),device=at.device)[None,:]==market[:,None]
        backend=replace(self,succeeded=self.succeeded | (selected & consumed[:,None]),fault=fault,
            terminal_consumed=None if self.terminal_consumed is None else self.terminal_consumed | (selected & consumed[:,None]))
        return backend,account,(self.conditions[root,market],self.resolutions[root,market],assets,tokens,delta,consumed,fault)
