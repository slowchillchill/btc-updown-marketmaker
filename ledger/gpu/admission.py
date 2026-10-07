"""原已编码Base短链NOE→平台/预留/生命周期的设备协调候选。

Ledger entry/local/platform三个序独立；原位置成功后才登记，缺前驱/关联保留待消费。
成功成本池和未配对成交贡献按首次经济成功接纳；返佣日归属、结算及完整账户读面
仍须接入，不能代替全Ledger或活动根恢复。
"""
from dataclasses import dataclass,replace,fields
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from asset_buy import take_ratio
from admission_lifecycle import LifecycleColumns,LifecycleDirectory
from reservation import ReservationBook,run_reserve_action,sequence_fault,RESERVED,BEFORE_SEND,SUBMITTED,PLATFORM_PENDING
from success_pool import select
from memory import EconomicColumns
from trade_success import TradeSuccess
from position_cost import PositionCosts
from order_funds import OrderFunds
from accepted_principal import AcceptedPrincipal
from rebate_contribution import RebateContributions
from core import OUT_EVIDENCE,OUT_FACT,OUT_DUPLICATE,OUT_ALIAS,OUT_LOGICAL_CONFLICT,OUT_ECONOMIC_CONFLICT,OUT_PENDING


def coordinate(value,original,valid):
    if value is original: return value
    roots=valid.numel()
    updates={}
    for field in fields(value):
        new,old=getattr(value,field.name),getattr(original,field.name)
        if new is old:
            updates[field.name]=new
        elif isinstance(new,ExactInt):
            updates[field.name]=new.select(valid.repeat_interleave(new.sign.numel()//roots),old)
        elif isinstance(new,ExactRational):
            mask=valid.repeat_interleave(new.numerator.sign.numel()//roots)
            updates[field.name]=ExactRational(new.numerator.select(mask,old.numerator),new.denominator.select(mask,old.denominator))
        elif isinstance(new,torch.Tensor) and new.ndim>=1 and new.size(0)==roots:
            mask=valid.reshape((roots,)+(1,)*(new.ndim-1))
            updates[field.name]=torch.where(mask,new,old)
        else:
            updates[field.name]=new
    return replace(value,**updates)


@dataclass(frozen=True)
class CashFunds:
    actual_available: object
    maker_available: object
    actual_reserved: object
    maker_reserved: object
    local_pending: object
    platform_pending: object
    fault: torch.Tensor


@dataclass(frozen=True)
class AdmissionLifecycle:
    platform: object
    success_pools: object
    trade_success: object
    position_costs: object
    unresolved_markets: object
    rebate_contributions: object
    rebate_days: object
    maker_cash: object  # 原Existing经济现金归Maker；结算新kind须按原归属另接。
    reservations: object
    orders: object
    order_funds: object
    trades: object
    columns: object
    lifecycle: object
    current_entry_sequence: object
    current_local_prefix: object
    global_requests: torch.Tensor
    item_ordinals: torch.Tensor
    applied: torch.Tensor
    applied_sequences: object
    available_through: torch.Tensor
    has_available: torch.Tensor
    available_at: torch.Tensor
    fault: torch.Tensor
    projected_markets: object | None = None
    resolutions: object | None = None
    accepted_principal: object | None = None
    market_balance_origin: object | None = None  # 原冷起点市场余额及经济prefix，None是Unknown。

    @classmethod
    def open(cls,platform,bank,columns,current_entry_sequence,current_local_prefix,success_pools,*,rebate_days=None,position_costs=None,order_funds=None,unresolved_markets=None,projected_markets=None,resolutions=None,lifecycle_capacity=None,accepted_principal=None,market_balance_origin=None):
        roots,width=columns.row_refs.shape
        if market_balance_origin is not None and (market_balance_origin.known.shape!=(roots,)
                or market_balance_origin.conditions.device!=columns.row_refs.device):
            raise TypeError('市场冷余额起点须与Ledger同根同设备')
        capacity=max(1,width) if lifecycle_capacity is None else lifecycle_capacity
        if type(capacity) is not int or capacity<1:
            raise ValueError('身份目录物理容量须为正整数，可从资源退出边界增长')
        if projected_markets is not None and (projected_markets.present.size(0) != roots or projected_markets.present.device != columns.row_refs.device):
            raise ValueError('市场投影人口须同根同设备')
        life=LifecycleColumns.from_projection(bank,columns)
        book=ReservationBook.empty(roots,capacity=max(1,width),device=columns.row_refs.device)
        empty=torch.full_like(book.request_slots,-1)
        opening_cash=take_ratio(platform.balances,torch.arange(roots,device=empty.device)*8)
        # 中途冷前缀不能将未提供的既有成本冒充已知零；新根显式传入empty。
        if position_costs is None:
            position_costs=PositionCosts.empty(roots,capacity,empty.device,known=False)
        if order_funds is None:
            order_funds=OrderFunds.empty(roots,capacity,capacity,width,empty.device,known=False)
        if accepted_principal is None:
            accepted_principal=AcceptedPrincipal.open(roots,torch.unique(success_pools.condition_slots),known=False)
        return cls(platform,success_pools,TradeSuccess.open(roots,capacity,empty.device),position_costs,unresolved_markets,
            RebateContributions.empty(roots,max(1,width),empty.device),rebate_days,opening_cash,book,LifecycleDirectory.empty(roots,capacity,device=empty.device),order_funds,
            LifecycleDirectory.empty(roots,capacity,device=empty.device),columns,life,
            current_entry_sequence,current_local_prefix,empty,empty.clone(),torch.zeros_like(columns.enabled),
            ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=empty.device)),
            torch.zeros(roots,dtype=torch.int64,device=empty.device),torch.zeros(roots,dtype=torch.bool,device=empty.device),
            torch.zeros_like(columns.row_refs),platform.fault | accepted_principal.fault | (0 if projected_markets is None else projected_markets.fault),projected_markets,resolutions,accepted_principal,market_balance_origin)

    def funds(self):
        roots=self.fault.numel()
        at=torch.arange(roots,device=self.fault.device)*8
        available,principal_balance,fee_balance=[take_ratio(self.platform.balances,at+offset) for offset in [1,2,3]]
        local,platform_pending=self.reservations.pending()
        pending=local+platform_pending
        reserved=principal_balance+fee_balance+pending
        actual=available-pending
        maker=self.maker_cash-reserved
        fault=self.fault | actual.fault | maker.fault | reserved.fault
        return CashFunds(actual,maker,reserved,reserved,local,platform_pending,fault)

    def append_columns(self,bank,incoming):
        """持续接入新NOE批；账户、原三序、旧position及资金关联原样承接。

        新记录人口给目录提供保守物理扩容上界，不作为业务数量上界。
        历史缓存退役仍由完整宿主按原引用生命周期处理。
        """
        from transport import append_root_fields
        roots,width=self.columns.row_refs.shape
        extra=incoming.row_refs.size(1)
        columns=self.columns.append(incoming)
        life=self.lifecycle.append(LifecycleColumns.from_projection(bank,incoming))
        blank=torch.zeros_like(incoming.enabled)
        zero=ExactInt.from_i64_tensor(torch.zeros(roots*extra,dtype=torch.int64,device=blank.device))
        order_capacity=max(self.orders.occupied.size(1),self.order_funds.occupied.size(1))+extra
        # 原冷前缀可安装更宽逐槽缓存；追加保留全部已绑定人口，不反向缩它们。
        trade_capacity=max(self.trades.occupied.size(1),self.trade_success.has_fact.size(1),
            self.position_costs.active.size(1),self.order_funds.trade_bound.size(1))+extra
        return replace(self,columns=columns,lifecycle=life,
            applied=torch.cat([self.applied,blank],1),
            applied_sequences=append_root_fields(self.applied_sequences,zero,roots,width,extra),
            available_at=torch.cat([self.available_at,torch.zeros_like(incoming.row_refs)],1),
            orders=self.orders.resized(order_capacity),trades=self.trades.resized(trade_capacity),
            trade_success=self.trade_success.resized(trade_capacity),
            position_costs=self.position_costs.resized(trade_capacity),
            order_funds=self.order_funds.resized(self.order_funds.occupied.size(1)+extra).append_records(extra,trade_capacity),
            rebate_contributions=self.rebate_contributions.resized(self.rebate_contributions.occupied.size(1)+extra))

    def resized_lifecycle(self, *, orders=None, trades=None):
        """冷资源边界增长真实身份目录及逐槽关联；不重登旧身份/已应用位置。"""
        order_capacity=self.orders.occupied.size(1) if orders is None else orders
        trade_capacity=self.trades.occupied.size(1) if trades is None else trades
        # 其余已安装缓存可比身份目录更宽；增长不能反向缩小它们。
        return replace(self,orders=self.orders.resized(order_capacity),trades=self.trades.resized(trade_capacity),
            trade_success=self.trade_success.resized(max(trade_capacity,self.trade_success.has_fact.size(1))),
            position_costs=self.position_costs.resized(max(trade_capacity,self.position_costs.active.size(1))),
            order_funds=self.order_funds.append_records(0,max(trade_capacity,self.order_funds.trade_bound.size(1))))

    def resized_contributions(self,capacity):
        """宿主只核人口容量请求后扩目录；原未消费NOE再试，不推进业务或随机数。"""
        return replace(self,rebate_contributions=self.rebate_contributions.resized(capacity))

    def resized_rebate_days(self,capacity):
        if self.rebate_days is None:
            raise ValueError('尚未绑定原condition身份序的日目录')
        return replace(self,rebate_days=self.rebate_days.resized(capacity))

    def resized_order_funds(self,capacity):
        return replace(self,order_funds=self.order_funds.resized(capacity))

    def extended_principal_conditions(self,conditions):
        """缺condition物理目录时增长；保留旧累计，原未提交NOE位置续跑。"""
        return replace(self,accepted_principal=self.accepted_principal.extended(conditions))

    def reserve_action(self,requests,conditions,principal,fee,enabled):
        funds=self.funds()
        actual,maker=funds.actual_available,funds.maker_available
        book,event=self.reservations.reserve_action(requests,conditions,principal,fee,enabled&(funds.fault==0)[:,None],actual,maker,
            self.current_entry_sequence,self.current_local_prefix)
        population = self.projected_markets
        ready = torch.ones_like(self.fault, dtype=torch.bool)
        if population is not None:
            from reservation import RESERVED
            for column in range(requests.size(1)):
                added = enabled[:, column] & (event.outcomes[:, column] == RESERVED)
                cached = (book.request_slots == requests[:, column, None]) & (book.request_slots >= 0)
                at = cached.to(torch.int32).argmax(1)
                original_condition = book.conditions.gather(1,at[:,None])[:,0]
                population, enough = population.admit(original_condition, added)
                ready &= enough
            ready &= population.fault == 0
            population = population.coordinated(self.projected_markets, ready & (event.fault == 0))
            old_matches = (self.reservations.request_slots[:,None,:] == requests[:,:,None]) & (self.reservations.request_slots[:,None,:] >= 0)
            old_found = old_matches.any(2)
            old_at = old_matches.to(torch.int32).argmax(2)
            old_outcomes = self.reservations.outcomes.gather(1,old_at)
            old_outcomes = torch.where(enabled & old_found,old_outcomes,0)
            old_coordinates = torch.arange(requests.size(0),device=requests.device)[:,None]*self.reservations.capacity+old_at
            old_sequence = _take(self.reservations.decision_sequences,old_coordinates.flatten())
            old_sequence = old_sequence.select((enabled & old_found).flatten(),old_sequence.constant(0))
            event = replace(event,outcomes=torch.where(ready[:,None],event.outcomes,old_outcomes),
                ledger_sequences=event.ledger_sequences.select(ready.repeat_interleave(requests.size(1)),old_sequence),
                current_ledger_sequence=event.current_ledger_sequence.select(ready,self.current_entry_sequence),
                current_local_prefix=event.current_local_prefix.select(ready,self.current_local_prefix),
                required_market_conditions=population.required_conditions,fault=event.fault | population.fault)
        return replace(self,reservations=coordinate(book,self.reservations,ready),
            projected_markets=population,current_entry_sequence=event.current_ledger_sequence.select(ready,self.current_entry_sequence),
            current_local_prefix=event.current_local_prefix.select(ready,self.current_local_prefix),
            fault=self.fault | funds.fault | event.fault | (0 if population is None else population.fault)),event

    def submission_started(self,request,event,global_request,item,enabled):
        sequence=self.current_entry_sequence+self.current_entry_sequence.constant(1)
        local=self.current_local_prefix+self.current_local_prefix.constant(1)
        fault=self.fault | torch.where(enabled,sequence_fault(sequence)|sequence_fault(local)|
            ((global_request<0)|(item<0)|(item>65535)).to(torch.int32),0)
        book,required,changed=self.reservations.submission_started(request,event,sequence,enabled&(fault==0))
        fault=fault | book.fault | (enabled&(required>book.capacity)).to(torch.int32)
        use=changed&(fault==0)
        same=(book.request_slots==request[:,None])&(book.request_slots>=0)
        mask=same & use[:,None]
        return replace(self,reservations=coordinate(book,self.reservations,fault==0),
            current_entry_sequence=sequence.select(use,self.current_entry_sequence),
            current_local_prefix=local.select(use,self.current_local_prefix),
            global_requests=torch.where(mask,global_request[:,None],self.global_requests),
            item_ordinals=torch.where(mask,item[:,None],self.item_ordinals),fault=fault)

    def apply_record(self,local,enabled,available_at,association_available):
        roots,width=self.applied.shape
        root=torch.arange(roots,device=local.device)
        index=root*width+local
        tag=self.lifecycle.schemas[root,local]
        outcome_kind=self.lifecycle.normalization_outcomes[root,local]
        requested=enabled & ~self.applied[root,local]
        not_applicable=requested & ((outcome_kind==OUT_DUPLICATE)|(outcome_kind==OUT_ALIAS)|
            (outcome_kind==OUT_LOGICAL_CONFLICT)|(outcome_kind==OUT_ECONOMIC_CONFLICT))
        requested=requested & ~not_applicable
        fault=self.fault | torch.where(requested,self.lifecycle.fault[root,local],0)
        economic=requested&(outcome_kind==OUT_FACT)&((tag==11)|(tag==18)|(tag==21))
        sequence=_take(self.columns.sequences,index)
        balances=take_ratio(self.columns.balances,(index[:,None]*8+torch.arange(8,device=index.device)).flatten())
        owners=self.platform.asset_roots
        assets=self.columns.asset_indices[root,local]
        outcome=self.columns.outcome_delta(index,owners)
        fault |= torch.where(economic,self.columns.outcome_fault(index,owners),0)
        next_entry=self.current_entry_sequence+self.current_entry_sequence.constant(1)
        fault=fault | torch.where(requested,sequence_fault(next_entry),0)
        redemption=economic & (tag==21)
        if self.resolutions is None:
            fault |= redemption.to(torch.int32)
        else:
            resolved_ready=self.resolutions.associated(self.lifecycle.conditions[root,local],
                self.lifecycle.resolution_identities[root,local])
            association_available=association_available & (~redemption | resolved_ready)
        platform,result=self.platform.apply_transition(sequence,balances,outcome,economic&(fault==0),
            torch.zeros_like(economic),association_available)
        cash_delta=take_ratio(balances,root*8)
        maker_cash=self.maker_cash+cash_delta
        fault=fault | torch.where(result.applied,maker_cash.fault,0)
        applicable=((outcome_kind==OUT_EVIDENCE)&((tag==3)|(tag==7)|(tag==8)|(tag==9)|
            (tag==12)|(tag==13)|(tag==14)|(tag==15)|(tag==16)|(tag==17)|(tag==19)|(tag==20))) | (outcome_kind==OUT_PENDING)
        use=(requested&applicable)|result.applied
        fault=fault | result.fault
        use=use&(fault==0)
        order=_take(self.lifecycle.orders,index)
        fill=_take(self.lifecycle.fills,index)
        canceled=(tag==15)&self.lifecycle.canceled_orders[root,local]
        is_order_release=economic&(((self.lifecycle.economic_tags[root,local]==2)|(self.lifecycle.economic_tags[root,local]==5)))&result.applied
        order_at,found,lookup_fault=self.orders.lookup(order)
        fault=fault | torch.where(use&((tag==3)|(tag==7)|(tag==19)|canceled|is_order_release|(economic&(self.lifecycle.economic_tags[root,local]==0))),
            Numbers.combine_fault_rows(lookup_fault),0)
        old_open=self.orders.flags[root,order_at]
        old_applied=self.orders.economic_applied[root,order_at]
        old_request=self.orders.requests[root,order_at]
        old_counted=self.orders.accepted_principal_counted[root,order_at]&found
        direct_request=self.lifecycle.request_slots[root,local]
        item=self.lifecycle.item_ordinals[root,local]
        association=(direct_request[:,None]>=0)&(item[:,None]>=0)&(self.global_requests>=0)&(self.item_ordinals>=0)&(
            self.global_requests==direct_request[:,None])&(self.item_ordinals==item[:,None])&(self.reservations.request_slots>=0)
        bound=association.any(dim=1)
        reservation_at=association.to(torch.int32).argmax(dim=1)
        binding=self.reservations.request_slots[root,reservation_at]
        accepted=(tag==3)&self.lifecycle.accepted[root,local]
        is_order_economic=economic&(self.lifecycle.economic_tags[root,local]==0)&result.applied
        # Economic来源可能没有DirectResponse关联；只用原显式绑定或已有订单绑定。
        request=torch.where((tag==3)&bound,binding,torch.where(bound,binding,old_request))
        has_request=bound | (found & (old_request>=0))
        lifecycle_use=use&(outcome_kind==OUT_EVIDENCE)
        confirm=lifecycle_use&(tag==3)&bound
        convert=use&is_order_economic&has_request
        # CPU definitive只在实际预留转态时返回本金；终态/重复pending不重新计数。
        reservation_match=(self.reservations.request_slots==request[:,None])&(self.reservations.outcomes==RESERVED)
        accepted_at=reservation_match.to(torch.int64).argmax(1)
        reservation_status=self.reservations.statuses[root,accepted_at]
        book=self.reservations.definitive(request,accepted | is_order_economic,old_applied | is_order_economic,
            next_entry,confirm|convert)
        transitioned=(confirm|convert)&reservation_match.any(1)&(
            (reservation_status==BEFORE_SEND)|(reservation_status==SUBMITTED)|((reservation_status==PLATFORM_PENDING)&(old_applied|is_order_economic)))
        accepted_condition=torch.where(transitioned,self.reservations.conditions[root,accepted_at],self.lifecycle.conditions[root,local])
        reserved_principal=take_ratio(self.reservations.principal,root*self.reservations.capacity+accepted_at)
        principal=select(reserved_principal,transitioned,take_ratio(balances,root*8+2))
        count_principal=~old_counted&((accepted&transitioned)|(use&is_order_economic&(accepted_condition>=0)))
        accepted_principal,principal_ready=self.accepted_principal.add(accepted_condition,principal,count_principal)
        fault|=accepted_principal.fault
        fault=fault | book.fault
        order_event=(lifecycle_use&((tag==7)|(tag==19)|accepted|canceled))|(use&(is_order_economic|is_order_release))
        open_flag=torch.where((tag==7)|(tag==19)|accepted,self.lifecycle.open_orders[root,local],old_open)
        open_flag=open_flag & ~(canceled|is_order_release)
        request=torch.where((accepted&bound)|(is_order_economic&has_request),request,old_request)
        # Target控制没有condition字段；沿原set_order_open保留已有关联，未知不猜。
        condition=torch.where(canceled,torch.where(found,self.orders.conditions[root,order_at],-1),
            self.lifecycle.conditions[root,local])
        orders,error=self.orders.set(order,condition,open_flag,next_entry,order_event,
            request=request,economic=old_applied | is_order_economic,counted=old_counted|count_principal)
        fault=fault | torch.where(order_event,error,0)
        trade_event=lifecycle_use&(tag==8)
        economic_trade=use & economic & ((self.lifecycle.economic_tags[root,local]>=6)&(self.lifecycle.economic_tags[root,local]<=8))
        trade_at,trade_found,trade_error=self.trades.lookup(fill)
        fault=fault | torch.where(trade_event|economic_trade,Numbers.combine_fault_rows(trade_error),0)
        known_fact=self.trade_success.has_fact[root,trade_at]
        pending=self.lifecycle.pending_trades[root,local] & (~known_fact | self.trades.flags[root,trade_at])
        trades,error=self.trades.set(fill,self.lifecycle.conditions[root,local],
            torch.where(trade_event,pending,self.trades.flags[root,trade_at]),
            next_entry,trade_event|economic_trade)
        fault=fault | torch.where(trade_event|economic_trade,error,0)
        lifecycle_ready=(orders.capacity_required<=orders.occupied.size(1)) & (trades.capacity_required<=trades.occupied.size(1)) & principal_ready
        use &= lifecycle_ready
        trade_event &= lifecycle_ready
        economic_trade &= lifecycle_ready
        # 真实份额/本金保持精确；D-0295只发生在成功池的派生成本分配端。
        base=_take(self.lifecycle.trade_base_units,index)
        quote=_take(self.lifecycle.trade_quote_units,index)
        quantity=ExactRational.ratio(base,base.constant(1_000_000))
        cost=ExactRational.ratio(quote,quote.constant(1_000_000))
        trade_success=self.trade_success.update(trade_at,self.lifecycle.conditions[root,local],self.lifecycle.tokens[root,local],
            quantity,cost,self.lifecycle.trade_dispositions[root,local],trade_event,economic_trade,
            self.lifecycle.economic_tags[root,local]==6,role=self.lifecycle.trade_roles[root,local],
            matched_at=self.lifecycle.trade_matched_at[root,local],
            immediate_fee=ExactRational.ratio(_take(self.lifecycle.trade_fee_units,index),base.constant(1_000_000)),
            maker_fee_read_kind=self.lifecycle.maker_fee_read_kind[root,local],
            maker_fee_equivalent=take_ratio(self.lifecycle.maker_fee_equivalent,index))
        zero_cost=ExactRational(base.constant(0),base.constant(1))
        applied_cost=zero_cost-take_ratio(balances,root*8+2)
        applied_fee=zero_cost-take_ratio(balances,root*8+3)
        applied_fill=economic_trade & (self.lifecycle.economic_tags[root,local]==6)
        compensated=economic_trade & (self.lifecycle.economic_tags[root,local]==8)
        fault=fault | torch.where(applied_fill,applied_cost.fault | applied_fee.fault,0)
        cost_asset=torch.where(self.lifecycle.economic_has_outcome[root,local],assets,-1)
        position_costs=self.position_costs.transition(trade_at,self.lifecycle.conditions[root,local],cost_asset,
            applied_cost,applied_fee,applied_fill,compensated)
        trade_success,success_pools=trade_success.apply_success(self.success_pools,trades,trade_at,(trade_event|economic_trade)&(fault==0))
        fault=fault | success_pools.fault
        contribution_event=lifecycle_use & (tag==9)
        contributions,required,_=self.rebate_contributions.add(fill,self.lifecycle.contribution_kinds[root,local],
            self.lifecycle.contribution_days[root,local],self.lifecycle.conditions[root,local],
            take_ratio(self.lifecycle.contribution_amounts,index),contribution_event & (fault==0))
        fault=fault | contributions.fault
        # 此候选目录容量等于本已接入NOE块人口，完整跨块宿主须先扩池再消费。
        # 不提交条目/position，容量不足仍保留原待消费记录。
        use=use & (~contribution_event | (required<=contributions.occupied.size(1)))
        maker_event=lifecycle_use & (tag==14)
        vector=self.lifecycle.maker_contributions
        vector_width=vector.present.size(1)
        vector_index=index[:,None]*vector_width+torch.arange(vector_width,device=index.device)[None,:]
        member_masks=vector.present[index]
        maker_ready=torch.ones_like(maker_event)
        # 先核全部原成功前驱，再准备整条向量；不能按条件/token猜trade关联。
        for member in range(vector_width):
            member_index=vector_index[:,member]
            member_trade=_take(vector.fields[0],member_index)
            at,present,member_fault=self.trades.lookup(member_trade)
            real=maker_event & member_masks[:,member]
            fault=fault | torch.where(real,Numbers.combine_fault_rows(member_fault),0)
            maker_ready=maker_ready & (~real | (present & self.trade_success.success_applied[root,at]))
        maker_use=maker_event & maker_ready & (fault==0)
        maker_capacity=torch.ones_like(maker_use)
        for member in range(vector_width):
            member_index=vector_index[:,member]
            contributions,member_required,_=contributions.add(_take(vector.fields[0],member_index),
                vector.fields[2][member_index],vector.fields[3][member_index],vector.fields[1][member_index],
                take_ratio(vector.fields[4],member_index),maker_use & member_masks[:,member])
            fault=fault | contributions.fault
            maker_capacity=maker_capacity & (member_required<=contributions.occupied.size(1))
        use=use & (~maker_event | (maker_ready & maker_capacity))
        days=self.rebate_days
        day_event=lifecycle_use & (tag==12)
        payment_event=lifecycle_use & (tag==13)
        if days is None:
            # 配置缺口不能静默消费原日记录；现Base路径尚无日记录时保留兼容。
            fault=fault | (maker_event | day_event | payment_event).to(torch.int32)
        else:
            days=days.observe_maker(self.lifecycle.conditions[root,local],self.lifecycle.maker_statuses[root,local],
                maker_event & maker_ready & maker_capacity & (fault==0))
            refs=self.lifecycle.rebate_refs
            ref_width=refs.present.size(1)
            ref_index=index[:,None]*ref_width+torch.arange(ref_width,device=index.device)[None,:]
            days,ready,_=days.finalize(contributions,self.lifecycle.rebate_kinds[root,local],
                self.lifecycle.rebate_days[root,local],take_ratio(self.lifecycle.rebate_accrued,index),
                take_ratio(self.lifecycle.rebate_payable,index),take_ratio(self.lifecycle.rebate_locked,index),
                _take(refs.values,ref_index.flatten()),refs.present[index],day_event & (fault==0))
            use=use & (~day_event | ready)
            days,ready,_=days.payment(self.lifecycle.rebate_kinds[root,local],self.lifecycle.rebate_days[root,local],
                take_ratio(self.lifecycle.rebate_paid,index),payment_event & (fault==0))
            fault=fault | days.fault
            use=use & (~payment_event | ready)
        order_funds,funds_ready=self.order_funds.advance(self.lifecycle,self.columns,trade_at,local,
            next_entry,self.applied_sequences,lifecycle_use & use,economic & result.applied & use)
        fault=fault | order_funds.fault
        population = self.projected_markets
        if population is not None:
            # 原空经济delta也创建account.market；仅保存终态订单/成交索引不会创建lifecycle.market。
            actual_condition = self.lifecycle.conditions[root,local]
            creates_market = (result.applied & (actual_condition >= 0)) | (
                order_event & ((tag != 3) | bound) & (actual_condition >= 0) & (old_open | open_flag)) | (
                trade_event & (actual_condition >= 0) & (self.trades.flags[root,trade_at] | pending))
            population, population_ready = population.admit(actual_condition, creates_market & use & (fault == 0))
            fault |= population.fault
            funds_ready &= population_ready
        use=use & funds_ready
        position_costs,removed_cost=position_costs.redeem(outcome,owners,redemption & result.applied & use)
        resolutions=self.resolutions
        resolved_event=lifecycle_use & (tag==17)
        request_event=lifecycle_use & (tag==20)
        if resolutions is None:
            # 缺完整Terminal/资产绑定仍显式故障，不能消费后冒充零应收。
            fault |= (resolved_event|request_event).to(torch.int32)
        else:
            accrued=take_ratio(balances,root*8+4)+take_ratio(balances,root*8+5)+take_ratio(balances,root*8+6)+take_ratio(balances,root*8+7)
            resolutions=resolutions.accrue_realized(self.lifecycle.conditions[root,local],accrued,
                economic & ((self.lifecycle.economic_tags[root,local]==9)|(self.lifecycle.economic_tags[root,local]==10)) & result.applied & use & (fault==0))
            resolutions=resolutions.observe(self.lifecycle.conditions[root,local],
                self.lifecycle.resolution_identities[root,local],self.lifecycle.resolution_terminals[root,local],
                success_pools,resolved_event & use & (fault==0))
            resolutions,request_ready=resolutions.observe_request(self.lifecycle.conditions[root,local],
                self.lifecycle.resolution_identities[root,local],self.lifecycle.settlement_policies[root,local],
                self.lifecycle.settlement_outcomes[root,local],self.lifecycle.settlement_observations[root,local],
                request_event & use & (fault==0))
            use &= ~request_event | request_ready
            resolutions,maker_delta=resolutions.redeem(self.lifecycle.conditions[root,local],
                self.lifecycle.resolution_identities[root,local],cash_delta,removed_cost,success_pools,
                redemption & result.applied & use & (fault==0))
            maker_cash=self.maker_cash+ExactRational(
                maker_delta.numerator.select(redemption,cash_delta.numerator),
                maker_delta.denominator.select(redemption,cash_delta.denominator))
            fault |= resolutions.root_fault | torch.where(result.applied,maker_cash.fault,0)
        markets=self.unresolved_markets
        if markets is not None:
            markets=markets.refresh(success_pools,trade_success,days,use & (fault==0),resolutions=resolutions)
            root_fault=Numbers.combine_fault_rows(torch.where(
                markets.roots[None,:]==root[:,None],markets.fault[None,:],0))
            fault=fault | root_fault
        success=use&(fault==0)
        valid=(fault==0)&funds_ready&lifecycle_ready
        # 多成员向量容量不足时保留请求，整个证据不提交任何部分贡献。
        contribution_commit=valid & (~maker_event | success)
        contributions=replace(coordinate(contributions,self.rebate_contributions,contribution_commit),
            capacity_required=contributions.capacity_required)
        if days is not None:
            days=replace(coordinate(days,self.rebate_days,valid & (~(maker_event | day_event | payment_event) | success)),
                capacity_required=days.capacity_required)
        # 各NOE记录候选共同成功，先前已消费位置/账本条目不回滚。
        platform=replace(platform,balances=ExactRational(
            platform.balances.numerator.select(valid.repeat_interleave(8),self.platform.balances.numerator),
            platform.balances.denominator.select(valid.repeat_interleave(8),self.platform.balances.denominator)),
            outcomes=ExactRational(platform.outcomes.numerator.select(valid[owners],self.platform.outcomes.numerator),
                platform.outcomes.denominator.select(valid[owners],self.platform.outcomes.denominator)),
            platform_source_prefix=platform.platform_source_prefix.select(valid,self.platform.platform_source_prefix),fault=fault)
        key=self.columns.positions[index]
        same=(self.columns.positions.reshape(roots,width,-1)==key[:,None,:]).all(dim=2)&self.lifecycle.emitted
        applied=self.applied | (same&success[:,None])
        return replace(self,platform=platform,
            accepted_principal=accepted_principal.coordinated(self.accepted_principal,valid),
            projected_markets=None if population is None else population.coordinated(self.projected_markets,valid),
            success_pools=success_pools.coordinated(self.success_pools,valid),
            trade_success=coordinate(trade_success,self.trade_success,valid),
            position_costs=coordinate(position_costs,self.position_costs,valid),
            unresolved_markets=None if markets is None else markets.coordinated(self.unresolved_markets,valid),
            resolutions=None if resolutions is None else resolutions.coordinated(self.resolutions,valid),
            rebate_contributions=contributions,rebate_days=days,maker_cash=ExactRational(
            maker_cash.numerator.select(success & economic,self.maker_cash.numerator),
            maker_cash.denominator.select(success & economic,self.maker_cash.denominator)),reservations=coordinate(book,self.reservations,valid),
            orders=replace(coordinate(orders,self.orders,valid),capacity_required=orders.capacity_required),
            trades=replace(coordinate(trades,self.trades,valid),capacity_required=trades.capacity_required),
            order_funds=replace(coordinate(order_funds,self.order_funds,valid),capacity_required=order_funds.capacity_required),
            current_entry_sequence=next_entry.select(success,self.current_entry_sequence),applied=applied,
            applied_sequences=_take(next_entry,root.repeat_interleave(width)).select((same&success[:,None]).flatten(),self.applied_sequences),
            available_through=torch.where(success,torch.where(self.has_available,torch.maximum(self.available_through,available_at),available_at),self.available_through),
            has_available=self.has_available | success,
            available_at=torch.where(same&success[:,None],available_at[:,None],self.available_at),fault=fault),success | (not_applicable & (fault==0))

    def apply_closed(self,closed,association_available,*,maximum_retry_rounds=None):
        roots,width=self.applied.shape
        candidate=self
        remaining=closed.emitted.clone()
        rounds=width if maximum_retry_rounds is None else maximum_retry_rounds
        if rounds<1:
            raise ValueError('Ledger有界消费轮数须为正；未完成人口由返回remaining保留')
        # 正常扫描按真实领取序。任何重试仍引用同一原位置，不改领取史。
        for _ in range(rounds):
            for column in range(closed.record_refs.size(1)):
                same=self.columns.row_refs==closed.record_refs[:,column,None]
                found=same.any(dim=1)
                at=same.to(torch.int32).argmax(dim=1)
                incoming=remaining[:,column]
                candidate=replace(candidate,fault=candidate.fault|closed.fault|(incoming&~found).to(torch.int32))
                eligible=association_available.gather(1,at[:,None]).flatten()
                candidate,consumed=candidate.apply_record(at,incoming&(candidate.fault==0),closed.available_at[:,column],eligible)
                already=candidate.applied.gather(1,at[:,None]).flatten()
                remaining[:,column]=incoming&~(consumed|already)
        return candidate,remaining
