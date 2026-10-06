"""原已编码Base短链NOE→平台/预留/生命周期的设备协调候选。

Ledger entry/local/platform三个序独立；原位置成功后才登记，缺前驱/关联保留待消费。
当前不含成功成本、贡献归属、结算或完整账户读面，不能代替全Ledger/活动根恢复。
"""
from dataclasses import dataclass,replace,fields
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from asset_buy import take_ratio
from admission_lifecycle import LifecycleColumns,LifecycleDirectory
from reservation import ReservationBook,run_reserve_action,sequence_fault
from memory import EconomicColumns
from core import OUT_EVIDENCE,OUT_FACT,OUT_DUPLICATE,OUT_ALIAS,OUT_LOGICAL_CONFLICT,OUT_ECONOMIC_CONFLICT,OUT_PENDING


def coordinate(value,original,valid):
    roots=valid.numel()
    updates={}
    for field in fields(value):
        new,old=getattr(value,field.name),getattr(original,field.name)
        if isinstance(new,ExactInt):
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
class AdmissionLifecycle:
    platform: object
    reservations: object
    orders: object
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

    @classmethod
    def open(cls,platform,bank,columns,current_entry_sequence,current_local_prefix):
        roots,width=columns.row_refs.shape
        life=LifecycleColumns.from_projection(bank,columns)
        book=ReservationBook.empty(roots,capacity=max(1,width),device=columns.row_refs.device)
        empty=torch.full_like(book.request_slots,-1)
        return cls(platform,book,LifecycleDirectory.empty(roots,max(1,width),device=empty.device),
            LifecycleDirectory.empty(roots,max(1,width),device=empty.device),columns,life,
            current_entry_sequence,current_local_prefix,empty,empty.clone(),torch.zeros_like(columns.enabled),
            ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=empty.device)),
            torch.zeros(roots,dtype=torch.int64,device=empty.device),torch.zeros(roots,dtype=torch.bool,device=empty.device),
            torch.zeros_like(columns.row_refs),platform.fault)

    def reserve_action(self,requests,conditions,principal,fee,enabled,maker_cash):
        roots=self.fault.numel()
        at=torch.arange(roots,device=self.fault.device)*8
        available,principal_balance,fee_balance=[take_ratio(self.platform.balances,at+offset) for offset in [1,2,3]]
        actual,maker=self.reservations.available(available,maker_cash,principal_balance,fee_balance)
        book,event=self.reservations.reserve_action(requests,conditions,principal,fee,enabled,actual,maker,
            self.current_entry_sequence,self.current_local_prefix)
        return replace(self,reservations=book,current_entry_sequence=event.current_ledger_sequence,
            current_local_prefix=event.current_local_prefix,fault=self.fault | event.fault),event

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
        economic=requested&(outcome_kind==OUT_FACT)&(tag==11)
        sequence=_take(self.columns.sequences,index)
        balances=take_ratio(self.columns.balances,(index[:,None]*8+torch.arange(8,device=index.device)).flatten())
        selected=take_ratio(self.columns.outcomes,index)
        owners=self.platform.asset_roots
        assets=self.columns.asset_indices[root,local]
        outcome=take_ratio(selected,owners)
        changed=torch.arange(owners.numel(),device=owners.device)==assets[owners]
        zero=ExactRational.ratio(outcome.numerator.constant(0),outcome.denominator.constant(1))
        outcome=ExactRational(outcome.numerator.select(changed,zero.numerator),outcome.denominator.select(changed,zero.denominator))
        next_entry=self.current_entry_sequence+self.current_entry_sequence.constant(1)
        fault=fault | torch.where(requested,sequence_fault(next_entry),0)
        platform,result=self.platform.apply_transition(sequence,balances,outcome,economic&(fault==0),
            torch.zeros_like(economic),association_available)
        applicable=((outcome_kind==OUT_EVIDENCE)&((tag==3)|(tag==7)|(tag==8)|(tag==9))) | (outcome_kind==OUT_PENDING)
        use=(requested&applicable)|result.applied
        fault=fault | result.fault
        use=use&(fault==0)
        order=_take(self.lifecycle.orders,index)
        fill=_take(self.lifecycle.fills,index)
        order_at,found,lookup_fault=self.orders.lookup(order)
        fault=fault | torch.where(use&((tag==3)|(tag==7)|(economic&(self.lifecycle.economic_tags[root,local]==0))),
            Numbers.combine_fault_rows(lookup_fault),0)
        old_open=self.orders.flags[root,order_at]
        old_applied=self.orders.economic_applied[root,order_at]
        old_request=self.orders.requests[root,order_at]
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
        book=self.reservations.definitive(request,accepted | is_order_economic,old_applied | is_order_economic,
            next_entry,confirm|convert)
        fault=fault | book.fault
        order_event=(lifecycle_use&((tag==7)|accepted))|(use&is_order_economic)
        open_flag=torch.where((tag==7)|accepted,self.lifecycle.open_orders[root,local],old_open)
        request=torch.where(accepted&bound,binding,old_request)
        orders,error=self.orders.set(order,self.lifecycle.conditions[root,local],open_flag,next_entry,order_event,
            request=request,economic=old_applied | is_order_economic)
        fault=fault | torch.where(order_event,error,0)
        trade_event=lifecycle_use&(tag==8)
        trades,error=self.trades.set(fill,self.lifecycle.conditions[root,local],self.lifecycle.pending_trades[root,local],
            next_entry,trade_event,terminal_sticky=True)
        fault=fault | torch.where(trade_event,error,0)
        success=use&(fault==0)
        valid=fault==0
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
        return replace(self,platform=platform,reservations=coordinate(book,self.reservations,valid),
            orders=coordinate(orders,self.orders,valid),trades=coordinate(trades,self.trades,valid),
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
