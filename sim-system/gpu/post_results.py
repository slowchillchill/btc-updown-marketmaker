"""原Post逐项实际结果正文；请求末项封口前保留此前腿，不重放或撤回效果。

result_refs由原请求材料目录供给，只是正文位置。订单/成交/账户身份仍来自
原唯一分配器；此目录不能用于推断平台或Ledger当前余额。
"""
from dataclasses import dataclass, fields, replace
import torch
from queues import ExactInt, ExactRational, _repeat, _take, _columns
from base_orders import take_quantity, put_quantity
from fill_pool import _put_integer
from asset_buy import take_ratio


@dataclass(frozen=True)
class PostItemResult:
    quantity: object
    order_sequence: object
    fee: object
    accepted_sequence: object
    outcomes: torch.Tensor  # Accepted/Rejected/EvidenceUnknown=0/1/2。
    reasons: torch.Tensor
    unknown_reason: torch.Tensor
    conditions: torch.Tensor
    assets: torch.Tensor
    processed_at: torch.Tensor
    requests: torch.Tensor
    ordinals: torch.Tensor
    items: torch.Tensor
    present: torch.Tensor
    fill_sequences: object
    fill_bases: object
    fill_quotes: object
    fill_prices: object
    fill_account_sequences: object
    fill_present: torch.Tensor  # [roots,maximum_fills]
    fill_economic_present: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PostResults:
    values: PostItemResult  # 根×正文槽；fill字段另有原逐fill轴。
    roots: int
    capacity: int
    fill_capacity: int

    @classmethod
    def empty(cls, quantity, capacity=1, fill_capacity=1):
        roots=quantity.fault.numel();device=quantity.fault.device
        zero=quantity.accepted_base_units.constant(0)
        flat=_repeat(zero,capacity); fill=_repeat(flat,fill_capacity)
        qty=replace(quantity,**{f.name:torch.zeros(roots*capacity,dtype=getattr(quantity,f.name).dtype,device=device)
            if isinstance(getattr(quantity,f.name),torch.Tensor) else flat for f in fields(quantity)})
        scalar=torch.full((roots*capacity,),-1,dtype=torch.int64,device=device)
        present=torch.zeros(roots*capacity,dtype=torch.bool,device=device)
        values=PostItemResult(qty,flat,flat,flat,scalar,scalar,scalar,scalar,scalar,scalar,scalar,scalar,scalar,present,
            fill,fill,fill,ExactRational(fill,fill.constant(1)),fill,
            torch.zeros((roots*capacity,fill_capacity),dtype=torch.bool,device=device),
            torch.zeros((roots*capacity,fill_capacity),dtype=torch.bool,device=device),torch.zeros_like(flat.fault))
        return cls(values,roots,capacity,fill_capacity)

    def read(self, refs):
        axis=torch.arange(self.roots,device=refs.device)
        safe=refs.clamp(0,self.capacity-1); index=axis*self.capacity+safe
        v=self.values;updates={}
        for field in fields(v):
            value=getattr(v,field.name)
            if field.name=='quantity':updates[field.name]=take_quantity(value,index)
            elif field.name.startswith('fill_') and field.name not in ('fill_present','fill_economic_present'):
                positions=(index[:,None]*self.fill_capacity+torch.arange(self.fill_capacity,device=refs.device)).flatten()
                updates[field.name]=take_ratio(value,positions) if isinstance(value,ExactRational) else _take(value,positions)
            elif isinstance(value,ExactInt):updates[field.name]=_take(value,index)
            else:updates[field.name]=value[index]
        updates["present"]=v.present[index]&(refs>=0)&(refs<self.capacity)
        return replace(v,**updates)

    def store(self, refs, item, enabled):
        axis=torch.arange(self.roots,device=refs.device)
        index=axis*self.capacity+refs.clamp(0,self.capacity-1)
        required=refs+1
        # 原正文槽不重写；重复请求由MutationRequests第一次回执/封口处理。
        fault=item.fault | ((enabled & (refs<0)) | (enabled & (refs>=0) & (refs<self.capacity) & self.values.present[index])).to(torch.int32)
        fault |= (enabled & (required>self.capacity)).to(torch.int32)*2
        fault |= (enabled & (item.fill_present.size(1)>self.fill_capacity)).to(torch.int32)*2
        if item.fill_present.size(1)>self.fill_capacity:
            return self,required,fault
        valid=enabled&(fault==0)
        updates={}
        old=self.values
        for f in fields(old):
            left,right=getattr(old,f.name),getattr(item,f.name)
            if f.name=='quantity':updates[f.name]=put_quantity(left,right,index,valid)
            elif f.name.startswith('fill_') and f.name not in ('fill_present','fill_economic_present'):
                width=item.fill_present.size(1)
                destination=(index[:,None]*self.fill_capacity+torch.arange(width,device=refs.device)).flatten()
                mask=valid.repeat_interleave(width)
                if isinstance(left,ExactRational):updates[f.name]=ExactRational(
                    _put_integer(left.numerator,right.numerator,destination,mask),
                    _put_integer(left.denominator,right.denominator,destination,mask))
                else:updates[f.name]=_put_integer(left,right,destination,mask)
            elif isinstance(left,ExactInt):updates[f.name]=_put_integer(left,right,index,valid)
            elif f.name in ('fill_present','fill_economic_present'):
                width=item.fill_present.size(1)
                incoming=torch.nn.functional.pad(right,(0,self.fill_capacity-width))
                updates[f.name]=left.index_copy(0,index,torch.where(valid[:,None],incoming,left[index]))
            else:updates[f.name]=left.index_copy(0,index,torch.where(valid,right,left[index]))
        return replace(self,values=replace(old,**updates)),required,fault

    def resized(self, capacity, fill_capacity):
        capacity=max(capacity,self.capacity);fill_capacity=max(fill_capacity,self.fill_capacity)
        if (capacity,fill_capacity)==(self.capacity,self.fill_capacity):return self
        axis=torch.arange(self.roots,device=self.values.present.device)
        template=take_quantity(self.values.quantity,axis*self.capacity)
        result=PostResults.empty(template,capacity,fill_capacity)
        # 冷资源复制按原槽承接，不重新分配任何业务身份。
        for slot in range(self.capacity):
            refs=torch.full_like(axis,slot)
            item=self.read(refs)
            result,_,fault=result.store(refs,item,item.present)
        return result

    @classmethod
    def item(cls,pending,terms,effects):
        accepted=effects.acceptance.submission_delta.emitted
        reason=effects.acceptance.rejection_reason
        outcome=torch.where(accepted,0,torch.where(reason==-2,2,1))
        frames=effects.acceptance.fills
        return PostItemResult(effects.prepared.quantity,effects.order.sequence,
            effects.prepared.fee.total_micro,effects.acceptance.submission_delta.sequence,
            outcome,reason,terms.unknown_reason,terms.conditions,terms.assets,pending.effective_at,pending.request_key,
            pending.ordinal,pending.item_ref,accepted|effects.acceptance.rejected,
            _columns([f.fill_sequence for f in frames]),effects.prepared.taker_bases,effects.prepared.taker_quotes,
            effects.prepared.taker_prices,_columns([f.event.economic_delta.sequence for f in frames]),
            torch.stack([f.event.emitted for f in frames],dim=1),
            torch.stack([f.event.economic_delta.emitted for f in frames],dim=1),effects.fault)


def coordinate_results(candidate,original,committed):
    updates={}
    for field in fields(original.values):
        left,right=getattr(original.values,field.name),getattr(candidate.values,field.name)
        mask=committed.repeat_interleave(original.capacity)
        if field.name=='quantity':
            updates[field.name]=put_quantity(left,right,torch.arange(mask.numel(),device=mask.device),mask)
        elif field.name.startswith('fill_') and field.name not in ('fill_present','fill_economic_present'):
            mask=mask.repeat_interleave(original.fill_capacity)
            updates[field.name]=ExactRational(right.numerator.select(mask,left.numerator),
                right.denominator.select(mask,left.denominator)) if isinstance(left,ExactRational) else right.select(mask,left)
        elif isinstance(left,ExactInt):updates[field.name]=right.select(mask,left)
        else:updates[field.name]=torch.where(mask[:,None] if left.ndim==2 else mask,right,left)
    return replace(candidate,values=replace(candidate.values,**updates))
