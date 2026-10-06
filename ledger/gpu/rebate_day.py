"""原日结果的有限condition归属与全额付款候选，不生成现金delta。

condition目录由原固定身份按字符串序冷绑定；day/kind和完整贡献前驱来自真实NOE。
容量不足/前驱缺失不消费原记录，宿主在物理扩池/前驱接纳后重试。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers,ExactInt,ExactRational,_take
from asset_buy import take_ratio
from success_pool import select,attribution
from reservation import grow_integer


def sum_population(values,rows,width):
    """精确加法树；没有模型投影或逐项舍入。"""
    if not width:
        zero=ExactInt.from_i64_tensor(torch.zeros(rows,dtype=torch.int64,device=values.numerator.words.device))
        return ExactRational.ratio(zero,zero.constant(1))
    while width>1:
        pairs=(width+1)//2
        root=torch.arange(rows,device=values.numerator.words.device)[:,None]*width
        first=root+torch.arange(pairs,device=root.device)[None,:]*2
        second=(first+1).minimum(root+width-1)
        a,b=take_ratio(values,first.flatten()),take_ratio(values,second.flatten())
        zero=ExactRational.ratio(b.numerator.constant(0),b.denominator.constant(1))
        b=select(b,(first+1<root+width).flatten(),zero)
        values=a+b
        width=pairs
    return values


@dataclass(frozen=True)
class RebateDays:
    condition_slots: torch.Tensor  # [roots,conditions]，原condition身份序，非物理添加序。
    occupied: torch.Tensor
    kinds: torch.Tensor
    days: torch.Tensor
    payment_observed: torch.Tensor
    accrued: object
    payable: object
    locked: object
    condition_contributions: object
    condition_payable: object
    condition_locked: object
    fault: torch.Tensor
    capacity_required: torch.Tensor

    @classmethod
    def empty(cls,conditions,capacity):
        if not conditions.is_cuda or conditions.dtype!=torch.int64 or conditions.ndim!=2 or capacity<1:
            raise TypeError('日归属需要同设备int64[roots,conditions]身份目录及正物理容量')
        roots,count=conditions.shape
        empty=torch.zeros((roots,capacity),dtype=torch.bool,device=conditions.device)
        def ratio(population):
            zero=ExactInt.from_i64_tensor(torch.zeros(population,dtype=torch.int64,device=conditions.device))
            return ExactRational.ratio(zero,zero.constant(1))
        value=ratio(roots*capacity)
        by_condition=ratio(roots*capacity*count)
        duplicate=(conditions[:,:,None]==conditions[:,None,:]) & ~torch.eye(count,dtype=torch.bool,device=conditions.device)[None,:,:]
        fault=((conditions<0).any(dim=1)|duplicate.any(dim=2).any(dim=1)).to(torch.int32)
        return cls(conditions,empty,torch.full_like(empty,-1,dtype=torch.int64),torch.zeros_like(empty,dtype=torch.int64),empty.clone(),
            value,value,value,by_condition,by_condition,by_condition,fault,
            torch.full((roots,),capacity,dtype=torch.int64,device=conditions.device))

    def resized(self,capacity):
        roots,old=self.occupied.shape
        count=self.condition_slots.size(1)
        if capacity<old:
            raise ValueError('未退役日目录不能缩小')
        def grow(value,columns):
            numerator=grow_integer(value.numerator,roots,old*columns,capacity*columns)
            denominator=grow_integer(value.denominator,roots,old*columns,capacity*columns)
            fresh=(torch.arange(capacity*columns,device=self.days.device)[None,:]>=old*columns).expand(roots,-1).flatten()
            denominator=denominator.constant(1).select(fresh,denominator)
            return ExactRational(numerator,denominator)
        return replace(self,occupied=torch.nn.functional.pad(self.occupied,(0,capacity-old),value=False),
            kinds=torch.nn.functional.pad(self.kinds,(0,capacity-old),value=-1),
            days=torch.nn.functional.pad(self.days,(0,capacity-old),value=0),
            payment_observed=torch.nn.functional.pad(self.payment_observed,(0,capacity-old),value=False),
            accrued=grow(self.accrued,1),payable=grow(self.payable,1),locked=grow(self.locked,1),
            condition_contributions=grow(self.condition_contributions,count),condition_payable=grow(self.condition_payable,count),
            condition_locked=grow(self.condition_locked,count),capacity_required=torch.full_like(self.capacity_required,capacity))

    def lookup(self,kind,day):
        matches=self.occupied & (self.kinds==kind[:,None]) & (self.days==day[:,None])
        found=matches.any(dim=1)
        existing=matches.to(torch.int32).argmax(dim=1)
        vacancy=(~self.occupied).to(torch.int32).argmax(dim=1)
        return torch.where(found,existing,vacancy),found

    def condition_weights(self,contributions,kind,day):
        roots,count=self.condition_slots.shape
        population=contributions.occupied.size(1)
        root=torch.arange(roots,device=self.days.device)[:,None,None]
        indices=(root*population+torch.arange(population,device=root.device)[None,None,:]).expand(roots,count,population).flatten()
        amounts=take_ratio(contributions.amounts,indices)
        valid=contributions.occupied & (contributions.kinds==kind[:,None]) & (contributions.days==day[:,None])
        member=valid[:,None,:] & (contributions.conditions[:,None,:]==self.condition_slots[:,:,None])
        zero=ExactRational.ratio(amounts.numerator.constant(0),amounts.denominator.constant(1))
        return sum_population(select(amounts,member.flatten(),zero),roots*count,population)

    def finalize(self,contributions,kind,day,accrued,payable,locked,refs,ref_mask,enabled):
        roots,capacity=self.occupied.shape
        count=self.condition_slots.size(1)
        root=torch.arange(roots,device=enabled.device)
        at,found=self.lookup(kind,day)
        ready=torch.ones_like(enabled)
        refs_count=ref_mask.size(1)
        population=contributions.occupied.size(1)
        if refs_count:
            a=(root[:,None,None]*population+torch.arange(population,device=root.device)[None,None,:]).expand(roots,refs_count,population).flatten()
            b=(root[:,None,None]*refs_count+torch.arange(refs_count,device=root.device)[None,:,None]).expand(roots,refs_count,population).flatten()
            relation=_take(contributions.trades,a).compare(_take(refs,b))
            present=(relation.order.reshape(roots,refs_count,population)==0) & contributions.occupied[:,None,:] & (
                contributions.kinds[:,None,:]==kind[:,None,None]) & (contributions.days[:,None,:]==day[:,None,None])
            ready=(~ref_mask | present.any(dim=2)).all(dim=1)
            reference_fault=Numbers.combine_fault_rows(torch.where(ref_mask[:,:,None],
                relation.fault.reshape(roots,refs_count,population),0).reshape(roots,-1))
        else:
            reference_fault=torch.zeros_like(self.fault)
        fresh=enabled & ~found & ready
        required=torch.where(fresh & self.occupied.all(dim=1),torch.full_like(at,capacity+1),capacity)
        error=accrued.fault | payable.fault | locked.fault | reference_fault | contributions.fault
        error=error | ((kind<0)|(kind>1)|(accrued.numerator.sign<0)|(payable.numerator.sign<0)|(locked.numerator.sign<0)).to(torch.int32)
        fault=self.fault | torch.where(fresh,error,0)
        use=fresh & (required<=capacity) & (fault==0)
        weights=self.condition_weights(contributions,kind,day)
        allocation_pay,fault_pay=attribution().allocate_ordered_packed(payable,weights,count,use)
        allocation_lock,fault_lock=attribution().allocate_ordered_packed(locked,weights,count,use)
        fault=fault | torch.where(use,fault_pay | fault_lock,0)
        use=use & (fault==0)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        def store(value,original):
            return select(take_ratio(value,root.repeat_interleave(capacity)),mask.flatten(),original)
        def store_conditions(value,original):
            indices=(root[:,None,None]*count+torch.arange(count,device=root.device)[None,None,:]).expand(roots,capacity,count).flatten()
            return select(take_ratio(value,indices),mask[:,:,None].expand(roots,capacity,count).flatten(),original)
        return (replace(self,occupied=self.occupied | mask,kinds=torch.where(mask,kind[:,None],self.kinds),
            days=torch.where(mask,day[:,None],self.days),accrued=store(accrued,self.accrued),
            payable=store(payable,self.payable),locked=store(locked,self.locked),
            condition_contributions=store_conditions(weights,self.condition_contributions),
            condition_payable=store_conditions(allocation_pay,self.condition_payable),condition_locked=store_conditions(allocation_lock,self.condition_locked),
            fault=fault,capacity_required=torch.maximum(self.capacity_required,required)),
            enabled & (found | (ready & (required<=capacity))) & (fault==0),use)

    def payment(self,kind,day,paid,enabled):
        roots,capacity=self.occupied.shape
        root=torch.arange(roots,device=enabled.device)
        at,found=self.lookup(kind,day)
        expected=take_ratio(self.payable,root*capacity+at)
        relation=paid.compare(expected)
        # 原B producer只有正全额付款；其它来源能力不从通用NOE类型自动获得。
        requested=enabled & found
        fault=self.fault | torch.where(requested,paid.fault | relation.fault | (
            (paid.numerator.sign<=0)|(relation.order!=0)).to(torch.int32),0)
        use=requested & ~self.payment_observed[root,at] & (fault==0)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        return replace(self,payment_observed=self.payment_observed | mask,fault=fault),requested & (fault==0),use
