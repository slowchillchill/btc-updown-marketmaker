"""原Cost累计差额及ExternalCash身份分类，随Ledger共同事务提交。"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from reservation import grow_integer


def _amount(roots,capacity,device):
    zero=ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
    return ExactRational(zero,zero.constant(1))


def _resize_amount(value,roots,old,capacity):
    new=(torch.arange(capacity,device=value.numerator.words.device)[None,:]>=old).expand(roots,capacity).flatten()
    denominator=grow_integer(value.denominator,roots,old,capacity)
    return ExactRational(grow_integer(value.numerator,roots,old,capacity),
        denominator.constant(1).select(new,denominator))


def _newer(left,right):
    """原完整SourceOrder：(tag,u64 private,ordinal)，编码低/高字不改变比较序。"""
    result=torch.zeros_like(left[...,0],dtype=torch.bool)
    for field in (3,1,2,0):
        result=(left[...,field]>right[...,field])|((left[...,field]==right[...,field])&result)
    return result


@dataclass(frozen=True)
class SettlementCosts:
    identities: torch.Tensor
    occupied: torch.Tensor
    conditions: torch.Tensor
    resolutions: torch.Tensor
    policies: torch.Tensor
    source_orders: torch.Tensor
    maker_category: torch.Tensor
    recognized: object
    paid: object
    known: torch.Tensor
    fault: torch.Tensor
    capacity_required: torch.Tensor

    @classmethod
    def empty(cls,roots,capacity,device,*,known=False):
        if roots<1 or capacity<1:raise ValueError('Cost目录需正根数和物理容量')
        ids=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        occupied=torch.zeros_like(ids,dtype=torch.bool)
        return cls(ids,occupied,ids.clone(),ids.clone(),ids.clone(),
            torch.zeros((roots,capacity,4),dtype=torch.int64,device=device),occupied.clone(),
            _amount(roots,capacity,device),_amount(roots,capacity,device),
            torch.full((roots,),known,dtype=torch.bool,device=device),
            torch.zeros(roots,dtype=torch.int32,device=device),torch.full((roots,),capacity,dtype=torch.int64,device=device))

    def resized(self,capacity):
        roots,old=self.occupied.shape
        if capacity<old:raise ValueError('未退役Cost身份不能缩容')
        return replace(self,**{name:torch.nn.functional.pad(getattr(self,name),(0,capacity-old),value=-1)
            for name in ('identities','conditions','resolutions','policies')},
            occupied=torch.nn.functional.pad(self.occupied,(0,capacity-old),value=False),
            maker_category=torch.nn.functional.pad(self.maker_category,(0,capacity-old),value=False),
            source_orders=torch.nn.functional.pad(self.source_orders,(0,0,0,capacity-old)),
            recognized=_resize_amount(self.recognized,roots,old,capacity),
            paid=_resize_amount(self.paid,roots,old,capacity),capacity_required=torch.full_like(self.capacity_required,capacity))

    def observe(self,identity,condition,resolution,policy,recognized,paid,has_correction,correction,
                source_order,resolution_maker,enabled):
        roots,width=self.occupied.shape;root=torch.arange(roots,device=identity.device)
        matches=self.occupied&(self.identities==identity[:,None]);found=matches.any(1)
        prior=self.occupied&(self.identities==correction[:,None]);prior_found=prior.any(1)
        prior_at=prior.to(torch.int64).argmax(1)
        predecessor=~has_correction|prior_found
        old_at=matches.to(torch.int64).argmax(1);free=(~self.occupied).to(torch.int64).argmax(1)
        at=torch.where(found,old_at,free)
        required=torch.where(enabled&predecessor&~found&self.occupied.all(1),width+1,width)
        error=((identity<0)|(condition<0)|(resolution<0)|(policy<0)|
            (has_correction&(correction<0))|((source_order<0)|(source_order>0xffffffff)).any(1)).to(torch.int32)
        fault=self.fault|torch.where(enabled,error|recognized.fault|paid.fault,0)
        ready=predecessor&(required<=width)&(fault==0)
        newer=~found|_newer(source_order,self.source_orders[root,old_at])
        use=enabled&ready&newer
        zero=ExactRational(recognized.numerator.constant(0),recognized.denominator.constant(1))
        old_recognized=select(take_ratio(self.recognized,root*width+old_at),found,zero)
        old_paid=select(take_ratio(self.paid,root*width+old_at),found,zero)
        rec_delta=select(recognized-old_recognized,use,zero)
        paid_delta=select(paid-old_paid,use,zero)
        fault|=torch.where(use,rec_delta.fault|paid_delta.fault,0)
        use&=fault==0
        maker=torch.where(has_correction,self.maker_category[root,prior_at],resolution_maker)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        incoming=take_ratio(recognized,root.repeat_interleave(width))
        incoming_paid=take_ratio(paid,root.repeat_interleave(width))
        candidate=replace(self,identities=torch.where(mask,identity[:,None],self.identities),occupied=self.occupied|mask,
            conditions=torch.where(mask,condition[:,None],self.conditions),
            resolutions=torch.where(mask,resolution[:,None],self.resolutions),
            policies=torch.where(mask,policy[:,None],self.policies),
            source_orders=torch.where(mask[:,:,None],source_order[:,None,:],self.source_orders),
            maker_category=torch.where(mask,maker[:,None],self.maker_category),
            recognized=select(incoming,mask.flatten(),self.recognized),paid=select(incoming_paid,mask.flatten(),self.paid),
            fault=fault,capacity_required=torch.maximum(self.capacity_required,required))
        return candidate,ready&(fault==0),rec_delta,paid_delta

    def read(self,conditions,present):
        roots,width=self.occupied.shape
        members=((self.conditions[:,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)&self.occupied
        from trade_success import sum_rows
        zero=ExactRational(self.recognized.numerator.constant(0),self.recognized.denominator.constant(1))
        recognized=sum_rows(select(self.recognized,members.flatten(),zero),roots,width)
        paid=sum_rows(select(self.paid,members.flatten(),zero),roots,width)
        return recognized,paid,self.known|~present.any(1),self.fault|recognized.fault|paid.fault

    def read_markets(self,roots,conditions):
        """同一目录按实际市场读取；代表资产顺序不替代condition身份。"""
        from trade_success import sum_rows
        width=self.occupied.size(1)
        index=roots[:,None]*width+torch.arange(width,device=roots.device)[None,:]
        recognized=take_ratio(self.recognized,index.flatten())
        paid=take_ratio(self.paid,index.flatten())
        member=self.occupied[roots] & (self.conditions[roots]==conditions[:,None])
        zero=ExactRational(recognized.numerator.constant(0),recognized.denominator.constant(1))
        recognized=sum_rows(select(recognized,member.flatten(),zero),roots.numel(),width)
        paid=sum_rows(select(paid,member.flatten(),zero),roots.numel(),width)
        return recognized,paid,self.known[roots],self.fault[roots]|recognized.fault|paid.fault


@dataclass(frozen=True)
class ExternalCashIdentities:
    identities: torch.Tensor
    occupied: torch.Tensor
    maker_category: torch.Tensor
    known: torch.Tensor
    fault: torch.Tensor
    capacity_required: torch.Tensor

    @classmethod
    def empty(cls,roots,capacity,device,*,known=False):
        if roots<1 or capacity<1:raise ValueError('ExternalCash目录需正根数和物理容量')
        ids=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        flags=torch.zeros_like(ids,dtype=torch.bool)
        return cls(ids,flags,flags.clone(),torch.full((roots,),known,dtype=torch.bool,device=device),
            torch.zeros(roots,dtype=torch.int32,device=device),torch.full((roots,),capacity,dtype=torch.int64,device=device))

    def resized(self,capacity):
        old=self.occupied.size(1)
        if capacity<old:raise ValueError('未退役ExternalCash身份不能缩容')
        return replace(self,identities=torch.nn.functional.pad(self.identities,(0,capacity-old),value=-1),
            occupied=torch.nn.functional.pad(self.occupied,(0,capacity-old),value=False),
            maker_category=torch.nn.functional.pad(self.maker_category,(0,capacity-old),value=False),
            capacity_required=torch.full_like(self.capacity_required,capacity))

    def lookup(self,identity):
        root=torch.arange(self.occupied.size(0),device=identity.device)
        same=self.occupied&(self.identities==identity[:,None]);found=same.any(1)
        at=same.to(torch.int64).argmax(1)
        return found,self.maker_category[root,at]

    def observe(self,identity,amount,has_correction,correction,enabled):
        width=self.occupied.size(1)
        same=self.occupied&(self.identities==identity[:,None]);found=same.any(1)
        prior_found,prior_maker=self.lookup(correction)
        predecessor=~has_correction|prior_found
        at=torch.where(found,same.to(torch.int64).argmax(1),(~self.occupied).to(torch.int64).argmax(1))
        required=torch.where(enabled&predecessor&~found&self.occupied.all(1),width+1,width)
        fault=self.fault|torch.where(enabled,amount.fault|((identity<0)|(has_correction&(correction<0))).to(torch.int32),0)
        ready=predecessor&(required<=width)&(fault==0)
        use=enabled&ready
        maker=torch.where(has_correction,prior_maker,amount.numerator.sign<0)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        return replace(self,identities=torch.where(mask,identity[:,None],self.identities),occupied=self.occupied|mask,
            maker_category=torch.where(mask,maker[:,None],self.maker_category),fault=fault,
            capacity_required=torch.maximum(self.capacity_required,required)),ready
