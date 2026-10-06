"""原返佣贡献事实索引；贡献不是现金或应收delta。

同一(kind,day)内按完整成交身份去重。Taker不等待成交成功详情；Maker向量的
前驱条件由调用者按原每个trade.success_applied分别检查，不猜测缺失资料。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers,ExactInt,ExactRational,_take
from asset_buy import take_ratio
from success_pool import select
from reservation import grow_integer


@dataclass(frozen=True)
class RebateContributions:
    trades: object
    occupied: torch.Tensor
    kinds: torch.Tensor
    days: torch.Tensor
    conditions: torch.Tensor
    amounts: object
    fault: torch.Tensor
    capacity_required: torch.Tensor

    @classmethod
    def empty(cls,roots,capacity,device):
        population=torch.zeros((roots,capacity),dtype=torch.bool,device=device)
        zero=ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        amount=ExactRational.ratio(zero,zero.constant(1))
        unknown=torch.full_like(population,-1,dtype=torch.int64)
        return cls(zero,population,unknown,torch.zeros_like(unknown),unknown.clone(),amount,
            torch.zeros(roots,dtype=torch.int32,device=device),torch.full((roots,),capacity,dtype=torch.int64,device=device))

    def resized(self,capacity):
        roots,old=self.occupied.shape
        if capacity<old:
            raise ValueError('未退役贡献目录不能缩小')
        mask=(torch.arange(capacity,device=self.occupied.device)[None,:]>=old).expand(roots,capacity).flatten()
        denominator=grow_integer(self.amounts.denominator,roots,old,capacity)
        denominator=denominator.constant(1).select(mask,denominator)
        return replace(self,trades=grow_integer(self.trades,roots,old,capacity),
            occupied=torch.nn.functional.pad(self.occupied,(0,capacity-old),value=False),
            kinds=torch.nn.functional.pad(self.kinds,(0,capacity-old),value=-1),
            days=torch.nn.functional.pad(self.days,(0,capacity-old),value=0),
            conditions=torch.nn.functional.pad(self.conditions,(0,capacity-old),value=-1),
            amounts=ExactRational(grow_integer(self.amounts.numerator,roots,old,capacity),denominator),
            capacity_required=torch.full_like(self.capacity_required,capacity))

    def add(self,trade,kind,day,condition,amount,enabled):
        roots,width=self.occupied.shape
        root=torch.arange(roots,device=enabled.device)
        expanded=_take(trade,root.repeat_interleave(width))
        relation=self.trades.compare(expanded)
        matches=self.occupied & (relation.order.reshape(roots,width)==0) & (
            self.kinds==kind[:,None]) & (self.days==day[:,None])
        found=matches.any(dim=1)
        at=(~self.occupied).to(torch.int32).argmax(dim=1)
        fresh=enabled & ~found
        required=torch.where(fresh & self.occupied.all(dim=1),torch.full_like(at,width+1),width)
        fault=self.fault | torch.where(enabled,Numbers.combine_fault_rows(relation.fault.reshape(roots,width)),0)
        fault=fault | torch.where(fresh,trade.fault | amount.fault | ((trade.sign<0)|(amount.numerator.sign<0)|
            (condition<0)|(kind<0)|(kind>1)).to(torch.int32),0)
        # 人口容量不足只请求扩池；不把本事实解释为业务拒绝或静默丢弃。
        use=fresh & (fault==0) & (required<=width)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        next_amount=take_ratio(amount,root.repeat_interleave(width))
        return replace(self,trades=expanded.select(mask.flatten(),self.trades),occupied=self.occupied | mask,
            kinds=torch.where(mask,kind[:,None],self.kinds),days=torch.where(mask,day[:,None],self.days),
            conditions=torch.where(mask,condition[:,None],self.conditions),
            amounts=select(next_amount,mask.flatten(),self.amounts),fault=fault,
            capacity_required=torch.maximum(self.capacity_required,required)),required,use
