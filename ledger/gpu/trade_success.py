"""原成交经济应用/成功观察双门；资料缺失不推断金额或提前配平。"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers,ExactRational,_take
from asset_buy import take_ratio
from success_pool import select


@dataclass(frozen=True)
class TradeSuccess:
    has_fact: torch.Tensor
    economic_applied: torch.Tensor
    success_applied: torch.Tensor
    dispositions: torch.Tensor
    conditions: torch.Tensor
    tokens: torch.Tensor
    quantity: object
    cost: object

    @classmethod
    def open(cls,roots,capacity,device):
        blank=torch.zeros((roots,capacity),dtype=torch.bool,device=device)
        columns=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        zero=Numbers.ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        ratio=ExactRational.ratio(zero,zero.constant(1))
        return cls(blank,blank.clone(),blank.clone(),columns,columns.clone(),columns.clone(),ratio,ratio)

    def update(self,slots,condition,token,quantity,cost,disposition,trade_enabled,economic_enabled,economic_applied):
        roots,width=self.has_fact.shape
        root=torch.arange(roots,device=slots.device)
        index=root*width+slots
        previous=self.dispositions[root,slots]
        # 原终局详情不由较旧pending观察覆盖；经济是否应用是另一条事实。
        take_fact=trade_enabled & (~self.has_fact[root,slots] | (previous<2))
        mask=torch.zeros_like(self.has_fact).scatter(1,slots[:,None],take_fact[:,None])
        economic=torch.zeros_like(self.has_fact).scatter(1,slots[:,None],economic_enabled[:,None])
        repeated=root.repeat_interleave(width)
        quantity=select(take_ratio(quantity,repeated),mask.flatten(),self.quantity)
        cost=select(take_ratio(cost,repeated),mask.flatten(),self.cost)
        return replace(self,has_fact=self.has_fact|mask,
            economic_applied=torch.where(economic,economic_applied[:,None],self.economic_applied),
            dispositions=torch.where(mask,disposition[:,None],self.dispositions),
            conditions=torch.where(mask,condition[:,None],self.conditions),tokens=torch.where(mask,token[:,None],self.tokens),
            quantity=quantity,cost=cost)

    def apply_success(self,pools,slots,enabled):
        roots,width=self.has_fact.shape
        root=torch.arange(roots,device=slots.device)
        index=root*width+slots
        use=enabled & self.has_fact[root,slots] & self.economic_applied[root,slots] & (
            self.dispositions[root,slots]==2) & ~self.success_applied[root,slots]
        pools,committed=pools.add(self.conditions[root,slots],self.tokens[root,slots],
            take_ratio(self.quantity,index),take_ratio(self.cost,index),use)
        mask=torch.zeros_like(self.success_applied).scatter(1,slots[:,None],committed[:,None])
        return replace(self,success_applied=self.success_applied|mask),pools
