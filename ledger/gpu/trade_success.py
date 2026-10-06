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
    unpaired_contribution: object

    @classmethod
    def open(cls,roots,capacity,device):
        blank=torch.zeros((roots,capacity),dtype=torch.bool,device=device)
        columns=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        zero=Numbers.ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        ratio=ExactRational.ratio(zero,zero.constant(1))
        return cls(blank,blank.clone(),blank.clone(),columns,columns.clone(),columns.clone(),ratio,ratio,ratio)

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

    def apply_success(self,pools,directory,slots,enabled):
        roots,width=self.has_fact.shape
        root=torch.arange(roots,device=slots.device)
        index=root*width+slots
        use=enabled & self.has_fact[root,slots] & self.economic_applied[root,slots] & (
            self.dispositions[root,slots]==2) & ~self.success_applied[root,slots]
        pools,committed=pools.add(self.conditions[root,slots],self.tokens[root,slots],
            take_ratio(self.quantity,index),take_ratio(self.cost,index),use)
        mask=torch.zeros_like(self.success_applied).scatter(1,slots[:,None],committed[:,None])
        empty=ExactRational.ratio(self.quantity.numerator.constant(0),self.quantity.denominator.constant(1))
        contributions=self.unpaired_contribution+select(self.quantity,mask.flatten(),empty)
        candidate=replace(self,success_applied=self.success_applied|mask,unpaired_contribution=contributions)
        return candidate.allocate_contributions(pools,directory,slots,committed)

    def allocate_contributions(self,pools,directory,slots,enabled):
        from device_identity import sim_trade_order_key
        from success_pool import attribution
        roots,width=self.has_fact.shape
        root=torch.arange(roots,device=slots.device)
        keys=sim_trade_order_key(directory.keys)
        error=Numbers.combine_fault_rows(torch.where(directory.occupied,keys.fault.reshape(roots,width),0))
        order=torch.arange(width,device=slots.device).expand(roots,width)
        # 固定同账户Trade命名空间，按原sim-N字符串字节序；10必须排在2前。
        for byte in range(19,-1,-1):
            column=keys.bytes.reshape(roots,width,20)[:,:,byte]
            order=order.gather(1,column.gather(1,order).argsort(dim=1,stable=True))
        inverse=order.argsort(dim=1)
        condition=self.conditions[root,slots]
        token=self.tokens[root,slots]
        matches=(pools.asset_roots[:,None]==root[None,:]) & (pools.condition_slots[:,None]==condition[None,:]) & (
            pools.token_slots[:,None]==token[None,:])
        first=matches.to(torch.int32).argmax(dim=0)
        contributions=self.unpaired_contribution
        for asset in [first,pools.counterpart[first]]:
            member=self.success_applied & (self.conditions==condition[:,None]) & (self.tokens==pools.token_slots[asset][:,None])
            sorted_member=member.gather(1,order)
            positions=(root[:,None]*width+order).flatten()
            sorted_contribution=take_ratio(contributions,positions)
            capacities=[]
            for column in range(width):
                at=root*width+column
                value=take_ratio(sorted_contribution,at)
                zero=ExactRational.ratio(value.numerator.constant(0),value.denominator.constant(1))
                capacities.append(select(value,sorted_member[:,column],zero))
            allocated,fault=attribution().allocate_ordered_device(take_ratio(pools.quantity,asset),tuple(capacities),enabled)
            error=error | torch.where(enabled,fault,0)
            from queues import _columns
            packed=ExactRational(_columns([value.numerator for value in allocated]),_columns([value.denominator for value in allocated]))
            original_positions=(root[:,None]*width+inverse).flatten()
            unsorted=take_ratio(packed,original_positions)
            contributions=select(unsorted,(member & enabled[:,None]).flatten(),contributions)
        return replace(self,unpaired_contribution=contributions),replace(pools,fault=pools.fault | torch.where(enabled,error,0))
