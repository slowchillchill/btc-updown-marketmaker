"""原成交经济应用/成功观察双门；资料缺失不推断金额或提前配平。"""
from dataclasses import dataclass,replace,fields
import torch
from queues import Numbers,ExactRational,_take
from asset_buy import take_ratio
from success_pool import select


def sum_rows(value, rows, width):
    """共享精确有理数的固定相邻配对归约；人口来自物理布局，无业务值读回。"""
    while width>1:
        positions=torch.arange(rows,device=value.numerator.words.device)[:,None]*width
        left=torch.arange(0,width,2,device=positions.device)
        right=left+1
        a=take_ratio(value,(positions+left).flatten())
        b=take_ratio(value,(positions+right.clamp_max(width-1)).flatten())
        zero=ExactRational(b.numerator.constant(0),b.denominator.constant(1))
        b=select(b,(right<width).expand(rows,-1).flatten(),zero)
        value=a+b
        width=left.numel()
    return value


@dataclass(frozen=True)
class ExecutedTradeRead:
    quantity: object  # [roots*2]；原公开读面Maker在前、Taker在后。
    principal: object
    actual_fee: object
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class EstimatedRebateRead:
    amount: object  # [roots*2]；原Maker、Taker顺序，不是实际应收delta。
    known: torch.Tensor
    fault: torch.Tensor


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
    details_known: torch.Tensor
    roles: torch.Tensor  # 原Trade枚举Taker=0/MakerInferred=1；不是读面数组序。
    matched_at: torch.Tensor
    immediate_fee: object
    maker_fee_read_kind: torch.Tensor
    maker_fee_equivalent: object

    @classmethod
    def open(cls,roots,capacity,device):
        blank=torch.zeros((roots,capacity),dtype=torch.bool,device=device)
        columns=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        zero=Numbers.ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        ratio=ExactRational.ratio(zero,zero.constant(1))
        return cls(blank,blank.clone(),blank.clone(),columns,columns.clone(),columns.clone(),ratio,ratio,ratio,
            blank.clone(),columns.clone(),torch.zeros_like(columns),ratio,torch.ones_like(columns),ratio)

    def resized(self,capacity):
        from transport import append_root_fields
        roots,old=self.has_fact.shape
        if capacity<old:
            raise ValueError('未退役成交目录不能缩小')
        if capacity==old:
            return self
        blank=type(self).open(roots,capacity-old,self.has_fact.device)
        updates={}
        for member in fields(self):
            a,b=getattr(self,member.name),getattr(blank,member.name)
            updates[member.name]=(torch.cat([a,b],1) if isinstance(a,torch.Tensor)
                else append_root_fields(a,b,roots,old,capacity-old))
        return replace(self,**updates)

    def update(self,slots,condition,token,quantity,cost,disposition,trade_enabled,economic_enabled,economic_applied,
               *,role=None,matched_at=None,immediate_fee=None,maker_fee_read_kind=None,maker_fee_equivalent=None):
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
        details={}
        if role is not None:
            # 来源详情只在真实TradeLifecycle到达时安装。旧冷初始化不伪造未知fee/role。
            detail_take=trade_enabled & (~self.details_known[root,slots] | take_fact)
            detail_mask=torch.zeros_like(self.details_known).scatter(1,slots[:,None],detail_take[:,None])
            equivalent_use=trade_enabled & (maker_fee_read_kind!=1)
            equivalent_mask=torch.zeros_like(self.details_known).scatter(1,slots[:,None],equivalent_use[:,None])
            details=dict(details_known=self.details_known|detail_mask,
                roles=torch.where(detail_mask,role[:,None],self.roles),matched_at=torch.where(detail_mask,matched_at[:,None],self.matched_at),
                immediate_fee=select(take_ratio(immediate_fee,repeated),detail_mask.flatten(),self.immediate_fee),
                maker_fee_read_kind=torch.where(equivalent_mask,maker_fee_read_kind[:,None],self.maker_fee_read_kind),
                maker_fee_equivalent=select(take_ratio(maker_fee_equivalent,repeated),equivalent_mask.flatten(),self.maker_fee_equivalent))
        return replace(self,**details,has_fact=self.has_fact|mask,
            economic_applied=torch.where(economic,economic_applied[:,None],self.economic_applied),
            dispositions=torch.where(mask,disposition[:,None],self.dispositions),
            conditions=torch.where(mask,condition[:,None],self.conditions),tokens=torch.where(mask,token[:,None],self.tokens),
            quantity=quantity,cost=cost)

    def executed(self,scope_conditions,scope_present):
        """原首次Trade详情统计及当前净费，不能用成功池替代执行量/费用。"""
        roots,width=self.has_fact.shape
        if scope_conditions.ndim!=2 or scope_conditions.size(0)!=roots or scope_present.shape!=scope_conditions.shape:
            raise ValueError('执行读scope须原根/condition人口')
        if scope_conditions.dtype!=torch.int64 or scope_present.dtype!=torch.bool or scope_conditions.device!=self.has_fact.device or scope_present.device!=self.has_fact.device:
            raise TypeError('读scope须同设备int64身份和bool存在位')
        member=(self.conditions[:,:,None]==scope_conditions[:,None,:]) & scope_present[:,None,:]
        member=member.any(2)&self.has_fact
        unknown=(member & ~self.details_known).any(1)
        # 原公开Market.executed数组[Maker,Taker]；真实Trade enum次序相反。
        roles=torch.tensor([1,0],device=self.has_fact.device,dtype=torch.int64)
        masks=member[:,None,:] & self.details_known[:,None,:] & (self.roles[:,None,:]==roles[None,:,None])
        positions=(torch.arange(roots,device=masks.device)[:,None,None]*width+
            torch.arange(width,device=masks.device)[None,None,:]).expand(roots,2,width).flatten()
        fields=[]
        zero=ExactRational(self.quantity.numerator.constant(0),self.quantity.denominator.constant(1))
        net_fee=select(self.immediate_fee,self.economic_applied.flatten(),zero)
        for value in [self.quantity,self.cost,net_fee]:
            values=take_ratio(value,positions)
            empty=ExactRational(values.numerator.constant(0),values.denominator.constant(1))
            fields.append(sum_rows(select(values,masks.flatten(),empty),roots*2,width))
        fault=torch.zeros(roots,dtype=torch.int32,device=masks.device)
        for value in fields:
            fault=fault|Numbers.combine_fault_rows(value.fault.reshape(roots,2))
        return ExecutedTradeRead(*fields,(~unknown[:,None]).expand(roots,2),fault)

    def estimated_rebate(self,directory,contributions,scope_conditions,scope_present,*,day=None):
        """原economic_read估计：失败/已贡献人口剔除，未经济应用的公开Trade仍参与。

        Maker只用公开费用等价Known；任一所需未知值令该scope Unavailable。
        原读面固定Maker .2/Taker .08，不引入模型或正式训练参数。
        """
        roots,width=self.has_fact.shape
        if scope_conditions.ndim!=2 or scope_conditions.size(0)!=roots or scope_present.shape!=scope_conditions.shape:
            raise ValueError('估计scope须原根/condition人口')
        if scope_conditions.dtype!=torch.int64 or scope_present.dtype!=torch.bool or scope_conditions.device!=self.has_fact.device or scope_present.device!=self.has_fact.device:
            raise TypeError('估计scope须同设备int64身份和bool存在位')
        member=((self.conditions[:,:,None]==scope_conditions[:,None,:])&scope_present[:,None,:]).any(2)
        member=member&self.has_fact&(self.dispositions<3)
        if day is not None:
            if day.dtype!=torch.int64 or day.shape!=(roots,) or day.device!=member.device:
                raise TypeError('估计day须同设备int64[roots]')
            # 比较日编号，避免极端i64时刻乘回午夜溢出；不增加成交时刻业务门。
            member=member&(~self.details_known | (torch.div(self.matched_at,86_400_000,rounding_mode='floor')==
                torch.div(day,86_400_000,rounding_mode='floor')[:,None]))
        capacity=contributions.occupied.size(1)
        root=torch.arange(roots,device=member.device)[:,None,None]
        trade_index=(root*width+torch.arange(width,device=member.device)[None,:,None]).expand(roots,width,capacity)
        contribution_index=(root*capacity+torch.arange(capacity,device=member.device)[None,None,:]).expand(roots,width,capacity)
        relation=_take(directory.keys,trade_index.flatten()).compare(_take(contributions.trades,contribution_index.flatten()))
        pair=directory.occupied[:,:,None]&contributions.occupied[:,None,:]
        same=(relation.order.reshape(roots,width,capacity)==0)&pair
        # 跨所有日的同kind原trade贡献均剔除，不按condition/token猜关联。
        kinds=torch.arange(2,device=member.device)
        contributed=(same[:,None,:,:] & (contributions.kinds[:,None,None,:]==kinds[None,:,None,None])).any(3)
        roles=1-kinds
        eligible=member[:,None,:]&~contributed
        unknown=(eligible&~self.details_known[:,None,:]).any(2)
        selected=eligible&self.details_known[:,None,:]&(self.roles[:,None,:]==roles[None,:,None])
        unknown=unknown|torch.stack([(selected[:,0,:]&(self.maker_fee_read_kind!=0)).any(1),
            torch.zeros(roots,dtype=torch.bool,device=member.device)],dim=1)
        index=(torch.arange(roots,device=member.device)[:,None,None]*width+
            torch.arange(width,device=member.device)[None,None,:]).expand(roots,2,width).flatten()
        equivalent=take_ratio(self.maker_fee_equivalent,index)
        taker=take_ratio(self.immediate_fee,index)
        maker_mask=(kinds[None,:,None]==0).expand(roots,2,width).flatten()
        fee=select(equivalent,maker_mask,taker)
        rate=ExactRational.ratio(Numbers.ExactInt.from_i64_tensor(
            (20-12*kinds)[None,:,None].expand(roots,2,width).flatten()),fee.denominator.constant(100))
        value=fee*rate
        zero=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        amount=sum_rows(select(value,selected.flatten(),zero),roots*2,width)
        fault=Numbers.combine_fault_rows(torch.where(pair&member[:,:,None],relation.fault.reshape(roots,width,capacity),0).reshape(roots,-1))
        fault=fault|Numbers.combine_fault_rows(amount.fault.reshape(roots,2))|contributions.fault
        return EstimatedRebateRead(amount,~unknown,fault)

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
            zero=ExactRational.ratio(sorted_contribution.numerator.constant(0),sorted_contribution.denominator.constant(1))
            capacities=select(sorted_contribution,sorted_member.flatten(),zero)
            packed,fault=attribution().allocate_ordered_packed(take_ratio(pools.quantity,asset),capacities,width,enabled)
            error=error | torch.where(enabled,fault,0)
            original_positions=(root[:,None]*width+inverse).flatten()
            unsorted=take_ratio(packed,original_positions)
            contributions=select(unsorted,(member & enabled[:,None]).flatten(),contributions)
        return replace(self,unpaired_contribution=contributions),replace(pools,fault=pools.fault | torch.where(enabled,error,0))
