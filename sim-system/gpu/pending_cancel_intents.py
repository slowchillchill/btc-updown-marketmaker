"""原取消正文的设备目录；市场请求到实际processing才读取活动订单。

显式目标已由原请求登记去重，input_item_refs保留全部原引用。市场目录只保存
condition，ExpandedCancel保留本次pending的实际人口，容量重试不重新展开。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt, _take


@dataclass(frozen=True)
class CancelFailureScenario:
    request_keys: torch.Tensor
    order_sequences: object
    input_sequences: object  # 原显式配置输入序，仅用于原scenario来源，不分配新身份。
    present: torch.Tensor
    fault: torch.Tensor

    def matches(self, request, order):
        from registry import identity_matches
        roots,width=self.present.shape
        equal,error=identity_matches(order,self.order_sequences,roots,1,width)
        active=self.present & (self.request_keys==request[:,None]) & equal[:,0,:]
        from queues import Numbers
        return active.any(1),self.fault|Numbers.combine_fault_rows(torch.where(active,error[:,0,:],0))


@dataclass(frozen=True)
class CancelPendingTarget:
    order_sequence: object
    target_ordinal: object
    input_item_refs: object
    active: torch.Tensor


@dataclass(frozen=True)
class ExpandedCancel:
    pending: object
    targets: tuple
    fault: torch.Tensor


@dataclass(frozen=True)
class PendingCancelIntents:
    market: torch.Tensor  # [roots,items]，原CancelMarket，不是Actor cutoff对象人口。
    conditions: torch.Tensor
    order_sequences: object
    target_ordinals: object
    input_item_refs: object
    fault: torch.Tensor

    def expand(self, pending, orders, enabled):
        from fields import RecordList
        from cancel_objects import sim_identity_order
        roots,width=self.market.shape
        axis=torch.arange(roots,device=self.market.device)
        safe=pending.item_ref.clamp(0,width-1)
        coordinates=axis*width+safe
        market=self.market[axis,safe]
        condition=self.conditions[axis,safe]
        active=pending.exists & enabled
        fault=pending.fault|self.fault
        fault |= (active & ((pending.item_ref<0)|(pending.item_ref>=width))).to(torch.int32)
        explicit=_take(self.order_sequences,coordinates)
        ordinal=_take(self.target_ordinals,coordinates)
        references=self.input_item_refs.present.size(1)
        positions=(coordinates[:,None]*references+torch.arange(references,device=axis.device)).flatten()
        refs=RecordList(tuple(value[positions] for value in self.input_item_refs.fields),
            self.input_item_refs.present[coordinates])
        quantity=orders.quantities
        population=orders.occupied & (quantity.quantity_kind.reshape(roots,-1)==0)
        population &= ((quantity.open_base_units.sign>0)|(quantity.pending_maker_depletion_base_units.sign>0)).reshape(roots,-1)
        population &= (orders.conditions==condition[:,None]) & active[:,None] & market[:,None]
        ranks,error=sim_identity_order(orders.sequences,orders.occupied)
        fault |= error
        order=ranks.argsort(dim=1)
        order=order.gather(1,(~population.gather(1,order)).to(torch.int32).argsort(dim=1,stable=True))
        targets=[]
        for column in range(orders.capacity):
            slot=order[:,column]
            sequence=_take(orders.sequences,axis*orders.capacity+slot)
            sequence=sequence.select(market,explicit)
            present=population.gather(1,slot[:,None])[:,0] | (active & ~market & (column==0))
            target_ordinal=ExactInt.from_i64_tensor(torch.full_like(axis,column)).select(market,ordinal)
            item_refs=RecordList(refs.fields,refs.present & ~market[:,None] & (column==0))
            targets.append(CancelPendingTarget(sequence,target_ordinal,item_refs,present))
        return ExpandedCancel(pending,tuple(targets),fault)
