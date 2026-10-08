"""原单订单Base Query的阶段5候选；查询不改变订单、资金或经济序。

保留水位必须来自显式RetentionAdvance，不从时钟或页面退役推断。
Query结果与请求完成、实际Direct反馈仍须由外层共同提交。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers,ExactRational


@dataclass(frozen=True)
class OrderQueryHistory:
    retained_from: torch.Tensor
    has_retained: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls,reference):
        """真实新SIM根尚未收到RetentionAdvance；不是历史根的缺省恢复来源。"""
        return cls(torch.zeros_like(reference),torch.zeros_like(reference,dtype=torch.bool),
            torch.zeros_like(reference,dtype=torch.int32))

    def advance(self,retained_from,has_retained,enabled):
        for value,dtype in ((retained_from,torch.int64),(has_retained,torch.bool),(enabled,torch.bool)):
            if value.shape!=self.retained_from.shape or value.dtype!=dtype or value.device!=self.retained_from.device:
                raise TypeError('显式订单保留水位须同设备根列')
        invalid=self.has_retained&(~has_retained|(retained_from<self.retained_from))
        fault=self.fault|(enabled&invalid).to(torch.int32)
        use=enabled&(fault==0)
        return replace(self,retained_from=torch.where(use,retained_from,self.retained_from),
            has_retained=torch.where(use,has_retained,self.has_retained),fault=fault)


@dataclass(frozen=True)
class BaseQueryTarget:
    item_refs: torch.Tensor
    order_sequence: object  # 已按原完整平台身份目录解析；不存在的合法身份不造订单。
    fault: torch.Tensor


@dataclass(frozen=True)
class PendingOrderQuery:
    pending: object
    order: object
    outcome: torch.Tensor  # Found/NotFound/OutOfRetention=0/1/2。
    state: torch.Tensor  # 原Open/Matched/Dust/Resolved/Canceled/Expired=0..5。
    matched: object
    active_quantity: object
    retained_from: torch.Tensor
    processed: torch.Tensor
    response: object
    fault: torch.Tensor


def query_pending_base(requests,orders,target,retained_from,has_retained,result_refs,enabled):
    pending=requests.next_pending()
    shape=(requests.roots,);device=requests.keys.device
    for value,dtype in ((retained_from,torch.int64),(has_retained,torch.bool),(enabled,torch.bool)):
        if value.shape!=shape or value.dtype!=dtype or value.device!=device:
            raise TypeError('单订单查询时钟/水位/人口须同设备根列')
    active=pending.exists&enabled
    read=orders.find(target.order_sequence)
    quantity=read.quantity
    remainder=quantity.pending_maker_depletion_base_units+quantity.open_base_units
    live=remainder.sign>0
    state=torch.where(quantity.market_resolved_canceled_base_units.sign>0,3,
        torch.where(quantity.venue_dust_closed_base_units.sign>0,2,
        torch.where(quantity.canceled_base_units.sign>0,4,
        torch.where(quantity.expired_base_units.sign>0,5,1))))
    state=torch.where(live,0,state).to(torch.int64)
    outside=has_retained&(~read.present|(~live&(read.last_updated_at<retained_from)))
    outcome=torch.where(outside,2,torch.where(read.present,0,1)).to(torch.int64)
    fault=requests.fault|orders.fault|torch.where(active,target.fault|read.fault|
        (target.item_refs!=pending.item_ref).to(torch.int32),0)
    fault|=torch.where(active&read.present,quantity.fault|remainder.fault,0)
    processed=active&(fault==0)
    candidate,response=requests.complete(pending,result_refs,processed,fault)
    fault|=response.fault
    processed&=fault==0
    matched_units=quantity.actual_matched_base_units.select(quantity.quantity_kind==1,quantity.matched_base_units)
    matched=ExactRational.ratio(matched_units,matched_units.constant(1_000_000))
    unfilled=ExactRational.ratio(remainder,remainder.constant(1_000_000))
    return candidate,PendingOrderQuery(pending,read,outcome,state,matched,unfilled,retained_from,
        processed,replace(response,emitted=response.emitted&(fault==0),fault=fault),fault)


def run_pending_base_query(*args,**kwargs):
    return Numbers.run_exact_batch(lambda:query_pending_base(*args,**kwargs))
