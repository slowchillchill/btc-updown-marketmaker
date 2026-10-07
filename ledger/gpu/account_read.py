"""原Ledger市场占款/all-in及开放订单/待成交计数；不借Registry人口推账本事实。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows
from reservation import BEFORE_SEND,SUBMITTED,PLATFORM_PENDING,RESERVED,sequence_fault


def _zero(rows,device):
    integer=ExactInt.from_i64_tensor(torch.zeros(rows,dtype=torch.int64,device=device))
    return ExactRational(integer,integer.constant(1))


def _sum(value,member,roots,width):
    if not width:return _zero(roots,member.device)
    zero=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
    return sum_rows(select(value,member.flatten(),zero),roots,width)


@dataclass(frozen=True)
class MarketBalanceOrigin:
    """同一实际冷checkpoint的市场本金/费与平台经济prefix；共享只读业务来源。

    known明确表示该冷起点市场图完整，空目录不自行证明完整。
    后续只加source_prefix之后的已应用原经济列；下一根安装须创建新来源。
    """
    conditions: torch.Tensor
    principal: object       # [roots*markets]
    fee: object
    source_prefix: object   # [roots]，此余额已经包括截至该平台序的经济事实。
    known: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def open(cls,conditions,principal,fee,source_prefix,known):
        roots=source_prefix.sign.numel();count=conditions.numel();device=conditions.device
        if (conditions.ndim!=1 or conditions.dtype!=torch.int64 or source_prefix.words.device!=device
                or known.shape!=(roots,) or known.dtype!=torch.bool or known.device!=device
                or principal.numerator.sign.numel()!=roots*count or fee.numerator.sign.numel()!=roots*count
                or principal.numerator.words.device!=device or fee.numerator.words.device!=device):
            raise TypeError('市场冷余额须原condition目录、同根精确本金/fee/prefix及完整历史known')
        duplicates=(conditions[:,None]==conditions[None,:])&~torch.eye(count,dtype=torch.bool,device=device)
        fault=sequence_fault(source_prefix)|((conditions<0).any()|duplicates.any()).to(torch.int32)
        fault|=Numbers.combine_fault_rows((principal.fault|fee.fault).reshape(roots,count))
        def clone(value):
            if isinstance(value,ExactInt):return ExactInt(value.words.clone(),value.sign.clone(),value.fault.clone())
            return ExactRational(clone(value.numerator),clone(value.denominator))
        return cls(conditions.clone(),clone(principal),clone(fee),clone(source_prefix),known.clone(),fault)

    @classmethod
    def zero_opening(cls,source_prefix,*,known):
        """仅实际确认市场余额全零的冷起点使用；调用者明确提供完整性。"""
        device=source_prefix.words.device
        return cls.open(torch.empty(0,dtype=torch.int64,device=device),_zero(0,device),_zero(0,device),source_prefix,known)


@dataclass(frozen=True)
class MarketAccountRead:
    all_in: object
    known: torch.Tensor
    counts: torch.Tensor  # [roots,2]，原Ledger open_order_count/pending_fill_count。
    counts_known: torch.Tensor
    fault: torch.Tensor


def read_market_account(ledger,conditions,present,*,history_known):
    roots,width=conditions.shape;device=conditions.device
    if (roots!=ledger.fault.numel() or conditions.dtype!=torch.int64 or conditions.device!=ledger.fault.device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or history_known.shape!=(roots,) or history_known.dtype!=torch.bool or history_known.device!=device):
        raise TypeError('账本市场读面需同根原scope和完整冷账户历史known')
    empty=~present.any(1);fault=ledger.fault.clone()
    origin=getattr(ledger,'market_balance_origin',None)
    principal=_zero(roots,device);fee=_zero(roots,device)
    balance_known=empty.clone()
    prefix=ledger.platform.platform_source_prefix
    if origin is not None:
        count=origin.conditions.numel()
        member=((origin.conditions[None,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
        principal=_sum(origin.principal,member,roots,count)
        fee=_sum(origin.fee,member,roots,count)
        prefix=origin.source_prefix
        ordering=ledger.platform.platform_source_prefix.compare(prefix)
        fault|=origin.fault|ordering.fault|(ordering.order<0).to(torch.int32)
        balance_known=origin.known|empty
    columns=ledger.columns;population=columns.row_refs.size(1)
    if population:
        member=((ledger.lifecycle.conditions[:,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
        previous=_take(prefix,torch.arange(roots,device=device).repeat_interleave(population))
        ordering=columns.sequences.compare(previous)
        selected=member & ledger.applied & columns.enabled & (ordering.order.reshape(roots,population)>0)
        fault|=Numbers.combine_fault_rows(torch.where(ledger.applied&columns.enabled,ordering.fault.reshape(roots,population),0))
        at=torch.arange(roots*population,device=device)*8
        principal=principal+_sum(take_ratio(columns.balances,at+2),selected,roots,population)
        fee=fee+_sum(take_ratio(columns.balances,at+3),selected,roots,population)
    book=ledger.reservations
    pending=((book.conditions[:,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
    pending&=(book.request_slots>=0)&(book.outcomes==RESERVED)
    pending&=(book.statuses==BEFORE_SEND)|(book.statuses==SUBMITTED)|(book.statuses==PLATFORM_PENDING)
    pending_amount=_sum(book.principal+book.fee,pending,roots,book.request_slots.size(1))
    costs=ledger.position_costs.read(conditions,present)
    all_in=principal+fee+pending_amount+costs.cost+costs.fee
    known=(balance_known & (costs.known|empty) & (history_known|empty))
    projected=getattr(ledger,'projected_markets',None)
    if projected is not None:
        exists=((projected.conditions[None,:,None]==conditions[:,None,:])
            & present[:,None,:] & projected.present[:,:,None]).flatten(1).any(1)
        absent=projected.known&~exists&history_known
        all_in=select(_zero(roots,device),absent,all_in)
        known|=absent
    counts=[];counts_known=history_known|empty
    for directory in (ledger.orders,ledger.trades):
        selected=directory.occupied & directory.flags
        member=((directory.conditions[:,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
        counts.append((selected&member).to(torch.int64).sum(1))
        counts_known&=empty|~(selected&(directory.conditions<0)).any(1)
    fault|=book.fault|costs.fault|all_in.fault
    return MarketAccountRead(all_in,known,torch.stack(counts,1),counts_known,fault)
