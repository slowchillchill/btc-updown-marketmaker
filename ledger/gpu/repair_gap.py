"""公开FinalSuccess逐资产修复差；不等待经济应用，也不读取成功池余额。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class RepairGapRead:
    quantity: object  # [assets]，真实counterpart成功份额减本腿，逐市场取正部。
    fault: torch.Tensor


def repair_gap(trades,pools):
    roots,width=trades.has_fact.shape
    assets=pools.asset_roots.numel();device=pools.asset_roots.device
    if roots!=pools.fault.numel() or trades.has_fact.device!=device:
        raise ValueError('修复差读取同根同设备公开成交与资产身份')
    if width:
        owner=pools.asset_roots
        member=(trades.has_fact[owner] & (trades.dispositions[owner]==2)
            & (trades.conditions[owner]==pools.condition_slots[:,None])
            & (trades.tokens[owner]==pools.token_slots[:,None]))
        at=owner[:,None]*width+torch.arange(width,device=device)[None,:]
        value=take_ratio(trades.quantity,at.flatten())
        zero=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        quantity=sum_rows(select(value,member.flatten(),zero),assets,width)
    else:
        integer=ExactInt.from_i64_tensor(torch.zeros(assets,dtype=torch.int64,device=device))
        quantity=ExactRational(integer,integer.constant(1))
    difference=take_ratio(quantity,pools.counterpart)-quantity
    zero=ExactRational(difference.numerator.constant(0),difference.denominator.constant(1))
    positive=select(difference,difference.numerator.sign>0,zero)
    fault=pools.fault|Numbers.combine_fault_rows(torch.where(pools.asset_roots[None,:]==
        torch.arange(roots,device=device)[:,None],(quantity.fault|difference.fault|positive.fault)[None,:],0))
    return RepairGapRead(positive,fault)
