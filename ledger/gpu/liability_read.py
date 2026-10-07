"""原已提交Economic V2额外负债；完整历史从开户至该冻结经济前缀。

不把经济settlement liability当作Cost Evidence recognized−paid市场读面。
中途缺冷负债历史保持Unknown；不从余额或空物理目录证明零。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class EconomicLiabilityRead:
    settlement: object
    taker: object
    known: torch.Tensor
    fault: torch.Tensor


def read_economic_liabilities(ledger,conditions,present,*,prefix_known):
    roots,width=conditions.shape;device=ledger.fault.device
    if (roots!=ledger.fault.numel() or conditions.dtype!=torch.int64 or conditions.device!=device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or prefix_known.shape!=(roots,) or prefix_known.dtype!=torch.bool or prefix_known.device!=device):
        raise TypeError('经济负债需同根scope及自开户完整经济历史known')
    empty=~present.any(1);population=ledger.columns.row_refs.size(1)
    # 原经济归约只接连续AccountTransition序。自开户完整保留须含全部已提交seq，
    # 中途冷起点即使有余额来源，也不能把遗失负债历史升级Known零。
    retained=ExactInt.from_i64_tensor((ledger.applied&ledger.columns.enabled).to(torch.int64).sum(1))
    coverage=retained.compare(ledger.platform.platform_source_prefix)
    complete=prefix_known&(coverage.order==0)&(coverage.fault==0)
    amounts=[];fault=ledger.fault.clone()
    if population:
        members=((ledger.lifecycle.conditions[:,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
        selected=(ledger.applied&ledger.columns.enabled&members).flatten()
        for source in (ledger.lifecycle.settlement_fee_liability,ledger.lifecycle.actual_taker_fee_liability):
            zero=ExactRational(source.numerator.constant(0),source.denominator.constant(1))
            amount=sum_rows(select(source,selected,zero),roots,population)
            fault|=amount.fault;amounts.append(amount)
    else:
        zero=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))
        amounts=[ExactRational(zero,zero.constant(1))]*2
    fault|=coverage.fault
    return EconomicLiabilityRead(*amounts,complete|empty,fault)
