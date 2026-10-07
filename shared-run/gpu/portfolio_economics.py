"""公共账户不依赖报价的经济读面；WCM覆盖全部经济市场，独立于模型scope。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational
from success_pool import select
from trade_success import sum_rows
from market_economic_read import read_market_economics
from liability_read import read_economic_liabilities
from rebate_read import read_rebates


@dataclass(frozen=True)
class PortfolioEconomicsRead:
    amounts: tuple[ExactRational,...]  # WCM/other residual/other receivable/other liability。
    known: torch.Tensor
    fault: torch.Tensor


def read_portfolio_economics(ledger,*,economic_prefix_known,success_prefix_known,settlement_prefix_known,
        rebate_prefix_known,economic_liability_prefix_known):
    roots=ledger.fault.numel();device=ledger.fault.device
    if any(value.shape!=(roots,) or value.dtype!=torch.bool or value.device!=device for value in
            (economic_prefix_known,success_prefix_known,settlement_prefix_known,rebate_prefix_known,economic_liability_prefix_known)):
        raise TypeError('账户经济须同根的实际完整经济/成功/结算/返佣/负债历史')
    integer=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))
    zero=ExactRational(integer,integer.constant(1))
    wcm=zero;other=zero;known=torch.zeros((roots,4),dtype=torch.bool,device=device)
    fault=ledger.fault.clone()
    if ledger.other_cash is not None:
        other=ledger.other_cash
        known[:,1]=economic_prefix_known
        fault|=other.fault
    # 这两项为原AccountEconomicsRead的明确业务零，不是缺其它来源的占位。
    known[:,2:]=True
    population=ledger.projected_markets
    if population is not None:
        conditions=population.conditions[None,:].expand(roots,-1)
        present=population.present
        market=read_market_economics(ledger,conditions,present,economic_prefix_known=economic_prefix_known,
            success_prefix_known=success_prefix_known,settlement_prefix_known=settlement_prefix_known)
        liabilities=read_economic_liabilities(ledger,conditions,present,prefix_known=economic_liability_prefix_known)
        # Contribution/Day不等待Trade或Economic，可能先于窄ProjectedMarkets人口。
        # 全账户确认未付须覆盖真实日归属的完整condition目录，不能借模型/市场投影筛掉。
        days=ledger.rebate_days
        rebate_conditions=(days.condition_slots if days is not None else
            torch.empty((roots,0),dtype=torch.int64,device=device))
        rebate=read_rebates(ledger,rebate_conditions,torch.ones_like(rebate_conditions,dtype=torch.bool),rebate_prefix_known)
        unpaid_rebate=sum_rows(rebate.confirmed_unpaid,roots,2)
        maker_fee=zero;fees_known=torch.zeros(roots,dtype=torch.bool,device=device)
        costs=ledger.settlement_costs
        if costs is not None:
            members=costs.occupied&costs.maker_category
            fee=costs.recognized-costs.paid
            absent=ExactRational(fee.numerator.constant(0),fee.denominator.constant(1))
            maker_fee=sum_rows(select(fee,members.flatten(),absent),roots,members.size(1))
            fees_known=costs.known
            fault|=costs.fault|maker_fee.fault
        wcm=ledger.maker_cash+market.amounts[0]+market.amounts[1]+unpaid_rebate-liabilities.taker-maker_fee
        known[:,0]=(economic_prefix_known&success_prefix_known&settlement_prefix_known&rebate_prefix_known
            &economic_liability_prefix_known&population.known&market.known[:,:2].all(1)&liabilities.known
            &rebate.amounts_known.all(1)&fees_known)
        fault|=population.fault|market.fault|liabilities.fault|rebate.fault|unpaid_rebate.fault|wcm.fault
    return PortfolioEconomicsRead((wcm,other,zero,zero),known,fault)
