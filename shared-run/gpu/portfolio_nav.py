"""同冻结全账户NAV；正净孤腿取同公共前缀估值，模型scope不筛市场。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from success_read import net_success
from trade_success import sum_rows
from market_economic_read import read_market_economics
from portfolio_economics import read_portfolio_economics


@dataclass(frozen=True)
class FrozenPortfolioValuation:
    cutoff: torch.Tensor
    prefixes: torch.Tensor
    prices: ExactRational  # 原Ledger资产；Fresh空bid为精确0。
    known: torch.Tensor
    fault: torch.Tensor


class PortfolioValuation:
    """冷绑定原token身份；冻结读取DPT，只返回本Ledger完整资产目录。"""
    def __init__(self,pm,asset_tokens,asset_roots):
        if len(asset_tokens)!=asset_roots.numel() or asset_roots.dtype!=torch.int64 or asset_roots.device!=pm.slots.device:
            raise TypeError('全账户估值须原Ledger资产身份/根及同设备公共列')
        identity={asset['token_id']:index for index,asset in enumerate(pm.assets)}
        if len(identity)!=len(pm.assets):raise ValueError('公共token身份重复')
        self.pm=pm
        self.asset_roots=asset_roots
        self.asset_ids=torch.tensor([identity.get(token,-1) for token in asset_tokens],dtype=torch.int64,device=pm.slots.device)

    def freeze(self,prefixes,cutoff):
        if (prefixes.shape!=cutoff.shape or prefixes.ndim!=1 or prefixes.dtype!=torch.int64
                or cutoff.dtype!=torch.int64 or prefixes.device!=self.asset_ids.device or cutoff.device!=prefixes.device):
            raise TypeError('估值冻结须同根同设备实际公共prefix及cutoff')
        read=self.pm.valuation_bid(prefixes)
        roots=prefixes.numel();owner=self.asset_roots
        valid=(owner>=0)&(owner<roots)
        safe_root=owner.clamp(0,roots-1)
        bound=self.asset_ids>=0
        if len(self.pm.assets):
            ids=self.asset_ids.clamp_min(0)
            prices=take_ratio(self.pm.decimals,read.price_ids[safe_root,ids])
            zero=ExactRational(prices.numerator.constant(0),prices.denominator.constant(1))
            prices=select(prices,read.bid_present[safe_root,ids]&bound,zero)
            known=read.value_present[safe_root,ids]&(read.quality[safe_root,ids]==0)&bound&valid
            source_fault=read.fault[safe_root,ids]
        else:
            integer=ExactInt.from_i64_tensor(torch.zeros(owner.numel(),dtype=torch.int64,device=owner.device))
            prices=ExactRational(integer,integer.constant(1));known=torch.zeros_like(owner,dtype=torch.bool)
            source_fault=torch.zeros_like(owner,dtype=torch.int32)
        fault=read.prefix_fault|(prefixes>self.pm.prefix_at(cutoff)).to(torch.int32)|Numbers.combine_fault_rows(torch.where(
            torch.arange(roots,device=owner.device)[:,None]==safe_root[None],
            (source_fault|prices.fault|(~valid).to(torch.int32))[None],0))
        return FrozenPortfolioValuation(cutoff.clone(),prefixes.clone(),prices,known,fault)


@dataclass(frozen=True)
class PortfolioNavRead:
    conservative: ExactRational
    directional: ExactRational
    known: torch.Tensor  # [roots,2]；Unavailable与精确0不同。
    fault: torch.Tensor


def read_portfolio_nav(ledger,valuation,cutoff,*,portfolio=None,**history):
    roots=ledger.fault.numel();device=ledger.fault.device
    integer=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))
    zero=ExactRational(integer,integer.constant(1))
    known=torch.zeros((roots,2),dtype=torch.bool,device=device)
    conservative=zero;directional=zero
    fault=ledger.fault.clone()
    population=ledger.projected_markets;resolutions=ledger.resolutions
    if population is None or resolutions is None:
        return PortfolioNavRead(conservative,directional,known,fault)
    if portfolio is None:portfolio=read_portfolio_economics(ledger,**history)
    owner=ledger.success_pools.asset_roots;assets=owner.numel()
    if (valuation.cutoff.shape!=(roots,) or valuation.cutoff.device!=device
            or valuation.known.shape!=(assets,) or valuation.prices.numerator.sign.numel()!=assets
            or valuation.prices.numerator.words.device!=device):
        raise TypeError('NAV须同冻结cutoff及原Ledger完整资产估值')
    fault|=portfolio.fault|valuation.fault|(valuation.cutoff!=cutoff).to(torch.int32)
    conditions=population.conditions[None].expand(roots,-1)
    market=read_market_economics(ledger,conditions,population.present,
        economic_prefix_known=history['economic_prefix_known'],success_prefix_known=history['success_prefix_known'],
        settlement_prefix_known=history['settlement_prefix_known'])
    net=net_success(ledger.success_pools,resolutions,prefix_known=history['settlement_prefix_known'])
    linked=(torch.arange(assets,device=device)[:,None,None]==resolutions.assets[None]).any(2)
    resolved=(linked&resolutions.present[None]).any(1)
    positive=(net.orphan.numerator.sign>0)&~resolved
    member=owner[None]==torch.arange(roots,device=device)[:,None]
    marks_known=~(member&positive[None]&~(net.known&valuation.known)[None]).any(1)
    amount=net.orphan*valuation.prices
    index=torch.arange(assets,device=device)[None].expand(roots,-1).flatten()
    incoming=take_ratio(amount,index)
    absent=ExactRational(incoming.numerator.constant(0),incoming.denominator.constant(1))
    mark=sum_rows(select(incoming,(member&positive[None]).flatten(),absent),roots,assets) if assets else zero
    directional_fee=zero;fees_known=torch.zeros(roots,dtype=torch.bool,device=device)
    costs=ledger.settlement_costs
    if costs is not None:
        fee=costs.recognized-costs.paid
        absent=ExactRational(fee.numerator.constant(0),fee.denominator.constant(1))
        directional_fee=sum_rows(select(fee,(costs.occupied&~costs.maker_category).flatten(),absent),roots,costs.occupied.size(1))
        fees_known=costs.known
        fault|=costs.fault|directional_fee.fault
    funds=ledger.funds()
    # 原Ledger在每个判定市场累计Directional现金；按真实market.roots归账户，
    # 不能把一个市场的物理槽位当作根，也不能重复计整账户冷现金。
    markets=resolutions.roots.numel()
    index=torch.arange(markets,device=device)[None].expand(roots,-1).flatten()
    cash=take_ratio(resolutions.directional_cash,index)
    absent=ExactRational(cash.numerator.constant(0),cash.denominator.constant(1))
    directional_cash=sum_rows(select(cash,(resolutions.roots[None]==torch.arange(roots,device=device)[:,None]).flatten(),absent),roots,markets) if markets else zero
    # 与原AccountEconomicsReadV1逐项公式等价，共用已归约的WCM避免重复返佣/负债。
    conservative=portfolio.amounts[0]-ledger.maker_cash+funds.actual_available+funds.actual_reserved+market.amounts[2]-directional_fee+mark
    directional=directional_cash+market.amounts[2]-directional_fee+mark
    known[:,0]=portfolio.known[:,0]&market.known[:,2]&marks_known&fees_known
    known[:,1]=(history['economic_prefix_known']&history['success_prefix_known']&history['settlement_prefix_known']
        &population.known&market.known[:,2]&marks_known&fees_known)
    fault|=market.fault|net.fault|funds.fault|mark.fault|conservative.fault|directional.fault
    return PortfolioNavRead(conservative,directional,known,fault)
