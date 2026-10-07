"""同一Ledger前缀的市场经济及六态读面；精确scope归约留在公共Ledger。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class MarketEconomicRead:
    amounts: tuple[ExactRational,...]  # guarantee/pair/orphan/profit/loss/max_loss/unresolved_orphan_cost。
    known: torch.Tensor  # [roots,7]，各字段按实际完整来源判定。
    states: ExactInt  # 原六态精确计数；scope没有经济成员时为Unresolved。
    states_known: torch.Tensor
    fault: torch.Tensor


def read_market_economics(ledger,conditions,present,*,economic_prefix_known,success_prefix_known,settlement_prefix_known):
    roots,width=conditions.shape;device=ledger.fault.device
    if (roots!=ledger.fault.numel() or conditions.dtype!=torch.int64 or conditions.device!=device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or any(value.shape!=(roots,) or value.dtype!=torch.bool or value.device!=device
                for value in (economic_prefix_known,success_prefix_known,settlement_prefix_known))):
        raise TypeError('市场经济读面需原scope及经济/成功/结算完整前缀')
    empty=~present.any(1)
    zero_int=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))
    zero=ExactRational(zero_int,zero_int.constant(1))
    amounts=[zero]*7;known=torch.zeros((roots,7),dtype=torch.bool,device=device)
    fault=ledger.fault.clone()
    state_counts=torch.zeros((roots,6),dtype=torch.int64,device=device)
    states_known=empty.clone()

    def members(owner,identity):
        return (owner[None,:]==torch.arange(roots,device=device)[:,None]) & (
            (identity[None,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)

    def reduce_amount(source,member):
        count=member.size(1)
        if count==0:return zero
        index=torch.arange(count,device=device)[None,:].expand(roots,count).flatten()
        value=take_ratio(source,index)
        absent=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        return sum_rows(select(value,member.flatten(),absent),roots,count)

    resolved=ledger.resolutions
    if resolved is not None:
        read=resolved.read(ledger.success_pools)
        member=members(resolved.roots,resolved.conditions)
        directory_known=~(member&~read.known[None,:]).any(1)
        complete=success_prefix_known&settlement_prefix_known&directory_known
        pair=take_ratio(ledger.success_pools.paired_quantity,resolved.assets[:,0])
        absent=ExactRational(pair.numerator.constant(0),pair.denominator.constant(1))
        amounts[0]=reduce_amount(select(absent,resolved.present,pair),member)
        amounts[1]=reduce_amount(read.paired_receivable,member)
        amounts[2]=reduce_amount(read.winner_orphan_receivable,member)
        costs=take_ratio(ledger.success_pools.cost,resolved.assets.flatten())
        orphan_cost=take_ratio(costs,torch.arange(resolved.roots.numel(),device=device)*2)+take_ratio(
            costs,torch.arange(resolved.roots.numel(),device=device)*2+1)
        amounts[6]=reduce_amount(select(absent,resolved.present,orphan_cost),member)
        known[:,:3]=complete[:,None]
        known[:,6]=complete
        # 无Request的Resolved状态要区分正应收与零，才依赖完整成功人口。
        needs_success=(member&resolved.present[None,:]&~resolved.requests.present[None,:]).any(1)
        states_known=settlement_prefix_known&directory_known&(~needs_success|success_prefix_known)
        for state in range(6):
            state_counts[:,state]=(member&(read.settlement_state[None,:]==state)).to(torch.int64).sum(1)
        # 完整冷前缀中无经济事实的已选市场，原CPU读取为Unresolved；不是缺历史补零。
        represented=((resolved.conditions[None,None,:]==conditions[:,:,None])&
            (resolved.roots[None,None,:]==torch.arange(roots,device=device)[:,None,None])).any(2)
        state_counts[:,0]+=(present&~represented).to(torch.int64).sum(1)
        fault|=Numbers.combine_fault_rows(torch.where(member,read.fault[None,:],0))
    markets=ledger.unresolved_markets
    if markets is not None:
        read=markets.read(ledger.success_pools,ledger.trade_success,ledger.rebate_days,
            resolutions=resolved,settlement_costs=ledger.settlement_costs)
        member=members(markets.roots,markets.conditions)
        complete=economic_prefix_known&success_prefix_known&settlement_prefix_known
        complete&=~(member&~read.known[None,:]).any(1)
        if ledger.settlement_costs is None or resolved is None:complete=torch.zeros_like(complete)
        for target,source in [(3,read.profit),(5,read.maximum_loss)]:
            amounts[target]=reduce_amount(source,member)
        loss=ExactRational(read.profit.numerator.constant(0),read.profit.denominator.constant(1))-read.profit
        absent=ExactRational(loss.numerator.constant(0),loss.denominator.constant(1))
        amounts[4]=reduce_amount(select(loss,loss.numerator.sign>0,absent),member)
        known[:,3:6]=complete[:,None]
        fault|=Numbers.combine_fault_rows(torch.where(member,read.fault[None,:],0))
    known|=empty[:,None]
    states_known|=empty
    for value in amounts:fault|=value.fault
    counts=ExactInt.from_i64_tensor(state_counts.flatten())
    return MarketEconomicRead(tuple(amounts),known,counts,states_known,fault)
