"""已准备公共/hidden根到唯一GPU账户与原连续执行时钟的冷组合。"""
from dataclasses import dataclass,replace
import torch
from actor_opening import open_actor_root
from actor_send import SubmittedActorRequests
from commitments import SubmittedLegs
from intent_terms import SubmittedBuyTerms
from queues import Numbers,ExactInt,ExactRational
from core import EvidenceBank
from cancel_projection import CancellationTargetHistory
from continuous_execution import ContinuousExecution
from send_clock import BoundedSendClock
from ledger_drain import PublicLedgerReceiver


@dataclass(frozen=True)
class ActorDirectory:
    conditions: tuple
    tokens: tuple                    # 原SIM token人口。
    public_tokens: tuple             # 原Actor公开资产人口。
    asset_ends: tuple
    market_indices: torch.Tensor
    condition_by_asset: torch.Tensor
    source_assets: torch.Tensor
    ask_rows: torch.Tensor


@dataclass
class ActorRuntime:
    clock: BoundedSendClock
    directory: ActorDirectory
    population: SubmittedActorRequests
    receiver: PublicLedgerReceiver
    authority: dict


def open_continuous_runtime(public,hidden,codec,request,rule,sampler,*,capacities,
        page_groups,hidden_page_buckets,taker_rate,maker_rate,fill_paths,cancel_scenario,gtd,auto_redemption):
    """场景/时延/物理容量显式供给；不准备材料或重判其EOF/资格。"""
    opening=open_actor_root(public,hidden,codec,request,page_groups=page_groups,
        hidden_page_buckets=hidden_page_buckets,auto_redemption=auto_redemption,capacities=capacities)
    sources=opening.sources;block=sources.block;device=block.level_assets.device
    t=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device=device)
    rows=[[i for i,(asset,side) in enumerate(zip(block.level_assets.cpu().tolist(),block.level_sides.cpu().tolist()))
        if asset==a and side==1] for a in range(len(opening.tokens))]
    width=max(map(len,rows),default=0)
    directory=ActorDirectory(opening.conditions,opening.tokens,
        tuple(a['token_id'] for a in public.columns['assets']),opening.asset_ends,sources.market_indices,
        sources.condition_by_asset,block.asset_axis,t([row+[-1]*(width-len(row)) for row in rows]))
    def empty_population():
        empty=t([[]]);integer=ExactInt.from_i64_tensor(empty.flatten())
        rational=ExactRational.ratio(integer,integer)
        return SubmittedActorRequests(submitted_legs=SubmittedLegs(empty.bool(),empty.bool(),empty,empty,rational,
            empty.bool(),empty,empty,empty.bool(),SubmittedBuyTerms(empty,empty.bool(),rational,rational,empty),integer,empty.bool()))
    population=Numbers.run_exact_batch(empty_population)
    public_state=replace(opening.public,cancellation_target_history=Numbers.run_exact_batch(
        lambda:CancellationTargetHistory.open(ExactInt.from_i64_tensor(t([])),t([[]],torch.bool))))
    source=sources.source;source.states=sources.states
    bank=lambda projection:projection.normalizer.bank if projection.normalizer is not None else EvidenceBank.empty(device)
    clock=BoundedSendClock(sources.runtime,opening.projection,public_state,source,{},gtd,bank,
        hidden_page_buckets=hidden_page_buckets)
    configure_continuous_execution(clock,directory,population,rule,codec,sampler,taker_rate,maker_rate,fill_paths,cancel_scenario)
    return ActorRuntime(clock,directory,population,PublicLedgerReceiver(),opening.authority)


def configure_continuous_execution(clock,directory,population,rule,codec,sampler,taker_rate,maker_rate,fill_paths,cancel_scenario):
    """原生产处理器共用真实目录；也供已有账户调用方接连续提交。"""
    device=directory.market_indices.device;t=lambda value:torch.tensor(value,dtype=torch.int64,device=device)
    market_ends=tuple(clock.source.prepared.columns['markets'][i]['window']['end_at_utc_ms']
        for i in directory.market_indices.cpu().tolist())
    backend=ContinuousExecution(population,rule,codec,sampler,directory.ask_rows,t(list(range(len(directory.tokens)))),
        t(directory.asset_ends),taker_rate,maker_rate,directory.condition_by_asset,directory.source_assets,fill_paths,cancel_scenario,
        condition_by_market=t([list(range(len(directory.conditions)))]),market_end_by_market=t([market_ends]))
    clock.handlers=backend.handlers()
    return backend


def grow_ledger_resources(runtime):
    """原Receiver资源退出的冷增长；保领取/waiting及业务前缀。"""
    captured=runtime.receiver.captured
    if captured is None or bool(captured.resources_fit.cpu()):return False
    clock=runtime.clock;ledger=clock.public.ledger;changed=False
    for name,attribute,method in (('orders','orders',None),('trades','trades',None),
            ('contributions','rebate_contributions','resized_contributions'),('days','rebate_days','resized_rebate_days'),
            ('order_funds','order_funds','resized_order_funds'),('costs','settlement_costs',None),('cash','external_cash',None)):
        need=int(captured.required[name].max().cpu());directory=getattr(ledger,attribute)
        if directory is None or need<=directory.occupied.size(1):continue
        if name in ('orders','trades'):ledger=ledger.resized_lifecycle(**{name:need})
        elif name in ('costs','cash'):ledger=ledger.resized_settlement_identities(**{name:need})
        else:ledger=getattr(ledger,method)(need)
        changed=True
    required_words=int(captured.candidate.required_words.max().cpu())
    from ledger_drain import installed_word_capacity
    if required_words>installed_word_capacity(captured.storage.state.ledger):
        runtime.receiver.word_capacity=required_words;changed=True
    if changed:clock.public=replace(clock.public,ledger=ledger)
    return changed
