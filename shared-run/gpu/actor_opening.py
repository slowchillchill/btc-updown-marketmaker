"""原SIM→NOE→Ledger冷开户结果机械安装；此后仅保留GPU活动账户。"""
from dataclasses import dataclass,replace
from fractions import Fraction
from pathlib import Path
import json
import torch
from queues import Numbers,ExactInt,ExactRational
from actor_sources import install_actor_sources


def load_actor_ledger_platform():
    """冷开户和活动恢复共用原固定Ledger类型身份，不执行开户。"""
    import importlib.util,sys
    from pathlib import Path
    name='actor_opening_ledger_platform'
    if name not in sys.modules:
        spec=importlib.util.spec_from_file_location(name,Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py')
        module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
    return sys.modules[name]


@dataclass(frozen=True)
class ActorOpening:
    sources: object
    public: object
    projection: object
    authority: dict
    conditions: tuple
    tokens: tuple
    asset_ends: tuple


@dataclass(frozen=True)
class ActorOpeningCapacities:
    segments: int
    requests: int
    request_actions: int
    orders: int
    fills: int
    result_requests: int
    result_items: int
    calendar_fills: int
    calendar_days: int
    ledger_orders: int
    ledger_trades: int
    rebate_days: int
    settlements: int


def open_actor_root(public,hidden,codec,request,*,page_groups=256,hidden_page_buckets=64,auto_redemption=False,capacities=None):
    """已准备完整目录不是已见市场事实；资金仅来自本次合法SessionSpec。"""
    from hidden_inputs import HiddenPmBlock
    from account_state import PlatformAccountState
    from base_orders import BaseOrders
    from order_quantity import BaseOrderQuantity
    from base_runtime import BaseSimRuntime
    from mutation_requests import MutationRequests
    from fill_finality import FillRecord
    from fill_pool import FillPool
    from maker_days import MakerDays
    from taker_days import TakerDays
    from rebate_coordinator import RebateCoordinator
    from post_results import PostResults
    from input_frontier import InputCommitFrontier
    from feedback import FeedbackEmitter
    from projection import Projection
    from core import EvidenceBank
    from consumption import PendingRecords
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from position_cost import PositionCosts
    from order_funds import OrderFunds
    from accepted_principal import AcceptedPrincipal
    from rebate_day import RebateDays
    from projected_markets import ProjectedMarkets
    from market_economics import UnresolvedMarkets
    from resolution import ResolutionSources,MarketResolutions
    from settlement_accounting import SettlementCosts,ExternalCashIdentities
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryState,PublicRegistryEvents,PublicCancellationState
    from commitments import SubmissionBindings
    # 旧调用保原物理配置；通用连续入口必须显式提供配置。
    caps=capacities or ActorOpeningCapacities(*([1]*13))
    count=len(hidden.arrays['bucket_times'])
    block=HiddenPmBlock.from_prepared(hidden.page(0,min(hidden_page_buckets,count)))
    device=block.level_assets.device
    t=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device=device)
    integer=lambda values:ExactInt.from_values(values,device=device)
    def rational(values):
        values=[Fraction(value) for value in values]
        return ExactRational.ratio(integer([v.numerator for v in values]),integer([v.denominator for v in values]))
    markets=public.columns['markets'];assets=public.columns['assets']
    conditions=tuple(condition for condition,_ in hidden.shared['banks']['source_scene'])
    by_condition={market['condition_id']:market for market in markets}
    by_token={assets[asset]['token_id']:(markets[market]['condition_id'],outcome)
        for market,asset,outcome in public.columns['market_assets']}
    tokens=tuple(asset['token_id'] for asset in block.assets)
    asset_markets=[conditions.index(by_token[token][0]) for token in tokens]
    outcomes=[by_token[token][1] for token in tokens]
    pairs=[next(i for i in range(len(tokens)) if asset_markets[i]==asset_markets[row] and outcomes[i]!=outcomes[row])
        for row in range(len(tokens))]
    first=[next(i for i in range(len(tokens)) if asset_markets[i]==market and outcomes[i]==0)
        for market in range(len(conditions))]
    request={**request,'scenario':{'schema_version':'platform_scenario_v1','markets':[
        {'market':by_condition[condition],'assets':[asset for asset in block.assets
            if by_token[asset['token_id']][0]==condition]} for condition in conditions]}}
    authority=json.loads(codec.open_actor_account_v2(json.dumps(request)))
    # 规则分派只取原Native权威结果；GPU执行安装前取得对应的完整typed输入。
    # Native此处只冷开户，没有消费hidden步骤；恢复不经过此入口。
    if authority['maker_model']=='PUBLIC_TRADE_CONSERVATIVE_FIFO_V1':
        hidden=hidden.for_public_trade(Path(request['log_path']).with_suffix('.hidden-v2'))
        block=HiddenPmBlock.from_prepared(hidden.page(0,min(hidden_page_buckets,len(hidden.arrays['bucket_times']))))
    projection=authority['ledger_projection'];balance=projection['platform_confirmed_projection']
    sequence,account_sequence,private_sequence,batch_sequence=authority['session_sequences']
    def build():
        zero=t([0]);yes=t([True],torch.bool);empty=t([[]]);blank=integer([0]);roots=t([0]*len(tokens))
        quantity=BaseOrderQuantity.from_taker_plan(integer([1]),integer([1]),blank,blank)
        fill=FillRecord.create(integer([1]),integer([1]),blank,zero,zero,~yes,request['finality_policy']['normal_path'])
        account=replace(PlatformAccountState.open(integer([int(Fraction(balance['cash_pusd'])*1000000)]),roots),
            next_sequence=integer([account_sequence]))
        runtime=BaseSimRuntime(block.empty_state(segment_capacity=caps.segments,maker_model=authority["maker_model"]),MutationRequests.empty(blank,capacity=caps.requests,action_capacity=caps.request_actions),
            BaseOrders.empty(quantity,capacity=caps.orders),account,FillPool.empty(fill,capacity=caps.fills),
            RebateCoordinator(MakerDays.empty(t([list(range(len(conditions)))]),
                t([[by_condition[c]['window']['end_at_utc_ms'] for c in conditions]]),
                t([[True]*len(conditions)],torch.bool),fills=caps.calendar_fills,days=caps.calendar_days),TakerDays.empty(1,device,fills=caps.calendar_fills,days=caps.calendar_days)),
            input_frontier=InputCommitFrontier(integer([sequence]),request['session']['run_id'],request['session']['session_id']),
            post_results=PostResults.empty(quantity,caps.result_requests,caps.result_items))
        module=load_actor_ledger_platform()
        names=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
            'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')
        platform=module.PlatformBalances.open(rational([balance[name] for name in names]),rational(['0']*len(tokens)),
            roots,integer([projection['platform_source_prefix']]))
        pools=SuccessPools.open(roots,t(asset_markets),t(list(range(len(tokens)))),t(pairs),1)
        columns=EconomicColumns.empty(1,device);unresolved=UnresolvedMarkets.open(pools,t(first))
        resolutions=MarketResolutions.open(pools,unresolved,ResolutionSources.bind(block.terminal_columns,tokens,device=device))
        ledger=AdmissionLifecycle.open(platform,EvidenceBank.empty(device),columns,
            integer([projection['ledger_entry_seq']]),integer([projection['ledger_local_prefix']]),pools,
            position_costs=PositionCosts.empty(1,len(conditions),device),order_funds=OrderFunds.empty(1,caps.ledger_orders,caps.ledger_trades,0,device),
            rebate_days=RebateDays.empty(t([list(range(len(conditions)))]),caps.rebate_days),
            accepted_principal=AcceptedPrincipal.open(1,t(list(range(len(conditions)))),known=True),
            projected_markets=ProjectedMarkets.open(t(list(range(len(conditions)))),t([[False]*len(conditions)],torch.bool),yes),
            unresolved_markets=unresolved,resolutions=resolutions,settlement_costs=SettlementCosts.empty(1,caps.settlements,device,known=True),
            external_cash=ExternalCashIdentities.empty(1,caps.settlements,device,known=True),other_cash=rational(['0']))
        pending=PendingRecords.from_columns(empty.bool(),empty,integer([]),integer([]),empty,integer([0]))
        public_state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,device)),ClosedAdmission.empty(columns),ledger,
            submission_bindings=SubmissionBindings.empty(1,device,prefix_known=True),
            cancellations=PublicCancellationState.open(integer([]),empty.bool()))
        emitter=replace(FeedbackEmitter.open(1,device=device),next_private_sequence=integer([private_sequence]),
            next_batch_sequence=integer([batch_sequence]))
        noe=Projection.open(emitter,zero,zero,zero,zero,roots,integer([0]),t([-1]),t([-1]),zero,zero)
        return runtime,public_state,noe
    runtime,public_state,noe=Numbers.run_exact_batch(build)
    sources=install_actor_sources(public,hidden,runtime,conditions,page_groups=page_groups,
        hidden_page_buckets=hidden_page_buckets)
    if auto_redemption:
        from settlement_redemption import RedemptionSources
        sources=replace(sources,runtime=Numbers.run_exact_batch(lambda:sources.runtime.install_auto_redemption(
            RedemptionSources.bind(sources.block.terminal_columns,
                tokens,[0]*len(tokens),list(range(len(tokens))),1))))
    return ActorOpening(sources,public_state,noe,authority,conditions,tokens,
        tuple(by_condition[by_token[token][0]]['window']['end_at_utc_ms'] for token in tokens))
