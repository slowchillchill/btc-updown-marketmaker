"""发布后根批次组合的定向组件检查；不代替真实Captured/时钟组合验证。

原有限来源提供开户与正式FOK意图。这里直接消费已发布正文，使用真实
MutationRequests/AdmissionLifecycle；尚缺时钟/Actor评分不由本检查证明。
"""
from dataclasses import dataclass,replace
from types import SimpleNamespace
from pathlib import Path
import importlib.util,json,os,sys,time
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('shared-run/gpu','sim-system/gpu','ledger/gpu',
    'execution-normalization/gpu','model-train/native-inputs/gpu','data-pipeline/gpu')]
from validate_pending_post import T,run,integers,rational,values,amounts,BALANCES
from core import EvidenceBank
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from position_cost import PositionCosts
from order_funds import OrderFunds
from accepted_principal import AcceptedPrincipal
from projected_markets import ProjectedMarkets
from registry import PublicCancellationState
from commitments import SubmittedLegs
from intent_terms import SubmittedBuyTerms
from action_selection import SelectedAction,ActionControl,reserve_selected
from mutation_requests import MutationRequests
from input_frontier import InputCommitFrontier
from latency import EmpiricalDelaySampler,byte_column
from rules import PublicRuleGpu
from actor_send import FrozenActorSend,ActorRequestSubmission,IntentRequestIdentities

@dataclass(frozen=True)
class SubmissionState:
    requests: object
    input_frontier: object

@dataclass(frozen=True)
class PublicSubmission:
    ledger: object
    cancellations: object

class PublishedBody:
    """本组件只接收已拥有正文/资金决定；无评分或随机数替代实现。"""
    def __init__(self,action,reservation):
        self.action=action;self.reservation=reservation
        self.candidate=SimpleNamespace(complete=T([True]*3,torch.bool))
        self.published=T([True]*3,torch.bool)
    def publish_and_reserve(self,publication,state,control,enabled=None):return state,publication

class CompletedClock:
    """已推进/领取的接口替身；本代表只核后续按根发送与重复返回。"""
    def __init__(self,runtime,public):self.runtime,self.public=runtime,public;self.calls=0
    def advance_before(self,at,enabled,**kwargs):
        self.calls+=1
        return SimpleNamespace(complete=T([True]*3,torch.bool))
    def receive_first(self,enabled=None):return None


def check(source,codec,*,two_buys=False):
    started=time.perf_counter();roots=3;yes=T([True]*roots,torch.bool);zero=T([0]*roots)
    balance=source['opening_ledger']['projection']['platform_confirmed_projection']
    spec=importlib.util.spec_from_file_location('actor_send_batch_platform',ROOT/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def opening():
        pools=SuccessPools.open(T([0,0,1,1,2,2]),T([0]*6),T([0,1]*3),T([1,0,3,2,5,4]),roots)
        platform=module.PlatformBalances.open(rational([balance[key] for _ in range(roots) for key in BALANCES]),
            rational(['0']*6),T([0,0,1,1,2,2]),integers([0]*roots))
        ledger=AdmissionLifecycle.open(platform,EvidenceBank.empty('cuda'),EconomicColumns.empty(roots,'cuda'),
            integers([0]*roots),integers([0]*roots),pools,position_costs=PositionCosts.empty(roots,1,'cuda'),
            order_funds=OrderFunds.empty(roots,1,1,0,'cuda'),accepted_principal=AcceptedPrincipal.open(roots,T([0]),known=True),
            projected_markets=ProjectedMarkets.open(T([0]),T([[False]]*roots,torch.bool),yes))
        ledger=replace(ledger,reservations=ledger.reservations.resized(2),
            global_requests=T([[-1,-1]]*roots),item_ordinals=T([[-1,-1]]*roots))
        cancellations=PublicCancellationState.open(integers([1,2,3]),T([[False]]*roots,torch.bool))
        return PublicSubmission(ledger,cancellations)
    public=run(opening)
    control=ActionControl(zero, T([0,0,2] if two_buys else [1,2,3]),T([[0,1]]*roots),T([source['sent_at'] if two_buys else 1414]*roots))
    original=source['posts'][0]['item']['order'];kind=original['kind']['FokMarket']
    spend=kind['spend_pusd'];price=kind['worst_price_pusd_per_share']
    from fractions import Fraction
    from pending_post_intents import _decimal
    target=_decimal(Fraction(spend)/Fraction(price))
    present=T([[True,False],[two_buys,False],[False,False]],torch.bool)
    terms=SubmittedBuyTerms(T([[2,0]]*roots),T([[False,False]]*roots,torch.bool),rational([price]*6),
        rational([spend,'0',spend if two_buys else '0','0','0','0']),T([[0,0]]*roots))
    legs=SubmittedLegs(present,torch.zeros_like(present),control.global_requests[:,None].expand(-1,2),
        T([[0,1]]*roots),rational([target,'0',target if two_buys else '0','0','0','0']),present,T([[0,0]]*roots),T([[0,1]]*roots),present,terms)
    action=SelectedAction(T([[0,0,0]]*roots),control,T([1,1 if two_buys else 5,0]),legs,T([[0,1]]*roots),
        terms.spend,rational(['0']*6),SimpleNamespace(markets=T([[-1],[0],[-1]])),
        integers([-1]*6),T([[False,False]]*roots,torch.bool),T([0]*roots,torch.int32))
    public,reservation=run(lambda:reserve_selected(public,action,yes))
    assert reservation.complete.all().cpu().item() and not reservation.decision.fault.any().cpu().item()
    frontier=run(lambda:InputCommitFrontier.open(roots,source['run_id'],source['session_id']))
    runtime=SubmissionState(run(lambda:MutationRequests.empty(integers([0]*roots),capacity=1,action_capacity=2)),frontier)
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
        source['profile_sha256'],[source['session_seed'].encode('ascii')]*roots)
    identity,offsets=byte_column([source['request_id'].encode(),b'earlier-request',b'batch-no-trade'] if two_buys else [b'earlier-request',b'batch-buy',b'batch-cancel',b'batch-no-trade'])
    # root1先前真实登记请求占满目录；此次Cancel只能续接，root0仍可完成Buy。
    requests,earlier=run(lambda:runtime.requests.register(sampler,identity,offsets,zero+int(two_buys),zero,
        runtime.input_frontier.next_sequence,T([1200]*roots),T([[0,-1]]*roots),T([[0,0]]*roots),
        T([[True,False]]*roots,torch.bool),T([[False,False]]*roots,torch.bool),zero,
        T([False,True,False],torch.bool),T([0]*roots,torch.int32)))
    frontier,_=run(lambda:runtime.input_frontier.commit(earlier.registered))
    runtime=replace(runtime,requests=requests,input_frontier=frontier)
    condition=original['condition_id'];tokens=tuple(e['payload']['event']['asset']['token_id'] for e in source['initial'])
    ids=IntentRequestIdentities(tuple({'global_request_id':value,'caller_correlation_id':None} for value in
        ((source['request_id'],source['request_id'],'batch-no-trade') if two_buys else ('batch-buy','batch-cancel','batch-no-trade'))),((condition,),)*roots,(tokens,)*roots,({},)*roots)
    rule=PublicRuleGpu(source['rule'])
    submission=ActorRequestSubmission(control,T([[0,1]]*roots),sampler,rule,identity,offsets,
        ((condition,),)*roots,(tokens,)*roots,({(0,None):0,(1,None):1},)*roots,((source['market_end'],),)*roots,
        codec,order_sequences=({},)*roots)
    publication=PublishedBody(action,reservation)
    clock=CompletedClock(runtime,public)
    sender=FrozenActorSend(publication,publication,control,ids,codec,submission,
        lambda state,delivery,enabled:(state,yes),(zero,'component-source','component-binding'),decision_cutoff=object())
    first=sender.execute(clock,yes)
    assert first.sent.cpu().tolist()==[True,False,False] and first.complete.cpu().tolist()==[True,False,True]
    assert first.receipt.registered.cpu().tolist()==[True,False,False]
    assert values(clock.runtime.input_frontier.next_sequence)==[2,2,1]
    buy_sequence=values(first.receipt.input_sequence)[0];buy_arrival=first.receipt.arrived_at.cpu()[0].item()
    first_prefix=values(clock.public.ledger.current_local_prefix)
    bound_sequence=values(submission.submitted_legs.input_sequences)[:2]
    # 原root0已成功；另一根先遇推进未就绪，再遇公开Ledger扫描未就绪。
    original_advance=clock.advance_before
    clock.advance_before=lambda *args,**kwargs:SimpleNamespace(complete=T([True,False,True],torch.bool))
    early=sender.execute(clock,yes)
    assert early.receipt is first.receipt and torch.equal(early.complete,first.complete)
    clock.advance_before=original_advance
    original_receive=sender.receive
    sender.receive=lambda state,delivery,enabled:(state,T([True,False,True],torch.bool))
    waiting=sender.execute(clock,yes)
    assert waiting.receipt is first.receipt and torch.equal(waiting.complete,first.complete)
    sender.receive=original_receive
    clock.runtime=replace(clock.runtime,requests=clock.runtime.requests.resized(2,2))
    second=sender.execute(clock,yes)
    assert second.sent.cpu().tolist()==[True,True,False] and second.complete.cpu().tolist()==[True]*roots
    assert second.receipt.registered.cpu().tolist()==[True,True,False]
    assert values(second.receipt.input_sequence)[:2]==[buy_sequence,2]
    assert second.receipt.arrived_at.cpu()[0].item()==buy_arrival
    assert values(clock.runtime.input_frontier.next_sequence)==[2,3,1]
    expected_prefix=[value+int(two_buys and root==1) for root,value in enumerate(first_prefix)]
    assert values(clock.public.ledger.current_local_prefix)==expected_prefix
    assert clock.public.cancellations.registered.cpu().tolist()==[[False],[not two_buys],[False]]
    if two_buys:
        assert submission.submitted_legs.receipt_known.cpu().tolist()==[[True,False],[True,False],[False,False]]
        assert submission.submitted_legs.input_sequence_known.cpu().tolist()==[[True,False],[True,False],[False,False]]
        assert values(submission.submitted_legs.input_sequences)[:2]==bound_sequence
        assert values(submission.submitted_legs.input_sequences)[2]==2
        # 下游原承诺读取完整receipt腿人口；没有Accepted仍保持真实target，不能丢旧root0。
        from commitments import freeze_leg_commitments,SubmissionBindings
        from registry import FrozenIntentOrders,PublicRegistryState,PublicRegistryEvents
        intents=run(lambda:FrozenIntentOrders(T([[]]*roots,torch.bool),integers([]),T([[]]*roots,torch.bool),
            rational([]),T([[]]*roots,torch.bool),T([[]]*roots,torch.bool)))
        registry=PublicRegistryState(PublicRegistryEvents.empty(roots,'cuda'))
        frozen=run(lambda:registry.freeze_orders(intents,control.execute_at))
        commitments=run(lambda:freeze_leg_commitments(submission.submitted_legs,SubmissionBindings.empty(roots,'cuda'),intents,frozen))
        assert commitments.present.cpu().tolist()==[[True,False],[True,False],[False,False]]
        assert not commitments.fault.any().cpu().item()
        # 同一原FOK的真实公开Accepted仅作下游读面输入；不代替设备接单验证。
        from validate_registry import event_columns,position_key
        from intent_terms import freeze_intent_orders
        from dataclasses import fields,is_dataclass
        row=next(row for row in source['records'] if 'Evidence' in row['record'] and
            'Control' in row['record']['Evidence']['evidence'])
        evidence=row['record']['Evidence'];accepted=evidence['evidence']['Control']['result']['Submission']['outcome']['Accepted']
        source_request=evidence['source_reference']['record']['DirectResponse']['association']['SubmissionItem']['global_request_id']
        assert source_request==source['request_id']
        catalog={'contexts':{row['position']['context_reference']:0},'runs':{row['position']['source_watermark']['run_id']:0},
            'references':{position_key(row):(0,0)}}
        events=run(lambda:event_columns([row],{accepted['order']['order_identity']:1},0,{source_request:0},catalog))
        events=replace(events,processing_at=T([[accepted['effective_processing_at_utc_ms']]]),condition_slots=T([[0]]))
        def duplicate(value):
            if value is None:return None
            if isinstance(value,torch.Tensor):return value.repeat((roots,)+(1,)*(value.dim()-1))
            if is_dataclass(value):return replace(value,**{f.name:duplicate(getattr(value,f.name)) for f in fields(value)})
            raise TypeError(type(value))
        events=duplicate(events);registry=PublicRegistryState(events)
        bindings=SubmissionBindings(events.received,events.request_slots,events.item_ordinals,events.private_sequences,
            events.receive_rank,events.orders,events.received,events.fault)
        intents,fault=run(lambda:freeze_intent_orders(submission.submitted_legs,bindings,integers([1]*roots),T([[True]]*roots,torch.bool)))
        formation=run(lambda:registry.freeze_order_formation(intents,submission.submitted_legs,T([1414]*roots),T([[True]]*roots,torch.bool)))
        assert not fault.any().cpu().item() and not formation.fault.any().cpu().item()
        assert formation.processing_known.cpu().tolist()==[[True],[True],[False]]
        assert values(formation.input_sequences)[:2]==[buy_sequence,2]
        assert formation.processing_at[:2].cpu().tolist()==[[accepted['effective_processing_at_utc_ms']]]*2
        assert not formation.source_known.any().cpu().item()

    occupied=clock.runtime.requests.occupied.clone();calls=clock.calls
    third=sender.execute(clock,yes)
    assert third.receipt is second.receipt and third.complete.cpu().tolist()==[True]*roots
    assert torch.equal(occupied,clock.runtime.requests.occupied) and clock.calls==calls
    assert values(clock.runtime.input_frontier.next_sequence)==[2,3,1]
    assert values(clock.public.ledger.current_local_prefix)==expected_prefix
    return dict(passed=True,scope='published body / real batch dispatch / capacity continuation / per-root receipt and NoTrade completion; clock and Actor evaluation not run',
        two_buys=two_buys,original_accepted_formation_read=two_buys,early_advance_preserves_receipt=True,early_ledger_preserves_receipt=True,first_sent=first.sent.cpu().tolist(),final_sent=third.sent.cpu().tolist(),next_input=values(clock.runtime.input_frontier.next_sequence),
        elapsed_seconds=time.perf_counter()-started)

if __name__=='__main__':
    source=json.loads(Path(sys.argv[1]).read_text())
    spec=importlib.util.spec_from_file_location('model_native_inputs',Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so')
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    print(json.dumps(check(source,codec,two_buys="--two-buys" in sys.argv)),flush=True)
