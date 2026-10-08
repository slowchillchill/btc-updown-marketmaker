"""实际Captured两根分轮启用、原现金NOE首次领取和当前Ledger预留。

同一原Native冻结输入作为两个独立回放根；只核发布/领取/预留责任。
推进阶段已完成的接口替身不证明两根完整source clock宿主。
"""
from dataclasses import replace,fields,is_dataclass
from pathlib import Path
from types import SimpleNamespace
import json,sys,time,importlib.util,os
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('model-train','model-train/scripts','shared-run/gpu','sim-system/gpu',
    'execution-normalization/gpu','ledger/gpu','model-train/native-inputs/gpu','data-pipeline/gpu')]
from validate_pending_post import T,run,integers,rational,values,BALANCES
from validate_portfolio_nav import exact
from model_train.actor_selection_validation import make_parts
from model_train.device_actor import CapturedDeviceActor,ActorSelection
from model_train.action_random import ActionRandom
from actor_send import FrozenActorSend,IntentRequestIdentities
from send_clock import BoundedSendClock
from commitments import SubmissionBindings
from registry import PublicRegistryEvents,PublicRegistryState
from public_ledger import PublicLedgerState
from consumption import PendingRecords,ReceiveGates
from closed import ClosedAdmission
from admission import AdmissionLifecycle
from memory import EconomicColumns
from core import EvidenceBank
from success_pool import SuccessPools
from position_cost import PositionCosts
from order_funds import OrderFunds
from accepted_principal import AcceptedPrincipal
from settlement_accounting import ExternalCashIdentities,SettlementCosts
from projected_markets import ProjectedMarkets
from projection import Projection
from feedback import FeedbackEmitter
from fields import RecordList
from latency import EmpiricalDelaySampler
from input_frontier import InputCommitFrontier
from public_device_page import PreparedPublicPage


def repeated(value):
    if value is None:return None
    if isinstance(value,torch.Tensor):return value.repeat((2,)+(1,)*(value.dim()-1))
    if is_dataclass(value):return replace(value,**{f.name:repeated(getattr(value,f.name)) for f in fields(value)})
    raise TypeError(type(value))


def check(source,page,compact,*,prepublish=False):
    started=time.perf_counter();yes=T([True,True],torch.bool);zero=T([0,0]);first=T([True,False],torch.bool)
    policy,single,_,control=make_parts(source,2)
    actor=replace(single.actor,**{name:(value if name in ('rules','condition_slots') else repeated(value))
        for name,value in vars(single.actor).items()})
    inputs=replace(single,actor=actor,**{name:repeated(value) for name,value in vars(single).items() if name!='actor'})
    random=ActionRandom(zero+181606,T([[0]*4]*2));control=replace(control,decisions=zero+8,global_requests=zero+9,
        leg_requests=T([[10,11]]*2),execute_at=zero+source['cash_steps'][0]['cutoff'])
    spec=importlib.util.spec_from_file_location('enabled_platform',ROOT/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    balances=source['checkpoint']['projection']['platform_confirmed_projection']
    def opening():
        pools=SuccessPools.open(T([0,0,1,1]),T([0]*4),T([0,1]*2),T([1,0,3,2]),2)
        platform=module.PlatformBalances.open(rational([balances[key] for _ in range(2) for key in BALANCES]),
            rational(['0']*4),T([0,0,1,1]),integers([0,0]))
        ledger=AdmissionLifecycle.open(platform,EvidenceBank.empty('cuda'),EconomicColumns.empty(2,'cuda'),
            integers([source['checkpoint']['projection']['ledger_entry_seq']]*2),
            integers([source['checkpoint']['projection']['ledger_local_prefix']]*2),
            pools,position_costs=PositionCosts.empty(2,1,'cuda'),order_funds=OrderFunds.empty(2,1,1,0,'cuda'),
            accepted_principal=AcceptedPrincipal.open(2,T([0]),known=True),
            projected_markets=ProjectedMarkets.open(T([0]),T([[False]]*2,torch.bool),yes),
            external_cash=ExternalCashIdentities.empty(2,1,'cuda',known=True),
            settlement_costs=SettlementCosts.empty(2,1,'cuda',known=True),
            other_cash=rational([source['checkpoint']['residual']['other_cash_pusd']]*2))
        ledger=replace(ledger,reservations=ledger.reservations.resized(2),global_requests=T([[-1,-1]]*2),item_ordinals=T([[-1,-1]]*2))
        columns=ledger.columns
        pending=PendingRecords.from_columns(columns.enabled,columns.row_refs,integers([]),integers([]),columns.row_refs,integers([0,0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(2,'cuda')),ClosedAdmission.empty(columns),ledger,
            submission_bindings=SubmissionBindings.empty(2,'cuda'))
    public=run(opening);before_funds=exact(run(public.ledger.funds).actual_available)
    # 原SIM ExternalCash负额事实与Economic delta原封不动进入所属GPU生产投影；仅root1接纳。
    step=source['cash_steps'][0];rows=step['rows'];life=next(iter(rows[0]['record'].values()))
    fact=life['evidence']['SettlementLifecycleV2']['result']['ExternalCash'];economic=next(iter(rows[1]['record'].values()))['fact']
    balance=economic['disposition']['AccountTransition']['balance_delta'];existing=balance['existing']
    assert fact['correction_of'] is None
    profile='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/profile/'profile.json').read_bytes(),profile,[b'e'*64]*2)
    projection=run(lambda:Projection.open(FeedbackEmitter.open(2),zero,zero,zero,zero,T([0,0,1,1]),integers([0,0]),zero,zero,zero,zero))
    projection=replace(projection,emitter=replace(projection.emitter,
        next_private_sequence=integers([life['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']]*2),
        next_batch_sequence=integers([rows[0]['position']['source_watermark']['projection_batch_sequence']]*2)))
    vector=RecordList((T([]),rational([])),T([[],[]],torch.bool))
    projection,batch=run(lambda:projection.external_cash_feedback(sampler,zero,rational([fact['signed_delta_pusd']]*2),zero+2,
        T([False,False],torch.bool),zero,integers([economic['economic_identity']['AccountTransition']['account_transition_seq']]*2),
        tuple(rational([existing[name+'_delta']]*2) for name in BALANCES),vector,vector,
        rational([balance['settlement_fee_liability_pusd_delta']]*2),rational([balance['actual_taker_fee_liability_pusd_delta']]*2),
        T([life['source_reference']['record']['SettlementLifecycle']['platform_event_at_utc_ms']]*2),~first,T([0,0],torch.int32),
        economic_present=~first,source_ordinal=T([life['source_order']['ordinal']]*2),record_ordinal=integers([rows[0]['position']['record_ordinal']]*2)))
    public=run(lambda:public.append_batch(projection.normalizer.bank,batch))
    frontier=run(lambda:InputCommitFrontier.open(2,'enabled-independent-replays','enabled-source'))
    prepared=PreparedPublicPage(Path(page),Path(compact));position=T([len(prepared.columns['groups'])-1]*2)
    frontier,_=run(lambda:frontier.public_group(prepared,position,yes))
    runtime=SimpleNamespace(orders=SimpleNamespace(roots=2,occupied=T([[False],[False]],torch.bool)),input_frontier=frontier)
    # dataclass runtime允许生产clock原replace；不执行SIM推进，此代表只核首次领取。
    from dataclasses import make_dataclass
    runtime=make_dataclass('Runtime',[('orders',object),('input_frontier',object)],frozen=True)(runtime.orders,frontier)
    clock=BoundedSendClock(runtime,projection,public,None,{},None,lambda p:p.normalizer.bank)
    clock.gates=ReceiveGates.unset(2).before(control.execute_at,yes)
    clock.advance_before=lambda at,enabled,**kw:SimpleNamespace(complete=enabled.clone())
    claims=[]
    def receive(state,delivery,enabled):
        claims.append(enabled.cpu().tolist())
        state_ledger,waiting=run(lambda:state.waiting.drain(state.ledger,enabled[:,None].expand_as(state.ledger.columns.enabled),passes=1))
        return replace(state,ledger=state_ledger,waiting=waiting),~(waiting.received&~waiting.consumed).any(1)&(state_ledger.fault==0)
    driver=CapturedDeviceActor(policy,inputs,random,word_capacity=16,path_capacity=32);publication=ActorSelection(random)
    sent=[]
    def dispatch(runtime,public,requests,action,enabled,**kwargs):
        sent.append(enabled.cpu().tolist())
        # Root0的发布/预留已成立；发送不是本代表的受检边界，保持容量退出。
        receipt=SimpleNamespace(registered=torch.zeros_like(enabled),duplicate=torch.zeros_like(enabled),fault=T([0,0],torch.int32))
        return runtime,public,receipt,torch.zeros_like(enabled)
    spec=importlib.util.spec_from_file_location('model_native_inputs',Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so')
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    identities=IntentRequestIdentities(tuple({'global_request_id':'enabled-replay','caller_correlation_id':None} for _ in range(2)),
        ((source['condition_id'],),)*2,(tuple(source['tokens']),)*2,({},)*2)
    sender=FrozenActorSend(publication,driver,control,identities,codec,dispatch,receive,(zero,'component','component'),decision_cutoff=object())
    original_advance=clock.advance_before
    clock.advance_before=lambda at,enabled,**kw:SimpleNamespace(complete=T([False,True],torch.bool))
    early=sender.execute(clock,first)
    assert early.complete.cpu().tolist()==[False,False] and early.receipt is None
    assert publication.action is None and random.counter.cpu().tolist()==[[0]*4]*2
    clock.advance_before=original_advance
    actual_receive=sender.receive
    sender.receive=lambda state,delivery,enabled:(state,T([False,True],torch.bool))
    waiting=sender.execute(clock,first)
    assert waiting.complete.cpu().tolist()==[False,False] and waiting.receipt is None
    assert publication.action is None and random.counter.cpu().tolist()==[[0]*4]*2
    sender.receive=actual_receive
    if prepublish:
        publication.publish(driver,control,yes)
        assert publication.published.cpu().tolist()==[True,True]
        published_counter=random.counter.clone()
    result=sender.execute(clock,first)
    assert publication.published.cpu().tolist()==([True,True] if prepublish else [True,False])
    assert random.counter[0,0].cpu().item()>0
    if prepublish:assert torch.equal(random.counter,published_counter)
    else:assert random.counter[1].cpu().tolist()==[0]*4
    assert publication.action.family.cpu().tolist()==[2,2]
    assert clock.public.pending_noe.active[1].sum().cpu().item()==2
    assert clock.public.waiting.received[1].sum().cpu().item()==0
    assert clock.runtime.input_frontier.next_sequence.sign.numel()==2
    assert clock.public.ledger.reservations.request_slots[1].cpu().tolist()==[-1,-1]
    assert values(clock.public.ledger.current_local_prefix)[1]==0
    first_sequence=values(clock.runtime.input_frontier.next_sequence)
    assert first_sequence==[3,2]
    old_counter=random.counter[0].clone();old_prefix=values(clock.public.ledger.current_local_prefix)[0]
    result=sender.execute(clock,~first)
    assert publication.published.cpu().tolist()==[True,True]
    assert torch.equal(random.counter[0],old_counter) and random.counter[1,0].cpu().item()>0
    if prepublish:assert torch.equal(random.counter,published_counter)
    assert sender.received.cpu().tolist()==[True,True]
    assert clock.public.pending_noe.active[1].sum().cpu().item()==0
    funds=run(clock.public.ledger.funds)
    assert exact(funds.actual_available)[1]==__import__('fractions').Fraction(step['cash']['actual_available_pusd'])
    assert publication.reservation.decision.outcomes[1].cpu().tolist()==[2,2]
    assert publication.reservation.decision.outcomes[0].cpu().tolist()==[1,1]
    assert values(clock.public.ledger.current_local_prefix)[0]==old_prefix
    assert values(clock.runtime.input_frontier.next_sequence)==[3,3]
    assert clock.observation_known.cpu().tolist()==[True,True]
    assert claims==[[True,False],[False,True]]
    after=random.counter.clone();state=clock.public
    sender.execute(clock,~first)
    assert torch.equal(random.counter,after) and clock.public is state
    return dict(passed=True,scope='real Captured two independent roots / enabled publication RNG / production first claim / current Ledger refusal; SIM advancement and request dispatch not tested',
        published_at_completion_before_partial_reservation=prepublish,unpublished_early_stage_not_complete=True,claims=claims,published=publication.published.cpu().tolist(),counters=random.counter.cpu().tolist(),
        funds_before=[str(x) for x in before_funds],next_input=values(clock.runtime.input_frontier.next_sequence),elapsed_seconds=time.perf_counter()-started)

if __name__=='__main__':
    print(json.dumps(check(json.loads(Path(sys.argv[1]).read_text()),sys.argv[2],sys.argv[3],prepublish='--prepublish' in sys.argv)),flush=True)
