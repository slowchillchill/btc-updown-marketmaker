"""原Native动作/原公共Ledger验证生产发布；不驱动SDK、发送或训练。"""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import json
from fractions import Fraction
import sys
import time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('model-train/scripts','sim-system/gpu','shared-run/gpu',
    'data-pipeline/gpu','ledger/gpu','execution-normalization/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational,values
from validate_portfolio_nav import cold_ledger,exact
from gpu_actor_support_validation import context
from public_ledger import PublicLedgerState
from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
from commitments import SubmissionBindings
from closed import ClosedAdmission
from projected_markets import ProjectedMarkets
from consumption import PendingRecords,ReceiveGates
from projection import Projection
from feedback import FeedbackEmitter
from fields import RecordList
from latency import EmpiricalDelaySampler
from action_selection import ActionControl
from actor_context import _owned
from input_cancel import CancelInputs
from .action_random import ActionRandom
from .action_inputs import ActorPolicyInputs
from .device_actor import CapturedDeviceActor,ActorSelection
from .gpu_policy import Policy100ms,ModelShape100ms



def make_parts(source,family,seed=181606,*,decision_required=True,actor=None):
    t=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool)
    actor=context(source) if actor is None else actor
    actor=replace(actor,condition_slots=zero.clone(),decision_required=t([decision_required],torch.bool),
        cancels=replace(actor.cancels,markets=torch.where(actor.cancels.kinds==2,0,actor.cancels.markets)))
    torch.manual_seed(181606);policy=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
    with torch.no_grad():
        policy.action.actor.family[-1].weight.zero_();policy.action.actor.family[-1].bias.zero_()
        policy.action.actor.family[-1].bias[family]=40.
        policy.action.actor.quantity.weight.zero_();policy.action.actor.quantity.bias.zero_()
        policy.action.actor.quantity.bias[7]=40.
        # 原36维前缀经真实编码/decoder进入16维variant隐藏轴。
        prefix=policy.action.prefix
        prefix[1].weight.zero_();prefix[1].bias.zero_()
        prefix[1].weight[0,12]=1.;prefix[1].weight[0,13]=-1.
        prefix[3].weight.zero_();prefix[3].bias.zero_();prefix[3].weight[0,0]=1.
        decoder=policy.action.actor.decoder[1]
        decoder.weight.zero_();decoder.bias.zero_()
        decoder.weight[0,48]=1.;decoder.weight[0,49]=-1.
        policy.action.actor.variant.weight.zero_();policy.action.actor.variant.bias.zero_()
        policy.action.actor.variant.weight[0,0]=40.;policy.action.actor.variant.weight[1,0]=-40.
        policy.action.actor.price.weight.zero_();policy.action.actor.price.bias.copy_(-torch.arange(999,device='cuda')*10.)
        policy.action.actor.fee_weight.weight.zero_()
    count=actor.cancels.present.size(1)
    cancels=CancelInputs(t(source['cancel_features'],torch.float32)[None,:] if 'cancel_features' in source and count else torch.zeros((1,count,184),device='cuda'),actor.cancels.present,
        torch.arange(count,device='cuda')[None,:],actor.fault)
    inputs=ActorPolicyInputs(actor,torch.randn(1,32,device='cuda'),torch.zeros((1,30),device='cuda'),cancels,
        zero+17,yes,actor.fault)
    random=ActionRandom(zero+seed,torch.zeros((1,4),dtype=torch.int64,device='cuda'))
    control=ActionControl(zero+8,zero+9,t([[10,11]]),t([source['cutoff']+1]))
    return policy,inputs,random,control


def compare_selected(native,source,publication,*,expected_variants=None,order_catalog=None):
    candidate=publication.candidate;action=publication.action
    original=native.actor(0)
    for choice in candidate.choices[0,:int(candidate.length.cpu()[0])].cpu().tolist():original.choose(choice)
    reference=original.validation_selection('actor-selection-request')
    wanted=reference['intent']
    if wanted is None:
        assert action.family.cpu().tolist()==[0]
    elif 'SubmitPairedBuy' in wanted or 'SubmitSingleBuy' in wanted:
        legs=wanted['SubmitPairedBuy']['orders'] if 'SubmitPairedBuy' in wanted else [wanted['SubmitSingleBuy']['order']]
        assert action.legs.present.cpu().tolist()==[[True,len(legs)==2]]
        for ordinal,leg in enumerate(legs):
            assert source['tokens'][action.assets[0,ordinal].cpu().item()]==leg['token_id']
            terms=action.legs.terms;variant,body=next(iter(leg['order'].items()))
            assert terms.variants[0,ordinal].cpu().item()=={'GtcLimit':0,'FokMarket':2}[variant]
            assert exact(terms.price)[ordinal]==Fraction(body.get('limit_price_pusd_per_share',body.get('worst_price_pusd_per_share')))
            if variant=='GtcLimit':assert exact(action.legs.target)[ordinal]==Fraction(body['size_shares'])
            else:assert exact(terms.spend)[ordinal]==Fraction(body['spend_pusd'])
        if expected_variants is not None:assert action.legs.terms.variants.cpu().tolist()==expected_variants
    elif 'CancelMarketOrders' in wanted:
        assert wanted['CancelMarketOrders']['condition_id']==source['condition_id']
        assert action.cancellation.market_kind.cpu().tolist()==[[True]]
        assert action.cancellation.markets.cpu().tolist()==[[0]]
    else:
        expected=[wanted['CancelOrder']['platform_order_id']] if 'CancelOrder' in wanted else wanted['CancelOrders']['platform_order_ids']
        present=action.cancel_present.cpu().tolist()[0]
        actual=[order_catalog[identity] for identity,enabled in zip(values(action.cancel_orders),present) if enabled]
        assert actual==expected,('原Native撤单身份与腿序',actual,expected)
    for ordinal,reserve in enumerate(reference['reserves']):
        assert exact(action.principal)[ordinal]==Fraction(reserve['requested_principal_pusd'])
        assert exact(action.fee)[ordinal]==Fraction(reserve['immediate_fee_reserve_pusd'])
    return reference


def public_state(ledger):
    return PublicLedgerState(None,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
        ClosedAdmission.empty(ledger.columns),ledger,submission_bindings=SubmissionBindings.empty(1,'cuda'))


def source_ledger(source):
    pm=SimpleNamespace(markets=[{'condition_id':source['condition_id']}],
        assets=[{'token_id':token,'outcome':{'index':i}} for i,token in enumerate(source['tokens'])],
        market_assets=[(0,i,None) for i in range(2)])
    return cold_ledger(source['checkpoint'],pm)


def run_single(native,source):
    started=time.perf_counter();run=Numbers.run_exact_batch
    initial=public_state(source_ledger(source));variants=[]
    intents=run(lambda:FrozenIntentOrders(torch.empty((1,0),dtype=torch.bool,device='cuda'),integers([]),
        torch.empty((1,0),dtype=torch.bool,device='cuda'),rational([]),
        torch.empty((1,0),dtype=torch.bool,device='cuda'),torch.empty((1,0),dtype=torch.bool,device='cuda')))
    before=run(lambda:initial.freeze_account(intents,torch.tensor([source['cutoff']],device='cuda')))
    for fok in (False,True):
        parts=make_parts(source,1);policy,inputs,random,control=parts
        if not fok:
            with torch.no_grad():
                policy.action.actor.variant.weight.zero_();policy.action.actor.variant.bias.copy_(
                    torch.tensor([40.,0.],device='cuda'))
        captured=CapturedDeviceActor(policy,inputs,random,path_capacity=32,word_capacity=16)
        publication=ActorSelection(random);current,_=captured.publish_and_reserve(publication,initial,control)
        compare_selected(native,source,publication)
        assert publication.published.cpu().tolist()==[True] and publication.reservation.complete.cpu().tolist()==[True]
        assert publication.reservation.decision.outcomes.cpu().tolist()[0][0]==1
        assert publication.action.legs.terms.variants.cpu().tolist()[0][0]==(2 if fok else 0)
        frozen=run(lambda:current.freeze_account(intents,control.execute_at))
        funds=run(current.ledger.funds)
        assert exact(frozen.funds.actual_available)==exact(funds.actual_available)
        assert exact(before.funds.actual_available)==[Fraction(source['checkpoint']['cash']['actual_available_pusd'])]
        counter=random.counter.clone();again,_=captured.publish_and_reserve(publication,current,control)
        assert again is current and torch.equal(random.counter,counter)
        variants.append(publication.action.legs.terms.variants.cpu().tolist()[0][0])
        print(json.dumps(dict(event='single_native_publication_checked',variant=variants[-1],elapsed_seconds=time.perf_counter()-started)),flush=True)
    return json.dumps(dict(single_original_body_and_reserve=True,variants=variants,public_freeze_reads_current_reservation=True,
        old_public_freeze_unchanged=True,elapsed_seconds=time.perf_counter()-started))


def run_cancel(native,source):
    started=time.perf_counter();run=Numbers.run_exact_batch
    t=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    original=source['actor_context']['cancel_intents']
    catalog=sorted({identity for intent in original for identity in
        ([intent['CancelOrder']['platform_order_id']] if 'CancelOrder' in intent else intent.get('CancelOrders',{}).get('platform_order_ids',[]))})
    physical=list(reversed(catalog));identity={name:i for i,name in enumerate(catalog)}
    pairs=[intent['CancelOrders']['platform_order_ids'] for intent in original if 'CancelOrders' in intent]
    count=len(original);orders=len(physical);pair_origins=[];members=[];markets=[]
    pair_number=0
    for intent in original:
        if 'CancelOrder' in intent:names=[intent['CancelOrder']['platform_order_id']]
        elif 'CancelOrders' in intent:names=intent['CancelOrders']['platform_order_ids']
        else:names=physical
        members.append([name in names for name in physical]);markets.append(0 if 'CancelMarketOrders' in intent else -1)
        pair_origins.append(pair_number if 'CancelOrders' in intent else -1)
        pair_number+=int('CancelOrders' in intent)
    base=context(source)
    actor=replace(base,cancels=replace(base.cancels,members=t([members],torch.bool),origin_pairs=t([pair_origins]),markets=t([markets])),
        order_identities=integers([identity[name] for name in physical]),
        paired_members=t([[[physical.index(name) for name in pair] for pair in pairs]]),condition_slots=t([0]))
    initial=public_state(source_ledger(source));checks=[]
    original_requests=initial.ledger.reservations.request_slots.clone()
    for family in (3,4,5):
        parts=make_parts(source,family,181629 if family==4 else 181606,actor=replace(actor,cancels=_owned(actor.cancels),
            order_identities=_owned(actor.order_identities),paired_members=actor.paired_members.clone()));policy,inputs,random,control=parts
        captured=CapturedDeviceActor(policy,inputs,random,path_capacity=32,word_capacity=16)
        # 已核实的有限工程种子；完整原支持及非零评分保持，不作覆盖搜索。
        publication=ActorSelection(random);current,_=captured.publish_and_reserve(publication,initial,control)
        compare_selected(native,source,publication,order_catalog=catalog)
        assert publication.published.cpu().tolist()==[True]
        ordinal=publication.candidate.prefix.cancel_index.cpu().tolist()[0]
        if family==4:assert ordinal==257
        assert random.counter.cpu().tolist()==[[1 if family==5 else 2,0,0,0]]
        counter=random.counter.clone();saved=values(publication.action.cancel_orders)
        inputs.actor.order_identities.words.fill_(0);inputs.actor.paired_members.fill_(0)
        inputs.actor.cancels.members.zero_();captured.prefix.cancel_index.fill_(-1)
        for _ in range(2):current,_=captured.publish_and_reserve(publication,current,control)
        assert values(publication.action.cancel_orders)==saved and torch.equal(random.counter,counter)
        assert values(current.ledger.current_local_prefix)==values(initial.ledger.current_local_prefix)
        assert exact(run(current.ledger.funds).actual_available)==exact(run(initial.ledger.funds).actual_available)
        assert torch.equal(current.ledger.reservations.request_slots,original_requests)
        checks.append(dict(family=family,global_ordinal=ordinal,seed=int(random.seed.cpu()[0])))
        print(json.dumps(dict(event='cancel_native_publication_checked',**checks[-1],elapsed_seconds=time.perf_counter()-started)),flush=True)
    return json.dumps(dict(original_single_pair_market_body=True,global_257_pair_original_leg_order=True,
        repeat_cancel_no_cash_prefix_or_rng_commit=True,old_order_directory_owned=True,checks=checks,
        source='original_frozen_Native_object_population_not_continuous_OrderFormation_host',elapsed_seconds=time.perf_counter()-started))

def run(native,source):
    started=time.perf_counter();run=Numbers.run_exact_batch
    def progress(event,**detail):
        print(json.dumps(dict(event=event,elapsed_seconds=time.perf_counter()-started,**detail)),flush=True)
    t=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool)
    tokens=source['tokens'];pm=SimpleNamespace(markets=[{'condition_id':source['condition_id']}],
        assets=[{'token_id':token,'outcome':{'index':i}} for i,token in enumerate(tokens)],market_assets=[(0,i,None) for i in range(2)])
    opening=cold_ledger(source['checkpoint'],pm)
    progress('opening_ready',cutoff=source['cutoff'])
    def state(ledger=opening):
        return PublicLedgerState(None,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
            ClosedAdmission.empty(ledger.columns),ledger,submission_bindings=SubmissionBindings.empty(1,'cuda'))
    def make(family,seed=181606,*,decision_required=True):
        return make_parts(source,family,seed,decision_required=decision_required)
    def driver(parts,capacity=32):
        policy,inputs,random,_=parts
        return CapturedDeviceActor(policy,inputs,random,path_capacity=capacity,word_capacity=16)
    def compare(publication):
        return compare_selected(native,source,publication,expected_variants=[[0,2]] if publication.action.family.cpu().tolist()==[2] else None)
    parts=make(2);small=driver(parts,1);publication=ActorSelection(parts[2]);initial=state()
    unchanged,_=small.publish_and_reserve(publication,initial,parts[3])
    assert publication.candidate is None and not publication.published.any().cpu().item()
    assert parts[2].counter.cpu().tolist()==[[0,0,0,0]] and unchanged is initial
    progress('prepublication_capacity_checked')
    captured=driver(parts)
    # Actor容量增长后同冻结/同游标完成。发布后预留槽仍只有1，保持已选两腿/RNG。
    pending,_=captured.publish_and_reserve(publication,initial,parts[3]);reference=compare(publication)
    assert publication.published.cpu().tolist()==[True] and not publication.reservation.complete.any().cpu().item()
    assert publication.reservation.decision.required_capacity.cpu().tolist()==[2]
    assert pending.ledger.reservations.request_slots.cpu().tolist()==[[-1]]
    assert values(pending.ledger.current_local_prefix)==values(initial.ledger.current_local_prefix)
    committed_random=parts[2].counter.clone();assert committed_random[0,0].cpu().item()>0
    progress('published_reservation_capacity_checked',choices=publication.candidate.choices.cpu().tolist())
    saved=_owned(publication.action)
    # 实际Actor暂存与控制目录覆写不能刷新旧动作；续接只消费原正文。
    captured.prefix.family.fill_(-1);parts[3].leg_requests.fill_(-1);parts[1].actor.assets.fill_(-1)
    widened=replace(pending,ledger=replace(pending.ledger,reservations=pending.ledger.reservations.resized(2),
        projected_markets=ProjectedMarkets.open(t([]),torch.empty((1,0),dtype=torch.bool,device='cuda'),yes)))
    market_pending,_=captured.publish_and_reserve(publication,widened,parts[3])
    assert not publication.reservation.complete.any().cpu().item()
    assert publication.reservation.decision.required_market_conditions.cpu().tolist()==[0]
    assert market_pending.ledger.reservations.request_slots.cpu().tolist()==[[-1,-1]]
    extended=replace(market_pending,ledger=replace(market_pending.ledger,
        projected_markets=market_pending.ledger.projected_markets.extended(zero)))
    reserved,_=captured.publish_and_reserve(publication,extended,parts[3])
    assert publication.reservation.complete.cpu().tolist()==[True]
    assert publication.reservation.decision.outcomes.cpu().tolist()==[[1,1]]
    assert exact(publication.action.principal)==exact(saved.principal)
    assert torch.equal(publication.action.assets,saved.assets) and torch.equal(parts[2].counter,committed_random)
    again,_=captured.publish_and_reserve(publication,reserved,parts[3]);assert again is reserved
    assert values(reserved.ledger.current_local_prefix)==[values(initial.ledger.current_local_prefix)[0]+2]
    funds=run(reserved.ledger.funds)
    total=sum(exact(saved.principal))+sum(exact(saved.fee))
    assert exact(funds.actual_available)==[exact(run(initial.ledger.funds).actual_available)[0]-total]
    progress('reservation_and_market_growth_checked')
    # 原SIM ExternalCash生成的Evidence/Economic连续追加；after只作CPU oracle。
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),sha,[b'e'*64])
    projection=run(lambda:Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,t([0,0]),integers([0]),zero,zero,zero,zero))
    identities={};cash_state=state(replace(opening,reservations=opening.reservations.resized(4)))
    first_delivery=source['cash_steps'][0]['rows'][0]['visibility']['Feedback']['delivery_sequence']
    empty=opening.columns.enabled
    cash_state=replace(cash_state,pending_noe=run(lambda:PendingRecords.from_columns(empty,opening.columns.row_refs,
        integers([]),integers([]),opening.columns.row_refs,integers([first_delivery-1]))),
        ledger=replace(opening.resized_settlement_identities(cash=3),reservations=opening.reservations.resized(4)))
    actual=maker=None
    for step in source['cash_steps']:
        rows=step['rows'];life=next(iter(rows[0]['record'].values()))
        fact=life['evidence']['SettlementLifecycleV2']['result']['ExternalCash']
        identities.setdefault(fact['identity'],len(identities))
        private=life['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        batch_sequence=rows[0]['position']['source_watermark']['projection_batch_sequence']
        projection=replace(projection,emitter=replace(projection.emitter,next_private_sequence=integers([private]),
            next_batch_sequence=integers([batch_sequence])))
        economic=next(iter(rows[1]['record'].values()))['fact']
        value=economic['disposition']['AccountTransition'];balance=value['balance_delta'];existing=balance['existing']
        vector=RecordList((t([]),rational([])),torch.empty((1,0),dtype=torch.bool,device='cuda'))
        sequence=economic['economic_identity']['AccountTransition']['account_transition_seq']
        projection,batch=run(lambda:projection.external_cash_feedback(sampler,t([identities[fact['identity']]]),
            rational([fact['signed_delta_pusd']]),t([2]),t([fact['correction_of'] is not None],torch.bool),
            t([identities.get(fact['correction_of'],0)]),integers([sequence]),
            tuple(rational([existing[name+'_delta']]) for name in ('cash_pusd','available_pusd','principal_encumbrance_pusd',
                'immediate_taker_fee_encumbrance_pusd','maker_rebate_receivable_pusd','taker_rebate_receivable_pusd',
                'maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')),vector,vector,
            rational([balance['settlement_fee_liability_pusd_delta']]),rational([balance['actual_taker_fee_liability_pusd_delta']]),
            t([life['source_reference']['record']['SettlementLifecycle']['platform_event_at_utc_ms']]),yes,t([0],torch.int32),
            economic_present=yes,source_ordinal=t([life['source_order']['ordinal']]),
            record_ordinal=integers([rows[0]['position']['record_ordinal']])))
        cash_state=run(lambda:cash_state.append_batch(projection.normalizer.bank,batch))
        gates=ReceiveGates.unset(1).set_receive(t([step['cutoff']]),yes).set_public(t([step['cutoff']]),yes)
        cash_state,delivery=run(lambda:cash_state.receive(projection.normalizer.bank,gates,cash_state.ledger.columns.enabled))
        assert delivery.fault.cpu().tolist()==[0]
        funds=run(cash_state.ledger.funds)
        assert exact(funds.actual_available)==[Fraction(step['cash']['actual_available_pusd'])]
        assert exact(funds.maker_available)==[Fraction(step['cash']['maker_available_pusd'])]
        assert values(cash_state.ledger.current_entry_sequence)==[step['projection']['ledger_entry_seq']]
        if actual is None:
            parts=make(2);capture=driver(parts);selection=ActorSelection(parts[2])
            cash_state,_=capture.publish_and_reserve(selection,cash_state,parts[3]);compare(selection)
            assert selection.reservation.decision.outcomes.cpu().tolist()==[[2,2]]
            actual=(parts,capture,selection,parts[2].counter.clone())
        elif maker is None:
            parts=make(2,181607);parts[3].global_requests.add_(20);parts[3].leg_requests.add_(20)
            capture=driver(parts);selection=ActorSelection(parts[2])
            cash_state,_=capture.publish_and_reserve(selection,cash_state,parts[3]);compare(selection)
            assert selection.reservation.decision.outcomes.cpu().tolist()==[[3,3]]
            maker=(parts,capture,selection,parts[2].counter.clone())
        else:
            for parts,capture,selection,counter in (actual,maker):
                repeated,_=capture.publish_and_reserve(selection,cash_state,parts[3])
                assert repeated is cash_state and torch.equal(counter,parts[2].counter)
            assert actual[2].reservation.decision.outcomes.cpu().tolist()==[[2,2]]
            assert maker[2].reservation.decision.outcomes.cpu().tolist()==[[3,3]]
        progress('cash_step_checked',cutoff=step['cutoff'],actual=step['cash']['actual_available_pusd'],maker=step['cash']['maker_available_pusd'])
    for family,required in ((0,True),(5,True),(0,False)):
        parts=make(family,decision_required=required);captured=driver(parts);selection=ActorSelection(parts[2])
        current,_=captured.publish_and_reserve(selection,initial,parts[3])
        if required:
            compare(selection);assert selection.published.cpu().tolist()==[True]
            assert parts[2].counter.cpu().tolist()==[[1,0,0,0]]
        else:
            assert selection.published.cpu().tolist()==[False] and selection.reservation is None
            assert parts[2].counter.cpu().tolist()==[[0,0,0,0]]
        prefix=values(current.ledger.current_local_prefix);counter=parts[2].counter.clone()
        for _ in range(2):current,_=captured.publish_and_reserve(selection,current,parts[3])
        assert values(current.ledger.current_local_prefix)==prefix and torch.equal(counter,parts[2].counter)
        assert current.ledger.reservations.request_slots.cpu().tolist()==[[-1]]
        progress('repeat_family_checked',family=family,decision_required=required)
    return json.dumps({'actor_selection_public_reservation':True,'same_native_body_and_reserves':True,
        'prepublication_actor_capacity':True,'postpublication_reservation_and_market_capacity':True,
        'repeat_buy_notrade_cancel_noact':True,'original_actual_and_maker_refusals_after_cash_correction':True,
        'sdk_sent':False,'elapsed_seconds':time.perf_counter()-started})
