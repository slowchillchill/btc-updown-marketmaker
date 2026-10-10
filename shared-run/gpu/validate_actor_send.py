"""原有限FOK来源→真实compact/Ck→开放sk/首次Ledger领取→Captured Actor实际发送。"""
from dataclasses import replace
from pathlib import Path
from fractions import Fraction
import importlib.util,json,os,sys,time,hashlib
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('model-train','sim-system/gpu','shared-run/gpu','data-pipeline/gpu',
    'ledger/gpu','execution-normalization/gpu','model-train/native-inputs/gpu')]
from validate_pending_post import check,T,run,integers,rational,values,amounts,checkpoint,original_semantic
from public_source import PublicGroupSource
from send_clock import BoundedSendClock
from actor_send import FrozenActorSend,IntentRequestIdentities,ActorRequestSubmission
from hidden_inputs import HiddenPmBlock
from hidden_frontier import HiddenBucketColumns,HiddenBucketFrontier
from scheduling import ScheduledTime
from core import EvidenceBank
from market_economics import UnresolvedMarkets
from queues import Numbers
from public_device_page import PreparedPublicPage,build_public_device_page,build_public_device_blocks
from hidden_device_page import PreparedHiddenMaterial,build_hidden_device_columns
from validate_joint_freeze import install_opening
from registry import FrozenIntentOrders,PublicCancellationState
from commitments import SubmittedLegs,bind_submission_receipt
from intent_terms import SubmittedBuyTerms
from cancel_objects import CancelObjectDirectory
from actor_context import publish_actor_context
from input_account import account_inputs_at_prefix
from input_history import SourceHistoryInputs
from input_100ms import MarketInputs100ms,MarketFreezeMemory
from portfolio_nav import PortfolioValuation
from model_train.actor_selection_validation import make_parts
from model_train.device_actor import CapturedDeviceActor,ActorSelection
from action_selection import ActionControl
from latency import byte_column
from pending_post_intents import _decimal
from input_frontier import InputCommitFrontier


def receive_current(state,delivery,stage,enabled=None):
    enabled=torch.ones_like(state.ledger.fault,dtype=torch.bool) if enabled is None else enabled
    while True:
        before=int((state.waiting.received&~state.waiting.consumed).sum().cpu())
        if not before:break
        ledger,waiting=run(lambda:state.waiting.drain(state.ledger,enabled[:,None].expand_as(state.ledger.columns.enabled),passes=1))
        state=replace(state,ledger=ledger,waiting=waiting)
        after=int((waiting.received&~waiting.consumed).sum().cpu())
        stage('receive_round',remaining=after,consumed=before-after,fault=ledger.fault.cpu().tolist())
        if after==before:break
    return state,~(state.waiting.received&~state.waiting.consumed).any(1)&(state.ledger.fault==0)


def bounded(source,runtime,public,noe,sampler,rule,slots,owners,receipt,intents,ask_directory,taker,maker,progress,codec,artifact,*,clock_only=False,initial_submit=None,no_trade=False,continuous=None):
    yes=T([True],torch.bool);zero=T([0]);started=time.perf_counter()
    timing=source['bounded_timing'];ck=T([timing['observation_at']]);sk=ck+timing['calculation_duration']+timing['dispatch_delay']
    assert sk.cpu().tolist()==[1414] and source['market_end']>1415
    # 原源没有GTD/FAK、结算请求/市场终态；所有已登记正文均是正式GTC/FOK。
    assert all(next(iter(p['item']['order']['kind'])) in ('GtcLimit','FokMarket') for p in source['posts'])
    assert not source['opening_ledger']['resolutions'] and not source['opening_ledger']['settlement_costs']
    compact=artifact/'public-compact';page=artifact/'public-page'
    if os.environ.get('B_GPU_PUBLIC_BLOCKS'):
        prepared=build_public_device_blocks(os.environ['B_GPU_COLUMNS_BINARY'],compact,page,groups_per_block=2)
    else:
        build_public_device_page(os.environ['B_GPU_COLUMNS_BINARY'],compact,page)
        prepared=PreparedPublicPage(page,compact)
    pm,market,reference,binance=prepared.install()
    public_source=PublicGroupSource(prepared,1,page_groups=3 if source.get('continuous_two_markets') else 2,observation_inputs=continuous is not None)
    hidden_source=source['bounded_hidden']
    assets=hidden_source['assets'];tokens=[asset['token_id'] for asset in assets]
    block=HiddenPmBlock(assets,[(tokens[a],side.upper().removesuffix('S'),_decimal(price)) for a,side,price in slots],
        source_binding=hidden_source['source_binding'])
    hidden_columns=artifact/'hidden-columns'
    build_hidden_device_columns(block,hidden_columns,hidden_source['source_binding'])
    prepared_hidden=PreparedHiddenMaterial(hidden_columns,hidden_source['source_binding'])
    block=HiddenPmBlock.from_prepared(prepared_hidden.page(0,min(1,len(block.buckets))))
    runtime=replace(runtime,hidden=run(lambda:block.empty_state(segment_capacity=runtime.hidden.queues.capacity)),
        hidden_frontier=HiddenBucketFrontier.open(HiddenBucketColumns.from_block(block),1))
    runtime=runtime.install_hidden_market_controls()
    public=replace(public,ledger=run(lambda:install_opening(replace(public.ledger,
        unresolved_markets=UnresolvedMarkets.open(public.ledger.success_pools,T(list(range(0,len(assets),2))))),source)),
        cancellations=run(lambda:PublicCancellationState.open(integers([]),T([[]],torch.bool))))
    bank=lambda p:p.normalizer.bank if p.normalizer is not None else EvidenceBank.empty('cuda')
    end=T([source['market_end']]*len(assets))
    batches=[]
    def refresh(world,choice):return replace(choice,requests=world.requests,fills=world.fills,calendars=world.calendars,
        hidden_frontier=world.hidden_frontier,settlement_registrations=world.settlement_registrations,order_query_history=world.order_query_history)
    def hidden(world,projection,choice):
        block=world.hidden_frontier.columns.source
        world=world.begin_hidden_bucket(choice)
        for i in range(block.reference_rows.size(0)):
            world=run(lambda:world.apply_hidden_reference(refresh(world,choice),T([i])))
        for i in range(len(block.operations)):
            world,effect=run(lambda:world.apply_hidden_step(refresh(world,choice),i,T([0]*len(assets)),
                T([[0]*world.hidden.queues.capacity]),rational([source['rule']['fee_rate']]),source['rule']['fee_exponent'],taker))
            # 本代表只有真实初始化/等量PriceChange，不冒充未装配的Maker成交宿主。
            assert not getattr(effect,'frames',())
        world,closed=run(world.finish_hidden_bucket)
        assert closed.cpu().tolist()==[True],world.hidden_frontier.fault.cpu().tolist()
        return world,projection,None,None
    def post(world,projection,choice):
        world=world.advance_role_time(choice,end);choice=refresh(world,choice)
        pending=world.requests.next_pending();refs=pending.item_ref
        asks=ask_directory[intents.assets.gather(1,refs[:,None])[:,0]]
        world,projection,batch,effect=world.post_pending(choice,intents,rule,codec.post_terms_batch_v2,asks,
            T([[0]*(asks.size(1)*world.hidden.queues.capacity)]),taker,owners,refs,projection,sampler,T(list(range(len(assets)))))
        batches.append(batch)
        return world,projection,batch,effect
    def finality(world,projection,choice):
        world,frame,effect=run(lambda:world.finality(choice,maker,taker,end))
        projection,batch=run(lambda:projection.fill_from_origin(sampler,world.orders,world.fills,frame,
            T(list(range(len(assets)))),world.calendars,taker,choice.at))
        batches.append(batch);return world,projection,batch,effect
    if initial_submit is not None:
        frontier=run(lambda:InputCommitFrontier.open(1,source["run_id"],source["session_id"]))
        # 原来源实际CreateSession/ActivateSession的冷初始化已成功，不移植CPU物理编号。
        frontier,_=run(lambda:frontier.commit(yes));frontier,_=run(lambda:frontier.commit(yes))
        runtime=replace(runtime,input_frontier=frontier)
    clock=BoundedSendClock(runtime,noe,public,public_source,{4:hidden,5:post,2:finality},ScheduledTime.absent(zero),bank,
        hidden_page_buckets=2)
    trace=[]
    def stage(name,**row):
        value=dict(stage=name,elapsed_seconds=time.perf_counter()-started,**row);trace.append(value);progress(value)
    if initial_submit is not None:
        advance=clock.advance_before(T([source["sent_at"]]),yes,progress=stage)
        assert not advance.fault.any().cpu().item() and advance.complete.cpu().item()
        old_input=clock.runtime.input_frontier.next_sequence
        clock.runtime,ledger,receipt,enough=run(lambda:initial_submit(clock.runtime,clock.public.ledger,old_input))
        assert enough.cpu().tolist()==[True] and receipt.registered.cpu().tolist()==[True]
        frontier,fault=run(lambda:clock.runtime.input_frontier.commit(receipt.registered))
        assert not fault.any().cpu().item()
        clock.runtime=replace(clock.runtime,input_frontier=frontier);clock.public=replace(clock.public,ledger=ledger)
        stage("original_request_commit",gpu_input_sequence=values(receipt.input_sequence),cpu_reference_input=source["input_sequence"])
        assert receipt.arrived_at.cpu().tolist()==[source["receipt"]["arrived_at"]]
    advance=clock.advance_before(ck,yes,progress=stage)
    assert not advance.fault.any().cpu().item() and advance.complete.cpu().item()
    assert not clock.public.pending_noe.active.any().cpu().item()
    checkpoint(clock.public.ledger,source['sent_ledger'])
    assert values(clock.runtime.account.cash)==[int(Fraction(source['cash'])*1000000)]
    if continuous is not None:
        return continuous(source,clock,pm,market,reference,binance,block,sampler,rule,receipt,intents,
            ask_directory,taker,maker,codec,artifact,stage)
    # 只消费实际完成Ck的完整原group，并将该位置上限交给原100ms编码器。
    source_snapshot=public_source.snapshot();prefix=public_source.prefix_at(pm,ck)
    decision_cutoff=None
    if clock.runtime.input_frontier is not None:
        frontier,decision_cutoff,fault=run(lambda:clock.runtime.input_frontier.observe(ck,clock.public.pending_noe,
            clock.public.waiting,yes))
        assert not fault.any().cpu().item()
        clock.runtime=replace(clock.runtime,input_frontier=frontier)
    assert len(prepared.columns['source_progress_changes'])==len(prepared.columns['groups'])
    history=SourceHistoryInputs(pm,market,reference,prepared.columns);start=zero-29000
    memory=history.empty(1,start)
    through=int(public_source.position.cpu()[0])
    if through:
        groups=torch.arange(through,device='cuda')
        memory,error=run(lambda:history.consume(groups,pm.causal_visible_at[groups],zero,start,memory))
        assert not error.any().cpu().item()
    encoder=MarketInputs100ms(pm,market,reference,binance,history,rule,prepared.columns,pm.causal_visible_at)
    endpoints=MarketFreezeMemory(public_source.prefix_at(pm,ck-100),ck-100,None,None)
    legs=run(lambda:SubmittedLegs(T([[True,False]],torch.bool),T([[False,False]],torch.bool),T([[0,-1]]),T([[0,1]]),
        rational(['1000.001','0']),T([[True,False]],torch.bool),T([[0,-1]]),T([[0,1]]),T([[True,False]],torch.bool),
        SubmittedBuyTerms(T([[2,0]]),T([[False,False]],torch.bool),rational(['0.5','0']),rational(['500.0005','0']),T([[0,-1]]))))
    legs=run(lambda:bind_submission_receipt(legs,zero,receipt))
    empty=T([[]]);directory=run(lambda:FrozenIntentOrders(empty.bool(),integers([]),empty.bool(),rational([]),empty.bool(),empty.bool()))
    valuation=PortfolioValuation(pm,tokens,clock.public.ledger.success_pools.asset_roots)
    known={name:yes for name in ('executed_prefix_known','success_prefix_known','rebate_prefix_known','settlement_prefix_known',
        'ledger_prefix_known','economic_liability_prefix_known','economic_prefix_known')}
    def freeze():
        market_freeze=encoder.freeze(ck,zero,start,memory,endpoints,applied_prefix=prefix)
        frame=market.frame(market_freeze.prefixes)
        cancel_markets=CancelObjectDirectory(frame.admitted,market.times[:,0][None,:],zero[:,None],yes[:,None],
            empty.bool(),empty.reshape(1,0,2),empty)
        account=clock.public.freeze_account(directory,ck,submitted_legs=legs,cancel_markets=cancel_markets,request_identity_order=torch.zeros_like(legs.request_slots))
        quoted=valuation.freeze(market_freeze.prefixes,ck)
        native=account_inputs_at_prefix(account,market,zero,market_freeze.prefixes,start,empty,empty.bool(),
            T([a['outcome']['index'] for a in assets]),row_role=market_freeze.role,valuation=quoted,**known)
        actor=publish_actor_context(account,native,encoder,market_freeze,zero,zero,T([[0,0,0]]),account.cancel_objects)
        return market_freeze,native,account,actor
    words=16
    while True:
        frozen=Numbers.device_exact_batch(freeze,word_capacity=words,device='cuda')
        market_freeze,native,account,actor=frozen.value
        if bool((frozen.fits&market_freeze.resources_fit.all()).cpu()):break
        words=1<<(max(4,int(frozen.required_words.cpu()))-1).bit_length()
        encoder.grow_resources(market_freeze.required.max(0).values.cpu().tolist())
    stage("freeze_check",actor_fault=actor.fault.cpu().tolist(),native_fault=native.inputs.fault.cpu().tolist(),
        account_fault=account.fault.cpu().tolist(),market_fault=market_freeze.fault.cpu().tolist(),
        actual_prefix=market_freeze.prefixes.cpu().tolist(),source_prefix=prefix.cpu().tolist(),
        funds_fault=account.funds.fault.cpu().tolist(),registry_fault=account.orders.fault.cpu().tolist())
    assert actor.fault.cpu().tolist()==[0] and torch.equal(market_freeze.prefixes,prefix)
    cash_before_freeze=values(actor.cash_micro)
    if clock_only:
        advance=clock.advance_before(sk,yes,progress=stage)
        assert not advance.fault.any().cpu().item() and advance.complete.cpu().item()
        delivery=run(clock.receive_first)
        clock.public,ready=receive_current(clock.public,delivery,stage)
        assert ready.cpu().tolist()==[True]
        checkpoint(clock.public.ledger,source['bounded_pre_send_ledger'])
        assert values(actor.cash_micro)==cash_before_freeze
        unreceived=clock.public.pending_noe.received_at[clock.public.pending_noe.active].cpu().tolist()
        assert unreceived and min(unreceived)==1414 and max(unreceived)==1415
        assert clock.runtime.hidden_frontier.scheduled()[0].at.cpu().tolist()==[1414]
        assert clock.runtime.hidden_frontier.scheduled()[0].present.cpu().item()
        next_public=public_source.next()
        assert next_public.at.cpu().tolist()==[1414] and not next_public.eof.cpu().item()
        availability=delivery.closed.available_at[delivery.closed.emitted].cpu().tolist()
        assert availability and all(at==1414 for at in availability)
        return dict(passed=True,scope='actual source clock / Ck freeze / first public Ledger claim; Actor sending not run',
            ck=1300,sk=1414,retained_received_at=unreceived,first_available_at=availability,
            source_position=public_source.position.cpu().tolist(),trace=trace,elapsed_seconds=time.perf_counter()-started)
    policy,inputs,random,control=make_parts({'cutoff':1300},1 if no_trade else 2,actor=actor)
    # 保留完整合法域，工程模型仅偏好原冷价位0.4的GTC，真正采样仍耗业务随机。
    with torch.no_grad():
        policy.action.actor.variant.weight.zero_();policy.action.actor.variant.bias.copy_(T([40,0]).float())
        if not no_trade:
            policy.action.actor.quantity.bias.zero_();policy.action.actor.quantity.bias[0]=40.
        policy.action.actor.price.bias.zero_();policy.action.actor.price.bias[39]=40.
    from model_train.frozen_inputs import FrozenPolicyInputs100ms
    from model_train.action_inputs import ActorPolicyInputs
    frozen_model=FrozenPolicyInputs100ms.publish(market_freeze,native.inputs,T([[0,0,0]]),frozen.fits)
    assert frozen_model.provided.all().cpu().item()
    with torch.no_grad():evaluation=policy.frozen_state_and_value(frozen_model,torch.zeros((1,1,32),device='cuda'),T([[0,0,0]]),zero+17)
    assert evaluation.ready.cpu().tolist()==[True] and evaluation.fault.cpu().tolist()==[0]
    from cancel_objects import global_cancel_slots
    slots_at=global_cancel_slots(actor.cancels)
    # 工程模型的零取消特征不参与本次Buy评分；真实全局对象顺序仍必须保持。
    cancels=replace(inputs.cancels,values=inputs.cancels.values.gather(1,slots_at[:,:,None].expand(-1,-1,184)),
        present=inputs.cancels.present.gather(1,slots_at),physical_slots=slots_at)
    inputs=ActorPolicyInputs.publish(actor,market_freeze,frozen_model,evaluation,cancels)
    from cancel_objects import global_cancel_slots
    stage("actor_inputs",fault=inputs.fault.cpu().tolist(),actor_fault=actor.fault.cpu().tolist(),
        market_role=market_freeze.role.cpu().tolist(),decision_required=actor.decision_required.cpu().tolist(),
        evaluation_fault=evaluation.fault.cpu().tolist(),cancel_fault=inputs.cancels.fault.cpu().tolist(),
        supplied_cancel_slots=inputs.cancels.physical_slots.cpu().tolist(),original_cancel_slots=global_cancel_slots(actor.cancels).cpu().tolist())
    control=ActionControl(zero+1,zero+1,T([[1,2]]),sk)
    driver=CapturedDeviceActor(policy,inputs,random,word_capacity=words,path_capacity=32)
    publication=ActorSelection(random);counter=random.counter.clone();cash=values(actor.cash_micro);old_prefix=prefix.clone()
    identity,offsets=byte_column([source['request_id'].encode(),b'bounded-actor-send'])
    prices={(a,price):i for i,(a,side,price) in enumerate(slots) if side=='bids'}
    for a in range(len(assets)):prices[a,None]=next(i for i,(owner,side,_) in enumerate(slots) if owner==a and side=='bids')
    submit=ActorRequestSubmission(control,T([[1,2]]),sampler,rule,identity,offsets,
        ((source['initial'][0]['market']['condition_id'],),),(tuple(tokens),),(prices,),((source['market_end'],),),codec,post_intents=intents)
    def receive(state,delivery,enabled):return receive_current(state,delivery,stage,enabled)
    identities=IntentRequestIdentities(({'global_request_id':'bounded-actor-send','caller_correlation_id':None},),
        ((source['initial'][0]['market']['condition_id'],),),(tuple(tokens),),({},))
    sender=FrozenActorSend(publication,driver,control,identities,codec,submit,receive,source_snapshot,decision_cutoff=decision_cutoff)
    cutoff_reference=decision_cutoff.receipt.original_reference(0)
    assert cutoff_reference["group_position"]==int(source_snapshot[0].cpu()[0])-1
    assert values(decision_cutoff.private_feedback_sequence)==[0]
    # 原宿主自动安装缺页，同一发送事件沿原开放时间端点完成。
    result=sender.execute(clock,yes,progress=stage)
    assert result.advance.complete.cpu().item()
    stage("publication_state",action_fault=None if publication.action is None else publication.action.fault.cpu().tolist(),
        published=publication.published.cpu().tolist(),candidate_complete=None if publication.candidate is None else publication.candidate.complete.cpu().tolist(),
        required_words=None if publication.required_words is None else publication.required_words.cpu().tolist(),
        word_capacity=driver.word_capacity,reservation_present=publication.reservation is not None,ledger_fault=clock.public.ledger.fault.cpu().tolist(),
        family=None if publication.action is None else publication.action.family.cpu().tolist(),
        choices=None if publication.candidate is None else publication.candidate.choices.cpu().tolist(),
        original_family_support=run(lambda:driver.actor.support(driver.actor.initial())).family.cpu().tolist())
    if no_trade:
        assert publication.action.family.cpu().tolist()==[0] and result.receipt is None
        assert sender.requests is None and not result.sent.any().cpu().item()
        assert random.counter[0,0].cpu().item()==counter[0,0].cpu().item()+1
        assert values(clock.runtime.input_frontier.next_sequence)==next(r["input_sequence"] for r in trace if r["stage"]=="publication_boundary")
        checkpoint(clock.public.ledger,source["bounded_pre_send_ledger"])
        saved_counter=random.counter.clone();saved_seq=values(clock.runtime.input_frontier.next_sequence)
        duplicate=sender.execute(clock,yes,progress=stage)
        assert duplicate.receipt is None and sender.requests is None
        assert torch.equal(random.counter,saved_counter) and values(clock.runtime.input_frontier.next_sequence)==saved_seq
        checkpoint(clock.public.ledger,source["bounded_pre_send_ledger"])
        return dict(passed=True,scope="real NoTrade publication/RNG once; no request/receipt or reservation; repeated stable",
            family=publication.action.family.cpu().tolist(),business_random= random.counter.cpu().tolist(),
            input_sequence=saved_seq,elapsed_seconds=time.perf_counter()-started)
    # 容量不足时发布可能已完成；只扩原预留/请求人口，不改变正文或随机流。
    if result.receipt is None and sender.publication.reservation is not None:
        published_counter=random.counter.clone();published_body=publication.action
        old=clock.public.ledger;required=int(sender.publication.reservation.decision.required_capacity.max().cpu())
        stage("published_capacity_continuation",capacity=old.reservations.capacity,required=required,
            counter=published_counter.cpu().tolist(),input_sequence=values(clock.runtime.input_frontier.next_sequence))
        if required>old.reservations.capacity:
            pad=required-old.reservations.capacity
            clock.public=replace(clock.public,ledger=replace(old,reservations=old.reservations.resized(required),
                global_requests=torch.nn.functional.pad(old.global_requests,(0,pad),value=-1),item_ordinals=torch.nn.functional.pad(old.item_ordinals,(0,pad),value=-1)))
        clock.runtime=replace(clock.runtime,requests=clock.runtime.requests.resized(2,2))
        result=sender.execute(clock,yes,progress=stage)
        assert publication.action is published_body and torch.equal(random.counter,published_counter)
    stage("send_decision",sent=result.sent.cpu().tolist(),receipt_present=result.receipt is not None,
        reservation_complete=publication.reservation.complete.cpu().tolist(),
        reservation_outcomes=publication.reservation.decision.outcomes.cpu().tolist(),
        required_capacity=publication.reservation.decision.required_capacity.cpu().tolist(),
        principal=[str(x) for x in amounts(publication.action.principal)],fee=[str(x) for x in amounts(publication.action.fee)])
    assert result.receipt is not None and result.sent.cpu().tolist()==[True]
    assert values(submit.submitted_legs.input_sequences)[0]==values(result.receipt.input_sequence)[0]
    assert values(clock.runtime.input_frontier.next_sequence)==[values(result.receipt.input_sequence)[0]+1]
    assert not torch.equal(counter,random.counter)
    assert values(actor.cash_micro)==cash and torch.equal(old_prefix,prefix) and torch.equal(sender.observation[0],source_snapshot[0])
    assert clock.runtime.hidden_frontier.scheduled()[0].at.cpu().tolist()==[1414]
    assert clock.runtime.hidden_frontier.scheduled()[0].present.cpu().item()
    unreceived=clock.public.pending_noe.received_at[clock.public.pending_noe.active].cpu().tolist()
    assert unreceived and min(unreceived)==1414 and max(unreceived)==1415
    received=clock.public.pending_noe.received_at[~clock.public.pending_noe.active].cpu().tolist()
    assert received and all(at<1414 for at in received)
    saved=(random.counter.clone(),values(clock.public.ledger.current_local_prefix),clock.runtime.requests.done.clone(),result.receipt,
        values(clock.runtime.input_frontier.next_sequence))
    duplicate=sender.execute(clock,yes,progress=stage)
    assert duplicate.receipt is saved[3] and torch.equal(saved[0],random.counter)
    assert values(clock.public.ledger.current_local_prefix)==saved[1] and torch.equal(saved[2],clock.runtime.requests.done)
    assert values(clock.runtime.input_frontier.next_sequence)==saved[4]
    assert submit.cutoff is decision_cutoff and cutoff_reference==decision_cutoff.receipt.original_reference(0)
    assert clock.last_observation.observed_at.cpu().tolist()==[1414]
    assert values(clock.last_observation.private_feedback_sequence)[0]>0
    probe=PublicGroupSource(prepared,1)
    while not probe.next().eof.cpu().item():
        if not probe.next().present.cpu().item():probe.load_next(1)
        assert probe.consume(yes).cpu().item()
    assert probe.position.cpu().tolist()==[len(prepared.columns["groups"])]
    return dict(passed=True,ck=1300,sk=1414,received_before_sk=received,retained_received_at=unreceived,
        actual_receipt_at=result.receipt.arrived_at.cpu().tolist(),business_random_before=counter.cpu().tolist(),
        business_random_after=random.counter.cpu().tolist(),logical_timing=timing,full_continuous_host_delivered=False,
        input_sequence=values(result.receipt.input_sequence),next_input=values(clock.runtime.input_frontier.next_sequence),
        ck_input_sequence=values(decision_cutoff.receipt.input_sequence),cutoff_reference=cutoff_reference,
        claimed_private_prefix=values(clock.last_observation.private_feedback_sequence),end_of_input_probe=probe.next().eof.cpu().tolist(),elapsed_seconds=time.perf_counter()-started)


def main():
    source=json.load(sys.stdin);artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    module_path=Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so'
    spec=importlib.util.spec_from_file_location('model_native_inputs',module_path);codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    def progress(row):
        with (artifact/'actor-send-progress.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
    report=check(source,codec.post_terms_batch_v2,progress,before_drive=lambda *args,**kwargs:bounded(*args,codec,artifact,clock_only='--source-clock' in sys.argv,no_trade='--no-trade' in sys.argv,**kwargs),
        defer_initial_send='--source-clock' not in sys.argv)
    print(json.dumps(report),flush=True)

if __name__=='__main__':main()
