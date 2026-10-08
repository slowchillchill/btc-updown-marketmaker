"""同一真实有界账户的连续观察/Actor/处理/公开形成/取消；CPU仅后验。"""
from validate_actor_send import *
from actor_send import SubmittedActorRequests,decimal_values
from continuous_actor import ContinuousActorRun
from continuous_execution import ContinuousExecution
from model_train.continuous_actor import ActorModelQueue
from model_train.continuous_inputs import ContinuousActorInputs
from model_train.action_random import ActionRandom
from model_train.action_inputs import ActorPolicyInputs
from model_train.gpu_policy import Policy100ms,ModelShape100ms
from cancel_projection import CancellationTargetHistory,targets_from_rows
from cancel_objects import global_cancel_slots
from cancel_requests import CancellationScopes,freeze_cancel_snapshot,freeze_scoped_cancel_requests
from input_cancel import cancel_inputs
from pending_cancel_intents import CancelFailureScenario
from asset_buy import take_ratio
from queues import _take
import copy
from dataclasses import fields,is_dataclass


def stored_actual(value):
    if value is None:return None
    if isinstance(value,torch.Tensor):return value.detach().cpu().tolist()
    if hasattr(value,'words') and hasattr(value,'sign'):return {'integer':values(value)}
    if hasattr(value,'numerator') and hasattr(value,'denominator'):return {'rational':[str(x) for x in amounts(value)]}
    if is_dataclass(value):return {f.name:stored_actual(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,(tuple,list)):return [stored_actual(x) for x in value]
    return value


def diagnostic_columns(value,names):
    def plain(column):
        if isinstance(column,torch.Tensor):return column.detach().cpu().tolist()
        if column is None:return None
        return values(column)
    return {name:plain(getattr(value,name)) for name in names}


def continuous(source,clock,pm,market,reference,binance,block,sampler,rule,receipt,intents,asks,taker,maker,codec,artifact,stage,*,second_buy=False):
    evidence=[];original_stage=stage
    def stage(name,**row):
        if name=="continuous_event":
            row.update(business_random=random.counter.cpu().tolist(),history_prefix=material.memory.last_prefix.cpu().tolist(),
                published=[s.publication.published.cpu().tolist() for s in senders],receipts=[s.receipt is not None for s in senders])
            evidence.append(dict(stage=name,**row))
        original_stage(name,**row)
    started=time.perf_counter();zero=T([0]);yes=T([True],torch.bool);empty=T([[]]);start=zero-29000
    identities=T([[0,0,0]]);condition=source['initial'][0]['market']['condition_id'];tokens=[a['token_id'] for a in source['bounded_hidden']['assets']]
    legs=run(lambda:SubmittedLegs(T([[True,False]],torch.bool),T([[False,False]],torch.bool),T([[0,-1]]),T([[0,1]]),
        rational(['1000.001','0']),T([[True,False]],torch.bool),T([[0,-1]]),T([[0,1]]),T([[True,False]],torch.bool),
        SubmittedBuyTerms(T([[2,0]]),T([[False,False]],torch.bool),rational(['0.5','0']),rational(['500.0005','0']),T([[0,-1]]))))
    legs=run(lambda:bind_submission_receipt(legs,zero,receipt))
    population=SubmittedActorRequests(intents,legs)
    # 此刻源账户尚无一次公开NOE，冷开户原记录明确没有取消历史。
    assert not clock.public.registry.events.received.any().cpu().item()
    history_targets=run(lambda:CancellationTargetHistory.open(integers([]),empty.bool()))
    clock.public=replace(clock.public,cancellation_target_history=history_targets)
    scenario=run(lambda:CancelFailureScenario(empty,integers([]),integers([]),empty.bool(),zero.to(torch.int32)))
    def paths(kind,world,selected):
        width=world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1)
        return T([[0]*width])
    backend=ContinuousExecution(population,rule,codec,sampler,asks,T(list(range(len(tokens)))),
        T([source['market_end']]*len(tokens)),taker,maker,zero.expand(len(tokens)),T(list(range(len(tokens)))),paths,scenario)
    clock.handlers=backend.handlers()
    mutation=clock.handlers[5]
    def diagnostic_mutation(world,projection,choice):
        candidate,projected,batches,effect=mutation(world,projection,choice)
        if effect is not None and effect.fault.any().cpu().item():
            stage('actual_mutation_fault',at=choice.at.cpu().tolist(),effect_type=type(effect).__name__,
                fault=effect.fault.cpu().tolist(),required_results=None if not hasattr(effect,'required_results') else effect.required_results.cpu().tolist(),
                result_capacity=None if world.cancel_results is None else world.cancel_results.capacity,
                pending=diagnostic_columns(world.requests.next_pending(),('request_key','item_ref','ordinal','effective_at','fault')))
        return candidate,projected,batches,effect
    clock.handlers[5]=diagnostic_mutation
    history=SourceHistoryInputs(pm,market,reference,clock.source.prepared.columns)
    encoder=MarketInputs100ms(pm,market,reference,binance,history,rule,clock.source.prepared.columns,pm.causal_visible_at)
    memory=history.empty(1,start);endpoints=MarketFreezeMemory(clock.source.prefix_at(pm,T([1200])),T([1200]),None,None)
    valuation=PortfolioValuation(pm,tokens,clock.public.ledger.success_pools.asset_roots)
    known={name:yes for name in ('executed_prefix_known','success_prefix_known','rebate_prefix_known','settlement_prefix_known',
        'ledger_prefix_known','economic_liability_prefix_known','economic_prefix_known')}
    request_ids=[source['request_id']]+['continuous-'+str(at) for at in range(1300,1900,100)]
    ranks={key:i for i,key in enumerate(sorted(request_ids))}
    def account_builder(public,frozen,bank):
        frame=market.frame(frozen.prefixes)
        markets=CancelObjectDirectory(frame.admitted,market.times[:,0][None,:],zero[:,None],yes[:,None],
            empty.bool(),empty.reshape(1,0,2),empty)
        leg_ranks=T([[ranks[request_ids[key]] if key>=0 else -1 for key in population.submitted_legs.request_slots.cpu().tolist()[0]]])
        account=population.freeze_account(public,frozen.endpoint_memory.previous_observation_ms,markets,leg_ranks)
        directory=account.intent_orders
        formation=public.registry.freeze_order_formation(directory,population.submitted_legs,account.cutoff,markets.market_present)
        location=formation.market;located=location>=0
        quoted=valuation.freeze(frozen.prefixes,account.cutoff)
        native=account_inputs_at_prefix(account,market,zero,frozen.prefixes,start,location,located,
            T([a['outcome']['index'] for a in source['bounded_hidden']['assets']]),row_role=frozen.role,valuation=quoted,**known)
        actor=publish_actor_context(account,native,encoder,frozen,zero,zero,identities,account.cancel_objects)
        if int(account.cutoff.cpu()[0]) in (1600,1700):
            from actor_support import DeviceNativeActor
            support=run(lambda:DeviceNativeActor(actor).support(DeviceNativeActor(actor).initial()))
            stage('actual_public_freeze_diagnostic',at=account.cutoff.cpu().tolist(),
                registry=diagnostic_columns(public.registry.events,('received','kinds','orders','private_sequences','delivery_sequences',
                    'record_ordinals','batch_sequences','record_refs','available_at','received_at','platform_at','processing_at',
                    'formation','condition_slots','request_slots','item_ordinals')),
                bindings=diagnostic_columns(public.submission_bindings,('received','accepted','request_slots','item_ordinals','orders','private_sequences','receive_rank')),
                intents=diagnostic_columns(directory,('present','identities','origin_known','receipt_known','term_leg_slots')),
                legs=diagnostic_columns(population.submitted_legs,('present','receipt_known','request_slots','item_ordinals','input_sequences','input_sequence_known')),
                formation=diagnostic_columns(formation,('evidence','market','processing_known','processing_at','input_sequences','item_ordinals','source_known','source_sequences')),
                pair=diagnostic_columns(account.cancel_directory,('paired_present','paired_members','paired_origin_order')),
                objects=diagnostic_columns(account.cancel_objects,('present','kinds','origin_pairs','members')),
                family_support=support.family.cpu().tolist())
        scopes=population.cancel_scopes(directory)
        objects=account.cancel_objects;object_scopes=CancellationScopes(objects.kinds==2,objects.markets,objects.members)
        target_history=public.cancellation_target_history
        # 相同原身份轴重定位既有目标；此冻结候选不修改持续历史。
        target_history=target_history.with_orders(directory.identities,directory.present)
        retained=directory.present
        snapshot=freeze_cancel_snapshot(public.cancellations,scopes,object_scopes,objects.present,location,located,
            retained,target_history.targets,account.cutoff)
        requests=freeze_scoped_cancel_requests(snapshot.state,scopes,object_scopes,location,snapshot.location_known,
            snapshot.targets,account.cutoff)
        leg_count=population.submitted_legs.present.size(1)
        sides=population.submitted_legs.outcome_indices.gather(1,directory.term_leg_slots.clamp(0,leg_count-1))
        sides=torch.where(directory.origin_known,sides,-1)
        funds=public.ledger.order_funds
        cancels=cancel_inputs(objects,account.cancel_directory,directory,account.orders,account.order_sources,
            funds,funds.read(public.ledger.lifecycle),bank,requests,account.cutoff,location,sides,zero[:,None],
            native.scopes.conditions,native.scopes.present)
        return account,native,actor,cancels
    if source.get('nonzero_history_samples'):
        # 原PublicFactWindow允许正初始容量；两条真实1413/1414事实形成已知窄窗需求。
        from fact_window import PublicFactWindow
        encoder.events.window=PublicFactWindow(clock.source.prepared.columns,initial_capacity=1)
    material=ContinuousActorInputs(encoder,history,memory,endpoints,zero,start,identities,account_builder,word_capacity=16)
    torch.manual_seed(181606)
    template=Policy100ms(ModelShape100ms(2,64)).cuda().eval();policies={}
    for at,family in [(1300,2),(1400,2 if second_buy else 0),(1500,0),(1600,0),(1700,4),(1800,0)]:
        policy=copy.deepcopy(template)
        with torch.no_grad():
            head=policy.action.actor
            head.family[-1].weight.zero_();head.family[-1].bias.zero_();head.family[-1].bias[family]=40.
            # 复用已验工程数量MLP：原lo/upper、hi/upper编码，偏好约10份。
            # 首个C1300原cash=2000、两tick=0.01给Pair upper=10000000百份单位。
            desired=1000/10000000
            prefix=policy.action.prefix
            prefix[1].weight.zero_();prefix[1].bias.zero_()
            prefix[1].weight[0,16]=1.;prefix[1].weight[0,20]=-desired;prefix[1].weight[0,0]=desired-1.
            prefix[1].weight[1].copy_(-prefix[1].weight[0])
            prefix[1].weight[2,17]=1.;prefix[1].weight[2,16]=-1.
            prefix[1].weight[3].copy_(-prefix[1].weight[2])
            prefix[3].weight.zero_();prefix[3].bias.zero_()
            prefix[3].weight[0,0]=1.;prefix[3].weight[0,1]=-1.
            prefix[3].weight[1,2]=1.;prefix[3].weight[1,3]=-1.
            decoder=policy.action.actor.decoder[1];decoder.weight.zero_();decoder.bias.zero_()
            column=policy.shape.hidden+policy.shape.hidden//2
            for row,offset in ((0,0),(2,1)):
                decoder.weight[row,column+offset]=1.;decoder.weight[row,column+2]=-1.
                decoder.weight[row+1].copy_(-decoder.weight[row])
            head.quantity.weight.zero_();head.quantity.bias.zero_()
            for j in range(16):
                head.quantity.weight[j,0]=-2*j*100000.;head.quantity.weight[j,1]=2*j*100000.
                head.quantity.weight[j,2]=-j*j/16*100000.;head.quantity.weight[j,3]=j*j/16*100000.
            head.variant.weight.zero_();head.variant.bias.copy_(T([40,0]).float())
            head.price.weight.zero_();head.price.bias.zero_();head.price.bias[38]=40.
        policies[at]=policy
    random=ActionRandom(zero+181606,T([[0,0,0,0]]));observations=[];senders=[];bound=[]
    history_checks=[];history_retry=False
    def freeze(world,at,enabled):
        nonlocal history_retry
        now=int(at.cpu()[0]);previous=int(material.endpoints.previous_observation_ms.cpu()[0])
        if source.get('nonzero_history_samples') and now==1500 and not history_retry:
            before=stored_actual(material.memory);endpoint=stored_actual(material.endpoints)
            small,small_fit=material.freeze(world,at,enabled)
            required=small.market.required.max(0).values.cpu().tolist()
            assert required[2]>encoder.events.window.fact_capacity
            assert not small.market.resources_fit.any().cpu().item() and not small_fit.any().cpu().item()
            assert stored_actual(material.memory)==before
            assert stored_actual(material.endpoints)==endpoint
            stage('history_resource_retry',at=now,capacity=encoder.events.window.fact_capacity,required=required,
                committed_history=material.memory.counts.cpu().tolist(),candidate_fit=small_fit.cpu().tolist(),
                memory_before=before,memory_after=stored_actual(material.memory),
                endpoint_before=endpoint,endpoint_after=stored_actual(material.endpoints))
            encoder.grow_resources(required);history_retry=True
        observed,accepted=material.freeze(world,at,enabled)
        if source.get('nonzero_history_samples') and accepted.any().cpu().item():
            import math
            samples=[(Fraction(value),time) for value,time in source['nonzero_history_samples'] if time<now]
            window=[v for v,time in samples if previous<=time<now]
            signed=lambda v:math.copysign(math.log1p(abs(float(v))),float(v)) if v else 0.
            expected_history=[len(samples)/16,0.,0.,0.]
            if len(samples)>1:
                square=sum((b[0]-a[0])**2 for a,b in zip(samples,samples[1:]))/(len(samples)-1)
                expected_history[1:]=[signed(samples[-1][0]-samples[-2][0]),math.log1p(math.sqrt(float(square))),signed(Fraction(samples[-1][1]-samples[0][1],1000))]
            expected_path=[signed(len(window)),0.,0.,0.,0.,0.]
            if window:
                expected_path[1:5]=[float(2*v-1) for v in (window[0],window[-1],min(window),max(window))]
                if len(window)>1:
                    square=sum((b-a)**2 for a,b in zip(window,window[1:]))/(len(window)-1)
                    expected_path[5]=math.log1p(math.sqrt(float(square)))
            assert torch.equal(observed.market.fast[0,542:566],torch.tensor(expected_history+[0.]*20,device='cuda',dtype=torch.float32))
            assert torch.equal(observed.market.fast[0,596:626],torch.tensor(expected_path+[0.]*24,device='cuda',dtype=torch.float32))
            assert material.memory.counts.cpu().tolist()==[[len(samples),0,0,0,0,0]]
            assert not material.memory.path_count.any().cpu().item()
            history_checks.append(dict(at=now,retained=len(samples),window_samples=len(window),history=expected_history,path=expected_path))
        stage('continuous_freeze',at=at.cpu().tolist(),accepted=accepted.cpu().tolist(),fault=observed.frozen.fault.cpu().tolist(),
            required=observed.market.required.cpu().tolist(),provided=observed.frozen.provided.all(1).cpu().tolist(),
            order_count=int(observed.account.orders.present.sum().cpu()),cancel_count=int(observed.actor.cancels.present.sum().cpu()))
        if accepted.any().cpu().item():observations.append(observed)
        return observed,accepted
    def policy_start(observation,enabled):
        at=int(observation.frozen.cutoff.cpu()[0]);bound.append(at)
        return policies[at],zero+at
    def sender_factory(task):
        observation=task.observation;at=int(observation.frozen.cutoff.cpu()[0]);key=request_ids.index('continuous-'+str(at))
        inputs=ActorPolicyInputs.publish(observation.actor,observation.market,observation.frozen,task.evaluation,observation.cancels)
        control=ActionControl(zero+key,zero+key,T([[2*key-1,2*key]]),task.send_at)
        driver=CapturedDeviceActor(task.policy,inputs,random,word_capacity=16,path_capacity=32)
        publication=ActorSelection(random)
        order_values=values(observation.account.intent_orders.identities)
        ids=codec.sim_order_ids_v1(order_values);orders=dict(zip(order_values,ids,strict=True))
        identity_bytes,offsets=byte_column([key.encode() for key in request_ids])
        slots={}
        submit=ActorRequestSubmission(control,control.leg_requests,sampler,rule,identity_bytes,offsets,
            ((condition,),),(tuple(tokens),),(slots,),((source['market_end'],),),codec,population=population,
            order_sequences=({value:key for key,value in orders.items()},))
        ids=IntentRequestIdentities(({'global_request_id':request_ids[key],'caller_correlation_id':None},),
            ((condition,),),(tuple(tokens),),(orders,))
        def receive(public,delivery,enabled):return receive_current(public,delivery,stage,enabled)
        sender=FrozenActorSend(publication,driver,control,ids,codec,submit,receive,observation.source_snapshot,
            decision_cutoff=observation.cutoff)
        senders.append(sender);return sender
    models=ActorModelQueue(torch.zeros((2,1,64),device='cuda'),identities,T([1300]),policy_start,
        lambda observation:(zero+114,zero+120 if int(observation.frozen.cutoff.cpu()[0])==1300 else zero),sender_factory)
    def receive_observation(public,delivery,enabled):
        at=clock.gates.public_visible_at.cpu().tolist()
        if at[0] in (1600,1700):
            stage('actual_closed_diagnostic',at=at,
                closed=diagnostic_columns(delivery.closed,('emitted','record_refs','delivered_sequences','available_at','fault')),
                projection_batches=[diagnostic_columns(batch,('rows','emitted','received_at','private_sequences',
                    'batch_sequences','record_ordinals','platform_at','request_slots','item_ordinals'))
                    for batch in public.ledger.columns.projection_batches])
        return receive_current(public,delivery,stage,enabled)
    shared=ContinuousActorRun(clock,T([1300]),models,freeze,receive_observation)
    continuations=0;page_changes=0
    while True:
        result=shared.advance_before(T([1901]),yes,progress=stage)
        if result.fault.any().cpu().item():
            advance=result.stage_result;effect=None if advance is None else advance.stage_result
            stage('actual_continuous_fault',fault=result.fault.cpu().tolist(),
                effect_type=type(effect).__name__,required_results=None if effect is None or not hasattr(effect,'required_results') else effect.required_results.cpu().tolist(),
                result_capacity=None if clock.runtime.cancel_results is None else clock.runtime.cancel_results.capacity,
                pending=diagnostic_columns(clock.runtime.requests.next_pending(),('request_slot','request_key','item_ref','ordinal','effective_at','exists','fault')),
                done=clock.runtime.requests.done.cpu().tolist(),input_sequence=values(clock.runtime.input_frontier.next_sequence),
                ledger_prefix=values(clock.public.ledger.current_local_prefix))
        advance=result.stage_result;effect=None if advance is None else advance.stage_result
        if effect is not None and hasattr(effect,'required_results'):
            capacity=1 if clock.runtime.cancel_results is None else clock.runtime.cancel_results.capacity
            required=int(effect.required_results.max().cpu())
            if required>capacity:
                assert result.fault.cpu().tolist()==[2] and effect.fault.cpu().tolist()==[2]
                from cancel_results import CancelResults
                old=clock.runtime.cancel_results or CancelResults(1)
                first_groups=old.groups;old_done=clock.runtime.requests.done.clone();old_random=random.counter.clone()
                old_input=values(clock.runtime.input_frontier.next_sequence);old_private=values(clock.projection.emitter.next_private_sequence)
                clock.runtime=replace(clock.runtime,cancel_results=old.resized(required))
                assert clock.runtime.cancel_results.groups is first_groups
                assert torch.equal(clock.runtime.requests.done,old_done) and torch.equal(random.counter,old_random)
                assert values(clock.runtime.input_frontier.next_sequence)==old_input and values(clock.projection.emitter.next_private_sequence)==old_private
                stage('cancel_result_capacity_continuation',capacity=capacity,required=required,done=old_done.cpu().tolist(),
                    input_sequence=old_input,random=old_random.cpu().tolist(),private_sequence=old_private)
                continuations+=1;continue
        assert result.fault.cpu().tolist()==[0],result.fault.cpu().tolist()
        if result.complete.cpu().item():break
        next_group=clock.source.next()
        if not next_group.present.cpu().item() and not next_group.eof.cpu().item():
            clock.source.load_next(1);page_changes+=1;stage('continuous_next_page',page_changes=page_changes,position=clock.source.position.cpu().tolist());continue
        required_requests=max([clock.runtime.requests.capacity]+[int(s.receipt.required_requests.max().cpu()) for s in senders if s.receipt is not None])
        required_legs=max([clock.public.ledger.reservations.capacity]+[int(s.publication.reservation.decision.required_capacity.max().cpu())
            for s in senders if s.publication.reservation is not None])
        old=clock.public.ledger
        if required_legs>old.reservations.capacity or required_requests>clock.runtime.requests.capacity:
            stage('continuous_capacity_requirement',requests=clock.runtime.requests.capacity,required_requests=required_requests,
                legs=old.reservations.capacity,required_legs=required_legs)
            if required_legs>old.reservations.capacity:
                pad=required_legs-old.reservations.capacity
                clock.public=replace(clock.public,ledger=replace(old,reservations=old.reservations.resized(required_legs),
                    global_requests=torch.nn.functional.pad(old.global_requests,(0,pad),value=-1),
                    item_ordinals=torch.nn.functional.pad(old.item_ordinals,(0,pad),value=-1)))
            if required_requests>clock.runtime.requests.capacity:
                clock.runtime=replace(clock.runtime,requests=clock.runtime.requests.resized(required_requests,clock.runtime.requests.action_capacity))
            continuations+=1
            continue
        frozen=observations[-1] if observations else None
        if frozen is not None:stage('blocked_unknown',fault=frozen.frozen.fault.cpu().tolist(),required=frozen.market.required.cpu().tolist())
        raise AssertionError('真实连续路径未完成且尚无已报告物理需求')
    assert [int(o.frozen.cutoff.cpu()[0]) for o in observations]==[1300,1400,1500,1600,1700,1800,1900]
    assert page_changes>=1 and continuations>=1
    assert bound==[1300,1400,1500,1600,1700,1800]
    stage('actual_continuous_decisions',families=[s.publication.action.family.cpu().tolist() for s in senders],
        choices=[s.publication.candidate.choices.cpu().tolist() for s in senders],
        requests=[s.requests for s in senders],post_outcomes=clock.runtime.post_results.values.outcomes.cpu().tolist(),
        post_reasons=clock.runtime.post_results.values.reasons.cpu().tolist())
    assert [s.publication.action.family.cpu().tolist()[0] for s in senders]==[2,2 if second_buy else 0,0,0,4]
    assert senders[0].receipt is not None and senders[4].receipt is not None
    assert len(population.post_intents.items[0])==(5 if second_buy else 3) and population.submitted_legs.present.sum().cpu().item()==(5 if second_buy else 3)
    assert any(Fraction(text)==Fraction('0.39') for text in decimal_values(clock.runtime.hidden.queues.price))
    assert values(observations[0].actor.cash_micro)!=values(observations[4].actor.cash_micro)
    pair_body=senders[0].requests[1][0];cancel_body=senders[4].requests[1][0]
    assert pair_body['method']=='POST_ORDERS' and cancel_body['method']=='CANCEL_ORDERS'
    assert clock.source.next().eof.cpu().item()
    assert not (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item()
    assert clock.runtime.post_results is not None and clock.runtime.cancel_results is not None
    # C1400在C1300发送前冻结；模型全部层沿同根顺序承接，没有按发送重置。
    actual=[task for task in models.tasks if task.evaluation is not None]
    assert [int(task.started_at.cpu()[0]) for task in actual]==[1300,1414,1528,1642,1756,1870]
    previous=torch.zeros((2,1,64),device='cuda')
    for task in actual:
        with torch.no_grad():expected=task.policy.frozen_state_and_value(task.observation.frozen,previous,identities,task.evaluation.policy_version)
        torch.testing.assert_close(task.evaluation.hidden,expected.hidden)
        previous=expected.hidden
    if not second_buy:
        assert not observations[3].account.cancel_directory.paired_present.any().cpu().item()
    else:
        old=observations[3].account.intent_orders
        assert old.origin_known.cpu().tolist()==[[True,True,True,False,False]]
        assert observations[3].account.cancel_directory.paired_present.any().cpu().item()
    assert observations[4].account.cancel_directory.paired_present.any().cpu().item()
    assert observations[-1].actor.cancels.present.sum().cpu().item() < observations[4].actor.cancels.present.sum().cpu().item()
    # 相同原完整组一次批量消费是独立调度对照，不由增量合并结果生成期望。
    groups=torch.arange(int(material.memory.last_prefix.cpu()[0])+1,device='cuda')
    once,history_fault=run(lambda:history.consume(groups,encoder.delivered_cutoffs[groups],zero,start,history.empty(1,start)))
    assert history_fault.cpu().tolist()==[0]
    once=run(lambda:history.freeze(once)).memory
    for field in fields(once):
        left,right=getattr(material.memory,field.name),getattr(once,field.name)
        if isinstance(left,torch.Tensor):assert torch.equal(left,right),field.name
        else:assert values(left)==values(right),field.name
    first_finish=next(row for row in evidence if row['kind']==2 and row['at']==[1414])
    assert first_finish['published'][0]==[True] and not first_finish['receipts'][0]
    assert first_finish['business_random'][0][0]>0
    for task in actual:
        at=int(task.observation.frozen.cutoff.cpu()[0])
        assert task.policy is policies[at] and task.evaluation.policy_version.cpu().tolist()==[at]
    pending_models=[task for task in models.tasks if task.active.any().cpu().item() and (task.stage<3).any().cpu().item()]
    assert any(task.completed_at is not None and task.completed_at.cpu().tolist()==[1984] for task in pending_models)
    if second_buy:
        assert senders[1].control.execute_at.cpu().tolist()==[1528] and senders[0].control.execute_at.cpu().tolist()==[1534]
        assert senders[1].receipt is not None and senders[0].receipt is not None
        early=senders[1].dispatch.prepared[1].cpu().tolist()[0];late=senders[0].dispatch.prepared[1].cpu().tolist()[0]
        assert early==[1,2] and late==[3,4]
        assert [json.loads(population.post_intents.items[0][ref])['item_id'] for ref in early]==['continuous-1400:0','continuous-1400:1']
        assert [json.loads(population.post_intents.items[0][ref])['item_id'] for ref in late]==['continuous-1300:0','continuous-1300:1']
    before_prefix=values(clock.public.ledger.current_local_prefix);before_random=random.counter.clone();before_input=values(clock.runtime.input_frontier.next_sequence)
    for sender in senders:
        repeat=sender.execute(clock,yes,progress=stage);assert repeat.receipt is sender.receipt
    assert values(clock.public.ledger.current_local_prefix)==before_prefix and torch.equal(random.counter,before_random)
    assert values(clock.runtime.input_frontier.next_sequence)==before_input
    result=dict(passed=True,scope='same runtime/account C1300→C1400 queued→c1414 before s1534→new Pair processed→public C1700→Pair cancel→C1900 receive→current Ledger',
        requests=[{'sent_at':int(s.control.execute_at.cpu()[0]),'input_sequence':values(s.receipt.input_sequence)[0],'body':s.requests[1][0]} for s in senders if s.requests is not None],
        observations=[int(o.frozen.cutoff.cpu()[0]) for o in observations],model_starts=[int(t.started_at.cpu()[0]) for t in actual],
        page_changes=page_changes,capacity_continuations=continuations,input_sequence=values(clock.runtime.input_frontier.next_sequence),
        business_random=random.counter.cpu().tolist(),ledger_prefix=before_prefix,private_received=clock.public.registry.events.received.size(1),
        cash=[str(x) for x in amounts(run(clock.public.ledger.funds).actual_available)],full_unbounded_host_delivered=False,elapsed_seconds=time.perf_counter()-started)
    if source.get('nonzero_history_samples'):
        assert history_retry and any(row['window_samples']>0 for row in history_checks) and history_checks[-1]['window_samples']==0
        result['nonzero_history_checks']=history_checks;result['history_capacity_retry']=history_retry
        assert torch.equal(observations[0].market.fast[0,542:546],torch.tensor(history_checks[0]['history'],device='cuda',dtype=torch.float32))
        assert torch.equal(observations[0].market.fast[0,596:602],torch.tensor(history_checks[0]['path'],device='cuda',dtype=torch.float32))
    result['requests'].sort(key=lambda row:row['sent_at'])
    rows=[]
    for batch in clock.public.ledger.columns.projection_batches:
        for column,on in enumerate(batch.emitted.cpu().tolist()[0]):
            if on:
                row=int(batch.rows[0,column].cpu())
                rows.append(dict(batch=values(batch.batch_sequences)[column],ordinal=values(batch.record_ordinals)[column],
                    private=values(batch.private_sequences)[column],received=int(batch.received_at[0,column].cpu()),
                    semantic=clock.projection.normalizer.bank.semantic[row:row+1].cpu().tolist()))
    actual_funds=run(clock.public.ledger.funds)
    (artifact/'actual-continuous-values.json').write_text(json.dumps(dict(noe=rows,
        post_results=stored_actual(clock.runtime.post_results.values),cancel_results=stored_actual(clock.runtime.cancel_results.groups),
        ledger=dict(entry=values(clock.public.ledger.current_entry_sequence),local=values(clock.public.ledger.current_local_prefix),
            platform_prefix=values(clock.public.ledger.platform.platform_source_prefix),balances=[str(x) for x in amounts(clock.public.ledger.platform.balances)],
            funds={name:[str(x) for x in amounts(getattr(actual_funds,name))] for name in ('actual_available','maker_available','actual_reserved','maker_reserved')}),
        evidence=evidence,scope='raw production outputs for independent CPU posterior'),indent=2)+'\n')
    (artifact/'actual-continuous-requests.json').write_text(json.dumps(result['requests'],indent=2)+'\n')
    return result


def main():
    source=json.load(sys.stdin);artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    path=Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so'
    spec=importlib.util.spec_from_file_location('model_native_inputs',path);codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    def progress(row):
        with (artifact/'continuous-progress.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
        print(json.dumps(row),file=sys.stderr,flush=True)
    report=check(source,codec.post_terms_batch_v2,progress,before_drive=lambda *args,**kwargs:bounded(*args,codec,artifact,continuous=lambda *a:continuous(*a,second_buy='--out-of-order-buy' in sys.argv),**kwargs),defer_initial_send=True)
    print(json.dumps(report),flush=True)

if __name__=='__main__':main()
