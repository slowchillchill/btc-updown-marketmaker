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
    if hasattr(value,'words') and hasattr(value,'sign'):return {'integer':values(value),'sign':value.sign.cpu().tolist(),'fault':value.fault.cpu().tolist()}
    if hasattr(value,'numerator') and hasattr(value,'denominator'):return {'rational':[str(x) for x in amounts(value)]}
    if is_dataclass(value):return {f.name:stored_actual(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,(tuple,list)):return [stored_actual(x) for x in value]
    return value


def assert_endpoints_uncommitted(before,after):
    assert before['previous_prefix']==after['previous_prefix']
    assert before['previous_observation_ms']==after['previous_observation_ms']
    for name in ('pm_sides','binance_sides'):
        previous,current=before[name],after[name]
        if previous is not None:
            assert previous==current
        elif current is not None:
            assert not any(value for row in current['known'] for value in row)
            assert not any(value for row in current['best_present'] for value in row)
            for field in ('total','best'):
                assert not any(current[field]['integer'])
                assert not any(current[field]['sign'])
                assert not any(current[field]['fault'])


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
    two=source.get('continuous_two_markets',False);offset=source.get('continuous_observation_offset',0)
    started=time.perf_counter();zero=T([0]);yes=T([True],torch.bool);empty=T([[]]);start=zero-29000
    market_indices=T([0,1]) if two else zero;row_roots=T([0,0]) if two else zero
    identities=T([[0,0,0],[0,0,1]]) if two else T([[0,0,0]]);condition_slots=T(list(range(2 if two else 1)))
    condition=source['initial'][0]['market']['condition_id'];tokens=[a['token_id'] for a in source['bounded_hidden']['assets']]
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
        T([event['market']['window']['end_at_utc_ms'] for event in source['initial']]),taker,maker,
        block.asset_markets,T(list(range(len(tokens)))),paths,scenario)
    if two:
        from maker_days import MakerDays
        from rebate_coordinator import RebateCoordinator
        clock.runtime=replace(clock.runtime,calendars=run(lambda:RebateCoordinator(
            MakerDays.empty(condition_slots[None,:],T([[301000,601000]]),yes[:,None].expand(1,2)),clock.runtime.calendars.taker)))
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
    memory=history.empty(len(market_indices),start[row_roots]);endpoints=MarketFreezeMemory(clock.source.prefix_at(pm,T([1200+offset]))[row_roots],T([1200+offset])[row_roots],None,None)
    valuation=PortfolioValuation(pm,tokens,clock.public.ledger.success_pools.asset_roots)
    known={name:yes for name in ('executed_prefix_known','success_prefix_known','rebate_prefix_known','settlement_prefix_known',
        'ledger_prefix_known','economic_liability_prefix_known','economic_prefix_known')}
    request_ids=[source['request_id']]+['continuous-'+str(at+offset) for at in range(1300,2800 if two else 1900,100)]
    ranks={key:i for i,key in enumerate(sorted(request_ids))}
    def account_builder(public,frozen,bank,market_indices,row_identities):
        frame=market.frame(frozen.prefixes)
        markets=CancelObjectDirectory(frame.admitted,market.times[:,0][None,:],condition_slots[None,:],yes[:,None].expand(1,len(condition_slots)),
            empty.bool(),empty.reshape(1,0,2),empty)
        leg_ranks=T([[ranks[request_ids[key]] if key>=0 else -1 for key in population.submitted_legs.request_slots.cpu().tolist()[0]]])
        account=population.freeze_account(public,frozen.endpoint_memory.previous_observation_ms,markets,leg_ranks)
        directory=account.intent_orders
        formation=public.registry.freeze_order_formation(directory,population.submitted_legs,account.cutoff,markets.market_present)
        location=formation.market;located=location>=0
        quoted=valuation.freeze(frozen.prefixes,account.cutoff)
        native=account_inputs_at_prefix(account,market,condition_slots,frozen.prefixes,start,location,located,
            T([a['outcome']['index'] for a in source['bounded_hidden']['assets']]),row_role=frozen.role,valuation=quoted,**known)
        actor=publish_actor_context(account,native,encoder,frozen,market_indices,condition_slots,row_identities,account.cancel_objects)
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
    material=ContinuousActorInputs(encoder,history,memory,endpoints,market_indices,start,identities,account_builder,word_capacity=16,row_roots=row_roots)
    torch.manual_seed(181606)
    template=Policy100ms(ModelShape100ms(2,64)).cuda().eval();policies={}
    for at,family in [(1300,2),(1400,2 if second_buy else 0),(1500,0),(1600,0),(1700,4),(1800,0)]:
        at+=offset
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
            for row,decoder_offset in ((0,0),(2,1)):
                decoder.weight[row,column+decoder_offset]=1.;decoder.weight[row,column+2]=-1.
                decoder.weight[row+1].copy_(-decoder.weight[row])
            head.quantity.weight.zero_();head.quantity.bias.zero_()
            for j in range(16):
                head.quantity.weight[j,0]=-2*j*100000.;head.quantity.weight[j,1]=2*j*100000.
                head.quantity.weight[j,2]=-j*j/16*100000.;head.quantity.weight[j,3]=j*j/16*100000.
            head.variant.weight.zero_();head.variant.bias.copy_(T([40,0]).float())
            head.price.weight.zero_();head.price.bias.zero_();head.price.bias[38]=40.
        policies[at]=policy
    if two:
        for at in range(1900+offset,2800+offset,100):
            policy=copy.deepcopy(policies[1800+offset])
            if at==2000+offset:
                with torch.no_grad():policy.action.actor.family[-1].bias[2]=40.;policy.action.actor.family[-1].bias[0]=0.
            policies[at]=policy
    random=ActionRandom(zero+181606,T([[0,0,0,0]]));observations=[];senders=[];bound=[]
    history_checks=[];history_retry=False;resource_candidate=None;end_retry=False;release_calls=[]
    def freeze(world,at,enabled):
        nonlocal history_retry,resource_candidate
        now=int(at.cpu()[0]);previous=int(material.endpoints.previous_observation_ms.cpu()[0])
        before=stored_actual(material.memory);endpoint=stored_actual(material.endpoints)
        observed,accepted=material.freeze(world,at,enabled)
        required=observed.market.required.max(0).values.cpu().tolist()
        shortfall=required[2]>encoder.events.window.fact_capacity
        if source.get('nonzero_history_samples') and not two and now==1500 and not history_retry:
            assert shortfall
        if shortfall:
            assert not observed.market.resources_fit.any().cpu().item() and not accepted.any().cpu().item()
            assert stored_actual(material.memory)==before
            assert_endpoints_uncommitted(endpoint,stored_actual(material.endpoints))
            stage('history_resource_retry',at=now,capacity=encoder.events.window.fact_capacity,required=required,
                committed_history=material.memory.counts.cpu().tolist(),candidate_fit=accepted.cpu().tolist(),
                memory_before=before,memory_after=stored_actual(material.memory),
                endpoint_before=endpoint,endpoint_after=stored_actual(material.endpoints))
            resource_candidate=(observed,required,models.hidden.clone(),models.identities.clone(),random.counter.clone())
        if source.get('nonzero_history_samples') and not two and accepted.any().cpu().item():
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
        if two and accepted.any().cpu().item():
            shared_slow=observed.frozen.slow[:,[i for i in range(433) if i not in (93,94)]]
            assert torch.equal(shared_slow[0],shared_slow[1])
            assert observed.account.cutoff.numel()==1 and world.public.ledger.fault.numel()==1
            expected_roles=[1,0] if now<2000+offset else [-1,1]
            assert observed.frozen.role.cpu().tolist()==expected_roles
            assert observed.native.scopes.current_market.cpu().tolist()==[0 if now<2000+offset else 1]
            if now<2000+offset:
                assert observed.frozen.slow[:,93:95].cpu().tolist()==[[1.,0.],[0.,1.]]
            else:
                assert observed.native.scopes.present[0,1,0].cpu().item()
            import math
            for row,samples in enumerate(source['two_market_history_samples']):
                if observed.frozen.role[row].cpu().item()<0:continue
                samples=[(Fraction(value),time) for value,time in samples if time<now]
                window=[value for value,time in samples if previous<=time<now]
                signed=lambda value:math.copysign(math.log1p(abs(float(value))),float(value)) if value else 0.
                expected_history=[len(samples)/16,0.,0.,0.]
                if len(samples)>1:
                    square=sum((b[0]-a[0])**2 for a,b in zip(samples,samples[1:]))/(len(samples)-1)
                    expected_history[1:]=[signed(samples[-1][0]-samples[-2][0]),math.log1p(math.sqrt(float(square))),signed(Fraction(samples[-1][1]-samples[0][1],1000))]
                expected_path=[signed(len(window)),0.,0.,0.,0.,0.]
                if window:
                    expected_path[1:5]=[float(2*value-1) for value in (window[0],window[-1],min(window),max(window))]
                    if len(window)>1:
                        square=sum((b-a)**2 for a,b in zip(window,window[1:]))/(len(window)-1)
                        expected_path[5]=math.log1p(math.sqrt(float(square)))
                torch.testing.assert_close(observed.frozen.fast[row,542:566],T(expected_history+[0.]*20,torch.float32),rtol=0,atol=0)
                torch.testing.assert_close(observed.frozen.fast[row,596:626],T(expected_path+[0.]*24,torch.float32),rtol=0,atol=0)
                assert observed.frozen.fast[row,440].cpu().item()==torch.tensor(math.log1p((market.times[row,1].cpu().item()-now)/1000),dtype=torch.float32).item()
            assert torch.equal(observed.frozen.fast[0,335:439],observed.frozen.fast[1,335:439]) if now<2000+offset else True
            assert material.endpoints.pm_sides.best_present.size(0)==2
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
            (tuple(event['market']['condition_id'] for event in source['initial'][::2]),),(tuple(tokens),),(slots,),
            (tuple(event['market']['window']['end_at_utc_ms'] for event in source['initial'][::2]),),codec,population=population,
            order_sequences=({value:key for key,value in orders.items()},))
        ids=IntentRequestIdentities(({'global_request_id':request_ids[key],'caller_correlation_id':None},),
            (tuple(event['market']['condition_id'] for event in source['initial'][::2]),),(tuple(tokens),),(orders,))
        def receive(public,delivery,enabled):return receive_current(public,delivery,stage,enabled)
        sender=FrozenActorSend(publication,driver,control,ids,codec,submit,receive,observation.source_snapshot,
            decision_cutoff=observation.cutoff)
        senders.append(sender);return sender
    def release_markets(ended):
        release_calls.append(ended.cpu().tolist());material.release_markets(ended)
    models=ActorModelQueue(torch.zeros((2,len(market_indices),64),device='cuda'),identities,T([1300+offset]),policy_start,
        lambda observation:(zero+(150 if two else 114),zero+120 if int(observation.frozen.cutoff.cpu()[0])==1300+offset else zero),sender_factory,
        row_roots=row_roots,market_ends=market.times[market_indices,1] if two else None,
        release_markets=release_markets if two else None)
    def receive_observation(public,delivery,enabled):
        at=clock.gates.public_visible_at.cpu().tolist()
        if at[0] in (1600,1700):
            stage('actual_closed_diagnostic',at=at,
                closed=diagnostic_columns(delivery.closed,('emitted','record_refs','delivered_sequences','available_at','fault')),
                projection_batches=[diagnostic_columns(batch,('rows','emitted','received_at','private_sequences',
                    'batch_sequences','record_ordinals','platform_at','request_slots','item_ordinals'))
                    for batch in public.ledger.columns.projection_batches])
        return receive_current(public,delivery,stage,enabled)
    shared=ContinuousActorRun(clock,T([1300+offset]),models,freeze,receive_observation)
    continuations=0;page_changes=0
    while True:
        result=shared.advance_before(T([2801+offset if two else 1901]),yes,progress=stage)
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
        if resource_candidate is not None:
            failed,required,hidden_before,identity_before,random_before=resource_candidate
            assert torch.equal(shared.next_observation,failed.account.cutoff)
            assert torch.equal(models.hidden,hidden_before) and torch.equal(models.identities,identity_before)
            assert torch.equal(random.counter,random_before)
            assert not models.tasks[-1].active.any().cpu().item()
            if two and int(failed.account.cutoff.cpu()[0])==2000+offset:
                # 无积压的原队列代表：失败的新截止不能替代已接纳观察前沿。
                released=[]
                probe=ActorModelQueue(hidden_before,identity_before,models.last_completed_at,policy_start,
                    models.timing,sender_factory,row_roots=row_roots,market_ends=models.market_ends,
                    release_markets=lambda mask:released.append(mask.cpu().tolist()))
                probe.enqueue(failed,torch.zeros_like(yes))
                probe.retire_consumed_markets(shared.next_observation-100)
                assert torch.equal(probe.hidden,hidden_before) and not released
                assert torch.equal(probe.identities,identity_before)
                probe.retire_consumed_markets(failed.account.cutoff)
                probe.retire_consumed_markets(failed.account.cutoff+100)
                assert released==[[True,False]] and probe.identities.cpu().tolist()==[[0,0,-1],[0,0,1]]
                assert probe.hidden[:,0].abs().sum().cpu().item()==0
                assert torch.equal(probe.hidden[:,1],hidden_before[:,1])
                end_retry=True
            encoder.grow_resources(required);history_retry=True;resource_candidate=None
            stage('whole_root_input_growth',at=shared.next_observation.cpu().tolist(),required=required,end_retry=end_retry)
            continue
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
    if two:
        return verify_two_market_continuous(source,clock,material,models,observations,senders,history_retry,
            history,encoder,random,evidence,started,artifact,stage,end_retry,release_calls)
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


def verify_two_market_continuous(source,clock,material,models,observations,senders,history_retry,
        history,encoder,random,evidence,started,artifact,stage,end_retry,release_calls):
    offset=source['continuous_observation_offset']
    assert history_retry and end_retry and release_calls==[[True,False]]
    assert [int(o.account.cutoff.cpu()[0]) for o in observations]==list(range(1300+offset,2900+offset,100))
    assert clock.public.ledger.fault.numel()==clock.runtime.account.available.sign.numel()==1
    actual=[task for task in models.tasks if task.evaluation is not None]
    assert [int(task.started_at.cpu()[0]) for task in actual]==list(range(1300+offset,2801+offset,150))
    # 独立逐市场模型前向：不调用队列/身份映射或其评估方法生成期望。
    previous=torch.zeros((2,2,64),device='cuda');warm_steps=0;opened=False
    for task in actual:
        observation=task.observation
        for role in (0,1):
            for row in range(2):
                if observation.frozen.role[row].cpu().item()!=role:continue
                selected=observation.frozen.rows(T([row]))
                with torch.no_grad():state,hidden,value=task.policy.state_and_value(*selected.model_inputs(),previous[:,row:row+1])
                torch.testing.assert_close(task.evaluation.hidden[:,row:row+1],hidden)
                torch.testing.assert_close(task.evaluation.state[row:row+1],state)
                if row==1 and role==0:warm_steps+=1
                if row==1 and role==1 and not opened:
                    assert warm_steps==7 and previous[:,row:row+1].abs().sum().cpu().item()>0
                    opened=True
                previous[:,row:row+1]=hidden
        assert task.active.numel()==task.stage.numel()==1
        assert task.evaluation.policy_version.cpu().tolist()==[int(observation.account.cutoff.cpu()[0])]*2
    assert opened
    actor_cutoffs=[int(sender.driver.inputs.actor.cutoff.cpu()[0]) for sender in senders]
    assert actor_cutoffs==[int(task.observation.account.cutoff.cpu()[0]) for task in actual if task.stage.cpu().item()>=2]
    assert all(sender.driver.inputs.actor.cutoff.numel()==1 for sender in senders)
    assert any(int(task.observation.account.cutoff.cpu()[0])==1900+offset and task.completed_at.cpu().tolist()==[2350+offset] for task in actual)
    assert models.hidden[:,0].abs().sum().cpu().item()==0 and models.hidden[:,1].abs().sum().cpu().item()>0
    assert material.memory.counts.cpu().tolist()==[[0]*6,[7,0,0,0,0,0]]
    assert not material.endpoints.pm_sides.known[0].any().cpu().item()
    assert material.endpoints.pm_sides.known[1].any().cpu().item()
    assert observations[0].frozen.fast[0,542].cpu().item()==observations[0].frozen.fast[1,542].cpu().item()==1/16
    assert observations[0].frozen.fast[0,597].cpu().item()!=observations[0].frozen.fast[1,597].cpu().item()
    assert observations[0].market.endpoint_memory.pm_sides.asset_indices.cpu().tolist()==[[0,1],[2,3]]
    # 同一真实完整组一次消费的独立调度对照；只比较未退役下一市场。
    groups=torch.arange(int(material.memory.last_prefix.max().cpu())+1,device='cuda')
    once,error=run(lambda:history.consume(groups,encoder.delivered_cutoffs[groups],T([0,1]),
        T([-29000,-29000]),history.empty(2,T([-29000,-29000]))))
    assert error.cpu().tolist()==[0,0]
    once=run(lambda:history.freeze(once)).memory
    for field in fields(once):
        left,right=getattr(material.memory,field.name),getattr(once,field.name)
        if isinstance(left,torch.Tensor):assert torch.equal(left[1],right[1]),field.name
        else:
            width=left.sign.numel()//2;indices=torch.arange(width,2*width,device='cuda')
            assert stored_actual(_take(left,indices))==stored_actual(_take(right,indices)),field.name
    # 原资格边界在同一真实公共材料上核验；仅输入组件，不倒退活动SIM/账户时钟。
    indices=T([0,1]);starts=T([-29000,-29000]);memory=history.empty(2,starts)
    endpoint=MarketFreezeMemory(encoder.prefix_at(T([offset-28200,offset-28200])),T([offset-28200,offset-28200]),None,None)
    boundary_roles=[]
    for at in (offset-28100,offset-28000,offset-27900):
        boundary_time=T([at,at]);prefix=encoder.prefix_at(boundary_time);groups=torch.arange(int(prefix.max().cpu())+1,device='cuda')
        memory,error=run(lambda:history.consume(groups,encoder.delivered_cutoffs[groups],indices,starts,memory,applied_prefix=prefix))
        assert error.cpu().tolist()==[0,0]
        frozen=run(lambda:encoder.freeze(boundary_time,indices,starts,memory,endpoint,applied_prefix=prefix))
        assert frozen.fault.cpu().tolist()==[0,0]
        boundary_roles.append(frozen.role.cpu().tolist());memory=frozen.history_memory;endpoint=frozen.endpoint_memory
    assert boundary_roles==[[1,-1],[1,0],[1,0]]
    late=T([1100+offset,1100+offset]);memory=history.empty(2,late);prefix=encoder.prefix_at(T([1300+offset,1300+offset]))
    first=torch.searchsorted(encoder.delivered_cutoffs,late,right=False)
    memory=replace(memory,last_prefix=first-1)
    groups=torch.arange(int(first.min().cpu()),int(prefix.max().cpu())+1,device='cuda')
    memory,error=run(lambda:history.consume(groups,encoder.delivered_cutoffs[groups],indices,late,memory,applied_prefix=prefix))
    frozen=run(lambda:encoder.freeze(T([1300+offset,1300+offset]),indices,late,memory,
        MarketFreezeMemory(encoder.prefix_at(T([1200+offset,1200+offset])),T([1200+offset,1200+offset]),None,None),applied_prefix=prefix))
    assert error.cpu().tolist()==[0,0] and frozen.role.cpu().tolist()==[-1,-1]
    assert not memory.counts.any().cpu().item()
    # 旧冻结仍持有不同市场的原输入；释放市场记忆不删除公共旧订单/经济人口。
    assert observations[0].frozen.role.cpu().tolist()==[1,0]
    assert clock.public.ledger.reservations.request_slots.size(0)==1
    bodies=[sender.requests[1][0] for sender in senders if sender.requests is not None]
    assert any(body['method']=='POST_ORDERS' and body['request']['items'][0]['item']['order']['condition_id']==source['initial'][2]['market']['condition_id'] for body in bodies)
    result=dict(passed=True,scope='one account/current trade plus T-30 next ContextOnly; next open inherits all GRU layers',
        account_roots=1,market_memories=2,observation_times=list(range(1300+offset,2900+offset,100)),
        model_starts=[int(task.started_at.cpu()[0]) for task in actual],warm_recurrences_before_open=warm_steps,
        boundary_roles=boundary_roles,late_root_roles=frozen.role.cpu().tolist(),
        old_observation_completes_after_market_end_at=2350+offset,shared_slow_except_role=True,
        history_path_and_countdown_checked=True,whole_root_freeze_capacity_retry=True,
        old_memory_released_after_pending_observations=True,new_market_request_from_original_Actor=True,
        end_resource_failure_preserves_accepted_frontier=end_retry,market_release_calls=release_calls,
        next_history_equals_once_consumed_original_groups=True,
        business_random=random.counter.cpu().tolist(),requests=bodies,
        full_root_history_or_economic_tail_delivered=False,elapsed_seconds=time.perf_counter()-started)
    (artifact/'two-market-report.json').write_text(json.dumps(result,indent=2))
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
