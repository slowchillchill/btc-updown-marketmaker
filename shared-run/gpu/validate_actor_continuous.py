"""同一真实有界账户的连续观察/Actor/处理/公开形成/取消；CPU仅后验。"""
from validate_actor_send import *
from actor_send import SubmittedActorRequests,decimal_values
from continuous_actor import ContinuousActorRun
if os.environ.get('B_GPU_CONTINUOUS_OLD_PRIORITY'):
    spec=importlib.util.spec_from_file_location('old_continuous_priority',os.environ['B_GPU_CONTINUOUS_OLD_PRIORITY'])
    previous=importlib.util.module_from_spec(spec);sys.modules[spec.name]=previous;spec.loader.exec_module(previous)
    ContinuousActorRun=previous.ContinuousActorRun
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
from ledger_drain import PublicLedgerReceiver
from task_requests import ActorRequestDirectory,ActorTaskRequests
import weakref,gc


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


def continuous(source,clock,pm,market,reference,binance,block,sampler,rule,receipt,intents,asks,taker,maker,codec,artifact,stage,*,second_buy=False,assembled,root_runtime=None):
    evidence=[];original_stage=stage;page_changes=0;hidden_pages=[]
    receiver=PublicLedgerReceiver() if root_runtime is None else root_runtime.receiver
    received_checks=[]
    def stage(name,**row):
        nonlocal page_changes
        if name=='publication_boundary' and os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL') and int(clock.gates.public_visible_at.cpu()[0])==87_000_009:
            actual=amounts(clock.public.ledger.platform.balances)[0]
            assert actual==same_tick_cash,('Sk不得读取同刻新增现金',actual,same_tick_cash)
            same_tick_boundaries.append(actual)
        if name=='public_page':page_changes+=1
        if name=='hidden_page':hidden_pages.append(row)
        if name=="continuous_event":
            row.update(business_random=random.counter.cpu().tolist(),history_prefix=material.memory.last_prefix.cpu().tolist(),
                published=[s.publication.published.cpu().tolist() for s in senders],receipts=[s.receipt is not None for s in senders])
            evidence.append(dict(stage=name,**row))
        original_stage(name,**row)
    two=source.get('continuous_two_markets',False);offset=source.get('continuous_observation_offset',0)
    gap=source.get('continuous_market_gap',False)
    full_warmup=source.get('continuous_full_warmup',False)
    first_observation=offset-28000 if full_warmup else 1300+offset
    started=time.perf_counter();zero=T([0]);yes=T([True],torch.bool);empty=T([[]]);start=zero-29000
    market_indices=assembled.market_indices;row_roots=T([0,0]) if two else zero
    identities=T([[0,0,0],[0,0,1]]) if two else T([[0,0,0]]);condition_slots=T(list(range(2 if two else 1)))
    conditions=source.get('cold_conditions',tuple(event['market']['condition_id'] for event in source['initial'][::2]))
    condition=conditions[0];tokens=source.get('cold_tokens',tuple(a['token_id'] for a in source['bounded_hidden']['assets']))
    asset_ends=source.get('cold_asset_ends',tuple(event['market']['window']['end_at_utc_ms'] for event in source['initial']))
    if root_runtime is None:
        legs=run(lambda:SubmittedLegs(T([[True,False]],torch.bool),T([[False,False]],torch.bool),T([[0,-1]]),T([[0,1]]),
            rational(['1000.001','0']),T([[True,False]],torch.bool),T([[0,-1]]),T([[0,1]]),T([[True,False]],torch.bool),
            SubmittedBuyTerms(T([[2,0]]),T([[False,False]],torch.bool),rational(['0.5','0']),rational(['500.0005','0']),T([[0,-1]]))))
        legs=run(lambda:bind_submission_receipt(legs,zero,receipt))
        population=SubmittedActorRequests(intents,legs)
        assert not clock.public.registry.events.received.any().cpu().item()
        history_targets=run(lambda:CancellationTargetHistory.open(integers([]),empty.bool()))
        clock.public=replace(clock.public,cancellation_target_history=history_targets)
        from actor_runtime import ActorDirectory,ActorRuntime,configure_continuous_execution
        directory=ActorDirectory(tuple(conditions),tuple(tokens),tuple(a['token_id'] for a in clock.source.prepared.columns['assets']),
            tuple(asset_ends),market_indices,assembled.condition_by_asset,block.asset_axis,asks)
        scenario=run(lambda:CancelFailureScenario(empty,integers([]),integers([]),empty.bool(),zero.to(torch.int32)))
        def paths(kind,world,selected):
            width=world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1)
            return T([[0]*width])
        configure_continuous_execution(clock,directory,population,rule,codec,sampler,taker,maker,paths,scenario)
        clock.source.states=(pm,market,reference,binance)
        root_runtime=ActorRuntime(clock,directory,population,receiver,{})
    else:population=root_runtime.population
    if two and 'cold_authority' not in source:
        from maker_days import MakerDays
        from rebate_coordinator import RebateCoordinator
        clock.runtime=replace(clock.runtime,calendars=run(lambda:RebateCoordinator(
            MakerDays.empty(condition_slots[None,:],T([[301000,601000]]),yes[:,None].expand(1,2)),clock.runtime.calendars.taker)))
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
    if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") and not two:policies[1900]=policies[1800]
    if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL'):policies[1900]=policies[1300]
    if two:
        for at in range(1900+offset,2800+offset,100):
            policy=copy.deepcopy(policies[1800+offset])
            if at==2000+offset:
                with torch.no_grad():policy.action.actor.family[-1].bias[2]=40.;policy.action.actor.family[-1].bias[0]=0.
            policies[at]=policy
    if 'cold_authority' in source:
        # 下一市场的原Actor GTC跨越真实0.5 ask，覆盖公开最终性与资金延续。
        with torch.no_grad():
            policies[2000+offset].action.actor.price.bias[38]=0.
            policies[2000+offset].action.actor.price.bias[49]=40.
    if full_warmup:
        for at in range(first_observation,1300+offset,100):policies[at]=policies[1800+offset]
        policies[2800+offset]=policies[1800+offset]
    if gap:
        for at in (302000,302100,302200):policies[at]=policies[1800+offset]
    random=ActionRandom(zero+181606,T([[0,0,0,0]]));observations=[];senders=[];bound=[]
    model_records=[];completed_records=[];retired_observations=[];retired_senders=[]
    expected_hidden=torch.zeros((2,len(market_indices),64),device=zero.device)
    warm_steps=0;opened=False
    empty_rows=[];history_checks=[];history_retry=False;resource_candidate=None;end_retry=False;release_calls=[]
    def freeze(world,at,enabled):
        nonlocal history_retry,resource_candidate
        now=int(at.cpu()[0]);previous=int(material.previous_observation_ms.cpu()[0])
        before=stored_actual(material.memory);endpoint=stored_actual(material.endpoints)
        random_before=random.counter.clone()
        observed,accepted=model_root.freeze(world,at,enabled)
        if observed is None:
            assert accepted.cpu().tolist()==[True] and torch.equal(random.counter,random_before)
            empty_rows.append(dict(at=now,hot_rows=len(material.market_indices),prefix=material.previous_prefix.cpu().tolist(),random=random.counter.cpu().tolist()))
            stage('no_active_market_observation',at=at.cpu().tolist(),hot_rows=len(material.market_indices),
                previous_prefix=material.previous_prefix.cpu().tolist(),random=random.counter.cpu().tolist())
            return observed,accepted
        required=observed.market.required.max(0).values.cpu().tolist()
        shortfall=required[2]>material.encoder.events.window.fact_capacity
        if source.get('nonzero_history_samples') and not two and now==1500 and not history_retry:
            assert shortfall
        if shortfall:
            assert not observed.market.resources_fit.any().cpu().item() and not accepted.any().cpu().item()
            assert stored_actual(material.memory)==before
            assert_endpoints_uncommitted(endpoint,stored_actual(material.endpoints))
            stage('history_resource_retry',at=now,capacity=material.encoder.events.window.fact_capacity,required=required,
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
            if len(shared_slow)==2:assert torch.equal(shared_slow[0],shared_slow[1])
            assert observed.account.cutoff.numel()==1 and world.public.ledger.fault.numel()==1
            expected_roles=([1] if now<301000 else [0]) if gap else ([1,0] if now<2000+offset else [1])
            assert observed.frozen.role.cpu().tolist()==expected_roles
            assert observed.native.scopes.current_market.cpu().tolist()==[0 if now<2000+offset else (-1 if gap else 1)]
            if now<2000+offset and not gap:
                assert observed.frozen.slow[:,93:95].cpu().tolist()==[[1.,0.],[0.,1.]]
            elif not gap:
                assert observed.native.scopes.present[0,1,0].cpu().item()
            import math
            for row,identity in enumerate(observed.frozen.identities.cpu().tolist()):
                market_row=identity[2]
                samples=source['two_market_history_samples'][market_row]
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
                assert observed.frozen.fast[row,440].cpu().item()==torch.tensor(math.log1p((market.times[market_row,1].cpu().item()-now)/1000),dtype=torch.float32).item()
            assert torch.equal(observed.frozen.fast[0,335:439],observed.frozen.fast[1,335:439]) if now<2000+offset and not gap else True
            assert material.endpoints.pm_sides.best_present.size(0)==len(material.market_indices)
        stage('continuous_freeze',at=at.cpu().tolist(),accepted=accepted.cpu().tolist(),fault=observed.frozen.fault.cpu().tolist(),
            required=observed.market.required.cpu().tolist(),provided=observed.frozen.provided.all(1).cpu().tolist(),
            order_count=int(observed.account.orders.present.sum().cpu()),cancel_count=int(observed.actor.cancels.present.sum().cpu()))
        if accepted.any().cpu().item():observations.append(observed)
        return observed,accepted
    def policy_start(observation,enabled):
        at=int(observation.frozen.cutoff.cpu()[0]);bound.append(at)
        return policies[at],zero+at
    def receive(public,delivery,enabled):
        public,ready=receiver(public,delivery,enabled)
        columns,waiting=public.ledger.columns,public.waiting
        pending=waiting.received&~waiting.consumed
        row=dict(at=clock.gates.public_visible_at.cpu().tolist(),
            platform_prefix=values(public.ledger.platform.platform_source_prefix),
            ready=ready.cpu().tolist(),waiting=[dict(column=column,row=int(columns.row_refs[root,column].cpu()),
                sequence=values(columns.sequences)[root*columns.row_refs.size(1)+column],
                first_available_at=int(waiting.available_at[root,column].cpu()))
                for root,column in pending.nonzero().cpu().tolist()],
            resources_fit=True if receiver.captured is None else bool(receiver.captured.resources_fit.cpu()))
        if 'cold_authority' in source:
            sequences=values(columns.sequences)
            row['late_predecessor']=[dict(row=int(columns.row_refs[root,column].cpu()),sequence=sequences[root*columns.row_refs.size(1)+column],
                received=bool(waiting.received[root,column].cpu()),consumed=bool(waiting.consumed[root,column].cpu()),
                first_available_at=int(waiting.available_at[root,column].cpu()))
                for root,column in columns.enabled.nonzero().cpu().tolist()
                if sequences[root*columns.row_refs.size(1)+column] in (7,8)]
        if receiver.captured is not None and not row['resources_fit']:
            row['required']={name:value.cpu().tolist() for name,value in receiver.captured.required.items()}
        received_checks.append(row);stage('public_ledger_receive',**row)
        return public,ready
    def sender_factory(task):
        sender=model_root.sender_factory(task)
        # 此消费者仅在有限代表即时比较，完成后原列表释放。
        sender.receive=receive
        senders.append(sender);return sender
    def model_started(task):
        nonlocal warm_steps,opened
        observation=task.observation;at=int(observation.account.cutoff.cpu()[0])
        original=next(value for value in observations if int(value.account.cutoff.cpu()[0])==at)
        assert observation is original
        assert task.policy is policies[at]
        assert task.evaluation.policy_version.cpu().tolist()==[at]*len(observation.frozen.role)
        if two:
            # 独立逐市场前向在真实开始点比较，不存历史task/策略/冻结。
            for role in (0,1):
                for row,identity in enumerate(observation.frozen.identities.cpu().tolist()):
                    market_row=identity[2]
                    if observation.frozen.role[row].cpu().item()!=role:continue
                    selected=observation.frozen.rows(T([row]))
                    with torch.no_grad():state,hidden,value=task.policy.state_and_value(*selected.model_inputs(),expected_hidden[:,market_row:market_row+1])
                    torch.testing.assert_close(task.evaluation.hidden[:,row:row+1],hidden)
                    torch.testing.assert_close(task.evaluation.state[row:row+1],state)
                    if market_row==1 and role==0:warm_steps+=1
                    if market_row==1 and role==1 and not opened:
                        assert warm_steps==(300 if full_warmup else 7) and expected_hidden[:,market_row:market_row+1].abs().sum().cpu().item()>0
                        opened=True
                    expected_hidden[:,market_row:market_row+1]=hidden
        else:
            with torch.no_grad():expected=task.policy.frozen_state_and_value(observation.frozen,expected_hidden,identities,task.evaluation.policy_version)
            torch.testing.assert_close(task.evaluation.hidden,expected.hidden)
            expected_hidden.copy_(expected.hidden)
        model_records.append(dict(cutoff=at,started=int(task.started_at.cpu()[0]),completed=task.completed_at.cpu().tolist(),
            version=task.evaluation.policy_version.cpu().tolist(),observation=weakref.ref(original)))
    def model_completed(task):
        assert ((task.stage==3)|~task.active).all().cpu().item()
        original=next(row for row in model_records if row['started']==int(task.started_at.cpu()[0]))
        version=task.evaluation.policy_version.cpu().tolist()
        assert task.observation is original['observation']()
        assert version==original['version']
        stage('completed_task_binding',cutoff=original['cutoff'],version=version,original_frozen=True)
        if task.sender is not None:
            assert task.sender.identities.contexts==task.request_identity.contexts
            assert torch.equal(task.sender.control.global_requests,task.request_identity.slots)
            assert (~task.active|task.sender.complete).all().cpu().item()
            retired_senders.append(weakref.ref(task.sender))
        retired_observations.append(weakref.ref(task.observation))
        completed_records.append(int(task.observation.account.cutoff.cpu()[0]))
    def released_refs():
        observations.clear();senders.clear();gc.collect()
        assert all(ref() is None for ref in retired_observations)
        assert all(ref() is None for ref in retired_senders)
        assert all(not ((task.stage==3)|~task.active).all().cpu().item() for task in models.tasks)
        return dict(pending_tasks=len(models.tasks),completed_models=len(completed_records),
            retired_observations=len(retired_observations),retired_senders=len(retired_senders),
            weak_references_released=True,queue_owns_only_unfinished=True)
    def release_markets(ended):
        release_calls.append(ended.cpu().tolist());material.release_markets(ended)
    def timing(observation):
        at=int(observation.account.cutoff.cpu()[0])
        duration=150 if two else 114
        if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") and not two and at==1800:duration=214
        if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL') and at==1900:duration=87_000_009-2084
        if full_warmup and at!=1900+offset:duration=20
        return zero+duration,zero+120 if at==1300+offset else zero
    from model_train.continuous_root import ContinuousActorRoot
    model_root=ContinuousActorRoot(root_runtime,codec,rule,sampler,started_at=start,first_observation=T([first_observation]),
        trading_until=T([2000]) if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") and not two else None,
        request_namespace=source['run_id']+'/'+source['session_id'],policy_at_start=policy_start,timing=timing,
        random=random,model_shape=template.shape,word_capacity=16,path_capacity=32)
    material=model_root.inputs;models=model_root.models;shared=model_root.run
    history=material.history;encoder=material.encoder
    request_directory=model_root.request_directory;task_requests=model_root.task_requests
    if 'cold_authority' not in source:request_directory.append(source['request_id'])
    models.sender_factory=sender_factory
    models.on_started=model_started;models.on_completed=model_completed;models.release_markets=release_markets
    if source.get('nonzero_history_samples'):
        encoder.events.window.fact_capacity=encoder.events.window.source_capacity=1
    def receive_observation(public,delivery,enabled):
        at=clock.gates.public_visible_at.cpu().tolist()
        if at[0] in (1600,1700):
            stage('actual_closed_diagnostic',at=at,
                closed=diagnostic_columns(delivery.closed,('emitted','record_refs','delivered_sequences','available_at','fault')),
                projection_batches=[diagnostic_columns(batch,('rows','emitted','received_at','private_sequences',
                    'batch_sequences','record_ordinals','platform_at','request_slots','item_ordinals'))
                    for batch in public.ledger.columns.projection_batches])
        return receive(public,delivery,enabled)
    shared.freeze=freeze;shared.receive=receive_observation
    def grow_cancel_results(effect,fault):
        if effect is not None and hasattr(effect,'required_results'):
            capacity=1 if clock.runtime.cancel_results is None else clock.runtime.cancel_results.capacity
            required=int(effect.required_results.max().cpu())
            if required>capacity:
                assert fault.cpu().tolist()==[2] and effect.fault.cpu().tolist()==[2]
                from cancel_results import CancelResults
                old=clock.runtime.cancel_results or CancelResults(1)
                first_groups=old.groups;old_done=clock.runtime.requests.done.clone();old_random=random.counter.clone()
                old_input=values(clock.runtime.input_frontier.next_sequence);old_private=values(clock.projection.emitter.next_private_sequence)
                assert model_root.grow_cancel_results(effect)
                assert clock.runtime.cancel_results.groups is first_groups
                assert torch.equal(clock.runtime.requests.done,old_done) and torch.equal(random.counter,old_random)
                assert values(clock.runtime.input_frontier.next_sequence)==old_input and values(clock.projection.emitter.next_private_sequence)==old_private
                stage('cancel_result_capacity_continuation',capacity=capacity,required=required,done=old_done.cpu().tolist(),
                    input_sequence=old_input,random=old_random.cpu().tolist(),private_sequence=old_private)
                return True
        return False
    continuations=0
    while True:
        result=shared.advance_before(T([302201 if gap else (2801+offset if two else 1901)]),yes,progress=stage)
        if result.fault.any().cpu().item():
            advance=result.stage_result;effect=None if advance is None else advance.stage_result
            stage('actual_continuous_fault',fault=result.fault.cpu().tolist(),
                effect_type=type(effect).__name__,required_results=None if effect is None or not hasattr(effect,'required_results') else effect.required_results.cpu().tolist(),
                result_capacity=None if clock.runtime.cancel_results is None else clock.runtime.cancel_results.capacity,
                pending=diagnostic_columns(clock.runtime.requests.next_pending(),('request_slot','request_key','item_ref','ordinal','effective_at','exists','fault')),
                done=clock.runtime.requests.done.cpu().tolist(),input_sequence=values(clock.runtime.input_frontier.next_sequence),
                ledger_prefix=values(clock.public.ledger.current_local_prefix))
        advance=result.stage_result;effect=None if advance is None else advance.stage_result
        if grow_cancel_results(effect,result.fault):
            continuations+=1;continue
        assert result.fault.cpu().tolist()==[0],result.fault.cpu().tolist()
        if result.complete.cpu().item():break
        if resource_candidate is not None:
            failed,required,hidden_before,identity_before,random_before=resource_candidate
            assert torch.equal(shared.next_observation,failed.account.cutoff)
            assert torch.equal(models.hidden,hidden_before) and torch.equal(models.identities,identity_before)
            assert torch.equal(random.counter,random_before)
            assert all(task.observation is not failed for task in models.tasks)
            if two and not gap and int(failed.account.cutoff.cpu()[0])==2000+offset:
                # 无积压的原队列代表：失败的新截止不能替代已接纳观察前沿。
                released=[]
                probe=ActorModelQueue(hidden_before,identity_before,models.last_completed_at,policy_start,
                    models.timing,sender_factory,row_roots=models.row_roots,market_ends=models.market_ends,
                    release_markets=lambda mask:released.append(mask.cpu().tolist()))
                probe.enqueue(failed,torch.zeros_like(yes))
                probe.retire_consumed_markets(shared.next_observation-100)
                assert torch.equal(probe.hidden,hidden_before) and not released
                assert torch.equal(probe.identities,identity_before)
                probe.retire_consumed_markets(failed.account.cutoff)
                probe.retire_consumed_markets(failed.account.cutoff+100)
                assert released==[[True,False]] and probe.identities.cpu().tolist()==[[0,0,1]]
                assert probe.hidden.shape==(2,1,64)
                assert torch.equal(probe.hidden[:,0],hidden_before[:,1])
                end_retry=True
            assert model_root.resume_resources(result)
            history_retry=True;resource_candidate=None
            stage('whole_root_input_growth',at=shared.next_observation.cpu().tolist(),required=required,end_retry=end_retry)
            continue
        required_requests=max([clock.runtime.requests.capacity]+[int(s.receipt.required_requests.max().cpu()) for s in senders if s.receipt is not None])
        required_legs=max([clock.public.ledger.reservations.capacity]+[int(s.publication.reservation.decision.required_capacity.max().cpu())
            for s in senders if s.publication.reservation is not None])
        old=clock.public.ledger
        if required_legs>old.reservations.capacity or required_requests>clock.runtime.requests.capacity:
            stage('continuous_capacity_requirement',requests=clock.runtime.requests.capacity,required_requests=required_requests,
                legs=old.reservations.capacity,required_legs=required_legs)
            assert model_root.resume_resources(result)
            continuations+=1
            continue
        if model_root.resume_resources(result):
            continuations+=1;continue
        frozen=observations[-1] if observations else None
        if frozen is not None:stage('blocked_unknown',fault=frozen.frozen.fault.cpu().tolist(),required=frozen.market.required.cpu().tolist())
        raise AssertionError('真实连续路径未完成且尚无已报告物理需求')
    real_tasks=len(observations)
    assert len(request_directory.keys)==real_tasks+int('cold_authority' not in source)
    assert request_directory.keys[int('cold_authority' not in source):]==[
        source['run_id']+'/'+source['session_id']+':actor:'+str(i) for i in range(real_tasks)]
    raw_bytes,raw_offsets=request_directory.columns();raw=bytes(raw_bytes.cpu().tolist());offsets=raw_offsets.cpu().tolist()
    assert [raw[offsets[i]:offsets[i+1]].decode('utf-8') for i in range(len(offsets)-1)]==request_directory.keys
    stage('actual_task_request_binding',real_tasks=real_tasks,keys=len(request_directory.keys),
        encoded_bytes=len(raw),byte_capacity=request_directory.bytes.numel(),offset_capacity=request_directory.offsets.numel(),
        failed_freeze_allocates_no_identity=True,started_task_keeps_original_binding=True)
    if os.environ.get('B_GPU_PUBLIC_BLOCKS'):
        # 有限正确性代表的原整页调度对照；不安装到生产来源或SIM/Ledger。
        from public_device_page import build_public_device_page
        _,_,_,_,full_columns=build_public_device_page(os.environ['B_GPU_COLUMNS_BINARY'],
            clock.source.prepared.material,artifact/'independent-full-window')
        from public_state import PublicPmState
        from market_state import PublicMarketState
        from reference_state import PublicReferenceState
        from binance_state import PublicBinanceState
        epm=PublicPmState(full_columns);emarket=PublicMarketState(epm,full_columns)
        ereference=PublicReferenceState(emarket,full_columns);ebinance=PublicBinanceState(epm,full_columns)
        history=SourceHistoryInputs(epm,emarket,ereference,full_columns,identity_registry=material.history.registry)
        encoder=MarketInputs100ms(epm,emarket,ereference,ebinance,history,rule,full_columns,epm.causal_visible_at)
        pm,market,reference,binance=epm,emarket,ereference,ebinance
    if two:
        assert page_changes and hidden_pages
        if gap:
            assert len(empty_rows)==10 and [row['at'] for row in empty_rows]==list(range(301000,302000,100))
            assert any(row['hot_rows']==0 for row in empty_rows)
            assert all(left['prefix'][0]<=right['prefix'][0] for left,right in zip(empty_rows,empty_rows[1:]))
            assert material.identities.cpu().tolist()==[[0,0,1]] and models.hidden.shape==(2,1,64)
            assert len(observations)==10 and warm_steps==2 and not opened
            old=next(row for row in model_records if row['cutoff']==300900)
            assert old['completed']==[301350] and 300900 in completed_records
            assert material.memory.counts.cpu().tolist()==[[1,0,0,0,0,0]]
            assert not clock.public.ledger.fault.any().cpu().item()
            result=dict(empty_market_interval=empty_rows,actual_observations=len(observations),
                current_hot_rows=len(material.market_indices),stable_market_identity=material.identities.cpu().tolist(),
                old_compute_crossed_end=True,new_market_context_steps=warm_steps,
                history_counts=material.memory.counts.cpu().tolist(),one_account=True,no_new_gap_observation_or_rng=True)
        else:
            result=verify_two_market_continuous(source,clock,material,models,observations,senders,history_retry,
                history,encoder,random,evidence,started,artifact,stage,end_retry,release_calls,model_records,warm_steps,opened)
        import shutil
        registry=material.history.registry
        result['source_identity_index']={'entries':registry.identity_count,'disk_bytes':registry.path.stat().st_size,
            'shared_across_current_next_market':history.registry is registry,'cache_hint_kib':2048}
        shutil.copyfile(registry.path,artifact/'source-identities.sqlite3')
        registry.close()
        frontier=clock.runtime.hidden_frontier
        root=frontier.columns.source.catalog
        _,hidden_eof=frontier.scheduled()
        assert isinstance(root,PreparedHiddenPages) and root.lifecycle_values is frontier.columns.source.lifecycle_values
        assert root.terminal_columns is frontier.columns.source.terminal_columns
        assert int(frontier.closed_bucket.cpu()[0])==frontier.columns.bucket_base+int(frontier.next_bucket.cpu()[0])-1
        result['dual_source_blocks']={'public_parts':len(clock.source.prepared.parts),
            'hidden_parts':len(root.parts),'hidden_buckets':len(root.arrays['bucket_times']),
            'public_page_changes':page_changes,'hidden_pages':hidden_pages,
            'public_position':clock.source.position.cpu().tolist(),
            'closed_global_bucket':frontier.closed_bucket.cpu().tolist(),
            'hidden_index_position':values(frontier.index_position),'hidden_eof':hidden_eof.cpu().tolist(),
            'root_terminal_and_lifecycle_shared':True,
            'allocated_peak_bytes':torch.cuda.max_memory_allocated(),
            'reserved_peak_bytes':torch.cuda.max_memory_reserved(),
            'process_max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'resource_scope':'since source installation peak reset; includes live source baseline, opening/continuous model and independent GPU oracle; Python process RSS since start excludes writer/Rust parent; no driver or OS-cache peak'}
        if 'cold_authority' in source:
            balance=source['cold_authority']['ledger_projection']['platform_confirmed_projection']
            assert balance['cash_pusd']=='2000' and balance['available_pusd']=='2000'
            assert balance['open_order_count']==0 and balance['pending_fill_count']==0 and not balance['outcome_balances']
            actual_tokens=list(tokens);public_tokens=[a['token_id'] for a in clock.source.prepared.columns['assets']]
            assert actual_tokens!=public_tokens
            if not gap:
                assert clock.runtime.fills.occupied.any().cpu().item()
                assert any(value>0 for value in amounts(clock.public.ledger.success_pools.quantity))
            # 这些时刻属于本次150ms模型延迟的短代表，不要求完整热身的20ms配置相同。
            late_predecessor=[]
            if not full_warmup and not gap:
                early=next(row for row in received_checks if row['at']==[301600])
                later=next(row for row in received_checks if row['at']==[301700])
                assert early['ready']==[True] and early['resources_fit'] and early['platform_prefix']==[6]
                expected_early=[dict(sequence=7,received=False,consumed=False,first_available_at=0),
                    dict(sequence=8,received=True,consumed=False,first_available_at=301600)]
                expected_later=[dict(sequence=7,received=True,consumed=True,first_available_at=301700),
                    dict(sequence=8,received=True,consumed=True,first_available_at=301600)]
                assert [{k:v for k,v in row.items() if k!='row'} for row in early['late_predecessor']]==expected_early
                assert later['ready']==[True] and later['platform_prefix']==[8] and not later['waiting']
                assert [{k:v for k,v in row.items() if k!='row'} for row in later['late_predecessor']]==expected_later
                early_rows=[row['row'] for row in early['late_predecessor']]
                assert len(set(early_rows))==2 and min(early_rows)>=0
                assert [row['row'] for row in later['late_predecessor']]==early_rows
                late_predecessor=[early,later]
            result['real_cold_opening']=dict(session_sequences=source['cold_authority']['session_sequences'],
                ledger_entry=source['cold_authority']['ledger_projection']['ledger_entry_seq'],activation=source['cold_authority']['activation'],
                initial_cash='2000',initial_orders=0,initial_legs=0,public_tokens=public_tokens,sim_tokens=actual_tokens,
                current_cash=str(amounts(clock.public.ledger.platform.balances)[0]),
                success_quantities=[str(v) for v in amounts(clock.public.ledger.success_pools.quantity)],
                source_platform_prefix=values(clock.public.ledger.platform.platform_source_prefix),late_predecessor=late_predecessor)
        result['task_retirement']=released_refs()
        (artifact/'two-market-report.json').write_text(json.dumps(result,indent=2))
        return result
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
    # 新任务身份依法改变原经验时延；C1900不能读取1939才处理的取消。
    # 在原1901边界先保留模型/冻结，再只推进真实已发请求及其可见反馈。
    cancel_frozen=observations[-1].actor.cancels.present.clone()
    pending_model_event=models.next_event()
    original_body=senders[4].requests;original_receipt=senders[4].receipt
    target_orders=[order for order,on in zip(values(senders[4].publication.action.cancel_orders),
        senders[4].publication.action.cancel_present.cpu().tolist()[0],strict=True) if on]
    directory=observations[-1].account.intent_orders
    target_columns=T([[order in target_orders for order in values(directory.identities)]],torch.bool)
    assert target_columns.sum().cpu().item()==len(target_orders)>0
    def cancellation_received():
        completions=clock.public.cancellations.completions
        identities=values(completions.requests)
        completed=any(on and request==int(senders[4].control.global_requests.cpu()[0])
            for request,on in zip(identities,completions.received.cpu().tolist()[0],strict=True))
        orders=run(lambda:clock.public.registry.freeze_orders(directory,clock.gates.public_visible_at))
        closed=bool((~target_columns|(orders.cancellable_known&~orders.cancellable)).all().cpu())
        return completed and closed,orders
    cancelled,closed_orders=cancellation_received()
    stage('actual_cancel_inflight_boundary',at=clock.gates.public_visible_at.cpu().tolist(),
        arrived_at=original_receipt.arrived_at.cpu().tolist(),received=cancelled,
        pending_model_at=pending_model_event.at.cpu().tolist())
    while not cancelled or clock.public.pending_noe.active.any().cpu().item() or (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item():
        event=clock.next_event(yes)
        assert event.present.all().cpu().item() and not event.fault.any().cpu().item()
        # 本有限材料的取消反馈早于下一模型完成；不是生产等待期限。
        assert not pending_model_event.present.any().cpu().item() or (event.at<pending_model_event.at).all().cpu().item()
        advance=clock.advance_through(event.at,yes,progress=stage)
        if grow_cancel_results(advance.stage_result,advance.fault):
            continuations+=1;continue
        assert advance.complete.all().cpu().item() and not advance.fault.any().cpu().item()
        delivery=clock.receive_first(enabled=yes)
        clock.public,ready=receive(clock.public,delivery,yes)
        assert ready.all().cpu().item() and not delivery.fault.any().cpu().item()
        cancelled,closed_orders=cancellation_received()
        stage('actual_cancel_feedback_continuation',at=event.at.cpu().tolist(),
            completed=cancelled,cancellable=closed_orders.cancellable.cpu().tolist(),
            pending_model_at=pending_model_event.at.cpu().tolist(),
            platform_prefix=values(clock.public.ledger.platform.platform_source_prefix),
            pending_noe=clock.public.pending_noe.active.cpu().tolist(),
            waiting=(clock.public.waiting.received&~clock.public.waiting.consumed).cpu().tolist())
    assert senders[4].requests is original_body and senders[4].receipt is original_receipt
    assert torch.equal(observations[-1].actor.cancels.present,cancel_frozen)
    assert shared.next_observation.cpu().tolist()==[2000] and len(observations)==7
    assert clock.runtime.post_results is not None and clock.runtime.cancel_results is not None
    # C1400在C1300发送前冻结；模型全部层沿同根顺序承接，没有按发送重置。
    assert [row['started'] for row in model_records]==[1300,1414,1528,1642,1756,1870]
    assert len(models.tasks)==2  # C1800计算中和C1900排队；旧发送对象已退役。
    # 新真实ID的Accepted/formed都已在C1600首次公开；仍核原请求的成员关联。
    old=observations[3].account.intent_orders
    assert old.origin_known.cpu().tolist()==[[True]*(5 if second_buy else 3)]
    pairs=observations[3].account.cancel_directory
    assert pairs.paired_members[pairs.paired_present].cpu().tolist()==([[1,2],[3,4]] if second_buy else [[1,2]])
    assert observations[4].account.cancel_directory.paired_present.any().cpu().item()
    assert observations[-1].account.orders.cancellable[target_columns].all().cpu().item()
    assert not closed_orders.cancellable[target_columns].any().cpu().item()
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
    pending_models=[task for task in models.tasks if task.active.any().cpu().item() and (task.stage<3).any().cpu().item()]
    assert any(task.completed_at is not None and task.completed_at.cpu().tolist()==[2084 if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") else 1984] for task in pending_models)
    if second_buy:
        assert senders[1].control.execute_at.cpu().tolist()==[1528] and senders[0].control.execute_at.cpu().tolist()==[1534]
        assert senders[1].receipt is not None and senders[0].receipt is not None
        early=senders[1].dispatch.prepared[1].cpu().tolist()[0];late=senders[0].dispatch.prepared[1].cpu().tolist()[0]
        assert early==[1,2] and late==[3,4]
        early_id=senders[1].identities.contexts[0]['global_request_id'];late_id=senders[0].identities.contexts[0]['global_request_id']
        assert [json.loads(population.post_intents.items[0][ref])['item_id'] for ref in early]==[early_id+':0',early_id+':1']
        assert [json.loads(population.post_intents.items[0][ref])['item_id'] for ref in late]==[late_id+':0',late_id+':1']
    before_prefix=values(clock.public.ledger.current_local_prefix);before_random=random.counter.clone();before_input=values(clock.runtime.input_frontier.next_sequence)
    for sender in senders:
        repeat=sender.execute(clock,yes,progress=stage);assert repeat.receipt is sender.receipt
    assert values(clock.public.ledger.current_local_prefix)==before_prefix and torch.equal(random.counter,before_random)
    assert values(clock.runtime.input_frontier.next_sequence)==before_input
    result=dict(passed=True,scope='same runtime/account C1300→C1400 queued→c1414 before s1534→new Pair processed→public C1700→Pair cancel→C1900 original frozen/inflight→actual cancel visibility→current Ledger',
        requests=[{'sent_at':int(s.control.execute_at.cpu()[0]),'input_sequence':values(s.receipt.input_sequence)[0],'body':s.requests[1][0]} for s in senders if s.requests is not None],
        observations=[int(o.frozen.cutoff.cpu()[0]) for o in observations],model_starts=[row['started'] for row in model_records],
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
    if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL'):
        same_tick_boundaries=[]
        # C1900已接纳；从原完成2084起算，finish=Sk且dispatch=0。
        sk=T([87_000_009])
        before=shared.advance_before(sk,yes,progress=stage)
        assert before.complete.cpu().tolist()==[True] and before.fault.cpu().tolist()==[0]
        same_tick_cash=amounts(clock.public.ledger.platform.balances)[0]
        assert clock.public.pending_noe.active.any().cpu().item()
        assert shared.next_observation.cpu().tolist()==[2000]
        after=shared.advance_before(sk+1,yes,progress=stage)
        while not after.complete.all().cpu().item():
            sender=senders[-1];old=clock.public.ledger
            required=int(sender.publication.reservation.decision.required_capacity.max().cpu())
            requests=clock.runtime.requests.capacity if sender.receipt is None else int(sender.receipt.required_requests.max().cpu())
            assert after.fault.cpu().tolist()==[0] and (required>old.reservations.capacity or requests>clock.runtime.requests.capacity)
            if required>old.reservations.capacity:
                pad=required-old.reservations.capacity
                clock.public=replace(clock.public,ledger=replace(old,reservations=old.reservations.resized(required),
                    global_requests=torch.nn.functional.pad(old.global_requests,(0,pad),value=-1),
                    item_ordinals=torch.nn.functional.pad(old.item_ordinals,(0,pad),value=-1)))
            if requests>clock.runtime.requests.capacity:
                clock.runtime=replace(clock.runtime,requests=clock.runtime.requests.resized(requests,clock.runtime.requests.action_capacity))
            after=shared.advance_before(sk+1,yes,progress=stage)
        assert after.complete.cpu().tolist()==[True] and after.fault.cpu().tolist()==[0]
        assert same_tick_boundaries
        sender=senders[-1]
        assert sender.publication.action.family.cpu().tolist()==[2]
        assert sender.control.execute_at.cpu().tolist()==sk.cpu().tolist()
        assert sender.sent.cpu().tolist()==[True] and sender.receipt.registered.cpu().tolist()==[True]
        assert sender.publication.reservation.decision.outcomes.cpu().tolist()==[[1,1]]
        cpu_accrued=Fraction(source['received_ledger']['projection']['platform_confirmed_projection']['taker_rebate_receivable_pusd'])
        payable=Fraction((cpu_accrued*10000).numerator//(cpu_accrued*10000).denominator,10000)
        assert amounts(clock.public.ledger.platform.balances)[0]==same_tick_cash+payable
        result.update(same_tick_finish_send_and_cash_checked=True,send_at=87_000_009,
            pre_send_cash=str(same_tick_cash),subsequent_payment=str(payable))
    elif os.environ.get('B_GPU_ACTOR_WINDOW_TAIL') and not two:
        original_observations=list(result['observations'])
        tail=shared.advance_before(T([2300]),yes,progress=stage)
        assert tail.complete.cpu().tolist()==[True] and tail.fault.cpu().tolist()==[0]
        assert shared.next_observation.cpu().tolist()==[2000]
        assert [int(o.frozen.cutoff.cpu()[0]) for o in observations]==original_observations
        assert models.hidden.shape==(2,0,64) and not len(models.identities)
        assert material.memory.counts.shape==(0,6) and not len(material.market_indices)
        assert material.endpoints.previous_prefix.numel()==0
        assert not models.tasks
        result['window_stops_at']=2000;result['tail_checked_through_open_endpoint']=2300
        result['first_accepted_model_finish_after_window']=2084
        result['last_accepted_model_finish_after_window']=max(row['completed'][0] for row in model_records)
        # 原CPU FOK参考给应收，原日规则给付款；金额不从GPU付款后态生成。
        cpu_accrued=Fraction(source['received_ledger']['projection']['platform_confirmed_projection']['taker_rebate_receivable_pusd'])
        assert cpu_accrued==Fraction('1.4000016')
        payable=Fraction((cpu_accrued*10000).numerator//(cpu_accrued*10000).denominator,10000)
        pre_day=clock.public.ledger.platform.balances
        assert amounts(pre_day)[5]==cpu_accrued
        pre_cash=amounts(pre_day)[0];pre_entry=values(clock.public.ledger.current_entry_sequence)
        before_day=shared.advance_before(T([87_000_000]),yes,progress=stage)
        assert before_day.complete.cpu().tolist()==[True] and before_day.fault.cpu().tolist()==[0]
        assert amounts(clock.public.ledger.platform.balances)[0]==pre_cash
        assert values(clock.public.ledger.current_entry_sequence)==pre_entry
        after_day=shared.advance_before(T([87_010_000]),yes,progress=stage)
        assert after_day.complete.cpu().tolist()==[True] and after_day.fault.cpu().tolist()==[0]
        paid_balance=amounts(clock.public.ledger.platform.balances)
        assert paid_balance[0]==pre_cash+payable and paid_balance[5]==0 and paid_balance[7]==cpu_accrued-payable
        assert values(clock.public.ledger.current_entry_sequence)==[pre_entry[0]+4]
        assert shared.next_observation.cpu().tolist()==[2000]
        assert not (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item()
        result['taker_day_at_utc_ms']=87_000_000;result['taker_paid_pusd']=str(payable)
        result['taker_locked_pusd']=str(cpu_accrued-payable)
        result['elapsed_seconds']=time.perf_counter()-started
    partial=verify_partial_queue_retirement(observations[0],policies[1300])
    del sender,pending_models
    result['task_retirement']=dict(released_refs(),partial_roots=partial)
    stage('actual_task_retirement',**result['task_retirement'])
    return result


def verify_two_market_continuous(source,clock,material,models,observations,senders,history_retry,
        history,encoder,random,evidence,started,artifact,stage,end_retry,release_calls,model_records,warm_steps,opened):
    offset=source['continuous_observation_offset']
    assert history_retry and end_retry and release_calls==[[True,False]]
    full_warmup=source.get('continuous_full_warmup',False)
    first_observation=offset-28000 if full_warmup else 1300+offset
    assert [int(o.account.cutoff.cpu()[0]) for o in observations]==list(range(first_observation,2900+offset,100))
    assert clock.public.ledger.fault.numel()==clock.runtime.account.available.sign.numel()==1
    expected_starts=(list(range(first_observation,2000+offset,100))+[2050+offset]+list(range(2100+offset,2801+offset,100))
        if full_warmup else list(range(1300+offset,2801+offset,150)))
    assert [row['started'] for row in model_records]==expected_starts
    assert opened
    actor_cutoffs=[int(sender.driver.inputs.actor.cutoff.cpu()[0]) for sender in senders]
    assert actor_cutoffs==[row['cutoff'] for row in model_records if row['completed'][0]<2801+offset]
    assert all(sender.driver.inputs.actor.cutoff.numel()==1 for sender in senders)
    old_completed=(2050 if full_warmup else 2350)+offset
    assert any(row['cutoff']==1900+offset and row['completed']==[old_completed] for row in model_records)
    assert models.identities.cpu().tolist()==material.identities.cpu().tolist()==[[0,0,1]]
    assert models.hidden.shape==(2,1,64) and models.hidden.abs().sum().cpu().item()>0
    assert material.memory.counts.cpu().tolist()==[[7,0,0,0,0,0]]
    assert material.endpoints.pm_sides.known[0].any().cpu().item()
    first_nonzero=next(observation for observation in observations if int(observation.account.cutoff.cpu()[0])==1300+offset)
    assert first_nonzero.frozen.fast[0,542].cpu().item()==first_nonzero.frozen.fast[1,542].cpu().item()==1/16
    assert first_nonzero.frozen.fast[0,597].cpu().item()!=first_nonzero.frozen.fast[1,597].cpu().item()
    assert first_nonzero.market.endpoint_memory.pm_sides.asset_indices.cpu().tolist()==[[0,1],[2,3]]
    # 同一真实完整组一次消费的独立调度对照；只比较未退役下一市场。
    groups=torch.arange(int(material.memory.last_prefix.max().cpu())+1,device='cuda')
    once,error=run(lambda:history.consume(groups,encoder.delivered_cutoffs[groups],T([0,1]),
        T([-29000,-29000]),history.empty(2,T([-29000,-29000]))))
    assert error.cpu().tolist()==[0,0]
    once=run(lambda:history.freeze(once)).memory
    for field in fields(once):
        left,right=getattr(material.memory,field.name),getattr(once,field.name)
        if isinstance(left,torch.Tensor):assert torch.equal(left[0],right[1]),field.name
        else:
            width=left.sign.numel();indices=torch.arange(width,2*width,device='cuda')
            assert stored_actual(left)==stored_actual(_take(right,indices)),field.name
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
        account_roots=1,market_memories=2,observation_times=list(range(first_observation,2900+offset,100)),
        model_starts=[row['started'] for row in model_records],warm_recurrences_before_open=warm_steps,
        boundary_roles=boundary_roles,late_root_roles=frozen.role.cpu().tolist(),
        old_observation_completes_after_market_end_at=old_completed,shared_slow_except_role=True,
        history_path_and_countdown_checked=True,whole_root_freeze_capacity_retry=True,
        old_memory_released_after_pending_observations=True,new_market_request_from_original_Actor=True,
        end_resource_failure_preserves_accepted_frontier=end_retry,market_release_calls=release_calls,
        next_history_equals_once_consumed_original_groups=True,
        business_random=random.counter.cpu().tolist(),requests=bodies,
        full_root_history_or_economic_tail_delivered=False,elapsed_seconds=time.perf_counter()-started)
    (artifact/'two-market-report.json').write_text(json.dumps(result,indent=2))
    return result


def verify_partial_queue_retirement(original,policy):
    """实际模型队列三根接口；发布/发送替身只检部分根拥有，不冒充SIM时钟。"""
    from types import SimpleNamespace
    from actor_send import ActorSendResult
    indices=T([0,0,0]);frozen=original.frozen.rows(indices)
    frozen=replace(frozen,identities=T([[0,0,0],[0,1,0],[0,2,0]]))
    observation=replace(original,frozen=frozen,market=original.market.rows(indices),
        account=SimpleNamespace(cutoff=frozen.cutoff),decision_rows=T([0,1,2]),row_roots=T([0,1,2]),root_cutoffs=frozen.cutoff)
    yes=T([True]*3,torch.bool);zero=T([0]*3)
    class Sender:
        def __init__(self,task):
            self.complete=torch.zeros_like(yes);self.allowed=yes.clone()
            self.driver=self;self.publication=self;self.released=False
            self.binding=task.request_identity;self.control=self.binding.control(task.send_at)
        def publish(self,driver,control,active):
            return SimpleNamespace(published=active,action=SimpleNamespace(family=zero+1))
        def execute(self,clock,active,**kwargs):
            self.complete|=active&self.allowed
            return ActorSendResult(self,None,self.complete.clone(),self.complete.clone())
        def release_capture(self):self.released=True
    def make_queue():
        identities=ActorTaskRequests(ActorRequestDirectory(zero.device),('partial/root0','partial/root1','partial/root2'))
        return ActorModelQueue(torch.zeros((2,3,64),device=zero.device),frozen.identities,frozen.cutoff,
            lambda observation,active:(policy,zero+1300),lambda observation:(zero+10,zero),Sender,
            request_identities=identities,market_ends=frozen.cutoff+5)
    queue=make_queue();queue.enqueue(observation,yes);queued=queue.tasks[0];binding=queued.request_identity
    for root in range(3):
        active=T([i==root for i in range(3)],torch.bool)
        accepted,fault=queue.start(active)
        assert torch.equal(accepted,active) and not fault.any().cpu().item()
        assert all(task.request_identity is binding for task in queue.tasks)
        assert any(item is queued for item in queue.tasks)==(root<2)
    assert len(queue.tasks)==3 and all(task.evaluation is not None for task in queue.tasks)
    # 一个started对象拥有三根：前两根完成仍不能释放第三根。
    queue=make_queue();queue.enqueue(observation,yes);binding=queue.tasks[0].request_identity
    accepted,fault=queue.start(yes);assert accepted.all().cpu().item() and not fault.any().cpu().item()
    assert len(queue.tasks)==1;task=queue.tasks[0];assert task.request_identity is binding
    first=T([True,False,False],torch.bool)
    assert torch.equal(queue.finish(first),first)
    complete,fault,blocked=queue.send(None,first)
    assert torch.equal(complete,first) and not fault.any().cpu().item() and blocked is None
    assert queue.tasks==[task] and not task.sender.released
    queue.retire_consumed_markets(frozen.cutoff+20)
    assert queue.identities.cpu().tolist()==[[0,1,0],[0,2,0]] and queue.hidden.shape==(2,2,64)
    rest=T([False,True,True],torch.bool);assert torch.equal(queue.finish(rest),rest)
    task.sender.allowed=T([True,True,False],torch.bool)
    complete,fault,blocked=queue.send(None,rest)
    assert complete.cpu().tolist()==[False,True,False] and queue.tasks==[task]
    assert task.request_identity is binding and not task.sender.released
    task.sender.allowed=yes;complete,fault,blocked=queue.send(None,T([False,False,True],torch.bool))
    assert complete.cpu().tolist()==[False,False,True] and not queue.tasks and task.sender.released
    return dict(actual_queue=True,partial_start_transfers_once=True,partial_finish_and_send_preserve_task=True,
        identity_unchanged=True,all_roots_complete_retires=True,scope='real queue/model forward; publication/send interface substitutes, no SIM clock')


def main():
    source=json.load(sys.stdin);artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    path=Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so'
    spec=importlib.util.spec_from_file_location('model_native_inputs',path);codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    def progress(row):
        with (artifact/'continuous-progress.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
        if source.get('continuous_full_warmup') and row['stage'] in ('continuous_event','continuous_freeze'):
            at=row['at'][0]
            if at%1000:return
            row=dict(row,observation_position=max(0,(at-271000)//100+1),observations_total=309)
        print(json.dumps(row),file=sys.stderr,flush=True)
    if source.get('continuous_two_markets'):
        from latency import EmpiricalDelaySampler
        from rules import PublicRuleGpu
        # 有限材料的真实行情价位目录；不以未来请求推账户或预留。
        slots=set()
        for asset,event in enumerate(source['initial']):
            for side in ('bids','asks'):
                for level in event['payload']['event'][side]:
                    slots.add((asset,side,Fraction(level['price_pusd_per_share'])))
        for event in source['changed']:
            if event['payload']['event_type']=='level_execution_transition':
                value=event['payload']['event'];slots.add((0,'asks' if value['side']=='ASK' else 'bids',Fraction(value['price_pusd_per_share'])))
        slots=sorted(slots)
        ask_rows=[[i for i,(a,side,_) in enumerate(slots) if a==asset and side=='asks'] for asset in range(len(source['initial']))]
        width=max(map(len,ask_rows));asks=T([row+[-1]*(width-len(row)) for row in ask_rows])
        sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
            source['profile_sha256'],[source['session_seed'].encode('ascii')])
        rule_path=ROOT/'sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json'
        cold_request=dict(log_path=str(artifact/'cold-session.log'),noe_root=str(artifact/'cold-noe'),rule_path=str(rule_path),
            rule_sha256=hashlib.sha256(rule_path.read_bytes()).hexdigest(),rules_as_of_utc_ms=2**63-1,
            implementation_identity=json.loads(rule_path.read_text())['provenance']['implementation_identity'],
            source_sha256=hashlib.sha256((artifact/'public-compact/manifest.json').read_bytes()).hexdigest(),
            finality_policy=dict(schema_version='trade_finality_policy_v2',normal_path=source['finality_path'],failure_overrides=[]),
            activation_id='gpu-cold-activate',session=dict(run_id=source['run_id'],session_id=source['session_id'],
                account_id='gpu-cold-account',caller_role='MODEL_TRAIN',caller_instance_id='gpu-cold-caller',
                creation_request_id='gpu-cold-create',response_bridge_binding_id='gpu-cold-bridge',
                initial_account_state=dict(available_pusd='2000'),session_seed=source['session_seed']))
        taker,maker=run(lambda:(rational([source['taker_rebate_rate']]),rational([source['maker_rebate_rate']])))
        report=bounded(source,None,None,None,sampler,PublicRuleGpu(source['rule']),slots,T([0]*len(slots)),None,None,asks,
            taker,maker,progress,codec,artifact,continuous=continuous,cold_request=cold_request)
    else:
        report=check(source,codec.post_terms_batch_v2,progress,before_drive=lambda *args,**kwargs:bounded(*args,codec,artifact,continuous=lambda *a,**k:continuous(*a,second_buy='--out-of-order-buy' in sys.argv,**k),**kwargs),defer_initial_send=True)

    print(json.dumps(report),flush=True)

if __name__=='__main__':main()
