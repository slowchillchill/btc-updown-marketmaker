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
    if isinstance(value,(bool,int,float,str)):return value
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


def cost_observation_case(root,artifact,stage,observations,senders,model_records,completed_records,evidence,received_checks,resume_validation):
    """原有限Root的宿主归属计时；分项允许嵌套，仅整段两端同步。"""
    from public_ledger import PublicLedgerState
    from public_trade_loop import _CapturedTrade
    from maker_projection_loop import _CapturedMakerProjection
    clock=root.clock;stats={};restore=[];segments=[];begins={};snapshot_seconds=0.
    def measured(name,call):
        def invoke(*args,**kwargs):
            start=time.perf_counter()
            try:return call(*args,**kwargs)
            finally:
                seconds=time.perf_counter()-start
                row=stats.setdefault(name,dict(calls=0,host_seconds=0.,max_host_seconds=0.))
                row['calls']+=1;row['host_seconds']+=seconds;row['max_host_seconds']=max(row['max_host_seconds'],seconds)
        return invoke
    def patch(owner,name,label):
        old=getattr(owner,name);setattr(owner,name,measured(label,old));restore.append((owner,name,old))
    handlers=clock.handlers.copy()
    for phase,handler in handlers.items():clock.handlers[phase]=measured('sim_handler_'+str(phase),handler)
    patch(PublicLedgerState,'append_batches','public_append_batches')
    patch(clock,'receive_first','public_delivery')
    patch(PublicLedgerReceiver,'__call__','actual_ledger_receive')
    patch(root.run,'receive','public_receive_with_validation')
    patch(root,'freeze','actual_freeze');patch(root.run,'freeze','freeze_with_validation')
    for name in ('start','finish','send'):patch(root.models,name,'model_'+name)
    for name in ('on_started','on_completed'):patch(root.models,name,'validation_'+name)
    patch(_CapturedTrade,'__init__','cold_public_trade_capture')
    patch(_CapturedMakerProjection,'__init__','cold_maker_projection_capture')
    def progress(name,**row):
        if name in ('sim_phase_begin','continuous_event_begin'):
            key=name+':'+str(row.get('phase',row.get('kind')));begins[key]=begins.get(key,0)+1
        measured('progress_with_validation',stage)(name,**row)
    def snapshot(endpoint):
        nonlocal snapshot_seconds
        started=time.perf_counter();orders=clock.runtime.orders
        out=dict(endpoint=endpoint,open_base_units=values(orders.quantities.open_base_units),
            pending_base_units=values(orders.quantities.pending_maker_depletion_base_units),matched_base_units=values(orders.quantities.matched_base_units),
            order_occupied=orders.occupied.cpu().tolist(),assets=orders.assets.cpu().tolist(),accepted_at=orders.accepted_at.cpu().tolist(),
            source_position=clock.source.position.cpu().tolist(),input_sequence=values(clock.runtime.input_frontier.next_sequence),
            ledger_entry=values(clock.public.ledger.current_entry_sequence),ledger_prefix=values(clock.public.ledger.current_local_prefix),
            platform_prefix=values(clock.public.ledger.platform.platform_source_prefix),cash=[str(x) for x in amounts(clock.public.ledger.platform.balances)],
            platform_outcomes=[str(x) for x in amounts(clock.public.ledger.platform.outcomes)],
            ledger_fault=clock.public.ledger.fault.cpu().tolist(),account_fault=clock.runtime.account.fault.cpu().tolist(),
            order_fault=orders.fault.cpu().tolist(),public_at=clock.gates.public_visible_at.cpu().tolist(),
            next_observation=root.run.next_observation.cpu().tolist(),business_random=root.random.counter.cpu().tolist())
        receipt=clock.runtime.input_frontier.public_receipt
        assert receipt.source is clock.source.prepared
        out['public_receipt']=dict(positions=receipt.positions.cpu().tolist(),input_sequence=values(receipt.input_sequence),original_reference=receipt.original_reference(0))
        assert not any(out['ledger_fault']+out['account_fault']+out['order_fault']),out
        snapshot_seconds+=time.perf_counter()-started;return out
    # Root/Native冷装配和材料准备在区间外；原initialize/prehistory在首段内。
    torch.cuda.synchronize();whole_start=time.perf_counter()
    try:
        for endpoint in (1301,1501,1535,1743,1792,1850,2001):
            start=time.perf_counter();attempts=0
            while True:
                attempts+=1;advance=root.run.advance_before(T([endpoint]),T([True],torch.bool),progress=progress)
                if resume_validation(advance):continue
                break
            row=snapshot(endpoint);row.update(attempts=attempts,host_seconds=time.perf_counter()-start)
            segments.append(row);progress('cost_observation_segment',**row)
        assert [int(o.frozen.cutoff.cpu()[0]) for o in observations]==[1300,1400,1500,1600,1700,1800,1900]
        assert [r['started'] for r in model_records]==[1300,1414,1528,1642,1756,1870]
        assert sorted(completed_records)==[1300,1400,1500,1600,1700]
        assert senders[1].control.execute_at.cpu().tolist()==[1528] and senders[0].control.execute_at.cpu().tolist()==[1534]
        assert senders[0].receipt is not None and senders[1].receipt is not None
        assert root.run.next_observation.cpu().tolist()==[2000]
        assert any(task.completed_at is not None and task.completed_at.cpu().tolist()==[2084] for task in root.models.tasks)
        assert not clock.public.waiting.received.logical_and(~clock.public.waiting.consumed).any().cpu().item()
        assert all(row['resources_fit'] for row in received_checks if row['ready']==[True])
        # 原独立预算：1742外部2份后本方1微份；1791外部2份后本方10份。
        # 不预设自然批数；先保存真实订单量，再核队列扣减和累计确认。
        (artifact/'cost-observation-state.json').write_text(json.dumps(segments,indent=2)+'\n')
        baseline,micro,positive,received=(segments[i] for i in (2,3,4,5))
        # 本区间无关闭；接纳的GTC matched/pending均为零，不能用接纳前open作耗损基线。
        depletion=lambda row:sum(row['matched_base_units'])+sum(row['pending_base_units'])
        assert depletion(micro)-depletion(baseline)==1,(baseline,micro)
        assert depletion(positive)-depletion(micro)==10_000_000,(micro,positive)
        assert micro['order_occupied']==positive['order_occupied'] and micro['accepted_at']==positive['accepted_at']
        assert sum(micro['open_base_units'])-sum(positive['open_base_units'])==10_000_000,(micro,positive)
        assert sum(received['matched_base_units'])-sum(baseline['matched_base_units'])==10_000_001 and sum(received['pending_base_units'])==0,(baseline,received)
        assert received['platform_prefix']==[7]  # 1850仅有SIM确认；不得为验证提前领取。
        batches=[]
        for batch in clock.public.ledger.columns.projection_batches:
            columns=batch.emitted.nonzero().cpu().tolist()
            if columns:batches.append(dict(rows=[int(batch.rows[r,c].cpu()) for r,c in columns],
                batch_sequences=values(batch.batch_sequences),record_ordinals=values(batch.record_ordinals),
                private_sequences=values(batch.private_sequences),received_at=batch.received_at.cpu().tolist()))
        # 只读原Ledger/NOE成员，定位1791的Maker fill，再关联其原经济列和首次领取。
        from fields import decode_fields
        ledger=clock.public.ledger;columns=ledger.columns;life=ledger.lifecycle;waiting=clock.public.waiting
        positions,position_fault=run(lambda:decode_fields(columns.positions,
            ['scalar','scalar','integer','integer'],torch.ones(columns.positions.size(0),dtype=torch.bool,device=columns.positions.device)))
        assert position_fault.cpu().tolist()==[0]*columns.positions.size(0)
        position_values=[positions[0].cpu().tolist(),positions[1].cpu().tolist(),values(positions[2]),values(positions[3])]
        fills=values(life.fills)
        maker_ids={fills[c] for _,c in ((life.schemas==8)&(life.trade_roles==1)&(life.trade_matched_at==1791)).nonzero().cpu().tolist()}
        assert maker_ids
        economic_members=[];maker_evidence=[];all_rows=[]
        sequences=values(columns.sequences);outcome=amounts(columns.outcomes);balances=amounts(columns.balances)
        for batch in columns.projection_batches:
            batch_sequences=values(batch.batch_sequences);ordinals=values(batch.record_ordinals);private=values(batch.private_sequences)
            for root_index,c in batch.emitted.nonzero().cpu().tolist():
                row_ref=int(batch.rows[root_index,c].cpu());match=(columns.row_refs[root_index]==row_ref).nonzero().flatten().cpu().tolist()
                assert len(match)==1,(row_ref,match)
                column=match[0];i=root_index*batch.rows.size(1)+c
                identity=[int(batch.context_slots[root_index,c].cpu()),int(batch.run_slots[root_index,c].cpu()),batch_sequences[i],ordinals[i]]
                assert [value[column] for value in position_values]==identity
                assert sequences[column]==values(batch.economic_sequences)[i]
                fact=dict(row=row_ref,column=column,position=columns.positions[column].cpu().tolist(),identity=identity,
                    private=private[i],platform_at=int(batch.platform_at[root_index,c].cpu()),received_at=int(batch.received_at[root_index,c].cpu()),
                    first_available_at=int(waiting.available_at[root_index,column].cpu()),ledger_available_at=int(ledger.available_at[root_index,column].cpu()),
                    received=bool(waiting.received[root_index,column].cpu()),consumed=bool(waiting.consumed[root_index,column].cpu()),
                    applied=bool(ledger.applied[root_index,column].cpu()),schema=int(life.schemas[root_index,column].cpu()),
                    fill=fills[column],economic_tag=int(life.economic_tags[root_index,column].cpu()),economic_enabled=bool(columns.enabled[root_index,column].cpu()),
                    sequence=sequences[column],asset_index=int(columns.asset_indices[root_index,column].cpu()),
                    outcome=str(outcome[column]),balances=[str(v) for v in balances[column*8:column*8+8]])
                all_rows.append(fact)
                if fact['fill'] in maker_ids and fact['schema']==8 and int(life.trade_roles[root_index,column].cpu())==1:maker_evidence.append(fact)
                if fact['fill'] in maker_ids and fact['economic_enabled'] and fact['economic_tag']==6:economic_members.append(fact)
        target_token=next(a['token_id'] for a in clock.source.prepared.columns['assets'] if a['outcome']['label']=='Down')
        target_asset=root.directory.tokens.index(target_token)
        ledger_report=dict(target_token=target_token,target_asset_index=target_asset,maker_fill_sequences=sorted(maker_ids),
            economic_records=economic_members,trade_evidence=maker_evidence,all_noe_rows=all_rows,
            baseline_outcomes=baseline['platform_outcomes'],final_outcomes=segments[-1]['platform_outcomes'],
            baseline_balances=baseline['cash'],final_balances=segments[-1]['cash'],receive_checks=received_checks)
        (artifact/'cost-observation-ledger.json').write_text(json.dumps(ledger_report,indent=2)+'\n')
        assert economic_members and maker_evidence
        for fact in economic_members:
            assert fact['asset_index']==target_asset and fact['received'] and fact['consumed'] and fact['applied'],fact
            assert fact['first_available_at']==fact['ledger_available_at'] and 1850<fact['first_available_at']<=2001,fact
        assert sum((Fraction(row['outcome']) for row in economic_members),Fraction())==Fraction(10_000_001,1_000_000)
        for asset,(before,after) in enumerate(zip(baseline['platform_outcomes'],segments[-1]['platform_outcomes'],strict=True)):
            assert Fraction(after)-Fraction(before)==(Fraction(10_000_001,1_000_000) if asset==target_asset else 0)
        # 其余已应用列本区间无现金变动；可直接核这批原经济列与原余额，不另归约账户。
        maker_columns={row['column'] for row in economic_members}
        other=[row for row in all_rows if row['economic_enabled'] and row['applied'] and row['sequence']>baseline['platform_prefix'][0] and row['column'] not in maker_columns]
        assert all(Fraction(row['balances'][0])==0 for row in other),other
        assert Fraction(segments[-1]['cash'][0])-Fraction(baseline['cash'][0])==sum((Fraction(row['balances'][0]) for row in economic_members),Fraction())
        assert any(row['at']==[1870] and row['platform_prefix']==[9] for row in received_checks)
        assert any(row['at']==[1947] and row['platform_prefix']==[10] for row in received_checks)
        torch.cuda.synchronize();whole_seconds=time.perf_counter()-whole_start
    finally:
        clock.handlers=handlers
        for owner,name,old in reversed(restore):setattr(owner,name,old)
    native=Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so'
    report=dict(passed=True,segments=segments,calls=stats,begin_counts=begins,whole_seconds=whole_seconds,
        snapshot_host_seconds=snapshot_seconds,natural_batches=batches,maker_ledger=ledger_report,completed_cutoffs_in_actual_retirement_order=completed_records,requests=[dict(at=int(s.control.execute_at.cpu()[0]),
            input_sequence=values(s.receipt.input_sequence),body=s.requests) for s in senders if s.receipt is not None],
        events=evidence,receive_checks=received_checks,native=dict(path=str(native),sha256=hashlib.sha256(native.read_bytes()).hexdigest()),
        observations=[int(o.frozen.cutoff.cpu()[0]) for o in observations],target_end=False,
        allocated_peak_bytes=torch.cuda.max_memory_allocated(),reserved_peak_bytes=torch.cuda.max_memory_reserved(),
        scope='one original out-of-order-buy Root; no PPO; original callbacks and assertions included; initial Root/material preparation outside, initialize inside; two outer CUDA synchronizations only',
        resource_scope='process allocator peaks since original cold installation reset; includes cold Root and interval, not driver/device-wide peak',
        timing_basis='host calls overlap/nest and are not additive; includes validation, progress I/O and snapshots; Run12 already exited before this run; actual GPU/process snapshot in execution binding; no exclusive GPU attribution or throughput claim')
    (artifact/'cost-observation-report.json').write_text(json.dumps(report,indent=2)+'\n')
    stage('cost_observation_complete',whole_seconds=whole_seconds,segments=len(segments),target_end=False)
    return report


def continuous(source,clock,pm,market,reference,binance,block,sampler,rule,receipt,intents,asks,taker,maker,codec,artifact,stage,*,second_buy=False,assembled,root_runtime=None):
    evidence=[];original_stage=stage;page_changes=0;hidden_pages=[]
    receiver=PublicLedgerReceiver() if root_runtime is None else root_runtime.receiver
    received_checks=[];reward_checks=[]
    reward_case=bool(os.environ.get("B_GPU_ACTOR_INTERVAL_REWARD"))
    finite_goal=source.get('finite_goal')
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
    if root_runtime is None or finite_goal or source.get('propagation_coverage'):
        legs=run(lambda:SubmittedLegs(T([[True,False]],torch.bool),T([[False,False]],torch.bool),T([[0,-1]]),T([[0,1]]),
            rational(['1000.001','0']),T([[True,False]],torch.bool),T([[0,-1]]),T([[0,1]]),T([[True,False]],torch.bool),
            SubmittedBuyTerms(T([[2,0]]),T([[False,False]],torch.bool),rational(['0.5','0']),rational(['500.0005','0']),T([[0,-1]]))))
        legs=run(lambda:bind_submission_receipt(legs,zero,receipt))
        population=SubmittedActorRequests(intents,legs) if root_runtime is None else root_runtime.population
        population.post_intents=intents;population.submitted_legs=legs
        assert not clock.public.registry.events.received.any().cpu().item()
        if root_runtime is None:
            history_targets=run(lambda:CancellationTargetHistory.open(integers([]),empty.bool()))
            clock.public=replace(clock.public,cancellation_target_history=history_targets)
            from actor_runtime import ActorDirectory,ActorRuntime,configure_continuous_execution
            directory=ActorDirectory(tuple(conditions),tuple(tokens),tuple(a['token_id'] for a in clock.source.prepared.columns['assets']),
                tuple(asset_ends),market_indices,assembled.condition_by_asset,block.asset_axis,asks)
            scenario=run(lambda:CancelFailureScenario(empty,integers([]),integers([]),empty.bool(),zero.to(torch.int32)))
            def paths(kind,world,selected):
                width=world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1)
                return T([[0]*width])
            execution=configure_continuous_execution(clock,directory,population,rule,codec,sampler,taker,maker,paths,scenario)
            clock.source.states=(pm,market,reference,binance)
            root_runtime=ActorRuntime(clock,directory,population,receiver,{},execution)
    else:population=root_runtime.population
    if two and 'cold_authority' not in source:
        from maker_days import MakerDays
        from rebate_coordinator import RebateCoordinator
        clock.runtime=replace(clock.runtime,calendars=run(lambda:RebateCoordinator(
            MakerDays.empty(condition_slots[None,:],T([[301000,601000]]),yes[:,None].expand(1,2)),clock.runtime.calendars.taker)))
    auto_rows=[]
    if os.environ.get('B_GPU_AUTO_LOOP'):
        from validate_auto_device_loop import observe as observe_auto
        auto_rows=observe_auto(root_runtime.execution,artifact/'auto',resources=False)
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
    template=Policy100ms(ModelShape100ms(2,64),propagation=(dict(horizon_ms=100,eta=1.)
        if os.environ.get("B_GPU_PROPAGATION") else None)).cuda().eval();policies={}
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
    if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL'):policies[1800]=policies[1300]
    if two:
        for at in range(1900+offset,2800+offset,100):
            policy=copy.deepcopy(policies[1800+offset])
            if at==2000+offset:
                with torch.no_grad():policy.action.actor.family[-1].bias[2]=40.;policy.action.actor.family[-1].bias[0]=0.
            policies[at]=policy
    if two and 'cold_authority' in source:
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
    warm_steps=0;opened=False;last_model_random=None
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
        if accepted.any().cpu().item():
            observations.append(observed)
            if model_root.reward_memory is not None and not reward_checks:
                memory=model_root.reward_memory
                nav,exposure=run(lambda:model_root.reward_reads(world.public))
                reward_checks.append(dict(at=now,E=str(amounts(exposure.amount)[0]),WCM=str(amounts(nav.amounts[0])[0]),
                    area='0',known=bool(exposure.known.cpu()[0]),fault=int(memory.fault.cpu()[0]),
                    prefix=values(world.public.ledger.platform.platform_source_prefix)[0]))
        return observed,accepted
    def policy_start(observation,enabled):
        at=int(observation.frozen.cutoff.cpu()[0]);bound.append(at)
        return policies[at],zero+at
    def receive(public,delivery,enabled):
        public,ready=root_receive(public,delivery,enabled)
        if model_root.reward_memory is not None:
            memory=model_root.reward_memory
            nav,exposure=run(lambda:model_root.reward_reads(public))
            entry=dict(at=int(clock.gates.public_visible_at.cpu()[0]),E=str(amounts(exposure.amount)[0]),
                WCM=str(amounts(nav.amounts[0])[0]),area=str(amounts(memory.area_pusd_ms)[0]),
                known=bool(exposure.known.cpu()[0]),fault=int(memory.fault.cpu()[0]),
                prefix=values(public.ledger.platform.platform_source_prefix)[0])
            from rebate_read import read_confirmed_rebates
            conditions=public.ledger.rebate_days.condition_slots
            rebates=run(lambda:read_confirmed_rebates(public.ledger,conditions,torch.ones_like(conditions,dtype=torch.bool),yes))
            entry.update(cash=str(amounts(public.ledger.platform.balances)[0]),
                confirmed_unpaid=str(sum(amounts(rebates.confirmed_unpaid),Fraction())),
                paid=str(sum(amounts(rebates.paid),Fraction())))
            previous=reward_checks[-1] if reward_checks else dict(at=int(memory.opened_at_ms.cpu()[0]),
                E=str(amounts(memory.exposure)[0]),area='0')
            expected=Fraction(previous['area'])+Fraction(previous['E'])*(entry['at']-previous['at'])
            assert Fraction(entry['area'])==expected,(previous,entry,expected)
            assert entry['known'] and entry['fault']==0
            reward_checks.append(entry);stage('actual_reward_receive',**entry)
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
                    with torch.no_grad():state,hidden,value=task.policy.state_and_value(*selected.model_inputs(),expected_hidden[:,market_row:market_row+1],value_context=None)
                    torch.testing.assert_close(task.evaluation.hidden[:,row:row+1],hidden)
                    torch.testing.assert_close(task.evaluation.state[row:row+1],state)
                    if market_row==1 and role==0:warm_steps+=1
                    if market_row==1 and role==1 and not opened:
                        assert warm_steps==(300 if full_warmup else 7) and expected_hidden[:,market_row:market_row+1].abs().sum().cpu().item()>0
                        opened=True
                    expected_hidden[:,market_row:market_row+1]=hidden
        else:
            from model_train.gpu_policy import finite_value_context
            context=None if models.goal_until is None else finite_value_context(observation.frozen.cutoff,
                models.goal_until[observation.row_roots],models.trading_until[observation.row_roots])
            with torch.no_grad():expected=task.policy.frozen_state_and_value(observation.frozen,expected_hidden,
                identities,task.evaluation.policy_version,value_context=context)
            torch.testing.assert_close(task.evaluation.state,expected.state)
            if context is None:assert task.evaluation.value is None
            else:
                torch.testing.assert_close(task.evaluation.value,expected.value)
                stage('finite_value_at_frozen_cutoff',cutoff=at,started=int(task.started_at.cpu()[0]),
                    context=context.cpu().tolist(),value=task.evaluation.value.cpu().tolist())
            torch.testing.assert_close(task.evaluation.hidden,expected.hidden)
            expected_hidden.copy_(expected.hidden)
        model_records.append(dict(cutoff=at,started=int(task.started_at.cpu()[0]),completed=task.completed_at.cpu().tolist(),
            version=task.evaluation.policy_version.cpu().tolist(),observation=weakref.ref(original)))
    def model_completed(task):
        nonlocal last_model_random
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
        if original["cutoff"]==1800:last_model_random=random.counter.clone()
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
        if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL') and at==1800:duration=87_000_009-1870
        if full_warmup and at!=1900+offset:duration=20
        return zero+duration,zero+(20 if reward_case else 120) if at==1300+offset else zero
    from model_train.continuous_root import ContinuousActorRoot
    learning_context=bool(os.environ.get('B_GPU_ACTOR_LOGICAL_CONTEXT'))
    learning_gap=bool(os.environ.get('B_GPU_ACTOR_LOGICAL_GAP'))
    gap_trade=301500 if os.environ.get('B_GPU_ACTOR_LOGICAL_GAP_CUTOFF') else 301600
    model_root=ContinuousActorRoot(root_runtime,codec,rule,sampler,started_at=start,first_observation=T([first_observation]),
        trading_until=T([source['auto_two_market']['trading_until']]) if source.get('auto_two_market') else T([1301]) if source.get('propagation_coverage') else T([gap_trade]) if learning_gap else T([303000]) if learning_context else T([2000]) if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") and not two else None,
        goal_until=T([303000]) if learning_gap else T([303000]) if learning_context else T([finite_goal["goal_until"] if finite_goal else 3000]) if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") and not two else None,
        request_namespace=source['run_id']+'/'+source['session_id'],policy_at_start=policy_start,timing=timing,
        random=random,model_shape=template.shape,word_capacity=16,path_capacity=32,
        reward_a_pusd=rational(["0.02"]) if reward_case else None)
    initial_request=not two
    if initial_request:model_root.request_directory.append(source['request_id'])
    if source.get('propagation_coverage'):
        return propagation_coverage_case(model_root,artifact,original_stage)
    if os.environ.get('B_GPU_ACTOR_JOINT_ROOTS'):
        return logical_two_roots_case(model_root,policies[1300],source,artifact,original_stage)
    if learning_gap:
        return logical_gap_case(model_root,policies[first_observation],source,artifact,original_stage)
    if learning_context:
        return logical_context_case(model_root,policies[first_observation],source,artifact,original_stage)
    if os.environ.get('B_GPU_ACTOR_LOGICAL_CUTOFF'):
        return logical_cutoff_case(model_root,policies[1300],source,artifact,original_stage)
    if os.environ.get('B_GPU_ACTOR_LOGICAL_BATCH'):
        return logical_batch_case(model_root,policies[1300],source,artifact,original_stage)
    root_receive=model_root.receive;model_root.receive=receive
    material=model_root.inputs;models=model_root.models;shared=model_root.run
    history=material.history;encoder=material.encoder
    request_directory=model_root.request_directory;task_requests=model_root.task_requests
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
    def resume_validation(result):
        nonlocal continuations,history_retry,resource_candidate,end_retry
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
            continuations+=1;return True
        assert result.fault.cpu().tolist()==[0],result.fault.cpu().tolist()
        if result.complete.cpu().item():return False
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
            return True
        required_requests=max([clock.runtime.requests.capacity]+[int(s.receipt.required_requests.max().cpu()) for s in senders if s.receipt is not None])
        required_legs=max([clock.public.ledger.reservations.capacity]+[int(s.publication.reservation.decision.required_capacity.max().cpu())
            for s in senders if s.publication.reservation is not None])
        old=clock.public.ledger
        if required_legs>old.reservations.capacity or required_requests>clock.runtime.requests.capacity:
            stage('continuous_capacity_requirement',requests=clock.runtime.requests.capacity,required_requests=required_requests,
                legs=old.reservations.capacity,required_legs=required_legs)
            assert model_root.resume_resources(result)
            continuations+=1
            return True
        if model_root.resume_resources(result):
            continuations+=1;return True
        frozen=observations[-1] if observations else None
        if frozen is not None:stage('blocked_unknown',fault=frozen.frozen.fault.cpu().tolist(),required=frozen.market.required.cpu().tolist())
        raise AssertionError('真实连续路径未完成且尚无已报告物理需求')
    if os.environ.get('B_GPU_ACTOR_COST_OBSERVATION'):
        return cost_observation_case(model_root,artifact,stage,observations,senders,model_records,completed_records,evidence,received_checks,resume_validation)
    while True:
        result=shared.advance_before(T([302201 if gap else (2801+offset if two else 1901)]),yes,progress=stage)
        if resume_validation(result):continue
        break
    real_tasks=len(observations)
    assert len(request_directory.keys)==real_tasks+int(initial_request)
    assert request_directory.keys[int(initial_request):]==[
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
        if source.get('auto_two_market'):
            from validate_auto_device_loop import two_market_tail
            result['auto_tail']=two_market_tail(model_root,auto_rows,source['auto_two_market'],stage)
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
    next_public=clock.source.next()
    if finite_goal:
        assert not next_public.eof.cpu().item() and next_public.at.cpu().tolist()==[finite_goal['terminal_admitted_at']]
    else:assert next_public.eof.cpu().item()
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
    expected_finish=87_000_009 if os.environ.get("B_GPU_ACTOR_SAME_TICK_TAIL") else (2084 if os.environ.get("B_GPU_ACTOR_WINDOW_TAIL") else 1984)
    assert any(task.completed_at is not None and task.completed_at.cpu().tolist()==[expected_finish] for task in pending_models)
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
    result=dict(passed=True,scope=f'same runtime/account C1300→C1400 queued→c1414 before s{1434 if reward_case else 1534}→new Pair processed→public C1700→Pair cancel→C1900 original frozen/inflight→actual cancel visibility→current Ledger',
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
    def export_current():
        rows=[]
        for batch in clock.public.ledger.columns.projection_batches:
            for column,on in enumerate(batch.emitted.cpu().tolist()[0]):
                if on:
                    row=int(batch.rows[0,column].cpu())
                    rows.append(dict(row=row,batch=values(batch.batch_sequences)[column],ordinal=values(batch.record_ordinals)[column],
                        private=values(batch.private_sequences)[column],received=int(batch.received_at[0,column].cpu()),
                        semantic=clock.projection.normalizer.bank.semantic[row:row+1].cpu().tolist()))
        actual_funds=run(clock.public.ledger.funds)
        (artifact/'actual-continuous-values.json').write_text(json.dumps(dict(noe=rows,
            post_results=stored_actual(clock.runtime.post_results.values),cancel_results=stored_actual(clock.runtime.cancel_results.groups),
            ledger=dict(entry=values(clock.public.ledger.current_entry_sequence),local=values(clock.public.ledger.current_local_prefix),
                platform_prefix=values(clock.public.ledger.platform.platform_source_prefix),balances=[str(x) for x in amounts(clock.public.ledger.platform.balances)],
                funds={name:[str(x) for x in amounts(getattr(actual_funds,name))] for name in ('actual_available','maker_available','actual_reserved','maker_reserved')}),
            evidence=evidence,receive_checks=received_checks,reward_checks=reward_checks,
            scope='raw production outputs for independent CPU posterior'),indent=2)+'\n')
    export_current()
    (artifact/'actual-continuous-requests.json').write_text(json.dumps(result['requests'],indent=2)+'\n')
    if os.environ.get('B_GPU_ACTOR_SAME_TICK_TAIL'):
        same_tick_boundaries=[]
        # C1800在1870已开始；有限长计算跨截止，finish=Sk且dispatch=0。
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
        assert [row['cutoff'] for row in model_records]==[1300,1400,1500,1600,1700,1800]
        assert sorted(completed_records)==[1300,1400,1500,1600,1700,1800]  # 完整人口；退役顺序保真实发送时刻。
        assert torch.equal(random.counter,last_model_random)  # C1800完成后C1900不调用策略或抽动作。
        assert models.last_completed_at.cpu().tolist()==[2084]
        result['unstarted_cutoffs_retired']=[1900]
        result['first_accepted_model_finish_after_window']=2084
        result['last_accepted_model_finish_after_window']=max(row['completed'][0] for row in model_records)
        # 原CPU FOK参考给应收，原日规则给付款；金额不从GPU付款后态生成。
        cpu_accrued=Fraction(source['received_ledger']['projection']['platform_confirmed_projection']['taker_rebate_receivable_pusd'])
        assert cpu_accrued==Fraction('1.4000016')
        payable=Fraction((cpu_accrued*10000).numerator//(cpu_accrued*10000).denominator,10000)
        pre_day=clock.public.ledger.platform.balances
        assert amounts(pre_day)[5]==cpu_accrued
        pre_cash=amounts(pre_day)[0];pre_entry=values(clock.public.ledger.current_entry_sequence)
        reward_endpoints=[]
        def endpoint(at):
            while True:
                advance,score=model_root.read_interval_reward(T([at]),yes,progress=stage)
                assert advance.fault.cpu().tolist()==[0]
                if advance.complete.all().cpu().item():break
                assert model_root.resume_resources(advance)
            assert score is not None and score.present.cpu().tolist()==score.known.cpu().tolist()==[True]
            assert score.fault.cpu().tolist()==[0]
            memory=model_root.reward_memory
            nav,_=run(lambda:model_root.reward_reads(clock.public))
            prior=reward_checks[-1]
            area=Fraction(prior['area'])+Fraction(prior['E'])*(at-prior['at'])
            expected=5*(amounts(nav.amounts[0])[0]-amounts(memory.initial_nav)[0]-Fraction(1,50)*area/6_000_000)
            assert amounts(score.exact)==[expected]
            assert score.dt_seconds.cpu().tolist()==[torch.tensor((at-1300)/1000,dtype=score.dt_seconds.dtype).item()]
            saved=stored_actual(memory);entry=values(clock.public.ledger.current_entry_sequence)
            again,repeated=model_root.read_interval_reward(T([at]),yes,progress=stage)
            assert again.complete.all().cpu().item() and stored_actual(model_root.reward_memory)==saved
            assert stored_actual(repeated)==stored_actual(score)
            assert values(clock.public.ledger.current_entry_sequence)==entry
            reward_endpoints.append(dict(at=at,training_score=str(expected),WCM=str(amounts(nav.amounts[0])[0]),
                area_pusd_ms=str(area),dt_seconds=score.dt_seconds.cpu().tolist(),known=True,fault=0))
            return advance
        if reward_case:
            assert model_root.reward_memory.opened_at_ms.cpu().tolist()==[1300]
            witness=next(row for row in reward_checks if row['at']==1434)
            before=next(row for row in reward_checks if row['at']==1400)
            assert Fraction(witness['E'])!=Fraction(before['E']),(before,witness)
            # 原CPU确认持仓的唯一腿差；不是从训练分倒推暴露。
            balances=source['received_ledger']['projection']['platform_confirmed_projection']['outcome_balances']
            assert Fraction(witness['E'])==sum((abs(Fraction(row['balance_shares'])) for row in balances),Fraction())
            endpoint(3000)  # 目标配置不封口；原87000000付款仍未发生。
        else:assert model_root.reward_memory is None
        before_day=endpoint(87_000_000) if reward_case else shared.advance_before(T([87_000_000]),yes,progress=stage)
        assert before_day.complete.cpu().tolist()==[True] and before_day.fault.cpu().tolist()==[0]
        if finite_goal:
            # Terminal/Auto已由同一原时钟在日成熟之前消费，现金只增加实际赢家持仓。
            winner=sum((Fraction(row['balance_shares']) for row in source['received_ledger']['projection']['platform_confirmed_projection']['outcome_balances'] if row['token_id']=='1'),Fraction())
            assert amounts(clock.public.ledger.platform.balances)[0]==pre_cash+winner
            assert amounts(clock.public.ledger.platform.outcomes)==[Fraction(),Fraction()]
            assert clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[True]]
        else:
            assert amounts(clock.public.ledger.platform.balances)[0]==pre_cash
            assert values(clock.public.ledger.current_entry_sequence)==pre_entry
        after_day=endpoint(87_010_000) if reward_case else shared.advance_before(T([87_010_000]),yes,progress=stage)
        export_current()  # 先保全真实目标后缀；校验失败不再丢失确认/付款正文与领取位置。
        if reward_case:
            if finite_goal:
                # 同次原Receiver可以连续闭合确认与付款，Root只观察返回的公开前缀。
                before=next(row for row in reward_checks if row['at']==87_000_010)
                paid=next(row for row in reward_checks if Fraction(row.get('paid','0'))==payable)
                waiting=next(row for row in received_checks if row['at']==[87_000_010])
                assert before['prefix']==8 and paid['prefix']==10
                assert any(row['sequence']==10 and row['first_available_at']==87_000_010 for row in waiting['waiting'])
                assert Fraction(before['paid'])==Fraction(before['confirmed_unpaid'])==0
                assert Fraction(paid['WCM'])-Fraction(before['WCM'])==payable
                assert Fraction(paid['cash'])-Fraction(before['cash'])==payable
                assert Fraction(paid['confirmed_unpaid'])==0 and Fraction(paid['paid'])==payable
                confirmation=dict(previous_prefix=before,closed_prefix=paid,
                    same_receive_closes_confirmation_and_payment=True)
            else:
                confirmed=next(row for row in reward_checks if Fraction(row.get('confirmed_unpaid','0'))==payable)
                paid=next(row for row in reward_checks if Fraction(row.get('paid','0'))==payable)
                before=reward_checks[reward_checks.index(confirmed)-1]
                assert Fraction(confirmed['WCM'])-Fraction(before['WCM'])==payable
                assert confirmed['WCM']==paid['WCM']
                assert Fraction(confirmed['confirmed_unpaid'])==payable and Fraction(paid['confirmed_unpaid'])==0
                assert Fraction(paid['paid'])-Fraction(confirmed['paid'])==payable
                assert Fraction(paid['cash'])-Fraction(confirmed['cash'])==payable
                confirmation=dict(confirmed_prefix=confirmed,paid_prefix=paid,payment_preserves_WCM=True)
            # 收益项不重计；训练分仍扣除按真实公开E积分的时间成本。
            result['economic_interval']=dict(a_pusd='1/50',opened_at=1300,receive_checks=reward_checks,
                endpoints=reward_endpoints,send_between_observations=witness,**confirmation,
                repeated_endpoint_pure=True,goal_does_not_seal_income=True)
            stage('actual_root_economic_interval',**result['economic_interval'])
        assert after_day.complete.cpu().tolist()==[True] and after_day.fault.cpu().tolist()==[0]
        paid_balance=amounts(clock.public.ledger.platform.balances)
        assert paid_balance[0]==pre_cash+payable+(winner if finite_goal else 0) and paid_balance[5]==0 and paid_balance[7]==cpu_accrued-payable
        if not finite_goal:assert values(clock.public.ledger.current_entry_sequence)==[pre_entry[0]+4]
        if finite_goal:
            assert clock.source.next().eof.cpu().item()
            assert reward_endpoints[-1]['at']==finite_goal['goal_until']
            assert Fraction(reward_checks[-1]['E'])==0
            result['finite_goal']=dict(finite_goal,terminal_facts_count=len(finite_goal['terminal_facts']),
                auto_cash_pusd=str(winner),goal_training_score=reward_endpoints[-1]['training_score'],
                actual_final_prefix=values(clock.public.ledger.platform.platform_source_prefix),
                actual_goal_ledger_entry=values(clock.public.ledger.current_entry_sequence),
                no_tail_observations=True,complete_target_posterior_pending=True)
            del result['finite_goal']['terminal_facts']
            export_current()  # 完整目标后缀，原pre-tail人口不冒充最终后验。
            stage('finite_goal_endpoint',**result['finite_goal'])
        assert shared.next_observation.cpu().tolist()==[2000]
        assert not (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item()
        result['taker_day_at_utc_ms']=87_000_000;result['taker_paid_pusd']=str(payable)
        result['taker_locked_pusd']=str(cpu_accrued-payable)
        result['elapsed_seconds']=time.perf_counter()-started
    if os.environ.get('B_GPU_ACTOR_WINDOW_TAIL') and not two:
        assert [row['cutoff'] for row in model_records]==[1300,1400,1500,1600,1700,1800]
        assert not models.tasks
        never_started=weakref.ref(observations[-1])
    partial=verify_partial_queue_retirement(observations[0],policies[1300])
    del sender,pending_models
    result['task_retirement']=dict(released_refs(),partial_roots=partial)
    if os.environ.get('B_GPU_ACTOR_WINDOW_TAIL') and not two:
        assert never_started() is None
        result['task_retirement']['unstarted_frozen_released']=True
        result['task_retirement']['unstarted_cutoffs']=[1900]
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


def logical_two_roots_case(first,policy,source,artifact,stage):
    """两次真实冷开户、各根合法前沿、统一全人口PPO；有限工程代表。"""
    from model_train.logical_batch import JointLearner,LogicalBatchConfig
    from model_train.continuous_root import ContinuousActorRoot
    from model_train.dt_advantages import normalize_advantages
    from actor_runtime import open_continuous_runtime
    from actor_opening import ActorOpeningCapacities
    from public_device_page import PreparedPublicBlocks
    from tensordict import TensorDict
    rule_path=ROOT/'sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json'
    second_artifact=artifact/'root-b';second_artifact.mkdir()
    request=dict(log_path=str(second_artifact/'cold-session.log'),noe_root=str(second_artifact/'cold-noe'),
        rule_path=str(rule_path),rule_sha256=hashlib.sha256(rule_path.read_bytes()).hexdigest(),
        rules_as_of_utc_ms=2**63-1,implementation_identity=json.loads(rule_path.read_text())['provenance']['implementation_identity'],
        source_sha256=hashlib.sha256((artifact/'public-compact/manifest.json').read_bytes()).hexdigest(),
        finality_policy=dict(schema_version='trade_finality_policy_v2',normal_path=source['finality_path'],failure_overrides=[]),
        activation_id=source['run_id']+'-root-b-activate',session=dict(run_id=source['run_id']+'-root-b',
            session_id=source['session_id']+'-root-b',account_id='joint-root-b-account',caller_role='MODEL_TRAIN',
            caller_instance_id='joint-root-b-caller',creation_request_id='joint-root-b-create',
            response_bridge_binding_id='joint-root-b-bridge',initial_account_state=dict(available_pusd='2000'),
            session_seed=source['session_seed']+'-root-b'))
    sampler=type(first.sampler).build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
        source['profile_sha256'],[request['session']['session_seed'].encode('ascii')])
    public=PreparedPublicBlocks(artifact/'public-page',artifact/'public-compact')
    hidden=PreparedHiddenPages(artifact/'hidden-pages',source['bounded_hidden']['source_binding'])
    scenario=run(lambda:CancelFailureScenario(T([[]]),integers([]),integers([]),T([[]],torch.bool),T([0]).to(torch.int32)))
    def paths(kind,world,selected):return T([[0]*(world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1))])
    taker,maker=run(lambda:(rational([source['taker_rebate_rate']]),rational([source['maker_rebate_rate']])))
    second_runtime=open_continuous_runtime(public,hidden,first.codec,request,first.rule,sampler,
        capacities=ActorOpeningCapacities(*([1]*13)),page_groups=3,hidden_page_buckets=1,
        taker_rate=taker,maker_rate=maker,fill_paths=paths,cancel_scenario=scenario,
        gtd=ScheduledTime.absent(T([0])),auto_redemption=True)
    (second_artifact/'cold-request.json').write_text(json.dumps(request,indent=2)+'\n')
    (second_artifact/'cold-authority.json').write_text(json.dumps(second_runtime.authority,indent=2)+'\n')
    b_goal=source['maker_page_material']['b_goal_until'] if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT') else 87010000
    second=ContinuousActorRoot(second_runtime,first.codec,first.rule,sampler,started_at=T([-29000]),
        first_observation=T([1300]),trading_until=T([2000]),goal_until=T([b_goal]),
        request_namespace=request['session']['run_id']+'/'+request['session']['session_id'],
        policy_at_start=lambda observation,active:(policy,T([0])),
        timing=lambda observation:(T([214 if int(observation.account.cutoff[0])==1800 else 114]),T([0])),
        random=ActionRandom(T([181607]),T([[0,0,0,0]])),model_shape=policy.shape,word_capacity=16,path_capacity=32,
        reward_a_pusd=rational(['0.02']))
    if os.environ.get('B_GPU_PUBLIC_STAGE_APPEND'):
        from validate_public_append import observe as observe_append
        observe_append(first.clock,artifact/'public-append-a',resources=True)
        observe_append(second.clock,artifact/'public-append-b')
    with torch.no_grad():
        policy.action.actor.family[-1].bias[2]=6.
        policy.action.actor.variant.bias.copy_(T([2,0]).float())
        policy.action.actor.price.bias[38]=6.
    config_a=LogicalBatchConfig(2,1,1,1,.2,.5,0.)
    config_b=replace(config_a,decisions=6)
    optimizer=torch.optim.AdamW(policy.parameters(),lr=.0001)
    joint=JointLearner(policy,optimizer,config_a,T([0]))
    tracking=bool(os.environ.get('B_GPU_MLFLOW'))
    if tracking:
        from mlflow import MlflowClient
        from model_train.training_report import MLflowReport,ReportWriteError
        tracking_uri='sqlite:///'+str(artifact/'mlflow.db')
        tracking_client=MlflowClient(tracking_uri=tracking_uri)
        experiment=tracking_client.create_experiment('issue-181-real-joint-engineering',
            artifact_location=(artifact/'mlflow-artifacts').as_uri())
        mlflow_run=tracking_client.create_run(experiment).info.run_id
        actual_binding=json.loads(Path(os.environ['B_GPU_CHECKPOINT_BINDING']).read_text())
        joint.reporter=MLflowReport(tracking_uri,mlflow_run,dict(formal_training=False,
            latency_profile_sha256=source['profile_sha256'],candidate_tree=actual_binding['tree'],
            versions={name:__import__('importlib.metadata',fromlist=['version']).version(name) for name in ('torch','torchrl','mlflow')},
            source_manifest_sha256=hashlib.sha256((artifact/'public-compact/manifest.json').read_bytes()).hexdigest(),
            preparation=json.loads((artifact/'source-preparation.json').read_text()),
            actual_native=actual_binding['binaries'],loss_scope='joint full-decision population; arithmetic mean over actual epochs'))
        (artifact/'mlflow-config.json').write_text(json.dumps(dict(tracking_uri=tracking_uri,run_id=mlflow_run),indent=2)+'\n')
    a=joint.add_root(first,config_a);b=joint.add_root(second,config_b)
    first.models.timing=lambda observation:(T([20]),T([400 if int(observation.account.cutoff[0])==1300 else 0]))
    roots=(first,second);batches=(a,b);starts=[[],[]];refs=[[],[]];published=[[],[]];sent=[[],[]];completions=[[],[]]
    if os.environ.get('B_GPU_PUBLIC_TRADE_DIAGNOSTIC'):
        from projection import Projection
        original_finish=Projection._finish
        def plain(value):
            if isinstance(value,torch.Tensor):return value.detach().cpu().tolist()
            if is_dataclass(value):return {f.name:plain(getattr(value,f.name)) for f in fields(value)}
            if isinstance(value,dict):return {k:plain(v) for k,v in value.items()}
            if isinstance(value,(tuple,list)):return [plain(v) for v in value]
            if isinstance(value,frozenset):return sorted(value)
            return value
        def finish(self,emitter,records,source_fault,**kwargs):
            result=original_finish(self,emitter,records,source_fault,**kwargs)
            if not Numbers.device_batch_active() and bool(result[1].fault.any().cpu()):
                path=artifact/'projection-fault.json'
                if not path.exists():
                    path.write_text(json.dumps(dict(binding={name:plain(getattr(self,name)) for name in
                        ('account_slots','condition_slots','token_slots','asset_indices','asset_roots','order_sequence')},
                        source_fault=plain(source_fault),emitter=plain(emitter),records=[dict(mapped=plain(mapped),kind=kind,
                            private=plain(private),batch=plain(batch),received=plain(received),category=plain(category),
                            platform=plain(platform),wanted=plain(wanted))
                            for mapped,kind,private,batch,received,category,platform,wanted in records],
                        normalizer_before=plain(self.normalizer),normalizer_after=plain(result[0].normalizer),
                        result=plain(result[1])),indent=2)+'\n')
                    stage('projection_fault_diagnostic_saved',path=str(path),fault=result[1].fault.cpu().tolist())
            return result
        Projection._finish=finish
    if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT'):
        from validate_maker_projection import observe
        for index,root in enumerate(roots):
            directory=artifact/('maker-root-'+str(index));directory.mkdir()
            observe(root.runtime.execution,directory)
    callback_probes=[]
    if os.environ.get('B_GPU_HIDDEN_REFERENCE_LOOP'):
        # 用真实冻结/任务执行跨UTC四回调的原时间用途；不重放模型或修改时延。
        import ast
        from validate_cross_utc_actor import observation_time,strategy_callbacks
        tree=ast.parse((Path(__file__).parent/'validate_cross_utc_actor.py').read_text())
        run_node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run_actor')
        functions={n.name:n for n in run_node.body if isinstance(n,ast.FunctionDef)}
        for _ in roots:
            namespace=dict(hashlib=hashlib,observation_time=observation_time,pending={})
            nodes=[functions['frozen_digest']]
            for name,original in [('probe_start','on_started'),('probe_complete','on_completed')]:
                time_row=next(i for i,n in enumerate(functions[original].body)
                    if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='at' for t in n.targets))
                statements=functions[original].body[time_row:time_row+2]
                prefix=ast.parse('frozen=task.observation.frozen').body if name=='probe_start' else []
                fn=ast.FunctionDef(name=name,args=ast.arguments(posonlyargs=[],args=[ast.arg(arg='task')],
                    kwonlyargs=[],kw_defaults=[],defaults=[]),body=prefix+statements+ast.parse('return at').body,
                    decorator_list=[])
                nodes.append(fn)
            exec(compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),
                'actual_cross_utc_callback_time_statements','exec'),namespace)
            callback_probes.append(namespace)
    (artifact/"joint-runtime-config.json").write_text(json.dumps(dict(
        a=dict(trading_until=2000,goal_until=87010000,duration_ms=20,dispatch_ms={"1300":400,"other":0}),
        b=dict(trading_until=2000,goal_until=b_goal,duration_ms={"1800":214,"other":114},dispatch_ms=0),
        config_a=vars(config_a),config_b=vars(config_b),propagation=policy.propagation),indent=2)+"\n")
    for index,(root,batch) in enumerate(zip(roots,batches,strict=True)):
        old_started=batch.started;old_published=batch.published
        def started(task,h_in,index=index,root=root,batch=batch,old_started=old_started):
            from model_train.gpu_policy import finite_value_context
            frozen=task.observation.frozen
            with torch.no_grad():expected=policy.frozen_state_and_value(frozen,h_in,frozen.identities,
                task.evaluation.policy_version,value_context=finite_value_context(frozen.cutoff,
                    root.goal_until[task.observation.row_roots],root.trading_until[task.observation.row_roots]))
            torch.testing.assert_close(expected.hidden,task.evaluation.hidden)
            torch.testing.assert_close(expected.value,task.evaluation.value)
            old_started(task,h_in);refs[index].append(weakref.ref(batch.states[-1]))
            if os.environ.get('B_GPU_HIDDEN_REFERENCE_LOOP'):
                from validate_cross_utc_actor import observation_time
                observation=task.observation
                assert observation_time(observation)==int(observation.cutoff.observed_at[0])
                assert torch.equal(observation.account.cutoff[observation.row_roots],observation.frozen.cutoff)
                config=dict(buy_cutoffs=[1300],durations={'1300':114},calculation_ms=20,dispatch_ms={'1300':400})
                lookup={0:'NoTrade',2:'buy'}
                choose,time=strategy_callbacks(config,lookup,root.zero)
                at=callback_probes[index]['probe_start'](task)
                selected,version=choose(observation,root.yes);calculation,dispatch=time(observation)
                assert selected==lookup[2 if at==1300 else 0] and version.cpu().tolist()==[1]
                assert calculation.cpu().tolist()==[114 if at==1300 else 20]
                assert dispatch.cpu().tolist()==[400 if at==1300 else 0]
                stage('cross_utc_callback_time_checked',root=index,cutoff=at,started_at=task.started_at.cpu().tolist(),
                    policy=selected,calculation=calculation.cpu().tolist(),dispatch=dispatch.cpu().tolist())
            starts[index].append(dict(cutoff=int(task.observation.account.cutoff[0]),at=int(task.started_at[0]),
                version=task.evaluation.policy_version.cpu().tolist()))
        def publication(task,accepted,index=index,old_published=old_published):
            old_published(task,accepted)
            completions[index].append(dict(cutoff=int(task.observation.account.cutoff[0]),at=int(task.completed_at[0])))
            if task.sender is not None:published[index].append(stored_actual(task.sender.publication.candidate))
        def completed(task,index=index):
            if callback_probes:
                at=callback_probes[index]['probe_complete'](task)
                stage('cross_utc_completion_key_checked',root=index,cutoff=at,send_at=task.send_at.cpu().tolist())
            if task.sender is not None:sent[index].append(dict(cutoff=int(task.observation.account.cutoff[0]),
                at=int(task.send_at[0]),path=stored_actual(task.sender.publication.candidate),
                request_contexts=stored_actual(task.request_identity.contexts)))
        batch.started=started;batch.published=publication;root.models.on_completed=completed
    def collect(root):
        while True:
            result=root.collect_learning_batch(T([2001 if root is first else 2085]),root.yes,
                progress=lambda name,**row:stage(name,root='a' if root is first else 'b',**row))
            if result is None or result.learning_boundary or result.complete.all().cpu().item():return result
            assert root.resume_resources(result),stored_actual(result)
    def prepare(root,target_end=False):
        label="a" if root is first else "b"
        stage("joint_prepare_begin",root=label,target_end=target_end)
        while True:
            advance,prepared=root.prepare_learning_batch(root.yes,target_end=target_end,
                progress=lambda name,**row:stage(name,root='a' if root is first else 'b',**row))
            if prepared is not None:
                stage("joint_prepare_end",root=label,target_end=target_end,decisions=root.learning.pending_count)
                return prepared
            assert advance is not None and root.resume_resources(advance),stored_actual(advance)
    assert collect(first).learning_boundary
    assert a.pending_count==2 and int(first.clock.gates.public_visible_at[0])==1500
    old_task=next(t for t in first.models.tasks if int(t.observation.account.cutoff[0])==1300)
    old_sender=old_task.sender;old_path=stored_actual(old_sender.publication.candidate)
    sender_ref=weakref.ref(old_sender)
    a_hidden=first.models.hidden.clone();a_rng=first.random.counter.clone()
    a_public=stored_actual(first.clock.public.ledger);a_position=first.clock.source.position
    initial_obligations=second.clock.runtime.resolution_obligations
    initial_fill_capacity=second.clock.runtime.fills.capacity
    initial_bindings=stored_actual(initial_obligations)
    second_collection=collect(second)
    assert second_collection.complete.all().cpu().item() and b.pending_count==6
    assert [(s["cutoff"],s["at"]) for s in starts[1]]==[(1300,1300),(1400,1414),(1500,1528),(1600,1642),(1700,1756),(1800,1870)]
    assert completions[1][-1]==dict(cutoff=1800,at=2084)
    assert not second.models.eligible_starts() and not second.models.tasks
    grown=second.clock.runtime.resolution_obligations
    assert second.clock.runtime.fills.capacity>initial_fill_capacity
    assert grown.pending.size(2)==second.clock.runtime.fills.capacity
    torch.testing.assert_close(grown.pending[:,:,:initial_fill_capacity],initial_obligations.pending)
    torch.testing.assert_close(grown.reference_indices,initial_obligations.reference_indices)
    assert stored_actual(initial_obligations)==initial_bindings
    stage("joint_fill_growth",old_capacity=initial_fill_capacity,new_capacity=second.clock.runtime.fills.capacity,
        obligation_capacity=grown.pending.size(2),old_bindings_unchanged=True)
    prepare(first)
    if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT'):
        while True:
            advance,point=second.read_interval_reward(T([source['maker_page_material']['maker_maturity']]),second.yes,
                progress=lambda name,**row:stage(name,root='b',**row))
            if point is not None:break
            assert second.resume_resources(advance)
        assert second.clock.runtime.calendars.maker.occupied.any().cpu().item()
        assert not second.clock.runtime.calendars.maker.mature.any().cpu().item()
        assert not second.clock.runtime.calendars.maker.paid.any().cpu().item()
        maker_before_cash=values(second.clock.runtime.account.cash)
        maker_before_rebates=amounts(second.clock.runtime.account.rebates)
    prepare(second,True)
    if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT'):
        calendar=second.clock.runtime.calendars.maker
        payable=amounts(calendar.payable);accrued=amounts(calendar.accrued);locked=amounts(calendar.locked)
        account=second.clock.runtime.account;public=second.clock.public;normalizer=second.clock.projection.normalizer
        # 先保存实际日结果和原NOE/Ledger读面；断言失败仍保留真实金额与付款资格。
        (artifact/'maker-target-state.json').write_text(json.dumps(dict(
            goal_until=b_goal,calendar=stored_actual(calendar),account=stored_actual(account),
            before_cash=maker_before_cash,before_rebates=[str(x) for x in maker_before_rebates],
            exact=dict(accrued=[str(x) for x in accrued],payable=[str(x) for x in payable],locked=[str(x) for x in locked]),
            noe_bank={f.name:(sorted(normalizer.bank.declared_schemas) if f.name=='declared_schemas' else
                stored_actual(getattr(normalizer.bank,f.name))) for f in fields(normalizer.bank)},
            batches=stored_actual(public.ledger.columns.projection_batches),
            ledger=dict(entry=values(public.ledger.current_entry_sequence),local=values(public.ledger.current_local_prefix),
                platform_prefix=values(public.ledger.platform.platform_source_prefix),
                balances=[str(x) for x in amounts(public.ledger.platform.balances)],
                maker_cash=[str(x) for x in amounts(public.ledger.maker_cash)])),indent=2)+'\n')
        assert torch.equal(calendar.mature,calendar.occupied)
        assert torch.equal(calendar.finalized,calendar.occupied)
        assert torch.equal(calendar.paid,calendar.occupied & (calendar.payable.numerator.sign.reshape_as(calendar.occupied)>0))
        assert values(account.cash)==[maker_before_cash[0]+int(sum(payable)*1000000)]
        assert amounts(account.rebates)[0]==sum(accrued)-sum(payable)-sum(locked)
        assert amounts(account.rebates)[2]==sum(locked)
        stage('maker_target_tail',goal_until=b_goal,maturity=source['maker_page_material']['maker_maturity'],
            before_cash=maker_before_cash,after_cash=values(second.clock.runtime.account.cash),
            payable=[str(value) for value in payable],actual_public_prefix=values(second.clock.public.ledger.platform.platform_source_prefix))
    assert int(first.clock.gates.public_visible_at[0])==1500 and first.clock.source.position==a_position
    assert stored_actual(first.clock.public.ledger)==a_public and not old_sender.complete.all().cpu().item()
    assert first.clock.source.prepared is not second.clock.source.prepared
    assert first.clock.runtime.hidden_frontier.columns.source.catalog is not second.clock.runtime.hidden_frontier.columns.source.catalog
    assert first.models.hidden is not second.models.hidden
    assert first.runtime is not second.runtime and first.receiver is not second.receiver
    assert first.random is not second.random and first.random.seed.cpu().tolist()!=second.random.seed.cpu().tolist()
    account_a=first.runtime.authority['ledger_projection']['account_instance_key']
    account_b=second.runtime.authority['ledger_projection']['account_instance_key']
    assert account_a!=account_b
    # 原合法Unknown经济读面；仅本次资格注入，全部原材料/路径/消费位置保留。
    no=~second.yes
    def unknown():
        from portfolio_economics import read_portfolio_economics
        portfolio=read_portfolio_economics(second.clock.public.ledger,economic_prefix_known=second.yes,
            success_prefix_known=no,settlement_prefix_known=second.yes,rebate_prefix_known=second.yes,
            economic_liability_prefix_known=second.yes)
        _,point=second.reward_memory.endpoint(second.goal_until,portfolio,second.yes)
        return point
    unknown_point=run(unknown);assert not unknown_point.nav_known.all().cpu().item()
    b.prepare(target_end=True,endpoint=unknown_point)
    preserved=[tuple(id(s) for s in batch.states) for batch in batches]
    paths_before=[[stored_actual(s.path) for s in batch.states] for batch in batches]
    parameters=[p.detach().clone() for p in policy.parameters()]
    stage("joint_unknown_update_begin")
    refusal=joint.update()
    stage("joint_unknown_update_end",updated=False)
    assert all(not r.updated and not r.ready for r in refusal.values()) and not optimizer.state
    assert joint.consumed_batches==0 and int(joint.version[0])==0
    if tracking:assert tracking_client.get_metric_history(mlflow_run,'decisions')==[] and joint.reporter.pending is None
    assert preserved==[tuple(id(s) for s in batch.states) for batch in batches]
    assert paths_before==[[stored_actual(s.path) for s in batch.states] for batch in batches]
    assert all(torch.equal(x,y) for x,y in zip(parameters,policy.parameters(),strict=True))
    del parameters,unknown_point
    prepare(second,True)
    raw_advantages=[];joint_records=[];propagation_records=[]
    if policy.propagation is not None:
        from model_train.propagation import prepare_labels
        before_reader=(first.clock.source.position.clone(),first.clock.source.prepared.current,first.clock.source.prepared.current_index)
        before_public=stored_actual(first.clock.public.ledger)
        before_rng=first.random.counter.clone()
        origins=[s.propagation_origin for s in a.prepared.decisions]
        # 普通根尚未公开到1600；本批尾标签不等待、不转移到后批。
        unripe=prepare_labels(first,origins,200)
        assert not unripe.valid[-1].any().cpu().item()
        crossed=prepare_labels(first,origins,1000)
        assert not crossed.valid.any().cpu().item()
        assert torch.equal(first.clock.source.position,before_reader[0])
        assert first.clock.source.prepared.current is before_reader[1] and first.clock.source.prepared.current_index==before_reader[2]
        assert stored_actual(first.clock.public.ledger)==before_public and torch.equal(first.random.counter,before_rng)
        label=a.prepared.propagation
        torch.testing.assert_close(label.value,T([[.01,0.],[-.01,0.]],torch.float32),rtol=1e-5,atol=1e-7)
        assert label.valid.all().cpu().item()
        causal=first.clock.source.prepared.causal
        assert (causal==1400).any()
        assert int(label.endpoint_prefix[0])==int(__import__("numpy").searchsorted(causal,1400,side="left")-1)
        assert any(not row.all().cpu().item() for row in b.prepared.propagation.valid)
        # 缺腿源于原Down ask删除，Up真实零变化仍为合法标签。
        assert b.prepared.propagation.valid[2:].any().cpu().item()
        stage("propagation_labels",a_value=label.value.cpu().tolist(),a_mask=label.valid.cpu().tolist(),
            b_value=b.prepared.propagation.value.cpu().tolist(),b_mask=b.prepared.propagation.valid.cpu().tolist(),
            horizon_ms=100,eta=1,strict_same_tick=True,reader_ledger_rng_unchanged=True)
    def prepared(active,statistics):
        stage("joint_statistics",roots=len(active),count=int(statistics.count))
        raw=torch.cat([block.advantage[block.present] for batch in active for block in batch.prepared.blocks])
        torch.testing.assert_close(statistics.mean,raw.mean())
        torch.testing.assert_close(statistics.m2,((raw-raw.mean())**2).sum(),rtol=1e-4,atol=1e-4)
        assert int(statistics.count)==len(raw)
        raw_advantages.append(raw.detach().cpu().tolist())
        if policy.propagation is not None:
            masks=torch.cat([batch.prepared.propagation.valid for batch in active])
            assert masks.shape==(len(raw),2)
            stage("propagation_population",legal_legs=int(masks.sum()),masked_legs=int((~masks).sum()))
    def replayed(data,epoch):
        stage("joint_replay",epoch=epoch,count=len(data),version=int(joint.version[0]))
        if epoch==0:
            torch.testing.assert_close(data['new_joint_log_prob'],data['old_joint_log_prob'],rtol=1e-4,atol=1e-4)
            torch.testing.assert_close(data['recomputed_value'],data['state_value'],rtol=1e-4,atol=1e-4)
        raw=torch.tensor(raw_advantages[-1],device='cuda')
        direct=(raw-raw.mean())/raw.std(unbiased=False).clamp_min(torch.finfo(raw.dtype).eps)
        torch.testing.assert_close(data['advantage'].flatten(),direct,rtol=1e-4,atol=1e-4)
        if policy.propagation is not None:
            mask=data["propagation_valid"];prediction=data["propagation_prediction"];label=data["propagation_label"]
            errors=(prediction-label).square()
            direct_aux=errors[mask].sum()/mask.sum().clamp_min(1)
            computed=torch.where(mask,errors,0.).sum()/mask.sum().clamp_min(1)
            torch.testing.assert_close(computed,direct_aux)
            gradients=torch.autograd.grad(computed,(policy.propagation_head.weight,
                next(policy.state.core.parameters())),retain_graph=True)
            assert all(g.abs().sum().cpu().item()>0 for g in gradients)
            propagation_records.append(dict(version=int(joint.version[0]),legal_legs=int(mask.sum()),
                masked_legs=int((~mask).sum()),loss=float(computed.detach()),label=label.cpu().tolist(),
                mask=mask.cpu().tolist(),head_and_shared_gradient=True))
        # 原ClipPPOLoss核拼接人口；root均值不作为oracle。
        losses=joint.loss(data)
        ratio=(data['new_joint_log_prob']-data['old_joint_log_prob']).exp().unsqueeze(-1)
        expected=-torch.minimum(ratio*data['advantage'],ratio.clamp(.8,1.2)*data['advantage']).mean()
        torch.testing.assert_close(losses['loss_objective'],expected)
        joint_records.append(dict(version=int(joint.version[0]),epoch=epoch,decisions=len(data),
            raw_advantage=raw_advantages[-1],old_joint=data['old_joint_log_prob'].detach().cpu().tolist(),
            new_joint=data['new_joint_log_prob'].detach().cpu().tolist(),losses={k:stored_actual(v) for k,v in losses.items()}))
    joint.on_prepared=prepared;joint.on_replayed=replayed
    stage("joint_update_begin",roots=2)
    if tracking:
        original_write=joint.reporter.write_pending
        def fail_report():raise OSError('finite validation: MLflow write failed after actual update')
        joint.reporter.write_pending=fail_report
        try:joint.update()
        except ReportWriteError as error:
            assert isinstance(error.__cause__,OSError)
            updates=error.update_results
            assert joint.consumed_batches==1 and int(joint.version[0])==1
            assert not a.states and not b.states and a.prepared is None and b.prepared is None
            from validate_active_checkpoint import own,assert_same
            after_step=own(dict(policy=policy.state_dict(),optimizer=optimizer.state_dict(),version=joint.version))
            summary=json.loads(json.dumps(joint.reporter.pending))
            (artifact/'mlflow-failed-write-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        else:raise AssertionError('报告失败应明确返回已经更新的事实')
        finally:joint.reporter.write_pending=original_write
        assert joint.reporter.pending is not None
        assert tracking_client.get_metric_history(mlflow_run,'loss_total')==[]
    else:updates=joint.update()
    stage("joint_update_end",version=int(joint.version[0]),consumed_batches=joint.consumed_batches)
    assert updates[a].updated and updates[b].updated and int(joint.version[0])==1
    assert joint.consumed_batches==1 and b.ended and not a.ended
    assert updates[a].decisions==2 and updates[b].decisions==6 and not updates[a].target_end and updates[b].target_end
    assert torch.equal(first.models.hidden,a_hidden) and torch.equal(first.random.counter,a_rng)
    assert stored_actual(old_sender.publication.candidate)==old_path and not old_sender.complete.all().cpu().item()
    assert a.states==[] and b.states==[] and not second.models.tasks
    gc.collect();assert all(ref() is None for items in refs for ref in items)
    assert all(root.clock.public_loop.captured is not None for root in (first,second))
    stage('public_loop_production_consumed',positions=[root.clock.source.position.cpu().tolist() for root in (first,second)],
        private_loop_counts=[int(root.clock.public_loop.captured.processed.cpu()) for root in (first,second)])
    if os.environ.get('B_GPU_ACTIVE_CHECKPOINT'):
        from model_train.active_checkpoint import save_joint_checkpoint
        binding=[dict(name='finite_joint_v1',duration_default=20,duration={},dispatch_default=0,dispatch={'1300':400}),
            dict(name='finite_joint_v1',duration_default=114,duration={'1800':214},dispatch_default=0,dispatch={})]
        source_binding=json.loads(Path(os.environ['B_GPU_CHECKPOINT_BINDING']).read_text())
        checkpoint=save_joint_checkpoint(joint,artifact/'checkpoints',bindings=binding,source_binding=source_binding,progress=stage)
        if tracking:
            # 保存成员先保原pending；完整latest发布后，更新摘要连同点引用补写。
            assert joint.reporter.pending is None
            assert_same(after_step,own(dict(policy=policy.state_dict(),optimizer=optimizer.state_dict(),version=joint.version)))
            assert joint.consumed_batches==1
            logged=tracking_client.get_metric_history(mlflow_run,'loss_total')
            expected=sum(float(value) for name,value in joint_records[0]['losses'].items() if name.startswith('loss_'))+propagation_records[0]['loss']
            assert len(logged)==1 and logged[0].step==1 and abs(logged[0].value-expected)<1e-4
            actual_profile=summary['report']['training_configuration']
            assert actual_profile['model_shape']==vars(policy.shape)
            assert actual_profile['input_version']==policy.input_version and actual_profile['value_version']==policy.value_version
            assert actual_profile['propagation']==policy.propagation
            assert actual_profile['optimizer']['param_groups'][0]['lr']==.0001
            assert actual_profile['roots'][0]['reward_a_pusd']==['1/50']
            assert actual_profile['roots'][1]['goal_until']==[87010000]
            assert summary['report']['roots']==2 and summary['metrics']['decisions']==8
            assert summary['metrics']['aux_valid']==13 and summary['metrics']['aux_masked']==3
            stage('mlflow_retry_report_only',step=1,loss=logged[0].value,weights_adam_version_consumption_unchanged=True)
        previous_latest=(artifact/'checkpoints/latest.json').read_bytes()
        def incomplete(path):
            raise OSError('finite validation: member write completed, manifest not published')
        try:
            save_joint_checkpoint(joint,artifact/'checkpoints',bindings=binding,
                source_binding=source_binding,progress=stage,before_manifest=incomplete)
        except OSError as error:
            assert str(error).startswith('finite validation:')
        else:raise AssertionError('未完整成员不应发布')
        assert (artifact/'checkpoints/latest.json').read_bytes()==previous_latest
        half=list((artifact/'checkpoints').glob('.step-*'))
        assert len(half)==1 and not (half[0]/'manifest.json').exists()
        # 当前checkpoint成员与后续真实执行对象隔离；只读oracle不借导出器。
        continuation_begin=[len(x) for x in starts];completion_begin=[len(x) for x in sent]
    b_position=second.clock.source.position;b_rng=second.random.counter.clone();b_cash=stored_actual(second.clock.public.ledger.platform.balances)
    assert collect(first).learning_boundary
    prepare(first);stage("joint_update_begin",roots=1)
    second_update=joint.update();stage("joint_update_end",version=int(joint.version[0]),consumed_batches=joint.consumed_batches)
    assert set(second_update)=={a} and second_update[a].updated
    assert joint.consumed_batches==2 and int(joint.version[0])==2
    # 只A推进自己的原开放时刻；B既有目标不再次消费、不造空样本。
    while True:
        advance=first.run.advance_before(T([1721]),first.yes,progress=stage)
        if advance.complete.all().cpu().item():break
        assert first.resume_resources(advance)
    assert next(row for row in sent[0] if row['cutoff']==1300)['at']==1720
    assert next(row for row in sent[0] if row['cutoff']==1300)['path']==old_path
    assert starts[0][2]['cutoff']==1500 and starts[0][2]['version']==[1]
    assert starts[0][4]['cutoff']==1700 and starts[0][4]['version']==[2]
    assert second.clock.source.position==b_position and torch.equal(second.random.counter,b_rng)
    assert stored_actual(second.clock.public.ledger.platform.balances)==b_cash and b.states==[]
    assert first.request_directory is not second.request_directory
    assert all(row['request_contexts'][0]['global_request_id']!=other['request_contexts'][0]['global_request_id']
        for row in sent[0] for other in sent[1])
    del old_task,old_sender;gc.collect();assert sender_ref() is None
    report=dict(scope='two real independent cold account Roots; one joint learner; finite engineering validation',
        config_a=vars(config_a),config_b=vars(config_b),optimizer=dict(kind='AdamW',lr=.0001,weight_decay=.01),
        accounts=[account_a,account_b],starts=starts,completions=completions,
        bounds=dict(trading_until=[2000,2000],goal_until=[87010000,b_goal],b_last_start=1870,b_last_finish=2084,b_cutoff1900_not_started=True),updates=[stored_actual(updates[a]),stored_actual(updates[b]),stored_actual(second_update[a])],
        joint_replays=joint_records,propagation=policy.propagation,propagation_replays=propagation_records,
        unknown_keeps_all_materials_zero_step=True,
        independent_readers_accounts_hidden_rng=True,old_send_path_unchanged=True,
        ended_root_not_reconsumed=True,consumed_batches=joint.consumed_batches)
    if tracking:
        assert [(m.step,m.value) for m in tracking_client.get_metric_history(mlflow_run,'decisions')]==[(1,8.),(2,2.)]
        assert [(m.step,m.value) for m in tracking_client.get_metric_history(mlflow_run,'aux_valid')]==[(1,13.),(2,2.)]
        assert len(tracking_client.get_metric_history(mlflow_run,'checkpoint_published'))==1
        report['mlflow']=dict(tracking_uri=tracking_uri,run_id=mlflow_run,steps=[1,2],
            write_failure_after_update_preserved=True,retry_report_only=True,global_loss_not_doubled=True)
    if os.environ.get('B_GPU_ACTIVE_CHECKPOINT'):
        from validate_active_checkpoint import direct_oracle
        oracle=direct_oracle(joint)
        oracle['starts']=[rows[begin:] for rows,begin in zip(starts,continuation_begin,strict=True)]
        oracle['sent']=[rows[begin:] for rows,begin in zip(sent,completion_begin,strict=True)]
        torch.save(oracle,artifact/'uninterrupted-oracle.pt')
        (artifact/'checkpoint-result.json').write_text(json.dumps(dict(checkpoint=str(checkpoint),
            oracle=str(artifact/'uninterrupted-oracle.pt'),endpoint=1721,new_process_required=True),indent=2)+'\n')
        if os.environ.get('B_GPU_HIDDEN_REFERENCE_LOOP'):
            from validate_active_checkpoint import advance_reference_tail
            tail=advance_reference_tail(joint,stage)
            tail_oracle=direct_oracle(joint)
            tail_oracle['starts']=[rows[begin:] for rows,begin in zip(starts,continuation_begin,strict=True)]
            tail_oracle['sent']=[rows[begin:] for rows,begin in zip(sent,completion_begin,strict=True)]
            torch.save(tail_oracle,artifact/'uninterrupted-reference-tail-oracle.pt')
            (artifact/'reference-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
        if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT'):
            from validate_active_checkpoint import advance_maker_tail
            tail=advance_maker_tail(joint,stage,artifact/'maker-post-checkpoint')
            tail_oracle=direct_oracle(joint)
            tail_oracle['starts']=[rows[begin:] for rows,begin in zip(starts,continuation_begin,strict=True)]
            tail_oracle['sent']=[rows[begin:] for rows,begin in zip(sent,completion_begin,strict=True)]
            torch.save(tail_oracle,artifact/'uninterrupted-maker-tail-oracle.pt')
            (artifact/'maker-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
        if os.environ.get('B_GPU_AUTO_CHECKPOINT'):
            from validate_active_checkpoint import advance_auto_tail
            tail=advance_auto_tail(joint,stage,artifact)
            tail_oracle=direct_oracle(joint)
            tail_oracle['starts']=[rows[begin:] for rows,begin in zip(starts,continuation_begin,strict=True)]
            tail_oracle['sent']=[rows[begin:] for rows,begin in zip(sent,completion_begin,strict=True)]
            torch.save(tail_oracle,artifact/'uninterrupted-auto-tail-oracle.pt')
            (artifact/'auto-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
    (artifact/'actual-joint-roots-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


def logical_batch_case(root,policy,source,artifact,stage):
    """真实Root有限批→联合路径ClipPPO→释放→同账户续采；工程参数无正式默认。"""
    from model_train.logical_batch import LogicalBatchConfig,LogicalBatch
    from model_train.gpu_policy import finite_value_context
    from portfolio_economics import read_portfolio_economics
    from exposure import read_portfolio_exposure
    config=LogicalBatchConfig(decisions=2,tbptt=1,burn_in=1,epochs=1,
        clip_epsilon=.2,critic_coefficient=.5,entropy_coefficient=0.)
    (artifact/'logical-batch-config.json').write_text(json.dumps(vars(config),indent=2)+'\n')
    with torch.no_grad():
        policy.action.actor.family[-1].bias[2]=6.
        policy.action.actor.variant.bias.copy_(T([2,0]).float())
        policy.action.actor.price.bias[38]=6.
    optimizer=torch.optim.AdamW(policy.parameters(),lr=.0001)
    learner=root.enable_learning(policy,optimizer,config,T([0]))
    replay_checks=[]
    def replayed(data,epoch):
        # 同行为参数、真实h_in与完整路径；同时比较联合ell和所有决定价值。
        if epoch==0:
            torch.testing.assert_close(data['new_joint_log_prob'],data['old_joint_log_prob'],rtol=1e-4,atol=1e-4)
            torch.testing.assert_close(data['recomputed_value'],data['state_value'],rtol=1e-4,atol=1e-4)
        replay_checks.append(dict(version=int(learner.version[0]),epoch=epoch,
            joint_old=data['old_joint_log_prob'].detach().cpu().tolist(),
            joint_replayed=data['new_joint_log_prob'].detach().cpu().tolist()))
    learner.on_replayed=replayed
    root.models.timing=lambda observation:(T([20]),T([400 if int(observation.account.cutoff.cpu()[0])==1300 else 0]))
    starts=[];publications=[];requests=[];references=[];updates=[]
    old_started=learner.started;old_published=learner.published
    def started(task,h_in):
        with torch.no_grad():
            rows=task.observation.row_roots;frozen=task.observation.frozen
            value_context=finite_value_context(frozen.cutoff,root.goal_until[rows],root.trading_until[rows])
            expected=policy.frozen_state_and_value(frozen,h_in,frozen.identities,
                task.evaluation.policy_version,value_context=value_context)
        torch.testing.assert_close(task.evaluation.hidden,expected.hidden)
        torch.testing.assert_close(task.evaluation.state,expected.state)
        torch.testing.assert_close(task.evaluation.value,expected.value)
        starts.append(dict(cutoff=int(task.observation.account.cutoff.cpu()[0]),
            started=int(task.started_at.cpu()[0]),version=task.evaluation.policy_version.cpu().tolist(),
            h_in_all_layers=True,role=frozen.role.cpu().tolist()))
        old_started(task,h_in)
        references.append(weakref.ref(learner.states[-1]))
    def published(task,accepted):
        old_published(task,accepted)
        if task.sender is not None:
            path=task.sender.publication.candidate
            publications.append(dict(cutoff=int(task.observation.account.cutoff.cpu()[0]),
                completed=int(task.completed_at.cpu()[0]),send_at=int(task.send_at.cpu()[0]),
                version=path.policy_version.cpu().tolist(),length=path.length.cpu().tolist(),
                choices=path.choices.cpu().tolist(),old_log_prob=path.log_prob.cpu().tolist(),
                rng=path.random.counter.cpu().tolist(),request_contexts=stored_actual(task.request_identity.contexts)))
    learner.started=started;learner.published=published
    def completed(task):
        if task.sender is not None and task.sender.requests is not None:
            sender=task.sender
            requests.append(dict(cutoff=int(task.observation.account.cutoff.cpu()[0]),
                sent_at=int(sender.control.execute_at.cpu()[0]),input_sequence=values(sender.receipt.input_sequence)[0],
                body=sender.requests[1][0],version=sender.publication.candidate.policy_version.cpu().tolist(),
                choices=sender.publication.candidate.choices.cpu().tolist(),
                rng=sender.publication.candidate.random.counter.cpu().tolist()))
    root.models.on_completed=completed
    def collect():
        while True:
            result=root.collect_learning_batch(T([2001]),root.yes,progress=stage)
            if result is None or result.learning_boundary:return result
            if result.complete.all().cpu().item():return result
            assert root.resume_resources(result),stored_actual(result)
    first=collect()
    assert first.learning_boundary and root.run.next_observation.cpu().tolist()==[1600]
    assert [s['cutoff'] for s in starts]==[1300,1400]
    assert starts[0]['started']==1300 and publications[0]['completed']==1320
    assert int(root.clock.gates.public_visible_at.cpu()[0])==1500
    old_task=next(t for t in root.models.tasks if int(t.observation.account.cutoff.cpu()[0])==1300)
    old_path=stored_actual(old_task.sender.publication.candidate);old_rng=root.random.counter.clone()
    old_hidden=root.models.hidden.clone()
    endpoint,value,ready,fault=learner.bootstrap()
    assert torch.equal(root.models.hidden,old_hidden) and torch.equal(root.random.counter,old_rng)
    assert ready.all().cpu().item() and not fault.any().cpu().item()
    # 原合法Unknown读面进入同一个真实批消费者；它不消费/清零，也不step。
    blocked=LogicalBatch(root,policy,torch.optim.AdamW(policy.parameters(),lr=.0001),config,T([0]))
    blocked.states=[replace(s) for s in learner.states];blocked.pending_count=learner.pending_count
    no=torch.zeros_like(root.yes)
    def unknown_point():
        portfolio=read_portfolio_economics(root.clock.public.ledger,economic_prefix_known=root.yes,
            success_prefix_known=no,settlement_prefix_known=root.yes,rebate_prefix_known=root.yes,
            economic_liability_prefix_known=root.yes)
        # 不修改known摘要；原读面按缺失success前缀形成Unknown。
        memory=root.reward_memory
        _,point=memory.endpoint(memory.last_at_ms,portfolio,root.yes)
        return point
    unknown=run(unknown_point)
    assert not unknown.nav_known.all().cpu().item()
    blocked.bootstrap=lambda:(unknown,*learner.bootstrap()[1:])
    before=[p.detach().clone() for p in policy.parameters()]
    refusal=blocked.update()
    assert not refusal.ready and not refusal.updated and len(blocked.states)==2
    assert not blocked.optimizer.state and all(torch.equal(a,b) for a,b in zip(before,policy.parameters(),strict=True))
    first_update=learner.update();assert first_update.updated and first_update.version==1
    assert torch.equal(root.models.hidden,old_hidden) and torch.equal(root.random.counter,old_rng)
    assert stored_actual(old_task.sender.publication.candidate)==old_path
    updates.append(stored_actual(first_update));del old_task,blocked,before,endpoint
    gc.collect();assert all(ref() is None for ref in references[:1])
    while True:
        result=collect()
        if result is not None and result.complete.all().cpu().item():break
        _,update=root.update_learning_batch(root.yes)
        assert update.updated
        updates.append(stored_actual(update));stage('logical_batch_update',**updates[-1])
    _,last=root.update_learning_batch(root.yes,target_end=True,progress=stage)
    assert last.updated and last.target_end and last.bootstrap.cpu().tolist()==[[0.]]
    updates.append(stored_actual(last))
    assert starts[2]['cutoff']==1500 and starts[2]['version']==[1]
    assert [s['cutoff'] for s in starts]==list(range(1300,2000,100))
    assert next(i for i,r in enumerate(requests) if r['cutoff']==1500)<next(i for i,r in enumerate(requests) if r['cutoff']==1300)
    sent_old=next(r for r in requests if r['cutoff']==1300)
    published_old=publications[0]
    assert sent_old['choices']==published_old['choices'] and sent_old['version']==published_old['version']
    assert sent_old['rng']==published_old['rng'] and sent_old['sent_at']==published_old['send_at']==1720
    assert learner.states==[] and not root.models.tasks
    gc.collect();assert all(ref() is None for ref in references)
    before_memory=stored_actual(root.reward_memory)
    _,goal=root.read_interval_reward(root.goal_until,root.yes,progress=stage)
    _,repeated=root.read_interval_reward(root.goal_until,root.yes,progress=stage)
    assert stored_actual(goal)==stored_actual(repeated) and stored_actual(root.reward_memory)==before_memory
    assert root.clock.source.next().eof.all().cpu().item()
    report=dict(scope='real single-account bounded collection/full joint-path ClipPPOLoss/update/resume; engineering fixture',
        config=vars(config),optimizer=dict(kind='AdamW',lr=.0001,weight_decay=.01),
        starts=starts,publications=publications,requests=requests,updates=updates,replay_checks=replay_checks,
        unknown_read_blocks_optimizer_retains_population=True,consumed_references_released=True,
        activity_hidden_not_written_by_learning=True,old_published_path_and_rng_unchanged=True,
        goal=stored_actual(goal),cash=[str(x) for x in amounts(root.clock.public.ledger.platform.balances)],
        platform_prefix=values(root.clock.public.ledger.platform.platform_source_prefix))
    (artifact/'actual-logical-batch-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


def logical_context_case(root,policy,source,artifact,stage):
    """原双市场同根：真实T−30 ContextOnly与当前决定共享一次全层递推。"""
    from model_train.logical_batch import LogicalBatchConfig
    from model_train.gpu_policy import finite_value_context
    config=LogicalBatchConfig(1,1,1,1,.2,.5,0.)
    optimizer=torch.optim.AdamW(policy.parameters(),lr=.0001)
    learner=root.enable_learning(policy,optimizer,config,T([0]))
    root.models.timing=lambda observation:(T([20]),T([0]))
    starts=[];owned=[];original=learner.started
    def started(task,h_in):
        frozen=task.observation.frozen
        assert frozen.role.cpu().tolist()==[1,0]
        with torch.no_grad():
            context=finite_value_context(frozen.cutoff,root.goal_until[task.observation.row_roots],
                root.trading_until[task.observation.row_roots])
            expected_hidden=h_in.clone()
            for role in (0,1):
                rows=torch.nonzero(frozen.role==role).flatten()
                part=policy.frozen_state_and_value(frozen.rows(rows),h_in[:,rows],frozen.identities[rows],
                    task.evaluation.policy_version[rows],value_context=context[rows])
                expected_hidden[:,rows]=part.hidden
                torch.testing.assert_close(part.state,task.evaluation.state[rows])
                torch.testing.assert_close(part.value,task.evaluation.value[rows])
        torch.testing.assert_close(expected_hidden,task.evaluation.hidden)
        starts.append(dict(cutoff=frozen.cutoff.cpu().tolist(),version=task.evaluation.policy_version.cpu().tolist(),
            h_in=h_in.detach().clone(),h_out=expected_hidden.detach().clone()))
        original(task,h_in)
        owned.append(weakref.ref(learner.states[-1]))
    learner.started=started
    def advance(horizon,collect):
        while True:
            result=(root.collect_learning_batch if collect else root.run.advance_before)(T([horizon]),root.yes,progress=stage)
            if result is None or result.learning_boundary or result.complete.all().cpu().item():return result
            assert root.resume_resources(result)
    boundary=advance(300401,True)
    assert boundary.learning_boundary and len(starts)==1
    assert int(root.clock.gates.public_visible_at[0])==300400
    assert root.run.next_observation.cpu().tolist()==[300500]
    queued=learner._successor();frozen=queued.observation.frozen
    assert frozen.cutoff.cpu().tolist()==[300400,300400] and frozen.role.cpu().tolist()==[1,0]
    h=root.models.hidden.clone();rng=root.random.counter.clone()
    _,bootstrap,ready,fault=learner.bootstrap()
    with torch.no_grad():
        expected=policy.frozen_state_and_value(frozen,h,frozen.identities,T([0,0]),
            value_context=finite_value_context(frozen.cutoff,T([303000,303000]),T([303000,303000])))
    torch.testing.assert_close(bootstrap,expected.value[queued.observation.decision_rows])
    assert ready.all().cpu().item() and not fault.any().cpu().item()
    assert not torch.equal(expected.hidden[:,1],h[:,1])
    assert torch.equal(root.models.hidden,h) and torch.equal(root.random.counter,rng)
    replay_checks=[]
    def replayed(data,epoch):
        if epoch==0:
            torch.testing.assert_close(data['new_joint_log_prob'],data['old_joint_log_prob'],rtol=1e-4,atol=1e-4)
            torch.testing.assert_close(data['recomputed_value'],data['state_value'],rtol=1e-4,atol=1e-4)
        replay_checks.append(dict(epoch=epoch,old=data['old_joint_log_prob'].detach().cpu().tolist(),
            replay=data['new_joint_log_prob'].detach().cpu().tolist(),
            old_value=data['state_value'].detach().cpu().tolist(),
            replayed_value=data['recomputed_value'].detach().cpu().tolist()))
    learner.on_replayed=replayed
    update=learner.update();assert update.updated
    gc.collect();assert owned[0]() is None
    assert torch.equal(root.models.hidden,h) and torch.equal(root.random.counter,rng)
    advance(300421,False)
    assert len(starts)==2 and starts[1]['version']==[1,1]
    assert torch.equal(starts[1]['h_in'],h)
    # 临时bootstrap没有提交；更新后未开始ContextOnly与决定首次真实递推一次。
    torch.testing.assert_close(root.models.hidden,starts[1]['h_out'])
    report=dict(scope='original two-market T-30 ContextOnly row + genuine next decision, one account; ordinary bootstrap only',
        config=vars(config),trading_until=303000,goal_until=303000,goal_closed=False,
        first_cutoff=300300,first_complete=300320,next_frozen=300400,next_complete=300420,
        roles=[1,0],all_gru_layers_compared=True,bootstrap_does_not_commit=True,
        next_first_bound_version=[1,1],update=stored_actual(update),replay_checks=replay_checks,
        context_only_no_policy_loss=True,consumed_reference_released=True)
    (artifact/'actual-logical-context-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report




def logical_cutoff_case(root,policy,source,artifact,stage):
    """原有限目标：最后已开始计算跨截止，不用旧排队决定作bootstrap。"""
    from model_train.logical_batch import LogicalBatchConfig
    config=LogicalBatchConfig(6,1,1,1,.2,.5,0.)
    with torch.no_grad():
        policy.action.actor.family[-1].bias.fill_(-40.)
        policy.action.actor.family[-1].bias[0]=40.
    learner=root.enable_learning(policy,torch.optim.AdamW(policy.parameters(),lr=.0001),config,T([0]))
    root.models.timing=lambda observation:(T([214 if int(observation.account.cutoff[0])==1800 else 114]),T([0]))
    starts=[];published=[];refs=[];last_rng=[];original=learner.started;publish=learner.published
    def started(task,h_in):
        original(task,h_in);refs.append(weakref.ref(learner.states[-1]))
        starts.append((int(task.observation.account.cutoff[0]),int(task.started_at[0])))
    def complete(task,accepted):
        publish(task,accepted)
        published.append((int(task.observation.account.cutoff[0]),int(task.completed_at[0])))
        if published[-1][0]==1800:last_rng.append(root.random.counter.clone())
    learner.started=started;learner.published=complete
    while True:
        result=root.collect_learning_batch(T([2100]),root.yes,progress=stage)
        if result.complete.all().cpu().item():break
        assert not result.learning_boundary and root.resume_resources(result)
    assert starts==[(1300,1300),(1400,1414),(1500,1528),(1600,1642),(1700,1756),(1800,1870)]
    assert published[-1]==(1800,2084) and learner.pending_count==6
    assert not root.models.tasks and root.models.hidden.shape[1]==0
    assert learner._successor() is None and learner.bootstrap() is None
    assert torch.equal(root.random.counter,last_rng[0])
    def replayed(data,epoch):
        if epoch==0:
            torch.testing.assert_close(data['new_joint_log_prob'],data['old_joint_log_prob'],rtol=1e-4,atol=1e-4)
            torch.testing.assert_close(data['recomputed_value'],data['state_value'],rtol=1e-4,atol=1e-4)
    learner.on_replayed=replayed
    while True:
        advance,update=root.update_learning_batch(root.yes,target_end=True,progress=stage)
        if update is not None:break
        assert root.resume_resources(advance)
    assert update.updated and update.decisions==6 and update.target_end
    assert update.bootstrap.cpu().tolist()==[[0.]] and learner.states==[]
    gc.collect();assert all(ref() is None for ref in refs)
    assert torch.equal(root.random.counter,last_rng[0])
    assert root.clock.source.next().eof.all().cpu().item()
    _,reward=root.read_interval_reward(root.goal_until,root.yes,progress=stage)
    report=dict(config=vars(config),starts=starts,published=published,trading_until=2000,
        goal_until=87010000,ineligible_C1900_not_bootstrap=True,no_retired_policy_or_rng=True,
        no_consumer_hot_rows_released=True,consumed_references_released=True,update=stored_actual(update),
        final_reward=stored_actual(reward),platform_prefix=values(root.clock.public.ledger.platform.platform_source_prefix))
    (artifact/'actual-logical-cutoff-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


def logical_gap_case(root,policy,source,artifact,stage):
    """真实5min窗口错开300ms：暂停批跨三个纯ContextOnly网格。"""
    from model_train.logical_batch import LogicalBatchConfig
    from model_train.gpu_policy import finite_value_context
    cutoff_case=bool(os.environ.get('B_GPU_ACTOR_LOGICAL_GAP_CUTOFF'))
    with torch.no_grad():
        policy.action.actor.family[-1].bias.fill_(-40.)
        policy.action.actor.family[-1].bias[0]=40.  # 真正NoTrade决定，不伪造ContextOnly动作。
    learner=root.enable_learning(policy,torch.optim.AdamW(policy.parameters(),lr=.0001),
        LogicalBatchConfig(7,2,1,1,.2,.5,0.),T([0]))
    root.models.timing=lambda observation:(T([80 if int(observation.account.cutoff[0])>=301000 else 20]),T([0]))
    starts=[];refs=[];original=learner.started
    def started(task,h_in):
        frozen=task.observation.frozen;rows=task.observation.row_roots
        context=finite_value_context(frozen.cutoff,root.goal_until[rows],root.trading_until[rows])
        with torch.no_grad():
            expected=task.evaluation.hidden.clone()
            for role in (0,1):
                selected=torch.nonzero(frozen.role==role).flatten()
                if not selected.numel():continue
                part=policy.frozen_state_and_value(frozen.rows(selected),h_in[:,selected],
                    frozen.identities[selected],task.evaluation.policy_version[selected],value_context=context[selected])
                expected[:,selected]=part.hidden
        torch.testing.assert_close(expected,task.evaluation.hidden)
        if starts:
            previous=starts[-1]
            mapping=(frozen.identities[:,None]==previous['identities'][None]).all(2)
            assert mapping.any(1).all().cpu().item()
            torch.testing.assert_close(h_in,previous['h_out'][:,mapping.to(torch.int64).argmax(1)])
        original(task,h_in);refs.append(weakref.ref(learner.states[-1]))
        starts.append(dict(cutoff=int(task.observation.account.cutoff[0]),at=int(task.started_at[0]),
            roles=frozen.role.cpu().tolist(),version=task.evaluation.policy_version.cpu().tolist(),
            h_out=expected.detach().clone(),identities=frozen.identities.clone()))
        stage('logical_gap_started',**{k:v for k,v in starts[-1].items() if k not in ('h_out','identities')})
    learner.started=started
    def advance(horizon,collect=True):
        while True:
            result=(root.collect_learning_batch if collect else root.run.advance_before)(T([horizon]),root.yes,progress=stage)
            if result is None or result.learning_boundary or result.complete.all().cpu().item():return result
            assert root.resume_resources(result)
    boundary=advance(301501 if cutoff_case else 301301)
    assert [s['cutoff'] for s in starts]==list(range(300300,301000,100))
    assert learner.pending_count==7 and all(s.published for s in learner.states)
    if cutoff_case:
        assert boundary.complete.all().cpu().item() and not boundary.learning_boundary
        assert learner._successor() is None and learner.bootstrap() is None
        assert not root.models.tasks
        before=[p.detach().clone() for p in policy.parameters()]
        refusal=learner.update()
        assert not refusal.updated and learner.pending_count==7
        assert all(torch.equal(a,b) for a,b in zip(before,policy.parameters(),strict=True))
        report=dict(cutoff_predecessor_duration_blocks_bootstrap=True,trading_until=301500,
            pending_decisions_retained=7,optimizer_not_called=True,declared_goal=303000,goal_closed=False)
    else:
        assert boundary.learning_boundary and int(root.clock.gates.public_visible_at[0])==301300
        plan=root.models.eligible_starts()
        assert [(int(t.observation.account.cutoff[0]),int(at[0])) for t,active,at in plan]==[
            (301000,301300),(301100,301380),(301200,301460),(301300,301540)]
        hidden=root.models.hidden.clone();rng=root.random.counter.clone()
        assert learner.bootstrap() is not None
        assert torch.equal(hidden,root.models.hidden) and torch.equal(rng,root.random.counter)
        def replayed(data,epoch):
            if epoch==0:
                torch.testing.assert_close(data['new_joint_log_prob'],data['old_joint_log_prob'],rtol=1e-4,atol=1e-4)
                torch.testing.assert_close(data['recomputed_value'],data['state_value'],rtol=1e-4,atol=1e-4)
        learner.on_replayed=replayed
        update=learner.update();assert update.updated
        gc.collect();assert all(ref() is None for ref in refs)
        advance(301621,False)
        tail=starts[7:]
        assert [(s['cutoff'],s['at']) for s in tail]==[(301000,301300),(301100,301380),(301200,301460),(301300,301540)]
        assert [s['roles'] for s in tail]==[[0],[0],[0],[1]]
        assert all(s['version']==[1] for s in tail)
        assert learner.pending_count==1 and len(learner.states)==4
        assert all(s.published for s in learner.states)
        assert all(s.path is None for s in learner.states[:3]) and learner.states[3].path is not None
        torch.testing.assert_close(root.models.hidden,tail[-1]['h_out'])
        report=dict(real_context_only_tasks=3,context_only_not_decisions=True,context_only_completed_without_sender=True,
            next_decision_started_at=301540,next_decision_cutoff=301300,next_version=1,
            all_gru_layers_compared=True,bootstrap_does_not_commit=True,consumed_references_released=True,
            update=stored_actual(update),goal_closed=False)
    report['starts']=[{k:v for k,v in s.items() if k not in ('h_out','identities')} for s in starts]
    (artifact/'actual-logical-gap-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


def propagation_coverage_case(root,artifact,stage):
    """实际有限选源到1450，EOF后只推进原经济/Actor时钟，不补公共事实。"""
    from model_train.propagation import frozen_origin,prepare_labels
    from public_device_page import PreparedPublicBlocks
    origins=[];decisions=[]
    def started(task):
        rows=task.observation.decision_rows
        rows=rows[task.observation.frozen.role[rows]==1]
        origins.append(frozen_origin(root,task,rows))
        decisions.append(task.observation.frozen.cutoff[rows].cpu().tolist())
    root.models.on_started=started
    while True:
        result=root.run.advance_before(T([2000]),root.yes,progress=stage)
        if result.complete.all().cpu().item():break
        assert root.resume_resources(result),stored_actual(result.fault)
    assert result.fault.cpu().tolist()==[0] and root.clock.source.next().eof.cpu().tolist()==[True]
    assert decisions==[[1300]],decisions
    prepared=root.clock.source.prepared
    assert prepared.received_range_utc_ms==dict(start=900,end=1450)
    assert int(prepared.causal[-1])==1351
    assert root.clock.gates.public_visible_at.cpu().tolist()==[2000]
    before=(root.clock.source.position.clone(),prepared.current,prepared.current_index,
        stored_actual(root.clock.public.ledger),root.random.counter.clone(),root.models.hidden.clone(),
        stored_actual(root.clock.runtime.account),stored_actual(root.clock.runtime.orders))
    inside=prepare_labels(root,origins,100);outside=prepare_labels(root,origins,200)
    edge=prepare_labels(root,origins,150)
    assert inside.valid.all().cpu().item() and edge.valid.all().cpu().item()
    assert not outside.valid.any().cpu().item()
    torch.testing.assert_close(inside.value,torch.zeros_like(inside.value),rtol=0,atol=0)
    assert torch.equal(root.clock.source.position,before[0])
    assert prepared.current is before[1] and prepared.current_index==before[2]
    assert stored_actual(root.clock.public.ledger)==before[3]
    assert torch.equal(root.random.counter,before[4]) and torch.equal(root.models.hidden,before[5])
    assert stored_actual(root.clock.runtime.account)==before[6] and stored_actual(root.clock.runtime.orders)==before[7]
    # 与恢复相同的材料读口重建；只读原manifest，不复制范围到catalog/Root。
    rebuilt=PreparedPublicBlocks(prepared.directory,prepared.material)
    child=rebuilt.read(0)
    assert rebuilt.received_range_utc_ms==child.received_range_utc_ms==prepared.received_range_utc_ms
    report=dict(scope='original cold Root, one real C1300 decision, EOF then economic clock2000; query only, no new PPO update',
        coverage=prepared.received_range_utc_ms,last_group_received=1350,last_group_causal=1351,
        market_end=2000,clock=2000,cutoffs=decisions,within=stored_actual(inside.valid),
        exact_material_end=stored_actual(edge.valid),outside=stored_actual(outside.valid),
        value=stored_actual(inside.value),source_position=stored_actual(root.clock.source.position),
        reader_economics_rng_hidden_unchanged=True,restored_reader_range=rebuilt.received_range_utc_ms)
    (artifact/'actual-propagation-coverage-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


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
    cold_request=None
    if source.get('continuous_two_markets') or source.get('finite_goal') or source.get('propagation_coverage'):
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
        if source.get('finite_goal') or source.get('propagation_coverage'):
            identity=source['opening_ledger']['projection']['account_instance_key']
            cold_request['session'].update(account_id=identity['account_id'],caller_role=identity['caller_role'],
                caller_instance_id=identity['caller_instance_id'],creation_request_id='create-'+source['run_id'],
                response_bridge_binding_id='bridge-control-v2')
            cold_request['activation_id']='activate-'+source['run_id']
    if source.get('continuous_two_markets'):
        taker,maker=run(lambda:(rational([source['taker_rebate_rate']]),rational([source['maker_rebate_rate']])))
        report=bounded(source,None,None,None,sampler,PublicRuleGpu(source['rule']),slots,T([0]*len(slots)),None,None,asks,
            taker,maker,progress,codec,artifact,continuous=continuous,cold_request=cold_request)
    else:
        report=check(source,codec.post_terms_batch_v2,progress,before_drive=lambda *args,**kwargs:bounded(*args,codec,artifact,continuous=lambda *a,**k:continuous(*a,second_buy='--out-of-order-buy' in sys.argv,**k),cold_request=cold_request,**kwargs),defer_initial_send=True)

    print(json.dumps(report),flush=True)

if __name__=='__main__':main()
