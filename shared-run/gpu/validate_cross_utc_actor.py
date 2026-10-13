"""真实跨UTC有限材料的原双分块准备与单账户连续装配验证。

策略、时延和物理容量由本次有限配置提供；不另实现业务归约。
"""
from pathlib import Path
import hashlib
import importlib.util
import copy
import json
import resource
import sys
import time
import torch  # 标准库platform须先于ledger/gpu同名文件加载。

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/path) for path in ('model-train','sim-system/gpu','shared-run/gpu',
    'data-pipeline/gpu','execution-normalization/gpu','ledger/gpu','model-train/native-inputs/gpu')]
from public_device_page import build_public_device_blocks,PreparedPublicBlocks
from hidden_device_page import build_hidden_device_pages,PreparedHiddenPages


def emit(stage,**fields):
    print(json.dumps(dict(stage=stage,**fields)),flush=True)


def observation_time(observation):
    """原Root的同前缀账户Ck；cutoff本身是含原收据的GroupDecisionCutoff。"""
    return int(observation.account.cutoff.cpu()[0])


def strategy_callbacks(config,policies,zero):
    buy_cutoffs=set(config['buy_cutoffs'])
    def policy_start(observation,enabled):
        at=observation_time(observation);return policies[2 if at in buy_cutoffs else 0],zero+1
    def timing(observation):
        at=observation_time(observation)
        return zero+config['durations'].get(str(at),config['calculation_ms']),zero+config['dispatch_ms'].get(str(at),0)
    return policy_start,timing


def phase(action):
    torch.cuda.synchronize();torch.cuda.reset_peak_memory_stats()
    started=time.perf_counter()
    allocated=torch.cuda.memory_allocated();reserved=torch.cuda.memory_reserved()
    result=action()
    torch.cuda.synchronize()
    measurement=dict(seconds=time.perf_counter()-started,
        allocated_baseline_bytes=allocated,reserved_baseline_bytes=reserved,
        allocated_peak_bytes=torch.cuda.max_memory_allocated(),reserved_peak_bytes=torch.cuda.max_memory_reserved(),
        process_max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return result,measurement


def prepare(config):
    artifact=Path(config['artifact'])
    public,measurement=phase(lambda:build_public_device_blocks(config['public_binary'],config['public_material'],
        artifact/'public-blocks',groups_per_block=config['public_groups_per_block'],progress=lambda stage,**row:emit(stage,**row)))
    emit('public_blocks_prepared',groups=len(public.columns['groups']),parts=len(public.parts),**measurement)
    slots=[(public.columns['assets'][asset]['token_id'],'BID' if side==0 else 'ASK',price)
        for asset,side,price in public.columns['slots']]
    _,measurement=phase(lambda:build_hidden_device_pages(public.columns['assets'],slots,artifact/'hidden-pages',
        config['hidden_binding'],buckets_per_part=config['hidden_buckets_per_part'],progress=lambda stage,**row:emit(stage,**row)))
    hidden=PreparedHiddenPages(artifact/'hidden-pages',config['hidden_binding'])
    emit('hidden_pages_prepared',buckets=len(hidden.arrays['bucket_times']),parts=len(hidden.parts),**measurement)


def cancellation_item_dictionary(requests):
    """沿原actor_send取消请求出现顺序读取独立item坐标；不借Post人口。"""
    mapping={}
    for request in requests:
        body=request['request']
        for member in body.get('items',[{'item':body['item']}] if 'item' in body else []):
            mapping.setdefault(member['item']['item_id'],len(mapping))
    return list(mapping)


class PosteriorPrefixRecorder:
    """验证侧原领取/提交读面；记录变化，不推进门或消费记录。"""
    def __init__(self,clock,receiver):
        self.clock=clock;self.receiver=receiver;self.checkpoints=[];self.previous=None

    def __call__(self,public,delivery,enabled):
        public,ready=self.receiver(public,delivery,enabled)
        self.observe(public,ready)
        return public,ready

    def observe(self,public,ready):
        from actor_send import exact_values
        from validate_pending_post import amounts
        from validate_cross_utc_noe import fields
        ledger=public.ledger;waiting=public.waiting;gates=self.clock.gates
        # 本跨UTC代表是一个原账户；根内原position不以物理行号代替。
        assert waiting.received.size(0)==1
        # 原pending与waiting随同一stage选择/前缀安装，不扫描历史自然批。
        received=waiting.received
        minimum=torch.iinfo(torch.int64).min
        times=torch.where(received,public.pending_noe.received_at,minimum)
        through=int(times.max().cpu()) if times.numel() else minimum
        batch=ledger.columns.projection_batches[-1] if ledger.columns.projection_batches else None
        published=None
        if batch is not None:
            from queues import _take
            column=batch.emitted.to(torch.int64).argmax(1)
            published=exact_values(_take(batch.batch_sequences,column))[0]
        consumed=waiting.consumed.sum(1).cpu().tolist()
        # 仅NOE消费人口变更、资源退出与首点需要资金快照。其它门/发布变化仍记录。
        financial=self.previous is None or consumed!=self.checkpoints[-1]['consumed_population'] or not bool(ready.all().cpu())
        position=None
        if financial and ledger.applied.numel() and bool(ledger.applied.any().cpu()):
            words=ledger.applied_sequences.words.to(torch.int64)&0xffffffff
            high=words[:,1] if words.size(1)>1 else torch.zeros_like(words[:,0])
            high=torch.where(ledger.applied[0],high,-1)
            low=torch.where(ledger.applied[0]&(high==high.max()),words[:,0],-1)
            last=int(low.argmax().cpu())
            position=fields(ledger.columns.positions[last].cpu().tolist())[2:]
        elif self.checkpoints:position=self.checkpoints[-1]['last_position']
        point=dict(visible_at=gates.public_visible_at.cpu().tolist(),
            closed_at=gates.closed_receive_at.cpu().tolist(),
            has_closed=gates.has_closed_receive.cpu().tolist(),has_visible=gates.has_public_visible.cpu().tolist(),
            receive_inclusive=None if gates.receive_inclusive is None else gates.receive_inclusive.cpu().tolist(),
            public_inclusive=None if gates.public_inclusive is None else gates.public_inclusive.cpu().tolist(),
            accepted_batch=published,received_through=None if through==minimum else through,
            received_population=waiting.next_rank.cpu().tolist(),consumed_population=consumed,
            last_position=position,ready=ready.cpu().tolist(),entry=exact_values(ledger.current_entry_sequence),
            local=exact_values(ledger.current_local_prefix),platform_prefix=exact_values(ledger.platform.platform_source_prefix),
            waiting=int((waiting.received&~waiting.consumed).sum().cpu()),fault=ledger.fault.cpu().tolist())
        # 同毫秒不同发布/领取或成功前缀不能合并；ready=false同样保留。
        signature=json.dumps(point,sort_keys=True)
        if signature!=self.previous:
            self.previous=signature
            point['financial']=financial
            if financial:
                point.update(balances=[str(value) for value in amounts(ledger.platform.balances)],
                    outcomes=[str(value) for value in amounts(ledger.platform.outcomes)])
            self.checkpoints.append(point)


def run_actor(config):
    from actor_runtime import open_continuous_runtime
    from actor_opening import ActorOpeningCapacities
    from queues import Numbers,ExactInt,ExactRational
    from pending_cancel_intents import CancelFailureScenario
    from latency import EmpiricalDelaySampler
    from rules import PublicRuleGpu
    from actor_send import exact_values
    from model_train.continuous_root import ContinuousActorRoot
    from model_train.action_random import ActionRandom
    from model_train.gpu_policy import Policy100ms,ModelShape100ms
    from validate_actor_continuous import stored_actual
    from validate_pending_post import amounts
    artifact=Path(config['artifact']);started=time.perf_counter()
    T=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device='cuda')
    zero=T([0]);yes=T([True],torch.bool)
    integer=lambda values:ExactInt.from_values(values,device='cuda')
    spec=importlib.util.spec_from_file_location('model_native_inputs',config['native_library'])
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    public=PreparedPublicBlocks(artifact/'public-blocks',config['public_material'])
    hidden=PreparedHiddenPages(artifact/'hidden-pages',config['hidden_binding'])
    request=config['opening']
    rule_context=json.loads(codec.public_rule_context_v1(json.dumps(dict(
        rule_path=request['rule_path'],rule_sha256=request['rule_sha256'],
        rules_as_of_utc_ms=request['rules_as_of_utc_ms'],market=public.columns['markets'][0]))))
    rule=PublicRuleGpu(rule_context)
    sampler=EmpiricalDelaySampler.build(Path(config['profile']).read_bytes(),config['profile_sha256'],
        [request['session']['session_seed'].encode('ascii')])
    def rates():
        from fractions import Fraction
        result=[]
        for name in ('taker_rebate_rate','maker_rebate_rate'):
            value=Fraction(rule_context[name]);result.append(ExactRational.ratio(integer([value.numerator]),integer([value.denominator])))
        return result
    taker,maker=Numbers.run_exact_batch(rates)
    empty=T([[]])
    scenario=Numbers.run_exact_batch(lambda:CancelFailureScenario(empty,integer([]),integer([]),empty.bool(),zero.to(torch.int32)))
    def paths(kind,world,selected):
        width=world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1)
        return T([[0]*width])
    from scheduling import ScheduledTime
    gtd=ScheduledTime.absent(zero)
    runtime,measurement=phase(lambda:open_continuous_runtime(public,hidden,codec,request,rule,sampler,
        capacities=ActorOpeningCapacities(**config['capacities']),page_groups=config['public_groups_per_block'],
        hidden_page_buckets=config['hidden_buckets_per_part'],taker_rate=taker,maker_rate=maker,
        fill_paths=paths,cancel_scenario=scenario,gtd=gtd,auto_redemption=True))
    (artifact/'actual-opening.json').write_text(json.dumps(runtime.authority,indent=2))
    (artifact/'actual-rule-context.json').write_text(json.dumps(rule_context,indent=2))
    emit('cold_account_installed',session_sequences=runtime.authority['session_sequences'],**measurement)
    cash=amounts(runtime.clock.public.ledger.platform.balances)
    assert cash[0]==2000 and cash[2]==cash[3]==0
    assert all(v==0 for v in amounts(runtime.clock.public.ledger.platform.outcomes))
    assert not runtime.population.submitted_legs.present.any().cpu().item()
    torch.manual_seed(config['model_seed']);shape=ModelShape100ms(2,64)
    template=Policy100ms(shape).cuda().eval();policies={}
    for family in (0,2):
        policy=copy.deepcopy(template)
        with torch.no_grad():
            head=policy.action.actor
            head.family[-1].weight.zero_();head.family[-1].bias.zero_();head.family[-1].bias[family]=40.
            head.variant.weight.zero_();head.variant.bias.copy_(T([40,0]).float())
            head.quantity.weight.zero_();head.quantity.bias.zero_();head.quantity.bias[config['quantity_bin']]=40.
            head.price.weight.zero_();head.price.bias.copy_(torch.arange(head.price.bias.numel(),device='cuda').float())
        policies[family]=policy
    buy_cutoffs=set(config['buy_cutoffs'])
    policy_start,timing=strategy_callbacks(config,policies,zero)
    random=ActionRandom(zero+config['model_seed'],T([[0,0,0,0]]))
    model=ContinuousActorRoot(runtime,codec,rule,sampler,started_at=T([config['started_at']]),
        first_observation=T([config['first_observation']]),trading_until=T([config['trading_until']]),goal_until=None,
        request_namespace=request['session']['run_id']+'/'+request['session']['session_id'],
        policy_at_start=policy_start,timing=timing,random=random,model_shape=shape,
        word_capacity=config['word_capacity'],path_capacity=config['path_capacity'])
    begun=completed=warm=decisions=0;pending={};expected_hidden={};requests=[]
    pages={'public_page':0,'hidden_page':0};last_print=0;boundary=[]
    posterior=PosteriorPrefixRecorder(runtime.clock,model.receiver)
    model.receiver=posterior
    checkpoints=posterior.checkpoints
    def frozen_digest(frozen):
        return hashlib.sha256(b''.join(v.detach().cpu().contiguous().numpy().tobytes()
            for v in (frozen.fast,frozen.slow,frozen.book_levels,frozen.book_mask,frozen.identities,frozen.cutoff))).hexdigest()
    def on_started(task):
        nonlocal begun,warm,decisions
        frozen=task.observation.frozen;keys=[tuple(row) for row in frozen.identities.cpu().tolist()]
        before=torch.stack([expected_hidden.get(key,torch.zeros((shape.layers,shape.hidden),device='cuda')) for key in keys],1)
        after=before.clone()
        for role in (0,1):
            indices=torch.nonzero(frozen.role==role).flatten()
            if indices.numel():
                with torch.no_grad():_,part,_=task.policy.state_and_value(*frozen.rows(indices).model_inputs(),before[:,indices],value_context=None)
                after[:,indices]=part
        torch.testing.assert_close(task.evaluation.hidden,after,rtol=0,atol=0)
        assert task.evaluation.policy_version.cpu().tolist()==[1]*len(keys)
        for i,key in enumerate(keys):expected_hidden[key]=after[:,i].clone()
        at=observation_time(task.observation);pending[at]=frozen_digest(frozen)
        begun+=1;warm+=int((frozen.role==0).sum().cpu());decisions+=int((frozen.role==1).sum().cpu())
        if abs(at-config['midnight'])<=300 or at in buy_cutoffs:
            boundary.append(dict(cutoff=at,identities=keys,role=frozen.role.cpu().tolist(),
                history_counts=model.inputs.memory.counts.cpu().tolist(),started_at=task.started_at.cpu().tolist()))
    def on_completed(task):
        nonlocal completed
        at=observation_time(task.observation);assert pending.pop(at)==frozen_digest(task.observation.frozen)
        completed+=1
        sender=task.sender
        if sender is not None and sender.requests is not None:
            for root,body in zip(*sender.requests,strict=True):
                requests.append(dict(root=root,cutoff=at,sent_at=int(task.send_at.cpu()[root]),body=body))
        if abs(at-config['midnight'])<=300:
            boundary.append(dict(completed_cutoff=at,completed_at=task.completed_at.cpu().tolist(),
                send_at=task.send_at.cpu().tolist(),input_sequence=exact_values(runtime.clock.runtime.input_frontier.next_sequence)))
    model.models.on_started=on_started;model.models.on_completed=on_completed
    def progress(stage,**row):
        nonlocal last_print
        if stage in pages:pages[stage]+=1
        now=time.perf_counter()
        if now-last_print<10 and stage not in ('drive_fault',):return
        last_print=now
        frontier=runtime.clock.runtime.hidden_frontier
        fifo=runtime.clock.runtime.hidden.queues
        occupied=fifo.kind!=0
        last_segment=int(torch.where(occupied,torch.arange(fifo.capacity,device=fifo.kind.device)[None,:]+1,0).max().cpu()) if fifo.rows else 0
        emit(stage,**row,observations=int((model.run.next_observation.cpu()[0]-config['first_observation'])//100),
            observations_total=(config['trading_until']-config['first_observation'])//100,
            begun=begun,completed=completed,queue=len(model.models.tasks),elapsed_seconds=now-started,
            hidden_closed_bucket=frontier.closed_bucket.cpu().tolist(),
            hidden_closed_at=frontier.closed_at.cpu().tolist(),
            hidden_next_bucket_global=(frontier.next_bucket+frontier.columns.bucket_base).cpu().tolist(),
            public_source_position=runtime.clock.source.position.cpu().tolist(),
            fifo_rows=fifo.rows,fifo_capacity=fifo.capacity,
            fifo_last_nonempty_column_end=last_segment,fifo_nonempty_segments=int(occupied.sum().cpu()))
    progress.ready=lambda stage:stage=='drive_fault' or time.perf_counter()-last_print>=10
    def advance(at):
        while True:
            result=model.run.advance_before(T([at]),yes,progress=progress)
            assert result.fault.cpu().tolist()==[0],stored_actual(result)
            if result.complete.all().cpu().item():return
            assert model.resume_resources(result),stored_actual(result)
    _,measurement=phase(lambda:advance(config['trading_until']))
    assert int(model.run.next_observation.cpu()[0])==config['trading_until']
    emit('trading_window_complete',begun=begun,completed=completed,warm=warm,decisions=decisions,**measurement)
    # horizon来自原时钟下一真实事件，尾段不生成观察或重置账户。
    tail_started=time.perf_counter()
    while True:
        event=runtime.clock.next_event(yes);task_event=model.models.next_event()
        active=[]
        if event.present.cpu().item():active.append(int(event.at.cpu()[0]))
        if task_event.present.cpu().item():active.append(int(task_event.at.cpu()[0]))
        if model.run.economic_resume.cpu().item():active.append(int(runtime.clock.gates.public_visible_at.cpu()[0]))
        if not active:break
        advance(min(active)+1)
    assert not model.models.tasks and not pending
    assert runtime.clock.public.ledger.fault.cpu().tolist()==[0]
    assert not (runtime.clock.public.waiting.received&~runtime.clock.public.waiting.consumed).any().cpu().item()
    assert runtime.clock.source.next().eof.cpu().item() and runtime.clock.runtime.hidden_frontier.scheduled()[1].cpu().item()
    assert pages['public_page']>0 and pages['hidden_page']>0
    assert len(requests)>=len(buy_cutoffs),requests
    (artifact/'actual-requests.json').write_text(json.dumps(requests,indent=2))
    ledger=runtime.clock.public.ledger;bank=runtime.clock.projection.normalizer.bank
    batches=[{name:stored_actual(getattr(batch,name)) for name in ('rows','emitted','received_at',
        'private_sequences','batch_sequences','record_ordinals','source_categories','platform_at','request_slots','cancellation_method')}
        for batch in ledger.columns.projection_batches]
    slots={slot for batch in batches for slot,on in zip(batch['request_slots'][0],batch['emitted'][0],strict=True)
        if on and slot>=0}
    actual=dict(bank={name:stored_actual(getattr(bank,name)) for name in
        ('logical_keys','semantic','cancellation','source_order','payload_schemas','kinds','fault')},batches=batches,
        requests={str(slot):model.request_directory.keys[slot] for slot in slots},
        cancel_item_ids=cancellation_item_dictionary((runtime.population.cancel_requests or ((),))[0]),
        conditions=runtime.directory.conditions,tokens=runtime.directory.tokens,
        canonical_ids=hidden.manifest['canonical_ids'],terminals=hidden.manifest['terminal_values'],
        row_refs=stored_actual(ledger.columns.row_refs),waiting=stored_actual(runtime.clock.public.waiting))
    (artifact/'actual-noe.json').write_text(json.dumps(actual))
    funds=Numbers.run_exact_batch(ledger.funds)
    final=dict(entry=exact_values(ledger.current_entry_sequence),local=exact_values(ledger.current_local_prefix),
        platform_prefix=exact_values(ledger.platform.platform_source_prefix),
        balances=[str(value) for value in amounts(ledger.platform.balances)],
        outcomes=[str(value) for value in amounts(ledger.platform.outcomes)],
        funds={name:[str(value) for value in amounts(getattr(funds,name))] for name in
            ('actual_available','maker_available','actual_reserved','maker_reserved')})
    report=dict(begun=begun,completed=completed,warm=warm,decisions=decisions,pages=pages,boundary=boundary,
        checkpoints=checkpoints,final_ledger=final,final_account=stored_actual(runtime.clock.runtime.account),
        post_results=stored_actual(runtime.clock.runtime.post_results.values),source_position=runtime.clock.source.position.cpu().tolist(),
        tail_seconds=time.perf_counter()-tail_started,elapsed_seconds=time.perf_counter()-started)
    (artifact/'actual-report.json').write_text(json.dumps(report))
    emit('real_cross_utc_finished',begun=begun,completed=completed,requests=len(requests),pages=pages,
        elapsed_seconds=time.perf_counter()-started)


if __name__=='__main__':
    config=json.loads(Path(sys.argv[2]).read_text())
    if sys.argv[1]=='prepare':prepare(config)
    elif sys.argv[1]=='run':run_actor(config)
    else:raise ValueError('只接当前有限准备与运行阶段')
