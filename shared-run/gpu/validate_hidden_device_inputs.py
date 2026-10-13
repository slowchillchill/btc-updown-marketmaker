"""原同页图已建后，以正式register产生夹在两桶间的新pending请求。"""
from dataclasses import replace
import torch
from queues import Numbers
from task_requests import ActorRequestDirectory
from base_runtime_storage import BaseRuntimeStorage,_tensor_pairs


def probe_request(root):
    clock=root.clock;entering=clock.runtime;world=entering;backend=root.runtime.execution
    original_requests=BaseRuntimeStorage.snapshot_output(entering.requests)
    original_pending=original_requests.next_pending()
    assert original_pending.exists.cpu().tolist()==[True] and original_pending.effective_at.cpu().tolist()==[1384]
    hidden,eof=world.hidden_frontier.scheduled()
    # 原成本端点允许停在物理页末。探针先加载同源相邻页，再以禁用人口冷建图；
    # 这不消费桶/组，不推进原clock，后面的新请求仍必须复用刚建立的同页图。
    if not hidden.present.all().cpu().item():
        loaded=world.hidden_frontier.load_next(clock.hidden_page_buckets)
        world=replace(world,hidden_frontier=loaded)
        hidden,eof=loaded.scheduled()
    # 当前真实Root的请求池可能已满；在建图前按此新请求的物理需求增长，
    # 随后的register保持这个布局。历史FOK及其完成位置仍保留。
    world=replace(world,requests=world.requests.resized(world.requests.capacity+1))
    closed=world.hidden_frontier.closed_at
    assert hidden.present.all().cpu().item() and (hidden.at>closed+1).all().cpu().item()
    until=hidden.at+1
    inactive=torch.zeros_like(root.yes)
    choice=world.choose(hidden,eof,until,inactive,gtd=clock.gtd,before=True)
    _,count=backend.hidden_transactions(world,choice,clock.source.next(),clock.gtd,until,inactive,before=True)
    assert count==0
    captured=backend.transaction_loop.captured
    assert captured is not None
    target=(hidden.at+torch.maximum(closed,clock.gates.closed_receive_at))//2
    directory=ActorRequestDirectory(target.device)
    for identity in root.request_directory.keys:directory.append(identity)
    key=directory.append(root.request_directory.keys[0]+':same-layout-probe')
    identities,offsets=directory.columns()
    keys=torch.full_like(target,key);zero=torch.zeros_like(target)
    sample=root.sampler.sample_requests_indexed(zero,identities,offsets,keys,zero)
    delay=sample.milliseconds.words[:,0].to(torch.int64)&0xffffffff
    sent=target-delay
    actions=world.requests.action_capacity
    items=torch.zeros((target.numel(),actions),dtype=torch.int64,device=target.device)
    ordinals=torch.arange(actions,device=target.device)[None,:].expand_as(items)
    present=ordinals==0
    request,receipt=Numbers.run_exact_batch(lambda:world.requests.register(root.sampler,identities,offsets,
        keys,zero,world.input_frontier.next_sequence,sent,items,ordinals,present,torch.zeros_like(present),
        zero,root.yes,world.requests.fault))
    assert receipt.registered.all().cpu().item() and not receipt.fault.any().cpu().item(),dict(
        capacity=world.requests.capacity,required=receipt.required_requests.cpu().tolist(),fault=receipt.fault.cpu().tolist())
    assert torch.equal(request.next_pending().effective_at,target)
    probe=replace(world,requests=request)
    choice=probe.choose(hidden,eof,until,root.yes,gtd=clock.gtd,before=True)
    assert torch.equal(choice.at,target) and choice.phase.cpu().tolist()==[5]
    result,count=backend.hidden_transactions(probe,choice,clock.source.next(),clock.gtd,until,root.yes,before=True)
    assert backend.transaction_loop.captured is captured,'同布局新请求不能靠换图读取'
    assert count==0 and torch.equal(result.hidden_frontier.closed_at,closed)
    assert torch.equal(captured.inputs.requests.next_pending().effective_at,target)
    actual=captured.view();scheduled,eof=actual.hidden_frontier.scheduled()
    selected=actual.choose(scheduled,eof,until,root.yes,gtd=clock.gtd,before=True)
    assert torch.equal(selected.at,target) and selected.phase.cpu().tolist()==[5]
    assert clock.runtime is entering,'探针不能替换原clock的runtime对象'
    for before,after in _tensor_pairs(original_requests,clock.runtime.requests):
        torch.testing.assert_close(before,after,rtol=0,atol=0)
    pending=clock.runtime.requests.next_pending()
    assert torch.equal(pending.exists,original_pending.exists) and torch.equal(pending.effective_at,original_pending.effective_at)
    return dict(same_capture=True,previous_hidden=closed.cpu().tolist(),request_at=target.cpu().tolist(),
        next_hidden=hidden.at.cpu().tolist(),phase=5,committed_buckets=count,actual_clock_unchanged=True,
        preserved_original_pending=original_pending.effective_at.cpu().tolist())


def probe_queue_binding(root,source,artifact):
    """原1384 FOK→同页1414 Level；完整子clock隔离，原handler独立对照。"""
    import copy
    import json
    from base_runtime_storage import _owned,_capacity_like,_shared
    from dataclasses import fields,is_dataclass
    from input_frontier import AppliedPublicGroup
    def owned(value):
        # AppliedPublicGroup.source是原已验收页的身份绑定，不是source.position。
        # 保该来源身份，其positions/input_sequence独立；其他共享字段仍仅用原合同。
        if isinstance(value,AppliedPublicGroup):
            return replace(value,positions=value.positions.clone(),input_sequence=owned(value.input_sequence))
        if is_dataclass(value):
            return replace(value,**{f.name:getattr(value,f.name) if _shared(value,f.name)
                else owned(getattr(value,f.name)) for f in fields(value)})
        if isinstance(value,tuple):return tuple(owned(item) for item in value)
        if isinstance(value,list):return [owned(item) for item in value]
        if isinstance(value,dict):return {key:owned(item) for key,item in value.items()}
        if isinstance(value,(bytes,float)):return value
        return _owned(value,value)
    from continuous_execution import ContinuousExecution
    from public_group_loop import PublicGroupDeviceLoop
    from maker_loop import _signature
    clock=root.clock;backend=root.runtime.execution
    saved=(owned(clock.runtime),owned(clock.projection),
        owned(clock.public),clock.source.position.clone(),clock.source.fault.clone(),
        owned(clock.gates),root.random.counter.clone(),torch.cuda.get_rng_state().clone())
    entering=clock.runtime
    cpu_effect=next(value for value in source['effects'] if value['key']['at']==1384)
    assert 'Accepted' in cpu_effect['outcome']
    amount=cpu_effect['checkpoint']['quantities']['QuoteBudget']
    matched=amount['actual_matched_base_units'];quote=amount['matched_quote_units']
    assert matched==1_000_001_000 and quote==500_000_500  # 原Rust fixture；单位为微份/微PUSD。
    def queue_values(queue):
        def rows(value):
            values=value.report_values();width=queue.capacity
            return [values[index*width:(index+1)*width] for index in range(queue.rows)]
        return dict(kind=queue.kind.cpu().tolist(),B=rows(queue.total_base),Q=rows(queue.total_quote),
            c=rows(queue.consumed_base),debt=queue.debt.report_values(),capacity=queue.capacity,
            word_widths={name:getattr(queue,name).words.size(1) for name in
                ('total_base','total_quote','consumed_base','account_remaining','debt')})
    assert entering.requests.next_pending().effective_at.cpu().tolist()==[1384]
    child=copy.copy(clock);child.runtime=owned(entering)
    child.projection=owned(clock.projection);child.public=owned(clock.public)
    child.gates=owned(clock.gates);child.gtd=owned(clock.gtd)
    child.observation_known=clock.observation_known.clone()
    child.last_observation=None if clock.last_observation is None else owned(clock.last_observation)
    child.public_loop=PublicGroupDeviceLoop();child.source=copy.copy(clock.source)
    child.source.position=clock.source.position.clone();child.source.fault=clock.source.fault.clone()
    def prepared_copy(value):
        result=copy.copy(value)
        for name in ('_tensors','_prefixes'):
            if hasattr(result,name):setattr(result,name,getattr(result,name).copy())
        current=getattr(result,'current',None)
        if isinstance(current,tuple):
            result.current=(current[0],prepared_copy(current[1]),current[2])
        elif current is not None:result.current=prepared_copy(current)
        return result
    child.source.install_listeners=[];child.source.prepared=prepared_copy(clock.source.prepared)
    # 普通Python公共前缀包装不属于BaseRuntimeStorage；仅测试用深复制
    # 保四个对象间关联，独立拥有其叶张量和host目录，不改生产存储合同。
    if child.source.states is not None:child.source.states=copy.deepcopy(child.source.states)
    if child.source.initial_input_pages is not None:child.source.initial_input_pages=list(child.source.initial_input_pages)
    # 页面材料不可变，页缓存拥有者隔离；原Root的活动source位置不被探针推进。
    frontier=child.runtime.hidden_frontier;block=copy.copy(frontier.columns.source)
    if getattr(block,'catalog',None) is not None:block.catalog=prepared_copy(block.catalog)
    frontier=replace(frontier,columns=replace(frontier.columns,source=block))
    # 1301已提交原1100桶；按原相邻页装配加载唯一1414桶。
    # 1413原消息数量不变，没有形成hidden桶，不能作为图复用前提。
    assert not frontier.applying.any().cpu().item()
    frontier=frontier.load_next(child.hidden_page_buckets)
    assert frontier.columns.at.cpu().tolist()==[1414],frontier.columns.at.cpu().tolist()
    child.runtime=replace(child.runtime,hidden_frontier=frontier)
    population=copy.copy(backend.population)
    for name,value in vars(population).items():
        if value is not None:setattr(population,name,owned(value))
    execution=ContinuousExecution(population,backend.rule,backend.codec,owned(backend.sampler),
        backend.ask_rows,backend.asset_tokens,backend.market_end,backend.taker_rate,backend.maker_rate,
        backend.condition_by_asset,backend.source_assets,backend.fill_paths,backend.cancel_scenario,
        condition_by_market=backend.condition_by_market,market_end_by_market=backend.market_end_by_market,
        redemption_sources=backend.redemption_sources,settlement_policy=backend.settlement_policy,
        resolution_identity_bytes=backend.resolution_identity_bytes,resolution_identity_offsets=backend.resolution_identity_offsets)
    child.handlers=execution.handlers();until=torch.full_like(root.yes,1415,dtype=torch.int64)
    hidden,eof=frontier.scheduled();choice=child.runtime.choose(hidden,eof,until,root.yes,gtd=child.gtd,before=True)
    child.runtime,count=execution.hidden_transactions(child.runtime,choice,child.source.next(),child.gtd,until,root.yes,before=True)
    assert count==0
    captured=execution.transaction_loop.captured;assert captured is not None
    cold_key=execution.transaction_loop.key;recorded_key=cold_key
    old=child.runtime.hidden.queues;frozen=owned(old);layout=_signature(old)
    diagnostic=artifact/'queue-binding-probe.jsonl'
    def record(label,**row):
        with diagnostic.open('a') as output:output.write(json.dumps(dict(label=label,**row))+'\n')
    def key_changes(left,right,path='key'):
        if isinstance(left,tuple) and isinstance(right,tuple) and len(left)==len(right):
            if len(left)==2 and isinstance(left[0],str) and left[0]==right[0]:
                return key_changes(left[1],right[1],path+'.'+left[0])
            return [row for index,(a,b) in enumerate(zip(left,right,strict=True))
                for row in key_changes(a,b,path+'['+str(index)+']')]
        return [] if left==right else [dict(path=path,before=repr(left),after=repr(right))]
    record('cold_key',key=repr(cold_key))
    initial_values=queue_values(old)
    tail=[0]*(old.capacity-1);empty=[0]*old.capacity
    record('before_fok',queue=initial_values,cpu_matched_base_micro_units=matched,cpu_matched_quote_micro_units=quote)
    assert initial_values['kind'][0]==[1]+tail and initial_values['B'][0]==[matched]+tail
    assert initial_values['Q'][0]==[quote]+tail and initial_values['c'][0]==empty and initial_values['debt'][0]==0
    original=child.handlers[5];records=[]
    def mutation(world,projection,choice):
        assert choice.at.cpu().tolist()==[1384]
        result=original(world,projection,choice)
        candidate=result[0].hidden.queues
        assert _signature(candidate)==layout and candidate.kind.data_ptr()!=old.kind.data_ptr()
        candidate_values=queue_values(candidate)
        record('candidate_fok',queue=candidate_values)
        # 原_compact保留死槽B/Q，只将kind置0；c与debt来自原Rust匹配量。
        assert candidate_values['B'][0]==[matched]+tail and candidate_values['Q'][0]==[quote]+tail
        assert candidate_values['kind'][0]==empty and candidate_values['c'][0]==[matched]+tail
        assert candidate_values['debt'][0]==matched
        records.append(dict(fok_at=choice.at.cpu().tolist(),new_queue=True,before=initial_values,
            candidate=candidate_values,cpu_matched_base_micro_units=matched,cpu_matched_quote_micro_units=quote))
        return result
    mutation.__dict__.update(original.__dict__);child.handlers[5]=mutation
    device=child.handlers[4].device_batch;hits=[]
    def run(world,choice,group,gtd,at,enabled,*,before):
        nonlocal recorded_key
        check=choice.phase.cpu().tolist()==[4] and choice.at.cpu().tolist()==[1414]
        if check:
            assert len(records)==1 and _signature(world.hidden.queues)==layout
            # 直接调用原hidden handler的非Stage入口：index_select/原FIFO算法，
            # 不借地址描述或候选scratch，逐原桶执行Auto及输入共同提交。
            expected=owned(world);projection=owned(child.projection)
            oracle=copy.copy(execution);oracle.queue_stage=None;oracle.pending_queue_stage=None
            for expected_at in (1414,):
                hidden,eof=expected.hidden_frontier.scheduled()
                selected=expected.choose(hidden,eof,at,enabled,gtd=gtd,before=before)
                assert selected.at.cpu().tolist()==[expected_at] and selected.phase.cpu().tolist()==[4]
                expected,projection,batches,effect=oracle.hidden(expected,projection,selected,staged=False)
                assert not batches and not effect.fault.any().cpu().item()
                expected,projection,autos=oracle.auto_loop.run(expected,projection,selected.at,selected.present,
                    oracle.sampler,preceding=batches)
                assert not autos
                frontier,fault=Numbers.run_exact_batch(lambda:expected.input_frontier.commit(selected.present))
                assert not fault.any().cpu().item()
                expected=replace(expected,input_frontier=frontier)
        result,count=device(world,choice,group,gtd,at,enabled,before=before)
        current_key=execution.transaction_loop.key
        if current_key!=recorded_key:
            record('key_changed',phase=choice.phase.cpu().tolist(),at=choice.at.cpu().tolist(),
                previous_changes=key_changes(recorded_key,current_key),cold_changes=key_changes(cold_key,current_key))
            recorded_key=current_key
        if check:
            record('after_level',queue=queue_values(result.hidden.queues),committed_buckets=count,
                same_capture=execution.transaction_loop.captured is captured)
            assert count==1 and execution.transaction_loop.captured is captured,'同页同布局新队列必须复用图'
            for left,right in _tensor_pairs(_capacity_like(expected,result),result):
                torch.testing.assert_close(left,right,rtol=0,atol=0)
            for left,right in _tensor_pairs(frozen,old):torch.testing.assert_close(left,right,rtol=0,atol=0)
            after=queue_values(result.hidden.queues)
            # 原Level1000.001→1000.002追加1000微份；应复用耗尽后的首槽。
            assert after['kind'][0]==[1]+tail and after['B'][0]==[1000]+tail and after['Q'][0]==[500]+tail
            assert after['c'][0]==empty and after['debt'][0]==matched
            hits.append(dict(after=after,level_at=[1414],same_capture=True,full_original_handler_state_equal=True,
                old_queue_unchanged=True,committed_buckets=count,closed_at=result.hidden_frontier.closed_at.cpu().tolist()))
        return result,count
    child.handlers[4].device_batch=run
    advance=child.advance_before(until,root.yes)
    assert advance.complete.all().cpu().item() and not advance.fault.any().cpu().item()
    assert len(hits)==1,'必须实际命中原FOK安装后的同页Level'
    assert clock.runtime is entering
    for prior,current in zip(saved[:3],(clock.runtime,clock.projection,clock.public),strict=True):
        for left,right in _tensor_pairs(prior,current):torch.testing.assert_close(left,right,rtol=0,atol=0)
    for prior,current in ((saved[3],clock.source.position),(saved[4],clock.source.fault),(saved[6],root.random.counter),
            (saved[7],torch.cuda.get_rng_state())):torch.testing.assert_close(prior,current,rtol=0,atol=0)
    for left,right in _tensor_pairs(saved[5],clock.gates):torch.testing.assert_close(left,right,rtol=0,atol=0)
    return dict(fok=records,levels=hits,actual_root_unchanged=True)


def probe_frontier_failure(root,progress,resume_validation):
    """原Root成功前缀→1740真实Level私有候选→u64提交耗尽，不装任何候选。"""
    from base_runtime_storage import _owned
    from public_source import public_group_selection
    until=torch.full_like(root.yes,1740,dtype=torch.int64)
    attempts=0
    while True:
        attempts+=1;advance=root.run.advance_before(until,root.yes,progress=progress)
        if resume_validation(advance):continue
        break
    clock=root.clock;execution=root.runtime.execution
    loaded=clock.runtime.hidden_frontier.load_next(clock.hidden_page_buckets)
    clock.runtime=replace(clock.runtime,hidden_frontier=loaded)
    target=until+1;hidden,eof=loaded.scheduled()
    choice=clock.runtime.choose(hidden,eof,target,root.yes,gtd=clock.gtd,before=True)
    assert choice.phase.cpu().tolist()==[4] and choice.at.cpu().tolist()==[1740] and not choice.fault.any().cpu().item()
    _,public=public_group_selection(clock.source.next(),choice,hidden,eof,loaded.columns.next_bucket_at,target,root.yes,before=True)
    assert not public.any().cpu().item()
    frontier=clock.runtime.input_frontier
    maximum=Numbers.run_exact_batch(lambda:frontier.next_sequence.constant((1<<64)-1))
    clock.runtime=replace(clock.runtime,input_frontier=replace(frontier,next_sequence=maximum))
    entering=clock.runtime;projection=clock.projection;public_state=clock.public
    saved=_owned(replace(entering,input_frontier=None),replace(entering,input_frontier=None))
    projection_saved=_owned(projection,projection);public_saved=_owned(public_state,public_state)
    source_position=clock.source.position.clone();random=root.random.counter.clone()
    original=clock.handlers[4];prepared=[];installed=[]
    def handler(world,projected,selected):
        result=original(world,projected,selected)
        private=execution.pending_queue_stage
        assert selected.at.cpu().tolist()==[1740] and private is not None
        rows=private.dirty.nonzero().flatten()
        assert rows.numel()==1
        local=private.take_rows(result[0].hidden.queues,rows)
        assert local.total_base.report_values()[0]==1 and local.kind.cpu().tolist()[0][0]==1
        prepared.append(dict(rows=rows.cpu().tolist(),candidate_base=local.total_base.report_values(),
            candidate_fault=None if result[3] is None else result[3].fault.cpu().tolist()))
        return result
    handler.__dict__.update(original.__dict__)
    def install(world):
        installed.append(True);return original.commit(world)
    handler.commit=install;clock.handlers[4]=handler
    try:failed=clock.advance_before(target,root.yes,progress=progress)
    finally:clock.handlers[4]=original
    assert failed.fault.cpu().tolist()==[1] and not failed.complete.any().cpu().item()
    assert len(prepared)==1 and not installed
    # count0宏返回可重装同值小状态包装；权威FIFO及Projection/public未共同安装。
    assert clock.runtime.hidden.queues is entering.hidden.queues
    assert clock.projection is projection and clock.public is public_state
    for left,right in _tensor_pairs(saved,replace(clock.runtime,input_frontier=None)):
        torch.testing.assert_close(left,right,rtol=0,atol=0)
    for saved_value,current in ((projection_saved,projection),(public_saved,public_state)):
        for left,right in _tensor_pairs(saved_value,current):torch.testing.assert_close(left,right,rtol=0,atol=0)
    assert clock.runtime.input_frontier.next_sequence.report_values()==[(1<<64)-1]
    assert torch.equal(source_position,clock.source.position) and torch.equal(random,root.random.counter)
    return dict(passed=True,scope='original Root prefix then real1740 Level; final input commit fails after private preparation',
        prefix_attempts=attempts,fault=failed.fault.cpu().tolist(),prepared=prepared,no_common_install=True,
        input_sequence_max_preserved=True,source_position=source_position.cpu().tolist(),business_random=random.cpu().tolist(),
        closed_bucket=entering.hidden_frontier.closed_bucket.cpu().tolist())
