"""独立新进程恢复原双根v1，沿原学习/发送链至1721；直接读面作oracle。"""
import hashlib
from validate_actor_continuous import *
from model_train.active_checkpoint import load_joint_checkpoint


def own(value):
    if isinstance(value,torch.Tensor):return value.detach().to('cpu',copy=True)
    if is_dataclass(value):return {f.name:own(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,dict):return {k:own(v) for k,v in value.items()}
    if type(value) is frozenset:return sorted(value)
    if isinstance(value,(tuple,list)):return [own(v) for v in value]
    if value is None or type(value) in (bool,int,float,str):return value
    raise TypeError('直接读面不是基础状态: '+type(value).__name__)


def public_receipt_reference(receipt,prepared):
    """只读正式来源引用；不把Prepared来源拥有者递归成业务副本。"""
    if receipt is None:return None
    source=receipt.source
    return dict(positions=own(receipt.positions),input_sequence=own(receipt.input_sequence),
        groups=[None if int(position)<0 else receipt.original_reference(root)
            for root,position in enumerate(receipt.positions.cpu().tolist())],
        source_is_current_prepared=source is prepared,
        directory=str(source.directory),material=str(source.material),
        prepared_manifest_sha256=hashlib.sha256((source.directory/'manifest.json').read_bytes()).hexdigest(),
        source_manifest_sha256=hashlib.sha256((source.material/'manifest.json').read_bytes()).hexdigest())


def input_frontier_reference(frontier,prepared):
    return dict(next_sequence=own(frontier.next_sequence),run_id=frontier.run_id,
        session_id=frontier.session_id,public_receipt=public_receipt_reference(frontier.public_receipt,prepared))


def task_cutoff_reference(cutoff,prepared):
    from input_frontier import GroupDecisionCutoff
    if isinstance(cutoff,GroupDecisionCutoff):
        return dict(receipt=public_receipt_reference(cutoff.receipt,prepared),observed_at=own(cutoff.observed_at),
            private_feedback_sequence=own(cutoff.private_feedback_sequence),run_id=cutoff.run_id,session_id=cutoff.session_id)
    return own(cutoff)


def direct_oracle(joint):
    roots=[]
    for batch in joint.roots:
        root=batch.root;clock=root.clock;state=clock.runtime
        roots.append(dict(
            source_position=own(clock.source.position),
            hidden_frontier={name:own(getattr(state.hidden_frontier,name)) for name in
                ('next_bucket','next_step','next_reference','applying','closed_at','has_closed','closed_bucket','index_position','fault')},
            gates=own(clock.gates),input_frontier=input_frontier_reference(state.input_frontier,clock.source.prepared),
            sim={name:own(getattr(state,name)) for name in ('orders','account','hidden','fills','calendars','resolution_obligations')},
            market_controls=own(state.market_controls),
            source_resolutions=None if state.source_resolutions is None else {name:own(getattr(state.source_resolutions,name))
                for name in ('reference_indices','terminal_ids','source_times','present','fault')},
            ledger=own(clock.public.ledger),waiting=own(clock.public.waiting),
            population=own(vars(root.population)),
            projection=own(clock.projection),
            hidden=own(root.models.hidden),identities=own(root.models.identities),
            random=own(root.random),request_keys=list(root.request_directory.keys),next_task=own(root.task_requests.next_task),
            tasks=[dict(cutoff=task_cutoff_reference(t.observation.cutoff,clock.source.prepared),stage=own(t.stage),started_at=own(t.started_at),
                completed_at=own(t.completed_at),send_at=own(t.send_at),identity=own(t.request_identity),
                candidate=None if t.sender is None else own(t.sender.publication.candidate),
                receipt=None if t.sender is None else own(t.sender.receipt)) for t in root.models.tasks],
            next_observation=own(root.run.next_observation),last_completed_at=own(root.models.last_completed_at),
            ended=batch.ended,pending_count=batch.pending_count,reward_memory=own(root.reward_memory),
            history=own(root.inputs.memory),endpoints=own(root.inputs.endpoints),
            history_identity_count=root.inputs.history.registry.identity_count,
            history_markets=dict(root.inputs.history.registry.markets)))
    return dict(roots=roots,policy=own(joint.policy.state_dict()),optimizer=own(joint.optimizer.state_dict()),
        version=own(joint.version),consumed_batches=joint.consumed_batches)


def finite_binding(spec,root):
    assert spec['name']=='finite_joint_v1'
    def timing(observation):
        cutoff=str(int(observation.account.cutoff[0]))
        return root.tensor([spec['duration'].get(cutoff,spec['duration_default'])]),root.tensor([spec['dispatch'].get(cutoff,spec['dispatch_default'])])
    def paths(kind,world,selected):
        width=world.hidden.queues.capacity*(selected.size(1) if kind=='taker' else 1)
        return root.tensor([[0]*width])
    return dict(timing=timing,fill_paths=paths)


def assert_same(expected,actual,path='state'):
    if isinstance(expected,torch.Tensor):
        assert isinstance(actual,torch.Tensor) and expected.shape==actual.shape and expected.dtype==actual.dtype,path
        if expected.is_floating_point():torch.testing.assert_close(actual,expected,rtol=1e-4,atol=1e-4,msg=path)
        else:assert torch.equal(expected,actual),path
    elif isinstance(expected,dict):
        assert expected.keys()==actual.keys(),path
        for key in expected:assert_same(expected[key],actual[key],path+'.'+str(key))
    elif isinstance(expected,list):
        assert len(expected)==len(actual),(path,len(expected),len(actual))
        for index,(a,b) in enumerate(zip(expected,actual,strict=True)):assert_same(a,b,path+'.'+str(index))
    else:assert expected==actual,(path,expected,actual)


def advance_reference_tail(joint,stage):
    """保1721检查点，沿同一Root交易截止资格续到原Terminal后的Lifecycle。"""
    a,b=joint.roots;root=a.root
    assert b.ended and not a.ended and int(joint.version[0])==2
    # 未到截止的C1700/C1800真实普通批仍须更新；不提前指定target_end。
    while True:
        advance=root.collect_learning_batch(T([2001]),root.yes,progress=stage)
        if advance is None or advance.learning_boundary:break
        assert root.resume_resources(advance)
        assert not advance.complete.all().cpu().item(),'本代表仍有C1900合法bootstrap'
    while True:
        advance,prepared=root.prepare_learning_batch(root.yes,progress=stage)
        if prepared is not None:break
        assert root.resume_resources(advance)
    # 沿原PPO消费者，但已有辅助梯度观察只用于原前两批，不把本尾段当新矩阵。
    joint.on_prepared=None;joint.on_replayed=None
    updated=joint.update();assert set(updated)=={a} and updated[a].updated and not updated[a].target_end
    assert int(joint.version[0])==3 and joint.consumed_batches==3
    while True:
        advance=root.collect_learning_batch(T([2001]),root.yes,progress=stage)
        if advance is None:raise AssertionError('2001前不应再形成有后继的整批')
        stage('reference_tail_collect_return',complete=advance.complete.cpu().tolist(),
            fault=advance.fault.cpu().tolist(),blocked=advance.blocked.cpu().tolist(),learning_boundary=advance.learning_boundary)
        if advance.complete.all().cpu().item():break
        assert not advance.learning_boundary
        assert root.resume_resources(advance),stored_actual(advance)
    assert not tuple(root.models.eligible_starts())
    assert a.pending_count==1 and [int(s.frozen.cutoff[0]) for s in a.states]==[1900]
    assert not any((t.stage==1).any().cpu().item() for t in root.models.tasks)
    backend=root.runtime.execution;original=backend.reference_loop.run;calls=[]
    def observed(world,choice,start,end,assets):
        source=world.hidden_frontier.columns.source
        before=world.hidden_frontier.next_reference.clone()
        result,index=original(world,choice,start,end,assets)
        tags=source.reference_rows[start:index,0].cpu().tolist()
        if tags:
            assert all(tag!=9 for tag in tags)
            assert torch.equal(backend.reference_loop.captured.inputs.resolutions.present,world.source_resolutions.present)
            assert torch.equal(backend.reference_loop.captured.inputs.obligations.reference_indices,world.resolution_obligations.reference_indices)
            calls.append(dict(start=start,end=index,tags=tags,before=before.cpu().tolist(),
                after=result.hidden_frontier.next_reference.cpu().tolist(),
                source_present=world.source_resolutions.present.cpu().tolist(),
                obligation_reference=world.resolution_obligations.reference_indices.cpu().tolist()))
        return result,index
    backend.reference_loop.run=observed
    try:
        while True:
            advance,reward=root.read_interval_reward(T([31004]),root.yes,progress=stage)
            if reward is not None:break
            assert root.resume_resources(advance)
    finally:backend.reference_loop.run=original
    assert len(calls)==1 and calls[0]['tags']==[4] and calls[0]['source_present']==[[True]]
    assert calls[0]['after']==[calls[0]['end']] and calls[0]['obligation_reference'][0][0]>=0
    assert root.clock.runtime.hidden_frontier.closed_at.cpu().tolist()==[31000]
    assert a.pending_count==1 and not a.ended and b.ended
    report=dict(endpoint=31004,version=int(joint.version[0]),consumed_batches=joint.consumed_batches,
        extra_ordinary_updates=1,last_unconsumed_decisions=a.pending_count,
        target_end=False,trading_until=root.trading_until.cpu().tolist(),calls=calls)
    stage('post_terminal_reference_consumed',**report)
    return report


def advance_auto_tail(joint,stage,artifact):
    """原1721后合法普通续采和Terminal尾段；不指定目标结束。"""
    from validate_auto_device_loop import observe
    root=joint.roots[0].root;owner=root.runtime.execution.auto_loop
    assert owner.current is None
    rows=observe(root.runtime.execution,Path(artifact)/'auto')
    reference=advance_reference_tail(joint,stage)
    torch.cuda.synchronize();started=time.perf_counter()
    while True:
        advance,reward=root.read_interval_reward(T([64001]),root.yes,progress=stage)
        if reward is not None:break
        assert root.resume_resources(advance)
    torch.cuda.synchronize();elapsed=time.perf_counter()-started
    assert len(rows)==1 and rows[0]['completed_markets']==[[True]] and rows[0]['fault']==[[0]],rows
    assert rows[0]['at'][0]>=31000 and all(max(value[0])<64001 for value in rows[0]['received_at'])
    assert root.clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[True]]
    assert root.clock.public.ledger.resolutions.auto_redeemed.cpu().tolist()==[True]
    assert joint.roots[0].pending_count==1 and not joint.roots[0].ended and joint.roots[1].ended
    world,projection=root.clock.runtime,root.clock.projection
    candidate,p,batches=owner.run(world,projection,T([64001]),root.yes,root.sampler)
    assert candidate is world and p is projection and not batches
    report=dict(endpoint=64001,reference_tail=reference,actual_calls=rows,version=int(joint.version[0]),
        consumed_batches=joint.consumed_batches,target_end=False,last_unconsumed_decisions=1,
        repeat_no_payment=True,full_31004_to_64001_seconds_including_public_append_receive=elapsed)
    stage('post_checkpoint_auto_received',**report)
    return report


def advance_maker_tail(joint,stage,artifact):
    """原v2/1721之后的合法Level材料；不追加学习量或指定target_end。"""
    from validate_maker_projection import observe
    artifact=Path(artifact);artifact.mkdir()
    a,b=joint.roots;root=a.root
    assert b.ended and not a.ended and int(joint.version[0])==2
    observations=observe(root.runtime.execution,artifact)
    torch.cuda.synchronize();started=time.perf_counter()
    while True:
        advance,reward=root.read_interval_reward(T([1801]),root.yes,progress=stage)
        if reward is not None:break
        assert root.resume_resources(advance)
    torch.cuda.synchronize();elapsed=time.perf_counter()-started
    times=(1742,1791) if os.environ.get('B_GPU_PUBLIC_TRADE') else (1741,1790)
    if os.environ.get('B_GPU_PUBLIC_TRADE'):
        assert root.clock.runtime.hidden.maker_model=='PUBLIC_TRADE_CONSERVATIVE_FIFO_V1'
        # 同一规范成交按价位返回全部页；空页也保留fault，不以第一页代表整条成交。
        micro=[row for row in observations if row['at']==[times[0]]]
        positive=[row for row in observations if row['at']==[times[1]]]
        assert micro and positive
        assert all(row['fault']==[0] for row in observations)
        assert all(row['emitting_frames']==0 for row in observations if row['at'] in ([1741],[1790]))
        assert sum(row['physical_frames'] for row in micro)>0
        assert sum(row['emitting_frames'] for row in micro)==0 and sum(row['batches'] for row in micro)==0
        assert sum(row['emitting_frames'] for row in positive)>=2 and sum(row['batches'] for row in positive)>=2
        before_micro=micro[-1]['before_public_trade']
        before_positive=positive[-1]['before_public_trade']
        # CPU价格优先：.4可见外部2份仅占本条预算，不从盘口消失。
        # 1742的2.000001份留下1微份pending；1791的12份再扣2份，己方耗损10份。
        delta=lambda before,after,key:sum(after[key])-sum(before[key])
        quantity_report=dict(micro_open_delta=delta(before_micro,micro[-1],'open_base_units'),
            micro_pending_delta=delta(before_micro,micro[-1],'pending_maker_depletion'),
            micro_matched_delta=delta(before_micro,micro[-1],'matched_base_units'),
            positive_open_delta=delta(before_positive,positive[-1],'open_base_units'),
            positive_pending_delta=delta(before_positive,positive[-1],'pending_maker_depletion'),
            positive_matched_delta=delta(before_positive,positive[-1],'matched_base_units'))
        (artifact/'public-trade-quantity-report.json').write_text(json.dumps(quantity_report,indent=2)+'\n')
        assert quantity_report==dict(micro_open_delta=-1,micro_pending_delta=1,micro_matched_delta=0,
            positive_open_delta=-10_000_000,positive_pending_delta=-1,positive_matched_delta=10_000_001),quantity_report
    else:
        micro=next(row for row in observations if row['at']==[times[0]])
        positive=next(row for row in observations if row['at']==[times[1]])
        assert micro['physical_frames']>0 and micro['emitting_frames']==0 and micro['batches']==0
        assert any(value>0 for value in micro['pending_maker_depletion'])
        assert positive['emitting_frames']>=2 and positive['batches']>=2 and positive['fault']==[0]
    assert int(joint.version[0])==2 and joint.consumed_batches==2 and not a.ended and b.ended
    report=dict(endpoint=1801,version=2,consumed_batches=2,target_end=False,
        trading_until=root.trading_until.cpu().tolist(),micro=micro,positive=positive,
        full_endpoint_seconds_including_SIM_projection_copy_loop_owned_public_append_receive=elapsed)
    stage('post_checkpoint_maker_consumed',**report)
    return report


def main():
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('material');parser.add_argument('output');args=parser.parse_args()
    artifact=Path(args.material);output=Path(args.output);output.mkdir()
    def stage(name,**row):print(json.dumps(dict(stage=name,**row)),flush=True)
    path=Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so'
    spec=importlib.util.spec_from_file_location('model_native_inputs',path);codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    joint=load_joint_checkpoint(artifact/'checkpoints',codec,finite_binding,progress=stage)
    if os.environ.get('B_GPU_PUBLIC_STAGE_APPEND'):
        from validate_public_append import observe as observe_append
        for index,batch in enumerate(joint.roots):observe_append(batch.root.clock,output/('public-append-'+str(index)))
    assert int(joint.version[0])==1 and joint.consumed_batches==1
    if os.environ.get('B_GPU_MLFLOW'):
        from mlflow import MlflowClient
        assert joint.reporter is not None and joint.reporter.pending is not None
        saved_report=json.loads(json.dumps(joint.reporter.pending))
        joint.reporter.write_pending()
        assert joint.reporter.pending is None and joint.consumed_batches==1
        assert saved_report['step']==1
        stage('restored_pending_report_written',step=1,run_id=joint.reporter.run_id)
    first,second=[batch.root for batch in joint.roots];a,b=joint.roots
    restored_public_position=first.clock.source.position.clone()
    assert first.clock.public_loop.captured is None
    expected=torch.load(artifact/'uninterrupted-oracle.pt',map_location='cpu',weights_only=True)
    # 原不中断分支实际位置证明本段是否有公开消费；合法静默不要求图存在。
    public_consumption_expected=bool((expected['roots'][0]['source_position']>restored_public_position.cpu()).any())
    assert b.ended and not a.ended and not a.states and not b.states
    starts=[[],[]];sent=[[],[]]
    for index,root in enumerate((first,second)):
        root.models.on_started=lambda task,index=index:starts[index].append(dict(cutoff=int(task.observation.account.cutoff[0]),
            at=int(task.started_at[0]),version=task.evaluation.policy_version.cpu().tolist()))
        def completed(task,index=index):
            if task.sender is not None:sent[index].append(dict(cutoff=int(task.observation.account.cutoff[0]),
                at=int(task.send_at[0]),path=stored_actual(task.sender.publication.candidate),request_contexts=stored_actual(task.request_identity.contexts)))
        root.models.on_completed=completed
    while True:
        result=first.collect_learning_batch(T([2001]),first.yes,progress=stage)
        if result.learning_boundary:break
        assert first.resume_resources(result)
    while True:
        advance,prepared=first.prepare_learning_batch(first.yes,progress=stage)
        if prepared is not None:break
        assert first.resume_resources(advance)
    stage('restored_update_begin')
    result=joint.update();assert set(result)=={a} and result[a].updated
    stage('restored_update_end',version=int(joint.version[0]))
    if os.environ.get('B_GPU_MLFLOW'):
        client=MlflowClient(tracking_uri=joint.reporter.tracking_uri)
        history=client.get_metric_history(joint.reporter.run_id,'decisions')
        assert sorted((m.step,m.value) for m in history)==[(1,8.),(1,8.),(2,2.),(2,2.)]
        assert joint.reporter.pending is None
        stage('restored_mlflow_same_run_history',steps=[m.step for m in history],
            historical_duplicate_steps_preserved=True,mlflow_not_business_restore_authority=True)
    while True:
        advance=first.run.advance_before(T([1721]),first.yes,progress=stage)
        if advance.complete.all().cpu().item():break
        assert first.resume_resources(advance)
    if public_consumption_expected:
        assert first.clock.public_loop.captured is not None
        assert (first.clock.source.position>restored_public_position).any().cpu().item()
        stage('restored_public_loop_consumed',position=first.clock.source.position.cpu().tolist())
    else:
        stage('restored_public_source_static',position=first.clock.source.position.cpu().tolist())
    actual=direct_oracle(joint);actual['starts']=starts;actual['sent']=sent
    assert_same(expected,actual)
    torch.save(actual,output/'restored-oracle.pt')
    if os.environ.get('B_GPU_HIDDEN_REFERENCE_LOOP'):
        tail=advance_reference_tail(joint,stage)
        actual_tail=direct_oracle(joint);actual_tail['starts']=starts;actual_tail['sent']=sent
        expected_tail=torch.load(artifact/'uninterrupted-reference-tail-oracle.pt',map_location='cpu',weights_only=True)
        assert_same(expected_tail,actual_tail)
        torch.save(actual_tail,output/'restored-reference-tail-oracle.pt')
        (output/'reference-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
    if os.environ.get('B_GPU_MAKER_PROJECTION_ROOT'):
        tail=advance_maker_tail(joint,stage,output/'maker-post-checkpoint')
        actual_tail=direct_oracle(joint);actual_tail['starts']=starts;actual_tail['sent']=sent
        expected_tail=torch.load(artifact/'uninterrupted-maker-tail-oracle.pt',map_location='cpu',weights_only=True)
        assert_same(expected_tail,actual_tail)
        torch.save(actual_tail,output/'restored-maker-tail-oracle.pt')
        (output/'maker-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
    if os.environ.get('B_GPU_AUTO_CHECKPOINT'):
        tail=advance_auto_tail(joint,stage,output)
        actual_tail=direct_oracle(joint);actual_tail['starts']=starts;actual_tail['sent']=sent
        expected_tail=torch.load(artifact/'uninterrupted-auto-tail-oracle.pt',map_location='cpu',weights_only=True)
        assert_same(expected_tail,actual_tail)
        torch.save(actual_tail,output/'restored-auto-tail-oracle.pt')
        (output/'auto-tail-report.json').write_text(json.dumps(tail,indent=2)+'\n')
    (output/'report.json').write_text(json.dumps(dict(version=int(actual['version'][0]),consumed_batches=actual['consumed_batches'],
        endpoint=1721,new_process=True,old_send_1720=next(row for row in sent[0] if row['cutoff']==1300),
        reference_tail=tail if os.environ.get('B_GPU_HIDDEN_REFERENCE_LOOP') else None,
        all_business_and_model_fields_compared=True,scope='finite two-root active restore, no crossUTC/formal training'),indent=2)+'\n')
    stage('active_restore_complete',endpoint=1721,version=int(actual['version'][0]))

if __name__=='__main__':main()
