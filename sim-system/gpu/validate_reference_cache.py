"""真实GTC之后两条TypedRaw TWAP，经原连续hidden入口核图复用。"""
from dataclasses import fields,is_dataclass,replace
from pathlib import Path
import json,os,time
import torch
from queues import Numbers,ExactInt
from base_runtime_storage import BaseRuntimeStorage
from continuous_execution import ContinuousExecution
from hidden_reference_loop import _inputs,_CapturedReferences


def same(a,b,path='runtime'):
    if isinstance(a,ExactInt):
        width=max(a.words.size(1),b.words.size(1));a=a.with_capacity(width);b=b.with_capacity(width)
        for name in ('words','sign','fault'):same(getattr(a,name),getattr(b,name),path+'.'+name)
    elif isinstance(a,torch.Tensor):assert torch.equal(a,b),path
    elif is_dataclass(a):
        for f in fields(a):
            if f.name not in ('source','columns'):same(getattr(a,f.name),getattr(b,f.name),path+'.'+f.name)
    elif isinstance(a,(tuple,list)):
        assert len(a)==len(b),path
        for i,(left,right) in enumerate(zip(a,b,strict=True)):same(left,right,path+str(i))
    else:assert a==b,path


def verify_cache(source,initial,projection,sampler,rebate):
    T=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    backend=ContinuousExecution(None,None,None,sampler,None,T([0,1]),T([source['market_end']]*2),rebate,
        None,T([0,0]),T([0,1]),None,None)
    assert (initial.hidden.queues.kind==2).sum().cpu().item()==1
    original=BaseRuntimeStorage(initial,initial).snapshot()
    world=initial;held=None;captured=None;records=[];costs=[];page_records=[]
    original_init=_CapturedReferences.__init__;original_replay=_CapturedReferences.replay
    def timed_init(owner,*args,**kwargs):
        torch.cuda.synchronize();start=time.perf_counter();original_init(owner,*args,**kwargs)
        torch.cuda.synchronize();costs.append(dict(stage='cold_capture',seconds=time.perf_counter()-start))
    def timed_replay(owner,*args,**kwargs):
        torch.cuda.synchronize();start=time.perf_counter();value=original_replay(owner,*args,**kwargs)
        torch.cuda.synchronize();costs.append(dict(stage='five_inputs_state_choice_copy_loop_owned_output',seconds=time.perf_counter()-start))
        return value
    try:
        _CapturedReferences.__init__=timed_init;_CapturedReferences.replay=timed_replay
        for at in (24000,25000):
            choice=world.choose(None,None,T([at]),T([True],torch.bool))
            assert choice.enabled(4).cpu().tolist()==[True]
            entered=world.begin_hidden_bucket(choice)
            bucket=int(entered.hidden_frontier.next_bucket.cpu()[0])
            begin=int(entered.hidden_frontier.next_reference.cpu()[0]);end=int(entered.hidden_frontier.columns.reference_offsets[bucket+1].cpu())
            assert end-begin==1 and int(entered.hidden_frontier.columns.source.reference_rows[begin,0].cpu())==1
            pure=entered
            for index in range(begin,end):pure=Numbers.run_exact_batch(lambda:pure.apply_hidden_reference(
                backend.refresh(pure,choice),T([index]),backend.source_assets))
            pure,_=Numbers.run_exact_batch(pure.finish_hidden_bucket)
            torch.cuda.synchronize();start=time.perf_counter()
            result,out_projection,batches,effect=backend.hidden(world,projection,choice)
            torch.cuda.synchronize();seconds=time.perf_counter()-start
            same(result,pure);same(out_projection,projection);assert not batches and effect is None
            if captured is None:captured=backend.reference_loop.captured
            else:assert backend.reference_loop.captured is captured,'同页同布局相邻桶必须复用本图'
            same(_inputs(entered),captured.inputs,'current_five_inputs')
            for name in ('next_bucket','applying'):
                assert getattr(result.hidden_frontier,name) is not getattr(captured.inputs,name)
            for name in ('initialized','matching_closed','book_closed'):
                assert getattr(result.hidden,name) is not getattr(captured.inputs,name)
            if held is not None:same(old_result,held,'old_output_after_next_replay')
            records.append(dict(at=at,bucket=bucket,begin=begin,end=end,handler_seconds=seconds,
                queue_shape=list(result.hidden.queues.kind.shape),account_slots=int((result.hidden.queues.kind==2).sum().cpu()),
                next_bucket=result.hidden_frontier.next_bucket.cpu().tolist(),fault=result.hidden_frontier.fault.cpu().tolist()))
            old_result=result;held=BaseRuntimeStorage(result,result).snapshot();world=result
        assert sum(value['stage']=='cold_capture' for value in costs)==1
        # 恢复的owner按原接口新建；同一业务状态首次冷建、随后同样跨桶复用。
        rebuilt=ContinuousExecution(None,None,None,sampler,None,T([0,1]),T([source['market_end']]*2),rebate,
            None,T([0,0]),T([0,1]),None,None)
        replayed=initial;new_capture=None
        for at in (24000,25000):
            choice=replayed.choose(None,None,T([at]),T([True],torch.bool))
            replayed,p,b,e=rebuilt.hidden(replayed,projection,choice)
            if new_capture is None:new_capture=rebuilt.reference_loop.captured
            else:assert rebuilt.reference_loop.captured is new_capture
        same(replayed,world);same(initial,original);same(old_result,held)
        assert sum(value['stage']=='cold_capture' for value in costs)==2
        # 同一真实材料换物理表示；已提交22000前沿及global index保持。
        from hidden_device_page import build_hidden_device_columns,PreparedHiddenMaterial
        from hidden_inputs import HiddenPmBlock
        from hidden_frontier import HiddenBucketColumns
        destination=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'cache-hidden-device-columns'
        build_hidden_device_columns(initial.hidden_frontier.columns.source,destination,source['source_binding'])
        prepared=PreparedHiddenMaterial(destination,source['source_binding'])
        first=HiddenPmBlock.from_prepared(prepared.page(0,3))
        paged=replace(initial,hidden_frontier=replace(initial.hidden_frontier,
            columns=HiddenBucketColumns.from_block(first)))
        page_owner=ContinuousExecution(None,None,None,sampler,None,T([0,1]),T([source['market_end']]*2),rebate,
            None,T([0,0]),T([0,1]),None,None)
        old_capture=None
        for at in (24000,25000):
            choice=paged.choose(None,None,T([at]),T([True],torch.bool))
            assert choice.enabled(4).cpu().tolist()==[True]
            entered=paged.begin_hidden_bucket(choice)
            bucket=int(entered.hidden_frontier.next_bucket.cpu()[0])
            begin=int(entered.hidden_frontier.next_reference.cpu()[0]);end=int(entered.hidden_frontier.columns.reference_offsets[bucket+1].cpu())
            expected=entered
            for index in range(begin,end):expected=Numbers.run_exact_batch(lambda:expected.apply_hidden_reference(
                page_owner.refresh(expected,choice),T([index]),page_owner.source_assets))
            expected,_=Numbers.run_exact_batch(expected.finish_hidden_bucket)
            paged,p,b,e=page_owner.hidden(paged,projection,choice)
            same(paged,expected);same(p,projection);assert not b and e is None
            assert page_owner.reference_loop.captured is not old_capture
            old_capture=page_owner.reference_loop.captured
            same(_inputs(entered),old_capture.inputs)
            page_records.append(dict(at=at,bucket_base=paged.hidden_frontier.columns.bucket_base,
                next_bucket=paged.hidden_frontier.next_bucket.cpu().tolist(),cold_rebound=True))
            if at==24000:
                assert not paged.hidden_frontier.columns.end_of_input
                next_frontier=paged.hidden_frontier.load_next(1)
                assert next_frontier.columns is not paged.hidden_frontier.columns
                same(next_frontier.index_position,paged.hidden_frontier.index_position)
                paged=replace(paged,hidden_frontier=next_frontier)
        for name in ('hidden','orders','account','fills','calendars','market_controls','source_resolutions','resolution_obligations'):
            same(getattr(paged,name),getattr(world,name),'paged_'+name)
        same(paged.hidden_frontier.index_position,world.hidden_frontier.index_position)
        same(initial,original);same(old_result,held)
        assert sum(value['stage']=='cold_capture' for value in costs)==4
    finally:
        _CapturedReferences.__init__=original_init;_CapturedReferences.replay=original_replay
    report=dict(scope='real_nonempty_GTC_two_TypedRaw_TWAP_buckets_current_handler_and_pure_reference',
        records=records,costs=costs,captures_per_owner=1,replays_per_owner=2,old_outputs_and_input_equal=True,
        new_owner_rebuild=True,page_records=page_records,page_change_cold_rebind=True,
        full_new_process_checkpoint=False,run11_background=False)
    (Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'reference-cache-report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'reference_cache':report}),flush=True)


def verify_tail(verify,*args):
    """只观察原尾段实际引用消费者，保Terminal后来源/义务重新绑定。"""
    from hidden_reference_loop import HiddenReferenceDeviceLoop
    original=HiddenReferenceDeviceLoop.run;trace=[]
    def observed(owner,world,choice,start,end,assets):
        previous=owner.captured
        result,index=original(owner,world,choice,start,end,assets)
        captured=owner.captured
        same(_inputs(world),captured.inputs)
        same(captured.inputs.resolutions,world.source_resolutions)
        if world.resolution_obligations is not None:
            for name in ('reference_indices','pending','sequences','auto_redeemed'):
                same(getattr(captured.inputs.obligations,name),getattr(world.resolution_obligations,name))
        block=world.hidden_frontier.columns.source
        trace.append(dict(at=choice.at.cpu().tolist(),start=start,end=end,returned=index,
            tags=block.reference_rows[start:end,0].cpu().tolist(),cold_rebound=captured is not previous,
            source_present=world.source_resolutions.present.cpu().tolist()))
        return result,index
    try:
        HiddenReferenceDeviceLoop.run=observed
        verify(*args)
    finally:HiddenReferenceDeviceLoop.run=original
    after=[item for item in trace if item['at']==[31000] and item['tags']==[4]]
    assert len(after)==1 and after[0]['source_present']==[[True]]
    (Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'reference-cache-tail-bindings.json').write_text(json.dumps(trace,indent=2)+'\n')
