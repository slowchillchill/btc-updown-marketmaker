"""原连续Maker页的同输入逐frame事实对照；诊断不驱动经济入口。"""
from dataclasses import fields,is_dataclass,replace
from pathlib import Path
import json,time,os
import torch
from queues import ExactInt,Numbers
from base_runtime_storage import _owned
from hidden_base_execution import coordinate_maker_frame


def equal(left,right,path='value'):
    if isinstance(left,ExactInt):
        width=max(left.words.size(1),right.words.size(1))
        for name in ('words','sign','fault'):
            a=getattr(left.with_capacity(width),name);b=getattr(right.with_capacity(width),name)
            assert torch.equal(a,b),path+'.'+name
    elif isinstance(left,torch.Tensor):
        if left.ndim==2 and right.ndim==2 and left.size(0)==right.size(0) and left.dtype==torch.int32:
            width=max(left.size(1),right.size(1))
            left=torch.nn.functional.pad(left,(0,width-left.size(1)))
            right=torch.nn.functional.pad(right,(0,width-right.size(1)))
        assert torch.equal(left,right),path
    elif is_dataclass(left):
        for field in fields(left):
            a,b=getattr(left,field.name),getattr(right,field.name)
            if field.name in ('columns','resolution_columns'):assert a is b,path+'.'+field.name
            else:equal(a,b,path+'.'+field.name)
    elif isinstance(left,(tuple,list)):
        assert len(left)==len(right),path
        for i,(a,b) in enumerate(zip(left,right,strict=True)):equal(a,b,path+str(i))
    else:assert left==right,path


def raw_equal(left,right,path='owned'):
    """同一输入／旧输出与自身快照比较全部缓冲，不沿跨布局补零规则。"""
    if isinstance(left,torch.Tensor):
        assert left.shape==right.shape and left.dtype==right.dtype and torch.equal(left,right),path
    elif is_dataclass(left):
        for field in fields(left):
            a,b=getattr(left,field.name),getattr(right,field.name)
            if field.name in ('columns','resolution_columns'):assert a is b,path+'.'+field.name
            else:raw_equal(a,b,path+'.'+field.name)
    elif isinstance(left,(tuple,list)):
        assert len(left)==len(right),path
        for n,(a,b) in enumerate(zip(left,right,strict=True)):raw_equal(a,b,path+'.'+str(n))
    else:assert left==right,path


def compare_projection(original,actual,old_batches,batches,artifact=None):
    for field in fields(original):
        if field.name!='normalizer':equal(getattr(original,field.name),getattr(actual,field.name),'projection.'+field.name)
    if original.normalizer is None:
        assert actual.normalizer is None and not batches
        return
    equal(original.normalizer.fault,actual.normalizer.fault,'normalizer.fault')
    # 未发射整frame的物理bank尾行不发布；每个真实自然批仍保全部原槽。
    meaningful=[batch for batch in old_batches if bool((batch.emitted.any()|batch.fault.any()).cpu())]
    assert len(meaningful)==len(batches),(len(meaningful),len(batches))
    mapping={}
    for old,new in zip(meaningful,batches,strict=True):
        for field in fields(old):
            if field.name not in ('rows','normalized'):equal(getattr(old,field.name),getattr(new,field.name),'batch.'+field.name)
        equal(old.normalized.outcomes,new.normalized.outcomes,'normalized.outcomes')
        equal(old.normalized.emitted,new.normalized.emitted,'normalized.emitted')
        equal(old.normalized.fault,new.normalized.fault,'normalized.fault')
        for a,b in zip(old.rows.flatten().cpu().tolist(),new.rows.flatten().cpu().tolist(),strict=True):
            mapping[a]=b
            for field in fields(original.normalizer.bank):
                if field.name=='declared_schemas':continue
                left=getattr(original.normalizer.bank,field.name)[a:a+1]
                right=getattr(actual.normalizer.bank,field.name)[b:b+1]
                if field.name in ('logical_keys','economic_keys','semantic','cancellation'):
                    width=max(left.size(1),right.size(1))
                    padded_left=torch.nn.functional.pad(left,(0,width-left.size(1)))
                    padded_right=torch.nn.functional.pad(right,(0,width-right.size(1)))
                    difference=(padded_left!=padded_right).nonzero().cpu().tolist()
                    if artifact is not None:
                        row=dict(field=field.name,original_row=a,actual_row=b,original_shape=list(left.shape),
                            actual_shape=list(right.shape),original_tokens=left.cpu().tolist(),actual_tokens=right.cpu().tolist(),
                            padded_difference_coordinates=difference)
                        with Path(artifact,'bank-comparison-rows.jsonl').open('a') as output:
                            output.write(json.dumps(row)+'\n')
                    assert torch.equal(padded_left,padded_right),'bank.'+field.name
                else:equal(left,right,'bank.'+field.name)
    for name in ('logical_rows','economic_rows'):
        a=getattr(original.normalizer,name);b=getattr(actual.normalizer,name)
        for root in range(a.size(0)):
            old=[int(x) for x in a[root].cpu().tolist() if x>=0]
            new=[int(x) for x in b[root].cpu().tolist() if x>=0]
            assert [mapping.get(row,row) for row in old]==new,name


def observe(backend,artifact,*,resources=False):
    """挂原真实消费者调用，oracle只调原Projection纯函数。"""
    owner=backend.maker_projection_loop
    # 同一有限代表进入后继端点时替换先前观察包装，业务仍只执行原类方法一次。
    original=type(owner).run.__get__(owner,type(owner));observations=[];held=[]
    def execute(world,projection,page,at,rebate,sampler,assets):
        inputs=(world.orders,world.fills,world.calendars,world.account,projection,page,at,rebate,assets)
        before=_owned(inputs,inputs)
        frontier=world.input_frontier
        frontier_values=None if frontier is None else _owned(frontier.next_sequence,frontier.next_sequence)
        receipt=None if frontier is None else frontier.public_receipt
        receipt_values=None if receipt is None else _owned((receipt.positions,receipt.input_sequence),
            (receipt.positions,receipt.input_sequence))
        source=None if receipt is None else receipt.source
        source_page=None if source is None else (source.current,source.current_index)
        previous_cache=owner.current;previous_key=owner.key
        torch.cuda.synchronize();started=time.perf_counter()
        actual,batches,fault=original(world,projection,page,at,rebate,sampler,assets)
        torch.cuda.synchronize();elapsed=time.perf_counter()-started
        count=int(page.count.cpu());reference=projection;old=[]
        feedback=bool(page.feedback_present().cpu())
        projection_fault=projection.emitter.fault|projection.emitter.next_private_sequence.fault|projection.emitter.next_batch_sequence.fault
        if projection.normalizer is not None:projection_fault|=projection.normalizer.fault
        if feedback or bool(projection_fault.any().cpu()):
            for index in range(count):
                frame=coordinate_maker_frame(page.read_device(at.new_tensor(index)),world.account,
                    (world.account.fault|world.orders.fault)==0,world.account.fault|world.orders.fault)
                reference,batch=Numbers.run_exact_batch(lambda:reference.fill_from_origin(sampler,world.orders,
                    world.fills,frame,assets,world.calendars,rebate,at))
                old.append(batch)
                if bool(batch.fault.any().cpu()):break
        compare_projection(reference,actual,old,batches,artifact)
        equal(before,inputs,'original_consumer_input')
        assert world.input_frontier is frontier
        if frontier is not None:equal(frontier.next_sequence,frontier_values,'input_sequence')
        if receipt is not None:
            assert frontier.public_receipt is receipt and receipt.source is source
            equal((receipt.positions,receipt.input_sequence),receipt_values,'receipt')
            assert source.current is source_page[0] and source.current_index==source_page[1]
        for live,saved in held:equal(live,saved,'previous_output')
        held.append(((actual,batches),_owned((actual,batches),(actual,batches))))
        events=[page.read_device(at.new_tensor(i)).event.emitted.cpu().tolist() for i in range(count)]
        row=dict(at=at.cpu().tolist(),physical_frames=count,capacity=page.capacity,
            emitting_frames=sum(any(v) for v in events),batches=len(batches),fault=fault.cpu().tolist(),
            queue_shape=list(world.hidden.queues.kind.shape),occupied_segments=int((world.hidden.queues.kind!=0).sum().cpu()),
            handler_seconds_including_cold_copy_replay_owned_output=elapsed,required_words=0 if count==0 else int(owner.current.required.cpu()),
            word_capacity=owner.width,input_and_prior_outputs_unchanged=True,original_per_frame_facts_equal=True)
        labels=('projection_layout','sim_page_layout','sampler','asset_tokens','word_capacity','device','stream')
        row['cold_rebuilt']=previous_cache is not owner.current
        row['cold_rebind_reasons']=[] if count==0 else ['first_layout'] if previous_key is None else ['consumer_mode'] if len(previous_key)!=len(owner.key) else [name for name,a,b in
            zip(labels,previous_key,owner.key,strict=True) if a!=b]
        row['bank_rows_before']=0 if projection.normalizer is None else projection.normalizer.bank.kinds.numel()
        from validate_base_execution import values
        row['pending_maker_depletion']=values(world.orders.quantities.pending_maker_depletion_base_units)
        if os.environ.get('B_GPU_PUBLIC_TRADE'):
            row['open_base_units']=values(world.orders.quantities.open_base_units)
            row['matched_base_units']=values(world.orders.quantities.matched_base_units)
        if int(at[0]) in (1740,1741,1780,1790):
            from validate_base_execution import rational
            from fractions import Fraction
            numerator,denominator=values(world.orders.prices.numerator),values(world.orders.prices.denominator)
            quantities=values(world.orders.quantities.open_base_units)
            row['actual_open_orders']=[dict(slot=i,asset=asset,accepted_at=accepted,
                price=str(Fraction(numerator[i],denominator[i])),open_base_units=quantities[i])
                for i,(asset,accepted,on) in enumerate(zip(world.orders.assets.flatten().cpu().tolist(),
                    world.orders.accepted_at.flatten().cpu().tolist(),world.orders.occupied.flatten().cpu().tolist(),strict=True)) if on]
        observations.append(row)
        print(json.dumps(dict(stage='actual_maker_projection_page',**row)),flush=True)
        if resources and feedback and not getattr(execute,'probed',False):
            execute.probed=True
            resource_probe(world,projection,page,at,rebate,sampler,assets,actual,batches,artifact)
        Path(artifact,'maker-page-report.json').write_text(json.dumps(observations,indent=2)+'\n')
        return actual,batches,fault
    original_effects=type(owner).run_effects.__get__(owner,type(owner))
    def execute_effects(world,projection,effects,frames,at,rebate,sampler,assets):
        inputs=(world.orders,world.fills,world.calendars,world.account,projection,effects,at,rebate,assets)
        before=_owned(inputs,inputs)
        frontier=world.input_frontier;sequence=None if frontier is None else _owned(frontier.next_sequence,frontier.next_sequence)
        receipt=None if frontier is None else frontier.public_receipt
        receipt_values=None if receipt is None else _owned((receipt.positions,receipt.input_sequence),(receipt.positions,receipt.input_sequence))
        source=None if receipt is None else receipt.source
        source_page=None if source is None else (source.current,source.current_index)
        previous_cache,previous_key=owner.current,owner.key
        torch.cuda.synchronize();begin=time.perf_counter()
        actual,batches,stopped,fault=original_effects(world,projection,effects,frames,at,rebate,sampler,assets)
        torch.cuda.synchronize();elapsed=time.perf_counter()-begin
        reference=projection;old=[];events=[];physical=0;stop=None;feedback_any=False;expected_fault=torch.zeros_like(at,dtype=torch.int32)
        for outer in range(0 if effects is None else int(effects.count.cpu())):
            stop=effects.read_device(at.new_tensor(outer))
            if bool(stop.fault.any().cpu()):expected_fault=stop.fault;break
            page=stop.maker_page;count=int(page.count.cpu());physical+=count
            events.extend(page.read_device(at.new_tensor(i)).event.emitted.cpu().tolist() for i in range(count))
            feedback=bool(page.feedback_present().cpu());feedback_any|=feedback
            projection_fault=reference.emitter.fault|reference.emitter.next_private_sequence.fault|reference.emitter.next_batch_sequence.fault
            if reference.normalizer is not None:projection_fault|=reference.normalizer.fault
            if feedback or bool(projection_fault.any().cpu()):
                for index in range(count):
                    frame=coordinate_maker_frame(page.read_device(at.new_tensor(index)),world.account,
                        (world.account.fault|world.orders.fault)==0,world.account.fault|world.orders.fault)
                    reference,batch=Numbers.run_exact_batch(lambda:reference.fill_from_origin(sampler,world.orders,
                        world.fills,frame,assets,world.calendars,rebate,at))
                    old.append(batch)
                    if bool(batch.fault.any().cpu()):break
            if old and bool(old[-1].fault.any().cpu()):expected_fault=old[-1].fault;break
        compare_projection(reference,actual,old,batches,artifact)
        equal(expected_fault,fault,'effects_fault');equal(stop,stopped,'stopped_effect')
        raw_equal(before,inputs,'effects_original_consumer_input')
        assert world.input_frontier is frontier
        if frontier is not None:equal(frontier.next_sequence,sequence,'effects_input_sequence')
        if receipt is not None:
            assert frontier.public_receipt is receipt and receipt.source is source
            equal((receipt.positions,receipt.input_sequence),receipt_values,'effects_receipt')
            assert source.current is source_page[0] and source.current_index==source_page[1]
        for live,saved in held:raw_equal(live,saved,'effects_previous_output')
        held.append(((actual,batches,stopped),_owned((actual,batches,stopped),(actual,batches,stopped))))
        row=dict(at=at.cpu().tolist(),physical_frames=physical,available_physical_frames=frames,
            capacity=0 if effects is None else effects.template.maker_page.capacity,
            effect_count=0 if effects is None else int(effects.count.cpu()),emitting_frames=sum(any(v) for v in events),
            batches=len(batches),fault=fault.cpu().tolist(),queue_shape=list(world.hidden.queues.kind.shape),
            occupied_segments=int((world.hidden.queues.kind!=0).sum().cpu()),
            handler_seconds_including_cold_copy_replay_owned_output=elapsed,
            required_words=0 if frames==0 else int(owner.current.required.cpu()),word_capacity=owner.width,
            input_and_prior_outputs_unchanged=True,original_per_frame_facts_equal=True,
            cold_rebuilt=previous_cache is not owner.current,
            bank_rows_before=0 if projection.normalizer is None else projection.normalizer.bank.kinds.numel())
        labels=('consumer_mode','physical_frame_count','projection_layout','sim_page_layout','sampler','asset_tokens','word_capacity','device','stream')
        row['cold_rebind_reasons']=[] if frames==0 else ['first_layout'] if previous_key is None else ['consumer_mode'] if len(previous_key)!=len(owner.key) else [name for name,a,b in zip(labels,previous_key,owner.key,strict=True) if a!=b]
        from validate_base_execution import values
        row['pending_maker_depletion']=values(world.orders.quantities.pending_maker_depletion_base_units)
        if os.environ.get('B_GPU_PUBLIC_TRADE'):
            row['open_base_units']=values(world.orders.quantities.open_base_units)
            row['matched_base_units']=values(world.orders.quantities.matched_base_units)
        observations.append(row);print(json.dumps(dict(stage='actual_maker_projection_page',**row)),flush=True)
        if resources and feedback_any and not getattr(execute_effects,'probed',False):
            execute_effects.probed=True
            resource_probe(world,projection,effects,at,rebate,sampler,assets,actual,batches,artifact,effect_frames=frames)
        Path(artifact,'maker-page-report.json').write_text(json.dumps(observations,indent=2)+'\n')
        return actual,batches,stopped,fault
    owner.run=execute;owner.run_effects=execute_effects
    return observations


def resource_probe(world,projection,page,at,rebate,sampler,assets,expected,batches,artifact,*,effect_frames=None):
    """原Numbers精度注入只核物理续接；不是经济材料自然触发的增长。"""
    from maker_projection_loop import MakerProjectionDeviceLoop,_CapturedMakerProjection
    from projection import Projection
    base=Projection.fill_from_origin;visited=[]
    def compute(self,*args,**kwargs):
        frame=args[3]
        # 第一实际反馈帧才注入原8字乘法请求；不改任何返回经济候选。
        value=frame.fill_sequence
        marker=value.constant(1).with_capacity(4)
        marker=ExactInt(marker.words.clone(),marker.sign.clone(),marker.fault.clone())
        marker.words[:,3].fill_(1<<30)
        request=marker*marker
        return base(self,*args,**kwargs)
    Projection.fill_from_origin=compute
    live_inputs=(world.orders,world.fills,world.calendars,world.account,projection,page)
    inputs=_owned(live_inputs,live_inputs)
    frontier=world.input_frontier
    frontier_values=None if frontier is None else _owned(frontier.next_sequence,frontier.next_sequence)
    receipt=None if frontier is None else frontier.public_receipt
    receipt_values=None if receipt is None else _owned((receipt.positions,receipt.input_sequence),(receipt.positions,receipt.input_sequence))
    source=None if receipt is None else receipt.source
    source_page=None if source is None else (source.current,source.current_index)
    init=_CapturedMakerProjection.__init__
    def capture(self,*args,**kwargs):
        init(self,*args,**kwargs);visited.append(self)
    _CapturedMakerProjection.__init__=capture
    try:
        probe=MakerProjectionDeviceLoop()
        def consume():
            if effect_frames is None:
                a,b,f=probe.run(world,projection,page,at,rebate,sampler,assets);return a,b,None,f
            return probe.run_effects(world,projection,page,effect_frames,at,rebate,sampler,assets)
        actual,new,stopped,fault=consume()
        compare_projection(expected,actual,batches,new,artifact)
        assert [v.width for v in visited]==[4,8]
        assert int(visited[0].required.cpu())==8 and not bool(visited[0].fits.cpu())
        assert int(visited[1].required.cpu())==0 and bool(visited[1].fits.cpu())
        assert int(visited[0].page.count.cpu())==0
        retained=_owned((actual,new,stopped),(actual,new,stopped))
        again,again_batches,_,_=consume()
        raw_equal((actual,new,stopped),retained,'resource_previous_output')
        raw_equal(live_inputs,inputs,'resource_original_input')
        assert world.input_frontier is frontier
        if frontier is not None:equal(frontier.next_sequence,frontier_values,'resource_input_sequence')
        if receipt is not None:
            assert frontier.public_receipt is receipt and receipt.source is source
            raw_equal((receipt.positions,receipt.input_sequence),receipt_values,'resource_receipt')
            assert source.current is source_page[0] and source.current_index==source_page[1]
        compare_projection(expected,again,batches,again_batches,artifact)
        report=dict(scope='original exact multiplication resource injection; economic candidate unchanged',
            widths=[v.width for v in visited],required=[int(v.required.cpu()) for v in visited],
            fits=[bool(v.fits.cpu()) for v in visited],no_insufficient_output_installed=True,
            recomputed_original_page=True,old_output_survives_next_replay=True,
            original_input_unchanged=True,fault=fault.cpu().tolist())
        Path(artifact,'maker-page-resource-report.json').write_text(json.dumps(report,indent=2)+'\n')
    finally:
        Projection.fill_from_origin=base;_CapturedMakerProjection.__init__=init


def common_fault(clock,shared,enabled,artifact,progress):
    """在原有正Maker桶前注入Projection原fault，核send_clock整阶段不安装。"""
    at=enabled.new_tensor([27000],dtype=torch.int64)
    advance=shared.advance_before(at,enabled,progress=progress)
    assert advance.complete.all().cpu().item() and not advance.fault.any().cpu().item()
    original=clock.projection
    bad=replace(original,emitter=replace(original.emitter,fault=original.emitter.fault|1))
    clock.projection=bad
    before=(clock.runtime,bad,clock.public);saved=_owned(before,before)
    position=clock.source.position.clone()
    failed=clock.advance_through(at,enabled,progress=progress)
    assert not failed.complete.any().cpu().item() and failed.fault.cpu().tolist()==[1]
    assert clock.runtime is before[0] and clock.projection is bad and clock.public is before[2]
    equal((clock.runtime,clock.projection,clock.public),saved,'fault_authority')
    assert torch.equal(clock.source.position,position)
    clock.projection=original
    report=dict(at=27000,scope='original Projection fault injection at actual nonempty Maker Level',
        fault=failed.fault.cpu().tolist(),runtime_projection_public_unchanged=True,
        source_position_unchanged=True,original_handler_retried_from_same_input=True)
    Path(artifact,'maker-page-common-fault.json').write_text(json.dumps(report,indent=2)+'\n')
