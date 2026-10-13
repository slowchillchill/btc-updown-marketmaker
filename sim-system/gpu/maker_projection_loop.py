"""一条Level的owned Maker页→原Projection/Normalizer纯候选设备循环。"""
from dataclasses import dataclass,fields,replace
import torch
from queues import Numbers
from base_runtime_storage import _owned,_tensor_pairs,SimFeedbackPage
from hidden_base_execution import coordinate_maker_frame
from maker_loop import _signature
from projection import Projection
from core import EvidenceBank,Normalizer


@dataclass(frozen=True)
class _Inputs:
    orders: object
    fills: object
    maker: object
    page: object
    at: object
    rebate: object
    fault: object
    asset_roots: object


def _copy(target,incoming,enabled=None):
    for left,right in _tensor_pairs(target,incoming):
        left.copy_(right if enabled is None else torch.where(enabled,right,left))


class _CapturedMakerProjection:
    def __init__(self,world,projection,page,at,rebate,sampler,assets,width,*,effect_frames=None):
        self.sampler=sampler;self.assets=assets;self.width=width
        self.multiple=effect_frames is not None;self.effect_frames=effect_frames
        inputs=_Inputs(world.orders,world.fills,world.calendars.maker,page,at,rebate,
            world.account.fault|world.orders.fault,world.account.asset_roots)
        widths=[width];private=_owned(inputs,inputs,word_widths=widths)
        initial=_owned(projection,projection,word_widths=widths)
        self.physical_width=max(widths)
        private=replace(private,page=private.page.with_capacity(self.physical_width))
        self.inputs=_owned(private,private,word_capacity=self.physical_width)
        self.initial_base=_owned(initial,initial,word_capacity=self.physical_width)
        # coordinate_delta只从账户借零值构造的形状和根映射；不读取旧余额。
        # constant(0)及Rational.zeros仍由原方法在本次精度上下文生成，fault由inputs传入。
        account=replace(world.account,asset_roots=self.inputs.asset_roots)
        calendars=replace(world.calendars,maker=self.inputs.maker)
        device=at.device;self.column=torch.zeros((),dtype=torch.int64,device=device)
        self.effect_index=torch.zeros_like(self.column);self.page_rows_used=torch.zeros_like(self.column)
        self.stop_fault=torch.zeros_like(at,dtype=torch.int32)
        def current_page():
            return self.inputs.page.read_device(self.effect_index.clamp_max(page.capacity-1)).maker_page if self.multiple else self.inputs.page
        if self.multiple:
            counts=page.member_buffer(page.template.maker_page.count)
            self.effect_index.copy_((counts>0).to(torch.int64).argmax())
        def frame():
            raw=current_page().read_device(self.column.clamp_max(current_page().capacity-1))
            return coordinate_maker_frame(raw,account,self.inputs.fault==0,self.inputs.fault)
        def prepare(p):
            return p.fill_from_origin(sampler,self.inputs.orders,self.inputs.fills,frame(),assets,
                calendars,self.inputs.rebate,self.inputs.at)
        preview=Numbers.device_exact_batch(lambda:prepare(self.initial_base),word_capacity=width,device=device)
        prospective,template=preview.value
        old=self.initial_base.normalizer
        if old is None:old=Normalizer.empty(EvidenceBank.empty(device),at.numel())
        self.base=old.bank.kinds.numel()
        # 仅用原preview的新增行布局；预计算业务结果从不安装。
        incoming=prospective.normalizer.bank
        from dataclasses import fields as bank_fields
        incoming=EvidenceBank(**{f.name:getattr(incoming,f.name)[self.base:].clone()
            for f in bank_fields(EvidenceBank) if f.name!='declared_schemas'},declared_schemas=incoming.declared_schemas)
        slots=effect_frames if self.multiple else page.capacity
        addition=slots*4*at.numel()
        self.initial=self.initial_base.reserve_records(incoming,addition)
        self.state=_owned(self.initial,self.initial)
        self.page=SimFeedbackPage(template,max(1,slots),device=device)
        self.ready=torch.zeros((),dtype=torch.bool,device=device)
        self.feedback=torch.zeros_like(self.ready)
        self.required=torch.zeros_like(self.column);self.fits=torch.ones_like(self.ready)
        self.yes=torch.ones_like(self.ready)
        def ready():
            return (self.column<current_page().count)&self.fits&((self.column==0)|
                (~self.state.emitter.fault.any()&~self.state.normalizer.fault.any()))
        def body():
            raw=frame();event=raw.event
            within=self.column<current_page().count
            projection_fault=self.state.emitter.fault.any()|self.state.normalizer.fault.any()|\
                self.state.emitter.next_private_sequence.fault.any()|self.state.emitter.next_batch_sequence.fault.any()
            # 原全页有反馈时，全部物理frame都调用原算术；空frame只不发布预留行。
            call_original=within&(self.feedback|projection_fault)
            process=(self.column<current_page().count)&(event.emitted.any()|event.economic_delta.emitted.any()|
                event.failure_delta.emitted.any()|event.rebate_delta.emitted.any()|event.fault.any()|
                projection_fault)
            result=Numbers.device_exact_batch(lambda:prepare(self.state),word_capacity=width,device=device)
            candidate,batch=result.value
            self.required.copy_(torch.maximum(self.required,torch.where(call_original,result.required_words,0)))
            self.fits.logical_and_(result.fits|~call_original)
            self.page.append(batch,process&result.fits,self.yes)
            _copy(self.state,candidate,process&result.fits)
            for name in ('condition_slots','token_slots','asset_indices','order_sequence','request_slots','item_ordinals'):
                _copy(getattr(self.state,name),getattr(candidate,name),call_original&result.fits)
            self.column.add_(1)
            if self.multiple:select_phase(torch.where(ready(),1,2))
            else:self.ready.copy_(ready())
        self.flags=[torch.zeros_like(self.ready) for _ in range(3)]
        def select_phase(phase):
            self.ready.copy_((self.effect_index<self.inputs.page.count)&self.fits&~self.stop_fault.any())
            for index,flag in enumerate(self.flags):flag.copy_(self.ready&(phase==index))
        def prepare_effect():
            effect=self.inputs.page.read_device(self.effect_index.clamp_max(page.capacity-1))
            self.stop_fault.copy_(effect.fault);self.column.zero_();self.page_rows_used.copy_(self.state.rows_used)
            self.feedback.copy_(effect.maker_page.feedback_present())
            select_phase(torch.where(effect.maker_page.count>0,1,2))
        def finish_effect():
            used=self.state.rows_used>self.page_rows_used
            fault=self.state.emitter.fault|self.state.normalizer.fault
            self.stop_fault.copy_(torch.where(used,fault,0))
            self.effect_index.add_((~self.stop_fault.any()).to(torch.int64));select_phase(0)
        def reset():
            _copy(self.state,self.initial);self.column.zero_();self.page.count.zero_()
            self.required.zero_();self.fits.fill_(True);self.effect_index.zero_();self.stop_fault.zero_()
            if self.multiple:select_phase(0)
            else:
                self.ready.copy_(ready());self.feedback.copy_(current_page().feedback_present())
        stream=torch.cuda.Stream(device=device);stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                reset()
                if self.multiple:prepare_effect()
                body()
                if self.multiple:finish_effect()
        torch.cuda.current_stream(device).wait_stream(stream);reset()
        if self.multiple:
            self.graphs=[]
            for operation in (reset,prepare_effect,body,finish_effect):
                graph=torch.cuda.CUDAGraph(keep_graph=True)
                with torch.cuda.graph(graph):operation()
                self.graphs.append(graph)
            self.loop=Numbers.public_extension().DeviceConditionalSequenceLoop(self.graphs[0].raw_cuda_graph(),
                [g.raw_cuda_graph() for g in self.graphs[1:]],self.flags,self.ready)
        else:
            self.body=torch.cuda.CUDAGraph(keep_graph=True)
            with torch.cuda.graph(self.body):body()
            self.entry=torch.cuda.CUDAGraph(keep_graph=True)
            with torch.cuda.graph(self.entry):reset()
            self.loop=Numbers.public_extension().DeviceConditionalLoop(
                self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)

    def replay(self,world,projection,page,at,rebate):
        incoming=_Inputs(world.orders,world.fills,world.calendars.maker,page.with_capacity(self.physical_width),at,rebate,
            world.account.fault|world.orders.fault,world.account.asset_roots)
        _copy(self.inputs,_owned(incoming,self.inputs))
        # 前缀保原已提交bank；只为本次页预留，不写权威Projection。
        old=projection.normalizer or Normalizer.empty(EvidenceBank.empty(at.device),at.numel())
        bank=self.initial.normalizer.bank
        for f in fields(EvidenceBank):
            if f.name=='declared_schemas':continue
            target,value=getattr(bank,f.name),getattr(old.bank,f.name)
            target.zero_()
            if value.ndim==2:value=torch.nn.functional.pad(value,(0,target.size(1)-value.size(1)))
            target[:self.base].copy_(value)
        self.initial.normalizer.logical_rows.fill_(-1);self.initial.normalizer.economic_rows.fill_(-1)
        self.initial.normalizer.logical_rows[:,:self.base].copy_(old.logical_rows)
        self.initial.normalizer.economic_rows[:,:self.base].copy_(old.economic_rows)
        self.initial.normalizer.fault.copy_(old.fault);self.initial.rows_used.zero_()
        for f in fields(Projection):
            if f.name not in ('normalizer','resolution_columns'):
                target=getattr(self.initial,f.name)
                _copy(target,_owned(getattr(projection,f.name),target))
        self.loop.replay()
        required=self.required.clone();fits=self.fits.clone()
        if not bool(fits.cpu()):return None,(),required,fits,None,None
        count=int(self.page.count.cpu());used=int(self.state.rows_used.cpu())
        stopped=page.read_device(torch.minimum(self.effect_index,(page.count-1).clamp_min(0))) if self.multiple else None
        if not used:return projection,(),required,fits,self.stop_fault.clone() if self.multiple else at.new_zeros(at.shape,dtype=torch.int32),_owned(stopped,stopped) if stopped is not None else None
        state=_owned(self.state,self.state)
        output=Projection(**{f.name:getattr(state,f.name) for f in fields(Projection)})
        output=replace(output,normalizer=state.normalizer.used_prefix(self.base+used))
        batches=tuple(_owned(self.page.read(i),self.page.read(i)) for i in range(count))
        fault=self.stop_fault.clone() if self.multiple else output.emitter.fault|output.normalizer.fault
        return output,batches,required,fits,fault,_owned(stopped,stopped) if stopped is not None else None


class MakerProjectionDeviceLoop:
    """只保一份当前物理布局；整个页从原进入Projection重算并共同交回。"""
    def __init__(self):self.current=None;self.key=None;self.width=4

    def run(self,world,projection,page,at,rebate,sampler,assets):
        if int(page.count.cpu())==0:
            return projection,(),at.new_zeros(at.shape,dtype=torch.int32)
        while True:
            key=(_signature(projection),_signature(_Inputs(world.orders,world.fills,world.calendars.maker,
                page,at,rebate,world.orders.fault,world.account.asset_roots)),id(sampler),id(assets),
                self.width,at.device,torch.cuda.current_stream(at.device).cuda_stream)
            if self.key!=key:
                self.current=None;self.key=None
                self.current=_CapturedMakerProjection(world,projection,page,at,rebate,sampler,assets,self.width);self.key=key
            result,batches,required,fits,fault,_=self.current.replay(world,projection,page,at,rebate)
            if bool(fits.cpu()):return result,batches,fault
            need=int(required.cpu());self.width=1<<(max(4,need)-1).bit_length()

    def run_effects(self,world,projection,effects,frames,at,rebate,sampler,assets):
        """同一原帧消费者连续处理价位页；预留量只来自实际物理帧数。"""
        if effects is None:return projection,(),None,at.new_zeros(at.shape,dtype=torch.int32)
        if frames==0:
            population=torch.arange(effects.capacity,device=at.device)<effects.count
            bad=(effects.member_buffer(effects.template.fault)!=0).any(1)&population
            positions=torch.arange(effects.capacity,device=at.device)
            index=torch.where(bad,positions,effects.capacity).min()
            index=torch.minimum(index,(effects.count-1).clamp_min(0))
            effect=effects.read_device(index)
            return projection,(),_owned(effect,effect) if int(effects.count.cpu()) else None,torch.where(bad.any(),effect.fault,0)
        while True:
            inputs=_Inputs(world.orders,world.fills,world.calendars.maker,effects,at,rebate,
                world.account.fault|world.orders.fault,world.account.asset_roots)
            key=('effects',frames,_signature(projection),_signature(inputs),id(sampler),id(assets),
                self.width,at.device,torch.cuda.current_stream(at.device).cuda_stream)
            if self.key!=key:
                self.current=None;self.key=None
                self.current=_CapturedMakerProjection(world,projection,effects,at,rebate,sampler,assets,self.width,effect_frames=frames)
                self.key=key
            result,batches,required,fits,fault,effect=self.current.replay(world,projection,effects,at,rebate)
            if bool(fits.cpu()):return result,batches,effect,fault
            need=int(required.cpu());self.width=1<<(max(4,need)-1).bit_length()
