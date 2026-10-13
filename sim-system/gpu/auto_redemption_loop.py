"""原Auto单市场纯候选的私有设备循环；共同安装仍归send_clock。"""
from dataclasses import dataclass,fields,replace
import torch
from queues import Numbers
from base_runtime import BaseSimRuntime
from base_runtime_storage import _owned,_tensor_pairs,SimFeedbackPage
from maker_loop import _signature
from projection import Projection
from core import EvidenceBank,Normalizer


@dataclass(frozen=True)
class _Maker:
    market_result: object
    fault: object
    market_ids: object


@dataclass(frozen=True)
class _Calendars:
    maker: object


@dataclass(frozen=True)
class _SourceTimes:
    source_times: object


@dataclass(frozen=True)
class _Obligations:
    reference_indices: object
    pending: object
    invalidated_by_quality: object
    auto_redeemed: object
    fault: object


@dataclass(frozen=True)
class _State:
    account: object
    auto_redeemed: object
    fault: object
    projection: object
    context: object


@dataclass(frozen=True)
class _Inputs:
    at: object
    enabled: object


def _copy(target,value,enabled=None):
    for left,right in _tensor_pairs(target,value):
        left.copy_(right if enabled is None else torch.where(enabled,right,left))


def _bindings(world):
    o=world.resolution_obligations;m=world.calendars.maker
    return (o.reference_indices,o.pending,o.invalidated_by_quality,
        m.market_result,m.fault,m.market_ids,world.source_resolutions.source_times,
        world.auto_redemption_sources)


class _CapturedAuto:
    def __init__(self,world,projection,at,enabled,sampler,context,width):
        self.width=width;self.sampler=sampler;device=at.device
        bindings=_bindings(world)
        references,pending,invalidated,result,maker_fault,markets,source_times,sources=bindings
        self.markets=references.size(1)
        private=_State(world.account,world.resolution_obligations.auto_redeemed,
            world.resolution_obligations.fault,projection,context)
        widths=[width,sources.payouts.numerator.words.size(1),sources.payouts.denominator.words.size(1)]
        initial=_owned(private,private,word_widths=widths)
        self.physical_width=max(widths)
        initial=_owned(initial,initial,word_capacity=self.physical_width)
        self.inputs=_owned(_Inputs(at,enabled),_Inputs(at,enabled))
        self.index=torch.zeros((),dtype=torch.int64,device=device)
        self.market=torch.zeros_like(self.index)
        self.required=torch.zeros_like(self.index);self.fits=torch.ones((),dtype=torch.bool,device=device)
        self.ready=torch.zeros_like(self.fits)
        self.yes=torch.ones_like(self.fits)
        def view(state):
            # 仅原单市场函数实际消费的只读绑定；不持有orders/FIFO或整个Runtime。
            obligations=_Obligations(references,pending,invalidated,state.auto_redeemed,state.fault)
            return BaseSimRuntime(None,None,None,state.account,None,
                _Calendars(_Maker(result,maker_fault,markets)),resolution_obligations=obligations,
                auto_redemption_sources=sources,source_resolutions=_SourceTimes(source_times))
        def prepare(state):
            current=view(state)
            ready=current.auto_market_ready(self.inputs.enabled).index_select(1,self.market.reshape(1)).squeeze(1)
            candidate,projected,batch,context=current.prepare_auto_market(self.inputs.at,ready,self.market,
                state.projection,self.sampler,context=state.context)
            return _State(candidate.account,candidate.resolution_obligations.auto_redeemed,
                candidate.resolution_obligations.fault,projected,context),batch
        preview=Numbers.device_exact_batch(lambda:prepare(initial),word_capacity=width,device=device)
        prospective,_=preview.value
        # 原单市场输出可借更宽操作数；统一准备私有可变精确列，不截断。
        widths=[]
        _owned(prospective.account,prospective.account,word_widths=widths)
        _owned(prospective.context,prospective.context,word_widths=widths)
        _owned(prospective.projection.emitter,prospective.projection.emitter,word_widths=widths)
        self.physical_width=max([self.physical_width,*widths])
        initial=_owned(initial,initial,word_capacity=self.physical_width)
        preview=Numbers.device_exact_batch(lambda:prepare(initial),word_capacity=width,device=device)
        prospective,template=preview.value
        old=initial.projection.normalizer or Normalizer.empty(EvidenceBank.empty(device),at.numel())
        self.base=old.bank.kinds.numel()
        incoming=prospective.projection.normalizer.bank
        incoming=EvidenceBank(**{f.name:getattr(incoming,f.name)[self.base:].clone()
            for f in fields(EvidenceBank) if f.name!='declared_schemas'},declared_schemas=incoming.declared_schemas)
        reserved=initial.projection.reserve_records(incoming,self.markets*2*at.numel())
        self.initial=replace(initial,projection=reserved)
        self.state=_owned(self.initial,self.initial)
        self.page=SimFeedbackPage(template,self.markets,device=device)
        def ready():
            qualified=view(self.state).auto_market_ready(self.inputs.enabled).any(0)
            available=qualified&(torch.arange(self.markets,device=device)>=self.index)
            self.market.copy_(torch.where(available,torch.arange(self.markets,device=device),self.markets).min().clamp_max(self.markets-1))
            return available.any()&self.fits&((self.page.count==0)|~self.state.fault.any())
        def body():
            result=Numbers.device_exact_batch(lambda:prepare(self.state),word_capacity=width,device=device)
            state,batch=result.value
            self.required.copy_(torch.maximum(self.required,result.required_words))
            self.fits.logical_and_(result.fits)
            # 原序号可借输入：保存本自然批后才覆盖私有候选。
            self.page.append(batch,result.fits,self.yes)
            _copy(self.state,state,result.fits)
            self.index.copy_(self.market+1)
            self.ready.copy_(ready())
        def reset():
            _copy(self.state,self.initial);self.index.zero_();self.page.count.zero_()
            self.required.zero_();self.fits.fill_(True);self.ready.copy_(ready())
        stream=torch.cuda.Stream(device=device);stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):reset();body()
        torch.cuda.current_stream(device).wait_stream(stream);reset()
        self.body=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):body()
        self.entry=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):reset()
        self.loop=Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)

    def replay(self,world,projection,at,enabled,context):
        old=projection.normalizer or Normalizer.empty(EvidenceBank.empty(at.device),at.numel())
        reserved=self.initial.projection
        bank=reserved.normalizer.bank
        for f in fields(EvidenceBank):
            if f.name=='declared_schemas':continue
            target,value=getattr(bank,f.name),getattr(old.bank,f.name);target.zero_()
            if value.ndim==2:value=torch.nn.functional.pad(value,(0,target.size(1)-value.size(1)))
            target[:self.base].copy_(value)
        normalizer=replace(reserved.normalizer,fault=old.fault.clone())
        normalizer.logical_rows.fill_(-1);normalizer.economic_rows.fill_(-1)
        normalizer.logical_rows[:,:self.base].copy_(old.logical_rows)
        normalizer.economic_rows[:,:self.base].copy_(old.economic_rows)
        values={f.name:getattr(projection,f.name) for f in fields(Projection)}
        values['normalizer']=normalizer
        p=type(reserved)(**values,row_base=self.base,rows_used=torch.zeros_like(reserved.rows_used))
        initial=_State(world.account,world.resolution_obligations.auto_redeemed,
            world.resolution_obligations.fault,p,context)
        _copy(self.initial,_owned(initial,self.initial));_copy(self.inputs,_Inputs(at,enabled))
        self.loop.replay()
        required=self.required.clone();fits=self.fits.clone()
        if not bool(fits.cpu()):return None,None,(),required,fits
        count=int(self.page.count.cpu())
        if not count:return world,projection,(),required,fits
        saved=_owned(self.state,self.state)
        output=Projection(**{f.name:getattr(saved.projection,f.name) for f in fields(Projection)})
        used=int(saved.projection.rows_used.cpu())
        output=replace(output,normalizer=saved.projection.normalizer.used_prefix(self.base+used))
        batches=tuple(_owned(self.page.read(i),self.page.read(i)) for i in range(count))
        world=replace(world,account=saved.account,resolution_obligations=replace(world.resolution_obligations,
            auto_redeemed=saved.auto_redeemed,fault=saved.fault))
        return world,output,batches,required,fits


class AutoRedemptionDeviceLoop:
    def __init__(self):self.current=None;self.key=None;self.width=4

    def run(self,world,projection,at,enabled,sampler,*,preceding=None):
        if world.auto_redemption_sources is None or world.resolution_obligations.reference_indices.size(1)==0:
            return world,projection,()
        # 一次入口读面只避免为零人口制造模板；逐市场资格与停止全留设备。
        if not bool(world.auto_market_ready(enabled).any().cpu()):return world,projection,()
        context=Numbers.run_exact_batch(lambda:projection.auto_batch_context(preceding,at))
        while True:
            key=(_signature(_State(world.account,world.resolution_obligations.auto_redeemed,
                world.resolution_obligations.fault,projection,context)),
                tuple(id(value) for value in _bindings(world)),id(sampler),self.width,
                at.device,torch.cuda.current_stream(at.device).cuda_stream)
            if key!=self.key:
                self.current=None;self.key=None
                self.current=_CapturedAuto(world,projection,at,enabled,sampler,context,self.width);self.key=key
            result,projected,batches,required,fits=self.current.replay(world,projection,at,enabled,context)
            if bool(fits.cpu()):return result,projected,batches
            need=int(required.cpu());self.width=1<<(max(4,need)-1).bit_length()
