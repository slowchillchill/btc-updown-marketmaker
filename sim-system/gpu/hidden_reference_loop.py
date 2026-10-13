"""同桶非Terminal引用的私有WHILE；原处理器和阶段提交门仍归原owner。"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers
from base_runtime_storage import _owned, _tensor_pairs, _capacity_like
from maker_loop import _signature


@dataclass(frozen=True)
class _Queue:
    kind: object
    order_ids: object
    total_base: object
    total_quote: object
    consumed_base: object
    account_remaining: object
    debt: object
    fault: object
    conservative: object


@dataclass(frozen=True)
class _Asset:
    tick: object
    tick_present: object
    quality: object
    external_shares: object
    queues: object
    fault: object


@dataclass(frozen=True)
class _Obligations:
    invalidated_by_quality: object
    fault: object


@dataclass(frozen=True)
class _State:
    hidden: object
    controls: object
    obligations: object
    next_reference: object
    fault: object


@dataclass(frozen=True)
class _Inputs:
    next_bucket: object
    applying: object
    initialized: object
    matching_closed: object
    book_closed: object
    resolutions: object
    obligations: object


def _inputs(world):
    return _Inputs(world.hidden_frontier.next_bucket,world.hidden_frontier.applying,
        world.hidden.initialized,world.hidden.matching_closed,world.hidden.book_closed,
        world.source_resolutions,world.resolution_obligations)


def _bind_inputs(world,inputs):
    return replace(world,hidden_frontier=replace(world.hidden_frontier,
        next_bucket=inputs.next_bucket,applying=inputs.applying),hidden=replace(world.hidden,
        initialized=inputs.initialized,matching_closed=inputs.matching_closed,book_closed=inputs.book_closed),
        source_resolutions=inputs.resolutions,resolution_obligations=inputs.obligations)


def _state(world):
    hidden=world.hidden;queue=hidden.queues;obligations=world.resolution_obligations
    return _State(_Asset(hidden.tick,hidden.tick_present,hidden.quality,hidden.external_shares,
        None,hidden.fault),
        world.market_controls,None if obligations is None else
            _Obligations(obligations.invalidated_by_quality,obligations.fault),
        world.hidden_frontier.next_reference,world.hidden_frontier.fault)


def _install(world,state):
    asset=state.hidden
    queue=world.hidden.queues if asset.queues is None else replace(world.hidden.queues,**vars(asset.queues))
    hidden=replace(world.hidden,**{name:value for name,value in vars(asset).items() if name!='queues'},queues=queue)
    obligations=world.resolution_obligations
    if obligations is not None:obligations=replace(obligations,**vars(state.obligations))
    return replace(world,hidden=hidden,market_controls=state.controls,resolution_obligations=obligations,
        hidden_frontier=replace(world.hidden_frontier,next_reference=state.next_reference,fault=state.fault))


def _copy(target,value,enabled=None):
    for left,right in _tensor_pairs(target,value):
        left.copy_(right if enabled is None else torch.where(enabled,right,left))


class _CapturedReferences:
    def __init__(self,world,choice,source_assets,width):
        self.world=world;self.source_assets=source_assets;device=choice.at.device
        # 这五列由原begin/finish/协调器换对象；图只读取固定地址的本次值。
        # 它们不是循环输出，replay仍从本次world安装独立候选。
        self.inputs=_owned(_inputs(world),_inputs(world))
        fixed_world=_bind_inputs(world,self.inputs)
        dictionary=world.hidden_frontier.columns.source.decimals
        price=world.hidden.queues.price
        state=_state(world);widths=[width,dictionary.numerator.words.size(1),dictionary.denominator.words.size(1),
            price.numerator.words.size(1),price.denominator.words.size(1)]
        private=_owned(state,state,word_widths=widths)
        self.initial=_owned(private,private,word_capacity=max(widths))
        self.state=_owned(self.initial,self.initial)
        self.choice=replace(choice,**{name:getattr(choice,name).clone() for name in ('at','phase','present','fault')})
        self.start=torch.zeros((),dtype=torch.int64,device=device)
        self.end=torch.zeros_like(self.start);self.index=torch.zeros_like(self.start)
        self.required=torch.zeros_like(self.start);self.fits=torch.ones((),dtype=torch.bool,device=device)
        self.ready=torch.zeros_like(self.fits)
        block=world.hidden_frontier.columns.source

        def ready():
            tag=block.reference_rows.index_select(0,self.index.clamp(0,block.reference_rows.size(0)-1).reshape(1))[0,0]
            return (self.index<self.end)&(tag!=9)&(tag!=3)&(tag!=8)&self.fits

        def body():
            def compute():
                candidate=_install(fixed_world,self.state)
                current=replace(self.choice,requests=candidate.requests,fills=candidate.fills,
                    calendars=candidate.calendars,hidden_frontier=candidate.hidden_frontier,
                    settlement_registrations=candidate.settlement_registrations,
                    order_query_history=candidate.order_query_history)
                return _state(candidate.apply_hidden_reference(current,
                    torch.ones_like(choice.at)*self.index,source_assets,ordinary=True))
            candidate=Numbers.device_exact_batch(compute,word_capacity=width,device=device)
            self.required.copy_(torch.maximum(self.required,candidate.required_words))
            self.fits.logical_and_(candidate.fits)
            # 不足的当前引用不安装；此前私有成功前缀在冷增长后原位续接。
            _copy(self.state,candidate.value,candidate.fits)
            self.index.add_(candidate.fits.to(torch.int64))
            # 原physical for不因业务fault提前结束，后续仍调用原frontier掩码。
            self.ready.copy_(ready())

        def reset():
            _copy(self.state,self.initial);self.index.copy_(self.start)
            self.required.zero_();self.fits.fill_(True);self.ready.copy_(ready())

        stream=torch.cuda.Stream(device=device);stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):reset();body()
        torch.cuda.current_stream(device).wait_stream(stream)
        reset()
        self.body=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):body()
        self.entry=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):reset()
        self.loop=Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)

    def replay(self,world,choice,start,end):
        _copy(self.inputs,_capacity_like(_inputs(world),self.inputs))
        _copy(self.initial,_capacity_like(_state(world),self.initial))
        for name in ('at','phase','present','fault'):getattr(self.choice,name).copy_(getattr(choice,name))
        self.start.fill_(start);self.end.fill_(end);self.loop.replay()
        return (_install(world,_owned(self.state,self.state)),self.index.clone(),
            self.required.clone(),self.fits.clone())


class HiddenReferenceDeviceLoop:
    """只持有一个当前world的可重建布局；物理索引不进入权威状态。"""
    def __init__(self):
        self.width=4;self.key=None;self.captured=None

    def run(self,world,choice,start,end,source_assets):
        while start<end:
            frontier=world.hidden_frontier;hidden=world.hidden;obligations=world.resolution_obligations
            # 所有原只读借用按实际world绑定，尤其Terminal产生的新判定/义务。
            borrowed=(frontier.columns,
                hidden.level_assets,hidden.asset_markets,hidden.market_populations,
                hidden.level_sides,hidden.queues.price,
                world.account.asset_roots,source_assets)
            key=(_signature(_state(world)),_signature(_inputs(world)),tuple(id(value) for value in borrowed),world.orders.roots,
                self.width,choice.at.device,torch.cuda.current_stream(choice.at.device).cuda_stream)
            if key!=self.key:
                self.captured=None;self.key=None
                self.captured=_CapturedReferences(world,choice,source_assets,self.width);self.key=key
            world,index,required,fits=self.captured.replay(world,choice,start,end)
            current=int(index.cpu())
            if bool(fits.cpu()):return world,current
            request=int(required.cpu());self.width=1<<(max(4,request)-1).bit_length()
            start=current
        return world,start
