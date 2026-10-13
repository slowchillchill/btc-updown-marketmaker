"""同桶原Level的设备分段图；实际Maker人口交回原经济处理器。"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers,QueueEffects,_repeat,level_queue_dispatch
from fifo_loop import _column,_empty
from base_runtime_storage import _owned,_capacity_like,_tensor_pairs
from hidden_base_execution import empty_level_maker_dispatch
from maker_loop import _signature


@dataclass(frozen=True)
class _State:
    hidden: object
    frontier: object
    controls: object
    obligations: object
    sequence: object


@dataclass(frozen=True)
class _Inputs:
    requests: object
    settlements: object
    queries: object
    orders: object
    account: object
    fills: object
    calendars: object
    resolutions: object
    queue_fault: object
    at: object
    present: object
    fault: object
    paths: object
    fee: object
    rebate: object


def _state(world):
    return _State(replace(world.hidden,queues=None),world.hidden_frontier,world.market_controls,
        world.resolution_obligations,None if world.input_frontier is None else world.input_frontier.next_sequence)


def _install(world,state):
    return replace(world,hidden=replace(state.hidden,queues=world.hidden.queues),hidden_frontier=state.frontier,
        market_controls=state.controls,resolution_obligations=state.obligations,
        input_frontier=None if world.input_frontier is None else replace(world.input_frontier,next_sequence=state.sequence))


def _inputs(world,choice,paths,fee,rebate):
    return _Inputs(world.requests,world.settlement_registrations,world.order_query_history,
        world.orders,world.account,world.fills,world.calendars,
        world.source_resolutions,world.hidden.queues.fault,choice.at,choice.present,choice.fault,paths,fee,rebate)


def _copy(target,value,enabled=None):
    for left,right in _tensor_pairs(target,value):
        left.copy_(right if enabled is None else torch.where(enabled,right,left))


class _CapturedLevels:
    def __init__(self,world,choice,stage,conditions,source_assets,paths,fee,exponent,rebate,width):
        self.world=world;self.choice=replace(choice,phase=torch.full_like(choice.phase,4));self.stage=stage;self.queue=world.hidden.queues
        self.conditions=conditions;self.source_assets=source_assets;self.exponent=exponent
        self.width=width;self.device=choice.at.device;self.capacity=self.queue.capacity
        self.initial=_owned(_state(world),_state(world),word_capacity=width)
        self.state=_owned(self.initial,self.initial)
        original=_inputs(world,choice,paths,fee,rebate)
        self.inputs=_owned(original,original,word_capacity=width)
        scalar=lambda dtype:torch.zeros((),dtype=dtype,device=self.device)
        self.start=scalar(torch.int64);self.end=scalar(torch.int64);self.index=scalar(torch.int64)
        self.column=scalar(torch.int64);self.limit=scalar(torch.int64)
        self.phase=scalar(torch.int64);self.required=scalar(torch.int64)
        self.fits=torch.ones((),dtype=torch.bool,device=self.device)
        self.running=scalar(torch.bool);self.safe_suffix=scalar(torch.bool);self.ready=scalar(torch.bool)
        self.stopped=scalar(torch.bool);self.flags=[scalar(torch.bool) for _ in range(3)]
        self.available=_empty(world.orders.roots*self.capacity,width,self.device)
        self.taken=_empty(world.orders.roots*self.capacity,width,self.device)
        self.left=_empty(world.orders.roots,width,self.device)
        self.axis=torch.arange(world.orders.roots,device=self.device)*self.capacity
        self.positions=torch.arange(self.capacity,device=self.device)[None,:].expand(world.orders.roots,self.capacity).flatten()
        # warm/capture不写本桶scratch；原业务输入只在replay启用。
        operations=[self.prepare,self.take_column,self.finish]
        self.reset();self.prepare();self.take_column();self.finish()
        stream=torch.cuda.Stream(device=self.device);stream.wait_stream(torch.cuda.current_stream(self.device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                self.reset()
                for operation in operations:operation()
        torch.cuda.current_stream(self.device).wait_stream(stream)
        self.graphs=[]
        for operation in [self.reset,*operations]:
            graph=torch.cuda.CUDAGraph(keep_graph=True)
            with torch.cuda.graph(graph):operation()
            self.graphs.append(graph)
        self.loop=Numbers.public_extension().DeviceConditionalSequenceLoop(self.graphs[0].raw_cuda_graph(),
            [g.raw_cuda_graph() for g in self.graphs[1:]],self.flags,self.ready)

    def view(self):
        inputs=self.inputs
        queue=replace(self.queue,fault=inputs.queue_fault)
        return replace(self.world,hidden=replace(self.state.hidden,queues=queue),hidden_frontier=self.state.frontier,
            requests=inputs.requests,settlement_registrations=inputs.settlements,order_query_history=inputs.queries,
            orders=inputs.orders,account=inputs.account,fills=inputs.fills,calendars=inputs.calendars,
            source_resolutions=inputs.resolutions,market_controls=self.state.controls,resolution_obligations=self.state.obligations,
            input_frontier=None if self.world.input_frontier is None else replace(self.world.input_frontier,next_sequence=self.state.sequence))

    def checked(self,compute):
        result=Numbers.device_exact_batch(compute,word_capacity=self.width,device=self.device)
        self.required.copy_(torch.maximum(self.required,result.required_words));self.fits.logical_and_(result.fits)
        return result.value

    def compute(self,dispatch):
        world=self.view();inputs=self.inputs
        choice=replace(self.choice,at=inputs.at,present=inputs.present,fault=inputs.fault,
            requests=world.requests,fills=world.fills,calendars=world.calendars,hidden_frontier=world.hidden_frontier,
            settlement_registrations=world.settlement_registrations,order_query_history=world.order_query_history)
        with level_queue_dispatch(dispatch),empty_level_maker_dispatch():
            return world.apply_hidden_selected(choice,torch.ones_like(inputs.at)*self.index,self.conditions,
                inputs.paths,inputs.fee,self.exponent,inputs.rebate,self.source_assets,world.hidden.level_sides,level_only=True)

    def select_phase(self,phase):
        self.phase.copy_(phase if isinstance(phase,torch.Tensor) else self.phase.new_full((),phase))
        block=self.world.hidden_frontier.columns.source
        tag=block.step_tags.index_select(0,self.index.clamp_max(block.step_tags.numel()-1).reshape(1))[0]
        self.ready.copy_((self.index<self.end)&(tag==2)&self.fits&~self.stopped)
        for index,flag in enumerate(self.flags):flag.copy_((self.phase==index)&self.ready)

    def reset(self):
        _copy(self.state,self.initial);self.index.copy_(self.start);self.column.zero_()
        self.required.zero_();self.fits.fill_(True);self.stopped.zero_();self.select_phase(0)

    def prepare(self):
        def dispatch(queue,before,after):
            self.local=queue;self.plan=queue.prepare_level_transition(before,after)
            zero=self.plan['zero'];maker=_repeat(zero,self.capacity)
            return queue,QueueEffects(queue.order_ids,maker,zero,torch.zeros_like(queue.fault,dtype=torch.bool),
                (queue.kind!=0).sum(1),queue.fault)
        self.checked(lambda:self.compute(dispatch))
        zero=self.plan['zero'];kind=self.local.kind.flatten();flat_zero=_repeat(zero,self.capacity)
        positions=torch.arange(self.capacity,device=self.device)[None,:]
        self.limit.copy_(torch.where(self.local.kind!=0,positions+1,0).max())
        live=(positions<self.limit).expand_as(self.local.kind).flatten()
        self.live=live;self.kind=kind
        self.consumed=self.local.consumed_base.select(live,flat_zero)
        self.account=self.local.account_remaining.select(live,flat_zero)
        available=self.checked(lambda:(self.local.total_base.select(live,flat_zero)-self.consumed).select(
            kind==1,self.account.select(kind==2,flat_zero)))
        _copy(self.available,available);_copy(self.left,self.plan['remaining'])
        self.safe_suffix.copy_(((available.sign>=0)&(available.fault==0)).all())
        self.taken.words.zero_();self.taken.sign.zero_();self.taken.fault.zero_();self.column.zero_()
        done=(self.limit==0)|(self.safe_suffix&~self.left.sign.any())
        self.select_phase(torch.where(done,2,1))

    def take_column(self):
        indices=self.axis+self.column.clamp_max(self.capacity-1)
        take,left=self.checked(lambda:_column(self.available,self.left,indices))
        for name in ('words','sign','fault'):getattr(self.taken,name).index_copy_(0,indices,getattr(take,name))
        _copy(self.left,left);self.column.add_(1)
        done=(self.column>=self.limit)|(self.safe_suffix&~self.left.sign.any())
        self.select_phase(torch.where(done,2,1))

    def finish(self):
        def queue_result():
            suffix=(self.positions>=self.column)&(self.positions<self.limit)
            taken=_repeat(self.left,self.capacity).select(suffix,self.taken)
            consumed=(self.consumed+taken).select(self.live&(self.kind==1),self.local.consumed_base)
            account=(self.account-taken).select(self.live&(self.kind==2),self.local.account_remaining)
            maker=taken.select(self.live&(self.kind==2),_repeat(self.plan['zero'],self.capacity))
            return self.local.finish_level_transition(self.plan,consumed,account,maker,self.left)
        local,effect=self.checked(queue_result)
        # 原eager Level仅在实际正base为空时循环零次；不看NOE N或emitted。
        maker=(effect.maker_base_units.sign>0).any()
        world,output=self.checked(lambda:self.compute(lambda queue,before,after:(local,effect)))
        resources=(output.required_segments<=self.queue.capacity).all()&(
            output.required_records<=world.fills.capacity).all()&(
            output.required_maker_fills<=world.calendars.maker.fill_occupied.size(1)).all()&(
            output.required_maker_days<=world.calendars.maker.occupied.size(1)).all()
        committed=self.fits&resources&~maker&~output.fault.any()
        self.stage.accept(world.hidden.queues,committed&self.running)
        _copy(self.state,_state(world),committed)
        self.index.add_(committed.to(torch.int64));self.stopped.copy_(~committed)
        self.select_phase(0)

    def replay(self,world,choice,start,end,paths,fee,rebate):
        _copy(self.initial,_capacity_like(_state(world),self.initial))
        _copy(self.inputs,_capacity_like(_inputs(world,choice,paths,fee,rebate),self.inputs))
        self.start.fill_(start);self.end.fill_(end);self.running.fill_(True);self.loop.replay()
        state=_owned(self.state,self.state)
        return _install(world,state),int(self.index.cpu()),int(self.required.cpu())


class HiddenLevelDeviceLoop:
    def __init__(self):self.width=4;self.key=None;self.captured=None

    def run(self,world,choice,start,end,stage,conditions,source_assets,paths,fee,exponent,rebate):
        if start==end or world.hidden.queues.rows==0:return world,start
        queue=world.hidden.queues
        # 恢复后fresh loop仍从4字启动；当前合法输入/旧scratch可能已扩宽。
        # 构图前协调物理字宽，不能等8→4固定缓冲复制失败后再申请资源。
        from public_trade_loop import _widths
        state=_state(world);inputs=_inputs(world,choice,paths,fee,rebate)
        self.width=max([self.width,Numbers.device_word_capacity(),*_widths(state),*_widths(inputs),
            *_widths(queue),*_widths(stage.scratch)])
        key=(_signature(state),_signature(inputs),
            _signature(queue),
            id(queue.price),id(stage),id(conditions),id(source_assets),exponent,self.width,choice.at.device,
            torch.cuda.current_stream(choice.at.device).cuda_stream)
        if key!=self.key:
            self.captured=None;self.key=None
            self.captured=_CapturedLevels(world,choice,stage,conditions,source_assets,paths,fee,exponent,rebate,self.width)
            self.key=key
        world,index,required=self.captured.replay(world,choice,start,end,paths,fee,rebate)
        if required>self.width:self.width=1<<(max(4,required)-1).bit_length()
        return world,index
