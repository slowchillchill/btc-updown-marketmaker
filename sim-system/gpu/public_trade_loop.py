"""单条PublicTrade的私有价位/列阶段图；没有经营游标或提前提交。"""
from dataclasses import dataclass, fields, is_dataclass, replace
import torch
from queues import Numbers, ExactInt, _take, _put, _repeat
from fifo_loop import _column as fifo_column, _empty
from maker_loop import _column as maker_column, _State as MakerState, _Inputs as MakerInputs, _copy, _signature
from base_runtime_storage import _owned, _shared, _capacity_like, SimFeedbackPage
from hidden_base_execution import prepare_base_level_transition, finish_base_level_transition


@dataclass(frozen=True)
class _State:
    rank: object
    queue_fault: object
    orders: object
    account: object
    fills: object
    maker: object
    taker_fault: object


@dataclass(frozen=True)
class _Inputs:
    order: object
    budget: object
    lowest: object
    enabled: object
    conditions: object
    at: object
    paths: object
    fee: object
    exponent: int
    rebate: object
    owners: object


def _state(world):
    return _State(world.hidden.queues.conservative, world.hidden.queues.fault,
        world.orders, world.account, world.fills, world.calendars.maker, world.calendars.taker.fault)


def _borrow_signature(value):
    if isinstance(value, torch.Tensor):return value.data_ptr(), tuple(value.shape), value.dtype
    if is_dataclass(value):return type(value), tuple((f.name, _borrow_signature(getattr(value, f.name))) for f in fields(value))
    if isinstance(value, tuple):return tuple(_borrow_signature(v) for v in value)
    return value


def _widths(value):
    if isinstance(value, ExactInt):return [value.words.size(1)]
    if is_dataclass(value):return [w for f in fields(value) if not _shared(value, f.name) for w in _widths(getattr(value, f.name))]
    if isinstance(value, tuple):return [w for v in value for w in _widths(v)]
    return []


class _LayoutNeeded(Exception):
    def __init__(self, key, state, inputs, queue, calendars, assets, width):
        self.key, self.state, self.inputs, self.queue, self.calendars, self.assets, self.width = key, state, inputs, queue, calendars, assets, width


class _CapturedTrade:
    def __init__(self, initial, inputs, queue, calendars, assets, width):
        from public_trade_execution import prepare_public_price, finish_public_price
        self.prepare_price_math, self.finish_price_math = prepare_public_price, finish_public_price
        self.width=width;self.inputs=_owned(inputs, inputs, word_capacity=width)
        self.assets=assets.clone();self.owners=self.inputs.owners
        self.initial=_owned(initial, initial, word_capacity=width)
        self.state=_owned(self.initial, self.initial)
        self.price_original=_owned(self.initial, self.initial)
        # 仅保图内实际只读的queue/taker字段，不持有Runtime或旧可变rank。
        readonly=replace(queue, conservative=None, fault=None)
        self.readonly_queue=_owned(readonly,readonly,word_capacity=width)
        readonly=replace(calendars, maker=None, taker=replace(calendars.taker, fault=None))
        self.readonly_calendars=_owned(readonly,readonly,word_capacity=width)
        device=inputs.at.device;self.device=device;self.capacity=queue.capacity;self.rows=queue.rows
        self.prices=inputs.order.size(1)
        scalar=lambda dtype:torch.zeros((), dtype=dtype, device=device)
        self.price=scalar(torch.int64);self.column=scalar(torch.int64);self.depth_column=scalar(torch.int64)
        self.phase=scalar(torch.int64);self.limit=scalar(torch.int64)
        self.safe_suffix=scalar(torch.bool);self.ready=scalar(torch.bool)
        self.required=scalar(torch.int64);self.fits=torch.ones((), dtype=torch.bool, device=device)
        self.yes=torch.ones_like(self.fits);self.flags=[scalar(torch.bool) for _ in range(6)]
        self.budget=_owned(self.inputs.budget,self.inputs.budget)
        self.available=_empty(queue.rows*queue.capacity,width,device)
        self.taken=_empty(queue.rows*queue.capacity,width,device)
        self.left=_empty(queue.rows,width,device);self.actual=_empty(queue.rows,width,device)
        self.positions=torch.arange(queue.capacity,device=device)[None,:].expand(queue.rows,queue.capacity).flatten()
        self.axis=torch.arange(queue.rows,device=device)*queue.capacity
        self.maker_index=scalar(torch.int64)
        self.maker_state=None;self.maker_inputs=None
        # 冷preview只取原单列反馈布局；结果不安装、不推进任何输入。
        self.reset();self.prepare_price();self.rank_column();self.depth_step();self.prepare_maker()
        preview=Numbers.device_exact_batch(lambda:maker_column(self.maker_state,self.maker_inputs,self.maker_index),
            word_capacity=width,device=device)
        _,frame=preview.value
        self.maker_page=SimFeedbackPage(frame,queue.capacity,device=device)
        self.maker_step()
        preview=Numbers.device_exact_batch(self.finish_candidate,word_capacity=width,device=device)
        *_,effect=preview.value
        self.effect_page=SimFeedbackPage(effect,self.prices,device=device)
        operations=[self.prepare_price,self.rank_column,self.depth_step,self.prepare_maker,self.maker_step,self.finish_price]
        stream=torch.cuda.Stream(device=device);stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                self.reset()
                for operation in operations:operation()
        torch.cuda.current_stream(device).wait_stream(stream)
        self.graphs=[]
        for operation in [self.reset,*operations]:
            graph=torch.cuda.CUDAGraph(keep_graph=True)
            with torch.cuda.graph(graph):operation()
            self.graphs.append(graph)
        self.loop=Numbers.public_extension().DeviceConditionalSequenceLoop(self.graphs[0].raw_cuda_graph(),
            [g.raw_cuda_graph() for g in self.graphs[1:]],self.flags,self.ready)

    def queue(self,state=None):
        state=self.state if state is None else state
        return replace(self.readonly_queue,conservative=state.rank,fault=state.queue_fault)

    def calendars(self,state=None):
        state=self.state if state is None else state
        return replace(self.readonly_calendars,maker=state.maker,
            taker=replace(self.readonly_calendars.taker,fault=state.taker_fault))

    def checked(self,compute):
        result=Numbers.device_exact_batch(compute,word_capacity=self.width,device=self.device)
        self.required.copy_(torch.maximum(self.required,result.required_words))
        self.fits.logical_and_(result.fits)
        return result.value

    def select_phase(self,phase):
        self.phase.copy_(phase if isinstance(phase,torch.Tensor) else self.phase.new_full((),phase))
        self.ready.copy_((self.price<self.prices)&self.fits)
        for index,flag in enumerate(self.flags):flag.copy_((self.phase==index)&self.ready)

    def reset(self):
        _copy(self.state,self.initial);_copy(self.budget,self.inputs.budget)
        self.price.zero_();self.column.zero_();self.depth_column.zero_();self.maker_index.zero_()
        self.actual.words.zero_();self.actual.sign.zero_();self.actual.fault.zero_()
        self.required.zero_();self.fits.fill_(True)
        if hasattr(self,'effect_page'):self.effect_page.count.zero_()
        self.select_phase(0)

    def prepare_price(self):
        rows=self.inputs.order.index_select(1,self.price.clamp_max(self.prices-1).reshape(1)).squeeze(1)
        self.current_rows=rows;owners=self.owners
        self.plan=self.checked(lambda:self.prepare_price_math(self.queue(),self.state.orders,self.state.account,
            rows,self.budget,self.inputs.lowest,self.inputs.enabled,owners))
        available,_,limit,safe=self.checked(lambda:self.state.rank.prepare_take(self.plan['row_budget']))
        _copy(self.available,available);_copy(self.left,self.plan['row_budget'])
        self.limit.copy_(limit);self.safe_suffix.copy_(safe)
        self.taken.words.zero_();self.taken.sign.zero_();self.taken.fault.zero_()
        self.column.zero_()
        rank_ready=(self.limit>0)&~(self.safe_suffix&~self.left.sign.any())
        self.select_phase(torch.where(rank_ready,1,torch.where(self.depth_column>=self.capacity,3,2)))

    def rank_column(self):
        indices=self.axis+self.column.clamp_max(self.capacity-1)
        take,left=self.checked(lambda:fifo_column(self.available,self.left,indices))
        _copy(self.taken,self.checked(lambda:_put(self.taken,indices,take)));_copy(self.left,left)
        self.column.add_(1)
        done=(self.column>=self.limit)|(self.safe_suffix&~self.left.sign.any())
        self.select_phase(torch.where(done,torch.where(self.depth_column>=self.capacity,3,2),1))

    def depth_step(self):
        column=self.depth_column.clamp_max(self.capacity-1);q=self.readonly_queue
        actual=self.checked(lambda:self.actual+(q._at(q.total_base,column)-q._at(q.consumed_base,column)).select(
            q.kind.index_select(1,column.reshape(1)).squeeze(1)==1,self.actual.constant(0)))
        _copy(self.actual,actual);self.depth_column.add_(1)
        self.select_phase(torch.where(self.depth_column>=self.capacity,3,2))

    def prepare_maker(self):
        suffix=(self.positions>=self.column)&(self.positions<self.limit)
        taken=self.checked(lambda:_repeat(self.left,self.capacity).select(suffix,self.taken))
        rank,left,ids,maker=self.checked(lambda:self.state.rank.finish_take(taken,self.left,self.available.constant(0)))
        # 原take的ids属于进入rank；compact安装不能覆盖后继Maker的来源列。
        ids=ids.clone()
        queue,orders,budget,effects=self.checked(lambda:self.finish_price_math(self.queue(),self.state.orders,self.state.account,
            self.budget,self.owners,self.plan,rank,left,ids,maker,self.actual))
        _copy(self.state.rank,queue.conservative);_copy(self.state.orders,orders);_copy(self.budget,budget)
        _copy(self.price_original,self.state)
        zero=self.inputs.budget.constant(0)
        self.prepared=self.checked(lambda:prepare_base_level_transition(self.queue(self.price_original),self.price_original.orders,
            self.price_original.account,self.price_original.fills,self.calendars(self.price_original),
            self.current_rows,zero,zero,self.plan['price'],self.inputs.conditions,self.inputs.at,
            self.plan['active'],self.inputs.paths,self.owners,self.assets,self.inputs.fee,self.inputs.exponent,self.inputs.rebate,
            queue_effects=effects))
        _copy(self.state.queue_fault,self.prepared['queue'].fault)
        self.maker_state=MakerState(self.state.orders,self.state.account,self.state.fills,self.state.maker,
            _owned(self.prepared['fault'],self.prepared['fault']),
            _owned(self.prepared['demand_records'],self.prepared['demand_records']),
            _owned(self.prepared['demand_maker_fills'],self.prepared['demand_maker_fills']),
            _owned(self.prepared['demand_maker_days'],self.prepared['demand_maker_days']))
        safe=self.prepared['safe']
        self.maker_inputs=MakerInputs(effects.order_ids,effects.maker_base_units,safe,self.assets[safe],
            self.inputs.conditions,self.plan['price'],self.inputs.at,self.inputs.paths,self.prepared['active'],
            self.inputs.fee,self.inputs.exponent,self.inputs.rebate)
        self.maker_index.zero_()
        if hasattr(self,'maker_page'):self.maker_page.count.zero_()
        amounts=effects.maker_base_units.sign.reshape(self.rows,self.capacity)[safe]
        positive=(self.prepared['active'][:,None]&(amounts>0)).any()
        self.select_phase(torch.where(positive,4,5))

    def maker_step(self):
        state,frame=self.checked(lambda:maker_column(self.maker_state,self.maker_inputs,self.maker_index.clamp_max(self.capacity-1)))
        self.maker_page.append(frame,self.yes,self.yes)
        _copy(self.maker_state,state);self.maker_index.add_(1)
        self.select_phase(torch.where(self.maker_index>=self.capacity,5,4))

    def finish_candidate(self):
        return finish_base_level_transition(self.prepared,self.queue(),self.maker_state.orders,self.maker_state.account,
            self.maker_state.fills,self.maker_state.maker,self.maker_state.fault,
            self.maker_state.records,self.maker_state.maker_fills,self.maker_state.maker_days,self.maker_page.snapshot())

    def finish_price(self):
        queue,orders,account,fills,calendars,effect=self.checked(self.finish_candidate)
        _copy(self.state,_State(queue.conservative,queue.fault,orders,account,fills,calendars.maker,calendars.taker.fault))
        self.effect_page.append(effect,self.yes,self.yes)
        self.price.add_(1);self.select_phase(0)

    def replay(self,state,inputs,queue,calendars,assets):
        readonly=replace(queue,conservative=None,fault=None)
        _copy(self.readonly_queue,_capacity_like(readonly,self.readonly_queue))
        readonly=replace(calendars,maker=None,taker=replace(calendars.taker,fault=None))
        _copy(self.readonly_calendars,_capacity_like(readonly,self.readonly_calendars))
        self.assets.copy_(assets)
        _copy(self.initial,_capacity_like(state,self.initial));_copy(self.inputs,_capacity_like(inputs,self.inputs))
        self.loop.replay();Numbers.record_resource_request(self.required.clone())
        output=_owned(self.state,self.state)
        return output,self.effect_page.snapshot()


class PublicTradeDeviceLoop:
    def __init__(self):self.current=None;self.key=None;self.width=4

    def run(self,compute):
        while True:
            try:return compute()
            except _LayoutNeeded as need:
                self.current=None;self.key=None
                self.width=max(self.width,need.width)
                self.current=_CapturedTrade(need.state,need.inputs,need.queue,need.calendars,need.assets,need.width)
                self.key=need.key

    def take(self,world,order,budget,lowest,enabled,conditions,at,paths,fee,exponent,rebate,*,queue=None,assets=None,owners=None):
        queue=world.hidden.queues if queue is None else queue
        assets=world.hidden.level_assets if assets is None else assets
        owners=world.account.asset_roots[assets] if owners is None else owners
        state=replace(_state(world),rank=queue.conservative,queue_fault=queue.fault)
        inputs=_Inputs(order,budget,lowest,enabled,conditions,at,paths,fee,exponent,rebate,owners)
        readonly_queue=replace(queue,conservative=None,fault=None)
        readonly_taker=replace(world.calendars.taker,fault=None)
        width=max([self.width,Numbers.device_word_capacity(),*_widths(state),*_widths(inputs)])
        key=(_signature(state),_signature(inputs),_signature(readonly_queue),_signature(readonly_taker),_signature(assets),
            width,at.device,torch.cuda.current_stream(at.device).cuda_stream)
        if key!=self.key:raise _LayoutNeeded(key,state,inputs,queue,world.calendars,assets,width)
        return self.current.replay(state,inputs,queue,world.calendars,assets)
