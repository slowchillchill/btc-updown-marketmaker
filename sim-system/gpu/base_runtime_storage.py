"""固定物理布局的SIM活动存储，供真实候选图提交后继续使用同一地址。

冷边界安装容量与独立存储；共享源块/密封路径bank保持只读。业务候选仍由
BaseSimRuntime生成。本层只提交已经共同协调的候选，不选择阶段或扩容量。
冻结观察必须取snapshot，不能借可原位提交的活动张量。
"""
from dataclasses import dataclass, fields, is_dataclass, replace
from contextvars import ContextVar
from contextlib import contextmanager
import torch
from queues import ExactInt, ExactRational, Numbers
from fill_finality import FillRecord
from hidden_base_execution import HiddenBaseEffects


_queue_stage = ContextVar('sim_queue_stage',default=None)


def queue_stage_for(queue):
    stage=_queue_stage.get()
    return stage if stage is not None and stage.matches(queue) else None


class StageQueueStorage:
    """原hidden桶的私有行缓冲；未改行借进入态，共同安装前不写活动FIFO。

    同行后继取最近成功候选。单步精度或池重算只产生pending行，不写scratch。
    dirty仅是本次候选的物理写集，不进入检查点或经济状态。
    """
    def __init__(self, queue):
        self.layout=queue
        self.scratch=None
        self.dirty=torch.zeros(queue.rows,dtype=torch.bool,device=queue.kind.device)
        self.pending={}
        self.counts=None
        self.installed_counts=None
        self.count_binding=None
        self.addresses=None
        self.address_values=None
        self.binding_owners=()

    @staticmethod
    def _binding_fields(queue):
        result=[(('kind',),queue.kind),(('order_ids',),queue.order_ids)]
        for name in ('total_base','total_quote','consumed_base','account_remaining','debt'):
            result.extend(((name,column),getattr(getattr(queue,name),column)) for column in ('words','sign','fault'))
        if queue.conservative is not None:
            result.extend((('conservative',name),getattr(queue.conservative,name)) for name in ('kind','order_ids'))
            result.extend((('conservative','remaining',column),getattr(queue.conservative.remaining,column))
                for column in ('words','sign','fault'))
        return result

    def _bind(self,queue):
        # 派生物理地址，不保存业务值。拥有者保留至本次设备工作完成；
        # raw CUDA读取同样登记当前流，释放不能早于该流中已排队的操作。
        fields=self._binding_fields(queue)
        self.binding_slots={path:index for index,(path,_) in enumerate(fields)}
        self.binding_owners=tuple(value for _,value in fields)
        values=tuple(value.data_ptr() for value in self.binding_owners)
        stream=torch.cuda.current_stream(queue.kind.device)
        for value in self.binding_owners:value.record_stream(stream)
        if values!=self.address_values:
            incoming=torch.tensor(values,dtype=torch.int64,device=queue.kind.device)
            if self.addresses is None:self.addresses=incoming
            else:self.addresses.copy_(incoming)
            self.address_values=values

    @staticmethod
    def _allocate(value):
        if isinstance(value,torch.Tensor):return torch.empty_like(value)
        if is_dataclass(value):return replace(value,**{f.name:StageQueueStorage._allocate(getattr(value,f.name)) for f in fields(value)})
        return value

    def matches(self, queue):
        return queue.price is self.original.price and queue.kind.shape==self.original.kind.shape

    @contextmanager
    def begin(self, queue):
        self.original=queue;self.dirty.zero_();self.pending.clear()
        self._bind(queue)
        if self.count_binding!=queue.kind.data_ptr() and self.installed_counts is not None:
            self.installed_counts.copy_((queue.kind!=0).sum(1))
        self.count_binding=queue.kind.data_ptr()
        if self.installed_counts is None:self.counts=None
        elif self.counts is None:self.counts=self.installed_counts.clone()
        else:self.counts.copy_(self.installed_counts)
        if self.scratch is not None:
            self.scratch=replace(self.scratch,price=queue.price,fault=queue.fault)
        token=_queue_stage.set(self)
        try:yield self
        finally:_queue_stage.reset(token)

    def take_rows(self, queue, rows):
        # 原host控制/资源协调可返回新的完整私有queue；局部读取随它刷新。
        # 捕获内以begin已经刷新过的固定描述为输入，不构造CPU地址数据。
        if not Numbers.device_batch_active():self._bind(queue)
        from queues import _take
        enabled=torch.ones_like(rows,dtype=torch.bool)
        def gather(path,value,count=1):
            output=torch.empty((rows.numel()*count,*value.shape[1:]),dtype=value.dtype,device=value.device)
            Numbers.public_extension().copy_bound_rows(output,self.addresses,self.binding_slots[path],rows,enabled,False)
            return output
        def exact(path,value,count):
            return ExactInt(*(gather((*path,name),getattr(value,name),count) for name in ('words','sign','fault')))
        rank=queue.conservative
        entering=replace(queue,price=ExactRational(_take(queue.price.numerator,rows),_take(queue.price.denominator,rows)),
            kind=gather(('kind',),queue.kind),order_ids=gather(('order_ids',),queue.order_ids),
            **{name:exact((name,),getattr(queue,name),queue.capacity) for name in
                ('total_base','total_quote','consumed_base','account_remaining')},
            debt=exact(('debt',),queue.debt,1),fault=queue.fault.index_select(0,rows),
            conservative=None if rank is None else replace(rank,
                kind=gather(('conservative','kind'),rank.kind),order_ids=gather(('conservative','order_ids'),rank.order_ids),
                remaining=exact(('conservative','remaining'),rank.remaining,queue.capacity)))
        # 私有scratch未准备行的字节不可作为业务值；只选择dirty行。
        if self.scratch is None:return entering
        saved=self.scratch.take_rows(rows,staged=False)
        selected=self.dirty.index_select(0,rows)
        return self._select(saved,entering,selected)

    @staticmethod
    def _select(candidate, original, enabled):
        flat=enabled.repeat_interleave(candidate.capacity)
        updates={name:torch.where(enabled[:,None],getattr(candidate,name),getattr(original,name)) for name in ('kind','order_ids')}
        updates.update({name:getattr(candidate,name).select(flat,getattr(original,name)) for name in
            ('total_base','total_quote','consumed_base','account_remaining')})
        return replace(original,**updates,debt=candidate.debt.select(enabled,original.debt),
            conservative=None if original.conservative is None else candidate.conservative.coordinate(original.conservative,enabled))

    def propose(self, queue, rows, incoming, enabled=None):
        if self.scratch is None:
            self.scratch=replace(queue,**{f.name:self._allocate(getattr(queue,f.name)) for f in fields(queue)
                if f.name not in ('price','fault')})
        enabled=torch.ones_like(rows,dtype=torch.bool) if enabled is None else enabled
        from account_state import grouped_fault
        candidate=replace(queue,fault=queue.fault|grouped_fault(incoming.fault*enabled.to(torch.int32),rows,queue.rows))
        self.pending[id(candidate)]=(candidate,rows,incoming,enabled)
        return candidate

    def coordinate(self, candidate, original, row_roots, fault):
        pending=self.pending.get(id(candidate))
        if pending is None:return None
        _,rows,incoming,enabled=pending
        entering=original.take_rows(rows)
        local=self._select(incoming,entering,fault.index_select(0,row_roots.index_select(0,rows))==0)
        local=replace(local,fault=incoming.fault|fault[row_roots[rows]])
        return self.propose(replace(original,fault=candidate.fault|fault[row_roots]),rows,local,enabled)

    def accept(self, queue, committed=None):
        pending=self.pending.get(id(queue))
        if pending is None:return queue
        _,rows,incoming,enabled=pending
        if committed is not None:enabled=enabled & committed
        widths=[getattr(incoming,name).words.size(1) for name in
            ('total_base','total_quote','consumed_base','account_remaining','debt')]
        if incoming.conservative is not None:widths.append(incoming.conservative.remaining.words.size(1))
        width=max(widths)
        self.scratch=replace(self.scratch,
            **{name:getattr(self.scratch,name).with_capacity(width) for name in
                ('total_base','total_quote','consumed_base','account_remaining','debt')},
            conservative=None if self.scratch.conservative is None else replace(self.scratch.conservative,
                remaining=self.scratch.conservative.remaining.with_capacity(width)))
        self._write(self.scratch,rows,incoming,enabled)
        self._copy_rows(self.dirty.reshape(-1,1),torch.ones_like(enabled).reshape(-1,1),rows,enabled)
        if self.counts is not None:self._copy_rows(self.counts.reshape(-1,1),(incoming.kind!=0).sum(1).reshape(-1,1),rows,enabled)
        self.pending.clear()
        return queue

    def segment_counts(self, queue):
        if self.counts is None:
            self.installed_counts=(self.original.kind!=0).sum(1)
            self.counts=self.installed_counts.clone()
        pending=self.pending.get(id(queue))
        if pending is None:return self.counts
        _,rows,incoming,enabled=pending
        result=self.counts.clone()
        self._copy_rows(result.reshape(-1,1),(incoming.kind!=0).sum(1).reshape(-1,1),rows,enabled)
        return result

    @staticmethod
    def _copy_rows(target,incoming,rows,enabled):
        Numbers.public_extension().copy_selected_rows(target,incoming,rows,enabled)

    @staticmethod
    def _write(target, rows, incoming,enabled=None):
        enabled=torch.ones_like(rows,dtype=torch.bool) if enabled is None else enabled
        copy=StageQueueStorage._copy_rows
        for name in ('kind','order_ids'):copy(getattr(target,name),getattr(incoming,name),rows,enabled)
        for name in ('total_base','total_quote','consumed_base','account_remaining','debt'):
            left,right=getattr(target,name),getattr(incoming,name)
            right=right.with_capacity(left.words.size(1))
            count=1 if name=='debt' else target.capacity
            for column in ('words','sign','fault'):
                a,b=getattr(left,column),getattr(right,column)
                tail=count*(a.size(1) if a.ndim==2 else 1)
                copy(a.reshape(target.rows,tail),b.reshape(rows.numel(),tail),rows,enabled)
        if target.conservative is not None:
            for name in ('kind','order_ids'):copy(getattr(target.conservative,name),getattr(incoming.conservative,name),rows,enabled)
            right=incoming.conservative.remaining.with_capacity(target.conservative.remaining.words.size(1))
            for name in ('words','sign','fault'):
                a,b=getattr(target.conservative.remaining,name),getattr(right,name)
                tail=target.capacity*(a.size(1) if a.ndim==2 else 1)
                copy(a.reshape(target.rows,tail),b.reshape(rows.numel(),tail),rows,enabled)

    def install(self, queue, committed=None, *, bound=False):
        # 仅原共同提交点调用；此时公开append及输入frontier已成功。
        if self.scratch is None:return queue
        # 原host资源重算/完整控制协调后的queue可能是新的同布局对象。
        # 设备物理批的当前权威对象已在begin绑定，图只读取固定描述向量。
        if not bound:self._bind(queue)
        if any(getattr(self.scratch,name).words.size(1)>getattr(queue,name).words.size(1) for name in
                ('total_base','total_quote','consumed_base','account_remaining','debt')):
            return self.materialize(queue,committed)
        rows=torch.arange(queue.rows,device=queue.kind.device)
        dirty=self.dirty if committed is None else self.dirty&committed
        # 图不能把安装地址绑定捕获时旧queue；原当前对象来自固定描述向量。
        for path,incoming in self._binding_fields(self.scratch):
            Numbers.public_extension().copy_bound_rows(incoming,self.addresses,self.binding_slots[path],rows,dirty,True)
        if self.counts is not None:
            if self.installed_counts is None:self.installed_counts=self.counts.clone()
            else:self.installed_counts.copy_(self.counts if committed is None else
                torch.where(committed,self.counts,self.installed_counts))
        return queue

    def materialize(self, queue, committed=None):
        """实际全盘控制入口需要完整私有候选；不安装到进入态。"""
        if self.scratch is None:return queue
        dirty=self.dirty if committed is None else self.dirty&committed
        rows=torch.arange(queue.rows,device=queue.kind.device)[dirty]
        token=_queue_stage.set(None)
        try:return queue.put_rows(rows,self.scratch.take_rows(rows,staged=False))
        finally:_queue_stage.reset(token)

    def rebind(self, queue):
        self.original=queue;self.layout=queue;self.scratch=None
        self.dirty=torch.zeros(queue.rows,dtype=torch.bool,device=queue.kind.device)
        self.pending.clear()
        self.counts=None;self.installed_counts=None;self.count_binding=queue.kind.data_ptr()
        self.addresses=None;self.address_values=None;self.binding_owners=()
        self._bind(queue)


@dataclass(frozen=True)
class FinalityFeedback:
    at: torch.Tensor
    transition: object
    market: object
    record: object  # 本次ordinal提交后的原成交详情，不能借后续迭代覆盖的活动列。


@dataclass(frozen=True)
class HiddenStepFeedback:
    at: torch.Tensor
    source_index: torch.Tensor
    effects: object


@dataclass(frozen=True)
class TerminalFeedback:
    effect: object
    batch: object


def _shared(value, name):
    return name in ('columns','resolution_columns','asset_roots') or isinstance(value, FillRecord) and name in (
        'offsets', 'states', 'lengths', 'economic_ordinals')


def _owned(value, layout, *, word_capacity=0, word_widths=None):
    if isinstance(value, ExactInt):
        # 布局只来自冷资源准备；不能截断原精确值或人口。
        width = max(value.words.size(1), layout.words.size(1), word_capacity)
        if word_widths is not None:
            word_widths.append(width)
        value = value.with_capacity(width)
        return ExactInt(value.words.clone(), value.sign.clone(), value.fault.clone())
    if isinstance(value, torch.Tensor):
        if value.shape != layout.shape or value.dtype != layout.dtype or value.device != layout.device:
            raise ValueError('活动SIM容量须先在冷边界与候选布局对齐')
        # 分别拥有各字段，不能让初始同值的cash/available等共享可写存储。
        return value.clone()
    if is_dataclass(value):
        return replace(value, **{field.name: getattr(value, field.name) if _shared(value, field.name)
            else _owned(getattr(value, field.name), getattr(layout, field.name),
                word_capacity=word_capacity, word_widths=word_widths) for field in fields(value)})
    if isinstance(value, tuple):
        if len(value) != len(layout):
            raise ValueError('固定SIM输出人口不一致')
        return tuple(_owned(item, target, word_capacity=word_capacity, word_widths=word_widths)
                     for item, target in zip(value, layout))
    if value is None or isinstance(value, (str, int, bool, frozenset)):
        if value != layout:
            raise ValueError('活动SIM物理布局元数据不一致')
        return value
    raise TypeError('活动SIM存储只接受原状态类型与已安装共享列')


def _capacity_like(value, layout):
    """固定输入只补Exact高位零；不先克隆一份再复制进私有地址。"""
    if isinstance(value, ExactInt):
        return value.with_capacity(layout.words.size(1))
    if is_dataclass(value):
        return replace(value, **{f.name:_capacity_like(getattr(value,f.name),getattr(layout,f.name))
            for f in fields(value) if not _shared(value,f.name)})
    if isinstance(value,tuple):
        return tuple(_capacity_like(a,b) for a,b in zip(value,layout,strict=True))
    return value


def _tensor_pairs(target, candidate):
    if target is candidate: return
    if isinstance(target, torch.Tensor):
        if (target.shape != candidate.shape or target.dtype != candidate.dtype or
                target.device != candidate.device):
            raise ValueError('候选物理布局变化须退出图并从未提交输入准备资源')
        yield target, candidate
    elif is_dataclass(target):
        for field in fields(target):
            if not _shared(target, field.name):
                yield from _tensor_pairs(getattr(target, field.name), getattr(candidate, field.name))
    elif isinstance(target,tuple):
        if len(target)!=len(candidate):raise ValueError('候选物理tuple人口变化须退出图')
        for left,right in zip(target,candidate):yield from _tensor_pairs(left,right)
    elif target != candidate:
        raise ValueError('候选不能更换活动SIM冷元数据')


def _clear_output(value):
    """设备零次迭代也输出已初始化的空反馈，而不是捕获池未写入的字节。"""
    if isinstance(value, ExactRational):
        _clear_output(value.numerator)
        _clear_output(value.denominator)
        value.denominator.words[:, 0].fill_(1)
        value.denominator.sign.fill_(1)
    elif isinstance(value, torch.Tensor):
        value.zero_()
    elif is_dataclass(value):
        for field in fields(value):
            if not _shared(value, field.name):
                _clear_output(getattr(value, field.name))
    elif isinstance(value, tuple):
        for item in value:
            _clear_output(item)


class BaseRuntimeStorage:
    """单一固定布局的活动运行状态；图与冻结副本的生命周期由宿主持有。"""
    def __init__(self, original, layout):
        self.runtime = _owned(original, layout)

    @staticmethod
    def stage_queue(queue):
        return StageQueueStorage(queue)

    def snapshot(self):
        return _owned(self.runtime, self.runtime)

    @staticmethod
    def snapshot_output(output):
        """保留本次已提交SIM输出，后续图重放不能覆盖待消费反馈。"""
        return _owned(output, output)

    def commit_hidden_base(self, candidate):
        """一条原LevelChange的字/段/fill/Maker人口提交门。"""
        state, output = candidate.value
        fit = candidate.fits & (output.required_segments <= self.runtime.hidden.queues.capacity).all()
        fit = fit & (output.required_records <= self.runtime.fills.capacity).all()
        fit = fit & (output.required_maker_fills <= self.runtime.calendars.maker.fill_occupied.size(1)).all()
        fit = fit & (output.required_maker_days <= self.runtime.calendars.maker.occupied.size(1)).all()
        return fit, self.commit(state, fit, output)

    def commit(self, candidate, resources_fit, output=None):
        """资源不足保留全部活动字段；原各根fault/回退由业务协调器负责。

        resources_fit须同时包含字容量与本次各池容量需求，不能仅取Exact fit。
        此标量只是本物理批的提交门，不替代业务根掩码或运行关闭证据。
        """
        device = self.runtime.account.cash.words.device
        if resources_fit.shape != () or resources_fit.dtype != torch.bool or resources_fit.device != device:
            raise TypeError('物理批提交门须为同设备bool标量')
        # 先构建完整固定布局映射，再发射原位写入；图内不读任何业务值。
        pairs = tuple(_tensor_pairs(self.runtime, candidate))
        with torch.no_grad():
            # emitted序号可能借原next_sequence；必须先保存，再改活动输入。
            saved_output = self.snapshot_output(self._committed_output(output, resources_fit)) if output is not None else None
            for target, incoming in pairs:
                target.copy_(torch.where(resources_fit, incoming, target))
        return saved_output

    def _committed_output(self, output, resources_fit):
        def committed(frame):
            event = frame.event
            updates = {}
            for name in ('economic_delta', 'failure_delta', 'rebate_delta'):
                delta = getattr(event, name)
                updates[name] = self.runtime.account.coordinate_delta(
                    delta, delta.emitted & resources_fit, delta.fault)
            return replace(frame, event=replace(event, emitted=event.emitted & resources_fit,
                terminal=event.terminal & resources_fit, **updates))
        # 容量需求/fault仍归冷退出协调，不把未提交候选当成成功反馈。
        if isinstance(output, HiddenBaseEffects):
            return replace(output, fills=tuple(committed(frame) for frame in output.fills))
        if isinstance(output, FinalityFeedback):
            market = output.market
            delta = market.account_delta
            market = replace(market, emitted=market.emitted & resources_fit,
                account_delta=self.runtime.account.coordinate_delta(
                    delta, delta.emitted & resources_fit, delta.fault))
            return replace(output, transition=committed(output.transition), market=market)
        if isinstance(output, HiddenStepFeedback):
            return replace(output, effects=self._committed_output(output.effects, resources_fit))
        if isinstance(output, TerminalFeedback):
            effect = output.effect
            events = tuple(replace(event,effect=replace(event.effect,
                closed=event.effect.closed & resources_fit,
                delta=self.runtime.account.coordinate_delta(event.effect.delta,
                    event.effect.delta.emitted & resources_fit,event.effect.delta.fault)))
                for event in effect.orders.events)
            effect = replace(effect,resolved=effect.resolved & resources_fit,
                orders=replace(effect.orders,events=events))
            batch = output.batch
            batch = replace(batch,emitted=batch.emitted & resources_fit,
                economic_enabled=batch.economic_enabled & resources_fit,
                normalized=replace(batch.normalized,emitted=batch.normalized.emitted & resources_fit))
            return replace(output,effect=effect,batch=batch)
        raise TypeError('固定地址输出提交只接已装配的hidden或最终性效果')


def _page_map(value, transform):
    if isinstance(value, torch.Tensor):
        return transform(value)
    if is_dataclass(value):
        return replace(value, **{field.name: getattr(value, field.name) if _shared(value, field.name)
            else _page_map(getattr(value, field.name), transform) for field in fields(value)})
    if isinstance(value, tuple):
        return tuple(_page_map(item, transform) for item in value)
    return value


class SimFeedbackPage:
    """固定物理反馈页，设备写槽；每槽保留本轮各根原效果及真实身份。

    count是物理迭代人口，不是fill/private/经济序号。页满只请求冷消费/换页，
    不丢义务；已写页在消费者与冻结引用解除前保持不变。
    """
    def __init__(self, template, capacity, *, device=None):
        if type(capacity) is not int or capacity < 1:
            raise ValueError('SIM反馈页须有正物理容量')
        self.capacity = capacity
        self.template = template
        self.buffers = []
        def allocate(value):
            self.buffers.append(torch.zeros((capacity, *value.shape), dtype=value.dtype, device=value.device))
            return value
        _page_map(template, allocate)
        self.count = torch.zeros((), dtype=torch.int64,
                                 device=template.at.device if device is None else device)

    def append(self, value, committed, population):
        slot = self.count.clamp(0, self.capacity-1).reshape(1)
        population = committed & population
        buffers = iter(self.buffers)
        def write(incoming):
            target = next(buffers)
            target.index_copy_(0, slot, torch.where(population,
                incoming.unsqueeze(0), target.index_select(0, slot)))
            return incoming
        _page_map(value, write)
        self.count.add_(population.to(torch.int64))

    def read(self, index):
        """冷消费已写物理槽；返回借同一不可变页的原类型视图。"""
        buffers = iter(self.buffers)
        return _page_map(self.template, lambda _: next(buffers)[index])

    def snapshot(self):
        """交给下一设备消费者的独立页；下次replay不能改写此输出。"""
        return SimFeedbackSnapshot(_owned(self.template,self.template),
            tuple(buffer.clone() for buffer in self.buffers),self.count.clone(),self.capacity)


@dataclass(frozen=True)
class SimFeedbackSnapshot:
    template: object
    buffers: tuple
    count: torch.Tensor
    capacity: int

    def member_buffer(self,member):
        """按原模板成员定位整页设备列；不读取人口或复制来源。"""
        buffers=iter(self.buffers);selected=[]
        def inspect(value):
            buffer=next(buffers)
            if value is member:selected.append(buffer)
            return value
        _page_map(self.template,inspect)
        if len(selected)!=1:raise ValueError('反馈成员须属于原页模板且只有一个物理列')
        return selected[0]

    def feedback_present(self):
        """原全页反馈判断留设备；未写尾槽不参与人口。"""
        event=self.template.event
        emitted=(event.emitted,event.economic_delta.emitted,event.failure_delta.emitted,
            event.rebate_delta.emitted)
        buffers=iter(self.buffers);selected=[]
        def inspect(value):
            buffer=next(buffers)
            if any(value is member for member in emitted):selected.append(buffer)
            return value
        _page_map(self.template,inspect)
        population=torch.arange(self.capacity,device=self.count.device)<self.count
        return torch.stack([value.any(dim=tuple(range(1,value.ndim)))&population
            for value in selected]).any()

    def read_device(self,index):
        """设备物理坐标gather；不以Tensor标量索引触发宿主取值。"""
        buffers=iter(self.buffers)
        return _page_map(self.template,lambda _:next(buffers).index_select(0,index.reshape(1)).squeeze(0))

    def with_capacity(self,width):
        """只为冷私有布局补原ExactInt高位零，不改变页人口或精确值。"""
        template=_owned(self.template,self.template,word_capacity=width)
        def nested(original,current):
            if isinstance(original,SimFeedbackSnapshot):return original.with_capacity(width)
            if is_dataclass(original):
                return replace(current,**{f.name:nested(getattr(original,f.name),getattr(current,f.name))
                    for f in fields(original) if not _shared(original,f.name)})
            if isinstance(original,tuple):return tuple(nested(a,b) for a,b in zip(original,current))
            return current
        template=nested(self.template,template)
        buffers=iter(self.buffers);expanded=[]
        def install(value):
            old=next(buffers)
            if old.shape[1:]!=value.shape:
                # 唯一形状变化来自上面的ExactInt.with_capacity。
                old=torch.nn.functional.pad(old,(0,value.size(-1)-old.size(-1)))
            expanded.append(old)
            return value
        _page_map(template,install)
        return replace(self,template=template,buffers=tuple(expanded))


class FinalityFeedbackPage(SimFeedbackPage):
    def append(self, value, committed):
        super().append(value, committed, value.transition.event.emitted.any())


class CapturedHiddenBuckets:
    """固定类型图按设备游标逐步消费原桶，图节点不随来源步骤数展开。

    每轮每根最多一个原step；参考等待、逐step共同提交与资源退出保持原义。
    源页仍固定地址持有，换页和反馈消费尚在冷边界。
    """
    def __init__(self, storage, horizon, enabled, conditions, selected_paths,
                 fee_rate, fee_exponent, taker_rate, market_end, *, word_capacity, feedback_capacity,
                 stage_progress=None, source_assets=None, level_sides=None):
        self.storage = storage
        source = storage.runtime.hidden_frontier.columns.source
        source_assets = source.asset_axis if source_assets is None else source_assets
        level_sides = source.level_sides if level_sides is None else level_sides
        self._inputs = (horizon, enabled, conditions, selected_paths, fee_rate, taker_rate,
                        market_end, source_assets, level_sides)
        self.ready = torch.zeros((), device=horizon.device, dtype=torch.bool)
        self.resources_fit = torch.ones((), device=horizon.device, dtype=torch.bool)
        self.required_words = torch.zeros((), device=horizon.device, dtype=torch.int64)
        self.required_feedback = torch.zeros((), device=horizon.device, dtype=torch.int64)
        self.required_segments = torch.zeros(storage.runtime.hidden.queues.rows,device=horizon.device,dtype=torch.int64)
        self.required_records = torch.zeros_like(enabled,dtype=torch.int64)
        self.required_maker_fills = torch.zeros_like(enabled,dtype=torch.int64)
        self.required_maker_days = torch.zeros_like(enabled,dtype=torch.int64)
        self.closed = torch.zeros_like(enabled)
        self.iterations = torch.zeros_like(horizon)
        self.reference_iterations = torch.zeros_like(horizon)
        def candidate(indices, step_allowed=None):
            runtime = storage.runtime
            choice = runtime.choose(None, None, horizon, enabled & self.resources_fit)
            if step_allowed is not None:
                choice = replace(choice, present=choice.present & step_allowed)
            return runtime.apply_hidden_selected(choice, indices, conditions, selected_paths,
                fee_rate, fee_exponent, taker_rate, source_assets, level_sides)
        if stage_progress is not None:
            stage_progress('cold_layout_begin')
        layout = Numbers.device_exact_batch(lambda:candidate(storage.runtime.hidden_frontier.next_step),
            word_capacity=word_capacity, device=horizon.device)
        self.page = SimFeedbackPage(HiddenStepFeedback(horizon, torch.zeros_like(horizon), layout.value[1]),
                                    feedback_capacity)
        def ready():
            return storage.runtime.choose(None, None, horizon, enabled).enabled(4).any()
        if stage_progress is not None:
            stage_progress('body_capture_begin')
        self.body = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            runtime = storage.runtime
            before_bucket = runtime.hidden_frontier.next_bucket.clone()
            before_step = runtime.hidden_frontier.next_step.clone()
            before_reference = runtime.hidden_frontier.next_reference.clone()
            before_applying = runtime.hidden_frontier.applying.clone()
            def begin():
                runtime = storage.runtime
                choice = runtime.choose(None, None, horizon, enabled & self.resources_fit)
                return runtime.advance_role_time(choice, market_end).begin_hidden_bucket(choice)
            opening = Numbers.device_exact_batch(begin, word_capacity=word_capacity, device=horizon.device)
            self.required_words.copy_(torch.maximum(self.required_words,opening.required_words))
            self.resources_fit.logical_and_(opening.fits)
            storage.commit(opening.value,self.resources_fit)
            reference_indices = storage.runtime.hidden_frontier.next_reference.clone()
            reference_round = storage.runtime.hidden_frontier.reference_enabled(reference_indices)
            def reference_candidate():
                runtime = storage.runtime
                choice = runtime.choose(None, None, horizon, enabled & self.resources_fit)
                return runtime.apply_hidden_reference(choice, reference_indices, source_assets)
            reference = Numbers.device_exact_batch(reference_candidate, word_capacity=word_capacity, device=horizon.device)
            self.required_words.copy_(torch.maximum(self.required_words,reference.required_words))
            self.resources_fit.logical_and_(reference.fits)
            storage.commit(reference.value, self.resources_fit)
            self.reference_iterations.add_((storage.runtime.hidden_frontier.next_reference > reference_indices).to(torch.int64))
            indices = storage.runtime.hidden_frontier.next_step.clone()
            value = Numbers.device_exact_batch(lambda:candidate(indices, ~reference_round),
                word_capacity=word_capacity, device=horizon.device)
            state, effect = value.value
            self.required_words.copy_(torch.maximum(self.required_words,value.required_words))
            for name in ('required_segments','required_records','required_maker_fills','required_maker_days'):
                target=getattr(self,name)
                target.copy_(torch.maximum(target,getattr(effect,name)))
            population = (torch.stack([frame.event.emitted for frame in effect.fills]).any() if effect.fills
                          else torch.zeros((),device=horizon.device,dtype=torch.bool))
            need = self.page.count+population.to(torch.int64)
            self.required_feedback.copy_(torch.maximum(self.required_feedback,need))
            fit = self.resources_fit & value.fits & (need <= self.page.capacity)
            fit = fit & (effect.required_segments <= storage.runtime.hidden.queues.capacity).all()
            fit = fit & (effect.required_records <= storage.runtime.fills.capacity).all()
            fit = fit & (effect.required_maker_fills <= storage.runtime.calendars.maker.fill_occupied.size(1)).all()
            fit = fit & (effect.required_maker_days <= storage.runtime.calendars.maker.occupied.size(1)).all()
            # 原进入step的完整来源句柄随反馈保存，不能用迭代后的游标。
            columns = storage.runtime.hidden_frontier.columns
            at = columns.at[before_bucket.clamp(0,columns.at.numel()-1)] if columns.at.numel() else torch.zeros_like(horizon)
            output = storage.commit(state,fit,HiddenStepFeedback(at,indices,effect))
            self.page.append(output,fit,population)
            self.iterations.add_((storage.runtime.hidden_frontier.next_step > indices).to(torch.int64))
            self.resources_fit.logical_and_(fit)
            finishing = Numbers.device_exact_batch(lambda:storage.runtime.finish_hidden_bucket(),
                word_capacity=word_capacity, device=horizon.device)
            self.required_words.copy_(torch.maximum(self.required_words,finishing.required_words))
            self.resources_fit.logical_and_(finishing.fits)
            storage.commit(finishing.value[0],self.resources_fit)
            self.closed.logical_or_(finishing.value[1] & self.resources_fit)
            frontier = storage.runtime.hidden_frontier
            progress = (frontier.next_bucket != before_bucket) | (frontier.next_step != before_step)
            progress = progress | (frontier.next_reference != before_reference)
            progress = progress | (frontier.applying != before_applying)
            self.ready.copy_(ready() & self.resources_fit & progress.any())
        if stage_progress is not None:
            stage_progress('body_capture_end')
        self.entry = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            self.required_words.zero_()
            self.required_feedback.zero_()
            self.required_segments.zero_()
            self.required_records.zero_()
            self.required_maker_fills.zero_()
            self.required_maker_days.zero_()
            self.resources_fit.fill_(True)
            self.closed.zero_()
            self.iterations.zero_()
            self.reference_iterations.zero_()
            self.ready.copy_(ready())
        if stage_progress is not None:
            stage_progress('native_instantiate_begin')
        self._loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)
        if stage_progress is not None:
            stage_progress('native_instantiate_end')

    def replay(self):
        self._loop.replay()


class CapturedFinalitySteps:
    """设备选择并连续消费当前最终性阶段，输出页与活动状态共同提交。

    其它阶段到来即交还宿主；尚不宣称统一多阶段driver或NOE页在图内追加。
    """
    def __init__(self, storage, horizon, enabled, maker_rate, taker_rate, market_end,
                 *, word_capacity, feedback_capacity):
        from fill_pool import _take_record
        self.storage = storage
        self._inputs = (horizon, enabled, maker_rate, taker_rate, market_end)
        self.ready = torch.zeros((), device=horizon.device, dtype=torch.bool)
        self.required_feedback = torch.zeros((), device=horizon.device, dtype=torch.int64)
        def ready():
            return storage.runtime.choose(None, None, horizon, enabled).enabled(2).any()
        # 页布局来自原纯候选，冷准备只安装资源，不提交业务。
        def compute():
            runtime = storage.runtime
            choice = runtime.choose(None, None, horizon, enabled)
            state, transition, market = runtime.finality(choice, maker_rate, taker_rate, market_end)
            index = torch.arange(state.fills.roots, device=horizon.device)*state.fills.capacity
            index = index + transition.slot.clamp(0, state.fills.capacity-1)
            return state, FinalityFeedback(choice.at, transition, market, _take_record(state.fills.records, index))
        layout = Numbers.device_exact_batch(compute, word_capacity=word_capacity, device=horizon.device)
        self.page = FinalityFeedbackPage(layout.value[1], feedback_capacity)
        self.body = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            self.candidate = Numbers.device_exact_batch(compute, word_capacity=word_capacity, device=horizon.device)
            self.required_feedback.copy_(self.page.count+1)
            self.resources_fit = self.candidate.fits & (self.required_feedback <= self.page.capacity)
            self.output = storage.commit(self.candidate.value[0], self.resources_fit, self.candidate.value[1])
            self.page.append(self.output, self.resources_fit)
            fault = self.output.transition.event.fault | self.output.market.fault
            self.ready.copy_(ready() & self.resources_fit & (fault == 0).all())
        self.entry = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            _clear_output(self.output)
            self.candidate.required_words.zero_()
            self.candidate.fits.fill_(True)
            self.resources_fit.fill_(True)
            self.required_feedback.zero_()
            self.ready.copy_(ready())
        self._loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(), self.body.raw_cuda_graph(), self.ready)

    def replay(self):
        self._loop.replay()


class CapturedHiddenBaseStep:
    """实际原LevelChange的设备条件入口；尚不分派其它源步/阶段。

    CUDA WHILE读真实游标/phase/资源门；成功消费后退出，需求不足也退出供冷协调。
    原固定源步索引及布局在冷边界绑定。这里不宣称跨步循环或完整资源重捕获。
    """
    def __init__(self, storage, index, horizon, enabled, conditions, selected_paths,
                 fee_rate, fee_exponent, rebate_rate, *, word_capacity):
        self.storage = storage
        self._inputs = (horizon, enabled, conditions, selected_paths, fee_rate, rebate_rate)
        self.ready = torch.zeros((), device=horizon.device, dtype=torch.bool)
        def ready():
            runtime = storage.runtime
            choice = runtime.choose(None, None, horizon, enabled)
            return (choice.enabled(4) & runtime.hidden_frontier.step_enabled(index)).any()
        self.body = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            def compute():
                runtime = storage.runtime
                choice = runtime.choose(None, None, horizon, enabled)
                return runtime.apply_hidden_step(choice, index, conditions, selected_paths,
                    fee_rate, fee_exponent, rebate_rate)
            self.candidate = Numbers.device_exact_batch(compute, word_capacity=word_capacity,
                device=horizon.device)
            self.resources_fit, self.output = storage.commit_hidden_base(self.candidate)
            self.ready.copy_(ready() & self.resources_fit)
        self.entry = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            # 零次迭代时不暴露旧输出或首次捕获的未初始化字段；不重新领取反馈。
            _clear_output(self.output)
            self.candidate.required_words.zero_()
            self.candidate.fits.fill_(True)
            self.resources_fit.fill_(True)
            self.ready.copy_(ready())
        # 图句柄是冷资源元数据；所有业务条件仍在上述设备列中。
        self._loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(), self.body.raw_cuda_graph(), self.ready)

    def replay(self):
        self._loop.replay()
