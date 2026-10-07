"""固定物理布局的SIM活动存储，供真实候选图提交后继续使用同一地址。

冷边界安装容量与独立存储；共享源块/密封路径bank保持只读。业务候选仍由
BaseSimRuntime生成。本层只提交已经共同协调的候选，不选择阶段或扩容量。
冻结观察必须取snapshot，不能借可原位提交的活动张量。
"""
from dataclasses import dataclass, fields, is_dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers
from fill_finality import FillRecord
from hidden_base_execution import HiddenBaseEffects


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


def _shared(value, name):
    return name == 'columns' or isinstance(value, FillRecord) and name in (
        'offsets', 'states', 'lengths', 'economic_ordinals')


def _owned(value, layout):
    if isinstance(value, ExactInt):
        # 布局只来自冷资源准备；不能截断原精确值或人口。
        width = max(value.words.size(1), layout.words.size(1))
        value = value.with_capacity(width)
        return ExactInt(value.words.clone(), value.sign.clone(), value.fault.clone())
    if isinstance(value, torch.Tensor):
        if value.shape != layout.shape or value.dtype != layout.dtype or value.device != layout.device:
            raise ValueError('活动SIM容量须先在冷边界与候选布局对齐')
        # 分别拥有各字段，不能让初始同值的cash/available等共享可写存储。
        return value.clone()
    if is_dataclass(value):
        return replace(value, **{field.name: getattr(value, field.name) if _shared(value, field.name)
            else _owned(getattr(value, field.name), getattr(layout, field.name)) for field in fields(value)})
    if isinstance(value, tuple):
        if len(value) != len(layout):
            raise ValueError('固定SIM输出人口不一致')
        return tuple(_owned(item, target) for item, target in zip(value, layout))
    if value is None or isinstance(value, (str, int, bool)):
        if value != layout:
            raise ValueError('活动SIM物理布局元数据不一致')
        return value
    raise TypeError('活动SIM存储只接受原状态类型与已安装共享列')


def _tensor_pairs(target, candidate):
    if isinstance(target, torch.Tensor):
        if (target.shape != candidate.shape or target.dtype != candidate.dtype or
                target.device != candidate.device):
            raise ValueError('候选物理布局变化须退出图并从未提交输入准备资源')
        yield target, candidate
    elif is_dataclass(target):
        for field in fields(target):
            if not _shared(target, field.name):
                yield from _tensor_pairs(getattr(target, field.name), getattr(candidate, field.name))
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
    def __init__(self, template, capacity):
        if type(capacity) is not int or capacity < 1:
            raise ValueError('SIM反馈页须有正物理容量')
        self.capacity = capacity
        self.template = template
        self.buffers = []
        def allocate(value):
            self.buffers.append(torch.zeros((capacity, *value.shape), dtype=value.dtype, device=value.device))
            return value
        _page_map(template, allocate)
        self.count = torch.zeros((), dtype=torch.int64, device=template.at.device)

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
