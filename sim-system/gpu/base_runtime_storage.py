"""固定物理布局的SIM活动存储，供真实候选图提交后继续使用同一地址。

冷边界安装容量与独立存储；共享源块/密封路径bank保持只读。业务候选仍由
BaseSimRuntime生成。本层只提交已经共同协调的候选，不选择阶段或扩容量。
冻结观察必须取snapshot，不能借可原位提交的活动张量。
"""
from dataclasses import fields, is_dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers
from fill_finality import FillRecord
from hidden_base_execution import HiddenBaseEffects


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
        if not isinstance(output, HiddenBaseEffects):
            raise TypeError('固定地址输出提交当前只接原hidden Base effects')
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
        return replace(output, fills=tuple(committed(frame) for frame in output.fills))


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
            self.resources_fit.fill_(True)
            self.ready.copy_(ready())
        # 图句柄是冷资源元数据；所有业务条件仍在上述设备列中。
        self._loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(), self.body.raw_cuda_graph(), self.ready)

    def replay(self):
        self._loop.replay()
