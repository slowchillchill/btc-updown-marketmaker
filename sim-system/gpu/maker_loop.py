"""原Level内Maker单列候选的私有设备WHILE；不提交账户或源游标。"""
from dataclasses import dataclass, fields, is_dataclass, replace
import torch
from queues import Numbers, _take
from maker_execution import maker_column
from base_runtime_storage import _owned, _tensor_pairs, _shared, SimFeedbackPage


@dataclass(frozen=True)
class _State:
    orders: object
    account: object
    fills: object
    maker: object
    fault: object
    records: object
    maker_fills: object
    maker_days: object


@dataclass(frozen=True)
class _Inputs:
    order_ids: object
    amounts: object
    rows: object
    assets: object
    condition: object
    price: object
    at: object
    paths: object
    active: object
    fee: object
    exponent: int
    rebate: object


def _column(state, inputs, column):
    capacity = inputs.order_ids.size(1)
    indices = inputs.rows * capacity + column
    root_indices = torch.arange(inputs.rows.numel(), device=inputs.at.device) * capacity + column
    result = maker_column(state.orders, state.account, state.fills, state.maker, state.fault,
        inputs.order_ids.flatten().index_select(0, indices), _take(inputs.amounts, indices),
        inputs.assets, inputs.condition, inputs.price, inputs.at,
        inputs.paths.flatten().index_select(0, root_indices), inputs.active,
        inputs.fee, inputs.exponent, inputs.rebate)
    orders, account, fills, maker, fault, frame, allocation, demand = result
    return _State(orders, account, fills, maker, fault,
        torch.maximum(state.records, allocation.required_records),
        torch.maximum(state.maker_fills, demand.fill_capacity),
        torch.maximum(state.maker_days, demand.day_capacity)), frame


def _copy(target, incoming):
    for left, right in _tensor_pairs(target, incoming):
        left.copy_(right)


def _signature(value):
    if isinstance(value, torch.Tensor):
        return tuple(value.shape), value.dtype
    if is_dataclass(value):
        return type(value), tuple((f.name, id(getattr(value, f.name)) if _shared(value, f.name)
            else _signature(getattr(value, f.name))) for f in fields(value))
    if isinstance(value, tuple):
        return tuple(_signature(item) for item in value)
    return value


class _LayoutNeeded(Exception):
    def __init__(self, key, state, inputs, width):
        self.key, self.state, self.inputs, self.width = key, state, inputs, width


class _CapturedMaker:
    def __init__(self, state, inputs, width):
        # 冷物理布局只补高位零；同一原拥有权遍历收集全部操作数字宽。
        word_widths = [width]
        self.inputs = _owned(inputs, inputs, word_widths=word_widths)
        private = _owned(state, state, word_widths=word_widths)
        self.initial = _owned(private, private, word_capacity=max(word_widths))
        self.state = _owned(self.initial, self.initial)
        device = inputs.at.device
        self.column = torch.zeros((), dtype=torch.int64, device=device)
        # 从最终私有state取完整反馈模板；预计算业务结果从不安装。
        preview = Numbers.device_exact_batch(lambda: _column(self.state, self.inputs, self.column),
            word_capacity=width, device=device)
        _, frame = preview.value
        self.page = SimFeedbackPage(frame, inputs.order_ids.size(1), device=device)
        self.ready = torch.zeros((), dtype=torch.bool, device=device)
        self.required = torch.zeros((), dtype=torch.int64, device=device)
        self.fits = torch.ones((), dtype=torch.bool, device=device)
        self.yes = torch.ones((), dtype=torch.bool, device=device)

        def body():
            candidate = Numbers.device_exact_batch(lambda: _column(self.state, self.inputs, self.column),
                word_capacity=width, device=device)
            state, transition = candidate.value
            # emitted可借旧next_sequence：完整反馈必须先保存，随后覆盖scratch。
            self.page.append(transition, self.yes, self.yes)
            _copy(self.state, state)
            self.required.copy_(torch.maximum(self.required, candidate.required_words))
            self.fits.logical_and_(candidate.fits)
            self.column.add_(1)
            # 原循环进入后处理全部列；业务fault不跳过后列的原错误位。
            self.ready.copy_((self.column < inputs.order_ids.size(1)) & self.fits)

        stream = torch.cuda.Stream(device=device)
        stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                _copy(self.state, self.initial)
                self.column.zero_()
                self.page.count.zero_()
                body()
        torch.cuda.current_stream(device).wait_stream(stream)
        _copy(self.state, self.initial)
        self.column.zero_()
        self.page.count.zero_()
        self.body = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            body()
        self.entry = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            _copy(self.state, self.initial)
            self.column.zero_()
            self.page.count.zero_()
            self.required.zero_()
            self.fits.fill_(True)
            amounts = self.inputs.amounts.sign.reshape(self.inputs.order_ids.shape)[self.inputs.rows]
            self.ready.copy_((self.inputs.active[:, None] & (amounts > 0)).any())
        self.loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(), self.body.raw_cuda_graph(), self.ready)

    def replay(self, state, inputs, *, device_feedback=False):
        # 原池布局严格绑定，不用更大图pad较小fill/day/order池。
        _copy(self.initial, _owned(state, self.initial))
        _copy(self.inputs, inputs)
        self.loop.replay()
        required = self.required.clone()
        Numbers.record_resource_request(required)
        output = _owned(self.state, self.state)
        if device_feedback:
            return output,self.page.snapshot()
        # 设备业务已全部完成；此唯一count读取只装箱原0/capacity物理反馈。
        count = int(self.page.count.cpu())
        frames = tuple(_owned(self.page.read(i), self.page.read(i)) for i in range(count))
        return output, frames


class MakerDeviceLoop:
    """一次未提交调用临时持有遇到的真实布局，退出后仅保最终布局。"""
    def __init__(self, *, device_feedback=False):
        self.device_feedback=device_feedback
        self.current = None
        self.layouts = None

    def run(self, compute):
        self.layouts = {} if self.current is None else {self.current[0]: self.current[1]}
        try:
            while True:
                try:
                    return compute()
                except _LayoutNeeded as need:
                    # 外层fifo/run_exact_batch的finally已恢复ContextVar，才冷capture。
                    self.layouts[need.key] = _CapturedMaker(need.state, need.inputs, need.width)
        finally:
            # 正常、业务故障与异常均释放本次其它布局；scratch不是经济状态。
            self.layouts = None

    def take(self, orders, account, fills, maker, fault, records, maker_fills, maker_days,
             order_ids, amounts, rows, assets, condition, price, at, paths, active,
             fee, exponent, rebate):
        state = _State(orders, account, fills, maker, fault, records, maker_fills, maker_days)
        inputs = _Inputs(order_ids, amounts, rows, assets, condition, price, at, paths,
                         active, fee, exponent, rebate)
        width = Numbers.device_word_capacity()
        key = (_signature(state), _signature(inputs), width, at.device,
               torch.cuda.current_stream(at.device).cuda_stream)
        captured = self.layouts.get(key) if self.layouts is not None else None
        if captured is None:
            raise _LayoutNeeded(key, state, inputs, width)
        self.current = key, captured
        output, frames = captured.replay(state, inputs,device_feedback=self.device_feedback)
        return (output.orders, output.account, output.fills, output.maker, output.fault, frames,
                output.records, output.maker_fills, output.maker_days)
