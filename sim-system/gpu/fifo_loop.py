"""原FIFO单列精确候选的设备WHILE；scratch不持有账户或源前沿。"""
import torch
from queues import Numbers, ExactInt, _take, _minimum


class _LayoutNeeded(Exception):
    def __init__(self, rows, capacity, width, device, stream):
        self.layout = rows, capacity, width, device, stream


def _column(available, remaining, indices):
    take = _minimum(_take(available, indices), remaining)
    return take, remaining - take


def _copy(target, value):
    target.words.copy_(value.words)
    target.sign.copy_(value.sign)
    target.fault.copy_(value.fault)


def _empty(rows, width, device):
    return ExactInt(torch.zeros((rows, width), dtype=torch.int32, device=device),
                    torch.zeros(rows, dtype=torch.int8, device=device),
                    torch.zeros(rows, dtype=torch.int32, device=device))


class _CapturedColumns:
    def __init__(self, rows, capacity, width, device):
        self.available = _empty(rows * capacity, width, device)
        self.input_remaining = _empty(rows, width, device)
        self.remaining = _empty(rows, width, device)
        self.takes = _empty(rows * capacity, width, device)
        self.limit = torch.zeros((), dtype=torch.int64, device=device)
        self.column = torch.zeros_like(self.limit)
        self.safe_suffix = torch.zeros((), dtype=torch.bool, device=device)
        self.ready = torch.zeros_like(self.safe_suffix)
        self.required_words = torch.zeros_like(self.limit)
        self.fits = torch.ones_like(self.safe_suffix)
        self.row_axis = torch.arange(rows, device=device) * capacity
        self.positions = torch.arange(capacity, device=device)[None, :].expand(rows, capacity).flatten()

        def body():
            indices = self.row_axis + self.column
            candidate = Numbers.device_exact_batch(
                lambda: _column(self.available, self.remaining, indices),
                word_capacity=width, device=device)
            take, remaining = candidate.value
            self.takes.words.index_copy_(0, indices, take.words)
            self.takes.sign.index_copy_(0, indices, take.sign)
            self.takes.fault.index_copy_(0, indices, take.fault)
            _copy(self.remaining, remaining)
            self.required_words.copy_(torch.maximum(self.required_words, candidate.required_words))
            self.fits.logical_and_(candidate.fits)
            self.column.add_(1)
            self.ready.copy_((self.column < self.limit) & self.fits &
                ~(self.safe_suffix & ~self.remaining.sign.any()))

        # 原扩展与算术先在旁流warm；仅操作私有scratch，不消费实际输入。
        stream = torch.cuda.Stream(device=device)
        stream.wait_stream(torch.cuda.current_stream(device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                self.column.zero_()
                body()
        torch.cuda.current_stream(device).wait_stream(stream)
        self.column.zero_()
        self.body = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            body()
        self.entry = torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            _copy(self.remaining, self.input_remaining)
            self.takes.words.zero_()
            self.takes.sign.zero_()
            self.takes.fault.zero_()
            self.column.zero_()
            self.required_words.zero_()
            self.fits.fill_(True)
            self.ready.copy_((self.limit > 0) &
                ~(self.safe_suffix & ~self.remaining.sign.any()))
        self.loop = Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(), self.body.raw_cuda_graph(), self.ready)

    def replay(self, available, remaining, limit, safe_suffix):
        width = self.available.words.size(1)
        _copy(self.available, available.with_capacity(width))
        _copy(self.input_remaining, remaining.with_capacity(width))
        self.limit.copy_(limit)
        self.safe_suffix.copy_(safe_suffix)
        self.loop.replay()
        # suffix只携当时remaining；各前缀列保自己的take.fault。
        suffix = (self.positions >= self.column) & (self.positions < self.limit)
        rows = self.remaining.sign.numel()
        capacity = self.positions.numel() // rows
        tail = ExactInt(self.remaining.words.repeat_interleave(capacity, 0),
                        self.remaining.sign.repeat_interleave(capacity),
                        self.remaining.fault.repeat_interleave(capacity))
        takes = tail.select(suffix, self.takes)
        remaining = ExactInt(self.remaining.words.clone(), self.remaining.sign.clone(),
                             self.remaining.fault.clone())
        required = self.required_words.clone()
        Numbers.record_resource_request(required)
        return takes, remaining


class FifoDeviceLoop:
    """一个连续宿主/当前流只保一个布局；已知字宽供外层同输入资源重算。"""
    def __init__(self):
        self.width = 4
        self.layout = None
        self.captured = None

    def run(self, compute):
        while True:
            try:
                return Numbers.run_exact_batch(compute, initial_word_capacity=self.width)
            except _LayoutNeeded as need:
                # run_exact_batch的finally已恢复原ContextVar，才允许冷capture。
                rows, capacity, width, device, stream = need.layout
                self.width = max(self.width, width)
                self.captured = None
                self.layout = None
                self.captured = _CapturedColumns(rows, capacity, self.width, device)
                self.layout = rows, capacity, self.width, device, stream

    def take(self, available, remaining, limit, safe_suffix, *, rows, capacity):
        device = available.words.device
        width = max(self.width, Numbers.device_word_capacity(), available.words.size(1),
                    remaining.words.size(1))
        stream = torch.cuda.current_stream(device).cuda_stream
        # 同一原候选可能先小池再续接大池；一个足够大的私有布局同时承接二者。
        # 只改变scratch物理表示，原queue.capacity、limit和段需求不变。
        captured_capacity = capacity
        if self.layout is not None and (self.layout[0], self.layout[3], self.layout[4]) == (rows, device, stream):
            captured_capacity = max(capacity, self.layout[1])
        layout = rows, captured_capacity, width, device, stream
        if layout != self.layout:
            raise _LayoutNeeded(*layout)
        padding = captured_capacity - capacity
        if padding:
            available = ExactInt(
                torch.nn.functional.pad(available.words.reshape(rows, capacity, -1),
                    (0, 0, 0, padding)).flatten(0, 1),
                torch.nn.functional.pad(available.sign.reshape(rows, capacity), (0, padding)).flatten(),
                torch.nn.functional.pad(available.fault.reshape(rows, capacity), (0, padding)).flatten())
        taken, remaining = self.captured.replay(available, remaining, limit, safe_suffix)
        if padding:
            indices = (torch.arange(rows, device=device)[:, None] * captured_capacity +
                       torch.arange(capacity, device=device)[None, :]).flatten()
            taken = _take(taken, indices)
        return taken, remaining
