"""公共精确CUDA整数和有理投影的模型消费。CPU仅在材料/报告批次装箱。"""
from dataclasses import dataclass
from pathlib import Path
from functools import lru_cache
import importlib.util
from contextvars import ContextVar
import torch


_batch = ContextVar('public_exact_resource_batch', default=None)


def combine_fault_rows(fault):
    """合并[batch,rows]的全部int32错误位，不以最大整数丢失其它位。"""
    bits = torch.arange(32, dtype=torch.int64, device=fault.device)
    flags = ((fault.to(torch.int64)[:, :, None] >> bits) & 1).bool().any(dim=1)
    return (flags.to(torch.int64) << bits).sum(dim=1).to(torch.int32)


def _bucket(required):
    return 1 << (max(4, required) - 1).bit_length()


def device_word_capacity():
    """公共候选算子共享本批物理容量；没有协调批时返回最小初始容量。"""
    state = _batch.get()
    return state['capacity'] if state else 4


def record_resource_request(required):
    """独立登记设备容量请求，不能让业务select丢失扩池请求。"""
    state = _batch.get()
    if state is not None and required.numel():
        state['requests'].append(required)


def run_exact_batch(compute, *, initial_word_capacity=4):
    """协调资源事务：compute只生成待提交结果，不能推进外部账户或随机数。

    全批只读取一次资源请求。容量不足时丢弃本批临时结果、扩池并原输入重算。
    本入口不读取业务数值、不把资源失败转换成合法域拒绝。真实环境协调器须在
    此入口成功后才提交状态；含随机采样时由协调器保存/恢复其原RNG快照。
    """
    capacity = _bucket(initial_word_capacity)
    while True:
        state = {'capacity': capacity, 'requests': []}
        token = _batch.set(state)
        try:
            result = compute()
        finally:
            _batch.reset(token)
        requests = state['requests']
        if not requests:
            return result
        # 这里只读取资源容量，不读取价格、余额、分支或选择路径。
        required = int(torch.stack([x.max() for x in requests]).max().cpu())
        if required <= capacity:
            return result
        capacity = _bucket(required)

@lru_cache(maxsize=1)
def public_extension():
    path=Path(__file__).resolve().parents[2]/"shared-run/gpu/extension.py"
    spec=importlib.util.spec_from_file_location("public_gpu_extension",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.load_extension()

@dataclass(frozen=True)
class ExactComparison:
    order: torch.Tensor
    fault: torch.Tensor

    def valid_mask(self):
        """业务条件必须同时检查此mask，不能把故障数解释为正常关系。"""
        return self.fault == 0

@dataclass(frozen=True)
class ExactInt:
    words: torch.Tensor
    sign: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def from_i64_tensor(cls, values):
        """设备坐标转换；INT64_MIN的绝对值位模式也精确保留为2^63。"""
        if values.dtype != torch.int64 or not values.is_cuda:
            raise TypeError('需设备int64坐标')
        values = values.reshape(-1)
        magnitude = torch.where(values < 0, -values, values)
        width = max(4, _batch.get()['capacity'] if _batch.get() else 4)
        words = torch.zeros((values.numel(), width), dtype=torch.int32, device=values.device)
        words[:, 0] = (magnitude & 0xffffffff).to(torch.int32)
        words[:, 1] = ((magnitude >> 32) & 0xffffffff).to(torch.int32)
        return cls(words, ((values > 0).to(torch.int8) - (values < 0).to(torch.int8)),
                   torch.zeros_like(values, dtype=torch.int32))

    @classmethod
    def from_values(cls, values, *, device="cuda"):
        """显式批次输入边界；不能用于训练逐条件往返。"""
        values=list(values);width=max(1,max((abs(v).bit_length()+31)//32 for v in values)) if values else 1
        state = _batch.get()
        width = _bucket(max(width, state['capacity'] if state else 4))
        words=[]
        for value in values:
            row=[]
            for i in range(width):
                w=(abs(value)>>(32*i))&0xffffffff;row.append(w if w<0x80000000 else w-0x100000000)
            words.append(row)
        return cls(torch.tensor(words,dtype=torch.int32,device=device).reshape(len(values),width),
                   torch.tensor([0 if v==0 else (-1 if v<0 else 1) for v in values],dtype=torch.int8,device=device),
                   torch.zeros(len(values),dtype=torch.int32,device=device))

    def _operate(self, other, op):
        state = _batch.get()
        left = self.with_capacity(state['capacity']) if state else self
        right = other.with_capacity(state['capacity']) if state else other
        w,s,r,rs,f,required=public_extension().integer_op(op,left.words,left.sign,right.words,right.sign)
        if state is not None and required.numel():
            state['requests'].append(required)
        f=f|self.fault|other.fault
        return ExactInt(w,s,f),ExactInt(r,rs,f)

    def with_capacity(self, capacity):
        if self.words.shape[1] >= capacity:
            return self
        words = torch.nn.functional.pad(self.words, (0, capacity - self.words.shape[1]))
        return ExactInt(words, self.sign, self.fault)

    def __add__(self, other):return self._operate(other,0)[0]
    def __sub__(self, other):return self._operate(other,1)[0]
    def __mul__(self, other):return self._operate(other,2)[0]
    def divrem(self, other):return self._operate(other,3)
    def compare(self, other):
        result = self._operate(other, 4)[0]
        return ExactComparison(result.sign, result.fault)

    def gcd(self, other):
        words = public_extension().gcd(self.words, other.words)
        sign = words.any(dim=1).to(torch.int8)
        return ExactInt(words, sign, self.fault | other.fault)

    def floor_sum(self, modulus, slope, offset):
        """Σ floor((slope*i+offset)/modulus), 0<=i<self；非负，零模数为fault。

        原Euclid循环在设备运行。输出字容量与其它精确运算同一纯候选协调重试。
        """
        state = _batch.get()
        original = (self, modulus, slope, offset)
        inputs = tuple(value.with_capacity(state['capacity']) if state else value for value in original)
        words, fault, required = public_extension().floor_sum(*(value.words for value in inputs))
        if state is not None and required.numel():
            state['requests'].append(required)
        for value in original:
            fault = fault | value.fault | (value.sign < 0).to(torch.int32)
        return ExactInt(words, words.any(dim=1).to(torch.int8), fault)

    def abs(self):
        return ExactInt(self.words, self.sign.abs(), self.fault)

    def negated(self):
        return ExactInt(self.words, -self.sign, self.fault)

    def select(self, mask, other):
        """同一批次的设备条件选择；mask为真取self，无标量回传。"""
        width = max(self.words.shape[1], other.words.shape[1])
        pad = torch.nn.functional.pad
        a = pad(self.words, (0, width - self.words.shape[1]))
        b = pad(other.words, (0, width - other.words.shape[1]))
        return ExactInt(torch.where(mask[:, None], a, b),
                        torch.where(mask, self.sign, other.sign),
                        torch.where(mask, self.fault, other.fault))

    def constant(self, value):
        """标量配置常量在设备广播，不读取活动数值。"""
        state = _batch.get()
        width = _bucket(max(self.words.shape[1], (abs(value).bit_length() + 31) // 32,
                            state['capacity'] if state else 4))
        words = torch.empty((self.words.shape[0], width), dtype=torch.int32,
                            device=self.words.device)
        for i in range(width):
            w = (abs(value) >> (32 * i)) & 0xffffffff
            words[:, i] = w if w < 0x80000000 else w - 0x100000000
        sign = torch.full_like(self.sign, (value > 0) - (value < 0))
        return ExactInt(words, sign, torch.zeros_like(self.fault))

    def project(self, denominator, *, operation=0):
        output,fault=public_extension().project(operation,self.words,self.sign*denominator.sign,denominator.words)
        return output,fault|self.fault|denominator.fault

    def report_values(self):
        """阶段报告/独立验证边界，生产条件循环不调用。"""
        words=self.words.cpu().tolist();signs=self.sign.cpu().tolist()
        return [s*sum((w&0xffffffff)<<(32*i) for i,w in enumerate(row)) for row,s in zip(words,signs)]


@dataclass(frozen=True)
class ExactRational:
    """与EconomicRational一致：正分母、GCD约分、零为0/1，错误保留。"""
    numerator: ExactInt
    denominator: ExactInt

    @classmethod
    def ratio(cls, numerator, denominator):
        invalid = numerator.fault | denominator.fault | (denominator.sign == 0).to(torch.int32)
        common = numerator.gcd(denominator)
        n, _ = numerator.divrem(common)
        d, _ = denominator.divrem(common)
        invalid = invalid | n.fault | d.fault
        n = ExactInt(n.words, n.sign * d.sign, invalid)
        return cls(n, ExactInt(d.words, d.sign.abs(), invalid))

    @classmethod
    def from_values(cls, numerators, denominators, *, device='cuda'):
        """材料输入/验证边界，正式活动运算不回传Python整数。"""
        return cls.ratio(ExactInt.from_values(numerators, device=device),
                         ExactInt.from_values(denominators, device=device))

    @property
    def fault(self):
        return self.numerator.fault | self.denominator.fault

    def __add__(self, other):
        n = self.numerator * other.denominator + other.numerator * self.denominator
        return ExactRational.ratio(n, self.denominator * other.denominator)

    def __sub__(self, other):
        n = self.numerator * other.denominator - other.numerator * self.denominator
        return ExactRational.ratio(n, self.denominator * other.denominator)

    def __mul__(self, other):
        return ExactRational.ratio(self.numerator * other.numerator,
                                   self.denominator * other.denominator)

    def divided_by(self, other):
        return ExactRational.ratio(self.numerator * other.denominator,
                                   self.denominator * other.numerator)

    def compare(self, other):
        return (self.numerator * other.denominator).compare(
            other.numerator * self.denominator)

    def floor(self):
        q, r = self.numerator.divrem(self.denominator)
        minus_one = q - q.constant(1)
        return minus_one.select((self.numerator.sign < 0) & (r.sign != 0), q)

    def project(self, *, operation=0):
        return self.numerator.project(self.denominator, operation=operation)
