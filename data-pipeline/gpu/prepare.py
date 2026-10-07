"""共享紧凑输入的批量设备转换/稳定排序/独立归约；不生成模型特征或账户。"""
from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
import torch


def _numbers():
    name = 'public_gpu_numbers'
    if name not in sys.modules:
        path = Path(__file__).resolve().parents[2] / 'contracts/gpu/numbers.py'
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@dataclass(frozen=True)
class DeviceDecimalColumns:
    coefficient: object
    denominator: object
    scale: torch.Tensor
    present: torch.Tensor

    def rational(self):
        return _numbers().ExactRational.ratio(self.coefficient, self.denominator)


def decimal_columns(raw_bytes, offsets, present):
    """消费规范ExactDecimal字典；raw source词法由块侧车完整保留。"""
    c, d, signs, scale, fault = _numbers().public_extension().decimal(raw_bytes, offsets, present)
    numerator = _numbers().ExactInt(c, signs, fault)
    denominator = _numbers().ExactInt(d, torch.ones_like(signs), fault)
    return DeviceDecimalColumns(numerator, denominator, scale, present)


def stable_source_order(key_bytes, key_lengths):
    """只排序原模块生成的原排序键；不按浮点时间或自行推导因果顺序。

    调用者按来源接入正确public/hidden键并沿permutation同步搬运所有列。
    前缀短键排在其长键之前；完全相同键稳定保留输入顺序。
    """
    if key_bytes.dtype != torch.uint8 or key_lengths.dtype != torch.int64:
        raise TypeError('需uint8[N,K]原排序键与int64[N]长度')
    if not key_bytes.is_cuda or key_bytes.dim() != 2 or key_lengths.shape != (key_bytes.shape[0],):
        raise ValueError('排序键必须是同批设备列')
    if key_lengths.device != key_bytes.device:
        raise ValueError('排序长度和键设备不同')
    order = torch.arange(key_bytes.shape[0], device=key_bytes.device)
    for column in range(key_bytes.shape[1] - 1, -1, -1):
        values = torch.where(key_lengths > column, key_bytes[:, column].to(torch.int16), -1)
        order = order[torch.argsort(values[order], stable=True)]
    fault = ((key_lengths < 0) | (key_lengths > key_bytes.shape[1])).to(torch.int32)
    return order, fault


def independent_tag_counts(tags, kinds):
    """独立类别归约供一次材料准备统计，不产生窗口或公共业务状态。"""
    return torch.bincount(tags, minlength=kinds)


def decimal_price_order(source, price_ids):
    """冷准备精确源价格排序；SourceDecimal的96位、scale<=28原域不转float。"""
    def keys():
        factor = source.coefficient.constant(1)
        for exponent in range(1, 29):
            factor = factor.constant(10**exponent).select(28-source.scale == exponent, factor)
        return source.coefficient * factor
    key = _numbers().run_exact_batch(keys)
    digits = key.words[price_ids].to(torch.int64) & 0xffffffff
    signs = key.sign[price_ids]
    order = torch.arange(price_ids.numel(), device=price_ids.device)
    for word in range(digits.shape[1]):
        digit = torch.where(signs < 0, 0xffffffff-digits[:, word], digits[:, word])
        order = order[torch.argsort(digit[order], stable=True)]
    order = order[torch.argsort(signs[order], stable=True)]
    return order, key.fault[price_ids]
