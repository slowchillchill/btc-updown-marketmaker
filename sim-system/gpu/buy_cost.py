"""原SIM LIMIT最大即时成本/市价单价估费的设备实现。无盘口或账户归约。"""
from dataclasses import dataclass
import importlib.util
from pathlib import Path
import sys
import torch


def _public_numbers():
    name = 'public_gpu_numbers'
    if name not in sys.modules:
        path = Path(__file__).resolve().parents[2] / 'contracts/gpu/numbers.py'
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


ExactInt = _public_numbers().ExactInt
ExactRational = _public_numbers().ExactRational


@dataclass(frozen=True)
class ExactBuyCost:
    principal_micro: ExactInt
    fee_1e5_units: ExactInt

    def total_micro(self):
        return self.principal_micro + self.fee_1e5_units * self.fee_1e5_units.constant(10)


def _with_fault(value, fault):
    return ExactInt(value.words, value.sign, value.fault | fault)


def _fee(target_centishares, price, rate, exponent):
    """n*r/100*[p(1-p)]^e，H5逐订单half-up；严格半单位条件。"""
    if not 1 <= exponent <= 0xffffffff:
        raise ValueError('封存费表exponent须沿原V3的u32且>=1约束')
    n = target_centishares
    one = ExactRational.ratio(n.constant(1), n.constant(1))
    invalid = n.fault | price.fault | rate.fault
    invalid = invalid | (n.sign < 0).to(torch.int32) | (rate.numerator.sign < 0).to(torch.int32)
    invalid = invalid | (price.numerator.sign <= 0).to(torch.int32)
    upper = price.compare(one)
    invalid = invalid | upper.fault | (upper.order >= 0).to(torch.int32)
    # 仅由当前分配容量推出上界。不存在固定整数位宽或数量域缩小。
    # n*r_num*2000 < 2^(32*(n_words+r_words)+11)，r_den>=1。
    bound_bits = 32 * (n.words.shape[1] + rate.numerator.words.shape[1]) + 11
    if 2 * exponent >= bound_bits:
        return _with_fault(n.constant(0), invalid)
    curve = price * (one - price)
    power = one
    remaining = exponent
    while remaining:
        if remaining & 1:
            power = power * curve
        remaining >>= 1
        if remaining:
            curve = curve * curve
    total = ExactRational.ratio(n * n.constant(1000), n.constant(1)) * rate * power
    q, r = total.numerator.divrem(total.denominator)
    half = (r * r.constant(2)).compare(total.denominator)
    rounded = (q + q.constant(1)).select(half.order >= 0, q)
    return _with_fault(rounded, invalid | total.fault | half.fault)


def market_fee_estimate(target_centishares, price, rate, exponent):
    """只是该候选价格的H5估费；不代表最大费、预留或真实成交。"""
    return _fee(target_centishares, price, rate, exponent)


def limit_buy_cost(target_centishares, price, rate, exponent, *,
                   post_only, minimum_principal_micro):
    """与SealedProcessRuleSnapshotV3::limit_buy_cost_v1相同分支和单位。"""
    n = target_centishares
    price_micro = ExactRational.ratio(price.numerator * n.constant(1_000_000),
                                      price.denominator).floor()
    principal, _ = (n * price_micro).divrem(n.constant(100))
    half = ExactRational.ratio(n.constant(1), n.constant(2))
    order = price.compare(half)
    fee_price = ExactRational(
        half.numerator.select(order.order > 0, price.numerator),
        half.denominator.select(order.order > 0, price.denominator))
    fee = _fee(n, fee_price, rate, exponent)
    below_minimum = principal.compare(minimum_principal_micro)
    skip = post_only | (below_minimum.order < 0)
    zero = n.constant(0)
    fee = zero.select(skip, fee)
    # 先沿原domain验证，再执行业务跳过；不得用post_only抹掉不合法输入。
    invalid = order.fault | below_minimum.fault | price.fault | rate.fault | n.fault
    original_price_upper = price.compare(ExactRational.ratio(n.constant(1), n.constant(1)))
    invalid = invalid | original_price_upper.fault
    invalid = invalid | (original_price_upper.order >= 0).to(torch.int32)
    invalid = invalid | (price.numerator.sign <= 0).to(torch.int32) | (n.sign < 0).to(torch.int32)
    invalid = invalid | (rate.numerator.sign < 0).to(torch.int32)
    return ExactBuyCost(_with_fault(principal, invalid), _with_fault(fee, invalid))
