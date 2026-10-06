"""原SIM逐订单即时taker费：先精确合计再H5，余量按原fill顺序分配。"""
from dataclasses import dataclass
import torch
from queues import Numbers, ExactInt, ExactRational, _take, _columns
from asset_buy import take_ratio


@dataclass(frozen=True)
class TakerFeeAllocation:
    total_micro: object  # ExactInt[roots]
    per_fill_micro: object  # ExactInt[roots*maximum_fills]，原planner顺序。
    fault: torch.Tensor


def allocate_taker_fee(base, price, rate, exponent, roots, maximum_fills):
    """base/price为原plan的根×fill连续布局，尾部base=0；rate为每根封存费率。

    fill序对应原候选next_fill_sequence递增序。按精确余数降序、同余数原序分配，
    不把每fill独立H5或浮点排序冒充逐订单合计。没有提交fill序号/账户。
    """
    if not 1 <= exponent <= 0xffffffff:
        raise ValueError('封存费表exponent须>=1且属原u32')
    if base.sign.shape != (roots*maximum_fills,) or rate.numerator.sign.shape != (roots,):
        raise ValueError('原成交量/费率布局不一致')
    if price.numerator.sign.shape != base.sign.shape or roots < 1 or maximum_fills < 0:
        raise ValueError('原fill价格或物理人口不一致')
    zero = rate.numerator.constant(0)
    fault = rate.fault | (rate.numerator.sign < 0).to(torch.int32)
    if maximum_fills == 0:
        return TakerFeeAllocation(zero,base.constant(0),fault)
    total_base = zero
    for column in range(maximum_fills):
        indices = torch.arange(roots,device=base.words.device)*maximum_fills+column
        value = _take(base,indices)
        total_base = total_base+value
        fault = fault | value.fault | (value.sign < 0).to(torch.int32)
    fault = fault | total_base.fault
    rows = roots*maximum_fills
    root_indices = torch.arange(rows,device=base.words.device)//maximum_fills
    one = ExactRational.ratio(base.constant(1),base.constant(1))
    filled = base.sign > 0
    upper = price.compare(one)
    input_fault = base.fault | price.fault | upper.fault
    input_fault = input_fault | ((price.numerator.sign <= 0) | (upper.order >= 0)).to(torch.int32)*filled.to(torch.int32)
    fault = fault | Numbers.combine_fault_rows(input_fault.reshape(roots,maximum_fills))
    # 与原严格early-zero等价的物理容量充分条件；不限制量域或放宽半单位边界。
    bits = 32*(total_base.words.shape[1]+rate.numerator.words.shape[1])
    if 2*exponent >= bits:
        flat_zero = base.constant(0)
        return TakerFeeAllocation(ExactInt(zero.words,zero.sign,fault),
            ExactInt(flat_zero.words,flat_zero.sign,fault[root_indices]),fault)
    curve = price*(one-price)
    power = one
    remaining = exponent
    while remaining:
        if remaining & 1:
            power = power*curve
        remaining >>= 1
        if remaining:
            curve = curve*curve
    contribution = ExactRational.ratio(base,base.constant(10))*take_ratio(rate,root_indices)*power
    floors,remainder = contribution.numerator.divrem(contribution.denominator)
    fractions = ExactRational.ratio(remainder,contribution.denominator)
    total = ExactRational.ratio(zero,zero.constant(1))
    floor_total = zero
    for column in range(maximum_fills):
        indices = torch.arange(roots,device=base.words.device)*maximum_fills+column
        total = total+take_ratio(contribution,indices)
        floor_total = floor_total+_take(floors,indices)
    integer,residue = total.numerator.divrem(total.denominator)
    half = (residue*residue.constant(2)).compare(total.denominator)
    rounded = integer+integer.constant(1).select(half.order >= 0,integer.constant(0))
    extras = rounded-floor_total
    fault = fault | total.fault | half.fault | extras.fault
    limit = extras.compare(extras.constant(maximum_fills))
    fault = fault | limit.fault | (extras.sign < 0).to(torch.int32) | (limit.order > 0).to(torch.int32)
    # 精确稳定bitonic排序网络。物理补齐仅尾部；未成交槽不参加余单位分配。
    width = 1 << (maximum_fills-1).bit_length()
    indices = torch.arange(width,device=base.words.device)[None,:].expand(roots,width).clone()
    def lookup(coordinates):
        safe = coordinates.clamp(0,maximum_fills-1)
        flat = (torch.arange(roots,device=base.words.device)[:,None]*maximum_fills+safe).flatten()
        live = (coordinates < maximum_fills) & filled[flat].reshape(roots,width)
        return take_ratio(fractions,flat),live
    size = 2
    while size <= width:
        distance = size//2
        while distance:
            peer = torch.arange(width,device=base.words.device) ^ distance
            right = indices[:,peer]
            left_fraction,left_live = lookup(indices)
            right_fraction,right_live = lookup(right)
            comparison = left_fraction.compare(right_fraction)
            fault = fault | Numbers.combine_fault_rows(comparison.fault.reshape(roots,width))
            left_first = (left_live & ~right_live) | ((left_live == right_live) &
                ((comparison.order.reshape(roots,width) > 0) |
                 ((comparison.order.reshape(roots,width) == 0) & (indices < right))))
            position = torch.arange(width,device=base.words.device)
            want_first = ((position & size) == 0) == ((position & distance) == 0)
            indices = torch.where(left_first == want_first[None,:],indices,right)
            distance //= 2
        size *= 2
    ranks = torch.empty_like(indices)
    ranks.scatter_(1,indices,torch.arange(width,device=base.words.device)[None,:].expand_as(indices))
    rank = ExactInt.from_i64_tensor(ranks[:,:maximum_fills].flatten())
    threshold = _take(extras,root_indices)
    comparison = rank.compare(threshold)
    fault = fault | Numbers.combine_fault_rows(comparison.fault.reshape(roots,maximum_fills))
    extra = floors.constant(1).select((comparison.order < 0) & filled,floors.constant(0))
    allocations = (floors+extra)*floors.constant(10)
    total_micro = rounded*rounded.constant(10)
    fault = fault | total_micro.fault | Numbers.combine_fault_rows(allocations.fault.reshape(roots,maximum_fills))
    total_micro = ExactInt(total_micro.words,total_micro.sign,total_micro.fault | fault)
    allocations = ExactInt(allocations.words,allocations.sign,allocations.fault | fault[root_indices])
    return TakerFeeAllocation(total_micro,allocations,fault)
