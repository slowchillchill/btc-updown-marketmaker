"""原maximum_base_for_limit_order_segment_v1的精确SIM设备候选。

舍入差不单调。设备复用公共floor-sum，求原模糊区间人口和末个可行量。
输入值、故障与容量请求分别保留；本算子不提交队列、账户或随机数。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt, Numbers


@dataclass(frozen=True)
class LimitSegmentResult:
    base: object
    quote: object
    present: object
    fault: object


def maximum_limit_segment(external_base, external_quote, consumed, maximum,
                          order_base, order_quote, matched, used):
    """全部ExactInt同设备[batch]。fault不能解释为业务None。"""
    inputs = (external_base, external_quote, consumed, maximum,
              order_base, order_quote, matched, used)
    width = max(Numbers.device_word_capacity(), *(value.words.shape[1] for value in inputs))
    packed = tuple(value.with_capacity(width) for value in inputs)
    words = torch.stack([value.words for value in packed], dim=1).contiguous()
    signs = torch.stack([value.sign for value in packed], dim=1).contiguous()
    base, quote, present, fault, required = Numbers.public_extension().limit_segment(words, signs)
    Numbers.record_resource_request(required)
    for value in inputs:
        fault = fault | value.fault
    present = present & (fault == 0)
    sign = present.to(torch.int8)
    return LimitSegmentResult(ExactInt(base, sign, fault), ExactInt(quote, sign, fault),
                              present, fault)
