"""原Native精确值与T5的设备投影；不推进公共业务状态。"""
import torch
from prepare import _numbers
from public_state import _take


def decimal_value(state, ids):
    numbers = _numbers()
    return numbers.ExactRational(_take(state.decimals.numerator, ids.reshape(-1)),
                                 _take(state.decimals.denominator, ids.reshape(-1)))


def price_value(value, *, pm):
    if pm:
        phi = value.numerator.constant(2)*value.numerator-value.denominator
        return phi.project(value.denominator)
    return value.project(operation=1)


def t5(state, anchor_ids, present, now):
    """唯一直接锚点：source/received/传输年龄及各自存在位，原精确毫秒差。"""
    numbers = _numbers()
    times = state.anchor_times[anchor_ids+1]
    source_present = present & times[...,0].bool()
    features, fault = [], torch.zeros(anchor_ids.numel(),dtype=torch.int32,device=anchor_ids.device)
    for left,right,known in ((now,times[...,1],source_present),
                            (now,times[...,2],present),(times[...,2],times[...,1],source_present)):
        delta = numbers.ExactInt.from_i64_tensor(left)-numbers.ExactInt.from_i64_tensor(right)
        value,error = delta.project(delta.constant(1000),operation=1)
        features.append(torch.where(known,value.reshape(anchor_ids.shape),0.0))
        fault = fault | error
    return torch.stack((*features,source_present.float(),present.float()),dim=-1),fault.reshape(anchor_ids.shape)
