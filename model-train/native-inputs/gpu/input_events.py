"""原Native事件104字段：23种事实计数及9组直接来源E9，窗口为合法前缀差。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from public_state import _take

# 与market.rs::event_class一致。DPT保留公共枚举名，模型分类只在Native拥有。
EVENT_CLASSES = {
    'pm.full_book':(0,0), 'pm.absolute_level_changes':(1,0), 'pm.best_bid_ask':(2,0),
    'pm.tick_size_change':(3,0), 'pm.public_trade':(4,0), 'pm.new_market':(5,0),
    'market.fixed_identity':(6,1), 'market.initial_tick':(7,1),
    'market.lifecycle_transition':(8,1), 'market.lifecycle_snapshot':(8,1),
    'market.trading_state':(9,1), 'market.terminal':(10,1), 'market.quality_boundary':(11,1),
    'reference.chainlink_spot':(12,2), 'reference.chainlink_twap30':(13,3),
    'reference.chainlink_twap60':(14,4), 'reference.market_price_anchor':(15,5),
    'reference.market_settlement':(16,5), 'reference.binance_depth':(17,6),
    'reference.binance_trade':(18,7), 'reference.anomaly_type':(19,8),
    'reference.anomaly_topic':(20,8), 'reference.anomaly_symbol':(21,8),
    'reference.anomaly_timestamp':(22,8),
}


@dataclass(frozen=True)
class NativeEvents:
    values: torch.Tensor  # [roots,104] = 原fast[335:439]
    fault: torch.Tensor
    required: torch.Tensor
    enough: torch.Tensor


def _extreme(value, present, *, maximum):
    """固定设备归约树；先精确比较，再保留原多字整数，不先投影极值。"""
    numbers = _numbers()
    roots, width = present.shape
    indices = torch.arange(roots*width,device=present.device).reshape(roots,width)
    fault = torch.zeros(roots,dtype=torch.int32,device=present.device)
    while width > 1:
        left = indices[:,0::2]
        right = indices[:,1::2]
        left_present = present[:,0::2]
        right_present = present[:,1::2]
        if left.shape[1] != right.shape[1]:
            right = torch.cat((right,left[:,-1:]),dim=1)
            right_present = torch.cat((right_present,torch.zeros_like(left_present[:,-1:])),dim=1)
        relation = _take(value,left.reshape(-1)).compare(_take(value,right.reshape(-1)))
        fault = fault | numbers.combine_fault_rows(relation.fault.reshape(roots,-1))
        choose_right = right_present & (~left_present | ((relation.order.reshape(left.shape) < 0)
                                                       if maximum else (relation.order.reshape(left.shape) > 0)))
        indices = torch.where(choose_right,right,left)
        present = left_present | right_present
        width = indices.shape[1]
    return _take(value,indices.reshape(-1)),present.reshape(-1),fault


def time_envelope(source_ms, received_ms, source_present, received_present, observation_ms):
    """原TimeStats E9。差值可到2^64-1，所有比较/除1000/投影保持精确。"""
    numbers = _numbers()
    roots, width = source_ms.shape
    known = (source_present,received_present,source_present & received_present)
    now = observation_ms[:,None].expand(-1,width)
    count = received_present.sum(dim=1)
    # 原E9的总样本数由调用方的存在行决定；来源总是有received，source可缺。
    output = torch.zeros((roots,9),device=source_ms.device)
    fault = torch.zeros(roots,dtype=torch.int32,device=source_ms.device)
    for index,(left,right,mask) in enumerate(((now,source_ms,known[0]),(now,received_ms,known[1]),
                                            (received_ms,source_ms,known[2]))):
        delta = numbers.ExactInt.from_i64_tensor(left)-numbers.ExactInt.from_i64_tensor(right)
        for bound,maximum in enumerate((False,True)):
            value,present,error = _extreme(delta,mask,maximum=maximum)
            projected,pf = value.project(value.constant(1000),operation=1)
            output[:,2*index+bound] = torch.where(present,projected,0.0)
            fault = fault | error | pf
        numerator = numbers.ExactInt.from_i64_tensor(mask.sum(dim=1))
        denominator = numbers.ExactInt.from_i64_tensor(count.clamp_min(1))
        fraction,pf = numerator.project(denominator)
        output[:,6+index] = torch.where(count>0,fraction,0.0)
        fault = fault | pf
    return output,fault


class EventInputs:
    def __init__(self, public_window):
        self.window = public_window
        try:
            mapping = [EVENT_CLASSES[kind] for kind in public_window.types]
        except KeyError as error:
            raise ValueError('Native不支持公共事实类型') from error
        # 末尾sentinel用于无任何事实的合法空材料。
        self.mapping = torch.tensor(mapping+[(0,0)],dtype=torch.int64,device=public_window.facts.device)

    def encode(self, previous_prefix, current_prefix, observation_ms):
        if observation_ms.shape != current_prefix.shape or observation_ms.dtype != torch.int64 or observation_ms.device != current_prefix.device:
            raise ValueError('需同设备int64观察UTC时间')
        numbers = _numbers()
        window = self.window.query(previous_prefix,current_prefix)
        fact_kind = self.mapping[window.facts[:,:,1],0]
        source_group = self.mapping[window.sources[:,:,1],1]
        count = ((fact_kind[:,:,None]==torch.arange(23,device=fact_kind.device))
                 & window.fact_present[:,:,None]).sum(dim=1)
        def compute():
            value = numbers.ExactInt.from_i64_tensor(count)
            values,error = value.project(value.constant(1),operation=1)
            result = torch.zeros((current_prefix.numel(),104),device=current_prefix.device)
            result[:,:23] = values.reshape(-1,23)
            fault = window.fault | numbers.combine_fault_rows(error.reshape(-1,23))
            # 每组并行处理所有根；不把来源分支读回CPU。
            for group in range(9):
                present = window.source_present & (source_group==group)
                envelope,ef = time_envelope(window.sources[:,:,3],window.sources[:,:,4],
                                            present & window.sources[:,:,2].bool(),present,observation_ms)
                result[:,23+9*group:32+9*group] = envelope
                fault = fault | ef
            return NativeEvents(result,fault,window.required,window.enough)
        return numbers.run_exact_batch(compute)
