"""同公开cutoff的原Native current/old选择，ContextOnly不替换账户current。"""
from dataclasses import dataclass
import torch
from prepare import _numbers


@dataclass(frozen=True)
class AccountScopes:
    conditions: torch.Tensor  # [roots,2,markets]，current单项，old为真实Ledger人口。
    present: torch.Tensor
    applicable: torch.Tensor
    population_known: torch.Tensor
    current_market: torch.Tensor
    fault: torch.Tensor


def account_scopes(market, condition_slots, prefixes, projected, cutoff, root_started_at):
    """字典排序在共享冷绑定中按原condition字符串生成；热选择只读设备前缀。

    admitted来自当前完整公开组，projected来自原Ledger三图实际成功写入并集。
    不把冷完整市场目录或所有成交身份当作已公开/已投影人口。
    """
    frame = market.frame(prefixes)
    admitted = frame.admitted
    times, condition_order = market.times, market.condition_order
    roots, markets = admitted.shape
    device = admitted.device
    for value in (condition_slots, condition_order):
        if value.shape != (markets,) or value.dtype != torch.int64 or value.device != device:
            raise TypeError('市场身份和原condition字典序须同设备int64[markets]')
    if (times.shape != (markets,2) or times.dtype != torch.int64 or times.device != device
            or admitted.dtype != torch.bool):
        raise TypeError('原公开固定窗口和可见人口布局不符')
    for value in (cutoff, root_started_at):
        if value.shape != (roots,) or value.dtype != torch.int64 or value.device != device:
            raise TypeError('原cutoff/根起点须同设备int64[roots]')
    if projected.present.size(0) != roots or projected.present.device != device:
        raise ValueError('原Ledger市场人口根布局不符')
    numbers = _numbers()
    expanded_start = times[:,0][None,:].expand(roots,markets).flatten()
    began = root_started_at[:,None].expand(roots,markets).flatten()
    threshold = numbers.ExactInt.from_i64_tensor(expanded_start)
    threshold = threshold - threshold.constant(30_000)
    relation = numbers.ExactInt.from_i64_tensor(began).compare(threshold)
    eligible = (relation.order <= 0).reshape(roots,markets) & admitted
    active = eligible & (times[None,:,0] <= cutoff[:,None]) & (cutoff[:,None] < times[None,:,1])
    fault = numbers.combine_fault_rows(relation.fault.reshape(roots,markets)) | projected.fault | frame.prefix_fault
    fault |= numbers.combine_fault_rows(frame.lifecycle.fault)
    # 物理顺序列必须是完整permutation。损坏布局不变成某个经营选择。
    safe_order = condition_order.clamp(0,max(0,markets-1))
    if markets:
        expected = torch.arange(markets,device=device)
        invalid_order = ((condition_order < 0) | (condition_order >= markets)).any() | (safe_order.sort().values != expected).any()
        fault |= invalid_order.to(torch.int32)
        prioritized = active[:,safe_order]
        at = prioritized.to(torch.int32).argmax(1)
        current = safe_order[at]
        has_current = prioritized.any(1)
        current_member = (expected[None,:] == current[:,None]) & has_current[:,None]
    else:
        current = torch.full((roots,),-1,dtype=torch.int64,device=device)
        has_current = torch.zeros(roots,dtype=torch.bool,device=device)
        current_member = admitted
    actual_population = ((condition_slots[None,:,None] == projected.conditions[None,None,:])
                         & projected.present[:,None,:]).any(2)
    old_member = eligible & actual_population & (times[None,:,1] <= cutoff[:,None])
    present = torch.stack((current_member,old_member),1)
    conditions = condition_slots[None,None,:].expand(roots,2,markets)
    applicable = torch.stack((has_current,torch.ones_like(has_current)),1)
    return AccountScopes(conditions,present,applicable,projected.known.clone(),
                         torch.where(has_current,current,-1),fault)
