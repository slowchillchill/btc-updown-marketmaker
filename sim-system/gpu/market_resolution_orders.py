"""原判定按平台订单字符串序关闭本市场Base订单的共同候选。

本入口只处理订单/队列/占款；合格判定身份、Resolution义务和Resolved反馈
仍由SIM市场协调器同次提交，不能单独用本结果推进hidden引用。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers
from account_state import grouped_fault
from base_execution import resolve_base_order
from base_acceptance import _coordinate_queue
from pending_base_execution import _coordinate_orders


@dataclass(frozen=True)
class MarketResolvedOrder:
    order: object  # 原完整order序号、请求/资产及计量前驱，slot不代业务身份。
    effect: object


@dataclass(frozen=True)
class MarketResolutionOrders:
    events: tuple  # 原sim-N字符串序；padding不产生关闭或经济人口。
    fault: torch.Tensor


def resolve_market_base_orders(orders, queue, account, condition, row_roots, matching_closed, enabled):
    """所有原订单作为同一候选；任一后继失败撤回本次前缀，保更早已提交事实。"""
    from device_identity import sim_trade_order_key
    roots, width = orders.roots, orders.capacity
    if (condition.dtype != torch.int64 or condition.shape != (roots,) or
        condition.device != orders.occupied.device or enabled.dtype != torch.bool or
        enabled.shape != (roots,) or enabled.device != orders.occupied.device):
        raise TypeError('判定市场与根掩码须为同设备完整目录')
    original_orders, original_queue, original_account = orders, queue, account
    keys = sim_trade_order_key(orders.sequences)
    fault = orders.fault | account.fault
    fault |= Numbers.combine_fault_rows(torch.where(orders.occupied, keys.fault.reshape(roots,width), 0))
    fault |= (enabled & (condition < 0)).to(torch.int32)
    # 原平台ID均为sim-{完整u64}；相同前缀后比较原十进制字节，不按数值或slot排序。
    key = keys.bytes.reshape(roots, width, -1)
    key = torch.where(orders.occupied[:,:,None], key, 255)
    permutation = torch.arange(width, device=condition.device)[None,:].expand(roots,width)
    for digit in range(key.size(2)-1, -1, -1):
        selected = key[:,:,digit].gather(1, permutation)
        permutation = permutation.gather(1, torch.argsort(selected, dim=1, stable=True))
    events = []
    for ordinal in range(width):
        read = orders.read(permutation[:,ordinal])
        requested = enabled & read.present & (read.conditions == condition) & (fault == 0)
        ids = read.slots[row_roots.clamp(0,roots-1)]
        queue, quantity, account, effect = resolve_base_order(queue, read.quantity, account,
            ids, row_roots, matching_closed, requested)
        # 关闭mask独立于delta.emitted，pending-only的合法零释放也更新真实计量。
        orders = orders.update(read, quantity, effect.closed)
        fault |= orders.fault | account.fault | effect.fault
        events.append(MarketResolvedOrder(read, effect))
    fault |= grouped_fault(queue.fault, row_roots.clamp(0,roots-1), roots)
    committed = fault == 0
    orders = _coordinate_orders(orders, original_orders, fault)
    queue = _coordinate_queue(queue, original_queue, row_roots.clamp(0,roots-1), fault)
    account = account.coordinate_result(original_account, fault)
    def coordinate(event):
        old = event.effect
        zero = old.canceled.constant(0)
        return replace(event, effect=replace(old,
            pending_before=old.pending_before.select(committed,zero),
            open_before=old.open_before.select(committed,zero),
            released_open_quote=old.released_open_quote.select(committed,zero),
            canceled=old.canceled.select(committed,zero),closed=old.closed & committed,
            delta=account.coordinate_delta(old.delta,committed,fault),fault=fault))
    return orders, queue, account, MarketResolutionOrders(tuple(map(coordinate,events)),fault)
