"""已准入GTC请求的设备接纳装配：请求、订单目录、队列、填单与账户共同候选。

源SDK terms、隐藏资产准入和sealed selector仍由原所属入口供给；本模块不
把公开末盘口当hidden，不自行推进双门或生成独立的经济/反馈序号。
"""
from dataclasses import dataclass, fields, replace
import torch
from queues import Numbers, ExactInt
from base_execution import prepare_base_order, append_base_remainder
from base_acceptance import accept_base_with_fills, _coordinate_queue, _coordinate_pool
from base_orders import put_quantity
from account_state import grouped_fault


@dataclass(frozen=True)
class FrozenBaseTerms:
    item_refs: torch.Tensor
    accepted_base: object
    accepted_quote: object
    limit_price: object
    assets: torch.Tensor
    bid_rows: torch.Tensor
    fault: torch.Tensor
    conditions: object = None  # 冷目录稳定坐标；来源仍为原实际SDK市场身份。


@dataclass(frozen=True)
class PendingBaseEffects:
    acceptance: object
    order: object
    response: object
    required_segments: torch.Tensor
    fault: torch.Tensor
    calendars: object = None
    required_day_fills: object = None
    required_days: object = None
    prepared: object = None  # 同次纯候选计量/逐段计划，供真实反馈装配，不从原oracle反造。


def _coordinate_orders(candidate, original, fault):
    valid = fault == 0
    flat = valid.repeat_interleave(candidate.capacity)
    index = torch.arange(flat.numel(), device=fault.device)
    updates = {}
    for field in fields(candidate):
        value, old = getattr(candidate, field.name), getattr(original, field.name)
        if field.name == 'quantities':
            updates[field.name] = put_quantity(old, value, index, flat)
        elif isinstance(value, ExactInt):
            updates[field.name] = value.select(valid if field.name == 'next_sequence' else flat, old)
        elif field.name == 'prices':
            updates[field.name] = type(value)(value.numerator.select(flat, old.numerator),
                                             value.denominator.select(flat, old.denominator))
        elif field.name == 'fault':
            updates[field.name] = fault
        else:
            updates[field.name] = torch.where(valid[:, None], value, old)
    return replace(candidate, **updates)


def accept_pending_base(requests, pending, orders, queue, account, fills, terms, ask_rows,
                        fee_rate, fee_exponent, selected_paths, taker_rebate_rate, row_roots, result_refs,
                        *, calendars=None, enabled=None):
    """从当前pending的原item引用规划。result_refs为本次原Post结果材料的预留引用。

    返回候选仍须与结果正文/反馈共同安装。整root某项故障只回退本动作，保留先前
    输入与经济成功前缀。物理资源不足由下层run入口扩容，不变成业务拒绝。
    """
    if pending.source is not requests:
        raise ValueError('接单须使用选择pending时同一请求状态')
    if enabled is not None and (enabled.shape!=pending.exists.shape or enabled.dtype!=torch.bool or enabled.device!=pending.exists.device):
        raise TypeError('实际接单phase需同设备bool[roots]')
    active = pending.exists if enabled is None else pending.exists & enabled
    source_fault = requests.fault | orders.fault | terms.fault*active.to(torch.int32)
    source_fault = source_fault | (active & (terms.item_refs != pending.item_ref)).to(torch.int32)
    prepared = prepare_base_order(queue, account, ask_rows, terms.accepted_base, terms.accepted_quote,
        terms.limit_price, fee_rate, fee_exponent, terms.assets, active & (source_fault == 0))
    # 不存在pending的padding terms不是新的业务错误；实际人口仍保存全部原故障。
    prepared = replace(prepared, fault=source_fault | prepared.fault*active.to(torch.int32),
        quantity=replace(prepared.quantity, fault=prepared.quantity.fault*active.to(torch.int32)))
    original_queue, original_account, original_fills, original_orders = queue, account, fills, orders
    original_calendars = calendars
    queue, account, fills, accepted = accept_base_with_fills(prepared, queue, account, fills,
        pending.effective_at, selected_paths, taker_rebate_rate, row_roots)
    orders, insertion = orders.insert(prepared.quantity, terms.limit_price, pending.request_key,
        pending.ordinal, pending.item_ref, terms.assets, terms.bid_rows, pending.effective_at,
        accepted.submission_delta.emitted,conditions=terms.conditions)
    if terms.conditions is not None:
        from asset_buy import take_ratio
        for column, frame in enumerate(accepted.fills):
            price_index = torch.arange(orders.roots, device=terms.assets.device)*prepared.maximum_fills+column
            fills = fills.bind_origin(frame.slot, insertion.sequence, insertion.slots, terms.conditions,
                torch.zeros_like(terms.assets), take_ratio(prepared.taker_prices, price_index),
                frame.event.emitted & insertion.emitted)
    elif calendars is not None:
        raise ValueError('持续最终性装配必须携带真实市场来源绑定')
    queue, remainder = append_base_remainder(queue, prepared.quantity, terms.bid_rows, insertion.slots,
                                             row_roots, insertion.emitted)
    root_fault = accepted.fault | insertion.fault | remainder.root_fault | fills.fault
    required_day_fills = required_days = None
    if calendars is not None:
        from queues import _take
        required_day_fills = torch.full_like(pending.ordinal, calendars.taker.fill_occupied.size(1))
        required_days = torch.full_like(pending.ordinal, calendars.taker.occupied.size(1))
        taker = calendars.taker
        for frame in accepted.fills:
            index = torch.arange(orders.roots, device=terms.assets.device)*fills.capacity+frame.slot
            taker, demand = taker.register(frame.fill_sequence, fills.records.matched_at[index],
                _take(fills.records.fee, index), frame.event.emitted & insertion.emitted)
            required_day_fills = torch.maximum(required_day_fills, demand.fill_capacity)
            required_days = torch.maximum(required_days, demand.day_capacity)
            root_fault = root_fault | demand.fault
        root_fault = root_fault | ((required_day_fills > taker.fill_occupied.size(1)) |
            (required_days > taker.occupied.size(1))).to(torch.int32)*2
        calendars = replace(calendars, taker=taker)
    # 物理请求在返回值完整保留；资源不足本动作暂不安装账户/成交/请求done。
    root_fault = root_fault | ((insertion.required_orders > orders.capacity) &
                               accepted.submission_delta.emitted).to(torch.int32)*2
    root_fault = root_fault | (accepted.required_records > fills.capacity).to(torch.int32)*2
    segment_fault = (remainder.required_segments > queue.capacity).to(torch.int32)*2
    root_fault = root_fault | grouped_fault(segment_fault, row_roots.clamp(0, orders.roots-1), orders.roots)
    outcome = active & (accepted.submission_delta.emitted | accepted.rejected)
    requests, response = requests.complete(pending, result_refs, outcome, root_fault)
    root_fault = root_fault | response.fault
    requests = replace(requests, fault=requests.fault | root_fault)
    committed = root_fault == 0
    account = account.coordinate_result(original_account, root_fault)
    queue = _coordinate_queue(queue, original_queue, row_roots.clamp(0, orders.roots-1), root_fault)
    fills = _coordinate_pool(fills, original_fills, root_fault)
    orders = _coordinate_orders(orders, original_orders, root_fault)
    if calendars is not None:
        from taker_days import coordinate_calendar
        calendars = replace(calendars,
            maker=coordinate_calendar(calendars.maker,original_calendars.maker,committed,root_fault),
            taker=coordinate_calendar(calendars.taker,original_calendars.taker,committed,root_fault))
    insertion = replace(insertion, emitted=insertion.emitted & committed, fault=root_fault)
    acceptance_events = []
    for frame in accepted.fills:
        event = frame.event
        event = replace(event, emitted=event.emitted & committed, terminal=event.terminal & committed,
            economic_delta=account.coordinate_delta(event.economic_delta, committed, root_fault),
            failure_delta=account.coordinate_delta(event.failure_delta, committed, root_fault),
            rebate_delta=account.coordinate_delta(event.rebate_delta, committed, root_fault), fault=root_fault)
        index = torch.arange(orders.roots, device=terms.assets.device)*fills.capacity+frame.slot
        acceptance_events.append(replace(frame, event=event, origin=fills.origins.take(index)))
    accepted = replace(accepted, submission_delta=account.coordinate_delta(accepted.submission_delta, committed, root_fault),
        rejected=accepted.rejected & committed, fills=tuple(acceptance_events), fault=root_fault)
    response = replace(response, emitted=response.emitted & committed, fault=root_fault)
    return requests, orders, queue, account, fills, PendingBaseEffects(accepted, insertion, response,
        remainder.required_segments, root_fault, calendars, required_day_fills, required_days, prepared)


def run_pending_base_acceptance(requests, orders, queue, account, fills, terms, ask_rows,
                                fee_rate, fee_exponent, selected_paths, taker_rebate_rate, row_roots, result_refs,
                                *, calendars=None, enabled=None):
    """仅在物理边界读取三个容量请求；原input/RNG/队列/账户不推进就重算。"""
    while True:
        pending = requests.next_pending()
        result = Numbers.run_exact_batch(lambda: accept_pending_base(requests, pending, orders, queue, account,
            fills, terms, ask_rows, fee_rate, fee_exponent, selected_paths, taker_rebate_rate, row_roots, result_refs,
            calendars=calendars,enabled=enabled))
        effects = result[-1]
        required_orders = int(effects.order.required_orders.max().cpu())
        required_fills = int(effects.acceptance.required_records.max().cpu())
        required_segments = int(effects.required_segments.max().cpu()) if effects.required_segments.numel() else 0
        required_day_fills = int(effects.required_day_fills.max().cpu()) if calendars is not None else 0
        required_days = int(effects.required_days.max().cpu()) if calendars is not None else 0
        calendar_fits = calendars is None or (required_day_fills <= calendars.taker.fill_occupied.size(1) and
                                              required_days <= calendars.taker.occupied.size(1))
        if required_orders <= orders.capacity and required_fills <= fills.capacity and required_segments <= queue.capacity and calendar_fits:
            return result
        orders = orders.resized(1 << (max(1, required_orders)-1).bit_length())
        fills = fills.resized(1 << (max(1, required_fills)-1).bit_length())
        old_capacity = queue.capacity
        queue = queue.resized(max(queue.capacity, 1 << (max(1, required_segments)-1).bit_length()))
        if calendars is not None:
            calendars = replace(calendars,taker=calendars.taker.resized(
                fills=max(calendars.taker.fill_occupied.size(1),1 << (max(1,required_day_fills)-1).bit_length()),
                days=max(calendars.taker.occupied.size(1),1 << (max(1,required_days)-1).bit_length())))
        if queue.capacity != old_capacity and queue.rows and ask_rows.size(1):
            # 段池增长只添加未成交padding；原ask×段路径坐标必须保持。
            selected_paths = torch.nn.functional.pad(
                selected_paths.reshape(orders.roots, ask_rows.size(1), old_capacity),
                (0, queue.capacity-old_capacity), value=0).flatten(1)
