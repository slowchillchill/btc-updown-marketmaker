"""单个原level transition产生的逐订单正quote Maker fill共同候选。

调用方按原FIFO encounter顺序逐个调用。不能把多条hidden transition合并后
送本入口；零quote仅由原订单数量保留pending，不创建成交或返佣前驱。
"""
from dataclasses import replace
import torch
from queues import Numbers
from fill_pool import FillPoolTransition


def maker_column(orders, account, fills, maker, fault, order_slot, amount,
                 assets, condition, source_price, at, selected_path, active,
                 fee_rate, fee_exponent, taker_rate):
    """一列原FIFO encounter的共同纯候选；设备循环与原固定循环共用。"""
    live = active & (amount.sign > 0)
    read = orders.read(order_slot)
    association = ~read.present | (read.assets != assets) | (read.conditions != condition)
    fault = fault | read.fault | torch.where(live, amount.fault | association.to(torch.int32), 0)
    quantity, depletion = read.quantity.maker_depletion(amount, live & (fault == 0))
    fault = fault | depletion.fault * live.to(torch.int32)
    orders = orders.update(read, quantity, live & (fault == 0), at)
    fills, account, maker, transition, allocation, demand = start_maker_fill(
        fills, account, maker, read, depletion.base, depletion.quote, condition,
        source_price, at, selected_path, depletion.emitted & (fault == 0),
        fee_rate, fee_exponent, taker_rate)
    fault = fault | allocation.fault | demand.fault | orders.fault
    return orders, account, fills, maker, fault, transition, allocation, demand


def start_maker_fill(pool, account, maker, order, base, quote, condition, source_price,
                     matched_at, selected_path, enabled, fee_rate, fee_exponent, taker_rate):
    """订单目录真实read、原depletion价和选定路径进入原fill/账户/日桶。"""
    original_pool, original_account, original_maker = pool, account, maker
    requested = enabled & (quote.sign > 0)
    fault = (pool.fault | account.fault | maker.fault | order.fault |
        (~order.present).to(torch.int32)*requested.to(torch.int32))
    fault = fault | (base.fault | quote.fault | source_price.fault)*enabled.to(torch.int32)
    record = pool.new_record(base, quote, base.constant(0), order.assets, matched_at,
                             requested & (fault == 0), selected_path)
    record, account, initial = record.advance_one(account, matched_at, taker_rate)
    pool, maker, allocation, demand = pool.enqueue_with_maker(record, maker, condition,
        source_price, fee_rate, fee_exponent)
    pool = pool.bind_origin(allocation.slots, order.sequence, order.slots, condition,
                           torch.ones_like(order.slots), source_price, allocation.emitted)
    fault = fault | pool.fault | initial.fault | demand.fault | account.fault
    shortage = ((allocation.required_records > pool.capacity) |
        (demand.fill_capacity > maker.fill_occupied.size(1)) | (demand.day_capacity > maker.occupied.size(1)))
    fault = fault | shortage.to(torch.int32)*2
    committed = allocation.emitted & demand.committed & (fault == 0)
    pool = pool.coordinate_result(original_pool, committed, fault)
    maker = maker.coordinate_result(original_maker, committed, fault)
    account = account.coordinate_result(original_account, fault)
    initial = replace(initial, emitted=initial.emitted & committed, terminal=initial.terminal & committed,
        fault=fault, economic_delta=account.coordinate_delta(initial.economic_delta, committed, fault),
        failure_delta=account.coordinate_delta(initial.failure_delta, committed, fault),
        rebate_delta=account.coordinate_delta(initial.rebate_delta, committed, fault))
    index = torch.arange(pool.roots, device=matched_at.device)*pool.capacity+allocation.slots
    transition = FillPoolTransition(allocation.sequence, allocation.slots, initial, pool.origins.take(index))
    return pool, account, maker, transition, replace(allocation, emitted=committed, fault=fault), demand


def run_start_maker_fill(pool, account, maker, order, base, quote, condition, source_price,
                         matched_at, selected_path, enabled, fee_rate, fee_exponent, taker_rate):
    """纯候选扩池重试不消费原fill/订单/RNG；只读取实际物理需求。"""
    while True:
        result = Numbers.run_exact_batch(lambda: start_maker_fill(pool, account, maker, order, base, quote,
            condition, source_price, matched_at, selected_path, enabled, fee_rate, fee_exponent, taker_rate))
        allocation, demand = result[-2:]
        required_pool = int(allocation.required_records.max().cpu())
        required_fills = int(demand.fill_capacity.max().cpu())
        required_days = int(demand.day_capacity.max().cpu())
        if required_pool <= pool.capacity and required_fills <= maker.fill_occupied.size(1) and required_days <= maker.occupied.size(1):
            return result
        pool = pool.resized(1 << (max(1, required_pool)-1).bit_length())
        maker = maker.resized(fills=max(maker.fill_occupied.size(1),1 << (max(1, required_fills)-1).bit_length()),
                              days=max(maker.occupied.size(1),1 << (max(1, required_days)-1).bit_length()))
