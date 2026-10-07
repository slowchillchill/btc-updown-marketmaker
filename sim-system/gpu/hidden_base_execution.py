"""一条原hidden LevelExecutionTransition的FIFO→Base订单→Maker fill事务。

每根本次只处理原序中的一条transition。逐Account encounter创建实际正quote
fill；整消息/桶顺序与关闭前沿由外层原hidden宿主保持。本层不推进公开可见性。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, _take
from asset_buy import take_ratio
from account_state import grouped_fault
from base_acceptance import _coordinate_queue, _coordinate_pool
from pending_base_execution import _coordinate_orders
from taker_days import coordinate_calendar
from maker_execution import start_maker_fill
from hidden_market import rational_select, rational_take


@dataclass(frozen=True)
class HiddenBaseEffects:
    fills: tuple
    required_segments: torch.Tensor
    required_records: torch.Tensor
    required_maker_fills: torch.Tensor
    required_maker_days: torch.Tensor
    fault: torch.Tensor


def apply_base_level_transition(queue, orders, account, fills, calendars, rows, before, after,
        source_price, condition, at, enabled, selected_paths, row_roots, row_assets,
        fee_rate, fee_exponent, taker_rate):
    """原价位行已绑定；不合并不同step、不按order seq重排原FIFO effects。"""
    roots = orders.roots
    device = orders.fault.device
    for value in (rows, condition, at):
        if value.shape != (roots,) or value.dtype != torch.int64 or value.device != device:
            raise TypeError('hidden坐标/时刻须同设备int64[roots]')
    if enabled.shape != (roots,) or enabled.dtype != torch.bool or enabled.device != device:
        raise TypeError('hidden接纳人口须同设备bool[roots]')
    if any(v.shape != (queue.rows,) or v.dtype != torch.int64 or v.device != device for v in (row_roots,row_assets)):
        raise TypeError('原队列根/资产绑定须同设备int64[rows]')
    if selected_paths.shape != (roots,queue.capacity) or selected_paths.dtype != torch.int64 or selected_paths.device != device:
        raise TypeError('原逐FIFO encounter路径坐标须同设备int64[roots,segments]')
    original_queue, original_orders, original_account = queue, orders, account
    original_fills, original_calendars = fills, calendars
    valid_mapping = (row_roots >= 0) & (row_roots < roots)
    owner = row_roots.clamp(0,roots-1)
    fault = orders.fault | account.fault | fills.fault | calendars.maker.fault | calendars.taker.fault
    fault = fault | (~valid_mapping).any().to(torch.int32) | grouped_fault(queue.fault,owner,roots)
    fault = fault | ((before.fault | after.fault | source_price.fault)*enabled.to(torch.int32))
    demand_records = fills.occupied.to(torch.int64).sum(1)
    demand_maker_fills = torch.full_like(rows,calendars.maker.fill_occupied.size(1))
    demand_maker_days = torch.full_like(rows,calendars.maker.occupied.size(1))
    if queue.rows == 0:
        fault = fault | enabled.to(torch.int32)
        calendars = replace(calendars,
            maker=coordinate_calendar(calendars.maker,calendars.maker,fault == 0,fault),
            taker=coordinate_calendar(calendars.taker,calendars.taker,fault == 0,fault))
        return (queue,replace(orders,fault=fault),replace(account,fault=fault),replace(fills,fault=fault),calendars,
            HiddenBaseEffects((),torch.empty(0,dtype=torch.int64,device=device),demand_records,
                              demand_maker_fills,demand_maker_days,fault))
    safe = rows.clamp(0,queue.rows-1)
    price_relation = take_ratio(queue.price,safe).compare(source_price)
    bad_row = (rows < 0) | (rows >= queue.rows) | (row_roots[safe] != torch.arange(roots,device=device))
    fault = fault | torch.where(enabled,bad_row.to(torch.int32) | price_relation.fault |
                                (price_relation.order != 0).to(torch.int32),0)
    active = enabled & (fault == 0)
    selected = valid_mapping & active[owner] & (torch.arange(queue.rows,device=device) == rows[owner])
    row_before = _take(before,owner).select(selected,queue.debt.constant(0))
    row_after = _take(after,owner).select(selected,queue.debt.constant(0))
    queue = replace(queue,fault=queue.fault | fault[owner])
    queue, effects = queue.apply_level_transition(row_before,row_after)
    fault = fault | grouped_fault(effects.fault,owner,roots)
    frames = []
    maker = calendars.maker
    for column in range(queue.capacity):
        # effects仍是原front遍历所得Account encounter顺序；物理slot只用于定位订单。
        index = safe*queue.capacity+column
        order_slot = effects.order_ids[safe,column]
        amount = _take(effects.maker_base_units,index)
        live = active & (amount.sign > 0)
        read = orders.read(order_slot)
        association = ~read.present | (read.assets != row_assets[safe]) | (read.conditions != condition)
        fault = fault | read.fault | torch.where(live,amount.fault | association.to(torch.int32),0)
        quantity, depletion = read.quantity.maker_depletion(amount,live & (fault == 0))
        fault = fault | depletion.fault*live.to(torch.int32)
        orders = orders.update(read,quantity,live & (fault == 0))
        fills, account, maker, transition, allocation, demand = start_maker_fill(fills,account,maker,read,
            depletion.base,depletion.quote,condition,source_price,at,selected_paths[:,column],
            depletion.emitted & (fault == 0),fee_rate,fee_exponent,taker_rate)
        fault = fault | allocation.fault | demand.fault | orders.fault
        demand_records = torch.maximum(demand_records,allocation.required_records)
        demand_maker_fills = torch.maximum(demand_maker_fills,demand.fill_capacity)
        demand_maker_days = torch.maximum(demand_maker_days,demand.day_capacity)
        frames.append(transition)
    fault = fault | grouped_fault((effects.required_segments > queue.capacity).to(torch.int32)*2,owner,roots)
    committed = fault == 0
    queue = _coordinate_queue(queue,original_queue,owner,fault)
    orders = _coordinate_orders(orders,original_orders,fault)
    fills = _coordinate_pool(fills,original_fills,fault)
    account = account.coordinate_result(original_account,fault)
    calendars = replace(calendars,
        maker=coordinate_calendar(maker,original_calendars.maker,committed,fault),
        taker=coordinate_calendar(calendars.taker,original_calendars.taker,committed,fault))
    def commit(frame):
        event = frame.event
        return replace(frame,event=replace(event,emitted=event.emitted & committed,
            terminal=event.terminal & committed,fault=fault,
            economic_delta=account.coordinate_delta(event.economic_delta,committed,fault),
            failure_delta=account.coordinate_delta(event.failure_delta,committed,fault),
            rebate_delta=account.coordinate_delta(event.rebate_delta,committed,fault)))
    return queue,orders,account,fills,calendars,HiddenBaseEffects(tuple(map(commit,frames)),
        effects.required_segments,demand_records,demand_maker_fills,demand_maker_days,fault)


def run_base_level_transition(queue, orders, account, fills, calendars, rows, before, after,
        source_price, condition, at, enabled, selected_paths, row_roots, row_assets,
        fee_rate, fee_exponent, taker_rate):
    """仅物理人口需求回读；重试保持同一原transition、RNG和全部已提交前缀。"""
    while True:
        result = Numbers.run_exact_batch(lambda:apply_base_level_transition(queue,orders,account,fills,calendars,
            rows,before,after,source_price,condition,at,enabled,selected_paths,row_roots,row_assets,
            fee_rate,fee_exponent,taker_rate))
        effect = result[-1]
        segment_capacity = int(effect.required_segments.max().cpu()) if queue.rows else 0
        fill_capacity = int(effect.required_records.max().cpu())
        maker_fills = int(effect.required_maker_fills.max().cpu())
        maker_days = int(effect.required_maker_days.max().cpu())
        if (segment_capacity <= queue.capacity and fill_capacity <= fills.capacity and
            maker_fills <= calendars.maker.fill_occupied.size(1) and maker_days <= calendars.maker.occupied.size(1)):
            return result
        old_capacity = queue.capacity
        queue = queue.resized(max(queue.capacity,1 << (max(1,segment_capacity)-1).bit_length()))
        selected_paths = torch.nn.functional.pad(selected_paths,(0,queue.capacity-old_capacity),value=0)
        fills = fills.resized(1 << (max(1,fill_capacity)-1).bit_length())
        calendars = replace(calendars,maker=calendars.maker.resized(
            fills=max(calendars.maker.fill_occupied.size(1),1 << (max(1,maker_fills)-1).bit_length()),
            days=max(calendars.maker.occupied.size(1),1 << (max(1,maker_days)-1).bit_length())))


def apply_hidden_base_transition(state, orders, account, fills, calendars, rows, before, after,
        source_price, condition, at, enabled, resync_inference, selected_paths, row_roots,
        fee_rate, fee_exponent, taker_rate):
    """资产前驱检查→唯一FIFO更新→订单/逐fill/账户，整条原step共同提交。

    before/after保留源shares全部精度；只在撮合单位边界转换micro量。
    不先调用HiddenAssetState.level_transition，再重复应用它的queue候选。
    """
    roots=orders.roots
    owner=account.asset_roots
    if owner.shape != state.initialized.shape:
        raise ValueError('hidden资产与账户完整资产目录不一致')
    level_roots=owner[state.level_assets]
    if row_roots.shape != level_roots.shape:
        raise ValueError('hidden原价位与FIFO行绑定不一致')
    original=state
    valid_row=(rows>=0) & (rows<state.queues.rows)
    safe=rows.clamp(0,max(0,state.queues.rows-1))
    fault=grouped_fault(state.fault,owner,roots)
    fault=fault | ((before.fault | after.fault | source_price.fault |
        (~valid_row).to(torch.int32))*enabled.to(torch.int32))
    fault=fault | grouped_fault((row_roots != level_roots).to(torch.int32),level_roots,roots)
    row_axis=torch.arange(state.queues.rows,device=owner.device)
    selected=enabled[level_roots] & (row_axis==rows[level_roots]) & (fault[level_roots]==0)
    row_before=rational_take(before,level_roots)
    row_after=rational_take(after,level_roots)
    prepared,old_units,new_units,queue_enabled=state.prepare_level_transition(
        selected,row_before,row_after,resync_inference[level_roots])
    fault=fault | grouped_fault(prepared.fault,owner,roots)
    # 原资产故障由本根所有候选共同携带；未选中根仍保留既有故障。
    candidate_orders=replace(orders,fault=orders.fault | fault)
    candidate_account=replace(account,fault=account.fault | fault)
    candidate_fills=replace(fills,fault=fills.fault | fault)
    use=enabled & valid_row & (fault==0)
    if state.queues.rows:
        use=use & queue_enabled[safe]
        old=_take(old_units,safe)
        new=_take(new_units,safe)
    else:
        old=new=account.cash.constant(0)
    queue,orders,account,fills,calendars,effects=apply_base_level_transition(state.queues,
        candidate_orders,candidate_account,candidate_fills,calendars,rows,old,new,source_price,
        condition,at,use,selected_paths,row_roots,state.level_assets,fee_rate,fee_exponent,taker_rate)
    fault=effects.fault | fault
    committed=(fault==0)[owner]
    prepared=replace(prepared,
        external_shares=rational_select(prepared.external_shares,committed[state.level_assets],original.external_shares),
        queues=queue,fault=original.fault | fault[owner])
    return prepared,orders,account,fills,calendars,replace(effects,fault=fault)


def run_hidden_base_transition(state, orders, account, fills, calendars, rows, before, after,
        source_price, condition, at, enabled, resync_inference, selected_paths, row_roots,
        fee_rate, fee_exponent, taker_rate):
    """重试同一原step；容量需求仅在纯候选边界回读，不推进源/RNG/运行前沿。"""
    while True:
        result=Numbers.run_exact_batch(lambda:apply_hidden_base_transition(state,orders,account,fills,calendars,
            rows,before,after,source_price,condition,at,enabled,resync_inference,selected_paths,row_roots,
            fee_rate,fee_exponent,taker_rate))
        effect=result[-1]
        segments=int(effect.required_segments.max().cpu()) if state.queues.rows else 0
        records=int(effect.required_records.max().cpu())
        maker_fills=int(effect.required_maker_fills.max().cpu())
        maker_days=int(effect.required_maker_days.max().cpu())
        if (segments<=state.queues.capacity and records<=fills.capacity and
            maker_fills<=calendars.maker.fill_occupied.size(1) and maker_days<=calendars.maker.occupied.size(1)):
            return result
        old=state.queues.capacity
        state=replace(state,queues=state.queues.resized(max(old,1 << (max(1,segments)-1).bit_length())))
        selected_paths=torch.nn.functional.pad(selected_paths,(0,state.queues.capacity-old),value=0)
        fills=fills.resized(max(fills.capacity,1 << (max(1,records)-1).bit_length()))
        calendars=replace(calendars,maker=calendars.maker.resized(
            fills=max(calendars.maker.fill_occupied.size(1),1 << (max(1,maker_fills)-1).bit_length()),
            days=max(calendars.maker.occupied.size(1),1 << (max(1,maker_days)-1).bit_length())))
