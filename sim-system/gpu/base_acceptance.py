"""原已编码Base接单的逐fill候选装配；源准入/请求时钟仍由原公共宿主负责。

原accept、每个原plan fill的首最终性及增长池在一个纯候选资源事务内计算。
不能把各ask聚合量当成一个trade，不能在容量重试前提交账户、队列或随机数。
"""
from dataclasses import dataclass,fields,replace
import torch
from queues import Numbers,ExactInt,_take
from fill_pool import FillPoolTransition
from account_state import grouped_fault


@dataclass(frozen=True)
class BaseAcceptanceEffects:
    submission_delta: object
    rejected: torch.Tensor
    fills: tuple
    required_records: torch.Tensor
    fault: torch.Tensor


def _coordinate_queue(candidate,original,row_roots,fault):
    enabled = fault[row_roots] == 0
    flat = enabled.repeat_interleave(candidate.capacity)
    return replace(candidate,
        kind=torch.where(enabled[:,None],candidate.kind,original.kind),
        order_ids=torch.where(enabled[:,None],candidate.order_ids,original.order_ids),
        total_base=candidate.total_base.select(flat,original.total_base),
        total_quote=candidate.total_quote.select(flat,original.total_quote),
        consumed_base=candidate.consumed_base.select(flat,original.consumed_base),
        account_remaining=candidate.account_remaining.select(flat,original.account_remaining),
        debt=candidate.debt.select(enabled,original.debt),fault=candidate.fault | fault[row_roots])


def _coordinate_pool(candidate,original,fault):
    enabled = (fault == 0).repeat_interleave(candidate.capacity)
    updates = {}
    for field in fields(candidate.records):
        value,old = getattr(candidate.records,field.name),getattr(original.records,field.name)
        if field.name in ('offsets','states','lengths','economic_ordinals'):
            updates[field.name] = value
        elif isinstance(value,ExactInt):
            updates[field.name] = value.select(enabled,old)
        else:
            updates[field.name] = torch.where(enabled,value,old)
    return replace(candidate,records=replace(candidate.records,**updates),
        occupied=torch.where((fault == 0)[:,None],candidate.occupied,original.occupied),
        sequences=candidate.sequences.select(enabled,original.sequences),
        next_sequence=candidate.next_sequence.select(fault == 0,original.next_sequence),fault=fault,
        origins=candidate.origins.coordinate(original.origins,fault == 0))


def accept_base_with_fills(prepared,queue,account,pool,matched_at,selected_paths,taker_rebate_rate,row_roots):
    """selected_paths[roots,maximum_fills]是原密封selector已经选定的设备bank坐标。"""
    roots = prepared.requested.numel()
    if matched_at.shape != (roots,) or matched_at.dtype != torch.int64 or matched_at.device != prepared.requested.device:
        raise TypeError('接单到达时刻须同设备int64[roots]')
    if selected_paths.shape != (roots,prepared.maximum_fills) or selected_paths.dtype != torch.int64 or selected_paths.device != matched_at.device:
        raise TypeError('逐原fill选定路径须同设备int64矩阵')
    if row_roots.shape != (queue.rows,) or row_roots.dtype != torch.int64 or row_roots.device != matched_at.device:
        raise TypeError('队列根映射须同设备int64[rows]')
    if pool.roots != roots:
        raise ValueError('接单与成交池根布局不一致')
    safe_roots = row_roots.clamp(0,roots-1)
    original_queue,original_account,original_pool = queue,account,pool
    queue,account,submission,rejected,fault = prepared.accept(queue,account)
    fault = fault | ((row_roots < 0) | (row_roots >= roots)).any().to(torch.int32)
    fault = fault | grouped_fault(queue.fault,safe_roots,roots)
    demanded = pool.occupied.to(torch.int64).sum(1)
    output = []
    for column in range(prepared.maximum_fills):
        coordinate = torch.arange(roots,device=matched_at.device)*prepared.maximum_fills+column
        base = _take(prepared.taker_bases,coordinate)
        quote = _take(prepared.taker_quotes,coordinate)
        fee = _take(prepared.fee.per_fill_micro,coordinate)
        active = submission.emitted & (base.sign > 0) & (fault == 0)
        record = pool.new_record(base,quote,fee,prepared.asset_indices,matched_at,active,selected_paths[:,column])
        record,account,transition = record.advance_one(account,matched_at,taker_rebate_rate)
        pool,allocation = pool.enqueue(record)
        demanded = torch.maximum(demanded,allocation.required_records)
        fault = fault | transition.fault | allocation.fault
        output.append(FillPoolTransition(allocation.sequence,allocation.slots,transition))
    fault = fault | account.fault | pool.fault
    committed = submission.emitted & (fault == 0)
    account = account.coordinate_result(original_account,fault)
    submission = account.coordinate_delta(submission,committed,fault)
    events = []
    for frame in output:
        event = frame.event
        event = replace(event,emitted=event.emitted & committed,terminal=event.terminal & committed,
            economic_delta=account.coordinate_delta(event.economic_delta,committed,fault),
            failure_delta=account.coordinate_delta(event.failure_delta,committed,fault),
            rebate_delta=account.coordinate_delta(event.rebate_delta,committed,fault),fault=fault)
        events.append(replace(frame,event=event))
    pool = _coordinate_pool(pool,original_pool,fault)
    queue = _coordinate_queue(queue,original_queue,safe_roots,fault)
    return queue,account,pool,BaseAcceptanceEffects(submission,rejected & (fault == 0),tuple(events),demanded,fault)


def run_base_acceptance_batch(prepared,queue,account,pool,matched_at,selected_paths,taker_rebate_rate,row_roots):
    while True:
        result = Numbers.run_exact_batch(lambda:accept_base_with_fills(prepared,queue,account,pool,
            matched_at,selected_paths,taker_rebate_rate,row_roots))
        requested = result[-1].required_records
        demand = int(requested.max().cpu()) if requested.numel() else 0
        if demand <= pool.capacity:
            return result
        pool = pool.resized(1 << (demand-1).bit_length())
