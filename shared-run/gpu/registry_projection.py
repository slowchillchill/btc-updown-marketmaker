"""实际设备NOE领取列到共享Registry公开字段的批量接缝。

解码原现行Base／QuoteBudget Submission、Accepted OrderLifecycle、TradeLifecycleV3和普通Query schema。
输入是NOE双门已领取引用；不读取Ledger应用结果，不经CPU逐条业务回调。
Orders分页查询/其它取消或登记schema由各自来源接入，不把未知类型解释为已支持。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from fields import decode_fields
from core import OUT_EVIDENCE
from registry import PublicRegistryEvents,BASE_ACCEPTED,QUOTE_ACCEPTED,ORDER_LIFECYCLE,TRADE,ORDER_QUERY


def concatenate_integer(values,roots):
    capacity=max(value.words.size(1) for value in values)
    prepared=[value.with_capacity(capacity) for value in values]
    return ExactInt(torch.cat([value.words.reshape(roots,-1,capacity) for value in prepared],1).flatten(0,1),
        torch.cat([value.sign.reshape(roots,-1) for value in prepared],1).flatten(),
        torch.cat([value.fault.reshape(roots,-1) for value in prepared],1).flatten())


@dataclass(frozen=True)
class ClosedRowLookup:
    rows: torch.Tensor
    at: torch.Tensor
    coordinates: torch.Tensor
    active: torch.Tensor
    fault: torch.Tensor
    matches: torch.Tensor
    match_fault: torch.Tensor


def resolve_closed_public_rows(bank,batches,closed):
    """原共享位置到来源批坐标；公开Registry与取消读取复用同一设备绑定。"""
    roots,width=closed.record_refs.shape
    device=closed.record_refs.device
    original_rows=torch.cat([batch.rows for batch in batches],1)
    original_active=torch.cat([batch.emitted for batch in batches],1)
    matching=(original_rows[:,:,None]==closed.record_refs[:,None,:])&original_active[:,:,None]
    count=matching.to(torch.int64).sum(1)
    match_fault=closed.fault|((count!=1)&closed.emitted).any(1).to(torch.int32)
    fault=match_fault
    at=matching.to(torch.int32).argmax(1)
    coordinates=(torch.arange(roots,device=device)[:,None]*original_rows.size(1)+at).flatten()
    rows=closed.record_refs.clamp(0,bank.kinds.numel()-1).flatten()
    invalid=(closed.record_refs<0)|(closed.record_refs>=bank.kinds.numel())
    fault=fault|(invalid&closed.emitted).any(1).to(torch.int32)
    fault=fault|Numbers.combine_fault_rows(torch.where(closed.emitted,bank.fault[rows].reshape(roots,width),0))
    outcomes=torch.cat([batch.normalized.outcomes for batch in batches],1).gather(1,at).flatten()
    active=closed.emitted.flatten()&(outcomes==OUT_EVIDENCE)&(fault==0).repeat_interleave(width)
    return ClosedRowLookup(rows,at,coordinates,active,fault,matching,match_fault)


def public_rows_from_closed(bank,batches,closed,receive_rank,*,source_rows=None):
    from commitments import SubmissionBindings
    roots,width=closed.record_refs.shape
    device=closed.record_refs.device
    if receive_rank.shape!=(roots,width) or receive_rank.dtype!=torch.int64 or receive_rank.device!=device:
        raise TypeError('Registry物理领取顺序需同设备int64[roots,records]')
    if width == 0:
        from dataclasses import replace
        events=replace(PublicRegistryEvents.empty(roots,device),fault=closed.fault)
        return events,SubmissionBindings(events.received,events.request_slots,events.item_ordinals,
            events.private_sequences,events.receive_rank,events.orders,events.received,events.fault)
    source_rows=resolve_closed_public_rows(bank,batches,closed) if source_rows is None else source_rows
    rows,at,coordinates,active,fault=(source_rows.rows,source_rows.at,source_rows.coordinates,source_rows.active,source_rows.fault)
    schemas=bank.payload_schemas[rows]
    layouts={3:['scalar']*4+['integer','scalar','integer','integer','scalar'],
        27:['scalar']*4+['integer','scalar','integer','scalar'],
        7:['scalar']*3+['integer','scalar','scalar'],
        8:['scalar']*3+['integer','integer']+['scalar']*4+['rational']+['integer']*3+['scalar','integer','scalar','rational'],
        19:['scalar']*3+['integer','scalar','scalar','scalar'],
        25:['scalar']*4+['integer']+['scalar']*3+['rational']*2+['scalar']*2+['integer']*2}
    decoded={}
    for schema,layout in layouts.items():
        enabled=active & (schemas==schema)
        decoded[schema],error=decode_fields(bank.semantic[rows],layout,enabled)
        fault=fault | Numbers.combine_fault_rows(torch.where(enabled,error,0).reshape(roots,width))
    market_close=active & (schemas==19)
    for index in range(6):
        left,right=decoded[7][index],decoded[19][index]
        decoded[7][index]=torch.where(market_close,right,left) if isinstance(left,torch.Tensor) else right.select(market_close,left)
    quote=active & (schemas==27)
    for index in range(7):
        left,right=decoded[3][index],decoded[27][index]
        decoded[3][index]=torch.where(quote,right,left) if isinstance(left,torch.Tensor) else right.select(quote,left)
    decoded[3][8]=torch.where(quote,decoded[27][7],decoded[3][8])
    submission_schema=(schemas==3)|quote
    accepted=active & submission_schema & (decoded[3][1]==0)
    formed=active & ((schemas==7)|(schemas==19))
    # Accepted与真实CallerCanceled分开；其它未接Order事件仍显式报告缺口。
    order_life=formed
    fault=fault | (order_life & (decoded[7][4]!=0)&(decoded[7][4]!=4)&~(market_close&(decoded[7][4]==5))).reshape(roots,width).any(1).to(torch.int32)
    closed_order=order_life&((decoded[7][4]==4)|(decoded[7][4]==5))
    formed=order_life&(decoded[7][4]==0)
    trades=active & (schemas==8)
    queries=active & (schemas==25) & (decoded[25][1]==0)
    received=(accepted|order_life|trades|queries).reshape(roots,width)
    zero=ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=device))
    order=decoded[3][4].select(accepted,zero)
    order=decoded[7][3].select(order_life,order)
    order=decoded[8][4].select(trades,order)
    order=decoded[25][4].select(queries,order)
    fill=decoded[8][3].select(trades,zero)
    kinds=torch.where(queries,ORDER_QUERY,torch.where(trades,TRADE,torch.where(order_life,ORDER_LIFECYCLE,torch.where(quote,QUOTE_ACCEPTED,BASE_ACCEPTED)))).reshape(roots,width)
    units=lambda value:ExactRational.ratio(value,value.constant(1_000_000))
    private=concatenate_integer([batch.private_sequences for batch in batches],roots)
    ordinals=concatenate_integer([batch.record_ordinals for batch in batches],roots)
    batch_sequences=concatenate_integer([batch.batch_sequences for batch in batches],roots)
    contexts=torch.cat([batch.context_slots for batch in batches],1).gather(1,at)
    requests=torch.cat([batch.request_slots for batch in batches],1).gather(1,at)
    items=torch.cat([batch.item_ordinals for batch in batches],1).gather(1,at)
    runs=torch.cat([batch.run_slots for batch in batches],1).gather(1,at)
    events=PublicRegistryEvents(received,kinds,order,fill,_take(private,coordinates),closed.delivered_sequences,
        _take(ordinals,coordinates),_take(batch_sequences,coordinates),contexts,runs,requests,items,closed.record_refs,closed.available_at,receive_rank,
        units(decoded[3][6]),units(decoded[3][7]),units(decoded[8][10]),
        decoded[8][8].reshape(roots,width),decoded[8][6].reshape(roots,width),
        (formed & (decoded[7][5]==1)).reshape(roots,width),formed.reshape(roots,width),
        torch.zeros_like(received),closed_order.reshape(roots,width),
        torch.cat([batch.platform_at for batch in batches],1).gather(1,at),
        torch.cat([batch.received_at for batch in batches],1).gather(1,at),fault,
        torch.where(queries,decoded[25][5],-1).reshape(roots,width),
        torch.where(queries,decoded[25][7],-1).reshape(roots,width))

    submission=active & submission_schema
    fault=fault | (submission & ((decoded[3][1]<0)|(decoded[3][1]>2))).reshape(roots,width).any(1).to(torch.int32)
    bindings=SubmissionBindings(submission.reshape(roots,width),requests,items,_take(private,coordinates),receive_rank,
        decoded[3][4],accepted.reshape(roots,width),fault)
    from dataclasses import replace
    # Accepted保存逐腿原处理时刻；Order/Trade/Query只保实际公开归属。
    condition=torch.where(accepted,decoded[3][2],-1)
    condition=torch.where(order_life,decoded[7][1],condition)
    condition=torch.where(trades,decoded[8][1],condition)
    condition=torch.where(queries,decoded[25][2],condition)
    return replace(events,fault=fault,processing_at=torch.where(accepted,decoded[3][8],-1).reshape(roots,width),
        condition_slots=condition.reshape(roots,width)),bindings


def order_events_from_closed(bank,batches,closed,receive_rank):
    return public_rows_from_closed(bank,batches,closed,receive_rank)[0]
