"""实际设备NOE领取列到共享Registry公开字段的批量接缝。

解码原现行Base Submission、Accepted OrderLifecycle、TradeLifecycleV3 schema。
输入是NOE双门已领取引用；不读取Ledger应用结果，不经CPU逐条业务回调。
更宽查询/取消/登记schema由各自来源接入，不把未知类型解释为已支持。
"""
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from fields import decode_fields
from core import OUT_EVIDENCE
from registry import PublicRegistryEvents,BASE_ACCEPTED,ORDER_LIFECYCLE,TRADE


def concatenate_integer(values,roots):
    capacity=max(value.words.size(1) for value in values)
    prepared=[value.with_capacity(capacity) for value in values]
    return ExactInt(torch.cat([value.words.reshape(roots,-1,capacity) for value in prepared],1).flatten(0,1),
        torch.cat([value.sign.reshape(roots,-1) for value in prepared],1).flatten(),
        torch.cat([value.fault.reshape(roots,-1) for value in prepared],1).flatten())


def order_events_from_closed(bank,batches,closed,receive_rank):
    roots,width=closed.record_refs.shape
    device=closed.record_refs.device
    if receive_rank.shape!=(roots,width) or receive_rank.dtype!=torch.int64 or receive_rank.device!=device:
        raise TypeError('Registry物理领取顺序需同设备int64[roots,records]')
    original_rows=torch.cat([batch.rows for batch in batches],1)
    original_active=torch.cat([batch.emitted for batch in batches],1)
    matching=(original_rows[:,:,None]==closed.record_refs[:,None,:]) & original_active[:,:,None]
    count=matching.to(torch.int64).sum(1)
    fault=closed.fault | ((count!=1)&closed.emitted).any(1).to(torch.int32)
    at=matching.to(torch.int32).argmax(1)
    coordinates=(torch.arange(roots,device=device)[:,None]*original_rows.size(1)+at).flatten()
    rows=closed.record_refs.clamp(0,bank.kinds.numel()-1).flatten()
    row_fault=(closed.record_refs<0)|(closed.record_refs>=bank.kinds.numel())
    fault=fault | (row_fault&closed.emitted).any(1).to(torch.int32)
    outcomes=torch.cat([batch.normalized.outcomes for batch in batches],1).gather(1,at).flatten()
    fault=fault | Numbers.combine_fault_rows(torch.where(closed.emitted,bank.fault[rows].reshape(roots,width),0))
    active=closed.emitted.flatten() & (outcomes==OUT_EVIDENCE) & (fault==0).repeat_interleave(width)
    schemas=bank.payload_schemas[rows]
    layouts={3:['scalar']*4+['integer','scalar','integer','integer','scalar'],
        7:['scalar']*3+['integer','scalar','scalar'],
        8:['scalar']*3+['integer','integer']+['scalar']*4+['rational']+['integer']*3+['scalar','integer','scalar','rational']}
    decoded={}
    for schema,layout in layouts.items():
        enabled=active & (schemas==schema)
        decoded[schema],error=decode_fields(bank.semantic[rows],layout,enabled)
        fault=fault | Numbers.combine_fault_rows(torch.where(enabled,error,0).reshape(roots,width))
    accepted=active & (schemas==3) & (decoded[3][1]==0)
    formed=active & (schemas==7)
    # 当前生产来源仅发Accepted；未实现的Order事件不得冒充该事件。
    fault=fault | (formed & (decoded[7][4]!=0)).reshape(roots,width).any(1).to(torch.int32)
    trades=active & (schemas==8)
    received=(accepted|formed|trades).reshape(roots,width)
    zero=ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=device))
    order=decoded[3][4].select(accepted,zero)
    order=decoded[7][3].select(formed,order)
    order=decoded[8][4].select(trades,order)
    fill=decoded[8][3].select(trades,zero)
    kinds=torch.where(trades,TRADE,torch.where(formed,ORDER_LIFECYCLE,BASE_ACCEPTED)).reshape(roots,width)
    units=lambda value:ExactRational.ratio(value,value.constant(1_000_000))
    private=concatenate_integer([batch.private_sequences for batch in batches],roots)
    ordinals=torch.cat([batch.record_ordinals for batch in batches],1).gather(1,at)
    return PublicRegistryEvents(received,kinds,order,fill,_take(private,coordinates),closed.delivered_sequences,
        ExactInt.from_i64_tensor(ordinals.flatten()),closed.available_at,receive_rank,
        units(decoded[3][6]),units(decoded[3][7]),units(decoded[8][10]),
        decoded[8][8].reshape(roots,width),decoded[8][6].reshape(roots,width),
        (formed & (decoded[7][5]==1)).reshape(roots,width),formed.reshape(roots,width),
        torch.zeros_like(received),torch.zeros_like(received),fault)
