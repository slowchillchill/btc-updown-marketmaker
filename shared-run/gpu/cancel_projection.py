"""实际公开领取NOE取消/完成字段的设备读取。

输出仍携带原共享位置与来源列。订单身份目录的增长、输入引用人口与
目标去重由首次领取宿主接线，不把未实现schema或未来记录当作取消结果。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from fields import decode_fields
from registry import CancellationCompletions
from registry_projection import resolve_closed_public_rows,concatenate_integer
from registry import identity_matches
from cancel_requests import CancellationScopes,CancellationTargets


@dataclass(frozen=True)
class PublicCancellationRows:
    received: torch.Tensor
    requests: object
    orders: object
    target_ordinals: object
    outcomes: torch.Tensor
    private_sequences: object
    batch_sequences: object
    record_ordinals: object
    delivery_sequences: object
    context_slots: torch.Tensor
    run_slots: torch.Tensor
    record_refs: torch.Tensor
    available_at: torch.Tensor
    input_item_refs: object
    platform_at: torch.Tensor
    received_at: torch.Tensor
    fault: torch.Tensor


def cancellation_rows_from_closed(bank,batches,closed,*,source_rows=None):
    roots,width=closed.record_refs.shape
    device=closed.record_refs.device
    source_rows=resolve_closed_public_rows(bank,batches,closed) if source_rows is None else source_rows
    rows,at,coordinates,active,fault=(source_rows.rows,source_rows.at,source_rows.coordinates,source_rows.active,source_rows.fault)
    schema=bank.payload_schemas[rows]
    target=active&(schema==15)
    complete=active&(schema==16)
    target_fields,error=decode_fields(bank.semantic[rows],['scalar','integer','scalar'],target)
    fault=fault|Numbers.combine_fault_rows(error.reshape(roots,width))
    identity,error=decode_fields(bank.logical_keys[rows],['scalar','scalar','scalar','integer'],target)
    fault=fault|Numbers.combine_fault_rows(error.reshape(roots,width))
    refs,error=decode_fields(bank.cancellation[rows],[('record_list',('scalar','scalar'))],target)
    fault=fault|Numbers.combine_fault_rows(error.reshape(roots,width))
    complete_identity,error=decode_fields(bank.logical_keys[rows],['scalar','scalar','scalar'],complete)
    fault=fault|Numbers.combine_fault_rows(error.reshape(roots,width))
    complete_fields,error=decode_fields(bank.semantic[rows],['scalar','integer','integer','integer'],complete)
    fault=fault|Numbers.combine_fault_rows(error.reshape(roots,width))
    requests=torch.cat([batch.request_slots for batch in batches],1).gather(1,at)
    invalid=target&((identity[1]!=4)|(identity[2]!=requests.flatten())|(target_fields[0]!=identity[0])|
        (target_fields[2]<0)|(target_fields[2]>2))
    invalid=invalid|(complete&((complete_identity[1]!=5)|(complete_identity[2]!=requests.flatten())|
        (complete_fields[0]!=complete_identity[0])))
    invalid=invalid|((target|complete)&(requests.flatten()<0))
    fault=fault|invalid.reshape(roots,width).any(1).to(torch.int32)
    request=ExactInt.from_i64_tensor(requests.flatten().clamp_min(0))
    private=_take(concatenate_integer([batch.private_sequences for batch in batches],roots),coordinates)
    batch=_take(concatenate_integer([batch.batch_sequences for batch in batches],roots),coordinates)
    ordinal=_take(concatenate_integer([batch.record_ordinals for batch in batches],roots),coordinates)
    counts=concatenate_integer(complete_fields[1:],roots)
    # concatenate_integer按字段拼列；转成每row的三个原outcome。
    field_coordinates=(torch.arange(roots,device=device)[:,None,None]*(width*3)+
        torch.arange(3,device=device)[None,None,:]*width+torch.arange(width,device=device)[None,:,None]).flatten()
    counts=_take(counts,field_coordinates)
    platform=torch.cat([item.platform_at for item in batches],1).gather(1,at)
    received=torch.cat([item.received_at for item in batches],1).gather(1,at)
    source=PublicCancellationRows(target.reshape(roots,width),request,target_fields[1],identity[3],
        target_fields[2].reshape(roots,width),private,batch,ordinal,closed.delivered_sequences,
        torch.cat([item.context_slots for item in batches],1).gather(1,at),
        torch.cat([item.run_slots for item in batches],1).gather(1,at),closed.record_refs,closed.available_at,refs[0],platform,received,fault)
    platform=torch.cat([item.platform_at for item in batches],1).gather(1,at)
    received=torch.cat([item.received_at for item in batches],1).gather(1,at)
    completions=CancellationCompletions(complete.reshape(roots,width),request,closed.delivered_sequences,ordinal,counts,
        closed.available_at,platform,received,fault,closed.record_refs,source.context_slots,source.run_slots,batch,private)
    return source,completions


def targets_from_rows(source,order_sequences,order_present):
    """原无损Order身份目录对照；目录存在不等于订单此刻位置已公开。"""
    roots,width=source.received.shape
    orders=order_present.size(1)
    same,error=identity_matches(source.orders,order_sequences,roots,width,orders)
    member=same&order_present[:,None,:]
    fault=source.fault|Numbers.combine_fault_rows(torch.where(source.received[:,:,None],error,0).reshape(roots,-1))
    fault=fault|((member.to(torch.int64).sum(2)!=1)&source.received).any(1).to(torch.int32)
    # 未能无损定位不能当不相交。物理目录须先增长重试，不能静默丢目标。
    scopes=CancellationScopes(torch.zeros_like(source.received),torch.full_like(source.record_refs,-1),member)
    return CancellationTargets(source.received,source.requests,scopes,source.outcomes,source.available_at,fault)


def append_targets(previous,incoming):
    """首次领取原顺序追加；原NOE存储仍拥有完整正文与源位置。"""
    from dataclasses import replace
    roots=previous.received.size(0)
    return CancellationTargets(torch.cat([previous.received,incoming.received],1),
        concatenate_integer([previous.requests,incoming.requests],roots),
        CancellationScopes(torch.cat([previous.scopes.market_kind,incoming.scopes.market_kind],1),
            torch.cat([previous.scopes.markets,incoming.scopes.markets],1),
            torch.cat([previous.scopes.order_members,incoming.scopes.order_members],1)),
        torch.cat([previous.outcomes,incoming.outcomes],1),torch.cat([previous.available_at,incoming.available_at],1),
        previous.fault|incoming.fault)


def receive_cancellations(state,targets,bank,batches,closed,order_sequences,order_present):
    """唯一NOE领取后先Registry接纳；Ledger等待不延迟这一接纳。

    closed来自实际双门唯一领取。Ledger重试不再次调用本函数；NOE bank和批位置
    是共享只读输入，不由Ledger派生读面或完成总数恢复目标人口。
    """
    rows,completions=cancellation_rows_from_closed(bank,batches,closed)
    incoming=targets_from_rows(rows,order_sequences,order_present)
    return state.receive(completions),append_targets(targets,incoming),rows


def cancellation_order_events(source,receive_rank):
    """原Control Canceled独立粘滞关闭，不等待另一条CallerCanceled lifecycle。"""
    from registry import PublicRegistryEvents,CANCEL_TARGET
    roots,width=source.received.shape
    if receive_rank.shape!=(roots,width) or receive_rank.dtype!=torch.int64 or receive_rank.device!=source.received.device:
        raise TypeError('首次取消领取顺序需要同设备int64[roots,records]')
    blank=torch.zeros_like(source.record_refs)
    absent=torch.zeros_like(source.received)
    zero=source.orders.constant(0)
    ratio=ExactRational.ratio(zero,zero.constant(1))
    return PublicRegistryEvents(source.received,torch.full_like(blank,CANCEL_TARGET),source.orders,zero,
        source.private_sequences,source.delivery_sequences,source.record_ordinals,source.batch_sequences,
        source.context_slots,source.run_slots,torch.full_like(blank,-1),torch.full_like(blank,-1),source.record_refs,
        source.available_at,receive_rank,ratio,ratio,ratio,blank,blank,absent,absent,
        source.received&(source.outcomes==0),absent,source.platform_at,source.received_at,source.fault)
