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


@dataclass(frozen=True)
class CancellationTargetHistory:
    """首次领取的完整目标历史及同轴身份目录；不借当前可撤订单人口。

    open只用于调用者已从真实开户/检查点证明完整空历史的入口。None表示
    没有安装历史。目录保持终态、被请求或被原目标引用的订单身份。
    """
    order_sequences: object
    order_present: torch.Tensor
    targets: object

    @classmethod
    def open(cls,sequences,present):
        roots,count=present.shape;device=present.device
        if (present.dtype!=torch.bool or sequences.sign.numel()!=present.numel()
                or sequences.words.device!=device):
            raise TypeError('取消目标目录需要同设备完整身份轴')
        blank=present[:,:0];columns=torch.empty((roots,0),dtype=torch.int64,device=device)
        zero=ExactInt.from_i64_tensor(columns.flatten())
        scopes=CancellationScopes(blank,columns,torch.empty((roots,0,count),dtype=torch.bool,device=device))
        targets=CancellationTargets(blank,zero,scopes,columns,columns,
            torch.zeros(roots,dtype=torch.int32,device=device))
        return cls(sequences,present,targets)

    def receive(self,rows):
        # 与唯一receive共同返回候选；Ledger等待/重试不调用本方法。
        from dataclasses import replace
        incoming=targets_from_rows(rows,self.order_sequences,self.order_present)
        roots,source_width=incoming.received.shape
        # 现有首次领取的冷人口装箱边界：只追加自然批的真实目标，不把全部NOE
        # 物理width/空领取padding复制为目标历史。后继固定容量宿主仍未交付。
        width=int(incoming.received.to(torch.int64).sum(1).max().cpu())
        if not width:
            return replace(self,targets=replace(self.targets,fault=self.targets.fault|incoming.fault))
        columns=(~incoming.received).long().argsort(dim=1,stable=True)[:,:width]
        coordinates=(torch.arange(roots,device=columns.device)[:,None]*source_width+columns).flatten()
        select=lambda value:value.gather(1,columns)
        scopes=CancellationScopes(select(incoming.scopes.market_kind),select(incoming.scopes.markets),
            incoming.scopes.order_members.gather(1,columns[:,:,None].expand(-1,-1,self.order_present.shape[1])))
        packed=CancellationTargets(select(incoming.received),_take(incoming.requests,coordinates),scopes,
            select(incoming.outcomes),select(incoming.available_at),incoming.fault)
        return replace(self,targets=append_targets(self.targets,packed))

    def with_orders(self,sequences,present):
        """冷目录增长/重排；保原目标成员，旧候选和旧冻结不随新轴覆盖。"""
        from dataclasses import replace
        roots,old_count=self.order_present.shape;new_count=present.shape[1]
        same,error=identity_matches(self.order_sequences,sequences,roots,old_count,new_count)
        same=same&self.order_present[:,:,None]&present[:,None,:]
        members=(self.targets.scopes.order_members[:,:,:,None]&same[:,None,:,:]).any(2)
        referenced=self.targets.scopes.order_members.any(1)
        missing=referenced&(same.to(torch.int64).sum(2)!=1)
        fault=self.targets.fault|missing.any(1).to(torch.int32)
        fault|=Numbers.combine_fault_rows(torch.where(referenced[:,:,None],error,0).flatten(1))
        targets=replace(self.targets,scopes=replace(self.targets.scopes,order_members=members),fault=fault)
        return type(self)(sequences,present,targets)


def frozen_cancellation_outcomes(bank,selected):
    """只解当次FrozenOrderSources已选latest_cancel正文，不再选一次latest。

    None保持没有反馈；原Canceled/NotCanceled/EvidenceUnknown三态独立。
    bank所指不可变块须仍在消费生命周期内。
    """
    roots,count=selected.present.shape
    result=torch.zeros_like(selected.record_refs)
    fault=torch.zeros(roots,dtype=torch.int32,device=result.device)
    width=bank.semantic.shape[0]
    if width:
        refs=selected.record_refs.flatten()
        present=selected.present.flatten()
        valid=(refs>=0)&(refs<width)
        safe=refs.clamp(0,width-1)
        active=present&valid
        fields,error=decode_fields(bank.semantic[safe],['scalar','integer','scalar'],active)
        result=fields[2].reshape(roots,count)
        invalid=present&(~valid|(bank.payload_schemas[safe]!=15)|(fields[2]<0)|(fields[2]>2))
        fault=Numbers.combine_fault_rows((error|invalid.to(torch.int32)).reshape(roots,count))
    else:
        fault=selected.present.any(1).to(torch.int32)
    return result,selected.present,fault


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
