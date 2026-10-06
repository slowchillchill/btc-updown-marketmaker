"""共享NOE字段到原订单/成交生命周期的设备读取与增长目录。

共享字段只在块接入时解码一次。业务序号保持完整精确整数，目录槽位不替代身份；
贡献/成功成本/结算经济归属由Ledger经济层接线，不由此处生成余额。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,Numbers,_take
from fields import decode_fields
from core import OUT_EVIDENCE,OUT_FACT,OUT_PENDING
from reservation import grow_integer


@dataclass(frozen=True)
class LifecycleColumns:
    schemas: torch.Tensor
    normalization_outcomes: torch.Tensor
    orders: object
    fills: object
    conditions: torch.Tensor
    open_orders: torch.Tensor
    pending_trades: torch.Tensor
    accepted: torch.Tensor
    economic_tags: torch.Tensor
    request_slots: torch.Tensor
    item_ordinals: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def from_projection(cls,bank,columns):
        roots,width=columns.row_refs.shape
        rows=columns.row_refs.flatten()
        keys=bank.logical_keys[rows]
        schemas=keys[:,4]
        active=torch.cat([batch.emitted for batch in columns.projection_batches],dim=1).flatten()
        outcomes=torch.cat([batch.normalized.outcomes for batch in columns.projection_batches],dim=1).flatten()
        material=active & ((outcomes==OUT_EVIDENCE)|(outcomes==OUT_FACT))
        enabled={tag:material&(schemas==tag) for tag in [3,7,8,9,11]}
        layouts={
            3:['scalar']*4+['integer','scalar','integer','integer','scalar'],
            7:['scalar']*3+['integer','scalar','scalar'],
            8:['scalar']*3+['integer','integer']+['scalar']*4+['rational']+['integer']*3+['scalar','integer','scalar','rational'],
            9:['scalar','scalar','integer','scalar','scalar','rational'],
            11:['scalar']*4+['integer','scalar']+['rational']*8+['scalar','scalar']+['rational']*3,
        }
        decoded={}
        fault=torch.where(material,bank.fault[rows],0)
        known=torch.zeros_like(active)
        for tag,layout in layouts.items():
            values,error=decode_fields(bank.semantic[rows],layout,enabled[tag])
            decoded[tag]=values
            fault=fault | error
            known=known | (schemas==tag)
        fault=fault | (material & ~known).to(torch.int32)
        zero=ExactInt.from_i64_tensor(torch.zeros_like(schemas))
        orders=decoded[3][4].select(enabled[3],zero)
        orders=decoded[7][3].select(enabled[7],orders)
        orders=decoded[8][4].select(enabled[8],orders)
        economic=enabled[11]
        economic_order=economic & ((decoded[11][3]==0)|(decoded[11][3]==2))
        orders=decoded[11][4].select(economic_order,orders)
        fills=decoded[8][3].select(enabled[8],zero)
        fills=decoded[9][2].select(enabled[9],fills)
        economic_fill=economic & (decoded[11][3]>=6)&(decoded[11][3]<=10)
        fills=decoded[11][4].select(economic_fill,fills)
        conditions=torch.zeros_like(schemas)
        for tag in [3,7,8,9,11]:
            condition_at=2 if tag==3 else 1
            conditions=torch.where(enabled[tag],decoded[tag][condition_at],conditions)
        open_orders=(enabled[3]&(decoded[3][5]==0))|(enabled[7]&(decoded[7][5]==0))
        pending=enabled[8]&((decoded[8][6]==0)|(decoded[8][6]==1))
        accepted=enabled[3]&(decoded[3][1]==0)
        request=torch.cat([batch.request_slots for batch in columns.projection_batches],dim=1)
        item=torch.cat([batch.item_ordinals for batch in columns.projection_batches],dim=1)
        return cls(schemas.reshape(roots,width),outcomes.reshape(roots,width),orders,fills,conditions.reshape(roots,width),
            open_orders.reshape(roots,width),pending.reshape(roots,width),accepted.reshape(roots,width),
            torch.where(economic,decoded[11][3],-1).reshape(roots,width),request,item,active.reshape(roots,width),
            torch.where(active,fault,0).reshape(roots,width))


@dataclass(frozen=True)
class LifecycleDirectory:
    keys: object
    occupied: torch.Tensor
    conditions: torch.Tensor
    requests: torch.Tensor
    economic_applied: torch.Tensor
    accepted_principal_counted: torch.Tensor
    flags: torch.Tensor  # 订单open或成交pending。
    last_sequences: object

    @classmethod
    def empty(cls,roots,capacity,*,device='cuda'):
        coordinates=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        empty=torch.zeros_like(coordinates,dtype=torch.bool)
        zero=ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        return cls(zero,empty,coordinates,coordinates.clone(),empty.clone(),empty.clone(),empty.clone(),zero)

    def resized(self,capacity):
        roots,old=self.occupied.shape
        if capacity<old:
            raise ValueError('不能缩小未退役的生命周期身份目录')
        def pad(value,fill):
            return torch.nn.functional.pad(value,(0,capacity-old),value=fill)
        return replace(self,keys=grow_integer(self.keys,roots,old,capacity),
            occupied=pad(self.occupied,False),conditions=pad(self.conditions,-1),requests=pad(self.requests,-1),
            economic_applied=pad(self.economic_applied,False),accepted_principal_counted=pad(self.accepted_principal_counted,False),
            flags=pad(self.flags,False),last_sequences=grow_integer(self.last_sequences,roots,old,capacity))

    def lookup(self,key):
        roots,width=self.occupied.shape
        expanded=_take(key,torch.arange(roots,device=self.occupied.device).repeat_interleave(width))
        same=self.keys.compare(expanded)
        matches=(same.order.reshape(roots,width)==0)&self.occupied
        found=matches.any(dim=1)
        at=matches.to(torch.int32).argmax(dim=1)
        vacancy=(~self.occupied).to(torch.int32).argmax(dim=1)
        return torch.where(found,at,vacancy),found,same.fault.reshape(roots,width).bitwise_or(self.keys.fault.reshape(roots,width))

    def set(self,key,condition,flag,ledger_sequence,enabled,*,request=None,economic=None,counted=None,terminal_sticky=False):
        roots,width=self.occupied.shape
        at,found,error=self.lookup(key)
        fault=Numbers.combine_fault_rows(error) | (enabled & ~found & self.occupied.all(dim=1)).to(torch.int32)
        expanded=_take(key,torch.arange(roots,device=at.device).repeat_interleave(width))
        sequence=_take(ledger_sequence,torch.arange(roots,device=at.device).repeat_interleave(width))
        old=self.flags.gather(1,at[:,None]).flatten()
        if terminal_sticky:
            flag=flag & (~found | old)
        use=enabled&(fault==0)
        mask=torch.zeros_like(self.occupied).scatter(1,at[:,None],use[:,None])
        return replace(self,keys=expanded.select(mask.flatten(),self.keys),occupied=self.occupied | mask,
            conditions=torch.where(mask,condition[:,None],self.conditions),flags=torch.where(mask,flag[:,None],self.flags),
            last_sequences=sequence.select(mask.flatten(),self.last_sequences),
            requests=self.requests if request is None else torch.where(mask,request[:,None],self.requests),
            economic_applied=self.economic_applied if economic is None else torch.where(mask,economic[:,None],self.economic_applied),
            accepted_principal_counted=self.accepted_principal_counted if counted is None else torch.where(mask,counted[:,None],self.accepted_principal_counted)),fault
