"""NOE已接纳记录的设备双时间门与原交付序；不承担来源归约或Ledger应用。

记录位置由所属NOE存储提供，record_refs仅是其物理引用，不改变业务身份。
源private序、source ordinal均完整u64精确值。初始化/目录事实走原独立入口。
"""
from dataclasses import dataclass, replace
import torch


def take(value, index):
    return type(value)(value.words[index],value.sign[index],value.fault[index])


def fault_rows(values, roots, width):
    bits=torch.arange(32,dtype=torch.int64,device=values.device)
    flags=(((values.reshape(roots,width,1).to(torch.int64)>>bits)&1)!=0).any(dim=1)
    return (flags.to(torch.int64)<<bits).sum(dim=1).to(torch.int32)


@dataclass(frozen=True)
class ClosedRecords:
    record_refs: torch.Tensor
    delivered_sequences: object
    available_at: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ReceiveGates:
    closed_receive_at: torch.Tensor
    public_visible_at: torch.Tensor
    has_closed_receive: torch.Tensor
    has_public_visible: torch.Tensor

    @classmethod
    def unset(cls,roots,*,device='cuda'):
        times=torch.zeros(roots,dtype=torch.int64,device=device)
        missing=torch.zeros(roots,dtype=torch.bool,device=device)
        return cls(times,times.clone(),missing,missing.clone())

    def set_receive(self,at,enabled):
        return replace(self,closed_receive_at=torch.where(enabled,at,self.closed_receive_at),
            has_closed_receive=self.has_closed_receive | enabled)

    def set_public(self,at,enabled):
        return replace(self,public_visible_at=torch.where(enabled,at,self.public_visible_at),
            has_public_visible=self.has_public_visible | enabled)


@dataclass(frozen=True)
class PendingRecords:
    active: torch.Tensor  # [roots,slots]
    received_at: torch.Tensor
    private_sequences: object
    source_ordinals: object
    record_refs: torch.Tensor
    delivered_through: object
    fault: torch.Tensor

    def append_batch(self, bank, batch):
        """追加已规范化批；保留原交付前缀，不重新开启领取器。

        宿主须在关闭receive时间之前接齐该时间的完整来源；读入不推进时间门。
        """
        from queues import ExactInt
        from transport import append_root_fields
        roots,width=self.active.shape
        if batch.rows.size(0)!=roots or batch.rows.device!=self.active.device:
            raise ValueError('NOE新批须同根同设备')
        incoming=type(self).from_columns(batch.emitted,batch.received_at,batch.private_sequences,
            ExactInt.from_i64_tensor(bank.source_order[batch.rows.flatten(),3]),batch.rows,self.delivered_through)
        extra=batch.rows.size(1)
        return replace(self,active=torch.cat([self.active,incoming.active],1),
            received_at=torch.cat([self.received_at,incoming.received_at],1),
            record_refs=torch.cat([self.record_refs,incoming.record_refs],1),
            private_sequences=append_root_fields(self.private_sequences,incoming.private_sequences,roots,width,extra),
            source_ordinals=append_root_fields(self.source_ordinals,incoming.source_ordinals,roots,width,extra),
            fault=self.fault | incoming.fault | batch.fault)

    @classmethod
    def from_columns(cls,active,received_at,private_sequences,source_ordinals,record_refs,delivered_through):
        roots,width=active.shape
        if active.dtype!=torch.bool or any(value.shape!=active.shape or value.dtype!=torch.int64
                or value.device!=active.device for value in [received_at,record_refs]):
            raise TypeError('NOE领取列需同设备bool/int64[root,slot]')
        if any(value.sign.numel()!=roots*width or value.words.device!=active.device
                for value in [private_sequences,source_ordinals]) or delivered_through.sign.numel()!=roots:
            raise ValueError('NOE来源序号与领取人口不符')
        fault=torch.zeros(roots,dtype=torch.int32,device=active.device)
        maximum=private_sequences.constant((1<<64)-1)
        for value in [private_sequences,source_ordinals]:
            upper=value.compare(maximum)
            error=value.fault | upper.fault | ((value.sign<0)|(upper.order>0)).to(torch.int32)
            fault=fault | fault_rows(torch.where(active.flatten(),error,torch.zeros_like(error)),roots,width)
        upper=delivered_through.compare(delivered_through.constant((1<<64)-1))
        fault=fault | delivered_through.fault | upper.fault | (
            (delivered_through.sign<0)|(upper.order>0)).to(torch.int32)
        return cls(active,received_at,private_sequences,source_ordinals,record_refs,delivered_through,fault)

    def take_closed(self,gates):
        roots,width=self.active.shape
        closed_receive_at,public_visible_at=gates.closed_receive_at,gates.public_visible_at
        for value in [closed_receive_at,public_visible_at]:
            if value.shape!=(roots,) or value.dtype!=torch.int64 or value.device!=self.active.device:
                raise TypeError('NOE时间门需同设备int64[roots]')
        for value in [gates.has_closed_receive,gates.has_public_visible]:
            if value.shape!=(roots,) or value.dtype!=torch.bool or value.device!=self.active.device:
                raise TypeError('NOE未设时间门须显式同设备bool[roots]')
        ready=self.active & (self.received_at<=closed_receive_at[:,None]) & (
            self.received_at<=public_visible_at[:,None])
        ready=ready & (gates.has_closed_receive & gates.has_public_visible)[:,None]
        order=torch.arange(width,device=self.active.device).expand(roots,width)
        def sort(key):
            nonlocal order
            order=order.gather(1,key.gather(1,order).argsort(dim=1,stable=True))
        # 原(received,private,source ordinal)；每个u64按两字无符号排序。
        for value in [self.source_ordinals,self.private_sequences]:
            for limb in [0,1]:
                key=(value.words[:,limb].to(torch.int64)&0xffffffff).reshape(roots,width) if value.words.size(1)>limb else torch.zeros_like(order)
                sort(key)
        sort(self.received_at)
        sort((~ready).to(torch.int64))
        coordinates=(torch.arange(roots,device=order.device)[:,None]*width+order).flatten()
        private=take(self.private_sequences,coordinates)
        sorted_ready=ready.gather(1,order)
        times=self.received_at.gather(1,order)
        boundary=torch.ones_like(sorted_ready)
        if width:
            equality=(private.words.reshape(roots,width,-1)[:,1:]==private.words.reshape(roots,width,-1)[:,:-1]).all(dim=2)
            boundary[:,1:]=(~equality)|(times[:,1:]!=times[:,:-1])
        boundary=boundary & sorted_ready
        groups=boundary.to(torch.int64).cumsum(dim=1)
        next_delivered=self.delivered_through+type(private).from_i64_tensor(boundary.to(torch.int64).sum(dim=1))
        upper=next_delivered.compare(next_delivered.constant((1<<64)-1))
        fault=self.fault | next_delivered.fault | upper.fault | (upper.order>0).to(torch.int32)
        emitted=sorted_ready & (fault==0)[:,None]
        delivery=take(self.delivered_through,torch.arange(roots,device=order.device).repeat_interleave(width)) + type(private).from_i64_tensor(groups.flatten())
        delivery=type(delivery)(delivery.words,delivery.sign,delivery.fault | fault.repeat_interleave(width))
        consumed=torch.zeros_like(self.active).scatter(1,order,emitted)
        remaining=replace(self,active=self.active & ~consumed,
            delivered_through=next_delivered.select(fault==0,self.delivered_through),fault=fault)
        return remaining,ClosedRecords(self.record_refs.gather(1,order),delivery,
            public_visible_at[:,None].expand(roots,width),emitted,fault)
