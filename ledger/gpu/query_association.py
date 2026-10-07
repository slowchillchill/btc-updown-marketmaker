"""原本地Query→reservation关联及事件幂等；不发送Query或迁移资金。

query/event/request是实际完整字符串的冷字典槽，目录资源不足保原前缀。
每个本地事件只存一次；同Query后继事件沿真实Ledger序替换其绑定。
唯一登记入口分配current_entry_sequence+1，首次空槽追加；没有乱序冷安装入口。
增长只尾补空槽，snapshot保原序。因此最后匹配槽就是当前Query绑定。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,_take
from reservation import grow_integer,sequence_fault


@dataclass(frozen=True)
class QueryAssociationDecision:
    ledger_sequences: object
    changed: torch.Tensor
    enough: torch.Tensor
    required_capacity: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class QueryAssociations:
    events: torch.Tensor
    queries: torch.Tensor
    requests: torch.Tensor
    ledger_sequences: object
    capacity_required: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls,roots,capacity,device):
        if roots<1 or capacity<1:raise ValueError('本地关联目录物理人口须正数')
        absent=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        seq=ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        return cls(absent,absent.clone(),absent.clone(),seq,
            torch.full((roots,),capacity,dtype=torch.int64,device=device),
            torch.zeros(roots,dtype=torch.int32,device=device))

    def resized(self,capacity):
        roots,old=self.events.shape
        if capacity<old:raise ValueError('不能缩小未退役的Query本地事件目录')
        pad=lambda v:torch.nn.functional.pad(v,(0,capacity-old),value=-1)
        return replace(self,events=pad(self.events),queries=pad(self.queries),requests=pad(self.requests),
            ledger_sequences=grow_integer(self.ledger_sequences,roots,old,capacity),
            capacity_required=torch.full_like(self.capacity_required,capacity))

    def lookup(self,query):
        roots,width=self.events.shape
        if query.shape!=(roots,) or query.dtype!=torch.int64 or query.device!=self.events.device:
            raise TypeError('Query请求需同设备int64[roots]实际身份槽')
        member=(self.queries==query[:,None])&(self.events>=0)&(query[:,None]>=0)
        at=torch.where(member,torch.arange(width,device=query.device)[None,:],0).max(1).values
        return self.requests.gather(1,at[:,None]).flatten(),member.any(1)

    def register(self,event,query,request,sequence,enabled):
        roots,width=self.events.shape
        for value in (event,query,request):
            if value.shape!=(roots,) or value.dtype!=torch.int64 or value.device!=self.events.device:
                raise TypeError('本地Query关联需同设备int64[roots]完整身份槽')
        if enabled.shape!=(roots,) or enabled.dtype!=torch.bool or enabled.device!=self.events.device:
            raise TypeError('本地Query许可需同设备bool[roots]')
        same=(self.events==event[:,None])&(self.events>=0)
        found=same.any(1)
        at=torch.where(found,same.to(torch.int64).argmax(1),(self.events<0).to(torch.int64).argmax(1))
        required=torch.where(enabled&~found&(self.events>=0).all(1),width+1,width)
        fault=self.fault|torch.where(enabled&~found,((event<0)|(query<0)|(request<0)).to(torch.int32),0)
        fault|=torch.where(enabled&~found,sequence_fault(sequence,positive=True),0)
        enough=required<=width
        use=enabled&~found&enough&(fault==0)
        mask=torch.zeros_like(same).scatter(1,at[:,None],use[:,None])
        root=torch.arange(roots,device=event.device)
        prior=_take(self.ledger_sequences,root*width+at)
        returned=sequence.select(use,prior.select(enabled&found,prior.constant(0)))
        expanded=_take(sequence,root.repeat_interleave(width))
        candidate=replace(self,events=torch.where(mask,event[:,None],self.events),
            queries=torch.where(mask,query[:,None],self.queries),requests=torch.where(mask,request[:,None],self.requests),
            ledger_sequences=expanded.select(mask.flatten(),self.ledger_sequences),capacity_required=required,fault=fault)
        return candidate,QueryAssociationDecision(returned,use,enough,required,fault)
