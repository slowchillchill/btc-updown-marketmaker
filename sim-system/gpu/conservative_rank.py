"""公开成交模型的数量排位；实际可执行深度仍归原PriceQueueBatch的B/Q/c。"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, Numbers, _take, _put, _repeat, _columns, _minimum


@dataclass(frozen=True)
class ConservativeRank:
    kind: torch.Tensor
    order_ids: torch.Tensor
    remaining: object

    @classmethod
    def empty(cls, queue):
        return cls(torch.zeros_like(queue.kind),torch.full_like(queue.order_ids,-1),
            queue.total_base.constant(0))

    @property
    def rows(self):return self.kind.size(0)

    @property
    def capacity(self):return self.kind.size(1)

    def at(self,value,column):
        return _take(value,torch.arange(self.rows,device=self.kind.device)*self.capacity+column)

    def compact(self):
        live=(self.kind!=0)&(self.remaining.sign.reshape_as(self.kind)>0)
        permutation=(~live).to(torch.int8).argsort(dim=1,stable=True)
        index=(torch.arange(self.rows,device=self.kind.device)[:,None]*self.capacity+permutation).flatten()
        keep=live.gather(1,permutation)
        return replace(self,kind=torch.where(keep,self.kind.gather(1,permutation),0),
            order_ids=torch.where(keep,self.order_ids.gather(1,permutation),-1),remaining=_take(self.remaining,index))

    def resized(self,capacity):
        padding=capacity-self.capacity
        if not padding:return self
        zero=self.at(self.remaining,0).constant(0)
        return replace(self,kind=torch.nn.functional.pad(self.kind,(0,padding)),
            order_ids=torch.nn.functional.pad(self.order_ids,(0,padding),value=-1),
            remaining=_columns([self.at(self.remaining,c) for c in range(self.capacity)]+[zero]*padding))

    def append(self,quantity,order_ids=None):
        packed=self.compact();count=(packed.kind!=0).sum(1);positive=quantity.sign>0
        last=(count-1).clamp_min(0)
        merge=positive&(count>0)&(packed.kind.gather(1,last[:,None])[:,0]==1) if order_ids is None else torch.zeros_like(positive)
        target=torch.where(merge,last,count)
        selected=(torch.arange(self.capacity,device=self.kind.device)[None,:]==target[:,None])&positive[:,None]
        previous=_take(packed.remaining,torch.arange(self.rows,device=self.kind.device)*self.capacity+target.clamp_max(self.capacity-1))
        amount=(previous+quantity).select(merge,quantity)
        remaining=_repeat(amount,self.capacity).select(selected.flatten(),packed.remaining)
        kind=1 if order_ids is None else 2
        ids=torch.full_like(packed.order_ids,-1) if order_ids is None else order_ids[:,None].expand_as(packed.order_ids)
        return replace(packed,kind=torch.where(selected,kind,packed.kind),
            order_ids=torch.where(selected,ids,packed.order_ids),remaining=remaining),count+(positive&~merge).to(torch.int64)

    def clear(self,enabled):
        return replace(self,kind=torch.where(enabled[:,None],0,self.kind),
            order_ids=torch.where(enabled[:,None],-1,self.order_ids),
            remaining=self.remaining.constant(0).select(enabled.repeat_interleave(self.capacity),self.remaining))

    def cancel(self,order_ids):
        selected=(self.kind==2)&(self.order_ids==order_ids[:,None]);zero=self.at(self.remaining,0).constant(0)
        cancelled=zero
        for c in range(self.capacity):cancelled=cancelled+self.at(self.remaining,c).select(selected[:,c],zero)
        packed=replace(self,kind=torch.where(selected,0,self.kind),
            remaining=self.remaining.constant(0).select(selected.flatten(),self.remaining)).compact()
        # 唯一被撤账户段两边的External合并；原External不含可计价的B/Q/c。
        if self.capacity>1:
            pair=(packed.kind[:,:-1]==1)&(packed.kind[:,1:]==1)
            left=pair.to(torch.int64).argmax(1);merge=pair.any(1)
            axis=torch.arange(self.rows,device=self.kind.device)*self.capacity
            a,b=axis+left,axis+left+1
            amount=_take(packed.remaining,a)+_take(packed.remaining,b)
            remaining=_put(packed.remaining,a,amount.select(merge,_take(packed.remaining,a)))
            remaining=_put(remaining,b,zero.select(merge,_take(remaining,b)))
            remove=(torch.arange(self.capacity,device=self.kind.device)[None,:]==(left+1)[:,None])&merge[:,None]
            packed=replace(packed,kind=torch.where(remove,0,packed.kind),remaining=remaining).compact()
        packed=packed.clear(~(packed.kind==2).any(1))
        return packed,cancelled,selected.any(1)

    def prepare_take(self,budget):
        zero=budget.constant(0);flat_zero=_repeat(zero,self.capacity)
        available=self.remaining.select((self.kind!=0).flatten(),flat_zero)
        positions=torch.arange(self.capacity,device=self.kind.device)[None,:]
        limit=torch.where(self.kind!=0,positions+1,0).max()
        return available,flat_zero,limit,((available.sign>=0)&(available.fault==0)).all()

    def finish_take(self,taken,left,flat_zero):
        maker=taken.select((self.kind==2).flatten(),flat_zero)
        candidate=replace(self,remaining=self.remaining-taken).compact()
        candidate=candidate.clear(~(candidate.kind==2).any(1))
        return candidate,left,self.order_ids,maker

    def take(self,budget,*,fifo_loop=None):
        available,flat_zero,limit,safe_suffix=self.prepare_take(budget)
        if fifo_loop is not None and not Numbers.device_batch_active():
            taken,left=fifo_loop.take(available,budget,limit,safe_suffix,rows=self.rows,capacity=self.capacity)
        else:
            left=budget;takes=[]
            for c in range(self.capacity):
                take=_minimum(self.at(available,c),left);takes.append(take);left=left-take
            taken=_columns(takes)
        return self.finish_take(taken,left,flat_zero)

    def coordinate(self,original,enabled):
        return replace(self,kind=torch.where(enabled[:,None],self.kind,original.kind),
            order_ids=torch.where(enabled[:,None],self.order_ids,original.order_ids),
            remaining=self.remaining.select(enabled.repeat_interleave(self.capacity),original.remaining))
