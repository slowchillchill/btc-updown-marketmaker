"""原SIM成交记录增长池及每根下一最终性义务的设备选择。

槽位按原fill sequence追加，终态记录留存至后续持久/NOE回收协调；不复用身份。
每次仅消费每根原(time,fill_seq,ordinal)最先的义务，完整毫秒顺序归外层宿主。
"""
from dataclasses import dataclass, fields, replace
import torch
from queues import Numbers, ExactInt, _take, _repeat
from fill_finality import FillRecord


_BANK = ('offsets','states','lengths','economic_ordinals')


def _take_record(record,index):
    result = {}
    for field in fields(record):
        value = getattr(record,field.name)
        result[field.name] = value if field.name in _BANK else _take(value,index) if isinstance(value,ExactInt) else value[index]
    return FillRecord(**result)


def _put_integer(original,incoming,index,enabled):
    old = _take(original,index)
    chosen = incoming.select(enabled,old)
    width = max(original.words.shape[1],chosen.words.shape[1])
    original,chosen = original.with_capacity(width),chosen.with_capacity(width)
    return ExactInt(original.words.index_copy(0,index,chosen.words),
        original.sign.index_copy(0,index,chosen.sign),original.fault.index_copy(0,index,chosen.fault))


def _put_record(original,incoming,index,enabled):
    result = {}
    for field in fields(original):
        value,new = getattr(original,field.name),getattr(incoming,field.name)
        if field.name in _BANK:
            if value is not new:
                raise ValueError('成交池只能使用同一不可变密封路径bank')
            result[field.name] = value
        elif isinstance(value,ExactInt):
            result[field.name] = _put_integer(value,new,index,enabled)
        else:
            result[field.name] = value.index_copy(0,index,torch.where(enabled,new,value[index]))
    return FillRecord(**result)


@dataclass(frozen=True)
class FillPoolEffects:
    slots: torch.Tensor
    sequence: object
    emitted: torch.Tensor
    required_records: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class FillPoolTransition:
    fill_sequence: object  # 原SIM序号；不是物理槽位或private feedback seq。
    slot: torch.Tensor
    event: object


@dataclass(frozen=True)
class FillPool:
    records: object  # FillRecord的逐成交字段[roots*capacity]，bank全池共享。
    occupied: torch.Tensor  # [roots,capacity]，完成并不等于可以丢掉记录。
    sequences: object
    next_sequence: object
    fault: torch.Tensor

    @property
    def roots(self):
        return self.occupied.shape[0]

    @property
    def capacity(self):
        return self.occupied.shape[1]

    @classmethod
    def empty(cls,template, *, capacity=4):
        if capacity < 1:
            raise ValueError('成交池物理容量必须正数')
        roots = template.base.sign.numel()
        values = {}
        for field in fields(template):
            value = getattr(template,field.name)
            values[field.name] = value if field.name in _BANK else _repeat(value.constant(0),capacity) if isinstance(value,ExactInt) else torch.zeros((roots*capacity,),device=value.device,dtype=value.dtype)
        values['ordinal'] = torch.full((roots*capacity,),-1,device=template.ordinal.device,dtype=torch.int64)
        zero = template.base.constant(0)
        return cls(FillRecord(**values),torch.zeros((roots,capacity),dtype=torch.bool,device=template.ordinal.device),
            _repeat(zero,capacity),zero.constant(1),zero.fault)

    def resized(self,capacity):
        if capacity <= self.capacity:
            return self
        coordinates = torch.arange(self.roots,device=self.occupied.device)[:,None]*self.capacity
        old = torch.arange(self.capacity,device=self.occupied.device).expand(self.roots,-1)
        # 新槽从本根原空槽的形状构造，随后逐字段清零；不复制别根业务值。
        safe = (coordinates+old).flatten()
        result = FillPool.empty(_take_record(self.records,coordinates.flatten()),capacity=capacity)
        target = (torch.arange(self.roots,device=self.occupied.device)[:,None]*capacity+old).flatten()
        enabled = torch.ones_like(target,dtype=torch.bool)
        return replace(result,records=_put_record(result.records,_take_record(self.records,safe),target,enabled),
            occupied=torch.nn.functional.pad(self.occupied,(0,capacity-self.capacity)),
            sequences=_put_integer(result.sequences,self.sequences,target,enabled),
            next_sequence=self.next_sequence,fault=self.fault)

    def new_record(self,base,quote,fee,asset_indices,matched_at,active,selected_path):
        """复用原bank创建新成交；精确量与时刻仍经原记录边界校验。"""
        # create只冷验证bank一次；新动态量在设备检查，不重建/读取路径正文。
        if any(value.shape != (self.roots,) for value in (asset_indices,matched_at,active,selected_path)):
            raise ValueError('新成交根布局不一致')
        if any(value.dtype != torch.int64 for value in (asset_indices,matched_at,selected_path)) or active.dtype != torch.bool:
            raise TypeError('新成交坐标/时间须int64，active须bool')
        if any(value.device != self.occupied.device for value in (asset_indices,matched_at,active,selected_path)):
            raise ValueError('新成交字段必须同设备')
        path = selected_path.clamp(0,self.records.lengths.numel()-1)
        end = self.records.offsets[path,self.records.lengths[path]-1]
        error = base.fault | quote.fault | fee.fault
        error = error | ((base.sign <= 0) | (quote.sign <= 0) | (fee.sign < 0) |
            (selected_path < 0) | (selected_path >= self.records.lengths.numel()) |
            (matched_at > (1<<63)-1-end)).to(torch.int32)
        return FillRecord(base,quote,fee,asset_indices,matched_at,active,
            torch.full_like(matched_at,-1),torch.zeros_like(active),base.constant(0),selected_path.clone(),
            self.records.offsets,self.records.states,self.records.lengths,self.records.economic_ordinals,
            error*active.to(torch.int32))

    def enqueue(self,record):
        """容量不足仅请求增长，不提交fill seq或选定路径；纯候选retry读原输入。"""
        count = self.occupied.to(torch.int64).sum(1)
        demand = count+record.active.to(torch.int64)
        slot = count.clamp_max(self.capacity-1)
        next_sequence = self.next_sequence+self.next_sequence.constant(1)
        upper = next_sequence.compare(next_sequence.constant((1<<64)-1))
        fault = self.fault | (record.fault | upper.fault | (upper.order > 0).to(torch.int32))*record.active.to(torch.int32)
        enabled = record.active & (count < self.capacity) & (fault == 0)
        coordinates = torch.arange(self.roots,device=slot.device)*self.capacity+slot
        records = _put_record(self.records,record,coordinates,enabled)
        sequences = _put_integer(self.sequences,self.next_sequence,coordinates,enabled)
        occupied = self.occupied.flatten().index_copy(0,coordinates,self.occupied.flatten()[coordinates] | enabled).reshape_as(self.occupied)
        return replace(self,records=records,sequences=sequences,occupied=occupied,
            next_sequence=next_sequence.select(enabled,self.next_sequence),fault=fault),FillPoolEffects(slot,self.next_sequence,enabled,demand,fault)

    def next_obligation(self):
        path = self.records.selected_path.clamp(0,self.records.lengths.numel()-1)
        ordinal = self.records.ordinal+1
        pending = self.occupied.flatten() & self.records.active & (ordinal < self.records.lengths[path])
        safe = torch.minimum(ordinal.clamp_min(0),self.records.lengths[path]-1)
        times = self.records.matched_at+self.records.offsets[path,safe]
        sentinel = torch.full_like(times,(1<<63)-1)
        masked = torch.where(pending,times,sentinel).reshape_as(self.occupied)
        # 槽按真实fill seq增长且不复用；min并列首槽保留原(time,fill_seq,ordinal)序。
        at = masked.min(1).values
        valid = pending.reshape_as(self.occupied)
        # 真义务可合法位于INT64_MAX；不能让同sentinel的已完成槽抢先。
        earliest = valid & (masked == at[:,None])
        slot = earliest.to(torch.int64).argmax(1)
        exists = valid.any(1)
        return at,slot,exists

    def advance_one(self,account,at,taker_rebate_rate):
        scheduled,slot,exists = self.next_obligation()
        coordinates = torch.arange(self.roots,device=slot.device)*self.capacity+slot
        current = _take_record(self.records,coordinates)
        current = replace(current,active=current.active & exists,
            fault=current.fault | self.fault | ((at > scheduled) & exists).to(torch.int32))
        current,account,transition = current.advance_one(account,at,taker_rebate_rate)
        records = _put_record(self.records,current,coordinates,transition.emitted)
        # 故障仍保留为根状态，不能把过期义务当成已完成或业务失败。
        result = FillPoolTransition(_take(self.sequences,coordinates),slot,transition)
        return replace(self,records=records,fault=self.fault | transition.fault),account,result


def run_fill_pool_batch(pool,compute):
    """同queue/精确字协调；compute返回末项FillPoolEffects，不推进外部状态或RNG。"""
    while True:
        result = Numbers.run_exact_batch(lambda:compute(pool))
        required = result[-1].required_records
        demand = int(required.max().cpu()) if required.numel() else 0
        if demand <= pool.capacity:
            return result
        pool = pool.resized(1 << (demand-1).bit_length())
