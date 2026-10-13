"""SIM真实正费用成交的Taker日桶、成熟屏障和唯一日结/付款候选。

成交身份使用完整u64 ExactInt；物理目录容量只限制本次候选，不改变业务合法域。
注册来自已接纳成交，终态来自已消费密封路径，金额来自实际返佣经济delta。
日钟与hidden/其它义务的先后顺序仍由SIM宿主协调，读取下一时刻不推进状态。
"""
from dataclasses import dataclass,replace,fields
import torch
from queues import Numbers,ExactInt,ExactRational,_take
from asset_buy import take_ratio
from hidden_market import rational_select,zeros
from fill_pool import _put_integer


DAY=86_400_000
MATURITY=87_000_000
MIN_DAY=-((1<<63)//DAY)*DAY
MAX_MATCHED=(((1<<63)-1-MATURITY)//DAY)*DAY+DAY-1


def _ratio(value):
    return ExactRational.ratio(value,value.constant(1))


def _store_ratio(old,new,index,enabled):
    return ExactRational(_put_integer(old.numerator,new.numerator,index,enabled),
        _put_integer(old.denominator,new.denominator,index,enabled))


def _grow(value,roots,old,new,denominator=False):
    zero=ExactInt.from_i64_tensor(torch.zeros(roots*new,dtype=torch.int64,device=value.words.device))
    if denominator:
        zero=zero.constant(1)
    index=(torch.arange(roots,device=value.words.device)[:,None]*new+
        torch.arange(old,device=value.words.device)[None,:]).flatten()
    return _put_integer(zero,value,index,torch.ones_like(index,dtype=torch.bool))


@dataclass(frozen=True)
class TakerDayDemand:
    committed: torch.Tensor
    fill_capacity: torch.Tensor
    day_capacity: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class TakerDayResult:
    day: torch.Tensor
    accrued: object
    payable: object
    locked: object
    contribution_refs: object
    reference_mask: torch.Tensor
    account_delta: object
    emitted: torch.Tensor
    fault: torch.Tensor


def coordinate_calendar(candidate, original, committed, fault):
    if candidate is original:
        return replace(original, fault=fault)
    roots = committed.numel()
    updates = {}
    for field in fields(candidate):
        new, old = getattr(candidate, field.name), getattr(original, field.name)
        if isinstance(new, ExactInt):
            updates[field.name] = new.select(committed.repeat_interleave(new.sign.numel() // roots), old)
        elif isinstance(new, ExactRational):
            mask = committed.repeat_interleave(new.numerator.sign.numel() // roots)
            updates[field.name] = ExactRational(new.numerator.select(mask, old.numerator), new.denominator.select(mask, old.denominator))
        else:
            mask = committed.reshape((roots,) + (1,) * (new.ndim - 1))
            updates[field.name] = torch.where(mask, new, old)
    updates['fault'] = fault
    return replace(candidate, **updates)


class RebateCalendarOperations:
    """两类返佣共用的日成熟、源金额日结与全额付款；前驱由各生产者登记。"""
    maturity_ms = MATURITY
    maker = False

    def next_clock(self):
        valid=self.occupied & ~self.mature
        times=self.days+self.maturity_ms
        sentinel=torch.full_like(times,(1<<63)-1)
        at=torch.where(valid,times,sentinel).min(1).values
        return at,valid.any(1)

    def advance_clock(self,at,enabled):
        if at.shape!=self.fault.shape or at.dtype!=torch.int64 or at.device!=self.days.device:
            raise TypeError('日钟须同设备int64[roots]')
        next_at,exists=self.next_clock()
        fault=self.fault | ((at>next_at) & exists & enabled).to(torch.int32)
        use=enabled & exists & (at==next_at) & (fault==0)
        mask=self.occupied & ~self.mature & (self.days+self.maturity_ms==at[:,None]) & use[:,None]
        return replace(self,mature=self.mature | mask,fault=fault),use

    def finalize_one(self,account,enabled):
        roots,capacity=self.occupied.shape
        root=torch.arange(roots,device=self.days.device)
        ready=self.occupied & self.mature & (self.pending==0) & ~self.finalized
        values=torch.where(ready,self.days,torch.full_like(self.days,(1<<63)-1))
        di=values.argmin(1)
        requested=enabled & ready.any(1) & (self.fault==0)
        index=root*capacity+di
        accrued=take_ratio(self.accrued,index)
        one=accrued.numerator.constant(1)
        enough=accrued.compare(_ratio(one))
        scaled=accrued.numerator*one.constant(10000)
        units,_=scaled.divrem(accrued.denominator)
        payable=ExactRational.ratio(units,one.constant(10000))
        payable=rational_select(payable,enough.order>=0,zeros(payable))
        locked=rational_select(accrued-payable,payable.numerator.sign>0,zeros(payable))
        fault=self.fault | torch.where(requested,accrued.fault | enough.fault | payable.fault | locked.fault,0)
        account,delta=account.finalize_rebate_day(locked,torch.full_like(enabled,self.maker),requested & (fault==0))
        fault=fault | account.fault
        use=requested & (fault==0)
        mask=torch.zeros_like(self.occupied).scatter(1,di[:,None],use[:,None])
        refs,ref_mask=self._references(self.days[root,di])
        return (replace(self,finalized=self.finalized | mask,
            payable=_store_ratio(self.payable,payable,index,use),locked=_store_ratio(self.locked,locked,index,use),fault=fault),account,
            TakerDayResult(self.days[root,di],accrued,payable,locked,refs,ref_mask,delta,use,fault))

    def pay_one(self,account,enabled):
        roots,capacity=self.occupied.shape
        root=torch.arange(roots,device=self.days.device)
        ready_to_finalize=self.occupied & self.mature & (self.pending==0) & ~self.finalized
        ready=self.occupied & self.finalized & ~self.paid & (self.payable.numerator.sign.reshape(roots,capacity)>0)
        values=torch.where(ready,self.days,torch.full_like(self.days,(1<<63)-1))
        di=values.argmin(1)
        use=enabled & ready.any(1) & ~ready_to_finalize.any(1) & (self.fault==0)
        index=root*capacity+di
        paid=take_ratio(self.payable,index)
        account,delta=account.pay_rebate_day(paid,torch.full_like(enabled,self.maker),use)
        fault=self.fault | account.fault
        use=use & delta.emitted & (fault==0)
        mask=torch.zeros_like(self.occupied).scatter(1,di[:,None],use[:,None])
        refs,ref_mask=self._references(self.days[root,di])
        return (replace(self,paid=self.paid | mask,fault=fault),account,
            TakerDayResult(self.days[root,di],take_ratio(self.accrued,index),paid,take_ratio(self.locked,index),
                refs,ref_mask,delta,use,fault))


@dataclass(frozen=True)
class TakerDays(RebateCalendarOperations):
    fill_occupied: torch.Tensor
    fill_sequences: object
    fill_days: torch.Tensor
    fill_fees: object
    terminal_consumed: torch.Tensor
    successful: torch.Tensor
    terminal_order: torch.Tensor  # 原终态消费序；物理人口计数，不是业务序号。
    next_terminal_order: torch.Tensor
    occupied: torch.Tensor
    days: torch.Tensor
    pending: torch.Tensor
    mature: torch.Tensor
    finalized: torch.Tensor
    paid: torch.Tensor
    accrued: object
    payable: object
    locked: object
    fault: torch.Tensor

    @classmethod
    def empty(cls,roots,device,*,fills=4,days=2):
        if roots<1 or fills<1 or days<1:
            raise ValueError('日桶与成交目录需要正物理容量')
        f=torch.zeros((roots,fills),dtype=torch.bool,device=device)
        d=torch.zeros((roots,days),dtype=torch.bool,device=device)
        def integer(n):
            return ExactInt.from_i64_tensor(torch.zeros(n,dtype=torch.int64,device=device))
        z=integer(roots*days)
        return cls(f,integer(roots*fills),torch.zeros_like(f,dtype=torch.int64),integer(roots*fills),
            f.clone(),f.clone(),torch.full_like(f,-1,dtype=torch.int64),
            torch.zeros(roots,dtype=torch.int64,device=device),d,torch.zeros_like(d,dtype=torch.int64),
            torch.zeros_like(d,dtype=torch.int64),d.clone(),d.clone(),d.clone(),_ratio(z),_ratio(z),_ratio(z),
            torch.zeros(roots,dtype=torch.int32,device=device))

    def resized(self,*,fills,days):
        roots,old_f=self.fill_occupied.shape
        old_d=self.occupied.size(1)
        if fills<old_f or days<old_d:
            raise ValueError('未退役返佣记录不能缩小物理人口')
        def pad(value,n,fill=0):
            return torch.nn.functional.pad(value,(0,n-value.size(1)),value=fill)
        def ratio(value):
            return ExactRational(_grow(value.numerator,roots,old_d,days),
                _grow(value.denominator,roots,old_d,days,True))
        return replace(self,fill_occupied=pad(self.fill_occupied,fills),
            fill_sequences=_grow(self.fill_sequences,roots,old_f,fills),fill_days=pad(self.fill_days,fills),
            fill_fees=_grow(self.fill_fees,roots,old_f,fills),terminal_consumed=pad(self.terminal_consumed,fills),
            successful=pad(self.successful,fills),terminal_order=pad(self.terminal_order,fills,-1),
            occupied=pad(self.occupied,days),days=pad(self.days,days),pending=pad(self.pending,days),
            mature=pad(self.mature,days),finalized=pad(self.finalized,days),paid=pad(self.paid,days),
            accrued=ratio(self.accrued),payable=ratio(self.payable),locked=ratio(self.locked))

    def _fill_lookup(self,sequence):
        roots,capacity=self.fill_occupied.shape
        owner=torch.arange(roots,device=self.days.device).repeat_interleave(capacity)
        relation=self.fill_sequences.compare(_take(sequence,owner))
        match=self.fill_occupied & (relation.order.reshape(roots,capacity)==0)
        return match.to(torch.int64).argmax(1),match.any(1),Numbers.combine_fault_rows(
            torch.where(self.fill_occupied,relation.fault.reshape(roots,capacity),0))

    def register(self,sequence,matched_at,fee,enabled):
        roots,fill_capacity=self.fill_occupied.shape
        day_capacity=self.occupied.size(1)
        root=torch.arange(roots,device=self.days.device)
        if matched_at.shape!=(roots,) or matched_at.dtype!=torch.int64 or matched_at.device!=self.days.device:
            raise TypeError('matched时刻须同设备int64[roots]')
        safe_time=matched_at.clamp(MIN_DAY,MAX_MATCHED)
        day=torch.div(safe_time,DAY,rounding_mode='floor')*DAY
        fi,found,lookup_fault=self._fill_lookup(sequence)
        same_day=self.occupied & (self.days==day[:,None])
        known_day=same_day.any(1)
        di=torch.where(known_day,same_day.to(torch.int64).argmax(1),(~self.occupied).to(torch.int64).argmax(1))
        vacancy=(~self.fill_occupied).to(torch.int64).argmax(1)
        fi=torch.where(found,fi,vacancy)
        old_fee=_take(self.fill_fees,root*fill_capacity+fi).compare(fee)
        upper=sequence.compare(sequence.constant((1<<64)-1))
        positive=enabled & (fee.sign>0)
        error=sequence.fault | fee.fault | upper.fault | ((sequence.sign<=0)|(upper.order>0)|(fee.sign<0)).to(torch.int32)
        # 原SIM只为正fee成交创建返佣日；零费不要求不存在的成熟时刻可表示。
        error=error | torch.where(positive,lookup_fault | ((matched_at<MIN_DAY)|(matched_at>MAX_MATCHED)).to(torch.int32),0)
        error=error | torch.where(positive & found,old_fee.fault | ((old_fee.order!=0)|
            (self.fill_days[root,fi]!=day)).to(torch.int32),0)
        fresh=positive & ~found
        error=error | (fresh & known_day & self.finalized[root,di]).to(torch.int32)
        need_f=torch.where(fresh & self.fill_occupied.all(1),fill_capacity+1,fill_capacity)
        need_d=torch.where(fresh & ~known_day & self.occupied.all(1),day_capacity+1,day_capacity)
        fault=self.fault | torch.where(enabled,error,0)
        use=fresh & (need_f<=fill_capacity) & (need_d<=day_capacity) & (fault==0)
        fm=torch.zeros_like(self.fill_occupied).scatter(1,fi[:,None],use[:,None])
        dm=torch.zeros_like(self.occupied).scatter(1,di[:,None],use[:,None])
        index=root*fill_capacity+fi
        result=replace(self,fill_occupied=self.fill_occupied | fm,
            fill_sequences=_put_integer(self.fill_sequences,sequence,index,use),
            fill_fees=_put_integer(self.fill_fees,fee,index,use),fill_days=torch.where(fm,day[:,None],self.fill_days),
            occupied=self.occupied | dm,days=torch.where(dm,day[:,None],self.days),
            pending=self.pending+dm.to(torch.int64),fault=fault)
        committed=enabled & (~positive | found | use) & (fault==0)
        return result,TakerDayDemand(committed,need_f,need_d,fault)

    def complete(self,sequence,fee,terminal,success,contribution):
        roots,fill_capacity=self.fill_occupied.shape
        root=torch.arange(roots,device=self.days.device)
        fi,found,lookup_fault=self._fill_lookup(sequence)
        requested=terminal & (fee.sign>0)
        day=self.fill_days[root,fi]
        dm=self.occupied & (self.days==day[:,None])
        di=dm.to(torch.int64).argmax(1)
        fresh=requested & found & ~self.terminal_consumed[root,fi]
        old=take_ratio(self.accrued,root*self.occupied.size(1)+di)
        added=old+contribution
        error=lookup_fault | fee.fault | contribution.fault | added.fault | (
            ~found | ~dm.any(1) | (self.pending[root,di]<=0) | self.finalized[root,di] |
            (contribution.numerator.sign<0) | (~success & (contribution.numerator.sign!=0))).to(torch.int32)
        # 重复终态无第二次前驱扣减；已经成功的此前前缀不回滚。
        fault=self.fault | torch.where(fresh | (requested & ~found),error,0)
        use=fresh & (fault==0)
        fm=torch.zeros_like(self.fill_occupied).scatter(1,fi[:,None],use[:,None])
        used_day=torch.zeros_like(self.occupied).scatter(1,di[:,None],use[:,None])
        result=replace(self,terminal_consumed=self.terminal_consumed | fm,
            successful=torch.where(fm,success[:,None],self.successful),
            terminal_order=torch.where(fm,self.next_terminal_order[:,None],self.terminal_order),
            next_terminal_order=self.next_terminal_order+use.to(torch.int64),
            pending=self.pending-used_day.to(torch.int64),
            accrued=_store_ratio(self.accrued,added,root*self.occupied.size(1)+di,use),fault=fault)
        return result,terminal & (~requested | found) & (fault==0)

    def _references(self,day):
        valid=self.fill_occupied & self.terminal_consumed & self.successful & (self.fill_days==day[:,None])
        key=torch.where(valid,self.terminal_order,torch.full_like(self.terminal_order,(1<<63)-1))
        order=key.argsort(dim=1,stable=True)
        roots,capacity=valid.shape
        index=(torch.arange(roots,device=day.device)[:,None]*capacity+order).flatten()
        return _take(self.fill_sequences,index),valid.gather(1,order)
