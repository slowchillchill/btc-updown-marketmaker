"""原双现金动作预留、发送登记与五态的精确设备候选。

request/event槽是冷字典引用，原请求身份不由订单或condition推断。容量不足只扩目录重算；
候选自身不发送，不替代全Ledger的entry序、NOE位置及订单关联协调。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from asset_buy import take_ratio

EMPTY,RESERVED,INSUFFICIENT_ACTUAL,INSUFFICIENT_MAKER=0,1,2,3
BEFORE_SEND,SUBMITTED,PLATFORM_PENDING,CONVERTED,RELEASED=range(5)


def choose(value,mask,other):
    return ExactRational(value.numerator.select(mask,other.numerator),value.denominator.select(mask,other.denominator))


def grow_integer(value,roots,old,new,fill=0):
    words=value.words.reshape(roots,old,-1)
    signs=value.sign.reshape(roots,old)
    fault=value.fault.reshape(roots,old)
    append=ExactInt.from_i64_tensor(torch.full((roots*(new-old),),fill,dtype=torch.int64,device=words.device)).with_capacity(words.size(2))
    return ExactInt(torch.cat([words,append.words.reshape(roots,new-old,-1)],dim=1).flatten(0,1),
        torch.cat([signs,append.sign.reshape(roots,new-old)],dim=1).flatten(),
        torch.cat([fault,append.fault.reshape(roots,new-old)],dim=1).flatten())


def sequence_fault(value,*,positive=False):
    upper=value.compare(value.constant((1<<64)-1))
    return value.fault | upper.fault | ((upper.order>0)|(value.sign<0)|
        ((value.sign==0) if positive else torch.zeros_like(value.sign,dtype=torch.bool))).to(torch.int32)


def take_root(value,index,width):
    return _take(value,torch.arange(index.numel(),device=index.device)*width+index)


@dataclass(frozen=True)
class ReservationDecision:
    outcomes: torch.Tensor
    ledger_sequences: object
    current_ledger_sequence: object
    current_local_prefix: object
    required_capacity: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ReservationBook:
    request_slots: torch.Tensor
    conditions: torch.Tensor
    principal: object
    fee: object
    outcomes: torch.Tensor
    statuses: torch.Tensor
    decision_sequences: object  # 原CachedReservationRequest.outcome的初始Reserved序，不随状态迁移。
    last_sequences: object
    event_slots: torch.Tensor
    event_sequences: object
    fault: torch.Tensor

    @classmethod
    def empty(cls,roots,*,capacity=1,device='cuda'):
        if roots<1 or capacity<1:
            raise ValueError('预留目录人口必须为正')
        coordinates=torch.full((roots,capacity),-1,dtype=torch.int64,device=device)
        zero=ExactInt.from_i64_tensor(torch.zeros(roots*capacity,dtype=torch.int64,device=device))
        ratio=ExactRational.ratio(zero,zero.constant(1))
        return cls(coordinates,coordinates.clone(),ratio,ratio,
            torch.zeros_like(coordinates),coordinates.clone(),zero,zero,coordinates.clone(),zero,
            torch.zeros(roots,dtype=torch.int32,device=device))

    @property
    def capacity(self):
        return self.request_slots.size(1)

    def resized(self,capacity):
        roots,old=self.request_slots.shape
        if capacity<old:
            raise ValueError('不能缩小仍有预留/局部事件身份的目录')
        def matrix(value,fill):
            return torch.nn.functional.pad(value,(0,capacity-old),value=fill)
        def ratio(value):
            return ExactRational(grow_integer(value.numerator,roots,old,capacity),
                grow_integer(value.denominator,roots,old,capacity,1))
        return replace(self,request_slots=matrix(self.request_slots,-1),conditions=matrix(self.conditions,-1),
            principal=ratio(self.principal),fee=ratio(self.fee),outcomes=matrix(self.outcomes,EMPTY),
            statuses=matrix(self.statuses,-1),decision_sequences=grow_integer(self.decision_sequences,roots,old,capacity),last_sequences=grow_integer(self.last_sequences,roots,old,capacity),
            event_slots=matrix(self.event_slots,-1),event_sequences=grow_integer(self.event_sequences,roots,old,capacity))

    def pending(self):
        roots,width=self.request_slots.shape
        local=(self.statuses==BEFORE_SEND)|(self.statuses==SUBMITTED)
        platform=self.statuses==PLATFORM_PENDING
        zero=take_ratio(self.principal,torch.arange(roots,device=local.device)*width)
        zero=ExactRational.ratio(zero.numerator.constant(0),zero.denominator.constant(1))
        local_total,platform_total=zero,zero
        for column in range(width):
            index=torch.arange(roots,device=local.device)*width+column
            amount=take_ratio(self.principal,index)+take_ratio(self.fee,index)
            local_total=local_total+choose(amount,local[:,column],zero)
            platform_total=platform_total+choose(amount,platform[:,column],zero)
        return local_total,platform_total

    def available(self,platform_available,maker_cash,platform_principal,platform_fee):
        local,platform=self.pending()
        pending=local+platform
        return platform_available-pending,maker_cash-platform_principal-platform_fee-pending

    def reserve_action(self,requests,conditions,principal,fee,enabled,actual_available,maker_available,
                       current_ledger_sequence,current_local_prefix):
        roots,width=requests.shape
        device=self.request_slots.device
        for value in [requests,conditions]:
            if value.dtype!=torch.int64 or value.shape!=enabled.shape or value.device!=device:
                raise TypeError('动作请求与condition目录须同设备int64[root,item]')
        if enabled.dtype!=torch.bool or enabled.device!=device or roots!=self.fault.numel():
            raise TypeError('动作预留mask须同设备bool[root,item]')
        if principal.numerator.sign.numel()!=roots*width or fee.numerator.sign.numel()!=roots*width:
            raise ValueError('动作预留精确金额人口不符')
        if width==0:
            empty=ExactInt.from_i64_tensor(torch.empty(0,dtype=torch.int64,device=device))
            return self,ReservationDecision(torch.empty_like(requests),empty,current_ledger_sequence,current_local_prefix,
                (self.request_slots>=0).sum(dim=1),self.fault)
        zero=ExactRational.ratio(current_ledger_sequence.constant(0),current_ledger_sequence.constant(1))
        total=zero
        new=[]
        fault=self.fault | sequence_fault(current_ledger_sequence) | sequence_fault(current_local_prefix)
        for column in range(width):
            cached=(self.request_slots==requests[:,column,None]).any(dim=1)
            prior=((requests[:,:column]==requests[:,column,None])&enabled[:,:column]).any(dim=1)
            fresh=enabled[:,column]&~cached&~prior
            new.append(fresh)
            amount=take_ratio(principal,torch.arange(roots,device=device)*width+column)
            charge=take_ratio(fee,torch.arange(roots,device=device)*width+column)
            total=total+choose(amount+charge,fresh,zero)
            fault=fault | torch.where(fresh,amount.fault|charge.fault|
                ((amount.numerator.sign<0)|(charge.numerator.sign<0)|(requests[:,column]<0)|(conditions[:,column]<0)).to(torch.int32),0)
        new=torch.stack(new,dim=1)
        actual=total.compare(actual_available)
        maker=total.compare(maker_available)
        fault=fault | total.fault | actual.fault | maker.fault
        refusal=torch.where(actual.order>0,INSUFFICIENT_ACTUAL,torch.where(maker.order>0,INSUFFICIENT_MAKER,RESERVED))
        required=(self.request_slots>=0).sum(dim=1)+new.sum(dim=1)
        # 原决定也须缓存失败；扩池不是业务InsufficientFunds。
        enough=required<=self.capacity
        book=self
        ledger_seq,local_prefix=current_ledger_sequence,current_local_prefix
        decisions=[]
        sequences=[]
        root_index=torch.arange(roots,device=device)
        for column in range(width):
            matches=(book.request_slots==requests[:,column,None])&(book.request_slots>=0)
            found=matches.any(dim=1)
            old=matches.to(torch.int32).argmax(dim=1)
            vacant=(book.request_slots<0).to(torch.int32).argmax(dim=1)
            fresh=new[:,column]&enough&(fault==0)
            reserved=fresh&(refusal==RESERVED)
            candidate_sequence=ledger_seq+ledger_seq.constant(1)
            candidate_prefix=local_prefix+local_prefix.constant(1)
            for value in [candidate_sequence,candidate_prefix]:
                bound=value.compare(value.constant((1<<64)-1))
                fault=fault | torch.where(reserved,value.fault|bound.fault|((value.sign<0)|(bound.order>0)).to(torch.int32),0)
            reserved=reserved&(fault==0)
            fresh=fresh&(fault==0)
            place=torch.where(found,old,vacant)
            mask=torch.zeros_like(book.request_slots,dtype=torch.bool).scatter(1,place[:,None],fresh[:,None])
            full_sequence=_take(candidate_sequence,root_index.repeat_interleave(self.capacity))
            root_seqmask=torch.zeros_like(mask).scatter(1,place[:,None],reserved[:,None])
            input_index=root_index*width+column
            p=take_ratio(principal,input_index)
            f=take_ratio(fee,input_index)
            p=ExactRational(_take(p.numerator,root_index.repeat_interleave(self.capacity)),_take(p.denominator,root_index.repeat_interleave(self.capacity)))
            f=ExactRational(_take(f.numerator,root_index.repeat_interleave(self.capacity)),_take(f.denominator,root_index.repeat_interleave(self.capacity)))
            book=replace(book,request_slots=torch.where(mask,requests[:,column,None],book.request_slots),
                conditions=torch.where(mask,conditions[:,column,None],book.conditions),
                principal=choose(p,root_seqmask.flatten(),book.principal),fee=choose(f,root_seqmask.flatten(),book.fee),
                outcomes=torch.where(mask,refusal[:,None],book.outcomes),
                statuses=torch.where(root_seqmask,BEFORE_SEND,book.statuses),
                decision_sequences=full_sequence.select(root_seqmask.flatten(),book.decision_sequences),
                last_sequences=full_sequence.select(root_seqmask.flatten(),book.last_sequences))
            decisions.append(torch.where(enabled[:,column],book.outcomes[root_index,place],EMPTY))
            sequences.append(take_root(book.decision_sequences,place,self.capacity))
            ledger_seq=candidate_sequence.select(reserved,ledger_seq)
            local_prefix=candidate_prefix.select(reserved,local_prefix)
        # 工程fault不提交当前动作目录；资金拒绝是已缓存的正常决定。
        valid=(fault==0)&enough
        slots_valid=valid[:,None]
        book=replace(book,request_slots=torch.where(slots_valid,book.request_slots,self.request_slots),
            conditions=torch.where(slots_valid,book.conditions,self.conditions),outcomes=torch.where(slots_valid,book.outcomes,self.outcomes),
            statuses=torch.where(slots_valid,book.statuses,self.statuses),principal=choose(book.principal,slots_valid.expand_as(self.request_slots).flatten(),self.principal),
            fee=choose(book.fee,slots_valid.expand_as(self.request_slots).flatten(),self.fee),
            decision_sequences=book.decision_sequences.select(slots_valid.expand_as(self.request_slots).flatten(),self.decision_sequences),
            last_sequences=book.last_sequences.select(slots_valid.expand_as(self.request_slots).flatten(),self.last_sequences),fault=fault)
        width_words=max(value.words.size(1) for value in sequences)
        seq=ExactInt(torch.stack([value.with_capacity(width_words).words for value in sequences],dim=1).flatten(0,1),
            torch.stack([value.sign for value in sequences],dim=1).flatten(),torch.stack([value.fault for value in sequences],dim=1).flatten())
        return book,ReservationDecision(torch.stack(decisions,dim=1),seq,ledger_seq.select(valid,current_ledger_sequence),
            local_prefix.select(valid,current_local_prefix),required,fault)

    def submission_started(self,request,event,ledger_sequence,enabled):
        roots,width=self.request_slots.shape
        for column in [request,event]:
            if column.shape!=(roots,) or column.dtype!=torch.int64 or column.device!=self.fault.device:
                raise TypeError('预留发送目录须同设备int64[roots]')
        if enabled.shape!=(roots,) or enabled.dtype!=torch.bool or enabled.device!=self.fault.device:
            raise TypeError('预留发送mask须同设备bool[roots]')
        found=(self.request_slots==request[:,None])&(self.outcomes==RESERVED)
        old_event=(self.event_slots==event[:,None]).any(dim=1)
        at=found.to(torch.int32).argmax(dim=1)
        eligible=enabled&~old_event&(self.statuses.gather(1,at[:,None]).flatten()==BEFORE_SEND)
        fault=self.fault | (enabled&~old_event&~found.any(dim=1)).to(torch.int32)
        fault=fault | torch.where(enabled,sequence_fault(ledger_sequence,positive=True)|
            ((request<0)|(event<0)).to(torch.int32),0)
        vacant=self.event_slots<0
        enough=vacant.any(dim=1)
        required=torch.maximum((self.request_slots>=0).sum(1),(self.event_slots>=0).sum(1)+eligible.to(torch.int64))
        use=eligible&enough&(fault==0)
        mask=torch.zeros_like(found).scatter(1,at[:,None],use[:,None])
        slot=vacant.to(torch.int32).argmax(dim=1)
        event_mask=torch.zeros_like(found).scatter(1,slot[:,None],use[:,None])
        expanded=_take(ledger_sequence,torch.arange(roots,device=request.device).repeat_interleave(width))
        return replace(self,statuses=torch.where(mask,SUBMITTED,self.statuses),
            last_sequences=expanded.select(mask.flatten(),self.last_sequences),
            event_slots=torch.where(event_mask,event[:,None],self.event_slots),
            event_sequences=expanded.select(event_mask.flatten(),self.event_sequences),fault=fault),required,use

    def definitive(self,request,accepted,economic_applied,ledger_sequence,enabled):
        roots=self.fault.numel()
        if request.shape!=(roots,) or request.dtype!=torch.int64 or request.device!=self.fault.device:
            raise TypeError('预留确定结果请求目录须同设备int64[roots]')
        for mask in [accepted,economic_applied,enabled]:
            if mask.shape!=(roots,) or mask.dtype!=torch.bool or mask.device!=self.fault.device:
                raise TypeError('预留确定结果mask须同设备bool[roots]')
        found=(self.request_slots==request[:,None])&(self.outcomes==RESERVED)
        at=found.to(torch.int32).argmax(dim=1)
        status=self.statuses.gather(1,at[:,None]).flatten()
        mutable=(status==BEFORE_SEND)|(status==SUBMITTED)|(status==PLATFORM_PENDING)
        fault=self.fault | (enabled&~found.any(dim=1)).to(torch.int32)
        fault=fault | torch.where(enabled,sequence_fault(ledger_sequence,positive=True)|(request<0).to(torch.int32),0)
        next_status=torch.where(accepted,torch.where(economic_applied,CONVERTED,PLATFORM_PENDING),RELEASED)
        change=enabled&mutable&(status!=next_status)&(fault==0)
        mask=torch.zeros_like(found).scatter(1,at[:,None],change[:,None])
        expanded=_take(ledger_sequence,torch.arange(request.numel(),device=request.device).repeat_interleave(self.capacity))
        return replace(self,statuses=torch.where(mask,next_status[:,None],self.statuses),
            last_sequences=expanded.select(mask.flatten(),self.last_sequences),fault=fault)


def run_reserve_action(book,*args):
    while True:
        result=Numbers.run_exact_batch(lambda:book.reserve_action(*args))
        required=int(result[1].required_capacity.max().cpu())
        if required<=book.capacity:
            return result
        book=book.resized(1<<(required-1).bit_length())
