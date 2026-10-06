"""D-0295派生归属：scale12 RNE候选与精确尾差；不量化真实资金或份额。

使用传入公共任意精度有理数及其设备扩池事务。顺序分配的调用者传原稳定身份序，
这里不以物理槽位重新排序，不将分母或业务合法域限制为固定GPU字宽。
"""
from dataclasses import dataclass
import torch


def select(value,mask,other):
    return type(value)(value.numerator.select(mask,other.numerator),value.denominator.select(mask,other.denominator))


def with_fault(value,fault):
    integer=type(value.numerator)
    return type(value)(integer(value.numerator.words,value.numerator.sign,value.numerator.fault | fault),
        integer(value.denominator.words,value.denominator.sign,value.denominator.fault | fault))


def zero(value):
    return type(value).ratio(value.numerator.constant(0),value.denominator.constant(1))


def bounded(value,low,high):
    lower=value.compare(low)
    candidate=select(low,lower.order<0,value)
    upper=candidate.compare(high)
    return with_fault(select(high,upper.order>0,candidate),lower.fault | upper.fault)


def round_scale12_rne(value):
    scale=value.numerator.constant(1_000_000_000_000)
    quotient,remainder=(value.numerator.abs()*scale).divrem(value.denominator)
    relation=(remainder*remainder.constant(2)).compare(value.denominator)
    odd=(quotient.words[:,0].to(torch.int64)&1)!=0
    increase=(relation.order>0)|((relation.order==0)&odd)
    increment=quotient.constant(1).select(increase,quotient.constant(0))
    rounded=quotient+increment
    signed=rounded.negated().select(value.numerator.sign<0,rounded)
    candidate=type(value).ratio(signed,scale)
    return with_fault(candidate,value.fault | relation.fault)


@dataclass(frozen=True)
class Split:
    allocated: object
    remaining: object
    fault: torch.Tensor


def split_scale12(total,part,whole):
    """单端RNE后差额守恒；零/全额端点保留源精细值。"""
    part_bound=part.compare(whole)
    domain=total.fault | part.fault | whole.fault | part_bound.fault | (
        (total.numerator.sign<0)|(part.numerator.sign<0)|(whole.numerator.sign<=0)|(part_bound.order>0)).to(torch.int32)
    empty=zero(total)
    one=type(whole).ratio(whole.numerator.constant(1),whole.denominator.constant(1))
    divisor=select(one,whole.numerator.sign<=0,whole)
    candidate=round_scale12_rne(total*part.divided_by(divisor))
    candidate=bounded(candidate,empty,total)
    allocated=select(total,part_bound.order==0,candidate)
    allocated=select(empty,(part.numerator.sign==0)|(total.numerator.sign==0),allocated)
    allocated=with_fault(allocated,domain)
    remaining=total-allocated
    fault=domain | allocated.fault | remaining.fault
    return Split(with_fault(allocated,fault),with_fault(remaining,fault),fault)


def allocate_ordered(total,capacities):
    """按原身份序逐项写回，当前/后续精确容量界保护scale12候选。"""
    capacity=zero(total)
    fault=total.fault | (total.numerator.sign<0).to(torch.int32)
    for value in capacities:
        capacity=capacity+value
        fault=fault | value.fault | (value.numerator.sign<0).to(torch.int32)
    relation=total.compare(capacity)
    fault=fault | relation.fault | (relation.order>0).to(torch.int32)
    remaining=total
    result=[]
    for value in capacities:
        future=capacity-value
        empty=zero(total)
        lower=bounded(remaining-future,empty,remaining)
        upper_relation=value.compare(remaining)
        upper=select(value,upper_relation.order<=0,remaining)
        one=type(capacity).ratio(capacity.numerator.constant(1),capacity.denominator.constant(1))
        safe_capacity=select(one,capacity.numerator.sign==0,capacity)
        proposed=split_scale12(remaining,value,safe_capacity)
        allocated=bounded(proposed.allocated,lower,upper)
        allocated=select(empty,value.numerator.sign==0,allocated)
        fault=fault | future.fault | lower.fault | upper_relation.fault | allocated.fault
        result.append(allocated)
        remaining=remaining-allocated
        capacity=future
    fault=fault | remaining.fault | (remaining.numerator.sign!=0).to(torch.int32)
    return tuple(with_fault(value,fault) for value in result),fault


def allocate_ordered_device(total,capacities,enabled):
    """同一顺序容量公式的设备循环；无新增成功根直接保留源贡献。

    只回传独立物理容量请求，由原纯计算批统一重试，不逐项读取业务值。
    """
    from queues import Numbers
    roots=total.numerator.sign.numel()
    columns=len(capacities)
    integer=type(total.numerator)
    width=max([Numbers.device_word_capacity(),total.numerator.words.shape[1],total.denominator.words.shape[1]]+
        [part.words.shape[1] for value in capacities for part in [value.numerator,value.denominator]])
    def padded(value):
        return value.with_capacity(width).words
    if columns:
        numerator=torch.stack([padded(value.numerator) for value in capacities],dim=1).reshape(roots*columns,width)
        denominator=torch.stack([padded(value.denominator) for value in capacities],dim=1).reshape(roots*columns,width)
        signs=torch.stack([value.numerator.sign for value in capacities],dim=1).reshape(-1)
        input_fault=total.fault.clone()
        for value in capacities:
            input_fault=input_fault | value.fault
    else:
        numerator=torch.empty((0,width),dtype=torch.int32,device=total.numerator.words.device)
        denominator=numerator.clone()
        signs=torch.empty(0,dtype=torch.int8,device=numerator.device)
        input_fault=total.fault
    input_fault=input_fault | (total.denominator.sign<=0).to(torch.int32)
    for value in capacities:
        input_fault=input_fault | (value.denominator.sign<=0).to(torch.int32)
    n,d,fault,required=Numbers.public_extension().ordered_attribution(padded(total.numerator),padded(total.denominator),
        total.numerator.sign,numerator,denominator,signs,enabled,columns)
    Numbers.record_resource_request(required)
    fault=fault | torch.where(enabled,input_fault,0)
    output=type(total)(integer(n,n.any(dim=1).to(torch.int8),fault.repeat_interleave(columns)),
        integer(d,d.any(dim=1).to(torch.int8),fault.repeat_interleave(columns)))
    result=[]
    for column in range(columns):
        at=torch.arange(roots,device=enabled.device)*columns+column
        original_fault=capacities[column].fault
        result.append(type(total)(integer(output.numerator.words[at].contiguous(),output.numerator.sign[at],output.numerator.fault[at] | original_fault),
            integer(output.denominator.words[at].contiguous(),output.denominator.sign[at],output.denominator.fault[at] | original_fault)))
    return tuple(result),fault
