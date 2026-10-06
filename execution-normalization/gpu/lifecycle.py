"""已接纳Base订单与Taker返佣贡献的原NOE字段设备映射。"""
from dataclasses import dataclass
import torch
from fields import encode_fields


@dataclass(frozen=True)
class LifecycleEvidence:
    logical_keys: torch.Tensor
    semantic: torch.Tensor
    source_order: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor
    payload_schema: int = 0


def source_order(private,ordinal):
    words=private.words.to(torch.int64)&0xffffffff
    high=words[:,1] if words.size(1)>1 else torch.zeros_like(words[:,0])
    return torch.stack([torch.full_like(high,2),words[:,0],high,ordinal],dim=1)


def input_fault(sequences,columns,ordinal):
    count=sequences[0].sign.numel()
    device=sequences[0].words.device
    fault=torch.zeros(count,dtype=torch.int32,device=device)
    for value in columns+[ordinal]:
        if value.shape!=(count,) or value.dtype!=torch.int64 or value.device!=device:
            raise TypeError('NOE生命周期标量须同设备int64[row]')
    for value in sequences:
        if value.sign.numel()!=count or value.words.device!=device:
            raise ValueError('NOE生命周期序号人口不符')
        relation=value.compare(value.constant((1<<64)-1))
        fault=fault | value.fault | relation.fault | ((value.sign<0)|(relation.order>0)).to(torch.int32)
    return fault | ((ordinal<0)|(ordinal>0xffffffff)).to(torch.int32)


def map_base_accepted(quantity,order_sequence,private,ordinal,account,condition,token,request,item,at,active,accepted):
    """真实Accepted输出两个不同证据：OrderLifecycle与Submission，private分别传入。"""
    fault=input_fault([order_sequence,private],[account,condition,token,request,item,at],ordinal) | quantity.fault
    fault=fault | ((account<0)|(condition<0)|(token<0)|(request<0)|(item<0)|(item>65535)).to(torch.int32)
    state=torch.where(quantity.open_base_units.sign>0,0,1)  # 原Open/Matched
    logical=encode_fields([account,torch.full_like(account,3),request,item])
    # 原Control::Submission Accepted/Base：不夹带预留或原始提交terms。
    zero=quantity.matched_base_units.constant(0)
    semantic=encode_fields([account,(~accepted).to(torch.int64),torch.where(accepted,condition,0),
        torch.where(accepted,token,0),order_sequence.select(accepted,zero),torch.where(accepted,state,0),
        quantity.matched_base_units.select(accepted,zero),quantity.open_base_units.select(accepted,zero),
        torch.where(accepted,at,0)])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,3)


def map_order_accepted(quantity,order_sequence,private,ordinal,account,condition,token,active):
    fault=input_fault([order_sequence,private],[account,condition,token],ordinal) | quantity.fault
    fault=fault | ((account<0)|(condition<0)|(token<0)).to(torch.int32)
    state=torch.where(quantity.open_base_units.sign>0,0,1)
    logical=encode_fields([account,torch.full_like(account,7),order_sequence,private])
    semantic=encode_fields([account,condition,token,order_sequence,torch.zeros_like(account),state])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,7)


def map_taker_contribution(record,frame,rebate_rate,private,ordinal,account,condition):
    """贡献不再改平台应收；实际应收变动仍是独立经济delta。"""
    from queues import ExactRational
    fault=input_fault([frame.fill_sequence,private],[account,condition],ordinal) | record.fault | frame.event.fault
    fault=fault | ((account<0)|(condition<0)).to(torch.int32)
    contribution=ExactRational.ratio(record.fee,record.fee.constant(1_000_000))*rebate_rate
    fault=fault | contribution.fault
    # 原UTC日归属使用成交matched时刻，负纪元仍向负无穷取整。
    day=torch.div(record.matched_at,86_400_000,rounding_mode='floor')*86_400_000
    fault=fault | (record.matched_at < -((1<<63)//86_400_000)*86_400_000).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,condition,frame.fill_sequence,torch.ones_like(account),day,contribution])
    emitted=frame.event.rebate_delta.emitted & (fault==0)
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),emitted,fault,9)


def map_day_finalized(kind,day,accrued,payable,locked,refs,private,ordinal,account,active):
    """原RebateDay逻辑身份；贡献引用保留原向量顺序和完整Trade身份。"""
    from fields import IntegerList
    if not isinstance(refs,IntegerList) or refs.present.size(0)!=account.numel():
        raise TypeError('日结果需要原贡献引用列表及对应根人口')
    fault=input_fault([private],[account,kind,day],ordinal)
    fault=fault | accrued.fault | payable.fault | locked.fault | refs.fault
    from queues import Numbers
    reference_limit=refs.values.compare(refs.values.constant((1<<64)-1))
    reference_error=reference_limit.fault | ((refs.values.sign<0)|(reference_limit.order>0)).to(torch.int32)
    fault=fault | Numbers.combine_fault_rows(torch.where(refs.present,reference_error.reshape_as(refs.present),0))
    fault=fault | ((account<0)|(kind<0)|(kind>1)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,12),kind,day])
    semantic=encode_fields([account,kind,day,accrued,payable,locked,refs])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,12)


def map_rebate_payment(kind,day,paid,private,ordinal,account,active):
    """付款自身按SettlementLifecycle身份；day_result指向同账户RebateDay。"""
    fault=input_fault([private],[account,kind,day],ordinal) | paid.fault
    fault=fault | ((account<0)|(kind<0)|(kind>1)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    # 固定原引用类型、账户、kind/day全部保留，不以该引用代替付款逻辑身份。
    semantic=encode_fields([account,kind,day,account,torch.full_like(account,12),kind,day,paid])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,13)


def map_maker_finalized(condition,status,contributions,private,ordinal,account,active):
    """一条原Maker证据保存完整贡献向量；不按元素拆反馈或重排人口。"""
    from fields import RecordList
    from queues import Numbers
    if not isinstance(contributions,RecordList) or len(contributions.fields)!=5:
        raise TypeError('Maker需要原trade/condition/kind/day/amount贡献向量')
    roots,capacity=contributions.present.shape
    if roots!=account.numel():
        raise ValueError('Maker贡献向量根人口不符')
    trade,conditions,kinds,days,amounts=contributions.fields
    for column in [conditions,kinds,days]:
        if column.shape!=(roots*capacity,) or column.dtype!=torch.int64 or column.device!=account.device:
            raise TypeError('Maker贡献标量需同设备int64[root*capacity]')
    comparison=trade.compare(trade.constant((1<<64)-1))
    error=trade.fault | comparison.fault | amounts.fault | ((trade.sign<0)|(comparison.order>0)).to(torch.int32)
    error=error.reshape(roots,capacity) | ((conditions.reshape(roots,capacity)!=condition[:,None]) |
        (kinds.reshape(roots,capacity)!=0)).to(torch.int32)
    fault=input_fault([private],[account,condition,status],ordinal) | contributions.fault
    fault=fault | Numbers.combine_fault_rows(torch.where(contributions.present,error,0))
    fault=fault | ((account<0)|(condition<0)|(status<0)|(status>2)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,condition,status,contributions])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,14)
