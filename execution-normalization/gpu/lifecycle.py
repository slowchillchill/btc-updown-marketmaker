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


@dataclass(frozen=True)
class OrderQueryResult:
    outcome: torch.Tensor  # Found/NotFound/OutOfRetention=0/1/2。
    order: object
    condition: torch.Tensor
    token: torch.Tensor
    state: torch.Tensor  # 原Open..Unmatched=0..7，Unknown=8。
    raw_state: torch.Tensor  # Unknown原字符串字典；已知状态为-1。
    quantity_kind: torch.Tensor  # Base/QuoteBudget/原V1无数量=0/1/2。
    matched: object
    open_quantity: object
    retention_kind: torch.Tensor  # OrderHistory/TradeHistory/RootSnapshot=0/1/2。
    retained_from: torch.Tensor
    requested_root: object
    earliest_root: object


def map_order_query(result,private,ordinal,account,request,active):
    """普通Control::OrderQuery沿原DirectResponseRequest逻辑身份，不伪装Submission。

    原V1无数量显式保留；Base/Quote数量是精确份额，不截为micro单位。
    Unknown状态及OutOfRetention原成员沿实际共享字典/完整u64保留。
    """
    from queues import ExactRational
    found=result.outcome==0
    retained=result.outcome==2
    scalar=[account,request,result.outcome,result.condition,result.token,result.state,result.raw_state,
        result.quantity_kind,result.retention_kind,result.retained_from]
    fault=input_fault([private,result.order,result.requested_root,result.earliest_root],scalar,ordinal)
    fault|=((account<0)|(request<0)|(result.outcome<0)|(result.outcome>2)).to(torch.int32)
    fault|=(found&((result.condition<0)|(result.token<0)|(result.state<0)|(result.state>8)|
        ((result.state==8)&(result.raw_state<0))|((result.state!=8)&(result.raw_state!=-1))|
        (result.quantity_kind<0)|(result.quantity_kind>2))).to(torch.int32)
    fault|=(retained&((result.retention_kind<0)|(result.retention_kind>2))).to(torch.int32)
    for value in (result.matched,result.open_quantity):
        if value.numerator.sign.shape!=account.shape or value.numerator.words.device!=account.device:
            raise TypeError('查询数量须同设备原精确份额[roots]')
        fault|=torch.where(found,value.fault|(value.numerator.sign<0).to(torch.int32),0)
    zero=result.order.constant(0)
    ratio=ExactRational(zero,zero.constant(1))
    select_ratio=lambda v,use:ExactRational(v.numerator.select(use,ratio.numerator),v.denominator.select(use,ratio.denominator))
    semantic=encode_fields([account,result.outcome,
        torch.where(found,result.condition,0),torch.where(found,result.token,0),result.order.select(found,zero),
        torch.where(found,result.state,0),torch.where(found,result.raw_state,-1),
        torch.where(found,result.quantity_kind,2),select_ratio(result.matched,found&(result.quantity_kind!=2)),
        select_ratio(result.open_quantity,found&(result.quantity_kind==0)),
        torch.where(retained,result.retention_kind,-1),torch.where(retained&(result.retention_kind<2),result.retained_from,0),
        result.requested_root.select(retained&(result.retention_kind==2),zero),
        result.earliest_root.select(retained&(result.retention_kind==2),zero)])
    logical=encode_fields([account,torch.full_like(account,5),request])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,25)


def map_legacy_submission(result,private,ordinal,account,request,item,active):
    """正式原V1 Submission正文，不为无measurement来源构造V2数量。"""
    accepted=result.outcome==0
    fault=input_fault([private,result.order],[account,request,item,result.outcome,
        result.condition,result.token,result.state,result.raw_state],ordinal)
    fault|=((account<0)|(request<0)|(item<0)|(item>65535)|(result.outcome<0)|(result.outcome>2)).to(torch.int32)
    fault|=(accepted&((result.condition<0)|(result.token<0)|(result.state<0)|(result.state>8)|
        ((result.state==8)&(result.raw_state<0))|((result.state!=8)&(result.raw_state!=-1)))).to(torch.int32)
    semantic=encode_fields([account,result.outcome,torch.where(accepted,result.condition,0),
        torch.where(accepted,result.token,0),result.order.select(accepted,result.order.constant(0)),
        torch.where(accepted,result.state,0),torch.where(accepted,result.raw_state,-1)])
    logical=encode_fields([account,torch.full_like(account,3),request,item])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,26)


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


def map_resolved_position(account,condition,resolution_identity,terminal_identity,private,ordinal,active):
    """原Resolved：完整Resolution/Terminal借同一不可变字典，不发经济delta。

    resolution_identity绑定原canonical字符串，terminal_identity绑定完整原Terminal
    （含原序资产/payout向量、winner与mapping rule）。字典页必须随记录存活，
    不能用相邻页的局部索引解释它们，也不能拿资产槽替代原实体身份。
    """
    fault=input_fault([private],[account,condition,resolution_identity,terminal_identity],ordinal)
    fault |= ((account<0)|(condition<0)|(resolution_identity<0)|(terminal_identity<0)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,condition,resolution_identity,terminal_identity])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,17)


def map_settlement_request(account,condition,resolution,policy,outcome,observation,private,ordinal,active):
    """原SettlementLifecycleV2::Request，固定SDK U256[1,2]完整保留。"""
    fault=input_fault([private],[account,condition,resolution,policy,outcome,observation],ordinal)
    fault |= ((account<0)|(condition<0)|(resolution<0)|(policy<0)|(outcome<0)|(outcome>3)|
        (observation<0)|(observation>10)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,condition,resolution,policy,torch.ones_like(account),
        torch.full_like(account,2),outcome,observation])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,20)


def map_market_resolved_order(order_sequence,resolution,private,ordinal,account,condition,token,active):
    """原MarketResolvedCanceled；关闭订单与释放金额为两个独立人口。"""
    fault=input_fault([order_sequence,private],[account,condition,token,resolution],ordinal)
    fault |= ((account<0)|(condition<0)|(token<0)|(resolution<0)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,7),order_sequence,private])
    semantic=encode_fields([account,condition,token,order_sequence,torch.full_like(account,5),
        torch.zeros_like(account),resolution])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active & (fault==0),fault,19)


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


@dataclass(frozen=True)
class CancellationEvidence(LifecycleEvidence):
    cancellation_fields: object = None  # 原input_item_refs，独立于Control正文等值。


def map_cancellation(order_sequence,target_ordinal,input_item_refs,outcome,private,ordinal,account,request,active):
    """原单目标Cancellation；target序u64与input item人口分别保持。

    input_item_refs保存原(item_id字典坐标,u16 ordinal)和原顺序；不能由目标数重建。
    """
    from fields import RecordList
    from queues import Numbers
    if not isinstance(input_item_refs,RecordList) or len(input_item_refs.fields)!=2:
        raise TypeError('撤单目标需原item_id/ordinal有序引用列表')
    item_ids,item_ordinals=input_item_refs.fields
    rows,capacity=input_item_refs.present.shape
    if rows!=account.numel():
        raise ValueError('撤单引用人口与反馈人口不符')
    for value in [item_ids,item_ordinals]:
        if value.shape!=(rows*capacity,) or value.dtype!=torch.int64 or value.device!=account.device:
            raise TypeError('撤单input item需同设备int64[row*capacity]字段')
    invalid=(item_ids<0)|(item_ordinals<0)|(item_ordinals>65535)
    fault=input_fault([order_sequence,target_ordinal,private],[account,request,outcome],ordinal)
    fault=fault|input_item_refs.fault|Numbers.combine_fault_rows(torch.where(input_item_refs.present,invalid.reshape(rows,capacity).to(torch.int32),0))
    fault=fault|((account<0)|(request<0)|(outcome<0)|(outcome>2)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,4),request,target_ordinal])
    semantic=encode_fields([account,order_sequence,outcome])
    return CancellationEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,15,
        encode_fields([input_item_refs]))


def map_cancellation_completed(canceled,no_effect,unknown,private,ordinal,account,request,active):
    """原DirectResponseRequest身份；完整三个u64计数，零目标仍有一条证据。"""
    fault=input_fault([canceled,no_effect,unknown,private],[account,request],ordinal)
    fault=fault|((account<0)|(request<0)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,5),request])
    semantic=encode_fields([account,canceled,no_effect,unknown])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,16)


def map_order_canceled(order_sequence,private,ordinal,account,condition,token,active):
    """真实CallerCanceled生命周期；控制Canceled不代替该独立private事实。"""
    fault=input_fault([order_sequence,private],[account,condition,token],ordinal)
    fault=fault|((account<0)|(condition<0)|(token<0)).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,7),order_sequence,private])
    semantic=encode_fields([account,condition,token,order_sequence,torch.full_like(account,4),torch.zeros_like(account)])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,7)
