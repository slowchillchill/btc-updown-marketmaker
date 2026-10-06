"""实际SIM逐fill最终性到NOE TradeLifecycleV3的设备字段映射。

账户/market/token目录为冷共享原身份。order/fill/private均完整u64业务序号；
其sim-N原字串只在冷导出物化，不把物理槽位或source ordinal当业务序号。
"""
from dataclasses import dataclass
import torch
from fields import encode_fields


@dataclass(frozen=True)
class TradeEvidence:
    logical_keys: torch.Tensor
    semantic: torch.Tensor
    source_order: torch.Tensor
    economic_application_sequence: object
    has_economic_application: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


def map_trade(record,frame,order_sequence,source_price,account_slots,condition_slots,token_slots,
              private_sequence,source_ordinal,*,liquidity_role,maker_fee_equivalent,
              maker_fee_read_kind):
    """read kind沿原Known/Unavailable/NotApplicable，不以0替代缺失。

    本层只映射成交证据；真实economic delta另生成AccountTransition，不能重复扣钱。
    """
    count=frame.fill_sequence.sign.numel()
    device=frame.event.emitted.device
    for value in [account_slots,condition_slots,token_slots,source_ordinal,liquidity_role,maker_fee_read_kind]:
        if value.shape!=(count,) or value.dtype!=torch.int64 or value.device!=device:
            raise TypeError('NOE成交目录/enum/source ordinal须同设备int64[row]')
    fault=record.fault | frame.event.fault | frame.fill_sequence.fault | order_sequence.fault | private_sequence.fault
    maximum=frame.fill_sequence.constant((1<<64)-1)
    for value in [frame.fill_sequence,order_sequence,private_sequence,record.economic_application_sequence]:
        comparison=value.compare(maximum)
        fault=fault | value.fault | comparison.fault | ((value.sign<0)|(comparison.order>0)).to(torch.int32)
    fault=fault | source_price.fault | torch.where(maker_fee_read_kind==0,maker_fee_equivalent.fault,
        torch.zeros_like(maker_fee_equivalent.fault))
    fault=fault | ((account_slots<0)|(condition_slots<0)|(token_slots<0)|(source_ordinal<0)|
        (source_ordinal>0xffffffff)|(liquidity_role<0)|(liquidity_role>1)|
        (maker_fee_read_kind<0)|(maker_fee_read_kind>2)).to(torch.int32)
    fault=fault | (((liquidity_role==0)&(maker_fee_read_kind!=2))|
        ((liquidity_role==1)&(maker_fee_read_kind==2))).to(torch.int32)
    state=frame.event.state.to(torch.int64)
    fault=fault | ((state<0)|(state>5)).to(torch.int32)
    applied=record.economic_applied
    # 原map_trade_disposition：失败保留首次economic seq，补偿后不改成未应用。
    disposition=torch.where(state==5,torch.where(applied,4,3),
        torch.where(state==4,2,torch.where(applied,1,0)))
    schema=torch.full_like(account_slots,8)  # 原LogicalKey::TradeLifecycle
    logical=encode_fields([account_slots,schema,frame.fill_sequence,private_sequence])
    zero=type(maker_fee_equivalent).ratio(record.base.constant(0),record.base.constant(1))
    from dataclasses import replace
    maker=replace(maker_fee_equivalent,
        numerator=maker_fee_equivalent.numerator.select(maker_fee_read_kind==0,zero.numerator),
        denominator=maker_fee_equivalent.denominator.select(maker_fee_read_kind==0,zero.denominator))
    semantic=encode_fields([account_slots,condition_slots,token_slots,frame.fill_sequence,order_sequence,
        state,disposition,record.matched_at,liquidity_role,source_price,record.base,record.quote,record.fee,
        applied,record.economic_application_sequence,maker_fee_read_kind,maker])
    words=private_sequence.words.to(torch.int64)&0xffffffff
    low=words[:,0]
    high=words[:,1] if words.size(1)>1 else torch.zeros_like(low)
    source=torch.stack([torch.full_like(low,2),low,high,source_ordinal],dim=1)
    emitted=frame.event.emitted & (fault==0)
    return TradeEvidence(logical,semantic,source,record.economic_application_sequence,applied,emitted,fault)
