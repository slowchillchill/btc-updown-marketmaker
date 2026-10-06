"""SIM单资产经济delta到统一V2 NOE字段；原真实资金不经D-0295量化。"""
from dataclasses import dataclass
import torch
from fields import encode_fields


@dataclass(frozen=True)
class EconomicEvidence:
    logical_keys: torch.Tensor
    economic_keys: torch.Tensor
    semantic: torch.Tensor
    source_order: torch.Tensor
    balances: tuple
    outcome: object
    has_outcome: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor
    sequence: object


def map_economic(delta,account_slots,condition_slots,token_slots,asset_indices,asset_roots,private_sequence,
                 source_ordinal,effective_at,transition_tag,entity_sequence,path_ordinal):
    """覆盖当前SIM接纳、逐fill应用/失败/返佣及单订单释放的原Existing转移。

    0=OrderAccepted，2=CallerCanceled，6=EconomicApplied，7=UnappliedFailure，
    8=Compensation，10=TakerRebate。其它转移必须由对应producer接齐其字段，
    不能把这里的单资产表示当作多资产结算或V2新增经济种类。
    """
    from queues import ExactRational,_take
    from asset_buy import take_ratio
    count=delta.emitted.numel()
    device=delta.emitted.device
    for value in [account_slots,condition_slots,token_slots,asset_indices,source_ordinal,effective_at,transition_tag,path_ordinal]:
        if value.shape!=(count,) or value.dtype!=torch.int64 or value.device!=device:
            raise TypeError('NOE经济目录/时刻/enum须同设备int64[row]')
    fault=delta.fault | delta.sequence.fault | private_sequence.fault | entity_sequence.fault
    maximum=delta.sequence.constant((1<<64)-1)
    for value in [delta.sequence,private_sequence,entity_sequence]:
        relation=value.compare(maximum)
        fault=fault | relation.fault | ((value.sign<0)|(relation.order>0)).to(torch.int32)
    transition_known=torch.zeros(count,dtype=torch.bool,device=device)
    for tag in [0,2,6,7,8,10]:
        transition_known=transition_known | (transition_tag==tag)
    path_used=(transition_tag>=6)&(transition_tag<=8)
    fault=fault | ((account_slots<0)|(condition_slots<0)|(token_slots<0)|
        (source_ordinal<0)|(source_ordinal>0xffffffff)|~transition_known|
        (path_used&((path_ordinal<0)|(path_ordinal>0xffffffff)))).to(torch.int32)
    movement=delta.movement
    if asset_roots.shape!=movement.outcomes.sign.shape or asset_roots.dtype!=torch.int64 or asset_roots.device!=device:
        raise TypeError('经济资产所属根须同设备int64[assets]')
    for value in [movement.cash,movement.available,movement.principal,movement.fee]:
        if value.sign.numel()!=count or value.words.device!=device:
            raise ValueError('经济余额人口不符')
    safe=asset_indices.clamp(0,max(movement.outcomes.sign.numel()-1,0))
    if movement.outcomes.sign.numel()==0:
        selected=delta.sequence.constant(0)
        fault=fault | (asset_indices!=-1).to(torch.int32)
    else:
        from account_state import grouped_fault
        owners=asset_roots.clamp(0,count-1)
        original_index=torch.arange(movement.outcomes.sign.numel(),device=device)
        extra=(movement.outcomes.sign!=0)&(original_index!=asset_indices[owners])
        extra=extra | (asset_roots<0) | (asset_roots>=count)
        fault=fault | grouped_fault(extra.to(torch.int32),owners,count)
        fault=fault | ((asset_indices<0)|(asset_indices>=movement.outcomes.sign.numel())).to(torch.int32)
        fault=fault | (asset_roots[safe]!=torch.arange(count,device=device)).to(torch.int32)
        selected=_take(movement.outcomes,safe)
    balances=tuple(ExactRational.ratio(value,value.constant(1_000_000)) for value in
        [movement.cash,movement.available,movement.principal,movement.fee]) + tuple(
            take_ratio(movement.rebates,torch.arange(count,device=device)*4+field) for field in range(4))
    outcome=ExactRational.ratio(selected,selected.constant(1_000_000))
    for value in [*balances,outcome]:
        fault=fault | value.fault
    has_outcome=selected.sign!=0
    zero=ExactRational.ratio(delta.sequence.constant(0),delta.sequence.constant(1))
    tag=torch.full_like(account_slots,11)  # 原AccountTransition逻辑键
    logical=encode_fields([account_slots,tag,delta.sequence])
    economic=encode_fields([account_slots,torch.ones_like(tag),delta.sequence])
    # V1/V2统一：原Existing转移及两项精确零liability均入同一schema。
    semantic=encode_fields([account_slots,condition_slots,effective_at,transition_tag,entity_sequence,
        torch.where(path_used,path_ordinal,0),*balances,has_outcome,
        torch.where(has_outcome,token_slots,0),outcome,zero,zero])
    words=private_sequence.words.to(torch.int64)&0xffffffff
    high=words[:,1] if words.size(1)>1 else torch.zeros(count,dtype=torch.int64,device=device)
    source=torch.stack([torch.full_like(high,2),words[:,0],high,source_ordinal],dim=1)
    return EconomicEvidence(logical,economic,semantic,source,balances,outcome,has_outcome,
        delta.emitted & (fault==0),fault,delta.sequence)
