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
    payload_schema: int = 11
    asset_indices: object | None = None
    outcome_list: object = None


def map_economic(delta,account_slots,condition_slots,token_slots,asset_indices,asset_roots,private_sequence,
                 source_ordinal,effective_at,transition_tag,entity_sequence,path_ordinal,*,resolution_identity=None,
                 settlement_fee_liability=None,actual_taker_fee_liability=None):
    """覆盖当前SIM接纳、逐fill应用/失败/返佣及单订单释放的原Existing转移。

    0=OrderAccepted，2=CallerCanceled，6=EconomicApplied，7=UnappliedFailure，
    8=Compensation，9=Maker市场汇总应计，10=TakerRebate。其它转移必须由对应producer接齐其字段，
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
    for tag in [0,2,6,7,8,9,10]:
        transition_known=transition_known | (transition_tag==tag)
    if resolution_identity is not None:
        if resolution_identity.shape!=(count,) or resolution_identity.dtype!=torch.int64 or resolution_identity.device!=device:
            raise TypeError('判定释放须完整原Resolution身份目录')
        transition_known |= transition_tag==5
        fault |= ((transition_tag!=5)|(resolution_identity<0)).to(torch.int32)
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
    # 原自然SIM/V1来源的liability为精确0；公共V2入口显式保留其真实额外delta。
    liabilities=[]
    for value in (settlement_fee_liability,actual_taker_fee_liability):
        value=zero if value is None else value
        if (value.numerator.sign.shape!=(count,) or value.denominator.sign.shape!=(count,)
                or value.numerator.words.device!=device or value.denominator.words.device!=device):
            raise TypeError('公共V2 liability须同设备精确pUSD[row]')
        fault |= value.fault
        liabilities.append(value)
    payload=[account_slots,condition_slots,effective_at,transition_tag,entity_sequence,
        torch.where(path_used,path_ordinal,0),*balances,has_outcome,
        torch.where(has_outcome,token_slots,0),outcome,*liabilities]
    if resolution_identity is not None:
        payload.append(resolution_identity)
    semantic=encode_fields(payload)
    words=private_sequence.words.to(torch.int64)&0xffffffff
    high=words[:,1] if words.size(1)>1 else torch.zeros(count,dtype=torch.int64,device=device)
    source=torch.stack([torch.full_like(high,2),words[:,0],high,source_ordinal],dim=1)
    return EconomicEvidence(logical,economic,semantic,source,balances,outcome,has_outcome,
        delta.emitted & (fault==0),fault,delta.sequence,18 if resolution_identity is not None else 11,asset_indices)


def map_rebate_economic(delta,account,asset_roots,private,ordinal,at,kind,day,paid):
    """原Existing日结/付款：无condition、无持仓项，day仍是有符号时间。"""
    from queues import ExactInt,ExactRational
    from asset_buy import take_ratio
    from lifecycle import input_fault,source_order
    from account_state import grouped_fault
    count=account.numel()
    device=account.device
    fault=input_fault([delta.sequence,private],[account,at,kind,day],ordinal) | delta.fault
    if paid.shape!=(count,) or paid.dtype!=torch.bool or paid.device!=device:
        raise TypeError('日经济变体须同设备bool[row]')
    if asset_roots.shape!=delta.movement.outcomes.sign.shape or asset_roots.dtype!=torch.int64 or asset_roots.device!=device:
        raise TypeError('日经济持仓所属目录须同设备int64[assets]')
    fault=fault | ((account<0)|(kind<0)|(kind>1)).to(torch.int32)
    outcome_error=delta.movement.outcomes.fault | (delta.movement.outcomes.sign!=0).to(torch.int32)
    outcome_error=outcome_error | ((asset_roots<0)|(asset_roots>=count)).to(torch.int32)
    fault=fault | grouped_fault(outcome_error,asset_roots.clamp(0,count-1),count)
    balances=tuple(ExactRational.ratio(value,value.constant(1_000_000)) for value in
        [delta.movement.cash,delta.movement.available,delta.movement.principal,delta.movement.fee]) + tuple(
            take_ratio(delta.movement.rebates,torch.arange(count,device=device)*4+field) for field in range(4))
    for value in balances:
        fault=fault | value.fault
    zero=ExactRational.ratio(delta.sequence.constant(0),delta.sequence.constant(1))
    logical=encode_fields([account,torch.full_like(account,11),delta.sequence])
    economic=encode_fields([account,torch.ones_like(account),delta.sequence])
    tag=torch.where(paid,13,11)+kind
    # 保持统一schema11，与原V1/V2 Existing归一相同；day不是u64实体序号。
    semantic=encode_fields([account,torch.full_like(account,-1),at,tag,ExactInt.from_i64_tensor(day),
        torch.zeros_like(account),*balances,torch.zeros_like(paid),torch.zeros_like(account),zero,zero,zero])
    return EconomicEvidence(logical,economic,semantic,source_order(private,ordinal),balances,zero,
        torch.zeros_like(paid),delta.emitted & (fault==0),fault,delta.sequence)


def map_resolution_redemption(delta,account,condition,resolution,policy,assets,tokens,asset_roots,
                              private,ordinal,at):
    """原请求式或Auto兑付经济事实，完整两腿delta保持原成员人口。"""
    from queues import ExactRational,Numbers,_take
    from asset_buy import take_ratio
    from lifecycle import input_fault,source_order
    from fields import RecordList
    from account_state import grouped_fault
    roots=account.numel()
    scalar=[account,condition,resolution,at]+([] if policy is None else [policy])
    fault=delta.fault | input_fault([delta.sequence,private],scalar,ordinal)
    if assets.shape!=(roots,2) or tokens.shape!=assets.shape:
        raise ValueError('兑付经济成员须同根原两腿资产/token绑定')
    safe=assets.clamp(0,max(asset_roots.numel()-1,0))
    selected=_take(delta.movement.outcomes,safe.flatten())
    valid=(assets>=0)&(assets<asset_roots.numel())&(tokens>=0)
    valid &= asset_roots[safe]==torch.arange(roots,device=account.device)[:,None]
    member=(torch.arange(asset_roots.numel(),device=account.device)[:,None]==assets[asset_roots]).any(1)
    extra=(delta.movement.outcomes.sign!=0)&~member
    fault |= Numbers.combine_fault_rows((~valid).to(torch.int32)) | grouped_fault(extra.to(torch.int32),asset_roots,roots)
    fault |= ((account<0)|(condition<0)|(resolution<0)|(assets[:,0]==assets[:,1])).to(torch.int32)
    if policy is not None: fault |= (policy<0).to(torch.int32)
    balances=tuple(ExactRational.ratio(value,value.constant(1_000_000)) for value in
        [delta.movement.cash,delta.movement.available,delta.movement.principal,delta.movement.fee])+tuple(
        take_ratio(delta.movement.rebates,torch.arange(roots,device=account.device)*4+field) for field in range(4))
    amounts=ExactRational.ratio(selected,selected.constant(1_000_000))
    present=selected.sign.reshape(roots,2)!=0
    semantic_members=RecordList((tokens.flatten(),amounts),present)
    transported=RecordList((assets.flatten(),amounts),present)
    zero=ExactRational(delta.sequence.constant(0),delta.sequence.constant(1))
    logical=encode_fields([account,torch.full_like(account,11),delta.sequence])
    economic=encode_fields([account,torch.ones_like(account),delta.sequence])
    semantic=encode_fields([account,condition,at,resolution]+([] if policy is None else [policy])+
        [*balances,semantic_members,zero,zero])
    return EconomicEvidence(logical,economic,semantic,source_order(private,ordinal),balances,zero,
        present.any(1),delta.emitted & (fault==0),fault,delta.sequence,29 if policy is None else 21,None,transported)
