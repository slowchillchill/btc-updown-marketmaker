"""同冻结公开修复差和剩余承诺→原current gap/cap；只在末端投影。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows
from repair_gap import repair_gap


@dataclass(frozen=True)
class RepairInputs:
    gap: torch.Tensor  # [roots,2sides,3]；old没有此字段。
    cap: torch.Tensor
    fault: torch.Tensor


def repair_inputs(ledger,conditions,present,applicable,asset_outcomes,*,prefix_known,
                  settlement_prefix_known=None,commitments=None):
    pools=ledger.success_pools;roots=pools.fault.numel();device=pools.asset_roots.device
    assets=pools.asset_roots.numel();numbers=_numbers()
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2)
            or conditions.dtype!=torch.int64 or conditions.device!=device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or applicable.shape!=(roots,2) or applicable.dtype!=torch.bool or applicable.device!=device
            or asset_outcomes.shape!=(assets,) or asset_outcomes.dtype!=torch.int64 or asset_outcomes.device!=device
            or prefix_known.shape!=(roots,) or prefix_known.dtype!=torch.bool or prefix_known.device!=device):
        raise TypeError('修复输入需要同设备原scope、outcome与完整公开Trade前缀')
    if settlement_prefix_known is None:
        settlement_prefix_known=torch.zeros_like(prefix_known)
    if (settlement_prefix_known.shape!=(roots,) or settlement_prefix_known.dtype!=torch.bool
            or settlement_prefix_known.device!=device):
        raise TypeError('修复输入需要同设备完整结算前缀')
    current=conditions[:,0];population=present[:,0];empty=~population.any(1)
    owner=pools.asset_roots
    relation=(owner[None,None,:]==torch.arange(roots,device=device)[:,None,None])
    relation=relation & (pools.condition_slots[None,None,:]==current[:,:,None])
    side=torch.arange(2,device=device)
    bound=(relation[:,None,:,:] & (asset_outcomes[None,None,None,:]==side[None,:,None,None])).any(3)
    binding_known=(~population[:,None,:]|bound).all(2)
    members=(relation & population[:,:,None]).any(1)
    selected=members[:,None,:] & (asset_outcomes[None,None,:]==side[None,:,None])
    resolutions=getattr(ledger,'resolutions',None)
    resolved=torch.zeros(roots,dtype=torch.bool,device=device)
    settlement_known=empty.clone()
    fault=ledger.trade_success.quantity.numerator.fault.new_zeros(roots)
    if resolutions is not None:
        match=((resolutions.roots[None,None,:]==torch.arange(roots,device=device)[:,None,None])
            & (resolutions.conditions[None,None,:]==current[:,:,None]))
        unique=match.to(torch.int64).sum(2)==1
        typed=(match & resolutions.known[None,None,:]).any(2)
        settlement_known=(~population|(unique&typed)).all(1)
        resolved=(match & population[:,:,None] & resolutions.known[None,None,:]
            & resolutions.present[None,None,:]).flatten(1).any(1)
        fault|=resolutions.root_fault
        fault|=numbers.combine_fault_rows(torch.where((match&population[:,:,None]).any(1),
            resolutions.fault[None,:],0))
    enabled=applicable[:,0] & ~resolved
    known=(prefix_known|empty) & (settlement_prefix_known|empty) & settlement_known
    known=known[:,None] & binding_known & enabled[:,None]
    read=repair_gap(ledger.trade_success,pools);fault|=read.fault
    if assets:
        at=torch.arange(assets,device=device)[None,None,:].expand(roots,2,assets).flatten()
        value=take_ratio(read.quantity,at)
        zero=numbers.ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        total=sum_rows(select(value,selected.flatten(),zero),roots*2,assets)
    else:
        integer=numbers.ExactInt.from_i64_tensor(torch.zeros(roots*2,dtype=torch.int64,device=device))
        total=numbers.ExactRational(integer,integer.constant(1))
    def encode(value,value_known):
        nonlocal fault
        projected,error=value.project(operation=1)
        fault|=numbers.combine_fault_rows(torch.where(enabled[:,None],error.reshape(roots,2),0))
        return torch.stack((torch.where(value_known,projected.reshape(roots,2),0.),
            value_known.float(),enabled[:,None].expand(roots,2).float()),-1)
    gap=encode(total,known)
    cap_known=torch.zeros_like(known)
    cap=total
    if commitments is not None:
        at=(torch.arange(roots,device=device)[:,None]*4+side[None,:]).flatten()
        difference=total-take_ratio(commitments.remaining,at)
        fault|=numbers.combine_fault_rows(torch.where(enabled[:,None],difference.fault.reshape(roots,2),0))
        zero=numbers.ExactRational(difference.numerator.constant(0),difference.denominator.constant(1))
        cap=select(difference,difference.numerator.sign>0,zero)
        cap_known=known&commitments.known[:,0]
        fault|=commitments.fault
    return RepairInputs(gap,encode(cap,cap_known),fault)


def write_repair_fields(slow,inputs):
    if slow.shape!=(inputs.gap.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.gap.device:
        raise TypeError('修复字段写入同设备float32 slow433')
    result=slow.clone()
    result[:,164:170]=inputs.gap.flatten(1)
    result[:,170:176]=inputs.cap.flatten(1)
    return result
