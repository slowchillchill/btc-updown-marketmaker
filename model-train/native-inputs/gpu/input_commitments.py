"""完整receipt逐腿承诺到原账户98—109；不以可撤订单人口代替责任。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class CommitmentInputs:
    values: torch.Tensor  # [roots,2scopes,2sides,3]
    fault: torch.Tensor


def commitment_inputs(commitments, conditions, present, applicable):
    roots, count = commitments.present.shape
    device = commitments.present.device
    numbers = _numbers()
    if (conditions.ndim != 3 or conditions.shape[:2] != (roots,2)
            or conditions.dtype != torch.int64 or conditions.device != device
            or present.shape != conditions.shape or present.dtype != torch.bool or present.device != device
            or applicable.shape != (roots,2) or applicable.dtype != torch.bool or applicable.device != device):
        raise TypeError('承诺字段需要同设备原作用域与适用位')
    metadata = [commitments.condition_slots,commitments.outcome_indices,commitments.association_known]
    if any(value is None for value in metadata):
        raise ValueError('承诺作用域需要原意图condition/token的真实固定outcome绑定')
    for value, dtype in zip(metadata,[torch.int64,torch.int64,torch.bool]):
        if value.shape != (roots,count) or value.dtype != dtype or value.device != device:
            raise TypeError('承诺关联列需绑定原逐腿人口')
    condition_known = commitments.condition_slots>=0
    member = ((commitments.condition_slots[:,None,:,None]==conditions[:,:,None,:])
        & present[:,:,None,:]).any(3) & commitments.present[:,None,:]
    outcome_known = commitments.association_known & (commitments.outcome_indices>=0) & (commitments.outcome_indices<2)
    # 原未知condition不能从非空scope静默排除；已知condition但token缺关联影响其两侧。
    unknown_condition = (commitments.present & ~condition_known).any(1)
    unknown_binding = (member & ~outcome_known[:,None,:]).any(2)
    side = torch.arange(2,device=device)
    population = member[:,:,None,:] & (commitments.outcome_indices[:,None,None,:]==side[None,None,:,None])
    known = ~((population & ~commitments.known[:,None,None,:]).any(3)
        | unknown_binding[:,:,None] | (unknown_condition[:,None,None] & present.any(2)[:,:,None]))
    if count:
        at = (torch.arange(roots,device=device)[:,None,None,None]*count
            + torch.arange(count,device=device)[None,None,None,:]).expand(roots,2,2,count)
        value = take_ratio(commitments.remaining,at.flatten())
        zero = numbers.ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        total = sum_rows(select(value,population.flatten(),zero),roots*4,count)
    else:
        integer = numbers.ExactInt.from_i64_tensor(torch.zeros(roots*4,dtype=torch.int64,device=device))
        total = numbers.ExactRational(integer,integer.constant(1))
    projected,error = total.project(operation=1)
    enabled = known & applicable[:,:,None]
    values = torch.stack((torch.where(enabled,projected.reshape(roots,2,2),0.),enabled.float(),
        applicable[:,:,None].expand(roots,2,2).float()),-1)
    fault = commitments.fault | numbers.combine_fault_rows(torch.where(applicable[:,:,None],error.reshape(roots,2,2),0).flatten(1))
    return CommitmentInputs(values,fault)


def write_commitment_fields(slow, inputs):
    if slow.shape != (inputs.values.size(0),433) or slow.dtype != torch.float32 or slow.device != inputs.values.device:
        raise TypeError('承诺字段写入同设备float32 slow433')
    result = slow.clone()
    result[:,98:110] = inputs.values.flatten(1)
    return result
