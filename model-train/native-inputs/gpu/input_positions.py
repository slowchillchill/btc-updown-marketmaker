"""同Ledger冻结前缀的原资产份额/实际持仓成本，不用成功池成本代替。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class PositionInputs:
    values: torch.Tensor  # [roots,2scopes,2fields,2sides,3]，原54—77。
    fault: torch.Tensor


def position_inputs(ledger, conditions, present, applicable, asset_outcomes):
    roots = ledger.fault.numel()
    owners = ledger.platform.asset_roots
    assets = owners.numel()
    device = owners.device
    if (conditions.ndim != 3 or conditions.shape[:2] != (roots,2)
            or conditions.dtype != torch.int64 or conditions.device != device
            or present.shape != conditions.shape or present.dtype != torch.bool or present.device != device
            or applicable.shape != (roots,2) or applicable.dtype != torch.bool or applicable.device != device
            or asset_outcomes.shape != (assets,) or asset_outcomes.dtype != torch.int64 or asset_outcomes.device != device):
        raise TypeError('持仓字段需同设备原scope、适用位和固定资产outcome索引')
    numbers = _numbers()
    slots = ledger.success_pools.condition_slots
    sides = torch.arange(2,device=device)
    # 原lsum只对真实Ledger市场记录查固定资产binding；不存在的市场是Known零。
    # 未安装人口时不能凭空目录证明不存在。缺少某侧真实binding保留Unknown。
    bound = ((slots[None,None,None,None,:] == conditions[:,:,None,:,None])
        & (owners[None,None,None,None,:] == torch.arange(roots,device=device)[:,None,None,None,None])
        & (asset_outcomes[None,None,None,None,:] == sides[None,None,:,None,None])).any(4)
    projected = ledger.projected_markets
    if projected is None:
        absent = torch.zeros_like(present)
    else:
        exists = ((conditions[:,:,:,None] == projected.conditions[None,None,None,:])
            & projected.present[:,None,None,:]).any(3)
        absent = ~exists & projected.known[:,None,None]
    binding_known = (~present[:,:,None,:] | absent[:,:,None,:] | bound).all(3)
    # 已知不存在的Ledger市场不参与余额、成本、known或字段fault归约。
    # 固定冷资产目录中的身份不能覆盖原lsum的空市场Known零。
    amount_present = present & ~absent
    scope = ((slots[None,None,:,None] == conditions[:,:,None,:]) & amount_present[:,:,None,:]).any(3)
    scope &= owners[None,None,:] == torch.arange(roots,device=device)[:,None,None]
    member = scope[:,:,None,:] & (asset_outcomes[None,None,None,:] == sides[None,None,:,None])
    coordinates = torch.arange(assets,device=device)[None,None,None,:].expand(roots,2,2,assets).flatten()
    costs = ledger.position_costs.asset_cost(owners)
    fields = []
    fault = ledger.fault.clone()
    for source, source_known in ((ledger.platform.outcomes,torch.ones_like(owners,dtype=torch.bool)),
                                 (costs.cost,costs.known)):
        value = take_ratio(source,coordinates)
        zero = numbers.ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        if assets:
            total = sum_rows(select(value,member.flatten(),zero),roots*4,assets)
        else:
            integer = numbers.ExactInt.from_i64_tensor(torch.zeros(roots*4,dtype=torch.int64,device=device))
            total = numbers.ExactRational(integer,integer.constant(1))
        known = binding_known & ~(member & ~source_known[None,None,None,:]).any(3)
        value, error = total.project(operation=1)
        fields.append(torch.stack((torch.where(known & applicable[:,:,None],value.reshape(roots,2,2),0.0),
            (known & applicable[:,:,None]).float(),applicable[:,:,None].expand(roots,2,2).float()),-1))
        fault |= numbers.combine_fault_rows(torch.where(applicable[:,:,None],error.reshape(roots,4).reshape(roots,2,2),0).reshape(roots,-1))
    fault |= numbers.combine_fault_rows(torch.where(scope,costs.fault[None,None,:],0).reshape(roots,-1))
    return PositionInputs(torch.stack(fields,2),fault)


def write_position_fields(slow, inputs):
    if slow.shape != (inputs.values.size(0),433) or slow.dtype != torch.float32 or slow.device != inputs.values.device:
        raise TypeError('持仓字段写入同设备float32 slow433')
    result = slow.clone()
    result[:,54:78] = inputs.values.reshape(slow.size(0),24)
    return result
