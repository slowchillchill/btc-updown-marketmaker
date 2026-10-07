"""原成功量/本金、固定配对和未配对孤腿的同前缀Native输入。

成功本金和配对成本沿D-0295归属；本模块不改变真实持仓、不做兑付或估值。
完整成功人口是否安装由根冷初始化明确提供，不能凭空池推断Known零。
"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class SuccessInputs:
    quantity: torch.Tensor  # [roots,2scopes,2sides,3]
    cost: torch.Tensor
    paired: torch.Tensor   # [roots,2scopes,3]，每市场只取一腿。
    orphan: torch.Tensor
    fault: torch.Tensor


def success_inputs(pools, conditions, present, applicable, asset_outcomes, *, prefix_known):
    roots = pools.fault.numel()
    assets = pools.asset_roots.numel()
    device = pools.asset_roots.device
    if (conditions.ndim != 3 or conditions.shape[:2] != (roots,2)
            or conditions.dtype != torch.int64 or conditions.device != device
            or present.shape != conditions.shape or present.dtype != torch.bool or present.device != device
            or applicable.shape != (roots,2) or applicable.dtype != torch.bool or applicable.device != device
            or asset_outcomes.shape != (assets,) or asset_outcomes.dtype != torch.int64 or asset_outcomes.device != device
            or prefix_known.shape != (roots,) or prefix_known.dtype != torch.bool or prefix_known.device != device):
        raise TypeError('成功字段需要同设备原作用域、资产outcome与完整成功前缀known')
    numbers = _numbers()
    owner = pools.asset_roots
    side = torch.arange(2,device=device)
    member = ((pools.condition_slots[None,None,:,None] == conditions[:,:,None,:])
        & present[:,:,None,:]).any(3)
    member &= owner[None,None,:] == torch.arange(roots,device=device)[:,None,None]
    sides = member[:,:,None,:] & (asset_outcomes[None,None,None,:] == side[None,None,:,None])
    bound = ((pools.condition_slots[None,None,None,None,:] == conditions[:,:,None,:,None])
        & (owner[None,None,None,None,:] == torch.arange(roots,device=device)[:,None,None,None,None])
        & (asset_outcomes[None,None,None,None,:] == side[None,None,:,None,None])).any(4)
    binding_known = (~present[:,:,None,:] | bound).all(3)
    known = ((prefix_known[:,None] | ~present.any(2)) & applicable)[:,:,None] & binding_known
    fault = pools.fault.clone()
    index = torch.arange(assets,device=device)[None,None,None,:].expand(roots,2,2,assets).flatten()
    def encode(source, population):
        nonlocal fault
        value = take_ratio(source,index)
        empty = numbers.ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        total = sum_rows(select(value,population.flatten(),empty),roots*4,assets)
        projected,error = total.project(operation=1)
        enabled = known
        fault |= numbers.combine_fault_rows(torch.where(applicable[:,:,None],error.reshape(roots,2,2),0).flatten(1))
        return torch.stack((torch.where(enabled,projected.reshape(roots,2,2),0.0),
            enabled.float(),applicable[:,:,None].expand(roots,2,2).float()),-1)
    quantity = encode(pools.success_quantity,sides)
    cost = encode(pools.success_cost,sides)
    orphan = encode(pools.quantity,sides)
    # 双腿各存相同配对量；原outcome0代表该市场一项，不将双腿加倍。
    paired_sides = encode(pools.paired_quantity,sides)
    paired_known = known.all(2)
    paired = torch.stack((torch.where(paired_known,paired_sides[:,:,0,0],0.0),
        paired_known.float(),applicable.float()),-1)
    return SuccessInputs(quantity,cost,paired,orphan,fault)


def write_success_fields(slow, inputs):
    if slow.shape != (inputs.quantity.size(0),433) or slow.dtype != torch.float32 or slow.device != inputs.quantity.device:
        raise TypeError('成功字段写入同设备float32 slow433')
    result = slow.clone()
    result[:,122:134] = inputs.quantity.flatten(1)
    result[:,134:146] = inputs.cost.flatten(1)
    for scope in range(2):
        result[:,146+scope*9:149+scope*9] = inputs.paired[:,scope]
        result[:,149+scope*9:155+scope*9] = inputs.orphan[:,scope].flatten(1)
    return result
