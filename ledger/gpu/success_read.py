"""原成功经济净读面；兑付沿同一公开resolution扣份额、清成功成本。"""
from dataclasses import dataclass
import torch
from queues import ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select


@dataclass(frozen=True)
class NetSuccessRead:
    quantity: object
    cost: object
    paired: object
    orphan: object
    known: torch.Tensor  # [assets]；完整结算历史和实际typed关联互相独立。
    fault: torch.Tensor


def net_success(pools,resolutions,*,prefix_known):
    roots=pools.fault.numel();assets=pools.asset_roots.numel();device=pools.asset_roots.device
    if prefix_known.shape!=(roots,) or prefix_known.dtype!=torch.bool or prefix_known.device!=device:
        raise TypeError('净成功读面需实际同根同设备完整结算前缀known')
    known=prefix_known[pools.asset_roots]
    fault=pools.fault.clone()
    if resolutions is not None:
        fault|=resolutions.root_fault
    if resolutions is None or not resolutions.assets.numel():
        # None没有已知业务含义；只有调用者明确安装完整冷历史才可声明没有兑付。
        return NetSuccessRead(pools.success_quantity,pools.success_cost,pools.paired_quantity,pools.quantity,known,fault)
    bound=(torch.arange(assets,device=device)[:,None,None]==resolutions.assets[None,:,:])
    members=bound.any(2)
    found=members.any(1)
    market=members.to(torch.int64).argmax(1)
    leg=bound.flatten(1).to(torch.int64).argmax(1)
    known&=~found|resolutions.known[market]
    faults=resolutions.fault[market]|resolutions.root_fault[pools.asset_roots]
    fault|=Numbers.combine_fault_rows(torch.where(pools.asset_roots[None,:]==
        torch.arange(roots,device=device)[:,None],torch.where(found,faults,0)[None,:],0))
    pair=take_ratio(resolutions.redeemed_pair,market)
    orphan=take_ratio(resolutions.redeemed_orphan,leg)
    zero=ExactRational(pair.numerator.constant(0),pair.denominator.constant(1))
    pair=select(pair,found,zero);orphan=select(orphan,found,zero)
    quantity=pools.success_quantity-pair-orphan
    paired=pools.paired_quantity-pair
    unpaired=pools.quantity-orphan
    # 原economic_read：本腿pair或orphan任何正额已兑付，成功本金成本读为零。
    cost=select(zero,(pair.numerator.sign>0)|(orphan.numerator.sign>0),pools.success_cost)
    error=quantity.fault|paired.fault|unpaired.fault|cost.fault
    fault|=Numbers.combine_fault_rows(torch.where(pools.asset_roots[None,:]==
        torch.arange(roots,device=device)[:,None],error[None,:],0))
    return NetSuccessRead(quantity,cost,paired,unpaired,known,fault)
