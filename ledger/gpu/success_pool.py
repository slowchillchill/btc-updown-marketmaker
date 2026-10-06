"""Ledger成功量/平均成本池设备候选，D-0295只限制派生成本归属。

调用者在原economic_applied且FinalSuccess的首次提交门传入真实成交量/本金。
资产、对腿和市场坐标来自冷固定身份目录；不按盘口、物理邻居或当前余额猜配对。
返佣日归属、成交贡献份额、结算和真实资金由各自状态另行协调。
"""
from dataclasses import dataclass,replace
from functools import lru_cache
from pathlib import Path
import importlib.util
import sys
import torch
from queues import ExactRational,Numbers,_take
from asset_buy import take_ratio


@lru_cache(maxsize=1)
def attribution():
    spec=importlib.util.spec_from_file_location('gpu_success_finite_attribution',Path(__file__).resolve().parents[2]/'contracts/gpu/finite_attribution.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    return module


def select(value,mask,original):
    return ExactRational(value.numerator.select(mask,original.numerator),value.denominator.select(mask,original.denominator))


@dataclass(frozen=True)
class SuccessPools:
    asset_roots: torch.Tensor
    condition_slots: torch.Tensor
    token_slots: torch.Tensor
    counterpart: torch.Tensor
    quantity: object
    cost: object
    success_quantity: object
    success_cost: object
    paired_quantity: object  # 对腿各存相同总配对量，读取按原市场一项，不求两腿和。
    paired_cost: object
    fault: torch.Tensor

    @classmethod
    def open(cls,asset_roots,conditions,tokens,counterpart,roots):
        count=asset_roots.numel()
        device=asset_roots.device
        for column in [asset_roots,conditions,tokens,counterpart]:
            if column.shape!=(count,) or column.dtype!=torch.int64 or column.device!=device:
                raise TypeError('成功池固定目录须同设备int64[assets]')
        if roots<1 or count<1:
            raise ValueError('成功池需要非空开户根与固定资产目录')
        safe=counterpart.clamp(0,count-1)
        bad=(asset_roots<0)|(asset_roots>=roots)|(conditions<0)|(tokens<0)|(counterpart<0)|(counterpart>=count)
        bad=bad | (safe==torch.arange(count,device=device)) | (counterpart[safe]!=torch.arange(count,device=device))
        bad=bad | (asset_roots[safe]!=asset_roots) | (conditions[safe]!=conditions) | (tokens[safe]==tokens)
        zero_integer=Numbers.ExactInt.from_i64_tensor(torch.zeros(count,dtype=torch.int64,device=device))
        zero=ExactRational.ratio(zero_integer,zero_integer.constant(1))
        fault=torch.zeros(roots,dtype=torch.int32,device=device).scatter_reduce(0,asset_roots.clamp(0,roots-1),bad.to(torch.int32),reduce='amax')
        return cls(asset_roots,conditions,tokens,safe,zero,zero,zero,zero,zero,zero,fault)

    def coordinated(self,original,valid):
        owner=self.asset_roots.clamp(0,valid.numel()-1)
        return replace(self,**{name:select(getattr(self,name),valid[owner],getattr(original,name)) for name in
            ['quantity','cost','success_quantity','success_cost','paired_quantity','paired_cost']})

    def add(self,condition,token,quantity,cost,enabled):
        roots=self.fault.numel()
        match=(self.asset_roots[:,None]==torch.arange(roots,device=enabled.device)[None,:]) & (
            self.condition_slots[:,None]==condition[None,:]) & (self.token_slots[:,None]==token[None,:])
        population=match.to(torch.int32).sum(dim=0)
        first=match.to(torch.int32).argmax(dim=0)
        second=self.counterpart[first]
        fault=self.fault | torch.where(enabled,quantity.fault | cost.fault | ((population!=1)|
            (quantity.numerator.sign<0)|(cost.numerator.sign<0)).to(torch.int32),0)
        owner=self.asset_roots.clamp(0,roots-1)
        incoming_quantity=take_ratio(quantity,owner)
        incoming_cost=take_ratio(cost,owner)
        selected=match.any(dim=1)&enabled[owner]
        next_quantity=self.quantity+select(incoming_quantity,selected,ExactRational.ratio(incoming_quantity.numerator.constant(0),incoming_quantity.denominator.constant(1)))
        next_cost=self.cost+select(incoming_cost,selected,ExactRational.ratio(incoming_cost.numerator.constant(0),incoming_cost.denominator.constant(1)))
        a,b=take_ratio(next_quantity,first),take_ratio(next_quantity,second)
        comparison=a.compare(b)
        paired=select(a,comparison.order<=0,b)
        # 零池不是分配域错误；无配对时分母用纯计算1，业务字段仍保持真实零。
        one=ExactRational.ratio(a.numerator.constant(1),a.denominator.constant(1))
        left=attribution().split_scale12(take_ratio(next_cost,first),paired,select(one,a.numerator.sign==0,a))
        right=attribution().split_scale12(take_ratio(next_cost,second),paired,select(one,b.numerator.sign==0,b))
        fault=fault | torch.where(enabled,comparison.fault | left.fault | right.fault,0)
        allocated=select(take_ratio(right.allocated,owner),torch.arange(owner.numel(),device=owner.device)==second[owner],take_ratio(left.allocated,owner))
        pair_mask=((torch.arange(owner.numel(),device=owner.device)==first[owner])|(
            torch.arange(owner.numel(),device=owner.device)==second[owner])) & enabled[owner]
        paired_by_asset=take_ratio(paired,owner)
        next_quantity=next_quantity-select(paired_by_asset,pair_mask,ExactRational.ratio(paired_by_asset.numerator.constant(0),paired_by_asset.denominator.constant(1)))
        next_cost=next_cost-select(allocated,pair_mask,ExactRational.ratio(allocated.numerator.constant(0),allocated.denominator.constant(1)))
        success_quantity=self.success_quantity+select(incoming_quantity,selected,ExactRational.ratio(incoming_quantity.numerator.constant(0),incoming_quantity.denominator.constant(1)))
        success_cost=self.success_cost+select(incoming_cost,selected,ExactRational.ratio(incoming_cost.numerator.constant(0),incoming_cost.denominator.constant(1)))
        paired_quantity=self.paired_quantity+select(paired_by_asset,pair_mask,ExactRational.ratio(paired_by_asset.numerator.constant(0),paired_by_asset.denominator.constant(1)))
        paired_cost=self.paired_cost+select(allocated,pair_mask,ExactRational.ratio(allocated.numerator.constant(0),allocated.denominator.constant(1)))
        for value in [next_quantity,next_cost,success_quantity,success_cost,paired_quantity,paired_cost]:
            fault=fault | Numbers.combine_fault_rows(value.fault[None,:].expand(roots,-1).masked_fill(owner[None,:]!=torch.arange(roots,device=owner.device)[:,None],0))
        valid=enabled & (fault==0)
        return replace(self,quantity=select(next_quantity,valid[owner],self.quantity),cost=select(next_cost,valid[owner],self.cost),
            success_quantity=select(success_quantity,valid[owner],self.success_quantity),success_cost=select(success_cost,valid[owner],self.success_cost),
            paired_quantity=select(paired_quantity,valid[owner],self.paired_quantity),paired_cost=select(paired_cost,valid[owner],self.paired_cost),fault=fault),valid
