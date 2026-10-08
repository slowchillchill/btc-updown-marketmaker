"""原NativeActor价格模型的精确市场估费；GTC为0，不使用LIMIT最大费。"""
import torch
from prepare import _numbers
from asset_buy import take_ratio
from buy_cost import market_fee_estimate
from actor_support import _take


def price_fee_features(actor,state,support):
    """返回[R,999]全局k-1列及逐根fault，保留原精确容量出口。

    只投影当前FOK腿的合法价格；其他列是模型装箱占位0，不是合法项。
    调用者仍使用同次support.price，并与Numbers fits及原路径共同接纳。
    """
    rows=actor.rows;device=actor.device;numbers=_numbers()
    if support.price.shape!=(rows,999) or support.price.device!=device:
        raise TypeError('价格费用需要同根同设备999列支持')
    side=actor._side(state)
    variant=state.variants.gather(1,side[:,None]).squeeze(1)
    active=((state.stage==4)|(state.stage==5))&(variant==1)
    selected=active[:,None]&support.price
    owners=torch.arange(rows,device=device).repeat_interleave(999)
    # 不适用的坐标用合法价格1、数量1计算占位；不改真实支持或金额。
    coordinates=torch.arange(1,1000,device=device)[None,:].expand(rows,-1)
    k=numbers.ExactInt.from_i64_tensor(torch.where(selected,coordinates,1).reshape(-1))
    quantity=_take(state.quantity,owners)
    quantity=quantity.select(selected.reshape(-1),quantity.constant(1))
    unit=_take(actor._unit(side),owners)
    price=numbers.ExactRational.ratio(k*unit,k.constant(10000))
    rate=take_ratio(actor.context.rules.values,torch.full_like(owners,2))
    fee=market_fee_estimate(quantity,price,rate,actor.exponent)
    projected,error=fee.project(fee.constant(100000),operation=1)
    values=torch.where(selected,projected.reshape(rows,999),0.)
    fault=state.fault|support.fault
    fault|=numbers.combine_fault_rows(torch.where(selected,error.reshape(rows,999),0))
    return values,fault
