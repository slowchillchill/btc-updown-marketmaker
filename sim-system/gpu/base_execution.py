"""原SIM Base订单的设备撮合、计量与账户候选装配。

输入是已经编码的原订单terms与原资产队列。请求准入、时钟、最终性路径和
NOE可见交付由各自公共协调器负责；本层不把接单当作成交经济应用。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, ExactInt, _take
from asset_buy import plan_asset_limit, apply_asset_limit, take_ratio
from taker_fee import allocate_taker_fee
from order_quantity import BaseOrderQuantity


@dataclass(frozen=True)
class PreparedBaseOrder:
    source_account: object
    asset_plan: object
    quantity: object
    fee: object
    asset_indices: torch.Tensor
    requested: torch.Tensor
    fault: torch.Tensor

    def accept(self, queue, account):
        if account is not self.source_account:
            raise ValueError('订单计划须与规划时的同一账户候选绑定')
        principal = self.quantity.initial_principal()
        required = self.quantity.accepted_quote_units+self.fee.total_micro
        account, delta, rejected = account.accept_order(principal,self.fee.total_micro,required,
            self.requested & (self.fault == 0))
        # 原接纳失败不消耗盘口；纯候选重试仍从同一source_queue/source_account开始。
        queue = apply_asset_limit(queue,self.asset_plan,delta.emitted)
        fault = self.fault | delta.fault
        return queue,account,delta,rejected,fault


def prepare_base_order(queue, account, ask_rows, accepted_base, accepted_quote, maximum_price,
                       fee_rate, fee_exponent, asset_indices, requested):
    """原LimitBase计划与逐订单费用；数量/编码域不在本层缩到机器整数。"""
    plan = plan_asset_limit(queue,ask_rows,accepted_base,accepted_quote,maximum_price)
    roots, width = ask_rows.shape
    # 各原ask行的段顺序与规划fill顺序一致；未成交槽仅为物理尾补齐。
    safe = ask_rows.clamp(0,max(queue.rows-1,0))
    if queue.rows == 0:
        base = accepted_base.constant(0)
        price = maximum_price
        fee = allocate_taker_fee(base,price,fee_rate,fee_exponent,roots,1)
    else:
        indices = (safe[:,:,None]*queue.capacity+torch.arange(queue.capacity,device=ask_rows.device)).flatten()
        base = _take(plan.queue_plan.delta_base,indices)
        present = (ask_rows >= 0).repeat_interleave(queue.capacity,1).flatten()
        base = base.select(present,base.constant(0))
        row_indices = safe.repeat_interleave(queue.capacity,1).flatten()
        price = take_ratio(queue.price,row_indices)
        fee = allocate_taker_fee(base,price,fee_rate,fee_exponent,roots,width*queue.capacity)
    quantity = BaseOrderQuantity.from_taker_plan(accepted_base,accepted_quote,plan.matched_base,plan.used_quote)
    fault = plan.fault | fee.fault | quantity.fault
    if asset_indices.shape != (roots,) or requested.shape != (roots,) or requested.dtype != torch.bool:
        raise ValueError('订单资产/请求布局不一致')
    return PreparedBaseOrder(account,plan,quantity,fee,asset_indices,requested,fault)


def reduce_order_maker(quantity, queue_effects, order_ids):
    """按原账户段身份接入实际maker depletion；不丢掉QueueEffects。"""
    roots = order_ids.numel()
    if queue_effects.order_ids.shape[0] != roots:
        raise ValueError('本接口须已按订单所属根选定原价位行')
    total = quantity.open_base_units.constant(0)
    for column in range(queue_effects.order_ids.shape[1]):
        index = torch.arange(roots,device=order_ids.device)*queue_effects.order_ids.shape[1]+column
        amount = _take(queue_effects.maker_base_units,index)
        selected = queue_effects.order_ids[:,column] == order_ids
        total = total+amount.select(selected,amount.constant(0))
    quantity = replace(quantity,fault=quantity.fault | queue_effects.fault)
    return quantity.maker_depletion(total,total.sign > 0)
