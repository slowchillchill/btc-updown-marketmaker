"""原公开返佣六项读面；事实与有限归属不在读取时重新分配。

估计来自Trade详情，未成熟/门槛以下来自贡献事实，确认未付/已付/locked
来自已冻结日归属。Payment只选择已形成payable的互补端点。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt, ExactRational, Numbers
from asset_buy import take_ratio
from success_pool import select
from rebate_day import sum_population


@dataclass(frozen=True)
class RebateRead:
    estimated: object
    accrued_unmatured: object
    below_threshold: object
    confirmed_unpaid: object
    paid: object
    locked: object
    estimated_known: torch.Tensor  # [roots,Maker/Taker]
    amounts_known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ConfirmedRebateRead:
    confirmed_unpaid: object
    paid: object
    locked: object
    amounts_known: torch.Tensor
    fault: torch.Tensor


def read_confirmed_rebates(admission,scope_conditions,scope_present,known_prefix):
    """只消费已归属Day金额，不读取Trade估计或要求成交详情先到。"""
    roots=admission.fault.numel();device=admission.fault.device
    if (known_prefix.shape!=(roots,) or known_prefix.dtype!=torch.bool or known_prefix.device!=device
            or scope_conditions.ndim!=2 or scope_conditions.size(0)!=roots or scope_conditions.dtype!=torch.int64
            or scope_conditions.device!=device or scope_present.shape!=scope_conditions.shape
            or scope_present.dtype!=torch.bool or scope_present.device!=device):
        raise TypeError('确认返佣读面需同根scope与实际完整返佣历史')
    integer=ExactInt.from_i64_tensor(torch.zeros(roots*2,dtype=torch.int64,device=device))
    zero=ExactRational(integer,integer.constant(1));outputs=[zero]*3;fault=admission.fault.clone()
    day=admission.rebate_days
    if day is not None:
        kinds=torch.arange(2,device=device)
        width,conditions=day.occupied.size(1),day.condition_slots.size(1)
        scope=((day.condition_slots[:,:,None]==scope_conditions[:,None,:])&scope_present[:,None,:]).any(2)
        selected=day.occupied[:,None,:,None]&scope[:,None,None,:]&(
            day.kinds[:,None,:,None]==kinds[None,:,None,None])
        below=day.accrued.compare(ExactRational(day.accrued.numerator.constant(1),day.accrued.denominator.constant(1)))
        selected &= below.order.reshape_as(day.occupied)[:,None,:,None]>=0
        base=torch.arange(roots,device=device)[:,None,None,None]*width*conditions
        index=(base+torch.arange(width,device=device)[None,None,:,None]*conditions+
            torch.arange(conditions,device=device)[None,None,None,:]).expand(roots,2,width,conditions).flatten()
        outputs=[]
        for value,mask in [(day.condition_payable,selected&~day.payment_observed[:,None,:,None]),
                (day.condition_payable,selected&day.payment_observed[:,None,:,None]),(day.condition_locked,selected)]:
            amount=take_ratio(value,index)
            absent=ExactRational(amount.numerator.constant(0),amount.denominator.constant(1))
            outputs.append(sum_population(select(amount,mask.flatten(),absent),roots*2,width*conditions))
        fault|=day.fault|Numbers.combine_fault_rows(below.fault.reshape_as(day.occupied))
    for value in outputs:fault|=Numbers.combine_fault_rows(value.fault.reshape(roots,2))
    return ConfirmedRebateRead(*outputs,known_prefix[:,None].expand(roots,2),fault)


def read_rebates(admission, scope_conditions, scope_present, known_prefix):
    roots, capacity = admission.rebate_contributions.occupied.shape
    device = admission.fault.device
    if known_prefix.shape != (roots,) or known_prefix.dtype != torch.bool or known_prefix.device != device:
        raise TypeError('返佣读面须显式声明同设备原完整冷前缀可用性')
    estimated = admission.trade_success.estimated_rebate(admission.trades,
        admission.rebate_contributions,scope_conditions,scope_present)
    contribution = admission.rebate_contributions
    kinds = torch.arange(2, device=device)
    scope = ((contribution.conditions[:, :, None] == scope_conditions[:, None, :]) &
        scope_present[:, None, :]).any(2)
    eligible = contribution.occupied[:, None, :] & scope[:, None, :] & (
        contribution.kinds[:, None, :] == kinds[None, :, None])
    # 每个贡献只归一个真实(kind,day)。日身份目录没有结果时仍是未成熟贡献。
    finalized = torch.zeros_like(contribution.occupied)
    small_day = torch.zeros_like(finalized)
    day = admission.rebate_days
    if day is not None:
        matches = contribution.occupied[:, :, None] & day.occupied[:, None, :] & (
            contribution.kinds[:, :, None] == day.kinds[:, None, :]) & (
            contribution.days[:, :, None] == day.days[:, None, :])
        finalized = matches.any(2)
        below = day.accrued.compare(ExactRational(day.accrued.numerator.constant(1),day.accrued.denominator.constant(1)))
        small_day = (matches & (below.order.reshape_as(day.occupied)[:, None, :] < 0)).any(2)
    positions = (torch.arange(roots, device=device)[:, None, None]*capacity +
        torch.arange(capacity, device=device)[None, None, :]).expand(roots,2,capacity).flatten()
    values = take_ratio(contribution.amounts, positions)
    zero = ExactRational(values.numerator.constant(0), values.denominator.constant(1))
    accrued = sum_population(select(values,(eligible & ~finalized[:, None, :]).flatten(),zero),roots*2,capacity)
    threshold = sum_population(select(values,(eligible & small_day[:, None, :]).flatten(),zero),roots*2,capacity)
    confirmed=read_confirmed_rebates(admission,scope_conditions,scope_present,known_prefix)
    unpaid,paid,locked=confirmed.confirmed_unpaid,confirmed.paid,confirmed.locked
    fault = admission.fault | estimated.fault | contribution.fault | confirmed.fault
    for value in [accrued,threshold,unpaid,paid,locked]:
        fault |= Numbers.combine_fault_rows(value.fault.reshape(roots,2))
    return RebateRead(estimated.amount,accrued,threshold,unpaid,paid,locked,
        estimated.known & known_prefix[:,None],confirmed.amounts_known,fault)
