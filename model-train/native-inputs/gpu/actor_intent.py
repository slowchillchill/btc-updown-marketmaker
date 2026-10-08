"""原合法终态的设备正文装配；复用公共逐腿条款、LIMIT成本及撤单作用域。"""
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from buy_cost import limit_buy_cost
from commitments import SubmittedLegs
from intent_terms import SubmittedBuyTerms
from cancel_requests import CancellationScopes
from cancel_objects import global_cancel_slots
from action_selection import SelectedAction
from asset_buy import take_ratio


def selected_action(candidate,context,control):
    roots=context.cutoff.numel();device=context.cutoff.device
    for value in (control.decisions,control.global_requests,control.execute_at):
        if value.shape!=(roots,) or value.dtype!=torch.int64 or value.device!=device:
            raise TypeError('选择发布需要同根原控制面身份和实际Execute时间')
    if control.leg_requests.shape!=(roots,2) or control.leg_requests.dtype!=torch.int64 or control.leg_requests.device!=device:
        raise TypeError('选择发布需要原逐腿Ledger请求身份字典')
    p=candidate.prefix;family=p.family
    root=torch.arange(roots,device=device)
    sides=torch.where((family==1)[:,None],p.single_side.clamp_min(0)[:,None],
        torch.arange(2,device=device)[None,:]).expand(roots,2)
    index=(root[:,None]*2+sides).flatten()
    n=_take(p.quantity,root.repeat_interleave(2))
    k=ExactInt.from_i64_tensor(p.prices.gather(1,sides).clamp_min(1).flatten())
    ticks=_take(context.tick_units,index)
    price=ExactRational.ratio(k*ticks,n.constant(10000))
    size=ExactRational.ratio(n,n.constant(100))
    spend=size*price
    variants=p.variants.gather(1,sides)
    present=((family==2)[:,None] | ((family==1)[:,None]&
        (torch.arange(2,device=device)[None,:]==0))).expand(roots,2)
    # 未被买入使用的行只提供合法算术占位，不建立正文腿或费用事实。
    safe_n=n.select(present.flatten(),n.constant(1))
    cost=limit_buy_cost(safe_n,price,
        take_ratio(context.rules.values,torch.full((roots*2,),2,device=device)),
        context.rules.context['fee_exponent'],post_only=torch.zeros(roots*2,dtype=torch.bool,device=device),
        minimum_principal_micro=(take_ratio(context.rules.limit_minimum,torch.zeros(roots*2,dtype=torch.int64,device=device)).numerator*
            n.constant(1000000)).divrem(take_ratio(context.rules.limit_minimum,
                torch.zeros(roots*2,dtype=torch.int64,device=device)).denominator)[0])
    principal=ExactRational.ratio(cost.principal_micro,cost.principal_micro.constant(1000000))
    fee_units=cost.fee_1e5_units.select((variants==0).flatten()&present.flatten(),cost.fee_1e5_units.constant(0))
    fee=ExactRational.ratio(fee_units,fee_units.constant(100000))
    quoted=(variants==1).flatten()
    target=ExactRational(spend.numerator.select(quoted,size.numerator),spend.denominator.select(quoted,size.denominator))
    zero=spend.numerator.constant(0)
    terms=SubmittedBuyTerms(torch.where(variants==1,2,0),torch.zeros_like(present),price,
        ExactRational(spend.numerator.select(quoted,zero),spend.denominator),torch.zeros_like(variants))
    conditions=context.identities[:,2,None].expand(roots,2)
    legs=SubmittedLegs(present,torch.zeros_like(present),control.global_requests[:,None].expand(roots,2),
        torch.arange(2,device=device)[None,:].expand(roots,2),target,present,conditions,sides,present,terms)
    count=context.cancels.present.size(1);orders=context.cancels.members.size(2)
    cancel=(family>=3)&(family<=5)
    physical=torch.zeros_like(family)
    members=torch.zeros((roots,1,orders),dtype=torch.bool,device=device)
    market=torch.full_like(family,-1)
    ordered=torch.zeros((roots,2),dtype=torch.int64,device=device)
    if count:
        physical=global_cancel_slots(context.cancels).gather(1,p.cancel_index.clamp_min(0)[:,None])[:,0]
        members=context.cancels.members[root,physical][:,None,:] & cancel[:,None,None]
        market_slot=context.cancels.markets[root,physical]
        if context.condition_slots is not None and context.condition_slots.numel():
            market=context.condition_slots[market_slot.clamp_min(0)]
        if orders:
            ordered[:,0]=members[:,0,:].to(torch.int32).argmax(1)
        if context.paired_members is not None and context.paired_members.size(1):
            pair=context.cancels.origin_pairs[root,physical].clamp_min(0)
            pair_members=context.paired_members[root,pair]
            ordered=torch.where((family==4)[:,None],pair_members,ordered)
    if context.order_identities is None:
        if orders:raise ValueError('发布撤单正文需要同cutoff原公开订单身份轴')
        cancel_orders=ExactInt.from_i64_tensor(torch.zeros(roots*2,dtype=torch.int64,device=device))
    else:
        cancel_orders=_take(context.order_identities,(root[:,None]*orders+ordered).flatten()) if orders else ExactInt.from_i64_tensor(
            torch.zeros(roots*2,dtype=torch.int64,device=device))
    if context.paired_members is None and count:
        # Pair不能用布尔members或formation_ranks重造腿序。
        missing=(family==4).to(torch.int32)
    else:missing=torch.zeros_like(candidate.fault)
    cancel_present=(family==3)[:,None]&(torch.arange(2,device=device)[None,:]==0)
    cancel_present |= (family==4)[:,None]
    scope=CancellationScopes((family==5)[:,None],market[:,None],members)
    fault=candidate.fault | context.fault | missing
    fault |= Numbers.combine_fault_rows(torch.where(present,(principal.fault|fee.fault).reshape(roots,2),0))
    return SelectedAction(context.identities,control,family,legs,context.assets.gather(1,sides),principal,fee,
        scope,cancel_orders,cancel_present,fault)
