"""原撤单184列的设备末端投影；资格、对象顺序及资金事实由公共模块提供。

只消费同cutoff的原意图关联、公开订单、选中来源和对象作用域请求。
输出沿完整全局ordinal，物理padding没有动作资格；不按family重新编号。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows
from registry import identity_matches
from cancel_objects import global_cancel_slots
from cancel_projection import frozen_cancellation_outcomes
from input_orders import feedback_time


@dataclass(frozen=True)
class CancelInputs:
    values: torch.Tensor       # [roots,physical_width,184]，present-first全局坐标。
    present: torch.Tensor
    physical_slots: torch.Tensor
    fault: torch.Tensor

    def snapshot(self):
        """图外发布当次真实捕获输出；下一次暂存重放不得覆盖旧观察。"""
        return CancelInputs(self.values.clone(),self.present.clone(),self.physical_slots.clone(),self.fault.clone())


def cancel_inputs(objects,directory,intents,orders,sources,funds,funds_read,bank,
                  requests,cutoff,order_markets,order_sides,market_conditions,
                  scope_conditions,scope_present):
    """requests必须是原action_snapshot按每个物理对象scope冻结的读面。

    order_markets/order_sides只来自截止公开身份绑定，未知为-1。
    scope_conditions[:,0/1]保原current/old选择；不从对象反推当前市场。
    原ModelIntent与NOE bank共享只读，发布者必须保持其消费生命周期。
    Numbers.device_exact_batch负责精确字容量共同接纳；本函数不提交业务状态。
    """
    roots,count=orders.present.shape; width=objects.present.shape[1];device=cutoff.device
    if (cutoff.shape!=(roots,) or cutoff.dtype!=torch.int64 or
            any(x.shape!=(roots,count) or x.dtype!=torch.int64 or x.device!=device
                for x in (order_markets,order_sides)) or scope_conditions.shape!=scope_present.shape
            or scope_conditions.shape[:2]!=(roots,2) or requests.pending_known.shape!=(roots,width)):
        raise TypeError('撤单184需要同设备原订单绑定、current/old人口和逐对象请求读面')
    if intents.terms is None or intents.term_leg_slots is None:
        raise ValueError('撤单条款需要实际原冻结ModelIntent及Accepted关联索引')
    result=torch.zeros((roots,width,184),device=device)
    fault=objects.fault|orders.fault|sources.fault|funds_read.fault|requests.fault
    slots=global_cancel_slots(objects)
    kinds=objects.kinds
    members=objects.members
    # 原订单模板只计算一次，Single/Pair读取它，不复制业务归约。
    templates=torch.zeros((roots,count,68),device=device)
    needed=(members&((kinds==0)|(kinds==1))[:,:,None]&objects.present[:,:,None]).any(1)
    def triple(value,known,applicable,shape,*,linear=False,enabled=None):
        nonlocal fault
        projected,error=value.project(operation=0 if linear else 1)
        enabled=applicable if enabled is None else enabled
        fault=fault|Numbers.combine_fault_rows(torch.where(enabled,error.reshape(shape),0).flatten(1))
        return torch.stack((torch.where(known&applicable,projected.reshape(shape),0.),
                            (known&applicable).float(),applicable.float()),-1)
    def ratio(integer):return ExactRational(integer,integer.constant(1))
    all_order=torch.ones_like(orders.present)
    template_condition=torch.full((roots,count),-1,dtype=torch.int64,device=device)
    market_count=market_conditions.shape[1]
    if market_count:
        valid=(order_markets>=0)&(order_markets<market_count)
        template_condition=torch.where(valid,market_conditions.gather(1,order_markets.clamp(0,market_count-1)),-1)
    location=(template_condition>=0)&((order_sides==0)|(order_sides==1))
    templates[:,:,0]=(location&(order_sides==0)).float()
    templates[:,:,1]=(location&(order_sides==1)).float()
    templates[:,:,2]=location.float();templates[:,:,3]=1.
    origin=intents.origin_known&intents.present
    leg_count=intents.terms.variants.shape[1]
    if leg_count and count:
        at=intents.term_leg_slots.clamp(0,leg_count-1)
        index=(torch.arange(roots,device=device)[:,None]*leg_count+at).flatten()
        terms=intents.terms;variant=terms.variants.gather(1,at)
        for code in range(4):templates[:,:,4+code]=(origin&(variant==code)).float()
        templates[:,:,8]=origin.float();templates[:,:,9]=1.
        limit=origin&(variant<=1)
        post=terms.post_only.gather(1,at)
        templates[:,:,10]=(limit&~post).float();templates[:,:,11]=(limit&post).float()
        templates[:,:,12]=limit.float()
        templates[:,:,13]=torch.where(origin,limit,
            orders.active_applicability_known&orders.active_applicable).float()
        price=take_ratio(terms.price,index)
        price=price*ratio(price.numerator.constant(2))-ratio(price.numerator.constant(1))
        templates[:,:,14:17]=triple(price,origin,all_order,(roots,count),linear=True,enabled=needed&origin)
        templates[:,:,17:20]=triple(intents.original_target,origin,all_order,(roots,count),enabled=needed&origin)
        quote=origin&((variant==2)|(variant==3))
        spend_applicable=quote|(~origin&orders.active_applicability_known&~orders.active_applicable)
        templates[:,:,20:23]=triple(take_ratio(terms.spend,index),quote,spend_applicable,(roots,count),enabled=needed&quote)
        expiry=ExactInt.from_i64_tensor(terms.expiry_seconds.gather(1,at).flatten())
        now=ExactInt.from_i64_tensor(cutoff[:,None].expand(roots,count).flatten())
        remaining=(expiry-expiry.constant(60))*expiry.constant(1000)-now
        expires=ExactRational(remaining,remaining.constant(1000))
        gtd=origin&(variant==1)
        templates[:,:,47:50]=triple(expires,gtd,gtd,(roots,count),enabled=needed&gtd)
        templates[:,:,50]=(origin|orders.active_applicability_known).float()
        templates[:,:,51]=templates[:,:,50]
        templates[:,:,53]=(origin|(~origin&orders.active_applicability_known&~orders.active_applicable)).float()
        invalid=origin&((intents.term_leg_slots<0)|(intents.term_leg_slots>=leg_count)|(variant<0)|(variant>3))
        fault=fault|(needed&invalid).any(1).to(torch.int32)
    else:
        fault=fault|(needed&origin).any(1).to(torch.int32)
        templates[:,:,9]=1.;templates[:,:,16]=1.;templates[:,:,19]=1.
        templates[:,:,13]=(orders.active_applicability_known&orders.active_applicable).float()
        templates[:,:,22]=(orders.active_applicability_known&~orders.active_applicable).float()
        templates[:,:,50]=orders.active_applicability_known.float();templates[:,:,51]=templates[:,:,50]
        templates[:,:,53]=(orders.active_applicability_known&~orders.active_applicable).float()
    templates[:,:,52]=orders.active_applicability_known.float()
    templates[:,:,23:26]=triple(orders.matched,orders.matched_known,all_order,(roots,count),enabled=needed&orders.matched_known)
    templates[:,:,26:29]=triple(orders.active,orders.active_known,
        orders.active_applicability_known&orders.active_applicable,(roots,count),enabled=needed&orders.active_known)
    fund_count=funds.occupied.shape[1]
    principal=fee=ExactRational(intents.original_target.numerator.constant(0),intents.original_target.denominator.constant(1))
    fund_known=torch.zeros_like(orders.present)
    if count and fund_count:
        same,error=identity_matches(intents.identities,funds.keys,roots,count,fund_count)
        member=same&funds.occupied[:,None,:]&orders.present[:,:,None]
        found=member.any(2);at=member.to(torch.int32).argmax(2)
        index=(torch.arange(roots,device=device)[:,None]*fund_count+at).flatten()
        principal=take_ratio(funds_read.principal,index);fee=take_ratio(funds_read.fee,index)
        fund_known=found&funds_read.known.gather(1,at)
        fault=fault|Numbers.combine_fault_rows(torch.where(member,error,0).flatten(1))
    for offset,value in ((29,principal),(32,fee)):
        templates[:,:,offset:offset+3]=triple(value,fund_known,all_order,(roots,count),enabled=needed&fund_known)
    for offset,source in ((35,sources.formed),(41,sources.latest_direct)):
        value,error=feedback_time(source,cutoff,orders.present)
        templates[:,:,offset:offset+6]=value;fault=fault|error
    templates[:,:,54:57]=torch.stack((torch.where(orders.cancellable_known,orders.cancellable,False).float(),
        orders.cancellable_known.float(),all_order.float()),-1)
    outcomes,cancel_known,error=frozen_cancellation_outcomes(bank,sources.latest_cancel)
    fault=fault|error
    for code in range(3):templates[:,:,57+code]=(cancel_known&(outcomes==code)).float()
    templates[:,:,60]=cancel_known.float();templates[:,:,61]=1.
    value,error=feedback_time(sources.latest_cancel,cutoff,sources.latest_cancel.present)
    templates[:,:,62:68]=value;fault=fault|error
    # 全局轴在最后一次同步gather；Pair模板仍按原item ordinal，不按形成顺序排腿。
    condition=torch.full((roots,width),-1,dtype=torch.int64,device=device)
    if count:
        single=members.to(torch.int32).argmax(2)
        condition=template_condition.gather(1,single)
        selected=templates.gather(1,single[:,:,None].expand(roots,width,68))
        result[:,:,6:74]=torch.where((kinds==0)[:,:,None],selected,0.)
        if directory.paired_members.shape[1]:
            pair=objects.origin_pairs.clamp(0,directory.paired_members.shape[1]-1)
            for leg in range(2):
                order=directory.paired_members[:,:,leg].gather(1,pair).clamp(0,count-1)
                selected=templates.gather(1,order[:,:,None].expand(roots,width,68))
                result[:,:,6+leg*68:74+leg*68]+=torch.where((kinds==1)[:,:,None],selected,0.)
                if leg==0:condition=torch.where(kinds==1,template_condition.gather(1,order),condition)
    if market_count:
        condition=torch.where(kinds==2,market_conditions.gather(1,objects.markets.clamp(0,market_count-1)),condition)
    for scope in range(2):
        matches=((condition[:,:,None]==scope_conditions[:,scope,None,:])&scope_present[:,scope,None,:]).any(2)&(condition>=0)
        result[:,:,3+scope]=matches.float()
    result[:,:,5]=result[:,:,3]+result[:,:,4]
    for kind in range(3):result[:,:,kind]=(kinds==kind).float()
    # Market聚合保原NA/Unknown传播；任一成员缺outcome令该市场两腿均Unknown。
    for side in range(2):
        selected=members&(order_sides[:,None,:]==side)
        known=~(members&((order_sides<0)|(order_sides>1))[:,None,:]).any(2)
        count_value=ratio(ExactInt.from_i64_tensor(selected.to(torch.int64).sum(2).flatten()))
        values=[count_value]
        if count:
            index=(torch.arange(roots,device=device)[:,None,None]*count+torch.arange(count,device=device)[None,None,:]).expand(roots,width,count).flatten()
            for source in (orders.active,principal,fee):
                value=take_ratio(source,index);zero=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
                values.append(sum_rows(select(value,selected.flatten(),zero),roots*width,count))
        else:values.extend([count_value]*3)
        knowns=[known,known&~(selected&(~orders.active_known|~orders.active_applicable)[:,None,:]).any(2),
                known&~(selected&~fund_known[:,None,:]).any(2)]
        for offset,value,k in zip((142,148,154,160),values,(*knowns,knowns[2])):
            enabled=(kinds==2)&objects.present
            encoded=triple(value,k,torch.ones_like(k),(roots,width),enabled=enabled&k)
            result[:,:,offset+side*3:offset+side*3+3]=torch.where(enabled[:,:,None],encoded,0.)
    pending=ratio(requests.pending_count)
    result[:,:,166:169]=triple(pending,requests.pending_known,torch.ones_like(requests.pending_known),(roots,width))
    counts=ratio(requests.response_counts)
    values,error=counts.project(operation=1)
    applicable=requests.counts_applicable[:,:,None].expand(roots,width,3)
    result[:,:,169:178]=torch.stack((torch.where(applicable&requests.counts_known,values.reshape(roots,width,3),0.),
        (applicable&requests.counts_known).float(),applicable.float()),-1).flatten(2)
    fault=fault|Numbers.combine_fault_rows(torch.where(applicable,error.reshape(roots,width,3),0).flatten(1))
    value,error=feedback_time(requests.latest_response,cutoff,~requests.latest_known|requests.has_latest,
        source_at=requests.latest_response.responded_at)
    result[:,:,178:184]=torch.where(requests.latest_known[:,:,None],value,
        torch.cat((torch.zeros_like(value[:,:,:5]),torch.ones_like(value[:,:,:1])),2))
    fault=fault|error
    return CancelInputs(result.gather(1,slots[:,:,None].expand(roots,width,184)),
        objects.present.gather(1,slots),slots,fault)
