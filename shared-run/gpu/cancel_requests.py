"""原撤单请求在对象作用域内的设备冻结读面。

请求完成与逐目标结果分别接纳。Unknown关联保留Unknown，不把未知
订单归属当成不相交，也不使用完成总数冒充对象内的目标人口。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,Numbers
from registry import identity_matches,choose_latest


@dataclass(frozen=True)
class CancellationScopes:
    market_kind: torch.Tensor  # [roots,scopes]，false表示明确订单ID集合。
    markets: torch.Tensor  # condition原身份字典坐标；不要求该市场已admitted。
    order_members: torch.Tensor  # [roots,scopes,order_identity_population]。


@dataclass(frozen=True)
class CancellationTargets:
    received: torch.Tensor  # [roots,targets]，每个实际原领取目标位置一次。
    requests: object
    scopes: CancellationScopes  # 原单目标Order scope，不能靠完成总数恢复。
    outcomes: torch.Tensor  # 原Canceled/NotCanceledNoEffect/EvidenceUnknownNoEffect=0/1/2。
    available_at: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ScopedCancellationRead:
    pending_count: object
    pending_known: torch.Tensor
    has_latest: torch.Tensor  # 有一个最新可能相关响应；known=false时仍不可当从未发生。
    latest_known: torch.Tensor
    response_counts: object
    counts_known: torch.Tensor  # [roots,scopes,3]；Unknown按各实际目标outcome传播。
    counts_applicable: torch.Tensor
    responded_at: torch.Tensor
    received_at: torch.Tensor
    fault: torch.Tensor


def scope_relation(left,right,order_markets,location_known):
    roots,left_count,_=left.order_members.shape
    right_count=right.market_kind.size(1)
    left_members=left.order_members[:,:,None,:]
    right_members=right.order_members[:,None,:,:]
    orders_intersect=(left_members&right_members).any(3)
    same_market=left.markets[:,:,None]==right.markets[:,None,:]
    left_to_market=(left_members&location_known[:,None,None,:]&
        (order_markets[:,None,None,:]==right.markets[:,None,:,None])).any(3)
    right_to_market=(right_members&location_known[:,None,None,:]&
        (order_markets[:,None,None,:]==left.markets[:,:,None,None])).any(3)
    left_unknown=(left_members&~location_known[:,None,None,:]).any(3).expand(roots,left_count,right_count)
    right_unknown=(right_members&~location_known[:,None,None,:]).any(3).expand(roots,left_count,right_count)
    lm,rm=left.market_kind[:,:,None],right.market_kind[:,None,:]
    both_market=lm&rm
    neither=~lm&~rm
    matches=torch.where(both_market,same_market,torch.where(neither,orders_intersect,
        torch.where(lm,right_to_market,left_to_market)))
    known=both_market|neither|matches|torch.where(lm,~right_unknown,~left_unknown)
    return matches,known


def freeze_scoped_cancel_requests(state,request_scopes,scopes,order_markets,location_known,targets,cutoff):
    roots,request_count=state.registered.shape
    scope_count=scopes.market_kind.size(1)
    width=state.completions.received.size(1)
    device=state.registered.device
    if cutoff.shape!=(roots,) or cutoff.dtype!=torch.int64 or cutoff.device!=device:
        raise TypeError('作用域冻结需要同设备可信int64[roots]cutoff')
    relation,known=scope_relation(scopes,request_scopes,order_markets,location_known)
    fault=state.completions.fault|targets.fault
    from cancel_objects import _sequence_fault
    for value in [state.completions.requests,state.completions.delivery_sequences,state.completions.record_ordinals]:
        fault=fault|_sequence_fault(value,state.completions.received)
    fault=fault|_sequence_fault(targets.requests,targets.received)
    fault=fault|((targets.received&((targets.outcomes<0)|(targets.outcomes>2))).any(1)).to(torch.int32)
    completed=torch.zeros_like(state.registered)
    member=torch.zeros((roots,request_count,width),dtype=torch.bool,device=device)
    if width:
        equal,error=identity_matches(state.requests,state.completions.requests,roots,request_count,width)
        visible=state.completions.received&(state.completions.available_at<=cutoff[:,None])
        member=equal&visible[:,None,:]
        completed=member.any(2)
        fault=fault|Numbers.combine_fault_rows(torch.where(member,error,0).reshape(roots,-1))
    pending=state.registered&~completed
    pending_count=ExactInt.from_i64_tensor((relation&pending[:,None,:]).to(torch.int64).sum(2).flatten())
    pending_known=~((~known)&pending[:,None,:]).any(2)
    has_latest=torch.zeros((roots,scope_count),dtype=torch.bool,device=device)
    latest_known=torch.ones_like(has_latest)
    counts=torch.zeros((roots,scope_count,3),dtype=torch.int64,device=device)
    counts_known=torch.ones_like(counts,dtype=torch.bool)
    responded=received=torch.zeros((roots,scope_count),dtype=torch.int64,device=device)
    if width:
        possible=(relation|~known)[:,:,:,None]&member[:,None,:,:]
        candidate=possible.any(2)
        order=torch.arange(width,device=device)[None,:].expand(roots,width)
        for sequence in [state.completions.record_ordinals,state.completions.delivery_sequences]:
            words=sequence.words.to(torch.int64)&0xffffffff
            for limb in [0,1]:
                key=words[:,limb].reshape(roots,width)
                order=order.gather(1,key.gather(1,order).argsort(dim=1,descending=True,stable=True))
        at,has_latest=choose_latest(candidate,order)
        latest_known=(possible&known[:,:,:,None]).any(2).gather(2,at[:,:,None])[:,:,0]|~has_latest
        responded=state.completions.responded_at.gather(1,at)
        received=state.completions.received_at.gather(1,at)
        root=torch.arange(roots,device=device)[:,None]
        from queues import _take
        latest_request=_take(state.completions.requests,(root*width+at).flatten())
        target_width=targets.received.size(1)
        if target_width:
            same,error=identity_matches(latest_request,targets.requests,roots,scope_count,target_width)
            target_relation,target_known=scope_relation(scopes,targets.scopes,order_markets,location_known)
            active=same&targets.received[:,None,:]&(targets.available_at<=cutoff[:,None])[:,None,:]&has_latest[:,:,None]
            invalid=(targets.outcomes<0)|(targets.outcomes>2)
            fault=fault|((invalid&targets.received).any(1)).to(torch.int32)
            fault=fault|Numbers.combine_fault_rows(torch.where(active,error,0).reshape(roots,-1))
            for outcome in range(3):
                population=active&(targets.outcomes==outcome)[:,None,:]
                counts[:,:,outcome]=(population&target_relation).to(torch.int64).sum(2)
                counts_known[:,:,outcome]=~(population&~target_known).any(2)
    counts_known=counts_known&latest_known[:,:,None]
    counts_applicable=has_latest
    return ScopedCancellationRead(pending_count,pending_known,has_latest,latest_known,
        ExactInt.from_i64_tensor(counts.flatten()),counts_known,counts_applicable,responded,received,fault)


@dataclass(frozen=True)
class CancellationSnapshot:
    state: object
    targets: CancellationTargets
    location_known: torch.Tensor
    needed_requests: torch.Tensor


def freeze_cancel_snapshot(state,request_scopes,objects,object_present,order_markets,location_known,
        retained_orders,targets,cutoff):
    """原action_snapshot的撤单needed投影；不改接纳中的完整Registry。

    retained_orders是原unfinished及needed提交outcomes的订单集合，由公开
    订单/逐腿目录设备装配。只保留在途、global latest、当前对象latest请求。
    """
    from dataclasses import replace
    roots,request_count=state.registered.shape
    width=state.completions.received.size(1)
    device=state.registered.device
    if request_count==0:
        return CancellationSnapshot(replace(state,completions=replace(state.completions,received=torch.zeros_like(state.completions.received))),
            replace(targets,received=torch.zeros_like(targets.received)),location_known&retained_orders,state.registered)
    relation,known=scope_relation(objects,request_scopes,order_markets,location_known)
    needed=state.registered.clone()
    completed=torch.zeros_like(needed)
    error=state.completions.fault
    if width:
        same,fault=identity_matches(state.requests,state.completions.requests,roots,request_count,width)
        visible=state.completions.received&(state.completions.available_at<=cutoff[:,None])
        member=same&visible[:,None,:]
        completed=member.any(2)
        needed=state.registered&~completed
        order=torch.arange(width,device=device)[None,:].expand(roots,width)
        for sequence in [state.completions.record_ordinals,state.completions.delivery_sequences]:
            words=sequence.words.to(torch.int64)&0xffffffff
            for limb in [0,1]:
                order=order.gather(1,words[:,limb].reshape(roots,width).gather(1,order).argsort(dim=1,descending=True,stable=True))
        # global与每个原对象独立选择原latest可能相关记录，Unknown也必须保留。
        global_candidate=member.any(1)[:,None,:]
        object_candidate=((relation|~known)[:,:,:,None]&member[:,None,:,:]).any(2)&object_present[:,:,None]
        candidate=torch.cat([global_candidate,object_candidate],1)
        at,present=choose_latest(candidate,order)
        request_at=member.to(torch.int32).argmax(1).gather(1,at)
        kept=torch.zeros_like(needed,dtype=torch.int64).scatter_add(1,request_at,present.to(torch.int64))>0
        needed=needed|kept
        completion_needed=(same&needed[:,:,None]).any(1)
        projected=replace(state.completions,received=visible&completion_needed)
        error=error|Numbers.combine_fault_rows(torch.where(member,fault,0).reshape(roots,-1))
    else:
        projected=state.completions
    target_count=targets.received.size(1)
    target_kept=torch.zeros_like(targets.received)
    referenced=torch.zeros_like(location_known)
    if target_count:
        same,fault=identity_matches(state.requests,targets.requests,roots,request_count,target_count)
        target_kept=(same&needed[:,:,None]).any(1)&targets.received&(targets.available_at<=cutoff[:,None])
        referenced=(targets.scopes.order_members&target_kept[:,:,None]).any(1)
        error=error|Numbers.combine_fault_rows(torch.where(same&needed[:,:,None],fault,0).reshape(roots,-1))
    explicit=(request_scopes.order_members&needed[:,:,None]).any(1)
    locations=location_known&(retained_orders|explicit|referenced)
    projected=replace(projected,fault=projected.fault|error)
    return CancellationSnapshot(replace(state,registered=state.registered&needed,completions=projected),
        replace(targets,received=target_kept),locations,needed)
