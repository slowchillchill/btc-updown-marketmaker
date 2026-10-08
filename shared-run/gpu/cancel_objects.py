"""原公开撤单对象的设备形成顺序与人口。

冷字典提供原身份/condition字符串的字典序坐标；业务证据、资格、
两条形成链的选择及拓扑推进都在设备，未知归属排在已知市场之后。
"""
from dataclasses import dataclass
import torch
from queues import Numbers


@dataclass(frozen=True)
class OrderFormation:
    evidence: torch.Tensor  # [roots,orders]，原公开订单证据存在。
    identity_order: torch.Tensor  # 完整原Identity字典序，不是sim数值seq排序。
    market: torch.Tensor  # 原已公开condition的槽；-1为未知/未公开。
    processing_known: torch.Tensor
    processing_at: torch.Tensor
    input_sequences: object  # 完整原receipt u64。
    item_ordinals: torch.Tensor
    source_known: torch.Tensor
    source_sequences: object  # 原formed private u64。
    fault: torch.Tensor


@dataclass(frozen=True)
class CancelObjectDirectory:
    market_present: torch.Tensor  # [roots,markets]，仅当前公开identity。
    market_start: torch.Tensor
    condition_order: torch.Tensor  # 原condition字符串冷字典序。
    requested: torch.Tensor  # cancel_objects_in原请求条件，不缩Single/Pair范围。
    paired_present: torch.Tensor  # [roots,pairs]，原两腿均Accepted的冻结SubmitPaired。
    paired_members: torch.Tensor  # [roots,pairs,2]，保原item ordinal。
    paired_origin_order: torch.Tensor  # 原request字符串字典序，保原稳定平局。


def assemble_cancel_directory(legs,bindings,intents,markets,request_identity_order):
    """同公开前缀的原完整双腿请求→Accepted订单；不按两个订单猜Pair。

    request_identity_order来自原请求身份字典序，与原腿轴一致；物理关联槽
    不能充当排序。每个ordinal0占一个Pair槽，成员按原ordinal0/1排列。
    """
    from registry import identity_matches
    roots,width=legs.present.shape;orders=intents.present.size(1)
    device=legs.present.device
    if (request_identity_order.shape!=(roots,width) or request_identity_order.dtype!=torch.int64
            or request_identity_order.device!=device):
        raise TypeError('Pair目录需要同原腿轴的请求身份字典序')
    same=(legs.request_slots[:,:,None]==legs.request_slots[:,None,:])
    population=legs.present & legs.receipt_known
    first=population & (legs.item_ordinals==0) & ((same & population[:,None,:]).sum(2)==2)
    second=same & population[:,None,:] & (legs.item_ordinals[:,None,:]==1)
    first &= second.sum(2)==1
    next_leg=second.to(torch.int32).argmax(2) if width else legs.item_ordinals
    binding=(legs.request_slots[:,:,None]==bindings.request_slots[:,None,:]) & (
        legs.item_ordinals[:,:,None]==bindings.item_ordinals[:,None,:])
    binding &= bindings.received[:,None,:] & bindings.accepted[:,None,:]
    if bindings.received.size(1):
        from queues import _take
        binding_at=binding.to(torch.int32).argmax(2)
        original=_take(bindings.orders,(torch.arange(roots,device=device)[:,None]*bindings.received.size(1)+binding_at).flatten())
        equal,error=identity_matches(original,intents.identities,roots,width,orders)
        matches=equal & intents.present[:,None,:] & binding.any(2)[:,:,None]
    else:
        matches=torch.zeros((roots,width,orders),dtype=torch.bool,device=device)
        error=torch.zeros_like(matches,dtype=torch.int32)
    known=(matches.sum(2)==1) & (binding.sum(2)==1)
    at=matches.to(torch.int32).argmax(2) if orders else torch.zeros_like(legs.item_ordinals)
    members=torch.stack((at,at.gather(1,next_leg)),2)
    present=first & known & known.gather(1,next_leg)
    fault=bindings.fault | Numbers.combine_fault_rows(torch.where(matches,error,0).reshape(roots,-1))
    fault |= (population & (request_identity_order<0)).any(1).to(torch.int32)
    fault |= (population & binding.any(2) & ~known).any(1).to(torch.int32)
    return CancelObjectDirectory(markets.market_present,markets.market_start,markets.condition_order,
        markets.requested,present,members,request_identity_order),fault


@dataclass(frozen=True)
class CancelObjectsRead:
    present: torch.Tensor  # [roots,orders+pairs+markets]，物理padding不参与人口。
    kinds: torch.Tensor  # 0=Single,1=Paired,2=Market。
    members: torch.Tensor  # [roots,objects,orders]，成员沿formation_ranks排序。
    formation_ranks: torch.Tensor  # [roots,orders]；非eligible为-1。
    origin_pairs: torch.Tensor  # Pair原目录槽；其余-1。
    markets: torch.Tensor  # Market原公开目录槽；其余-1。
    fault: torch.Tensor


def global_cancel_slots(objects):
    """原完整对象顺序的全局ordinal→物理槽；尾部padding没有业务ordinal。

    Single/Pair/Market共同压紧，不能按family重新编号。成员与Pair来源
    仍从原物理槽读取，不改变其腿序或形成顺序。
    """
    return (~objects.present).to(torch.int32).argsort(dim=1,stable=True)


def _sort(order, key):
    return order.gather(1,key.gather(1,order).argsort(dim=1,stable=True))


def _sort_u64(order, value, roots, width):
    words=value.words.to(torch.int64)&0xffffffff
    for limb in [0,1]:
        order=_sort(order,words[:,limb].reshape(roots,width))
    return order


def _sequence_fault(value, enabled):
    roots,width=enabled.shape
    bound=value.compare(value.constant((1<<64)-1))
    invalid=value.fault|bound.fault|((value.sign<0)|(bound.order>0)).to(torch.int32)
    return Numbers.combine_fault_rows(torch.where(enabled,invalid.reshape(roots,width),0))


def sim_identity_order(sequences, population):
    """同根Order命名空间的原sim-u64字典序，复用所属公共设备十进制原语。

    sequences是业务order序号而非物理槽。固定账户和Order前缀由根绑定，
    因此同根完整Identity排序与后缀字节排序等价，不回CPU拼动态字串。
    """
    from device_identity import sim_trade_order_key
    roots,width=population.shape
    keys=sim_trade_order_key(sequences)
    order=torch.arange(width,device=population.device)[None,:].expand(roots,width)
    for byte in range(19,-1,-1):
        order=_sort(order,keys.bytes.reshape(roots,width,20)[:,:,byte])
    ranks=order.argsort(dim=1)
    fault=Numbers.combine_fault_rows(torch.where(population,keys.fault.reshape(roots,width),0))
    return ranks,fault


def freeze_cancel_objects(order_read, formation, directory):
    roots,count=formation.evidence.shape
    device=formation.evidence.device
    market_count=directory.market_present.size(1)
    pair_count=directory.paired_present.size(1)
    fault=formation.fault|order_read.fault
    eligible=formation.evidence & order_read.present & ~(order_read.cancellable_known & ~order_read.cancellable)
    order=torch.arange(count,device=device)[None,:].expand(roots,count)
    market_order=torch.arange(market_count,device=device)[None,:].expand(roots,market_count)
    market_order=_sort(market_order,directory.condition_order)
    market_order=_sort(market_order,directory.market_start)
    market_order=_sort(market_order,(~directory.market_present).to(torch.int32))
    market_ranks=torch.empty_like(market_order).scatter(1,market_order,
        torch.arange(market_count,device=device)[None,:].expand(roots,market_count))
    if count==0:
        fault=fault|directory.paired_present.any(1).to(torch.int32)
        selected=_sort(market_order,(~directory.requested).to(torch.int32))
        present=torch.cat([torch.zeros_like(directory.paired_present),
            (directory.market_present&directory.requested).gather(1,selected)],1)
        kinds=torch.cat([torch.ones_like(directory.paired_present,dtype=torch.int64),torch.full_like(selected,2)],1)
        origin=torch.full_like(kinds,-1)
        locations=torch.cat([torch.full_like(directory.paired_present,-1,dtype=torch.int64),selected],1)
        return CancelObjectsRead(present,kinds,torch.zeros((roots,pair_count+market_count,0),dtype=torch.bool,device=device),
            order,origin,locations,fault)
    valid_market=(formation.market>=0)&(formation.market<market_count)
    if market_count:
        safe_market=formation.market.clamp(0,market_count-1)
        valid_market=valid_market & directory.market_present.gather(1,safe_market)
        groups=torch.where(valid_market,market_ranks.gather(1,safe_market),market_count)
    else:
        groups=torch.full_like(formation.market,0)
    # 每组只采用覆盖全部有序证据的一条链，否则合并两条链的相邻边。
    processing=eligible & formation.processing_known
    source=eligible & formation.source_known
    fault=fault|_sequence_fault(formation.input_sequences,processing)|_sequence_fault(formation.source_sequences,source)
    fault=fault|Numbers.combine_fault_rows(torch.where(processing,
        ((formation.item_ordinals<0)|(formation.item_ordinals>65535)).to(torch.int32),0))
    group_population=torch.zeros((roots,market_count+1),dtype=torch.int64,device=device)
    def population(mask):
        return group_population.scatter_add(1,groups,mask.to(torch.int64))
    evidenced=population(processing|source)
    complete_processing=(population(processing)==evidenced).gather(1,groups)
    complete_source=(population(source)==evidenced).gather(1,groups)
    chain_masks=[processing,source & ~complete_processing]
    chain_masks[0]=processing & (complete_processing|~complete_source)
    successors=[]
    for chain,mask in enumerate(chain_masks):
        sorting=_sort(order,formation.identity_order)
        if chain==0:
            sorting=_sort(sorting,formation.item_ordinals)
            sorting=_sort_u64(sorting,formation.input_sequences,roots,count)
            sorting=_sort(sorting,formation.processing_at)
        else:
            sorting=_sort_u64(sorting,formation.source_sequences,roots,count)
        sorting=_sort(sorting,(~mask).to(torch.int32))
        sorting=_sort(sorting,groups)
        successor=torch.full_like(order,-1)
        if count>1:
            before,after=sorting[:,:-1],sorting[:,1:]
            adjacent=mask.gather(1,before)&mask.gather(1,after)&(groups.gather(1,before)==groups.gather(1,after))
            successor=successor.scatter(1,before,torch.where(adjacent,after,-1))
        successors.append(successor)
    # 两条相同边只记一次；每个node至多两条后继，避免整图平方邻接矩阵。
    successors[1]=torch.where(successors[1]==successors[0],-1,successors[1])
    indegree=torch.zeros_like(order)
    for successor in successors:
        indegree=indegree.scatter_add(1,successor.clamp_min(0),(successor>=0).to(torch.int64))
    tie_order=_sort(_sort(order,formation.identity_order),groups)
    pending=eligible.clone()
    ranks=torch.full_like(order,-1)
    ordered=torch.zeros_like(order)
    emitted=torch.zeros_like(eligible)
    # 物理人口是循环界限；无额外业务深度/人口限制，不读设备条件回CPU。
    for ordinal in range(count):
        ready=pending & (indegree==0)
        selected=ready.gather(1,tie_order).to(torch.int32).argmax(1)
        at=tie_order.gather(1,selected[:,None])
        active=ready.any(1)
        previous=ranks.gather(1,at)
        ranks=ranks.scatter(1,at,torch.where(active[:,None],ordinal,previous))
        pending=pending.scatter(1,at,pending.gather(1,at)&~active[:,None])
        ordered[:,ordinal]=at[:,0]
        emitted[:,ordinal]=active
        for successor in successors:
            next_at=successor.gather(1,at)
            decrement=(active[:,None]&(next_at>=0)).to(torch.int64)
            indegree=indegree.scatter_add(1,next_at.clamp_min(0),-decrement)
    valid_pair=directory.paired_present.clone()
    if count:
        pair_in_range=((directory.paired_members>=0)&(directory.paired_members<count)).all(2)
        fault=fault|(valid_pair&~pair_in_range).any(1).to(torch.int32)
        members=directory.paired_members.clamp(0,count-1)
        pair_ranks=ranks.gather(1,members.flatten(1)).reshape(roots,pair_count,2)
        valid_pair=valid_pair&pair_in_range&(pair_ranks>=0).all(2)
    else:
        pair_ranks=torch.full((roots,pair_count,2),-1,dtype=torch.int64,device=device)
        valid_pair=torch.zeros_like(valid_pair)
        members=directory.paired_members
    pair_order=torch.arange(pair_count,device=device)[None,:].expand(roots,pair_count)
    pair_order=_sort(pair_order,directory.paired_origin_order)
    pair_order=_sort(pair_order,pair_ranks.max(2).values)
    pair_order=_sort(pair_order,pair_ranks.min(2).values)
    pair_order=_sort(pair_order,(~valid_pair).to(torch.int32))
    requested=directory.market_present&directory.requested
    selected_market=_sort(market_order,(~requested).to(torch.int32))
    population=torch.cat([emitted,valid_pair.gather(1,pair_order),requested.gather(1,selected_market)],1)
    kinds=torch.cat([torch.zeros_like(order),torch.ones_like(pair_order),torch.full_like(selected_market,2)],1)
    membership=torch.zeros((roots,count+pair_count+market_count,count),dtype=torch.bool,device=device)
    if count:
        membership[:,:count,:]=torch.zeros((roots,count,count),dtype=torch.bool,device=device).scatter(2,ordered[:,:,None],emitted[:,:,None])
        for leg in range(2):
            at=members[:,:,leg].gather(1,pair_order)
            membership[:,count:count+pair_count,:].scatter_(2,at[:,:,None],valid_pair.gather(1,pair_order)[:,:,None])
        membership[:,count+pair_count:,:]=(formation.market[:,None,:]==selected_market[:,:,None])&(ranks>=0)[:,None,:]&requested.gather(1,selected_market)[:,:,None]
    origin=torch.cat([torch.full_like(order,-1),pair_order,torch.full_like(selected_market,-1)],1)
    markets=torch.cat([torch.full_like(order,-1),torch.full_like(pair_order,-1),selected_market],1)
    return CancelObjectsRead(population,kinds,membership,ranks,origin,markets,fault)
