"""原公开订单、成交和承诺的设备冻结读取。

输入是首次领取的原NOE字段与冻结意图目录；不读SIM内部账户或队列。
消费位置和公开时刻由共享领取器保留，Ledger等待不延迟此公开读取。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt, ExactRational, Numbers, _take
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows

# 所属字段布局：Base接受、Quote接受、明确关闭、Trade、撤单目标响应、普通Query。
BASE_ACCEPTED, QUOTE_ACCEPTED, ORDER_LIFECYCLE, TRADE, CANCEL_TARGET, ORDER_QUERY = range(6)


@dataclass(frozen=True)
class PublicRegistryEvents:
    received: torch.Tensor  # [roots,events]，来自首次领取位置，不是Ledger applied。
    kinds: torch.Tensor
    orders: object
    trades: object
    private_sequences: object
    delivery_sequences: object
    record_ordinals: object
    batch_sequences: object
    context_slots: torch.Tensor
    run_slots: torch.Tensor
    request_slots: torch.Tensor
    item_ordinals: torch.Tensor
    record_refs: torch.Tensor  # NOE共享不可变bank引用；不是新的业务位置。
    available_at: torch.Tensor  # [roots,events]，原首次公开领取的available_at。
    receive_rank: torch.Tensor  # 物理领取顺序，不能替代业务u64序号。
    matched: object
    open_quantity: object
    trade_quantity: object
    roles: torch.Tensor  # 原Taker=0/MakerInferred=1。
    dispositions: torch.Tensor
    accepted_matched: torch.Tensor
    formation: torch.Tensor
    canceled: torch.Tensor
    lifecycle_closed: torch.Tensor
    platform_at: torch.Tensor  # 原source reference的平台事件/回应时刻。
    received_at: torch.Tensor  # 原source reference的实际反馈收到时刻。
    fault: torch.Tensor
    query_states: torch.Tensor | None = None
    query_quantity_kinds: torch.Tensor | None = None

    processing_at: torch.Tensor | None = None  # 原Accepted自身effective_processing_at，不是完整响应时刻。
    condition_slots: torch.Tensor | None = None  # 原公开condition共享字典坐标；未知为-1。

    def __post_init__(self):
        # 原非Query构造者不新增业务成员；新增列只在ORDER_QUERY人口内有意义。
        for name in ('query_states','query_quantity_kinds','processing_at','condition_slots'):
            value=getattr(self,name)
            if value is None:
                object.__setattr__(self,name,torch.full_like(self.kinds,-1))
            elif value.shape!=self.received.shape or value.dtype!=torch.int64 or value.device!=self.received.device:
                raise TypeError('Query公开字段须同设备int64[roots,events]')


    @classmethod
    def empty(cls,roots,device):
        columns=torch.empty((roots,0),dtype=torch.int64,device=device)
        blank=columns.bool()
        zero=ExactInt.from_i64_tensor(columns.flatten())
        ratio=ExactRational(zero,zero.constant(1))
        return cls(blank,columns,zero,zero,zero,zero,zero,zero,columns,columns,columns,columns,columns,columns,columns,
            ratio,ratio,ratio,columns,columns,blank,blank,blank,blank,columns,columns,torch.zeros(roots,dtype=torch.int32,device=device))


@dataclass(frozen=True)
class FrozenIntentOrders:
    present: torch.Tensor  # [roots,orders]，原已公开订单目录；不可提前暴露未来槽位。
    identities: object
    origin_known: torch.Tensor
    original_target: object
    quoted: torch.Tensor
    receipt_known: torch.Tensor
    terms: object | None = None  # 共享原SubmittedLegs条款，不从formed/Query反推。
    term_leg_slots: torch.Tensor | None = None


@dataclass(frozen=True)
class PublicOrdersRead:
    present: torch.Tensor
    matched: object
    matched_known: torch.Tensor
    active: object
    active_known: torch.Tensor
    active_applicable: torch.Tensor
    active_applicability_known: torch.Tensor
    commitment_present: torch.Tensor
    cancellable: torch.Tensor
    cancellable_known: torch.Tensor
    commitment: object
    commitment_known: torch.Tensor
    unfinished: torch.Tensor
    fault: torch.Tensor


def identity_matches(left, right, roots, left_width, right_width):
    root = torch.arange(roots, device=left.words.device)[:, None, None]
    a = (root*left_width+torch.arange(left_width, device=root.device)[None, :, None]).expand(roots,left_width,right_width)
    b = (root*right_width+torch.arange(right_width, device=root.device)[None, None, :]).expand(roots,left_width,right_width)
    relation = _take(left, a.flatten()).compare(_take(right, b.flatten()))
    return relation.order.reshape(roots,left_width,right_width)==0, relation.fault.reshape(roots,left_width,right_width)


def source_priority(sequence, receive_rank):
    roots, width = receive_rank.shape
    order = receive_rank.argsort(dim=1, stable=True)
    words = sequence.words.to(torch.int64)&0xffffffff
    for limb in [0,1]:
        key = words[:,limb].reshape(roots,width)
        order = order.gather(1,key.gather(1,order).argsort(dim=1,descending=True,stable=True))
    return order


def choose_latest(member, priority):
    roots, objects, width = member.shape
    ordered = member.gather(2,priority[:,None,:].expand(roots,objects,width))
    first = ordered.to(torch.int32).argmax(2)
    return priority.gather(1,first), member.any(2)


def query_selection(events,member):
    """首次领取按原delivery单调递增；首次终态锁定known_query，direct继续更新。

    Query数量只提供Base/Quote适用性，不能替代Submission measurement。
    """
    priority=delivery_priority(events.delivery_sequences,events.record_ordinals,events.receive_rank)
    queries=member & (events.kinds==ORDER_QUERY)[:,None,:]
    latest,known=choose_latest(queries,priority)
    terminal=queries & ((events.query_states>=1)&(events.query_states<=5))[:,None,:]
    first_terminal,closed=choose_latest(terminal,priority.flip(1))
    return torch.where(closed,first_terminal,latest),known,closed


def frozen_order_read(events, intents):
    """原GTC/Quote公开计量、最终性覆盖和未闭合人口；不生成提交或现金变化。"""
    roots, width = events.received.shape
    count = intents.present.size(1)
    device = events.received.device
    if intents.present.shape!=(roots,count) or intents.present.dtype!=torch.bool or intents.present.device!=device:
        raise TypeError('订单目录需要同设备bool[roots,orders]公开人口')
    if count==0:
        blank=intents.present
        return PublicOrdersRead(blank,intents.original_target,blank,intents.original_target,blank,blank,blank,blank,
            blank,blank,intents.original_target,blank,blank,events.fault)
    if width==0:
        blank=torch.zeros_like(intents.present)
        return PublicOrdersRead(intents.present,intents.original_target,blank,intents.original_target,blank,
            ~intents.quoted,intents.origin_known,intents.present & intents.receipt_known & intents.origin_known & (intents.original_target.numerator.sign!=0).reshape(roots,count),
            blank,blank,intents.original_target,intents.origin_known,intents.present,events.fault)
    same, comparison_fault = identity_matches(intents.identities,events.orders,roots,count,width)
    member = same & events.received[:,None,:] & intents.present[:,:,None]
    priority = source_priority(events.private_sequences,events.receive_rank)
    at, accepted = choose_latest(member & ((events.kinds==BASE_ACCEPTED)|(events.kinds==QUOTE_ACCEPTED))[:,None,:],priority)
    coordinates = (torch.arange(roots,device=device)[:,None]*width+at).flatten()
    initial = take_ratio(events.matched,coordinates)
    initial_open = take_ratio(events.open_quantity,coordinates)
    quote = accepted & (events.kinds.gather(1,at)==QUOTE_ACCEPTED)
    query_at,query_known,query_closed=query_selection(events,member)
    query_kind=events.query_quantity_kinds.gather(1,query_at)
    query_applicable=query_known & (query_kind!=2)
    base_applicable=torch.where(accepted,~quote,
        torch.where(query_applicable,query_kind==0,~intents.quoted))
    applicability_known=accepted|query_applicable|intents.origin_known
    life_at, life_known = choose_latest(member & (events.kinds==ORDER_LIFECYCLE)[:,None,:], priority)
    formed_at, formed_known = choose_latest(member & ((events.kinds==ORDER_LIFECYCLE)&events.formation)[:,None,:], priority)
    formed_matched = formed_known & events.accepted_matched.gather(1,formed_at)
    closed = query_closed | (life_known & events.lifecycle_closed.gather(1,life_at)) | (
        member & ((events.kinds==CANCEL_TARGET)&events.canceled)[:,None,:]).any(2)
    # Trade按完整身份的最高private保留一份；排序相邻键去重，不建历史平方矩阵。
    candidate = events.received & (events.kinds==TRADE)
    representative = first_identity_population(events.trades,torch.zeros_like(events.kinds),candidate,priority)
    quantity = events.trade_quantity
    roles = events.roles
    disposition = events.dispositions
    trade_to_order = same & representative[:,None,:]
    indices = (torch.arange(roots,device=device)[:,None,None]*width+
        torch.arange(width,device=device)[None,None,:]).expand(roots,count,width).flatten()
    values = take_ratio(quantity,indices)
    zero = ExactRational(values.numerator.constant(0),values.denominator.constant(1))
    def sum_selected(mask):
        return sum_rows(select(values,mask.flatten(),zero),roots*count,width)
    maker = sum_selected(trade_to_order & (roles==1)[:,None,:])
    taker = sum_selected(trade_to_order & (roles==0)[:,None,:])
    pending = sum_selected(trade_to_order & (disposition<2)[:,None,:])
    matched = select(initial,quote.flatten(),initial+maker)
    active = initial_open-maker
    empty = ExactRational(active.numerator.constant(0),active.denominator.constant(1))
    active = select(active,~closed.flatten(),empty)
    active_known = base_applicable & applicability_known & ((accepted & (active.numerator.sign>=0).reshape(roots,count)) | closed)
    cancellable_known = closed|quote|formed_matched|active_known
    cancellable = active_known & (active.numerator.sign>0).reshape(roots,count) & ~closed & ~quote & ~formed_matched
    coverage = taker.compare(initial)
    covered = (coverage.order==0).reshape(roots,count)
    all_final = ~(trade_to_order & (disposition<2)[:,None,:]).any(2)
    base_remaining = active+pending
    quote_finished = covered & all_final
    commitment = select(intents.original_target,quote.flatten(),base_remaining)
    commitment = select(empty,(quote & quote_finished).flatten(),commitment)
    commitment = select(commitment,accepted.flatten(),intents.original_target)
    commitment_known = (~accepted)|quote|(~quote & covered & active_known)
    commitment_known = commitment_known & intents.origin_known
    commitment_present = intents.present & intents.receipt_known & intents.origin_known & ~(
        commitment_known & (commitment.numerator.sign==0).reshape(roots,count))
    pending_any = (trade_to_order & (disposition<2)[:,None,:]).any(2)
    unfinished = intents.present & (~(cancellable_known & ~cancellable)|pending_any|
        ~(commitment_known & (commitment.numerator.sign==0).reshape(roots,count)))
    fault = events.fault.clone()
    for sequence in [events.orders,events.trades,events.private_sequences,events.delivery_sequences,events.record_ordinals,events.batch_sequences]:
        limit=sequence.compare(sequence.constant((1<<64)-1))
        invalid=sequence.fault | limit.fault | ((sequence.sign<0)|(limit.order>0)).to(torch.int32)
        fault=fault | Numbers.combine_fault_rows(torch.where(events.received,invalid.reshape(roots,width),0))
    invalid=(events.kinds<BASE_ACCEPTED)|(events.kinds>ORDER_QUERY)
    invalid=invalid | ((events.kinds==TRADE)&((events.roles<0)|(events.roles>1)|(events.dispositions<0)|(events.dispositions>4)))
    invalid=invalid | ((events.kinds==ORDER_QUERY)&((events.query_states<0)|(events.query_states>8)|
        (events.query_quantity_kinds<0)|(events.query_quantity_kinds>2)))
    fault=fault | Numbers.combine_fault_rows(torch.where(events.received,invalid.to(torch.int32),0))
    fault = fault | Numbers.combine_fault_rows(torch.where(member,comparison_fault,0).reshape(roots,-1))
    for value in [matched,active,commitment]:
        fault = fault | Numbers.combine_fault_rows(torch.where(intents.present,value.fault.reshape(roots,count),0))
    fault = fault | Numbers.combine_fault_rows(torch.where(accepted,coverage.fault.reshape(roots,count),0))
    return PublicOrdersRead(intents.present,matched,accepted,active,active_known,
        base_applicable,applicability_known,
        commitment_present,cancellable,cancellable_known,
        commitment,commitment_known,unfinished,fault)


def first_identity_population(key,category,population,priority):
    roots,width=population.shape
    device=population.device
    order=priority
    # namespace + 完整u64字典坐标；同private继续保留首次领取。
    words=key.words.to(torch.int64)&0xffffffff
    for limb in [0,1]:
        column=words[:,limb].reshape(roots,width)
        order=order.gather(1,column.gather(1,order).argsort(dim=1,stable=True))
    order=order.gather(1,category.gather(1,order).argsort(dim=1,stable=True))
    # 将不属于这次权威来源人口的padding放尾；不让它遮蔽真实来源。
    order=order.gather(1,(~population).gather(1,order).to(torch.int64).argsort(dim=1,stable=True))
    sorted_category=category.gather(1,order)
    sorted_words=words.reshape(roots,width,-1).gather(1,order[:,:,None].expand(roots,width,words.size(1)))
    same=torch.zeros((roots,width),dtype=torch.bool,device=device)
    if width>1:
        same[:,1:]=(sorted_category[:,1:]==sorted_category[:,:-1]) & (sorted_words[:,1:,:2]==sorted_words[:,:-1,:2]).all(2)
    selected=population.gather(1,order)&~same
    return torch.zeros_like(selected).scatter(1,order,selected)

def compact_order_events(events):
    """保留原Registry各身份的必要来源人口，不累计每次版本的整段历史。

    accepted、formed、latest lifecycle、明确撤成和每Trade最高private分别保留。
    一个事件同时是formed/latest时只存一份；旧冻结结果持其不可变张量。
    """
    from dataclasses import fields,replace
    roots,width=events.received.shape
    if width == 0:
        return events
    device=events.received.device
    priority=source_priority(events.private_sequences,events.receive_rank)
    key=ExactInt(events.orders.words,events.orders.sign,events.orders.fault)
    trade=events.kinds==TRADE
    key=events.trades.select(trade.flatten(),key)
    category=torch.where(events.kinds<=QUOTE_ACCEPTED,0,events.kinds-1)
    ordinary=events.received & (events.kinds!=ORDER_QUERY) & ((events.kinds!=CANCEL_TARGET)|events.canceled)
    keep=first_identity_population(key,category,ordinary,priority)
    keep=keep | first_identity_population(key,category,events.received & (events.kinds==ORDER_LIFECYCLE) & events.formation,priority)
    # 最新Direct按原领取delivery/record ordinal选择，三类取消结果均保留。
    direct=(events.kinds<=QUOTE_ACCEPTED)|(events.kinds==CANCEL_TARGET)|(events.kinds==ORDER_QUERY)
    direct_priority=delivery_priority(events.delivery_sequences,events.record_ordinals,events.receive_rank)
    keep=keep | first_identity_population(events.orders,torch.zeros_like(events.kinds),events.received & direct,direct_priority)
    # latest_cancel是独立来源；较新的非撤单Direct不能抹掉此前NoEffect/Unknown响应。
    keep=keep | first_identity_population(events.orders,torch.zeros_like(events.kinds),
        events.received & (events.kinds==CANCEL_TARGET),direct_priority)
    # known_query与latest_direct各有真实消费者；首次终态后晚Open不能抹前者。
    queries=events.received & (events.kinds==ORDER_QUERY)
    terminal=queries & (events.query_states>=1) & (events.query_states<=5)
    keep=keep | first_identity_population(events.orders,torch.zeros_like(events.kinds),queries,direct_priority)
    keep=keep | first_identity_population(events.orders,torch.zeros_like(events.kinds),terminal,direct_priority.flip(1))
    # root fault保存全部输入的故障，不因来源替换或压紧洗掉失败。
    fault=events.fault.clone()
    for value in [events.orders,events.trades,events.private_sequences,events.delivery_sequences,events.record_ordinals,events.batch_sequences]:
        limit=value.compare(value.constant((1<<64)-1))
        invalid=value.fault | limit.fault | ((value.sign<0)|(limit.order>0)).to(torch.int32)
        fault=fault | Numbers.combine_fault_rows(torch.where(events.received,invalid.reshape(roots,width),0))
    for value in [events.matched,events.open_quantity,events.trade_quantity]:
        fault=fault | Numbers.combine_fault_rows(torch.where(events.received,value.fault.reshape(roots,width),0))
    kind_fault=(events.kinds<BASE_ACCEPTED)|(events.kinds>ORDER_QUERY)
    kind_fault=kind_fault | ((events.kinds==TRADE)&((events.roles<0)|(events.roles>1)|(events.dispositions<0)|(events.dispositions>4)))
    kind_fault=kind_fault | ((events.kinds==ORDER_QUERY)&((events.query_states<0)|(events.query_states>8)|
        (events.query_quantity_kinds<0)|(events.query_quantity_kinds>2)))
    fault=fault | Numbers.combine_fault_rows(torch.where(events.received,kind_fault.to(torch.int32),0))
    # 唯一CPU读数是该物理块的最大保留人口，用于分配；不读业务条件逐项推进。
    capacity=max(1,int(keep.to(torch.int64).sum(1).max().cpu()))
    order=(~keep).to(torch.int64).argsort(dim=1,stable=True)[:,:capacity]
    coordinates=(torch.arange(roots,device=device)[:,None]*width+order).flatten()
    payload={}
    for field in fields(events):
        value=getattr(events,field.name)
        if field.name=='fault':payload[field.name]=fault
        elif field.name=='received':payload[field.name]=keep.gather(1,order)
        elif isinstance(value,torch.Tensor):payload[field.name]=value.gather(1,order)
        elif isinstance(value,ExactRational):payload[field.name]=take_ratio(value,coordinates)
        else:payload[field.name]=_take(value,coordinates)
    return PublicRegistryEvents(**payload)


@dataclass(frozen=True)
class PublicRegistryState:
    """首次领取的设备来源人口；append不改旧状态，冻结结果保持原值。"""
    events: PublicRegistryEvents

    def receive(self, incoming):
        # 两端已由公共领取器绑定同根账户、源位置和公开可见前缀。
        # 容量/精确值重试属于纯候选；调用者成功提交前不得消费源位置。
        from dataclasses import fields
        roots, old_width = self.events.received.shape
        if incoming.received.size(0) != roots:
            raise ValueError('公开Registry块需要相同根布局')
        new_width = incoming.received.size(1)
        def join_integer(old, new):
            capacity = max(old.words.size(1), new.words.size(1))
            old, new = old.with_capacity(capacity), new.with_capacity(capacity)
            return ExactInt(
                torch.cat([old.words.reshape(roots,old_width,capacity),new.words.reshape(roots,new_width,capacity)],1).flatten(0,1),
                torch.cat([old.sign.reshape(roots,old_width),new.sign.reshape(roots,new_width)],1).flatten(),
                torch.cat([old.fault.reshape(roots,old_width),new.fault.reshape(roots,new_width)],1).flatten())
        payload = {}
        for field in fields(self.events):
            old, new = getattr(self.events,field.name), getattr(incoming,field.name)
            if field.name == 'fault':
                payload[field.name] = old | new
            elif isinstance(old,torch.Tensor):
                payload[field.name] = torch.cat([old,new],1)
            elif isinstance(old,ExactRational):
                payload[field.name] = ExactRational(join_integer(old.numerator,new.numerator),join_integer(old.denominator,new.denominator))
            else:
                payload[field.name] = join_integer(old,new)
        return PublicRegistryState(compact_order_events(PublicRegistryEvents(**payload)))

    def freeze_order_sources(self, intents, cutoff):
        from dataclasses import replace
        roots=self.events.received.size(0)
        if cutoff.shape!=(roots,) or cutoff.dtype!=torch.int64 or cutoff.device!=self.events.received.device:
            raise TypeError('冻结cutoff需要同设备int64[roots]可信公开逻辑时刻')
        visible=self.events.received & (self.events.available_at<=cutoff[:,None])
        return frozen_order_sources(replace(self.events,received=visible),intents)

    def freeze_order_formation(self, intents, legs, cutoff, market_present):
        from dataclasses import replace
        from cancel_objects import order_formation_from_public
        visible=self.events.received & (self.events.available_at<=cutoff[:,None])
        return order_formation_from_public(replace(self.events,received=visible),intents,legs,market_present)

    def freeze_orders(self, intents, cutoff):
        from dataclasses import replace
        roots = self.events.received.size(0)
        if cutoff.shape != (roots,) or cutoff.dtype != torch.int64 or cutoff.device != self.events.received.device:
            raise TypeError('冻结cutoff需要同设备int64[roots]可信公开逻辑时刻')
        visible = self.events.received & (self.events.available_at <= cutoff[:,None])
        return frozen_order_read(replace(self.events,received=visible),intents)


@dataclass(frozen=True)
class CancellationCompletions:
    received: torch.Tensor  # [roots,events]，完整请求完成，不是逐目标回应。
    requests: object  # 原全局请求身份的无损字典坐标。
    delivery_sequences: object
    record_ordinals: object
    counts: object  # ExactInt[roots*events*3]，原完成人口三类，不重算目标数。
    available_at: torch.Tensor
    responded_at: torch.Tensor
    received_at: torch.Tensor
    fault: torch.Tensor
    record_refs: torch.Tensor
    context_slots: torch.Tensor
    run_slots: torch.Tensor
    batch_sequences: object
    private_sequences: object


@dataclass(frozen=True)
class CancellationResponseRef:
    present: torch.Tensor
    record_refs: torch.Tensor
    context_slots: torch.Tensor
    run_slots: torch.Tensor
    batch_sequences: object
    record_ordinals: object
    private_sequences: object
    delivery_sequences: object
    available_at: torch.Tensor
    responded_at: torch.Tensor
    received_at: torch.Tensor
    requests: object


def completion_reference(completions,at,present):
    roots,width=completions.received.shape
    count=at.size(1)
    if width==0:
        blank=torch.zeros_like(at)
        exact=ExactInt.from_i64_tensor(blank.flatten())
        return CancellationResponseRef(present,torch.full_like(blank,-1),blank,blank,
            exact,exact,exact,exact,blank,blank,blank,exact)
    coordinates=(torch.arange(roots,device=at.device)[:,None]*width+at).flatten()
    return CancellationResponseRef(present,completions.record_refs.gather(1,at),
        completions.context_slots.gather(1,at),completions.run_slots.gather(1,at),
        _take(completions.batch_sequences,coordinates),_take(completions.record_ordinals,coordinates),
        _take(completions.private_sequences,coordinates),_take(completions.delivery_sequences,coordinates),
        completions.available_at.gather(1,at),completions.responded_at.gather(1,at),completions.received_at.gather(1,at),
        _take(completions.requests,coordinates))


@dataclass(frozen=True)
class GlobalCancellationRead:
    pending_count: object
    has_latest: torch.Tensor
    response_counts: object
    counts_applicable: torch.Tensor
    responded_at: torch.Tensor
    received_at: torch.Tensor
    fault: torch.Tensor
    latest_response: CancellationResponseRef


@dataclass(frozen=True)
class PublicCancellationState:
    requests: object
    registered: torch.Tensor  # [roots,requests]，发送登记的原receipt证明；完成不抹原身份。
    completions: CancellationCompletions

    @classmethod
    def open(cls, requests, registered):
        """原请求身份目录和真实receipt登记；空完成不是从未发送的证明。"""
        if registered.ndim != 2 or registered.dtype != torch.bool or requests.sign.numel() != registered.numel() or requests.words.device != registered.device:
            raise TypeError('撤单请求目录须同设备精确身份和bool[roots,requests]登记人口')
        roots = registered.size(0)
        columns = torch.empty((roots, 0), dtype=torch.int64, device=registered.device)
        zero = ExactInt.from_i64_tensor(columns.flatten())
        source_fault = Numbers.combine_fault_rows(torch.where(registered, requests.fault.reshape_as(registered), 0))
        completed = CancellationCompletions(columns.bool(), zero, zero, zero, zero,
            columns, columns, columns, source_fault,
            columns, columns, columns, zero, zero)
        return cls(requests, registered, completed)

    def register(self, receipt_registered):
        from dataclasses import replace
        if receipt_registered.shape != self.registered.shape or receipt_registered.dtype != torch.bool or receipt_registered.device != self.registered.device:
            raise TypeError('原receipt登记需要同设备bool请求人口')
        registered = self.registered | receipt_registered
        source_fault = Numbers.combine_fault_rows(torch.where(registered, self.requests.fault.reshape_as(registered), 0))
        return replace(self,registered=registered,
            completions=replace(self.completions,fault=self.completions.fault | source_fault))

    def receive(self, incoming):
        # 共享设备列追加沿用同一资源事务；保留全部原完成引用的领取顺序。
        from dataclasses import fields
        roots, old_width = self.completions.received.shape
        new_width = incoming.received.size(1)
        payload = {}
        for field in fields(self.completions):
            old,new = getattr(self.completions,field.name),getattr(incoming,field.name)
            if field.name == 'fault':
                payload[field.name] = old | new
            elif isinstance(old,torch.Tensor):
                payload[field.name] = torch.cat([old,new],1)
            else:
                components = 3 if field.name == 'counts' else 1
                capacity = max(old.words.size(1),new.words.size(1))
                old,new = old.with_capacity(capacity),new.with_capacity(capacity)
                payload[field.name] = ExactInt(
                    torch.cat([old.words.reshape(roots,old_width*components,capacity),new.words.reshape(roots,new_width*components,capacity)],1).flatten(0,1),
                    torch.cat([old.sign.reshape(roots,old_width*components),new.sign.reshape(roots,new_width*components)],1).flatten(),
                    torch.cat([old.fault.reshape(roots,old_width*components),new.fault.reshape(roots,new_width*components)],1).flatten())
        completed = CancellationCompletions(**payload)
        width = old_width + new_width
        # 仅把设备上的实际人口总量回传给物理分配器；不回传完成身份或业务选择。
        # 稳定压紧完整记录坐标，等待重试中的空领取不积累块宽度的padding。
        retained = int(completed.received.to(torch.int64).sum(1).max().cpu())
        order = (~completed.received).argsort(dim=1,stable=True)[:,:retained]
        coordinates = (torch.arange(roots,device=order.device)[:,None]*width+order).flatten()
        compacted = {}
        for field in fields(completed):
            value = getattr(completed,field.name)
            if field.name == 'fault':
                compacted[field.name] = compacted.get(field.name,value) | value
            elif isinstance(value,torch.Tensor):
                compacted[field.name] = value.gather(1,order)
            else:
                components = 3 if field.name == 'counts' else 1
                population = coordinates[:,None]*components+torch.arange(components,device=order.device)[None,:]
                # 字段故障沿实际人口保留；全块来源故障在空人口时也不丢失。
                enabled = completed.received[:,:,None].expand(roots,width,components).reshape(roots,-1)
                compacted['fault'] = compacted.get('fault',completed.fault) | Numbers.combine_fault_rows(
                    torch.where(enabled,value.fault.reshape(roots,-1),0))
                compacted[field.name] = _take(value,population.flatten())
        return PublicCancellationState(self.requests,self.registered,CancellationCompletions(**compacted))

    def freeze_global(self, cutoff):
        roots,width = self.completions.received.shape
        count = self.registered.size(1)
        device = self.registered.device
        if cutoff.shape != (roots,) or cutoff.dtype != torch.int64 or cutoff.device != device:
            raise TypeError('冻结cutoff需要同设备int64[roots]可信公开逻辑时刻')
        request_fault = Numbers.combine_fault_rows(torch.where(self.registered, self.requests.fault.reshape_as(self.registered), 0))
        source_fault = self.completions.fault | request_fault
        if width == 0:
            pending=ExactInt.from_i64_tensor(self.registered.to(torch.int64).sum(1))
            absent=torch.zeros(roots,dtype=torch.bool,device=device)
            counts=ExactInt.from_i64_tensor(torch.zeros(roots*3,dtype=torch.int64,device=device))
            time=torch.zeros(roots,dtype=torch.int64,device=device)
            return GlobalCancellationRead(pending,absent,counts,absent,time,time,source_fault | pending.fault,
                completion_reference(self.completions,torch.zeros((roots,1),dtype=torch.int64,device=device),absent[:,None]))
        visible = self.completions.received & (self.completions.available_at <= cutoff[:,None])
        matches,comparison_fault = identity_matches(self.requests,self.completions.requests,roots,count,width)
        member = matches & self.registered[:,:,None] & visible[:,None,:]
        pending = self.registered & ~member.any(2)
        pending_count = ExactInt.from_i64_tensor(pending.to(torch.int64).sum(1))
        candidate = member.any(1)
        order = torch.arange(width,device=device)[None,:].expand(roots,width)
        # 原latest_complete按(delivery_sequence,record_ordinal)，两个完整u64均不截成i64。
        for sequence in [self.completions.record_ordinals,self.completions.delivery_sequences]:
            words = sequence.words.to(torch.int64)&0xffffffff
            for limb in [0,1]:
                key=words[:,limb].reshape(roots,width)
                order=order.gather(1,key.gather(1,order).argsort(dim=1,descending=True,stable=True))
        at,has_latest = choose_latest(candidate[:,None,:],order)
        at,has_latest = at[:,0],has_latest[:,0]
        global_at=torch.arange(roots,device=device)*width+at
        population=global_at[:,None]*3+torch.arange(3,device=device)[None,:]
        counts=_take(self.completions.counts,population.flatten())
        fault=source_fault | Numbers.combine_fault_rows(torch.where(member,comparison_fault,0).reshape(roots,-1))
        for sequence,components in [(self.completions.requests,1),(self.completions.delivery_sequences,1),
                (self.completions.record_ordinals,1),(self.completions.batch_sequences,1),
                (self.completions.private_sequences,1),(self.completions.counts,3)]:
            limit=sequence.compare(sequence.constant((1<<64)-1))
            invalid=sequence.fault | limit.fault | ((sequence.sign<0)|(limit.order>0)).to(torch.int32)
            enabled=visible[:,:,None].expand(roots,width,components).reshape(roots,-1)
            fault=fault | Numbers.combine_fault_rows(torch.where(enabled,invalid.reshape(roots,-1),0))
        fault=fault | pending_count.fault
        return GlobalCancellationRead(pending_count,has_latest,counts,has_latest,
            self.completions.responded_at.gather(1,at[:,None])[:,0],
            self.completions.received_at.gather(1,at[:,None])[:,0],fault,completion_reference(self.completions,at[:,None],has_latest[:,None]))


@dataclass(frozen=True)
class OrderFeedbackSource:
    """冻结来源引用与原双时间；不存在时不补造接受或领取时刻。"""
    present: torch.Tensor
    record_refs: torch.Tensor
    platform_at: torch.Tensor
    received_at: torch.Tensor
    available_at: torch.Tensor
    context_slots: torch.Tensor
    run_slots: torch.Tensor
    batch_sequences: object
    record_ordinals: object
    private_sequences: object
    delivery_sequences: object


@dataclass(frozen=True)
class FrozenOrderSources:
    formed: OrderFeedbackSource
    latest_direct: OrderFeedbackSource
    fault: torch.Tensor
    latest_cancel: OrderFeedbackSource | None = None
    known_query: OrderFeedbackSource | None = None


def delivery_priority(delivery, ordinal, receive_rank):
    """原replace_delivery的u64(delivery,record ordinal)最大值；不是private顺序。"""
    roots,width=receive_rank.shape
    order=receive_rank.argsort(dim=1,stable=True)
    for sequence in [ordinal,delivery]:
        words=sequence.words.to(torch.int64)&0xffffffff
        for limb in [0,1]:
            key=words[:,limb].reshape(roots,width)
            order=order.gather(1,key.gather(1,order).argsort(dim=1,descending=True,stable=True))
    return order


def frozen_order_sources(events,intents):
    roots,width=events.received.shape
    count=intents.present.size(1)
    device=events.received.device
    if width==0 or count==0:
        blank=torch.zeros((roots,count),dtype=torch.bool,device=device)
        columns=torch.zeros((roots,count),dtype=torch.int64,device=device)
        zero=ExactInt.from_i64_tensor(columns.flatten())
        source=OrderFeedbackSource(blank,torch.full_like(columns,-1),columns,columns,columns,columns,columns,zero,zero,zero,zero)
        return FrozenOrderSources(source,source,events.fault,source,source)
    same,error=identity_matches(intents.identities,events.orders,roots,count,width)
    member=same&events.received[:,None,:]&intents.present[:,:,None]
    fault=events.fault|Numbers.combine_fault_rows(torch.where(member,error,0).reshape(roots,-1))
    def select_source(population,priority):
        at,known=choose_latest(member&population[:,None,:],priority)
        coordinates=(torch.arange(roots,device=device)[:,None]*width+at).flatten()
        def column(value):
            return value.gather(1,at)
        return OrderFeedbackSource(known,torch.where(known,column(events.record_refs),-1),
            column(events.platform_at),column(events.received_at),column(events.available_at),
            column(events.context_slots),column(events.run_slots),_take(events.batch_sequences,coordinates),
            _take(events.record_ordinals,coordinates),_take(events.private_sequences,coordinates),
            _take(events.delivery_sequences,coordinates))
    formed=select_source((events.kinds==ORDER_LIFECYCLE)&events.formation,source_priority(events.private_sequences,events.receive_rank))
    direct=select_source((events.kinds<=QUOTE_ACCEPTED)|(events.kinds==CANCEL_TARGET)|(events.kinds==ORDER_QUERY),
        delivery_priority(events.delivery_sequences,events.record_ordinals,events.receive_rank))
    cancellation=select_source(events.kinds==CANCEL_TARGET,
        delivery_priority(events.delivery_sequences,events.record_ordinals,events.receive_rank))
    # 以完整来源选择相同known_query；原canonical condition/token仍在共享NOE bank。
    query_at,query_known,_=query_selection(events,member)
    # 与其他公开来源相同的原引用绑定，不创建第二份查询正文。
    coordinates=(torch.arange(roots,device=device)[:,None]*width+query_at).flatten()
    def query_column(value):return value.gather(1,query_at)
    query=OrderFeedbackSource(query_known,torch.where(query_known,query_column(events.record_refs),-1),
        query_column(events.platform_at),query_column(events.received_at),query_column(events.available_at),
        query_column(events.context_slots),query_column(events.run_slots),_take(events.batch_sequences,coordinates),
        _take(events.record_ordinals,coordinates),_take(events.private_sequences,coordinates),_take(events.delivery_sequences,coordinates))
    return FrozenOrderSources(formed,direct,fault,cancellation,query)
