"""原Ledger等待队列的固定地址候选提交与设备条件扫描。

首次领取/Registry接纳由PublicLedgerState.receive_first提交，图只处理同一waiting。
源bank/lifecycle/完整位置保持不可变；冻结必须借snapshot，不能借活动地址。
"""
from dataclasses import dataclass, fields, is_dataclass, replace
import torch
from queues import ExactInt, Numbers
from admission import AdmissionLifecycle
from closed import ClosedDrainCursor
from resolution import MarketResolutions


def _shared(value, name):
    return ((isinstance(value, AdmissionLifecycle) and name in ('columns','lifecycle','market_balance_origin'))
        or (isinstance(value, MarketResolutions) and name == 'sources')
        or (isinstance(value, ClosedDrainCursor) and name == 'order'))


def _owned(value, layout, word_capacity=0):
    if isinstance(value, ExactInt):
        value=value.with_capacity(max(value.words.size(1),layout.words.size(1),word_capacity))
        return ExactInt(value.words.clone(),value.sign.clone(),value.fault.clone())
    if isinstance(value, torch.Tensor):
        if value.shape != layout.shape or value.dtype != layout.dtype or value.device != layout.device:
            raise ValueError('Ledger活动人口须先在冷边界对齐')
        return value.clone()
    if is_dataclass(value):
        return replace(value, **{f.name: getattr(value,f.name) if _shared(value,f.name)
            else _owned(getattr(value,f.name),getattr(layout,f.name),word_capacity) for f in fields(value)})
    if isinstance(value,tuple):
        if len(value)!=len(layout): raise ValueError('Ledger冷布局人口不同')
        return tuple(_owned(a,b,word_capacity) for a,b in zip(value,layout))
    if value is None or isinstance(value,(str,int,bool)):
        if value != layout: raise ValueError('Ledger候选不能改冷元数据')
        return value
    raise TypeError('Ledger活动存储须为原状态与已安装共享列')


def _pairs(target,candidate):
    if target is candidate: return
    if isinstance(target,torch.Tensor):
        if target.shape != candidate.shape or target.dtype != candidate.dtype or target.device != candidate.device:
            raise ValueError('Ledger候选布局改变须退出图增长资源')
        yield target,candidate
    elif is_dataclass(target):
        for f in fields(target):
            if _shared(target,f.name):
                if getattr(target,f.name) is not getattr(candidate,f.name):
                    raise ValueError('Ledger扫描不能更换源页/位置或本轮顺序')
            else:
                yield from _pairs(getattr(target,f.name),getattr(candidate,f.name))
    elif isinstance(target,tuple):
        if len(target)!=len(candidate): raise ValueError('Ledger冷布局人口不同')
        for a,b in zip(target,candidate): yield from _pairs(a,b)
    elif target != candidate:
        raise ValueError('Ledger扫描不能改变冷配置')


@dataclass(frozen=True)
class LedgerDrainState:
    ledger: object
    waiting: object
    cursor: object


class LedgerDrainStorage:
    def __init__(self, ledger, waiting, *, word_capacity, layout=None, cursor=None):
        self.source_ledger=ledger
        self.source_waiting=waiting
        original=LedgerDrainState(ledger,waiting,waiting.start_drain() if cursor is None else cursor)
        self.state=_owned(original,original if layout is None else layout,word_capacity)

    def snapshot(self):
        return _owned(self.state,self.state)

    def commit(self,candidate,resources_fit):
        pairs=tuple(_pairs(self.state,candidate))
        with torch.no_grad():
            for target,incoming in pairs:
                target.copy_(torch.where(resources_fit,incoming,target))


class CapturedLedgerDrain:
    """一次一原领取位置，完整轮无进展退出；字/目录不足保本位置。

    页接入/目录增长及首次Registry物理装箱仍在图外冷边界。成功记录立即共同
    提交Ledger、waiting及cursor；不以固定最大业务轮数丢弃未解引用。
    """
    def __init__(self, storage, association_available, *, word_capacity, enabled=None, progress=None):
        self.storage=storage
        self.association_available=association_available
        device=association_available.device
        self.enabled=torch.ones((),dtype=torch.bool,device=device) if enabled is None else enabled
        if self.enabled.shape!=() or self.enabled.dtype!=torch.bool or self.enabled.device!=device:
            raise TypeError('本物理扫描的许可须为同设备bool标量')
        self.ready=torch.zeros((),dtype=torch.bool,device=device)
        self.resources_fit=torch.ones((),dtype=torch.bool,device=device)
        self.iterations=torch.zeros_like(storage.state.cursor.position)
        self.required={name:torch.zeros_like(storage.state.cursor.position)
            for name in ('orders','trades','contributions','days','order_funds','conditions','costs','cash')}
        def compute():
            state=storage.state
            ledger,waiting,cursor=state.waiting.drain_one(state.ledger,association_available,state.cursor)
            return LedgerDrainState(ledger,waiting,cursor)
        # 原候选 warm后捕获；本步骤不安装warm业务状态。
        if progress is not None: progress('warm')
        self.warm=Numbers.device_exact_batch(compute,word_capacity=word_capacity,device=device)
        if progress is not None: progress('capture_body')
        self.body=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.body):
            self.candidate=Numbers.device_exact_batch(compute,word_capacity=word_capacity,device=device)
            ledger=self.candidate.value.ledger
            needs={'orders':ledger.orders.capacity_required,'trades':ledger.trades.capacity_required,
                'contributions':ledger.rebate_contributions.capacity_required,
                'order_funds':ledger.order_funds.capacity_required,
                'days':torch.zeros_like(self.required['days']) if ledger.rebate_days is None else ledger.rebate_days.capacity_required,
                'costs':torch.zeros_like(self.required['costs']) if ledger.settlement_costs is None else ledger.settlement_costs.capacity_required,
                'cash':torch.zeros_like(self.required['cash']) if ledger.external_cash is None else ledger.external_cash.capacity_required,
                'conditions':torch.full_like(self.required['conditions'],-1) if ledger.projected_markets is None else ledger.projected_markets.required_conditions}
            needs['conditions']=torch.where(needs['conditions']>=0,needs['conditions'],ledger.accepted_principal.required_conditions)
            fit=self.candidate.fits
            for name,directory in (('orders',ledger.orders),('trades',ledger.trades),
                    ('contributions',ledger.rebate_contributions),('order_funds',ledger.order_funds)):
                fit=fit & (needs[name]<=directory.occupied.size(1)).all()
            if ledger.rebate_days is not None:
                fit=fit & (needs['days']<=ledger.rebate_days.occupied.size(1)).all()
            for name,directory in (('costs',ledger.settlement_costs),('cash',ledger.external_cash)):
                if directory is not None:fit=fit & (needs[name]<=directory.occupied.size(1)).all()
            fit=fit & (needs['conditions']<0).all()
            self.resources_fit.copy_(fit)
            for name,need in needs.items(): self.required[name].copy_(need)
            scanning=storage.state.cursor.scanning.clone()
            storage.commit(self.candidate.value,fit)
            self.iterations.add_((scanning & fit).to(torch.int64))
            self.ready.copy_(storage.state.cursor.scanning.any() & fit)
        if progress is not None: progress('capture_entry')
        self.entry=torch.cuda.CUDAGraph(keep_graph=True)
        with torch.cuda.graph(self.entry):
            self.resources_fit.fill_(True)
            self.candidate.required_words.zero_()
            self.candidate.fits.fill_(True)
            self.iterations.zero_()
            for name,need in self.required.items(): need.fill_(-1 if name=='conditions' else 0)
            self.ready.copy_(storage.state.cursor.scanning.any() & self.enabled)
        if progress is not None: progress('instantiate_native')
        self._loop=Numbers.public_extension().DeviceConditionalLoop(
            self.entry.raw_cuda_graph(),self.body.raw_cuda_graph(),self.ready)

    def replay(self):
        self._loop.replay()


def installed_word_capacity(value):
    """冷安装只查物理字shape，不读业务数或源条件回CPU。"""
    if isinstance(value,ExactInt): return value.words.size(1)
    if is_dataclass(value): return max((installed_word_capacity(getattr(value,f.name)) for f in fields(value)),default=4)
    if isinstance(value,(tuple,list)): return max((installed_word_capacity(v) for v in value),default=4)
    return 4


class PublicLedgerReceiver:
    """首次公开领取之后续原waiting扫描；保留原资源需求供冷增长。"""
    def __init__(self,*,word_capacity=None,progress=None):
        self.word_capacity=word_capacity;self.progress=progress;self.captured=None

    def __call__(self,state,delivery,enabled):
        self.captured=None
        pending=(state.waiting.received&~state.waiting.consumed)&enabled[:,None]
        if bool(pending.any().cpu()):
            capacity=installed_word_capacity(state.ledger) if self.word_capacity is None else self.word_capacity
            storage=LedgerDrainStorage(state.ledger,state.waiting,word_capacity=capacity)
            self.captured=CapturedLedgerDrain(storage,enabled[:,None].expand_as(state.ledger.columns.enabled),
                word_capacity=capacity,progress=self.progress)
            state=state.drain_captured(self.captured)
        # 整轮无进展表示合法前驱等待；仅物理不足须续此刻扫描。
        fit=torch.ones_like(enabled) if self.captured is None else self.captured.resources_fit.expand_as(enabled)
        return state,enabled&fit&((state.ledger.fault|state.waiting.fault|delivery.fault)==0)
