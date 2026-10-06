"""真实公共GPU状态到完整Native fast626；冻结只生成不可变输入和候选端点。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from public_state import _take
from input_books import PmBookInputs
from input_sides import SideInputs, PmSideMemory
from input_pm_fields import pm_fields
from input_lifecycle import LifecycleInputs
from input_reference import ReferenceInputs
from input_binance import BinanceInputs
from input_events import EventInputs
from input_trades import TradeInputs
from input_rules import RuleInputs
from fact_window import PublicFactWindow,PublicTradeWindow


@dataclass(frozen=True)
class MarketFreezeMemory:
    previous_prefix: torch.Tensor
    previous_observation_ms: torch.Tensor
    pm_sides: object
    binance_sides: object


@dataclass(frozen=True)
class FrozenMarket100ms:
    fast: torch.Tensor
    book_levels: torch.Tensor
    book_mask: torch.Tensor
    rule: torch.Tensor
    role: torch.Tensor              # -1不产生状态行，0 ContextOnly，1 DecisionRequired。
    prefixes: torch.Tensor
    history_memory: object
    endpoint_memory: MarketFreezeMemory
    fault: torch.Tensor


def _endpoint_commit(candidate,previous,active):
    """只在真实状态行冻结后接纳端点；inactive不是已观察到Unknown。"""
    numbers = _numbers()
    roots,sides = candidate.known.shape
    if previous is None:
        zero = candidate.total.constant(0)
        previous = PmSideMemory(zero,zero,torch.zeros_like(candidate.known),torch.zeros_like(candidate.best_present),
                                candidate.asset_indices.clone())
    selected = active[:,None].expand(roots,sides)
    return PmSideMemory(candidate.total.select(selected.reshape(-1),previous.total),
        candidate.best.select(selected.reshape(-1),previous.best),
        torch.where(selected,candidate.known,previous.known),torch.where(selected,candidate.best_present,previous.best_present),
        torch.where(active[:,None],candidate.asset_indices,previous.asset_indices))


class MarketInputs100ms:
    def __init__(self, shared, market, reference, binance, source_history, process_rules, columns, delivered_cutoffs):
        self.shared,self.market,self.source_history = shared,market,source_history
        if (delivered_cutoffs.shape!=(shared.group_count,) or delivered_cutoffs.dtype!=torch.int64
                or delivered_cutoffs.device!=shared.slots.device):
            raise ValueError('需全部完整组的同设备实际交付cutoff')
        # 上游必须提供真实delay与跨块前沿；这里不选择正式延迟profile。
        self.delivered_cutoffs = delivered_cutoffs
        self.delivery_fault = (((delivered_cutoffs[1:]<delivered_cutoffs[:-1]).any()
            | (delivered_cutoffs<shared.causal_visible_at).any()).to(torch.int32))
        self.packer = PmBookInputs(shared)
        self.sides = SideInputs(shared)
        self.binance = BinanceInputs(binance)
        self.binance_state = binance
        self.reference = ReferenceInputs(reference)
        self.lifecycle = LifecycleInputs(market)
        self.events = EventInputs(PublicFactWindow(columns))
        self.trades = TradeInputs(shared,PublicTradeWindow(columns))
        self.rules = RuleInputs(shared,process_rules)

    def prefix_at(self, observation_ms):
        return torch.searchsorted(self.delivered_cutoffs,observation_ms,right=True)-1

    def freeze(self, observation_ms, market_indices, root_started_at, source_memory, endpoint_memory):
        """观察时刻固定在UTC100ms；调用方先接纳截至C的全部完整组，再冻结。

        不提交账户/订单/随机数/GRU。只有全部公共及Native计算成功后，协调器才能一次接纳
        history_memory和endpoint_memory；模型计算积压不会改写这些冻结张量。
        source_memory须来自同一根/市场连续consume，跨块仍须迁移公共坐标与交付前沿。
        """
        roots = len(observation_ms);device = self.shared.slots.device;numbers = _numbers()
        for value in (observation_ms,market_indices,root_started_at,endpoint_memory.previous_prefix,
                      endpoint_memory.previous_observation_ms):
            if value.shape!=(roots,) or value.dtype!=torch.int64 or value.device!=device:
                raise ValueError('需同设备int64根批次')
        prefixes = self.prefix_at(observation_ms)
        valid_market = (market_indices>=0)&(market_indices<self.market.market_count)
        if not self.market.market_count: raise ValueError('完整fast装配需要真实固定市场人口')
        safe = market_indices.clamp(0,self.market.market_count-1)
        assets = self.market.asset_pairs[safe]
        market_frame = self.market.frame(prefixes)
        observed = self.shared.observation(prefixes)
        frame = self.shared.frame(prefixes)
        now = numbers.ExactInt.from_i64_tensor(observation_ms)
        elapsed = now-numbers.ExactInt.from_i64_tensor(endpoint_memory.previous_observation_ms)
        step = elapsed.compare(elapsed.constant(100))
        fault = step.fault | (step.order!=0).to(torch.int32) | (observation_ms.remainder(100)!=0).to(torch.int32)
        fault |= (~valid_market).to(torch.int32) | self.delivery_fault
        fault |= (source_memory.last_prefix!=prefixes).to(torch.int32)
        fault |= (source_memory.root_started_at!=root_started_at).to(torch.int32)
        fault |= ((source_memory.market_ids!=0)&(source_memory.market_ids!=self.source_history.market_ids[safe])).to(torch.int32)
        began = now.compare(numbers.ExactInt.from_i64_tensor(root_started_at))
        arrived = now.compare(numbers.ExactInt.from_i64_tensor(source_memory.last_cutoff))
        fault |= began.fault | arrived.fault | ((began.order<0)|(arrived.order<0)).to(torch.int32)
        fault |= ((endpoint_memory.previous_prefix < -1) | (endpoint_memory.previous_prefix > prefixes)).to(torch.int32)
        start = numbers.ExactInt.from_i64_tensor(self.market.times[safe,0])
        end = numbers.ExactInt.from_i64_tensor(self.market.times[safe,1])
        qualified = numbers.ExactInt.from_i64_tensor(root_started_at).compare(start-start.constant(30000))
        entered = now.compare(start-start.constant(30000));opened = now.compare(start);ended = now.compare(end)
        active = (valid_market & market_frame.admitted[torch.arange(roots,device=device),safe]
                  & (qualified.order<=0) & (entered.order>=0) & (ended.order<0))
        fault |= qualified.fault | entered.fault | opened.fault | ended.fault
        role = torch.where(active,(opened.order>=0).to(torch.int64),-1)
        pm = self.sides.encode(frame,assets,observation_ms,endpoint_memory.pm_sides)
        pm_books = self.packer.pack(frame,assets,observation_ms)
        fields = pm_fields(self.shared,observed,assets,observation_ms)
        symbols = torch.zeros_like(market_indices)
        binance = self.binance.encode(self.binance_state.frame(prefixes),self.binance_state.observation(prefixes),
            symbols,observation_ms,endpoint_memory.binance_sides)
        reference = self.reference.encode(self.reference.state.frame(prefixes),market_frame,market_indices,observation_ms)
        lifecycle = self.lifecycle.encode(market_frame,market_indices,assets,observation_ms)
        events = self.events.encode(endpoint_memory.previous_prefix,prefixes,observation_ms)
        trades = self.trades.encode(endpoint_memory.previous_prefix,prefixes,assets,observation_ms)
        rules = self.rules.encode(observed,assets,observation_ms)
        history = self.source_history.freeze(source_memory)
        feature_fault = pm.fault | pm_books.fault | fields.fault | binance.fault | reference.fault
        feature_fault |= lifecycle.fault | events.fault | trades.fault | rules.fault | history.fault
        fault |= torch.where(active,feature_fault,0)
        fast = torch.cat((reference.values,pm.values.reshape(roots,76),binance.side.values.reshape(roots,38),
            fields.bba.reshape(roots,56),fields.trade.reshape(roots,36),binance.trade,
            trades.values,events.values,lifecycle.values,history.history,rules.values,history.path),dim=1)
        if fast.shape!=(roots,626): raise RuntimeError('Native fast626字段装配错误')
        fast = torch.where(active[:,None],fast,0.0)
        capacity = max(pm_books.levels.shape[2],binance.packed.levels.shape[2])
        def pad(books):
            return (torch.nn.functional.pad(books.levels,(0,0,0,capacity-books.levels.shape[2])),
                    torch.nn.functional.pad(books.mask,(0,capacity-books.mask.shape[2])))
        pm_levels,pm_mask = pad(pm_books);binance_levels,binance_mask = pad(binance.packed)
        levels = torch.cat((pm_levels,binance_levels),dim=1)
        mask = torch.cat((pm_mask,binance_mask),dim=1)&active[:,None,None]
        levels = torch.where(mask[:,:,:,None],levels,0.0)
        candidate = MarketFreezeMemory(prefixes.clone(),observation_ms.clone(),
            _endpoint_commit(pm.memory,endpoint_memory.pm_sides,active),
            _endpoint_commit(binance.side.memory,endpoint_memory.binance_sides,active))
        return FrozenMarket100ms(fast,levels,mask,rules.values,role,prefixes.clone(),history.memory,candidate,fault)
