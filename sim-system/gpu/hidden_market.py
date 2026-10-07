"""原SIM hidden资产行情与FIFO候选；公开观察及交付前沿不由本状态推进。

资产/价位坐标由已验收源块冷映射；未来价位只有空存储，不能作为模型可见盘口。
事件调用消费同设备掩码和精确源数量。完整桶/义务/请求调度由所属SIM协调器负责。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers, PriceQueueBatch, _take


def rational_take(value, indices):
    return ExactRational(_take(value.numerator, indices), _take(value.denominator, indices))


def rational_select(value, mask, other):
    return ExactRational(value.numerator.select(mask, other.numerator),
                         value.denominator.select(mask, other.denominator))


def zeros(value):
    return ExactRational.ratio(value.numerator.constant(0), value.denominator.constant(1))


def micro_units(value):
    """原micro_units_v1：非负精确源量乘1e6后向下取整，不将细源状态自身量化。"""
    amount = (value * ExactRational.ratio(value.numerator.constant(1_000_000),
                                        value.denominator.constant(1))).floor()
    invalid = value.fault | (value.numerator.sign < 0).to(torch.int32)
    return ExactInt(amount.words, amount.sign, amount.fault | invalid)


@dataclass(frozen=True)
class HiddenAssetState:
    level_assets: torch.Tensor  # int64[levels]，含根的稳定资产坐标。
    asset_markets: torch.Tensor  # int64[assets]，原SIM目录市场归属。
    market_populations: torch.Tensor
    initialized: torch.Tensor  # bool[assets]；初始空盘也是已初始化。
    quality: torch.Tensor  # int64[assets]，固定顺序 Fresh/Stale/Missing/Unknown/Gap/Invalid。
    tick: object  # ExactRational[assets]
    tick_present: torch.Tensor
    matching_closed: torch.Tensor  # bool[assets]；end后不再应用初始盘/档位到撮合状态。
    book_closed: torch.Tensor  # 原Active→PostEnd时清行情簿；Preloading与matching关闭不同。
    external_shares: object  # ExactRational[levels]；原档位前驱保持全部源精度。
    queues: PriceQueueBatch
    fault: torch.Tensor  # int32[assets]，错误候选不可提交到活动状态。

    @classmethod
    def empty(cls, level_assets, prices, asset_markets, markets, *, segment_capacity=4):
        assets = asset_markets.numel()
        if level_assets.dtype != torch.int64 or not level_assets.is_cuda:
            raise TypeError('价位所属资产须为设备int64列')
        if level_assets.shape != prices.numerator.sign.shape or assets < 1:
            raise ValueError('价位及资产坐标形状不一致')
        device = level_assets.device
        if asset_markets.dtype != torch.int64 or asset_markets.device != device or asset_markets.shape != (assets,):
            raise TypeError('资产所属市场须为同设备int64[assets]')
        valid = (level_assets >= 0) & (level_assets < assets)
        safe_assets = level_assets.clamp(0, assets - 1)
        fault = torch.zeros(assets, dtype=torch.int32, device=device)
        fault.scatter_reduce_(0, safe_assets, (~valid).to(torch.int32), reduce='amax')
        tick_zero = ExactInt.from_i64_tensor(torch.zeros(assets, dtype=torch.int64, device=device))
        populations = torch.zeros(markets, dtype=torch.int64, device=device).scatter_add(
            0, asset_markets, torch.ones_like(asset_markets))
        return cls(safe_assets, asset_markets, populations, torch.zeros(assets, dtype=torch.bool, device=device),
                   torch.full((assets,), 3, dtype=torch.int64, device=device),
                   ExactRational.ratio(tick_zero, tick_zero.constant(1)),
                   torch.zeros(assets, dtype=torch.bool, device=device),
                   torch.zeros(assets, dtype=torch.bool, device=device),
                   torch.zeros(assets, dtype=torch.bool, device=device), zeros(prices),
                   PriceQueueBatch.empty(prices, segment_capacity), fault)

    def _mask(self, enabled):
        if enabled.dtype != torch.bool or enabled.shape != self.initialized.shape or enabled.device != self.quality.device:
            raise TypeError('资产事件掩码须为同设备bool[assets]')
        return enabled

    def _asset_fault(self, level_fault):
        # 按位OR，不使用整数max丢失同资产多价位的不同错误位。
        bits = torch.arange(32, dtype=torch.int64, device=self.fault.device)
        values = ((level_fault.to(torch.int64)[:, None] >> bits) & 1).to(torch.int32)
        reduced = torch.zeros((self.fault.numel(), 32), dtype=torch.int32, device=self.fault.device)
        reduced = reduced.scatter_reduce(0, self.level_assets[:, None].expand_as(values), values, reduce='amax')
        return (reduced.to(torch.int64) << bits).sum(1).to(torch.int32)

    def advance_matching_time(self, now, end, *, enabled=None):
        """使用SIM实际处理时钟；冷预取时间或公开观察时钟不能替代此调用。"""
        for column in (now, end):
            if column.shape != self.initialized.shape or column.dtype != torch.int64 or column.device != self.quality.device:
                raise TypeError('处理时刻与市场end须为同设备int64[assets]')
        reached = now >= end
        if enabled is not None:
            reached = reached & self._mask(enabled)
        newly_closed = reached & ~self.matching_closed
        queues = self.queues.clear_matching(newly_closed[self.level_assets])
        return replace(self, matching_closed=self.matching_closed | reached, queues=queues)._close_active_book()

    def _close_active_book(self):
        initialized = torch.zeros_like(self.market_populations).scatter_add(
            0, self.asset_markets, self.initialized.to(torch.int64))
        active = (initialized == self.market_populations) & (self.market_populations > 0)
        closed = self.book_closed | (active[self.asset_markets] & self.matching_closed)
        return replace(self, book_closed=closed,
                       external_shares=rational_select(zeros(self.external_shares), closed[self.level_assets], self.external_shares))

    def initial_tick(self, enabled, tick):
        enabled = self._mask(enabled)
        fault = self.fault | (tick.fault | (tick.numerator.sign <= 0).to(torch.int32)) * enabled.to(torch.int32)
        valid = enabled & (fault == 0)
        return replace(self, tick=rational_select(tick, valid, self.tick),
                       tick_present=self.tick_present | valid, fault=fault)

    def initial_snapshot(self, enabled, shares, tick, quality):
        """原InitialL2Snapshot只接纳一次；所有原档位的数量同次供给，空资产仍初始化。"""
        enabled = self._mask(enabled)
        enabled = enabled & ~self.book_closed
        fault = self.fault | (enabled & self.initialized).to(torch.int32)
        invalid_tick = tick.fault | (tick.numerator.sign <= 0).to(torch.int32)
        fault = fault | (invalid_tick * enabled.to(torch.int32))
        amounts = micro_units(shares)
        level_mask = enabled[self.level_assets]
        fault = fault | self._asset_fault(amounts.fault * level_mask.to(torch.int32))
        valid = enabled & (fault == 0)
        level_valid = valid[self.level_assets]
        quantities = amounts.select(level_valid & ~self.matching_closed[self.level_assets], amounts.constant(0))
        queues, required = self.queues.append_external(quantities)
        queue_fault = self._asset_fault(queues.fault)
        fault = fault | queue_fault
        valid = valid & (fault == 0)
        return replace(self, initialized=self.initialized | valid,
                       quality=torch.where(valid, quality, self.quality),
                       tick=rational_select(tick, valid, self.tick),
                       tick_present=self.tick_present | valid,
                       external_shares=rational_select(shares, valid[self.level_assets], self.external_shares),
                       queues=queues, fault=fault)._close_active_book(), required

    def tick_change(self, enabled, previous, previous_present, current):
        enabled = self._mask(enabled)
        relation = self.tick.compare(previous)
        conflict = previous_present & (~self.tick_present | (relation.order != 0))
        error = relation.fault | previous.fault | current.fault
        error = error | (~self.initialized | conflict | (current.numerator.sign <= 0)).to(torch.int32)
        fault = self.fault | error * enabled.to(torch.int32)
        valid = enabled & (fault == 0)
        return replace(self, tick=rational_select(current, valid, self.tick),
                       tick_present=self.tick_present | valid, fault=fault)

    def quality_boundary(self, enabled, previous, current, resync_required):
        enabled = self._mask(enabled)
        fault = self.fault | (enabled & (self.quality != previous)).to(torch.int32)
        valid = enabled & (fault == 0)
        clear = valid & resync_required
        queues = self.queues.clear_external(clear[self.level_assets])
        return replace(self, quality=torch.where(valid, current, self.quality), queues=queues,
                       external_shares=rational_select(zeros(self.external_shares), clear[self.level_assets], self.external_shares),
                       fault=fault | self._asset_fault(queues.fault))

    def prepare_level_transition(self, enabled, before, after, resync_inference):
        """检查原资产/精确档位前驱；队列由同一次SIM事务更新一次。"""
        if enabled.dtype != torch.bool or enabled.shape != self.level_assets.shape or enabled.device != self.level_assets.device:
            raise TypeError('档位事件掩码须为同设备bool[levels]')
        enabled = enabled & ~self.book_closed[self.level_assets]
        relation = self.external_shares.compare(before)
        ready = self.initialized[self.level_assets]
        allowed = (self.quality[self.level_assets] == 0) | resync_inference
        error = before.fault | after.fault | relation.fault
        error = error | (~ready | ~allowed | (relation.order != 0)).to(torch.int32)
        error = error | (before.numerator.sign < 0).to(torch.int32) | (after.numerator.sign < 0).to(torch.int32)
        error = error | self.fault[self.level_assets]
        fault = self.fault | self._asset_fault(error * enabled.to(torch.int32))
        valid = enabled & (fault[self.level_assets] == 0)
        old_units = micro_units(before)
        new_units = micro_units(after)
        return replace(self, external_shares=rational_select(after, valid, self.external_shares),
                       fault=fault), old_units, new_units, valid & ~self.matching_closed[self.level_assets]

    def level_transition(self, enabled, before, after, resync_inference):
        """无账户订单消费者的资产候选；复用同一前驱检查与唯一队列更新。"""
        prepared, old_units, new_units, valid = self.prepare_level_transition(enabled,before,after,resync_inference)
        queues, effects = self.queues.apply_level_transition(
            old_units.select(valid, old_units.constant(0)), new_units.select(valid, new_units.constant(0)))
        return replace(prepared,queues=queues,fault=prepared.fault | self._asset_fault(effects.fault)), effects


def run_hidden_asset_batch(state, compute):
    """纯候选扩池；未成功前不推进hidden/public前沿、订单、账户或随机数。"""
    while True:
        result = Numbers.run_exact_batch(lambda: compute(state))
        output = result[-1]
        demand = output.required_segments if hasattr(output, 'required_segments') else output
        required = int(demand.max().cpu()) if demand.numel() else 0
        if required <= state.queues.capacity:
            return result
        state = replace(state, queues=state.queues.resized(1 << (required - 1).bit_length()))
