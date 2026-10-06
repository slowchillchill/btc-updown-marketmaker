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
    initialized: torch.Tensor  # bool[assets]；初始空盘也是已初始化。
    quality: torch.Tensor  # int64[assets]，固定顺序 Fresh/Stale/Missing/Unknown/Gap/Invalid。
    tick: object  # ExactRational[assets]
    tick_present: torch.Tensor
    external_shares: object  # ExactRational[levels]；原档位前驱保持全部源精度。
    queues: PriceQueueBatch
    fault: torch.Tensor  # int32[assets]，错误候选不可提交到活动状态。

    @classmethod
    def empty(cls, level_assets, prices, assets, *, segment_capacity=4):
        if level_assets.dtype != torch.int64 or not level_assets.is_cuda:
            raise TypeError('价位所属资产须为设备int64列')
        if level_assets.shape != prices.numerator.sign.shape or assets < 1:
            raise ValueError('价位及资产坐标形状不一致')
        device = level_assets.device
        valid = (level_assets >= 0) & (level_assets < assets)
        safe_assets = level_assets.clamp(0, assets - 1)
        fault = torch.zeros(assets, dtype=torch.int32, device=device)
        fault.scatter_reduce_(0, safe_assets, (~valid).to(torch.int32), reduce='amax')
        tick_zero = ExactInt.from_i64_tensor(torch.zeros(assets, dtype=torch.int64, device=device))
        return cls(safe_assets, torch.zeros(assets, dtype=torch.bool, device=device),
                   torch.full((assets,), 3, dtype=torch.int64, device=device),
                   ExactRational.ratio(tick_zero, tick_zero.constant(1)),
                   torch.zeros(assets, dtype=torch.bool, device=device), zeros(prices),
                   PriceQueueBatch.empty(prices, segment_capacity), fault)

    def _mask(self, enabled):
        if enabled.dtype != torch.bool or enabled.shape != self.initialized.shape or enabled.device != self.quality.device:
            raise TypeError('资产事件掩码须为同设备bool[assets]')
        return enabled

    def _asset_fault(self, level_fault):
        # 按位OR，不使用整数max丢失同资产多价位的不同错误位。
        result = torch.zeros_like(self.fault)
        for bit in range(32):
            values = ((level_fault.to(torch.int64) >> bit) & 1).to(torch.int32)
            reduced = torch.zeros_like(result).scatter_reduce(0, self.level_assets, values, reduce='amax')
            result = result | (reduced << bit)
        return result

    def initial_tick(self, enabled, tick):
        enabled = self._mask(enabled)
        fault = self.fault | (tick.fault | (tick.numerator.sign <= 0).to(torch.int32)) * enabled.to(torch.int32)
        valid = enabled & (fault == 0)
        return replace(self, tick=rational_select(tick, valid, self.tick),
                       tick_present=self.tick_present | valid, fault=fault)

    def initial_snapshot(self, enabled, shares, tick, quality):
        """原InitialL2Snapshot只接纳一次；所有原档位的数量同次供给，空资产仍初始化。"""
        enabled = self._mask(enabled)
        fault = self.fault | (enabled & self.initialized).to(torch.int32)
        invalid_tick = tick.fault | (tick.numerator.sign <= 0).to(torch.int32)
        fault = fault | (invalid_tick * enabled.to(torch.int32))
        amounts = micro_units(shares)
        level_mask = enabled[self.level_assets]
        fault = fault | self._asset_fault(amounts.fault * level_mask.to(torch.int32))
        valid = enabled & (fault == 0)
        level_valid = valid[self.level_assets]
        quantities = amounts.select(level_valid, amounts.constant(0))
        queues, required = self.queues.append_external(quantities)
        queue_fault = self._asset_fault(queues.fault)
        fault = fault | queue_fault
        valid = valid & (fault == 0)
        return replace(self, initialized=self.initialized | valid,
                       quality=torch.where(valid, quality, self.quality),
                       tick=rational_select(tick, valid, self.tick),
                       tick_present=self.tick_present | valid,
                       external_shares=rational_select(shares, valid[self.level_assets], self.external_shares),
                       queues=queues, fault=fault), required

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

    def level_transition(self, enabled, before, after, resync_inference):
        """原精确档位前驱检查后驱动队列；PublicTrade/BBA没有本调用。"""
        if enabled.dtype != torch.bool or enabled.shape != self.level_assets.shape or enabled.device != self.level_assets.device:
            raise TypeError('档位事件掩码须为同设备bool[levels]')
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
        queues, effects = self.queues.apply_level_transition(
            old_units.select(valid, old_units.constant(0)), new_units.select(valid, new_units.constant(0)))
        fault = fault | self._asset_fault(effects.fault)
        return replace(self, external_shares=rational_select(after, valid, self.external_shares),
                       queues=queues, fault=fault), effects


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
