"""原未判定市场的利润/最大历史亏损读面，随每条Ledger提交更新。

配对和派生成本来自D-0295成功池；费用来自公开Trade净费，返佣只取
已冻结日payable归属。孤腿本金独立读出，不提前计入未判定利润。
Resolution/请求式结算的payout、费用、兑付和负债尚须原消费者接齐。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers
from asset_buy import take_ratio
from success_pool import select
from rebate_day import sum_population


@dataclass(frozen=True)
class UnresolvedMarketRead:
    guarantee: object
    paired_cost: object
    orphan_cost: object
    profit: object
    maximum_loss: object
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class UnresolvedMarkets:
    asset_indices: torch.Tensor  # 每市场一个原固定资产，配对量不能重复求和。
    roots: torch.Tensor
    conditions: torch.Tensor
    profit: object
    maximum_loss: object
    known: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def open(cls, pools, asset_indices, *, known=True):
        if asset_indices.dtype != torch.int64 or asset_indices.device != pools.asset_roots.device or asset_indices.ndim != 1:
            raise TypeError('市场代表资产须同设备int64[markets]固定目录')
        assets = pools.asset_roots.numel()
        if assets < 1:
            raise ValueError('市场目录需原固定双资产')
        safe = asset_indices.clamp(0, assets-1)
        roots = pools.asset_roots[safe]
        conditions = pools.condition_slots[safe]
        duplicate = (roots[:, None] == roots[None, :]) & (conditions[:, None] == conditions[None, :])
        duplicate &= ~torch.eye(safe.numel(), dtype=torch.bool, device=safe.device)
        fault = ((asset_indices < 0) | (asset_indices >= assets) | duplicate.any(1)).to(torch.int32)
        zero = ExactInt.from_i64_tensor(torch.zeros(safe.numel(), dtype=torch.int64, device=safe.device))
        ratio = ExactRational(zero, zero.constant(1))
        return cls(safe, roots, conditions, ratio, ratio,
            torch.full_like(safe, known, dtype=torch.bool), fault)

    def values(self, pools, trades, days, resolutions=None):
        count = self.roots.numel()
        first, second = self.asset_indices, pools.counterpart[self.asset_indices]
        guarantee = take_ratio(pools.paired_quantity, first)
        paired_cost = take_ratio(pools.paired_cost, first) + take_ratio(pools.paired_cost, second)
        orphan_cost = take_ratio(pools.cost, first) + take_ratio(pools.cost, second)
        trade_width = trades.has_fact.size(1)
        trade_index = self.roots[:, None]*trade_width + torch.arange(trade_width, device=first.device)[None, :]
        member = trades.has_fact[self.roots] & (trades.conditions[self.roots] == self.conditions[:, None])
        detail_missing = (member & ~trades.details_known[self.roots]).any(1)
        fee = take_ratio(trades.immediate_fee, trade_index.flatten())
        fee_use = member & trades.details_known[self.roots] & trades.economic_applied[self.roots]
        zero = ExactRational(fee.numerator.constant(0), fee.denominator.constant(1))
        net_fee = sum_population(select(fee, fee_use.flatten(), zero), count, trade_width)
        rebate = ExactRational(guarantee.numerator.constant(0), guarantee.denominator.constant(1))
        fault = self.fault | pools.fault[self.roots] | guarantee.fault | paired_cost.fault | orphan_cost.fault | net_fee.fault
        if days is not None:
            width, conditions = days.occupied.size(1), days.condition_slots.size(1)
            positions = (self.roots[:, None, None]*width*conditions +
                torch.arange(width, device=first.device)[None, :, None]*conditions +
                torch.arange(conditions, device=first.device)[None, None, :])
            values = take_ratio(days.condition_payable, positions.expand(count, width, conditions).flatten())
            member_day = days.occupied[self.roots, :, None] & (
                days.condition_slots[self.roots, None, :] == self.conditions[:, None, None])
            absent = ExactRational(values.numerator.constant(0), values.denominator.constant(1))
            rebate = sum_population(select(values, member_day.flatten(), absent), count, width*conditions)
            fault |= rebate.fault | days.fault[self.roots]
        profit = guarantee - paired_cost + rebate - net_fee
        if resolutions is not None:
            orphan = resolutions.orphan_value(pools)
            zero = ExactRational(orphan.numerator.constant(0), orphan.denominator.constant(1))
            profit = profit + select(orphan-orphan_cost, resolutions.present, zero)
            fault |= resolutions.fault | orphan.fault
        fault |= profit.fault
        return guarantee, paired_cost, orphan_cost, profit, ~detail_missing, fault

    def refresh(self, pools, trades, days, committed_roots, *, resolutions=None):
        """候选先计算完整精确值，随后随同一Ledger事务提交，不能冻结时才补最大值。"""
        _, _, _, profit, complete, fault = self.values(pools, trades, days, resolutions)
        zero = ExactRational(profit.numerator.constant(0), profit.denominator.constant(1))
        loss = zero - profit
        comparison = loss.compare(self.maximum_loss)
        maximum = select(loss, comparison.order > 0, self.maximum_loss)
        fault |= loss.fault | comparison.fault
        use = committed_roots[self.roots]
        return replace(self, profit=select(profit, use & (fault == 0), self.profit),
            maximum_loss=select(maximum, use & complete & self.known & (fault == 0), self.maximum_loss),
            known=self.known & (~use | complete), fault=self.fault | torch.where(use, fault, 0))

    def coordinated(self, original, valid_roots):
        use = valid_roots[self.roots]
        return replace(self, profit=select(self.profit, use, original.profit),
            maximum_loss=select(self.maximum_loss, use, original.maximum_loss),
            known=torch.where(use, self.known, original.known))

    def read(self, pools, trades, days, *, resolutions=None):
        guarantee, paired_cost, orphan_cost, _, complete, fault = self.values(pools, trades, days, resolutions)
        if resolutions is not None:
            zero = ExactRational(guarantee.numerator.constant(0), guarantee.denominator.constant(1))
            guarantee = select(zero, resolutions.present, guarantee)
            orphan_cost = select(zero, resolutions.present, orphan_cost)
        return UnresolvedMarketRead(guarantee, paired_cost, orphan_cost, self.profit,
            self.maximum_loss, self.known & complete, fault)
