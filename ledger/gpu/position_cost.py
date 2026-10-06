"""原已应用fill成本/费用，补偿关联实际应用项，不沿失败ordinal另造金额。

本读面只覆盖已接入的Existing成交经济路径；市场兑付成本清理须由原结算
消费者另接。目录槽位沿LifecycleDirectory原trade身份，真实金额不量化。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactRational, Numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class PositionCostRead:
    cost: object
    fee: object
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class AssetPositionCostRead:
    cost: object
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PositionCosts:
    active: torch.Tensor
    conditions: torch.Tensor
    assets: torch.Tensor
    application_ordinals: torch.Tensor
    cost: object
    fee: object
    known: torch.Tensor

    @classmethod
    def empty(cls, roots, capacity, device, *, known=True):
        shape = (roots, capacity)
        active = torch.zeros(shape, dtype=torch.bool, device=device)
        identity = torch.full(shape, -1, dtype=torch.int64, device=device)
        zero = Numbers.ExactInt.from_i64_tensor(torch.zeros(roots*capacity, dtype=torch.int64, device=device))
        ratio = ExactRational(zero, zero.constant(1))
        return cls(active, identity, identity.clone(), identity.clone(), ratio, ratio,
            torch.full((roots,), known, dtype=torch.bool, device=device))

    def transition(self, slots, conditions, assets, application_ordinals, cost, fee, applied, compensated):
        roots, width = self.active.shape
        repeated = torch.arange(roots, device=slots.device).repeat_interleave(width)
        install = torch.zeros_like(self.active).scatter(1, slots[:, None], applied[:, None])
        remove = torch.zeros_like(self.active).scatter(1, slots[:, None], compensated[:, None])
        return replace(self, active=(self.active | install) & ~remove,
            conditions=torch.where(install, conditions[:, None], self.conditions),
            assets=torch.where(install, assets[:, None], self.assets),
            application_ordinals=torch.where(install, application_ordinals[:, None], self.application_ordinals),
            cost=select(take_ratio(cost, repeated), install.flatten(), self.cost),
            fee=select(take_ratio(fee, repeated), install.flatten(), self.fee))

    def read(self, scope_conditions=None, scope_present=None):
        roots, width = self.active.shape
        member = self.active
        if scope_conditions is not None:
            if scope_conditions.shape != scope_present.shape or scope_conditions.size(0) != roots:
                raise ValueError('成本scope须原根/condition人口')
            member = member & ((self.conditions[:, :, None] == scope_conditions[:, None, :])
                & scope_present[:, None, :]).any(2)
        zero = ExactRational(self.cost.numerator.constant(0), self.cost.denominator.constant(1))
        cost = sum_rows(select(self.cost, member.flatten(), zero), roots, width)
        fee = sum_rows(select(self.fee, member.flatten(), zero), roots, width)
        return PositionCostRead(cost, fee, self.known, cost.fault | fee.fault)

    def asset_cost(self, asset_roots):
        roots, width = self.active.shape
        if asset_roots.dtype != torch.int64 or asset_roots.device != self.active.device:
            raise TypeError('成本资产须同设备原根坐标')
        assets = asset_roots.numel()
        positions = asset_roots[:, None]*width + torch.arange(width, device=asset_roots.device)[None, :]
        member = self.active[asset_roots] & (self.assets[asset_roots] == torch.arange(assets, device=asset_roots.device)[:, None])
        value = take_ratio(self.cost, positions.flatten())
        zero = ExactRational(value.numerator.constant(0), value.denominator.constant(1))
        cost = sum_rows(select(value, member.flatten(), zero), assets, width)
        return AssetPositionCostRead(cost, self.known[asset_roots], cost.fault)
