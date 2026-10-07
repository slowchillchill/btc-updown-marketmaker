"""SIM逐成交原订单、角色和来源价；物理fill槽与原u64身份分开。"""
from dataclasses import dataclass, replace
import torch
from queues import ExactRational, _take
from asset_buy import take_ratio


@dataclass(frozen=True)
class FillOrigins:
    order_sequences: object
    order_slots: torch.Tensor
    conditions: torch.Tensor
    roles: torch.Tensor  # 0=Taker，1=MakerInferred，-1=尚未绑定。
    prices: object

    @classmethod
    def empty(cls, zero, roots, capacity):
        from queues import _repeat
        zero = _repeat(zero.constant(0), capacity)
        absent = torch.full((roots, capacity), -1, dtype=torch.int64, device=zero.words.device)
        return cls(zero, absent, absent, absent, ExactRational(zero, zero.constant(1)))

    def take(self, index):
        return FillOrigins(_take(self.order_sequences, index), self.order_slots.flatten()[index],
            self.conditions.flatten()[index], self.roles.flatten()[index], take_ratio(self.prices, index))

    def put(self, incoming, index, enabled):
        from fill_pool import _put_integer
        def column(original, value):
            return original.flatten().index_copy(0, index,
                torch.where(enabled, value.flatten(), original.flatten()[index])).reshape_as(original)
        return replace(self,
            order_sequences=_put_integer(self.order_sequences, incoming.order_sequences, index, enabled),
            order_slots=column(self.order_slots, incoming.order_slots),
            conditions=column(self.conditions, incoming.conditions), roles=column(self.roles, incoming.roles),
            prices=ExactRational(_put_integer(self.prices.numerator, incoming.prices.numerator, index, enabled),
                _put_integer(self.prices.denominator, incoming.prices.denominator, index, enabled)))

    def coordinate(self, original, enabled):
        capacity = self.roles.shape[1]
        index = torch.arange(self.roles.numel(), device=self.roles.device)
        return original.put(self.take(index), index, enabled.repeat_interleave(capacity))
