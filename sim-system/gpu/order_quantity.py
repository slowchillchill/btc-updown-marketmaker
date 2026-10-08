"""原SIM Base/QuoteBudget计量共用订单目录；撮合段、账户与NOE各自维护。"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt


@dataclass(frozen=True)
class MakerFillCandidate:
    base: object
    quote: object
    emitted: object
    fault: object


@dataclass(frozen=True)
class BaseOrderQuantity:
    accepted_base_units: object
    matched_base_units: object
    pending_maker_depletion_base_units: object
    open_base_units: object
    venue_dust_closed_base_units: object
    canceled_base_units: object
    expired_base_units: object
    market_resolved_canceled_base_units: object
    accepted_quote_units: object
    matched_quote_units: object
    open_encumbered_quote_units: object
    released_price_improvement_quote_units: object
    venue_dust_released_quote_units: object
    canceled_released_quote_units: object
    expired_released_quote_units: object
    market_resolved_released_quote_units: object
    fault: object

    quantity_kind: object = None  # 原OrderQuantityProjectionV2：Base/QuoteBudget=0/1。
    accepted_quote_budget_units: object = None
    released_or_unspent_quote_units: object = None
    actual_matched_base_units: object = None
    sdk_taker_amount_audit_base_units: object = None

    def __post_init__(self):
        zero = self.accepted_base_units.constant(0)
        if self.quantity_kind is None:
            object.__setattr__(self, 'quantity_kind', torch.zeros_like(self.fault, dtype=torch.int64))
        for name in ('accepted_quote_budget_units', 'released_or_unspent_quote_units',
                     'actual_matched_base_units', 'sdk_taker_amount_audit_base_units'):
            if getattr(self, name) is None:
                object.__setattr__(self, name, zero)

    @classmethod
    def from_quote_plan(cls, budget, audit_base, matched_base, matched_quote):
        zero = budget.constant(0)
        fault = budget.fault | audit_base.fault | matched_base.fault | matched_quote.fault
        released = budget-matched_quote
        fault |= released.fault | ((budget.sign <= 0) | (audit_base.sign < 0) |
            (matched_base.sign < 0) | (matched_quote.sign < 0) | (released.sign < 0)).to(torch.int32)
        values = [zero]*16
        values[9] = matched_quote
        return cls(*values, fault, torch.ones_like(fault, dtype=torch.int64),
            budget, released, matched_base, audit_base)

    @classmethod
    def from_taker_plan(cls, accepted_base, accepted_quote, matched_base, matched_quote):
        zero = accepted_base.constant(0)
        fault = zero.fault
        for value in (accepted_base,accepted_quote,matched_base,matched_quote):
            fault = fault | value.fault | (value.sign < 0).to(fault.dtype)
        fault = fault | ((accepted_base.sign <= 0) | (accepted_quote.sign <= 0)).to(fault.dtype)
        upper = matched_base.compare(accepted_base)
        fault = fault | upper.fault | (upper.order > 0).to(fault.dtype)
        denominator = accepted_base.select(accepted_base.sign > 0,accepted_base.constant(1))
        allocated = (matched_base*accepted_quote).divrem(denominator)[0]
        improvement = allocated-matched_quote
        remaining = accepted_base-matched_base
        encumbered = accepted_quote-allocated
        fault = fault | allocated.fault | improvement.fault | remaining.fault | encumbered.fault
        fault = fault | ((improvement.sign < 0) | (remaining.sign < 0) | (encumbered.sign < 0)).to(fault.dtype)
        return cls(accepted_base,matched_base,zero,remaining,zero,zero,zero,zero,accepted_quote,
            matched_quote,encumbered,improvement,zero,zero,zero,zero,fault)

    def initial_principal(self):
        base_principal = self.matched_quote_units+self.open_encumbered_quote_units
        quote_principal = self.accepted_quote_budget_units-self.released_or_unspent_quote_units
        value = quote_principal.select(self.quantity_kind == 1, base_principal)
        return ExactInt(value.words,value.sign,value.fault | self.fault)

    def _enabled(self,enabled):
        if enabled.dtype != torch.bool or enabled.shape != self.fault.shape or enabled.device != self.fault.device:
            raise TypeError('订单事件掩码须为同设备bool[orders]')
        return enabled

    def maker_depletion(self, amount, enabled):
        """零累计quote只保留pending；不生成假fill或推进fill sequence。"""
        enabled = self._enabled(enabled)
        zero = amount.constant(0)
        error = amount.fault | (amount.sign < 0).to(self.fault.dtype)
        remaining = self.open_base_units-amount
        pending = self.pending_maker_depletion_base_units+amount
        matched_after = self.matched_base_units+pending
        denominator = self.accepted_base_units.select(self.accepted_base_units.sign > 0,amount.constant(1))
        before = (self.matched_base_units*self.accepted_quote_units).divrem(denominator)[0]
        after = (matched_after*self.accepted_quote_units).divrem(denominator)[0]
        quote = after-before
        encumbered = self.open_encumbered_quote_units-quote
        next_matched_quote = self.matched_quote_units+quote
        upper = matched_after.compare(self.accepted_base_units)
        error = error | remaining.fault | pending.fault | quote.fault | encumbered.fault | upper.fault | next_matched_quote.fault
        error = error | ((remaining.sign < 0) | (quote.sign < 0) | (encumbered.sign < 0) | (upper.order > 0)).to(self.fault.dtype)
        fault = self.fault | error*enabled.to(self.fault.dtype)
        valid = enabled & (fault == 0)
        emitted = valid & (quote.sign > 0)
        candidate = replace(self,
            open_base_units=remaining.select(valid,self.open_base_units),
            pending_maker_depletion_base_units=zero.select(emitted,pending.select(valid,self.pending_maker_depletion_base_units)),
            matched_base_units=matched_after.select(emitted,self.matched_base_units),
            matched_quote_units=next_matched_quote.select(emitted,self.matched_quote_units),
            open_encumbered_quote_units=encumbered.select(emitted,self.open_encumbered_quote_units),fault=fault)
        return candidate,MakerFillCandidate(pending.select(emitted,zero),quote.select(emitted,zero),emitted,fault)

    def cancel(self, enabled):
        """原可撤量含pending+open；释放只取原open_encumbered，不伪造pending成交。"""
        return self._close(enabled, 'canceled_base_units', 'canceled_released_quote_units')

    def market_resolution(self, enabled):
        """原判定关闭写market_resolved计量；零释放仍可关闭pending-only订单。"""
        return self._close(enabled, 'market_resolved_canceled_base_units',
                           'market_resolved_released_quote_units')

    def _close(self, enabled, base_field, quote_field):
        enabled = self._enabled(enabled)
        canceled = self.pending_maker_depletion_base_units+self.open_base_units
        released = self.open_encumbered_quote_units
        next_canceled = getattr(self, base_field)+canceled
        next_released = getattr(self, quote_field)+released
        error = canceled.fault | released.fault | next_canceled.fault | next_released.fault
        fault = self.fault | error*enabled.to(self.fault.dtype)
        valid = enabled & (fault == 0) & (canceled.sign > 0)
        zero = canceled.constant(0)
        candidate = replace(self,
            pending_maker_depletion_base_units=zero.select(valid,self.pending_maker_depletion_base_units),
            open_base_units=zero.select(valid,self.open_base_units),
            open_encumbered_quote_units=zero.select(valid,self.open_encumbered_quote_units),
            fault=fault, **{base_field:next_canceled.select(valid,getattr(self,base_field)),
                           quote_field:next_released.select(valid,getattr(self,quote_field))})
        return candidate,canceled.select(valid,zero),released.select(valid,zero),valid
