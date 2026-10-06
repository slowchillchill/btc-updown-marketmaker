"""SIM Maker市场前驱、原逐fill贡献向量及共享日桶。

一个市场首次触及一天才登记一个日级前驱。市场结束与所有成交终态共同关闭它。
源金额保持精确；贡献按原数值fill sequence排序，日引用按市场结果发射序追加。
"""
from dataclasses import dataclass, replace, fields
import torch
from queues import Numbers, ExactInt, ExactRational, _take
from asset_buy import take_ratio
from taker_fee import allocate_taker_fee
from taker_days import RebateCalendarOperations, DAY, MIN_DAY, _ratio, _store_ratio, _grow
from fill_pool import _put_integer


MATURITY = 89_100_000
MAX_MATCHED = (((1 << 63) - 1 - MATURITY) // DAY) * DAY + DAY - 1


@dataclass(frozen=True)
class MakerDemand:
    committed: torch.Tensor
    fill_capacity: torch.Tensor
    day_capacity: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class MakerMarketResult:
    condition: torch.Tensor
    status: torch.Tensor
    fill_sequences: object
    days: torch.Tensor
    contributions: object
    member_mask: torch.Tensor
    accrued: object
    account_delta: object
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class MakerDays(RebateCalendarOperations):
    maturity_ms = MATURITY
    maker = True

    market_ids: torch.Tensor
    market_admitted: torch.Tensor
    market_ends: torch.Tensor
    end_reached: torch.Tensor
    market_pending: torch.Tensor
    market_result: torch.Tensor
    market_days: torch.Tensor  # [roots,markets,days]；已触及人口，终态不抹除身份。
    fill_occupied: torch.Tensor
    fill_sequences: object
    fill_markets: torch.Tensor
    fill_days: torch.Tensor
    fill_bases: object
    fill_prices: object
    fee_equivalent: object
    contributions: object
    terminal_consumed: torch.Tensor
    successful: torch.Tensor
    contribution_order: torch.Tensor
    next_contribution_order: torch.Tensor
    occupied: torch.Tensor
    days: torch.Tensor
    pending: torch.Tensor
    mature: torch.Tensor
    finalized: torch.Tensor
    paid: torch.Tensor
    accrued: object
    payable: object
    locked: object
    fault: torch.Tensor

    def coordinate_result(self, original, committed, fault):
        roots = committed.numel()
        updates = {}
        for field in fields(self):
            new, old = getattr(self, field.name), getattr(original, field.name)
            if isinstance(new, ExactInt):
                updates[field.name] = new.select(committed.repeat_interleave(new.sign.numel() // roots), old)
            elif isinstance(new, ExactRational):
                mask = committed.repeat_interleave(new.numerator.sign.numel() // roots)
                updates[field.name] = ExactRational(new.numerator.select(mask, old.numerator), new.denominator.select(mask, old.denominator))
            else:
                mask = committed.reshape((roots,) + (1,) * (new.ndim - 1))
                updates[field.name] = torch.where(mask, new, old)
        updates['fault'] = fault
        return replace(self, **updates)

    @classmethod
    def empty(cls, market_ids, market_ends, market_admitted, *, fills=4, days=2):
        if fills < 1 or days < 1 or market_ids.ndim != 2 or market_ids.size(0) < 1 or market_ids.size(1) < 1:
            raise ValueError('Maker目录须正物理容量及真实市场目录')
        if market_ids.dtype != torch.int64 or market_ends.dtype != torch.int64 or market_admitted.dtype != torch.bool:
            raise TypeError('市场身份/结束时刻须int64，准入须bool')
        if market_ids.shape != market_ends.shape or market_ids.shape != market_admitted.shape or any(
                v.device != market_ids.device for v in [market_ends, market_admitted]):
            raise ValueError('Maker市场列须同设备同人口')
        roots, markets = market_ids.shape
        f = torch.zeros((roots, fills), dtype=torch.bool, device=market_ids.device)
        d = torch.zeros((roots, days), dtype=torch.bool, device=market_ids.device)
        def integer(n):
            return ExactInt.from_i64_tensor(torch.zeros(n, dtype=torch.int64, device=market_ids.device))
        z = integer(roots * days)
        duplicate = (market_ids[:, :, None] == market_ids[:, None, :]) & market_admitted[:, :, None] & market_admitted[:, None, :]
        duplicate = duplicate & ~torch.eye(markets, dtype=torch.bool, device=market_ids.device)[None]
        fault = ((market_admitted & (market_ids < 0)).any(1) | duplicate.any(2).any(1)).to(torch.int32)
        return cls(
            market_ids, market_admitted, market_ends, torch.zeros_like(market_admitted),
            torch.zeros_like(market_ids), torch.full_like(market_ids, -1),
            torch.zeros((roots, markets, days), dtype=torch.bool, device=market_ids.device),
            f, integer(roots * fills), torch.full_like(f, -1, dtype=torch.int64),
            torch.zeros_like(f, dtype=torch.int64), integer(roots * fills),
            _ratio(integer(roots * fills)), _ratio(integer(roots * fills)), _ratio(integer(roots * fills)),
            f.clone(), f.clone(), torch.full_like(f, -1, dtype=torch.int64),
            torch.zeros(roots, dtype=torch.int64, device=market_ids.device),
            d, torch.zeros_like(d, dtype=torch.int64), torch.zeros_like(d, dtype=torch.int64),
            d.clone(), d.clone(), d.clone(), _ratio(z), _ratio(z), _ratio(z), fault,
        )

    def resized(self, *, fills, days):
        roots, old_f = self.fill_occupied.shape
        old_d = self.occupied.size(1)
        if fills < old_f or days < old_d:
            raise ValueError('未退役Maker身份不能缩容')
        def pad(v, n, value=0):
            return torch.nn.functional.pad(v, (0, n - v.size(-1)), value=value)
        def ratio(v, old, new):
            return ExactRational(_grow(v.numerator, roots, old, new), _grow(v.denominator, roots, old, new, True))
        return replace(self,
            market_days=pad(self.market_days, days), fill_occupied=pad(self.fill_occupied, fills),
            fill_sequences=_grow(self.fill_sequences, roots, old_f, fills),
            fill_markets=pad(self.fill_markets, fills, -1), fill_days=pad(self.fill_days, fills),
            fill_bases=_grow(self.fill_bases, roots, old_f, fills), fill_prices=ratio(self.fill_prices, old_f, fills),
            fee_equivalent=ratio(self.fee_equivalent, old_f, fills), contributions=ratio(self.contributions, old_f, fills),
            terminal_consumed=pad(self.terminal_consumed, fills), successful=pad(self.successful, fills),
            contribution_order=pad(self.contribution_order, fills, -1), occupied=pad(self.occupied, days),
            days=pad(self.days, days), pending=pad(self.pending, days), mature=pad(self.mature, days),
            finalized=pad(self.finalized, days), paid=pad(self.paid, days),
            accrued=ratio(self.accrued, old_d, days), payable=ratio(self.payable, old_d, days), locked=ratio(self.locked, old_d, days))

    def _lookup(self, sequence):
        roots, capacity = self.fill_occupied.shape
        owner = torch.arange(roots, device=self.days.device).repeat_interleave(capacity)
        relation = self.fill_sequences.compare(_take(sequence, owner))
        matches = self.fill_occupied & (relation.order.reshape(roots, capacity) == 0)
        fault = Numbers.combine_fault_rows(torch.where(self.fill_occupied, relation.fault.reshape(roots, capacity), 0))
        return matches.to(torch.int64).argmax(1), matches.any(1), fault

    def register(self, sequence, condition, matched_at, base, price, enabled, fee_rate, fee_exponent):
        roots, capacity = self.fill_occupied.shape
        root = torch.arange(roots, device=self.days.device)
        for value in [condition, matched_at]:
            if value.shape != (roots,) or value.dtype != torch.int64 or value.device != self.days.device:
                raise TypeError('Maker市场/成交时刻须同设备int64[roots]')
        safe_time = matched_at.clamp(MIN_DAY, MAX_MATCHED)
        day = torch.div(safe_time, DAY, rounding_mode='floor') * DAY
        same_market = self.market_admitted & (self.market_ids == condition[:, None])
        mi = same_market.to(torch.int64).argmax(1)
        known_market = same_market.any(1)
        fi, found, lookup_fault = self._lookup(sequence)
        fi = torch.where(found, fi, (~self.fill_occupied).to(torch.int64).argmax(1))
        same_day = self.occupied & (self.days == day[:, None])
        known_day = same_day.any(1)
        di = torch.where(known_day, same_day.to(torch.int64).argmax(1), (~self.occupied).to(torch.int64).argmax(1))
        old_base = _take(self.fill_bases, root * capacity + fi).compare(base)
        old_price = take_ratio(self.fill_prices, root * capacity + fi).compare(price)
        upper = sequence.compare(sequence.constant((1 << 64) - 1))
        p_upper = price.compare(_ratio(base.constant(1)))
        # 原Maker TradeLifecycle各阶段都提供真实逐fill H5费用等价。
        fee = allocate_taker_fee(base, price, fee_rate, fee_exponent, roots, 1)
        equivalent = ExactRational.ratio(fee.total_micro, base.constant(1_000_000))
        error = sequence.fault | base.fault | price.fault | upper.fault | p_upper.fault
        error = error | ((sequence.sign <= 0) | (upper.order > 0) | (base.sign <= 0) |
            (price.numerator.sign <= 0) | (p_upper.order >= 0) | ~known_market |
            (matched_at < MIN_DAY) | (matched_at > MAX_MATCHED)).to(torch.int32)
        mismatch = found & ((self.fill_markets[root, fi] != mi) | (self.fill_days[root, fi] != day) |
            (old_base.order != 0) | (old_price.order != 0))
        error = error | old_base.fault | old_price.fault | lookup_fault | mismatch.to(torch.int32) | fee.fault | equivalent.fault
        fresh = enabled & ~found
        error = error | (fresh & ((self.market_result[root, mi] >= 0) | self.end_reached[root, mi] |
            (known_day & self.finalized[root, di]))).to(torch.int32)
        fault = self.fault | torch.where(enabled, error, 0)
        fill_demand = self.fill_occupied.to(torch.int64).sum(1) + fresh.to(torch.int64)
        day_demand = self.occupied.to(torch.int64).sum(1) + (fresh & ~known_day).to(torch.int64)
        use = fresh & (fill_demand <= capacity) & (day_demand <= self.occupied.size(1)) & (fault == 0)
        fm = torch.zeros_like(self.fill_occupied).scatter(1, fi[:, None], use[:, None])
        dm = torch.zeros_like(self.occupied).scatter(1, di[:, None], use[:, None])
        touched = self.market_days[root, mi, di]
        market_days = self.market_days.clone()
        market_days[root, mi, di] = touched | use
        added = torch.zeros_like(self.market_pending).scatter(1, mi[:, None], use.to(torch.int64)[:, None])
        result = replace(self,
            fill_occupied=self.fill_occupied | fm, fill_sequences=_put_integer(self.fill_sequences, sequence, root * capacity + fi, use),
            fill_markets=torch.where(fm, mi[:, None], self.fill_markets), fill_days=torch.where(fm, day[:, None], self.fill_days),
            fill_bases=_put_integer(self.fill_bases, base, root * capacity + fi, use),
            fill_prices=_store_ratio(self.fill_prices, price, root * capacity + fi, use),
            fee_equivalent=_store_ratio(self.fee_equivalent, equivalent, root * capacity + fi, use),
            market_pending=self.market_pending + added, market_days=market_days,
            occupied=self.occupied | dm, days=torch.where(dm, day[:, None], self.days),
            pending=self.pending + (dm & ~touched[:, None]).to(torch.int64), fault=fault)
        return result, MakerDemand((~enabled | found | use) & (fault == 0), fill_demand, day_demand, fault)

    def complete(self, sequence, terminal, success, maker_rate):
        roots, capacity = self.fill_occupied.shape
        root = torch.arange(roots, device=self.days.device)
        fi, found, lookup_fault = self._lookup(sequence)
        mi = self.fill_markets[root, fi].clamp_min(0)
        requested = terminal & ~self.terminal_consumed[root, fi]
        equivalent = take_ratio(self.fee_equivalent, root * capacity + fi)
        contribution = equivalent * maker_rate
        fault = self.fault | torch.where(terminal, lookup_fault | (~found).to(torch.int32), 0)
        fault = fault | torch.where(requested, 
            (self.market_pending[root, mi] <= 0).to(torch.int32), 0)
        fault = fault | torch.where(requested & success, contribution.fault | (maker_rate.numerator.sign < 0).to(torch.int32), 0)
        use = requested & found & (fault == 0)
        fm = torch.zeros_like(self.fill_occupied).scatter(1, fi[:, None], use[:, None])
        subtract = torch.zeros_like(self.market_pending).scatter(1, mi[:, None], use.to(torch.int64)[:, None])
        result = replace(self, terminal_consumed=self.terminal_consumed | fm,
            successful=torch.where(fm, success[:, None], self.successful),
            market_pending=self.market_pending - subtract,
            contributions=_store_ratio(self.contributions, contribution, root * capacity + fi, use & success), fault=fault)
        return result, terminal & (~requested | found) & (fault == 0)

    def next_end(self):
        valid = self.market_admitted & ~self.end_reached
        at = torch.where(valid, self.market_ends, torch.full_like(self.market_ends, (1 << 63) - 1)).min(1).values
        # 市场目录的物理顺序须由冷绑定保留原condition字串序。
        mi = (valid & (self.market_ends == at[:, None])).to(torch.int64).argmax(1)
        return at, mi, valid.any(1)

    def advance_end(self, at, enabled):
        if at.shape != self.fault.shape or at.dtype != torch.int64 or at.device != self.days.device:
            raise TypeError('市场结束钟须同设备int64[roots]')
        scheduled, mi, exists = self.next_end()
        fault = self.fault | ((at > scheduled) & exists & enabled).to(torch.int32)
        use = enabled & exists & (at == scheduled) & (fault == 0)
        mask = torch.zeros_like(self.end_reached).scatter(1, mi[:, None], use[:, None])
        return replace(self, end_reached=self.end_reached | mask, fault=fault), mi, use

    def _ordered(self, valid):
        roots, capacity = valid.shape
        order = torch.arange(capacity, device=valid.device).expand(roots, capacity)
        words = self.fill_sequences.words.to(torch.int64) & 0xffffffff
        for limb in [0, 1]:
            keys = words[:, limb].reshape(roots, capacity) if words.size(1) > limb else torch.zeros_like(order)
            order = order.gather(1, keys.gather(1, order).argsort(dim=1, stable=True))
        order = order.gather(1, (~valid).to(torch.int64).gather(1, order).argsort(dim=1, stable=True))
        index = (torch.arange(roots, device=valid.device)[:, None] * capacity + order).flatten()
        return order, index, valid.gather(1, order)

    def finalize_market(self, account, market_slot, enabled):
        roots, capacity = self.fill_occupied.shape
        root = torch.arange(roots, device=self.days.device)
        mi = market_slot.clamp(0, self.market_ids.size(1) - 1)
        valid_slot = (market_slot >= 0) & (market_slot < self.market_ids.size(1))
        fault = self.fault | (enabled & ~valid_slot).to(torch.int32)
        requested = enabled & self.market_admitted[root, mi] & self.end_reached[root, mi] & (self.market_pending[root, mi] == 0) & (self.market_result[root, mi] < 0) & (fault == 0)
        valid = self.fill_occupied & self.terminal_consumed & self.successful & (self.fill_markets == mi[:, None])
        order, index, members = self._ordered(valid)
        refs = _take(self.fill_sequences, index)
        amounts = take_ratio(self.contributions, index)
        days = self.fill_days.gather(1, order)
        # 汇总逐fill原金额；不重新进行H5，不先按日拆成多个账户delta。
        accrued = _ratio(account.cash.constant(0))
        by_day = self.accrued
        for column in range(capacity):
            piece = take_ratio(amounts, root * capacity + column)
            piece = ExactRational(piece.numerator.select(members[:, column], piece.numerator.constant(0)), piece.denominator)
            accrued = accrued + piece
            for di in range(self.occupied.size(1)):
                old = take_ratio(by_day, root * self.occupied.size(1) + di)
                add = members[:, column] & (days[:, column] == self.days[:, di])
                by_day = _store_ratio(by_day, old + piece, root * self.occupied.size(1) + di, requested & add)
        fault = fault | torch.where(requested, accrued.fault | Numbers.combine_fault_rows(by_day.fault.reshape_as(self.occupied)), 0)
        touched = self.market_days[root, mi, :]
        fault = fault | (requested & (touched & (self.pending <= 0)).any(1)).to(torch.int32)
        account, delta = account.accrue_rebate(accrued, torch.ones_like(enabled), requested & (accrued.numerator.sign > 0) & (fault == 0))
        fault = fault | account.fault
        use = requested & (fault == 0)
        status = (~members.any(1)).to(torch.int64)
        market_result = self.market_result.clone()
        market_result[root, mi] = torch.where(use, status, market_result[root, mi])
        rank = members.to(torch.int64).cumsum(1) - 1 + self.next_contribution_order[:, None]
        stored_rank = torch.zeros_like(self.contribution_order).scatter(1, order, rank)
        return (replace(self, market_result=market_result,
            pending=self.pending - (touched & use[:, None]).to(torch.int64),
            accrued=ExactRational(by_day.numerator.select(use.repeat_interleave(self.occupied.size(1)), self.accrued.numerator),
                by_day.denominator.select(use.repeat_interleave(self.occupied.size(1)), self.accrued.denominator)),
            contribution_order=torch.where(valid & use[:, None], stored_rank, self.contribution_order),
            next_contribution_order=self.next_contribution_order + (members & use[:, None]).to(torch.int64).sum(1), fault=fault), account,
            MakerMarketResult(self.market_ids[root, mi], status, refs, days, amounts, members, accrued, delta, use, fault))

    def _references(self, day):
        valid = self.fill_occupied & self.successful & (self.contribution_order >= 0) & (self.fill_days == day[:, None])
        key = torch.where(valid, self.contribution_order, torch.full_like(self.contribution_order, (1 << 63) - 1))
        order = key.argsort(dim=1, stable=True)
        roots, capacity = valid.shape
        index = (torch.arange(roots, device=day.device)[:, None] * capacity + order).flatten()
        return _take(self.fill_sequences, index), valid.gather(1, order)
