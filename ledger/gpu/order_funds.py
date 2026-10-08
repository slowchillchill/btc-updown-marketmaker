"""原NOE经济分录到逐订单本金/费用的只读归属，不新增资金事实。

真实经济先于Trade详情时保留原记录引用。关联到达后才归入订单，
此前同condition（或未知condition）的金额读面为Unknown。
本目录绑定当前共享NOE块；跨块宿主须迁移未关联引用及身份后再退役。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, ExactInt, ExactRational, _take
from asset_buy import take_ratio
from success_pool import select
from rebate_day import sum_population
from reservation import grow_integer


@dataclass(frozen=True)
class OrderFundsRead:
    principal: object
    fee: object
    known: torch.Tensor
    last_sequence: object
    has_last_sequence: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class OrderFunds:
    keys: object
    occupied: torch.Tensor
    conditions: torch.Tensor
    principal: object
    fee: object
    has_origin: torch.Tensor
    last_sequence: object
    has_last_sequence: torch.Tensor
    trade_orders: object
    trade_bound: torch.Tensor
    unassigned_records: torch.Tensor
    known: torch.Tensor
    fault: torch.Tensor
    capacity_required: torch.Tensor

    @classmethod
    def empty(cls, roots, capacity, trade_capacity, records, device, *, known=True):
        if min(capacity, trade_capacity) < 1 or records < 0:
            raise ValueError('资金目录需要正身份容量及非负共享记录人口')
        blank = torch.zeros((roots, capacity), dtype=torch.bool, device=device)
        keys = ExactInt.from_i64_tensor(torch.zeros(roots * capacity, dtype=torch.int64, device=device))
        ratio = ExactRational(keys, keys.constant(1))
        trade_keys = ExactInt.from_i64_tensor(torch.zeros(roots * trade_capacity, dtype=torch.int64, device=device))
        return cls(keys, blank, torch.full_like(blank, -1, dtype=torch.int64),
            ratio, ratio, blank.clone(), keys, blank.clone(), trade_keys,
            torch.zeros((roots, trade_capacity), dtype=torch.bool, device=device),
            torch.zeros((roots, records), dtype=torch.bool, device=device),
            torch.full((roots,), known, dtype=torch.bool, device=device),
            torch.zeros(roots, dtype=torch.int32, device=device),
            torch.full((roots,), capacity, dtype=torch.int64, device=device))

    def resized(self, capacity):
        roots, old = self.occupied.shape
        if capacity < old:
            raise ValueError('未退役订单资金目录不能缩小')
        def pad(value, fill):
            return torch.nn.functional.pad(value, (0, capacity-old), value=fill)
        def ratio(value):
            numerator = grow_integer(value.numerator, roots, old, capacity)
            denominator = grow_integer(value.denominator, roots, old, capacity)
            fresh = (torch.arange(capacity, device=self.occupied.device) >= old).expand(roots, -1).flatten()
            denominator = denominator.constant(1).select(fresh, denominator)
            return ExactRational(numerator, denominator)
        return replace(self, keys=grow_integer(self.keys, roots, old, capacity),
            occupied=pad(self.occupied, False), conditions=pad(self.conditions, -1),
            principal=ratio(self.principal), fee=ratio(self.fee), has_origin=pad(self.has_origin, False),
            last_sequence=grow_integer(self.last_sequence, roots, old, capacity),
            has_last_sequence=pad(self.has_last_sequence, False),
            capacity_required=torch.full_like(self.capacity_required, capacity))

    def lookup(self, order):
        roots, width = self.occupied.shape
        repeated = torch.arange(roots, device=self.occupied.device).repeat_interleave(width)
        relation = self.keys.compare(_take(order, repeated))
        same = (relation.order.reshape(roots, width) == 0) & self.occupied
        found = same.any(1)
        index = torch.where(found, same.to(torch.int64).argmax(1), (~self.occupied).to(torch.int64).argmax(1))
        return index, found, Numbers.combine_fault_rows(relation.fault.reshape(roots, width))

    def append_records(self,extra,trade_capacity):
        """扩展原未关联record及trade关联缓存；旧关联不重建。"""
        roots,old=self.trade_bound.shape
        if trade_capacity<old:
            raise ValueError('未退役成交关联不能缩小')
        return replace(self,trade_orders=grow_integer(self.trade_orders,roots,old,trade_capacity),
            trade_bound=torch.nn.functional.pad(self.trade_bound,(0,trade_capacity-old),value=False),
            unassigned_records=torch.nn.functional.pad(self.unassigned_records,(0,extra),value=False))

    def advance(self, lifecycle, columns, trade_slots, local, ledger_sequence, applied_sequences,
                evidence, economic):
        roots, capacity = self.occupied.shape
        width = columns.row_refs.size(1)
        if self.unassigned_records.shape != (roots, width):
            raise ValueError('未关联资金须绑定同一共享NOE记录人口')
        root = torch.arange(roots, device=local.device)
        position = root * width + local
        tag = lifecycle.schemas[root, local]
        economic_tag = lifecycle.economic_tags[root, local]
        trade = evidence & (tag == 8)
        accepted = evidence & ((tag == 3)|(tag==26)|(tag==27)) & lifecycle.accepted[root, local]
        observed = trade | accepted | (evidence & ((tag == 7) | (tag == 19)))
        order_economic = economic & ((economic_tag == 0) | (economic_tag == 2) | (economic_tag == 5))
        fill_economic = economic & (economic_tag >= 6) & (economic_tag <= 8)
        trade_capacity = self.trade_bound.size(1)
        binding_position = root * trade_capacity + trade_slots
        bound = self.trade_bound[root, trade_slots]
        source_order = _take(lifecycle.orders, position)
        order = source_order.select(observed | order_economic, _take(self.trade_orders, binding_position))
        row_requested = observed | order_economic | (fill_economic & bound)
        at, found, lookup_fault = self.lookup(order)
        required = torch.where(row_requested & ~found & self.occupied.all(1), capacity+1, capacity)
        fault = self.fault | torch.where(row_requested, lookup_fault, 0)
        ready = (required <= capacity) & (fault == 0)
        row = row_requested & ready

        # 原已提交经济记录保留自身条目序。数值归约不生成另一份经济身份。
        repeated = root.repeat_interleave(width)
        same_fill = lifecycle.fills.compare(_take(_take(lifecycle.fills, position), repeated))
        pending = self.unassigned_records & (same_fill.order.reshape(roots, width) == 0) & trade[:, None]
        fault |= Numbers.combine_fault_rows(torch.where(pending, same_fill.fault.reshape(roots, width), 0))
        zero = ExactRational(ledger_sequence.constant(0), ledger_sequence.constant(1))
        direct_index = position * 8
        deltas = [take_ratio(columns.balances, direct_index + offset) for offset in (2, 3)]
        changes = []
        for offset, direct in zip((2, 3), deltas):
            all_values = take_ratio(columns.balances, torch.arange(roots*width, device=local.device)*8 + offset)
            absent = ExactRational(all_values.numerator.constant(0), all_values.denominator.constant(1))
            prior = sum_population(select(all_values, pending.flatten(), absent), roots, width)
            changes.append(select(direct, row & (order_economic | fill_economic), zero) + prior)
        amount_fault = changes[0].fault | changes[1].fault
        fault |= torch.where(row, amount_fault, 0)
        mask = torch.zeros_like(self.occupied).scatter(1, at[:, None], (row & (fault == 0))[:, None])
        directory_index = root * capacity + at
        next_principal = take_ratio(self.principal, directory_index) + changes[0]
        next_fee = take_ratio(self.fee, directory_index) + changes[1]
        fault |= torch.where(row, next_principal.fault | next_fee.fault, 0)

        # last_sequence取实际非零分录的最大Ledger序；Trade详情空条目不刷新资金序。
        amounts = []
        for offset in (2, 3):
            amounts.append(take_ratio(columns.balances,
                torch.arange(roots*width, device=local.device)*8+offset).numerator.sign.reshape(roots, width) != 0)
        pending_nonzero = pending & (amounts[0] | amounts[1])
        words = applied_sequences.words.to(torch.int64) & 0xffffffff
        hi = words[:, 1].reshape(roots, width)
        lo = words[:, 0].reshape(roots, width)
        maximum_hi = torch.where(pending_nonzero, hi, 0).max(1).values
        maximum_lo = torch.where(pending_nonzero & (hi == maximum_hi[:, None]), lo, 0).max(1).values
        max_words = torch.stack((maximum_lo, maximum_hi), 1).to(torch.int32)
        max_words = torch.nn.functional.pad(max_words, (0, max(0, applied_sequences.words.size(1)-2)))
        maximum = ExactInt(max_words, (pending_nonzero.any(1)).to(torch.int8),
            Numbers.combine_fault_rows(torch.where(pending_nonzero, applied_sequences.fault.reshape(roots, width), 0)))
        previous = _take(self.last_sequence, directory_index)
        comparison = maximum.compare(previous)
        maximum = maximum.select(comparison.order > 0, previous)
        direct_nonzero = (deltas[0].numerator.sign != 0) | (deltas[1].numerator.sign != 0)
        changed = (order_economic | (fill_economic & bound)) & direct_nonzero
        origin = order_economic & (economic_tag == 0)
        latest = ledger_sequence.select(changed | origin, maximum)
        fault |= torch.where(row, comparison.fault | latest.fault, 0)
        row &= fault == 0
        mask &= row[:, None]
        condition = torch.where(observed | order_economic | fill_economic,
            lifecycle.conditions[root, local], self.conditions[root, at])
        condition = torch.where(condition >= 0, condition, self.conditions[root, at])
        known_last = self.has_last_sequence[root, at] | pending_nonzero.any(1) | changed | origin
        trade_mask = torch.zeros_like(self.trade_bound).scatter(1, trade_slots[:, None], (trade & ready & (fault == 0))[:, None])
        added = torch.zeros_like(self.unassigned_records).scatter(1, local[:, None],
            (fill_economic & ~bound & ready & (fault == 0))[:, None])
        cleared = pending & ready[:, None] & (fault == 0)[:, None]
        candidate = replace(self, keys=_take(order, root.repeat_interleave(capacity)).select(mask.flatten(), self.keys),
            occupied=self.occupied | mask, conditions=torch.where(mask, condition[:, None], self.conditions),
            principal=select(take_ratio(next_principal, root.repeat_interleave(capacity)), mask.flatten(), self.principal),
            fee=select(take_ratio(next_fee, root.repeat_interleave(capacity)), mask.flatten(), self.fee),
            has_origin=self.has_origin | (mask & origin[:, None]),
            last_sequence=_take(latest, root.repeat_interleave(capacity)).select(mask.flatten(), self.last_sequence),
            has_last_sequence=torch.where(mask, known_last[:, None], self.has_last_sequence),
            trade_orders=_take(source_order, root.repeat_interleave(trade_capacity)).select(trade_mask.flatten(), self.trade_orders),
            trade_bound=self.trade_bound | trade_mask,
            unassigned_records=(self.unassigned_records | added) & ~cleared,
            fault=fault, capacity_required=torch.maximum(self.capacity_required, required))
        return candidate, ready & (fault == 0)

    def read(self, lifecycle):
        unknown = self.unassigned_records[:, None, :] & ((lifecycle.conditions[:, None, :] < 0) |
            (lifecycle.conditions[:, None, :] == self.conditions[:, :, None]))
        known = self.occupied & self.has_origin & self.known[:, None] & ~unknown.any(2)
        return OrderFundsRead(self.principal, self.fee, known, self.last_sequence,
            self.has_last_sequence, self.fault)
