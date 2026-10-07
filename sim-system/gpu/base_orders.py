"""原SIM Base订单的持续设备目录；原u64 order序号与FIFO物理引用分开。

每根slot只作其队列Account段的稳定引用。真实sim-N身份由完整sequence恢复；
扩目录不改变slot。源准入/quote订单/密封路径选择及完整宿主仍归各原入口。
"""
from dataclasses import dataclass, fields, replace
import torch
from queues import ExactInt, ExactRational, Numbers, _take, _repeat
from order_quantity import BaseOrderQuantity
from fill_pool import _put_integer
from account_state import grouped_fault


def _sum_depletion(amounts, coordinates, active, population):
    """按设备坐标稳定分组；精确分段前缀和，避免每订单扫描所有历史段。"""
    count = coordinates.numel()
    if count == 0:
        zero = amounts.constant(0)
        return ExactInt(torch.zeros((population, zero.words.shape[1]), dtype=torch.int32, device=zero.words.device),
                        torch.zeros(population, dtype=torch.int8, device=zero.words.device),
                        torch.zeros(population, dtype=torch.int32, device=zero.words.device))
    coordinate = torch.where(active, coordinates, population)
    permutation = torch.argsort(coordinate, stable=True)
    coordinate = coordinate[permutation]
    selected = _take(amounts, permutation)
    total = selected.select(active[permutation], selected.constant(0))
    rows = torch.arange(count, device=coordinates.device)
    stride = 1
    while stride < count:
        predecessor = (rows-stride).clamp_min(0)
        same = (rows >= stride) & (coordinate == coordinate[predecessor])
        previous = _take(total, predecessor)
        total = total+previous.select(same, previous.constant(0))
        stride *= 2
    final = torch.ones(count, dtype=torch.bool, device=coordinates.device)
    final[:-1] = coordinate[:-1] != coordinate[1:]
    # 原分组最后位置只做设备整数归约；不能以重复index_copy覆盖精确和。
    ends = torch.full((population+1,), -1, dtype=torch.int64, device=coordinates.device)
    ends = ends.scatter_reduce(0, coordinate, torch.where(final, rows, -1), reduce='amax')[:population]
    result = _take(total, ends.clamp_min(0))
    return result.select(ends >= 0, result.constant(0))


def take_quantity(quantity, index):
    return BaseOrderQuantity(**{field.name: _take(getattr(quantity, field.name), index)
        if isinstance(getattr(quantity, field.name), ExactInt) else getattr(quantity, field.name)[index]
        for field in fields(quantity)})


def put_quantity(original, incoming, index, enabled):
    updates = {}
    for field in fields(original):
        old, new = getattr(original, field.name), getattr(incoming, field.name)
        if isinstance(old, ExactInt):
            updates[field.name] = _put_integer(old, new, index, enabled)
        else:
            updates[field.name] = old.index_copy(0, index, torch.where(enabled, new, old[index]))
    return replace(original, **updates)


@dataclass(frozen=True)
class BaseOrderRead:
    source: object
    slots: torch.Tensor
    sequence: object
    quantity: object
    price: object
    request_keys: torch.Tensor
    item_ordinals: torch.Tensor
    item_refs: torch.Tensor
    assets: torch.Tensor
    bid_rows: torch.Tensor
    accepted_at: torch.Tensor
    present: torch.Tensor
    fault: torch.Tensor
    conditions: object = None
    last_updated_at: object = None


@dataclass(frozen=True)
class BaseOrderInsertion:
    slots: torch.Tensor
    sequence: object
    emitted: torch.Tensor
    required_orders: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class OrderMakerEffects:
    slots: torch.Tensor
    sequences: object
    base: object
    quote: object
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class BaseOrders:
    quantities: object  # BaseOrderQuantity[roots*capacity]，真实完整计量。
    sequences: object
    next_sequence: object
    prices: object
    occupied: torch.Tensor
    request_keys: torch.Tensor
    item_ordinals: torch.Tensor
    item_refs: torch.Tensor
    assets: torch.Tensor
    bid_rows: torch.Tensor
    accepted_at: torch.Tensor
    fault: torch.Tensor
    conditions: object = None
    last_updated_at: object = None  # 原实际订单变化时钟；不由查询、公开时间或保留水位刷新。

    @property
    def roots(self):
        return self.occupied.shape[0]

    @property
    def capacity(self):
        return self.occupied.shape[1]

    @classmethod
    def empty(cls, template, *, capacity=4):
        if capacity < 1:
            raise ValueError('订单物理目录容量必须正数')
        roots, device = template.accepted_base_units.sign.numel(), template.fault.device
        zero = template.accepted_base_units.constant(0)
        flat = _repeat(zero, capacity)
        quantities = BaseOrderQuantity(**{field.name: torch.zeros_like(flat.fault)
            if field.name == 'fault' else flat for field in fields(template)})
        shape = (roots, capacity)
        absent = torch.full(shape, -1, dtype=torch.int64, device=device)
        return cls(quantities, flat, zero.constant(1), ExactRational(flat, flat.constant(1)),
            torch.zeros(shape, dtype=torch.bool, device=device), absent, absent, absent, absent, absent,
            torch.zeros(shape, dtype=torch.int64, device=device), torch.zeros_like(zero.fault), absent,
            torch.zeros(shape, dtype=torch.int64, device=device))

    def resized(self, capacity):
        if capacity <= self.capacity:
            return self
        root = torch.arange(self.roots, device=self.occupied.device)[:, None]
        old_index = (root*self.capacity+torch.arange(self.capacity, device=root.device)).flatten()
        new_index = (root*capacity+torch.arange(self.capacity, device=root.device)).flatten()
        result = BaseOrders.empty(take_quantity(self.quantities, root.flatten()*self.capacity), capacity=capacity)
        enabled = torch.ones_like(old_index, dtype=torch.bool)
        quantities = put_quantity(result.quantities, take_quantity(self.quantities, old_index), new_index, enabled)
        sequence = _put_integer(result.sequences, self.sequences, new_index, enabled)
        price = ExactRational(_put_integer(result.prices.numerator, self.prices.numerator, new_index, enabled),
                              _put_integer(result.prices.denominator, self.prices.denominator, new_index, enabled))
        updates = {name: torch.nn.functional.pad(getattr(self, name), (0, capacity-self.capacity),
                   value=0 if name in ('occupied', 'accepted_at','last_updated_at') else -1)
                   for name in ('occupied', 'request_keys', 'item_ordinals', 'item_refs', 'assets', 'bid_rows', 'accepted_at','conditions','last_updated_at')}
        return replace(result, **updates, quantities=quantities, sequences=sequence, prices=price,
                       next_sequence=self.next_sequence, fault=self.fault)

    def insert(self, quantity, price, request_keys, item_ordinals, item_refs, assets, bid_rows,
               accepted_at, enabled, *, conditions=None):
        """仅成功接纳人口安装订单；与账户/填单/队列在外层共同提交。

        slot增长请求不耗order序号。原接纳失败或SDK拒绝不调用enabled。
        """
        roots = torch.arange(self.roots, device=self.occupied.device)
        count = self.occupied.to(torch.int64).sum(1)
        slot = count.clamp_max(self.capacity-1)
        index = roots*self.capacity+slot
        next_sequence = self.next_sequence+self.next_sequence.constant(1)
        upper = next_sequence.compare(next_sequence.constant((1 << 64)-1))
        unit = price.numerator.constant(1)
        valid_price = price.compare(ExactRational(unit, unit))
        error = quantity.fault | price.fault | upper.fault | valid_price.fault
        error = error | ((upper.order > 0) | (price.numerator.sign <= 0) | (valid_price.order >= 0) |
            (request_keys < 0) | (item_ordinals < 0) | (item_ordinals > 65535) | (item_refs < 0) |
            (assets < 0) | ((quantity.open_base_units.sign > 0) & (bid_rows < 0))).to(torch.int32)
        fault = self.fault | error*enabled.to(torch.int32)
        if conditions is not None:
            if conditions.shape != (self.roots,) or conditions.dtype != torch.int64 or conditions.device != self.occupied.device:
                raise TypeError('原订单市场须同设备int64[roots]')
            fault = fault | (enabled & (conditions < 0)).to(torch.int32)
        emitted = enabled & (count < self.capacity) & (fault == 0)
        updates = {}
        for name, value in (('request_keys', request_keys), ('item_ordinals', item_ordinals),
            ('item_refs', item_refs), ('assets', assets), ('bid_rows', bid_rows), ('accepted_at', accepted_at),
            ('last_updated_at', accepted_at)):
            old = getattr(self, name)
            updates[name] = old.flatten().index_copy(0, index, torch.where(emitted, value, old[roots, slot])).reshape_as(old)
        if conditions is not None:
            updates['conditions'] = self.conditions.flatten().index_copy(0,index,
                torch.where(emitted,conditions,self.conditions[roots,slot])).reshape_as(self.conditions)
        updates['occupied'] = self.occupied.flatten().index_copy(0, index,
            self.occupied[roots, slot] | emitted).reshape_as(self.occupied)
        candidate = replace(self, **updates,
            quantities=put_quantity(self.quantities, quantity, index, emitted),
            sequences=_put_integer(self.sequences, self.next_sequence, index, emitted),
            prices=ExactRational(_put_integer(self.prices.numerator, price.numerator, index, emitted),
                                _put_integer(self.prices.denominator, price.denominator, index, emitted)),
            next_sequence=next_sequence.select(emitted, self.next_sequence), fault=fault)
        return candidate, BaseOrderInsertion(slot, self.next_sequence, emitted,
                                             count+enabled.to(torch.int64), fault)

    def read(self, slots):
        """不存在目标返回present=false；取消入口据此形成原NotFound，不造订单。"""
        if slots.shape != (self.roots,) or slots.dtype != torch.int64 or slots.device != self.occupied.device:
            raise TypeError('订单slot须同设备int64[roots]')
        root = torch.arange(self.roots, device=slots.device)
        safe = slots.clamp(0, self.capacity-1)
        index = root*self.capacity+safe
        present = (slots >= 0) & (slots < self.capacity) & self.occupied[root, safe]
        price = ExactRational(_take(self.prices.numerator, index), _take(self.prices.denominator, index))
        return BaseOrderRead(self, slots, _take(self.sequences, index), take_quantity(self.quantities, index), price,
            self.request_keys[root, safe], self.item_ordinals[root, safe], self.item_refs[root, safe],
            self.assets[root, safe], self.bid_rows[root, safe], self.accepted_at[root, safe], present, self.fault,
            self.conditions[root,safe],self.last_updated_at[root,safe])

    def find(self,sequence):
        """按完整原u64身份找当前Base订单；slot只在本目录内返回。"""
        if sequence.sign.shape!=(self.roots,) or sequence.words.device!=self.occupied.device:
            raise ValueError('原订单序号需同根同设备精确列')
        owner=torch.arange(self.roots,device=self.occupied.device).repeat_interleave(self.capacity)
        comparison=self.sequences.compare(_take(sequence,owner))
        matches=self.occupied & (comparison.order.reshape_as(self.occupied)==0)
        slots=torch.where(matches.any(1),matches.to(torch.int64).argmax(1),-1)
        read=self.read(slots)
        error=Numbers.combine_fault_rows(torch.where(self.occupied,comparison.fault.reshape_as(self.occupied),0))
        return replace(read,fault=read.fault | error)

    def update(self, read, quantity, enabled, at):
        if read.source is not self:
            raise ValueError('订单更新须绑定读取时同一目录状态')
        if at.shape!=(self.roots,) or at.dtype!=torch.int64 or at.device!=self.occupied.device:
            raise TypeError('订单实际变化时钟须同设备int64[roots]')
        error = self.fault | quantity.fault*enabled.to(torch.int32)
        error = error | (enabled & ~read.present).to(torch.int32)
        valid = enabled & read.present & (error == 0)
        index = torch.arange(self.roots, device=read.slots.device)*self.capacity+read.slots.clamp(0, self.capacity-1)
        updated=self.last_updated_at.flatten().index_copy(0,index,
            torch.where(valid,at,self.last_updated_at.flatten()[index])).reshape_as(self.last_updated_at)
        return replace(self, quantities=put_quantity(self.quantities, quantity, index, valid),
            last_updated_at=updated,fault=error)

    def reduce_maker(self, effects, row_roots, at):
        """全部真实Account段depletion按根内slot路由，不按price/余额猜订单。

        返回逐订单人口供原FillPool创建maker fill；零quote仍消费base并更新pending和变化时钟。
        """
        rows, segment_capacity = effects.order_ids.shape
        if row_roots.shape != (rows,) or row_roots.dtype != torch.int64:
            raise ValueError('maker队列行根映射错误')
        if at.shape!=(self.roots,) or at.dtype!=torch.int64 or at.device!=self.occupied.device:
            raise TypeError('maker实际变化时钟须同设备int64[roots]')
        mapping = (row_roots >= 0) & (row_roots < self.roots)
        safe_roots = row_roots.clamp(0, self.roots-1)
        fault = self.fault | (~mapping).any().to(torch.int32) | grouped_fault(effects.fault, safe_roots, self.roots)
        slots = torch.arange(self.capacity, device=self.occupied.device)[None, :].expand(self.roots, -1)
        ids = effects.order_ids
        live = effects.maker_base_units.sign.reshape(rows, segment_capacity) > 0
        safe_slot = ids.clamp(0, self.capacity-1)
        known = self.occupied[safe_roots[:, None], safe_slot] & (ids >= 0) & (ids < self.capacity)
        row_error = Numbers.combine_fault_rows((live & ~known).to(torch.int32))
        fault = fault | grouped_fault(row_error, safe_roots, self.roots)
        coordinate = safe_roots[:, None]*self.capacity+safe_slot
        amount = _sum_depletion(effects.maker_base_units, coordinate.flatten(),
                               (live & known & mapping[:, None]).flatten(), self.roots*self.capacity)
        quantities, event = self.quantities.maker_depletion(amount, self.occupied.flatten() & (amount.sign > 0))
        fault = fault | Numbers.combine_fault_rows(event.fault.reshape(self.roots, self.capacity))
        enabled = (fault == 0).repeat_interleave(self.capacity)
        index = torch.arange(self.roots*self.capacity, device=self.occupied.device)
        changed=enabled & self.occupied.flatten() & (amount.sign>0)
        updated=torch.where(changed.reshape_as(self.occupied),at[:,None],self.last_updated_at)
        candidate = replace(self, quantities=put_quantity(self.quantities, quantities, index, enabled),
            last_updated_at=updated,fault=fault)
        emitted = event.emitted & enabled
        return candidate, OrderMakerEffects(slots, self.sequences,
            event.base.select(emitted, event.base.constant(0)), event.quote.select(emitted, event.quote.constant(0)),
            emitted.reshape(self.roots, self.capacity), fault)
