"""SIM原价位FIFO的设备候选状态。行是独立根/资产/侧/精确价位，段保持原顺序。

外部段保留原B/Q/c，账户段保留订单身份/剩量；扩池与精确算术重试不提交状态。
本模块只产生队列候选及maker depletion，不生成NOE或经济入账。
"""
from dataclasses import dataclass, replace
import torch
from buy_cost import _public_numbers


Numbers = _public_numbers()
ExactInt = Numbers.ExactInt
ExactRational = Numbers.ExactRational


def _take(value, index):
    return ExactInt(value.words[index], value.sign[index], value.fault[index])


def _repeat(value, count):
    return ExactInt(value.words.repeat_interleave(count, 0), value.sign.repeat_interleave(count),
                    value.fault.repeat_interleave(count))


def _columns(values):
    width = max(value.words.shape[1] for value in values)
    words = [torch.nn.functional.pad(value.words, (0, width-value.words.shape[1])) for value in values]
    return ExactInt(torch.stack(words, 1).flatten(0, 1), torch.stack([value.sign for value in values], 1).flatten(),
                    torch.stack([value.fault for value in values], 1).flatten())


def _minimum(a, b):
    relation = a.compare(b)
    chosen = a.select(relation.order <= 0, b)
    return ExactInt(chosen.words, chosen.sign, chosen.fault | relation.fault)


def _row_fault(value, rows, capacity):
    return Numbers.combine_fault_rows(value.fault.reshape(rows, capacity))


@dataclass(frozen=True)
class QueueEffects:
    order_ids: torch.Tensor  # [rows,capacity]，与maker_base_units逐段对应，不能按订单新编号。
    maker_base_units: object
    remaining_cancelled: object
    cancel_found: torch.Tensor
    required_segments: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PriceQueueBatch:
    price: object  # ExactRational[rows]，原精确价位；不反解模型投影。
    kind: torch.Tensor  # [rows,capacity] 0空/1External/2Account，有效段先于尾部空槽。
    order_ids: torch.Tensor
    total_base: object  # ExactInt[rows*capacity]
    total_quote: object
    consumed_base: object
    account_remaining: object
    debt: object  # ExactInt[rows]
    fault: torch.Tensor

    @property
    def rows(self):
        return self.kind.shape[0]

    @property
    def capacity(self):
        return self.kind.shape[1]

    @classmethod
    def empty(cls, price, capacity=4):
        if capacity < 1:
            raise ValueError('物理段池容量须为正；可按资源请求增长')
        rows = price.numerator.sign.numel()
        zero = price.numerator.constant(0)
        flat_zero = _repeat(zero, capacity)
        shape = (rows, capacity)
        one = ExactRational.ratio(zero.constant(1),zero.constant(1))
        upper = price.compare(one)
        fault = price.fault | upper.fault | (price.numerator.sign<=0).to(torch.int32) | (upper.order>=0).to(torch.int32)
        return cls(price, torch.zeros(shape, dtype=torch.int8, device=zero.words.device),
                   torch.full(shape, -1, dtype=torch.int64, device=zero.words.device),
                   flat_zero, flat_zero, flat_zero, flat_zero, zero, fault)

    def _at(self, value, column):
        index = torch.arange(self.rows, device=self.kind.device) * self.capacity + column
        return _take(value, index)

    def resized(self, capacity):
        if capacity < self.capacity:
            raise ValueError('扩池不能删除已有段')
        padding = capacity-self.capacity
        if not padding:
            return self
        # 仅物理表示改变，原字段/段顺序/债务不变。
        def enlarged(value):
            original = [self._at(value, column) for column in range(self.capacity)]
            return _columns(original+[self.debt.constant(0)]*padding)
        return replace(self, kind=torch.nn.functional.pad(self.kind, (0,padding)),
                       order_ids=torch.nn.functional.pad(self.order_ids, (0,padding), value=-1),
                       total_base=enlarged(self.total_base),total_quote=enlarged(self.total_quote),
                       consumed_base=enlarged(self.consumed_base),account_remaining=enlarged(self.account_remaining))

    def _compact(self, enabled=None):
        if enabled is None:
            enabled = torch.ones(self.rows, dtype=torch.bool, device=self.kind.device)
        external = (self.total_base-self.consumed_base).sign.reshape(self.rows,self.capacity)>0
        account = self.account_remaining.sign.reshape(self.rows,self.capacity)>0
        present = ((self.kind==1)&external)|((self.kind==2)&account)
        permutation = torch.argsort((~present).to(torch.int8),dim=1,stable=True)
        original = torch.arange(self.capacity, device=self.kind.device)[None, :].expand_as(permutation)
        permutation = torch.where(enabled[:, None], permutation, original)
        indices = (torch.arange(self.rows,device=self.kind.device)[:,None]*self.capacity+permutation).flatten()
        kinds = self.kind.gather(1,permutation)
        orders = self.order_ids.gather(1,permutation)
        retained = present.gather(1,permutation) | ~enabled[:, None]
        return replace(self,kind=torch.where(retained,kinds,0),order_ids=torch.where(retained,orders,-1),
                       total_base=_take(self.total_base,indices),total_quote=_take(self.total_quote,indices),
                       consumed_base=_take(self.consumed_base,indices),account_remaining=_take(self.account_remaining,indices))

    def _append(self, quantity, order_ids=None):
        packed = self._compact()
        positive = quantity.sign>0
        count = (packed.kind!=0).sum(1)
        required = count+positive.to(torch.int64)
        target = torch.arange(self.capacity,device=self.kind.device)[None,:]==count[:,None]
        target = target & positive[:,None]
        root_fault = quantity.fault | packed.fault | (quantity.sign<0).to(torch.int32)
        if order_ids is None:
            amount_quote = (ExactRational.ratio(quantity,quantity.constant(1))*self.price).floor()
            root_fault = root_fault | amount_quote.fault
            base = _repeat(quantity,self.capacity)
            quote = _repeat(amount_quote,self.capacity)
            remaining = _repeat(quantity.constant(0),self.capacity)
            kind = 1
            ids = torch.full_like(packed.order_ids,-1)
        else:
            if order_ids.shape!=(self.rows,) or order_ids.dtype!=torch.int64 or order_ids.device!=self.kind.device:
                raise TypeError('订单坐标须为同设备int64[rows]')
            root_fault = root_fault | (positive & (order_ids<0)).to(torch.int32)
            duplicate = ((packed.kind==2)&(packed.order_ids==order_ids[:,None])).any(1)&positive
            root_fault = root_fault | duplicate.to(torch.int32)
            base = quote = _repeat(quantity.constant(0),self.capacity)
            remaining = _repeat(quantity,self.capacity)
            kind = 2
            ids = order_ids[:,None].expand_as(packed.order_ids)
        target = target & (root_fault==0)[:,None]
        flat = target.flatten()
        state = replace(packed,kind=torch.where(target,kind,packed.kind),order_ids=torch.where(target,ids,packed.order_ids),
                        total_base=base.select(flat,packed.total_base),total_quote=quote.select(flat,packed.total_quote),
                        consumed_base=_repeat(quantity.constant(0),self.capacity).select(flat,packed.consumed_base),
                        account_remaining=remaining.select(flat,packed.account_remaining),fault=root_fault)
        return state,required

    def append_external(self, quantity):
        return self._append(quantity)

    def append_account(self, order_ids, quantity):
        return self._append(quantity,order_ids)

    def clear_external(self, enabled):
        clear = enabled[:,None]&(self.kind==1)
        state = replace(self,kind=torch.where(clear,0,self.kind),
                        debt=self.debt.constant(0).select(enabled,self.debt))
        return state._compact()

    def cancel(self, order_ids):
        selected = (self.kind==2)&(self.order_ids==order_ids[:,None])
        zero = self.debt.constant(0)
        total = zero
        for column in range(self.capacity):
            total = total+self._at(self.account_remaining,column).select(selected[:,column],zero)
        state = replace(self,kind=torch.where(selected,0,self.kind),
                        account_remaining=_repeat(zero,self.capacity).select(selected.flatten(),self.account_remaining))._compact()
        return state,total,selected.any(1)

    def apply_taker_slice(self, columns, delta_base):
        """应用原planner的External段，不改变同价位计划中其他片的原段坐标。

        接单后按原计划顺序调用；该价位最后片后调用finish_taker_plan，期间不穿插队列变更。
        """
        if columns.shape!=(self.rows,) or columns.dtype!=torch.int64 or columns.device!=self.kind.device:
            raise TypeError('段坐标须为同设备int64[rows]')
        index = torch.arange(self.rows,device=self.kind.device)*self.capacity+columns.clamp(0,self.capacity-1)
        total = _take(self.total_base,index)
        quote = _take(self.total_quote,index)
        consumed = _take(self.consumed_base,index)
        kinds = self.kind.gather(1,columns.clamp(0,self.capacity-1)[:,None])[:,0]
        next_consumed = consumed+delta_base
        upper = next_consumed.compare(total)
        valid = (kinds==1)&(total.sign>0)&(quote.sign>0)&(columns>=0)&(columns<self.capacity)
        fault = self.fault | delta_base.fault | upper.fault | (~valid).to(torch.int32)
        fault = fault | (delta_base.sign<0).to(torch.int32) | (upper.order>0).to(torch.int32)
        # 错误仍保留，但不得用零分母作为批中占位继续做除法。
        denominator = total.select(total.sign>0,total.constant(1))
        before_quote,_ = (consumed*quote).divrem(denominator)
        after_quote,_ = (next_consumed*quote).divrem(denominator)
        used_quote = after_quote-before_quote
        next_debt = self.debt+delta_base
        fault = fault | used_quote.fault | next_debt.fault
        fault = fault | ((delta_base.sign>0)&(used_quote.sign<=0)).to(torch.int32)
        target = torch.arange(self.capacity,device=self.kind.device)[None,:]==columns[:,None]
        target = target & (fault==0)[:,None]
        candidate = replace(self,consumed_base=_repeat(next_consumed,self.capacity).select(target.flatten(),self.consumed_base),
                            debt=next_debt.select(fault==0,self.debt),fault=fault)
        return candidate,used_quote

    def finish_taker_plan(self, enabled):
        """仅在对应价位的原计划最后片应用后压紧；其他价位仍保持规划时坐标。"""
        if enabled.shape != (self.rows,) or enabled.dtype != torch.bool or enabled.device != self.kind.device:
            raise TypeError('完成价位掩码须为同设备bool[rows]')
        return self._compact(enabled & (self.fault == 0))

    def apply_level_transition(self, before, after):
        """原减少先抵taker debt，再逐段耗尽；增加永远追加尾段，绝不合并原B/Q。"""
        zero = before.constant(0)
        relation = after.compare(before)
        added = (after-before).select(relation.order>0,zero)
        removed = (before-after).select(relation.order<0,zero)
        offset = _minimum(removed,self.debt)
        remaining = removed-offset
        debt = self.debt-offset
        roots_fault = self.fault | before.fault | after.fault | relation.fault
        roots_fault = roots_fault | (before.sign<0).to(torch.int32) | (after.sign<0).to(torch.int32)
        consumed_columns = []
        account_columns = []
        maker_columns = []
        for column in range(self.capacity):
            base = self._at(self.total_base,column)
            consumed = self._at(self.consumed_base,column)
            account = self._at(self.account_remaining,column)
            kind = self.kind[:,column]
            available = (base-consumed).select(kind==1,account.select(kind==2,zero))
            take = _minimum(available,remaining)
            remaining = remaining-take
            consumed_columns.append((consumed+take).select(kind==1,consumed))
            account_columns.append((account-take).select(kind==2,account))
            maker_columns.append(take.select(kind==2,zero))
        roots_fault = roots_fault | remaining.fault | debt.fault | (remaining.sign>0).to(torch.int32)
        state = replace(self,consumed_base=_columns(consumed_columns),account_remaining=_columns(account_columns),
                        debt=debt,fault=roots_fault)._compact()
        state,required = state.append_external(added)
        maker = _columns(maker_columns)
        roots_fault = roots_fault | state.fault | _row_fault(maker,self.rows,self.capacity)
        effects = QueueEffects(self.order_ids,maker,zero,torch.zeros_like(self.fault,dtype=torch.bool),required,roots_fault)
        return state,effects


def run_queue_batch(queue,compute):
    """compute(queue)纯计算候选；只协调段容量和精确字容量，不读取业务条件。"""
    while True:
        result = Numbers.run_exact_batch(lambda:compute(queue))
        demand = result[-1].required_segments if hasattr(result[-1],'required_segments') else result[-1]
        required = int(demand.max().cpu()) if demand.numel() else 0
        if required<=queue.capacity:
            return result
        queue = queue.resized(1<<(required-1).bit_length())
