"""SIM原价位FIFO的设备候选状态。行是独立根/资产/侧/精确价位，段保持原顺序。

外部段保留原B/Q/c，账户段保留订单身份/剩量；扩池与精确算术重试不提交状态。
本模块只产生队列候选及maker depletion，不生成NOE或经济入账。
"""
from dataclasses import dataclass, replace
from contextvars import ContextVar
from contextlib import contextmanager
import torch
from buy_cost import _public_numbers


Numbers = _public_numbers()
ExactInt = Numbers.ExactInt
ExactRational = Numbers.ExactRational

_level_dispatch = ContextVar('fifo_level_dispatch',default=None)


@contextmanager
def level_queue_dispatch(prepare):
    """设备分段图供应同一原Level的队列候选；不改变源准入。"""
    token=_level_dispatch.set(prepare)
    try:yield
    finally:_level_dispatch.reset(token)


def _take(value, index):
    return ExactInt(value.words[index], value.sign[index], value.fault[index])


def _put(value, index, incoming):
    width = max(value.words.shape[1], incoming.words.shape[1])
    value, incoming = value.with_capacity(width), incoming.with_capacity(width)
    return ExactInt(value.words.index_copy(0, index, incoming.words),
                    value.sign.index_copy(0, index, incoming.sign),
                    value.fault.index_copy(0, index, incoming.fault))


def _repeat(value, count):
    return ExactInt(value.words.repeat_interleave(count, 0), value.sign.repeat_interleave(count),
                    value.fault.repeat_interleave(count))


def _columns(values):
    width = max(value.words.shape[1] for value in values)
    words = [value.with_capacity(width).words for value in values]
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
class QuoteTakerPlan:
    """同一精确价位的原Quote目标规划；后续价位仍须按原ask升序衔接剩余预算。"""
    source: object  # 冻结PriceQueueBatch；禁止把旧计划应用到已变更或已经消费的队列。
    delta_base: object  # [rows*capacity]，保持规划时段坐标。
    delta_quote: object
    consumed_after: object
    matched_base: object  # [rows]
    used_quote: object
    stopped: torch.Tensor  # 原None/部分段停止，不能跳到后价继续吃。
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
    conservative: object = None

    def take_rows(self, rows, *, staged=True):
        """按原行坐标提取FIFO，保留每行段次序及原B/Q/c。"""
        from base_runtime_storage import queue_stage_for
        stage=queue_stage_for(self) if staged else None
        if stage is not None:return stage.take_rows(self,rows)
        flat=(rows[:,None]*self.capacity+torch.arange(self.capacity,device=rows.device)[None,:]).flatten()
        rank=self.conservative
        value = replace(self,price=ExactRational(_take(self.price.numerator,rows),_take(self.price.denominator,rows)),
            kind=self.kind.index_select(0,rows),order_ids=self.order_ids.index_select(0,rows),
            **{name:_take(getattr(self,name),flat) for name in
                ('total_base','total_quote','consumed_base','account_remaining')},
            debt=_take(self.debt,rows),fault=self.fault.index_select(0,rows),
            conservative=None if rank is None else replace(rank,kind=rank.kind.index_select(0,rows),
                order_ids=rank.order_ids.index_select(0,rows),remaining=_take(rank.remaining,flat)))
        return value

    def put_rows(self, rows, incoming, *, enabled=None):
        """纯候选安装；阶段存储可在共同提交点使用同一原行坐标。"""
        from base_runtime_storage import queue_stage_for
        stage=queue_stage_for(self)
        if stage is not None:return stage.propose(self,rows,incoming,enabled)
        if enabled is not None:
            from base_runtime_storage import _owned,StageQueueStorage
            output=_owned(self,self,word_capacity=max(incoming.total_base.words.size(1),incoming.debt.words.size(1)))
            StageQueueStorage._write(output,rows,incoming,enabled)
            from account_state import grouped_fault
            return replace(output,fault=self.fault|grouped_fault(incoming.fault*enabled.to(torch.int32),rows,self.rows))
        flat=(rows[:,None]*self.capacity+torch.arange(self.capacity,device=rows.device)[None,:]).flatten()
        rank=self.conservative
        return replace(self,kind=self.kind.index_copy(0,rows,incoming.kind),
            order_ids=self.order_ids.index_copy(0,rows,incoming.order_ids),
            **{name:_put(getattr(self,name),flat,getattr(incoming,name)) for name in
                ('total_base','total_quote','consumed_base','account_remaining')},
            debt=_put(self.debt,rows,incoming.debt),fault=self.fault.index_copy(0,rows,incoming.fault),
            conservative=None if rank is None else replace(rank,kind=rank.kind.index_copy(0,rows,incoming.conservative.kind),
                order_ids=rank.order_ids.index_copy(0,rows,incoming.conservative.order_ids),
                remaining=_put(rank.remaining,flat,incoming.conservative.remaining)))

    def segment_counts(self):
        from base_runtime_storage import queue_stage_for
        stage=queue_stage_for(self)
        return (self.kind!=0).sum(1) if stage is None else stage.segment_counts(self)

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
        from base_runtime_storage import queue_stage_for,_queue_stage
        stage=queue_stage_for(self)
        if stage is not None:
            current=stage.materialize(self)
            token=_queue_stage.set(None)
            try:result=current.resized(capacity)
            finally:_queue_stage.reset(token)
            stage.rebind(result)
            return result
        # 仅物理表示改变，原字段/段顺序/债务不变。
        def enlarged(value):
            original = [self._at(value, column) for column in range(self.capacity)]
            return _columns(original+[self.debt.constant(0)]*padding)
        return replace(self, kind=torch.nn.functional.pad(self.kind, (0,padding)),
                       order_ids=torch.nn.functional.pad(self.order_ids, (0,padding), value=-1),
                       total_base=enlarged(self.total_base),total_quote=enlarged(self.total_quote),
                       consumed_base=enlarged(self.consumed_base),account_remaining=enlarged(self.account_remaining),
                       conservative=None if self.conservative is None else self.conservative.resized(capacity))

    def append_empty_rows(self, price):
        """为新合法报价建立空FIFO；旧行坐标、段与债务都不移动。"""
        # 原FOK/已有GTC价位的新增目录人口可为空；不重造不可变价格或复制全FIFO。
        if price.numerator.sign.numel()==0:return self
        empty=type(self).empty(price,self.capacity)
        def integer(left,right):
            capacity=max(left.words.size(1),right.words.size(1))
            left,right=left.with_capacity(capacity),right.with_capacity(capacity)
            return ExactInt(torch.cat((left.words,right.words)),torch.cat((left.sign,right.sign)),torch.cat((left.fault,right.fault)))
        return replace(self,price=ExactRational(integer(self.price.numerator,price.numerator),integer(self.price.denominator,price.denominator)),
            kind=torch.cat((self.kind,empty.kind)),order_ids=torch.cat((self.order_ids,empty.order_ids)),
            **{name:integer(getattr(self,name),getattr(empty,name)) for name in
               ('total_base','total_quote','consumed_base','account_remaining','debt')},fault=torch.cat((self.fault,empty.fault)),
            conservative=self._append_rank_rows(empty))

    def _append_rank_rows(self,empty):
        if self.conservative is None:return None
        from conservative_rank import ConservativeRank
        rank=self.conservative;tail=ConservativeRank.empty(empty)
        width=max(rank.remaining.words.size(1),tail.remaining.words.size(1))
        a,b=rank.remaining.with_capacity(width),tail.remaining.with_capacity(width)
        return ConservativeRank(torch.cat((rank.kind,tail.kind)),torch.cat((rank.order_ids,tail.order_ids)),
            ExactInt(torch.cat((a.words,b.words)),torch.cat((a.sign,b.sign)),torch.cat((a.fault,b.fault))))

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
        if self.conservative is None:return self._append(quantity,order_ids)
        rank=self.conservative
        fault=self.fault|quantity.fault|(quantity.sign<0).to(torch.int32)
        fault|=((quantity.sign>0)&(order_ids<0)).to(torch.int32)
        fault|=((rank.kind==2)&(rank.order_ids==order_ids[:,None])).any(1).to(torch.int32)*(quantity.sign>0).to(torch.int32)
        quantity=quantity.select(fault==0,quantity.constant(0))
        seed=quantity.constant(0)
        for c in range(self.capacity):
            seed=seed+(self._at(self.total_base,c)-self._at(self.consumed_base,c)).select(self.kind[:,c]==1,seed.constant(0))
        seed=seed.select((quantity.sign>0)&~(rank.kind==2).any(1),seed.constant(0))
        rank,first=rank.append(seed)
        rank,required=rank.append(quantity,order_ids)
        return replace(self,conservative=rank,fault=fault|seed.fault|_row_fault(rank.remaining,self.rows,self.capacity)),torch.maximum(first,required)

    def clear_external(self, enabled):
        clear = enabled[:,None]&(self.kind==1)
        state = replace(self,kind=torch.where(clear,0,self.kind),
                        debt=self.debt.constant(0).select(enabled,self.debt))
        return state._compact()

    def clear_matching(self, enabled):
        """原end_matching清整个撮合队列；订单及经济义务仍由各自状态保留。"""
        clear = enabled[:, None]
        return replace(self, kind=torch.where(clear, 0, self.kind),
                       order_ids=torch.where(clear, -1, self.order_ids),
                       debt=self.debt.constant(0).select(enabled, self.debt),
                       conservative=None if self.conservative is None else self.conservative.clear(enabled))

    def cancel(self, order_ids):
        if self.conservative is not None:
            rank,total,found=self.conservative.cancel(order_ids)
            return replace(self,conservative=rank),total,found
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

    def plan_quote_budget(self, budget):
        """沿原maximum_base_for_quote_budget_v1规划全部External段，尚不改变队列。

        只消费quote目标。FOK宿主须在所有合法价位规划完且整预算满足后才一次应用；
        不能将本方法当成GTC的LimitBase目标或完整接单能力。
        """
        zero = budget.constant(0)
        one = budget.constant(1)
        remaining = budget
        matched = used = zero
        stopped = torch.zeros(self.rows, dtype=torch.bool, device=self.kind.device)
        roots_fault = self.fault | budget.fault | (budget.sign < 0).to(torch.int32)
        base_columns = []
        quote_columns = []
        consumed_columns = []
        for column in range(self.capacity):
            base = self._at(self.total_base, column)
            quote = self._at(self.total_quote, column)
            consumed = self._at(self.consumed_base, column)
            relation = consumed.compare(base)
            external = self.kind[:, column] == 1
            invalid = (base.sign <= 0) | (quote.sign < 0) | (consumed.sign < 0) | (relation.order > 0)
            roots_fault = roots_fault | relation.fault | base.fault | quote.fault | consumed.fault
            roots_fault = roots_fault | (external & invalid).to(torch.int32)
            candidate = external & (quote.sign > 0) & (relation.order < 0)
            candidate = candidate & (remaining.sign > 0) & ~stopped & (roots_fault == 0)
            # 未选中的空槽只用非零除数占位；不会将未知源字段解释为业务零。
            safe_base = base.select(base.sign > 0, one)
            safe_quote = quote.select(quote.sign > 0, one)
            consumed_quote, _ = (consumed * quote).divrem(safe_base)
            remaining_quote = quote - consumed_quote
            covers = remaining.compare(remaining_quote)
            partial_end, _ = (((consumed_quote + remaining + one) * base) - one).divrem(safe_quote)
            end = base.select(covers.order >= 0, partial_end)
            delta_base = end - consumed
            end_quote, _ = (end * quote).divrem(safe_base)
            delta_quote = end_quote - consumed_quote
            affordable = delta_quote.compare(remaining)
            usable = (delta_base.sign > 0) & (delta_quote.sign > 0) & (affordable.order <= 0)
            roots_fault = roots_fault | covers.fault | affordable.fault | delta_base.fault | delta_quote.fault
            selected = candidate & usable & (roots_fault == 0)
            taken_base = delta_base.select(selected, zero)
            taken_quote = delta_quote.select(selected, zero)
            matched = matched + taken_base
            used = used + taken_quote
            remaining = remaining - taken_quote
            partial = delta_base.compare(base - consumed)
            roots_fault = roots_fault | partial.fault | matched.fault | used.fault | remaining.fault
            stopped = stopped | (candidate & (~usable | (partial.order < 0)))
            base_columns.append(taken_base)
            quote_columns.append(taken_quote)
            consumed_columns.append((consumed + taken_base).select(selected, consumed))
        return QuoteTakerPlan(self, _columns(base_columns), _columns(quote_columns), _columns(consumed_columns),
                              matched, used, stopped, roots_fault)

    def plan_limit_base(self, total_base, total_quote, matched_before, used_before):
        """原GTC LimitBase目标：每片以订单累计成交/付款承接精确价格改善余额。"""
        from limit_segment import maximum_limit_segment
        zero = total_base.constant(0)
        matched, used = matched_before, used_before
        stopped = torch.zeros(self.rows,dtype=torch.bool,device=self.kind.device)
        fault = self.fault | total_base.fault | total_quote.fault | matched.fault | used.fault
        fault = fault | ((total_base.sign <= 0) | (total_quote.sign < 0) | (matched.sign < 0) | (used.sign < 0)).to(torch.int32)
        relation = matched.compare(total_base)
        fault = fault | relation.fault | (relation.order > 0).to(torch.int32)
        safe_total = total_base.select(total_base.sign > 0,total_base.constant(1))
        allocation = (matched*total_quote).divrem(safe_total)[0]
        relation = used.compare(allocation)
        fault = fault | relation.fault | allocation.fault | (relation.order > 0).to(torch.int32)
        base_columns,quote_columns,consumed_columns = [],[],[]
        for column in range(self.capacity):
            base = self._at(self.total_base,column)
            quote = self._at(self.total_quote,column)
            consumed = self._at(self.consumed_base,column)
            remaining = base-consumed
            target_remaining = total_base-matched
            candidate = (self.kind[:,column] == 1) & (quote.sign > 0) & (remaining.sign > 0)
            candidate = candidate & (target_remaining.sign > 0) & ~stopped & (fault == 0)
            maximum = _minimum(remaining,target_remaining).select(candidate,zero)
            # 空槽用合法物理占位，仍携带原完整source fault，不产生假流动性。
            safe_base = base.select(base.sign > 0,total_base.constant(1))
            result = maximum_limit_segment(safe_base,quote,consumed.select(candidate,zero),maximum,
                                          total_base,total_quote,matched,used)
            fault = fault | result.fault * candidate.to(torch.int32)
            selected = candidate & result.present & (fault == 0)
            delta_base = result.base.select(selected,zero)
            delta_quote = result.quote.select(selected,zero)
            matched = matched+delta_base
            used = used+delta_quote
            fault = fault | matched.fault | used.fault
            partial = delta_base.compare(remaining)
            fault = fault | partial.fault
            stopped = stopped | (candidate & (~result.present | (partial.order < 0)))
            base_columns.append(delta_base)
            quote_columns.append(delta_quote)
            consumed_columns.append(consumed+delta_base)
        return QuoteTakerPlan(self,_columns(base_columns),_columns(quote_columns),_columns(consumed_columns),
            matched-matched_before,used-used_before,stopped,fault)

    def apply_quote_plan(self, plan, accepted):
        """接单协调器证明全计划可接纳后应用同一冻结队列的计划；拒绝根无队列变化。"""
        if plan.source is not self:
            raise ValueError('taker计划只能应用到其原冻结队列，不能重复消费或跨状态应用')
        if accepted.shape != (self.rows,) or accepted.dtype != torch.bool or accepted.device != self.kind.device:
            raise TypeError('接单掩码须为同设备bool[rows]')
        roots_fault = self.fault | plan.fault
        next_debt = self.debt + plan.matched_base
        roots_fault = roots_fault | next_debt.fault
        enabled = accepted & (roots_fault == 0)
        candidate = replace(self,
            consumed_base=plan.consumed_after.select(enabled.repeat_interleave(self.capacity), self.consumed_base),
            debt=next_debt.select(enabled, self.debt), fault=roots_fault)
        return candidate.finish_taker_plan(enabled)

    def prepare_level_transition(self, before, after):
        """原Level的逐段循环入口；设备分派复用同一精确算术。"""
        zero = before.constant(0)
        relation = after.compare(before)
        added = (after-before).select(relation.order>0,zero)
        removed = (before-after).select(relation.order<0,zero)
        offset = _minimum(removed,self.debt)
        remaining = removed-offset
        debt = self.debt-offset
        roots_fault = self.fault | before.fault | after.fault | relation.fault
        roots_fault = roots_fault | (before.sign<0).to(torch.int32) | (after.sign<0).to(torch.int32)
        return dict(zero=zero,added=added,remaining=remaining,debt=debt,fault=roots_fault)

    def finish_level_transition(self, prepared, consumed_after, account_after, maker, remaining):
        """原循环结果的append/compact及故障归并；不合并外部段。"""
        zero,added,debt=(prepared[name] for name in ('zero','added','debt'))
        roots_fault = prepared['fault'] | remaining.fault | debt.fault | (remaining.sign>0).to(torch.int32)
        state = replace(self,consumed_base=consumed_after,account_remaining=account_after,
                        debt=debt,fault=roots_fault)
        state,required = state.append_external(added)
        roots_fault = roots_fault | state.fault | _row_fault(maker,self.rows,self.capacity)
        if self.conservative is not None:
            ranked=(self.conservative.kind==2).any(1)
            rank,rank_required=self.conservative.append(added.select(ranked,zero))
            state=replace(state,conservative=rank)
            required=torch.maximum(required,rank_required)
        effects = QueueEffects(self.order_ids,maker,zero,torch.zeros_like(self.fault,dtype=torch.bool),required,roots_fault)
        return state,effects

    def apply_level_transition(self, before, after, *, fifo_loop=None):
        """原减少先抵taker debt，再逐段耗尽；增加永远追加尾段，绝不合并原B/Q。"""
        dispatch=_level_dispatch.get()
        if dispatch is not None:return dispatch(self,before,after)
        prepared=self.prepare_level_transition(before,after)
        zero,remaining=(prepared[name] for name in ('zero','remaining'))
        columns = self.capacity
        eager = not Numbers.device_batch_active() and self.rows > 0
        if eager and fifo_loop is not None:
            positions = torch.arange(self.capacity, device=self.kind.device)[None, :]
            limit = torch.where(self.kind != 0, positions + 1, 0).max()
            live = (positions < limit).expand(self.rows, self.capacity).flatten()
            kind = self.kind.flatten()
            flat_zero = _repeat(zero, self.capacity)
            # 全局有效prefix不变；死尾在算术前屏蔽，输出仍保留原完整字段。
            consumed = self.consumed_base.select(live, flat_zero)
            account = self.account_remaining.select(live, flat_zero)
            base = self.total_base.select(live, flat_zero)
            available = (base - consumed).select(kind == 1, account.select(kind == 2, flat_zero))
            safe_suffix = ((available.sign >= 0) & (available.fault == 0)).all()
            taken, remaining = fifo_loop.take(available, remaining, limit, safe_suffix,
                rows=self.rows, capacity=self.capacity)
            consumed_after = (consumed + taken).select(live & (kind == 1), self.consumed_base)
            account_after = (account - taken).select(live & (kind == 2), self.account_remaining)
            maker = taken.select(live & (kind == 2), flat_zero)
        elif eager:
            # 空尾槽的take恒零；仅eager省算术，不改变物理列或原FIFO顺序。
            positions = torch.arange(self.capacity,device=self.kind.device) + 1
            columns = int(torch.where(self.kind != 0,positions[None,:],0).max().cpu())
            consumed_after, account_after = self.consumed_base, self.account_remaining
            maker = _repeat(zero,self.capacity)
            if columns:
                row_axis = torch.arange(self.rows,device=self.kind.device)
                indices = (row_axis[:,None]*self.capacity +
                           torch.arange(columns,device=self.kind.device)[None,:]).flatten()
                consumed, account = _take(self.consumed_base,indices), _take(self.account_remaining,indices)
                kind = self.kind[:,:columns].flatten()
                flat_zero = _repeat(zero,columns)
                available = (_take(self.total_base,indices)-consumed).select(
                    kind==1,account.select(kind==2,flat_zero))
                zero_suffix = ((available.sign >= 0) & (available.fault == 0)).all().cpu().item()
                takes = []
                for column in range(columns):
                    if zero_suffix and not remaining.sign.any().cpu().item():
                        # 原零后缀仍携带逐行fault和共同字宽；前缀真实take保留。
                        remaining = remaining.with_capacity(max(available.words.shape[1], remaining.words.shape[1]))
                        takes.extend([remaining] * (columns-column))
                        break
                    take = _minimum(_take(available,row_axis*columns+column),remaining)
                    remaining = remaining-take
                    # 保存各列当时的fault，不能用末尾remaining.fault回填前列。
                    takes.append(take)
                taken = _columns(takes)
                consumed_after = _put(consumed_after,indices,(consumed+taken).select(kind==1,consumed))
                account_after = _put(account_after,indices,(account-taken).select(kind==2,account))
                maker = _put(maker,indices,taken.select(kind==2,flat_zero))
        else:
            consumed_columns, account_columns, maker_columns = [], [], []
            for column in range(columns):
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
            consumed_after, account_after = _columns(consumed_columns), _columns(account_columns)
            maker = _columns(maker_columns)
        return self.finish_level_transition(prepared,consumed_after,account_after,maker,remaining)


def run_queue_batch(queue,compute):
    """compute(queue)纯计算候选；只协调段容量和精确字容量，不读取业务条件。"""
    while True:
        result = Numbers.run_exact_batch(lambda:compute(queue))
        demand = result[-1].required_segments if hasattr(result[-1],'required_segments') else result[-1]
        required = int(demand.max().cpu()) if demand.numel() else 0
        if required<=queue.capacity:
            return result
        queue = queue.resized(1<<(required-1).bit_length())
