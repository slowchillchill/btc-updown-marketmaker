"""原SIM请求登记、pending动作顺序及完成响应的设备状态。

request_keys/item_refs引用同根不可变原身份/意图目录。input_sequence由整根
输入提交前沿提供，不是请求计数器。SDK准入及CancelMarket目标展开仍归原入口。
本状态不接纳订单、不分配order/fill/private序号，不提前关闭hidden毫秒。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, ExactInt, _take, _repeat
from latency import DelaySample
from fill_pool import _put_integer


@dataclass(frozen=True)
class RequestReceipt:
    slot: torch.Tensor
    input_sequence: object
    arrived_at: torch.Tensor
    scheduled_count: torch.Tensor
    registered: torch.Tensor
    duplicate: torch.Tensor
    required_requests: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PendingMutation:
    source: object
    request_slot: torch.Tensor
    action_slot: torch.Tensor
    request_key: torch.Tensor
    item_ref: torch.Tensor
    ordinal: torch.Tensor
    input_sequence: object
    effective_at: torch.Tensor
    exists: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class CompletedMutation:
    request_slot: torch.Tensor
    request_key: torch.Tensor
    method: torch.Tensor
    responded_at: torch.Tensor
    received_at: torch.Tensor
    item_refs: torch.Tensor
    result_refs: torch.Tensor
    ordinals: torch.Tensor
    present: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class MutationRequests:
    keys: torch.Tensor  # [roots,capacity]；原全局request UTF8目录引用，不是新业务身份。
    occupied: torch.Tensor
    methods: torch.Tensor
    input_sequences: object  # 完整u64[roots*capacity]，物理槽不代替原序号。
    sent_at: torch.Tensor
    arrived_at: torch.Tensor
    inbound_delays: object
    item_refs: torch.Tensor  # [roots,capacity,action_capacity]，原首次item正文引用。
    ordinals: torch.Tensor
    effective_at: torch.Tensor
    present: torch.Tensor
    done: torch.Tensor
    result_refs: torch.Tensor
    sealed: torch.Tensor
    fault: torch.Tensor

    @property
    def roots(self):
        return self.keys.shape[0]

    @property
    def capacity(self):
        return self.keys.shape[1]

    @property
    def action_capacity(self):
        return self.item_refs.shape[2]

    @classmethod
    def empty(cls, template, *, capacity=4, action_capacity=2):
        if capacity < 1 or action_capacity < 1:
            raise ValueError('请求/动作物理容量必须正数，可按真实人口增长')
        roots, device = template.sign.numel(), template.words.device
        shape = (roots, capacity)
        actions = (*shape, action_capacity)
        zero = template.constant(0)
        return cls(torch.full(shape, -1, dtype=torch.int64, device=device),
            torch.zeros(shape, dtype=torch.bool, device=device),
            torch.zeros(shape, dtype=torch.int64, device=device), _repeat(zero, capacity),
            torch.zeros(shape, dtype=torch.int64, device=device),
            torch.zeros(shape, dtype=torch.int64, device=device), _repeat(zero, capacity),
            torch.full(actions, -1, dtype=torch.int64, device=device),
            torch.zeros(actions, dtype=torch.int64, device=device),
            torch.zeros(actions, dtype=torch.int64, device=device),
            torch.zeros(actions, dtype=torch.bool, device=device),
            torch.zeros(actions, dtype=torch.bool, device=device),
            torch.full(actions, -1, dtype=torch.int64, device=device),
            torch.zeros(shape, dtype=torch.bool, device=device), torch.zeros_like(template.fault))

    def resized(self, capacity, action_capacity=None):
        capacity = max(capacity, self.capacity)
        action_capacity = max(action_capacity or self.action_capacity, self.action_capacity)
        if (capacity, action_capacity) == (self.capacity, self.action_capacity):
            return self
        result = MutationRequests.empty(_take(self.input_sequences,
            torch.arange(self.roots, device=self.keys.device)*self.capacity),
            capacity=capacity, action_capacity=action_capacity)
        updates = {}
        old_index = torch.arange(self.roots, device=self.keys.device)[:, None]*self.capacity
        old_index = (old_index+torch.arange(self.capacity, device=self.keys.device)).flatten()
        new_index = (torch.arange(self.roots, device=self.keys.device)[:, None]*capacity+
                     torch.arange(self.capacity, device=self.keys.device)).flatten()
        for name in ('input_sequences', 'inbound_delays'):
            updates[name] = _put_integer(getattr(result, name), _take(getattr(self, name), old_index),
                                        new_index, torch.ones_like(new_index, dtype=torch.bool))
        for name in ('keys', 'occupied', 'methods', 'sent_at', 'arrived_at', 'sealed'):
            updates[name] = torch.nn.functional.pad(getattr(self, name), (0, capacity-self.capacity),
                                                    value=-1 if name == 'keys' else 0)
        for name in ('item_refs', 'ordinals', 'effective_at', 'present', 'done', 'result_refs'):
            updates[name] = torch.nn.functional.pad(getattr(self, name),
                (0, action_capacity-self.action_capacity, 0, capacity-self.capacity),
                value=-1 if name in ('item_refs', 'result_refs') else 0)
        return replace(result, **updates, fault=self.fault)

    def register(self, sampler, identity_bytes, identity_offsets, request_keys, methods,
                 input_sequences, sent_at, item_refs, ordinals, present, taker_delayed,
                 taker_delay_ms, enabled, source_fault):
        """原入口已验证的首次动作列；同ID先查旧receipt，后来正文不覆盖也不拒绝。

        身份字典/原item校验故障由source_fault携带。SDK正文及method适用条件
        不在此复制。这里校验原u16 ordinal、u64 input序与checked_add时钟表示。
        """
        shape = (self.roots,)
        for value in (request_keys, methods, sent_at, taker_delay_ms):
            if value.shape != shape or value.dtype != torch.int64 or value.device != self.keys.device:
                raise TypeError('请求坐标/时刻须同设备int64[roots]')
        if enabled.shape != shape or enabled.dtype != torch.bool or source_fault.shape != shape:
            raise TypeError('请求掩码/源故障根布局错误')
        expected = (self.roots, self.action_capacity)
        for value in (item_refs, ordinals, present, taker_delayed):
            if value.shape != expected or value.device != self.keys.device:
                raise ValueError('原动作列须与已增长物理动作容量一致')
        if item_refs.dtype != torch.int64 or ordinals.dtype != torch.int64 or (
                present.dtype != torch.bool or taker_delayed.dtype != torch.bool):
            raise TypeError('动作引用/ordinal为int64，人口/延迟类型为bool')
        if input_sequences.sign.shape != shape or input_sequences.words.device != self.keys.device:
            raise ValueError('原input_sequence根布局错误')
        match = self.occupied & (self.keys == request_keys[:, None])
        duplicate = enabled & match.any(1)
        fresh = enabled & ~duplicate
        count = self.occupied.to(torch.int64).sum(1)
        slot = torch.where(duplicate, match.to(torch.int64).argmax(1), count.clamp_max(self.capacity-1))
        roots = torch.arange(self.roots, device=self.keys.device)
        coordinate = roots*self.capacity+slot
        # 确定性纯采样不改变RNG；故障与后来正文只对fresh人口适用。
        key_valid = (request_keys >= 0) & (request_keys < identity_offsets.numel()-1)
        safe_keys = request_keys.clamp(0, max(identity_offsets.numel()-2, 0))
        if identity_offsets.numel() < 2:
            raise ValueError('原身份字节目录尚未安装')
        # 原字节目录只读共享，设备索引保留首次global_request_id的完整字节。
        outbound = sampler.sample_requests_indexed(roots, identity_bytes, identity_offsets, safe_keys,
                                                   torch.zeros_like(roots))
        inbound = sampler.sample_requests_indexed(roots, identity_bytes, identity_offsets, safe_keys,
                                                  torch.ones_like(roots))
        arrival, arrival_fault = outbound.arrival_at(sent_at)
        delay = torch.where(taker_delayed & present, taker_delay_ms[:, None], 0)
        overflow = ((delay < 0) | (arrival[:, None] > (1 << 63)-1-delay)) & present
        effective = arrival[:, None]+delay
        sequence_error = input_sequences.compare(input_sequences.constant((1 << 64)-1))
        order = torch.argsort(torch.where(present, ordinals, 65536), dim=1, stable=True)
        sorted_ordinal, sorted_present = ordinals.gather(1, order), present.gather(1, order)
        repeated = (sorted_ordinal[:, 1:] == sorted_ordinal[:, :-1]) & sorted_present[:, 1:] & sorted_present[:, :-1]
        error = source_fault | input_sequences.fault | sequence_error.fault | arrival_fault | inbound.milliseconds.fault
        error = error | ((~key_valid) | (input_sequences.sign < 0) | (sequence_error.order > 0) |
            (((ordinals < 0) | (ordinals > 65535) | (item_refs < 0)) & present).any(1) |
            repeated.any(1) | ~present.any(1) | overflow.any(1)).to(torch.int32)
        fault = self.fault | error*fresh.to(torch.int32)
        registered = fresh & (count < self.capacity) & (fault == 0)
        values = {'item_refs': item_refs, 'ordinals': ordinals, 'effective_at': effective,
                  'present': present, 'done': torch.zeros_like(present),
                  'result_refs': torch.full_like(item_refs, -1)}
        updates = {}
        for name, value in values.items():
            value = value.gather(1, order)
            old = getattr(self, name)
            chosen = torch.where(registered[:, None], value, old[roots, slot])
            updates[name] = old.flatten(0, 1).index_copy(0, coordinate, chosen).reshape_as(old)
        for name, value in (('keys', request_keys), ('methods', methods), ('sent_at', sent_at),
                            ('arrived_at', arrival), ('occupied', torch.ones_like(enabled)),
                            ('sealed', torch.zeros_like(enabled))):
            old = getattr(self, name)
            updates[name] = old.flatten().index_copy(0, coordinate,
                torch.where(registered, value, old[roots, slot])).reshape_as(old)
        updates['input_sequences'] = _put_integer(self.input_sequences, input_sequences, coordinate, registered)
        updates['inbound_delays'] = _put_integer(self.inbound_delays, inbound.milliseconds, coordinate, registered)
        candidate = replace(self, **updates, fault=fault)
        receipt = RequestReceipt(slot, _take(candidate.input_sequences, coordinate),
            candidate.arrived_at[roots, slot], candidate.present[roots, slot].to(torch.int64).sum(1),
            registered, duplicate & (fault == 0), count+fresh.to(torch.int64), fault)
        return candidate, receipt

    def next_pending(self):
        """每根仅选原(time,u64 input_sequence,ordinal)第一项；合法INT64_MAX非空。"""
        active = self.occupied[:, :, None] & self.present & ~self.done & (self.fault == 0)[:, None, None]
        times = torch.where(active, self.effective_at, (1 << 63)-1)
        earliest = times.flatten(1).min(1).values
        selected = active & (times == earliest[:, None, None])
        words = self.input_sequences.with_capacity(2).words.reshape(self.roots, self.capacity, -1)
        # 已登记序号保证u64；高/低字按无符号值选择，不将它转有符号int64排序。
        for word in (1, 0):
            component = (words[:, :, word].to(torch.int64) & 0xffffffff)[:, :, None].expand_as(times)
            minimum = torch.where(selected, component, 1 << 32).flatten(1).min(1).values
            selected = selected & (component == minimum[:, None, None])
        minimum_ordinal = torch.where(selected, self.ordinals, 65536).flatten(1).min(1).values
        selected = selected & (self.ordinals == minimum_ordinal[:, None, None])
        flat = selected.flatten(1).to(torch.int64).argmax(1)
        request_slot, action_slot = flat // self.action_capacity, flat % self.action_capacity
        roots = torch.arange(self.roots, device=self.keys.device)
        exists = active.flatten(1).any(1)
        return PendingMutation(self, request_slot, action_slot, self.keys[roots, request_slot],
            self.item_refs[roots, request_slot, action_slot], self.ordinals[roots, request_slot, action_slot],
            _take(self.input_sequences, roots*self.capacity+request_slot), earliest, exists, self.fault)

    def complete(self, pending, result_refs, enabled, effect_fault):
        """实际动作候选成功后消费一次；最后项封口，结果始终按原ordinal保留。

        调用者必须把本候选与对应SIM动作共同提交；故障不得当RejectedNoEffect。
        pending对象须来自本状态，纯资源重试不安装done或发射响应。
        """
        current = self.next_pending()
        if pending.source is not self:
            raise ValueError('pending动作只能提交到选择时的同一请求状态')
        identity = (pending.request_slot == current.request_slot) & (pending.action_slot == current.action_slot)
        identity = identity & (pending.item_ref == current.item_ref) & (pending.request_key == current.request_key)
        identity = identity & (pending.effective_at == current.effective_at) & (pending.exists == current.exists)
        fault = self.fault | effect_fault*enabled.to(torch.int32)
        fault = fault | (enabled & (~identity | ~current.exists | (result_refs < 0))).to(torch.int32)
        roots = torch.arange(self.roots, device=self.keys.device)
        slot, action = current.request_slot, current.action_slot
        coordinate = (roots*self.capacity+slot)*self.action_capacity+action
        consumed = enabled & current.exists & (fault == 0)
        done = self.done.flatten().index_copy(0, coordinate,
            self.done[roots, slot, action] | consumed).reshape_as(self.done)
        results = self.result_refs.flatten().index_copy(0, coordinate,
            torch.where(consumed, result_refs, self.result_refs[roots, slot, action])).reshape_as(self.result_refs)
        finished = consumed & (~self.present[roots, slot] | done[roots, slot]).all(1) & ~self.sealed[roots, slot]
        responded = torch.where(self.present[roots, slot], self.effective_at[roots, slot], -(1 << 63)).max(1).values
        delay = _take(self.inbound_delays, roots*self.capacity+slot)
        received, response_fault = DelaySample(delay, torch.empty(0, device=self.keys.device)).arrival_at(responded)
        fault = fault | response_fault*finished.to(torch.int32)
        valid = fault == 0
        done = torch.where(valid[:, None, None], done, self.done)
        results = torch.where(valid[:, None, None], results, self.result_refs)
        finished = finished & valid
        req_index = roots*self.capacity+slot
        sealed = self.sealed.flatten().index_copy(0, req_index,
            self.sealed[roots, slot] | finished).reshape_as(self.sealed)
        candidate = replace(self, done=done, result_refs=results, sealed=sealed, fault=fault)
        return candidate, CompletedMutation(slot, self.keys[roots, slot], self.methods[roots, slot],
            responded, received, self.item_refs[roots, slot], results[roots, slot], self.ordinals[roots, slot],
            self.present[roots, slot], finished, fault)

    def run_registration(self, *args, **kwargs):
        """只读取物理容量请求；扩目录后从原请求/原状态重算纯候选。"""
        state = self
        while True:
            candidate, receipt = Numbers.run_exact_batch(lambda: state.register(*args, **kwargs))
            required = int(receipt.required_requests.max().cpu())
            if required <= state.capacity:
                return candidate, receipt
            state = state.resized(1 << (required-1).bit_length())

    def coordinate_result(self,original,committed,fault):
        """与发送方Ledger登记或Direct发射共同提交，不消费失败/资源不足的请求。"""
        if self.keys.shape!=original.keys.shape or self.item_refs.shape!=original.item_refs.shape:
            raise ValueError('共同提交须使用同次冷容量的请求前驱')
        from dataclasses import fields
        updates={}
        for field in fields(self):
            if field.name=='fault':continue
            new,old=getattr(self,field.name),getattr(original,field.name)
            if new is old:updates[field.name]=new
            elif isinstance(new,ExactInt):
                updates[field.name]=new.select(committed.repeat_interleave(self.capacity),old)
            else:
                updates[field.name]=torch.where(committed.reshape((self.roots,)+(1,)*(new.ndim-1)),new,old)
        return replace(self,**updates,fault=fault)
