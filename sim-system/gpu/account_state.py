"""原SIM平台账户的精确设备候选；Ledger只由原NOE交付经济事实推进。

现金/可用/本金及即时费用占款、持仓按原micro单位；返佣字段仍为完整精确有理数。
本层不把平台接受、最终成交状态和经济应用合并，不承担NOE去重或公开可见门。
"""
from dataclasses import dataclass, replace
import torch
from queues import Numbers, ExactInt, ExactRational, _take
from hidden_market import rational_select, zeros


def grouped_fault(values, owners, roots):
    bits = torch.arange(32,dtype=torch.int64,device=values.device)
    flags = ((values.to(torch.int64)[:,None] >> bits) & 1).to(torch.int32)
    reduced = torch.zeros((roots,32),dtype=torch.int32,device=values.device)
    reduced = reduced.scatter_reduce(0,owners[:,None].expand_as(flags),flags,reduce='amax')
    return (reduced.to(torch.int64) << bits).sum(1).to(torch.int32)


@dataclass(frozen=True)
class AccountMovement:
    cash: object
    available: object
    principal: object
    fee: object
    outcomes: object  # ExactInt[assets]
    rebates: object  # ExactRational[roots*4]，maker/taker receivable、maker/taker locked。


@dataclass(frozen=True)
class PlatformAccountDelta:
    sequence: object  # 原next_account_transition_seq；未emit行不可转NOE。
    emitted: torch.Tensor
    movement: AccountMovement
    fault: torch.Tensor


@dataclass(frozen=True)
class PlatformAccountState:
    asset_roots: torch.Tensor
    cash: object
    available: object
    principal: object
    fee: object
    outcomes: object
    rebates: object
    next_sequence: object
    fault: torch.Tensor

    @classmethod
    def open(cls, cash, asset_roots):
        roots = cash.sign.numel()
        if roots < 1 or asset_roots.dtype != torch.int64 or asset_roots.device != cash.words.device:
            raise ValueError('账户及资产所属根形状不一致')
        valid = (asset_roots >= 0) & (asset_roots < roots)
        # 冷目录坐标错误保留为fault；不能gather越界或把外根资产当本根持仓。
        safe = asset_roots.clamp(0,roots-1)
        fault = cash.fault | (cash.sign < 0).to(torch.int32)
        fault = fault | grouped_fault((~valid).to(torch.int32),safe,roots)
        zero = cash.constant(0)
        outcome = ExactInt.from_i64_tensor(torch.zeros_like(asset_roots))
        rebate_zero = ExactInt.from_i64_tensor(torch.zeros(roots*4,dtype=torch.int64,device=cash.words.device))
        return cls(safe,cash,cash,zero,zero,outcome,
                   ExactRational.ratio(rebate_zero,rebate_zero.constant(1)),cash.constant(1),fault)

    def no_movement(self):
        zero = self.cash.constant(0)
        return AccountMovement(zero,zero,zero,zero,self.outcomes.constant(0),zeros(self.rebates))

    def apply_movement(self, movement, enabled):
        roots = self.cash.sign.numel()
        if enabled.shape != (roots,) or enabled.dtype != torch.bool or enabled.device != self.cash.words.device:
            raise TypeError('账户提交掩码须为同设备bool[roots]')
        integer_fields = ('cash','available','principal','fee')
        next_fields = {}
        error = torch.zeros_like(self.fault)
        for name in integer_fields:
            delta = getattr(movement,name)
            if delta.sign.shape != (roots,):
                raise ValueError('账户资金delta形状不一致')
            value = getattr(self,name)+delta
            error = error | value.fault | (value.sign < 0).to(torch.int32)
            next_fields[name] = value
        if movement.outcomes.sign.shape != self.outcomes.sign.shape or movement.rebates.numerator.sign.shape != self.rebates.numerator.sign.shape:
            raise ValueError('账户持仓或返佣delta形状不一致')
        next_outcomes = self.outcomes+movement.outcomes
        error = error | grouped_fault(next_outcomes.fault | (next_outcomes.sign < 0).to(torch.int32),self.asset_roots,roots)
        next_rebates = self.rebates+movement.rebates
        rebate_error = next_rebates.fault | (next_rebates.numerator.sign < 0).to(torch.int32)
        error = error | Numbers.combine_fault_rows(rebate_error.reshape(roots,4))
        next_sequence = self.next_sequence+self.next_sequence.constant(1)
        upper = next_sequence.compare(next_sequence.constant((1<<64)-1))
        error = error | next_sequence.fault | upper.fault | (upper.order > 0).to(torch.int32)
        fault = self.fault | error*enabled.to(torch.int32)
        valid = enabled & (fault == 0)
        for name in integer_fields:
            next_fields[name] = next_fields[name].select(valid,getattr(self,name))
        candidate = replace(self,**next_fields,
            outcomes=next_outcomes.select(valid[self.asset_roots],self.outcomes),
            rebates=rational_select(next_rebates,valid.repeat_interleave(4),self.rebates),
            next_sequence=next_sequence.select(valid,self.next_sequence),fault=fault)
        emitted_movement = AccountMovement(*(getattr(movement,name).select(valid,self.cash.constant(0)) for name in integer_fields),
            movement.outcomes.select(valid[self.asset_roots],self.outcomes.constant(0)),
            rational_select(movement.rebates,valid.repeat_interleave(4),zeros(self.rebates)))
        return candidate,PlatformAccountDelta(self.next_sequence,valid,emitted_movement,fault)

    def accept_order(self, principal, fee, required_available, requested):
        """原apply_order_accepted_with_required_available；现金不变，只从可用转占款。"""
        relation = self.available.compare(required_available)
        input_fault = principal.fault | fee.fault | required_available.fault | relation.fault
        input_fault = input_fault | ((principal.sign <= 0) | (fee.sign < 0) | (required_available.sign < 0)).to(torch.int32)
        fault = self.fault | input_fault*requested.to(torch.int32)
        candidate = replace(self,fault=fault)
        sufficient = relation.order >= 0
        enabled = requested & sufficient & (fault == 0)
        total = principal+fee
        movement = replace(self.no_movement(),available=total.negated(),principal=principal,fee=fee)
        candidate,delta = candidate.apply_movement(movement,enabled)
        rejected = requested & ~sufficient & (fault == 0)
        return candidate,delta,rejected

    def fill_economic(self, base, quote, fee, asset_indices, requested):
        """原经济应用：扣现金/占款并增持仓；MATCHED/FinalSuccess本身不调用本方法。"""
        roots = self.cash.sign.numel()
        if asset_indices.shape != (roots,) or asset_indices.dtype != torch.int64 or asset_indices.device != self.cash.words.device:
            raise TypeError('成交资产须为同设备int64[roots]')
        if self.asset_roots.numel() == 0:
            candidate = replace(self,fault=self.fault | requested.to(torch.int32))
            return candidate.apply_movement(self.no_movement(),torch.zeros_like(requested))
        valid_asset = (asset_indices >= 0) & (asset_indices < self.asset_roots.numel())
        safe = asset_indices.clamp(0,self.asset_roots.numel()-1)
        valid_asset = valid_asset & (self.asset_roots[safe] == torch.arange(roots,device=safe.device))
        error = base.fault | quote.fault | fee.fault
        error = error | ((base.sign <= 0) | (quote.sign <= 0) | (fee.sign < 0) | ~valid_asset).to(torch.int32)
        candidate = replace(self,fault=self.fault | error*requested.to(torch.int32))
        # 每个资产唯一根归属；用固定资产列gather，foreign坐标不会产生业务写入。
        selected = (torch.arange(self.asset_roots.numel(),device=safe.device) == safe[self.asset_roots])
        outcome = _take(base,self.asset_roots).select(selected,self.outcomes.constant(0))
        movement = replace(self.no_movement(),cash=(quote+fee).negated(),principal=quote.negated(),fee=fee.negated(),outcomes=outcome)
        return candidate.apply_movement(movement,requested & (candidate.fault == 0))

    def release_principal(self, amount, requested):
        """原取消/GTD/dust释放经济量；零量不发delta，不释放即时fee占款。"""
        error = amount.fault | (amount.sign < 0).to(torch.int32)
        candidate = replace(self,fault=self.fault | error*requested.to(torch.int32))
        movement = replace(self.no_movement(),available=amount,principal=amount.negated())
        return candidate.apply_movement(movement,requested & (amount.sign > 0) & (candidate.fault == 0))
