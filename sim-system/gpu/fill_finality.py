"""原密封正常成功路径的设备成交记录与独立经济转移候选。

每个记录批次每根至多一个当前成交。完整撮合宿主须将实际产生的记录放入增长池，
按原时间/fill sequence选择当前批次；本模块不承担该全局调度或失败路径补偿。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactRational


@dataclass(frozen=True)
class NormalFillTransition:
    emitted: torch.Tensor
    ordinal: torch.Tensor
    state: torch.Tensor
    economic_delta: object
    rebate_delta: object
    fault: torch.Tensor


@dataclass(frozen=True)
class NormalFillRecord:
    base: object
    quote: object
    fee: object
    asset_indices: torch.Tensor
    matched_at: torch.Tensor
    active: torch.Tensor
    ordinal: torch.Tensor
    economic_applied: torch.Tensor
    offsets: torch.Tensor
    states: torch.Tensor
    economic_ordinal: int
    fault: torch.Tensor

    @classmethod
    def create(cls, base, quote, fee, asset_indices, matched_at, active, sealed_normal_path):
        # 冷入口仅接受该明确命名的正常成功路径组件；不将失败选择改成成功。
        codes = {'MATCHED_NOT_BROADCASTED':0,'MATCHED':1,'RETRYING':2,'MINED':3,'CONFIRMED':4}
        steps = sealed_normal_path['steps']
        if not steps or steps[-1]['state'] != 'CONFIRMED' or any(step['state'] not in codes for step in steps):
            raise ValueError('本组件只消费原密封正常成功路径；失败补偿归完整最终性协调器')
        offsets = [step['offset_ms'] for step in steps]
        ordinal = sealed_normal_path['economic_application_ordinal']
        if offsets[0] != 0 or any(a >= b for a,b in zip(offsets,offsets[1:])) or ordinal is None or not 0 <= ordinal < len(steps):
            raise ValueError('正常成功路径的时间或经济应用位置不合法')
        roots = base.sign.numel()
        if asset_indices.shape != (roots,) or matched_at.shape != (roots,) or active.shape != (roots,):
            raise ValueError('成交记录布局不一致')
        if asset_indices.dtype != torch.int64 or matched_at.dtype != torch.int64 or active.dtype != torch.bool:
            raise TypeError('成交坐标/时间须int64，active须bool')
        if any(value.device != base.words.device for value in (asset_indices,matched_at,active)):
            raise ValueError('成交记录须在同一设备')
        error = base.fault | quote.fault | fee.fault
        error = error | ((base.sign <= 0) | (quote.sign <= 0) | (fee.sign < 0)).to(torch.int32)
        error = error | (matched_at > (1<<63)-1-offsets[-1]).to(torch.int32)
        fault = error*active.to(torch.int32)
        return cls(base,quote,fee,asset_indices,matched_at,active,
            torch.full((roots,),-1,dtype=torch.int64,device=matched_at.device),
            torch.zeros(roots,dtype=torch.bool,device=matched_at.device),
            torch.tensor(offsets,dtype=torch.int64,device=matched_at.device),
            torch.tensor([codes[step['state']] for step in steps],dtype=torch.int8,device=matched_at.device),ordinal,fault)

    def advance_one(self, account, at, taker_rebate_rate):
        """只推进到下一原时点；重复调用不重复经济应用，跳过未消费义务保留fault。"""
        if at.shape != self.matched_at.shape or at.dtype != torch.int64 or at.device != self.matched_at.device:
            raise TypeError('最终性时刻须同设备int64[roots]')
        original_account = account
        next_ordinal = self.ordinal+1
        pending = self.active & (next_ordinal < self.offsets.numel())
        safe = next_ordinal.clamp(0,self.offsets.numel()-1)
        scheduled = self.matched_at+self.offsets[safe]
        fault = self.fault | ((at > scheduled) & pending).to(torch.int32)
        ready = pending & (at == scheduled) & (fault == 0)
        economic = ready & (next_ordinal == self.economic_ordinal) & ~self.economic_applied
        account, economic_delta = account.fill_economic(self.base,self.quote,self.fee,self.asset_indices,economic)
        state = self.states[safe]
        accrual = ExactRational.ratio(self.fee,self.fee.constant(1_000_000))*taker_rebate_rate
        success = ready & (state == 4) & (self.fee.sign > 0) & (account.fault == 0)
        account, rebate_delta = account.accrue_rebate(accrual,torch.zeros_like(ready),success)
        fault = fault | economic_delta.fault | rebate_delta.fault
        committed = ready & (fault == 0)
        account = account.coordinate_result(original_account,fault)
        economic_delta = account.coordinate_delta(economic_delta,committed,fault)
        rebate_delta = account.coordinate_delta(rebate_delta,committed,fault)
        record = replace(self,ordinal=torch.where(committed,next_ordinal,self.ordinal),
            economic_applied=self.economic_applied | economic_delta.emitted,fault=fault)
        transition = NormalFillTransition(committed,next_ordinal,state,economic_delta,rebate_delta,fault)
        return record,account,transition
