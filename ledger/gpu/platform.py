"""Ledger原平台全局余额/直接前驱的精确设备候选。

消费已领取NOE经济列；经济关联及原位置去重由Ledger协调器提供，不能拿SIM
平台确认覆盖这些检查。本层不改变派生归属、不生成资金或代替完整Ledger。
"""
from dataclasses import dataclass, replace
import torch


def select(value, enabled, previous):
    kind=type(value)
    return kind(value.numerator.select(enabled,previous.numerator),
                value.denominator.select(enabled,previous.denominator))


def root_fault(values, roots, width):
    bits=torch.arange(32,dtype=torch.int64,device=values.device)
    flags=(((values.reshape(roots,width,1).to(torch.int64)>>bits)&1)!=0).any(dim=1)
    return (flags.to(torch.int64)<<bits).sum(1).to(torch.int32)


@dataclass(frozen=True)
class PlatformTransition:
    applied: torch.Tensor
    already_applied: torch.Tensor
    missing_predecessor: torch.Tensor
    missing_association: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PlatformBalances:
    balances: object  # ExactRational[roots*8]：cash,available,principal,fee,maker/taker receivable,maker/taker locked。
    outcomes: object  # ExactRational[assets]；原token目录引用不改。
    asset_roots: torch.Tensor
    platform_source_prefix: object
    fault: torch.Tensor

    @classmethod
    def open(cls,balances,outcomes,asset_roots,prefix):
        roots=prefix.sign.numel()
        if balances.numerator.sign.numel()!=roots*8 or outcomes.numerator.sign.numel()!=asset_roots.numel():
            raise ValueError('Ledger平台余额布局不符')
        if asset_roots.dtype!=torch.int64 or asset_roots.device!=prefix.words.device:
            raise TypeError('Ledger资产根须同设备int64')
        if roots<1:
            raise ValueError('Ledger开户根为空')
        valid=(asset_roots>=0)&(asset_roots<roots)
        safe=asset_roots.clamp(0,roots-1)
        fault=prefix.fault | root_fault(balances.fault,roots,8)
        # 未知物理资产归属是工程故障，不补成某根的合法资产。
        for bit in range(32):
            row=(outcomes.fault.to(torch.int64)>>bit)&1
            reduced=torch.zeros(roots,dtype=torch.int64,device=safe.device).scatter_reduce(0,safe,row,reduce='amax')
            fault=fault | (reduced<<bit).to(torch.int32)
        invalid=torch.zeros(roots,dtype=torch.int32,device=safe.device).scatter_reduce(
            0,safe,(~valid).to(torch.int32),reduce='amax')
        upper=prefix.compare(prefix.constant((1<<64)-1))
        fault=fault | invalid | upper.fault | ((upper.order>0)|(prefix.sign<0)).to(torch.int32)
        return cls(balances,outcomes,safe,prefix,fault)

    def apply_transition(self,sequence,balance_delta,outcome_delta,enabled,position_applied,association_available):
        """原期望prefix+1；缺前驱/关联不记位置、不动余额，重复位置不重复应用。

        不添加余额非负或GPU位宽业务门；全部实际值保持精确。调用者在整个
        Ledger生命周期/预留候选成功后才能提交本余额及原NOE位置。
        """
        roots=self.fault.numel()
        for flag in [enabled,position_applied,association_available]:
            if flag.shape!=(roots,) or flag.dtype!=torch.bool or flag.device!=self.asset_roots.device:
                raise TypeError('Ledger应用掩码需同设备bool[roots]')
        if sequence.sign.numel()!=roots or balance_delta.numerator.sign.numel()!=roots*8 or outcome_delta.numerator.sign.shape!=self.outcomes.numerator.sign.shape:
            raise ValueError('Ledger经济列人口不符')
        requested=enabled & ~position_applied
        expected=self.platform_source_prefix+self.platform_source_prefix.constant(1)
        equal=sequence.compare(expected)
        upper=expected.compare(expected.constant((1<<64)-1))
        eligible=requested & association_available
        missing=eligible & (equal.order!=0)
        available=requested & ~missing & association_available
        next_balances=self.balances+balance_delta
        next_outcomes=self.outcomes+outcome_delta
        header_error=sequence.fault | equal.fault | upper.fault | (upper.order>0).to(torch.int32)
        error=root_fault(next_balances.fault,roots,8)
        for bit in range(32):
            row=(next_outcomes.fault.to(torch.int64)>>bit)&1
            reduced=torch.zeros(roots,dtype=torch.int64,device=row.device).scatter_reduce(0,self.asset_roots,row,reduce='amax')
            error=error | (reduced<<bit).to(torch.int32)
        # 算术容量请求在公共run_exact_batch独立收集；缺前驱不把临时值提交。
        fault=self.fault | torch.where(eligible,header_error,torch.zeros_like(header_error))
        fault=fault | torch.where(available,error,torch.zeros_like(error))
        committed=available & (fault==0)
        candidate=replace(self,balances=select(next_balances,committed.repeat_interleave(8),self.balances),
            outcomes=select(next_outcomes,committed[self.asset_roots],self.outcomes),
            platform_source_prefix=sequence.select(committed,self.platform_source_prefix),fault=fault)
        return candidate,PlatformTransition(committed,enabled & position_applied,
            missing & (fault==0),requested & ~association_available & (fault==0),fault)
