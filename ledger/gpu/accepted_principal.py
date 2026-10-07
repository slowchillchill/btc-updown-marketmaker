"""原生命周期累计接纳本金；独立于当前净OrderFunds，随每条订单事务精确提交。"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class AcceptedPrincipal:
    conditions: torch.Tensor  # 原固定condition字典坐标；增长须保留旧成员。
    amounts: object           # [roots,conditions]精确累计，不随取消/释放/兑付减少。
    known: torch.Tensor       # 实际完整冷前缀；中途未安装历史不能从空数组猜Known零。
    required_conditions: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def open(cls, roots, conditions, *, known=False, amounts=None):
        if roots<1 or conditions.ndim!=1 or conditions.dtype!=torch.int64:
            raise TypeError('累计本金需原根人口和同设备int64 condition目录')
        device=conditions.device;count=conditions.numel()
        zero=ExactInt.from_i64_tensor(torch.zeros(roots*count,dtype=torch.int64,device=device))
        if amounts is None: amounts=ExactRational(zero,zero.constant(1))
        if any(value.sign.shape!=(roots*count,) or value.words.device!=device
                for value in (amounts.numerator,amounts.denominator)):
            raise TypeError('累计本金冷值需与原根/condition目录对齐')
        if isinstance(known,bool): known=torch.full((roots,),known,dtype=torch.bool,device=device)
        if known.shape!=(roots,) or known.dtype!=torch.bool or known.device!=device:
            raise TypeError('累计本金冷前缀known需同设备bool[roots]')
        duplicate=conditions[:,None]==conditions[None,:]
        duplicate&=~torch.eye(count,dtype=torch.bool,device=device)
        fault=torch.zeros(roots,dtype=torch.int32,device=device)
        fault|=((conditions<0).any()|duplicate.any()).to(torch.int32)
        fault|=Numbers.combine_fault_rows(amounts.fault.reshape(roots,count))
        return cls(conditions.clone(),amounts,known.clone(),torch.full((roots,),-1,dtype=torch.int64,device=device),fault)

    def add(self, condition, amount, enabled):
        roots=self.known.numel();count=self.conditions.numel();device=self.conditions.device
        if (condition.shape!=(roots,) or condition.dtype!=torch.int64 or condition.device!=device
                or enabled.shape!=(roots,) or enabled.dtype!=torch.bool or enabled.device!=device
                or amount.numerator.sign.shape!=(roots,) or amount.numerator.words.device!=device):
            raise TypeError('累计本金候选需同根原condition、精确本金与启用位')
        same=condition[:,None]==self.conditions[None,:]
        found=same.any(1)
        fault=self.fault|torch.where(enabled,amount.fault|(condition<0).to(torch.int32),0)
        ready=~enabled|(found&(fault==0))
        at=torch.arange(roots,device=device)[:,None].expand(roots,count).flatten()
        incoming=take_ratio(amount,at)
        zero=ExactRational(incoming.numerator.constant(0),incoming.denominator.constant(1))
        total=self.amounts+select(incoming,(same&enabled[:,None]&ready[:,None]).flatten(),zero)
        fault|=Numbers.combine_fault_rows(total.fault.reshape(roots,count))
        required=torch.where(self.required_conditions>=0,self.required_conditions,
            torch.where(enabled&~found,condition,-1))
        return replace(self,amounts=total,required_conditions=required,fault=fault),ready&(fault==0)

    def coordinated(self, original, committed):
        return replace(self,amounts=select(self.amounts,committed.repeat_interleave(self.conditions.numel()),original.amounts),
            known=torch.where(committed,self.known,original.known))

    def extended(self, conditions):
        candidate=type(self).open(self.known.numel(),conditions,known=self.known)
        same=self.conditions[:,None]==conditions[None,:]
        if self.conditions.numel():
            index=same.to(torch.int64).argmax(0)
            roots=torch.arange(self.known.numel(),device=conditions.device)[:,None]
            amount=take_ratio(self.amounts,(roots*self.conditions.numel()+index[None]).flatten())
            absent=candidate.amounts
            amount=select(amount,same.any(0)[None].expand(self.known.numel(),-1).flatten(),absent)
        else: amount=candidate.amounts
        missing=~same.any(1)
        required=torch.where((self.required_conditions[:,None]==conditions[None]).any(1),-1,self.required_conditions)
        return replace(candidate,amounts=amount,required_conditions=required,
            fault=self.fault|candidate.fault|missing.any().to(torch.int32))

    def read(self, conditions, present):
        roots=self.known.numel();count=self.conditions.numel()
        if (conditions.ndim!=2 or conditions.size(0)!=roots or conditions.dtype!=torch.int64
                or conditions.device!=self.conditions.device or present.shape!=conditions.shape
                or present.dtype!=torch.bool or present.device!=self.conditions.device):
            raise TypeError('累计本金读面需同设备原作用域condition及存在位')
        if not count:
            zero=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=self.conditions.device))
            return ExactRational(zero,zero.constant(1)),self.known|~present.any(1),self.fault
        member=((self.conditions[None,:,None]==conditions[:,None,:])&present[:,None,:]).any(2)
        at=(torch.arange(roots,device=self.conditions.device)[:,None]*count+torch.arange(count,device=self.conditions.device)[None]).flatten()
        value=take_ratio(self.amounts,at)
        zero=ExactRational(value.numerator.constant(0),value.denominator.constant(1))
        total=sum_rows(select(value,member.flatten(),zero),roots,count)
        # 完整冷历史中未有接纳的市场为Known零；物理目录缺项只在add时请求增长。
        known=self.known|~present.any(1)
        return total,known,self.fault|total.fault
