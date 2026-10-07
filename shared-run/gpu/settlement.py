"""原公开结算协调器的设备候选；首次发送资格不限制已有请求查询/重试。

account/condition/canonical resolution/policy由同根无损目录绑定，物理市场行
只选择请求人口。输出固定SDK indexSets=[1,2]，不允许Actor选择金额或赢家。
"""
from dataclasses import dataclass, replace
import torch


@dataclass(frozen=True)
class SettlementActions:
    roots: torch.Tensor
    conditions: torch.Tensor
    resolutions: torch.Tensor
    requested_at: torch.Tensor
    request: torch.Tensor
    query: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class SettlementCoordinator:
    handled: torch.Tensor  # 原同condition最后已处理公开observation；-1为None。

    @classmethod
    def open(cls, resolutions):
        return cls(torch.full_like(resolutions.conditions, -1))

    def advance(self, frozen, enabled):
        ledger=frozen.ledger
        resolutions=ledger.resolutions
        commitments=frozen.commitments
        if resolutions is None or commitments is None or commitments.condition_slots is None or commitments.association_known is None:
            raise ValueError('结算协调须同次公开判定及完整receipt逐腿承诺绑定')
        if enabled.shape!=frozen.cutoff.shape or enabled.dtype!=torch.bool or enabled.device!=frozen.cutoff.device:
            raise TypeError('结算推进人口须同设备bool[roots]')
        if self.handled.shape!=resolutions.conditions.shape:
            raise ValueError('结算handled目录须对应原公开condition人口')
        roots=resolutions.roots
        read=resolutions.read(ledger.success_pools)
        predecessors=resolutions.economic_predecessors(ledger.trade_success,ledger.rebate_days)
        legs=commitments.present[roots]
        leg_known=commitments.association_known[roots]
        # 无法关联的实际承诺不能证明本condition承诺为空。
        blocked=(legs & (~leg_known | (commitments.condition_slots[roots]==resolutions.conditions[:,None]))).any(1)
        positive=(read.paired_receivable.numerator.sign>0)|(read.winner_orphan_receivable.numerator.sign>0)
        state=resolutions.requests
        existing=state.present
        terminal=(state.outcomes==2)|(state.outcomes==3)
        fresh=state.observations!=self.handled
        fault=frozen.fault[roots] | read.fault | predecessors.fault | commitments.fault[roots]
        waiting=frozen.pending_noe_count.sign[roots]!=0
        active=enabled[roots] & read.present & ~waiting & (fault==0)
        first=~existing & read.known & predecessors.known & predecessors.closed & ~blocked & positive
        repeat=existing & ~terminal & fresh
        request=active & (first | (repeat & (state.observations==7)))
        query=active & repeat & (state.observations==2)
        # 原已有请求非终局每个新观察均记handled，包括不产生动作的观察。
        handled=torch.where(active & repeat,state.observations,self.handled)
        return replace(self,handled=handled), SettlementActions(roots,resolutions.conditions,
            read.identities,frozen.cutoff[roots],request,query,fault)
