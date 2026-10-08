"""公共原动作正文的Execute消费；不评分、不抽样、不发送或伪造平台时刻。"""
from dataclasses import dataclass,replace
import torch


@dataclass(frozen=True)
class ActionControl:
    """控制面提供稳定身份的原字典坐标和实际Execute时点，不能用物理槽造ID。"""
    decisions: torch.Tensor
    global_requests: torch.Tensor
    leg_requests: torch.Tensor  # [roots,2]原LedgerSubmissionItemIdentity字典。
    execute_at: torch.Tensor


@dataclass(frozen=True)
class SelectedAction:
    identities: torch.Tensor
    control: ActionControl
    family: torch.Tensor  # -1NoAct/0NoTrade/1Single/2Paired/3SingleCancel/4PairCancel/5MarketCancel。
    legs: object          # 原SubmittedLegs/SubmittedBuyTerms正文；receipt仍未知。
    assets: torch.Tensor  # 与正文腿轴一致的原token目录。
    principal: object
    fee: object
    cancellation: object  # 原CancellationScopes。
    cancel_orders: object # 原订单身份，Pair按原item ordinal，不按形成rank。
    cancel_present: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class SelectionReservation:
    decision: object
    complete: torch.Tensor  # 物理目录已足够；资金拒绝同样完成且保原决定。


def reserve_selected(state,action,enabled):
    """一次整动作调用原AdmissionLifecycle；只安装其实际返回的Ledger。

    NoTrade/Cancel没有预留。资源不足保原成功前缀，原需求交外层冷增长。
    enabled由所属选择发布协调者保留；重复消费不再调用当前根预留。
    """
    requested=action.legs.present & enabled[:,None] & (action.fault==0)[:,None]
    ledger,decision=state.ledger.reserve_action(action.control.leg_requests,
        action.legs.condition_slots,action.principal,action.fee,requested)
    complete=(decision.required_capacity<=ledger.reservations.capacity)&(decision.fault==0)
    if decision.required_market_conditions is not None:
        complete &= decision.required_market_conditions<0
    complete &= action.fault==0
    return replace(state,ledger=ledger),SelectionReservation(decision,complete)
