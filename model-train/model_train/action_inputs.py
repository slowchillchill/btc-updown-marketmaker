"""模型真正开始时绑定的自有精确动作输入；不反解slow或持有活动Ledger。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class ActorPolicyInputs:
    actor: object
    state: torch.Tensor
    rules: torch.Tensor
    cancels: object
    policy_version: torch.Tensor
    ready: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def publish(cls,actor,market,frozen,evaluation,cancels):
        from actor_context import _owned
        from cancel_objects import global_cancel_slots
        rows=actor.cutoff.numel();device=actor.cutoff.device
        slots=global_cancel_slots(actor.cancels)
        if (market.rule.shape!=(rows,30) or evaluation.state.dim()!=2 or evaluation.state.shape[0]!=rows
                or evaluation.policy_version.shape!=(rows,) or cancels.values.shape[:2]!=cancels.present.shape
                or cancels.values.dim()!=3 or cancels.values.shape[-1]!=184 or cancels.present.shape[0]!=rows
                or cancels.physical_slots.shape!=slots.shape or frozen.identities.shape!=actor.identities.shape
                or frozen.cutoff.shape!=(rows,) or any(x.shape!=(rows,) for x in
                    (evaluation.ready,evaluation.fault,market.role,market.fault,market.resources_fit,cancels.fault))):
            raise TypeError('实际模型/规则/184输入须匹配同根冻结')
        if (market.rule.device!=device or evaluation.state.device!=device
                or evaluation.policy_version.device!=device or cancels.values.device!=device
                or any(x.device!=device for x in (frozen.cutoff,frozen.identities,evaluation.ready,
                    evaluation.fault,market.role,market.fault,market.resources_fit,cancels.fault,
                    cancels.present,cancels.physical_slots))):
            raise TypeError('动作输入须在同设备发布')
        fault=actor.fault|evaluation.fault|market.fault|cancels.fault
        fault|=(market.endpoint_memory.previous_observation_ms!=actor.cutoff).to(torch.int32)
        fault|=(frozen.cutoff!=actor.cutoff).to(torch.int32)
        fault|=(frozen.identities!=actor.identities).any(1).to(torch.int32)
        fault|=(market.role==1).logical_xor(actor.decision_required).to(torch.int32)
        fault|=(cancels.physical_slots!=slots).any(1).to(torch.int32)
        # actor已拥有实际精确值；只有不可变规则共享，其余发布不再借下一暂存。
        context=type(actor)(**{name:(value if name=='rules' else _owned(value))
            for name,value in vars(actor).items()})
        return cls(context,evaluation.state.clone(),market.rule.clone(),cancels.snapshot(),
            evaluation.policy_version.clone(),(evaluation.ready&market.resources_fit).clone(),fault.clone())
