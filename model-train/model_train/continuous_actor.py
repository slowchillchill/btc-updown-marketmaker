"""冻结观察的模型FIFO；待发送动作按原实际时刻选择，不施加发送FIFO。"""
from dataclasses import dataclass,replace
import torch


@dataclass(frozen=True)
class ModelEvent:
    at: torch.Tensor
    kind: torch.Tensor  # 1开始/2完成/3发送。
    present: torch.Tensor
    task: torch.Tensor


@dataclass
class FrozenActorTask:
    observation: object  # FrozenPolicyInputs100ms及同Ck原Actor/市场/取消材料的拥有者。
    active: torch.Tensor
    stage: torch.Tensor  # 0排队/1计算中/2已完成待发送/3完成。
    calculation_duration: torch.Tensor
    dispatch_delay: torch.Tensor
    started_at: torch.Tensor | None = None
    completed_at: torch.Tensor | None = None
    send_at: torch.Tensor | None = None
    policy: object = None
    evaluation: object = None
    sender: object = None


class ActorModelQueue:
    """同根全部GRU层与冻结队列；权重拥有者提供开始时的不可变版本。

    timing返回显式有限逻辑calculation/dispatch，不使用GPU墙钟。
    sender_factory将本任务已经绑定的评估/版本接原FrozenActorSend。
    """
    def __init__(self,hidden,identities,initial_completed_at,policy_at_start,timing,sender_factory):
        self.hidden=hidden.clone();self.identities=identities.clone()
        self.last_completed_at=initial_completed_at.clone()
        self.policy_at_start=policy_at_start;self.timing=timing;self.sender_factory=sender_factory
        self.tasks=[]

    def enqueue(self,observation,active):
        calculation,dispatch=self.timing(observation)
        self.tasks.append(FrozenActorTask(observation,(active&(observation.frozen.role>=0)).clone(),torch.zeros_like(active,dtype=torch.int64),
            calculation.clone(),dispatch.clone()))

    def next_event(self):
        at=torch.full_like(self.last_completed_at,torch.iinfo(torch.int64).max)
        kind=torch.zeros_like(at);task_index=torch.full_like(at,-1);present=torch.zeros_like(at,dtype=torch.bool)
        running=torch.zeros_like(present)
        for task in self.tasks:running |= task.active&(task.stage==1)
        for index,task in enumerate(self.tasks):
            row=task.observation.frozen
            waiting=task.active&(task.stage==0)&~running
            completing=task.active&(task.stage==1)
            sending=task.active&(task.stage==2)
            candidate=torch.maximum(row.cutoff,self.last_completed_at)
            if task.completed_at is not None:candidate=torch.where(completing,task.completed_at,candidate)
            if task.send_at is not None:candidate=torch.where(sending,task.send_at,candidate)
            k=torch.where(completing,2,torch.where(sending,3,1))
            valid=waiting|completing|sending
            # 模型同刻先完成再开始；同类同刻保持原观察序。发送只按实际时间。
            select=valid&(~present|(candidate<at)|((candidate==at)&(k==2)&(kind==1)))
            at=torch.where(select,candidate,at);kind=torch.where(select,k,kind)
            task_index=torch.where(select,index,task_index);present |= valid
        return ModelEvent(at,kind,present,task_index)

    def start(self,enabled):
        event=self.next_event()
        ready=torch.zeros_like(enabled);fault=torch.zeros_like(enabled,dtype=torch.int32)
        for index,task in enumerate(tuple(self.tasks)):
            active=enabled&(event.task==index)&(event.kind==1)
            if not bool(active.any().cpu()):continue
            policy,version=self.policy_at_start(task.observation,active)
            with torch.no_grad():evaluation=policy.frozen_state_and_value(task.observation.frozen,self.hidden,self.identities,version)
            fault |= torch.where(active,evaluation.fault,0)
            accepted=active&evaluation.ready&(evaluation.fault==0);ready |= accepted
            # 仅本次真实开始的根绑定这个版本，未开始根仍留原冻结队列。
            started=FrozenActorTask(task.observation,accepted,torch.ones_like(task.stage),
                task.calculation_duration,task.dispatch_delay,event.at.clone(),event.at+task.calculation_duration,
                event.at+task.calculation_duration+task.dispatch_delay,policy,evaluation)
            self.tasks.append(started)
            task.stage=torch.where(accepted,3,task.stage)
        return ready,fault

    def finish(self,enabled):
        event=self.next_event()
        complete=torch.zeros_like(enabled)
        for index,task in enumerate(self.tasks):
            active=enabled&(event.task==index)&(event.kind==2)
            if not bool(active.any().cpu()):continue
            actionable=task.observation.frozen.role==1
            accepted=active&~actionable
            if bool((active&actionable).any().cpu()):
                if task.sender is None:task.sender=self.sender_factory(task)
                publication=task.sender.publication.publish(task.sender.driver,task.sender.control,active&actionable)
                accepted |= active&actionable&publication.published
                if publication.action is not None:
                    inactive=publication.action.family<1
                    task.sender.complete |= accepted&inactive
                    actionable &= ~inactive
            self.hidden=torch.where(accepted[None,:,None],task.evaluation.hidden,self.hidden)
            self.last_completed_at=torch.where(accepted,event.at,self.last_completed_at)
            task.stage=torch.where(accepted,torch.where(actionable,2,3),task.stage)
            complete |= accepted
        return complete

    def send(self,clock,enabled,*,progress=None):
        event=self.next_event();complete=torch.zeros_like(enabled)
        fault=torch.zeros_like(enabled,dtype=torch.int32);blocked_advance=None
        for index,task in enumerate(self.tasks):
            active=enabled&(event.task==index)&(event.kind==3)
            if not bool(active.any().cpu()):continue
            if task.sender is None:task.sender=self.sender_factory(task)
            result=task.sender.execute(clock,active,progress=progress)
            complete |= active&result.complete
            if result.advance is not None:
                fault |= torch.where(active,result.advance.fault,0)
                if bool((active&~result.advance.complete).any().cpu()):blocked_advance=result.advance
            task.stage=torch.where(active&result.complete,3,task.stage)
        return complete,fault,blocked_advance
