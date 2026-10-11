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
    request_identity: object = None  # 同一真实任务的RequestContext/稳定槽；started副本不再分配。


class ActorModelQueue:
    """同根全部GRU层与冻结队列；权重拥有者提供开始时的不可变版本。

    timing返回显式有限逻辑calculation/dispatch，不使用GPU墙钟。
    sender_factory将本任务已经绑定的评估/版本接原FrozenActorSend。
    """
    def __init__(self,hidden,identities,initial_completed_at,policy_at_start,timing,sender_factory,*,row_roots=None,market_ends=None,release_markets=None,request_identities=None,on_started=None,on_completed=None):
        self.hidden=hidden.clone();self.identities=identities.clone()
        self.last_completed_at=initial_completed_at.clone()
        self.policy_at_start=policy_at_start;self.timing=timing;self.sender_factory=sender_factory
        self.tasks=[]
        self.row_roots=torch.arange(len(identities),device=identities.device) if row_roots is None else row_roots
        self.market_ends=market_ends;self.release_markets=release_markets
        self.request_identities=request_identities
        self.on_started=on_started;self.on_completed=on_completed
        self.input_owner=None

    def bind_inputs(self, owner):
        self.input_owner=owner
        self.sync_inputs()

    def sync_inputs(self):
        """稳定业务身份到当前热行的机械选择；新市场只建立空GRU。"""
        owner=self.input_owner
        if owner is None:return
        identities=owner.identities
        hidden=self.hidden.new_zeros((self.hidden.size(0),len(identities),self.hidden.size(2)))
        if len(self.identities) and len(identities):
            matches=(identities[:,None]==self.identities[None]).all(2)
            selected=torch.nonzero(matches.any(1)).flatten()
            hidden[:,selected]=self.hidden[:,matches[selected].to(torch.int64).argmax(1)]
        self.hidden=hidden;self.identities=identities.clone();self.row_roots=owner.row_roots.clone()
        self.market_ends=owner.encoder.market.times[owner.market_indices,1]

    def _task_rows(self, observation):
        return self.row_roots if observation.row_roots is None else observation.row_roots

    def _hot_rows(self, observation):
        return (observation.frozen.identities[:,None]==self.identities[None]).all(2).to(torch.int64).argmax(1)

    def enqueue(self,observation,active):
        if observation is None:return
        calculation,dispatch=self.timing(observation)
        has_rows=torch.zeros_like(active,dtype=torch.int32).scatter_reduce(0,self._task_rows(observation),
            (observation.frozen.role>=0).to(torch.int32),reduce='amax').bool()
        accepted=active&has_rows
        if not bool(accepted.any().cpu()):return
        identity=None if self.request_identities is None else self.request_identities.bind(accepted)
        self.tasks.append(FrozenActorTask(observation,accepted.clone(),torch.zeros_like(active,dtype=torch.int64),
            calculation.clone(),dispatch.clone(),request_identity=identity))

    def next_event(self):
        at=torch.full_like(self.last_completed_at,torch.iinfo(torch.int64).max)
        kind=torch.zeros_like(at);task_index=torch.full_like(at,-1);present=torch.zeros_like(at,dtype=torch.bool)
        running=torch.zeros_like(present)
        for task in self.tasks:running |= task.active&(task.stage==1)
        for index,task in enumerate(self.tasks):
            row=task.observation.account
            waiting=task.active&(task.stage==0)&~running
            completing=task.active&(task.stage==1)
            sending=task.active&(task.stage==2)
            cutoff=row.cutoff if task.observation.root_cutoffs is None else task.observation.root_cutoffs
            candidate=torch.maximum(cutoff,self.last_completed_at)
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
            from .frozen_inputs import FrozenPolicyEvaluation
            frozen=task.observation.frozen;rows=self._task_rows(task.observation);positions=self._hot_rows(task.observation)
            state=torch.zeros((len(rows),self.hidden.size(2)),device=self.hidden.device)
            value=torch.zeros(len(rows),device=self.hidden.device)
            next_hidden=self.hidden[:,positions].clone();row_fault=torch.zeros_like(rows,dtype=torch.int32)
            row_ready=torch.ones_like(rows,dtype=torch.bool)
            # 独立市场批量递推；ContextOnly批先于DecisionRequired批。
            for role in (0,1):
                indices=torch.nonzero(active[rows]&(frozen.role==role)).flatten()
                if not indices.numel():continue
                with torch.no_grad():part=policy.frozen_state_and_value(frozen.rows(indices),
                    self.hidden[:,positions[indices]],frozen.identities[indices],version[rows[indices]])
                state[indices]=part.state;value[indices]=part.value;next_hidden[:,indices]=part.hidden
                row_fault[indices]=part.fault;row_ready[indices]=part.ready
            evaluation=FrozenPolicyEvaluation(state,value,next_hidden,version[rows].clone(),row_ready,row_fault)
            failure=torch.zeros_like(active,dtype=torch.int32).scatter_reduce(0,rows,
                ((~row_ready)|(row_fault!=0)).to(torch.int32),reduce='amax')
            fault |= torch.where(active,failure,0)
            accepted=active&(failure==0);ready |= accepted
            # 仅本次真实开始的根绑定这个版本，未开始根仍留原冻结队列。
            started=FrozenActorTask(task.observation,accepted,torch.ones_like(task.stage),
                task.calculation_duration,task.dispatch_delay,event.at.clone(),event.at+task.calculation_duration,
                event.at+task.calculation_duration+task.dispatch_delay,policy,evaluation,
                request_identity=task.request_identity)
            if bool(accepted.any().cpu()):
                self.tasks.append(started)
                if self.on_started is not None:self.on_started(started)
            task.stage=torch.where(accepted,3,task.stage)
        self._retire_completed()
        return ready,fault

    def finish(self,enabled):
        event=self.next_event()
        complete=torch.zeros_like(enabled)
        for index,task in enumerate(self.tasks):
            active=enabled&(event.task==index)&(event.kind==2)
            if not bool(active.any().cpu()):continue
            observation=task.observation;rows=self._task_rows(observation)
            actionable=torch.zeros_like(active).scatter(0,rows[observation.decision_rows],
                observation.frozen.role[observation.decision_rows]==1)
            accepted=active&~actionable
            if bool((active&actionable).any().cpu()):
                if task.sender is None:task.sender=self.make_sender(task)
                publication=task.sender.publication.publish(task.sender.driver,task.sender.control,active&actionable)
                accepted |= active&actionable&publication.published
                if publication.action is not None:
                    inactive=publication.action.family<1
                    task.sender.complete |= accepted&inactive
                    actionable &= ~inactive
            positions=self._hot_rows(observation)
            selected=torch.nonzero(accepted[rows]).flatten()
            self.hidden[:,positions[selected]]=task.evaluation.hidden[:,selected]
            self.last_completed_at=torch.where(accepted,event.at,self.last_completed_at)
            task.stage=torch.where(accepted,torch.where(actionable,2,3),task.stage)
            self._release_completed_capture(task)
            complete |= accepted
        self._retire_completed()
        return complete

    def send(self,clock,enabled,*,progress=None):
        event=self.next_event();complete=torch.zeros_like(enabled)
        fault=torch.zeros_like(enabled,dtype=torch.int32);blocked_advance=None
        for index,task in enumerate(self.tasks):
            active=enabled&(event.task==index)&(event.kind==3)
            if not bool(active.any().cpu()):continue
            if task.sender is None:task.sender=self.make_sender(task)
            result=task.sender.execute(clock,active,progress=progress)
            complete |= active&result.complete
            if result.advance is not None:
                fault |= torch.where(active,result.advance.fault,0)
                if bool((active&~result.advance.complete).any().cpu()):blocked_advance=result.advance
            task.stage=torch.where(active&result.complete,3,task.stage)
            self._release_completed_capture(task)
        self._retire_completed()
        return complete,fault,blocked_advance

    def _retire_completed(self):
        # 当前方法已用完事件下标；稳定保留未开始/续接/待发送对象。
        pending=[]
        for task in self.tasks:
            if bool(((task.stage==3)|~task.active).all().cpu()):
                # queued被started承接只是转移拥有，不再交付一次业务完成。
                if task.evaluation is not None and self.on_completed is not None:self.on_completed(task)
            else:pending.append(task)
        self.tasks=pending

    @staticmethod
    def _release_completed_capture(task):
        # 原stage3表示该根已完成；其它根的续接/待发送仍持有本次捕获。
        if task.sender is not None and bool(((task.stage==3)|~task.active).all().cpu()):
            task.sender.driver.release_capture()

    def make_sender(self, task):
        observation=task.observation.actor_view();indices=task.observation.decision_rows
        evaluation=replace(task.evaluation,state=task.evaluation.state[indices],
            value=task.evaluation.value[indices],hidden=task.evaluation.hidden[:,indices],
            policy_version=task.evaluation.policy_version[indices],ready=task.evaluation.ready[indices],
            fault=task.evaluation.fault[indices])
        return self.sender_factory(replace(task,observation=observation,evaluation=evaluation))

    def retire_consumed_markets(self,observed_at):
        if self.market_ends is None:return
        ended=self.market_ends<=observed_at[self.row_roots]
        pending=torch.zeros_like(ended)
        for task in self.tasks:
            rows=self._task_rows(task.observation)
            needed=(task.active&(task.stage<2))[rows]&(task.observation.frozen.role>=0)
            identities=task.observation.frozen.identities[needed]
            pending |= (self.identities[:,None]==identities[None]).all(2).any(1)
        released=ended&~pending
        if not bool(released.any().cpu()):return
        keep=torch.nonzero(~released).flatten()
        if self.release_markets is not None:self.release_markets(released)
        self.hidden=self.hidden[:,keep];self.identities=self.identities[keep]
        self.row_roots=self.row_roots[keep];self.market_ends=self.market_ends[keep]
