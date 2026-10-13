"""单账户Root当前有界批的真实消费者；采集顺序、联合路径及活动hidden分别拥有。"""
from dataclasses import dataclass,replace
import torch
from tensordict import TensorDict
from tensordict.nn import TensorDictModule
from torchrl.objectives import ClipPPOLoss
from .gpu_policy import finite_value_context
from .interval_reward import interval_between
from .propagation import frozen_origin,prepare_labels
from .gpu_exact import run_exact_batch
from .dt_advantages import dt_advantages,normalize_advantages
from .action_replay import ReplaySchedule,replay_action,entropy_sample_objective


@dataclass(frozen=True)
class LogicalBatchConfig:
    decisions: int
    tbptt: int
    burn_in: int
    epochs: int
    clip_epsilon: float
    critic_coefficient: float
    entropy_coefficient: float = 0.


@dataclass
class CollectedState:
    frozen: object
    endpoint: object
    h_in: torch.Tensor
    value: torch.Tensor
    version: torch.Tensor
    started_at: torch.Tensor
    decision_rows: torch.Tensor
    propagation_origin: object = None
    inputs: object = None
    path: object = None
    published_at: torch.Tensor | None = None
    published: bool = False


@dataclass(frozen=True)
class LogicalUpdate:
    ready: bool
    updated: bool
    decisions: int
    version: int
    loss: torch.Tensor | None
    target_end: bool
    bootstrap: torch.Tensor | None


class JointPathPPOLoss(ClipPPOLoss):
    """原ClipPPOLoss只消费完整路径的一项联合ell；不复制裁剪或价值损失。"""
    def _get_cur_log_prob(self,tensordict):
        return tensordict['new_joint_log_prob'],None,False


@dataclass(frozen=True)
class PreparedLogicalBatch:
    decisions: list
    old: torch.Tensor
    bootstrap: torch.Tensor
    blocks: list
    statistics: object
    schedules: list
    target_end: bool
    propagation: object = None
    report_reward: dict | None = None


class LogicalBatch:
    """一个真实账户的采集/准备；参数和更新只归共同学习器。"""
    def __init__(self,root,policy,optimizer,config,version,*,learner=None):
        self.root=root;self.config=config;self.states=[]
        self._started={};self.pending_count=0;self.prepared=None;self.ended=False
        self.on_replayed=None
        self.learner=learner if learner is not None else JointLearner(policy,optimizer,config,version)
        self.learner.roots.append(self)
        self.entropy_random=(replace(root.random.snapshot(),seed=root.random.seed^0x1810336)
            if self.learner.config.entropy_coefficient else None)

    @property
    def policy(self):return self.learner.policy
    @property
    def optimizer(self):return self.learner.optimizer
    @property
    def version(self):return self.learner.version

    def policy_at_start(self,observation,active):
        return self.policy,self.version.clone()

    @staticmethod
    def _decision_rows(task):
        # 账户代表仍由原输入/Actor拥有；仅其真实DecisionRequired发布路径参与损失。
        rows=task.observation.decision_rows
        return rows[task.observation.frozen.role[rows]==1]

    def started(self,task,h_in):
        # 原装配目前是单账户；保存的行身份仍为完整根/账户/市场键。
        record=CollectedState(task.observation.frozen,task.observation.reward_endpoint,h_in,
            None if task.evaluation.value is None else task.evaluation.value.detach().clone(),
            task.evaluation.policy_version.clone(),task.started_at.clone(),self._decision_rows(task))
        if self.policy.propagation is not None and record.decision_rows.numel():
            record.propagation_origin=frozen_origin(self.root,task,record.decision_rows)
        self.states.append(record);self._started[id(task)]=record
        if record.decision_rows.numel():
            self.pending_count+=1
            if self.pending_count>=self.config.decisions:self.root.models.start_paused.fill_(True)

    def published(self,task,accepted):
        record=self._started.get(id(task))
        if record is None or record.published:return
        if record.decision_rows.numel():
            if task.sender is None or task.sender.publication.candidate is None:return
            record.inputs=task.sender.driver.inputs
            record.path=task.sender.publication.candidate
        record.published_at=task.completed_at.clone();record.published=True
        # 不保已完成调度对象；在途发送仍由原队列/发送器维护。
        del self._started[id(task)]

    def _successor(self):
        for task,active,_ in self.root.models.eligible_starts():
            rows=self.root.models._task_rows(task.observation)
            selected=self._decision_rows(task)
            if bool(active[rows[selected]].any().cpu()):return task
        return None

    def boundary(self):
        running=any(bool((task.active&(task.stage==1)).any().cpu()) for task in self.root.models.tasks)
        return self.pending_count>=self.config.decisions and not running and self._successor() is not None

    @torch.no_grad()
    def bootstrap(self):
        """按旧版本和稳定身份递推未开始ContextOnly；不提交活动h或调用Actor。"""
        models=self.root.models
        identities=models.identities.clone();hidden=models.hidden.clone()
        for task,active,_ in models.eligible_starts():
            frozen=task.observation.frozen
            positions=(frozen.identities[:,None]==identities[None]).all(2).to(torch.int64).argmax(1)
            rows=models._task_rows(task.observation)
            context=finite_value_context(frozen.cutoff,self.root.goal_until[rows],self.root.trading_until[rows])
            evaluation=self.policy.frozen_state_and_value(frozen,hidden[:,positions],frozen.identities,
                self.version[rows],value_context=context)
            advancing=active[rows]&(frozen.role>=0)
            hidden[:,positions[advancing]]=evaluation.hidden[:,advancing]
            selected=self._decision_rows(task)
            selected=selected[active[rows[selected]]]
            if selected.numel():
                return task.observation.reward_endpoint,evaluation.value[selected],evaluation.ready[selected],evaluation.fault[selected]
        return None

    def _unroll(self,records):
        """临时批张量按稳定市场身份装列；缺市场是padding，ContextOnly仍递推。"""
        all_records=records
        keys=torch.unique(torch.cat([s.frozen.identities for s in all_records]),dim=0)
        layers,_,width=all_records[0].h_in.shape
        device=keys.device
        initial=torch.zeros((layers,len(keys),width),device=device)
        seen=torch.zeros(len(keys),dtype=torch.bool,device=device)
        for record in all_records:
            locations=(record.frozen.identities[:,None]==keys[None]).all(2).to(torch.int64).argmax(1)
            new=~seen[locations];initial[:,locations[new]]=record.h_in[:,new];seen[locations]=True
        states=[];values=[]
        for begin in range(0,len(records),self.config.tbptt):
            chunk=records[begin:begin+self.config.tbptt]
            prefix=records[:begin][-self.config.burn_in:] if self.config.burn_in else []
            sequence=prefix+chunk
            # 当前截断的真实h_in；burn-in如存在，则从其真实全层入口重算。
            h=initial.clone();seen.zero_()
            for sample in sequence:
                loc=(sample.frozen.identities[:,None]==keys[None]).all(2).to(torch.int64).argmax(1)
                new=~seen[loc];h[:,loc[new]]=sample.h_in[:,new];seen[loc]=True
            depth=max(s.frozen.book_levels.shape[2] for s in sequence)
            fast=sequence[0].frozen.fast.new_zeros((len(sequence),len(keys),626))
            slow=fast.new_zeros((len(sequence),len(keys),433))
            books=fast.new_zeros((len(sequence),len(keys),6,depth,9))
            masks=torch.zeros(books.shape[:-1],dtype=torch.bool,device=device)
            advance=torch.zeros((len(sequence),len(keys)),dtype=torch.bool,device=device)
            context=fast.new_zeros((len(sequence),len(keys),2));locations=[]
            for i,sample in enumerate(sequence):
                frozen=sample.frozen
                at=(frozen.identities[:,None]==keys[None]).all(2).to(torch.int64).argmax(1);locations.append(at)
                fast[i,at]=frozen.fast;slow[i,at]=frozen.slow
                books[i,at,:,:frozen.book_levels.shape[2]]=frozen.book_levels
                masks[i,at,:,:frozen.book_mask.shape[2]]=frozen.book_mask
                advance[i,at]=frozen.role>=0
                context[i,at]=finite_value_context(frozen.cutoff,self.root.goal_until.expand(len(at)),
                    self.root.trading_until.expand(len(at)))
            n=len(prefix);columns=(fast,slow,books,masks)
            z,v,_=self.policy.learning_unroll(tuple(x[:n] for x in columns),advance[:n],
                tuple(x[n:] for x in columns),advance[n:],h,value_context=context[n:])
            for i,sample in enumerate(chunk):
                states.append(z[i,locations[n+i]]);values.append(v[i,locations[n+i]])
        return states,values

    def prepare(self,*,target_end=False,endpoint=None):
        decisions=[s for s in self.states if s.decision_rows.numel()]
        if not decisions or any(not s.published for s in self.states):
            return None
        successor=None if target_end else self.bootstrap()
        if not target_end and successor is None:
            return None
        end=endpoint if target_end else successor[0]
        rewards=[run_exact_batch(lambda s=s,r=r:interval_between(s.endpoint,r,self.root.reward_a_pusd,self.root.yes))
            for s,r in zip(decisions,[s.endpoint for s in decisions[1:]]+[end],strict=True)]
        old=torch.stack([s.value[s.decision_rows] for s in decisions],1).unsqueeze(-1)
        bootstrap=torch.zeros_like(old[:,0]) if target_end else successor[1][:,None]
        following=torch.cat((old[:,1:],bootstrap[:,None]),1)
        reward=torch.stack([r.value for r in rewards],1).unsqueeze(-1).to(old.dtype)
        dt=torch.stack([r.dt_seconds for r in rewards],1).unsqueeze(-1).to(old.dtype)
        known=torch.stack([r.known for r in rewards],1).unsqueeze(-1)
        faults=torch.stack([r.fault for r in rewards],1).unsqueeze(-1)
        yes=torch.ones_like(old,dtype=torch.bool);no=torch.zeros_like(yes)
        end_mask=no.clone();end_mask[:,-1]=target_end
        bootstrap_known=yes.clone()
        if successor is not None:
            bootstrap_known[:,-1]=successor[2][:,None]
            faults[:,-1]|=successor[3][:,None]
        blocks=[];continuation=None
        for finish in range(old.shape[1],0,-self.config.tbptt):
            begin=max(0,finish-self.config.tbptt);slice_=slice(begin,finish)
            block=dt_advantages(old[:,slice_],following[:,slice_],reward[:,slice_],dt[:,slice_],no[:,slice_],
                no[:,slice_],yes[:,slice_],known[:,slice_],bootstrap_known[:,slice_],faults[:,slice_],
                target_end=end_mask[:,slice_],continuation=continuation)
            blocks.append(block);continuation=block.continuation
        blocks.reverse();statistics=blocks[0].statistics
        for block in blocks[1:]:statistics=statistics.merge(block.statistics)
        report_reward=None
        if self.learner.reporter is not None:
            from .training_report import reward_summary
            report_reward=reward_summary(rewards)
        self.prepared=PreparedLogicalBatch(decisions,old,bootstrap.detach(),blocks,statistics,
            [ReplaySchedule.for_update(s.path) for s in decisions],target_end,
            None if self.policy.propagation is None else prepare_labels(self.root,
                [s.propagation_origin for s in decisions],self.policy.propagation["horizon_ms"]),report_reward)
        return self.prepared

    def _replay(self,statistics):
        prepared=self.prepared;decisions=prepared.decisions;old=prepared.old
        advantages=torch.cat([normalize_advantages(b,statistics) for b in prepared.blocks],1).reshape(-1,1)
        targets=torch.cat([b.value_target for b in prepared.blocks],1).reshape(-1,1).detach()
        states,values=self._unroll(self.states);ells=[];new_values=[];entropy=[];predictions=[]
        entropy_random=self.entropy_random
        for i,sample in enumerate(self.states):
            if not sample.decision_rows.numel():continue
            if self.policy.propagation is not None:
                predictions.append(self.policy.propagation_head(states[i][sample.decision_rows]))
            at=len(ells);inputs=replace(sample.inputs,state=states[i][sample.decision_rows])
            replay=replay_action(self.policy,inputs,sample.path,prepared.schedules[at],word_capacity=self.root.word_capacity)
            if not bool((replay.fits&(replay.fault==0).all()).cpu()):return None
            ells.append(replay.log_prob);new_values.append(values[i][sample.decision_rows])
            if self.learner.config.entropy_coefficient:
                from .device_actor import CapturedDeviceActor
                driver=CapturedDeviceActor(self.policy,inputs,entropy_random,
                    word_capacity=self.root.word_capacity,path_capacity=self.root.path_capacity)
                path=driver.replay().snapshot();driver.release_capture()
                entropy_random=path.random.snapshot()
                scored=replay_action(self.policy,inputs,path,ReplaySchedule.for_update(path),word_capacity=self.root.word_capacity)
                if not bool((scored.fits&(scored.fault==0).all()).cpu()):return None
                _,surrogate=entropy_sample_objective(scored,target_scale=self.learner.config.entropy_coefficient)
                entropy.append(surrogate)
        data=TensorDict({'new_joint_log_prob':torch.cat(ells),'old_joint_log_prob':torch.cat(
            [s.path.log_prob.detach() for s in decisions]),'recomputed_value':torch.cat(new_values)[:,None],
            'state_value':old.reshape(-1,1).detach(),'advantage':advantages,'value_target':targets},
            batch_size=[len(advantages)],device=old.device)
        if prepared.propagation is not None:
            data["propagation_prediction"]=torch.cat(predictions)
            data["propagation_label"]=prepared.propagation.value
            data["propagation_valid"]=prepared.propagation.valid
        return data,entropy,entropy_random

    def _consume(self):
        self.ended=self.prepared.target_end
        self.prepared=None;self.states=[];self._started.clear();self.pending_count=0
        # 已结束根不恢复start，正常根仍沿原冻结网格续采。
        if not self.ended:self.root.models.start_paused.zero_()

    def update(self,*,target_end=False,endpoint=None):
        if not self.ended:self.prepare(target_end=target_end,endpoint=endpoint)
        results=self.learner.update()
        return results.get(self,LogicalUpdate(False,False,0,int(self.version[0]),None,target_end,None))


class JointLearner:
    """全部参与根准备后，共同统计/共同step；唯一参数、版本和消费位置。"""
    def __init__(self,policy,optimizer,config,version,*,reporter=None):
        self.policy=policy;self.optimizer=optimizer;self.config=config;self.version=version.clone()
        self.roots=[];self.consumed_batches=0;self.on_replayed=None;self.on_prepared=None;self.reporter=reporter
        actor=TensorDictModule(torch.nn.Identity(),in_keys=['new_joint_log_prob'],out_keys=['new_joint_log_prob'])
        critic=TensorDictModule(torch.nn.Identity(),in_keys=['recomputed_value'],out_keys=['state_value'])
        self.loss=JointPathPPOLoss(actor,critic,clip_epsilon=config.clip_epsilon,
            entropy_bonus=False,critic_coeff=config.critic_coefficient,functional=False,
            normalize_advantage=False,device=version.device)
        self.loss.set_keys(sample_log_prob='old_joint_log_prob')

    def add_root(self,root,config):
        return root.join_learning(self,config)

    def update(self):
        active=[r for r in self.roots if not r.ended]
        if not active:return {}
        def refusal():
            return {r:LogicalUpdate(False,False,r.pending_count,int(self.version[0]),None,
                False if r.prepared is None else r.prepared.target_end,
                None if r.prepared is None else r.prepared.bootstrap) for r in active}
        if any(r.prepared is None for r in active):return refusal()
        statistics=active[0].prepared.statistics
        for root in active[1:]:statistics=statistics.merge(root.prepared.statistics)
        if not bool(statistics.ready.cpu()):return refusal()
        if self.on_prepared is not None:self.on_prepared(active,statistics)
        import time
        started=time.perf_counter()
        root_reports=None if self.reporter is None else [dict(
            root_index=self.roots.index(r),account=r.root.runtime.authority['ledger_projection']['account_instance_key'],
            decisions=len(r.prepared.decisions),target_end=r.prepared.target_end,
            reward=r.prepared.report_reward,config=vars(r.config),economics=r.root.report_economics()) for r in active]
        previous_mode=self.policy.training;self.policy.train();steps=0;final_loss=None
        epoch_reports=[]
        try:
            for epoch in range(self.config.epochs):
                computed=[]
                for root in active:
                    result=root._replay(statistics)
                    if result is None:
                        if steps:raise RuntimeError('共同批在已step后路径失效；保留真实权重及未完成批，不可当作未更新重试')
                        return refusal()
                    computed.append(result)
                # 每根同theta重算完成后才构成全体有效决定人口的一次loss。
                data=torch.cat([x[0] for x in computed],0)
                for root,result in zip(active,computed,strict=True):
                    if root.on_replayed is not None:root.on_replayed(result[0],epoch)
                if self.on_replayed is not None:self.on_replayed(data,epoch)
                losses=self.loss(data);final_loss=sum(v for k,v in losses.items() if k.startswith('loss_'))
                aux_count=0;aux_masked=0;aux_error=None;aux_contribution=final_loss.new_zeros(())
                if self.policy.propagation is not None:
                    mask=data["propagation_valid"]
                    # 全根逐腿总数；零合法标签没有辅助贡献，不把未观测预测误差报告为零。
                    delta=torch.where(mask,data["propagation_prediction"]-data["propagation_label"],0.)
                    propagation_loss=delta.square().sum()/mask.sum().clamp_min(1)
                    aux_contribution=self.policy.propagation["eta"]*propagation_loss
                    final_loss=final_loss+aux_contribution
                    if self.reporter is not None:
                        aux_count=int(mask.sum().detach().cpu());aux_masked=mask.numel()-aux_count
                        aux_error=float(delta.square().sum().detach().cpu())
                entropy=[v for _,items,_ in computed for v in items]
                if entropy:final_loss=final_loss+torch.cat(entropy).mean()
                if self.reporter is not None:
                    loss_names=[k for k in losses.keys() if k.startswith('loss_')]
                    scalars=torch.stack([losses[k].detach() for k in loss_names]+[final_loss.detach(),aux_contribution.detach()]).cpu().tolist()
                    epoch_reports.append(dict(epoch=epoch,losses=dict(zip(loss_names,scalars[:-2],strict=True)),
                        total=scalars[-2],aux_contribution=scalars[-1],aux_error_sum=aux_error,
                        aux_valid=aux_count,aux_masked=aux_masked))
                self.optimizer.zero_grad(set_to_none=True);final_loss.backward();self.optimizer.step();steps+=1
                for root,result in zip(active,computed,strict=True):root.entropy_random=result[2]
        finally:self.policy.train(previous_mode)
        self.version+=1;self.consumed_batches+=1
        results={r:LogicalUpdate(True,True,len(r.prepared.decisions),int(self.version[0]),
            final_loss.detach(),r.prepared.target_end,r.prepared.bootstrap) for r in active}
        report=None
        if self.reporter is not None:
            from .training_report import training_configuration
            report=dict(training_configuration=training_configuration(self),version=int(self.version[0]),decisions=int(statistics.count),roots=len(active),
                optimizer_steps=steps,loss_scope='arithmetic mean of actual epoch losses before each step; no final-weight reevaluation',
                epochs=epoch_reports,config=vars(self.config),update_seconds=time.perf_counter()-started,
                device_peak_allocated_bytes=torch.cuda.max_memory_allocated(self.version.device),
                peak_scope='current process CUDA allocator since last external reset; not exclusive/shared-device peak',
                roots_report=root_reports)
        for root in active:root._consume()
        if report is not None:
            last=epoch_reports[-1]
            metrics=dict(policy_version=report['version'],decisions=report['decisions'],roots=report['roots'],
                optimizer_steps=steps,target_end_roots=sum(r['target_end'] for r in report['roots_report']),
                update_seconds=report['update_seconds'],device_peak_allocated_bytes=report['device_peak_allocated_bytes'],loss_total=sum(e['total'] for e in epoch_reports)/steps,
                loss_final_epoch=last['total'],aux_valid=last['aux_valid'],aux_masked=last['aux_masked'],
                aux_mse=None if not last['aux_valid'] else last['aux_error_sum']/last['aux_valid'],
                aux_error_sum=last['aux_error_sum'],aux_contribution=last['aux_contribution'])
            for key in last['losses']:metrics[key]=sum(e['losses'][key] for e in epoch_reports)/steps
            metrics['reward_score_sum']=sum(sum(r['reward']['projected_sum']) for r in report['roots_report'])
            metrics['reward_duration_seconds_sum']=sum(sum(r['reward']['duration_seconds']) for r in report['roots_report'])
            from .training_report import economic_metrics
            metrics.update(economic_metrics(report['roots_report']))
            from .training_report import ReportWriteError
            try:self.reporter.publish('update',self.consumed_batches,metrics,report)
            except ReportWriteError as error:
                error.update_results=results
                raise
        return results
