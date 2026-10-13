"""已冷开户单账户的连续模型装配；策略、逻辑时延和边界由调用方供给。"""
from dataclasses import replace
import torch
from actor_send import ActorRequestSubmission,IntentRequestIdentities,FrozenActorSend,exact_values
from task_requests import ActorRequestDirectory,ActorTaskRequests
from continuous_actor import ContinuousActorRun
from input_history import SourceHistoryInputs
from input_100ms import MarketInputs100ms
from input_account import account_inputs_at_prefix
from input_cancel import cancel_inputs
from actor_context import publish_actor_context
from cancel_objects import CancelObjectDirectory
from cancel_requests import CancellationScopes,freeze_cancel_snapshot,freeze_scoped_cancel_requests
from portfolio_nav import PortfolioValuation
from .continuous_inputs import ContinuousActorInputs
from .continuous_actor import ActorModelQueue
from .action_inputs import ActorPolicyInputs
from .device_actor import CapturedDeviceActor,ActorSelection
from .gpu_exact import run_exact_batch
from .interval_reward import RewardIntervalMemory
from portfolio_economics import read_portfolio_economics
from exposure import read_portfolio_exposure


class ContinuousActorRoot:
    def __init__(self,runtime,codec,rule,sampler,*,started_at,first_observation,trading_until,goal_until,
            request_namespace,policy_at_start,timing,random,model_shape,word_capacity,path_capacity,reward_a_pusd=None):
        self.runtime=runtime;self.input_requirement=None
        self.reward_a_pusd=reward_a_pusd;self.reward_memory=None
        self.trading_until=trading_until;self.goal_until=goal_until;self.learning=None
        self.clock=runtime.clock;self.directory=runtime.directory;self.population=runtime.population
        self.receiver=runtime.receiver;self.codec=codec;self.rule=runtime.execution.rule if runtime.execution is not None else rule;self.sampler=sampler
        rule=self.rule
        self.initial_observation=first_observation;self.initialized=False
        self.started_at=started_at;self.random=random;self.word_capacity=word_capacity;self.path_capacity=path_capacity
        device=started_at.device
        self.tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device=device)
        self.zero=torch.zeros_like(started_at);self.yes=torch.ones_like(started_at,dtype=torch.bool);self.empty=self.tensor([[]])
        assets=self.clock.source.prepared.columns['assets']
        self.outcomes=self.tensor([next(a['outcome']['index'] for a in assets if a['token_id']==token) for token in self.directory.tokens])
        markets=self.clock.source.prepared.columns['markets']
        self.condition_slots=self.tensor([self.directory.conditions.index(m['condition_id']) for m in markets])
        self.market_ends=tuple(markets[i]['window']['end_at_utc_ms'] for i in self.directory.market_indices.cpu().tolist())
        self.request_directory=ActorRequestDirectory(device)
        self.task_requests=ActorTaskRequests(self.request_directory,(request_namespace,))
        page=self.clock.source.initial_input_pages[0] if self.clock.source.initial_input_pages else None
        pm,market,reference,binance=page.install() if page is not None else self.clock.source.states
        columns=page.columns if page is not None else self.clock.source.current_columns
        history=SourceHistoryInputs(pm,market,reference,columns)
        encoder=MarketInputs100ms(pm,market,reference,binance,history,rule,columns,pm.causal_visible_at)
        initial_prefix=self.clock.source.initial_prefix_at(pm,first_observation-100)
        identities=self.tensor([[0,0,i] for i in range(len(self.directory.conditions))])
        roots=self.tensor([0]*len(identities))
        self.inputs=ContinuousActorInputs(encoder,history,self.directory.market_indices,started_at,identities,self.account_builder,
            word_capacity=word_capacity,row_roots=roots,root_prefix=initial_prefix,root_previous_at=first_observation-100)
        self.models=ActorModelQueue(torch.zeros((model_shape.layers,0,model_shape.hidden),device=device),identities[:0],
            first_observation,policy_at_start,timing,self.sender_factory,row_roots=roots[:0],market_ends=market.times[:0,1],
            release_markets=self.inputs.release_markets,request_identities=self.task_requests,trading_until=trading_until,goal_until=goal_until)
        self.run=ContinuousActorRun(self.clock,first_observation,self.models,self.freeze,self.receive,
            trading_until=trading_until,inputs=self.inputs,initialize=self.initialize)

    def initialize(self,horizon,enabled,*,progress=None):
        """首窗前沿由原时钟实际消费；未完成返回原需求，不能以旧组补首窗历史。"""
        if self.initialized:return None
        baseline=self.initial_observation-100
        at=torch.minimum(baseline,horizon)
        gates=self.clock.gates
        existing=gates.has_public_visible&(gates.public_visible_at>=at)
        stage=self.clock.advance_before(at,enabled&~existing,progress=progress)
        stage=replace(stage,complete=stage.complete|(enabled&existing))
        completed=enabled&stage.complete&(stage.fault==0)&(at==baseline)
        prefix=self.clock.source.initial_prefix_at(self.inputs.encoder.shared,at)
        self.inputs.previous_prefix=torch.where(completed,prefix,self.inputs.previous_prefix)
        self.initialized=bool(completed.all().cpu())
        return stage

    def reward_reads(self,public):
        """同一已接纳Ledger前缀的原WCM/E读面；不另归约经济事实。"""
        portfolio=read_portfolio_economics(public.ledger,**{name:self.yes for name in (
            'economic_prefix_known','success_prefix_known','settlement_prefix_known',
            'rebate_prefix_known','economic_liability_prefix_known')})
        exposure=read_portfolio_exposure(public.ledger,success_prefix_known=self.yes,settlement_prefix_known=self.yes)
        return portfolio,exposure

    def report_economics(self):
        """同一已提交前缀的纯读小报告；不推进时钟或领取NOE。"""
        from portfolio_nav import read_portfolio_nav
        from .training_report import exact_report
        at=self.clock.gates.public_visible_at
        def read():
            ledger=self.clock.public.ledger
            portfolio,exposure=self.reward_reads(self.clock.public)
            pm=self.inputs.encoder.shared
            valuation=PortfolioValuation(pm,self.directory.tokens,ledger.success_pools.asset_roots)
            quoted=valuation.freeze(pm.prefix_at(at),at)
            nav=read_portfolio_nav(ledger,quoted,at,portfolio=portfolio,
                economic_prefix_known=self.yes,success_prefix_known=self.yes,settlement_prefix_known=self.yes,
                rebate_prefix_known=self.yes,economic_liability_prefix_known=self.yes)
            return portfolio,exposure,nav,ledger.funds()
        portfolio,exposure,nav,funds=run_exact_batch(read)
        return dict(at_ms=at.cpu().tolist(),ledger_prefix=exact_values(self.clock.public.ledger.current_entry_sequence),
            local_prefix=exact_values(self.clock.public.ledger.current_local_prefix),
            platform_prefix=exact_values(self.clock.public.ledger.platform.platform_source_prefix),
            unit='pUSD',wcm=exact_report(portfolio.amounts[0]),wcm_known=portfolio.known[:,0].cpu().tolist(),
            exposure=exact_report(exposure.amount),exposure_known=exposure.known.cpu().tolist(),
            actual_available=exact_report(funds.actual_available),actual_reserved=exact_report(funds.actual_reserved),
            nav_conservative=exact_report(nav.conservative),nav_directional=exact_report(nav.directional),
            nav_known=nav.known.cpu().tolist(),fault=(portfolio.fault|exposure.fault|nav.fault|funds.fault).cpu().tolist(),
            nav_scope='original full-account valuation at the same committed public prefix; cash is not profit')

    def receive(self,public,delivery,enabled):
        # 原receiver只调用一次。资源不足仍可能带回已成功提交的前缀。
        public,ready=self.receiver(public,delivery,enabled)
        if self.reward_memory is not None:
            def observe():
                exposure=read_portfolio_exposure(public.ledger,success_prefix_known=self.yes,settlement_prefix_known=self.yes)
                return self.reward_memory.observe(self.clock.gates.public_visible_at,exposure,enabled)
            self.reward_memory=run_exact_batch(observe)
        return public,ready

    def read_interval_reward(self,cutoff,enabled,*,progress=None):
        """推进开放端点并真实领取，纯读起点至端点累计训练分；不重置区间。"""
        advance=self.run.advance_before(cutoff,enabled,progress=progress)
        if not bool((~enabled|advance.complete).all().cpu()):return advance,None
        delivery=self.clock.receive_first(enabled=enabled)
        self.clock.public,ready=self.receive(self.clock.public,delivery,enabled)
        fault=advance.fault|delivery.fault|self.clock.public.ledger.fault|self.clock.public.waiting.fault
        advance=replace(advance,complete=advance.complete&ready,blocked=advance.blocked|(enabled&~ready),fault=fault)
        if self.reward_memory is None or not bool((~enabled|ready).all().cpu()):return advance,None
        def close():
            portfolio,_=self.reward_reads(self.clock.public)
            _,reward=self.reward_memory.close(cutoff,portfolio,self.reward_a_pusd,enabled)
            return reward
        return advance,run_exact_batch(close)

    def account_builder(self,encoder,public,frozen,bank,market_indices,row_identities):
        population=self.population;request_directory=self.request_directory
        tokens=self.directory.tokens;condition_slots=self.condition_slots;start=self.started_at
        zero=self.zero;yes=self.yes;empty=self.empty;T=self.tensor
        known={name:self.yes for name in ('executed_prefix_known','success_prefix_known','rebate_prefix_known','settlement_prefix_known',
            'ledger_prefix_known','economic_liability_prefix_known','economic_prefix_known')}
        market=encoder.market
        valuation=PortfolioValuation(encoder.shared,tokens,public.ledger.success_pools.asset_roots)
        frame=market.frame(frozen.prefixes)
        markets=CancelObjectDirectory(frame.admitted,market.times[:,0][None,:],condition_slots[None,:],yes[:,None].expand(1,len(condition_slots)),
            empty.bool(),empty.reshape(1,0,2),empty)
        leg_ranks=request_directory.identity_order(population.submitted_legs.request_slots)
        account=population.freeze_account(public,frozen.endpoint_memory.previous_observation_ms,markets,leg_ranks)
        directory=account.intent_orders
        formation=public.registry.freeze_order_formation(directory,population.submitted_legs,account.cutoff,markets.market_present)
        location=formation.market;located=location>=0
        quoted=valuation.freeze(frozen.prefixes,account.cutoff)
        native=account_inputs_at_prefix(account,market,condition_slots,frozen.prefixes,start,location,located,
            self.outcomes,row_role=frozen.role,valuation=quoted,**known)
        actor=publish_actor_context(account,native,encoder,frozen,market_indices,condition_slots,row_identities,account.cancel_objects)
        scopes=population.cancel_scopes(directory)
        objects=account.cancel_objects;object_scopes=CancellationScopes(objects.kinds==2,objects.markets,objects.members)
        target_history=public.cancellation_target_history
        # 相同原身份轴重定位既有目标；此冻结候选不修改持续历史。
        target_history=target_history.with_orders(directory.identities,directory.present)
        retained=directory.present
        snapshot=freeze_cancel_snapshot(public.cancellations,scopes,object_scopes,objects.present,location,located,
            retained,target_history.targets,account.cutoff)
        requests=freeze_scoped_cancel_requests(snapshot.state,scopes,object_scopes,location,snapshot.location_known,
            snapshot.targets,account.cutoff)
        leg_count=population.submitted_legs.present.size(1)
        sides=(population.submitted_legs.outcome_indices.gather(1,directory.term_leg_slots.clamp(0,leg_count-1))
            if leg_count else torch.full_like(directory.term_leg_slots,-1))
        sides=torch.where(directory.origin_known,sides,-1)
        funds=public.ledger.order_funds
        cancels=cancel_inputs(objects,account.cancel_directory,directory,account.orders,account.order_sources,
            funds,funds.read(public.ledger.lifecycle),bank,requests,account.cutoff,location,sides,zero[:,None],
            native.scopes.conditions,native.scopes.present)
        return account,native,actor,cancels

    def sender_factory(self,task):
        clock=self.clock;random=self.random;codec=self.codec;sampler=self.sampler;rule=self.rule
        directory=self.directory;population=self.population;request_directory=self.request_directory
        observation=task.observation
        inputs=ActorPolicyInputs.publish(observation.actor,observation.market,observation.frozen,task.evaluation,observation.cancels)
        control=task.request_identity.control(task.send_at)
        driver=CapturedDeviceActor(task.policy,inputs,random,word_capacity=self.word_capacity,path_capacity=self.path_capacity)
        publication=ActorSelection(random)
        order_values=exact_values(observation.account.intent_orders.identities)
        ids=codec.sim_order_ids_v1(order_values);orders=dict(zip(order_values,ids,strict=True))
        slots={}
        submit=ActorRequestSubmission(control,control.leg_requests,sampler,rule,None,None,
            (directory.conditions,),(directory.tokens,),(slots,),
            (self.market_ends,),codec,population=population,
            order_sequences=({value:key for key,value in orders.items()},),identity_directory=request_directory)
        ids=IntentRequestIdentities(task.request_identity.contexts,
            (directory.conditions,),(directory.public_tokens,),(orders,))
        sender=FrozenActorSend(publication,driver,control,ids,codec,submit,self.receive,observation.source_snapshot,
            decision_cutoff=observation.cutoff)
        return sender

    def freeze(self,world,at,enabled):
        frozen,accepted=self.inputs.freeze(world,at,enabled)
        self.input_requirement=None if frozen is None or bool(accepted.all().cpu()) else frozen.market.required.max(0).values
        if (self.reward_a_pusd is not None and self.reward_memory is None and frozen is not None
                and bool(accepted.all().cpu())):
            def open_interval():
                portfolio,exposure=self.reward_reads(world.public)
                return RewardIntervalMemory.open(at,portfolio,exposure)
            self.reward_memory=run_exact_batch(open_interval)
        if self.learning is not None and frozen is not None and bool(accepted.all().cpu()):
            def endpoint():
                portfolio,_=self.reward_reads(world.public)
                return self.reward_memory.endpoint(at,portfolio,accepted)
            self.reward_memory,point=run_exact_batch(endpoint)
            frozen=replace(frozen,reward_endpoint=point)
        return frozen,accepted

    def enable_learning(self,policy,optimizer,config,version,*,reporter=None):
        """单根也由同一共同学习器消费；不是另一套更新算法。"""
        from .logical_batch import JointLearner
        return self.join_learning(JointLearner(policy,optimizer,config,version,reporter=reporter),config)

    def join_learning(self,learner,config):
        from .logical_batch import LogicalBatch
        self.learning=LogicalBatch(self,learner.policy,learner.optimizer,config,learner.version,learner=learner)
        self.models.learning_consumer=self.learning
        self.models.policy_at_start=self.learning.policy_at_start
        return self.learning

    def collect_learning_batch(self,horizon,enabled,*,progress=None):
        """原事件循环到真实学习停点；不为排空推进尚未到期的发送。"""
        if self.learning.boundary():return None
        return self.run.advance_before(horizon,enabled,progress=progress,learning_boundary=self.learning.boundary)

    def prepare_learning_batch(self,enabled,*,target_end=False,progress=None):
        """普通批纯自举；目标末批只在显式有限目标实际推进/领取后关闭。"""
        if self.learning.ended:return None,None
        endpoint=None
        if target_end:
            advance,reward=self.read_interval_reward(self.goal_until,enabled,progress=progress)
            if reward is None:return advance,None
            def close_endpoint():
                portfolio,_=self.reward_reads(self.clock.public)
                return self.reward_memory.endpoint(self.goal_until,portfolio,enabled)
            self.reward_memory,endpoint=run_exact_batch(close_endpoint)
        return None,self.learning.prepare(target_end=target_end,endpoint=endpoint)

    def update_learning_batch(self,enabled,*,target_end=False,progress=None):
        advance,prepared=self.prepare_learning_batch(enabled,target_end=target_end,progress=progress)
        if prepared is None:return advance,None
        return None,self.learning.learner.update()[self.learning]

    def grow_inputs(self):
        if self.input_requirement is None:return False
        need=self.input_requirement.cpu().tolist();encoder=self.inputs.encoder
        capacities=(encoder.packer.capacity,encoder.binance.book.capacity,encoder.events.window.fact_capacity,
            encoder.events.window.source_capacity,encoder.trades.window.capacity)
        if not any(a>b for a,b in zip(need,capacities)):return False
        encoder.grow_resources(need)
        return True

    def resume_resources(self,result):
        """只使用本次失败的原人口需求；调用方从同一原事件再推进。"""
        changed=self.grow_inputs();clock=self.clock
        advance=result.stage_result;effect=None if advance is None else advance.stage_result
        changed=self.grow_cancel_results(effect) or changed
        senders=[task.sender for task in self.models.tasks if task.sender is not None]
        requests=max([clock.runtime.requests.capacity]+[int(s.receipt.required_requests.max().cpu()) for s in senders if s.receipt is not None])
        legs=max([clock.public.ledger.reservations.capacity]+[int(s.publication.reservation.decision.required_capacity.max().cpu())
            for s in senders if s.publication.reservation is not None])
        ledger=clock.public.ledger
        if legs>ledger.reservations.capacity:
            pad=legs-ledger.reservations.capacity
            clock.public=replace(clock.public,ledger=replace(ledger,reservations=ledger.reservations.resized(legs),
                global_requests=torch.nn.functional.pad(ledger.global_requests,(0,pad),value=-1),
                item_ordinals=torch.nn.functional.pad(ledger.item_ordinals,(0,pad),value=-1)))
            changed=True
        if requests>clock.runtime.requests.capacity:
            clock.runtime=replace(clock.runtime,requests=clock.runtime.requests.resized(requests,clock.runtime.requests.action_capacity))
            changed=True
        from actor_runtime import grow_ledger_resources
        return grow_ledger_resources(self.runtime) or changed

    def grow_cancel_results(self,effect):
        if effect is None or not hasattr(effect,'required_results'):return False
        from cancel_results import CancelResults
        old=self.clock.runtime.cancel_results or CancelResults(len(self.zero))
        need=int(effect.required_results.max().cpu())
        if need<=old.capacity:return False
        self.clock.runtime=replace(self.clock.runtime,cancel_results=old.resized(need))
        return True
