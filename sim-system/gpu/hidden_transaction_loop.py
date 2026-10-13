"""原send_clock选择器的有序hidden物理批；每个原桶分别共同安装。"""
from dataclasses import replace
import torch
from queues import Numbers
from hidden_level_loop import _CapturedLevels,_state,_inputs,_install,_copy
from base_runtime_storage import _owned,_capacity_like
from maker_loop import _signature
from public_source import public_group_selection


class _CapturedTransactions(_CapturedLevels):
    def __init__(self,world,choice,stage,execution,group,gtd,at,enabled,before,width):
        self.macro_bound=False
        paths=execution.fill_paths('maker',world,None)
        from asset_buy import take_ratio
        fee=Numbers.run_exact_batch(lambda:take_ratio(execution.rule.values,torch.full_like(at,2)))
        super().__init__(world,choice,stage,execution.condition_by_asset,execution.source_assets,paths,fee,
            execution.rule.context['fee_exponent'],execution.taker_rate,width)
        self.group=_owned(group,group);self.gtd=None if gtd is None else _owned(gtd,gtd)
        self.until=at.clone();self.enabled=enabled.clone();self.before=before
        self.committed=_owned(self.initial,self.initial)
        self.reference=torch.zeros_like(self.index);self.reference_end=torch.zeros_like(self.index)
        self.processed=torch.zeros_like(self.index)
        self.transaction_ready=torch.zeros_like(self.ready)
        self.flags=[torch.zeros_like(self.ready) for _ in range(7)]
        self.macro_bound=True
        operations=[self.select_transaction,self.begin_transaction,self.reference_step,
            self.prepare,self.take_column,self.finish,self.finish_transaction]
        # 所有warm/捕获仍关闭activity安装；只操作私有小状态。
        stream=torch.cuda.Stream(device=self.device);stream.wait_stream(torch.cuda.current_stream(self.device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                self.reset_transactions()
                for operation in operations:operation()
        torch.cuda.current_stream(self.device).wait_stream(stream)
        self.graphs=[]
        for operation in [self.reset_transactions,*operations]:
            graph=torch.cuda.CUDAGraph(keep_graph=True)
            with torch.cuda.graph(graph):operation()
            self.graphs.append(graph)
        self.loop=Numbers.public_extension().DeviceConditionalSequenceLoop(self.graphs[0].raw_cuda_graph(),
            [g.raw_cuda_graph() for g in self.graphs[1:]],self.flags,self.ready)

    def phase_to(self,phase,ready=None):
        phase=phase if isinstance(phase,torch.Tensor) else self.phase.new_full((),phase)
        self.phase.copy_(phase)
        self.ready.copy_(self.fits&~self.stopped if ready is None else ready&self.fits&~self.stopped)
        for index,flag in enumerate(self.flags):flag.copy_((self.phase==index)&self.ready)

    def select_phase(self,phase):
        if not self.macro_bound:return super().select_phase(phase)
        phase=phase if isinstance(phase,torch.Tensor) else self.phase.new_full((),phase)
        block=self.world.hidden_frontier.columns.source
        tag=block.step_tags.index_select(0,self.index.clamp_max(block.step_tags.numel()-1).reshape(1))[0]
        done=self.index>=self.end
        # 真实类型/经济出口结束当前未提交桶，宿主原handler重算它。
        self.phase_to(torch.where(done,6,phase+3),done|(tag==2))

    def current_choice(self,world):
        return replace(self.choice,at=self.inputs.at,present=self.inputs.present,fault=self.inputs.fault,
            requests=world.requests,fills=world.fills,calendars=world.calendars,hidden_frontier=world.hidden_frontier,
            settlement_registrations=world.settlement_registrations,order_query_history=world.order_query_history)

    def reset_transactions(self):
        _copy(self.state,self.initial);_copy(self.committed,self.initial)
        self.processed.zero_();self.required.zero_();self.fits.fill_(True);self.stopped.zero_()
        self.phase_to(0)

    def select_transaction(self):
        world=self.view();frontier=world.hidden_frontier
        hidden,eof=frontier.scheduled()
        choice=world.choose(hidden,eof,self.until,self.enabled,gtd=self.gtd,before=self.before)
        bound=frontier.columns.next_bucket_at
        due,take=public_group_selection(self.group,choice,hidden,eof,bound,self.until,self.enabled,before=self.before)
        known=hidden.present|eof
        if bound is not None:known|=(bound.expand_as(self.until)>=self.until) if self.before else (bound.expand_as(self.until)>self.until)
        choice=replace(choice,present=choice.present&(~due|(choice.at<=self.group.at)))
        phase=torch.where(choice.present,choice.phase,torch.iinfo(torch.int64).max).min()
        active=choice.present&(choice.phase==4)
        ready=active.any()&(phase==4)&~take.any()&~(due&~self.group.present).any()&~(self.enabled&~known).any()
        ready&=~(choice.fault|self.group.fault).any()
        self.inputs.at.copy_(choice.at);self.inputs.present.copy_(active);self.inputs.fault.copy_(choice.fault)
        self.phase_to(1,ready)

    def begin_transaction(self):
        self.stage.dirty.zero_();self.stage.counts.copy_(self.stage.installed_counts)
        world=self.view();world=world.begin_hidden_bucket(self.current_choice(world))
        _copy(self.state,_state(world));frontier=world.hidden_frontier;active=self.inputs.present&frontier.applying
        maximum=torch.full_like(frontier.next_bucket,torch.iinfo(torch.int64).max)
        slots=(frontier.next_bucket+1).clamp_max(frontier.columns.at.numel())
        self.reference.copy_(torch.where(active,frontier.next_reference,maximum).min())
        self.reference_end.copy_(torch.where(active,frontier.columns.reference_offsets[slots],0).max())
        self.index.copy_(torch.where(active,frontier.next_step,maximum).min())
        self.end.copy_(torch.where(active,frontier.columns.step_offsets[slots],0).max())
        self.next_reference_phase()

    def next_reference_phase(self):
        block=self.world.hidden_frontier.columns.source
        tag=block.reference_rows.index_select(0,self.reference.clamp_max(block.reference_rows.size(0)-1).reshape(1))[0,0]
        done=self.reference>=self.reference_end
        supported=(tag!=3)&(tag!=8)&(tag!=9)
        self.phase_to(torch.where(done,torch.where(self.index>=self.end,6,3),2),done|supported)
        if block.step_tags.numel():
            step=block.step_tags.index_select(0,self.index.clamp_max(block.step_tags.numel()-1).reshape(1))[0]
            self.ready.logical_and_(~done|(self.index>=self.end)|(step==2))
            for index,flag in enumerate(self.flags):flag.copy_((self.phase==index)&self.ready)

    def reference_step(self):
        def compute():
            world=self.view()
            return _state(world.apply_hidden_reference(self.current_choice(world),
                torch.ones_like(self.inputs.at)*self.reference,self.source_assets,ordinary=True))
        state=self.checked(compute)
        _copy(self.state,state,self.fits);self.reference.add_(self.fits.to(torch.int64))
        self.next_reference_phase()

    def finish_transaction(self):
        def compute():
            world,_=self.view().finish_hidden_bucket()
            auto=torch.zeros((),dtype=torch.bool,device=self.device)
            if world.auto_redemption_sources is not None:
                auto=world.auto_market_ready(self.inputs.present).any()
            fault=world.hidden_frontier.fault
            if world.input_frontier is not None:
                frontier,commit_fault=world.input_frontier.commit(self.inputs.present)
                world=replace(world,input_frontier=frontier);fault=fault|commit_fault
            return _state(world),fault,auto
        state,fault,auto=self.checked(compute)
        committed=self.fits&~fault.any()&~auto
        # 前面的候选没有任何真实反馈；原公开append为空，输入frontier成功后才安装。
        self.stage.install(self.queue,committed&self.running,bound=True)
        _copy(self.state,state,committed);_copy(self.committed,state,committed)
        self.processed.add_(committed.to(torch.int64));self.stopped.copy_(~committed);self.phase_to(0)

    def replay_transactions(self,world,choice,group,gtd,at,enabled,execution):
        paths=execution.fill_paths('maker',world,None)
        from asset_buy import take_ratio
        fee=Numbers.run_exact_batch(lambda:take_ratio(execution.rule.values,torch.full_like(at,2)))
        _copy(self.initial,_capacity_like(_state(world),self.initial))
        _copy(self.inputs,_capacity_like(_inputs(world,choice,paths,fee,execution.taker_rate),self.inputs))
        _copy(self.group,group)
        if gtd is not None:_copy(self.gtd,gtd)
        self.until.copy_(at);self.enabled.copy_(enabled);self.running.fill_(True)
        self.loop.replay()
        # 当前桶若需真实消费者/资源，丢弃它；只带回此前各桶共同安装的成功前缀。
        result=_install(world,_owned(self.committed,self.committed))
        self.stage.dirty.zero_();self.stage.pending.clear()
        return result,int(self.processed.cpu()),int(self.required.cpu())


class HiddenTransactionDeviceLoop:
    def __init__(self):self.width=4;self.key=None;self.captured=None

    def run(self,world,choice,stage,execution,group,gtd,at,enabled,*,before):
        queue=world.hidden.queues;block=world.hidden_frontier.columns.source
        # 零物理列无capturable类型读取，仍交原合法空桶处理器。
        if not block.step_tags.numel() or not block.reference_rows.size(0):return world,0
        key=(_signature(_state(world)),_signature(_inputs(world,choice,
            execution.fill_paths('maker',world,None),execution.taker_rate,execution.taker_rate)),
            _signature(queue),
            id(queue.price),id(stage),id(execution.condition_by_asset),id(execution.source_assets),
            _signature(group),_signature(gtd),before,self.width,at.device,torch.cuda.current_stream(at.device).cuda_stream)
        if key!=self.key:
            self.captured=None;self.key=None
            self.captured=_CapturedTransactions(world,choice,stage,execution,group,gtd,at,enabled,before,self.width)
            self.key=key
        result,count,required=self.captured.replay_transactions(world,choice,group,gtd,at,enabled,execution)
        if required>self.width:self.width=1<<(max(4,required)-1).bit_length()
        return result,count
