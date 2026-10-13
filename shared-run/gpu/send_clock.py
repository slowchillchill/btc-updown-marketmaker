"""有界真实来源到开放Execute端点；推进/领取不替代原模块业务处理器。"""
from dataclasses import dataclass,replace
import torch
from consumption import ReceiveGates
from queues import Numbers
from public_source import public_group_selection
from public_group_loop import PublicGroupDeviceLoop


@dataclass(frozen=True)
class SendAdvance:
    complete: torch.Tensor
    public_page_missing: torch.Tensor
    hidden_page_missing: torch.Tensor
    fault: torch.Tensor
    processed: int
    stage_result: object = None


class BoundedSendClock:
    """拥有实际公开消费位置，SIM/NOE/账户仍由各自原状态拥有。

    handlers处理原phase，返回共同候选(runtime,projection,batch,effect)。无法完成
    当前资源事务时返回原进入状态，effect保原需求；不会把资源不足当业务完成。
    gtd必须由本装配的实际请求/订单来源供给，不用None默认为不存在。
    """
    def __init__(self,runtime,projection,public,source,handlers,gtd,bank,*,hidden_page_buckets=64):
        if type(hidden_page_buckets) is not int or hidden_page_buckets<1:
            raise ValueError('物理hidden页容量须为正整数')
        self.hidden_page_buckets=hidden_page_buckets
        self.runtime,self.projection,self.public=runtime,projection,public
        self.source,self.handlers,self.gtd,self.bank=source,handlers,gtd,bank
        self.gates=ReceiveGates.unset(runtime.orders.roots,device=runtime.orders.occupied.device)
        self.public_loop=PublicGroupDeviceLoop()
        self.last_observation=None
        self.observation_known=torch.zeros_like(self.gates.has_public_visible)

    def advance_before(self,at,enabled,*,progress=None):
        return self._advance(at,enabled,before=True,progress=progress)

    def advance_through(self,at,enabled,*,progress=None):
        """尾段处理真实同刻事件后关闭双门；不借at+1改变实际公开时刻。"""
        return self._advance(at,enabled,before=False,progress=progress)

    def next_event(self,enabled):
        """原SIM、来源与未领取反馈的下一真实时刻；缺页仍保真实下界。"""
        from scheduling import ScheduledTime
        maximum=torch.full_like(self.source.position,torch.iinfo(torch.int64).max)
        frontier=self.runtime.hidden_frontier
        hidden,eof=frontier.scheduled()
        choice=self.runtime.choose(hidden,eof,maximum,enabled,gtd=self.gtd)
        group=self.source.next()
        times=[choice.at,group.at];present=[choice.present,enabled&~group.eof]
        if frontier.columns.next_bucket_at is not None:
            times.append(frontier.columns.next_bucket_at.expand_as(maximum))
            present.append(enabled&~hidden.present&~eof)
        pending=self.public.pending_noe
        if pending.active.size(1):
            times.append(torch.where(pending.active,pending.received_at,maximum[:,None]).min(1).values)
            present.append(enabled&pending.active.any(1))
        valid=torch.stack(present,1);at=torch.stack(times,1)
        next_at=torch.where(valid,at,maximum[:,None]).min(1).values
        return ScheduledTime(next_at,valid.any(1),choice.fault|group.fault|pending.fault)

    def _advance(self,at,enabled,*,before,progress=None):
        processed=0
        while True:
            group=self.source.next()
            frontier=self.runtime.hidden_frontier
            if frontier is None:
                raise ValueError('发送推进需要绑定实际hidden reader前沿，不能用None隐藏来源')
            hidden,eof=frontier.scheduled()
            choice=self.runtime.choose(hidden,eof,at,enabled,gtd=self.gtd,before=before)
            hidden_bound=frontier.columns.next_bucket_at
            public_due,take_public=public_group_selection(group,choice,hidden,eof,hidden_bound,at,enabled,before=before)
            public_choice=choice
            hidden_known=hidden.present|eof
            if hidden_bound is not None:hidden_known|=(hidden_bound.expand_as(at)>=at) if before else (hidden_bound.expand_as(at)>at)
            missing_hidden=enabled&~hidden_known
            missing_public=public_due&~group.present
            # 冷资源装配仅安装原页；不消费组/桶、不推进时钟，也不替换旧冻结。
            if bool(missing_hidden.any().cpu()):
                loaded=frontier.load_next(self.hidden_page_buckets)
                if loaded is not frontier:
                    self.runtime=replace(self.runtime,hidden_frontier=loaded)
                    if progress:progress('hidden_page',begin=loaded.columns.bucket_base,
                        buckets=loaded.columns.at.numel())
                    continue
            if bool(missing_public.any().cpu()):
                previous=(self.source.page_begin,self.source.page_end)
                self.source.load_next()
                if previous!=(self.source.page_begin,self.source.page_end):
                    if progress:progress('public_page',begin=self.source.page_begin,end=self.source.page_end)
                    continue
            # 暂缺公开页仅允许处理不晚于其真实下一组的SIM阶段，不能跨源空档。
            choice=replace(choice,present=choice.present&(~public_due|(choice.at<=group.at)))
            # 同刻先原SIM；连续公开循环保未按当前组屏蔽的public_choice。
            fault=choice.fault|group.fault
            if bool((fault!=0).any().cpu()):
                if progress:progress('drive_fault',fault=fault.cpu().tolist(),requests=self.runtime.requests.fault.cpu().tolist(),
                    orders=self.runtime.orders.fault.cpu().tolist(),account=self.runtime.account.fault.cpu().tolist(),
                    hidden=self.runtime.hidden.fault.cpu().tolist(),frontier=frontier.fault.cpu().tolist(),
                    fills=self.runtime.fills.fault.cpu().tolist(),maker=self.runtime.calendars.maker.fault.cpu().tolist(),
                    taker=self.runtime.calendars.taker.fault.cpu().tolist(),controls=self.runtime.market_controls.fault.cpu().tolist())
                return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,fault,processed)
            if bool(take_public.any().cpu()):
                input_frontier=self.runtime.input_frontier
                # 首个真实receipt沿原入口产生；缺页冷装配也仍由原宿主处理。
                if at.is_cuda and (input_frontier is None or input_frontier.public_receipt is not None) and not bool(
                        (missing_public|missing_hidden).any().cpu()):
                    result=self.public_loop.run(self.source,input_frontier,public_choice,hidden,eof,
                        hidden_bound,at,enabled,before=before,progress=progress)
                    self.source.position=result.position
                    if input_frontier is not None:self.runtime=replace(self.runtime,input_frontier=result.frontier)
                    processed+=result.processed
                    if progress:progress('public_group',processed=processed,position=self.source.position.detach().cpu().tolist())
                    if bool(result.fault.any().cpu()):
                        return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,result.fault,processed)
                    continue
                if self.runtime.input_frontier is not None:
                    frontier,commit_fault=Numbers.run_exact_batch(lambda:self.runtime.input_frontier.public_group(self.source.prepared,group.position,take_public))
                    if bool(commit_fault.any().cpu()):return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,commit_fault,processed)
                    self.runtime=replace(self.runtime,input_frontier=frontier)
                self.source.consume(take_public)
                processed+=1
                if progress:progress('public_group',processed=processed,position=self.source.position.detach().cpu().tolist())
                continue
            if not bool(choice.present.any().cpu()):
                complete=enabled&~public_due&~missing_hidden
                fresh=complete&(~self.gates.has_closed_receive|(self.gates.closed_receive_at!=at))
                if self.runtime.input_frontier is not None:
                    frontier,commit_fault=Numbers.run_exact_batch(lambda:self.runtime.input_frontier.commit(fresh))
                    if bool(commit_fault.any().cpu()):return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,commit_fault,processed)
                    self.runtime=replace(self.runtime,input_frontier=frontier)
                self.gates=self.gates.before(at,complete) if before else self.gates.set_receive(at,complete).set_public(at,complete)
                return SendAdvance(complete,missing_public,missing_hidden,fault,processed)
            # 只读取一次原设备已选阶段用于调用所属处理器，不在CPU选择订单或市场。
            phase=int(choice.phase[choice.present].min().cpu())
            active=choice.present&(choice.phase==phase)
            choice=replace(choice,present=active)
            handler=self.handlers.get(phase)
            if handler is None:
                raise NotImplementedError(f'本来源实际出现phase={phase}，原处理器尚未装配')
            if progress:progress('sim_phase_begin',processed=processed,phase=phase)
            runtime,projection,batch,effect=handler(self.runtime,self.projection,choice)
            batches=() if batch is None else batch if isinstance(batch,tuple) else (batch,)
            handler_fault=torch.zeros_like(fault)
            for emitted in batches:handler_fault |= emitted.fault
            if effect is not None and hasattr(effect,'fault'):handler_fault |= effect.fault
            if bool((handler_fault!=0).any().cpu()):
                # 未完成候选不替换此刻权威状态；此前成功pending/公开组仍保留。
                return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,handler_fault,processed,effect)
            # 原handler成功后准备整个阶段；精度重算不触碰SIM/Projection/frontier。
            public=self.public if not batches else Numbers.run_exact_batch(
                lambda:self.public.append_batches(self.bank(projection),batches))
            if phase==4 and runtime.input_frontier is not None:
                frontier,commit_fault=Numbers.run_exact_batch(lambda:runtime.input_frontier.commit(active))
                if bool(commit_fault.any().cpu()):return SendAdvance(torch.zeros_like(enabled),missing_public,missing_hidden,commit_fault,processed)
                runtime=replace(runtime,input_frontier=frontier)
            self.runtime,self.projection,self.public=runtime,projection,public
            processed+=1
            if progress:progress('sim_phase',processed=processed,phase=phase,at=choice.at.detach().cpu().tolist())

    def receive_first(self,enabled=None):
        """按就绪根使用真实耗尽门；其它根保原首次领取/观察前缀。"""
        enabled=torch.ones_like(self.gates.has_public_visible) if enabled is None else enabled
        gates=replace(self.gates,has_closed_receive=self.gates.has_closed_receive & enabled,
            has_public_visible=self.gates.has_public_visible & enabled)
        self.public,delivery=self.public.receive_first(self.bank(self.projection),gates)
        if self.runtime.input_frontier is not None:
            fresh=gates.has_public_visible & (~self.observation_known if self.last_observation is None else
                (~self.observation_known | (gates.public_visible_at!=self.last_observation.observed_at)))
            frontier,cutoff,fault=Numbers.run_exact_batch(lambda:self.runtime.input_frontier.observe(
                gates.public_visible_at,self.public.pending_noe,self.public.waiting,fresh))
            self.runtime=replace(self.runtime,input_frontier=frontier)
            if self.last_observation is not None:
                from admission import coordinate
                committed=fresh & (fault==0)
                receipt=Numbers.run_exact_batch(lambda:coordinate(cutoff.receipt,self.last_observation.receipt,committed))
                cutoff=replace(Numbers.run_exact_batch(lambda:coordinate(cutoff,self.last_observation,committed)),receipt=receipt)
            self.last_observation=cutoff
            self.observation_known |= fresh & (fault==0)
            delivery=replace(delivery,fault=delivery.fault|fault)
        return delivery
