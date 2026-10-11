"""连续观察的历史/窗口拥有者；只接已处理且已公开领取的原业务前沿。"""
from dataclasses import dataclass,replace
import torch
from .frozen_inputs import FrozenPolicyInputs100ms


@dataclass(frozen=True)
class FrozenActorObservation:
    frozen: FrozenPolicyInputs100ms
    market: object
    account: object
    native: object
    actor: object
    cancels: object
    cutoff: object
    source_snapshot: object
    decision_rows: torch.Tensor
    row_roots: torch.Tensor | None = None
    root_cutoffs: torch.Tensor | None = None

    def actor_view(self):
        """只给原Actor根级当前市场；ContextOnly不建立第二动作人口。"""
        return replace(self,frozen=self.frozen.rows(self.decision_rows),
            market=self.market.rows(self.decision_rows),
            row_roots=None if self.row_roots is None else self.row_roots[self.decision_rows])


class ContinuousActorInputs:
    """提交每个完整组一次，成功冻结后承接原history/endpoint候选记忆。

    account_builder沿公共Registry/Bindings读取同Ck完整账户/Actor/184。
    不读取SIM未来订单，也不在这里提前绑定权重或计算模型。
    """
    def __init__(self,encoder,history,market_indices,root_started_at,identities,account_builder,*,word_capacity,root_prefix,root_previous_at,row_roots=None):
        self.encoder=encoder;self.history=history
        self.root_started_at=root_started_at
        self.account_builder=account_builder;self.word_capacity=word_capacity
        self.directory_markets=market_indices;self.directory_identities=identities
        self.directory_roots=torch.arange(len(market_indices),device=market_indices.device) if row_roots is None else row_roots
        # 根窗口属于公共流，不能由一个市场端点代持。
        self.previous_prefix=root_prefix.clone();self.previous_observation_ms=root_previous_at.clone()
        self.hot_directory=market_indices.new_empty(0)
        self.market_indices=market_indices[:0];self.identities=identities[:0];self.row_roots=self.directory_roots[:0]
        self.memory=history.empty(0,root_started_at[:0])
        from input_100ms import MarketFreezeMemory
        self.endpoints=MarketFreezeMemory(root_prefix[:0],root_previous_at[:0],None,None)

    def prepare_rows(self,clock,at):
        """只配置本次原资格要求的物理行；旧计算所需行由模型完成后释放。"""
        from queues import Numbers
        prefix=clock.source.prefix_at(self.encoder.shared,at)
        roots=self.directory_roots
        roles,_=Numbers.run_exact_batch(lambda:self.encoder.roles(at[roots],self.directory_markets,
            self.root_started_at[roots],prefix,row_roots=roots))
        candidates=torch.nonzero(roles>=0).flatten()
        present=(candidates[:,None]==self.hot_directory[None]).any(1)
        new=candidates[~present]
        if new.numel():
            owners=roots[new]
            fresh=self.history.empty(len(new),self.root_started_at[owners])
            fresh=replace(fresh,last_prefix=self.previous_prefix[owners],last_cutoff=self.previous_observation_ms[owners])
            self.memory=self.memory.append(fresh)
            def extend(side):
                if side is None:return None
                from input_sides import PmSideMemory
                sides=side.known.size(1)
                zero=Numbers.ExactInt.from_i64_tensor(torch.zeros(len(new)*sides,dtype=torch.int64,device=at.device))
                def join(left):
                    width=max(left.words.size(1),zero.words.size(1))
                    return Numbers.ExactInt(torch.cat((torch.nn.functional.pad(left.words,(0,width-left.words.size(1))),
                        torch.nn.functional.pad(zero.words,(0,width-zero.words.size(1))))),torch.cat((left.sign,zero.sign)),torch.cat((left.fault,zero.fault)))
                return PmSideMemory(join(side.total),join(side.best),
                    torch.cat((side.known,torch.zeros((len(new),sides),dtype=torch.bool,device=at.device))),
                    torch.cat((side.best_present,torch.zeros((len(new),sides),dtype=torch.bool,device=at.device))),
                    torch.cat((side.asset_indices,torch.zeros((len(new),side.asset_indices.size(1)),dtype=torch.int64,device=at.device))))
            self.endpoints=replace(self.endpoints,
                previous_prefix=torch.cat((self.endpoints.previous_prefix,self.previous_prefix[owners])),
                previous_observation_ms=torch.cat((self.endpoints.previous_observation_ms,self.previous_observation_ms[owners])),
                pm_sides=extend(self.endpoints.pm_sides),binance_sides=extend(self.endpoints.binance_sides))
            self.hot_directory=torch.cat((self.hot_directory,new));self._set_rows()
        return roles[self.hot_directory]

    def _set_rows(self):
        self.market_indices=self.directory_markets[self.hot_directory]
        self.identities=self.directory_identities[self.hot_directory]
        self.row_roots=self.directory_roots[self.hot_directory]

    def bind_source(self,source):
        if not hasattr(source.prepared,'parts'):return
        self.pending_inputs=[self.encoder]
        self.observations_open=True
        source.install_listeners.append(self.install_public_block)
        pages=source.initial_input_pages or []
        source.initial_input_pages=None
        for page in pages:self.install_public_block(page,page.install())

    def stop_observations(self):
        if hasattr(self,'pending_inputs'):
            self.pending_inputs.clear();self.observations_open=False

    def install_public_block(self,page,states):
        from input_100ms import MarketInputs100ms
        from input_history import SourceHistoryInputs
        pm,market,reference,binance=states
        if self.encoder.shared.group_base==pm.group_base:return
        history=SourceHistoryInputs(pm,market,reference,page.columns,identity_registry=self.history.registry)
        encoder=MarketInputs100ms(pm,market,reference,binance,history,self.encoder.process_rules,page.columns,pm.causal_visible_at)
        for owner,previous,name in ((encoder.packer,self.encoder.packer,'capacity'),
                (encoder.binance.book,self.encoder.binance.book,'capacity'),
                (encoder.events.window,self.encoder.events.window,'fact_capacity'),
                (encoder.events.window,self.encoder.events.window,'source_capacity'),
                (encoder.trades.window,self.encoder.trades.window,'capacity')):
            setattr(owner,name,getattr(previous,name))
        self.encoder,self.history=encoder,history
        if self.observations_open:self.pending_inputs.append(encoder)

    def _window_inputs(self):
        """只合并尚未被冻结接纳的真实窗口行；原事件/成交计算仍由Native拥有。"""
        from fact_window import PublicFactWindow,PublicTradeWindow
        from input_events import EventInputs
        from input_trades import TradeInputs
        from input_history import _join
        pieces=getattr(self,'pending_inputs',[self.encoder])
        facts=[];sources=[];trades=[];units=[];source_offset=decimal_offset=0
        for encoder in pieces:
            window=encoder.events.window
            rows=window.facts.clone();rows[:,2:4]+=source_offset
            facts.append(rows);sources.append(window.sources)
            source_offset+=len(window.sources)
            rows=encoder.trades.window.rows.clone()
            rows[:,[2,4,7]]+=decimal_offset
            trades.append(rows);units.append(encoder.trades.units)
            decimal_offset+=encoder.trades.units.sign.numel()
        fw=PublicFactWindow.__new__(PublicFactWindow)
        fw.group_count=self.encoder.shared.group_count
        fw.types=self.encoder.events.window.types
        fw.fact_capacity=self.encoder.events.window.fact_capacity;fw.source_capacity=self.encoder.events.window.source_capacity
        fw.facts=torch.cat(facts);fw.sources=torch.cat(sources)
        fw.fact_padded=torch.cat((fw.facts.new_zeros((1,4)),fw.facts));fw.source_padded=torch.cat((fw.sources.new_zeros((1,5)),fw.sources))
        fw.fact_groups=fw.facts[:,0].contiguous();fw.source_groups=fw.sources[:,0].contiguous()
        tw=PublicTradeWindow.__new__(PublicTradeWindow)
        tw.group_count=fw.group_count;tw.asset_count=len(self.encoder.shared.assets);tw.capacity=self.encoder.trades.window.capacity
        tw.rows=torch.cat(trades);tw.padded=torch.cat((tw.rows.new_zeros((1,11)),tw.rows));tw.groups=tw.rows[:,0].contiguous()
        ti=TradeInputs.__new__(TradeInputs);ti.state=self.encoder.shared;ti.window=tw
        ti.units=_join(units,0,[(v.sign.numel(),) for v in units])
        return EventInputs(fw),ti

    def freeze(self,clock,at,enabled):
        from queues import Numbers
        from admission import coordinate
        from input_100ms import MarketFreezeMemory,_endpoint_commit
        from input_account import model_account_rows
        from input_events import NativeEvents
        prefix=clock.source.prefix_at(self.encoder.shared,at)
        roles=self.prepare_rows(clock,at)
        selected=torch.nonzero((roles>=0)&enabled[self.row_roots]).flatten()
        rows=self.row_roots[selected];indices=self.market_indices[selected];identities=self.identities[selected]
        source_memory=self.memory.rows(selected);endpoint_memory=self.endpoints.rows(selected)
        begin=int((self.previous_prefix[enabled]+1).min().cpu());end=int(prefix[enabled].max().cpu())+1
        if not selected.numel():
            # 公共观察截止仍接纳，无状态行/Actor/随机数/伪NoTrade。
            self._commit_root_window(prefix,at,enabled)
            return None,enabled.clone()
        row_prefix=prefix[rows];row_at=at[rows];row_enabled=enabled[rows]
        started=self.root_started_at[rows]
        pieces=getattr(self,'pending_inputs',[self.encoder])
        def consume():
            memory=source_memory;error=torch.zeros_like(row_prefix,dtype=torch.int32)
            for encoder in pieces:
                first=max(begin,encoder.shared.group_base);last=min(end,encoder.shared.group_count)
                groups=torch.arange(first,max(first,last),device=at.device)
                memory,part_error=encoder.source_history.consume(groups,
                    encoder.delivered_cutoffs[groups-encoder.shared.group_base],indices,started,memory,
                    applied_prefix=row_prefix,enabled=row_enabled)
                error |= part_error
            return memory,error
        memory,error=Numbers.run_exact_batch(consume)
        events_encoder,trades_encoder=Numbers.run_exact_batch(self._window_inputs)
        def encode():
            # 完整组统计按根/窗口只编码一次，市场行只读同一结果。
            events=events_encoder.encode(self.previous_prefix,prefix,at)
            row_events=NativeEvents(events.values[rows],events.fault[rows],events.required[rows],events.enough[rows])
            original_trades=self.encoder.trades
            self.encoder.trades=trades_encoder
            try:
                market=self.encoder.freeze(row_at,indices,started,memory,endpoint_memory,
                applied_prefix=row_prefix,events=row_events)
            finally:self.encoder.trades=original_trades
            decisions=[]
            account_roots=torch.unique(rows,sorted=True)
            for root in account_roots:
                members=torch.nonzero(rows==root).flatten()
                order=torch.argsort(market.role[members],descending=True,stable=True)
                decisions.append(members[order[0]])
            decision_rows=torch.stack(decisions)
            arguments=(self.encoder,clock.public,market.rows(decision_rows),clock.bank(clock.projection),
                indices[decision_rows],identities[decision_rows])
            account,native,actor,cancels=self.account_builder(*arguments)
            return market,account,native,actor,cancels,decision_rows,account_roots
        candidate=Numbers.device_exact_batch(encode,word_capacity=self.word_capacity,device=at.device)
        market,account,native,actor,cancels,decision_rows,account_roots=candidate.value
        account_rows=model_account_rows(native.inputs,torch.searchsorted(account_roots,rows),market.role)
        frozen=FrozenPolicyInputs100ms.publish(market,account_rows,identities,candidate.fits)
        legal=market.role>=0
        row_fit=market.resources_fit&(error==0)&(frozen.fault==0)&frozen.provided.all(1)
        failures=torch.zeros_like(enabled,dtype=torch.int32).scatter_reduce(0,rows,
            (legal&~row_fit).to(torch.int32),reduce='amax')
        account_fault=torch.zeros_like(failures).scatter(0,account_roots,account.fault)
        accepted=enabled&candidate.fits&(account_fault==0)&(failures==0)
        committed=Numbers.run_exact_batch(lambda:coordinate(market.history_memory,source_memory,accepted[rows]))
        endpoint=MarketFreezeMemory(torch.where(accepted[rows],market.endpoint_memory.previous_prefix,endpoint_memory.previous_prefix),
            torch.where(accepted[rows],market.endpoint_memory.previous_observation_ms,endpoint_memory.previous_observation_ms),
            _endpoint_commit(market.endpoint_memory.pm_sides,endpoint_memory.pm_sides,accepted[rows]),
            _endpoint_commit(market.endpoint_memory.binance_sides,endpoint_memory.binance_sides,accepted[rows]))
        # 按物理行回写本次子批；旧计算保有的闭市记忆不参与新freeze。
        def merge(previous,current,total):
            from dataclasses import fields
            from public_state import _take
            if previous is None:
                if current is None:return None
                from input_sides import PmSideMemory
                sides=current.known.size(1);zero=Numbers.ExactInt.from_i64_tensor(torch.zeros(total*sides,dtype=torch.int64,device=at.device))
                previous=PmSideMemory(zero,zero,torch.zeros((total,sides),dtype=torch.bool,device=at.device),
                    torch.zeros((total,sides),dtype=torch.bool,device=at.device),torch.zeros((total,current.asset_indices.size(1)),dtype=torch.int64,device=at.device))
            if isinstance(previous,torch.Tensor):return previous.index_copy(0,selected,current)
            if hasattr(previous,'words'):
                width=max(previous.words.size(1),current.words.size(1));units=previous.sign.numel()//total
                positions=(selected[:,None]*units+torch.arange(units,device=at.device)).flatten()
                return Numbers.ExactInt(torch.nn.functional.pad(previous.words,(0,width-previous.words.size(1))).index_copy(0,positions,torch.nn.functional.pad(current.words,(0,width-current.words.size(1)))),
                    previous.sign.index_copy(0,positions,current.sign),previous.fault.index_copy(0,positions,current.fault))
            return replace(previous,**{f.name:merge(getattr(previous,f.name),getattr(current,f.name),total) for f in fields(previous)})
        self.memory=merge(self.memory,committed,len(self.hot_directory))
        self.endpoints=merge(self.endpoints,endpoint,len(self.hot_directory))
        self._commit_root_window(prefix,at,accepted)
        return FrozenActorObservation(frozen,market,account,native,actor,cancels,clock.last_observation,
            clock.source.snapshot(),decision_rows,rows.clone(),at.clone()),accepted

    def _commit_root_window(self,prefix,at,accepted):
        self.previous_prefix=torch.where(accepted,prefix,self.previous_prefix)
        self.previous_observation_ms=torch.where(accepted,at,self.previous_observation_ms)
        if hasattr(self,'pending_inputs'):
            consumed=int((self.previous_prefix+1).min().cpu())
            self.pending_inputs=[encoder for encoder in self.pending_inputs
                if encoder is self.encoder or encoder.shared.group_count>consumed]

    def release_markets(self, ended):
        """只移除无需旧模型计算的物理行，根公共窗口及经济事实保持。"""
        keep=torch.nonzero(~ended).flatten()
        self.memory=self.memory.rows(keep);self.endpoints=self.endpoints.rows(keep)
        self.hot_directory=self.hot_directory[keep];self._set_rows()
