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

    def actor_view(self):
        """只给原Actor根级当前市场；ContextOnly不建立第二动作人口。"""
        return replace(self,frozen=self.frozen.rows(self.decision_rows),
            market=self.market.rows(self.decision_rows))


class ContinuousActorInputs:
    """提交每个完整组一次，成功冻结后承接原history/endpoint候选记忆。

    account_builder沿公共Registry/Bindings读取同Ck完整账户/Actor/184。
    不读取SIM未来订单，也不在这里提前绑定权重或计算模型。
    """
    def __init__(self,encoder,history,memory,endpoints,market_indices,root_started_at,identities,account_builder,*,word_capacity,row_roots=None):
        self.encoder=encoder;self.history=history;self.memory=memory;self.endpoints=endpoints
        self.market_indices=market_indices;self.root_started_at=root_started_at;self.identities=identities
        self.account_builder=account_builder;self.word_capacity=word_capacity
        self.row_roots=torch.arange(len(market_indices),device=market_indices.device) if row_roots is None else row_roots
        self.root_rows=torch.stack([torch.nonzero(self.row_roots==root).flatten()[0]
            for root in range(len(root_started_at))])

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
        rows=self.row_roots;row_prefix=prefix[rows];row_at=at[rows];row_enabled=enabled[rows]
        started=self.root_started_at[rows]
        begin=int((self.memory.last_prefix+1).min().cpu());end=int(prefix[enabled].max().cpu())+1
        pieces=getattr(self,'pending_inputs',[self.encoder])
        def consume():
            memory=self.memory;error=torch.zeros_like(row_prefix,dtype=torch.int32)
            for encoder in pieces:
                first=max(begin,encoder.shared.group_base);last=min(end,encoder.shared.group_count)
                groups=torch.arange(first,max(first,last),device=at.device)
                memory,part_error=encoder.source_history.consume(groups,
                    encoder.delivered_cutoffs[groups-encoder.shared.group_base],self.market_indices,started,memory,
                    applied_prefix=row_prefix,enabled=row_enabled)
                error |= part_error
            return memory,error
        memory,error=Numbers.run_exact_batch(consume)
        events_encoder,trades_encoder=Numbers.run_exact_batch(self._window_inputs)
        def encode():
            # 完整组统计按根/窗口只编码一次，市场行只读同一结果。
            events=events_encoder.encode(self.endpoints.previous_prefix[self.root_rows],prefix,at)
            row_events=NativeEvents(events.values[rows],events.fault[rows],events.required[rows],events.enough[rows])
            original_trades=self.encoder.trades
            self.encoder.trades=trades_encoder
            try:
                market=self.encoder.freeze(row_at,self.market_indices,started,memory,self.endpoints,
                applied_prefix=row_prefix,events=row_events)
            finally:self.encoder.trades=original_trades
            decisions=[]
            for root in range(len(at)):
                members=torch.nonzero(rows==root).flatten()
                order=torch.argsort(market.role[members],descending=True,stable=True)
                decisions.append(members[order[0]])
            decision_rows=torch.stack(decisions)
            account,native,actor,cancels=self.account_builder(self.encoder,clock.public,market.rows(decision_rows),clock.bank(clock.projection),
                self.market_indices[decision_rows],self.identities[decision_rows])
            return market,account,native,actor,cancels,decision_rows
        candidate=Numbers.device_exact_batch(encode,word_capacity=self.word_capacity,device=at.device)
        market,account,native,actor,cancels,decision_rows=candidate.value
        account_rows=model_account_rows(native.inputs,rows,market.role)
        frozen=FrozenPolicyInputs100ms.publish(market,account_rows,self.identities,candidate.fits)
        legal=market.role>=0
        row_fit=market.resources_fit&(error==0)&(frozen.fault==0)&frozen.provided.all(1)
        failures=torch.zeros_like(enabled,dtype=torch.int32).scatter_reduce(0,rows,
            (legal&~row_fit).to(torch.int32),reduce='amax')
        accepted=enabled&candidate.fits&(account.fault==0)&(failures==0)
        self.memory=Numbers.run_exact_batch(lambda:coordinate(market.history_memory,self.memory,accepted[rows]))
        self.endpoints=MarketFreezeMemory(torch.where(accepted[rows],market.endpoint_memory.previous_prefix,self.endpoints.previous_prefix),
            torch.where(accepted[rows],market.endpoint_memory.previous_observation_ms,self.endpoints.previous_observation_ms),
            _endpoint_commit(market.endpoint_memory.pm_sides,self.endpoints.pm_sides,accepted[rows]),
            _endpoint_commit(market.endpoint_memory.binance_sides,self.endpoints.binance_sides,accepted[rows]))
        if hasattr(self,'pending_inputs'):
            consumed=int((self.memory.last_prefix+1).min().cpu())
            self.pending_inputs=[encoder for encoder in self.pending_inputs
                if encoder is self.encoder or encoder.shared.group_count>consumed]
        return FrozenActorObservation(frozen,market,account,native,actor,cancels,clock.last_observation,
            clock.source.snapshot(),decision_rows),accepted

    def release_markets(self, ended):
        """模型确认旧观察已消费后释放市场缓存；公共账户与源前沿保持。"""
        from queues import Numbers
        from admission import coordinate
        empty=self.history.empty(len(self.market_indices),self.root_started_at[self.row_roots])
        empty=replace(empty,last_prefix=self.memory.last_prefix,last_cutoff=self.memory.last_cutoff,
            market_ids=self.memory.market_ids)
        self.memory=Numbers.run_exact_batch(lambda:coordinate(empty,self.memory,ended))
        # select保留未退役值，退役值归零；不改根窗口的prefix/time。
        def clear(side):
            if side is None:return None
            selected=ended[:,None].expand_as(side.known).flatten()
            return replace(side,total=side.total.constant(0).select(selected,side.total),
                best=side.best.constant(0).select(selected,side.best),
                known=side.known&~ended[:,None],best_present=side.best_present&~ended[:,None])
        self.endpoints=replace(self.endpoints,pm_sides=Numbers.run_exact_batch(lambda:clear(self.endpoints.pm_sides)),
            binance_sides=Numbers.run_exact_batch(lambda:clear(self.endpoints.binance_sides)))
