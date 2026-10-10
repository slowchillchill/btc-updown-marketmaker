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
        groups=torch.arange(begin,max(begin,end),device=at.device)
        memory,error=Numbers.run_exact_batch(lambda:self.history.consume(groups,self.encoder.delivered_cutoffs[groups],
            self.market_indices,started,self.memory,applied_prefix=row_prefix,enabled=row_enabled))
        def encode():
            # 完整组统计按根/窗口只编码一次，市场行只读同一结果。
            events=self.encoder.events.encode(self.endpoints.previous_prefix[self.root_rows],prefix,at)
            row_events=NativeEvents(events.values[rows],events.fault[rows],events.required[rows],events.enough[rows])
            market=self.encoder.freeze(row_at,self.market_indices,started,memory,self.endpoints,
                applied_prefix=row_prefix,events=row_events)
            decisions=[]
            for root in range(len(at)):
                members=torch.nonzero(rows==root).flatten()
                order=torch.argsort(market.role[members],descending=True,stable=True)
                decisions.append(members[order[0]])
            decision_rows=torch.stack(decisions)
            account,native,actor,cancels=self.account_builder(clock.public,market.rows(decision_rows),clock.bank(clock.projection),
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
