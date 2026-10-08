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


class ContinuousActorInputs:
    """提交每个完整组一次，成功冻结后承接原history/endpoint候选记忆。

    account_builder沿公共Registry/Bindings读取同Ck完整账户/Actor/184。
    不读取SIM未来订单，也不在这里提前绑定权重或计算模型。
    """
    def __init__(self,encoder,history,memory,endpoints,market_indices,root_started_at,identities,account_builder,*,word_capacity):
        self.encoder=encoder;self.history=history;self.memory=memory;self.endpoints=endpoints
        self.market_indices=market_indices;self.root_started_at=root_started_at;self.identities=identities
        self.account_builder=account_builder;self.word_capacity=word_capacity

    def freeze(self,clock,at,enabled):
        from queues import Numbers
        from admission import coordinate
        from input_100ms import MarketFreezeMemory,_endpoint_commit
        prefix=clock.source.prefix_at(self.encoder.shared,at)
        begin=int((self.memory.last_prefix+1).min().cpu());end=int(prefix[enabled].max().cpu())+1
        groups=torch.arange(begin,max(begin,end),device=at.device)
        memory,error=Numbers.run_exact_batch(lambda:self.history.consume(groups,self.encoder.delivered_cutoffs[groups],
            self.market_indices,self.root_started_at,self.memory,applied_prefix=prefix,enabled=enabled))
        def encode():
            market=self.encoder.freeze(at,self.market_indices,self.root_started_at,memory,self.endpoints,applied_prefix=prefix)
            account,native,actor,cancels=self.account_builder(clock.public,market,clock.bank(clock.projection))
            return market,account,native,actor,cancels
        candidate=Numbers.device_exact_batch(encode,word_capacity=self.word_capacity,device=at.device)
        market,account,native,actor,cancels=candidate.value
        frozen=FrozenPolicyInputs100ms.publish(market,native.inputs,self.identities,candidate.fits)
        accepted=enabled&candidate.fits&market.resources_fit&(error==0)&(frozen.fault==0)&frozen.provided.all(1)
        self.memory=Numbers.run_exact_batch(lambda:coordinate(market.history_memory,self.memory,accepted))
        self.endpoints=MarketFreezeMemory(torch.where(accepted,market.endpoint_memory.previous_prefix,self.endpoints.previous_prefix),
            torch.where(accepted,market.endpoint_memory.previous_observation_ms,self.endpoints.previous_observation_ms),
            _endpoint_commit(market.endpoint_memory.pm_sides,self.endpoints.pm_sides,accepted),
            _endpoint_commit(market.endpoint_memory.binance_sides,self.endpoints.binance_sides,accepted))
        return FrozenActorObservation(frozen,market,account,native,actor,cancels,clock.last_observation,clock.source.snapshot()),accepted
