"""唯一公开NOE领取→Registry接纳→Ledger待消费的设备协作入口。

只处理已提供的真实双时间门，不驱动SIM/public时钟，不重新领取旧waiting。
输入bank/批列共享只读；NOE、Registry与Ledger保留各自原成功前缀。
跨块迁移、开户/登记、请求登记和活动恢复仍由完整宿主负责。
"""
from dataclasses import dataclass, replace
import torch
from registry_projection import public_rows_from_closed
from cancel_projection import cancellation_rows_from_closed, cancellation_order_events


@dataclass(frozen=True)
class PublicDelivery:
    closed: object
    submission_bindings: object
    cancellation_targets: object
    cancellation_completions: object
    fault: torch.Tensor


@dataclass(frozen=True)
class PublicLedgerState:
    pending_noe: object
    registry: object
    waiting: object
    ledger: object
    cancellations: object | None = None  # 原已登记请求人口；None不是Known空请求集合。

    def receive(self, bank, gates, association_available, *, passes=None):
        """资源重试只重算整个纯候选；调用方成功后接纳返回状态和完整来源引用。"""
        pending, closed = self.pending_noe.take_closed(gates)
        batches = self.ledger.columns.projection_batches
        # 物理领取rank按真实emitted人口递增；不是private/delivery业务序号。
        rank = self.waiting.next_rank[:, None] + closed.emitted.to(torch.int64).cumsum(1)-1
        events, bindings = public_rows_from_closed(bank, batches, closed, rank)
        targets, completions = cancellation_rows_from_closed(bank, batches, closed)
        registry = self.registry.receive(events)
        registry = registry.receive(cancellation_order_events(targets, rank))
        cancellations = None if self.cancellations is None else self.cancellations.receive(completions)
        cancellation_fault = completions.fault if cancellations is None else cancellations.completions.fault
        # Registry只在首次领取路径更新。Ledger再试始终引用原waiting和首次visibility。
        waiting = self.waiting.accept(self.ledger.columns, closed)
        # 原Registry接纳失败时不得继续提交同根Ledger；其它独立根仍能推进。
        ledger_input=replace(self.ledger,fault=self.ledger.fault | registry.events.fault | targets.fault | cancellation_fault)
        ledger, waiting = waiting.drain(ledger_input, association_available, passes=passes)
        fault = closed.fault | registry.events.fault | targets.fault | cancellation_fault | waiting.fault | ledger.fault
        return replace(self, pending_noe=pending, registry=registry, waiting=waiting, ledger=ledger, cancellations=cancellations), PublicDelivery(
            closed, bindings, targets, completions, fault)

    def freeze_account(self, intents, cutoff):
        """在真实公开推进/apply_closed之后一次冻结，不领取反馈或读取SIM。

        cutoff须为此状态实际ActionBefore的逻辑时间；压紧状态不支持任意历史回读。
        旧冻结引用保持原值，后续候选替换不会刷新资金、来源或待消费人口。
        """
        from queues import Numbers
        roots = self.ledger.fault.numel()
        if cutoff.shape != (roots,) or cutoff.dtype != torch.int64 or cutoff.device != self.ledger.fault.device:
            raise TypeError('账户冻结需同设备int64[roots]的真实公开cutoff')
        orders = self.registry.freeze_orders(intents, cutoff)
        sources = self.registry.freeze_order_sources(intents, cutoff)
        funds = self.ledger.funds()
        pending = Numbers.ExactInt.from_i64_tensor((self.waiting.received & ~self.waiting.consumed).to(torch.int64).sum(1))
        requests = None if self.cancellations is None else self.cancellations.freeze_global(cutoff)
        fault = self.ledger.fault | self.waiting.fault | orders.fault | sources.fault | funds.fault | pending.fault
        if requests is not None:
            fault |= requests.fault
        return FrozenPublicAccount(cutoff.clone(), self.ledger, orders, sources, funds, pending, requests, fault)


@dataclass(frozen=True)
class FrozenPublicAccount:
    cutoff: torch.Tensor
    ledger: object  # 同一次不可变候选前缀；不能替换为模型完成时的最新Ledger。
    orders: object
    order_sources: object
    funds: object
    pending_noe_count: object
    cancellations: object | None
    fault: torch.Tensor
