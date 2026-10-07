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
        # Registry只在首次领取路径更新。Ledger再试始终引用原waiting和首次visibility。
        waiting = self.waiting.accept(self.ledger.columns, closed)
        ledger, waiting = waiting.drain(self.ledger, association_available, passes=passes)
        fault = closed.fault | registry.events.fault | targets.fault | completions.fault | waiting.fault | ledger.fault
        return replace(self, pending_noe=pending, registry=registry, waiting=waiting, ledger=ledger), PublicDelivery(
            closed, bindings, targets, completions, fault)
