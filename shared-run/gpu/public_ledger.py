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

    submission_bindings: object | None = None  # 冷前缀须安装已公开关联；None不证明完整历史。

    def append_batch(self,bank,batch):
        """原SIM/NOE发射新批后追加一次；不重开账户，不领取或推进可见门。

        bank为同一持续Normalizer的追加后只读存储，原物理row引用须保持。
        消费只在receive处理完整关闭来源；未来已预取材料不可借此提高公开时间。
        """
        from memory import EconomicColumns
        from transport import select_batch_columns
        # 只读取物理人口以分配缓存；空发射不添加padding历史，fault仍保留。
        population=batch.emitted.to(torch.int64).sum(1)
        width=int(population.max().cpu())
        if width==0:
            return replace(self,pending_noe=replace(self.pending_noe,fault=self.pending_noe.fault | batch.fault),
                waiting=replace(self.waiting,fault=self.waiting.fault | batch.fault),
                ledger=replace(self.ledger,fault=self.ledger.fault | batch.fault))
        order=(~batch.emitted).to(torch.int64).argsort(dim=1,stable=True)[:,:width]
        batch=select_batch_columns(batch,order)
        incoming=EconomicColumns.from_projection_batches([batch])
        ledger=self.ledger.append_columns(bank,incoming)
        return replace(self,pending_noe=self.pending_noe.append_batch(bank,batch),
            ledger=replace(ledger,fault=ledger.fault | batch.fault),
            waiting=self.waiting.append_columns(incoming))

    def receive_first(self, bank, gates):
        """首次公开领取/Registry接纳候选；不在本事务里重新领取Ledger waiting。

        当前动态人口装箱仍在冷边界。固定存储消费者先接纳本候选，再沿同一
        waiting/首次visibility调用Ledger设备扫描；不是提前推进receive/public门。
        """
        pending, closed = self.pending_noe.take_closed(gates)
        batches = self.ledger.columns.projection_batches
        # 物理领取rank按真实emitted人口递增；不是private/delivery业务序号。
        rank = self.waiting.next_rank[:, None] + closed.emitted.to(torch.int64).cumsum(1)-1
        events, bindings = public_rows_from_closed(bank, batches, closed, rank)
        targets, completions = cancellation_rows_from_closed(bank, batches, closed)
        from commitments import SubmissionBindings
        old_bindings = self.submission_bindings
        if old_bindings is None:
            old_bindings = SubmissionBindings.empty(closed.emitted.size(0),closed.emitted.device,prefix_known=False)
        submission_bindings = old_bindings.receive(bindings)
        registry = self.registry.receive(events)
        registry = registry.receive(cancellation_order_events(targets, rank))
        cancellations = None if self.cancellations is None else self.cancellations.receive(completions)
        cancellation_fault = completions.fault if cancellations is None else cancellations.completions.fault
        # Registry只在首次领取路径更新。Ledger再试始终引用原waiting和首次visibility。
        waiting = self.waiting.accept(self.ledger.columns, closed)
        # 原Registry接纳失败时不得继续提交同根Ledger；其它独立根仍能推进。
        ledger_input=replace(self.ledger,fault=self.ledger.fault | registry.events.fault | targets.fault | cancellation_fault | submission_bindings.fault)
        fault = closed.fault | registry.events.fault | targets.fault | cancellation_fault | waiting.fault | ledger_input.fault | submission_bindings.fault
        return replace(self, pending_noe=pending, registry=registry, waiting=waiting, ledger=ledger_input, cancellations=cancellations, submission_bindings=submission_bindings), PublicDelivery(
            closed, bindings, targets, completions, fault)

    def receive(self, bank, gates, association_available, *, passes=None):
        """兼容冷候选入口；首次接纳与Ledger扫描沿同一原公共路径。"""
        state, delivery = self.receive_first(bank, gates)
        ledger, waiting = state.waiting.drain(state.ledger, association_available, passes=passes)
        fault = delivery.fault | waiting.fault | ledger.fault
        return replace(state, ledger=ledger, waiting=waiting), replace(delivery, fault=fault)

    def drain_captured(self, captured):
        """首次接纳后，原生设备循环提交当前waiting；资源不足保成功前缀。

        调用方在冷资源边界读取captured的字/目录需求，增长后保cursor续跑。
        此函数不领取、不observe Registry、不提高双门；结果快照不借活动地址。
        """
        storage=captured.storage
        if storage.source_ledger is not self.ledger or storage.source_waiting is not self.waiting:
            raise ValueError('设备扫描须绑定当前原Ledger/waiting来源')
        captured.replay()
        committed=storage.snapshot()
        return replace(self,ledger=committed.ledger,waiting=committed.waiting)

    def freeze_account(self, intents, cutoff, *, submitted_legs=None):
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
        if self.submission_bindings is not None:
            fault |= self.submission_bindings.fault
        commitments = None
        if submitted_legs is not None:
            from commitments import freeze_leg_commitments
            if self.submission_bindings is None:
                raise ValueError('冻结完整请求承诺需安装原公开Submission关联前缀')
            commitments = freeze_leg_commitments(submitted_legs,self.submission_bindings,intents,orders)
            fault |= commitments.fault
        if requests is not None:
            fault |= requests.fault
        return FrozenPublicAccount(cutoff.clone(), self.ledger, orders, sources, funds, pending, requests, fault, commitments)


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
    commitments: object | None = None  # 完整receipt逐腿读面；None不是Known空承诺集合。
