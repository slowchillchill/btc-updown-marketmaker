"""唯一公开NOE领取→Registry接纳→Ledger待消费的设备协作入口。

只处理已提供的真实双时间门，不驱动SIM/public时钟，不重新领取旧waiting。
输入bank/批列共享只读；NOE、Registry与Ledger保留各自原成功前缀。
跨块迁移、开户/登记、请求登记和活动恢复仍由完整宿主负责。
"""
from dataclasses import dataclass, replace
import torch
from registry_projection import public_rows_from_closed,resolve_closed_public_rows
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
    cancellation_target_history: object | None = None  # 首次领取目标与稳定身份轴；None保持未安装。

    def append_batch(self,bank,batch):
        """原SIM/NOE发射新批后追加一次；不重开账户，不领取或推进可见门。

        bank为同一持续Normalizer的追加后只读存储，原物理row引用须保持。
        消费只在receive处理完整关闭来源；未来已预取材料不可借此提高公开时间。
        """
        return self.append_batches(bank,(batch,))

    def append_batches(self,bank,batches):
        """完整阶段的纯追加候选；一次物理尺寸出口，不领取、不安装权威状态。

        Pending/Economic/Lifecycle先按输入张量上界在设备准备；只用实际前缀
        接原目录增长。自然批保各自来源与列，不把临时尾区发布为事实。
        """
        if not batches:return self
        from memory import EconomicColumns
        from admission_lifecycle import LifecycleColumns
        from queues import ExactInt
        from transport import stage_batch_layout,select_batch_columns,concatenate_root_fields
        layout=stage_batch_layout(batches);selected=layout.batches
        roots=self.pending_noe.active.size(0);upper=layout.order.size(1)
        batch_fault=torch.stack([batch.fault for batch in batches],1)
        from consumption import fault_rows
        fault=fault_rows(batch_fault,roots,len(batches))
        zero_fault=fault_rows(torch.where(layout.widths[None,:]==0,batch_fault,0),roots,len(batches))
        if upper:
            original=EconomicColumns.from_projection_batches(selected)
            life=LifecycleColumns.from_projection(bank,original).select_records(layout.order)
            incoming=original.select_records(layout.order)
            private=concatenate_root_fields([batch.private_sequences for batch in selected],roots,
                [batch.rows.size(1) for batch in selected])
            pending=type(self.pending_noe).from_columns(
                torch.cat([batch.emitted for batch in selected],1),
                torch.cat([batch.received_at for batch in selected],1),private,
                ExactInt.from_i64_tensor(bank.source_order[original.row_refs.flatten(),3]),
                original.row_refs,self.pending_noe.delivered_through).select_records(layout.order)
        # 只读一次各自然批实际物理宽；原动态类型仅使用已准备的前缀。
        widths=layout.widths.cpu().tolist();used=sum(widths)
        if not used:
            return replace(self,pending_noe=replace(self.pending_noe,fault=self.pending_noe.fault|fault),
                waiting=replace(self.waiting,fault=self.waiting.fault|fault),
                ledger=replace(self.ledger,fault=self.ledger.fault|fault))
        compact=tuple(select_batch_columns(batch,torch.arange(width,device=batch.rows.device)[None,:].expand(roots,-1))
            for batch,width in zip(selected,widths,strict=True) if width)
        prefix=torch.arange(used,device=layout.order.device)[None,:].expand(roots,-1)
        incoming=incoming.select_records(prefix,batches=compact);life=life.select_records(prefix)
        pending=pending.select_records(prefix)
        ledger=self.ledger.append_columns(bank,incoming,lifecycle=life)
        return replace(self,pending_noe=replace(self.pending_noe.append_records(pending),
            fault=self.pending_noe.fault|pending.fault|fault),
            ledger=replace(ledger,fault=ledger.fault|fault),
            waiting=replace(self.waiting.append_columns(incoming),fault=self.waiting.fault|zero_fault))

    def receive_first(self, bank, gates):
        """首次公开领取/Registry接纳候选；不在本事务里重新领取Ledger waiting。

        当前动态人口装箱仍在冷边界。固定存储消费者先接纳本候选，再沿同一
        waiting/首次visibility调用Ledger设备扫描；不是提前推进receive/public门。
        """
        pending, closed = self.pending_noe.take_closed(gates)
        batches = self.ledger.columns.projection_batches
        # 物理领取rank按真实emitted人口递增；不是private/delivery业务序号。
        rank = self.waiting.next_rank[:, None] + closed.emitted.to(torch.int64).cumsum(1)-1
        source_rows=resolve_closed_public_rows(bank,batches,closed) if closed.record_refs.size(1) else None
        events, bindings = public_rows_from_closed(bank, batches, closed, rank,source_rows=source_rows)
        targets, completions = cancellation_rows_from_closed(bank, batches, closed,source_rows=source_rows)
        from commitments import SubmissionBindings
        old_bindings = self.submission_bindings
        if old_bindings is None:
            old_bindings = SubmissionBindings.empty(closed.emitted.size(0),closed.emitted.device,prefix_known=False)
        submission_bindings = old_bindings.receive(bindings)
        registry = self.registry.receive(events)
        registry = registry.receive(cancellation_order_events(targets, rank))
        cancellations = None if self.cancellations is None else self.cancellations.receive(completions)
        history = self.cancellation_target_history
        if history is not None:
            # 同次已领取Registry才建立身份；不从SIM订单或未来Accepted补人口。
            identities,present=registry.order_population()
            history=history.with_orders(identities,present).receive(targets)
        cancellation_fault = completions.fault if cancellations is None else cancellations.completions.fault
        if history is not None:
            cancellation_fault = cancellation_fault | history.targets.fault
        # Registry只在首次领取路径更新。Ledger再试始终引用原waiting和首次visibility。
        waiting = self.waiting.accept(self.ledger.columns, closed,source_rows=source_rows)
        # 原Registry接纳失败时不得继续提交同根Ledger；其它独立根仍能推进。
        ledger_input=replace(self.ledger,fault=self.ledger.fault | registry.events.fault | targets.fault | cancellation_fault | submission_bindings.fault)
        fault = closed.fault | registry.events.fault | targets.fault | cancellation_fault | waiting.fault | ledger_input.fault | submission_bindings.fault
        return replace(self, pending_noe=pending, registry=registry, waiting=waiting, ledger=ledger_input, cancellations=cancellations, submission_bindings=submission_bindings,cancellation_target_history=history), PublicDelivery(
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

    def reserve_selected(self, action, enabled):
        """Execute边界消费原完整选择；读取此刻实际Ledger，不读取旧冻结资金。"""
        from action_selection import reserve_selected
        return reserve_selected(self,action,enabled)

    def freeze_account(self, intents, cutoff, *, submitted_legs=None,
                       order_formation=None,cancel_markets=None,request_identity_order=None):
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
        directory=objects=None
        if order_formation is None and cancel_markets is not None and submitted_legs is not None:
            order_formation=self.registry.freeze_order_formation(intents,submitted_legs,cutoff,cancel_markets.market_present)
        if order_formation is not None:
            if submitted_legs is None or cancel_markets is None or request_identity_order is None:
                raise ValueError('撤单冻结需要原腿、公开形成来源、市场列和请求身份字典序')
            from cancel_objects import assemble_cancel_directory,freeze_cancel_objects
            directory,pair_fault=assemble_cancel_directory(submitted_legs,self.submission_bindings,
                intents,cancel_markets,request_identity_order)
            objects=freeze_cancel_objects(orders,order_formation,directory)
            fault=fault | pair_fault | objects.fault
        return FrozenPublicAccount(cutoff.clone(), self.ledger, orders, sources, funds, pending, requests,
            fault, commitments,intents,directory,objects)


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
    intent_orders: object | None = None
    cancel_directory: object | None = None
    cancel_objects: object | None = None
