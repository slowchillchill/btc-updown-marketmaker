use crate::*;
use contracts::{ConditionId, ExactDecimal, ExecutionReadValueV1, ModelIntentV1};
use execution_normalization::{MemoryLogicalIdentityV1, NoeConsumedRecordRef};
use ledger::*;
use sim_system::{
    execution::*,
    public_consumption::{PublicObservationFrameV1, TrustedGroupDecisionCutoffV1},
};
use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

#[derive(Clone)]
struct RequestFunds {
    id: LedgerRequestIdentityV1,
    condition: ConditionId,
    state: ReservationStatusV1,
    principal: ExactDecimal,
    fee: ExactDecimal,
    seq: u64,
}
#[derive(Clone)]
struct OrderFunds {
    id: MemoryLogicalIdentityV1,
    principal: ExecutionReadValueV1<ExactDecimal>,
    fee: ExecutionReadValueV1<ExactDecimal>,
    seq: Option<u64>,
}
/// 持有同一组必要标量和原Arc引用；后续动作不能改写本组读取。
pub struct ActionBeforeRead<R, G = SharedExecutionRegistry<R>, A = OwnedActionAccountV2> {
    pub(crate) account: A,
    pending_noe_record_count: usize,
    cutoff: TrustedGroupDecisionCutoffV1,
    registry: G,
    completion: PhantomData<R>,
    requests: Vec<RequestFunds>,
    orders: Vec<OrderFunds>,
}
impl<R: Clone + std::fmt::Debug + Eq, G: Borrow<SharedExecutionRegistry<R>>, A: ActionAccountReadV2>
    ActionBeforeRead<R, G, A>
{
    /// 已领取、因依赖未满足而尚待 Ledger 应用的原记录数；零不证明未来事实已齐。
    pub fn pending_noe_record_count(&self) -> usize {
        self.pending_noe_record_count
    }
    pub fn economics(&self) -> &AccountEconomicsReadV3 {
        &self.account.full().economics
    }
    pub fn cash_available(&self) -> rust_decimal::Decimal {
        self.account.cash_available()
    }
    pub fn profit_and_equity(
        &self,
    ) -> (
        contracts::economics::EconomicRational,
        ExecutionReadValueV1<contracts::economics::EconomicRational>,
    ) {
        self.account.profit_and_equity()
    }
    /// 同截止按原请求读取相关原owner成交；缺失事实保持Unavailable。
    pub fn request_trade_economics(
        &self,
        request: &str,
    ) -> ExecutionReadValueV1<Vec<TradeEconomicReadV1<'_>>> {
        self.account.request_trade_economics(request)
    }
    pub fn market_economics(&self, condition: &ConditionId) -> Option<MarketEconomicNumbersV1> {
        self.account.market(condition)
    }
    pub fn market_funds(&self, condition: &ConditionId) -> MarketFundsNumbersV1 {
        self.account.market_funds(condition)
    }
    pub fn cutoff(&self) -> &TrustedGroupDecisionCutoffV1 {
        &self.cutoff
    }
    pub fn ledger(&self) -> &LedgerProjectionV2 {
        &self.account.full().ledger
    }
    pub fn orders(&self) -> impl Iterator<Item = ExecutionOrderRead<'_, R>> {
        self.registry.borrow().unfinished_orders()
    }
    pub fn commitments(&self) -> impl Iterator<Item = IntentCommitmentReadV1<'_>> {
        self.registry.borrow().all_commitments()
    }
    pub fn cancel_objects(&self) -> ExecutionReadValueV1<Vec<CancelObjectRead<'_, R>>> {
        self.registry.borrow().cancel_objects()
    }
    pub fn cancel_objects_in<'a>(
        &'a self,
        current: Option<&'a ConditionId>,
        old: &'a [ConditionId],
    ) -> ExecutionReadValueV1<Vec<CancelObjectRead<'a, R>>> {
        self.registry
            .borrow()
            .cancel_objects_in(current.into_iter().chain(old))
    }
    pub fn request_funds(&self) -> impl Iterator<Item = RequestFundsReadV1<'_>> {
        self.requests.iter().map(|r| RequestFundsReadV1 {
            request_identity: &r.id,
            condition_id: &r.condition,
            reservation_state: r.state,
            principal_pusd: r.principal.clone(),
            immediate_fee_pusd: r.fee.clone(),
            last_ledger_entry_seq: r.seq,
        })
    }
    pub fn order_funds(&self) -> impl Iterator<Item = OrderFundsReadV1<'_>> {
        self.orders.iter().map(|r| OrderFundsReadV1 {
            order_identity: &r.id,
            principal_encumbrance_pusd: r.principal.clone(),
            immediate_fee_encumbrance_pusd: r.fee.clone(),
            last_ledger_entry_seq: r.seq,
        })
    }
    /// 默认拥有式消费者仍取得完整独立冻结；借用计算不调用此转换。
    pub fn to_owned_read(&self) -> ActionBeforeRead<R> {
        ActionBeforeRead {
            account: self.account.full().clone(),
            pending_noe_record_count: self.pending_noe_record_count,
            cutoff: self.cutoff.clone(),
            registry: self.registry.borrow().action_snapshot(),
            requests: self.requests.clone(),
            orders: self.orders.clone(),
            completion: PhantomData,
        }
    }
}
/// 保留缺直接前驱的原领取引用；没有第二业务归约或跳号/超时政策。
pub struct SharedExecution<P: SharedOrderEvidence> {
    ledger: LedgerWriterV1,
    registry: SharedExecutionRegistry<P::Completion>,
    observation: sim_system::execution::MemoryNoeObservation<P>,
    pending: Vec<NoeConsumedRecordRef<P::Completion>>,
    pending_retry_after: Option<u64>,
}
impl<P: SharedOrderEvidence> SharedExecution<P> {
    /// 调用方已经在原运行后台绑定此新Ledger的记录接收器。
    pub fn from_opening(
        mut ledger: LedgerWriterV1,
        observation: sim_system::execution::MemoryNoeObservation<P>,
    ) -> Result<Option<Self>, LedgerErrorV1> {
        let Some(opening) = observation.opening_record() else {
            return Ok(None);
        };
        P::apply(&mut ledger, &opening)?;
        let registry = SharedExecutionRegistry::new(Arc::new(opening.context().clone()));
        Ok(Some(Self {
            ledger,
            registry,
            observation,
            pending: Vec::new(),
            pending_retry_after: None,
        }))
    }
    pub fn reserve_action_v2(
        &mut self,
        intents: &[ReserveIntentV1],
    ) -> Result<Vec<LedgerReservationOutcomeV1>, LedgerErrorV1> {
        self.ledger.try_reserve_action_v2(intents)
    }
    /// 原稳定请求身份的发送登记；不改变其他 execute 消费者的默认行为。
    pub fn record_submission_started(
        &mut self,
        event: &LedgerLocalEventIdentityV1,
        request: &LedgerRequestIdentityV1,
        item: &LedgerSubmissionItemIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ledger.record_submission_started(event, request, item)
    }
    pub(crate) fn record_prepared_submission_started(
        &mut self,
        event: &LedgerLocalEventIdentityV1,
        request: &LedgerRequestIdentityV1,
        item: &LedgerSubmissionItemIdentityV1,
        platform_order_id: &str,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        use execution_normalization::{MemoryEconomicFactReferenceV1, NormalizationIdentity};
        let account = Arc::new(self.registry.context().account_identity.clone());
        let key = execution_normalization::SourceNeutralEntityKeyV1::Order {
            source_order_id: platform_order_id.to_owned(),
        };
        let order = <MemoryLogicalIdentityV1 as NormalizationIdentity<
            MemoryEconomicFactReferenceV1,
        >>::entity(&account, &key)
        .expect("原NOE账户内的规范订单身份");
        self.ledger
            .record_prepared_submission_started(event, request, item, &order)
    }
    pub(crate) fn release_known_unsent(
        &mut self,
        event: &LedgerLocalEventIdentityV1,
        request: &LedgerRequestIdentityV1,
    ) -> Result<LedgerApplyOutcomeV1, LedgerErrorV1> {
        self.ledger.release_known_unsent(event, request)
    }
    /// 已领取但仍缺经济前驱的原 NOE 引用数量。
    pub fn pending_noe_record_count(&self) -> usize {
        self.pending.len()
    }
    pub fn ledger(&self) -> &LedgerWriterV1 {
        &self.ledger
    }
    pub fn registry(&self) -> &SharedExecutionRegistry<P::Completion> {
        &self.registry
    }
    /// false 表示仍有待经济应用的原记录，不阻止实到观察与模型组。
    pub fn apply_closed(&mut self) -> Result<bool, LedgerErrorV1> {
        let mut received = false;
        for record in self.observation.take_closed_records() {
            received = true;
            self.registry.observe(&record);
            self.pending.push(record);
        }
        if self.pending.is_empty() {
            return Ok(true);
        }
        if !received && self.pending_retry_after == Some(self.ledger.applied_entry_sequence()) {
            return Ok(false);
        }
        self.pending_retry_after = None;
        loop {
            let mut waiting = Vec::new();
            let mut applied = false;
            for record in std::mem::take(&mut self.pending) {
                match P::apply(&mut self.ledger, &record)? {
                    LedgerMemoryApplyOutcomeV1::Applied(
                        LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. },
                    )
                    | LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation => {
                        waiting.push(record)
                    }
                    _ => {
                        applied = true;
                    }
                }
            }
            self.pending = waiting;
            if self.pending.is_empty() {
                return Ok(true);
            }
            if !applied {
                self.pending_retry_after = Some(self.ledger.applied_entry_sequence());
                return Ok(false);
            }
        }
    }
    /// 与模型帧无关：所有本组事实应用后才驱动原结算宿主。
    pub fn advance_settlement(
        &mut self,
        coordinator: &mut SettlementCoordinatorV1,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<bool, SharedSettlementErrorV1> {
        if !self
            .apply_closed()
            .map_err(SharedSettlementErrorV1::Ledger)?
        {
            return Ok(false);
        }
        let Some(mut decision) = driver.decision() else {
            return Ok(false);
        };
        let Some(read) = self.ledger.economic_read() else {
            return Ok(false);
        };
        let opening = self.observation.opening_record().expect("共享根已经开户");
        coordinator
            .advance(
                &self.registry,
                &read,
                &opening.context().source.account_instance_key,
                &mut decision,
            )
            .map_err(SharedSettlementErrorV1::Sim)?;
        Ok(true)
    }
    pub(crate) fn prepare_action_group(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<TrustedGroupDecisionCutoffV1>, LedgerErrorV1> {
        self.apply_closed()?;
        // 领取后重新取得真实 decision；经济依赖仍保留原引用等待，不复用旧 cutoff。
        let Some(decision) = driver.decision() else {
            return Ok(None);
        };
        let frames = decision.frames();
        if frames.is_empty() {
            return Ok(None);
        }
        let cutoff = frames[0].decision_cutoff.clone();
        self.observe_group(&frames);
        Ok(Some(cutoff))
    }
    /// 接纳本组已受信的公共规则；不应用反馈，不构造模型输入或发出请求。
    pub fn observe_group(&mut self, frames: &[PublicObservationFrameV1<'_>]) {
        for frame in frames {
            if self.registry.observe_market(&frame.rule_context) {
                self.pending_retry_after = None;
            }
        }
    }
    pub fn action_before(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<ActionBeforeRead<P::Completion>>, LedgerErrorV1> {
        let Some(cutoff) = self.prepare_action_group(driver)? else {
            return Ok(None);
        };
        let decision = driver.decision().expect("当前组");
        let frames = decision.frames();
        Ok(self.freeze_action_before(cutoff, &frames))
    }
    /// 从当前已应用前缀冻结读取；调用方先显式推进事实及接纳公共组。
    pub fn freeze_action_before(
        &self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'_>],
    ) -> Option<ActionBeforeRead<P::Completion>> {
        self.read_action_before(self.registry.action_snapshot(), cutoff, frames)
    }
    /// 同步调用期间借用原登记；Rust 借用阻止该范围内推进事实。
    pub fn borrow_action_before<'a>(
        &'a self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'_>],
    ) -> Option<ActionBeforeRead<P::Completion, &'a SharedExecutionRegistry<P::Completion>>> {
        self.read_action_before(&self.registry, cutoff, frames)
    }
    fn read_action_before<G: Borrow<SharedExecutionRegistry<P::Completion>>>(
        &self,
        registry: G,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'_>],
    ) -> Option<ActionBeforeRead<P::Completion, G>> {
        let economic = self.ledger.economic_read()?;
        let request_trades = crate::necessary::freeze_request_trades(
            &self.ledger,
            &crate::necessary::request_trade_identities(&self.ledger, &self.registry),
        );
        let (requests, orders) = self.read_action_funds(registry.borrow());
        Some(ActionBeforeRead {
            pending_noe_record_count: self.pending.len(),
            account: OwnedActionAccountV2 {
                request_trades,
                economics: AccountEconomicsReadV3::from_ledger_and_observations(
                    &economic,
                    frames.iter().map(|f| f.observation),
                ),
                ledger: economic.into_base(),
            },
            cutoff,
            registry,
            completion: PhantomData,
            requests,
            orders,
        })
    }
    pub(crate) fn borrow_necessary_action_before<'a>(
        &'a self,
        cutoff: TrustedGroupDecisionCutoffV1,
        frames: &[PublicObservationFrameV1<'a>],
        driver: Option<&'a PublicConsumptionStateV1>,
    ) -> Option<
        ActionBeforeRead<
            P::Completion,
            &'a SharedExecutionRegistry<P::Completion>,
            BorrowedActionAccountV2<'a>,
        >,
    > {
        let registry = &self.registry;
        let (requests, orders) = self.read_action_funds(registry);
        Some(ActionBeforeRead {
            account: BorrowedActionAccountV2::new(
                &self.ledger,
                crate::necessary::request_trade_identities(&self.ledger, registry),
                frames,
                driver,
            )?,
            pending_noe_record_count: self.pending.len(),
            cutoff,
            registry,
            completion: PhantomData,
            requests,
            orders,
        })
    }
    fn read_action_funds(
        &self,
        registry: &SharedExecutionRegistry<P::Completion>,
    ) -> (Vec<RequestFunds>, Vec<OrderFunds>) {
        let funds = self.ledger.funds_read_v1();

        let required_requests: std::collections::BTreeSet<_> = registry
            .unfinished_orders()
            .filter_map(|o| o.origin)
            .filter_map(|origin| {
                funds.submission_request(&LedgerSubmissionItemIdentityV1 {
                    global_request_id: origin.intent.global_request_id().into(),
                    batch_item_ordinal: origin.batch_item_ordinal,
                })
            })
            .collect();
        let request_ids: std::collections::BTreeSet<_> = funds
            .active_requests()
            .map(|r| r.request_identity)
            .chain(required_requests)
            .collect();
        let requests = request_ids
            .into_iter()
            .filter_map(|id| funds.request(id))
            .map(|r| RequestFunds {
                id: r.request_identity.clone(),
                condition: r.condition_id.clone(),
                state: r.reservation_state,
                principal: r.principal_pusd,
                fee: r.immediate_fee_pusd,
                seq: r.last_ledger_entry_seq,
            })
            .collect();
        let orders = registry
            .unfinished_orders()
            .filter_map(|o| funds.order(o.order_identity))
            .map(|r| OrderFunds {
                id: r.order_identity.clone(),
                principal: r.principal_encumbrance_pusd,
                fee: r.immediate_fee_encumbrance_pusd,
                seq: r.last_ledger_entry_seq,
            })
            .collect();
        (requests, orders)
    }
    pub(crate) fn observe_public_rules(&mut self, driver: &PublicConsumptionStateV1) {
        for rules in driver.newly_public_rule_contexts() {
            if self.registry.observe_market(&rules) {
                self.pending_retry_after = None;
            }
        }
        // 原登记只接纳消费到的公开市场。首次读取/跳组后补齐元数据，平时仅处理本组新登记。
        if self.registry.observed_market_count() < driver.public_observation_count() {
            for rules in driver.public_rule_contexts() {
                if self.registry.observe_market(rules) {
                    self.pending_retry_after = None;
                }
            }
        }
    }
    /// 模型已选原值和已有预留意图；整动作预留、提交开始与原执行只发生一次。
    /// 非模型消费者仍可调用原 execute，不强制增加预留策略。
    pub async fn execute_reserved(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        intent: Option<Arc<ModelIntentV1>>,
        reserves: &[ReserveIntentV1],
    ) -> Result<Option<MutationRequestReceiptV2>, SharedActionErrorV1> {
        // 已冻结但尚未登记SIM的请求仍复用首次原意图；首次准备/预留失败不创建冻结状态。
        let intent = intent.map(|incoming| {
            self.registry
                .frozen_intent(&context.global_request_id)
                .map_or(incoming, |frozen| frozen.intent_arc())
        });
        let prepared = intent
            .as_ref()
            .map(|intent| {
                intent_bridge::IntentBridge::bind_sim_group(decision)
                    .prepare(context.clone(), intent)
            })
            .transpose()
            .map_err(|failure| SharedActionErrorV1::Sim(failure.error))?;
        if !reserves.is_empty() {
            let outcomes = self
                .reserve_action_v2(reserves)
                .map_err(SharedActionErrorV1::Ledger)?;
            if outcomes
                .iter()
                .any(|v| !matches!(v, LedgerReservationOutcomeV1::Reserved { .. }))
            {
                return Err(SharedActionErrorV1::Reservation(outcomes));
            }
            for (ordinal, reserve) in reserves.iter().enumerate() {
                let item = LedgerSubmissionItemIdentityV1 {
                    global_request_id: context.global_request_id.clone(),
                    batch_item_ordinal: ordinal as u16,
                };
                let event = LedgerLocalEventIdentityV1(
                    serde_json::to_string(&("submission-started", &item)).expect("身份序列化"),
                );
                if let Some(id) = prepared
                    .as_ref()
                    .and_then(|p| p.order_id(item.batch_item_ordinal))
                {
                    self.record_prepared_submission_started(
                        &event,
                        &reserve.request_identity,
                        &item,
                        &id,
                    )
                    .map_err(SharedActionErrorV1::Ledger)?;
                } else {
                    self.record_submission_started(&event, &reserve.request_identity, &item)
                        .map_err(SharedActionErrorV1::Ledger)?;
                }
            }
        }
        if let (Some(prepared), Some(intent)) = (prepared, intent) {
            self.execute_prepared_with_request_effect(decision, context, intent, prepared)
                .map(Some)
                .map_err(|failure| SharedActionErrorV1::Sim(failure.error))
        } else {
            Ok(None)
        }
    }
    /// None为宿主已作出的NoTrade；不制造意图，不调用Bridge。
    pub async fn execute(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        intent: Option<Arc<ModelIntentV1>>,
    ) -> Result<Option<MutationRequestReceiptV2>, SimRunActorErrorV2> {
        self.execute_with_request_effect(decision, context, intent)
            .await
            .map_err(|failure| failure.error)
    }
    pub(crate) fn execute_prepared_with_request_effect(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        intent: Arc<ModelIntentV1>,
        prepared: PreparedBusinessRequestV1,
    ) -> Result<MutationRequestReceiptV2, RequestFailureV1> {
        let frozen = self
            .registry
            .freeze(context.global_request_id.clone(), intent);
        let receipt = intent_bridge::IntentBridge::bind_sim_group(decision)
            .execute_prepared_with_request_effect(prepared)?;
        self.registry.record_sim_receipt(&frozen, &receipt);
        Ok(receipt)
    }
    pub(crate) async fn execute_with_request_effect(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        intent: Option<Arc<ModelIntentV1>>,
    ) -> Result<Option<MutationRequestReceiptV2>, RequestFailureV1> {
        let Some(intent) = intent else {
            return Ok(None);
        };
        let frozen = self
            .registry
            .freeze(context.global_request_id.clone(), intent);
        let receipt = intent_bridge::IntentBridge::bind_sim_group(decision)
            .execute_with_request_effect(context, frozen.intent())
            .await?;
        self.registry.record_sim_receipt(&frozen, &receipt);
        Ok(Some(receipt))
    }
}

#[derive(Debug)]
pub enum SharedActionErrorV1 {
    Ledger(LedgerErrorV1),
    Reservation(Vec<LedgerReservationOutcomeV1>),
    Sim(SimRunActorErrorV2),
}

#[derive(Debug)]
pub enum SharedSettlementErrorV1 {
    Ledger(LedgerErrorV1),
    Sim(SimRunActorErrorV2),
}

/// 版本选择仅连接同一真实NOE消费与Ledger应用核心。
pub trait SharedOrderEvidence: sim_system::execution::MemoryOrderEvidence {
    fn apply(
        ledger: &mut LedgerWriterV1,
        record: &NoeConsumedRecordRef<Self::Completion>,
    ) -> Result<LedgerMemoryApplyOutcomeV1, LedgerErrorV1>;
}
impl SharedOrderEvidence for sim_system::execution::MeasuredOrderEvidence {
    fn apply(
        ledger: &mut LedgerWriterV1,
        record: &NoeConsumedRecordRef<Self::Completion>,
    ) -> Result<LedgerMemoryApplyOutcomeV1, LedgerErrorV1> {
        ledger.apply_noe_memory_v1(record)
    }
}
impl SharedOrderEvidence for sim_system::execution::MeasuredCancellationEvidence {
    fn apply(
        ledger: &mut LedgerWriterV1,
        record: &NoeConsumedRecordRef<Self::Completion>,
    ) -> Result<LedgerMemoryApplyOutcomeV1, LedgerErrorV1> {
        ledger.apply_noe_memory_v2(record)
    }
}
pub type SharedExecutionV1 = SharedExecution<sim_system::execution::MeasuredOrderEvidence>;
pub type SharedExecutionV2 = SharedExecution<sim_system::execution::MeasuredCancellationEvidence>;
pub type ActionBeforeReadV1 =
    ActionBeforeRead<contracts::execution_normalization::NoCancellationRequestCompletionV0>;
pub type ActionBeforeReadV2<G = SharedExecutionRegistryV2, A = OwnedActionAccountV2> =
    ActionBeforeRead<contracts::execution_normalization::CancellationRequestCompletedV1, G, A>;

impl<G: Borrow<SharedExecutionRegistryV2>, A: ActionAccountReadV2> ActionBeforeReadV2<G, A> {
    pub fn pending_cancellation_count(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> ExecutionReadValueV1<u64> {
        self.registry.borrow().pending_cancellation_count(scope)
    }
    pub fn cancellation_requests(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> CancellationRequestsReadV1<'_> {
        self.registry.borrow().cancellation_requests(scope)
    }
}

/// 原登记与尚缺前驱的已领取引用，不重建领取时点。
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "R:serde::Serialize",
    deserialize = "R:serde::Deserialize<'de>"
))]
struct SharedExecutionCheckpointV1<R> {
    registry: SharedExecutionRegistry<R>,
    pending: Vec<NoeConsumedRecordRef<R>>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SharedExecutionCheckpointFormatV2 {
    SharedExecutionV2,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "R:serde::Serialize",
    deserialize = "R:serde::Deserialize<'de>"
))]
struct SharedExecutionCheckpointV2<R> {
    format: SharedExecutionCheckpointFormatV2,
    pool: execution_normalization::NoeCheckpointBatchPoolV1<R>,
    registry:
        crate::registry::RegistryCheckpointV2<execution_normalization::NoeCheckpointRecordRefV1>,
    pending: Vec<execution_normalization::NoeCheckpointRecordRefV1>,
}
impl<P: SharedOrderEvidence> SharedExecution<P>
where
    P::Completion: serde::de::DeserializeOwned,
{
    pub fn write_checkpoint(
        &self,
        writer: impl std::io::Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut pool = execution_normalization::NoeCheckpointBatchPoolV1::default();
        let registry = self.registry.capture_checkpoint(&mut pool);
        let pending = self.pending.iter().map(|r| pool.capture(r)).collect();
        let saved = SharedExecutionCheckpointV2 {
            format: SharedExecutionCheckpointFormatV2::SharedExecutionV2,
            pool,
            registry,
            pending,
        };
        ciborium::ser::into_writer(&saved, writer)?;
        Ok(())
    }
    pub fn restore_checkpoint(
        ledger: LedgerWriterV1,
        observation: sim_system::execution::MemoryNoeObservation<P>,
        reader: impl std::io::Read,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // 旧读取只在边界导入一次，原登记和消费可见性保持。
        #[derive(serde::Deserialize)]
        #[serde(untagged, bound(deserialize = "R:serde::Deserialize<'de>"))]
        enum Saved<R> {
            Current(SharedExecutionCheckpointV2<R>),
            Legacy(SharedExecutionCheckpointV1<R>),
        }
        let saved: Saved<P::Completion> = ciborium::de::from_reader(reader)?;
        let (registry, pending) = match saved {
            Saved::Current(saved) => (
                saved.registry.restore(&saved.pool)?,
                saved
                    .pending
                    .into_iter()
                    .map(|r| saved.pool.restore(r))
                    .collect::<Result<_, _>>()?,
            ),
            Saved::Legacy(saved) => (saved.registry, saved.pending),
        };
        Ok(Self {
            ledger,
            registry,
            observation,
            pending,
            pending_retry_after: None,
        })
    }
}
