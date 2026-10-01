use crate::*;
use contracts::{ConditionId, ExactDecimal, ExecutionReadValueV1, ModelIntentV1};
use execution_normalization::{MemoryLogicalIdentityV1, NoeConsumedRecordRef};
use ledger::*;
use sim_system::{
    execution::*,
    public_consumption::{PublicObservationFrameV1, TrustedGroupDecisionCutoffV1},
};
use std::sync::Arc;

struct RequestFunds {
    id: LedgerRequestIdentityV1,
    condition: ConditionId,
    state: ReservationStatusV1,
    principal: ExactDecimal,
    fee: ExactDecimal,
    seq: u64,
}
struct OrderFunds {
    id: MemoryLogicalIdentityV1,
    principal: ExecutionReadValueV1<ExactDecimal>,
    fee: ExecutionReadValueV1<ExactDecimal>,
    seq: Option<u64>,
}
/// 持有同一组必要标量和原Arc引用；后续动作不能改写本组读取。
pub struct ActionBeforeRead<R> {
    economics: AccountEconomicsReadV2,
    pending_noe_record_count: usize,
    cutoff: TrustedGroupDecisionCutoffV1,
    ledger: LedgerProjectionV2,
    registry: SharedExecutionRegistry<R>,
    requests: Vec<RequestFunds>,
    orders: Vec<OrderFunds>,
}
impl<R: Clone + std::fmt::Debug + Eq> ActionBeforeRead<R> {
    /// 已领取、因依赖未满足而尚待 Ledger 应用的原记录数；零不证明未来事实已齐。
    pub fn pending_noe_record_count(&self) -> usize {
        self.pending_noe_record_count
    }
    pub fn economics(&self) -> &AccountEconomicsReadV2 {
        &self.economics
    }
    pub fn cutoff(&self) -> &TrustedGroupDecisionCutoffV1 {
        &self.cutoff
    }
    pub fn ledger(&self) -> &LedgerProjectionV2 {
        &self.ledger
    }
    pub fn orders(&self) -> impl Iterator<Item = ExecutionOrderRead<'_, R>> {
        self.registry.unfinished_orders()
    }
    pub fn commitments(&self) -> impl Iterator<Item = IntentCommitmentReadV1<'_>> {
        self.registry.all_commitments()
    }
    pub fn cancel_objects(&self) -> ExecutionReadValueV1<Vec<CancelObjectRead<'_, R>>> {
        self.registry.cancel_objects()
    }
    pub fn cancel_objects_in<'a>(
        &'a self,
        current: Option<&'a ConditionId>,
        old: &'a [ConditionId],
    ) -> ExecutionReadValueV1<Vec<CancelObjectRead<'a, R>>> {
        self.registry
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
}
/// 保留缺直接前驱的原领取引用；没有第二业务归约或跳号/超时政策。
pub struct SharedExecution<P: SharedOrderEvidence> {
    ledger: LedgerWriterV1,
    registry: SharedExecutionRegistry<P::Completion>,
    observation: sim_system::execution::MemoryNoeObservation<P>,
    pending: Vec<NoeConsumedRecordRef<P::Completion>>,
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
    pub fn ledger(&self) -> &LedgerWriterV1 {
        &self.ledger
    }
    pub fn registry(&self) -> &SharedExecutionRegistry<P::Completion> {
        &self.registry
    }
    /// false 表示仍有待经济应用的原记录，不阻止实到观察与模型组。
    pub fn apply_closed(&mut self) -> Result<bool, LedgerErrorV1> {
        for record in self.observation.take_closed_records() {
            self.registry.observe(&record);
            self.pending.push(record);
        }
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
            self.registry.observe_market(&frame.rule_context);
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
        let registry = self.registry.action_snapshot();
        let funds = self.ledger.funds_read_v1();
        let ledger = funds.projection()?;
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
        let requests = funds
            .requests()
            .filter(|r| {
                !matches!(
                    r.reservation_state,
                    ReservationStatusV1::Converted | ReservationStatusV1::Released
                ) || required_requests.contains(r.request_identity)
            })
            .map(|r| RequestFunds {
                id: r.request_identity.clone(),
                condition: r.condition_id.clone(),
                state: r.reservation_state,
                principal: r.principal_pusd,
                fee: r.immediate_fee_pusd,
                seq: r.last_ledger_entry_seq,
            })
            .collect();
        let orders = funds
            .orders()
            .filter(|r| registry.contains_unfinished_order(r.order_identity))
            .map(|r| OrderFunds {
                id: r.order_identity.clone(),
                principal: r.principal_encumbrance_pusd,
                fee: r.immediate_fee_encumbrance_pusd,
                seq: r.last_ledger_entry_seq,
            })
            .collect();
        Some(ActionBeforeRead {
            pending_noe_record_count: self.pending.len(),
            economics: AccountEconomicsReadV2::from_ledger_and_observations(
                &self.ledger.economic_read().expect("同组 Ledger 已开户"),
                frames.iter().map(|f| f.observation),
            ),
            cutoff,
            ledger,
            registry,
            requests,
            orders,
        })
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
                self.record_submission_started(&event, &reserve.request_identity, &item)
                    .map_err(SharedActionErrorV1::Ledger)?;
            }
        }
        self.execute(decision, context, intent)
            .await
            .map_err(SharedActionErrorV1::Sim)
    }
    /// None为宿主已作出的NoTrade；不制造意图，不调用Bridge。
    pub async fn execute(
        &mut self,
        decision: &mut PublicGroupDecisionV1<'_>,
        context: RequestContextV2,
        intent: Option<Arc<ModelIntentV1>>,
    ) -> Result<Option<MutationRequestReceiptV2>, SimRunActorErrorV2> {
        let Some(intent) = intent else {
            return Ok(None);
        };
        let frozen = self
            .registry
            .freeze(context.global_request_id.clone(), intent);
        let receipt = intent_bridge::IntentBridge::bind_sim_group(decision)
            .execute(context, frozen.intent())
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
pub type ActionBeforeReadV2 =
    ActionBeforeRead<contracts::execution_normalization::CancellationRequestCompletedV1>;

impl ActionBeforeReadV2 {
    pub fn cancellation_requests(
        &self,
        scope: Option<&CancellationScopeV1<'_>>,
    ) -> CancellationRequestsReadV1<'_> {
        self.registry.cancellation_requests(scope)
    }
}
