use crate::*;
use contracts::{ConditionId, ExactDecimal, ExecutionReadValueV1, ModelIntentV1};
use execution_normalization::{MemoryLogicalIdentityV1, NoeConsumedRecordRefV1};
use ledger::*;
use sim_system::{execution::*, public_consumption::TrustedGroupDecisionCutoffV1};
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
pub struct ActionBeforeReadV1 {
    economics: AccountEconomicsReadV1,
    cutoff: TrustedGroupDecisionCutoffV1,
    ledger: LedgerProjectionV2,
    registry: SharedExecutionRegistryV1,
    requests: Vec<RequestFunds>,
    orders: Vec<OrderFunds>,
}
impl ActionBeforeReadV1 {
    pub fn economics(&self) -> &AccountEconomicsReadV1 {
        &self.economics
    }
    pub fn cutoff(&self) -> &TrustedGroupDecisionCutoffV1 {
        &self.cutoff
    }
    pub fn ledger(&self) -> &LedgerProjectionV2 {
        &self.ledger
    }
    pub fn orders(&self) -> impl Iterator<Item = ExecutionOrderReadV1<'_>> {
        self.registry.orders()
    }
    pub fn commitments(&self) -> impl Iterator<Item = IntentCommitmentReadV1<'_>> {
        self.registry.all_commitments()
    }
    pub fn cancel_objects(&self) -> ExecutionReadValueV1<Vec<CancelObjectReadV1<'_>>> {
        self.registry.cancel_objects()
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
pub struct SharedExecutionV1 {
    ledger: LedgerWriterV1,
    registry: SharedExecutionRegistryV1,
    observation: MemoryNoeObservationV3,
    pending: Vec<NoeConsumedRecordRefV1>,
}
impl SharedExecutionV1 {
    /// 调用方已经在原运行后台绑定此新Ledger的记录接收器。
    pub fn from_opening(
        mut ledger: LedgerWriterV1,
        observation: MemoryNoeObservationV3,
    ) -> Result<Option<Self>, LedgerErrorV1> {
        let Some(opening) = observation.opening_record() else {
            return Ok(None);
        };
        ledger.apply_noe_memory_v1(&opening)?;
        let registry = SharedExecutionRegistryV1::new(Arc::new(opening.context().clone()));
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
    pub fn registry(&self) -> &SharedExecutionRegistryV1 {
        &self.registry
    }
    /// false精确表示本组仍有尚未应用的外生事实，不产出模型视图。
    pub fn apply_closed(&mut self) -> Result<bool, LedgerErrorV1> {
        self.pending.extend(self.observation.take_closed_records());
        loop {
            let mut waiting = Vec::new();
            let mut applied = false;
            for record in std::mem::take(&mut self.pending) {
                match self.ledger.apply_noe_memory_v1(&record)? {
                    LedgerMemoryApplyOutcomeV1::Applied(
                        LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. },
                    )
                    | LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation => {
                        waiting.push(record)
                    }
                    _ => {
                        self.registry.observe(&record);
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
    pub fn action_before(
        &mut self,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<Option<ActionBeforeReadV1>, LedgerErrorV1> {
        if !self.apply_closed()? {
            return Ok(None);
        }
        // 领取和完整应用后重新取得真实decision；绝不复用next_group的旧cutoff。
        let Some(decision) = driver.decision() else {
            return Ok(None);
        };
        let frames = decision.frames();
        if frames.is_empty() {
            return Ok(None);
        }
        let cutoff = frames[0].decision_cutoff.clone();
        for frame in &frames {
            self.registry.observe_market(&frame.rule_context);
        }
        let registry = self.registry.action_snapshot();
        let funds = self.ledger.funds_read_v1();
        let Some(ledger) = funds.projection() else {
            return Ok(None);
        };
        let required_requests: std::collections::BTreeSet<_> = registry
            .orders()
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
            .filter(|r| registry.order(r.order_identity).is_some())
            .map(|r| OrderFunds {
                id: r.order_identity.clone(),
                principal: r.principal_encumbrance_pusd,
                fee: r.immediate_fee_encumbrance_pusd,
                seq: r.last_ledger_entry_seq,
            })
            .collect();
        Ok(Some(ActionBeforeReadV1 {
            economics: AccountEconomicsReadV1::from_ledger(
                &self.ledger.economic_read().expect("同组 Ledger 已开户"),
            ),
            cutoff,
            ledger,
            registry,
            requests,
            orders,
        }))
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
pub enum SharedSettlementErrorV1 {
    Ledger(LedgerErrorV1),
    Sim(SimRunActorErrorV2),
}
