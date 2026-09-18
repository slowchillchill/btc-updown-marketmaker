mod memory;
pub use memory::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextV1, NoeEconomicFactReferenceV1,
    NoeInclusiveSequenceRangeV1, NoeResultReferenceV1, NoeRunConsumptionManifestV1,
    SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1,
    SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralCancellationOutcomeV1,
    SourceNeutralControlResult, SourceNeutralMakerRebateAccrualStatusV1,
    SourceNeutralMarketEnrollmentResultV1, SourceNeutralOrderLifecycleEventV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralOrderQueryResult,
    SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1, SourceNeutralOrdersPage,
    SourceNeutralOrdersQueryResult, SourceNeutralOutcomeBalanceDeltaV1, SourceNeutralQueryCursorV1,
    SourceNeutralQueryOutOfRetentionV1, SourceNeutralSettlementLifecycleResultV1,
    SourceNeutralSubmissionOutcomeV1, SourceNeutralTradeEconomicDispositionV1,
    SourceNeutralTradeLifecycleResultV1, SourceNeutralTradeStateV1, SourceNeutralTradesPageV1,
    SourceNeutralTradesQueryResultV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, CancelTargetOutcomeV2, DirectResponseMetadataV2,
    MakerRebateAccrualStatusV2, OrderLifecycleEventV2, OrderProjectionV2, OrderQueryResultV2,
    OrdersQueryResultV2, PlatformAccountDeltaReasonV2, PlatformAccountDeltaV2, QueryCursorV2,
    QueryOutOfRetentionV2, SettlementLifecycleEventV2, SimulatedOrderKnownStatusV2,
    SimulatedOrderStatusV2, SimulatedTradeKnownStatusV2, SimulatedTradeStatusV2,
    SubmissionItemOutcomeV2, SubmissionResponseItemV2, TradeEconomicDispositionV2,
    TradeProjectionV2, TradesQueryResultV2,
};
use execution_normalization::{
    DurableCommitOutcomeV1, DurableContextStore, DurableErrorV1, NoeTradePayload,
    NormalizationBatchInput, NormalizationIdentity, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEntityKeyV1, SourceNeutralEvidence,
    SourceNeutralEvidenceKind, SourceNeutralLogicalKeyV1,
};

use super::{
    BusinessDirectResponseV2, BusinessProjectionBatch, BusinessProjectionBatchPayloadV1,
    BusinessProjectionBatchV2, BusinessProjectionPortV2,
};
use crate::runtime_v2::RunOpenedV2;

pub struct SimNoeProjectionPort<
    T: NoeFeedbackPayload,
    P: OrderEvidenceProjection = LegacyOrderEvidence,
> where
    T::Result: NoeTradePayload<ControlFor<P>>,
    ControlFor<P>: serde::de::DeserializeOwned,
{
    root: PathBuf,
    opened: RunOpenedV2,
    run_opened_reference: DurableSourceReferenceV1,
    account_opening_reference: DurableSourceReferenceV1,
    account_opening_evidence_identity: String,
    implementation_identity: String,
    seed: Option<SimV2ContextSeedV1>,
    store: Option<DurableContextStore<T::Result, ControlFor<P>>>,
    observation: SimV2NoeObservationV1,
}

#[derive(Clone, Default)]
pub struct SimV2NoeObservationV1 {
    result_references: Arc<Mutex<Vec<NoeResultReferenceV1>>>,
}

impl SimV2NoeObservationV1 {
    pub fn result_references(&self) -> Vec<NoeResultReferenceV1> {
        self.result_references
            .lock()
            .expect("NOE 只读观察状态锁未损坏")
            .clone()
    }
}

impl<T: NoeFeedbackPayload, P: OrderEvidenceProjection> SimNoeProjectionPort<T, P>
where
    T::Result: NoeTradePayload<ControlFor<P>>,
    ControlFor<P>: serde::de::DeserializeOwned,
{
    pub fn new(
        root: impl AsRef<Path>,
        opened: RunOpenedV2,
        run_opened_reference: DurableSourceReferenceV1,
        account_opening_reference: DurableSourceReferenceV1,
        account_opening_evidence_identity: String,
        implementation_identity: String,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            opened,
            run_opened_reference,
            account_opening_reference,
            account_opening_evidence_identity,
            implementation_identity,
            seed: None,
            store: None,
            observation: SimV2NoeObservationV1::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn store_for_resource_observation(
        &self,
    ) -> &DurableContextStore<T::Result, ControlFor<P>> {
        self.store.as_ref().expect("实际接纳开户后存在耐久上下文")
    }

    pub fn observation(&self) -> SimV2NoeObservationV1 {
        self.observation.clone()
    }

    pub fn result_references(&self) -> Vec<NoeResultReferenceV1> {
        self.store
            .as_ref()
            .into_iter()
            .flat_map(|store| store.commits())
            .flat_map(|commit| commit.result_references())
            .collect()
    }

    pub fn run_consumption_manifest(&self) -> Option<NoeRunConsumptionManifestV1> {
        let store = self.store.as_ref()?;
        let source_watermark = store.durable_through()?.clone();
        let last_commit = store.commits().last()?;
        Some(NoeRunConsumptionManifestV1 {
            context_reference: store.context().context_reference.clone(),
            input_projection_batch_range: NoeInclusiveSequenceRangeV1 {
                first: 1,
                last: source_watermark.projection_batch_sequence,
            },
            output_commit_range: NoeInclusiveSequenceRangeV1 {
                first: 0,
                last: last_commit.commit_sequence,
            },
            source_watermark,
            context_container: store.context_container_reference().clone(),
            commit_containers: store
                .commits()
                .iter()
                .map(|commit| commit.container_reference.clone())
                .collect(),
        })
    }

    fn refresh_observation(&self) {
        *self
            .observation
            .result_references
            .lock()
            .expect("NOE 只读观察状态锁未损坏") = self.result_references();
    }

    fn append_latest_observation(&self) {
        let store = self.store.as_ref().expect("提交后 NOE 耐久句柄存在");
        let commit = store.commits().last().expect("提交后 NOE commit 存在");
        self.observation
            .result_references
            .lock()
            .expect("NOE 只读观察状态锁未损坏")
            .extend(commit.result_references());
    }

    fn open_from_first_batch(&mut self, batch: &BusinessProjectionBatch<T>) -> bool {
        if self.store.is_some() {
            return true;
        }
        if batch.run_id != self.opened.run_id {
            return false;
        }
        let Some(opening) = batch.account_opening.as_ref() else {
            return false;
        };
        let seed = SimV2ContextSeedV1 {
            run_id: self.opened.run_id.clone(),
            account_instance_key: opening.account_instance_key.clone(),
            run_opened: self.run_opened_reference.clone(),
            account_opening: self.account_opening_reference.clone(),
            account_opening_evidence_identity: self.account_opening_evidence_identity.clone(),
            implementation_identity: self.implementation_identity.clone(),
        };
        match DurableContextStore::<T::Result, ControlFor<P>>::open_or_create_sim_v2(
            &self.root,
            seed.clone(),
        ) {
            Ok(store) => {
                self.seed = Some(seed);
                self.store = Some(store);
                self.refresh_observation();
                true
            }
            Err(_) => false,
        }
    }

    fn reopen_after_uncertain(&mut self, target_sequence: u64) -> bool {
        self.store = None;
        let Some(seed) = self.seed.clone() else {
            return false;
        };
        let Ok(store) = DurableContextStore::<T::Result, ControlFor<P>>::open_or_create_sim_v2(
            &self.root, seed,
        ) else {
            return false;
        };
        let durable = store.durable_through().is_some_and(|watermark| {
            watermark.run_id == self.opened.run_id
                && watermark.projection_batch_sequence >= target_sequence
        });
        self.store = Some(store);
        self.refresh_observation();
        durable
    }
}

impl<T: NoeFeedbackPayload, P: OrderEvidenceProjection> BusinessProjectionPortV2
    for SimNoeProjectionPort<T, P>
where
    T::Result: NoeTradePayload<ControlFor<P>>,
    ControlFor<P>: serde::de::DeserializeOwned,
{
    fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
        self.try_accept_payload(Arc::new(BusinessProjectionBatchPayloadV1::V2(batch)))
    }
    fn try_accept_payload(&mut self, payload: Arc<BusinessProjectionBatchPayloadV1>) -> bool {
        let Some(batch) = T::batch(payload.as_ref()) else {
            return false;
        };
        if !self.open_from_first_batch(&batch) || batch.run_id != self.opened.run_id {
            return false;
        }
        let Some(store) = self.store.as_ref() else {
            return false;
        };
        if batch.account_opening.as_ref().is_some_and(|opening| {
            opening.account_instance_key != store.context().source.account_instance_key
        }) {
            return false;
        }
        let input = match BatchMapper::<T, P>::new(store.context(), &batch).map() {
            Ok(input) => input,
            Err(()) => return false,
        };
        let target_sequence = batch.projection_batch_sequence;
        let outcome = self
            .store
            .as_mut()
            .expect("映射时已经确认耐久句柄存在")
            .commit_batch(input, durable_now_utc_ms());
        match outcome {
            Ok(DurableCommitOutcomeV1::Committed { .. }) => {
                self.append_latest_observation();
                true
            }
            Ok(DurableCommitOutcomeV1::AlreadyDurable { .. }) => true,
            Err(DurableErrorV1::DurabilityUncertain { .. }) => {
                self.reopen_after_uncertain(target_sequence)
            }
            Err(_) => false,
        }
    }

    fn durable_through(&self, run_id: &str) -> u64 {
        if run_id != self.opened.run_id {
            return 0;
        }
        self.store
            .as_ref()
            .and_then(DurableContextStore::<T::Result, ControlFor<P>>::durable_through)
            .filter(|watermark| watermark.run_id == run_id)
            .map_or(0, |watermark| watermark.projection_batch_sequence)
    }
}

struct BatchMapper<
    'a,
    T: NoeFeedbackPayload<I>,
    P: OrderEvidenceProjection<I> = LegacyOrderEvidence,
    I = StableLogicalIdentityV1,
    F = NoeEconomicFactReferenceV1,
> where
    I: NormalizationIdentity<F> + serde::Serialize,
{
    context: &'a ExecutionNormalizationContextV1,
    account: Arc<contracts::execution_normalization::SourceNeutralAccountIdentityV1>,
    identity: std::marker::PhantomData<(I, F)>,
    batch: &'a BusinessProjectionBatch<T>,
    watermark: SimV2SourceWatermarkV1,
    evidence: Vec<SourceNeutralEvidence<T::Result, ControlFor<P, I>, I>>,
    next_ordinal: u32,
}

impl<'a, T: NoeFeedbackPayload<I>, P: OrderEvidenceProjection<I>, I, F> BatchMapper<'a, T, P, I, F>
where
    I: NormalizationIdentity<F> + serde::Serialize,
{
    fn new(
        context: &'a ExecutionNormalizationContextV1,
        batch: &'a BusinessProjectionBatch<T>,
    ) -> Self {
        Self::with_account(context, Arc::new(context.account_identity.clone()), batch)
    }

    fn with_account(
        context: &'a ExecutionNormalizationContextV1,
        account: Arc<contracts::execution_normalization::SourceNeutralAccountIdentityV1>,
        batch: &'a BusinessProjectionBatch<T>,
    ) -> Self {
        Self {
            account,
            context,
            identity: std::marker::PhantomData,
            batch,
            watermark: SimV2SourceWatermarkV1 {
                run_id: batch.run_id.clone(),
                projection_batch_sequence: batch.projection_batch_sequence,
            },
            evidence: Vec::new(),
            next_ordinal: 0,
        }
    }

    fn map(mut self) -> Result<NormalizationBatchInput<T::Result, ControlFor<P, I>, I>, ()> {
        if self.batch.run_id != self.context.account_identity.run_scope {
            return Err(());
        }
        if let Some(opening) = &self.batch.account_opening {
            if opening.account_instance_key != self.context.source.account_instance_key {
                return Err(());
            }
            self.push(
                SimV2SourceRecordReferenceV1::AccountOpening,
                SourceNeutralLogicalKeyV1::AccountOpening {
                    opening_evidence_identity: self
                        .context
                        .source
                        .account_opening_evidence_identity
                        .clone(),
                },
                SourceNeutralEvidenceKind::Economic {
                    evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                        opening_evidence_identity: self
                            .context
                            .source
                            .account_opening_evidence_identity
                            .clone(),
                        available_pusd: opening.initial_account_state.available_pusd.clone(),
                    },
                },
            );
        }
        for market in &self.batch.initial_markets {
            self.push(
                SimV2SourceRecordReferenceV1::AccountOpening,
                SourceNeutralLogicalKeyV1::InitialMarket {
                    condition_id: market.market.condition_id.0.clone(),
                },
                SourceNeutralEvidenceKind::InitialMarket {
                    result: market.clone(),
                },
            );
        }
        if let Some(enrollment) = &self.batch.market_enrollment {
            self.push(
                SimV2SourceRecordReferenceV1::MarketEnrollment {
                    enrollment_sequence: enrollment.enrollment_sequence,
                },
                SourceNeutralLogicalKeyV1::MarketEnrollment {
                    enrollment_request_id: enrollment.enrollment_request_id.clone(),
                },
                SourceNeutralEvidenceKind::MarketEnrollment {
                    result: SourceNeutralMarketEnrollmentResultV1 {
                        enrollment_request_id: enrollment.enrollment_request_id.clone(),
                        market: enrollment.market.clone(),
                        assets: enrollment.assets.clone(),
                    },
                },
            );
        }
        for response in self.batch.direct_responses() {
            self.map_direct_response(response)?;
        }
        for feedback in &self.batch.order_lifecycle {
            self.map_order_lifecycle(feedback)?;
        }
        for feedback in &self.batch.trade_lifecycle {
            self.map_trade_lifecycle(feedback)?;
        }
        for feedback in &self.batch.settlement_lifecycle {
            self.map_settlement_lifecycle(feedback)?;
        }
        Ok(NormalizationBatchInput {
            source_watermark: self.watermark,
            evidence: self.evidence,
        })
    }

    fn map_direct_response(&mut self, response: &BusinessDirectResponseV2) -> Result<(), ()> {
        match response {
            BusinessDirectResponseV2::PostOrder(response) => {
                self.push_submission(&response.metadata, &response.item)
            }
            BusinessDirectResponseV2::PostOrders(response) => {
                for item in &response.items {
                    self.push_submission(&response.metadata, item)?;
                }
                Ok(())
            }
            BusinessDirectResponseV2::CancelOrder(response) => {
                self.push_cancellation(&response.metadata, &response.target)
            }
            BusinessDirectResponseV2::CancelOrders(response) => {
                for target in &response.targets {
                    self.push_cancellation(&response.metadata, target)?;
                }
                Ok(())
            }
            BusinessDirectResponseV2::CancelMarketOrders(response) => {
                for target in &response.targets {
                    self.push_cancellation(&response.metadata, target)?;
                }
                Ok(())
            }
            BusinessDirectResponseV2::CancelAllOrders(response) => {
                for target in &response.targets {
                    self.push_cancellation(&response.metadata, target)?;
                }
                Ok(())
            }
            BusinessDirectResponseV2::Order(response) => {
                let result = self.map_order_query(&response.result)?;
                self.push_direct_request(
                    &response.metadata,
                    SourceNeutralControlResult::OrderQuery { result },
                );
                Ok(())
            }
            BusinessDirectResponseV2::Orders(response) => {
                let result = self.map_orders_query(&response.result)?;
                self.push_direct_request(
                    &response.metadata,
                    SourceNeutralControlResult::OrdersQuery { result },
                );
                Ok(())
            }
            BusinessDirectResponseV2::Trades(response) => {
                let result = self.map_trades_query(&response.result)?;
                self.push_direct_request(
                    &response.metadata,
                    SourceNeutralControlResult::TradesQuery { result },
                );
                Ok(())
            }
        }
    }

    fn push_submission(
        &mut self,
        metadata: &DirectResponseMetadataV2,
        item: &SubmissionResponseItemV2,
    ) -> Result<(), ()> {
        let outcome = match &item.outcome {
            SubmissionItemOutcomeV2::Accepted(accepted) => P::accepted(
                SourceNeutralOrderSnapshotV1 {
                    order_identity: self.entity_order(&accepted.platform_order_id)?,
                    condition_id: item.condition_id.clone(),
                    token_id: item.token_id.clone(),
                    state: map_order_state(&accepted.platform_order_status),
                },
                accepted,
                item.effective_processing_at_utc_ms,
            ),
            SubmissionItemOutcomeV2::RejectedNoEffect(_) => P::rejected(),
            SubmissionItemOutcomeV2::EvidenceUnknownNoEffect(_) => P::unknown(),
        };
        self.push(
            direct_record(
                metadata,
                SimV2DirectResponseAssociationV1::SubmissionItem {
                    global_request_id: metadata.global_request_id.clone(),
                    batch_item_ordinal: item.batch_item_ordinal,
                },
            ),
            SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id: metadata.global_request_id.clone(),
                batch_item_ordinal: item.batch_item_ordinal,
            },
            SourceNeutralEvidenceKind::Control {
                result: SourceNeutralControlResult::Submission { outcome },
            },
        );
        Ok(())
    }

    fn push_cancellation(
        &mut self,
        metadata: &DirectResponseMetadataV2,
        target: &contracts::CancelTargetResponseV2,
    ) -> Result<(), ()> {
        let outcome = match &target.outcome {
            CancelTargetOutcomeV2::Canceled(_) => SourceNeutralCancellationOutcomeV1::Canceled,
            CancelTargetOutcomeV2::NotCanceledNoEffect(_) => {
                SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect
            }
            CancelTargetOutcomeV2::EvidenceUnknownNoEffect => {
                SourceNeutralCancellationOutcomeV1::EvidenceUnknownNoEffect
            }
        };
        self.push(
            direct_record(
                metadata,
                SimV2DirectResponseAssociationV1::CancellationTarget {
                    global_request_id: metadata.global_request_id.clone(),
                    method: metadata.method,
                    target_ordinal: target.target_ordinal,
                    input_item_refs: target.input_item_refs.clone(),
                },
            ),
            SourceNeutralLogicalKeyV1::CancellationTarget {
                global_request_id: metadata.global_request_id.clone(),
                target_ordinal: target.target_ordinal,
            },
            SourceNeutralEvidenceKind::Control {
                result: SourceNeutralControlResult::Cancellation {
                    order_identity: self.entity_order(&target.platform_order_id)?,
                    outcome,
                },
            },
        );
        Ok(())
    }

    fn push_direct_request(
        &mut self,
        metadata: &DirectResponseMetadataV2,
        result: ControlFor<P, I>,
    ) {
        self.push(
            direct_record(
                metadata,
                SimV2DirectResponseAssociationV1::Request {
                    global_request_id: metadata.global_request_id.clone(),
                },
            ),
            SourceNeutralLogicalKeyV1::DirectResponseRequest {
                global_request_id: metadata.global_request_id.clone(),
            },
            SourceNeutralEvidenceKind::Control { result },
        );
    }

    fn map_order_query(
        &self,
        result: &OrderQueryResultV2,
    ) -> Result<SourceNeutralOrderQueryResult<P::Snapshot>, ()> {
        match result {
            OrderQueryResultV2::Found(order) => Ok(SourceNeutralOrderQueryResult::Found {
                order: self.map_order(order)?,
            }),
            OrderQueryResultV2::NotFound => Ok(SourceNeutralOrderQueryResult::NotFound),
            OrderQueryResultV2::OutOfRetention(range) => Ok(
                SourceNeutralOrderQueryResult::OutOfRetention(map_out_of_retention(range)),
            ),
        }
    }

    fn map_orders_query(
        &self,
        result: &OrdersQueryResultV2,
    ) -> Result<SourceNeutralOrdersQueryResult<P::Snapshot>, ()> {
        match result {
            OrdersQueryResultV2::Page(page) => {
                let items = page
                    .items
                    .iter()
                    .map(|order| self.map_order(order))
                    .collect::<Result<_, _>>()?;
                Ok(SourceNeutralOrdersQueryResult::Page(
                    SourceNeutralOrdersPage {
                        root_sequence: page.root_sequence,
                        items,
                        next_cursor: page.next_cursor.as_ref().map(map_cursor),
                    },
                ))
            }
            OrdersQueryResultV2::InvalidCursor => Ok(SourceNeutralOrdersQueryResult::InvalidCursor),
            OrdersQueryResultV2::OutOfRetention(range) => Ok(
                SourceNeutralOrdersQueryResult::OutOfRetention(map_out_of_retention(range)),
            ),
        }
    }

    fn map_trades_query(
        &self,
        result: &TradesQueryResultV2,
    ) -> Result<SourceNeutralTradesQueryResultV1<I>, ()> {
        match result {
            TradesQueryResultV2::Page(page) => {
                let items = page
                    .items
                    .iter()
                    .map(|trade| self.map_trade(trade))
                    .collect::<Result<_, _>>()?;
                Ok(SourceNeutralTradesQueryResultV1::Page(
                    SourceNeutralTradesPageV1 {
                        root_sequence: page.root_sequence,
                        items,
                        next_cursor: page.next_cursor.as_ref().map(map_cursor),
                    },
                ))
            }
            TradesQueryResultV2::InvalidCursor => {
                Ok(SourceNeutralTradesQueryResultV1::InvalidCursor)
            }
            TradesQueryResultV2::OutOfRetention(range) => Ok(
                SourceNeutralTradesQueryResultV1::OutOfRetention(map_out_of_retention(range)),
            ),
        }
    }

    fn map_order(&self, order: &OrderProjectionV2) -> Result<P::Snapshot, ()> {
        Ok(P::snapshot(
            SourceNeutralOrderSnapshotV1 {
                order_identity: self.entity_order(&order.platform_order_id)?,
                condition_id: order.condition_id.clone(),
                token_id: order.token_id.clone(),
                state: map_order_state(&order.status),
            },
            order,
        ))
    }

    fn map_trade(
        &self,
        trade: &TradeProjectionV2,
    ) -> Result<SourceNeutralTradeLifecycleResultV1<I>, ()> {
        Ok(SourceNeutralTradeLifecycleResultV1 {
            trade_identity: self.entity_trade(&trade.trade_id)?,
            order_identity: self.entity_order(&trade.platform_order_id)?,
            condition_id: trade.condition_id.clone(),
            token_id: trade.token_id.clone(),
            state: map_trade_state(&trade.status),
            economic_disposition: map_trade_disposition(trade.economic_disposition),
        })
    }

    fn map_order_lifecycle(
        &mut self,
        feedback: &contracts::OrderLifecycleFeedbackV2,
    ) -> Result<(), ()> {
        self.check_account(&feedback.metadata.account_instance_key)?;
        let order_identity = self.entity_order(&feedback.platform_order_id)?;
        let (event, account_delta) = match &feedback.event {
            OrderLifecycleEventV2::OrderAccepted {
                status,
                account_delta,
            } => (
                SourceNeutralOrderLifecycleEventV1::Accepted {
                    state: map_order_state(status),
                },
                Some(account_delta),
            ),
            OrderLifecycleEventV2::FakUnspentReleased { account_delta, .. } => (
                SourceNeutralOrderLifecycleEventV1::FakUnspentReleased,
                Some(account_delta),
            ),
            OrderLifecycleEventV2::VenueDustTerminal { account_delta, .. } => (
                SourceNeutralOrderLifecycleEventV1::VenueDustTerminal,
                account_delta.as_ref(),
            ),
            OrderLifecycleEventV2::MarketResolvedCanceled {
                resolution_event_id,
                account_delta,
                ..
            } => (
                SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled {
                    resolution_identity: self.entity_resolution(resolution_event_id)?,
                },
                account_delta.as_ref(),
            ),
            OrderLifecycleEventV2::CallerCanceled { account_delta, .. } => (
                SourceNeutralOrderLifecycleEventV1::CallerCanceled,
                account_delta.as_ref(),
            ),
            OrderLifecycleEventV2::GtdExpired { account_delta, .. } => (
                SourceNeutralOrderLifecycleEventV1::GtdExpired,
                account_delta.as_ref(),
            ),
        };
        let record = SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence: feedback.metadata.private_feedback_sequence,
            platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
        };
        self.push(
            record.clone(),
            SourceNeutralLogicalKeyV1::OrderLifecycle {
                order_identity: feedback.platform_order_id.clone(),
                occurrence: feedback.metadata.private_feedback_sequence,
            },
            SourceNeutralEvidenceKind::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    order_identity,
                    condition_id: feedback.condition_id.clone(),
                    token_id: feedback.token_id.clone(),
                    event,
                },
            },
        );
        if let Some(delta) = account_delta {
            self.push_economic(record, delta)?;
        }
        Ok(())
    }

    fn map_trade_lifecycle(&mut self, feedback: &T) -> Result<(), ()> {
        let legacy = feedback.legacy();
        self.check_account(&legacy.metadata.account_instance_key)?;
        let record = SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence: legacy.metadata.private_feedback_sequence,
            platform_event_at_utc_ms: legacy.metadata.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: legacy.metadata.feedback_received_at_utc_ms,
        };
        self.push(
            record.clone(),
            SourceNeutralLogicalKeyV1::TradeLifecycle {
                trade_identity: legacy.trade_id.clone(),
                occurrence: legacy.metadata.private_feedback_sequence,
            },
            {
                let identity = SourceNeutralTradeLifecycleResultV1 {
                    trade_identity: self.entity_trade(&legacy.trade_id)?,
                    order_identity: self.entity_order(&legacy.platform_order_id)?,
                    condition_id: legacy.condition_id.clone(),
                    token_id: legacy.token_id.clone(),
                    state: map_trade_state(&legacy.status),
                    economic_disposition: map_trade_disposition(legacy.economic_disposition),
                };
                match feedback.enriched(identity.clone()) {
                    Some(result) => SourceNeutralEvidenceKind::TradeLifecycleV3 { result },
                    None => SourceNeutralEvidenceKind::TradeLifecycle {
                        result: feedback.normalized(identity),
                    },
                }
            },
        );
        if let Some(delta) = &legacy.account_delta {
            self.push_economic(record, delta)?;
        }
        Ok(())
    }

    fn map_settlement_lifecycle(
        &mut self,
        feedback: &contracts::SettlementLifecycleFeedbackV2,
    ) -> Result<(), ()> {
        self.check_account(&feedback.metadata.account_instance_key)?;
        if let SettlementLifecycleEventV2::ExternalCash {
            fact,
            account_delta,
        } = &feedback.event
        {
            let record = SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
                feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
            };
            self.push(record.clone(), SourceNeutralLogicalKeyV1::SettlementLifecycle { private_feedback_sequence: feedback.metadata.private_feedback_sequence },
                SourceNeutralEvidenceKind::SettlementLifecycleV2 { result: contracts::economics::SourceNeutralSettlementLifecycleResultV2::ExternalCash(contracts::economics::ExternalCashFactV1 {
                    identity: self.external_cash_identity(&fact.identity)?, signed_delta_pusd: fact.signed_delta_pusd.clone(), attribution: fact.attribution.clone(),
                    correction_of: fact.correction_of.as_ref().map(|id| self.external_cash_identity(id)).transpose()?,
                }) });
            self.push_requested_economic(
                record,
                account_delta,
                &contracts::ExactDecimal("0".into()),
            )?;
            return Ok(());
        }
        if let SettlementLifecycleEventV2::RequestedSettlement {
            result,
            account_delta,
            settlement_fee_liability_pusd_delta,
        } = &feedback.event
        {
            use contracts::economics::{
                RequestedSettlementResultV1 as Raw, SettlementCostFactV1, SettlementRequestFactV1,
                SourceNeutralSettlementLifecycleResultV2 as Normalized,
            };
            let result = match result {
                Raw::Request(fact) => Normalized::Request(SettlementRequestFactV1 {
                    index_sets: fact.index_sets,
                    outcome: fact.outcome,
                    observation: fact.observation,
                    settlement_request_id: self
                        .settlement_request_identity(&fact.settlement_request_id)?,
                }),
                Raw::Cost(fact) => Normalized::Cost(SettlementCostFactV1 {
                    settlement_request_id: self
                        .settlement_request_identity(&fact.settlement_request_id)?,
                    cost_identity: self.settlement_cost_identity(
                        &fact.settlement_request_id.resolution_identity,
                        &fact.cost_identity,
                    )?,
                    recognized_pusd: fact.recognized_pusd.clone(),
                    paid_pusd: fact.paid_pusd.clone(),
                    correction_of: fact
                        .correction_of
                        .as_ref()
                        .map(|id| {
                            self.settlement_cost_identity(
                                &fact.settlement_request_id.resolution_identity,
                                id,
                            )
                        })
                        .transpose()?,
                }),
            };
            let record = SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
                feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
            };
            self.push(
                record.clone(),
                SourceNeutralLogicalKeyV1::SettlementLifecycle {
                    private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                },
                SourceNeutralEvidenceKind::SettlementLifecycleV2 { result },
            );
            if let Some(delta) = account_delta {
                self.push_requested_economic(record, delta, settlement_fee_liability_pusd_delta)?;
            }
            return Ok(());
        }
        if let SettlementLifecycleEventV2::ResolvedPosition {
            condition_id,
            resolution_event_id,
            resolution,
        } = &feedback.event
        {
            self.push(
                SimV2SourceRecordReferenceV1::SettlementLifecycle {
                    private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                    platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
                    feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
                },
                SourceNeutralLogicalKeyV1::SettlementLifecycle {
                    private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                },
                SourceNeutralEvidenceKind::SettlementLifecycleV2 {
                    result:
                        contracts::economics::SourceNeutralSettlementLifecycleResultV2::Resolved(
                            contracts::economics::ResolvedPositionFactV1 {
                                condition_id: condition_id.clone(),
                                resolution_identity: self.entity_resolution(resolution_event_id)?,
                                payout_vector: resolution.clone(),
                            },
                        ),
                },
            );
            return Ok(());
        }
        let (result, account_delta) = match &feedback.event {
            SettlementLifecycleEventV2::ExternalCash { .. }
            | SettlementLifecycleEventV2::RequestedSettlement { .. }
            | SettlementLifecycleEventV2::ResolvedPosition { .. } => {
                unreachable!("上方已规范化合格判定")
            }
            SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                condition_id,
                status,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::MakerRebateAccrualFinalized {
                    condition_id: condition_id.clone(),
                    status: match status {
                        MakerRebateAccrualStatusV2::Calculated => {
                            SourceNeutralMakerRebateAccrualStatusV1::Calculated
                        }
                        MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill => {
                            SourceNeutralMakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill
                        }
                        MakerRebateAccrualStatusV2::UnknownIncomplete => {
                            SourceNeutralMakerRebateAccrualStatusV1::UnknownIncomplete
                        }
                    },
                },
                account_delta.as_ref(),
            ),
            SettlementLifecycleEventV2::TakerRebateAccrualApplied {
                trade_id,
                condition_id,
                attribution_day_start_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::TakerRebateAccrualApplied {
                    trade_identity: self.entity_trade(trade_id)?,
                    condition_id: condition_id.clone(),
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                },
                Some(account_delta),
            ),
            SettlementLifecycleEventV2::MakerRebateDayFinalized {
                attribution_day_start_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::MakerRebateDayFinalized {
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                },
                account_delta.as_ref(),
            ),
            SettlementLifecycleEventV2::TakerRebateDayFinalized {
                attribution_day_start_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::TakerRebateDayFinalized {
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                },
                account_delta.as_ref(),
            ),
            SettlementLifecycleEventV2::MakerRebatePaid {
                attribution_day_start_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::MakerRebatePaid {
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                },
                Some(account_delta),
            ),
            SettlementLifecycleEventV2::TakerRebatePaid {
                attribution_day_start_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::TakerRebatePaid {
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                },
                Some(account_delta),
            ),
            SettlementLifecycleEventV2::AutoRedeemApplied {
                condition_id,
                resolution_event_id,
                resolution_source_event_at_utc_ms,
                account_delta,
                ..
            } => (
                SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
                    condition_id: condition_id.clone(),
                    resolution_identity: self.entity_resolution(resolution_event_id)?,
                    resolution_source_event_at_utc_ms: *resolution_source_event_at_utc_ms,
                },
                account_delta.as_ref(),
            ),
        };
        let record = SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence: feedback.metadata.private_feedback_sequence,
            platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
        };
        let enriched = if T::economic_details() {
            self.economic_settlement(&feedback.event)?
        } else {
            None
        };
        let key = match &enriched {
            Some(contracts::economics::SourceNeutralSettlementLifecycleResultV2::DayFinalized(
                day,
            )) => SourceNeutralLogicalKeyV1::RebateDay {
                kind: day.kind,
                attribution_day_start_utc_ms: day.attribution_day_start_utc_ms,
            },
            _ => SourceNeutralLogicalKeyV1::SettlementLifecycle {
                private_feedback_sequence: feedback.metadata.private_feedback_sequence,
            },
        };
        self.push(
            record.clone(),
            key,
            match enriched {
                Some(result) => SourceNeutralEvidenceKind::SettlementLifecycleV2 { result },
                None => SourceNeutralEvidenceKind::SettlementLifecycle { result },
            },
        );
        if let Some(delta) = account_delta {
            self.push_economic(record, delta)?;
        }
        Ok(())
    }

    fn economic_settlement(
        &self,
        event: &SettlementLifecycleEventV2,
    ) -> Result<Option<contracts::economics::SourceNeutralSettlementLifecycleResultV2<I>>, ()> {
        use contracts::economics::{
            RebateContributionFactV1, RebateDayResultV1, RebateKindV1 as K, RebatePaymentFactV1,
            SourceNeutralSettlementLifecycleResultV2 as S,
        };
        Ok(Some(match event {
            SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                condition_id,
                status,
                contributions,
                ..
            } => S::MakerAccrualFinalized {
                condition_id: condition_id.clone(),
                status: match status {
                    MakerRebateAccrualStatusV2::Calculated => {
                        SourceNeutralMakerRebateAccrualStatusV1::Calculated
                    }
                    MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill => {
                        SourceNeutralMakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill
                    }
                    MakerRebateAccrualStatusV2::UnknownIncomplete => {
                        SourceNeutralMakerRebateAccrualStatusV1::UnknownIncomplete
                    }
                },
                contributions: contributions
                    .iter()
                    .map(|c| {
                        Ok(RebateContributionFactV1 {
                            trade_identity: self.entity_trade(&c.trade_id)?,
                            condition_id: condition_id.clone(),
                            kind: K::Maker,
                            attribution_day_start_utc_ms: c.attribution_day_start_utc_ms,
                            contribution_pusd: c.rebate_contribution_pusd.clone(),
                        })
                    })
                    .collect::<Result<_, ()>>()?,
            },
            SettlementLifecycleEventV2::TakerRebateAccrualApplied {
                trade_id,
                condition_id,
                attribution_day_start_utc_ms,
                accrual_pusd,
                ..
            } => S::Contribution(RebateContributionFactV1 {
                trade_identity: self.entity_trade(trade_id)?,
                condition_id: condition_id.clone(),
                kind: K::Taker,
                attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                contribution_pusd: accrual_pusd.clone(),
            }),
            SettlementLifecycleEventV2::MakerRebateDayFinalized {
                attribution_day_start_utc_ms,
                accrual_pusd,
                payable_pusd,
                locked_remainder_pusd,
                contribution_trade_ids,
                ..
            }
            | SettlementLifecycleEventV2::TakerRebateDayFinalized {
                attribution_day_start_utc_ms,
                accrual_pusd,
                payable_pusd,
                locked_remainder_pusd,
                contribution_trade_ids,
                ..
            } => S::DayFinalized(RebateDayResultV1 {
                kind: if matches!(
                    event,
                    SettlementLifecycleEventV2::MakerRebateDayFinalized { .. }
                ) {
                    K::Maker
                } else {
                    K::Taker
                },
                attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                accrued_pusd: accrual_pusd.clone(),
                payable_pusd: payable_pusd.clone(),
                locked_remainder_pusd: locked_remainder_pusd.clone(),
                contribution_refs: contribution_trade_ids
                    .iter()
                    .map(|trade| self.entity_trade(trade))
                    .collect::<Result<_, _>>()?,
            }),
            SettlementLifecycleEventV2::MakerRebatePaid {
                attribution_day_start_utc_ms,
                paid_pusd,
                ..
            }
            | SettlementLifecycleEventV2::TakerRebatePaid {
                attribution_day_start_utc_ms,
                paid_pusd,
                ..
            } => {
                let kind = if matches!(event, SettlementLifecycleEventV2::MakerRebatePaid { .. }) {
                    K::Maker
                } else {
                    K::Taker
                };
                S::Payment(RebatePaymentFactV1 {
                    kind,
                    attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                    day_result_identity: I::logical(
                        &self.account,
                        &SourceNeutralLogicalKeyV1::RebateDay {
                            kind,
                            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
                        },
                    )
                    .map_err(|_| ())?,
                    paid_pusd: paid_pusd.clone(),
                })
            }
            _ => return Ok(None),
        }))
    }

    fn external_cash_identity(&self, identity: &str) -> Result<I, ()> {
        I::entity(
            &self.account,
            &SourceNeutralEntityKeyV1::ExternalCash {
                source_cash_id: identity.into(),
            },
        )
        .map_err(|_| ())
    }

    fn settlement_request_identity(
        &self,
        request: &contracts::economics::SettlementRequestIdV1<String>,
    ) -> Result<contracts::economics::SettlementRequestIdV1<I>, ()> {
        self.check_account(&request.account)?;
        Ok(contracts::economics::SettlementRequestIdV1 {
            account: request.account.clone(),
            condition_id: request.condition_id.clone(),
            resolution_identity: self.entity_resolution(&request.resolution_identity)?,
            policy_version: request.policy_version.clone(),
        })
    }

    fn settlement_cost_identity(&self, resolution: &str, cost: &str) -> Result<I, ()> {
        I::entity(
            &self.account,
            &SourceNeutralEntityKeyV1::SettlementCost {
                source_resolution_id: resolution.to_owned(),
                source_cost_id: cost.to_owned(),
            },
        )
        .map_err(|_| ())
    }

    fn push_requested_economic(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        delta: &PlatformAccountDeltaV2,
        liability: &contracts::ExactDecimal,
    ) -> Result<(), ()> {
        use contracts::economics::{
            SourceNeutralAccountBalanceDeltaV2, SourceNeutralAccountTransitionKindV2 as K,
            SourceNeutralEconomicDispositionV2,
        };
        self.check_account(&delta.account_instance_key)?;
        let PlatformAccountDeltaReasonV2::RequestedSettlement(raw) = &delta.reason else {
            return Err(());
        };
        let transition = match raw {
            K::ResolutionRedeemApplied {
                resolution_identity,
                settlement_request_id,
            } => K::ResolutionRedeemApplied {
                resolution_identity: self.entity_resolution(resolution_identity)?,
                settlement_request_id: self.settlement_request_identity(settlement_request_id)?,
            },
            K::SettlementCostRecognized {
                settlement_request_id,
                cost_identity,
            } => K::SettlementCostRecognized {
                settlement_request_id: self.settlement_request_identity(settlement_request_id)?,
                cost_identity: self.settlement_cost_identity(
                    &settlement_request_id.resolution_identity,
                    cost_identity,
                )?,
            },
            K::SettlementCostPaid {
                settlement_request_id,
                cost_identity,
            } => K::SettlementCostPaid {
                settlement_request_id: self.settlement_request_identity(settlement_request_id)?,
                cost_identity: self.settlement_cost_identity(
                    &settlement_request_id.resolution_identity,
                    cost_identity,
                )?,
            },
            K::ExternalCashApplied { cash_identity } => K::ExternalCashApplied {
                cash_identity: self.external_cash_identity(cash_identity)?,
            },
            K::Existing(_) => return Err(()),
        };
        self.push(
            record,
            SourceNeutralLogicalKeyV1::AccountTransition {
                account_transition_seq: delta.account_transition_seq,
            },
            SourceNeutralEvidenceKind::EconomicV2 {
                evidence: execution_normalization::SourceNeutralEconomicEvidenceV2 {
                    economic_key:
                        execution_normalization::SourceNeutralEconomicKeyV1::AccountTransition {
                            account_transition_seq: delta.account_transition_seq,
                        },
                    disposition: SourceNeutralEconomicDispositionV2::AccountTransition {
                        economic_effective_at_utc_ms: delta.platform_effective_at_utc_ms,
                        condition_id: delta.condition_id.clone(),
                        transition,
                        balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV2 {
                            existing: normalized_balance_delta(delta),
                            settlement_fee_liability_pusd_delta: liability.clone(),
                            actual_taker_fee_liability_pusd_delta: contracts::ExactDecimal(
                                "0".into(),
                            ),
                        }),
                    },
                },
            },
        );
        Ok(())
    }

    fn push_economic(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        delta: &PlatformAccountDeltaV2,
    ) -> Result<(), ()> {
        self.check_account(&delta.account_instance_key)?;
        let transition = match &delta.reason {
            PlatformAccountDeltaReasonV2::RequestedSettlement(_) => return Err(()),
            PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied { platform_order_id } => {
                SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                    order_identity: self.entity_order(platform_order_id)?,
                }
            }
            PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased { platform_order_id } => {
                SourceNeutralAccountTransitionKindV1::FakUnspentEncumbranceReleased {
                    order_identity: self.entity_order(platform_order_id)?,
                }
            }
            PlatformAccountDeltaReasonV2::CallerCancellationEncumbranceReleased {
                platform_order_id,
            } => SourceNeutralAccountTransitionKindV1::CallerCancellationEncumbranceReleased {
                order_identity: self.entity_order(platform_order_id)?,
            },
            PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
                platform_order_id,
                encoded_expires_at_utc_ms,
            } => SourceNeutralAccountTransitionKindV1::GtdExpirationEncumbranceReleased {
                order_identity: self.entity_order(platform_order_id)?,
                encoded_expires_at_utc_ms: *encoded_expires_at_utc_ms,
            },
            PlatformAccountDeltaReasonV2::VenueDustEncumbranceReleased { platform_order_id } => {
                SourceNeutralAccountTransitionKindV1::VenueDustEncumbranceReleased {
                    order_identity: self.entity_order(platform_order_id)?,
                }
            }
            PlatformAccountDeltaReasonV2::MarketResolutionEncumbranceReleased {
                resolution_event_id,
                platform_order_id,
            } => SourceNeutralAccountTransitionKindV1::MarketResolutionEncumbranceReleased {
                resolution_identity: self.entity_resolution(resolution_event_id)?,
                order_identity: self.entity_order(platform_order_id)?,
            },
            PlatformAccountDeltaReasonV2::FillFinalityEconomicApplied {
                trade_id,
                path_ordinal,
            } => SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
                trade_identity: self.entity_trade(trade_id)?,
                path_ordinal: *path_ordinal,
            },
            PlatformAccountDeltaReasonV2::FillFinalityUnappliedFailureReleased {
                trade_id,
                path_ordinal,
            } => SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased {
                trade_identity: self.entity_trade(trade_id)?,
                path_ordinal: *path_ordinal,
            },
            PlatformAccountDeltaReasonV2::FillFinalityCompensationApplied {
                trade_id,
                path_ordinal,
            } => SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
                trade_identity: self.entity_trade(trade_id)?,
                path_ordinal: *path_ordinal,
            },
            PlatformAccountDeltaReasonV2::MakerRebateAccrualFinalized => {
                SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized
            }
            PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied { trade_id } => {
                SourceNeutralAccountTransitionKindV1::TakerRebateAccrualApplied {
                    trade_identity: self.entity_trade(trade_id)?,
                }
            }
            PlatformAccountDeltaReasonV2::MakerRebateDayFinalized {
                attribution_day_utc_ms,
            } => SourceNeutralAccountTransitionKindV1::MakerRebateDayFinalized {
                attribution_day_utc_ms: *attribution_day_utc_ms,
            },
            PlatformAccountDeltaReasonV2::TakerRebateDayFinalized {
                attribution_day_utc_ms,
            } => SourceNeutralAccountTransitionKindV1::TakerRebateDayFinalized {
                attribution_day_utc_ms: *attribution_day_utc_ms,
            },
            PlatformAccountDeltaReasonV2::MakerRebatePaid {
                attribution_day_utc_ms,
            } => SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
                attribution_day_utc_ms: *attribution_day_utc_ms,
            },
            PlatformAccountDeltaReasonV2::TakerRebatePaid {
                attribution_day_utc_ms,
            } => SourceNeutralAccountTransitionKindV1::TakerRebatePaid {
                attribution_day_utc_ms: *attribution_day_utc_ms,
            },
            PlatformAccountDeltaReasonV2::AutoRedeemApplied {
                resolution_event_id,
            } => SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
                resolution_identity: self.entity_resolution(resolution_event_id)?,
            },
        };
        self.push(
            record,
            SourceNeutralLogicalKeyV1::AccountTransition {
                account_transition_seq: delta.account_transition_seq,
            },
            SourceNeutralEvidenceKind::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                    account_transition_seq: delta.account_transition_seq,
                    economic_effective_at_utc_ms: delta.platform_effective_at_utc_ms,
                    condition_id: delta.condition_id.clone(),
                    transition,
                    balance_delta: Box::new(normalized_balance_delta(delta)),
                },
            },
        );
        Ok(())
    }

    fn check_account(&self, account: &AccountInstanceKeyV1) -> Result<(), ()> {
        (account == &self.context.source.account_instance_key)
            .then_some(())
            .ok_or(())
    }

    fn entity_order(&self, source_order_id: &str) -> Result<I, ()> {
        I::entity(
            &self.account,
            &SourceNeutralEntityKeyV1::Order {
                source_order_id: source_order_id.to_owned(),
            },
        )
        .map_err(|_| ())
    }

    fn entity_trade(&self, source_trade_id: &str) -> Result<I, ()> {
        I::entity(
            &self.account,
            &SourceNeutralEntityKeyV1::Trade {
                source_trade_id: source_trade_id.to_owned(),
            },
        )
        .map_err(|_| ())
    }

    fn entity_resolution(&self, source_resolution_id: &str) -> Result<I, ()> {
        I::entity(
            &self.account,
            &SourceNeutralEntityKeyV1::Resolution {
                source_resolution_id: source_resolution_id.to_owned(),
            },
        )
        .map_err(|_| ())
    }

    fn push(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        logical_key: SourceNeutralLogicalKeyV1,
        kind: SourceNeutralEvidenceKind<T::Result, ControlFor<P, I>, I>,
    ) {
        let source_ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.evidence.push(SourceNeutralEvidence {
            source_reference: SimV2SourceReferenceV1 {
                context_reference: self.context.context_reference.clone(),
                watermark: self.watermark.clone(),
                record,
            },
            source_ordinal,
            logical_key,
            kind,
        });
    }
}

fn direct_record(
    metadata: &DirectResponseMetadataV2,
    association: SimV2DirectResponseAssociationV1,
) -> SimV2SourceRecordReferenceV1 {
    SimV2SourceRecordReferenceV1::DirectResponse {
        private_feedback_sequence: metadata.private_feedback_sequence,
        platform_responded_at_utc_ms: metadata.platform_responded_at_utc_ms,
        response_received_at_utc_ms: metadata.response_received_at_utc_ms,
        association,
    }
}

fn map_order_state(status: &SimulatedOrderStatusV2) -> SourceNeutralOrderStateV1 {
    match status.known {
        Some(SimulatedOrderKnownStatusV2::Live) => SourceNeutralOrderStateV1::Open,
        Some(SimulatedOrderKnownStatusV2::Matched) => SourceNeutralOrderStateV1::Matched,
        Some(SimulatedOrderKnownStatusV2::VenueDustTerminal) => {
            SourceNeutralOrderStateV1::VenueDustTerminal
        }
        Some(SimulatedOrderKnownStatusV2::CanceledMarketResolved) => {
            SourceNeutralOrderStateV1::MarketResolvedCanceled
        }
        Some(SimulatedOrderKnownStatusV2::Canceled) => SourceNeutralOrderStateV1::Canceled,
        Some(SimulatedOrderKnownStatusV2::Expired) => SourceNeutralOrderStateV1::Expired,
        Some(SimulatedOrderKnownStatusV2::Delayed) => SourceNeutralOrderStateV1::Delayed,
        Some(SimulatedOrderKnownStatusV2::Unmatched) => SourceNeutralOrderStateV1::Unmatched,
        None => SourceNeutralOrderStateV1::Unknown {
            raw: status.raw.clone(),
        },
    }
}

fn map_trade_state(status: &SimulatedTradeStatusV2) -> SourceNeutralTradeStateV1 {
    match status.known {
        Some(SimulatedTradeKnownStatusV2::MatchedNotBroadcasted) => {
            SourceNeutralTradeStateV1::MatchedNotBroadcasted
        }
        Some(SimulatedTradeKnownStatusV2::Matched) => SourceNeutralTradeStateV1::Matched,
        Some(SimulatedTradeKnownStatusV2::Mined) => SourceNeutralTradeStateV1::Mined,
        Some(SimulatedTradeKnownStatusV2::Confirmed) => SourceNeutralTradeStateV1::Confirmed,
        Some(SimulatedTradeKnownStatusV2::Retrying) => SourceNeutralTradeStateV1::Retrying,
        Some(SimulatedTradeKnownStatusV2::Failed) => SourceNeutralTradeStateV1::Failed,
        None => SourceNeutralTradeStateV1::Unknown {
            raw: status.raw.clone(),
        },
    }
}

fn map_trade_disposition(
    disposition: TradeEconomicDispositionV2,
) -> SourceNeutralTradeEconomicDispositionV1 {
    match disposition {
        TradeEconomicDispositionV2::PendingUnapplied => {
            SourceNeutralTradeEconomicDispositionV1::PendingUnapplied
        }
        TradeEconomicDispositionV2::PendingApplied => {
            SourceNeutralTradeEconomicDispositionV1::PendingApplied
        }
        TradeEconomicDispositionV2::FinalSuccess => {
            SourceNeutralTradeEconomicDispositionV1::FinalSuccess
        }
        TradeEconomicDispositionV2::FinalFailureUnapplied => {
            SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied
        }
        TradeEconomicDispositionV2::FinalFailureCompensated => {
            SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
        }
    }
}

fn map_cursor(cursor: &QueryCursorV2) -> SourceNeutralQueryCursorV1 {
    SourceNeutralQueryCursorV1 {
        root_sequence: cursor.root_sequence,
        next_offset: cursor.next_offset,
    }
}

fn map_out_of_retention(range: &QueryOutOfRetentionV2) -> SourceNeutralQueryOutOfRetentionV1 {
    match range {
        QueryOutOfRetentionV2::OrderHistory {
            retained_from_utc_ms,
        } => SourceNeutralQueryOutOfRetentionV1::OrderHistory {
            retained_from_utc_ms: *retained_from_utc_ms,
        },
        QueryOutOfRetentionV2::TradeHistory {
            retained_from_utc_ms,
        } => SourceNeutralQueryOutOfRetentionV1::TradeHistory {
            retained_from_utc_ms: *retained_from_utc_ms,
        },
        QueryOutOfRetentionV2::RootSnapshot {
            requested_root_sequence,
            earliest_retained_root_sequence,
        } => SourceNeutralQueryOutOfRetentionV1::RootSnapshot {
            requested_root_sequence: *requested_root_sequence,
            earliest_retained_root_sequence: *earliest_retained_root_sequence,
        },
    }
}

fn durable_now_utc_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// 只在公开反馈版本边界选型，核心处理前保留完整成交证据。
pub trait NoeFeedbackPayload<I = StableLogicalIdentityV1>: Clone {
    fn economic_details() -> bool {
        false
    }
    type Result: Clone + std::fmt::Debug + Eq + serde::Serialize;
    fn legacy(&self) -> &contracts::TradeLifecycleFeedbackV2;
    fn normalized(&self, identity: SourceNeutralTradeLifecycleResultV1<I>) -> Self::Result;
    fn enriched(
        &self,
        _identity: SourceNeutralTradeLifecycleResultV1<I>,
    ) -> Option<contracts::economics::SourceNeutralTradeLifecycleResultV3<I>> {
        None
    }

    fn batch(
        payload: &BusinessProjectionBatchPayloadV1,
    ) -> Option<Arc<BusinessProjectionBatch<Self>>>;
}
impl<I: Clone + std::fmt::Debug + Eq + serde::Serialize> NoeFeedbackPayload<I>
    for contracts::TradeLifecycleFeedbackV2
{
    type Result = SourceNeutralTradeLifecycleResultV1<I>;
    fn legacy(&self) -> &Self {
        self
    }
    fn normalized(&self, identity: SourceNeutralTradeLifecycleResultV1<I>) -> Self::Result {
        identity
    }
    fn batch(
        payload: &BusinessProjectionBatchPayloadV1,
    ) -> Option<Arc<BusinessProjectionBatch<Self>>> {
        match payload {
            BusinessProjectionBatchPayloadV1::V2(b) => Some(Arc::clone(b)),
            _ => None,
        }
    }
}
impl<I: Clone + std::fmt::Debug + Eq + serde::Serialize> NoeFeedbackPayload<I>
    for contracts::TradeLifecycleFeedbackV3
{
    type Result = contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2<I>;
    fn economic_details() -> bool {
        true
    }
    fn legacy(&self) -> &contracts::TradeLifecycleFeedbackV2 {
        &self.feedback
    }
    fn normalized(&self, identity: SourceNeutralTradeLifecycleResultV1<I>) -> Self::Result {
        Self::Result {
            trade_identity: identity.trade_identity,
            order_identity: identity.order_identity,
            condition_id: identity.condition_id,
            token_id: identity.token_id,
            state: identity.state,
            economic_disposition: identity.economic_disposition,
            fill_sequence: self.fill_sequence,
            matched_at_utc_ms: self.matched_at_utc_ms,
            liquidity_role: self.liquidity_role,
            source_price_pusd_per_share: self.source_price_pusd_per_share.clone(),
            base_quantity_shares: self.base_quantity_shares.clone(),
            quote_amount_pusd: self.quote_amount_pusd.clone(),
            immediate_taker_fee_pusd: self.immediate_taker_fee_pusd.clone(),
            economic_application_account_transition_seq: self
                .economic_application_account_transition_seq,
        }
    }
    fn enriched(
        &self,
        identity: SourceNeutralTradeLifecycleResultV1<I>,
    ) -> Option<contracts::economics::SourceNeutralTradeLifecycleResultV3<I>> {
        Some(contracts::economics::SourceNeutralTradeLifecycleResultV3 {
            result: self.normalized(identity),
            maker_fee_equivalent_pusd: match self.liquidity_role {
                contracts::TradeLiquidityRoleV2::Taker => {
                    contracts::ExecutionReadValueV1::NotApplicable
                }
                contracts::TradeLiquidityRoleV2::MakerInferred => self
                    .maker_fee_equivalent_pusd
                    .clone()
                    .map(contracts::ExecutionReadValueV1::Known)
                    .unwrap_or(contracts::ExecutionReadValueV1::Unavailable),
            },
        })
    }
    fn batch(
        payload: &BusinessProjectionBatchPayloadV1,
    ) -> Option<Arc<BusinessProjectionBatch<Self>>> {
        match payload {
            BusinessProjectionBatchPayloadV1::V3(b) => Some(Arc::clone(b)),
            _ => None,
        }
    }
}
pub type SimV2NoeProjectionPortV1 = SimNoeProjectionPort<contracts::TradeLifecycleFeedbackV2>;
pub type SimV2NoeProjectionPortV2 = SimNoeProjectionPort<contracts::TradeLifecycleFeedbackV3>;
#[cfg(test)]
type BatchMapperV1<'a> = BatchMapper<'a, contracts::TradeLifecycleFeedbackV2>;

type ControlFor<P, I = StableLogicalIdentityV1> = SourceNeutralControlResult<
    <P as OrderEvidenceProjection<I>>::Submission,
    <P as OrderEvidenceProjection<I>>::Snapshot,
    I,
>;
/// 只选择源中立数量载荷；源序、身份、经济和耐久处理继续共用原实现。
pub trait OrderEvidenceProjection<I = StableLogicalIdentityV1> {
    type Submission: Clone + std::fmt::Debug + Eq + serde::Serialize;
    type Snapshot: Clone + std::fmt::Debug + Eq + serde::Serialize;
    fn accepted(
        order: SourceNeutralOrderSnapshotV1<I>,
        source: &contracts::AcceptedSubmissionItemV2,
        effective_processing_at_utc_ms: i64,
    ) -> Self::Submission;
    fn rejected() -> Self::Submission;
    fn unknown() -> Self::Submission;
    fn snapshot(
        identity: SourceNeutralOrderSnapshotV1<I>,
        source: &OrderProjectionV2,
    ) -> Self::Snapshot;
}
#[derive(Clone, Debug, Default)]
pub struct LegacyOrderEvidence;
impl<I: Clone + std::fmt::Debug + Eq + serde::Serialize> OrderEvidenceProjection<I>
    for LegacyOrderEvidence
{
    type Submission = SourceNeutralSubmissionOutcomeV1<I>;
    type Snapshot = SourceNeutralOrderSnapshotV1<I>;
    fn accepted(
        order: SourceNeutralOrderSnapshotV1<I>,
        _: &contracts::AcceptedSubmissionItemV2,
        _: i64,
    ) -> Self::Submission {
        SourceNeutralSubmissionOutcomeV1::Accepted { order }
    }
    fn rejected() -> Self::Submission {
        SourceNeutralSubmissionOutcomeV1::RejectedNoEffect
    }
    fn unknown() -> Self::Submission {
        SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect
    }
    fn snapshot(
        identity: SourceNeutralOrderSnapshotV1<I>,
        _: &OrderProjectionV2,
    ) -> Self::Snapshot {
        identity
    }
}
#[derive(Clone, Debug, Default)]
pub struct MeasuredOrderEvidence;
impl<I: Clone + std::fmt::Debug + Eq + serde::Serialize> OrderEvidenceProjection<I>
    for MeasuredOrderEvidence
{
    type Submission = contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2<I>;
    type Snapshot = contracts::execution_normalization::SourceNeutralOrderSnapshotV2<I>;
    fn accepted(
        order: SourceNeutralOrderSnapshotV1<I>,
        source: &contracts::AcceptedSubmissionItemV2,
        effective_processing_at_utc_ms: i64,
    ) -> Self::Submission {
        use contracts::{
            SubmissionMeasurementV2,
            execution_normalization::SourceNeutralSubmissionMeasurementV1 as M,
        };
        let measurement = match &source.measurement {
            SubmissionMeasurementV2::Base {
                matched_base_quantity_shares,
                open_base_quantity_shares,
                ..
            } => M::Base {
                matched_base_quantity_shares: matched_base_quantity_shares.clone(),
                open_base_quantity_shares: open_base_quantity_shares.clone(),
            },
            SubmissionMeasurementV2::QuoteBudget {
                actual_matched_base_quantity_shares,
                ..
            } => M::QuoteBudget {
                actual_matched_base_quantity_shares: actual_matched_base_quantity_shares.clone(),
            },
        };
        Self::Submission::Accepted {
            order,
            measurement,
            effective_processing_at_utc_ms,
        }
    }
    fn rejected() -> Self::Submission {
        Self::Submission::RejectedNoEffect
    }
    fn unknown() -> Self::Submission {
        Self::Submission::EvidenceUnknownNoEffect
    }
    fn snapshot(
        identity: SourceNeutralOrderSnapshotV1<I>,
        source: &OrderProjectionV2,
    ) -> Self::Snapshot {
        use contracts::{
            OrderQuantityProjectionV2,
            execution_normalization::SourceNeutralOrderQuantityEvidenceV1 as Q,
        };
        let quantities = match &source.quantities {
            OrderQuantityProjectionV2::Base {
                matched_base_quantity_shares,
                active_unfilled_base_quantity_shares,
                ..
            } => Q::Base {
                matched_base_quantity_shares: matched_base_quantity_shares.clone(),
                active_unfilled_base_quantity_shares: active_unfilled_base_quantity_shares.clone(),
            },
            OrderQuantityProjectionV2::QuoteBudget {
                actual_matched_base_quantity_shares,
                ..
            } => Q::QuoteBudget {
                actual_matched_base_quantity_shares: actual_matched_base_quantity_shares.clone(),
            },
        };
        Self::Snapshot {
            order_identity: identity.order_identity,
            condition_id: identity.condition_id,
            token_id: identity.token_id,
            state: identity.state,
            quantities,
        }
    }
}
pub type SimV2NoeProjectionPortV3 =
    SimNoeProjectionPort<contracts::TradeLifecycleFeedbackV3, MeasuredOrderEvidence>;
#[cfg(test)]
pub(super) fn project_real_query_for_tests(
    source_path: &Path,
    opened: &RunOpenedV2,
    opening: &Arc<contracts::AccountOpeningFactV2>,
    query: &super::QueryActionResultV2,
) -> serde_json::Value {
    let directory = tempfile::tempdir().unwrap();
    let source = DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            source_path.to_string_lossy().into_owned(),
        ),
    };
    let mut port = SimV2NoeProjectionPortV3::new(
        directory.path(),
        opened.clone(),
        source.clone(),
        source,
        "opening".into(),
        "order-query-test".into(),
    );
    let batch = super::BusinessProjectionBatchV3 {
        initial_markets: Vec::new(),
        run_id: opened.run_id.clone(),
        projection_batch_sequence: 1,
        account_opening: Some(Arc::clone(opening)),
        market_enrollment: None,
        direct_responses: vec![],
        order_lifecycle: vec![],
        trade_lifecycle: vec![],
        settlement_lifecycle: vec![],
    };
    assert!(port.open_from_first_batch(&batch));
    let mapper = BatchMapper::<_, MeasuredOrderEvidence>::new(
        port.store.as_ref().unwrap().context(),
        &batch,
    );
    match query {
        super::QueryActionResultV2::Order(r) => {
            serde_json::to_value(mapper.map_order_query(r).unwrap()).unwrap()
        }
        super::QueryActionResultV2::Orders(r) => {
            serde_json::to_value(mapper.map_orders_query(r).unwrap()).unwrap()
        }
        super::QueryActionResultV2::Trades(_) => unreachable!(),
    }
}

#[cfg(test)]
pub(super) fn assert_order_controls_v3(
    root: &Path,
    context: &contracts::execution_normalization::ExecutionNormalizationContextReferenceV1,
    payloads: &[Arc<BusinessProjectionBatchPayloadV1>],
) {
    use contracts::execution_normalization::{
        SourceNeutralControlResultV2, SourceNeutralSubmissionMeasurementV1,
        SourceNeutralSubmissionOutcomeV2,
    };
    use execution_normalization::{
        DurableContextStoreV3, NormalizationRecordV3, SourceNeutralEvidenceKindV3,
    };
    let mut store = DurableContextStoreV3::open(root, context).unwrap();
    let records: Vec<_> = store
        .commits()
        .iter()
        .flat_map(|c| c.records.iter().cloned())
        .collect();
    let mut seen_submission = false;
    let mut seen_rejected = false;
    let mut seen_batch_time_difference = false;
    for record in &records {
        if let NormalizationRecordV3::Evidence {
            evidence:
                SourceNeutralEvidenceKindV3::Control {
                    result:
                        SourceNeutralControlResultV2::Submission {
                            outcome:
                                SourceNeutralSubmissionOutcomeV2::Accepted {
                                    measurement,
                                    effective_processing_at_utc_ms,
                                    ..
                                },
                        },
                },
            source_reference,
            ..
        } = record
        {
            let contracts::execution_normalization::SimV2SourceRecordReferenceV1::DirectResponse {
                association:
                    SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id,
                        batch_item_ordinal,
                    },
                ..
            } = &source_reference.record
            else {
                panic!("提交源关联");
            };
            let item = payloads
                .iter()
                .find_map(|p| match p.as_ref() {
                    BusinessProjectionBatchPayloadV1::V3(b) => {
                        b.direct_responses().find_map(|r| match r {
                            BusinessDirectResponseV2::PostOrder(r)
                                if &r.metadata.global_request_id == global_request_id =>
                            {
                                Some(&r.item)
                            }
                            BusinessDirectResponseV2::PostOrders(r)
                                if &r.metadata.global_request_id == global_request_id =>
                            {
                                r.items
                                    .iter()
                                    .find(|i| &i.batch_item_ordinal == batch_item_ordinal)
                            }
                            _ => None,
                        })
                    }
                    _ => None,
                })
                .unwrap();
            assert_eq!(*batch_item_ordinal, item.batch_item_ordinal);
            assert_eq!(
                *effective_processing_at_utc_ms,
                item.effective_processing_at_utc_ms
            );
            if global_request_id == "quantity-batch"
                && let SimV2SourceRecordReferenceV1::DirectResponse {
                    platform_responded_at_utc_ms,
                    ..
                } = source_reference.record
            {
                seen_batch_time_difference |=
                    *effective_processing_at_utc_ms != platform_responded_at_utc_ms;
            }

            let expected = match item.item_id.as_str() {
                "two-fills-item" => SourceNeutralSubmissionMeasurementV1::Base {
                    matched_base_quantity_shares: contracts::ExactDecimal("10.005".into()),
                    open_base_quantity_shares: contracts::ExactDecimal("0".into()),
                },
                "improved-fak" => SourceNeutralSubmissionMeasurementV1::QuoteBudget {
                    actual_matched_base_quantity_shares: contracts::ExactDecimal("6.000001".into()),
                },
                "partial-base" => SourceNeutralSubmissionMeasurementV1::Base {
                    matched_base_quantity_shares: contracts::ExactDecimal("3.999999".into()),
                    open_base_quantity_shares: contracts::ExactDecimal("3.000001".into()),
                },
                "zero-matched" => SourceNeutralSubmissionMeasurementV1::Base {
                    matched_base_quantity_shares: contracts::ExactDecimal("0".into()),
                    open_base_quantity_shares: contracts::ExactDecimal("7".into()),
                },
                _ => panic!("仅使用真实代表请求"),
            };
            assert_eq!(*measurement, expected);
            if item.item_id == "two-fills-item" {
                let contracts::SubmissionItemOutcomeV2::Accepted(accepted) = &item.outcome else {
                    unreachable!()
                };
                assert_eq!(
                    accepted.terminal_reason,
                    Some(contracts::SimOrderTerminalReasonV2::VenueDustTerminal)
                );
            }
            seen_submission = true;
        }
    }
    for record in &records {
        if let NormalizationRecordV3::Evidence {
            evidence: SourceNeutralEvidenceKindV3::Control { result },
            ..
        } = record
        {
            use contracts::execution_normalization::{
                SourceNeutralOrderQuantityEvidenceV1 as Q, SourceNeutralOrderQueryResultV2 as O,
                SourceNeutralOrdersQueryResultV2 as Os,
            };
            match result {
                SourceNeutralControlResultV2::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
                } => seen_rejected = true,
                SourceNeutralControlResultV2::OrderQuery {
                    result: O::Found { order },
                } => assert_eq!(
                    order.quantities,
                    Q::Base {
                        matched_base_quantity_shares: contracts::ExactDecimal("3.999999".into()),
                        active_unfilled_base_quantity_shares: contracts::ExactDecimal(
                            "3.000001".into()
                        )
                    }
                ),
                SourceNeutralControlResultV2::OrdersQuery {
                    result: Os::Page(page),
                } => {
                    assert_eq!(page.items.len(), 2);
                    assert_eq!(
                        page.items[0].quantities,
                        Q::Base {
                            matched_base_quantity_shares: contracts::ExactDecimal(
                                "3.999999".into()
                            ),
                            active_unfilled_base_quantity_shares: contracts::ExactDecimal(
                                "3.000001".into()
                            )
                        }
                    );
                    assert_eq!(
                        page.items[1].quantities,
                        Q::Base {
                            matched_base_quantity_shares: contracts::ExactDecimal("0".into()),
                            active_unfilled_base_quantity_shares: contracts::ExactDecimal(
                                "7".into()
                            )
                        }
                    );
                    let source = payloads
                        .iter()
                        .find_map(|p| match p.as_ref() {
                            BusinessProjectionBatchPayloadV1::V3(b) => {
                                b.direct_responses().find_map(|r| match r {
                                    BusinessDirectResponseV2::Orders(r) => match &r.result {
                                        OrdersQueryResultV2::Page(p) => Some(p),
                                        _ => None,
                                    },
                                    _ => None,
                                })
                            }
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(page.root_sequence, source.root_sequence);
                    assert_eq!(
                        page.next_cursor
                            .as_ref()
                            .map(|c| (c.root_sequence, c.next_offset)),
                        source
                            .next_cursor
                            .as_ref()
                            .map(|c| (c.root_sequence, c.next_offset))
                    );
                }
                _ => {}
            }
        }
    }
    assert!(seen_rejected);
    assert!(
        seen_batch_time_difference,
        "不得将各项处理时间替换成整批响应时间"
    );
    assert!(seen_submission, "真实提交数量不能在 NOE 丢失");
    assert!(records.iter().any(|r| matches!(r, NormalizationRecordV3::Evidence { evidence: SourceNeutralEvidenceKindV3::Control { result: SourceNeutralControlResultV2::OrderQuery { result: contracts::execution_normalization::SourceNeutralOrderQueryResultV2::Found { .. } } }, .. })));
    assert!(records.iter().any(|r| matches!(
        r,
        NormalizationRecordV3::Evidence {
            evidence: SourceNeutralEvidenceKindV3::Control {
                result: SourceNeutralControlResultV2::OrdersQuery { .. }
            },
            ..
        }
    )));
    let batch = payloads
        .iter()
        .find_map(|p| match p.as_ref() {
            BusinessProjectionBatchPayloadV1::V3(b)
                if b.direct_responses()
                    .any(|r| matches!(r, BusinessDirectResponseV2::PostOrder(_))) =>
            {
                Some(b)
            }
            _ => None,
        })
        .unwrap();
    let original = BatchMapper::<_, MeasuredOrderEvidence>::new(store.context(), batch)
        .map()
        .unwrap();
    let next = store.durable_through().unwrap().projection_batch_sequence + 1;
    for change in 0..3 {
        let mut input = original.clone();
        input.source_watermark.projection_batch_sequence = next + change;
        for e in &mut input.evidence {
            e.source_reference.watermark = input.source_watermark.clone();
        }
        if change != 0 {
            input.evidence.retain(|e| {
                matches!(
                    e.kind,
                    SourceNeutralEvidenceKind::Control {
                        result: SourceNeutralControlResultV2::Submission { .. }
                    }
                )
            });
            for e in &mut input.evidence {
                if let SourceNeutralEvidenceKind::Control {
                    result:
                        SourceNeutralControlResultV2::Submission {
                            outcome:
                                SourceNeutralSubmissionOutcomeV2::Accepted {
                                    measurement:
                                        SourceNeutralSubmissionMeasurementV1::Base {
                                            open_base_quantity_shares,
                                            ..
                                        },
                                    effective_processing_at_utc_ms,
                                    ..
                                },
                        },
                } = &mut e.kind
                {
                    if change == 1 {
                        *open_base_quantity_shares = contracts::ExactDecimal("1".into());
                    } else {
                        *effective_processing_at_utc_ms += 1;
                    }
                }
            }
        }
        store.commit_batch_v3(input, durable_now_utc_ms()).unwrap();
        let last = &store.commits().last().unwrap().records;
        if change == 0 {
            assert!(
                last.iter()
                    .all(|r| matches!(r, NormalizationRecordV3::Duplicate { .. }))
            );
        } else {
            assert!(
                last.iter()
                    .all(|r| matches!(r, NormalizationRecordV3::Conflict { .. }))
            );
        }
    }
    let mut alias = original.clone();
    alias.source_watermark.projection_batch_sequence = next + 3;
    alias
        .evidence
        .retain(|e| matches!(e.kind, SourceNeutralEvidenceKind::Economic { .. }));
    alias.evidence.truncate(1);
    assert_eq!(alias.evidence.len(), 1);
    alias.evidence[0].source_reference.watermark = alias.source_watermark.clone();
    alias.evidence[0].logical_key = SourceNeutralLogicalKeyV1::EconomicEvidence {
        evidence_identity: "same-real-order-economic-evidence".into(),
    };
    store.commit_batch_v3(alias, durable_now_utc_ms()).unwrap();
    assert!(matches!(
        store.commits().last().unwrap().records.as_slice(),
        [NormalizationRecordV3::EconomicAlias { .. }]
    ));
}

#[cfg(test)]
pub(super) fn assert_v3_rebuilt_evidence_semantics(
    root: &std::path::Path,
    context: &contracts::execution_normalization::ExecutionNormalizationContextReferenceV1,
    batch: &super::BusinessProjectionBatchV3,
) {
    use execution_normalization::{
        DurableContextStoreV2, NormalizationCoreV1, NormalizationRecordV2,
    };
    let mut store = DurableContextStoreV2::open(root, context).unwrap();
    let original = BatchMapper::<_, LegacyOrderEvidence>::new(store.context(), batch)
        .map()
        .unwrap();
    let next = store.durable_through().unwrap().projection_batch_sequence + 1;
    let mut duplicate = original.clone();
    duplicate.source_watermark.projection_batch_sequence = next;
    for e in &mut duplicate.evidence {
        e.source_reference.watermark = duplicate.source_watermark.clone();
    }
    // 已签发的核心入口仍可显式选择新载荷，且与耐久核心共用同一实现。
    let core = NormalizationCoreV1::<
        contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    >::new(store.context().clone());
    core.prepare_batch_v2(duplicate.clone()).unwrap();
    store
        .commit_batch_v2(duplicate, durable_now_utc_ms())
        .unwrap();
    assert!(
        store
            .commits()
            .last()
            .unwrap()
            .records
            .iter()
            .all(|r| matches!(r, NormalizationRecordV2::Duplicate { .. }))
    );
    // 同一真实经济事实以另一个证据身份再次提供，沿现有别名逻辑引用已恢复事实。
    let mut economic = original
        .evidence
        .iter()
        .find(|e| matches!(e.kind, SourceNeutralEvidenceKind::Economic { .. }))
        .unwrap()
        .clone();
    economic.logical_key = SourceNeutralLogicalKeyV1::EconomicEvidence {
        evidence_identity: format!(
            "repeated-public-evidence:{:?}",
            economic.source_reference.record
        ),
    };
    let mut alias = original.clone();
    alias.source_watermark.projection_batch_sequence = next + 1;
    economic.source_reference.watermark = alias.source_watermark.clone();
    alias.evidence = vec![economic];
    store.commit_batch_v2(alias, durable_now_utc_ms()).unwrap();
    assert!(matches!(
        store.commits().last().unwrap().records.as_slice(),
        [NormalizationRecordV2::EconomicAlias { .. }]
    ));
    // 仅新字段不同的反证不得被降为旧版后判作重复。
    for (offset, change_sequence) in [(2, true), (3, false)] {
        let mut changed = original
            .evidence
            .iter()
            .find(|e| matches!(e.kind, SourceNeutralEvidenceKind::TradeLifecycleV3 { .. }))
            .unwrap()
            .clone();
        if let SourceNeutralEvidenceKind::TradeLifecycleV3 { result } = &mut changed.kind {
            if change_sequence {
                result.result.fill_sequence += 1;
            } else {
                result.result.matched_at_utc_ms += 1;
            }
        }
        let mut conflicting = original.clone();
        conflicting.source_watermark.projection_batch_sequence = next + offset;
        changed.source_reference.watermark = conflicting.source_watermark.clone();
        conflicting.evidence = vec![changed];
        store
            .commit_batch_v2(conflicting, durable_now_utc_ms())
            .unwrap();
        assert!(matches!(
            store.commits().last().unwrap().records.as_slice(),
            [NormalizationRecordV2::Conflict { .. }]
        ));
    }
}

#[cfg(test)]
mod tests {
    use execution_normalization::SourceNeutralEvidenceKindV1;
    use std::fs;
    use std::time::Instant;

    use contracts::execution_normalization::ImmutableSourceReferenceV1;
    #[cfg(feature = "gate-b-flow")]
    use contracts::execution_normalization::SourceNeutralEconomicDispositionV1;
    use contracts::{
        AcceptedSubmissionItemV2, AssetIdentityV1, CancelMarketOrdersDirectResponseV2,
        CancelNotCanceledReasonV2, CancelOrderDirectResponseV2, CancelOrdersDirectResponseV2,
        CancelTargetResponseV2, CanceledTargetMeasurementV2, ClobMarketId, ConditionId,
        ExactDecimal, ExecutionMethodV2, InitialAccountStateV2, LifecycleFeedbackMetadataV2,
        MarketEnrollmentV2, MarketIdentityV2, MarketResolutionTerminalV3, MarketWindowV2,
        OrderDirectResponseV2, OrderLifecycleFeedbackV2, OrdersDirectResponseV2, OutcomeIdentityV1,
        PlatformAccountBalanceDeltaV1, PlatformOutcomeBalanceDeltaV1, PlatformSourceV1,
        PostOrderDirectResponseV2, PostOrdersDirectResponseV2, SettlementLifecycleFeedbackV2,
        Sha256Hex, SimCallerRoleV1, SubmissionMeasurementV2, SubmissionResponseItemV2, TokenId,
        TradeLifecycleFeedbackV2, TradeLiquidityRoleV2, TradesDirectResponseV2,
    };
    use execution_normalization::{NormalizationRecordV1, read_noe_result_v1};
    #[cfg(feature = "gate-b-flow")]
    use ledger::{AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerErrorV1, LedgerWriterV1};
    use tempfile::TempDir;

    use super::*;
    use crate::platform_rules::ImplementationIdentityV1;
    use crate::run_v2::StoredDirectResponseV2;
    use crate::runtime_v2::RunDurabilityRoleV2;

    fn sha(value: char) -> Sha256Hex {
        Sha256Hex(std::iter::repeat_n(value, 64).collect())
    }

    fn account_key(account_id: &str) -> AccountInstanceKeyV1 {
        AccountInstanceKeyV1 {
            run_id: "run-noe-58".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-noe-58".to_owned(),
            account_id: account_id.to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        }
    }

    fn opened() -> RunOpenedV2 {
        RunOpenedV2 {
            projection_version: Default::default(),
            run_id: "run-noe-58".to_owned(),
            durability_role: RunDurabilityRoleV2::CleanRun,
            platform_rule_snapshot_file_sha256: sha('1'),
            trade_finality_policy_file_sha256: sha('2'),
            platform_scenario_file_sha256: sha('3'),
            proxy_latency_profile_file_sha256: sha('4'),
            implementation_identity: ImplementationIdentityV1 {
                git_commit: "a".repeat(40),
                source_identity_sha256: sha('5'),
                cargo_lock_sha256: sha('6'),
            },
            market_event_source_identity_sha256: Some(sha('7')),
        }
    }

    fn new_port(root: &Path) -> SimV2NoeProjectionPortV1 {
        SimV2NoeProjectionPortV1::new(
            root,
            opened(),
            DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(sha('8')),
            },
            DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(sha('9')),
            },
            "opening-noe-58".to_owned(),
            "noe-stage-2-test".to_owned(),
        )
    }

    fn opening(account_id: &str) -> Arc<contracts::AccountOpeningFactV2> {
        Arc::new(contracts::AccountOpeningFactV2 {
            creation_request_id: "opening-noe-58".to_owned(),
            account_instance_key: account_key(account_id),
            session_id: "session-noe-58".to_owned(),
            initial_platform_scenario_file_sha256: sha('3'),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("2000".to_owned()),
            },
        })
    }

    fn market() -> MarketEnrollmentV2 {
        MarketEnrollmentV2 {
            enrollment_request_id: "enroll-noe-58".to_owned(),
            enrollment_sequence: 1,
            market: MarketIdentityV2 {
                condition_id: ConditionId("condition-noe-58".to_owned()),
                clob_market_id: ClobMarketId("clob-noe-58".to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_000,
                    end_at_utc_ms: 2_000,
                },
            },
            assets: [
                AssetIdentityV1 {
                    token_id: TokenId("token-up-noe-58".to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId("token-down-noe-58".to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        }
    }

    fn metadata(id: &str, method: ExecutionMethodV2, sequence: u64) -> DirectResponseMetadataV2 {
        DirectResponseMetadataV2 {
            global_request_id: id.to_owned(),
            caller_correlation_id: None,
            method,
            platform_responded_at_utc_ms: 100 + sequence as i64,
            response_received_at_utc_ms: 110 + sequence as i64,
            private_feedback_sequence: sequence,
        }
    }

    fn status() -> SimulatedOrderStatusV2 {
        SimulatedOrderStatusV2 {
            raw: "LIVE".to_owned(),
            known: Some(SimulatedOrderKnownStatusV2::Live),
        }
    }

    fn submission_item(outcome: SubmissionItemOutcomeV2) -> SubmissionResponseItemV2 {
        SubmissionResponseItemV2 {
            item_id: "item-noe-58".to_owned(),
            batch_item_ordinal: 0,
            condition_id: ConditionId("condition-noe-58".to_owned()),
            token_id: TokenId("token-up-noe-58".to_owned()),
            effective_processing_at_utc_ms: 100,
            outcome,
        }
    }

    fn accepted_submission() -> SubmissionItemOutcomeV2 {
        SubmissionItemOutcomeV2::Accepted(AcceptedSubmissionItemV2 {
            platform_order_id: "order-noe-58".to_owned(),
            platform_order_status: status(),
            fills: Vec::new(),
            terminal_reason: None,
            measurement: SubmissionMeasurementV2::Base {
                accepted_base_quantity_shares: ExactDecimal("1".to_owned()),
                matched_base_quantity_shares: ExactDecimal("0".to_owned()),
                open_base_quantity_shares: ExactDecimal("1".to_owned()),
                venue_dust_closed_base_quantity_shares: ExactDecimal("0".to_owned()),
                order_type_terminated_base_quantity_shares: ExactDecimal("0".to_owned()),
                accepted_quote_amount_pusd: ExactDecimal("10".to_owned()),
                matched_quote_amount_pusd: ExactDecimal("0".to_owned()),
                open_encumbered_quote_amount_pusd: ExactDecimal("10".to_owned()),
                venue_dust_released_quote_amount_pusd: ExactDecimal("0".to_owned()),
                released_price_improvement_quote_amount_pusd: ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbered_pusd: ExactDecimal("0".to_owned()),
            },
            account_transition_sequences: vec![1],
        })
    }

    fn cancel_target(outcome: CancelTargetOutcomeV2) -> CancelTargetResponseV2 {
        CancelTargetResponseV2 {
            target_ordinal: 0,
            platform_order_id: "order-noe-58".to_owned(),
            input_item_refs: Vec::new(),
            effective_processing_at_utc_ms: 120,
            outcome,
        }
    }

    fn direct_responses() -> Vec<Arc<StoredDirectResponseV2>> {
        vec![
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::PostOrder(PostOrderDirectResponseV2 {
                    metadata: metadata("post-one", ExecutionMethodV2::PostOrder, 1),
                    item: submission_item(accepted_submission()),
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::PostOrders(PostOrdersDirectResponseV2 {
                    metadata: metadata("post-many", ExecutionMethodV2::PostOrders, 2),
                    items: vec![submission_item(
                        SubmissionItemOutcomeV2::EvidenceUnknownNoEffect(
                            contracts::SubmissionEvidenceUnknownReasonV2::Preloading,
                        ),
                    )],
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::CancelOrder(CancelOrderDirectResponseV2 {
                    metadata: metadata("cancel-one", ExecutionMethodV2::CancelOrder, 3),
                    target: cancel_target(CancelTargetOutcomeV2::NotCanceledNoEffect(
                        CancelNotCanceledReasonV2::NotFoundOrNotOwned,
                    )),
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::CancelOrders(CancelOrdersDirectResponseV2 {
                    metadata: metadata("cancel-many", ExecutionMethodV2::CancelOrders, 4),
                    targets: vec![cancel_target(
                        CancelTargetOutcomeV2::EvidenceUnknownNoEffect,
                    )],
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::CancelMarketOrders(CancelMarketOrdersDirectResponseV2 {
                    metadata: metadata("cancel-market", ExecutionMethodV2::CancelMarketOrders, 5),
                    targets: vec![
                        cancel_target(CancelTargetOutcomeV2::Canceled(
                            CanceledTargetMeasurementV2 {
                                canceled_base_quantity_shares: ExactDecimal("1".to_owned()),
                                released_quote_amount_pusd: ExactDecimal("10".to_owned()),
                                account_transition_seq: Some(3),
                            },
                        )),
                        CancelTargetResponseV2 {
                            target_ordinal: 1,
                            ..cancel_target(CancelTargetOutcomeV2::NotCanceledNoEffect(
                                CancelNotCanceledReasonV2::NoCancellableRemainder,
                            ))
                        },
                    ],
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::CancelAllOrders(
                    contracts::CancelAllOrdersDirectResponseV2 {
                        metadata: metadata("cancel-all", ExecutionMethodV2::CancelAllOrders, 6),
                        targets: vec![cancel_target(CancelTargetOutcomeV2::Canceled(
                            CanceledTargetMeasurementV2 {
                                canceled_base_quantity_shares: ExactDecimal("1".to_owned()),
                                released_quote_amount_pusd: ExactDecimal("10".to_owned()),
                                account_transition_seq: Some(3),
                            },
                        ))],
                    },
                ),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::Order(OrderDirectResponseV2 {
                    metadata: metadata("order", ExecutionMethodV2::Order, 7),
                    result: OrderQueryResultV2::NotFound,
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::Orders(OrdersDirectResponseV2 {
                    metadata: metadata("orders", ExecutionMethodV2::Orders, 8),
                    result: OrdersQueryResultV2::InvalidCursor,
                }),
            )),
            Arc::new(StoredDirectResponseV2::Final(
                BusinessDirectResponseV2::Trades(TradesDirectResponseV2 {
                    metadata: metadata("trades", ExecutionMethodV2::Trades, 9),
                    result: TradesQueryResultV2::OutOfRetention(
                        QueryOutOfRetentionV2::TradeHistory {
                            retained_from_utc_ms: 50,
                        },
                    ),
                }),
            )),
        ]
    }

    fn zero_balance_delta() -> PlatformAccountBalanceDeltaV1 {
        PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: ExactDecimal("0".to_owned()),
            available_pusd_delta: ExactDecimal("0".to_owned()),
            principal_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
            immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
            outcome_balance_deltas: vec![PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId("token-up-noe-58".to_owned()),
                balance_shares_delta: ExactDecimal("0".to_owned()),
            }],
            maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
        }
    }

    fn delta(sequence: u64, reason: PlatformAccountDeltaReasonV2) -> PlatformAccountDeltaV2 {
        PlatformAccountDeltaV2 {
            account_instance_key: account_key("account-noe-58"),
            account_transition_seq: sequence,
            reason,
            condition_id: Some(ConditionId("condition-noe-58".to_owned())),
            platform_effective_at_utc_ms: 200 + sequence as i64,
            balance_delta: zero_balance_delta(),
        }
    }

    fn lifecycle_metadata(sequence: u64) -> LifecycleFeedbackMetadataV2 {
        LifecycleFeedbackMetadataV2 {
            account_instance_key: account_key("account-noe-58"),
            platform_event_at_utc_ms: 200 + sequence as i64,
            feedback_received_at_utc_ms: 210 + sequence as i64,
            private_feedback_sequence: sequence,
        }
    }

    fn order_lifecycle() -> Vec<OrderLifecycleFeedbackV2> {
        let feedback = |sequence, event| OrderLifecycleFeedbackV2 {
            metadata: lifecycle_metadata(sequence),
            platform_order_id: "order-noe-58".to_owned(),
            condition_id: ConditionId("condition-noe-58".to_owned()),
            token_id: TokenId("token-up-noe-58".to_owned()),
            event,
        };
        vec![
            feedback(
                20,
                OrderLifecycleEventV2::OrderAccepted {
                    status: status(),
                    account_delta: delta(
                        1,
                        PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                            platform_order_id: "order-noe-58".to_owned(),
                        },
                    ),
                },
            ),
            feedback(
                21,
                OrderLifecycleEventV2::FakUnspentReleased {
                    released_quote_amount_pusd: ExactDecimal("1".to_owned()),
                    account_delta: delta(
                        2,
                        PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased {
                            platform_order_id: "order-noe-58".to_owned(),
                        },
                    ),
                },
            ),
            feedback(
                22,
                OrderLifecycleEventV2::VenueDustTerminal {
                    closed_base_quantity_shares: ExactDecimal("0.1".to_owned()),
                    released_quote_amount_pusd: ExactDecimal("1".to_owned()),
                    account_delta: None,
                },
            ),
            feedback(
                23,
                OrderLifecycleEventV2::MarketResolvedCanceled {
                    resolution_event_id: "resolution-noe-58".to_owned(),
                    canceled_base_quantity_shares: ExactDecimal("1".to_owned()),
                    released_quote_amount_pusd: ExactDecimal("1".to_owned()),
                    account_delta: Some(delta(
                        3,
                        PlatformAccountDeltaReasonV2::MarketResolutionEncumbranceReleased {
                            resolution_event_id: "resolution-noe-58".to_owned(),
                            platform_order_id: "order-noe-58".to_owned(),
                        },
                    )),
                },
            ),
            feedback(
                24,
                OrderLifecycleEventV2::CallerCanceled {
                    canceled_base_quantity_shares: ExactDecimal("1".to_owned()),
                    released_quote_amount_pusd: ExactDecimal("1".to_owned()),
                    account_delta: None,
                },
            ),
            feedback(
                25,
                OrderLifecycleEventV2::GtdExpired {
                    encoded_expires_at_utc_ms: 1_000,
                    expired_base_quantity_shares: ExactDecimal("1".to_owned()),
                    released_quote_amount_pusd: ExactDecimal("1".to_owned()),
                    account_delta: Some(delta(
                        4,
                        PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
                            platform_order_id: "order-noe-58".to_owned(),
                            encoded_expires_at_utc_ms: 1_000,
                        },
                    )),
                },
            ),
        ]
    }

    fn trade_lifecycle() -> Vec<TradeLifecycleFeedbackV2> {
        vec![TradeLifecycleFeedbackV2 {
            metadata: lifecycle_metadata(30),
            trade_id: "trade-noe-58".to_owned(),
            platform_order_id: "order-noe-58".to_owned(),
            condition_id: ConditionId("condition-noe-58".to_owned()),
            token_id: TokenId("token-up-noe-58".to_owned()),
            status: SimulatedTradeStatusV2 {
                raw: "CONFIRMED".to_owned(),
                known: Some(SimulatedTradeKnownStatusV2::Confirmed),
            },
            economic_disposition: TradeEconomicDispositionV2::FinalSuccess,
            liquidity_role: TradeLiquidityRoleV2::Taker,
            source_price_pusd_per_share: ExactDecimal("0.5".to_owned()),
            base_quantity_shares: ExactDecimal("1".to_owned()),
            quote_amount_pusd: ExactDecimal("0.5".to_owned()),
            immediate_taker_fee_pusd: ExactDecimal("0".to_owned()),
            economic_application_account_transition_seq: Some(5),
            account_delta: Some(delta(
                5,
                PlatformAccountDeltaReasonV2::FillFinalityEconomicApplied {
                    trade_id: "trade-noe-58".to_owned(),
                    path_ordinal: 0,
                },
            )),
        }]
    }

    fn settlement_lifecycle() -> Vec<SettlementLifecycleFeedbackV2> {
        let feedback = |sequence, event| SettlementLifecycleFeedbackV2 {
            metadata: lifecycle_metadata(sequence),
            event,
        };
        vec![
            feedback(
                40,
                SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                    condition_id: ConditionId("condition-noe-58".to_owned()),
                    status: MakerRebateAccrualStatusV2::Calculated,
                    contributions: Vec::new(),
                    accrual_pusd: ExactDecimal("0".to_owned()),
                    account_delta: Some(delta(
                        6,
                        PlatformAccountDeltaReasonV2::MakerRebateAccrualFinalized,
                    )),
                },
            ),
            feedback(
                41,
                SettlementLifecycleEventV2::TakerRebateAccrualApplied {
                    trade_id: "trade-noe-58".to_owned(),
                    condition_id: ConditionId("condition-noe-58".to_owned()),
                    matched_at_utc_ms: 100,
                    attribution_day_start_utc_ms: 0,
                    actual_taker_fee_pusd: ExactDecimal("0".to_owned()),
                    rebate_rate: ExactDecimal("0".to_owned()),
                    accrual_pusd: ExactDecimal("0".to_owned()),
                    account_delta: delta(
                        7,
                        PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied {
                            trade_id: "trade-noe-58".to_owned(),
                        },
                    ),
                },
            ),
            feedback(
                42,
                SettlementLifecycleEventV2::MakerRebateDayFinalized {
                    contribution_trade_ids: Vec::new(),
                    attribution_day_start_utc_ms: 0,
                    accrual_pusd: ExactDecimal("0".to_owned()),
                    payable_pusd: ExactDecimal("0".to_owned()),
                    locked_remainder_pusd: ExactDecimal("0".to_owned()),
                    account_delta: None,
                },
            ),
            feedback(
                43,
                SettlementLifecycleEventV2::TakerRebateDayFinalized {
                    contribution_trade_ids: Vec::new(),
                    attribution_day_start_utc_ms: 0,
                    accrual_pusd: ExactDecimal("0".to_owned()),
                    payable_pusd: ExactDecimal("0".to_owned()),
                    locked_remainder_pusd: ExactDecimal("0".to_owned()),
                    account_delta: Some(delta(
                        8,
                        PlatformAccountDeltaReasonV2::TakerRebateDayFinalized {
                            attribution_day_utc_ms: 0,
                        },
                    )),
                },
            ),
            feedback(
                44,
                SettlementLifecycleEventV2::MakerRebatePaid {
                    attribution_day_start_utc_ms: 0,
                    paid_pusd: ExactDecimal("0".to_owned()),
                    account_delta: delta(
                        9,
                        PlatformAccountDeltaReasonV2::MakerRebatePaid {
                            attribution_day_utc_ms: 0,
                        },
                    ),
                },
            ),
            feedback(
                45,
                SettlementLifecycleEventV2::TakerRebatePaid {
                    attribution_day_start_utc_ms: 0,
                    paid_pusd: ExactDecimal("0".to_owned()),
                    account_delta: delta(
                        10,
                        PlatformAccountDeltaReasonV2::TakerRebatePaid {
                            attribution_day_utc_ms: 0,
                        },
                    ),
                },
            ),
            feedback(
                46,
                SettlementLifecycleEventV2::AutoRedeemApplied {
                    condition_id: ConditionId("condition-noe-58".to_owned()),
                    resolution_event_id: "resolution-noe-58".to_owned(),
                    resolution_source_event_at_utc_ms: 100,
                    resolution: MarketResolutionTerminalV3::Resolved {
                        winning_asset_id: "token-up-noe-58".to_owned(),
                        winning_outcome: "Up".to_owned(),
                        payout_vector: Vec::new(),
                        mapping_rule_version: "rule-noe-58".to_owned(),
                    },
                    redeemed_outcome_balances: Vec::new(),
                    redeemed_pusd: ExactDecimal("0".to_owned()),
                    account_delta: None,
                },
            ),
        ]
    }

    fn representative_batch(account_id: &str) -> Arc<BusinessProjectionBatchV2> {
        Arc::new(BusinessProjectionBatchV2 {
            initial_markets: Vec::new(),
            run_id: "run-noe-58".to_owned(),
            projection_batch_sequence: 1,
            account_opening: Some(opening(account_id)),
            market_enrollment: Some(market()),
            direct_responses: direct_responses(),
            order_lifecycle: order_lifecycle(),
            trade_lifecycle: trade_lifecycle(),
            settlement_lifecycle: settlement_lifecycle(),
        })
    }

    fn empty_batch(sequence: u64, with_opening: bool) -> Arc<BusinessProjectionBatchV2> {
        Arc::new(BusinessProjectionBatchV2 {
            initial_markets: Vec::new(),
            run_id: "run-noe-58".to_owned(),
            projection_batch_sequence: sequence,
            account_opening: with_opening.then(|| opening("account-noe-58")),
            market_enrollment: None,
            direct_responses: Vec::new(),
            order_lifecycle: Vec::new(),
            trade_lifecycle: Vec::new(),
            settlement_lifecycle: Vec::new(),
        })
    }

    fn tree_bytes(path: &Path) -> u64 {
        fs::read_dir(path)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| {
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    tree_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }

    fn peak_resident_memory_bytes() -> u64 {
        fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap()
            * 1024
    }

    #[test]
    fn representative_public_v2_batch_maps_all_current_families_once() {
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        let observation = port.observation();
        assert!(observation.result_references().is_empty());
        assert!(port.try_accept(representative_batch("account-noe-58")));
        assert_eq!(port.durable_through("run-noe-58"), 1);
        assert!(port.try_accept(representative_batch("account-noe-58")));
        assert_eq!(port.durable_through("run-noe-58"), 1);

        let references = port.result_references();
        assert_eq!(observation.result_references(), references);
        let records: Vec<_> = references
            .iter()
            .map(|reference| read_noe_result_v1(reference).unwrap().record)
            .collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record,
                    NormalizationRecordV1::Evidence {
                        evidence: SourceNeutralEvidenceKindV1::Control { .. },
                        ..
                    }
                ))
                .count(),
            10
        );
        let market_cancel_methods = records
            .iter()
            .filter_map(|record| match record {
                NormalizationRecordV1::Evidence {
                    source_reference:
                        SimV2SourceReferenceV1 {
                            record:
                                SimV2SourceRecordReferenceV1::DirectResponse {
                                    association:
                                        SimV2DirectResponseAssociationV1::CancellationTarget {
                                            global_request_id,
                                            method,
                                            ..
                                        },
                                    ..
                                },
                            ..
                        },
                    ..
                } if global_request_id == "cancel-market" => Some(*method),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            market_cancel_methods,
            vec![
                ExecutionMethodV2::CancelMarketOrders,
                ExecutionMethodV2::CancelMarketOrders,
            ],
            "市场撤单的混合逐目标结果必须机械保留同一方法身份"
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record,
                    NormalizationRecordV1::Evidence {
                        evidence: SourceNeutralEvidenceKindV1::OrderLifecycle { .. },
                        ..
                    }
                ))
                .count(),
            6
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record,
                    NormalizationRecordV1::Evidence {
                        evidence: SourceNeutralEvidenceKindV1::SettlementLifecycle { .. },
                        ..
                    }
                ))
                .count(),
            7
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, NormalizationRecordV1::EconomicFact { .. }))
                .count(),
            11
        );
        let manifest = port.run_consumption_manifest().unwrap();
        assert_eq!(manifest.commit_containers.len(), 1);
        assert_eq!(manifest.source_watermark.projection_batch_sequence, 1);
    }

    #[cfg(feature = "gate-b-flow")]
    #[test]
    fn real_noe_order_rejects_a_missing_predecessor_then_applies_the_original_prefix() {
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        assert!(port.try_accept(representative_batch("account-noe-58")));
        let references = port.observation().result_references();
        let transition_indices = references
            .iter()
            .enumerate()
            .filter_map(|(index, reference)| {
                matches!(
                        read_noe_result_v1(reference).unwrap().record,
                        NormalizationRecordV1::EconomicFact {
                            fact: contracts::execution_normalization::SourceNeutralEconomicFactV1 {
                                disposition:
                                    SourceNeutralEconomicDispositionV1::AccountTransition { .. },
                                ..
                            },
                            ..
                        }
                    )
                    .then_some(index)
            })
            .take(2)
            .collect::<Vec<_>>();
        assert_eq!(transition_indices.len(), 2);
        let first = transition_indices[0];
        let second = transition_indices[1];
        let policy = AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".to_owned()),
        };
        let mut recovered = LedgerWriterV1::new(policy.clone());
        for reference in &references[..first] {
            match recovered.apply_noe_result(reference) {
                Ok(_) | Err(LedgerErrorV1::NonEconomicRecord) => {}
                other => panic!("原序前缀必须可应用：{other:?}"),
            }
        }
        let before = recovered.projection().unwrap();
        assert_eq!(
            recovered.apply_noe_result(&references[second]).unwrap(),
            LedgerApplyOutcomeV1::MissingPlatformPredecessor {
                expected_account_transition_seq: 1,
                found_account_transition_seq: 2,
            }
        );
        assert_eq!(recovered.projection().unwrap(), before);
        for reference in &references[first..] {
            match recovered.apply_noe_result(reference) {
                Ok(_) | Err(LedgerErrorV1::NonEconomicRecord) => {}
                other => panic!("恢复后的原序后缀必须可应用：{other:?}"),
            }
        }

        let mut original = LedgerWriterV1::new(policy);
        for reference in &references {
            match original.apply_noe_result(reference) {
                Ok(_) | Err(LedgerErrorV1::NonEconomicRecord) => {}
                other => panic!("全新 Ledger 必须接受 NOE 原序：{other:?}"),
            }
        }
        assert_eq!(recovered.projection(), original.projection());
    }

    #[test]
    fn false_never_advances_and_reopen_adjudicates_present_or_absent_suffixes() {
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        assert!(!port.try_accept(empty_batch(1, false)));
        assert_eq!(port.durable_through("run-noe-58"), 0);
        assert!(port.try_accept(empty_batch(1, true)));
        assert_eq!(port.durable_through("run-noe-58"), 1);
        assert!(!port.try_accept(empty_batch(3, false)));
        assert_eq!(port.durable_through("run-noe-58"), 1);
        assert!(port.reopen_after_uncertain(1));
        assert!(!port.reopen_after_uncertain(2));
        assert_eq!(port.durable_through("run-noe-58"), 1);
        assert!(port.try_accept(empty_batch(2, false)));

        let mut reopened = new_port(temporary.path());
        assert!(reopened.try_accept(empty_batch(1, true)));
        assert_eq!(reopened.durable_through("run-noe-58"), 2);
        assert_eq!(
            reopened.result_references().len(),
            port.result_references().len()
        );
    }

    #[test]
    fn wrong_run_or_account_cannot_change_the_healthy_instance() {
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        assert!(port.try_accept(empty_batch(1, true)));
        let wrong_account = Arc::new(BusinessProjectionBatchV2 {
            initial_markets: Vec::new(),
            run_id: "run-noe-58".to_owned(),
            projection_batch_sequence: 2,
            account_opening: Some(opening("other-account")),
            market_enrollment: None,
            direct_responses: Vec::new(),
            order_lifecycle: Vec::new(),
            trade_lifecycle: Vec::new(),
            settlement_lifecycle: Vec::new(),
        });
        assert!(!port.try_accept(wrong_account));
        assert_eq!(port.durable_through("run-noe-58"), 1);
        assert_eq!(port.durable_through("other-run"), 0);

        let wrong_run = BusinessProjectionBatchV2 {
            initial_markets: Vec::new(),
            run_id: "other-run".to_owned(),
            projection_batch_sequence: 2,
            account_opening: None,
            market_enrollment: None,
            direct_responses: Vec::new(),
            order_lifecycle: Vec::new(),
            trade_lifecycle: Vec::new(),
            settlement_lifecycle: Vec::new(),
        };
        assert!(!port.try_accept(Arc::new(wrong_run)));
        assert_eq!(port.durable_through("run-noe-58"), 1);
    }

    #[test]
    fn distinct_markets_keep_distinct_source_neutral_results() {
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        assert!(port.try_accept(empty_batch(1, true)));

        let mut first_market = market();
        first_market.enrollment_sequence = 2;
        let mut second_market = market();
        second_market.enrollment_request_id = "enroll-noe-58-second".to_owned();
        second_market.enrollment_sequence = 3;
        second_market.market.condition_id = ConditionId("condition-noe-58-second".to_owned());
        second_market.market.clob_market_id = ClobMarketId("clob-noe-58-second".to_owned());
        second_market.assets[0].token_id = TokenId("token-up-noe-58-second".to_owned());
        second_market.assets[1].token_id = TokenId("token-down-noe-58-second".to_owned());

        for (sequence, enrollment) in [(2, first_market), (3, second_market)] {
            assert!(port.try_accept(Arc::new(BusinessProjectionBatchV2 {
                initial_markets: Vec::new(),
                run_id: "run-noe-58".to_owned(),
                projection_batch_sequence: sequence,
                account_opening: None,
                market_enrollment: Some(enrollment),
                direct_responses: Vec::new(),
                order_lifecycle: Vec::new(),
                trade_lifecycle: Vec::new(),
                settlement_lifecycle: Vec::new(),
            })));
        }

        let markets: Vec<_> = port
            .result_references()
            .iter()
            .filter_map(
                |reference| match read_noe_result_v1(reference).unwrap().record {
                    NormalizationRecordV1::Evidence {
                        evidence: SourceNeutralEvidenceKindV1::MarketEnrollment { result },
                        ..
                    } => Some(result),
                    _ => None,
                },
            )
            .collect();
        assert_eq!(markets.len(), 2);
        assert_ne!(
            markets[0].market.condition_id,
            markets[1].market.condition_id
        );
        assert_ne!(markets[0].assets, markets[1].assets);
    }

    #[test]
    fn long_trajectory_reports_observations_without_a_numeric_hard_gate() {
        const BATCHES: u64 = 512;
        let temporary = TempDir::new().unwrap();
        let mut port = new_port(temporary.path());
        let started = Instant::now();
        assert!(port.try_accept(empty_batch(1, true)));
        let mut halfway_bytes = 0;
        for sequence in 2..=BATCHES {
            assert!(port.try_accept(empty_batch(sequence, false)));
            if sequence == BATCHES / 2 {
                halfway_bytes = tree_bytes(temporary.path());
            }
        }
        let elapsed = started.elapsed();
        let durable_disk_bytes = tree_bytes(temporary.path());
        let reopen_started = Instant::now();
        let mut reopened = new_port(temporary.path());
        assert!(reopened.try_accept(empty_batch(1, true)));
        let reopen_elapsed = reopen_started.elapsed();
        assert_eq!(reopened.durable_through("run-noe-58"), BATCHES);
        let growth_bytes = durable_disk_bytes.saturating_sub(halfway_bytes);
        let growth_batches = BATCHES - BATCHES / 2;

        eprintln!(
            "NOE_STAGE2_RESOURCE_OBSERVATION {}",
            serde_json::json!({
                "trajectory_batches": BATCHES,
                "elapsed_milliseconds": elapsed.as_millis(),
                "throughput_batches_per_second": BATCHES as f64 / elapsed.as_secs_f64(),
                "peak_resident_memory_bytes": peak_resident_memory_bytes(),
                "peak_pending_projection_batches": 1,
                "durable_disk_bytes": durable_disk_bytes,
                "reopen_elapsed_microseconds": reopen_elapsed.as_micros(),
                "durable_bytes_growth_per_batch": growth_bytes as f64 / growth_batches as f64,
            })
        );
    }
}

fn normalized_balance_delta(delta: &PlatformAccountDeltaV2) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: delta.balance_delta.cash_pusd_delta.clone(),
        available_pusd_delta: delta.balance_delta.available_pusd_delta.clone(),
        principal_encumbrance_pusd_delta: delta
            .balance_delta
            .principal_encumbrance_pusd_delta
            .clone(),
        immediate_taker_fee_encumbrance_pusd_delta: delta
            .balance_delta
            .immediate_taker_fee_encumbrance_pusd_delta
            .clone(),
        outcome_balance_deltas: delta
            .balance_delta
            .outcome_balance_deltas
            .iter()
            .map(|balance| SourceNeutralOutcomeBalanceDeltaV1 {
                token_id: balance.token_id.clone(),
                balance_shares_delta: balance.balance_shares_delta.clone(),
            })
            .collect(),
        maker_rebate_receivable_pusd_delta: delta
            .balance_delta
            .maker_rebate_receivable_pusd_delta
            .clone(),
        taker_rebate_receivable_pusd_delta: delta
            .balance_delta
            .taker_rebate_receivable_pusd_delta
            .clone(),
        maker_rebate_locked_remainder_pusd_delta: delta
            .balance_delta
            .maker_rebate_locked_remainder_pusd_delta
            .clone(),
        taker_rebate_locked_remainder_pusd_delta: delta
            .balance_delta
            .taker_rebate_locked_remainder_pusd_delta
            .clone(),
    }
}
