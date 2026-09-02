use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, NoeInclusiveSequenceRangeV1, NoeResultReferenceV1,
    NoeRunConsumptionManifestV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SimV2SourceReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralCancellationOutcomeV1,
    SourceNeutralControlResultV1, SourceNeutralMakerRebateAccrualStatusV1,
    SourceNeutralMarketEnrollmentResultV1, SourceNeutralOrderLifecycleEventV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralOrderQueryResultV1,
    SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1, SourceNeutralOrdersPageV1,
    SourceNeutralOrdersQueryResultV1, SourceNeutralOutcomeBalanceDeltaV1,
    SourceNeutralQueryCursorV1, SourceNeutralQueryOutOfRetentionV1,
    SourceNeutralSettlementLifecycleResultV1, SourceNeutralSubmissionOutcomeV1,
    SourceNeutralTradeEconomicDispositionV1, SourceNeutralTradeLifecycleResultV1,
    SourceNeutralTradeStateV1, SourceNeutralTradesPageV1, SourceNeutralTradesQueryResultV1,
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
    DurableCommitOutcomeV1, DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1,
    SimV2ContextSeedV1, SourceNeutralEconomicEvidenceV1, SourceNeutralEntityKeyV1,
    SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1,
};

use super::{BusinessDirectResponseV2, BusinessProjectionBatchV2, BusinessProjectionPortV2};
use crate::runtime_v2::RunOpenedV2;

pub struct SimV2NoeProjectionPortV1 {
    root: PathBuf,
    opened: RunOpenedV2,
    run_opened_reference: DurableSourceReferenceV1,
    account_opening_reference: DurableSourceReferenceV1,
    account_opening_evidence_identity: String,
    implementation_identity: String,
    seed: Option<SimV2ContextSeedV1>,
    store: Option<DurableContextStoreV1>,
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

impl SimV2NoeProjectionPortV1 {
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

    fn open_from_first_batch(&mut self, batch: &BusinessProjectionBatchV2) -> bool {
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
        match DurableContextStoreV1::open_or_create_sim_v2(&self.root, seed.clone()) {
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
        let Ok(store) = DurableContextStoreV1::open_or_create_sim_v2(&self.root, seed) else {
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

impl BusinessProjectionPortV2 for SimV2NoeProjectionPortV1 {
    fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
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
        let input = match BatchMapperV1::new(store, &batch).map() {
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
            .and_then(DurableContextStoreV1::durable_through)
            .filter(|watermark| watermark.run_id == run_id)
            .map_or(0, |watermark| watermark.projection_batch_sequence)
    }
}

struct BatchMapperV1<'a> {
    store: &'a DurableContextStoreV1,
    batch: &'a BusinessProjectionBatchV2,
    watermark: SimV2SourceWatermarkV1,
    evidence: Vec<SourceNeutralEvidenceV1>,
    next_ordinal: u32,
}

impl<'a> BatchMapperV1<'a> {
    fn new(store: &'a DurableContextStoreV1, batch: &'a BusinessProjectionBatchV2) -> Self {
        Self {
            store,
            batch,
            watermark: SimV2SourceWatermarkV1 {
                run_id: batch.run_id.clone(),
                projection_batch_sequence: batch.projection_batch_sequence,
            },
            evidence: Vec::new(),
            next_ordinal: 0,
        }
    }

    fn map(mut self) -> Result<NormalizationBatchInputV1, ()> {
        if self.batch.run_id != self.store.context().account_identity.run_scope {
            return Err(());
        }
        if let Some(opening) = &self.batch.account_opening {
            if opening.account_instance_key != self.store.context().source.account_instance_key {
                return Err(());
            }
            self.push(
                SimV2SourceRecordReferenceV1::AccountOpening,
                SourceNeutralLogicalKeyV1::AccountOpening {
                    opening_evidence_identity: self
                        .store
                        .context()
                        .source
                        .account_opening_evidence_identity
                        .clone(),
                },
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                        opening_evidence_identity: self
                            .store
                            .context()
                            .source
                            .account_opening_evidence_identity
                            .clone(),
                        available_pusd: opening.initial_account_state.available_pusd.clone(),
                    },
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
                SourceNeutralEvidenceKindV1::MarketEnrollment {
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
        Ok(NormalizationBatchInputV1 {
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
                    SourceNeutralControlResultV1::OrderQuery { result },
                );
                Ok(())
            }
            BusinessDirectResponseV2::Orders(response) => {
                let result = self.map_orders_query(&response.result)?;
                self.push_direct_request(
                    &response.metadata,
                    SourceNeutralControlResultV1::OrdersQuery { result },
                );
                Ok(())
            }
            BusinessDirectResponseV2::Trades(response) => {
                let result = self.map_trades_query(&response.result)?;
                self.push_direct_request(
                    &response.metadata,
                    SourceNeutralControlResultV1::TradesQuery { result },
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
            SubmissionItemOutcomeV2::Accepted(accepted) => {
                SourceNeutralSubmissionOutcomeV1::Accepted {
                    order: SourceNeutralOrderSnapshotV1 {
                        order_identity: self.entity_order(&accepted.platform_order_id)?,
                        condition_id: item.condition_id.clone(),
                        token_id: item.token_id.clone(),
                        state: map_order_state(&accepted.platform_order_status),
                    },
                }
            }
            SubmissionItemOutcomeV2::RejectedNoEffect(_) => {
                SourceNeutralSubmissionOutcomeV1::RejectedNoEffect
            }
            SubmissionItemOutcomeV2::EvidenceUnknownNoEffect(_) => {
                SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect
            }
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
            SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Submission { outcome },
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
            SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Cancellation {
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
        result: SourceNeutralControlResultV1,
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
            SourceNeutralEvidenceKindV1::Control { result },
        );
    }

    fn map_order_query(
        &self,
        result: &OrderQueryResultV2,
    ) -> Result<SourceNeutralOrderQueryResultV1, ()> {
        match result {
            OrderQueryResultV2::Found(order) => Ok(SourceNeutralOrderQueryResultV1::Found {
                order: self.map_order(order)?,
            }),
            OrderQueryResultV2::NotFound => Ok(SourceNeutralOrderQueryResultV1::NotFound),
            OrderQueryResultV2::OutOfRetention(range) => Ok(
                SourceNeutralOrderQueryResultV1::OutOfRetention(map_out_of_retention(range)),
            ),
        }
    }

    fn map_orders_query(
        &self,
        result: &OrdersQueryResultV2,
    ) -> Result<SourceNeutralOrdersQueryResultV1, ()> {
        match result {
            OrdersQueryResultV2::Page(page) => {
                let items = page
                    .items
                    .iter()
                    .map(|order| self.map_order(order))
                    .collect::<Result<_, _>>()?;
                Ok(SourceNeutralOrdersQueryResultV1::Page(
                    SourceNeutralOrdersPageV1 {
                        root_sequence: page.root_sequence,
                        items,
                        next_cursor: page.next_cursor.as_ref().map(map_cursor),
                    },
                ))
            }
            OrdersQueryResultV2::InvalidCursor => {
                Ok(SourceNeutralOrdersQueryResultV1::InvalidCursor)
            }
            OrdersQueryResultV2::OutOfRetention(range) => Ok(
                SourceNeutralOrdersQueryResultV1::OutOfRetention(map_out_of_retention(range)),
            ),
        }
    }

    fn map_trades_query(
        &self,
        result: &TradesQueryResultV2,
    ) -> Result<SourceNeutralTradesQueryResultV1, ()> {
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

    fn map_order(&self, order: &OrderProjectionV2) -> Result<SourceNeutralOrderSnapshotV1, ()> {
        Ok(SourceNeutralOrderSnapshotV1 {
            order_identity: self.entity_order(&order.platform_order_id)?,
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            state: map_order_state(&order.status),
        })
    }

    fn map_trade(
        &self,
        trade: &TradeProjectionV2,
    ) -> Result<SourceNeutralTradeLifecycleResultV1, ()> {
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
            SourceNeutralEvidenceKindV1::OrderLifecycle {
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

    fn map_trade_lifecycle(
        &mut self,
        feedback: &contracts::TradeLifecycleFeedbackV2,
    ) -> Result<(), ()> {
        self.check_account(&feedback.metadata.account_instance_key)?;
        let record = SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence: feedback.metadata.private_feedback_sequence,
            platform_event_at_utc_ms: feedback.metadata.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
        };
        self.push(
            record.clone(),
            SourceNeutralLogicalKeyV1::TradeLifecycle {
                trade_identity: feedback.trade_id.clone(),
                occurrence: feedback.metadata.private_feedback_sequence,
            },
            SourceNeutralEvidenceKindV1::TradeLifecycle {
                result: SourceNeutralTradeLifecycleResultV1 {
                    trade_identity: self.entity_trade(&feedback.trade_id)?,
                    order_identity: self.entity_order(&feedback.platform_order_id)?,
                    condition_id: feedback.condition_id.clone(),
                    token_id: feedback.token_id.clone(),
                    state: map_trade_state(&feedback.status),
                    economic_disposition: map_trade_disposition(feedback.economic_disposition),
                },
            },
        );
        if let Some(delta) = &feedback.account_delta {
            self.push_economic(record, delta)?;
        }
        Ok(())
    }

    fn map_settlement_lifecycle(
        &mut self,
        feedback: &contracts::SettlementLifecycleFeedbackV2,
    ) -> Result<(), ()> {
        self.check_account(&feedback.metadata.account_instance_key)?;
        let (result, account_delta) = match &feedback.event {
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
        self.push(
            record.clone(),
            SourceNeutralLogicalKeyV1::SettlementLifecycle {
                private_feedback_sequence: feedback.metadata.private_feedback_sequence,
            },
            SourceNeutralEvidenceKindV1::SettlementLifecycle { result },
        );
        if let Some(delta) = account_delta {
            self.push_economic(record, delta)?;
        }
        Ok(())
    }

    fn push_economic(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        delta: &PlatformAccountDeltaV2,
    ) -> Result<(), ()> {
        self.check_account(&delta.account_instance_key)?;
        let transition = match &delta.reason {
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
            SourceNeutralEvidenceKindV1::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                    account_transition_seq: delta.account_transition_seq,
                    economic_effective_at_utc_ms: delta.platform_effective_at_utc_ms,
                    condition_id: delta.condition_id.clone(),
                    transition,
                    balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV1 {
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
                    }),
                },
            },
        );
        Ok(())
    }

    fn check_account(&self, account: &AccountInstanceKeyV1) -> Result<(), ()> {
        (account == &self.store.context().source.account_instance_key)
            .then_some(())
            .ok_or(())
    }

    fn entity_order(
        &self,
        source_order_id: &str,
    ) -> Result<contracts::execution_normalization::StableLogicalIdentityV1, ()> {
        self.store
            .entity_identity(&SourceNeutralEntityKeyV1::Order {
                source_order_id: source_order_id.to_owned(),
            })
            .map_err(|_| ())
    }

    fn entity_trade(
        &self,
        source_trade_id: &str,
    ) -> Result<contracts::execution_normalization::StableLogicalIdentityV1, ()> {
        self.store
            .entity_identity(&SourceNeutralEntityKeyV1::Trade {
                source_trade_id: source_trade_id.to_owned(),
            })
            .map_err(|_| ())
    }

    fn entity_resolution(
        &self,
        source_resolution_id: &str,
    ) -> Result<contracts::execution_normalization::StableLogicalIdentityV1, ()> {
        self.store
            .entity_identity(&SourceNeutralEntityKeyV1::Resolution {
                source_resolution_id: source_resolution_id.to_owned(),
            })
            .map_err(|_| ())
    }

    fn push(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        logical_key: SourceNeutralLogicalKeyV1,
        kind: SourceNeutralEvidenceKindV1,
    ) {
        let source_ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.evidence.push(SourceNeutralEvidenceV1 {
            source_reference: SimV2SourceReferenceV1 {
                context_reference: self.store.context().context_reference.clone(),
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

#[cfg(test)]
mod tests {
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
            market_event_source_identity_sha256: sha('7'),
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
