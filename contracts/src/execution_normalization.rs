use serde::{Deserialize, Serialize};

use crate::{
    AccountInstanceKeyV1, AssetIdentityV1, CancelInputItemRefV2, ConditionId, ExactDecimal,
    ExecutionMethodV2, MarketIdentityV2, Sha256Hex, TokenId,
};

pub const EXECUTION_NORMALIZATION_CONTRACT_V1: &str = "execution_normalization_v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecutionNormalizationContextReferenceV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceNeutralAccountIdentityV1 {
    pub source_category: String,
    pub environment: String,
    pub run_scope: String,
    pub account_scope: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ImmutableSourceReferenceV1 {
    Identity(String),
    Sha256(Sha256Hex),
    Location(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DurableSourceReferenceV1 {
    pub source: ImmutableSourceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimV2ContextSourceV1 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub run_opened: DurableSourceReferenceV1,
    pub account_opening_evidence_identity: String,
    pub account_opening: DurableSourceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNormalizationContextV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub account_identity: SourceNeutralAccountIdentityV1,
    pub source: SimV2ContextSourceV1,
    pub contract_version: String,
    pub implementation_identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimV2SourceWatermarkV1 {
    pub run_id: String,
    pub projection_batch_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimV2DirectResponseAssociationV1 {
    Request {
        global_request_id: String,
    },
    SubmissionItem {
        global_request_id: String,
        batch_item_ordinal: u16,
    },
    CancellationTarget {
        global_request_id: String,
        method: ExecutionMethodV2,
        target_ordinal: u64,
        input_item_refs: Vec<CancelInputItemRefV2>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimV2SourceRecordReferenceV1 {
    AccountOpening,
    MarketEnrollment {
        enrollment_sequence: u64,
    },
    DirectResponse {
        private_feedback_sequence: u64,
        platform_responded_at_utc_ms: i64,
        response_received_at_utc_ms: i64,
        association: SimV2DirectResponseAssociationV1,
    },
    OrderLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
    TradeLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
    SettlementLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimV2SourceReferenceV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub watermark: SimV2SourceWatermarkV1,
    pub record: SimV2SourceRecordReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StableLogicalIdentityV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoeEconomicFactReferenceV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceNeutralEconomicIdentityV1 {
    AccountOpening {
        account_identity: SourceNeutralAccountIdentityV1,
        opening_evidence_identity: String,
    },
    AccountTransition {
        account_identity: SourceNeutralAccountIdentityV1,
        account_transition_seq: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOutcomeBalanceDeltaV1 {
    pub token_id: TokenId,
    pub balance_shares_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralAccountBalanceDeltaV1 {
    pub cash_pusd_delta: ExactDecimal,
    pub available_pusd_delta: ExactDecimal,
    pub principal_encumbrance_pusd_delta: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal,
    pub outcome_balance_deltas: Vec<SourceNeutralOutcomeBalanceDeltaV1>,
    pub maker_rebate_receivable_pusd_delta: ExactDecimal,
    pub taker_rebate_receivable_pusd_delta: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd_delta: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralAccountTransitionKindV1 {
    OrderAcceptedEncumbranceApplied {
        order_identity: StableLogicalIdentityV1,
    },
    FakUnspentEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    CallerCancellationEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    GtdExpirationEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
        encoded_expires_at_utc_ms: i64,
    },
    VenueDustEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    MarketResolutionEncumbranceReleased {
        resolution_identity: StableLogicalIdentityV1,
        order_identity: StableLogicalIdentityV1,
    },
    FillFinalityEconomicApplied {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    FillFinalityUnappliedFailureReleased {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    FillFinalityCompensationApplied {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    MakerRebateAccrualFinalized,
    TakerRebateAccrualApplied {
        trade_identity: StableLogicalIdentityV1,
    },
    MakerRebateDayFinalized {
        attribution_day_utc_ms: i64,
    },
    TakerRebateDayFinalized {
        attribution_day_utc_ms: i64,
    },
    MakerRebatePaid {
        attribution_day_utc_ms: i64,
    },
    TakerRebatePaid {
        attribution_day_utc_ms: i64,
    },
    AutoRedeemApplied {
        resolution_identity: StableLogicalIdentityV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralEconomicDispositionV1 {
    AccountOpening {
        available_pusd: ExactDecimal,
    },
    AccountTransition {
        economic_effective_at_utc_ms: i64,
        condition_id: Option<ConditionId>,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: Box<SourceNeutralAccountBalanceDeltaV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEconomicFactV1 {
    pub fact_reference: NoeEconomicFactReferenceV1,
    pub logical_identity: StableLogicalIdentityV1,
    pub economic_identity: SourceNeutralEconomicIdentityV1,
    pub source_reference: SimV2SourceReferenceV1,
    pub disposition: SourceNeutralEconomicDispositionV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeSourceCommitBoundaryV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub source_watermark: SimV2SourceWatermarkV1,
    pub durable_available_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoePublishedContainerReferenceV1 {
    pub location: String,
    pub boundary_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeResultReferenceV1 {
    pub commit_container: NoePublishedContainerReferenceV1,
    pub record_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralOrderStateV1 {
    Open,
    Matched,
    VenueDustTerminal,
    MarketResolvedCanceled,
    Canceled,
    Expired,
    Delayed,
    Unmatched,
    Unknown { raw: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOrderSnapshotV1 {
    pub order_identity: StableLogicalIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub state: SourceNeutralOrderStateV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralSubmissionOutcomeV1 {
    Accepted { order: SourceNeutralOrderSnapshotV1 },
    RejectedNoEffect,
    EvidenceUnknownNoEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralCancellationOutcomeV1 {
    Canceled,
    NotCanceledNoEffect,
    EvidenceUnknownNoEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralQueryCursorV1 {
    pub root_sequence: u64,
    pub next_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralQueryOutOfRetentionV1 {
    OrderHistory {
        retained_from_utc_ms: i64,
    },
    TradeHistory {
        retained_from_utc_ms: i64,
    },
    RootSnapshot {
        requested_root_sequence: u64,
        earliest_retained_root_sequence: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralOrderQueryResult<O> {
    Found { order: O },
    NotFound,
    OutOfRetention(SourceNeutralQueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOrdersPage<O> {
    pub root_sequence: u64,
    pub items: Vec<O>,
    pub next_cursor: Option<SourceNeutralQueryCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralOrdersQueryResult<O> {
    Page(SourceNeutralOrdersPage<O>),
    InvalidCursor,
    OutOfRetention(SourceNeutralQueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralTradeStateV1 {
    MatchedNotBroadcasted,
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
    Unknown { raw: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralTradeEconomicDispositionV1 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralTradeLifecycleResultV1 {
    pub trade_identity: StableLogicalIdentityV1,
    pub order_identity: StableLogicalIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub state: SourceNeutralTradeStateV1,
    pub economic_disposition: SourceNeutralTradeEconomicDispositionV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralTradeLifecycleResultV2 {
    pub trade_identity: StableLogicalIdentityV1,
    pub order_identity: StableLogicalIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub state: SourceNeutralTradeStateV1,
    pub economic_disposition: SourceNeutralTradeEconomicDispositionV1,
    pub fill_sequence: u64,
    pub matched_at_utc_ms: i64,
    pub liquidity_role: crate::TradeLiquidityRoleV2,
    pub source_price_pusd_per_share: ExactDecimal,
    pub base_quantity_shares: ExactDecimal,
    pub quote_amount_pusd: ExactDecimal,
    pub immediate_taker_fee_pusd: ExactDecimal,
    pub economic_application_account_transition_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralTradesPageV1 {
    pub root_sequence: u64,
    pub items: Vec<SourceNeutralTradeLifecycleResultV1>,
    pub next_cursor: Option<SourceNeutralQueryCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralTradesQueryResultV1 {
    Page(SourceNeutralTradesPageV1),
    InvalidCursor,
    OutOfRetention(SourceNeutralQueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralControlResult<S, O> {
    Submission {
        outcome: S,
    },
    Cancellation {
        order_identity: StableLogicalIdentityV1,
        outcome: SourceNeutralCancellationOutcomeV1,
    },
    OrderQuery {
        result: SourceNeutralOrderQueryResult<O>,
    },
    OrdersQuery {
        result: SourceNeutralOrdersQueryResult<O>,
    },
    TradesQuery {
        result: SourceNeutralTradesQueryResultV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralOrderLifecycleEventV1 {
    Accepted {
        state: SourceNeutralOrderStateV1,
    },
    FakUnspentReleased,
    VenueDustTerminal,
    MarketResolvedCanceled {
        resolution_identity: StableLogicalIdentityV1,
    },
    CallerCanceled,
    GtdExpired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOrderLifecycleResultV1 {
    pub order_identity: StableLogicalIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub event: SourceNeutralOrderLifecycleEventV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralMarketEnrollmentResultV1 {
    pub enrollment_request_id: String,
    pub market: MarketIdentityV2,
    pub assets: [AssetIdentityV1; 2],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralMakerRebateAccrualStatusV1 {
    Calculated,
    ZeroNoEligibleMakerFill,
    UnknownIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralSettlementLifecycleResultV1 {
    MakerRebateAccrualFinalized {
        condition_id: ConditionId,
        status: SourceNeutralMakerRebateAccrualStatusV1,
    },
    TakerRebateAccrualApplied {
        trade_identity: StableLogicalIdentityV1,
        condition_id: ConditionId,
        attribution_day_start_utc_ms: i64,
    },
    MakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
    },
    TakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
    },
    MakerRebatePaid {
        attribution_day_start_utc_ms: i64,
    },
    TakerRebatePaid {
        attribution_day_start_utc_ms: i64,
    },
    AutoRedeemApplied {
        condition_id: ConditionId,
        resolution_identity: StableLogicalIdentityV1,
        resolution_source_event_at_utc_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeInclusiveSequenceRangeV1 {
    pub first: u64,
    pub last: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeComponentCapabilityReceiptV1 {
    pub contract_identity: String,
    pub implementation_identity: String,
    pub test_report: NoePublishedContainerReferenceV1,
    pub resource_report: NoePublishedContainerReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeRunConsumptionManifestV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub input_projection_batch_range: NoeInclusiveSequenceRangeV1,
    pub output_commit_range: NoeInclusiveSequenceRangeV1,
    pub source_watermark: SimV2SourceWatermarkV1,
    pub context_container: NoePublishedContainerReferenceV1,
    pub commit_containers: Vec<NoePublishedContainerReferenceV1>,
}

// 两代控制结果只改变所属测量成员，其他分支及编码保持一致。
pub type SourceNeutralOrderQueryResultV1 =
    SourceNeutralOrderQueryResult<SourceNeutralOrderSnapshotV1>;
pub type SourceNeutralOrdersPageV1 = SourceNeutralOrdersPage<SourceNeutralOrderSnapshotV1>;
pub type SourceNeutralOrdersQueryResultV1 =
    SourceNeutralOrdersQueryResult<SourceNeutralOrderSnapshotV1>;
pub type SourceNeutralControlResultV1 =
    SourceNeutralControlResult<SourceNeutralSubmissionOutcomeV1, SourceNeutralOrderSnapshotV1>;
pub type SourceNeutralOrderQueryResultV2 =
    SourceNeutralOrderQueryResult<SourceNeutralOrderSnapshotV2>;
pub type SourceNeutralOrdersPageV2 = SourceNeutralOrdersPage<SourceNeutralOrderSnapshotV2>;
pub type SourceNeutralOrdersQueryResultV2 =
    SourceNeutralOrdersQueryResult<SourceNeutralOrderSnapshotV2>;
pub type SourceNeutralControlResultV2 =
    SourceNeutralControlResult<SourceNeutralSubmissionOutcomeV2, SourceNeutralOrderSnapshotV2>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralSubmissionMeasurementV1 {
    Base {
        matched_base_quantity_shares: ExactDecimal,
        open_base_quantity_shares: ExactDecimal,
    },
    QuoteBudget {
        actual_matched_base_quantity_shares: ExactDecimal,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralSubmissionOutcomeV2 {
    Accepted {
        order: SourceNeutralOrderSnapshotV1,
        measurement: SourceNeutralSubmissionMeasurementV1,
        effective_processing_at_utc_ms: i64,
    },
    RejectedNoEffect,
    EvidenceUnknownNoEffect,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralOrderQuantityEvidenceV1 {
    Base {
        matched_base_quantity_shares: ExactDecimal,
        active_unfilled_base_quantity_shares: ExactDecimal,
    },
    QuoteBudget {
        actual_matched_base_quantity_shares: ExactDecimal,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOrderSnapshotV2 {
    pub order_identity: StableLogicalIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub state: SourceNeutralOrderStateV1,
    pub quantities: SourceNeutralOrderQuantityEvidenceV1,
}
