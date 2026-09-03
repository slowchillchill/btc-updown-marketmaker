use serde::{Deserialize, Serialize};

pub mod execution_normalization;
mod model_intent;

pub use model_intent::{BuyLegIntentV1, BuyOrderIntentV1, ModelIntentV1};

pub const EXECUTION_MARKET_EVENT_SCHEMA_V3: &str = "execution_market_event_v3";
pub const CANONICAL_REFERENCE_EVENT_SCHEMA_V1: &str = "canonical_reference_event_v1";
pub const CANONICAL_EVENT_RECORD_SCHEMA_V1: &str = "canonical_event_record_v1";
pub const MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3: &str = "market_observation_causal_shell_v3";
pub const GATE_A_CAPABILITY_MATRIX_SCHEMA_V2: &str = "gate_a_capability_matrix_v2";
pub const GATE_A_RECEIPT_SCHEMA_V3: &str = "real_execution_tape_receipt_v3";
pub const EVENT_TIME_INDEX_SCHEMA_V2: &str = "event_time_index_v2";
pub const VISIBILITY_INDEX_SCHEMA_V2: &str = "visibility_index_v2";
pub const ACCOUNT_INSTANCE_KEY_SCHEMA_V1: &str = "account_instance_key_v1";
pub const ACCOUNT_OPENING_ANCHOR_SCHEMA_V1: &str = "account_opening_anchor_v1";
pub const PLATFORM_ACCOUNT_DELTA_SCHEMA_V1: &str = "platform_account_delta_v1";
pub const MARKET_ENROLLMENT_SCHEMA_V1: &str = "market_enrollment_v1";
pub const SIM_EXECUTION_CONTRACT_V2: &str = "sim_execution_contract_v2";
pub const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
pub const DATASET_DATE_FLOOR: &str = "2026-07-30";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimCallerRoleV1 {
    ModelTrain,
    OfflineEvaluation,
    Paper,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PlatformSourceV1 {
    Simulated,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountInstanceKeyV1 {
    pub run_id: String,
    pub caller_role: SimCallerRoleV1,
    pub caller_instance_id: String,
    pub account_id: String,
    pub platform_source: PlatformSourceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialAccountStateV2 {
    pub available_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountOpeningFactV2 {
    pub creation_request_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub session_id: String,
    pub initial_platform_scenario_file_sha256: Sha256Hex,
    pub initial_account_state: InitialAccountStateV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformOutcomeBalanceV1 {
    pub token_id: TokenId,
    pub balance_shares: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformAccountSnapshotV1 {
    pub cash_pusd: ExactDecimal,
    pub available_pusd: ExactDecimal,
    pub principal_encumbrance_pusd: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd: ExactDecimal,
    pub outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    pub maker_rebate_receivable_pusd: ExactDecimal,
    pub taker_rebate_receivable_pusd: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountOpeningAnchorV1 {
    pub schema_version: String,
    pub anchor_id: String,
    pub anchor_sha256: Sha256Hex,
    pub account_instance_key: AccountInstanceKeyV1,
    pub session_id: String,
    pub initial_scenario_sha256: Sha256Hex,
    pub initial_market_set_sha256: Sha256Hex,
    pub initial_account_state_sha256: Sha256Hex,
    pub initial_platform_account: PlatformAccountSnapshotV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "predecessor_type",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum PlatformAccountDeltaPredecessorV1 {
    OpeningAnchor {
        anchor_id: String,
        anchor_sha256: Sha256Hex,
    },
    AccountDelta {
        delta_id: String,
        delta_sha256: Sha256Hex,
        account_transition_seq: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PlatformAccountDeltaReasonV1 {
    OrderAcceptedEncumbranceApplied,
    FakUnspentEncumbranceReleased,
    CallerCancellationEncumbranceReleased,
    GtdExpirationEncumbranceReleased,
    VenueDustEncumbranceReleased,
    MarketResolutionEncumbranceReleased,
    FillFinalityEconomicApplied,
    FillFinalityUnappliedFailureReleased,
    FillFinalityCompensationApplied,
    MakerRebateAccrualFinalized,
    TakerRebateAccrualApplied,
    MakerRebateDayFinalized,
    TakerRebateDayFinalized,
    MakerRebatePaid,
    TakerRebatePaid,
    AutoRedeemApplied,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformOutcomeBalanceDeltaV1 {
    pub token_id: TokenId,
    pub balance_shares_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformAccountBalanceDeltaV1 {
    pub cash_pusd_delta: ExactDecimal,
    pub available_pusd_delta: ExactDecimal,
    pub principal_encumbrance_pusd_delta: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal,
    pub outcome_balance_deltas: Vec<PlatformOutcomeBalanceDeltaV1>,
    pub maker_rebate_receivable_pusd_delta: ExactDecimal,
    pub taker_rebate_receivable_pusd_delta: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd_delta: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformAccountDeltaV1 {
    pub schema_version: String,
    pub delta_id: String,
    pub delta_sha256: Sha256Hex,
    pub account_instance_key: AccountInstanceKeyV1,
    pub account_transition_seq: u64,
    pub predecessor: PlatformAccountDeltaPredecessorV1,
    pub reason: PlatformAccountDeltaReasonV1,
    pub reason_identity: String,
    pub condition_id: Option<ConditionId>,
    pub platform_effective_at_utc_ms: i64,
    pub balance_delta: PlatformAccountBalanceDeltaV1,
    pub account_summary_before_sha256: Sha256Hex,
    pub account_summary_after_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PlatformAccountDeltaReasonV2 {
    OrderAcceptedEncumbranceApplied {
        platform_order_id: String,
    },
    FakUnspentEncumbranceReleased {
        platform_order_id: String,
    },
    CallerCancellationEncumbranceReleased {
        platform_order_id: String,
    },
    GtdExpirationEncumbranceReleased {
        platform_order_id: String,
        encoded_expires_at_utc_ms: i64,
    },
    VenueDustEncumbranceReleased {
        platform_order_id: String,
    },
    MarketResolutionEncumbranceReleased {
        resolution_event_id: String,
        platform_order_id: String,
    },
    FillFinalityEconomicApplied {
        trade_id: String,
        path_ordinal: u32,
    },
    FillFinalityUnappliedFailureReleased {
        trade_id: String,
        path_ordinal: u32,
    },
    FillFinalityCompensationApplied {
        trade_id: String,
        path_ordinal: u32,
    },
    MakerRebateAccrualFinalized,
    TakerRebateAccrualApplied {
        trade_id: String,
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
        resolution_event_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformAccountDeltaV2 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub account_transition_seq: u64,
    pub reason: PlatformAccountDeltaReasonV2,
    pub condition_id: Option<ConditionId>,
    pub platform_effective_at_utc_ms: i64,
    pub balance_delta: PlatformAccountBalanceDeltaV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEnrollmentIdentityV1 {
    pub enrollment_id: String,
    pub enrollment_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEnrollmentV1 {
    pub schema_version: String,
    pub enrollment_id: String,
    pub enrollment_sha256: Sha256Hex,
    pub enrollment_request_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor_id: String,
    pub opening_anchor_sha256: Sha256Hex,
    pub previous_enrollment: Option<MarketEnrollmentIdentityV1>,
    pub market: MarketIdentityV2,
    pub assets: [AssetIdentityV1; 2],
    pub enrollment_chain_length: u64,
    pub enrollment_chain_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEnrollmentV2 {
    pub enrollment_request_id: String,
    pub enrollment_sequence: u64,
    pub market: MarketIdentityV2,
    pub assets: [AssetIdentityV1; 2],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCursorV2 {
    pub root_sequence: u64,
    pub next_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulatedOrderKnownStatusV2 {
    Live,
    Matched,
    VenueDustTerminal,
    CanceledMarketResolved,
    Canceled,
    Expired,
    Delayed,
    Unmatched,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedOrderStatusV2 {
    pub raw: String,
    pub known: Option<SimulatedOrderKnownStatusV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulatedTradeKnownStatusV2 {
    MatchedNotBroadcasted,
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedTradeStatusV2 {
    pub raw: String,
    pub known: Option<SimulatedTradeKnownStatusV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKindProjectionV2 {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimOrderTerminalReasonV2 {
    FakCompleted,
    VenueDustTerminal,
    MarketResolvedCanceled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "measurement", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderQuantityProjectionV2 {
    Base {
        accepted_base_quantity_shares: ExactDecimal,
        matched_base_quantity_shares: ExactDecimal,
        active_unfilled_base_quantity_shares: ExactDecimal,
        venue_dust_closed_base_quantity_shares: ExactDecimal,
        market_resolved_canceled_base_quantity_shares: ExactDecimal,
        canceled_base_quantity_shares: ExactDecimal,
        expired_base_quantity_shares: ExactDecimal,
        accepted_quote_amount_pusd: ExactDecimal,
        matched_quote_amount_pusd: ExactDecimal,
        open_encumbered_quote_amount_pusd: ExactDecimal,
        venue_dust_released_quote_amount_pusd: ExactDecimal,
        market_resolved_released_quote_amount_pusd: ExactDecimal,
        released_price_improvement_quote_amount_pusd: ExactDecimal,
        canceled_released_quote_amount_pusd: ExactDecimal,
        expired_released_quote_amount_pusd: ExactDecimal,
    },
    QuoteBudget {
        accepted_quote_budget_pusd: ExactDecimal,
        matched_quote_amount_pusd: ExactDecimal,
        released_or_unspent_quote_amount_pusd: ExactDecimal,
        actual_matched_base_quantity_shares: ExactDecimal,
        sdk_taker_amount_audit_base_quantity_shares: ExactDecimal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderProjectionV2 {
    pub platform_order_id: String,
    pub global_request_id: String,
    pub item_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub created_at_utc_ms: i64,
    pub kind: OrderKindProjectionV2,
    pub expires_at_utc_ms: Option<i64>,
    pub source_price_pusd_per_share: ExactDecimal,
    pub status: SimulatedOrderStatusV2,
    pub terminal_reason: Option<SimOrderTerminalReasonV2>,
    pub quantities: OrderQuantityProjectionV2,
    pub immediate_taker_fee_encumbered_pusd: ExactDecimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeLiquidityRoleV2 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeEconomicDispositionV2 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeProjectionV2 {
    pub trade_id: String,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub matched_at_utc_ms: i64,
    pub status: SimulatedTradeStatusV2,
    pub liquidity_role: TradeLiquidityRoleV2,
    pub source_price_pusd_per_share: ExactDecimal,
    pub base_quantity_shares: ExactDecimal,
    pub quote_amount_pusd: ExactDecimal,
    pub immediate_taker_fee_pusd: ExactDecimal,
    pub economic_disposition: TradeEconomicDispositionV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryOutOfRetentionV2 {
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
pub enum OrderQueryResultV2 {
    Found(Box<OrderProjectionV2>),
    NotFound,
    OutOfRetention(QueryOutOfRetentionV2),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersPageV2 {
    pub root_sequence: u64,
    pub items: Vec<OrderProjectionV2>,
    pub next_cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesPageV2 {
    pub root_sequence: u64,
    pub items: Vec<TradeProjectionV2>,
    pub next_cursor: Option<QueryCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrdersQueryResultV2 {
    Page(OrdersPageV2),
    InvalidCursor,
    OutOfRetention(QueryOutOfRetentionV2),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradesQueryResultV2 {
    Page(TradesPageV2),
    InvalidCursor,
    OutOfRetention(QueryOutOfRetentionV2),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMethodV2 {
    PostOrder,
    PostOrders,
    CancelOrder,
    CancelOrders,
    CancelMarketOrders,
    CancelAllOrders,
    Order,
    Orders,
    Trades,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResponseMetadataV2 {
    pub global_request_id: String,
    pub caller_correlation_id: Option<String>,
    pub method: ExecutionMethodV2,
    pub platform_responded_at_utc_ms: i64,
    pub response_received_at_utc_ms: i64,
    pub private_feedback_sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionRejectedReasonV2 {
    EndAtReached,
    ExplicitTradingStateVeto,
    MarketNotInScenario,
    AssetNotInMarket,
    OrderEncodingRejected,
    BelowMinimumShares,
    PostOnlyWouldTake,
    BelowMarketableLimitBuyMinimumPrincipal,
    GtdExpirationTooSoon,
    InsufficientAvailablePusd,
    FokNotFullyFilled,
    FakNoMatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionEvidenceUnknownReasonV2 {
    Preloading,
    TargetAssetNotFresh(MarketDataQuality),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionFillV2 {
    pub trade_id: String,
    pub source_price_pusd_per_share: ExactDecimal,
    pub base_quantity_shares: ExactDecimal,
    pub quote_amount_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "measurement", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SubmissionMeasurementV2 {
    Base {
        accepted_base_quantity_shares: ExactDecimal,
        matched_base_quantity_shares: ExactDecimal,
        open_base_quantity_shares: ExactDecimal,
        venue_dust_closed_base_quantity_shares: ExactDecimal,
        order_type_terminated_base_quantity_shares: ExactDecimal,
        accepted_quote_amount_pusd: ExactDecimal,
        matched_quote_amount_pusd: ExactDecimal,
        open_encumbered_quote_amount_pusd: ExactDecimal,
        venue_dust_released_quote_amount_pusd: ExactDecimal,
        released_price_improvement_quote_amount_pusd: ExactDecimal,
        immediate_taker_fee_encumbered_pusd: ExactDecimal,
    },
    QuoteBudget {
        accepted_quote_budget_pusd: ExactDecimal,
        used_quote_amount_pusd: ExactDecimal,
        released_or_unspent_quote_amount_pusd: ExactDecimal,
        actual_matched_base_quantity_shares: ExactDecimal,
        sdk_taker_amount_audit_base_quantity_shares: ExactDecimal,
        immediate_taker_fee_encumbered_pusd: ExactDecimal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedSubmissionItemV2 {
    pub platform_order_id: String,
    pub platform_order_status: SimulatedOrderStatusV2,
    pub fills: Vec<SubmissionFillV2>,
    pub terminal_reason: Option<SimOrderTerminalReasonV2>,
    pub measurement: SubmissionMeasurementV2,
    pub account_transition_sequences: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionItemOutcomeV2 {
    Accepted(AcceptedSubmissionItemV2),
    RejectedNoEffect(SubmissionRejectedReasonV2),
    EvidenceUnknownNoEffect(SubmissionEvidenceUnknownReasonV2),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionResponseItemV2 {
    pub item_id: String,
    pub batch_item_ordinal: u16,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub effective_processing_at_utc_ms: i64,
    pub outcome: SubmissionItemOutcomeV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelInputItemRefV2 {
    pub item_id: String,
    pub batch_item_ordinal: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanceledTargetMeasurementV2 {
    pub canceled_base_quantity_shares: ExactDecimal,
    pub released_quote_amount_pusd: ExactDecimal,
    pub account_transition_seq: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelNotCanceledReasonV2 {
    NotFoundOrNotOwned,
    NoCancellableRemainder,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelTargetOutcomeV2 {
    Canceled(CanceledTargetMeasurementV2),
    NotCanceledNoEffect(CancelNotCanceledReasonV2),
    EvidenceUnknownNoEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTargetResponseV2 {
    pub target_ordinal: u64,
    pub platform_order_id: String,
    pub input_item_refs: Vec<CancelInputItemRefV2>,
    pub effective_processing_at_utc_ms: i64,
    pub outcome: CancelTargetOutcomeV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub item: SubmissionResponseItemV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrdersDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub items: Vec<SubmissionResponseItemV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub target: CancelTargetResponseV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrdersDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub targets: Vec<CancelTargetResponseV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelMarketOrdersDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub targets: Vec<CancelTargetResponseV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAllOrdersDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub targets: Vec<CancelTargetResponseV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub result: OrderQueryResultV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub result: OrdersQueryResultV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesDirectResponseV2 {
    pub metadata: DirectResponseMetadataV2,
    pub result: TradesQueryResultV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleFeedbackMetadataV2 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub platform_event_at_utc_ms: i64,
    pub feedback_received_at_utc_ms: i64,
    pub private_feedback_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderLifecycleEventV2 {
    OrderAccepted {
        status: SimulatedOrderStatusV2,
        account_delta: PlatformAccountDeltaV2,
    },
    FakUnspentReleased {
        released_quote_amount_pusd: ExactDecimal,
        account_delta: PlatformAccountDeltaV2,
    },
    VenueDustTerminal {
        closed_base_quantity_shares: ExactDecimal,
        released_quote_amount_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    MarketResolvedCanceled {
        resolution_event_id: String,
        canceled_base_quantity_shares: ExactDecimal,
        released_quote_amount_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    CallerCanceled {
        canceled_base_quantity_shares: ExactDecimal,
        released_quote_amount_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    GtdExpired {
        encoded_expires_at_utc_ms: i64,
        expired_base_quantity_shares: ExactDecimal,
        released_quote_amount_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderLifecycleFeedbackV2 {
    pub metadata: LifecycleFeedbackMetadataV2,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub event: OrderLifecycleEventV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeLifecycleFeedbackV2 {
    pub metadata: LifecycleFeedbackMetadataV2,
    pub trade_id: String,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub status: SimulatedTradeStatusV2,
    pub economic_disposition: TradeEconomicDispositionV2,
    pub liquidity_role: TradeLiquidityRoleV2,
    pub source_price_pusd_per_share: ExactDecimal,
    pub base_quantity_shares: ExactDecimal,
    pub quote_amount_pusd: ExactDecimal,
    pub immediate_taker_fee_pusd: ExactDecimal,
    pub economic_application_account_transition_seq: Option<u64>,
    pub account_delta: Option<PlatformAccountDeltaV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MakerRebateAccrualStatusV2 {
    Calculated,
    ZeroNoEligibleMakerFill,
    UnknownIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MakerRebateFillContributionV2 {
    pub trade_id: String,
    pub matched_at_utc_ms: i64,
    pub attribution_day_start_utc_ms: i64,
    pub base_quantity_shares: ExactDecimal,
    pub price_pusd_per_share: ExactDecimal,
    pub maker_fee_equivalent_pusd: ExactDecimal,
    pub rebate_rate: ExactDecimal,
    pub rebate_contribution_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementLifecycleEventV2 {
    MakerRebateAccrualFinalized {
        condition_id: ConditionId,
        status: MakerRebateAccrualStatusV2,
        contributions: Vec<MakerRebateFillContributionV2>,
        accrual_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    TakerRebateAccrualApplied {
        trade_id: String,
        condition_id: ConditionId,
        matched_at_utc_ms: i64,
        attribution_day_start_utc_ms: i64,
        actual_taker_fee_pusd: ExactDecimal,
        rebate_rate: ExactDecimal,
        accrual_pusd: ExactDecimal,
        account_delta: PlatformAccountDeltaV2,
    },
    MakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: ExactDecimal,
        payable_pusd: ExactDecimal,
        locked_remainder_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    TakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: ExactDecimal,
        payable_pusd: ExactDecimal,
        locked_remainder_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
    MakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: ExactDecimal,
        account_delta: PlatformAccountDeltaV2,
    },
    TakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: ExactDecimal,
        account_delta: PlatformAccountDeltaV2,
    },
    AutoRedeemApplied {
        condition_id: ConditionId,
        resolution_event_id: String,
        resolution_source_event_at_utc_ms: i64,
        resolution: MarketResolutionTerminalV3,
        redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
        redeemed_pusd: ExactDecimal,
        account_delta: Option<PlatformAccountDeltaV2>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementLifecycleFeedbackV2 {
    pub metadata: LifecycleFeedbackMetadataV2,
    pub event: SettlementLifecycleEventV2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DatasetDateEligibility {
    IneligibleBeforeDatasetFloor,
    Unassigned,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha256Hex(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalEventId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExactDecimal(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConditionId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClobMarketId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TokenId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "mapping",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum MappedMarketLifecycleStateV1 {
    Known { value: KnownMarketLifecycleState },
    UnknownUnmodeled { raw_value: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnownMarketLifecycleState {
    Candidate,
    Prefetched,
    Active,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeIdentityV1 {
    pub index: u32,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetIdentityV1 {
    pub token_id: TokenId,
    pub outcome: OutcomeIdentityV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketWindowV2 {
    pub start_at_utc_ms: i64,
    pub end_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketIdentityV2 {
    pub condition_id: ConditionId,
    pub clob_market_id: ClobMarketId,
    pub window: MarketWindowV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorAdmissionCoordinateV1 {
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub admitted_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTimeV2 {
    pub source_event_at_utc_ms: Option<i64>,
    pub received_at_utc_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalSourceKind {
    BinanceAggtrade,
    BinanceDepthSnapshot,
    BinanceDepthUpdate,
    PolymarketMarketMetadata,
    PolymarketOrderbook,
    PolymarketRtdsChainlinkBtcUsd,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceNativeIdentityV1 {
    BinanceAggregateTrade {
        aggregate_trade_id: u64,
    },
    BinanceDepthSnapshot {
        last_update_id: u64,
    },
    BinanceDepthUpdate {
        first_update_id: u64,
        final_update_id: u64,
    },
    SemanticEvidenceSha256 {
        sha256: Sha256Hex,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTimeIndexEntryV2 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub source_event_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityIndexEntryV2 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub received_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEvidenceRefV2 {
    pub source_kind: CanonicalSourceKind,
    pub source_native_identity: Option<SourceNativeIdentityV1>,
    pub source_event_at_utc_ms: Option<i64>,
    pub received_at_utc_ms: i64,
    pub collector_admission: Option<CollectorAdmissionCoordinateV1>,
    pub input_manifest_sha256: Sha256Hex,
    pub input_file_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLineageV2 {
    pub canonical_rule_version: String,
    pub source_evidence: Vec<SourceEvidenceRefV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataQuality {
    Fresh,
    Stale,
    Missing,
    UnknownNotObserved,
    ConfirmedGap,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventQualityV2 {
    pub availability: MarketDataQuality,
    pub resync: ResyncQuality,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResyncQuality {
    NotRequired,
    Required,
    ResyncBoundary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceLevelV1 {
    pub price_pusd_per_share: ExactDecimal,
    pub quantity_shares: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketLifecycleTransitionV2 {
    pub previous_state: Option<MappedMarketLifecycleStateV1>,
    pub current_state: MappedMarketLifecycleStateV1,
    pub assets: Vec<AssetIdentityV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTradingStateV2 {
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub accepting_orders: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialL2SnapshotV2 {
    pub asset: AssetIdentityV1,
    pub initial_tick_size_pusd_per_share: ExactDecimal,
    pub source_sequence_epoch_id: String,
    pub source_cursor: u64,
    pub book_digest_sha256: Sha256Hex,
    pub bids: Vec<PriceLevelV1>,
    pub asks: Vec<PriceLevelV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BestBidAskV2 {
    pub asset: AssetIdentityV1,
    pub best_bid_pusd_per_share: Option<ExactDecimal>,
    pub best_ask_pusd_per_share: Option<ExactDecimal>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickSizeChangeV2 {
    pub asset: AssetIdentityV1,
    pub old_tick_size_pusd_per_share: Option<ExactDecimal>,
    pub new_tick_size_pusd_per_share: ExactDecimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicTradeIdSourceV1 {
    PlatformTransactionHash,
    DerivedSourceEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicTradeV2 {
    pub asset: AssetIdentityV1,
    pub trade_id: String,
    pub trade_id_source: PublicTradeIdSourceV1,
    pub price_pusd_per_share: ExactDecimal,
    pub quantity_shares: ExactDecimal,
    pub aggressor_side: TradeSide,
    pub fee_rate_bps: Option<ExactDecimal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LevelExecutionBasis {
    TradeEvidence,
    LevelDeltaInference,
    BookResyncDeltaInference,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceAttributionQuality {
    Exact,
    Unattributed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueueResultQuality {
    Exact,
    DeterministicInferred,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelExecutionTransitionV3 {
    pub causal_group_id: Sha256Hex,
    pub market: MarketIdentityV2,
    pub asset: AssetIdentityV1,
    pub side: BookSide,
    pub price_pusd_per_share: ExactDecimal,
    pub quantity_before_shares: ExactDecimal,
    pub quantity_after_shares: ExactDecimal,
    pub attributed_trade_quantity_shares: ExactDecimal,
    pub already_covered_quantity_shares: ExactDecimal,
    pub net_added_quantity_shares: ExactDecimal,
    pub net_reduced_quantity_shares: ExactDecimal,
    pub queue_consumption_quantity_shares: ExactDecimal,
    pub basis: LevelExecutionBasis,
    pub platform_effective_at_utc_ms: i64,
    pub visible_at_utc_ms: i64,
    pub source_attribution_quality: SourceAttributionQuality,
    pub queue_result_quality: QueueResultQuality,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "asset",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum QualityScopeV1 {
    Market,
    Asset(AssetIdentityV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityBoundaryV2 {
    pub scope: QualityScopeV1,
    pub previous_quality: MarketDataQuality,
    pub current_quality: MarketDataQuality,
    pub reason_code: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayoutEntryV1 {
    pub asset: AssetIdentityV1,
    pub payout_pusd_per_share: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum MarketResolutionTerminalV3 {
    Resolved {
        winning_asset_id: String,
        winning_outcome: String,
        payout_vector: Vec<PayoutEntryV1>,
        mapping_rule_version: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "event_type",
    content = "event",
    rename_all = "snake_case"
)]
pub enum ExecutionMarketEventPayloadV3 {
    MarketLifecycleTransition(MarketLifecycleTransitionV2),
    MarketTradingState(MarketTradingStateV2),
    InitialL2Snapshot(InitialL2SnapshotV2),
    BestBidAsk(BestBidAskV2),
    TickSizeChange(TickSizeChangeV2),
    PublicTrade(PublicTradeV2),
    LevelExecutionTransition(LevelExecutionTransitionV3),
    QualityBoundary(QualityBoundaryV2),
    MarketResolutionTerminal(MarketResolutionTerminalV3),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionMarketEventV3 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub market: MarketIdentityV2,
    pub time: EventTimeV2,
    pub lineage: EventLineageV2,
    pub quality: EventQualityV2,
    pub payload: ExecutionMarketEventPayloadV3,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferencePriceLevelV1 {
    pub price_usdt_per_btc: ExactDecimal,
    pub quantity_btc: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainlinkBtcUsdReferenceV1 {
    pub symbol: String,
    pub price_usd_per_btc: ExactDecimal,
    pub message_timestamp_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceDepthStateReferenceV1 {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<ReferencePriceLevelV1>,
    pub asks: Vec<ReferencePriceLevelV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceAggregateTradeReferenceV1 {
    pub symbol: String,
    pub aggregate_trade_id: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub price_usdt_per_btc: ExactDecimal,
    pub quantity_btc: ExactDecimal,
    pub buyer_is_maker: bool,
    pub event_time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "reference_type",
    content = "reference",
    rename_all = "snake_case"
)]
pub enum CanonicalReferenceEventPayloadV1 {
    ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1),
    BinanceDepthState(BinanceDepthStateReferenceV1),
    BinanceAggregateTrade(BinanceAggregateTradeReferenceV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalReferenceEventV1 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub time: EventTimeV2,
    pub lineage: EventLineageV2,
    pub quality: EventQualityV2,
    pub payload: CanonicalReferenceEventPayloadV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "record_type",
    content = "record",
    rename_all = "snake_case"
)]
pub enum CanonicalEventRecordV1 {
    ExecutionMarketEvent(Box<ExecutionMarketEventV3>),
    ReferenceEvent(Box<CanonicalReferenceEventV1>),
}

impl CanonicalEventRecordV1 {
    pub fn as_execution_market_event(&self) -> Option<&ExecutionMarketEventV3> {
        match self {
            Self::ExecutionMarketEvent(event) => Some(event.as_ref()),
            Self::ReferenceEvent(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationVisibilityCutoffV2 {
    pub received_at_utc_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationComponentId {
    PolymarketL2,
    BestBidAsk,
    CurrentTick,
    PublicTrade,
    MarketLifecycle,
    MarketResolution,
    ChainlinkBtcUsd,
    BinanceBtcUsdtDepth,
    BinanceBtcUsdtTrade,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationComponentStateV2 {
    pub component: ObservationComponentId,
    pub quality: MarketDataQuality,
    pub source_event_at_utc_ms: Option<i64>,
    pub received_at_utc_ms: Option<i64>,
    pub information_age_ms: Option<u64>,
    pub observed_transport_delay_ms: Option<u64>,
    pub last_visible_event_id: Option<CanonicalEventId>,
    pub state_digest_sha256: Option<Sha256Hex>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationLineageV3 {
    pub canonical_event_record_contract_version: String,
    pub execution_event_contract_version: String,
    pub reference_event_contract_version: String,
    pub input_set_identity_sha256: Sha256Hex,
    pub visibility_index_manifest_sha256: Sha256Hex,
    pub contributing_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketObservationCausalShellV3 {
    pub schema_version: String,
    pub observation_id: Sha256Hex,
    pub market: MarketIdentityV2,
    pub lineage: ObservationLineageV3,
    pub visibility_cutoff: ObservationVisibilityCutoffV2,
    pub market_state_digest_sha256: Sha256Hex,
    pub component_states: Vec<ObservationComponentStateV2>,
    pub missing_mask: Vec<ObservationComponentId>,
    pub canonical_hash_sha256: Sha256Hex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GateACapabilityId {
    InputAdmissionAndSixSchemaMapping,
    CanonicalIdentityAndDualTimeProjection,
    MarketIdentityWindowAndLifecycle,
    InitialL2SnapshotEpochCursorAndDigest,
    PriceLevelQuantityTransition,
    BestBidAsk,
    DynamicTickSize,
    PublicTradeEvidence,
    LevelExecutionTransition,
    QualityGapResyncAndWatermark,
    MarketResolutionTerminal,
    ObservationSharedLineageAndVisibilityCutoff,
    ImmutableEventStoreAndDualIndexes,
    DeterminismAndSourceTraceability,
    BoundedResourcesAndAtomicPublication,
    ForbiddenLowFrequencyFields,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CapabilityStatus {
    Pass,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateACapabilityEntryV2 {
    pub capability: GateACapabilityId,
    pub status: CapabilityStatus,
    pub contract_field_paths: Vec<String>,
    pub evidence_ids: Vec<String>,
    pub blocking_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateACapabilityMatrixV2 {
    pub schema_version: String,
    pub entries: Vec<GateACapabilityEntryV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAReceiptV3 {
    pub schema_version: String,
    pub receipt_id: Sha256Hex,
    pub requirement_id: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub canonical_event_record_contract_version: String,
    pub execution_event_contract_version: String,
    pub reference_event_contract_version: String,
    pub observation_contract_version: String,
    pub event_time_index_contract_version: String,
    pub visibility_index_contract_version: String,
    pub dataset_lineage_root: String,
    pub dataset_date_floor: String,
    pub covered_utc_dates: Vec<String>,
    pub input_set_identity_sha256: Sha256Hex,
    pub supported_schema_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub canonical_event_store_manifest_sha256: Sha256Hex,
    pub event_time_index_manifest_sha256: Sha256Hex,
    pub visibility_index_manifest_sha256: Sha256Hex,
    pub observation_manifest_sha256: Sha256Hex,
    pub capability_matrix_sha256: Sha256Hex,
    pub run_manifest_sha256: Sha256Hex,
    pub training_dataset_ready: bool,
    pub training_split_manifest_id: String,
    pub training_split_manifest_sha256: String,
}
