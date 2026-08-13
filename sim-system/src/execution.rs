use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub use contracts::{
    AccountInstanceKeyV1, AccountOpeningAnchorV1, ConditionId, MarketEnrollmentV1,
    PlatformSourceV1, TokenId,
};
use contracts::{
    MarketDataQuality, MarketResolutionTerminalV3, PlatformAccountDeltaV1,
    PlatformOutcomeBalanceV1, Sha256Hex,
};
use polymarket_client_sdk_v2::clob::order_builder::{
    BuyOrderEncoding, encode_limit_buy_order, encode_market_buy_order,
};
pub use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::account::{
    INITIAL_ACCOUNT_STATE_SCHEMA_V1, InitialAccountStateErrorV1, InitialAccountStateV1,
};
use crate::account::{PlatformAccountStateV1, validate_platform_account_delta_integrity};
use crate::artifacts;
use crate::artifacts::{
    DurableRunLifecycleV1, SimulationEvidenceReferenceV1, SimulationRunIntentBindingV1,
};
use crate::finality::{TradeFinalityPolicyV1, TradeFinalityStateV1};
use crate::latency_profile::EmpiricalDelayProfile;
use crate::market::{
    ClosedMarketEventBucketV1, DurableMarketBucketReferenceV1, MarketBucketApplyDispositionV1,
    MarketEventErrorV1, MarketEventPathV1, MarketLifecyclePhaseV1, MarketStateV1,
    MillisecondBucketClosureV1,
};
use crate::matching::{
    CancelOrderStateDispositionV1, ExecutionStateV1, MatchingMathErrorV1, MatchingStateErrorV1,
    QueueMatchPlanV1, SettlementLifecycleActionV1, SettlementLifecycleEffectV1, SimFillRecordV1,
    SimLiquidityRoleV1, SimOrderKindStateV1, SimOrderRecordV1, SimOrderTerminalReasonV1,
    TerminalExecutionMarketEvictionV1, TradeEconomicDispositionStateV1,
    TradeFinalityLifecycleEffectsV1, TradeFinalityTransitionEffectV1,
    allocate_taker_fee_micro_units_v1, cumulative_segment_quote_units_v1,
    decimal_from_micro_units_v1, micro_units_v1, stable_fill_id,
};
use crate::measurement::{ComponentGoldenLoadContractV1, ComponentGoldenRunMeasurementV1};
use crate::persistence::{
    DurableCommitFailureV1, DurableResourcePreviewV1, ImmutableArtifactMemberV1,
    LocalDurabilityStoreV1, PersistenceErrorV1, RecoveredDurabilityV1, RecoveredEvidenceV1,
};
use crate::platform_rules::{SealedProcessRuleSnapshot, SnapshotAccessMode};
use crate::rebate::{MakerRebateMarketResultStatusV1, RebateKindV1, RebateSettlementActionV1};
use crate::resource::{
    ResourceAdmissionStateV1, ResourceLimitKindV1, ResourceLimitReachedV1, ResourceLimitScopeV1,
    ResourceQuantityV1, SimulationResourceHighWaterV1, SimulationResourcePolicyV1,
    SimulationResourceUsageV1,
};
#[cfg(test)]
use crate::resource::{
    SimulationDurableResourceUsageV1, SimulationRunResourceUsageV1,
    SimulationSessionResourceUsageV1,
};
pub use crate::scenario::{
    BTC_UPDOWN_5M_MARKET_FAMILY_V1, MARKET_SET_SCHEMA_V1, MarketSetV1, PLATFORM_SCENARIO_SCHEMA_V1,
    PlatformMarketV1, PlatformScenarioErrorV1, PlatformScenarioV1,
};
pub use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, MarketEnrollmentRequestV1,
    ModelVisibleCursorV1, RequestContextV1, RequestRegistrationDispositionV1, RequestTransitV1,
    SIM_EXECUTION_CONTRACT_V1, SessionCreationControlResultV2, SessionCreationDispositionV2,
    SessionError as SimExecutionContractErrorV1, SessionInitializationControlDispositionV1,
    SessionInitializationControlResultV1, SessionInitializationSnapshotV1,
    SessionInitializationStateV1, SessionSpecV2, SimCallerRoleV1, SimProcessRootErrorV1,
};
use crate::session::{
    InputPathSealOutcomeV1, MutationResolutionDispositionV1, ParsedLogicalActionV1,
    ParsedMethodTimingV1, PlatformPrefixCursorV1, PreparedSimSessionV2, PublicVisibilityCursorV1,
    QueuedLogicalActionV1, RegisteredLogicalScheduleV1, SessionCheckpointErrorV1, SessionError,
    SimProcessRoot, SimProcessRootCheckpointV1, SimSession, SimSessionCheckpointV1,
    validate_durable_market_enrollment_chain,
};

const POST_ORDERS_MAX_ITEMS: usize = 15;
const CANCEL_ORDERS_MAX_ITEMS: usize = 1_000;
pub const SIM_QUERY_PAGE_SIZE_V1: usize = 100;
const SIM_QUERY_PAGE_POLICY_V1: &str = "PROJECT_QUERY_PAGE_100_V1";
const CANONICAL_REQUEST_HASH_DOMAIN: &str = "SIM_EXECUTION_CANONICAL_REQUEST_V1";
const MUTATION_PAYLOAD_HASH_DOMAIN: &str = "SIM_EXECUTION_MUTATION_PAYLOAD_V1";
const DURABLE_RUN_CHECKPOINT_SCHEMA_V1: &str = "sim_durable_run_checkpoint_v1";
const DURABLE_TRANSACTION_EVIDENCE_SCHEMA_V1: &str = "sim_durable_transaction_evidence_v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimBuyOrderKindV1 {
    GtcLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
    },
    GtdLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
        expires_at_utc_ms: i64,
    },
    FokMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
    FakMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
}

impl SimBuyOrderKindV1 {
    fn method_timing(&self) -> ParsedMethodTimingV1 {
        match self {
            Self::GtcLimit { .. } | Self::GtdLimit { .. } => ParsedMethodTimingV1::Ordinary,
            Self::FokMarket { .. } | Self::FakMarket { .. } => {
                ParsedMethodTimingV1::FokFakMarketBuy
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimBuyOrderV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub kind: SimBuyOrderKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderItemV1 {
    pub item_id: String,
    pub order: SimBuyOrderV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderItemV1 {
    pub item_id: String,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<PostOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<CancelOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAllOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRequestV1 {
    pub context: RequestContextV1,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersFilterV1 {
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesFilterV1 {
    pub trade_id: Option<String>,
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
    pub after_utc_ms: Option<i64>,
    pub before_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpaquePageCursorV1(String);

impl OpaquePageCursorV1 {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersRequestV1 {
    pub context: RequestContextV1,
    pub filter: OrdersFilterV1,
    pub cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesRequestV1 {
    pub context: RequestContextV1,
    pub filter: TradesFilterV1,
    pub cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRegistrationReceiptV1 {
    pub execution_contract_version: String,
    pub global_request_id: String,
    pub disposition: RequestRegistrationDispositionV1,
    pub transit: RequestTransitV1,
    pub scheduled_action_count: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredItemV1 {
    pub item_id: String,
    pub scheduled_action_ordinal: u16,
    pub effective_processing_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub item: RegisteredItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrdersRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub items: Vec<RegisteredItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub item: RegisteredItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub items: Vec<RegisteredItemV1>,
}

macro_rules! simple_registration {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name {
            pub receipt: RequestRegistrationReceiptV1,
        }
    };
}

simple_registration!(CancelAllOrdersRegistrationV1);
simple_registration!(OrderRegistrationV1);
simple_registration!(OrdersRegistrationV1);
simple_registration!(TradesRegistrationV1);

pub const SIMULATED_EXECUTION_EVIDENCE_V1: &str = "SIMULATED";
pub const ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1: &str = "order_lifecycle_feedback_v1";
pub const TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1: &str = "trade_lifecycle_feedback_v1";
pub const SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1: &str = "settlement_lifecycle_feedback_v1";
pub const L2_BOOK_DELTA_FIFO_ESTIMATE_V1: &str = "L2_BOOK_DELTA_FIFO_ESTIMATE_V1";
pub const L2_AGGREGATE_SYNTHETIC_MAKER_V1: &str = "L2_AGGREGATE_SYNTHETIC_MAKER_V1";
pub const SUBMISSION_FEE_AGGREGATION_V1: &str =
    "SUM_EXACT_PER_LEVEL_THEN_MIDPOINT_AWAY_FROM_ZERO_5DP_V1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionFeeRuleIdentityV1 {
    pub formula_version: String,
    pub aggregation_version: String,
    pub precision_decimal_places: u32,
    pub minimum_nonzero_fee_pusd: Decimal,
    pub builder_fee_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionResponseMetadataV1 {
    pub execution_contract_version: String,
    pub evidence_source: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_role: SimCallerRoleV1,
    pub caller_instance_id: String,
    pub global_request_id: String,
    pub caller_correlation_id: Option<String>,
    pub method_identity: String,
    pub platform_responded_at_utc_ms: i64,
    pub response_received_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub private_feedback_sequence: u64,
    pub delivery_id: String,
    pub rule_snapshot_sha256: Sha256Hex,
    pub scenario_sha256: Sha256Hex,
    pub queue_model_version: String,
    pub segment_quantization_model_version: String,
    pub fee_rule: SubmissionFeeRuleIdentityV1,
    pub payload_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionPlatformOrderStatusV1 {
    Live,
    Matched,
    VenueDustTerminal,
    Delayed,
    Unmatched,
    Unknown(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionTerminalReasonV1 {
    FakCompleted,
    VenueDustTerminal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionFillV1 {
    pub simulated_trade_id: String,
    pub source_price_pusd_per_share: Decimal,
    pub base_quantity_shares: Decimal,
    pub quote_amount_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSubmissionMeasurementV1 {
    pub accepted_base_quantity_shares: Decimal,
    pub matched_base_quantity_shares: Decimal,
    pub open_base_quantity_shares: Decimal,
    pub venue_dust_closed_base_quantity_shares: Decimal,
    pub order_type_terminated_base_quantity_shares: Decimal,
    pub accepted_quote_amount_pusd: Decimal,
    pub matched_quote_amount_pusd: Decimal,
    pub open_encumbered_quote_amount_pusd: Decimal,
    pub venue_dust_released_quote_amount_pusd: Decimal,
    pub released_price_improvement_quote_amount_pusd: Decimal,
    pub immediate_taker_fee_encumbered_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteBudgetSubmissionMeasurementV1 {
    pub accepted_quote_budget_pusd: Decimal,
    pub used_quote_amount_pusd: Decimal,
    pub released_or_unspent_quote_amount_pusd: Decimal,
    pub actual_matched_base_quantity_shares: Decimal,
    pub sdk_taker_amount_audit_shares: Decimal,
    pub immediate_taker_fee_encumbered_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionMeasurementV1 {
    Base(BaseSubmissionMeasurementV1),
    QuoteBudget(QuoteBudgetSubmissionMeasurementV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedSubmissionItemV1 {
    pub simulated_platform_order_id: String,
    pub platform_order_status: SubmissionPlatformOrderStatusV1,
    pub fills: Vec<SubmissionFillV1>,
    pub terminal_reason: Option<SubmissionTerminalReasonV1>,
    pub measurement: SubmissionMeasurementV1,
    pub account_deltas: Vec<AccountDeltaReferenceV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionRejectedReasonV1 {
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
pub enum SubmissionEvidenceUnknownReasonV1 {
    Preloading,
    TargetAssetNotFresh(MarketDataQuality),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionItemOutcomeV1 {
    Accepted(Box<AcceptedSubmissionItemV1>),
    RejectedNoEffect(SubmissionRejectedReasonV1),
    EvidenceUnknownNoEffect(SubmissionEvidenceUnknownReasonV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionEffectDispositionV1 {
    NewEffect,
    ExistingEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionResponseItemV1 {
    pub item_id: String,
    pub batch_item_ordinal: u16,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub effective_processing_at_utc_ms: i64,
    pub submission_effect_identity: String,
    pub submission_effect_formed_at_utc_ms: i64,
    pub submission_effect_disposition: SubmissionEffectDispositionV1,
    pub outcome: SubmissionItemOutcomeV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub item: SubmissionResponseItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub items: Vec<SubmissionResponseItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelInputItemRefV1 {
    pub item_id: String,
    pub input_item_ordinal: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelNotCanceledReasonV1 {
    NotFoundOrNotOwned,
    NoCancellableRemainder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelEvidenceUnknownReasonV1 {
    RetainedEvidenceIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanceledTargetMeasurementV1 {
    pub canceled_base_quantity_shares: Decimal,
    pub released_quote_amount_pusd: Decimal,
    pub account_delta: Option<AccountDeltaReferenceV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelTargetOutcomeV1 {
    Canceled(CanceledTargetMeasurementV1),
    NotCanceledNoEffect(CancelNotCanceledReasonV1),
    EvidenceUnknownNoEffect(CancelEvidenceUnknownReasonV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelEffectDispositionV1 {
    NewEffect,
    ExistingEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTargetResponseV1 {
    pub platform_order_id: String,
    pub target_ordinal: u64,
    pub input_item_refs: Vec<CancelInputItemRefV1>,
    pub effective_processing_at_utc_ms: i64,
    pub cancel_effect_identity: String,
    pub cancel_effect_formed_at_utc_ms: i64,
    pub cancel_effect_disposition: CancelEffectDispositionV1,
    pub outcome: CancelTargetOutcomeV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub target: CancelTargetResponseV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub targets: Vec<CancelTargetResponseV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelAllOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub targets: Vec<CancelTargetResponseV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulatedOrderKnownStatusV1 {
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
pub struct SimulatedOrderStatusV1 {
    pub raw: String,
    pub known: Option<SimulatedOrderKnownStatusV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimulatedTradeKnownStatusV1 {
    MatchedNotBroadcasted,
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedTradeStatusV1 {
    pub raw: String,
    pub known: Option<SimulatedTradeKnownStatusV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKindProjectionV1 {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderProjectionV1 {
    pub platform_order_id: String,
    pub global_request_id: String,
    pub item_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub created_at_utc_ms: i64,
    pub last_updated_at_utc_ms: i64,
    pub kind: OrderKindProjectionV1,
    pub expires_at_utc_ms: Option<i64>,
    pub source_price_pusd_per_share: Decimal,
    pub status: SimulatedOrderStatusV1,
    pub accepted_base_quantity_shares: Option<Decimal>,
    pub matched_base_quantity_shares: Decimal,
    pub pending_maker_depletion_base_quantity_shares: Decimal,
    pub open_base_quantity_shares: Decimal,
    pub venue_dust_closed_base_quantity_shares: Decimal,
    pub market_resolved_canceled_base_quantity_shares: Decimal,
    pub canceled_base_quantity_shares: Decimal,
    pub expired_base_quantity_shares: Decimal,
    pub accepted_quote_amount_pusd: Decimal,
    pub matched_quote_amount_pusd: Decimal,
    pub open_encumbered_quote_amount_pusd: Decimal,
    pub venue_dust_released_quote_amount_pusd: Decimal,
    pub market_resolved_released_quote_amount_pusd: Decimal,
    pub released_price_improvement_quote_amount_pusd: Decimal,
    pub canceled_released_quote_amount_pusd: Decimal,
    pub expired_released_quote_amount_pusd: Decimal,
    pub immediate_taker_fee_encumbered_pusd: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeLiquidityRoleV1 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeEconomicDispositionV1 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeProjectionV1 {
    pub simulated_trade_id: String,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub matched_at_utc_ms: i64,
    pub status: SimulatedTradeStatusV1,
    pub liquidity_role: TradeLiquidityRoleV1,
    pub source_price_pusd_per_share: Decimal,
    pub base_quantity_shares: Decimal,
    pub quote_amount_pusd: Decimal,
    pub immediate_taker_fee_pusd: Decimal,
    pub economic_disposition: TradeEconomicDispositionV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryRetentionScopeV1 {
    OrderHistory,
    TradeHistory,
    RootSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryOutOfRetentionV1 {
    pub scope: QueryRetentionScopeV1,
    pub retained_from_utc_ms: Option<i64>,
    pub requested_root_sequence: Option<u64>,
    pub earliest_retained_root_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderQueryResultV1 {
    Found(Box<OrderProjectionV1>),
    NotFound,
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersPageV1 {
    pub root_identity: String,
    pub query_projection_revision: u64,
    pub items: Vec<OrderProjectionV1>,
    pub next_cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesPageV1 {
    pub root_identity: String,
    pub query_projection_revision: u64,
    pub items: Vec<TradeProjectionV1>,
    pub next_cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrdersQueryResultV1 {
    Page(OrdersPageV1),
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradesQueryResultV1 {
    Page(TradesPageV1),
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: OrderQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: OrdersQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradesDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: TradesQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseDeliveryConfirmationV1 {
    pub session_id: String,
    pub delivery_id: String,
    pub private_feedback_sequence: u64,
    pub payload_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownstreamDurabilityProofV1 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub durable_through: ResponseDeliveryConfirmationV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDeltaReferenceV1 {
    pub account_transition_seq: u64,
    pub delta_id: String,
    pub delta_sha256: Sha256Hex,
}

impl From<&PlatformAccountDeltaV1> for AccountDeltaReferenceV1 {
    fn from(delta: &PlatformAccountDeltaV1) -> Self {
        Self {
            account_transition_seq: delta.account_transition_seq,
            delta_id: delta.delta_id.clone(),
            delta_sha256: delta.delta_sha256.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderLifecycleEventKindV1 {
    OrderAccepted,
    FakUnspentReleased,
    VenueDustTerminal,
    MarketResolvedCanceled,
    CallerCanceled,
    GtdExpired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderLifecycleFeedbackMetadataV1 {
    pub schema_version: String,
    pub evidence_source: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub session_id: String,
    pub delivery_id: String,
    pub platform_event_at_utc_ms: i64,
    pub feedback_received_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub private_feedback_sequence: u64,
    pub payload_sha256: Sha256Hex,
}

impl OrderLifecycleFeedbackMetadataV1 {
    #[must_use]
    pub fn delivery_confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        ResponseDeliveryConfirmationV1 {
            session_id: self.session_id.clone(),
            delivery_id: self.delivery_id.clone(),
            private_feedback_sequence: self.private_feedback_sequence,
            payload_sha256: self.payload_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderLifecycleFeedbackV1 {
    pub metadata: OrderLifecycleFeedbackMetadataV1,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub event_kind: OrderLifecycleEventKindV1,
    pub order_status: SimulatedOrderStatusV1,
    pub venue_dust_closed_base_quantity_shares: Decimal,
    pub venue_dust_released_quote_amount_pusd: Decimal,
    pub market_resolved_canceled_base_quantity_shares: Decimal,
    pub market_resolved_released_quote_amount_pusd: Decimal,
    pub account_delta: PlatformAccountDeltaV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeLifecycleFeedbackMetadataV1 {
    pub schema_version: String,
    pub evidence_source: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub session_id: String,
    pub delivery_id: String,
    pub platform_event_at_utc_ms: i64,
    pub feedback_received_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub private_feedback_sequence: u64,
    pub payload_sha256: Sha256Hex,
}

impl TradeLifecycleFeedbackMetadataV1 {
    #[must_use]
    pub fn delivery_confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        ResponseDeliveryConfirmationV1 {
            session_id: self.session_id.clone(),
            delivery_id: self.delivery_id.clone(),
            private_feedback_sequence: self.private_feedback_sequence,
            payload_sha256: self.payload_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeLifecycleFeedbackV1 {
    pub metadata: TradeLifecycleFeedbackMetadataV1,
    pub simulated_trade_id: String,
    pub platform_order_id: String,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub status: SimulatedTradeStatusV1,
    pub economic_disposition: TradeEconomicDispositionV1,
    pub liquidity_role: TradeLiquidityRoleV1,
    pub base_quantity_shares: Decimal,
    pub quote_amount_pusd: Decimal,
    pub immediate_taker_fee_pusd: Decimal,
    pub economic_application_delta: Option<AccountDeltaReferenceV1>,
    pub account_delta: Option<PlatformAccountDeltaV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MakerRebateAccrualStatusV1 {
    Calculated,
    ZeroNoEligibleMakerFill,
    UnknownIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MakerRebateFillContributionV1 {
    pub simulated_trade_id: String,
    pub matched_at_utc_ms: i64,
    pub attribution_day_start_utc_ms: i64,
    pub base_quantity_shares: Decimal,
    pub price_pusd_per_share: Decimal,
    pub maker_fee_equivalent_pusd: Decimal,
    pub rebate_rate: Decimal,
    pub rebate_contribution_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementLifecycleEventV1 {
    MakerRebateAccrualFinalized {
        condition_id: ConditionId,
        status: MakerRebateAccrualStatusV1,
        contributions: Vec<MakerRebateFillContributionV1>,
        accrual_pusd: Decimal,
        account_delta: Option<PlatformAccountDeltaV1>,
    },
    TakerRebateAccrualApplied {
        simulated_trade_id: String,
        condition_id: ConditionId,
        matched_at_utc_ms: i64,
        attribution_day_start_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
        rebate_rate: Decimal,
        accrual_pusd: Decimal,
        account_delta: PlatformAccountDeltaV1,
    },
    MakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: Decimal,
        payable_pusd: Decimal,
        locked_remainder_pusd: Decimal,
        account_delta: Option<PlatformAccountDeltaV1>,
    },
    TakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: Decimal,
        payable_pusd: Decimal,
        locked_remainder_pusd: Decimal,
        account_delta: Option<PlatformAccountDeltaV1>,
    },
    MakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: Decimal,
        account_delta: PlatformAccountDeltaV1,
    },
    TakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: Decimal,
        account_delta: PlatformAccountDeltaV1,
    },
    AutoRedeemApplied {
        condition_id: ConditionId,
        resolution_event_id: String,
        resolution_source_event_at_utc_ms: i64,
        resolution: MarketResolutionTerminalV3,
        redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
        redeemed_pusd: Decimal,
        account_delta: Option<PlatformAccountDeltaV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementLifecycleFeedbackMetadataV1 {
    pub schema_version: String,
    pub evidence_source: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub session_id: String,
    pub delivery_id: String,
    pub platform_event_at_utc_ms: i64,
    pub feedback_received_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub private_feedback_sequence: u64,
    pub payload_sha256: Sha256Hex,
}

impl SettlementLifecycleFeedbackMetadataV1 {
    #[must_use]
    pub fn delivery_confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        ResponseDeliveryConfirmationV1 {
            session_id: self.session_id.clone(),
            delivery_id: self.delivery_id.clone(),
            private_feedback_sequence: self.private_feedback_sequence,
            payload_sha256: self.payload_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementLifecycleFeedbackV1 {
    pub metadata: SettlementLifecycleFeedbackMetadataV1,
    pub settlement_identity: String,
    pub event: SettlementLifecycleEventV1,
}

impl SubmissionResponseMetadataV1 {
    #[must_use]
    pub fn delivery_confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        ResponseDeliveryConfirmationV1 {
            session_id: self.session_id.clone(),
            delivery_id: self.delivery_id.clone(),
            private_feedback_sequence: self.private_feedback_sequence,
            payload_sha256: self.payload_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionResponseAcceptanceV1 {
    Accepted(ResponseDeliveryConfirmationV1),
    Full,
    Disconnected,
}

type PostOrderResponseHandlerV1 = Box<
    dyn FnMut(Arc<PostOrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type PostOrdersResponseHandlerV1 = Box<
    dyn FnMut(Arc<PostOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type CancelOrderResponseHandlerV1 = Box<
    dyn FnMut(Arc<CancelOrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type CancelOrdersResponseHandlerV1 = Box<
    dyn FnMut(Arc<CancelOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type CancelAllOrdersResponseHandlerV1 = Box<
    dyn FnMut(Arc<CancelAllOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1
        + Send
        + 'static,
>;
type OrderResponseHandlerV1 =
    Box<dyn FnMut(Arc<OrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static>;
type OrdersResponseHandlerV1 =
    Box<dyn FnMut(Arc<OrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static>;
type TradesResponseHandlerV1 =
    Box<dyn FnMut(Arc<TradesDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static>;
type OrderLifecycleFeedbackHandlerV1 = Box<
    dyn FnMut(Arc<OrderLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type TradeLifecycleFeedbackHandlerV1 = Box<
    dyn FnMut(Arc<TradeLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
>;
type SettlementLifecycleFeedbackHandlerV1 = Box<
    dyn FnMut(Arc<SettlementLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1
        + Send
        + 'static,
>;

pub struct SubmissionResponseBridgeV1 {
    post_order_handler: PostOrderResponseHandlerV1,
    post_orders_handler: PostOrdersResponseHandlerV1,
    cancel_order_handler: CancelOrderResponseHandlerV1,
    cancel_orders_handler: CancelOrdersResponseHandlerV1,
    cancel_all_orders_handler: CancelAllOrdersResponseHandlerV1,
    order_handler: OrderResponseHandlerV1,
    orders_handler: OrdersResponseHandlerV1,
    trades_handler: TradesResponseHandlerV1,
    order_lifecycle_feedback_handler: OrderLifecycleFeedbackHandlerV1,
    trade_lifecycle_feedback_handler: TradeLifecycleFeedbackHandlerV1,
    settlement_lifecycle_feedback_handler: SettlementLifecycleFeedbackHandlerV1,
}

impl SubmissionResponseBridgeV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new<
        PostOrderHandler,
        PostOrdersHandler,
        CancelOrderHandler,
        CancelOrdersHandler,
        CancelAllOrdersHandler,
        OrderHandler,
        OrdersHandler,
        TradesHandler,
        OrderLifecycleFeedbackHandler,
        TradeLifecycleFeedbackHandler,
        SettlementLifecycleFeedbackHandler,
    >(
        post_order_handler: PostOrderHandler,
        post_orders_handler: PostOrdersHandler,
        cancel_order_handler: CancelOrderHandler,
        cancel_orders_handler: CancelOrdersHandler,
        cancel_all_orders_handler: CancelAllOrdersHandler,
        order_handler: OrderHandler,
        orders_handler: OrdersHandler,
        trades_handler: TradesHandler,
        order_lifecycle_feedback_handler: OrderLifecycleFeedbackHandler,
        trade_lifecycle_feedback_handler: TradeLifecycleFeedbackHandler,
        settlement_lifecycle_feedback_handler: SettlementLifecycleFeedbackHandler,
    ) -> Self
    where
        PostOrderHandler: FnMut(Arc<PostOrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
        PostOrdersHandler: FnMut(Arc<PostOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
        CancelOrderHandler: FnMut(Arc<CancelOrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
        CancelOrdersHandler: FnMut(Arc<CancelOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
        CancelAllOrdersHandler: FnMut(Arc<CancelAllOrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
        OrderHandler:
            FnMut(Arc<OrderDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
        OrdersHandler:
            FnMut(Arc<OrdersDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
        TradesHandler:
            FnMut(Arc<TradesDirectResponseV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
        OrderLifecycleFeedbackHandler:
            FnMut(Arc<OrderLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
        TradeLifecycleFeedbackHandler:
            FnMut(Arc<TradeLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1 + Send + 'static,
        SettlementLifecycleFeedbackHandler: FnMut(Arc<SettlementLifecycleFeedbackV1>) -> SubmissionResponseAcceptanceV1
            + Send
            + 'static,
    {
        Self {
            post_order_handler: Box::new(post_order_handler),
            post_orders_handler: Box::new(post_orders_handler),
            cancel_order_handler: Box::new(cancel_order_handler),
            cancel_orders_handler: Box::new(cancel_orders_handler),
            cancel_all_orders_handler: Box::new(cancel_all_orders_handler),
            order_handler: Box::new(order_handler),
            orders_handler: Box::new(orders_handler),
            trades_handler: Box::new(trades_handler),
            order_lifecycle_feedback_handler: Box::new(order_lifecycle_feedback_handler),
            trade_lifecycle_feedback_handler: Box::new(trade_lifecycle_feedback_handler),
            settlement_lifecycle_feedback_handler: Box::new(settlement_lifecycle_feedback_handler),
        }
    }

    fn accept_post_order(
        &mut self,
        response: Arc<PostOrderDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.post_order_handler)(response)
    }

    fn accept_post_orders(
        &mut self,
        response: Arc<PostOrdersDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.post_orders_handler)(response)
    }

    fn accept_cancel_order(
        &mut self,
        response: Arc<CancelOrderDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.cancel_order_handler)(response)
    }

    fn accept_cancel_orders(
        &mut self,
        response: Arc<CancelOrdersDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.cancel_orders_handler)(response)
    }

    fn accept_cancel_all_orders(
        &mut self,
        response: Arc<CancelAllOrdersDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.cancel_all_orders_handler)(response)
    }

    fn accept_order(
        &mut self,
        response: Arc<OrderDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.order_handler)(response)
    }

    fn accept_orders(
        &mut self,
        response: Arc<OrdersDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.orders_handler)(response)
    }

    fn accept_trades(
        &mut self,
        response: Arc<TradesDirectResponseV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.trades_handler)(response)
    }

    fn accept_order_lifecycle_feedback(
        &mut self,
        feedback: Arc<OrderLifecycleFeedbackV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.order_lifecycle_feedback_handler)(feedback)
    }

    fn accept_trade_lifecycle_feedback(
        &mut self,
        feedback: Arc<TradeLifecycleFeedbackV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.trade_lifecycle_feedback_handler)(feedback)
    }

    fn accept_settlement_lifecycle_feedback(
        &mut self,
        feedback: Arc<SettlementLifecycleFeedbackV1>,
    ) -> SubmissionResponseAcceptanceV1 {
        (self.settlement_lifecycle_feedback_handler)(feedback)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizedPlatformRevisionV1 {
    EventTime(PlatformPrefixCursorV1),
    ReceivedTime(PublicVisibilityCursorV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedBuyOrderV1 {
    GtcLimit {
        limit_price_pusd_per_share: Decimal,
        maker_amount_pusd: Decimal,
        taker_amount_shares: Decimal,
        post_only: bool,
    },
    GtdLimit {
        limit_price_pusd_per_share: Decimal,
        maker_amount_pusd: Decimal,
        taker_amount_shares: Decimal,
        post_only: bool,
        expires_at_utc_ms: i64,
    },
    FokMarket {
        worst_price_pusd_per_share: Decimal,
        maker_amount_pusd: Decimal,
        taker_amount_shares: Decimal,
    },
    FakMarket {
        worst_price_pusd_per_share: Decimal,
        maker_amount_pusd: Decimal,
        taker_amount_shares: Decimal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedSubmissionV1 {
    pub global_request_id: String,
    pub action_identity: String,
    pub item_id: String,
    pub batch_item_ordinal: u16,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub order: PreparedBuyOrderV1,
    pub effective_processing_at_utc_ms: i64,
    pub authorized_platform_revision: AuthorizedPlatformRevisionV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionAdmissionRejectionV1 {
    EndAtReached,
    ExplicitTradingStateVeto,
    MarketNotInScenario,
    AssetNotInMarket,
    OrderEncodingRejected,
    BelowMinimumShares,
    PostOnlyWouldTake,
    BelowMarketableLimitBuyMinimumPrincipal,
    GtdExpirationTooSoon,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionEvidenceUnknownV1 {
    Preloading,
    TargetAssetNotFresh(MarketDataQuality),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionAdmissionDecisionV1 {
    Prepared(Box<PreparedSubmissionV1>),
    RejectedNoEffect(SubmissionAdmissionRejectionV1),
    EvidenceUnknownNoEffect(SubmissionEvidenceUnknownV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionPreparationErrorV1 {
    Session(SessionError),
    RequestNotFound(String),
    ActionIsNotSubmission(String),
    ActionMappingConflict(String),
    MissingCurrentTick(String),
    TimeOverflow,
}

impl From<SessionError> for SubmissionPreparationErrorV1 {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionProcessingErrorV1 {
    Session(SessionError),
    Preparation(SubmissionPreparationErrorV1),
    Matching(MatchingStateErrorV1),
    Math(MatchingMathErrorV1),
    InternalInvariant(&'static str),
}

impl From<SessionError> for SubmissionProcessingErrorV1 {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

impl From<SubmissionPreparationErrorV1> for SubmissionProcessingErrorV1 {
    fn from(value: SubmissionPreparationErrorV1) -> Self {
        Self::Preparation(value)
    }
}

impl From<MatchingStateErrorV1> for SubmissionProcessingErrorV1 {
    fn from(value: MatchingStateErrorV1) -> Self {
        Self::Matching(value)
    }
}

impl From<MatchingMathErrorV1> for SubmissionProcessingErrorV1 {
    fn from(value: MatchingMathErrorV1) -> Self {
        Self::Math(value)
    }
}

fn fatal_session_processing_error(error: &SessionError) -> bool {
    matches!(
        error,
        SessionError::RequestNotFound(_)
            | SessionError::RequestNotLogicallyReleased(_)
            | SessionError::ActionNotLogicallyReleased(_)
            | SessionError::RequestIdentityConflict(_)
            | SessionError::TimeOverflow
            | SessionError::ResponseBeforeArrival
            | SessionError::ResponseBeforeLogicalCompletion
            | SessionError::FeedbackIdentityConflict(_)
            | SessionError::PrivateFactScheduleClosed
            | SessionError::PrivateFactScheduleCrossesExpectedResponse(_)
            | SessionError::PrivateFactScheduleCrossesPendingAction(_)
            | SessionError::PrivateFactScheduleCrossesPendingPlatformWork(_)
            | SessionError::PrivateFactIdentityConflict(_)
            | SessionError::PrivateEmissionSequenceConflict(_)
            | SessionError::ActionScheduleEmpty
            | SessionError::ActionScheduleSealed(_)
            | SessionError::ActionScheduleNotSealed(_)
            | SessionError::ActionProcessingInProgress(_)
            | SessionError::ActionIdentityConflict(_)
            | SessionError::MutationIdentityConflict(_)
            | SessionError::QueryIdentityConflict(_)
            | SessionError::PlatformRevisionRegression
            | SessionError::ExecutionState(_)
            | SessionError::ResponseDeliveryIdentityConflict
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionDriveDispositionV1 {
    NoAction,
    ItemProcessed { request_completed: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmissionDeliveryDispositionV1 {
    NoVisibleResponse,
    Delivered,
    Full,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableRequestRegistrationV1 {
    request: PendingDomainRequestV1,
    transit: RequestTransitV1,
    actions: Vec<QueuedLogicalActionV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableLogicalActionResultV1 {
    action_identity: String,
    global_request_id: String,
    batch_item_ordinal: u16,
    query_projection_revision: Option<u64>,
    part: DirectResponsePartV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactedConfirmedPrivateDeliveryV1 {
    kind: CompactedPrivateDeliveryKindV1,
    confirmation: ResponseDeliveryConfirmationV1,
    received_at_utc_ms: i64,
    private_emission_sequence: u64,
    account_deltas: Vec<PlatformAccountDeltaV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum CompactedPrivateDeliveryKindV1 {
    Direct,
    Lifecycle,
}

impl CompactedConfirmedPrivateDeliveryV1 {
    fn try_from_delivery(
        delivery: &SealedPrivateDeliveryV1,
    ) -> Result<Self, SessionCheckpointErrorV1> {
        let mut account_deltas = Vec::new();
        delivery.visit_account_deltas(&mut |delta| {
            account_deltas.push(delta.clone());
            Ok(())
        })?;
        let (received_at_utc_ms, private_emission_sequence) =
            delivery.received_at_and_emission_sequence();
        Ok(Self {
            kind: match delivery {
                SealedPrivateDeliveryV1::Direct(_) => CompactedPrivateDeliveryKindV1::Direct,
                SealedPrivateDeliveryV1::Lifecycle(_) => CompactedPrivateDeliveryKindV1::Lifecycle,
            },
            confirmation: delivery.confirmation(),
            received_at_utc_ms,
            private_emission_sequence,
            account_deltas,
        })
    }

    fn received_at_and_emission_sequence(&self) -> (i64, u64) {
        (self.received_at_utc_ms, self.private_emission_sequence)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DurableLogicalActionRecoveryContextV1 {
    query_projection_revision: u64,
    query_retention: (u64, Option<i64>, Option<i64>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum DurablePrivateOutputDraftV1 {
    Direct { draft: DirectResponseDraftV1 },
    Lifecycle { draft: LifecycleFeedbackDraftV1 },
}

impl DurablePrivateOutputDraftV1 {
    fn delivery_id(&self) -> &str {
        match self {
            Self::Direct { draft } => &draft.delivery_id,
            Self::Lifecycle { draft } => draft.delivery_id(),
        }
    }

    fn session_id(&self) -> &str {
        match self {
            Self::Direct { draft } => &draft.context.session_id,
            Self::Lifecycle { draft } => draft.session_id(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DurableTransactionFactV1 {
    RunCreated {
        run_id: String,
        market_event_source_identity_sha256: Sha256Hex,
        run_intent_binding: SimulationRunIntentBindingV1,
        resource_policy: SimulationResourcePolicyV1,
        resource_policy_sha256: Sha256Hex,
    },
    RunArtifactInputsSealed {
        run_id: String,
    },
    ComponentGoldenMeasurementRecorded {
        run_id: String,
        measurement: Box<ComponentGoldenRunMeasurementV1>,
        measurement_sha256: Sha256Hex,
    },
    SessionCreated {
        session_spec: SessionSpecV2,
        opening_anchor: Box<AccountOpeningAnchorV1>,
        initialization: SessionInitializationSnapshotV1,
    },
    SessionActivated {
        request: ActivateSessionRequestV1,
        result: SessionInitializationSnapshotV1,
    },
    SessionInitializationFailed {
        request: FailSessionInitializationRequestV1,
        result: SessionInitializationSnapshotV1,
    },
    MarketEnrolled {
        session_id: String,
        enrollment: MarketEnrollmentV1,
    },
    RequestRegistered {
        session_id: String,
        registration: DurableRequestRegistrationV1,
    },
    PlatformBucketApplied {
        session_id: String,
        bucket: DurableMarketBucketReferenceV1,
    },
    ExecutionAdvanced {
        session_id: String,
    },
    PrivateOutputPrepared {
        session_id: String,
        delivery: SealedPrivateDeliveryV1,
    },
    PrivateOutputConfirmed {
        session_id: String,
        confirmation: ResponseDeliveryConfirmationV1,
    },
    IngressClosed {
        session_id: String,
    },
    DownstreamDurabilityAdvanced {
        session_id: String,
        proof: DownstreamDurabilityProofV1,
    },
    RetentionPrepared {
        session_id: String,
        scope: QueryRetentionScopeV1,
        root_sequence_floor: Option<u64>,
        retained_from_utc_ms: Option<i64>,
    },
    MarketHotStateEvicted {
        session_id: String,
        condition_id: String,
        terminal_orders: BTreeMap<String, OrderProjectionV1>,
        terminal_trades: BTreeMap<String, TradeProjectionV1>,
        execution_witness: Box<TerminalExecutionMarketEvictionV1>,
    },
    SessionRetentionExpired {
        session_id: String,
    },
    SessionCleanRunCompleted {
        session_id: String,
    },
    ResourceLimitReached {
        session_id: Option<String>,
        limit: ResourceLimitReachedV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableTransactionEvidenceV1 {
    schema_version: String,
    fact: DurableTransactionFactV1,
    query_projection_revision: Option<u64>,
    logical_platform_driven_through_utc_ms: Option<i64>,
    private_fact_schedule_closed_through_utc_ms: Option<i64>,
    account_deltas: Vec<PlatformAccountDeltaV1>,
    logical_action_results: Vec<DurableLogicalActionResultV1>,
    private_output_drafts: Vec<DurablePrivateOutputDraftV1>,
    transient_input_session_id: Option<String>,
    transient_input: ResourceQuantityV1,
    resource_usage: Option<SimulationResourceUsageV1>,
}

impl DurableTransactionEvidenceV1 {
    fn new(fact: DurableTransactionFactV1) -> Self {
        Self {
            schema_version: DURABLE_TRANSACTION_EVIDENCE_SCHEMA_V1.to_owned(),
            fact,
            query_projection_revision: None,
            logical_platform_driven_through_utc_ms: None,
            private_fact_schedule_closed_through_utc_ms: None,
            account_deltas: Vec::new(),
            logical_action_results: Vec::new(),
            private_output_drafts: Vec::new(),
            transient_input_session_id: None,
            transient_input: ResourceQuantityV1::default(),
            resource_usage: None,
        }
    }

    fn session_id(&self) -> Option<&str> {
        match &self.fact {
            DurableTransactionFactV1::RunCreated { .. }
            | DurableTransactionFactV1::RunArtifactInputsSealed { .. }
            | DurableTransactionFactV1::ComponentGoldenMeasurementRecorded { .. } => None,
            DurableTransactionFactV1::SessionCreated { session_spec, .. } => {
                Some(&session_spec.session_id)
            }
            DurableTransactionFactV1::SessionActivated { request, .. } => Some(&request.session_id),
            DurableTransactionFactV1::SessionInitializationFailed { request, .. } => {
                Some(&request.session_id)
            }
            DurableTransactionFactV1::MarketEnrolled { session_id, .. }
            | DurableTransactionFactV1::RequestRegistered { session_id, .. }
            | DurableTransactionFactV1::PlatformBucketApplied { session_id, .. }
            | DurableTransactionFactV1::ExecutionAdvanced { session_id, .. }
            | DurableTransactionFactV1::PrivateOutputPrepared { session_id, .. }
            | DurableTransactionFactV1::PrivateOutputConfirmed { session_id, .. }
            | DurableTransactionFactV1::IngressClosed { session_id, .. }
            | DurableTransactionFactV1::DownstreamDurabilityAdvanced { session_id, .. }
            | DurableTransactionFactV1::RetentionPrepared { session_id, .. }
            | DurableTransactionFactV1::MarketHotStateEvicted { session_id, .. }
            | DurableTransactionFactV1::SessionRetentionExpired { session_id }
            | DurableTransactionFactV1::SessionCleanRunCompleted { session_id } => Some(session_id),
            DurableTransactionFactV1::ResourceLimitReached { session_id, .. } => {
                session_id.as_deref()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableRunCheckpointV1 {
    schema_version: String,
    execution_contract_version: String,
    market_event_source_identity_sha256: Sha256Hex,
    run_intent_binding: SimulationRunIntentBindingV1,
    resource_policy: SimulationResourcePolicyV1,
    resource_policy_sha256: Sha256Hex,
    component_golden_measurement_sha256: Option<Sha256Hex>,
    resource_high_water: SimulationResourceHighWaterV1,
    run_resource_admission: ResourceAdmissionStateV1,
    session_resource_admission: BTreeMap<String, ResourceAdmissionStateV1>,
    lifecycle: DurableRunLifecycleV1,
    process_root: SimProcessRootCheckpointV1,
    sessions: BTreeMap<String, ExecutionSessionCheckpointV1>,
}

type DurableEvictedMarketStateV1 = (
    BTreeMap<String, OrderProjectionV1>,
    BTreeMap<String, TradeProjectionV1>,
    TerminalExecutionMarketEvictionV1,
);

type DurableEvictedMarketsBySessionV1 =
    BTreeMap<String, BTreeMap<String, DurableEvictedMarketStateV1>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableSessionCompletionKindV1 {
    PaperRetentionExpired,
    CleanRunCompleted,
}

#[derive(Default)]
struct DurableEvidenceProjectionV1 {
    retain_retired_audit_state: bool,
    run_id: Option<String>,
    market_event_source_identity_sha256: Option<Sha256Hex>,
    run_intent_binding: Option<SimulationRunIntentBindingV1>,
    resource_policy: Option<SimulationResourcePolicyV1>,
    resource_policy_sha256: Option<Sha256Hex>,
    component_golden_measurement: Option<ComponentGoldenRunMeasurementV1>,
    component_golden_measurement_sha256: Option<Sha256Hex>,
    run_resource_admission: ResourceAdmissionStateV1,
    session_resource_admission: BTreeMap<String, ResourceAdmissionStateV1>,
    resource_high_water: SimulationResourceHighWaterV1,
    lifecycle: Option<DurableRunLifecycleV1>,
    session_creations: BTreeMap<
        String,
        (
            SessionSpecV2,
            AccountOpeningAnchorV1,
            SessionInitializationSnapshotV1,
        ),
    >,
    initialization_results: BTreeMap<String, SessionInitializationSnapshotV1>,
    market_enrollments: BTreeMap<String, Vec<MarketEnrollmentV1>>,
    requests: BTreeMap<String, BTreeMap<String, DurableRequestRegistrationV1>>,
    last_platform_buckets:
        BTreeMap<String, BTreeMap<MarketEventPathV1, DurableMarketBucketReferenceV1>>,
    account_deltas: BTreeMap<String, BTreeMap<u64, PlatformAccountDeltaV1>>,
    logical_action_results: BTreeMap<String, BTreeMap<String, DurableLogicalActionResultV1>>,
    logical_action_result_contexts:
        BTreeMap<String, BTreeMap<String, DurableLogicalActionRecoveryContextV1>>,
    prepared_outputs: BTreeMap<String, BTreeMap<u64, SealedPrivateDeliveryV1>>,
    confirmed_outputs: BTreeMap<String, BTreeMap<u64, ResponseDeliveryConfirmationV1>>,
    private_output_drafts: BTreeMap<String, BTreeMap<String, DurablePrivateOutputDraftV1>>,
    downstream_durable: BTreeMap<String, BTreeMap<u64, DownstreamDurabilityProofV1>>,
    closed_ingress_sessions: BTreeSet<String>,
    query_projection_revisions: BTreeMap<String, u64>,
    logical_platform_driven_through_by_session: BTreeMap<String, i64>,
    private_fact_schedule_closed_through_by_session: BTreeMap<String, i64>,
    query_retention: BTreeMap<String, (u64, Option<i64>, Option<i64>)>,
    evicted_markets: DurableEvictedMarketsBySessionV1,
    completed_sessions: BTreeMap<String, DurableSessionCompletionKindV1>,
}

#[derive(Default)]
struct ArtifactEvidenceIndexV1 {
    session_created: BTreeMap<String, SimulationEvidenceReferenceV1>,
    initialization_terminal: BTreeMap<String, SimulationEvidenceReferenceV1>,
    enrollments: BTreeMap<String, Vec<(MarketEnrollmentV1, SimulationEvidenceReferenceV1)>>,
    requests: BTreeMap<String, BTreeMap<String, SimulationEvidenceReferenceV1>>,
    logical_actions: BTreeMap<
        String,
        BTreeMap<String, Vec<(DurableLogicalActionResultV1, SimulationEvidenceReferenceV1)>>,
    >,
    buckets: BTreeMap<
        String,
        BTreeMap<
            MarketEventPathV1,
            (
                DurableMarketBucketReferenceV1,
                SimulationEvidenceReferenceV1,
            ),
        >,
    >,
    account_deltas:
        BTreeMap<String, BTreeMap<u64, (PlatformAccountDeltaV1, SimulationEvidenceReferenceV1)>>,
    prepared_outputs:
        BTreeMap<String, BTreeMap<u64, (SealedPrivateDeliveryV1, SimulationEvidenceReferenceV1)>>,
    confirmed_outputs: BTreeMap<
        String,
        BTreeMap<
            u64,
            (
                ResponseDeliveryConfirmationV1,
                SimulationEvidenceReferenceV1,
            ),
        >,
    >,
    downstream_durable: BTreeMap<
        String,
        BTreeMap<u64, (DownstreamDurabilityProofV1, SimulationEvidenceReferenceV1)>,
    >,
    evicted_markets: BTreeMap<String, BTreeMap<String, SimulationEvidenceReferenceV1>>,
    completed_sessions: BTreeMap<
        String,
        (
            artifacts::SimulationSessionCompletionKindV1,
            SimulationEvidenceReferenceV1,
        ),
    >,
}

impl ArtifactEvidenceIndexV1 {
    fn apply(
        &mut self,
        evidence: &RecoveredEvidenceV1<DurableTransactionEvidenceV1>,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let reference = SimulationEvidenceReferenceV1 {
            sequence: evidence.sequence,
            file_sha256: evidence.content_sha256.clone(),
        };
        match &evidence.body.fact {
            DurableTransactionFactV1::SessionCreated { session_spec, .. } => {
                self.session_created
                    .insert(session_spec.session_id.clone(), reference.clone());
            }
            DurableTransactionFactV1::SessionActivated { request, .. } => {
                self.initialization_terminal
                    .insert(request.session_id.clone(), reference.clone());
            }
            DurableTransactionFactV1::SessionInitializationFailed { request, .. } => {
                self.initialization_terminal
                    .insert(request.session_id.clone(), reference.clone());
            }
            DurableTransactionFactV1::MarketEnrolled {
                session_id,
                enrollment,
            } => self
                .enrollments
                .entry(session_id.clone())
                .or_default()
                .push((enrollment.clone(), reference.clone())),
            DurableTransactionFactV1::RequestRegistered {
                session_id,
                registration,
            } => {
                self.requests.entry(session_id.clone()).or_default().insert(
                    registration.request.context().global_request_id.clone(),
                    reference.clone(),
                );
            }
            DurableTransactionFactV1::PlatformBucketApplied { session_id, bucket } => {
                self.buckets
                    .entry(session_id.clone())
                    .or_default()
                    .insert(bucket.path, (bucket.clone(), reference.clone()));
            }
            DurableTransactionFactV1::PrivateOutputPrepared {
                session_id,
                delivery,
            } => {
                self.prepared_outputs
                    .entry(session_id.clone())
                    .or_default()
                    .insert(
                        delivery.confirmation().private_feedback_sequence,
                        (delivery.clone(), reference.clone()),
                    );
            }
            DurableTransactionFactV1::PrivateOutputConfirmed {
                session_id,
                confirmation,
            } => {
                self.confirmed_outputs
                    .entry(session_id.clone())
                    .or_default()
                    .insert(
                        confirmation.private_feedback_sequence,
                        (confirmation.clone(), reference.clone()),
                    );
            }
            DurableTransactionFactV1::DownstreamDurabilityAdvanced { session_id, proof } => {
                self.downstream_durable
                    .entry(session_id.clone())
                    .or_default()
                    .insert(
                        proof.durable_through.private_feedback_sequence,
                        (proof.clone(), reference.clone()),
                    );
            }
            DurableTransactionFactV1::MarketHotStateEvicted {
                session_id,
                condition_id,
                ..
            } => {
                self.evicted_markets
                    .entry(session_id.clone())
                    .or_default()
                    .insert(condition_id.clone(), reference.clone());
            }
            DurableTransactionFactV1::SessionRetentionExpired { session_id } => {
                self.completed_sessions.insert(
                    session_id.clone(),
                    (
                        artifacts::SimulationSessionCompletionKindV1::PaperRetentionExpired,
                        reference.clone(),
                    ),
                );
            }
            DurableTransactionFactV1::SessionCleanRunCompleted { session_id } => {
                self.completed_sessions.insert(
                    session_id.clone(),
                    (
                        artifacts::SimulationSessionCompletionKindV1::CleanRunCompleted,
                        reference.clone(),
                    ),
                );
            }
            DurableTransactionFactV1::RunCreated { .. }
            | DurableTransactionFactV1::RunArtifactInputsSealed { .. }
            | DurableTransactionFactV1::ComponentGoldenMeasurementRecorded { .. }
            | DurableTransactionFactV1::ExecutionAdvanced { .. }
            | DurableTransactionFactV1::IngressClosed { .. }
            | DurableTransactionFactV1::ResourceLimitReached { .. }
            | DurableTransactionFactV1::RetentionPrepared { .. } => {}
        }
        if let Some(session_id) = evidence.body.session_id() {
            for delta in &evidence.body.account_deltas {
                self.account_deltas
                    .entry(session_id.to_owned())
                    .or_default()
                    .insert(
                        delta.account_transition_seq,
                        (delta.clone(), reference.clone()),
                    );
            }
            for result in &evidence.body.logical_action_results {
                self.logical_actions
                    .entry(session_id.to_owned())
                    .or_default()
                    .entry(result.global_request_id.clone())
                    .or_default()
                    .push((result.clone(), reference.clone()));
            }
        }
        Ok(())
    }
}

fn validate_run_artifact_seal_readiness(
    process_root: &SimProcessRoot,
    checkpoint: &DurableRunCheckpointV1,
    evidence_projection: &DurableEvidenceProjectionV1,
) -> Result<(), artifacts::SimulationRunArtifactErrorV1> {
    validate_run_intent_binding_for_rules(
        &checkpoint.run_intent_binding,
        process_root.rule_snapshot().as_ref(),
    )?;
    let component_measurement_required = checkpoint.run_intent_binding.workload_kind
        == artifacts::SimulationWorkloadKindV1::ComponentGolden;
    if component_measurement_required != evidence_projection.component_golden_measurement.is_some()
        || evidence_projection.component_golden_measurement_sha256
            != checkpoint.component_golden_measurement_sha256
    {
        return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
            "COMPONENT_GOLDEN 运行缺少与最终检查点一致的封口前测量正文",
        ));
    }
    if evidence_projection.run_id.as_deref() != Some(process_root.run_id())
        || evidence_projection.session_creations.len()
            != process_root.capital_issuance_fence_count()
        || evidence_projection.session_creations.is_empty()
    {
        return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
            "运行、会话创建事实与资本签发围栏不一致".to_owned(),
        ));
    }
    for (session_id, (spec, _, created_initialization)) in &evidence_projection.session_creations {
        let terminal = evidence_projection
            .initialization_results
            .get(session_id)
            .unwrap_or(created_initialization);
        let completion_kind = evidence_projection.completed_sessions.get(session_id);
        let completion_matches_role = matches!(
            (spec.caller_role, completion_kind),
            (
                SimCallerRoleV1::Paper,
                Some(DurableSessionCompletionKindV1::PaperRetentionExpired)
            ) | (
                SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation,
                Some(DurableSessionCompletionKindV1::CleanRunCompleted)
            )
        );
        match terminal.state {
            SessionInitializationStateV1::Active
                if completion_matches_role && !checkpoint.sessions.contains_key(session_id) => {}
            SessionInitializationStateV1::InitializationFailedClosed
                if completion_kind.is_none() && checkpoint.sessions.contains_key(session_id) => {}
            SessionInitializationStateV1::PreparedLedgerInit => {
                return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                    "仍有未完成初始化决定的会话",
                ));
            }
            SessionInitializationStateV1::Active => {
                return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                    "仍有尚未退休的活动会话",
                ));
            }
            SessionInitializationStateV1::InitializationFailedClosed => {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                    "失败关闭会话的角色或活动检查点集合不一致：{session_id}"
                )));
            }
        }

        if terminal.state == SessionInitializationStateV1::Active {
            let buckets = evidence_projection.last_platform_buckets.get(session_id);
            let public_input_closed = matches!(
                buckets.and_then(|buckets| buckets.get(&MarketEventPathV1::PublicVisibility)),
                Some(DurableMarketBucketReferenceV1 {
                    closure: MillisecondBucketClosureV1::EndOfInput,
                    ..
                })
            );
            let hidden_input_closed = matches!(
                buckets.and_then(|buckets| buckets.get(&MarketEventPathV1::HiddenPlatform)),
                Some(DurableMarketBucketReferenceV1 {
                    closure: MillisecondBucketClosureV1::EndOfInput,
                    ..
                })
            );
            if !public_input_closed
                || (spec.caller_role != SimCallerRoleV1::Paper && !hidden_input_closed)
            {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                    "活动完成会话的适用输入路径没有由耐久 EOF 事实闭合：{session_id}"
                )));
            }
            let mut expected_market_ids = process_root
                .scenario()
                .market_set()
                .markets()
                .iter()
                .map(|market| market.market().condition_id.0.clone())
                .collect::<BTreeSet<_>>();
            for enrollment in evidence_projection
                .market_enrollments
                .get(session_id)
                .into_iter()
                .flat_map(|enrollments| enrollments.iter())
            {
                expected_market_ids.insert(enrollment.market.condition_id.0.clone());
            }
            let evicted_market_ids = evidence_projection
                .evicted_markets
                .get(session_id)
                .map(|markets| markets.keys().cloned().collect::<BTreeSet<_>>())
                .unwrap_or_default();
            if evicted_market_ids != expected_market_ids {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                    "活动完成会话的全部登记市场没有由耐久驱逐事实闭合：{session_id}"
                )));
            }
            let confirmed = evidence_projection
                .confirmed_outputs
                .get(session_id)
                .map(BTreeMap::len)
                .unwrap_or_default();
            let prepared = evidence_projection
                .prepared_outputs
                .get(session_id)
                .map(BTreeMap::len)
                .unwrap_or_default();
            let downstream = evidence_projection
                .downstream_durable
                .get(session_id)
                .map(BTreeMap::len)
                .unwrap_or_default();
            if prepared != confirmed || confirmed != downstream {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                    "活动完成会话的私有输出与强下游耐久前缀没有闭合：{session_id}"
                )));
            }
            if spec.caller_role == SimCallerRoleV1::Paper
                && !paper_query_retention_is_fully_pruned(session_id, evidence_projection)?
            {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                    "模拟盘完成会话的查询保留水位没有完全裁剪：{session_id}"
                )));
            }
        }
    }
    for (session_id, session_checkpoint) in &checkpoint.sessions {
        let evicted_markets = evidence_projection
            .evicted_markets
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        let (state, downstream) = ExecutionSessionStateV1::restore(
            process_root,
            session_checkpoint.clone(),
            &evicted_markets,
        )
        .map_err(|error| artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string()))?;
        let initialization = state.session.initialization_snapshot().map_err(|error| {
            artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
        })?;
        if initialization.state != SessionInitializationStateV1::InitializationFailedClosed
            || !state.session.is_quiescent_for_retention()
            || !state.pending_requests.is_empty()
            || !state.mutation_payloads.is_empty()
            || !state.submission_effect_records.is_empty()
            || !state.cancel_effect_records.is_empty()
            || state.query_state != QueryStateV1::new()
            || !state.direct_response_outbox.entries.is_empty()
            || !state.lifecycle_outbox.entries.is_empty()
            || !state.lifecycle_outbox.batches.is_empty()
            || !state.compacted_confirmed_private_deliveries.is_empty()
            || !state.confirmed_private_deliveries.is_empty()
            || downstream.is_some()
            || state.session.private_feedback_sequence() != 0
            || evidence_projection
                .market_enrollments
                .get(session_id)
                .is_some_and(|enrollments| !enrollments.is_empty())
            || evidence_projection
                .last_platform_buckets
                .get(session_id)
                .is_some_and(|buckets| !buckets.is_empty())
            || evidence_projection
                .requests
                .get(session_id)
                .is_some_and(|requests| !requests.is_empty())
        {
            return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                "失败关闭会话仍含业务、输出、查询或结算状态",
            ));
        }
    }
    Ok(())
}

fn paper_query_retention_is_fully_pruned(
    session_id: &str,
    evidence_projection: &DurableEvidenceProjectionV1,
) -> Result<bool, artifacts::SimulationRunArtifactErrorV1> {
    let Some((root_floor, Some(order_floor), Some(trade_floor))) =
        evidence_projection.query_retention.get(session_id).copied()
    else {
        return Ok(false);
    };
    let registrations = evidence_projection.requests.get(session_id);
    let initial_page_count = evidence_projection
        .logical_action_results
        .get(session_id)
        .into_iter()
        .flat_map(BTreeMap::values)
        .filter(|result| {
            let request = registrations
                .and_then(|registrations| registrations.get(&result.global_request_id))
                .map(|registration| &registration.request);
            matches!(
                (request, &result.part),
                (
                    Some(PendingDomainRequestV1::Orders(OrdersRequestV1 {
                        cursor: None,
                        ..
                    })),
                    DirectResponsePartV1::Orders {
                        result: OrdersQueryResultV1::Page(_),
                        ..
                    }
                ) | (
                    Some(PendingDomainRequestV1::Trades(TradesRequestV1 {
                        cursor: None,
                        ..
                    })),
                    DirectResponsePartV1::Trades {
                        result: TradesQueryResultV1::Page(_),
                        ..
                    }
                )
            )
        })
        .count();
    let expected_next_root_sequence = u64::try_from(initial_page_count)
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| {
            artifacts::SimulationRunArtifactErrorV1::Integrity(format!(
                "模拟盘查询根数量溢出：{session_id}"
            ))
        })?;
    if root_floor != expected_next_root_sequence {
        return Ok(false);
    }
    Ok(evidence_projection
        .evicted_markets
        .get(session_id)
        .into_iter()
        .flat_map(BTreeMap::values)
        .all(|(orders, trades, _)| {
            orders
                .values()
                .all(|order| order.last_updated_at_utc_ms < order_floor)
                && trades
                    .values()
                    .all(|trade| trade.matched_at_utc_ms < trade_floor)
        }))
}

fn validate_artifact_publication_recovery_eligibility(
    evidence_projection: &DurableEvidenceProjectionV1,
) -> Result<(), artifacts::SimulationRunArtifactErrorV1> {
    for (session_id, (spec, _, created_initialization)) in &evidence_projection.session_creations {
        if spec.caller_role == SimCallerRoleV1::Paper {
            continue;
        }
        let terminal = evidence_projection
            .initialization_results
            .get(session_id)
            .unwrap_or(created_initialization);
        if terminal.state != SessionInitializationStateV1::Active
            || evidence_projection.completed_sessions.get(session_id)
                != Some(&DurableSessionCompletionKindV1::CleanRunCompleted)
        {
            return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                "非模拟盘运行只有 clean run 完成后才能跨进程恢复产物发布",
            ));
        }
    }
    Ok(())
}

fn validate_run_intent_binding_for_rules(
    binding: &SimulationRunIntentBindingV1,
    rules: &SealedProcessRuleSnapshot,
) -> Result<(), artifacts::SimulationRunArtifactErrorV1> {
    binding.validate()?;
    if binding.implementation_identity != rules.snapshot().provenance.implementation_identity {
        return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
            "运行意图实现身份与实际规则快照 provenance 不一致".to_owned(),
        ));
    }
    Ok(())
}

struct PreparedRunArtifactBundleV1 {
    manifest_sha256: Sha256Hex,
    through_commit_sequence: u64,
    evidence_head_sha256: Sha256Hex,
    members: Vec<ImmutableArtifactMemberV1>,
}

fn artifact_sha256(bytes: &[u8]) -> Sha256Hex {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("String 写入不会失败");
    }
    Sha256Hex(encoded)
}

fn artifact_json_bytes<T: Serialize>(
    value: &T,
    label: &'static str,
) -> Result<Vec<u8>, artifacts::SimulationRunArtifactErrorV1> {
    serde_json::to_vec(value).map_err(|error| {
        artifacts::SimulationRunArtifactErrorV1::Integrity(format!("编码{label}失败：{error}"))
    })
}

fn artifact_confirmation(
    confirmation: &ResponseDeliveryConfirmationV1,
) -> artifacts::SimulationDeliveryConfirmationAuditV1 {
    artifacts::SimulationDeliveryConfirmationAuditV1 {
        private_feedback_sequence: confirmation.private_feedback_sequence,
        delivery_id: confirmation.delivery_id.clone(),
        payload_sha256: confirmation.payload_sha256.clone(),
    }
}

fn build_session_artifact_audit(
    session_id: &str,
    projection: &DurableEvidenceProjectionV1,
    index: &ArtifactEvidenceIndexV1,
) -> Result<artifacts::SimulationSessionAuditV1, artifacts::SimulationRunArtifactErrorV1> {
    let integrity = |message: String| artifacts::SimulationRunArtifactErrorV1::Integrity(message);
    let (spec, opening_anchor, created_initialization) = projection
        .session_creations
        .get(session_id)
        .ok_or_else(|| integrity(format!("缺少会话创建事实：{session_id}")))?;
    let session_created_evidence = index
        .session_created
        .get(session_id)
        .cloned()
        .ok_or_else(|| integrity(format!("缺少会话创建证据引用：{session_id}")))?;
    let initialization = projection
        .initialization_results
        .get(session_id)
        .unwrap_or(created_initialization);
    let initialization_evidence = index.initialization_terminal.get(session_id).cloned();
    let lifecycle = match initialization.state {
        SessionInitializationStateV1::Active => {
            let activation_id = initialization
                .activation_id
                .clone()
                .ok_or_else(|| integrity(format!("活动会话缺少激活身份：{session_id}")))?;
            let activation_evidence = initialization_evidence
                .ok_or_else(|| integrity(format!("活动会话缺少激活证据：{session_id}")))?;
            let (completion_kind, completion_evidence) = index
                .completed_sessions
                .get(session_id)
                .cloned()
                .ok_or_else(|| integrity(format!("活动会话缺少完成证据：{session_id}")))?;
            if initialization.failure_id.is_some() {
                return Err(integrity(format!("活动会话携带失败身份：{session_id}")));
            }
            artifacts::SimulationSessionLifecycleV1::ActiveCompleted {
                activation_id,
                activation_evidence,
                completion_kind,
                completion_evidence,
            }
        }
        SessionInitializationStateV1::InitializationFailedClosed => {
            let failure_id = initialization
                .failure_id
                .clone()
                .ok_or_else(|| integrity(format!("失败关闭会话缺少失败身份：{session_id}")))?;
            let failure_evidence = initialization_evidence
                .ok_or_else(|| integrity(format!("失败关闭会话缺少失败证据：{session_id}")))?;
            if initialization.activation_id.is_some()
                || index.completed_sessions.contains_key(session_id)
            {
                return Err(integrity(format!(
                    "失败关闭会话携带激活或退休事实：{session_id}"
                )));
            }
            artifacts::SimulationSessionLifecycleV1::InitializationFailedClosed {
                failure_id,
                failure_evidence,
            }
        }
        SessionInitializationStateV1::PreparedLedgerInit => {
            return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                "仍有未完成初始化决定的会话",
            ));
        }
    };

    let delta_pairs = index
        .account_deltas
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let deltas = delta_pairs
        .iter()
        .map(|(sequence, (delta, _))| (*sequence, delta.clone()))
        .collect::<BTreeMap<_, _>>();
    if projection
        .account_deltas
        .get(session_id)
        .cloned()
        .unwrap_or_default()
        != deltas
    {
        return Err(integrity(format!(
            "账户变化证据索引与耐久投影不一致：{session_id}"
        )));
    }
    let account = PlatformAccountStateV1::replay_restored_delta_chain(opening_anchor, &deltas)
        .map_err(|error| integrity(format!("账户变化链不能独立重放：{session_id}:{error}")))?;
    let final_sequence = u64::try_from(deltas.len())
        .map_err(|_| integrity(format!("账户变化数量溢出：{session_id}")))?;
    let account_delta_head = delta_pairs.last_key_value().map(|(_, (delta, reference))| {
        artifacts::SimulationAccountDeltaHeadV1 {
            delta_id: delta.delta_id.clone(),
            delta_sha256: delta.delta_sha256.clone(),
            evidence: reference.clone(),
        }
    });

    let enrollment_pairs = index
        .enrollments
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let enrollments = enrollment_pairs
        .iter()
        .map(|(enrollment, _)| enrollment.clone())
        .collect::<Vec<_>>();
    if projection
        .market_enrollments
        .get(session_id)
        .cloned()
        .unwrap_or_default()
        != enrollments
    {
        return Err(integrity(format!(
            "市场登记证据索引与耐久投影不一致：{session_id}"
        )));
    }
    validate_durable_market_enrollment_chain(
        &enrollments,
        &opening_anchor.account_instance_key,
        opening_anchor,
    )
    .map_err(|error| integrity(format!("市场登记链不能独立重算：{session_id}:{error}")))?;
    let enrollment_count = u64::try_from(enrollment_pairs.len())
        .map_err(|_| integrity(format!("市场登记数量溢出：{session_id}")))?;
    let enrollment_head = enrollment_pairs.last().map(|(enrollment, reference)| {
        artifacts::SimulationEnrollmentChainHeadV1 {
            enrollment_id: enrollment.enrollment_id.clone(),
            enrollment_sha256: enrollment.enrollment_sha256.clone(),
            chain_sha256: enrollment.enrollment_chain_sha256.clone(),
            evidence: reference.clone(),
        }
    });

    let registrations = projection
        .requests
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let registration_references = index.requests.get(session_id).cloned().unwrap_or_default();
    let prepared = index
        .prepared_outputs
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let confirmed = index
        .confirmed_outputs
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let downstream = index
        .downstream_durable
        .get(session_id)
        .cloned()
        .unwrap_or_default();
    let prepared_keys = prepared.keys().copied().collect::<BTreeSet<_>>();
    let confirmed_keys = confirmed.keys().copied().collect::<BTreeSet<_>>();
    let downstream_keys = downstream.keys().copied().collect::<BTreeSet<_>>();
    let confirmed_through = u64::try_from(prepared.len())
        .map_err(|_| integrity(format!("反馈数量溢出：{session_id}")))?;
    let expected_feedback_keys = (1..=confirmed_through).collect::<BTreeSet<_>>();
    if prepared_keys != expected_feedback_keys
        || confirmed_keys != expected_feedback_keys
        || downstream_keys != expected_feedback_keys
    {
        return Err(integrity(format!(
            "准备、确认与强下游反馈前缀没有完整闭合：{session_id}"
        )));
    }
    let mut deliveries = BTreeMap::new();
    let mut direct_delivery_by_request = BTreeMap::new();
    for sequence in 1..=confirmed_through {
        let (delivery, prepared_evidence) =
            prepared.get(&sequence).expect("已核对准备输出连续前缀");
        let (confirmation, confirmed_evidence) =
            confirmed.get(&sequence).expect("已核对确认输出连续前缀");
        let (proof, downstream_evidence) =
            downstream.get(&sequence).expect("已核对下游证明连续前缀");
        if delivery.confirmation() != *confirmation
            || proof.durable_through != *confirmation
            || delivery
                .recompute_payload_sha256()
                .map_err(|error| integrity(error.to_string()))?
                != confirmation.payload_sha256
        {
            return Err(integrity(format!(
                "私有输出正文、确认或强下游证明不一致：{session_id}:{sequence}"
            )));
        }
        let kind = match delivery {
            SealedPrivateDeliveryV1::Direct(response) => {
                let request_id = response.metadata().global_request_id.clone();
                if direct_delivery_by_request
                    .insert(request_id, sequence)
                    .is_some()
                {
                    return Err(integrity(format!(
                        "请求对应多个直接响应：{session_id}:{sequence}"
                    )));
                }
                artifacts::SimulationPrivateDeliveryKindV1::Direct
            }
            SealedPrivateDeliveryV1::Lifecycle(_) => {
                artifacts::SimulationPrivateDeliveryKindV1::Lifecycle
            }
        };
        deliveries.insert(
            sequence,
            artifacts::SimulationPrivateDeliveryAuditV1 {
                kind,
                confirmation: artifact_confirmation(confirmation),
                prepared_evidence: prepared_evidence.clone(),
                confirmed_evidence: confirmed_evidence.clone(),
                downstream_durable_evidence: downstream_evidence.clone(),
            },
        );
    }

    let mut requests = BTreeMap::new();
    for (request_id, registration) in &registrations {
        let registration_evidence = registration_references
            .get(request_id)
            .cloned()
            .ok_or_else(|| integrity(format!("请求缺少登记证据引用：{request_id}")))?;
        let expected_actions = registration
            .actions
            .iter()
            .map(|action| {
                (
                    action.action_identity.clone(),
                    action.key.batch_item_ordinal,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut logical_actions = index
            .logical_actions
            .get(session_id)
            .and_then(|by_request| by_request.get(request_id))
            .cloned()
            .unwrap_or_default();
        logical_actions.sort_by_key(|(result, _)| result.batch_item_ordinal);
        let actual_actions = logical_actions
            .iter()
            .map(|(result, _)| (result.action_identity.clone(), result.batch_item_ordinal))
            .collect::<BTreeMap<_, _>>();
        if actual_actions != expected_actions
            || logical_actions.iter().any(|(result, _)| {
                result.global_request_id != *request_id
                    || result.part.ordinal() != usize::from(result.batch_item_ordinal)
                    || !result
                        .part
                        .method_compatible(registration.request.direct_response_method())
            })
        {
            return Err(integrity(format!(
                "请求的耐久逻辑动作结果没有精确闭合：{request_id}"
            )));
        }
        let direct_delivery_sequence = direct_delivery_by_request
            .remove(request_id)
            .ok_or_else(|| integrity(format!("请求缺少最终直接响应：{request_id}")))?;
        requests.insert(
            request_id.clone(),
            artifacts::SimulationRequestClosureV1 {
                global_request_id: request_id.clone(),
                registration_evidence,
                logical_actions: logical_actions
                    .into_iter()
                    .map(
                        |(result, evidence)| artifacts::SimulationLogicalActionAuditV1 {
                            action_identity: result.action_identity,
                            batch_item_ordinal: result.batch_item_ordinal,
                            evidence,
                        },
                    )
                    .collect(),
                direct_delivery_sequence,
            },
        );
    }
    if !direct_delivery_by_request.is_empty()
        || registration_references.keys().collect::<BTreeSet<_>>()
            != registrations.keys().collect::<BTreeSet<_>>()
    {
        return Err(integrity(format!(
            "直接响应或请求登记引用存在孤立成员：{session_id}"
        )));
    }

    let buckets = index.buckets.get(session_id).cloned().unwrap_or_default();
    let mut input_ranges = Vec::new();
    for path in [
        MarketEventPathV1::HiddenPlatform,
        MarketEventPathV1::PublicVisibility,
    ] {
        let last_bucket =
            buckets.get(&path).map(
                |(bucket, evidence)| artifacts::SimulationLastInputBucketV1 {
                    bucket_at_utc_ms: bucket.bucket_at_utc_ms,
                    start_index_position: bucket.start_index_position,
                    next_index_position: bucket.next_index_position,
                    bucket_sha256: bucket.bucket_sha256.clone(),
                    closure: match bucket.closure {
                        MillisecondBucketClosureV1::NextEventAt(next_event_at_utc_ms) => {
                            artifacts::SimulationInputClosureV1::NextEventAt {
                                next_event_at_utc_ms,
                            }
                        }
                        MillisecondBucketClosureV1::EndOfInput => {
                            artifacts::SimulationInputClosureV1::EndOfInput
                        }
                    },
                    evidence: evidence.clone(),
                },
            );
        if last_bucket.is_some()
            || (path == MarketEventPathV1::PublicVisibility
                && spec.caller_role == SimCallerRoleV1::Paper)
        {
            input_ranges.push(artifacts::SimulationInputRangeV1 {
                path: match path {
                    MarketEventPathV1::HiddenPlatform => {
                        artifacts::SimulationInputPathV1::HiddenPlatform
                    }
                    MarketEventPathV1::PublicVisibility => {
                        artifacts::SimulationInputPathV1::PublicVisibility
                    }
                },
                next_index_position: last_bucket
                    .as_ref()
                    .map_or(0, |bucket| bucket.next_index_position),
                last_bucket,
            });
        }
    }
    let (
        query_root_sequence_floor,
        order_history_retained_from_utc_ms,
        trade_history_retained_from_utc_ms,
    ) = projection
        .query_retention
        .get(session_id)
        .copied()
        .unwrap_or((1, None, None));
    let downstream_confirmation = downstream
        .last_key_value()
        .map(|(_, (proof, _))| artifact_confirmation(&proof.durable_through));
    Ok(artifacts::SimulationSessionAuditV1 {
        session_id: session_id.to_owned(),
        session_created_evidence,
        account: artifacts::SimulationAccountAuditV1 {
            account_instance_key: opening_anchor.account_instance_key.clone(),
            opening_anchor_id: opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: opening_anchor.anchor_sha256.clone(),
            final_account_transition_seq: final_sequence,
            final_account_summary_sha256: account.account_summary_sha256().clone(),
            account_delta_count: final_sequence,
            account_delta_head,
        },
        creation_request_id: spec.creation_request_id.clone(),
        response_bridge_binding_id: spec.response_bridge_binding_id.clone(),
        session_seed_sha256: spec.session_seed.clone(),
        enrollment_count,
        enrollment_head,
        requests,
        input_ranges,
        feedback: artifacts::SimulationFeedbackAuditV1 {
            next_private_feedback_sequence: confirmed_through
                .checked_add(1)
                .ok_or_else(|| integrity(format!("反馈下一序号溢出：{session_id}")))?,
            process_confirmed_through: confirmed_through,
            downstream_durable_through: confirmed_through,
            downstream_durable_confirmation: downstream_confirmation,
            deliveries,
        },
        query_projection_revision: projection
            .query_projection_revisions
            .get(session_id)
            .copied()
            .unwrap_or(0),
        query_root_sequence_floor,
        order_history_retained_from_utc_ms,
        trade_history_retained_from_utc_ms,
        evicted_market_evidence: index
            .evicted_markets
            .get(session_id)
            .cloned()
            .unwrap_or_default(),
        lifecycle,
    })
}

fn prepare_run_artifact_bundle(
    process_root: &SimProcessRoot,
    recovered: &RecoveredDurabilityV1<DurableTransactionEvidenceV1, DurableRunCheckpointV1>,
    resource_high_water: &SimulationResourceHighWaterV1,
) -> Result<PreparedRunArtifactBundleV1, artifacts::SimulationRunArtifactErrorV1> {
    let integrity = |message: String| artifacts::SimulationRunArtifactErrorV1::Integrity(message);
    let mut projection = DurableEvidenceProjectionV1::for_artifacts();
    let mut index = ArtifactEvidenceIndexV1::default();
    for evidence in &recovered.evidence {
        if evidence.body.schema_version != DURABLE_TRANSACTION_EVIDENCE_SCHEMA_V1 {
            return Err(integrity(format!(
                "耐久证据 schema 不受支持：{}",
                evidence.body.schema_version
            )));
        }
        projection
            .apply(&evidence.body)
            .map_err(|error| integrity(error.to_string()))?;
        index
            .apply(evidence)
            .map_err(|error| integrity(error.to_string()))?;
    }
    if recovered.checkpoint.lifecycle != DurableRunLifecycleV1::ArtifactInputsSealed
        || projection.lifecycle != Some(DurableRunLifecycleV1::ArtifactInputsSealed)
        || projection.run_intent_binding.as_ref() != Some(&recovered.checkpoint.run_intent_binding)
        || projection.market_event_source_identity_sha256.as_ref()
            != Some(&recovered.checkpoint.market_event_source_identity_sha256)
    {
        return Err(integrity(
            "运行封口、意图或市场事件源事实与最终检查点不一致".to_owned(),
        ));
    }
    validate_run_artifact_seal_readiness(process_root, &recovered.checkpoint, &projection)?;
    let evidence_count = u64::try_from(recovered.evidence.len())
        .map_err(|_| integrity("证据数量超出 u64".to_owned()))?;
    if evidence_count != recovered.through_commit_sequence
        || recovered
            .evidence
            .last()
            .is_none_or(|evidence| evidence.content_sha256.0.is_empty())
    {
        return Err(integrity("证据前缀序号或链头非法".to_owned()));
    }
    let evidence_bytes = recovered
        .evidence
        .iter()
        .try_fold(0_u64, |total, evidence| {
            total
                .checked_add(evidence.envelope_byte_length)
                .ok_or_else(|| integrity("证据字节数溢出".to_owned()))
        })?;
    let evidence_head_sha256 = recovered
        .evidence
        .last()
        .expect("运行创建和封口至少产生两条证据")
        .content_sha256
        .clone();

    let mut sessions = Vec::with_capacity(projection.session_creations.len());
    for session_id in projection.session_creations.keys() {
        sessions.push(build_session_artifact_audit(
            session_id,
            &projection,
            &index,
        )?);
    }
    if let Some(measurement) = projection.component_golden_measurement.as_ref() {
        if measurement.trajectories.len() != sessions.len() {
            return Err(integrity("组件黄金测量轨迹与运行会话数量不一致".to_owned()));
        }
        for trajectory in measurement.trajectories.values() {
            let binding = &trajectory.runtime_binding;
            let (spec, opening_anchor, _) = projection
                .session_creations
                .get(&binding.session_id)
                .ok_or_else(|| {
                integrity(format!(
                    "组件黄金测量轨迹引用未知会话：{}",
                    binding.session_id
                ))
            })?;
            if binding.session_seed_sha256 != spec.session_seed
                || binding.response_bridge_binding_id != spec.response_bridge_binding_id
                || binding.account_instance_key != opening_anchor.account_instance_key
            {
                return Err(integrity(format!(
                    "组件黄金测量轨迹与耐久会话身份不一致：{}",
                    binding.session_id
                )));
            }
        }
    }
    let (capital_issuance_fence_count, capital_issuance_fence_sha256) = recovered
        .checkpoint
        .process_root
        .capital_issuance_fence_audit()
        .map_err(|error| integrity(error.to_string()))?;
    let request_count = projection
        .requests
        .values()
        .try_fold(0_u64, |total, requests| {
            total
                .checked_add(
                    u64::try_from(requests.len())
                        .map_err(|_| integrity("请求数量超出 u64".to_owned()))?,
                )
                .ok_or_else(|| integrity("请求数量溢出".to_owned()))
        })?;
    let logical_action_result_count =
        projection
            .logical_action_results
            .values()
            .try_fold(0_u64, |total, results| {
                total
                    .checked_add(
                        u64::try_from(results.len())
                            .map_err(|_| integrity("逻辑动作结果数量超出 u64".to_owned()))?,
                    )
                    .ok_or_else(|| integrity("逻辑动作结果数量溢出".to_owned()))
            })?;
    let account_delta_count =
        projection
            .account_deltas
            .values()
            .try_fold(0_u64, |total, deltas| {
                total
                    .checked_add(
                        u64::try_from(deltas.len())
                            .map_err(|_| integrity("账户变化数量超出 u64".to_owned()))?,
                    )
                    .ok_or_else(|| integrity("账户变化数量溢出".to_owned()))
            })?;
    let private_output_count =
        projection
            .prepared_outputs
            .values()
            .try_fold(0_u64, |total, outputs| {
                total
                    .checked_add(
                        u64::try_from(outputs.len())
                            .map_err(|_| integrity("私有输出数量超出 u64".to_owned()))?,
                    )
                    .ok_or_else(|| integrity("私有输出数量溢出".to_owned()))
            })?;
    let market_bucket_count = recovered
        .evidence
        .iter()
        .try_fold(0_u64, |total, evidence| {
            total
                .checked_add(u64::from(matches!(
                    evidence.body.fact,
                    DurableTransactionFactV1::PlatformBucketApplied { .. }
                )))
                .ok_or_else(|| integrity("市场桶数量溢出".to_owned()))
        })?;
    let maximum_account_transition_seq = projection
        .account_deltas
        .values()
        .filter_map(|deltas| deltas.last_key_value().map(|(sequence, _)| *sequence))
        .max()
        .unwrap_or(0);
    let maximum_private_feedback_sequence = projection
        .confirmed_outputs
        .values()
        .filter_map(|outputs| outputs.last_key_value().map(|(sequence, _)| *sequence))
        .max()
        .unwrap_or(0);
    let maximum_input_index_position = projection
        .last_platform_buckets
        .values()
        .flat_map(BTreeMap::values)
        .map(|bucket| bucket.next_index_position)
        .max()
        .unwrap_or(0);
    let maximum_query_projection_revision = projection
        .query_projection_revisions
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let audit = artifacts::SimulationRunAuditV1 {
        schema_version: artifacts::SIMULATION_RUN_AUDIT_SCHEMA_V1.to_owned(),
        run_id: process_root.run_id().to_owned(),
        through_commit_sequence: recovered.through_commit_sequence,
        sessions,
        capital_issuance_fence_count,
        capital_issuance_fence_sha256,
        component_golden_measurement: projection.component_golden_measurement.clone(),
        component_golden_measurement_sha256: projection.component_golden_measurement_sha256.clone(),
        resource_observation: artifacts::SimulationResourceObservationV1 {
            measurement_scope:
                artifacts::SimulationResourceObservationScopeV1::DurableLogicalObservationsOnly,
            resource_policy_id: recovered.checkpoint.resource_policy.policy_id.clone(),
            resource_policy_sha256: recovered.checkpoint.resource_policy_sha256.clone(),
            resource_high_water: resource_high_water.clone(),
            evidence_file_count: evidence_count,
            evidence_bytes,
            session_count: u64::try_from(projection.session_creations.len())
                .map_err(|_| integrity("会话数量超出 u64".to_owned()))?,
            request_count,
            logical_action_result_count,
            account_delta_count,
            private_output_count,
            market_bucket_count,
            maximum_account_transition_seq,
            maximum_private_feedback_sequence,
            maximum_input_index_position,
            maximum_query_projection_revision,
        },
        invariants: artifacts::SimulationInvariantReportV1 {
            evidence_predecessor_chain_verified: true,
            durable_projection_verified: true,
            account_delta_chains_verified: true,
            private_output_closure_verified: true,
            completion_facts_and_durable_closure_verified: true,
        },
    };
    let scenario = process_root.scenario();
    let reconstructed_scenario =
        PlatformScenarioV1::try_new(scenario.market_set().markets().to_vec())
            .map_err(|error| integrity(format!("场景正文不能独立重算：{error}")))?;
    if &reconstructed_scenario != scenario.as_ref() {
        return Err(integrity("进程场景正文与密封身份不一致".to_owned()));
    }
    let scenario_artifact = artifacts::SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        market_set_sha256: scenario.market_set_sha256().clone(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let audit_bytes = artifact_json_bytes(&audit, "运行审计正文")?;
    let scenario_bytes = artifact_json_bytes(&scenario_artifact, "场景正文")?;
    let finality_bytes = process_root
        .finality_policy()
        .canonical_content_json()
        .to_vec();
    let rule_bytes = process_root.rule_snapshot().file_bytes().to_vec();
    if artifact_sha256(&rule_bytes) != *process_root.rule_snapshot().file_sha256() {
        return Err(integrity("规则快照正文与文件 SHA-256 不一致".to_owned()));
    }
    let latency_bytes = process_root
        .latency_profile()
        .canonical_file_bytes()
        .to_vec();
    if artifact_sha256(&latency_bytes) != *process_root.latency_profile().file_sha256() {
        return Err(integrity(
            "代理时延配置档正文与文件 SHA-256 不一致".to_owned(),
        ));
    }
    let audit_sha256 = artifact_sha256(&audit_bytes);
    let scenario_file_sha256 = artifact_sha256(&scenario_bytes);
    let finality_file_sha256 = artifact_sha256(&finality_bytes);
    let rule_file_sha256 = artifact_sha256(&rule_bytes);
    let latency_file_sha256 = artifact_sha256(&latency_bytes);
    let mut artifact_members = vec![
        artifacts::SimulationArtifactMemberV1 {
            schema_version: artifacts::SIMULATION_RUN_AUDIT_SCHEMA_V1.to_owned(),
            relative_path: artifacts::SIMULATION_RUN_AUDIT_FILE_NAME_V1.to_owned(),
            content_identity: audit_sha256.clone(),
            file_sha256: audit_sha256.clone(),
            byte_length: u64::try_from(audit_bytes.len())
                .map_err(|_| integrity("运行审计正文字节数超出 u64".to_owned()))?,
        },
        artifacts::SimulationArtifactMemberV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            relative_path: artifacts::PLATFORM_SCENARIO_FILE_NAME_V1.to_owned(),
            content_identity: scenario.scenario_sha256().clone(),
            file_sha256: scenario_file_sha256,
            byte_length: u64::try_from(scenario_bytes.len())
                .map_err(|_| integrity("场景正文字节数超出 u64".to_owned()))?,
        },
        artifacts::SimulationArtifactMemberV1 {
            schema_version: crate::finality::TRADE_FINALITY_POLICY_SCHEMA_V1.to_owned(),
            relative_path: artifacts::TRADE_FINALITY_POLICY_FILE_NAME_V1.to_owned(),
            content_identity: process_root.finality_policy().content_sha256().clone(),
            file_sha256: finality_file_sha256,
            byte_length: u64::try_from(finality_bytes.len())
                .map_err(|_| integrity("最终性策略正文字节数超出 u64".to_owned()))?,
        },
        artifacts::SimulationArtifactMemberV1 {
            schema_version: process_root
                .rule_snapshot()
                .snapshot()
                .schema_version
                .clone(),
            relative_path: artifacts::PLATFORM_RULE_SNAPSHOT_FILE_NAME_V1.to_owned(),
            content_identity: process_root
                .rule_snapshot()
                .snapshot()
                .content_sha256
                .clone(),
            file_sha256: rule_file_sha256,
            byte_length: u64::try_from(rule_bytes.len())
                .map_err(|_| integrity("规则快照正文字节数超出 u64".to_owned()))?,
        },
        artifacts::SimulationArtifactMemberV1 {
            schema_version: crate::latency_profile::PROXY_LATENCY_PROFILE_SCHEMA_V1.to_owned(),
            relative_path: artifacts::PROXY_LATENCY_PROFILE_FILE_NAME_V1.to_owned(),
            content_identity: process_root.latency_profile().file_sha256().clone(),
            file_sha256: latency_file_sha256,
            byte_length: u64::try_from(latency_bytes.len())
                .map_err(|_| integrity("时延配置档正文字节数超出 u64".to_owned()))?,
        },
    ];
    artifact_members.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let intent_bytes =
        artifact_json_bytes(&recovered.checkpoint.run_intent_binding, "运行意图绑定")?;
    let manifest = artifacts::SimulationRunManifestV1 {
        schema_version: artifacts::SIMULATION_RUN_MANIFEST_SCHEMA_V1.to_owned(),
        run_id: process_root.run_id().to_owned(),
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        market_event_schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        order_lifecycle_feedback_schema_version: ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
        trade_lifecycle_feedback_schema_version: TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
        settlement_lifecycle_feedback_schema_version: SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1
            .to_owned(),
        run_intent_binding: recovered.checkpoint.run_intent_binding.clone(),
        run_intent_content_sha256: artifact_sha256(&intent_bytes),
        component_golden_measurement_sha256: recovered
            .checkpoint
            .component_golden_measurement_sha256
            .clone(),
        evidence_prefix: artifacts::SimulationEvidencePrefixV1 {
            run_root_relative_path: "evidence".to_owned(),
            through_commit_sequence: recovered.through_commit_sequence,
            evidence_count,
            evidence_bytes,
            evidence_head_sha256: evidence_head_sha256.clone(),
        },
        market_event_source_identity_sha256: recovered
            .checkpoint
            .market_event_source_identity_sha256
            .clone(),
        rule_snapshot_file_sha256: process_root.rule_snapshot().file_sha256().clone(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        scenario_market_set_sha256: scenario.market_set_sha256().clone(),
        finality_policy_id: process_root.finality_policy().policy_id().to_owned(),
        finality_policy_content_sha256: process_root.finality_policy().content_sha256().clone(),
        latency_profile_id: process_root.latency_profile().profile_id().to_owned(),
        latency_profile_version: process_root.latency_profile().profile_version(),
        latency_profile_file_sha256: process_root.latency_profile().file_sha256().clone(),
        latency_sampler_version: crate::latency_profile::PROXY_LATENCY_SAMPLER_VERSION_V1
            .to_owned(),
        latency_sampler_domains: vec![
            "REQUEST_OUTBOUND_V1".to_owned(),
            "DIRECT_RESPONSE_INBOUND_V1".to_owned(),
            "LIFECYCLE_FEEDBACK_INBOUND_V1".to_owned(),
        ],
        artifacts: artifact_members,
    };
    let manifest_bytes = artifact_json_bytes(&manifest, "运行清单")?;
    let manifest_sha256 = artifact_sha256(&manifest_bytes);
    let members = vec![
        ImmutableArtifactMemberV1 {
            file_name: artifacts::SIMULATION_RUN_MANIFEST_FILE_NAME_V1,
            bytes: manifest_bytes,
        },
        ImmutableArtifactMemberV1 {
            file_name: artifacts::SIMULATION_RUN_AUDIT_FILE_NAME_V1,
            bytes: audit_bytes,
        },
        ImmutableArtifactMemberV1 {
            file_name: artifacts::PLATFORM_SCENARIO_FILE_NAME_V1,
            bytes: scenario_bytes,
        },
        ImmutableArtifactMemberV1 {
            file_name: artifacts::TRADE_FINALITY_POLICY_FILE_NAME_V1,
            bytes: finality_bytes,
        },
        ImmutableArtifactMemberV1 {
            file_name: artifacts::PLATFORM_RULE_SNAPSHOT_FILE_NAME_V1,
            bytes: rule_bytes,
        },
        ImmutableArtifactMemberV1 {
            file_name: artifacts::PROXY_LATENCY_PROFILE_FILE_NAME_V1,
            bytes: latency_bytes,
        },
    ];
    Ok(PreparedRunArtifactBundleV1 {
        manifest_sha256,
        through_commit_sequence: recovered.through_commit_sequence,
        evidence_head_sha256,
        members,
    })
}

fn validate_durable_output_projection(
    session_id: &str,
    state: &ExecutionSessionStateV1,
    downstream_confirmation: Option<&ResponseDeliveryConfirmationV1>,
    prepared: Option<&BTreeMap<u64, SealedPrivateDeliveryV1>>,
    confirmed: Option<&BTreeMap<u64, ResponseDeliveryConfirmationV1>>,
    durable_drafts: Option<&BTreeMap<String, DurablePrivateOutputDraftV1>>,
    downstream: Option<&BTreeMap<u64, DownstreamDurabilityProofV1>>,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    let prepared = prepared.cloned().unwrap_or_default();
    let confirmed = confirmed.cloned().unwrap_or_default();
    let durable_drafts = durable_drafts.cloned().unwrap_or_default();
    let process_confirmed_through = state.session.private_feedback_sequence();
    let expected_sequences = (1..=process_confirmed_through).collect::<BTreeSet<_>>();
    if confirmed.keys().copied().collect::<BTreeSet<_>>() != expected_sequences {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "仅追加输出确认事实不是连续前缀：{session_id}"
        )));
    }
    let expected_confirmed = state
        .compacted_confirmed_private_deliveries
        .iter()
        .map(|(sequence, delivery)| (*sequence, delivery.confirmation.clone()))
        .chain(
            state
                .confirmed_private_deliveries
                .iter()
                .map(|(sequence, delivery)| (*sequence, delivery.confirmation())),
        )
        .collect::<BTreeMap<_, _>>();
    if confirmed != expected_confirmed {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "仅追加输出确认事实与检查点不一致：{session_id}"
        )));
    }

    let mut checkpoint_outputs = state.confirmed_private_deliveries.clone();
    for (sequence, compacted) in &state.compacted_confirmed_private_deliveries {
        let delivery = prepared.get(sequence).ok_or_else(|| {
            SimExecutionPersistenceErrorV1::Session(format!(
                "已裁剪确认输出缺少完整仅追加正文：{session_id}:{sequence}"
            ))
        })?;
        let expected_compacted = CompactedConfirmedPrivateDeliveryV1::try_from_delivery(delivery)?;
        if &expected_compacted != compacted
            || checkpoint_outputs
                .insert(*sequence, delivery.clone())
                .is_some()
        {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "已裁剪确认输出与完整仅追加正文不一致：{session_id}:{sequence}"
            )));
        }
    }
    let mut insert_checkpoint_output =
        |delivery: SealedPrivateDeliveryV1| -> Result<(), SimExecutionPersistenceErrorV1> {
            let sequence = delivery.confirmation().private_feedback_sequence;
            if checkpoint_outputs.insert(sequence, delivery).is_some() {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "检查点私有输出序号重复：{session_id}:{sequence}"
                )));
            }
            Ok(())
        };
    for sealed in state
        .direct_response_outbox
        .entries
        .values()
        .filter_map(|entry| entry.sealed.as_ref())
    {
        insert_checkpoint_output(SealedPrivateDeliveryV1::Direct(sealed.clone()))?;
    }
    for sealed in state
        .lifecycle_outbox
        .entries
        .values()
        .filter_map(|entry| entry.sealed.as_ref())
    {
        insert_checkpoint_output(SealedPrivateDeliveryV1::Lifecycle(sealed.clone()))?;
    }
    if prepared != checkpoint_outputs {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "仅追加密封输出事实与检查点不是精确双向对应：{session_id}"
        )));
    }

    let checkpoint_drafts =
        private_output_drafts_in_checkpoint(&state.checkpoint(downstream_confirmation.cloned()))?;
    for (delivery_id, draft) in &checkpoint_drafts {
        if durable_drafts.get(delivery_id) != Some(draft) {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "检查点私有输出草稿缺少同正文仅追加事实：{session_id}:{delivery_id}"
            )));
        }
    }
    let prepared_by_delivery_id = prepared
        .values()
        .map(|delivery| (delivery.confirmation().delivery_id, delivery))
        .collect::<BTreeMap<_, _>>();
    for (delivery_id, draft) in &durable_drafts {
        if checkpoint_drafts.contains_key(delivery_id) {
            continue;
        }
        let Some(delivery) = prepared_by_delivery_id.get(delivery_id) else {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "仅追加私有输出草稿没有活动或密封正文：{session_id}:{delivery_id}"
            )));
        };
        let sequence = delivery.confirmation().private_feedback_sequence;
        if seal_durable_private_output_draft(draft, sequence)? != **delivery {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "仅追加私有输出草稿不能重建密封正文：{session_id}:{delivery_id}"
            )));
        }
    }
    for (sequence, delivery) in &prepared {
        let delivery_id = &delivery.confirmation().delivery_id;
        let Some(draft) = durable_drafts.get(delivery_id) else {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "密封输出缺少形成期完整草稿事实：{session_id}:{sequence}"
            )));
        };
        if seal_durable_private_output_draft(draft, *sequence)? != *delivery {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "密封输出与形成期完整草稿事实不一致：{session_id}:{sequence}"
            )));
        }
    }
    let last_downstream =
        downstream.and_then(|proofs| proofs.last_key_value().map(|(_, proof)| proof));
    match (last_downstream, downstream_confirmation) {
        (None, None) => {}
        (Some(proof), Some(confirmation))
            if proof.durable_through == *confirmation
                && proof.account_instance_key
                    == *state.session.execution_state().account_instance_key() => {}
        _ => {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "下游耐久水位事实与检查点不一致：{session_id}"
            )));
        }
    }
    Ok(())
}

fn seal_durable_private_output_draft(
    draft: &DurablePrivateOutputDraftV1,
    private_feedback_sequence: u64,
) -> Result<SealedPrivateDeliveryV1, SimExecutionPersistenceErrorV1> {
    let error = |error: SimExecutionContractErrorV1| {
        SimExecutionPersistenceErrorV1::Session(format!(
            "耐久私有输出草稿无法按规范重新密封：{error}"
        ))
    };
    match draft {
        DurablePrivateOutputDraftV1::Direct { draft } => {
            let candidate = crate::session::VisiblePrivateFactV1 {
                evidence: crate::session::PrivateVisibilityEvidenceV1::DirectResponse {
                    global_request_id: draft.context.global_request_id.clone(),
                },
                received_at_utc_ms: draft.response_received_at_utc_ms,
                private_emission_sequence: draft.private_emission_sequence,
                private_feedback_sequence,
            };
            Ok(SealedPrivateDeliveryV1::Direct(
                seal_direct_response(draft, &candidate).map_err(error)?,
            ))
        }
        DurablePrivateOutputDraftV1::Lifecycle { draft } => {
            let candidate = crate::session::VisiblePrivateFactV1 {
                evidence: crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback {
                    feedback_id: draft.feedback_id().to_owned(),
                },
                received_at_utc_ms: draft.timing().feedback_received_at_utc_ms,
                private_emission_sequence: draft.timing().private_emission_sequence,
                private_feedback_sequence,
            };
            let sealed = match draft {
                LifecycleFeedbackDraftV1::Order(draft) => SealedLifecycleFeedbackV1::Order(
                    seal_order_lifecycle_feedback(draft, &candidate).map_err(error)?,
                ),
                LifecycleFeedbackDraftV1::Trade(draft) => SealedLifecycleFeedbackV1::Trade(
                    seal_trade_lifecycle_feedback(draft, &candidate).map_err(error)?,
                ),
                LifecycleFeedbackDraftV1::Settlement(draft) => {
                    SealedLifecycleFeedbackV1::Settlement(
                        seal_settlement_lifecycle_feedback(draft, &candidate).map_err(error)?,
                    )
                }
            };
            Ok(SealedPrivateDeliveryV1::Lifecycle(sealed))
        }
    }
}

fn validate_durable_bucket_paths_for_recovery(
    session_id: &str,
    caller_role: SimCallerRoleV1,
    buckets: Option<&BTreeMap<MarketEventPathV1, DurableMarketBucketReferenceV1>>,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    if caller_role == SimCallerRoleV1::Paper
        && buckets.is_some_and(|buckets| buckets.contains_key(&MarketEventPathV1::HiddenPlatform))
    {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "模拟盘仅追加证据包含不可达的隐藏平台桶：{session_id}"
        )));
    }
    Ok(())
}

impl DurableEvidenceProjectionV1 {
    fn for_artifacts() -> Self {
        Self {
            retain_retired_audit_state: true,
            ..Self::default()
        }
    }

    fn discard_retired_session_state(&mut self, session_id: &str) {
        self.initialization_results.remove(session_id);
        self.market_enrollments.remove(session_id);
        self.requests.remove(session_id);
        self.last_platform_buckets.remove(session_id);
        self.account_deltas.remove(session_id);
        self.logical_action_results.remove(session_id);
        self.logical_action_result_contexts.remove(session_id);
        self.prepared_outputs.remove(session_id);
        self.confirmed_outputs.remove(session_id);
        self.private_output_drafts.remove(session_id);
        self.downstream_durable.remove(session_id);
        self.closed_ingress_sessions.remove(session_id);
        self.query_projection_revisions.remove(session_id);
        self.logical_platform_driven_through_by_session
            .remove(session_id);
        self.private_fact_schedule_closed_through_by_session
            .remove(session_id);
        self.query_retention.remove(session_id);
        self.evicted_markets.remove(session_id);
    }

    fn apply(
        &mut self,
        evidence: &DurableTransactionEvidenceV1,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let conflict = |message: String| SimExecutionPersistenceErrorV1::Session(message);
        let permitted_post_seal_resource_fact = matches!(
            &evidence.fact,
            DurableTransactionFactV1::ResourceLimitReached {
                session_id: None,
                limit: ResourceLimitReachedV1 {
                    scope: ResourceLimitScopeV1::Durable,
                    ..
                },
            }
        );
        if self.lifecycle == Some(DurableRunLifecycleV1::ArtifactInputsSealed)
            && !permitted_post_seal_resource_fact
        {
            return Err(conflict(
                "运行产物输入封口事实之后仍出现耐久业务事实".to_owned(),
            ));
        }
        let session_id = evidence.session_id().map(ToOwned::to_owned);
        if let Some(session_id) = &session_id
            && self.completed_sessions.contains_key(session_id)
        {
            return Err(conflict(format!(
                "会话完成事实之后仍出现耐久会话事实：{session_id}"
            )));
        }
        if matches!(
            evidence.fact,
            DurableTransactionFactV1::SessionCreated { .. }
        ) {
            if self.run_id.is_none() {
                return Err(conflict("会话创建事实出现在运行创建事实之前".to_owned()));
            }
        } else if let Some(session_id) = &session_id
            && !self.session_creations.contains_key(session_id)
            && !matches!(
                evidence.fact,
                DurableTransactionFactV1::ResourceLimitReached { .. }
            )
        {
            return Err(conflict(format!(
                "耐久会话事实缺少前置创建事实：{session_id}"
            )));
        }
        match (&session_id, evidence.query_projection_revision) {
            (Some(session_id), Some(revision)) => {
                let previous = self
                    .query_projection_revisions
                    .get(session_id)
                    .copied()
                    .unwrap_or(0);
                if revision < previous || revision > previous.saturating_add(1) {
                    return Err(conflict(format!(
                        "查询投影修订号没有单调逐项推进：{session_id}:{previous}->{revision}"
                    )));
                }
                self.query_projection_revisions
                    .insert(session_id.clone(), revision);
            }
            (Some(_), None) | (None, None) => {}
            (None, Some(_)) => {
                return Err(conflict(
                    "查询投影修订号与会话耐久事实归属不一致".to_owned(),
                ));
            }
        }
        let apply_watermark = |watermarks: &mut BTreeMap<String, i64>,
                               session_id: &str,
                               candidate: Option<i64>,
                               label: &str|
         -> Result<(), SimExecutionPersistenceErrorV1> {
            let Some(candidate) = candidate else {
                return Ok(());
            };
            if watermarks
                .get(session_id)
                .is_some_and(|previous| candidate < *previous)
            {
                return Err(conflict(format!(
                    "{label}没有单调推进：{session_id}:{candidate}"
                )));
            }
            watermarks.insert(session_id.to_owned(), candidate);
            Ok(())
        };
        if let Some(session_id) = &session_id {
            apply_watermark(
                &mut self.logical_platform_driven_through_by_session,
                session_id,
                evidence.logical_platform_driven_through_utc_ms,
                "逻辑平台耐久水位",
            )?;
            apply_watermark(
                &mut self.private_fact_schedule_closed_through_by_session,
                session_id,
                evidence.private_fact_schedule_closed_through_utc_ms,
                "私有事实计划耐久水位",
            )?;
        } else if evidence.logical_platform_driven_through_utc_ms.is_some()
            || evidence
                .private_fact_schedule_closed_through_utc_ms
                .is_some()
        {
            return Err(conflict("运行级耐久事实不能携带会话水位".to_owned()));
        }
        match &evidence.fact {
            DurableTransactionFactV1::RunCreated {
                run_id,
                market_event_source_identity_sha256,
                run_intent_binding,
                resource_policy,
                resource_policy_sha256,
            } => {
                if self.run_id.replace(run_id.clone()).is_some() {
                    return Err(conflict("耐久证据包含多个运行创建事实".to_owned()));
                }
                if self
                    .market_event_source_identity_sha256
                    .replace(market_event_source_identity_sha256.clone())
                    .is_some()
                {
                    return Err(conflict("耐久证据包含多个市场事件源身份".to_owned()));
                }
                run_intent_binding
                    .validate()
                    .map_err(|error| conflict(error.to_string()))?;
                let actual_resource_policy_sha256 = resource_policy
                    .content_sha256()
                    .map_err(|error| conflict(error.to_string()))?;
                if &actual_resource_policy_sha256 != resource_policy_sha256
                    || self
                        .resource_policy
                        .replace(resource_policy.clone())
                        .is_some()
                    || self
                        .resource_policy_sha256
                        .replace(resource_policy_sha256.clone())
                        .is_some()
                {
                    return Err(conflict(
                        "耐久运行资源策略正文、身份或唯一性冲突".to_owned(),
                    ));
                }
                if self
                    .run_intent_binding
                    .replace(run_intent_binding.clone())
                    .is_some()
                    || self
                        .lifecycle
                        .replace(DurableRunLifecycleV1::Open)
                        .is_some()
                {
                    return Err(conflict(
                        "耐久证据包含多个运行意图或生命周期起点".to_owned(),
                    ));
                }
            }
            DurableTransactionFactV1::RunArtifactInputsSealed { run_id } => {
                if self.run_id.as_ref() != Some(run_id)
                    || self.lifecycle != Some(DurableRunLifecycleV1::Open)
                {
                    return Err(conflict(
                        "运行产物输入封口事实缺少同一开放运行前驱".to_owned(),
                    ));
                }
                self.lifecycle = Some(DurableRunLifecycleV1::ArtifactInputsSealed);
            }
            DurableTransactionFactV1::ComponentGoldenMeasurementRecorded {
                run_id,
                measurement,
                measurement_sha256,
            } => {
                let Some(run_intent_binding) = self.run_intent_binding.as_ref() else {
                    return Err(conflict("组件黄金测量事实缺少运行意图前驱".to_owned()));
                };
                let expected_workload_sha256 = ComponentGoldenLoadContractV1::component_golden_v1()
                    .content_sha256()
                    .map_err(|error| conflict(error.to_string()))?;
                let actual_measurement_sha256 = measurement
                    .content_sha256()
                    .map_err(|error| conflict(error.to_string()))?;
                let component_sessions_completed = self.session_creations.len() == 2
                    && self.completed_sessions.len() == 2
                    && self
                        .session_creations
                        .iter()
                        .all(|(session_id, (spec, _, _))| {
                            spec.caller_role == SimCallerRoleV1::ModelTrain
                                && self.completed_sessions.get(session_id)
                                    == Some(&DurableSessionCompletionKindV1::CleanRunCompleted)
                        });
                if self.component_golden_measurement.is_some()
                    || self.component_golden_measurement_sha256.is_some()
                {
                    return Err(conflict("组件黄金测量事实重复".to_owned()));
                }
                if self.run_id.as_ref() != Some(run_id)
                    || !component_sessions_completed
                    || measurement.run_id != *run_id
                    || measurement.workload_contract_sha256 != expected_workload_sha256
                    || run_intent_binding.workload_kind
                        != artifacts::SimulationWorkloadKindV1::ComponentGolden
                    || run_intent_binding.deterministic_script_contract
                        != artifacts::SimulationDeterministicScriptContractV1::ComponentGoldenV1
                    || measurement.environment.build.source_commit
                        != run_intent_binding.implementation_identity.git_commit
                    || measurement.environment.build.source_tree_sha256
                        != run_intent_binding
                            .implementation_identity
                            .source_identity_sha256
                    || measurement.environment.build.cargo_lock_sha256
                        != run_intent_binding.implementation_identity.cargo_lock_sha256
                    || actual_measurement_sha256 != *measurement_sha256
                {
                    return Err(conflict(
                        "组件黄金测量正文、运行身份或唯一性冲突".to_owned(),
                    ));
                }
                self.component_golden_measurement = Some(measurement.as_ref().clone());
                self.component_golden_measurement_sha256 = Some(measurement_sha256.clone());
            }
            DurableTransactionFactV1::SessionCreated {
                session_spec,
                opening_anchor,
                initialization,
            } => {
                if self
                    .session_creations
                    .insert(
                        session_spec.session_id.clone(),
                        (
                            session_spec.clone(),
                            opening_anchor.as_ref().clone(),
                            initialization.clone(),
                        ),
                    )
                    .is_some()
                {
                    return Err(conflict(format!(
                        "会话创建事实重复：{}",
                        session_spec.session_id
                    )));
                }
            }
            DurableTransactionFactV1::SessionActivated { request, result } => {
                let Some((_, opening_anchor, _)) = self.session_creations.get(&request.session_id)
                else {
                    return Err(conflict(format!(
                        "会话激活事实缺少创建事实：{}",
                        request.session_id
                    )));
                };
                if result.state != SessionInitializationStateV1::Active
                    || result.activation_id.as_deref() != Some(&request.activation_id)
                    || result.failure_id.is_some()
                    || request.account_instance_key != opening_anchor.account_instance_key
                    || request.opening_anchor_id != opening_anchor.anchor_id
                    || request.opening_anchor_sha256 != opening_anchor.anchor_sha256
                {
                    return Err(conflict(format!(
                        "会话激活事实正文不一致：{}",
                        request.session_id
                    )));
                }
                if self
                    .initialization_results
                    .insert(request.session_id.clone(), result.clone())
                    .is_some()
                {
                    return Err(conflict(format!(
                        "会话初始化终态事实重复：{}",
                        request.session_id
                    )));
                }
            }
            DurableTransactionFactV1::SessionInitializationFailed { request, result } => {
                let Some((_, opening_anchor, _)) = self.session_creations.get(&request.session_id)
                else {
                    return Err(conflict(format!(
                        "会话失败关闭事实缺少创建事实：{}",
                        request.session_id
                    )));
                };
                if result.state != SessionInitializationStateV1::InitializationFailedClosed
                    || result.failure_id.as_deref() != Some(&request.failure_id)
                    || result.activation_id.is_some()
                    || request.account_instance_key != opening_anchor.account_instance_key
                    || request.opening_anchor_id != opening_anchor.anchor_id
                    || request.opening_anchor_sha256 != opening_anchor.anchor_sha256
                {
                    return Err(conflict(format!(
                        "会话失败关闭事实正文不一致：{}",
                        request.session_id
                    )));
                }
                if self
                    .initialization_results
                    .insert(request.session_id.clone(), result.clone())
                    .is_some()
                {
                    return Err(conflict(format!(
                        "会话初始化终态事实重复：{}",
                        request.session_id
                    )));
                }
            }
            DurableTransactionFactV1::MarketEnrolled {
                session_id,
                enrollment,
            } => self
                .market_enrollments
                .entry(session_id.clone())
                .or_default()
                .push(enrollment.clone()),
            DurableTransactionFactV1::RequestRegistered {
                session_id,
                registration,
            } => {
                let request_id = registration.request.context().global_request_id.clone();
                if self
                    .requests
                    .entry(session_id.clone())
                    .or_default()
                    .insert(request_id.clone(), registration.clone())
                    .is_some()
                {
                    return Err(conflict(format!("耐久请求事实重复：{request_id}")));
                }
            }
            DurableTransactionFactV1::PlatformBucketApplied { session_id, bucket } => {
                let path_buckets = self
                    .last_platform_buckets
                    .entry(session_id.clone())
                    .or_default();
                if let Some(previous) = path_buckets.get(&bucket.path) {
                    let closure_upgrade = previous.bucket_at_utc_ms == bucket.bucket_at_utc_ms
                        && previous.start_index_position == bucket.start_index_position
                        && previous.next_index_position == bucket.next_index_position
                        && previous.bucket_sha256 == bucket.bucket_sha256
                        && matches!(previous.closure, MillisecondBucketClosureV1::NextEventAt(_))
                        && matches!(bucket.closure, MillisecondBucketClosureV1::EndOfInput);
                    if !closure_upgrade
                        && (previous.next_index_position != bucket.start_index_position
                            || previous.bucket_at_utc_ms >= bucket.bucket_at_utc_ms
                            || matches!(previous.closure, MillisecondBucketClosureV1::EndOfInput))
                    {
                        return Err(conflict(format!(
                            "耐久平台桶事实不连续：{session_id}:{:?}",
                            bucket.path
                        )));
                    }
                } else if bucket.start_index_position != 0 {
                    return Err(conflict(format!(
                        "耐久平台桶事实没有从零游标开始：{session_id}:{:?}",
                        bucket.path
                    )));
                }
                path_buckets.insert(bucket.path, bucket.clone());
            }
            DurableTransactionFactV1::PrivateOutputPrepared {
                session_id,
                delivery,
            } => {
                let confirmation = delivery.confirmation();
                if confirmation.session_id != *session_id
                    || self
                        .prepared_outputs
                        .entry(session_id.clone())
                        .or_default()
                        .insert(confirmation.private_feedback_sequence, delivery.clone())
                        .is_some()
                {
                    return Err(conflict(format!(
                        "耐久待交付输出事实重复或身份冲突：{}",
                        confirmation.delivery_id
                    )));
                }
            }
            DurableTransactionFactV1::PrivateOutputConfirmed {
                session_id,
                confirmation,
            } => {
                if confirmation.session_id != *session_id
                    || self
                        .confirmed_outputs
                        .entry(session_id.clone())
                        .or_default()
                        .insert(confirmation.private_feedback_sequence, confirmation.clone())
                        .is_some()
                {
                    return Err(conflict(format!(
                        "耐久输出确认事实重复或身份冲突：{}",
                        confirmation.delivery_id
                    )));
                }
            }
            DurableTransactionFactV1::DownstreamDurabilityAdvanced { session_id, proof } => {
                let sequence = proof.durable_through.private_feedback_sequence;
                let Some((_, opening_anchor, _)) = self.session_creations.get(session_id) else {
                    return Err(conflict(format!(
                        "下游耐久证明缺少会话创建事实：{session_id}"
                    )));
                };
                if proof.durable_through.session_id != *session_id
                    || proof.account_instance_key != opening_anchor.account_instance_key
                    || self
                        .confirmed_outputs
                        .get(session_id)
                        .and_then(|confirmations| confirmations.get(&sequence))
                        != Some(&proof.durable_through)
                {
                    return Err(conflict(format!("下游耐久证明会话身份冲突：{session_id}")));
                }
                let previous = self
                    .downstream_durable
                    .get(session_id)
                    .and_then(|proofs| proofs.last_key_value().map(|(sequence, _)| *sequence))
                    .unwrap_or(0);
                if sequence != previous + 1 {
                    return Err(conflict(format!(
                        "下游耐久证明没有逐项连续推进：{session_id}"
                    )));
                }
                self.downstream_durable
                    .entry(session_id.clone())
                    .or_default()
                    .insert(sequence, proof.clone());
            }
            DurableTransactionFactV1::IngressClosed { session_id, .. } => {
                if !self.closed_ingress_sessions.insert(session_id.clone()) {
                    return Err(conflict(format!("耐久入口关闭事实重复：{session_id}")));
                }
            }
            DurableTransactionFactV1::RetentionPrepared {
                session_id,
                scope,
                root_sequence_floor,
                retained_from_utc_ms,
            } => {
                let retention = self
                    .query_retention
                    .entry(session_id.clone())
                    .or_insert((1, None, None));
                match scope {
                    QueryRetentionScopeV1::RootSnapshot => {
                        let Some(floor) = root_sequence_floor else {
                            return Err(conflict(format!(
                                "查询根保留事实缺少根序号：{session_id}"
                            )));
                        };
                        if retained_from_utc_ms.is_some() || *floor < retention.0 {
                            return Err(conflict(format!(
                                "查询根保留事实倒退或字段冲突：{session_id}"
                            )));
                        }
                        retention.0 = *floor;
                    }
                    QueryRetentionScopeV1::OrderHistory => {
                        let Some(floor) = retained_from_utc_ms else {
                            return Err(conflict(format!(
                                "订单历史保留事实缺少时点：{session_id}"
                            )));
                        };
                        if root_sequence_floor.is_some()
                            || *floor < 0
                            || retention.1.is_some_and(|current| *floor < current)
                        {
                            return Err(conflict(format!(
                                "订单历史保留事实倒退或字段冲突：{session_id}"
                            )));
                        }
                        retention.1 = Some(*floor);
                    }
                    QueryRetentionScopeV1::TradeHistory => {
                        let Some(floor) = retained_from_utc_ms else {
                            return Err(conflict(format!(
                                "成交历史保留事实缺少时点：{session_id}"
                            )));
                        };
                        if root_sequence_floor.is_some()
                            || *floor < 0
                            || retention.2.is_some_and(|current| *floor < current)
                        {
                            return Err(conflict(format!(
                                "成交历史保留事实倒退或字段冲突：{session_id}"
                            )));
                        }
                        retention.2 = Some(*floor);
                    }
                }
            }
            DurableTransactionFactV1::MarketHotStateEvicted {
                session_id,
                condition_id,
                terminal_orders,
                terminal_trades,
                execution_witness,
            } => {
                if execution_witness.condition_id != *condition_id
                    || terminal_orders.iter().any(|(key, order)| {
                        key != &order.platform_order_id || order.condition_id.0 != *condition_id
                    })
                    || terminal_trades.iter().any(|(key, trade)| {
                        key != &trade.simulated_trade_id || trade.condition_id.0 != *condition_id
                    })
                    || terminal_trades
                        .keys()
                        .any(|trade_id| !execution_witness.fills.contains_key(trade_id))
                    || self
                        .evicted_markets
                        .entry(session_id.clone())
                        .or_default()
                        .insert(
                            condition_id.clone(),
                            (
                                terminal_orders.clone(),
                                terminal_trades.clone(),
                                execution_witness.as_ref().clone(),
                            ),
                        )
                        .is_some()
                {
                    return Err(conflict(format!(
                        "终态市场热状态驱逐事实重复或身份冲突：{session_id}:{condition_id}"
                    )));
                }
            }
            DurableTransactionFactV1::SessionRetentionExpired { session_id } => {
                let Some((spec, _, created_initialization)) =
                    self.session_creations.get(session_id)
                else {
                    return Err(conflict(format!(
                        "模拟盘保留期完成事实缺少创建前驱：{session_id}"
                    )));
                };
                let terminal = self
                    .initialization_results
                    .get(session_id)
                    .unwrap_or(created_initialization);
                if spec.caller_role != SimCallerRoleV1::Paper
                    || terminal.state != SessionInitializationStateV1::Active
                {
                    return Err(conflict(format!(
                        "模拟盘保留期完成事实与会话角色或初始化终态冲突：{session_id}"
                    )));
                }
                if self
                    .completed_sessions
                    .insert(
                        session_id.clone(),
                        DurableSessionCompletionKindV1::PaperRetentionExpired,
                    )
                    .is_some()
                {
                    return Err(conflict(format!("会话完成事实重复：{session_id}")));
                }
                self.session_resource_admission.remove(session_id);
            }
            DurableTransactionFactV1::SessionCleanRunCompleted { session_id } => {
                let Some((spec, _, created_initialization)) =
                    self.session_creations.get(session_id)
                else {
                    return Err(conflict(format!(
                        "clean run 完成事实缺少创建前驱：{session_id}"
                    )));
                };
                let terminal = self
                    .initialization_results
                    .get(session_id)
                    .unwrap_or(created_initialization);
                if !matches!(
                    spec.caller_role,
                    SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
                ) || terminal.state != SessionInitializationStateV1::Active
                {
                    return Err(conflict(format!(
                        "clean run 完成事实与会话角色或初始化终态冲突：{session_id}"
                    )));
                }
                if self
                    .completed_sessions
                    .insert(
                        session_id.clone(),
                        DurableSessionCompletionKindV1::CleanRunCompleted,
                    )
                    .is_some()
                {
                    return Err(conflict(format!("会话完成事实重复：{session_id}")));
                }
                self.session_resource_admission.remove(session_id);
            }
            DurableTransactionFactV1::ResourceLimitReached { session_id, limit } => {
                let Some(resource_policy) = self.resource_policy.as_ref() else {
                    return Err(conflict("资源超限事实缺少运行资源策略".to_owned()));
                };
                if resource_policy
                    .validate_limit_fact(limit)
                    .map_err(|error| conflict(error.to_string()))
                    .is_err()
                {
                    return Err(conflict("资源超限事实没有绑定运行资源策略".to_owned()));
                }
                match limit.scope {
                    ResourceLimitScopeV1::Session => {
                        let Some(session_id) = session_id else {
                            return Err(conflict("会话资源超限事实缺少会话身份".to_owned()));
                        };
                        let admission = self
                            .session_resource_admission
                            .entry(session_id.clone())
                            .or_default();
                        if admission
                            .blocking_limit()
                            .is_some_and(|existing| existing.same_admission_barrier(limit))
                        {
                            return Err(conflict(format!("会话资源超限事实重复：{session_id}")));
                        }
                        admission.apply_limit(limit.clone());
                    }
                    ResourceLimitScopeV1::Run | ResourceLimitScopeV1::Durable => {
                        if session_id.is_some() {
                            return Err(conflict("运行资源超限事实身份冲突或重复".to_owned()));
                        }
                        if self
                            .run_resource_admission
                            .blocking_limit()
                            .is_some_and(|existing| existing.same_admission_barrier(limit))
                        {
                            return Err(conflict("运行资源超限事实身份冲突或重复".to_owned()));
                        }
                        self.run_resource_admission.apply_limit(limit.clone());
                    }
                }
            }
            DurableTransactionFactV1::ExecutionAdvanced { .. } => {}
        }
        let resource_usage = evidence
            .resource_usage
            .as_ref()
            .ok_or_else(|| conflict("耐久事实缺少事务后资源使用量".to_owned()))?;
        if evidence.transient_input != ResourceQuantityV1::default()
            && evidence.transient_input_session_id.is_none()
        {
            return Err(conflict("耐久瞬时输入缺少所属会话身份".to_owned()));
        }
        if let (Some(fact_session_id), Some(transient_session_id)) = (
            session_id.as_deref(),
            evidence.transient_input_session_id.as_deref(),
        ) && fact_session_id != transient_session_id
        {
            return Err(conflict("耐久事实会话与瞬时输入所属会话不一致".to_owned()));
        }
        let resource_policy = self
            .resource_policy
            .as_ref()
            .ok_or_else(|| conflict("耐久资源使用量缺少运行资源策略".to_owned()))?;
        if let DurableTransactionFactV1::ResourceLimitReached { limit, .. } = &evidence.fact {
            if evidence.transient_input != ResourceQuantityV1::default() {
                let attempted_input_is_bounded = match (limit.scope, limit.kind) {
                    (
                        ResourceLimitScopeV1::Session | ResourceLimitScopeV1::Run,
                        ResourceLimitKindV1::InputQueue,
                    ) => {
                        evidence.transient_input.records <= limit.candidate.records
                            && evidence.transient_input.bytes <= limit.candidate.bytes
                    }
                    _ => {
                        evidence.transient_input.records
                            <= resource_policy.session.input_queue.max_records
                            && evidence.transient_input.bytes
                                <= resource_policy.session.input_queue.max_bytes
                            && evidence.transient_input.records
                                <= resource_policy.run.input_queue.max_records
                            && evidence.transient_input.bytes
                                <= resource_policy.run.input_queue.max_bytes
                    }
                };
                if !attempted_input_is_bounded {
                    return Err(conflict("资源超限事实没有约束被拒绝的瞬时输入".to_owned()));
                }
            }
        } else if resource_usage.session.input_queue.records < evidence.transient_input.records
            || resource_usage.session.input_queue.bytes < evidence.transient_input.bytes
            || resource_usage.run.input_queue.records < evidence.transient_input.records
            || resource_usage.run.input_queue.bytes < evidence.transient_input.bytes
        {
            return Err(conflict(
                "耐久资源使用量没有覆盖该事务的瞬时输入".to_owned(),
            ));
        }
        let resource_validation = if matches!(
            evidence.fact,
            DurableTransactionFactV1::ResourceLimitReached { .. }
        ) {
            resource_policy.validate_failure_candidate(resource_usage)
        } else {
            resource_policy.validate_candidate(resource_usage)
        };
        resource_validation.map_err(|error| conflict(error.to_string()))?;
        self.resource_high_water.observe(resource_usage);
        if let Some(session_id) = session_id {
            let target = self.account_deltas.entry(session_id.clone()).or_default();
            for delta in &evidence.account_deltas {
                if target
                    .insert(delta.account_transition_seq, delta.clone())
                    .is_some()
                {
                    return Err(conflict(format!(
                        "耐久账户变化序号 {} 重复",
                        delta.account_transition_seq
                    )));
                }
            }
            let action_results = self
                .logical_action_results
                .entry(session_id.clone())
                .or_default();
            let action_result_contexts = self
                .logical_action_result_contexts
                .entry(session_id.clone())
                .or_default();
            let action_result_context = DurableLogicalActionRecoveryContextV1 {
                query_projection_revision: self
                    .query_projection_revisions
                    .get(&session_id)
                    .copied()
                    .unwrap_or(0),
                query_retention: self
                    .query_retention
                    .get(&session_id)
                    .copied()
                    .unwrap_or((1, None, None)),
            };
            if !evidence.logical_action_results.is_empty()
                && !matches!(
                    evidence.fact,
                    DurableTransactionFactV1::ExecutionAdvanced { .. }
                )
            {
                return Err(conflict("逻辑动作结果只能随执行推进事实提交".to_owned()));
            }
            for result in &evidence.logical_action_results {
                if action_results
                    .insert(result.action_identity.clone(), result.clone())
                    .is_some()
                    || action_result_contexts
                        .insert(result.action_identity.clone(), action_result_context)
                        .is_some()
                {
                    return Err(conflict(format!(
                        "耐久逻辑动作结果重复：{}",
                        result.action_identity
                    )));
                }
            }
            let drafts = self
                .private_output_drafts
                .entry(session_id.clone())
                .or_default();
            for draft in &evidence.private_output_drafts {
                if draft.session_id() != session_id {
                    return Err(conflict(format!(
                        "耐久私有输出草稿会话身份冲突：{}",
                        draft.delivery_id()
                    )));
                }
                if drafts
                    .insert(draft.delivery_id().to_owned(), draft.clone())
                    .is_some()
                {
                    return Err(conflict(format!(
                        "耐久私有输出草稿重复：{}",
                        draft.delivery_id()
                    )));
                }
            }
            if !self.retain_retired_audit_state
                && matches!(
                    evidence.fact,
                    DurableTransactionFactV1::SessionRetentionExpired { .. }
                        | DurableTransactionFactV1::SessionCleanRunCompleted { .. }
                )
            {
                self.discard_retired_session_state(&session_id);
            }
        } else if !evidence.account_deltas.is_empty() {
            return Err(conflict("运行级耐久事实不能携带账户变化".to_owned()));
        } else if !evidence.logical_action_results.is_empty() {
            return Err(conflict("运行级耐久事实不能携带逻辑动作结果".to_owned()));
        } else if !evidence.private_output_drafts.is_empty() {
            return Err(conflict("运行级耐久事实不能携带私有输出草稿".to_owned()));
        }
        Ok(())
    }
}

struct PersistentRunCoordinatorV1 {
    store: LocalDurabilityStoreV1,
    current: DurableRunCheckpointV1,
    recovery_required: Option<crate::persistence::PersistenceErrorV1>,
    transient_input_by_session: BTreeMap<String, ResourceQuantityV1>,
}

type SharedPersistentRunV1 = Arc<Mutex<PersistentRunCoordinatorV1>>;

fn encoded_resource_bytes<T: Serialize>(value: &T) -> Result<u64, SessionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| SessionError::Durability(format!("资源计量序列化失败：{error}")))?;
    u64::try_from(bytes.len())
        .map_err(|_| SessionError::Durability("资源计量字节超出 u64".to_owned()))
}

fn resource_quantity(records: usize, bytes: u64) -> Result<ResourceQuantityV1, SessionError> {
    Ok(ResourceQuantityV1::new(
        u64::try_from(records)
            .map_err(|_| SessionError::Durability("资源记录数量超出 u64".to_owned()))?,
        bytes,
    ))
}

fn add_resource_quantity(
    left: ResourceQuantityV1,
    right: ResourceQuantityV1,
) -> Result<ResourceQuantityV1, SessionError> {
    Ok(ResourceQuantityV1::new(
        left.records
            .checked_add(right.records)
            .ok_or_else(|| SessionError::Durability("资源记录数量溢出".to_owned()))?,
        left.bytes
            .checked_add(right.bytes)
            .ok_or_else(|| SessionError::Durability("资源字节溢出".to_owned()))?,
    ))
}

fn session_resource_usage(
    checkpoint: &ExecutionSessionCheckpointV1,
) -> Result<crate::resource::SimulationSessionResourceUsageV1, SessionError> {
    let core = checkpoint.session.resource_core_counts();
    let component_bytes = checkpoint.session.resource_component_bytes()?;
    let checkpoint_bytes = encoded_resource_bytes(checkpoint)?;
    let request_bytes = encoded_resource_bytes(&(
        &checkpoint.pending_requests,
        &checkpoint.mutation_payloads,
        &checkpoint.submission_effect_records,
        &checkpoint.cancel_effect_records,
    ))?;
    let feedback_bytes = encoded_resource_bytes(&(
        &checkpoint.direct_response_outbox,
        &checkpoint.lifecycle_outbox,
        &checkpoint.compacted_confirmed_private_deliveries,
        &checkpoint.confirmed_private_deliveries,
    ))?;
    let query_bytes = encoded_resource_bytes(&checkpoint.query_state)?;
    let query_records = checkpoint
        .query_state
        .roots
        .len()
        .saturating_add(checkpoint.query_state.issued_cursors.len())
        .saturating_add(checkpoint.query_state.continuation_by_request.len())
        .saturating_add(checkpoint.query_state.terminal_projections.orders.len())
        .saturating_add(checkpoint.query_state.terminal_projections.trades.len())
        .saturating_add(
            checkpoint
                .query_state
                .roots
                .values()
                .map(|root| match &root.rows {
                    QueryRootRowsV1::Orders(rows) => rows.len(),
                    QueryRootRowsV1::Trades(rows) => rows.len(),
                })
                .sum::<usize>(),
        );
    let feedback_records = checkpoint
        .direct_response_outbox
        .entries
        .len()
        .saturating_add(checkpoint.lifecycle_outbox.entries.len())
        .saturating_add(checkpoint.lifecycle_outbox.batches.len())
        .saturating_add(checkpoint.compacted_confirmed_private_deliveries.len())
        .saturating_add(checkpoint.confirmed_private_deliveries.len());
    let request_records = checkpoint
        .pending_requests
        .len()
        .saturating_add(core.pending_actions)
        .saturating_add(checkpoint.mutation_payloads.len());
    let index_records = core
        .indexes
        .saturating_add(checkpoint.pending_requests.len())
        .saturating_add(checkpoint.mutation_payloads.len())
        .saturating_add(checkpoint.submission_effect_records.len())
        .saturating_add(checkpoint.cancel_effect_records.len())
        .saturating_add(query_records)
        .saturating_add(feedback_records);
    let in_memory_records = index_records
        .saturating_add(core.hot_order_and_fill_records)
        .saturating_add(core.internal_events);
    Ok(crate::resource::SimulationSessionResourceUsageV1 {
        in_memory_state: resource_quantity(in_memory_records, checkpoint_bytes)?,
        hot_markets: resource_quantity(core.hot_markets, component_bytes.hot_markets)?,
        input_queue: ResourceQuantityV1::default(),
        request_queue: resource_quantity(request_records, request_bytes)?,
        feedback_queue: resource_quantity(feedback_records, feedback_bytes)?,
        active_orders: resource_quantity(core.active_orders, component_bytes.active_orders)?,
        internal_events: resource_quantity(core.internal_events, component_bytes.internal_events)?,
        query_cache: resource_quantity(query_records, query_bytes)?,
        indexes: resource_quantity(index_records, component_bytes.indexes)?,
    })
}

fn candidate_resource_usage(
    checkpoint: &DurableRunCheckpointV1,
    session_id: Option<&str>,
    durable: DurableResourcePreviewV1,
) -> Result<SimulationResourceUsageV1, SessionError> {
    let session = match session_id.and_then(|id| checkpoint.sessions.get(id)) {
        Some(checkpoint) => session_resource_usage(checkpoint)?,
        None => {
            let mut maximum = crate::resource::SimulationSessionResourceUsageV1::default();
            for checkpoint in checkpoint.sessions.values() {
                let usage = session_resource_usage(checkpoint)?;
                maximum.in_memory_state.records = maximum
                    .in_memory_state
                    .records
                    .max(usage.in_memory_state.records);
                maximum.in_memory_state.bytes = maximum
                    .in_memory_state
                    .bytes
                    .max(usage.in_memory_state.bytes);
                for (target, source) in [
                    (&mut maximum.hot_markets, usage.hot_markets),
                    (&mut maximum.input_queue, usage.input_queue),
                    (&mut maximum.request_queue, usage.request_queue),
                    (&mut maximum.feedback_queue, usage.feedback_queue),
                    (&mut maximum.active_orders, usage.active_orders),
                    (&mut maximum.internal_events, usage.internal_events),
                    (&mut maximum.query_cache, usage.query_cache),
                    (&mut maximum.indexes, usage.indexes),
                ] {
                    target.records = target.records.max(source.records);
                    target.bytes = target.bytes.max(source.bytes);
                }
            }
            maximum
        }
    };
    let mut run = crate::resource::SimulationRunResourceUsageV1::default();
    for session_checkpoint in checkpoint.sessions.values() {
        let usage = session_resource_usage(session_checkpoint)?;
        run.in_memory_state = add_resource_quantity(run.in_memory_state, usage.in_memory_state)?;
        run.hot_markets = add_resource_quantity(run.hot_markets, usage.hot_markets)?;
        run.input_queue = add_resource_quantity(run.input_queue, usage.input_queue)?;
        run.request_queue = add_resource_quantity(run.request_queue, usage.request_queue)?;
        run.feedback_queue = add_resource_quantity(run.feedback_queue, usage.feedback_queue)?;
        run.active_orders = add_resource_quantity(run.active_orders, usage.active_orders)?;
        run.internal_events = add_resource_quantity(run.internal_events, usage.internal_events)?;
        run.query_cache = add_resource_quantity(run.query_cache, usage.query_cache)?;
        run.indexes = add_resource_quantity(run.indexes, usage.indexes)?;
    }
    let run_checkpoint_bytes = encoded_resource_bytes(checkpoint)?;
    run.in_memory_state.bytes = run_checkpoint_bytes;
    run.sessions = resource_quantity(
        checkpoint.process_root.resource_session_creation_count(),
        run_checkpoint_bytes,
    )?;
    run.capital_fence = resource_quantity(
        checkpoint.process_root.resource_capital_fence_count(),
        encoded_resource_bytes(&checkpoint.process_root)?,
    )?;
    Ok(SimulationResourceUsageV1 {
        session,
        run,
        durable: crate::resource::SimulationDurableResourceUsageV1 {
            evidence: ResourceQuantityV1::new(durable.evidence_file_count, durable.evidence_bytes),
            checkpoints: ResourceQuantityV1::new(
                durable.checkpoint_file_count,
                durable.checkpoint_bytes,
            ),
            artifacts: ResourceQuantityV1::new(durable.artifact_file_count, durable.artifact_bytes),
            runtime_root: ResourceQuantityV1::new(
                durable.runtime_root_file_count,
                durable.runtime_root_bytes,
            ),
        },
    })
}

impl PersistentRunCoordinatorV1 {
    fn commit(
        &mut self,
        evidence: &DurableTransactionEvidenceV1,
        mut candidate: DurableRunCheckpointV1,
    ) -> Result<(), SessionError> {
        self.require_business_writes_open()?;
        if candidate == self.current {
            return Ok(());
        }
        let mut evidence = evidence.clone();
        self.validate_resource_identity(&candidate)?;
        let session_admission_change_is_completion =
            evidence.session_id().is_some_and(|session_id| {
                let mut expected = self.current.session_resource_admission.clone();
                expected.remove(session_id);
                expected == candidate.session_resource_admission
                    && matches!(
                        evidence.fact,
                        DurableTransactionFactV1::SessionRetentionExpired { .. }
                            | DurableTransactionFactV1::SessionCleanRunCompleted { .. }
                    )
            });
        let measurement_change_is_recording = matches!(
            &evidence.fact,
            DurableTransactionFactV1::ComponentGoldenMeasurementRecorded {
                measurement_sha256,
                ..
            } if self.current.component_golden_measurement_sha256.is_none()
                && candidate.component_golden_measurement_sha256.as_ref()
                    == Some(measurement_sha256)
        );
        if candidate.resource_high_water != self.current.resource_high_water
            || candidate.run_resource_admission != self.current.run_resource_admission
            || (candidate.component_golden_measurement_sha256
                != self.current.component_golden_measurement_sha256
                && !measurement_change_is_recording)
            || (candidate.session_resource_admission != self.current.session_resource_admission
                && !session_admission_change_is_completion)
        {
            return Err(SessionError::Durability(
                "普通耐久事务不能改写资源高水位或准入状态".to_owned(),
            ));
        }
        let usage = self.observe_candidate_resources(&mut evidence, &mut candidate)?;
        if let Err(error) = candidate.resource_policy.validate_candidate(&usage) {
            let Some(limit) = error.limit_reached().cloned() else {
                return Err(SessionError::Durability(error.to_string()));
            };
            return self.persist_resource_limit(evidence.session_id(), limit);
        }
        if let Err(error) = self.store.commit_classified(&evidence, &candidate) {
            if let DurableCommitFailureV1::RecoveryRequired(cause) = &error {
                self.recovery_required = Some(cause.clone());
            }
            return Err(session_durable_commit_error(error));
        }
        self.current = candidate;
        Ok(())
    }

    fn commit_new_ingress(
        &mut self,
        evidence: &DurableTransactionEvidenceV1,
        candidate: DurableRunCheckpointV1,
    ) -> Result<(), SessionError> {
        if candidate == self.current {
            return Ok(());
        }
        self.require_new_ingress_open(evidence.session_id())?;
        self.commit(evidence, candidate)
    }

    fn validate_resource_identity(
        &self,
        candidate: &DurableRunCheckpointV1,
    ) -> Result<(), SessionError> {
        let actual_sha256 = candidate
            .resource_policy
            .content_sha256()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        if candidate.resource_policy != self.current.resource_policy
            || candidate.resource_policy_sha256 != self.current.resource_policy_sha256
            || actual_sha256 != candidate.resource_policy_sha256
        {
            return Err(SessionError::Durability(
                "候选检查点改变了运行资源策略正文或身份".to_owned(),
            ));
        }
        Ok(())
    }

    fn observe_candidate_resources(
        &self,
        evidence: &mut DurableTransactionEvidenceV1,
        candidate: &mut DurableRunCheckpointV1,
    ) -> Result<SimulationResourceUsageV1, SessionError> {
        for _ in 0..16 {
            let preview = self
                .store
                .preview_commit_resources(evidence, candidate)
                .map_err(|error| SessionError::Durability(error.to_string()))?;
            let resource_session_id = evidence
                .transient_input_session_id
                .as_deref()
                .or_else(|| evidence.session_id());
            let mut usage = candidate_resource_usage(candidate, resource_session_id, preview)?;
            let session_transient = resource_session_id
                .and_then(|session_id| self.transient_input_by_session.get(session_id))
                .copied()
                .unwrap_or_default();
            if evidence.transient_input != session_transient {
                return Err(SessionError::Durability(
                    "耐久事实的瞬时输入计量与协调器预留不一致".to_owned(),
                ));
            }
            let rejected_resource_attempt = matches!(
                evidence.fact,
                DurableTransactionFactV1::ResourceLimitReached { .. }
            );
            if !rejected_resource_attempt {
                usage.session.input_queue.records = usage
                    .session
                    .input_queue
                    .records
                    .max(session_transient.records);
                usage.session.input_queue.bytes =
                    usage.session.input_queue.bytes.max(session_transient.bytes);
            }
            usage.run.input_queue = self
                .transient_input_by_session
                .iter()
                .filter(|(session_id, _)| {
                    !rejected_resource_attempt || Some(session_id.as_str()) != resource_session_id
                })
                .try_fold(ResourceQuantityV1::default(), |total, (_, quantity)| {
                    add_resource_quantity(total, *quantity)
                })?;
            let before_high_water = candidate.resource_high_water.clone();
            let before_usage = evidence.resource_usage.clone();
            evidence.resource_usage = Some(usage.clone());
            candidate.resource_high_water.observe(&usage);
            if candidate.resource_high_water == before_high_water
                && evidence.resource_usage == before_usage
            {
                return Ok(usage);
            }
        }
        Err(SessionError::Durability(
            "资源高水位与候选检查点字节没有收敛".to_owned(),
        ))
    }

    fn reserve_transient_input(
        &mut self,
        session_id: &str,
        quantity: ResourceQuantityV1,
    ) -> Result<(), SessionError> {
        self.require_bound_market_input_open(session_id, quantity)?;
        if self.transient_input_by_session.contains_key(session_id) {
            return Err(SessionError::Durability(format!(
                "同一会话已经持有瞬时输入资源预留：{session_id}"
            )));
        }
        self.transient_input_by_session
            .insert(session_id.to_owned(), quantity);
        let result = (|| {
            let durable = self
                .store
                .current_resources()
                .map_err(|error| SessionError::Durability(error.to_string()))?;
            let mut usage = candidate_resource_usage(&self.current, Some(session_id), durable)?;
            usage.session.input_queue = quantity;
            usage.run.input_queue = self
                .transient_input_by_session
                .values()
                .try_fold(ResourceQuantityV1::default(), |total, item| {
                    add_resource_quantity(total, *item)
                })?;
            self.current
                .resource_policy
                .validate_candidate(&usage)
                .map_err(|error| {
                    error
                        .limit_reached()
                        .cloned()
                        .map(SessionError::ResourceLimitReached)
                        .unwrap_or_else(|| SessionError::Durability(error.to_string()))
                })
        })();
        if let Err(SessionError::ResourceLimitReached(limit)) = result {
            let persisted = self.persist_resource_limit(Some(session_id), limit);
            self.transient_input_by_session.remove(session_id);
            return persisted;
        }
        if result.is_err() {
            self.transient_input_by_session.remove(session_id);
        }
        result
    }

    fn release_transient_input(&mut self, session_id: &str) -> Result<(), SessionError> {
        if self.transient_input_by_session.remove(session_id).is_none() {
            return Err(SessionError::Durability(format!(
                "会话缺少瞬时输入资源预留：{session_id}"
            )));
        }
        Ok(())
    }

    fn persist_resource_limit(
        &mut self,
        attempted_session_id: Option<&str>,
        limit: ResourceLimitReachedV1,
    ) -> Result<(), SessionError> {
        if let Some(existing) = match limit.scope {
            ResourceLimitScopeV1::Session => attempted_session_id
                .and_then(|session_id| self.current.session_resource_admission.get(session_id))
                .and_then(ResourceAdmissionStateV1::blocking_limit),
            ResourceLimitScopeV1::Run | ResourceLimitScopeV1::Durable => {
                self.current.run_resource_admission.blocking_limit()
            }
        } && existing.same_admission_barrier(&limit)
        {
            return Err(SessionError::ResourceLimitReached(existing.clone()));
        }
        let fact_session_id = match limit.scope {
            ResourceLimitScopeV1::Session => Some(
                attempted_session_id
                    .ok_or_else(|| SessionError::Durability("会话资源超限缺少会话身份".to_owned()))?
                    .to_owned(),
            ),
            ResourceLimitScopeV1::Run | ResourceLimitScopeV1::Durable => None,
        };
        let mut candidate = self.current.clone();
        match limit.scope {
            ResourceLimitScopeV1::Session => candidate
                .session_resource_admission
                .entry(fact_session_id.clone().expect("已形成会话身份"))
                .or_default()
                .apply_limit(limit.clone()),
            ResourceLimitScopeV1::Run | ResourceLimitScopeV1::Durable => {
                candidate.run_resource_admission.apply_limit(limit.clone());
            }
        }
        let mut evidence =
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::ResourceLimitReached {
                session_id: fact_session_id,
                limit: limit.clone(),
            });
        evidence.transient_input_session_id = attempted_session_id.map(ToOwned::to_owned);
        evidence.transient_input = attempted_session_id
            .and_then(|session_id| self.transient_input_by_session.get(session_id))
            .copied()
            .unwrap_or_default();
        let usage = match self.observe_candidate_resources(&mut evidence, &mut candidate) {
            Ok(usage) => usage,
            Err(error) => {
                let cause = PersistenceErrorV1::Integrity(error.to_string());
                self.recovery_required = Some(cause.clone());
                return Err(SessionError::DurabilityRecoveryRequired(cause.to_string()));
            }
        };
        if let Err(error) = candidate.resource_policy.validate_failure_candidate(&usage) {
            let cause = PersistenceErrorV1::Integrity(error.to_string());
            self.recovery_required = Some(cause.clone());
            return Err(SessionError::DurabilityRecoveryRequired(cause.to_string()));
        }
        match self.store.commit_classified(&evidence, &candidate) {
            Ok(_) => {
                self.current = candidate;
                Err(SessionError::ResourceLimitReached(limit))
            }
            Err(DurableCommitFailureV1::DefinitelyNotCommitted(error))
            | Err(DurableCommitFailureV1::RecoveryRequired(error)) => {
                self.recovery_required = Some(error.clone());
                Err(SessionError::DurabilityRecoveryRequired(error.to_string()))
            }
        }
    }

    fn require_business_writes_open(&self) -> Result<(), SessionError> {
        self.require_recovery_not_required()?;
        if self.current.lifecycle == DurableRunLifecycleV1::ArtifactInputsSealed {
            return Err(SessionError::RunArtifactInputsSealed);
        }
        Ok(())
    }

    fn require_new_ingress_open(&self, session_id: Option<&str>) -> Result<(), SessionError> {
        self.require_business_writes_open()?;
        if let Some(limit) = self.current.run_resource_admission.blocking_limit() {
            return Err(SessionError::ResourceLimitReached(limit.clone()));
        }
        if let Some(limit) = session_id
            .and_then(|session_id| self.current.session_resource_admission.get(session_id))
            .and_then(ResourceAdmissionStateV1::blocking_limit)
        {
            return Err(SessionError::ResourceLimitReached(limit.clone()));
        }
        Ok(())
    }

    fn require_bound_market_input_open(
        &self,
        session_id: &str,
        _quantity: ResourceQuantityV1,
    ) -> Result<(), SessionError> {
        self.require_business_writes_open()?;
        for limit in [
            self.current.run_resource_admission.blocking_limit(),
            self.current
                .session_resource_admission
                .get(session_id)
                .and_then(ResourceAdmissionStateV1::blocking_limit),
        ]
        .into_iter()
        .flatten()
        {
            if limit.scope == ResourceLimitScopeV1::Durable
                || limit.kind == ResourceLimitKindV1::InputQueue
            {
                return Err(SessionError::ResourceLimitReached(limit.clone()));
            }
        }
        Ok(())
    }

    fn require_recovery_not_required(&self) -> Result<(), SessionError> {
        match &self.recovery_required {
            Some(error) => Err(SessionError::DurabilityRecoveryRequired(error.to_string())),
            None => Ok(()),
        }
    }

    fn seal_artifact_inputs(
        &mut self,
        run_id: &str,
    ) -> Result<(), artifacts::SimulationRunArtifactErrorV1> {
        self.require_recovery_not_required().map_err(|error| {
            artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(error.to_string())
        })?;
        if self.current.lifecycle == DurableRunLifecycleV1::ArtifactInputsSealed {
            return Ok(());
        }
        let mut candidate = self.current.clone();
        candidate.lifecycle = DurableRunLifecycleV1::ArtifactInputsSealed;
        let mut evidence =
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RunArtifactInputsSealed {
                run_id: run_id.to_owned(),
            });
        let usage = self
            .observe_candidate_resources(&mut evidence, &mut candidate)
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
            })?;
        if let Err(error) = self.current.resource_policy.validate_candidate(&usage) {
            let Some(limit) = error.limit_reached().cloned() else {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                    error.to_string(),
                ));
            };
            return match self.persist_resource_limit(None, limit) {
                Err(SessionError::ResourceLimitReached(persisted)) => {
                    Err(artifacts::SimulationRunArtifactErrorV1::ResourceLimitReached(persisted))
                }
                Err(SessionError::DurabilityRecoveryRequired(message)) => Err(
                    artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(message),
                ),
                Err(other) => Err(artifacts::SimulationRunArtifactErrorV1::Durability(
                    other.to_string(),
                )),
                Ok(()) => Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                    "资源超限事实提交意外返回成功".to_owned(),
                )),
            };
        }
        match self.store.commit_classified(&evidence, &candidate) {
            Ok(_) => {
                self.current = candidate;
                Ok(())
            }
            Err(DurableCommitFailureV1::DefinitelyNotCommitted(error)) => Err(
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string()),
            ),
            Err(DurableCommitFailureV1::RecoveryRequired(error)) => {
                self.recovery_required = Some(error.clone());
                Err(
                    artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(
                        error.to_string(),
                    ),
                )
            }
        }
    }
}

fn session_durable_commit_error(error: DurableCommitFailureV1) -> SessionError {
    match error {
        DurableCommitFailureV1::DefinitelyNotCommitted(error) => {
            SessionError::Durability(error.to_string())
        }
        DurableCommitFailureV1::RecoveryRequired(error) => {
            SessionError::DurabilityRecoveryRequired(error.to_string())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PaperMarketEventRecoveryReadRequestV1 {
    pub bucket_at_utc_ms: i64,
    pub start_visibility_index_position: u64,
    pub next_visibility_index_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperMarketEventRecoveryReadV1 {
    pub encoded_events: Vec<Vec<u8>>,
    /// `Some(t)` 表示来源中下一条公开事件位于 `t`；`None` 表示来源独立证明已经到达 EOF。
    pub next_event_at_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaperMarketEventRecoveryReadErrorV1 {
    InvalidSourceIdentity,
    SourceUnavailable(String),
    RangeUnavailable(String),
    SourceIntegrity(String),
}

impl std::fmt::Display for PaperMarketEventRecoveryReadErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSourceIdentity => {
                write!(formatter, "市场事件源身份不是规范小写 SHA-256")
            }
            Self::SourceUnavailable(message) => write!(formatter, "市场事件源不可用：{message}"),
            Self::RangeUnavailable(message) => write!(formatter, "市场事件范围不可用：{message}"),
            Self::SourceIntegrity(message) => write!(formatter, "市场事件源完整性失败：{message}"),
        }
    }
}

impl std::error::Error for PaperMarketEventRecoveryReadErrorV1 {}

type PaperMarketEventRecoveryReadHandlerV1 = dyn FnMut(
        PaperMarketEventRecoveryReadRequestV1,
    ) -> Result<PaperMarketEventRecoveryReadV1, PaperMarketEventRecoveryReadErrorV1>
    + Send;

pub struct PaperMarketEventRecoveryReaderV1 {
    source_identity_sha256: Sha256Hex,
    read: Box<PaperMarketEventRecoveryReadHandlerV1>,
}

impl PaperMarketEventRecoveryReaderV1 {
    /// 构造由组合根负责真实性的窄恢复读取端口。
    ///
    /// 本构造器只验证来源身份的规范编码；调用方必须让读取实现从其实际打开的不可变来源
    /// 重算并核对该身份，再按请求坐标返回正文与独立闭合见证。核心会把这里声明的身份与
    /// 运行创建事实及检查点精确比较，但不能识别一个违反本信任合同、主动伪报身份的实现。
    pub fn try_new(
        source_identity_sha256: Sha256Hex,
        read: impl FnMut(
            PaperMarketEventRecoveryReadRequestV1,
        )
            -> Result<PaperMarketEventRecoveryReadV1, PaperMarketEventRecoveryReadErrorV1>
        + Send
        + 'static,
    ) -> Result<Self, PaperMarketEventRecoveryReadErrorV1> {
        if !is_canonical_sha256_hex(&source_identity_sha256) {
            return Err(PaperMarketEventRecoveryReadErrorV1::InvalidSourceIdentity);
        }
        Ok(Self {
            source_identity_sha256,
            read: Box::new(read),
        })
    }

    #[must_use]
    pub fn source_identity_sha256(&self) -> &Sha256Hex {
        &self.source_identity_sha256
    }

    fn read(
        &mut self,
        request: PaperMarketEventRecoveryReadRequestV1,
    ) -> Result<PaperMarketEventRecoveryReadV1, PaperMarketEventRecoveryReadErrorV1> {
        (self.read)(request)
    }
}

fn is_canonical_sha256_hex(value: &Sha256Hex) -> bool {
    value.0.len() == 64
        && value
            .0
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug)]
pub enum SimExecutionPersistenceErrorV1 {
    Store(String),
    ProcessRoot(String),
    Session(String),
    MarketEventSource(String),
    UnsupportedCheckpoint(String),
    NoCheckpoint,
    SessionNotRecoverable(String),
    SessionRetentionExpired(String),
    ResponseBridgeBindingConflict,
    RecoveredSessionAlreadyAttached(String),
    RunArtifactInputsSealed,
}

impl std::fmt::Display for SimExecutionPersistenceErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(message) => write!(formatter, "本地耐久存储失败：{message}"),
            Self::ProcessRoot(message) => write!(formatter, "恢复运行根失败：{message}"),
            Self::Session(message) => write!(formatter, "恢复会话失败：{message}"),
            Self::MarketEventSource(message) => {
                write!(formatter, "恢复市场事件源失败：{message}")
            }
            Self::UnsupportedCheckpoint(version) => {
                write!(formatter, "不支持的运行检查点版本：{version}")
            }
            Self::NoCheckpoint => write!(formatter, "本地耐久目录没有已提交检查点"),
            Self::SessionNotRecoverable(session_id) => {
                write!(formatter, "会话不属于可恢复的模拟盘会话：{session_id}")
            }
            Self::SessionRetentionExpired(session_id) => {
                write!(
                    formatter,
                    "会话已经越过保留期并从恢复集合删除：{session_id}"
                )
            }
            Self::ResponseBridgeBindingConflict => write!(formatter, "恢复桥绑定身份冲突"),
            Self::RecoveredSessionAlreadyAttached(session_id) => {
                write!(formatter, "恢复会话句柄已经被领取：{session_id}")
            }
            Self::RunArtifactInputsSealed => write!(formatter, "运行产物输入已经封口"),
        }
    }
}

impl std::error::Error for SimExecutionPersistenceErrorV1 {}

impl From<PersistenceErrorV1> for SimExecutionPersistenceErrorV1 {
    fn from(value: PersistenceErrorV1) -> Self {
        Self::Store(value.to_string())
    }
}

impl From<SessionCheckpointErrorV1> for SimExecutionPersistenceErrorV1 {
    fn from(value: SessionCheckpointErrorV1) -> Self {
        Self::Session(value.to_string())
    }
}

fn validate_paper_market_event_recovery_anchor(
    reference: &DurableMarketBucketReferenceV1,
    reader: &mut PaperMarketEventRecoveryReaderV1,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    if reference.path != MarketEventPathV1::PublicVisibility {
        return Err(SimExecutionPersistenceErrorV1::MarketEventSource(
            "模拟盘恢复读取端口只允许公开可见路径".to_owned(),
        ));
    }
    let read = reader
        .read(PaperMarketEventRecoveryReadRequestV1 {
            bucket_at_utc_ms: reference.bucket_at_utc_ms,
            start_visibility_index_position: reference.start_index_position,
            next_visibility_index_position: reference.next_index_position,
        })
        .map_err(|error| SimExecutionPersistenceErrorV1::MarketEventSource(error.to_string()))?;
    let closure = read
        .next_event_at_utc_ms
        .map_or(MillisecondBucketClosureV1::EndOfInput, |next_at| {
            MillisecondBucketClosureV1::NextEventAt(next_at)
        });
    let actual = ClosedMarketEventBucketV1::public(
        reference.bucket_at_utc_ms,
        reference.start_index_position,
        reference.next_index_position,
        closure,
        read.encoded_events,
    )
    .validated_durable_reference_for_recovery()
    .map_err(|error| {
        SimExecutionPersistenceErrorV1::MarketEventSource(format!(
            "最后公开闭桶正文无法通过规范入口：{error}"
        ))
    })?;
    if actual != *reference {
        return Err(SimExecutionPersistenceErrorV1::MarketEventSource(
            "最后公开闭桶的坐标、正文身份或闭合见证与耐久引用不一致".to_owned(),
        ));
    }
    Ok(())
}

/// 阶段 3 运行根只接受显式 `run_id`、已密封最终性策略与 `SessionSpecV2`。
///
/// 旧会话规格不存在兼容别名：
///
/// ```compile_fail,E0432
/// use sim_system::execution::SessionSpecV1;
/// ```
///
/// 旧的无运行身份、无最终性策略构造路径也不存在：
///
/// ```compile_fail,E0599
/// use std::sync::Arc;
/// use sim_system::execution::{PlatformScenarioV1, SimExecutionProcessV1};
/// use sim_system::latency_profile::EmpiricalDelayProfile;
/// use sim_system::platform_rules::SealedProcessRuleSnapshot;
/// fn old_constructor(
///     rules: Arc<SealedProcessRuleSnapshot>,
///     profile: Arc<EmpiricalDelayProfile>,
///     scenario: Arc<PlatformScenarioV1>,
/// ) {
///     let _ = SimExecutionProcessV1::new(rules, profile, scenario);
/// }
/// ```
pub struct SimExecutionProcessV1 {
    inner: SimProcessRoot,
    persistence: Option<SharedPersistentRunV1>,
    recoverable_session_ids: BTreeSet<String>,
    attached_recovered_session_ids: BTreeSet<String>,
    recovered_evicted_markets: DurableEvictedMarketsBySessionV1,
}

#[cfg(test)]
pub(crate) struct SimulationResourceDiagnosticV1 {
    pub(crate) sessions: BTreeMap<String, SimulationSessionResourceUsageV1>,
    pub(crate) run: SimulationRunResourceUsageV1,
    pub(crate) durable: SimulationDurableResourceUsageV1,
    pub(crate) high_water: SimulationResourceHighWaterV1,
}

pub struct NewPreparedSessionV2 {
    pub control: SessionCreationControlResultV2,
    pub session: SimExecutionSessionV1,
}

pub(crate) enum SessionRetirementFailureV1 {
    Rejected {
        error: SimExecutionContractErrorV1,
        session: Box<SimExecutionSessionV1>,
    },
    RecoveryRequired {
        error: SimExecutionContractErrorV1,
    },
}

struct RecoveredRunBaselineV1 {
    store: LocalDurabilityStoreV1,
    checkpoint: DurableRunCheckpointV1,
    evidence_projection: DurableEvidenceProjectionV1,
    inner: SimProcessRoot,
}

fn recover_run_baseline(
    finality_policy: Arc<TradeFinalityPolicyV1>,
    rule_snapshot: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    persistence_root: PathBuf,
) -> Result<RecoveredRunBaselineV1, SimExecutionPersistenceErrorV1> {
    if rule_snapshot.access_mode() != SnapshotAccessMode::Recovery {
        return Err(SimExecutionPersistenceErrorV1::ProcessRoot(
            "耐久恢复必须注入已经核对当前实现身份的 RECOVERY 规则快照".to_owned(),
        ));
    }
    let store = LocalDurabilityStoreV1::try_open_recovery(persistence_root)?;
    let recovered = store
        .recover::<DurableTransactionEvidenceV1, DurableRunCheckpointV1>()?
        .ok_or(SimExecutionPersistenceErrorV1::NoCheckpoint)?;
    let mut evidence_projection = DurableEvidenceProjectionV1::default();
    for evidence in &recovered.evidence {
        if evidence.body.schema_version != DURABLE_TRANSACTION_EVIDENCE_SCHEMA_V1 {
            return Err(SimExecutionPersistenceErrorV1::UnsupportedCheckpoint(
                evidence.body.schema_version.clone(),
            ));
        }
        evidence_projection.apply(&evidence.body)?;
    }
    let checkpoint = recovered.checkpoint;
    if checkpoint.schema_version != DURABLE_RUN_CHECKPOINT_SCHEMA_V1 {
        return Err(SimExecutionPersistenceErrorV1::UnsupportedCheckpoint(
            checkpoint.schema_version,
        ));
    }
    if checkpoint.execution_contract_version != SIM_EXECUTION_CONTRACT_V1 {
        return Err(SimExecutionPersistenceErrorV1::UnsupportedCheckpoint(
            checkpoint.execution_contract_version,
        ));
    }
    validate_run_intent_binding_for_rules(&checkpoint.run_intent_binding, rule_snapshot.as_ref())
        .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
    let actual_resource_policy_sha256 = checkpoint
        .resource_policy
        .content_sha256()
        .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
    checkpoint
        .resource_policy
        .validate_candidate(&SimulationResourceUsageV1::default())
        .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
    if evidence_projection.run_intent_binding.as_ref() != Some(&checkpoint.run_intent_binding)
        || evidence_projection.lifecycle != Some(checkpoint.lifecycle)
        || !is_canonical_sha256_hex(&checkpoint.market_event_source_identity_sha256)
        || evidence_projection
            .market_event_source_identity_sha256
            .as_ref()
            != Some(&checkpoint.market_event_source_identity_sha256)
        || evidence_projection.resource_policy.as_ref() != Some(&checkpoint.resource_policy)
        || evidence_projection.resource_policy_sha256.as_ref()
            != Some(&checkpoint.resource_policy_sha256)
        || evidence_projection.component_golden_measurement_sha256
            != checkpoint.component_golden_measurement_sha256
        || actual_resource_policy_sha256 != checkpoint.resource_policy_sha256
        || evidence_projection.run_resource_admission != checkpoint.run_resource_admission
        || evidence_projection.session_resource_admission != checkpoint.session_resource_admission
        || evidence_projection.resource_high_water != checkpoint.resource_high_water
    {
        return Err(SimExecutionPersistenceErrorV1::Session(
            "运行意图、生命周期、市场事件源或资源事实与检查点不一致".to_owned(),
        ));
    }
    let current_resources = store.current_resources()?;
    let current_usage = candidate_resource_usage(&checkpoint, None, current_resources)
        .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
    checkpoint
        .resource_policy
        .validate_failure_candidate(&current_usage)
        .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
    let mut checkpoint_scoped_usage = current_usage.clone();
    checkpoint_scoped_usage.durable.runtime_root.records = checkpoint_scoped_usage
        .durable
        .runtime_root
        .records
        .saturating_sub(current_usage.durable.artifacts.records);
    checkpoint_scoped_usage.durable.runtime_root.bytes = checkpoint_scoped_usage
        .durable
        .runtime_root
        .bytes
        .saturating_sub(current_usage.durable.artifacts.bytes);
    checkpoint_scoped_usage.durable.artifacts = ResourceQuantityV1::default();
    if !checkpoint
        .resource_high_water
        .covers(&checkpoint_scoped_usage)
    {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "资源高水位没有覆盖恢复时的当前资源使用量：high_water={:?}, current={current_usage:?}",
            checkpoint.resource_high_water
        )));
    }
    let inner = SimProcessRoot::restore_from_checkpoint(
        checkpoint.process_root.clone(),
        finality_policy,
        rule_snapshot,
        latency_profile,
        scenario,
    )?;
    let durable_active_session_ids = evidence_projection
        .session_creations
        .keys()
        .filter(|session_id| {
            !evidence_projection
                .completed_sessions
                .contains_key(*session_id)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    if evidence_projection.run_id.as_deref() != Some(inner.run_id())
        || evidence_projection.session_creations.len() != inner.capital_issuance_fence_count()
        || durable_active_session_ids
            != checkpoint.sessions.keys().cloned().collect::<BTreeSet<_>>()
        || inner.registered_session_count() != checkpoint.sessions.len()
    {
        return Err(SimExecutionPersistenceErrorV1::Session(
            "耐久运行、会话创建事实或检查点集合不一致".to_owned(),
        ));
    }
    for (session_id, (spec, opening_anchor, _)) in &evidence_projection.session_creations {
        if !inner.validate_durable_capital_fence(spec, opening_anchor)
            || evidence_projection
                .completed_sessions
                .contains_key(session_id)
                != inner.is_session_retired(session_id)
        {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "会话资本围栏或完成事实与运行根不一致：{session_id}"
            )));
        }
    }
    if checkpoint.lifecycle == DurableRunLifecycleV1::ArtifactInputsSealed {
        let mut artifact_projection = DurableEvidenceProjectionV1::for_artifacts();
        for evidence in &recovered.evidence {
            artifact_projection.apply(&evidence.body)?;
        }
        validate_run_artifact_seal_readiness(&inner, &checkpoint, &artifact_projection)
            .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
    }
    Ok(RecoveredRunBaselineV1 {
        store,
        checkpoint,
        evidence_projection,
        inner,
    })
}

impl SimExecutionProcessV1 {
    #[cfg(test)]
    pub(crate) fn resource_snapshot_for_tests(
        &self,
    ) -> Result<SimulationResourceDiagnosticV1, SessionError> {
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            SessionError::Durability("运行没有本地耐久状态，不能读取资源诊断".to_owned())
        })?;
        let coordinator = persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        let durable = coordinator
            .store
            .current_resources()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        let mut sessions = BTreeMap::new();
        for session_id in coordinator.current.sessions.keys() {
            let usage =
                candidate_resource_usage(&coordinator.current, Some(session_id.as_str()), durable)?;
            sessions.insert(session_id.clone(), usage.session);
        }
        let shared = candidate_resource_usage(&coordinator.current, None, durable)?;
        let mut high_water = coordinator.current.resource_high_water.clone();
        high_water.observe(&shared);
        Ok(SimulationResourceDiagnosticV1 {
            sessions,
            run: shared.run,
            durable: shared.durable,
            high_water,
        })
    }

    #[cfg(test)]
    pub(crate) fn resource_transaction_usages_for_tests(
        &self,
    ) -> Result<Vec<SimulationResourceUsageV1>, SessionError> {
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            SessionError::Durability("运行没有本地耐久状态，不能读取事务资源证据".to_owned())
        })?;
        let coordinator = persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        let recovered = coordinator
            .store
            .recover::<DurableTransactionEvidenceV1, DurableRunCheckpointV1>()
            .map_err(|error| SessionError::Durability(error.to_string()))?
            .ok_or_else(|| SessionError::Durability("运行缺少已提交耐久前缀".to_owned()))?;
        recovered
            .evidence
            .into_iter()
            .map(|entry| {
                entry
                    .body
                    .resource_usage
                    .ok_or_else(|| SessionError::Durability("耐久事务缺少资源使用量".to_owned()))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
    ) -> Result<Self, SimProcessRootErrorV1> {
        Ok(Self {
            inner: SimProcessRoot::try_new(
                run_id,
                finality_policy,
                rule_snapshot,
                latency_profile,
                scenario,
            )?,
            persistence: None,
            recoverable_session_ids: BTreeSet::new(),
            attached_recovered_session_ids: BTreeSet::new(),
            recovered_evicted_markets: BTreeMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_local_persistence(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
        run_intent_binding: SimulationRunIntentBindingV1,
        market_event_source_identity_sha256: Sha256Hex,
        persistence_root: PathBuf,
    ) -> Result<Self, SimExecutionPersistenceErrorV1> {
        Self::try_new_with_local_persistence_and_resource_policy(
            run_id,
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
            run_intent_binding,
            market_event_source_identity_sha256,
            persistence_root,
            SimulationResourcePolicyV1::component_golden_v1(),
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_local_persistence_and_resource_policy_for_tests(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
        run_intent_binding: SimulationRunIntentBindingV1,
        market_event_source_identity_sha256: Sha256Hex,
        persistence_root: PathBuf,
        resource_policy: SimulationResourcePolicyV1,
    ) -> Result<Self, SimExecutionPersistenceErrorV1> {
        Self::try_new_with_local_persistence_and_resource_policy(
            run_id,
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
            run_intent_binding,
            market_event_source_identity_sha256,
            persistence_root,
            resource_policy,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_with_local_persistence_and_resource_policy(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
        run_intent_binding: SimulationRunIntentBindingV1,
        market_event_source_identity_sha256: Sha256Hex,
        persistence_root: PathBuf,
        resource_policy: SimulationResourcePolicyV1,
    ) -> Result<Self, SimExecutionPersistenceErrorV1> {
        if rule_snapshot.access_mode() == SnapshotAccessMode::Recovery {
            return Err(SimExecutionPersistenceErrorV1::ProcessRoot(
                "新的耐久运行不能使用 RECOVERY 规则快照".to_owned(),
            ));
        }
        if !is_canonical_sha256_hex(&market_event_source_identity_sha256) {
            return Err(SimExecutionPersistenceErrorV1::MarketEventSource(
                PaperMarketEventRecoveryReadErrorV1::InvalidSourceIdentity.to_string(),
            ));
        }
        validate_run_intent_binding_for_rules(&run_intent_binding, rule_snapshot.as_ref())
            .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
        let inner = SimProcessRoot::try_new(
            run_id.clone(),
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
        )
        .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
        let store = LocalDurabilityStoreV1::try_new(persistence_root)?;
        if store.recover_json()?.is_some() {
            return Err(SimExecutionPersistenceErrorV1::Store(
                "新运行的本地耐久目录已经包含 CURRENT".to_owned(),
            ));
        }
        let resource_policy_sha256 = resource_policy
            .content_sha256()
            .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
        let mut checkpoint = DurableRunCheckpointV1 {
            schema_version: DURABLE_RUN_CHECKPOINT_SCHEMA_V1.to_owned(),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_source_identity_sha256: market_event_source_identity_sha256.clone(),
            run_intent_binding: run_intent_binding.clone(),
            resource_policy: resource_policy.clone(),
            resource_policy_sha256: resource_policy_sha256.clone(),
            component_golden_measurement_sha256: None,
            resource_high_water: SimulationResourceHighWaterV1::default(),
            run_resource_admission: ResourceAdmissionStateV1::default(),
            session_resource_admission: BTreeMap::new(),
            lifecycle: DurableRunLifecycleV1::Open,
            process_root: inner.checkpoint()?,
            sessions: BTreeMap::new(),
        };
        let mut evidence =
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RunCreated {
                run_id: run_id.clone(),
                market_event_source_identity_sha256,
                run_intent_binding,
                resource_policy: resource_policy.clone(),
                resource_policy_sha256,
            });
        let mut coordinator = PersistentRunCoordinatorV1 {
            store,
            current: checkpoint.clone(),
            recovery_required: None,
            transient_input_by_session: BTreeMap::new(),
        };
        let usage = coordinator
            .observe_candidate_resources(&mut evidence, &mut checkpoint)
            .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
        resource_policy
            .validate_candidate(&usage)
            .map_err(|error| SimExecutionPersistenceErrorV1::ProcessRoot(error.to_string()))?;
        coordinator.store.commit(&evidence, &checkpoint)?;
        coordinator.current = checkpoint;
        Ok(Self {
            inner,
            persistence: Some(Arc::new(Mutex::new(coordinator))),
            recoverable_session_ids: BTreeSet::new(),
            attached_recovered_session_ids: BTreeSet::new(),
            recovered_evicted_markets: BTreeMap::new(),
        })
    }

    pub fn recover_paper_from_local_persistence(
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
        mut market_event_reader: PaperMarketEventRecoveryReaderV1,
        persistence_root: PathBuf,
    ) -> Result<Self, SimExecutionPersistenceErrorV1> {
        let RecoveredRunBaselineV1 {
            store,
            checkpoint,
            evidence_projection,
            inner,
        } = recover_run_baseline(
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
            persistence_root,
        )?;
        if market_event_reader.source_identity_sha256()
            != &checkpoint.market_event_source_identity_sha256
        {
            return Err(SimExecutionPersistenceErrorV1::MarketEventSource(
                "恢复读取端口、运行创建事实与检查点没有绑定同一市场事件源身份".to_owned(),
            ));
        }
        if evidence_projection
            .session_creations
            .values()
            .any(|(spec, _, _)| spec.caller_role != SimCallerRoleV1::Paper)
        {
            return Err(SimExecutionPersistenceErrorV1::Session(
                "模拟盘恢复事实包含非模拟盘会话".to_owned(),
            ));
        }
        let mut recoverable_session_ids = BTreeSet::new();
        for (session_id, session_checkpoint) in &checkpoint.sessions {
            if session_id != session_checkpoint.session.session_id() {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "检查点会话键与正文身份不符：{session_id}"
                )));
            }
            let (spec, opening_anchor, created_initialization) = evidence_projection
                .session_creations
                .get(session_id)
                .ok_or_else(|| {
                    SimExecutionPersistenceErrorV1::Session(format!(
                        "检查点会话缺少仅追加创建事实：{session_id}"
                    ))
                })?;
            if !inner.validate_durable_session_creation(
                spec,
                opening_anchor,
                created_initialization,
            ) {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "会话创建事实与运行根不一致：{session_id}"
                )));
            }
            let current_initialization = inner
                .current_initialization_for_session(session_id)
                .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
            let expected_initialization = evidence_projection
                .initialization_results
                .get(session_id)
                .unwrap_or(created_initialization);
            if &current_initialization != expected_initialization {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "会话初始化事实与运行根不一致：{session_id}"
                )));
            }
            let evicted_markets = evidence_projection
                .evicted_markets
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            let (state, downstream_confirmation) = ExecutionSessionStateV1::restore(
                &inner,
                session_checkpoint.clone(),
                &evicted_markets,
            )?;
            if state.session.session_spec().caller_role != SimCallerRoleV1::Paper {
                return Err(SimExecutionPersistenceErrorV1::Session(
                    "本地恢复检查点包含非模拟盘会话".to_owned(),
                ));
            }
            if state.accepting_new_ingress
                == evidence_projection
                    .closed_ingress_sessions
                    .contains(session_id)
            {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "耐久入口关闭事实与检查点状态不一致：{session_id}"
                )));
            }
            let durable_query_revision = evidence_projection
                .query_projection_revisions
                .get(session_id)
                .copied()
                .unwrap_or(0);
            if state.session.platform_state_revision() != durable_query_revision {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "查询投影修订号与仅追加事实不一致：{session_id}"
                )));
            }
            let durable_logical_platform_through = evidence_projection
                .logical_platform_driven_through_by_session
                .get(session_id)
                .copied();
            let durable_private_schedule_through = evidence_projection
                .private_fact_schedule_closed_through_by_session
                .get(session_id)
                .copied();
            if session_checkpoint.session.logical_platform_driven_through()
                != durable_logical_platform_through
                || session_checkpoint
                    .session
                    .private_fact_schedule_closed_through()
                    != durable_private_schedule_through
            {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "逻辑平台或私有事实计划水位与仅追加事实不一致：{session_id}"
                )));
            }
            let durable_query_retention = evidence_projection
                .query_retention
                .get(session_id)
                .copied()
                .unwrap_or((1, None, None));
            if state.query_state.retention_projection() != durable_query_retention {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "查询保留水位与仅追加事实不一致：{session_id}"
                )));
            }
            let expected_enrollments = evidence_projection
                .market_enrollments
                .get(session_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if state.session.durable_market_enrollment_chain() != expected_enrollments {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "仅追加市场登记事实与检查点链不一致：{session_id}"
                )));
            }
            let expected_requests = evidence_projection
                .requests
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            if expected_requests.len() != state.pending_requests.len() {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "仅追加请求事实与检查点请求集合不一致：{session_id}"
                )));
            }
            for (request_id, registration) in &expected_requests {
                if state.pending_requests.get(request_id) != Some(&registration.request) {
                    return Err(SimExecutionPersistenceErrorV1::Session(format!(
                        "仅追加请求正文与检查点不一致：{request_id}"
                    )));
                }
                state.session.validate_recovered_request_schedule(
                    request_id,
                    &registration.transit,
                    &registration.actions,
                )?;
            }
            validate_durable_logical_action_results(
                session_id,
                &state,
                &expected_requests,
                evidence_projection.logical_action_results.get(session_id),
                evidence_projection
                    .logical_action_result_contexts
                    .get(session_id),
                evidence_projection.private_output_drafts.get(session_id),
            )?;
            let buckets = evidence_projection.last_platform_buckets.get(session_id);
            validate_durable_bucket_paths_for_recovery(
                session_id,
                state.session.session_spec().caller_role,
                buckets,
            )?;
            for path in [
                MarketEventPathV1::HiddenPlatform,
                MarketEventPathV1::PublicVisibility,
            ] {
                let reference = buckets.and_then(|buckets| buckets.get(&path));
                let path_is_applicable = path != MarketEventPathV1::HiddenPlatform
                    || state.session.session_spec().caller_role != SimCallerRoleV1::Paper;
                if path_is_applicable
                    && !state.session.durable_bucket_state_matches(path, reference)
                {
                    return Err(SimExecutionPersistenceErrorV1::Session(format!(
                        "最后平台桶事实与输入游标不一致：{session_id}:{path:?}"
                    )));
                }
                if path == MarketEventPathV1::PublicVisibility
                    && let Some(reference) = reference
                {
                    validate_paper_market_event_recovery_anchor(
                        reference,
                        &mut market_event_reader,
                    )?;
                }
            }
            let checkpoint_deltas = account_deltas_in_checkpoint(session_checkpoint)?;
            let evidence_deltas = evidence_projection
                .account_deltas
                .get(session_id)
                .cloned()
                .unwrap_or_default();
            if checkpoint_deltas != evidence_deltas {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "仅追加账户变化事实与检查点不一致：{session_id}"
                )));
            }
            validate_durable_output_projection(
                session_id,
                &state,
                downstream_confirmation.as_ref(),
                evidence_projection.prepared_outputs.get(session_id),
                evidence_projection.confirmed_outputs.get(session_id),
                evidence_projection.private_output_drafts.get(session_id),
                evidence_projection.downstream_durable.get(session_id),
            )?;
            if checkpoint.lifecycle == DurableRunLifecycleV1::Open {
                recoverable_session_ids.insert(session_id.clone());
            }
        }
        Ok(Self {
            inner,
            persistence: Some(Arc::new(Mutex::new(PersistentRunCoordinatorV1 {
                store,
                current: checkpoint,
                recovery_required: None,
                transient_input_by_session: BTreeMap::new(),
            }))),
            recoverable_session_ids,
            attached_recovered_session_ids: BTreeSet::new(),
            recovered_evicted_markets: evidence_projection.evicted_markets,
        })
    }

    pub fn recover_for_artifact_publication(
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
        persistence_root: PathBuf,
    ) -> Result<Self, SimExecutionPersistenceErrorV1> {
        let RecoveredRunBaselineV1 {
            store,
            checkpoint,
            evidence_projection: _,
            inner,
        } = recover_run_baseline(
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
            persistence_root,
        )?;
        let recovered = store
            .recover::<DurableTransactionEvidenceV1, DurableRunCheckpointV1>()?
            .ok_or(SimExecutionPersistenceErrorV1::NoCheckpoint)?;
        let mut artifact_projection = DurableEvidenceProjectionV1::for_artifacts();
        for evidence in &recovered.evidence {
            artifact_projection.apply(&evidence.body)?;
        }
        validate_run_artifact_seal_readiness(&inner, &checkpoint, &artifact_projection)
            .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;
        validate_artifact_publication_recovery_eligibility(&artifact_projection)
            .map_err(|error| SimExecutionPersistenceErrorV1::Session(error.to_string()))?;

        let mut coordinator = PersistentRunCoordinatorV1 {
            store,
            current: checkpoint,
            recovery_required: None,
            transient_input_by_session: BTreeMap::new(),
        };
        coordinator
            .seal_artifact_inputs(inner.run_id())
            .map_err(|error| SimExecutionPersistenceErrorV1::Store(error.to_string()))?;
        Ok(Self {
            inner,
            persistence: Some(Arc::new(Mutex::new(coordinator))),
            recoverable_session_ids: BTreeSet::new(),
            attached_recovered_session_ids: BTreeSet::new(),
            recovered_evicted_markets: BTreeMap::new(),
        })
    }

    pub fn reattach_recovered_paper_session(
        &mut self,
        session_id: &str,
        response_bridge_binding_id: &str,
        response_bridge: SubmissionResponseBridgeV1,
    ) -> Result<SimExecutionSessionV1, SimExecutionPersistenceErrorV1> {
        if let Some(persistence) = &self.persistence {
            let coordinator = persistence.lock().map_err(|_| {
                SimExecutionPersistenceErrorV1::Store("耐久协调器锁不可用".to_owned())
            })?;
            if coordinator.current.lifecycle == DurableRunLifecycleV1::ArtifactInputsSealed {
                return Err(SimExecutionPersistenceErrorV1::RunArtifactInputsSealed);
            }
        }
        if self.attached_recovered_session_ids.contains(session_id) {
            return Err(
                SimExecutionPersistenceErrorV1::RecoveredSessionAlreadyAttached(
                    session_id.to_owned(),
                ),
            );
        }
        if !self.recoverable_session_ids.contains(session_id) {
            if self.inner.is_session_retired(session_id) {
                return Err(SimExecutionPersistenceErrorV1::SessionRetentionExpired(
                    session_id.to_owned(),
                ));
            }
            return Err(SimExecutionPersistenceErrorV1::SessionNotRecoverable(
                session_id.to_owned(),
            ));
        }
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            SimExecutionPersistenceErrorV1::Session("运行没有本地耐久状态".to_owned())
        })?;
        let checkpoint = persistence
            .lock()
            .map_err(|_| SimExecutionPersistenceErrorV1::Store("耐久协调器锁不可用".to_owned()))?
            .current
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                SimExecutionPersistenceErrorV1::SessionNotRecoverable(session_id.to_owned())
            })?;
        let evicted_markets = self
            .recovered_evicted_markets
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        let (state, downstream_durable_confirmation) =
            ExecutionSessionStateV1::restore(&self.inner, checkpoint, &evicted_markets)?;
        if state.session.session_spec().response_bridge_binding_id != response_bridge_binding_id {
            return Err(SimExecutionPersistenceErrorV1::ResponseBridgeBindingConflict);
        }
        self.attached_recovered_session_ids
            .insert(session_id.to_owned());
        self.recovered_evicted_markets.remove(session_id);
        Ok(SimExecutionSessionV1::from_recovered_state(
            state,
            response_bridge,
            Some(Arc::clone(persistence)),
            downstream_durable_confirmation,
        ))
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        self.inner.run_id()
    }

    #[must_use]
    pub fn finality_policy(&self) -> &Arc<TradeFinalityPolicyV1> {
        self.inner.finality_policy()
    }

    pub(crate) fn record_component_golden_measurement(
        &mut self,
        measurement: ComponentGoldenRunMeasurementV1,
    ) -> Result<Sha256Hex, artifacts::SimulationRunArtifactErrorV1> {
        measurement.validate().map_err(|error| {
            artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
        })?;
        if measurement.run_id != self.inner.run_id() {
            return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                "封口前测量正文与运行身份不一致".to_owned(),
            ));
        }
        let measurement_sha256 = measurement.content_sha256().map_err(|error| {
            artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
        })?;
        let persistence = self
            .persistence
            .as_ref()
            .ok_or(artifacts::SimulationRunArtifactErrorV1::RunNotPersistent)?;
        let mut coordinator = persistence.lock().map_err(|_| {
            artifacts::SimulationRunArtifactErrorV1::Durability("耐久协调器锁不可用".to_owned())
        })?;
        coordinator
            .require_business_writes_open()
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
            })?;
        if coordinator.current.run_intent_binding.workload_kind
            != artifacts::SimulationWorkloadKindV1::ComponentGolden
            || coordinator
                .current
                .run_intent_binding
                .deterministic_script_contract
                != artifacts::SimulationDeterministicScriptContractV1::ComponentGoldenV1
        {
            return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                "只有 COMPONENT_GOLDEN_V1 运行可以记录组件黄金测量正文".to_owned(),
            ));
        }
        if let Some(existing) = coordinator
            .current
            .component_golden_measurement_sha256
            .as_ref()
        {
            return if existing == &measurement_sha256 {
                Ok(existing.clone())
            } else {
                Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                    "同一运行已经绑定不同的组件黄金测量正文".to_owned(),
                ))
            };
        }
        if !coordinator.current.sessions.is_empty() {
            return Err(artifacts::SimulationRunArtifactErrorV1::RunNotReady(
                "组件黄金测量只能在两条轨迹完成后记录",
            ));
        }
        let mut candidate = coordinator.current.clone();
        candidate.component_golden_measurement_sha256 = Some(measurement_sha256.clone());
        let evidence = DurableTransactionEvidenceV1::new(
            DurableTransactionFactV1::ComponentGoldenMeasurementRecorded {
                run_id: self.inner.run_id().to_owned(),
                measurement: Box::new(measurement),
                measurement_sha256: measurement_sha256.clone(),
            },
        );
        match coordinator.commit(&evidence, candidate) {
            Ok(()) => Ok(measurement_sha256),
            Err(SessionError::ResourceLimitReached(limit)) => {
                Err(artifacts::SimulationRunArtifactErrorV1::ResourceLimitReached(limit))
            }
            Err(SessionError::DurabilityRecoveryRequired(message)) => {
                Err(artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(message))
            }
            Err(error) => Err(artifacts::SimulationRunArtifactErrorV1::Durability(
                error.to_string(),
            )),
        }
    }

    pub fn seal_run_artifacts(
        &mut self,
    ) -> Result<artifacts::SimulationRunArtifactReceiptV1, artifacts::SimulationRunArtifactErrorV1>
    {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or(artifacts::SimulationRunArtifactErrorV1::RunNotPersistent)?;
        let mut coordinator = persistence.lock().map_err(|_| {
            artifacts::SimulationRunArtifactErrorV1::Durability("耐久协调器锁不可用".to_owned())
        })?;
        coordinator
            .require_recovery_not_required()
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(
                    error.to_string(),
                )
            })?;
        let current_process_root = self.inner.checkpoint().map_err(|error| {
            artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
        })?;
        if coordinator.current.process_root != current_process_root {
            return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                "运行根内存基线与耐久检查点不一致".to_owned(),
            ));
        }
        if coordinator.current.lifecycle == DurableRunLifecycleV1::Open {
            let recovered = coordinator
                .store
                .recover::<DurableTransactionEvidenceV1, DurableRunCheckpointV1>()
                .map_err(|error| {
                    artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
                })?
                .ok_or_else(|| {
                    artifacts::SimulationRunArtifactErrorV1::Integrity(
                        "耐久运行缺少已提交前缀".to_owned(),
                    )
                })?;
            if recovered.checkpoint != coordinator.current {
                return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                    "封口前磁盘检查点与耐久协调器基线不一致".to_owned(),
                ));
            }
            let mut projection = DurableEvidenceProjectionV1::for_artifacts();
            for evidence in &recovered.evidence {
                projection.apply(&evidence.body).map_err(|error| {
                    artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
                })?;
            }
            validate_run_artifact_seal_readiness(&self.inner, &coordinator.current, &projection)?;
            coordinator.seal_artifact_inputs(self.inner.run_id())?;
        }
        self.recoverable_session_ids.clear();
        self.attached_recovered_session_ids.clear();
        self.recovered_evicted_markets.clear();
        let recovered = coordinator
            .store
            .recover::<DurableTransactionEvidenceV1, DurableRunCheckpointV1>()
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
            })?
            .ok_or_else(|| {
                artifacts::SimulationRunArtifactErrorV1::Integrity(
                    "封口运行缺少已提交证据前缀".to_owned(),
                )
            })?;
        if recovered.checkpoint != coordinator.current {
            return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                "封口后的磁盘检查点与耐久协调器基线不一致".to_owned(),
            ));
        }
        let existing_artifact_identity = coordinator
            .store
            .existing_artifact_bundle_identity()
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
            })?;
        let mut resource_high_water = coordinator.current.resource_high_water.clone();
        let bundle = {
            let mut converged = None;
            for _ in 0..16 {
                let candidate =
                    prepare_run_artifact_bundle(&self.inner, &recovered, &resource_high_water)?;
                if let Some(existing_identity) = existing_artifact_identity.as_ref()
                    && &candidate.manifest_sha256 != existing_identity
                {
                    let preview = coordinator.store.current_resources().map_err(|error| {
                        artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
                    })?;
                    let usage = candidate_resource_usage(&coordinator.current, None, preview)
                        .map_err(|error| {
                            artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string())
                        })?;
                    let before = resource_high_water.clone();
                    resource_high_water.observe(&usage);
                    if resource_high_water == before {
                        return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                            "既有运行产物身份不能由当前封口证据和资源高水位重算".to_owned(),
                        ));
                    }
                    continue;
                }
                let preview = coordinator
                    .store
                    .preview_artifact_bundle_resources(
                        &candidate.manifest_sha256,
                        &candidate.members,
                    )
                    .map_err(|error| {
                        artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
                    })?;
                let usage = candidate_resource_usage(&coordinator.current, None, preview).map_err(
                    |error| artifacts::SimulationRunArtifactErrorV1::Integrity(error.to_string()),
                )?;
                if let Err(error) = coordinator
                    .current
                    .resource_policy
                    .validate_candidate(&usage)
                {
                    if let Some(limit) = error.limit_reached().cloned() {
                        if let Some(candidate_identity) = coordinator
                            .store
                            .existing_artifact_candidate_identity()
                            .map_err(|error| {
                                artifacts::SimulationRunArtifactErrorV1::Durability(
                                    error.to_string(),
                                )
                            })?
                        {
                            coordinator
                                .store
                                .remove_validated_artifact_candidate(
                                    &candidate_identity,
                                    &candidate.members,
                                )
                                .map_err(|error| {
                                    artifacts::SimulationRunArtifactErrorV1::Durability(
                                        error.to_string(),
                                    )
                                })?;
                        }
                        let persisted = coordinator.persist_resource_limit(None, limit);
                        return match persisted {
                            Err(SessionError::ResourceLimitReached(persisted)) => Err(
                                artifacts::SimulationRunArtifactErrorV1::ResourceLimitReached(
                                    persisted,
                                ),
                            ),
                            Err(SessionError::DurabilityRecoveryRequired(message)) => Err(
                                artifacts::SimulationRunArtifactErrorV1::DurabilityRecoveryRequired(
                                    message,
                                ),
                            ),
                            Err(error) => Err(artifacts::SimulationRunArtifactErrorV1::Durability(
                                error.to_string(),
                            )),
                            Ok(()) => Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                                "资源超限事实提交意外返回成功".to_owned(),
                            )),
                        };
                    }
                    return Err(artifacts::SimulationRunArtifactErrorV1::Integrity(
                        error.to_string(),
                    ));
                }
                let before = resource_high_water.clone();
                resource_high_water.observe(&usage);
                if resource_high_water == before {
                    converged = Some(candidate);
                    break;
                }
            }
            converged.ok_or_else(|| {
                artifacts::SimulationRunArtifactErrorV1::Integrity(
                    "运行产物资源高水位与固定成员字节没有收敛".to_owned(),
                )
            })?
        };
        let artifact_directory = coordinator
            .store
            .publish_immutable_artifact_bundle(&bundle.manifest_sha256, &bundle.members)
            .map_err(|error| {
                artifacts::SimulationRunArtifactErrorV1::Durability(error.to_string())
            })?;
        Ok(artifacts::SimulationRunArtifactReceiptV1 {
            manifest_sha256: bundle.manifest_sha256,
            artifact_directory,
            through_commit_sequence: bundle.through_commit_sequence,
            evidence_head_sha256: bundle.evidence_head_sha256,
        })
    }

    pub fn create_session(
        &mut self,
        spec: SessionSpecV2,
        response_bridge: SubmissionResponseBridgeV1,
    ) -> Result<NewPreparedSessionV2, SimExecutionContractErrorV1> {
        if let Some(persistence) = &self.persistence {
            persistence
                .lock()
                .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?
                .require_new_ingress_open(Some(&spec.session_id))?;
            let mode_matches_role = match spec.caller_role {
                SimCallerRoleV1::Paper => {
                    matches!(
                        self.inner.rule_snapshot().access_mode(),
                        SnapshotAccessMode::CurrentQuery | SnapshotAccessMode::Recovery
                    )
                }
                SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                    self.inner.rule_snapshot().access_mode() == SnapshotAccessMode::HistoricalAsOf
                }
            };
            if !mode_matches_role {
                return Err(SessionError::InvalidSessionSpec(
                    "耐久会话角色与规则快照访问模式不匹配",
                ));
            }
        }
        let response_outbox_capacity = spec.response_outbox_capacity;
        let mut candidate_root = self.inner.clone();
        let PreparedSimSessionV2 { session, control } = candidate_root.create_session(spec)?;
        let session_id = session.session_spec().session_id.clone();
        let state = ExecutionSessionStateV1 {
            session,
            pending_requests: BTreeMap::new(),
            mutation_payloads: BTreeMap::new(),
            submission_effect_records: BTreeMap::new(),
            cancel_effect_records: BTreeMap::new(),
            query_state: QueryStateV1::new(),
            direct_response_outbox: LogicalDirectResponseOutboxV1::new(response_outbox_capacity),
            lifecycle_outbox: LogicalLifecycleOutboxV1::new(response_outbox_capacity),
            compacted_confirmed_private_deliveries: BTreeMap::new(),
            confirmed_private_deliveries: BTreeMap::new(),
            accepting_new_ingress: true,
        };
        if let Some(persistence) = &self.persistence {
            let mut coordinator = persistence
                .lock()
                .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
            let current_process_root = self
                .inner
                .checkpoint()
                .map_err(|error| SessionError::Durability(error.to_string()))?;
            if coordinator.current.process_root != current_process_root
                || coordinator.current.sessions.contains_key(&session_id)
            {
                return Err(SessionError::Durability(
                    "运行根内存基线与当前耐久检查点不一致".to_owned(),
                ));
            }
            let mut checkpoint = coordinator.current.clone();
            checkpoint.process_root = candidate_root
                .checkpoint()
                .map_err(|error| SessionError::Durability(error.to_string()))?;
            checkpoint
                .sessions
                .insert(session_id.clone(), state.checkpoint(None));
            let evidence =
                DurableTransactionEvidenceV1::new(DurableTransactionFactV1::SessionCreated {
                    session_spec: state.session.session_spec().clone(),
                    opening_anchor: Box::new(control.opening_anchor.clone()),
                    initialization: control.initialization.clone(),
                });
            coordinator.commit_new_ingress(&evidence, checkpoint)?;
        }
        self.inner = candidate_root;
        Ok(NewPreparedSessionV2 {
            control,
            session: SimExecutionSessionV1::from_recovered_state(
                state,
                response_bridge,
                self.persistence.clone(),
                None,
            ),
        })
    }

    pub fn retry_create_session(
        &self,
        spec: &SessionSpecV2,
    ) -> Result<SessionCreationControlResultV2, SimExecutionContractErrorV1> {
        self.inner.retry_create_session(spec)
    }

    pub fn activate_session(
        &mut self,
        request: ActivateSessionRequestV1,
    ) -> Result<SessionInitializationControlResultV1, SimExecutionContractErrorV1> {
        if self.persistence.is_none() {
            return self.inner.activate_session(request);
        }
        let candidate_root = self.inner.detached_clone()?;
        let result = candidate_root.activate_session(request.clone())?;
        let persistence = self.persistence.as_ref().expect("已检查持久协调器");
        let mut coordinator = persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        let current_process_root = self
            .inner
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        if coordinator.current.process_root != current_process_root {
            return Err(SessionError::Durability(
                "运行根内存基线与当前耐久检查点不一致".to_owned(),
            ));
        }
        let mut checkpoint = coordinator.current.clone();
        checkpoint.process_root = candidate_root
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        coordinator.commit(
            &DurableTransactionEvidenceV1::new(DurableTransactionFactV1::SessionActivated {
                request: request.clone(),
                result: result.snapshot.clone(),
            }),
            checkpoint,
        )?;
        let committed = self.inner.activate_session(request)?;
        debug_assert_eq!(committed, result);
        Ok(committed)
    }

    pub fn fail_session_initialization(
        &mut self,
        request: FailSessionInitializationRequestV1,
    ) -> Result<SessionInitializationControlResultV1, SimExecutionContractErrorV1> {
        if self.persistence.is_none() {
            return self.inner.fail_session_initialization(request);
        }
        let candidate_root = self.inner.detached_clone()?;
        let result = candidate_root.fail_session_initialization(request.clone())?;
        let persistence = self.persistence.as_ref().expect("已检查持久协调器");
        let mut coordinator = persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        let current_process_root = self
            .inner
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        if coordinator.current.process_root != current_process_root {
            return Err(SessionError::Durability(
                "运行根内存基线与当前耐久检查点不一致".to_owned(),
            ));
        }
        let mut checkpoint = coordinator.current.clone();
        checkpoint.process_root = candidate_root
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        coordinator.commit(
            &DurableTransactionEvidenceV1::new(
                DurableTransactionFactV1::SessionInitializationFailed {
                    request: request.clone(),
                    result: result.snapshot.clone(),
                },
            ),
            checkpoint,
        )?;
        let committed = self.inner.fail_session_initialization(request)?;
        debug_assert_eq!(committed, result);
        Ok(committed)
    }

    pub fn session_initialization(
        &self,
        opening_anchor: &AccountOpeningAnchorV1,
    ) -> Result<SessionInitializationSnapshotV1, SimExecutionContractErrorV1> {
        self.inner.session_initialization(opening_anchor)
    }

    pub fn enroll_market(
        &self,
        session: &mut SimExecutionSessionV1,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV1, SimExecutionContractErrorV1> {
        let mut candidate_session = session.session.clone();
        let result = self
            .inner
            .enroll_market(&mut candidate_session, request.clone())?;
        let mut candidate_checkpoint = session.durable_checkpoint();
        candidate_checkpoint.session = candidate_session.checkpoint();
        let session_id = session.session_spec().session_id.clone();
        session.commit_durable_candidate_new_ingress(
            candidate_checkpoint,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::MarketEnrolled {
                session_id,
                enrollment: result.clone(),
            }),
        )?;
        session.session = candidate_session;
        Ok(result)
    }

    pub fn advance_downstream_durability(
        &self,
        session: &mut SimExecutionSessionV1,
        proof: DownstreamDurabilityProofV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let (Some(process_persistence), Some(session_persistence)) =
            (&self.persistence, &session.persistence)
        else {
            return Err(SessionError::Durability(
                "下游耐久水位只能由启用同一本地耐久根的组合根推进".to_owned(),
            ));
        };
        if !Arc::ptr_eq(process_persistence, session_persistence) {
            return Err(SessionError::Durability(
                "会话不属于当前本地耐久运行根".to_owned(),
            ));
        }
        session.advance_downstream_durability(proof)
    }

    pub(crate) fn evict_terminal_market(
        &self,
        session: &mut SimExecutionSessionV1,
        condition_id: &str,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let (Some(process_persistence), Some(session_persistence)) =
            (&self.persistence, &session.persistence)
        else {
            return Err(SessionError::Durability(
                "终态市场热状态只能从启用同一本地耐久根的组合根驱逐".to_owned(),
            ));
        };
        if !Arc::ptr_eq(process_persistence, session_persistence) {
            return Err(SessionError::Durability(
                "会话不属于当前本地耐久运行根".to_owned(),
            ));
        }
        if !session.session.is_quiescent_for_retention()
            || !session.direct_response_outbox.entries.is_empty()
            || !session.lifecycle_outbox.entries.is_empty()
            || !session.lifecycle_outbox.batches.is_empty()
        {
            return Err(SessionError::RetentionBarrierNotSatisfied(
                "仍有待处理动作、请求响应或生命周期输出",
            ));
        }
        let confirmed_through = session.session.private_feedback_sequence();
        let downstream_through = session
            .downstream_durable_confirmation
            .as_ref()
            .map_or(0, |confirmation| confirmation.private_feedback_sequence);
        if downstream_through != confirmed_through
            || !session.confirmed_private_deliveries.is_empty()
        {
            return Err(SessionError::RetentionBarrierNotSatisfied(
                "强下游耐久水位尚未越过全部已确认输出",
            ));
        }
        if !session
            .query_state
            .terminal_market_projection_matches(condition_id, session.session.execution_state())
            .map_err(|error| SessionError::ExecutionState(format!("{error:?}")))?
        {
            return Err(SessionError::RetentionBarrierNotSatisfied(
                "终态紧凑查询投影尚未与待删除热状态精确闭合",
            ));
        }
        let public_lifecycle_phase = session
            .session
            .public_markets()
            .market(condition_id)
            .map(MarketStateV1::lifecycle_phase)
            .ok_or(SessionError::RetentionBarrierNotSatisfied(
                "终态市场热状态不存在或已经驱逐",
            ))?;
        let lifecycle_phase = match session.session.session_spec().caller_role {
            SimCallerRoleV1::Paper => public_lifecycle_phase,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                let hidden_lifecycle_phase = session
                    .session
                    .hidden_platform_markets()
                    .and_then(|markets| markets.market(condition_id))
                    .map(MarketStateV1::lifecycle_phase)
                    .ok_or(SessionError::RetentionBarrierNotSatisfied(
                        "隐藏终态市场热状态不存在或已经驱逐",
                    ))?;
                if hidden_lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal
                    || public_lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal
                {
                    return Err(SessionError::RetentionBarrierNotSatisfied(
                        "训练或离线市场的隐藏与公开终态尚未共同闭合",
                    ));
                }
                hidden_lifecycle_phase
            }
        };
        let expected = session.durable_checkpoint();
        let mut candidate_state = session.runtime_state();
        let (terminal_orders, terminal_trades) = candidate_state
            .query_state
            .terminal_market_projections(condition_id);
        let finality_policy = Arc::clone(candidate_state.session.finality_policy());
        let execution_witness = candidate_state
            .session
            .execution_state_mut()
            .evict_terminal_market_hot_state(
                condition_id,
                lifecycle_phase,
                finality_policy.as_ref(),
            )
            .map_err(SessionError::from)?;
        candidate_state
            .session
            .public_markets_mut()
            .evict_terminal_market(condition_id)
            .map_err(|error| SessionError::ExecutionState(error.to_string()))?;
        if let Some(hidden_markets) = candidate_state.session.hidden_platform_markets_mut() {
            hidden_markets
                .evict_terminal_market(condition_id)
                .map_err(|error| SessionError::ExecutionState(error.to_string()))?;
        }
        let candidate = candidate_state.checkpoint(session.downstream_durable_confirmation.clone());
        session.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::MarketHotStateEvicted {
                session_id: session.session.session_spec().session_id.clone(),
                condition_id: condition_id.to_owned(),
                terminal_orders,
                terminal_trades,
                execution_witness: Box::new(execution_witness),
            }),
        )?;
        session.replace_runtime_state(candidate_state);
        Ok(())
    }

    pub(crate) fn retire_session(
        &mut self,
        mut session: SimExecutionSessionV1,
    ) -> Result<(), SessionRetirementFailureV1> {
        match self.commit_session_completion(
            &mut session,
            DurableSessionCompletionKindV1::PaperRetentionExpired,
        ) {
            Ok(()) => Ok(()),
            Err(error @ SessionError::DurabilityRecoveryRequired(_)) => {
                Err(SessionRetirementFailureV1::RecoveryRequired { error })
            }
            Err(error) => Err(SessionRetirementFailureV1::Rejected {
                error,
                session: Box::new(session),
            }),
        }
    }

    pub(crate) fn complete_clean_run_session(
        &mut self,
        mut session: SimExecutionSessionV1,
    ) -> Result<(), SessionRetirementFailureV1> {
        match self.commit_session_completion(
            &mut session,
            DurableSessionCompletionKindV1::CleanRunCompleted,
        ) {
            Ok(()) => Ok(()),
            Err(error @ SessionError::DurabilityRecoveryRequired(_)) => {
                Err(SessionRetirementFailureV1::RecoveryRequired { error })
            }
            Err(error) => Err(SessionRetirementFailureV1::Rejected {
                error,
                session: Box::new(session),
            }),
        }
    }

    fn commit_session_completion(
        &mut self,
        session: &mut SimExecutionSessionV1,
        completion_kind: DurableSessionCompletionKindV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let (Some(process_persistence), Some(session_persistence)) =
            (&self.persistence, &session.persistence)
        else {
            return Err(SessionError::Durability(
                "会话完成只能由启用同一本地耐久根的组合根提交".to_owned(),
            ));
        };
        if !Arc::ptr_eq(process_persistence, session_persistence) {
            return Err(SessionError::Durability(
                "会话不属于当前本地耐久运行根".to_owned(),
            ));
        }
        session.session.require_active()?;
        let session_id = session.session.session_spec().session_id.clone();
        let all_public_markets_evicted = session
            .session
            .public_markets()
            .evicted_condition_ids()
            .count()
            == session.session.market_directory().markets().len();
        let all_hidden_markets_evicted =
            session
                .session
                .hidden_platform_markets()
                .is_none_or(|markets| {
                    markets.evicted_condition_ids().count()
                        == session.session.market_directory().markets().len()
                });
        let role_matches_completion = matches!(
            (session.session.session_spec().caller_role, completion_kind),
            (
                SimCallerRoleV1::Paper,
                DurableSessionCompletionKindV1::PaperRetentionExpired
            ) | (
                SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation,
                DurableSessionCompletionKindV1::CleanRunCompleted
            )
        );
        let input_paths_exhausted = match session.session.session_spec().caller_role {
            SimCallerRoleV1::Paper => matches!(
                session
                    .session
                    .input_path_seal(MarketEventPathV1::PublicVisibility),
                Some(InputPathSealOutcomeV1::Exhausted)
            ),
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                matches!(
                    session
                        .session
                        .input_path_seal(MarketEventPathV1::HiddenPlatform),
                    Some(InputPathSealOutcomeV1::Exhausted)
                ) && matches!(
                    session
                        .session
                        .input_path_seal(MarketEventPathV1::PublicVisibility),
                    Some(InputPathSealOutcomeV1::Exhausted)
                )
            }
        };
        let query_retention_ready = completion_kind
            != DurableSessionCompletionKindV1::PaperRetentionExpired
            || session.query_state.is_fully_pruned_for_session_retirement();
        let confirmed_through = session.session.private_feedback_sequence();
        let downstream_through = session
            .downstream_durable_confirmation
            .as_ref()
            .map_or(0, |confirmation| confirmation.private_feedback_sequence);
        if !role_matches_completion
            || !all_public_markets_evicted
            || !all_hidden_markets_evicted
            || !session.session.is_quiescent_for_retention()
            || !query_retention_ready
            || !session.direct_response_outbox.entries.is_empty()
            || !session.lifecycle_outbox.entries.is_empty()
            || !session.lifecycle_outbox.batches.is_empty()
            || !session.confirmed_private_deliveries.is_empty()
            || downstream_through != confirmed_through
            || !input_paths_exhausted
        {
            return Err(SessionError::RetentionBarrierNotSatisfied(
                "市场、查询、输出、输入终态或会话静默屏障尚未全部满足",
            ));
        }
        if session
            .session
            .execution_state()
            .next_rebate_clock_at_utc_ms()
            .is_some()
        {
            return Err(SessionError::RetentionBarrierNotSatisfied(
                "返佣日结或付款时钟尚未全部闭合",
            ));
        }

        let mut candidate_root = self.inner.detached_clone()?;
        candidate_root.retire_session(&session_id)?;
        let mut coordinator = process_persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        let expected_process_root = self
            .inner
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        let expected_session = session.durable_checkpoint();
        if coordinator.current.process_root != expected_process_root
            || coordinator.current.sessions.get(&session_id) != Some(&expected_session)
        {
            return Err(SessionError::Durability(
                "会话退休内存基线与当前耐久检查点不一致".to_owned(),
            ));
        }
        let mut candidate = coordinator.current.clone();
        candidate.process_root = candidate_root
            .checkpoint()
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        if candidate.sessions.remove(&session_id).is_none() {
            return Err(SessionError::Durability(
                "会话退休缺少当前耐久会话正文".to_owned(),
            ));
        }
        candidate.session_resource_admission.remove(&session_id);
        let completion_fact = match completion_kind {
            DurableSessionCompletionKindV1::PaperRetentionExpired => {
                DurableTransactionFactV1::SessionRetentionExpired {
                    session_id: session_id.clone(),
                }
            }
            DurableSessionCompletionKindV1::CleanRunCompleted => {
                DurableTransactionFactV1::SessionCleanRunCompleted {
                    session_id: session_id.clone(),
                }
            }
        };
        coordinator.commit(
            &DurableTransactionEvidenceV1::new(completion_fact),
            candidate,
        )?;
        drop(coordinator);

        self.inner = candidate_root;
        self.recoverable_session_ids.remove(&session_id);
        self.attached_recovered_session_ids.remove(&session_id);
        self.recovered_evicted_markets.remove(&session_id);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_durable_commit_before_publish_for_tests(&self) {
        self.persistence
            .as_ref()
            .expect("测试运行必须启用本地耐久存储")
            .lock()
            .expect("测试耐久协调器锁必须可用")
            .store
            .fail_before_checkpoint_publish_once();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_durable_commit_after_checkpoint_before_evidence_for_tests(&self) {
        self.persistence
            .as_ref()
            .expect("测试运行必须启用本地耐久存储")
            .lock()
            .expect("测试耐久协调器锁必须可用")
            .store
            .fail_after_checkpoint_before_evidence_publish_once();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_durable_commit_after_evidence_commit_for_tests(&self) {
        self.persistence
            .as_ref()
            .expect("测试运行必须启用本地耐久存储")
            .lock()
            .expect("测试耐久协调器锁必须可用")
            .store
            .fail_after_evidence_commit_once();
    }

    #[cfg(test)]
    pub(crate) fn fail_before_artifact_bundle_publish_once_for_tests(&self) {
        self.persistence
            .as_ref()
            .expect("测试运行必须启用本地耐久存储")
            .lock()
            .expect("测试耐久协调器锁必须可用")
            .store
            .fail_before_artifact_bundle_publish_once();
    }

    #[cfg(test)]
    pub(crate) fn create_active_session_for_tests(
        &mut self,
        spec: SessionSpecV2,
        response_bridge: SubmissionResponseBridgeV1,
    ) -> SimExecutionSessionV1 {
        let session_id = spec.session_id.clone();
        let prepared = self
            .create_session(spec, response_bridge)
            .expect("测试会话必须成功进入准备态");
        self.activate_session(ActivateSessionRequestV1 {
            session_id,
            account_instance_key: prepared.control.account_instance_key.clone(),
            opening_anchor_id: prepared.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: prepared.control.opening_anchor.anchor_sha256.clone(),
            activation_id: format!(
                "component-test-activation:{}",
                prepared.control.opening_anchor.anchor_id
            ),
        })
        .expect("测试会话必须成功激活");
        prepared.session
    }
}

#[cfg(test)]
pub(crate) fn execution_process_for_tests(
    rule_snapshot: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
) -> SimExecutionProcessV1 {
    SimExecutionProcessV1::try_new(
        "component-test-run".to_owned(),
        crate::finality::successful_policy_for_tests(),
        rule_snapshot,
        latency_profile,
        scenario,
    )
    .expect("valid component-test process root")
}

#[cfg(test)]
pub(crate) fn accepting_submission_response_bridge() -> SubmissionResponseBridgeV1 {
    SubmissionResponseBridgeV1::new(
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
    )
}

/// 八方法模拟领域登记入口。
///
/// 这些方法不接受旧名称、改单或 SDK 的签名订单：
///
/// ```compile_fail,E0599
/// use sim_system::execution::{PostOrderRequestV1, SimExecutionSessionV1};
/// fn old_name(session: &mut SimExecutionSessionV1, request: PostOrderRequestV1) {
///     session.submit_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{OrderRequestV1, SimExecutionSessionV1};
/// fn old_query_name(session: &mut SimExecutionSessionV1, request: OrderRequestV1) {
///     session.query_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{PostOrderRequestV1, SimExecutionSessionV1};
/// fn no_amend(session: &mut SimExecutionSessionV1, request: PostOrderRequestV1) {
///     session.amend_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{CancelAllOrdersRequestV1, SimExecutionSessionV1};
/// fn no_extra_sdk_method(
///     session: &mut SimExecutionSessionV1,
///     request: CancelAllOrdersRequestV1,
/// ) {
///     session.cancel_market_orders(request);
/// }
/// ```
///
/// ```compile_fail,E0308
/// use polymarket_client_sdk_v2::clob::types::SignedOrder;
/// use sim_system::execution::SimExecutionSessionV1;
/// fn no_signed_order(session: &mut SimExecutionSessionV1, order: SignedOrder) {
///     session.post_order(order);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::SimExecutionSessionV1;
/// fn no_response_poll(session: &mut SimExecutionSessionV1) {
///     session.poll_response();
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::SimExecutionSessionV1;
/// fn no_response_take(session: &mut SimExecutionSessionV1) {
///     session.take_response();
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::SimExecutionSessionV1;
/// fn no_response_ack(session: &mut SimExecutionSessionV1) {
///     session.ack_response();
/// }
/// ```
pub struct SimExecutionSessionV1 {
    session: SimSession,
    pending_requests: BTreeMap<String, PendingDomainRequestV1>,
    mutation_payloads: BTreeMap<String, Sha256Hex>,
    submission_effect_records: BTreeMap<String, SubmissionEffectRecordV1>,
    cancel_effect_records: BTreeMap<String, CancelEffectRecordV1>,
    query_state: QueryStateV1,
    direct_response_outbox: LogicalDirectResponseOutboxV1,
    lifecycle_outbox: LogicalLifecycleOutboxV1,
    compacted_confirmed_private_deliveries: BTreeMap<u64, CompactedConfirmedPrivateDeliveryV1>,
    confirmed_private_deliveries: BTreeMap<u64, SealedPrivateDeliveryV1>,
    accepting_new_ingress: bool,
    response_bridge: SubmissionResponseBridgeV1,
    persistence: Option<SharedPersistentRunV1>,
    downstream_durable_confirmation: Option<ResponseDeliveryConfirmationV1>,
    bridge_replayed_through_private_feedback_sequence: u64,
}

#[derive(Clone)]
struct ExecutionSessionStateV1 {
    session: SimSession,
    pending_requests: BTreeMap<String, PendingDomainRequestV1>,
    mutation_payloads: BTreeMap<String, Sha256Hex>,
    submission_effect_records: BTreeMap<String, SubmissionEffectRecordV1>,
    cancel_effect_records: BTreeMap<String, CancelEffectRecordV1>,
    query_state: QueryStateV1,
    direct_response_outbox: LogicalDirectResponseOutboxV1,
    lifecycle_outbox: LogicalLifecycleOutboxV1,
    compacted_confirmed_private_deliveries: BTreeMap<u64, CompactedConfirmedPrivateDeliveryV1>,
    confirmed_private_deliveries: BTreeMap<u64, SealedPrivateDeliveryV1>,
    accepting_new_ingress: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSessionCheckpointV1 {
    session: SimSessionCheckpointV1,
    pending_requests: BTreeMap<String, PendingDomainRequestV1>,
    mutation_payloads: BTreeMap<String, Sha256Hex>,
    submission_effect_records: BTreeMap<String, SubmissionEffectRecordV1>,
    cancel_effect_records: BTreeMap<String, CancelEffectRecordV1>,
    query_state: QueryStateV1,
    direct_response_outbox: LogicalDirectResponseOutboxV1,
    lifecycle_outbox: LogicalLifecycleOutboxV1,
    compacted_confirmed_private_deliveries: BTreeMap<u64, CompactedConfirmedPrivateDeliveryV1>,
    confirmed_private_deliveries: BTreeMap<u64, SealedPrivateDeliveryV1>,
    accepting_new_ingress: bool,
    downstream_durable_confirmation: Option<ResponseDeliveryConfirmationV1>,
}

impl ExecutionSessionStateV1 {
    fn checkpoint(
        &self,
        downstream_durable_confirmation: Option<ResponseDeliveryConfirmationV1>,
    ) -> ExecutionSessionCheckpointV1 {
        ExecutionSessionCheckpointV1 {
            session: self.session.checkpoint(),
            pending_requests: self.pending_requests.clone(),
            mutation_payloads: self.mutation_payloads.clone(),
            submission_effect_records: self.submission_effect_records.clone(),
            cancel_effect_records: self.cancel_effect_records.clone(),
            query_state: self.query_state.clone(),
            direct_response_outbox: self.direct_response_outbox.clone(),
            lifecycle_outbox: self.lifecycle_outbox.clone(),
            compacted_confirmed_private_deliveries: self
                .compacted_confirmed_private_deliveries
                .clone(),
            confirmed_private_deliveries: self.confirmed_private_deliveries.clone(),
            accepting_new_ingress: self.accepting_new_ingress,
            downstream_durable_confirmation,
        }
    }

    fn restore(
        root: &SimProcessRoot,
        checkpoint: ExecutionSessionCheckpointV1,
        evicted_markets: &BTreeMap<String, DurableEvictedMarketStateV1>,
    ) -> Result<(Self, Option<ResponseDeliveryConfirmationV1>), SessionCheckpointErrorV1> {
        let downstream_durable_confirmation = checkpoint.downstream_durable_confirmation.clone();
        let state = Self {
            session: root.restore_session_from_checkpoint_with_evicted_markets(
                checkpoint.session,
                &evicted_markets
                    .iter()
                    .map(|(condition_id, (_, _, witness))| (condition_id.clone(), witness.clone()))
                    .collect(),
            )?,
            pending_requests: checkpoint.pending_requests,
            mutation_payloads: checkpoint.mutation_payloads,
            submission_effect_records: checkpoint.submission_effect_records,
            cancel_effect_records: checkpoint.cancel_effect_records,
            query_state: checkpoint.query_state,
            direct_response_outbox: checkpoint.direct_response_outbox,
            lifecycle_outbox: checkpoint.lifecycle_outbox,
            compacted_confirmed_private_deliveries: checkpoint
                .compacted_confirmed_private_deliveries,
            confirmed_private_deliveries: checkpoint.confirmed_private_deliveries,
            accepting_new_ingress: checkpoint.accepting_new_ingress,
        };
        let pending_request_ids = state
            .pending_requests
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if state.session.recovered_request_ids() != pending_request_ids {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "执行层待处理请求集合与会话调度记录不一致".to_owned(),
            ));
        }
        for (global_request_id, request) in &state.pending_requests {
            state.session.validate_recovered_request_binding(
                global_request_id,
                request.context(),
                &canonical_request_sha256(request),
            )?;
        }
        let expected_capacity = state.session.session_spec().response_outbox_capacity.get();
        if state.direct_response_outbox.capacity != expected_capacity
            || state.lifecycle_outbox.capacity != expected_capacity
            || state.direct_response_outbox.entries.len() > expected_capacity
            || state.lifecycle_outbox.batches.len() > expected_capacity
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "恢复响应发件箱容量与会话规格不一致或已经超限".to_owned(),
            ));
        }
        let terminal_condition_ids = state.session.resolution_terminal_condition_ids();
        state.query_state.validate_recovered(
            state.session.session_spec(),
            &state.pending_requests,
            &terminal_condition_ids,
            state.session.execution_state(),
            evicted_markets,
        )?;
        let mut expected_candidate_condition_ids = BTreeSet::new();
        let mut expected_candidate_token_ids = BTreeSet::new();
        for request in state.pending_requests.values() {
            let (condition_ids, token_ids) = request.candidate_market_use();
            expected_candidate_condition_ids.extend(
                condition_ids.into_iter().filter(|condition_id| {
                    !state.session.market_directory().contains(condition_id)
                }),
            );
            expected_candidate_token_ids.extend(
                token_ids
                    .into_iter()
                    .filter(|token_id| !state.session.market_directory().contains_token(token_id)),
            );
        }
        state.session.validate_recovered_paper_market_candidates(
            &expected_candidate_condition_ids,
            &expected_candidate_token_ids,
        )?;
        validate_recovered_private_output_closure(&state)?;
        validate_confirmed_private_deliveries(&state, downstream_durable_confirmation.as_ref())?;
        let opening_anchor = root
            .opening_anchor_for_session(state.session.session_spec().session_id.as_str())
            .ok_or_else(|| {
                SessionCheckpointErrorV1::InvalidSessionState("恢复会话缺少原开户锚点".to_owned())
            })?;
        validate_recovered_account_delta_chain(
            &state,
            state.session.execution_state(),
            opening_anchor,
            &evicted_markets
                .iter()
                .map(|(condition_id, (_, _, witness))| (condition_id.clone(), witness.clone()))
                .collect(),
        )?;
        Ok((state, downstream_durable_confirmation))
    }
}

fn validate_recovered_private_output_closure(
    state: &ExecutionSessionStateV1,
) -> Result<(), SessionCheckpointErrorV1> {
    let pending = state.session.recovered_pending_private_facts();
    let mut pending_direct = BTreeMap::new();
    let mut pending_lifecycle = BTreeMap::new();
    for (ordinal, (evidence, received_at, emission_sequence)) in pending.iter().enumerate() {
        let private_feedback_sequence = state
            .session
            .private_feedback_sequence()
            .checked_add(u64::try_from(ordinal + 1).map_err(|_| {
                SessionCheckpointErrorV1::InvalidSessionState(
                    "待接收私有事实序号无法表示".to_owned(),
                )
            })?)
            .ok_or_else(|| {
                SessionCheckpointErrorV1::InvalidSessionState("待接收私有事实序号溢出".to_owned())
            })?;
        let value = (*received_at, *emission_sequence, private_feedback_sequence);
        match evidence {
            crate::session::PrivateVisibilityEvidenceV1::DirectResponse { global_request_id } => {
                pending_direct.insert(global_request_id.clone(), value);
            }
            crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } => {
                pending_lifecycle.insert(feedback_id.clone(), value);
            }
        }
    }

    let drafted_direct = state
        .direct_response_outbox
        .entries
        .iter()
        .filter(|(_, entry)| entry.draft.is_some())
        .map(|(request_id, _)| request_id.clone())
        .collect::<BTreeSet<_>>();
    let unformed_direct = state
        .session
        .recovered_unformed_direct_response_request_ids();
    let pending_direct_ids = pending_direct.keys().cloned().collect::<BTreeSet<_>>();
    let expected_direct_entries = unformed_direct
        .union(&pending_direct_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    if drafted_direct != pending_direct_ids
        || state
            .direct_response_outbox
            .entries
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_direct_entries
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "待接收直接响应与直接响应发件箱不一致".to_owned(),
        ));
    }
    for (request_id, entry) in &state.direct_response_outbox.entries {
        let request = state.pending_requests.get(request_id).ok_or_else(|| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "直接响应发件箱缺少请求正文：{request_id}"
            ))
        })?;
        let expected_platform_responded_at = entry
            .parts
            .iter()
            .flatten()
            .map(DirectResponsePartV1::effective_processing_at_utc_ms)
            .max();
        if entry.method != request.direct_response_method()
            || entry.delivery_id
                != LogicalDirectResponseOutboxV1::delivery_id(
                    &state.session.session_spec().session_id,
                    request_id,
                    entry.method,
                )
            || entry.parts.len() != request.action_count()
            || entry
                .parts
                .iter()
                .flatten()
                .any(|part| !part.method_compatible(entry.method))
            || entry.platform_responded_at_utc_ms != expected_platform_responded_at
            || (entry.draft.is_none() && entry.sealed.is_some())
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "直接响应发件箱正文与请求不一致：{request_id}"
            )));
        }
        let Some((received_at, emission_sequence, private_feedback_sequence)) =
            pending_direct.get(request_id).copied()
        else {
            continue;
        };
        let draft = entry.draft.as_ref().expect("已核对直接响应草稿集合");
        let complete_parts = entry.parts.iter().cloned().collect::<Option<Vec<_>>>();
        if draft.context.global_request_id != *request_id
            || draft.context != *request.context()
            || draft.method != entry.method
            || draft.delivery_id != entry.delivery_id
            || entry.platform_responded_at_utc_ms != Some(draft.platform_responded_at_utc_ms)
            || draft.response_received_at_utc_ms != received_at
            || draft.private_emission_sequence != emission_sequence
            || complete_parts.as_ref() != Some(&draft.parts)
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "待接收直接响应草稿与统一私有队列不一致：{request_id}"
            )));
        }
        if let Some(sealed) = &entry.sealed {
            let delivery = SealedPrivateDeliveryV1::Direct(sealed.clone());
            let confirmation = delivery.confirmation();
            let expected = seal_direct_response(
                draft,
                &crate::session::VisiblePrivateFactV1 {
                    evidence: crate::session::PrivateVisibilityEvidenceV1::DirectResponse {
                        global_request_id: request_id.clone(),
                    },
                    received_at_utc_ms: received_at,
                    private_emission_sequence: emission_sequence,
                    private_feedback_sequence,
                },
            )
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "待接收直接响应无法按草稿重新密封：{error}"
                ))
            })?;
            if confirmation.session_id != state.session.session_spec().session_id
                || confirmation.delivery_id != entry.delivery_id
                || confirmation.private_feedback_sequence != private_feedback_sequence
                || confirmation.payload_sha256 != delivery.recompute_payload_sha256()?
                || expected != *sealed
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "待接收直接响应密封正文与统一私有队列不一致：{request_id}"
                )));
            }
        }
    }

    if state.lifecycle_outbox.entries.len() != pending_lifecycle.len()
        || state
            .lifecycle_outbox
            .entries
            .keys()
            .any(|feedback_id| !pending_lifecycle.contains_key(feedback_id))
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "待接收生命周期反馈与生命周期发件箱不一致".to_owned(),
        ));
    }
    let mut expected_feedback_ids = BTreeMap::<String, BTreeSet<String>>::new();
    let mut expected_received_at_counts = BTreeMap::<String, BTreeMap<i64, usize>>::new();
    for (feedback_id, entry) in &state.lifecycle_outbox.entries {
        let (received_at, emission_sequence, private_feedback_sequence) = pending_lifecycle
            .get(feedback_id)
            .copied()
            .expect("已核对生命周期反馈集合");
        if entry.draft.feedback_id() != feedback_id
            || entry.draft.timing().feedback_received_at_utc_ms != received_at
            || entry.draft.timing().private_emission_sequence != emission_sequence
            || !state.lifecycle_outbox.batches.contains_key(&entry.batch_id)
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "待接收生命周期反馈草稿与统一私有队列不一致：{feedback_id}"
            )));
        }
        expected_feedback_ids
            .entry(entry.batch_id.clone())
            .or_default()
            .insert(feedback_id.clone());
        *expected_received_at_counts
            .entry(entry.batch_id.clone())
            .or_default()
            .entry(received_at)
            .or_default() += 1;
        if let Some(sealed) = &entry.sealed {
            let delivery = SealedPrivateDeliveryV1::Lifecycle(sealed.clone());
            let confirmation = delivery.confirmation();
            let candidate = crate::session::VisiblePrivateFactV1 {
                evidence: crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback {
                    feedback_id: feedback_id.clone(),
                },
                received_at_utc_ms: received_at,
                private_emission_sequence: emission_sequence,
                private_feedback_sequence,
            };
            let expected = match &entry.draft {
                LifecycleFeedbackDraftV1::Order(draft) => SealedLifecycleFeedbackV1::Order(
                    seal_order_lifecycle_feedback(draft, &candidate).map_err(|error| {
                        SessionCheckpointErrorV1::InvalidSessionState(format!(
                            "待接收订单反馈无法按草稿重新密封：{error}"
                        ))
                    })?,
                ),
                LifecycleFeedbackDraftV1::Trade(draft) => SealedLifecycleFeedbackV1::Trade(
                    seal_trade_lifecycle_feedback(draft, &candidate).map_err(|error| {
                        SessionCheckpointErrorV1::InvalidSessionState(format!(
                            "待接收成交反馈无法按草稿重新密封：{error}"
                        ))
                    })?,
                ),
                LifecycleFeedbackDraftV1::Settlement(draft) => {
                    SealedLifecycleFeedbackV1::Settlement(
                        seal_settlement_lifecycle_feedback(draft, &candidate).map_err(|error| {
                            SessionCheckpointErrorV1::InvalidSessionState(format!(
                                "待接收结算反馈无法按草稿重新密封：{error}"
                            ))
                        })?,
                    )
                }
            };
            if confirmation.session_id != state.session.session_spec().session_id
                || confirmation.delivery_id != entry.draft.delivery_id()
                || confirmation.private_feedback_sequence != private_feedback_sequence
                || confirmation.payload_sha256 != delivery.recompute_payload_sha256()?
                || expected != *sealed
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "待接收生命周期反馈密封正文与统一私有队列不一致：{feedback_id}"
                )));
            }
            delivery.validate_account_deltas()?;
        }
    }
    for (batch_id, batch) in &state.lifecycle_outbox.batches {
        if batch.feedback_ids != expected_feedback_ids.remove(batch_id).unwrap_or_default()
            || batch.feedback_received_at_counts
                != expected_received_at_counts
                    .remove(batch_id)
                    .unwrap_or_default()
            || batch
                .open_work_ids
                .iter()
                .any(|work_id| state.lifecycle_outbox.work_batch_ids.get(work_id) != Some(batch_id))
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "生命周期发件箱批次索引不一致：{batch_id}"
            )));
        }
    }
    let referenced_batch_ids = state
        .lifecycle_outbox
        .entries
        .values()
        .map(|entry| entry.batch_id.clone())
        .chain(state.lifecycle_outbox.work_batch_ids.values().cloned())
        .collect::<BTreeSet<_>>();
    let partially_processed_requests = state.session.recovered_partially_processed_request_ids();
    let expected_open_work_ids = state
        .pending_requests
        .iter()
        .filter(|(request_id, request)| {
            mutation_binding(request).is_some()
                && partially_processed_requests.contains(request_id.as_str())
        })
        .map(|(request_id, _)| LogicalLifecycleOutboxV1::request_batch_id(request_id))
        .collect::<BTreeSet<_>>();
    if !expected_feedback_ids.is_empty()
        || !expected_received_at_counts.is_empty()
        || state
            .lifecycle_outbox
            .work_batch_ids
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_open_work_ids
        || state
            .lifecycle_outbox
            .work_batch_ids
            .iter()
            .any(|(work_id, batch_id)| {
                state
                    .lifecycle_outbox
                    .batches
                    .get(batch_id)
                    .is_none_or(|batch| !batch.open_work_ids.contains(work_id))
            })
        || state
            .lifecycle_outbox
            .batches
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != referenced_batch_ids
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "生命周期发件箱批次互引不一致".to_owned(),
        ));
    }
    Ok(())
}

fn validate_recovered_account_delta_chain(
    state: &ExecutionSessionStateV1,
    validation_execution: &ExecutionStateV1,
    opening_anchor: &AccountOpeningAnchorV1,
    evicted_markets: &BTreeMap<String, TerminalExecutionMarketEvictionV1>,
) -> Result<(), SessionCheckpointErrorV1> {
    let mut deltas = BTreeMap::new();
    let mut insert = |delta: &PlatformAccountDeltaV1| -> Result<(), SessionCheckpointErrorV1> {
        validate_platform_account_delta_integrity(delta).map_err(|error| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "恢复生命周期正文包含损坏的平台账户变化：{error}"
            ))
        })?;
        if let Some(existing) = deltas.insert(delta.account_transition_seq, delta.clone())
            && existing != *delta
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "账户变化序号 {} 对应多个正文",
                delta.account_transition_seq
            )));
        }
        Ok(())
    };
    for compacted in state.compacted_confirmed_private_deliveries.values() {
        for delta in &compacted.account_deltas {
            insert(delta)?;
        }
    }
    for delivery in state.confirmed_private_deliveries.values() {
        delivery.visit_account_deltas(&mut insert)?;
    }
    for entry in state.lifecycle_outbox.entries.values() {
        entry.draft.visit_account_deltas(&mut insert)?;
        if let Some(sealed) = &entry.sealed {
            SealedPrivateDeliveryV1::Lifecycle(sealed.clone()).visit_account_deltas(&mut insert)?;
        }
    }
    validation_execution
        .validate_recovered_account_delta_chain(
            opening_anchor,
            &deltas,
            state.session.finality_policy().as_ref(),
            evicted_markets,
        )
        .map_err(|error| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "恢复账户变化链不连续：{error:?}"
            ))
        })
}

fn account_deltas_in_checkpoint(
    checkpoint: &ExecutionSessionCheckpointV1,
) -> Result<BTreeMap<u64, PlatformAccountDeltaV1>, SessionCheckpointErrorV1> {
    let mut deltas = BTreeMap::new();
    let mut insert = |delta: &PlatformAccountDeltaV1| -> Result<(), SessionCheckpointErrorV1> {
        if let Some(existing) = deltas.insert(delta.account_transition_seq, delta.clone())
            && existing != *delta
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "账户变化序号 {} 对应多个正文",
                delta.account_transition_seq
            )));
        }
        Ok(())
    };
    for compacted in checkpoint.compacted_confirmed_private_deliveries.values() {
        for delta in &compacted.account_deltas {
            insert(delta)?;
        }
    }
    for delivery in checkpoint.confirmed_private_deliveries.values() {
        delivery.visit_account_deltas(&mut insert)?;
    }
    for entry in checkpoint.lifecycle_outbox.entries.values() {
        entry.draft.visit_account_deltas(&mut insert)?;
        if let Some(sealed) = &entry.sealed {
            SealedPrivateDeliveryV1::Lifecycle(sealed.clone()).visit_account_deltas(&mut insert)?;
        }
    }
    Ok(deltas)
}

fn private_output_drafts_in_checkpoint(
    checkpoint: &ExecutionSessionCheckpointV1,
) -> Result<BTreeMap<String, DurablePrivateOutputDraftV1>, SessionCheckpointErrorV1> {
    let mut drafts = BTreeMap::new();
    for entry in checkpoint.direct_response_outbox.entries.values() {
        let Some(draft) = &entry.draft else {
            continue;
        };
        let durable = DurablePrivateOutputDraftV1::Direct {
            draft: draft.clone(),
        };
        if drafts
            .insert(durable.delivery_id().to_owned(), durable)
            .is_some()
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "检查点包含重复私有输出交付身份".to_owned(),
            ));
        }
    }
    for entry in checkpoint.lifecycle_outbox.entries.values() {
        let durable = DurablePrivateOutputDraftV1::Lifecycle {
            draft: entry.draft.clone(),
        };
        if drafts
            .insert(durable.delivery_id().to_owned(), durable)
            .is_some()
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "检查点包含重复私有输出交付身份".to_owned(),
            ));
        }
    }
    Ok(drafts)
}

fn durable_logical_action_result(
    request_id: &str,
    request: &PendingDomainRequestV1,
    ordinal: usize,
    query_projection_revision: Option<u64>,
    part: &DirectResponsePartV1,
) -> Result<DurableLogicalActionResultV1, SessionCheckpointErrorV1> {
    let batch_item_ordinal = u16::try_from(ordinal).map_err(|_| {
        SessionCheckpointErrorV1::InvalidSessionState("逻辑动作结果序号超出 u16 边界".to_owned())
    })?;
    if part.ordinal() != ordinal || !part.method_compatible(request.direct_response_method()) {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
            "逻辑动作结果正文与请求方法或序号不一致：{request_id}:{ordinal}"
        )));
    }
    Ok(DurableLogicalActionResultV1 {
        action_identity: format!("{}:{request_id}:{batch_item_ordinal}", request.method_tag()),
        global_request_id: request_id.to_owned(),
        batch_item_ordinal,
        query_projection_revision,
        part: part.clone(),
    })
}

fn logical_action_results_in_checkpoint(
    checkpoint: &ExecutionSessionCheckpointV1,
) -> Result<BTreeMap<String, DurableLogicalActionResultV1>, SessionCheckpointErrorV1> {
    let mut results = BTreeMap::new();
    let query_revisions = checkpoint.session.recovered_query_revisions();
    for (request_id, entry) in &checkpoint.direct_response_outbox.entries {
        let request = checkpoint.pending_requests.get(request_id).ok_or_else(|| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "逻辑动作结果缺少请求正文：{request_id}"
            ))
        })?;
        for (ordinal, part) in entry.parts.iter().enumerate() {
            let Some(part) = part else {
                continue;
            };
            let result = durable_logical_action_result(
                request_id,
                request,
                ordinal,
                query_revisions.get(request_id).copied(),
                part,
            )?;
            if results
                .insert(result.action_identity.clone(), result)
                .is_some()
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "逻辑动作结果身份重复：{request_id}:{ordinal}"
                )));
            }
        }
    }
    Ok(results)
}

fn validate_durable_logical_action_results(
    session_id: &str,
    state: &ExecutionSessionStateV1,
    registrations: &BTreeMap<String, DurableRequestRegistrationV1>,
    durable_results: Option<&BTreeMap<String, DurableLogicalActionResultV1>>,
    durable_contexts: Option<&BTreeMap<String, DurableLogicalActionRecoveryContextV1>>,
    durable_drafts: Option<&BTreeMap<String, DurablePrivateOutputDraftV1>>,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    let mut expected = logical_action_results_in_checkpoint(&state.checkpoint(None))?;
    let query_revisions = state.session.recovered_query_revisions();
    for draft in durable_drafts.into_iter().flat_map(BTreeMap::values) {
        let DurablePrivateOutputDraftV1::Direct { draft } = draft else {
            continue;
        };
        let request_id = &draft.context.global_request_id;
        let request = state.pending_requests.get(request_id).ok_or_else(|| {
            SimExecutionPersistenceErrorV1::Session(format!(
                "耐久直接响应草稿缺少请求正文：{session_id}:{request_id}"
            ))
        })?;
        for (ordinal, part) in draft.parts.iter().enumerate() {
            let result = durable_logical_action_result(
                request_id,
                request,
                ordinal,
                query_revisions.get(request_id).copied(),
                part,
            )?;
            if let Some(existing) = expected.insert(result.action_identity.clone(), result.clone())
                && existing != result
            {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "活动与历史逻辑动作结果正文冲突：{}",
                    result.action_identity
                )));
            }
        }
    }
    let durable_results = durable_results.cloned().unwrap_or_default();
    let durable_contexts = durable_contexts.cloned().unwrap_or_default();
    if durable_results != expected {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "仅追加逻辑动作结果与检查点及输出草稿不一致：{session_id}"
        )));
    }
    if durable_contexts.keys().collect::<BTreeSet<_>>()
        != durable_results.keys().collect::<BTreeSet<_>>()
    {
        return Err(SimExecutionPersistenceErrorV1::Session(format!(
            "逻辑动作结果与形成事务上下文集合不一致：{session_id}"
        )));
    }
    for result in durable_results.values() {
        let registration = registrations
            .get(&result.global_request_id)
            .ok_or_else(|| {
                SimExecutionPersistenceErrorV1::Session(format!(
                    "耐久逻辑动作结果缺少请求登记事实：{}",
                    result.action_identity
                ))
            })?;
        let action = registration
            .actions
            .iter()
            .find(|action| action.key.batch_item_ordinal == result.batch_item_ordinal)
            .ok_or_else(|| {
                SimExecutionPersistenceErrorV1::Session(format!(
                    "耐久逻辑动作结果缺少动作计划：{}",
                    result.action_identity
                ))
            })?;
        if action.action_identity != result.action_identity
            || action.global_request_id != result.global_request_id
            || result.part.effective_processing_at_utc_ms()
                != action.key.effective_processing_at_utc_ms
        {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "耐久逻辑动作结果与动作计划不一致：{}",
                result.action_identity
            )));
        }
        let context = durable_contexts
            .get(&result.action_identity)
            .expect("已核对动作结果与恢复上下文集合精确相等");
        let is_initial_query = matches!(
            &registration.request,
            PendingDomainRequestV1::Order(_)
                | PendingDomainRequestV1::Orders(OrdersRequestV1 { cursor: None, .. })
                | PendingDomainRequestV1::Trades(TradesRequestV1 { cursor: None, .. })
        );
        if is_initial_query
            && result.query_projection_revision != Some(context.query_projection_revision)
        {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "初始查询结果没有绑定形成事务的投影修订号：{}",
                result.action_identity
            )));
        }
    }
    state
        .session
        .validate_recovered_action_results(&durable_results.keys().cloned().collect())?;
    validate_recovered_idempotency_graph(state, &durable_results)?;
    let expected_queries = durable_results
        .values()
        .filter_map(|result| {
            let request = state.pending_requests.get(&result.global_request_id)?;
            let revision = result.query_projection_revision?;
            Some((
                result.global_request_id.clone(),
                (canonical_request_sha256(request), revision),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    state
        .session
        .validate_recovered_query_records(&expected_queries)?;
    state.query_state.validate_durable_action_results(
        state.session.session_spec(),
        &state.pending_requests,
        registrations,
        &durable_results,
        &durable_contexts,
    )?;
    Ok(())
}

fn insert_unique_binding<K: Ord + Clone, V: PartialEq + Clone>(
    target: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    message: &'static str,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    if let Some(existing) = target.get(&key) {
        if existing != &value {
            return Err(SimExecutionPersistenceErrorV1::Session(message.to_owned()));
        }
    } else {
        target.insert(key, value);
    }
    Ok(())
}

fn post_item_for_recovered_result(
    request: &PendingDomainRequestV1,
    ordinal: u16,
) -> Option<&PostOrderItemV1> {
    match request {
        PendingDomainRequestV1::PostOrder(request) if ordinal == 0 => Some(&request.item),
        PendingDomainRequestV1::PostOrders(request) => request.items.get(usize::from(ordinal)),
        _ => None,
    }
}

fn validate_recovered_idempotency_graph(
    state: &ExecutionSessionStateV1,
    durable_results: &BTreeMap<String, DurableLogicalActionResultV1>,
) -> Result<(), SimExecutionPersistenceErrorV1> {
    let mut expected_payloads = BTreeMap::new();
    for request in state.pending_requests.values() {
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(request) {
            insert_unique_binding(
                &mut expected_payloads,
                idempotency_key,
                payload_sha256,
                "同一变更幂等键对应多个请求正文",
            )?;
        }
    }
    if state.mutation_payloads != expected_payloads {
        return Err(SimExecutionPersistenceErrorV1::Session(
            "变更请求幂等载荷索引与仅追加请求正文不一致".to_owned(),
        ));
    }

    let spec = state.session.session_spec();
    let mut expected_mutations = BTreeMap::new();
    let mut expected_submission_effects = BTreeMap::new();
    let mut submission_new_effect_origins = BTreeMap::<String, BTreeSet<String>>::new();
    let mut submission_first_requests = BTreeMap::new();
    let mut expected_cancel_effect_ids = BTreeSet::new();
    let mut cancel_new_effect_origins = BTreeMap::<String, BTreeSet<String>>::new();
    let mut cancel_first_requests = BTreeMap::new();
    for result in durable_results.values() {
        let request = state
            .pending_requests
            .get(&result.global_request_id)
            .ok_or_else(|| {
                SimExecutionPersistenceErrorV1::Session(format!(
                    "逻辑动作结果缺少变更请求正文：{}",
                    result.action_identity
                ))
            })?;
        let Some((idempotency_key, payload_sha256)) = mutation_binding(request) else {
            continue;
        };
        match request {
            PendingDomainRequestV1::PostOrder(_) | PendingDomainRequestV1::PostOrders(_) => {
                let item = post_item_for_recovered_result(request, result.batch_item_ordinal)
                    .ok_or_else(|| {
                        SimExecutionPersistenceErrorV1::Session(format!(
                            "提交动作结果没有对应请求项：{}",
                            result.action_identity
                        ))
                    })?;
                let DirectResponsePartV1::Submission(response) = &result.part else {
                    return Err(SimExecutionPersistenceErrorV1::Session(format!(
                        "提交动作结果类型不一致：{}",
                        result.action_identity
                    )));
                };
                let item_identity = submission_item_idempotency_identity(
                    spec,
                    &idempotency_key,
                    result.batch_item_ordinal,
                );
                let effect_identity =
                    submission_item_effect_identity(&item_identity, &payload_sha256);
                let action_order = (
                    result.part.effective_processing_at_utc_ms(),
                    request.context().caller_sequence,
                    result.batch_item_ordinal,
                    result.global_request_id.clone(),
                );
                submission_first_requests
                    .entry(effect_identity.clone())
                    .and_modify(|existing| {
                        if action_order < *existing {
                            *existing = action_order.clone();
                        }
                    })
                    .or_insert(action_order);
                if response.item_id != item.item_id
                    || response.batch_item_ordinal != result.batch_item_ordinal
                    || response.condition_id != item.order.condition_id
                    || response.token_id != item.order.token_id
                    || response.submission_effect_identity != effect_identity
                    || response.submission_effect_formed_at_utc_ms
                        > response.effective_processing_at_utc_ms
                {
                    return Err(SimExecutionPersistenceErrorV1::Session(format!(
                        "提交动作结果未精确绑定请求项与幂等效果：{}",
                        result.action_identity
                    )));
                }
                match response.submission_effect_disposition {
                    SubmissionEffectDispositionV1::NewEffect => {
                        if response.submission_effect_formed_at_utc_ms
                            != response.effective_processing_at_utc_ms
                        {
                            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                                "新提交幂等效果形成时点与动作时点不一致：{}",
                                result.action_identity
                            )));
                        }
                        submission_new_effect_origins
                            .entry(effect_identity.clone())
                            .or_default()
                            .insert(result.global_request_id.clone());
                    }
                    SubmissionEffectDispositionV1::ExistingEffect => {}
                }
                insert_unique_binding(
                    &mut expected_mutations,
                    item_identity,
                    (payload_sha256.clone(), effect_identity.clone()),
                    "提交变更幂等记录冲突",
                )?;
                insert_unique_binding(
                    &mut expected_submission_effects,
                    effect_identity,
                    SubmissionEffectRecordV1 {
                        outcome: response.outcome.clone(),
                        formed_at_utc_ms: response.submission_effect_formed_at_utc_ms,
                    },
                    "提交幂等效果正文冲突",
                )?;
            }
            PendingDomainRequestV1::CancelOrder(_)
            | PendingDomainRequestV1::CancelOrders(_)
            | PendingDomainRequestV1::CancelAllOrders(_) => {
                let request_identity = cancel_request_idempotency_identity(spec, &idempotency_key);
                let root_effect_identity =
                    cancel_item_effect_identity(&request_identity, &payload_sha256);
                let action_order = (
                    result.part.effective_processing_at_utc_ms(),
                    request.context().caller_sequence,
                    result.batch_item_ordinal,
                    result.global_request_id.clone(),
                );
                cancel_first_requests
                    .entry(root_effect_identity.clone())
                    .and_modify(|existing| {
                        if action_order < *existing {
                            *existing = action_order.clone();
                        }
                    })
                    .or_insert(action_order);
                insert_unique_binding(
                    &mut expected_mutations,
                    request_identity,
                    (payload_sha256.clone(), root_effect_identity.clone()),
                    "撤单变更幂等记录冲突",
                )?;
                expected_cancel_effect_ids.insert(root_effect_identity.clone());
                let record = state
                    .cancel_effect_records
                    .get(&root_effect_identity)
                    .ok_or_else(|| {
                        SimExecutionPersistenceErrorV1::Session(format!(
                            "撤单动作结果缺少幂等效果正文：{}",
                            result.action_identity
                        ))
                    })?;
                let is_origin = record.originating_global_request_id == result.global_request_id;
                let expected_disposition = if is_origin {
                    CancelEffectDispositionV1::NewEffect
                } else {
                    CancelEffectDispositionV1::ExistingEffect
                };
                if record.formed_at_utc_ms > result.part.effective_processing_at_utc_ms()
                    || (is_origin
                        && record.formed_at_utc_ms != result.part.effective_processing_at_utc_ms())
                {
                    return Err(SimExecutionPersistenceErrorV1::Session(format!(
                        "撤单幂等效果形成时点与动作时点不一致：{}",
                        result.action_identity
                    )));
                }
                if is_origin {
                    cancel_new_effect_origins
                        .entry(root_effect_identity.clone())
                        .or_default()
                        .insert(result.global_request_id.clone());
                }
                match (&result.part, request) {
                    (
                        DirectResponsePartV1::CancelTarget(response),
                        PendingDomainRequestV1::CancelOrder(_)
                        | PendingDomainRequestV1::CancelOrders(_),
                    ) => {
                        let groups = cancel_target_groups(request).map_err(|error| {
                            SimExecutionPersistenceErrorV1::Session(format!(
                                "恢复撤单目标分组失败：{error:?}"
                            ))
                        })?;
                        if record.targets.len() != groups.len() {
                            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                                "撤单幂等效果目标集合与请求不一致：{}",
                                result.action_identity
                            )));
                        }
                        let group = groups
                            .get(usize::from(result.batch_item_ordinal))
                            .ok_or_else(|| {
                                SimExecutionPersistenceErrorV1::Session(format!(
                                    "撤单动作结果缺少目标分组：{}",
                                    result.action_identity
                                ))
                            })?;
                        let target = record
                            .targets
                            .get(usize::from(result.batch_item_ordinal))
                            .ok_or_else(|| {
                                SimExecutionPersistenceErrorV1::Session(format!(
                                    "撤单幂等效果缺少目标：{}",
                                    result.action_identity
                                ))
                            })?;
                        if response.platform_order_id != group.platform_order_id
                            || target.platform_order_id != group.platform_order_id
                            || response.target_ordinal != u64::from(group.unique_target_ordinal)
                            || response.input_item_refs != group.input_item_refs
                            || response.cancel_effect_identity != target.effect_identity
                            || response.cancel_effect_formed_at_utc_ms != record.formed_at_utc_ms
                            || response.cancel_effect_disposition != expected_disposition
                            || response.outcome != target.outcome
                            || target.effect_identity
                                != cancel_target_effect_identity(
                                    &root_effect_identity,
                                    &target.platform_order_id,
                                    response.target_ordinal,
                                )
                        {
                            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                                "撤单动作结果未精确绑定目标与幂等效果：{}",
                                result.action_identity
                            )));
                        }
                    }
                    (
                        DirectResponsePartV1::CancelAll {
                            effective_processing_at_utc_ms,
                            targets,
                        },
                        PendingDomainRequestV1::CancelAllOrders(_),
                    ) => {
                        if targets.len() != record.targets.len()
                            || targets.iter().zip(&record.targets).enumerate().any(
                                |(ordinal, (response, target))| {
                                    response.platform_order_id != target.platform_order_id
                                        || response.target_ordinal
                                            != u64::try_from(ordinal).expect("撤单目标数量适合 u64")
                                        || !response.input_item_refs.is_empty()
                                        || response.effective_processing_at_utc_ms
                                            != *effective_processing_at_utc_ms
                                        || response.cancel_effect_identity != target.effect_identity
                                        || response.cancel_effect_formed_at_utc_ms
                                            != record.formed_at_utc_ms
                                        || response.cancel_effect_disposition
                                            != expected_disposition
                                        || response.outcome != target.outcome
                                        || target.effect_identity
                                            != cancel_target_effect_identity(
                                                &root_effect_identity,
                                                &target.platform_order_id,
                                                response.target_ordinal,
                                            )
                                },
                            )
                        {
                            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                                "全撤动作结果未精确绑定目标与幂等效果：{}",
                                result.action_identity
                            )));
                        }
                    }
                    _ => {
                        return Err(SimExecutionPersistenceErrorV1::Session(format!(
                            "撤单动作结果类型不一致：{}",
                            result.action_identity
                        )));
                    }
                }
            }
            PendingDomainRequestV1::Order(_)
            | PendingDomainRequestV1::Orders(_)
            | PendingDomainRequestV1::Trades(_) => unreachable!("已排除非变更请求"),
        }
    }
    for (request_id, request) in &state.pending_requests {
        if matches!(request, PendingDomainRequestV1::CancelOrders(_)) {
            let completed = durable_results
                .values()
                .filter(|result| result.global_request_id == *request_id)
                .count();
            if completed != 0 && completed != request.action_count() {
                return Err(SimExecutionPersistenceErrorV1::Session(format!(
                    "批量撤单只能在零项或全部目标完成后形成耐久边界：{request_id}"
                )));
            }
        }
    }
    if state.submission_effect_records != expected_submission_effects
        || expected_submission_effects.keys().any(|effect_identity| {
            let first_request = submission_first_requests
                .get(effect_identity)
                .map(|(_, _, _, request_id)| request_id);
            submission_new_effect_origins
                .get(effect_identity)
                .is_none_or(|origins| {
                    origins.len() != 1
                        || first_request.is_none_or(|request_id| !origins.contains(request_id))
                })
        })
        || state
            .cancel_effect_records
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_cancel_effect_ids
        || expected_cancel_effect_ids.iter().any(|effect_identity| {
            let Some(record) = state.cancel_effect_records.get(effect_identity) else {
                return true;
            };
            cancel_new_effect_origins
                .get(effect_identity)
                .is_none_or(|origins| {
                    let first_request = cancel_first_requests
                        .get(effect_identity)
                        .map(|(_, _, _, request_id)| request_id);
                    origins.len() != 1
                        || !origins.contains(&record.originating_global_request_id)
                        || first_request.is_none_or(|request_id| {
                            request_id != &record.originating_global_request_id
                        })
                })
        })
    {
        return Err(SimExecutionPersistenceErrorV1::Session(
            "变更幂等效果缓存与耐久动作结果不一致".to_owned(),
        ));
    }
    state
        .session
        .validate_recovered_mutation_records(&expected_mutations)?;
    Ok(())
}

fn expected_confirmation_at(
    state: &ExecutionSessionStateV1,
    sequence: u64,
) -> Option<ResponseDeliveryConfirmationV1> {
    state
        .compacted_confirmed_private_deliveries
        .get(&sequence)
        .map(|delivery| delivery.confirmation.clone())
        .or_else(|| {
            state
                .confirmed_private_deliveries
                .get(&sequence)
                .map(SealedPrivateDeliveryV1::confirmation)
        })
}

fn validate_confirmed_private_deliveries(
    state: &ExecutionSessionStateV1,
    downstream_durable_confirmation: Option<&ResponseDeliveryConfirmationV1>,
) -> Result<(), SessionCheckpointErrorV1> {
    let process_confirmed_through = state.session.private_feedback_sequence();
    let mut confirmed_private_history = Vec::new();
    let expected_count = usize::try_from(process_confirmed_through).map_err(|_| {
        SessionCheckpointErrorV1::InvalidSessionState(
            "进程内确认水位无法映射为已确认载荷数量".to_owned(),
        )
    })?;
    if state
        .compacted_confirmed_private_deliveries
        .len()
        .checked_add(state.confirmed_private_deliveries.len())
        .is_none_or(|count| count != expected_count)
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "已确认私有载荷正文不是从 1 开始的连续前缀".to_owned(),
        ));
    }
    for sequence in 1..=process_confirmed_through {
        let compacted = state.compacted_confirmed_private_deliveries.get(&sequence);
        let full = state.confirmed_private_deliveries.get(&sequence);
        if compacted.is_some() == full.is_some() {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "已确认私有载荷序号必须恰在完整或裁剪前缀之一：{sequence}"
            )));
        }
        let confirmation = compacted
            .map(|delivery| delivery.confirmation.clone())
            .or_else(|| full.map(SealedPrivateDeliveryV1::confirmation))
            .expect("上方已经证明正文恰存在一处");
        if confirmation.session_id != state.session.session_spec().session_id
            || confirmation.private_feedback_sequence != sequence
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "已确认私有载荷序号或会话身份不符：{sequence}"
            )));
        }
        if let Some(delivery) = full {
            let recomputed_payload_sha256 = delivery.recompute_payload_sha256()?;
            if confirmation.payload_sha256 != recomputed_payload_sha256 {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "已确认私有载荷正文与摘要不符：{sequence}"
                )));
            }
            delivery.validate_account_deltas()?;
            confirmed_private_history.push(delivery.received_at_and_emission_sequence());
        } else if let Some(compacted) = compacted {
            for delta in &compacted.account_deltas {
                validate_platform_account_delta_integrity(delta).map_err(|error| {
                    SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "已裁剪确认输出含损坏账户变化：{sequence}:{error}"
                    ))
                })?;
            }
            confirmed_private_history.push(compacted.received_at_and_emission_sequence());
        }
    }
    state
        .session
        .validate_recovered_private_history(&confirmed_private_history)?;
    if let Some(durable) = downstream_durable_confirmation
        && (durable.private_feedback_sequence == 0
            || durable.private_feedback_sequence > process_confirmed_through
            || expected_confirmation_at(state, durable.private_feedback_sequence).as_ref()
                != Some(durable))
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "下游耐久消费水位与已确认载荷正文不符".to_owned(),
        ));
    }
    Ok(())
}

impl SimExecutionSessionV1 {
    fn require_durable_business_writes_open(&self) -> Result<(), SessionError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?
            .require_business_writes_open()
    }

    fn require_new_ingress_open(&self) -> Result<(), SessionError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?
            .require_new_ingress_open(Some(&self.session.session_spec().session_id))
    }

    fn reserve_transient_input(&self, quantity: ResourceQuantityV1) -> Result<(), SessionError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?
            .reserve_transient_input(&self.session.session_spec().session_id, quantity)
    }

    fn release_transient_input(&self) -> Result<(), SessionError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?
            .release_transient_input(&self.session.session_spec().session_id)
    }

    fn runtime_state(&self) -> ExecutionSessionStateV1 {
        ExecutionSessionStateV1 {
            session: self.session.clone(),
            pending_requests: self.pending_requests.clone(),
            mutation_payloads: self.mutation_payloads.clone(),
            submission_effect_records: self.submission_effect_records.clone(),
            cancel_effect_records: self.cancel_effect_records.clone(),
            query_state: self.query_state.clone(),
            direct_response_outbox: self.direct_response_outbox.clone(),
            lifecycle_outbox: self.lifecycle_outbox.clone(),
            compacted_confirmed_private_deliveries: self
                .compacted_confirmed_private_deliveries
                .clone(),
            confirmed_private_deliveries: self.confirmed_private_deliveries.clone(),
            accepting_new_ingress: self.accepting_new_ingress,
        }
    }

    fn replace_runtime_state(&mut self, state: ExecutionSessionStateV1) {
        self.session = state.session;
        self.pending_requests = state.pending_requests;
        self.mutation_payloads = state.mutation_payloads;
        self.submission_effect_records = state.submission_effect_records;
        self.cancel_effect_records = state.cancel_effect_records;
        self.query_state = state.query_state;
        self.direct_response_outbox = state.direct_response_outbox;
        self.lifecycle_outbox = state.lifecycle_outbox;
        self.compacted_confirmed_private_deliveries = state.compacted_confirmed_private_deliveries;
        self.confirmed_private_deliveries = state.confirmed_private_deliveries;
        self.accepting_new_ingress = state.accepting_new_ingress;
    }

    fn commit_durable_transition(
        &self,
        expected: &ExecutionSessionCheckpointV1,
        candidate: ExecutionSessionCheckpointV1,
        evidence: DurableTransactionEvidenceV1,
    ) -> Result<(), SessionError> {
        self.commit_durable_transition_inner(expected, candidate, evidence, false)
    }

    fn commit_durable_transition_new_ingress(
        &self,
        expected: &ExecutionSessionCheckpointV1,
        candidate: ExecutionSessionCheckpointV1,
        evidence: DurableTransactionEvidenceV1,
    ) -> Result<(), SessionError> {
        self.commit_durable_transition_inner(expected, candidate, evidence, true)
    }

    fn commit_durable_transition_inner(
        &self,
        expected: &ExecutionSessionCheckpointV1,
        candidate: ExecutionSessionCheckpointV1,
        mut evidence: DurableTransactionEvidenceV1,
        new_ingress: bool,
    ) -> Result<(), SessionError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        let session_id = self.session.session_spec().session_id.clone();
        let mut coordinator = persistence
            .lock()
            .map_err(|_| SessionError::Durability("耐久协调器锁不可用".to_owned()))?;
        coordinator.require_business_writes_open()?;
        if coordinator.current.sessions.get(&session_id) != Some(expected) {
            return Err(SessionError::Durability(
                "会话内存基线与当前耐久检查点不一致".to_owned(),
            ));
        }
        if candidate == *expected {
            return Ok(());
        }
        if new_ingress {
            coordinator.require_new_ingress_open(Some(&session_id))?;
        }
        let before_deltas = account_deltas_in_checkpoint(expected)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        let after_deltas = account_deltas_in_checkpoint(&candidate)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        evidence.account_deltas = after_deltas
            .into_iter()
            .filter_map(|(sequence, delta)| {
                (!before_deltas.contains_key(&sequence)).then_some(delta)
            })
            .collect();
        let before_action_results = logical_action_results_in_checkpoint(expected)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        let after_action_results = logical_action_results_in_checkpoint(&candidate)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        for (action_identity, result) in &before_action_results {
            if after_action_results
                .get(action_identity)
                .is_some_and(|after| after != result)
            {
                return Err(SessionError::Durability(format!(
                    "耐久逻辑动作结果在形成后发生变化：{action_identity}"
                )));
            }
        }
        evidence.logical_action_results = after_action_results
            .into_iter()
            .filter_map(|(action_identity, result)| {
                (!before_action_results.contains_key(&action_identity)).then_some(result)
            })
            .collect();
        let before_drafts = private_output_drafts_in_checkpoint(expected)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        let after_drafts = private_output_drafts_in_checkpoint(&candidate)
            .map_err(|error| SessionError::Durability(error.to_string()))?;
        for (delivery_id, draft) in &after_drafts {
            if let Some(before) = before_drafts.get(delivery_id)
                && before != draft
            {
                return Err(SessionError::Durability(format!(
                    "耐久私有输出草稿在形成后发生变化：{delivery_id}"
                )));
            }
        }
        evidence.private_output_drafts = after_drafts
            .into_iter()
            .filter_map(|(delivery_id, draft)| {
                (!before_drafts.contains_key(&delivery_id)).then_some(draft)
            })
            .collect();
        let before_revision = expected.session.platform_state_revision();
        let after_revision = candidate.session.platform_state_revision();
        evidence.query_projection_revision =
            (after_revision != before_revision).then_some(after_revision);
        let before_logical_platform_through = expected.session.logical_platform_driven_through();
        let after_logical_platform_through = candidate.session.logical_platform_driven_through();
        evidence.logical_platform_driven_through_utc_ms = (after_logical_platform_through
            != before_logical_platform_through)
            .then_some(after_logical_platform_through)
            .flatten();
        let before_private_schedule_through =
            expected.session.private_fact_schedule_closed_through();
        let after_private_schedule_through =
            candidate.session.private_fact_schedule_closed_through();
        evidence.private_fact_schedule_closed_through_utc_ms = (after_private_schedule_through
            != before_private_schedule_through)
            .then_some(after_private_schedule_through)
            .flatten();
        let mut run_checkpoint = coordinator.current.clone();
        run_checkpoint.sessions.insert(session_id, candidate);
        if new_ingress {
            coordinator.commit_new_ingress(&evidence, run_checkpoint)
        } else {
            coordinator.commit(&evidence, run_checkpoint)
        }
    }

    fn commit_durable_candidate(
        &self,
        candidate: ExecutionSessionCheckpointV1,
        evidence: DurableTransactionEvidenceV1,
    ) -> Result<(), SessionError> {
        let expected = self.durable_checkpoint();
        self.commit_durable_transition(&expected, candidate, evidence)
    }

    fn commit_durable_candidate_new_ingress(
        &self,
        candidate: ExecutionSessionCheckpointV1,
        evidence: DurableTransactionEvidenceV1,
    ) -> Result<(), SessionError> {
        let expected = self.durable_checkpoint();
        self.commit_durable_transition_new_ingress(&expected, candidate, evidence)
    }

    fn close_ingress_durably(&mut self) -> Result<(), SessionError> {
        let before_state = self.runtime_state();
        let expected = self.durable_checkpoint();
        self.accepting_new_ingress = false;
        let candidate = self.durable_checkpoint();
        if let Err(error) = self.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::IngressClosed {
                session_id: self.session.session_spec().session_id.clone(),
            }),
        ) {
            self.replace_runtime_state(before_state);
            return Err(error);
        }
        Ok(())
    }

    fn advance_downstream_durability(
        &mut self,
        proof: DownstreamDurabilityProofV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        self.require_durable_business_writes_open()?;
        if self.persistence.is_none() {
            return Err(SessionError::Durability(
                "未启用本地耐久存储的会话不能推进下游耐久水位".to_owned(),
            ));
        }
        if proof.account_instance_key != *self.session.execution_state().account_instance_key() {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        let current_sequence = self
            .downstream_durable_confirmation
            .as_ref()
            .map_or(0, |confirmation| confirmation.private_feedback_sequence);
        if self.downstream_durable_confirmation.as_ref() == Some(&proof.durable_through) {
            return Ok(());
        }
        let expected_sequence = current_sequence
            .checked_add(1)
            .ok_or(SessionError::PrivateEmissionSequenceConflict(u64::MAX))?;
        let actual_sequence = proof.durable_through.private_feedback_sequence;
        if actual_sequence != expected_sequence
            || self
                .confirmed_private_deliveries
                .get(&actual_sequence)
                .is_none_or(|delivery| delivery.confirmation() != proof.durable_through)
        {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        let compacted = CompactedConfirmedPrivateDeliveryV1::try_from_delivery(
            self.confirmed_private_deliveries
                .get(&actual_sequence)
                .expect("上方已经核对当前完整确认输出"),
        )
        .map_err(|error| SessionError::Durability(error.to_string()))?;
        let expected = self.durable_checkpoint();
        let mut candidate = expected.clone();
        candidate.downstream_durable_confirmation = Some(proof.durable_through.clone());
        if candidate
            .compacted_confirmed_private_deliveries
            .insert(actual_sequence, compacted.clone())
            .is_some()
            || candidate
                .confirmed_private_deliveries
                .remove(&actual_sequence)
                .is_none()
        {
            return Err(SessionError::Durability(
                "下游耐久水位对应输出不能原子裁剪".to_owned(),
            ));
        }
        self.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(
                DurableTransactionFactV1::DownstreamDurabilityAdvanced {
                    session_id: self.session.session_spec().session_id.clone(),
                    proof: proof.clone(),
                },
            ),
        )?;
        self.compacted_confirmed_private_deliveries
            .insert(actual_sequence, compacted);
        self.confirmed_private_deliveries.remove(&actual_sequence);
        self.downstream_durable_confirmation = Some(proof.durable_through);
        self.bridge_replayed_through_private_feedback_sequence = self
            .bridge_replayed_through_private_feedback_sequence
            .max(actual_sequence);
        Ok(())
    }

    fn from_recovered_state(
        state: ExecutionSessionStateV1,
        response_bridge: SubmissionResponseBridgeV1,
        persistence: Option<SharedPersistentRunV1>,
        downstream_durable_confirmation: Option<ResponseDeliveryConfirmationV1>,
    ) -> Self {
        Self {
            session: state.session,
            pending_requests: state.pending_requests,
            mutation_payloads: state.mutation_payloads,
            submission_effect_records: state.submission_effect_records,
            cancel_effect_records: state.cancel_effect_records,
            query_state: state.query_state,
            direct_response_outbox: state.direct_response_outbox,
            lifecycle_outbox: state.lifecycle_outbox,
            compacted_confirmed_private_deliveries: state.compacted_confirmed_private_deliveries,
            confirmed_private_deliveries: state.confirmed_private_deliveries,
            accepting_new_ingress: state.accepting_new_ingress,
            response_bridge,
            persistence,
            bridge_replayed_through_private_feedback_sequence: downstream_durable_confirmation
                .as_ref()
                .map_or(0, |confirmation| confirmation.private_feedback_sequence),
            downstream_durable_confirmation,
        }
    }

    fn durable_checkpoint(&self) -> ExecutionSessionCheckpointV1 {
        ExecutionSessionStateV1 {
            session: self.session.clone(),
            pending_requests: self.pending_requests.clone(),
            mutation_payloads: self.mutation_payloads.clone(),
            submission_effect_records: self.submission_effect_records.clone(),
            cancel_effect_records: self.cancel_effect_records.clone(),
            query_state: self.query_state.clone(),
            direct_response_outbox: self.direct_response_outbox.clone(),
            lifecycle_outbox: self.lifecycle_outbox.clone(),
            compacted_confirmed_private_deliveries: self
                .compacted_confirmed_private_deliveries
                .clone(),
            confirmed_private_deliveries: self.confirmed_private_deliveries.clone(),
            accepting_new_ingress: self.accepting_new_ingress,
        }
        .checkpoint(self.downstream_durable_confirmation.clone())
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Task41DiagnosticsV1 {
    pub(crate) market_external_book_level_count: usize,
    pub(crate) execution_queue_level_count: usize,
    pub(crate) execution_queue_segment_count: usize,
    pub(crate) execution_queue_debt_base_units: u128,
    pub(crate) input_seal: Option<(
        crate::market::MarketEventPathV1,
        crate::session::InputPathSealOutcomeV1,
    )>,
    pub(crate) hot_orders: Vec<OrderProjectionV1>,
    pub(crate) compact_orders: Vec<OrderProjectionV1>,
    pub(crate) hot_trades: Vec<TradeProjectionV1>,
    pub(crate) compact_trades: Vec<TradeProjectionV1>,
    pub(crate) retained_rebate_fill_state_count: usize,
    pub(crate) query_root_body_count: usize,
    pub(crate) issued_cursor_count: usize,
    pub(crate) earliest_retained_root_sequence: u64,
    pub(crate) pending_direct_response_count: usize,
    pub(crate) pending_lifecycle_feedback_count: usize,
    pub(crate) compacted_confirmed_private_delivery_count: usize,
    pub(crate) full_confirmed_private_delivery_count: usize,
    pub(crate) confirmed_direct_response_confirmations: Vec<ResponseDeliveryConfirmationV1>,
    pub(crate) confirmed_lifecycle_feedback_confirmations: Vec<ResponseDeliveryConfirmationV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SubmissionEffectRecordV1 {
    outcome: SubmissionItemOutcomeV1,
    formed_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OrderLifecycleFeedbackSeedV1 {
    platform_order_id: String,
    condition_id: String,
    token_id: String,
    event_kind: OrderLifecycleEventKindV1,
    order_status: SimulatedOrderStatusV1,
    venue_dust_closed_base_units: u128,
    venue_dust_released_quote_units: u128,
    market_resolved_canceled_base_units: u128,
    market_resolved_released_quote_units: u128,
    account_delta: Arc<PlatformAccountDeltaV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SettlementLifecycleFeedbackSeedV1 {
    settlement_identity: String,
    platform_event_at_utc_ms: i64,
    event: SettlementLifecycleEventDraftV1,
}

type PreparedSubmissionResultV1 = (
    SubmissionItemOutcomeV1,
    Option<ExecutionStateV1>,
    Vec<OrderLifecycleFeedbackSeedV1>,
    Vec<TradeFinalityLifecycleEffectsV1>,
);

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogicalPlatformDriveErrorV1 {
    Session(SessionError),
    Matching(MatchingStateErrorV1),
}

impl From<SessionError> for LogicalPlatformDriveErrorV1 {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

impl From<MatchingStateErrorV1> for LogicalPlatformDriveErrorV1 {
    fn from(value: MatchingStateErrorV1) -> Self {
        Self::Matching(value)
    }
}

impl From<LogicalPlatformDriveErrorV1> for SubmissionProcessingErrorV1 {
    fn from(value: LogicalPlatformDriveErrorV1) -> Self {
        match value {
            LogicalPlatformDriveErrorV1::Session(error) => Self::Session(error),
            LogicalPlatformDriveErrorV1::Matching(error) => Self::Matching(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CancelTargetEffectV1 {
    platform_order_id: String,
    effect_identity: String,
    outcome: CancelTargetOutcomeV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CancelEffectRecordV1 {
    originating_global_request_id: String,
    formed_at_utc_ms: i64,
    targets: Vec<CancelTargetEffectV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CancelTargetInputGroupV1 {
    platform_order_id: String,
    unique_target_ordinal: u16,
    input_item_refs: Vec<CancelInputItemRefV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ListQueryMethodV1 {
    Orders,
    Trades,
}

impl ListQueryMethodV1 {
    fn identity(self) -> &'static str {
        match self {
            Self::Orders => "orders",
            Self::Trades => "trades",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum QueryRootRowsV1 {
    Orders(Arc<[OrderProjectionV1]>),
    Trades(Arc<[TradeProjectionV1]>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QueryRootV1 {
    root_sequence: u64,
    root_identity: Sha256Hex,
    method: ListQueryMethodV1,
    filter_sha256: Sha256Hex,
    query_projection_revision: u64,
    retention_floor_at_issue: u64,
    projection_sha256: Sha256Hex,
    rows: QueryRootRowsV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IssuedCursorV1 {
    root_sequence: u64,
    root_identity: Sha256Hex,
    method: ListQueryMethodV1,
    filter_sha256: Sha256Hex,
    query_projection_revision: u64,
    retention_floor_at_issue: u64,
    next_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QueryStateV1 {
    next_root_sequence: u64,
    earliest_retained_root_sequence: u64,
    roots: BTreeMap<u64, QueryRootV1>,
    issued_cursors: BTreeMap<String, IssuedCursorV1>,
    continuation_by_request: BTreeMap<String, IssuedCursorV1>,
    order_history_retained_from_utc_ms: Option<i64>,
    trade_history_retained_from_utc_ms: Option<i64>,
    terminal_projections: Arc<TerminalQueryProjectionsV1>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct TerminalQueryProjectionsV1 {
    orders: BTreeMap<String, OrderProjectionV1>,
    trades: BTreeMap<String, TradeProjectionV1>,
}

impl QueryStateV1 {
    fn new() -> Self {
        Self {
            next_root_sequence: 1,
            earliest_retained_root_sequence: 1,
            roots: BTreeMap::new(),
            issued_cursors: BTreeMap::new(),
            continuation_by_request: BTreeMap::new(),
            order_history_retained_from_utc_ms: None,
            trade_history_retained_from_utc_ms: None,
            terminal_projections: Arc::new(TerminalQueryProjectionsV1::default()),
        }
    }

    fn validate_recovered(
        &self,
        spec: &SessionSpecV2,
        requests: &BTreeMap<String, PendingDomainRequestV1>,
        terminal_condition_ids: &BTreeSet<String>,
        execution: &ExecutionStateV1,
        evicted_markets: &BTreeMap<String, DurableEvictedMarketStateV1>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        if self.next_root_sequence == 0
            || self.earliest_retained_root_sequence == 0
            || self.earliest_retained_root_sequence > self.next_root_sequence
            || self.roots.keys().any(|sequence| {
                *sequence < self.earliest_retained_root_sequence
                    || *sequence >= self.next_root_sequence
            })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "恢复查询根水位或序号范围不一致".to_owned(),
            ));
        }
        for (sequence, root) in &self.roots {
            let projection_sha256 = match (&root.method, &root.rows) {
                (ListQueryMethodV1::Orders, QueryRootRowsV1::Orders(rows)) => {
                    orders_projection_sha256(rows)
                }
                (ListQueryMethodV1::Trades, QueryRootRowsV1::Trades(rows)) => {
                    trades_projection_sha256(rows)
                }
                _ => {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "查询根方法与正文类型不一致：{sequence}"
                    )));
                }
            };
            let expected_identity = query_root_identity(
                spec,
                root.method,
                &root.filter_sha256,
                root.query_projection_revision,
                root.retention_floor_at_issue,
                *sequence,
                &projection_sha256,
            );
            if root.root_sequence != *sequence
                || root.projection_sha256 != projection_sha256
                || root.root_identity != expected_identity
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "查询根身份或投影摘要不符：{sequence}"
                )));
            }
        }
        for (token, binding) in &self.issued_cursors {
            if !self.roots.contains_key(&binding.root_sequence)
                || cursor_token(binding) != *token
                || self.roots.get(&binding.root_sequence).is_none_or(|root| {
                    root.root_identity != binding.root_identity
                        || root.method != binding.method
                        || root.filter_sha256 != binding.filter_sha256
                        || root.query_projection_revision != binding.query_projection_revision
                        || root.retention_floor_at_issue != binding.retention_floor_at_issue
                        || binding.next_index == 0
                        || binding.next_index % SIM_QUERY_PAGE_SIZE_V1 != 0
                        || binding.next_index
                            >= match &root.rows {
                                QueryRootRowsV1::Orders(rows) => rows.len(),
                                QueryRootRowsV1::Trades(rows) => rows.len(),
                            }
                })
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(
                    "恢复查询游标身份、根引用或下一位置不一致".to_owned(),
                ));
            }
        }
        let expected_continuations = requests
            .iter()
            .filter_map(|(request_id, request)| match request {
                PendingDomainRequestV1::Orders(request) => request
                    .cursor
                    .as_ref()
                    .map(|cursor| (request_id, cursor.as_str())),
                PendingDomainRequestV1::Trades(request) => request
                    .cursor
                    .as_ref()
                    .map(|cursor| (request_id, cursor.as_str())),
                _ => None,
            })
            .map(|(request_id, token)| (request_id.clone(), token.to_owned()))
            .collect::<BTreeMap<_, _>>();
        if self.continuation_by_request.len() != expected_continuations.len()
            || self
                .continuation_by_request
                .iter()
                .any(|(request_id, binding)| {
                    expected_continuations.get(request_id).is_none_or(|token| {
                        *token != cursor_token(binding)
                            || (binding.root_sequence >= self.earliest_retained_root_sequence
                                && self
                                    .issued_cursors
                                    .get(token)
                                    .is_none_or(|issued| issued != binding))
                            || (binding.root_sequence < self.earliest_retained_root_sequence
                                && self.issued_cursors.contains_key(token))
                            || requests
                                .get(request_id)
                                .is_none_or(|request| match request {
                                    PendingDomainRequestV1::Orders(request) => {
                                        binding.method != ListQueryMethodV1::Orders
                                            || binding.filter_sha256
                                                != orders_filter_sha256(&request.filter)
                                    }
                                    PendingDomainRequestV1::Trades(request) => {
                                        binding.method != ListQueryMethodV1::Trades
                                            || binding.filter_sha256
                                                != trades_filter_sha256(&request.filter)
                                    }
                                    _ => true,
                                })
                    })
                })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "恢复查询续页映射未精确绑定请求中的已签发游标".to_owned(),
            ));
        }
        let mut expected_terminal_orders = execution
            .orders()
            .filter(|order| {
                terminal_condition_ids.contains(&order.condition_id)
                    && self.terminal_order_is_retained(order.last_updated_at_utc_ms)
            })
            .map(|order| {
                project_order(order)
                    .map(|projection| (projection.platform_order_id.clone(), projection))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "恢复终态订单查询投影失败：{error:?}"
                ))
            })?;
        let mut expected_terminal_trades = execution
            .fills()
            .filter(|fill| {
                terminal_condition_ids.contains(&fill.condition_id)
                    && self.trade_is_retained(fill.matched_at_utc_ms)
            })
            .map(|fill| {
                project_trade(fill)
                    .map(|projection| (projection.simulated_trade_id.clone(), projection))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "恢复终态成交查询投影失败：{error:?}"
                ))
            })?;
        for (condition_id, (orders, trades, witness)) in evicted_markets {
            if witness.condition_id != *condition_id
                || !terminal_condition_ids.contains(condition_id)
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "驱逐市场紧凑事实与会话终态集合不一致：{condition_id}"
                )));
            }
            for (platform_order_id, order) in orders {
                if self.terminal_order_is_retained(order.last_updated_at_utc_ms)
                    && expected_terminal_orders
                        .insert(platform_order_id.clone(), order.clone())
                        .is_some()
                {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "驱逐市场终态订单与热投影重复：{platform_order_id}"
                    )));
                }
            }
            for (trade_id, trade) in trades {
                if self.trade_is_retained(trade.matched_at_utc_ms)
                    && expected_terminal_trades
                        .insert(trade_id.clone(), trade.clone())
                        .is_some()
                {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "驱逐市场终态成交与热投影重复：{trade_id}"
                    )));
                }
            }
        }
        if self.terminal_projections.orders != expected_terminal_orders
            || self.terminal_projections.trades != expected_terminal_trades
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "紧凑终态查询投影与当前热订单和成交不一致".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_durable_action_results(
        &self,
        spec: &SessionSpecV2,
        requests: &BTreeMap<String, PendingDomainRequestV1>,
        registrations: &BTreeMap<String, DurableRequestRegistrationV1>,
        durable_results: &BTreeMap<String, DurableLogicalActionResultV1>,
        durable_contexts: &BTreeMap<String, DurableLogicalActionRecoveryContextV1>,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let error = |message: String| SimExecutionPersistenceErrorV1::Session(message);
        let mut initial_pages = Vec::new();
        let mut expected_cursor_tokens = BTreeSet::new();
        let mut continuation_pages = Vec::new();

        for result in durable_results.values() {
            let context = durable_contexts
                .get(&result.action_identity)
                .ok_or_else(|| {
                    error(format!(
                        "查询动作结果缺少形成事务上下文：{}",
                        result.action_identity
                    ))
                })?;
            let request = requests.get(&result.global_request_id).ok_or_else(|| {
                error(format!(
                    "查询动作结果缺少请求正文：{}",
                    result.action_identity
                ))
            })?;
            let registration = registrations
                .get(&result.global_request_id)
                .ok_or_else(|| {
                    error(format!(
                        "查询动作结果缺少登记事实：{}",
                        result.action_identity
                    ))
                })?;
            let action = registration
                .actions
                .iter()
                .find(|action| action.action_identity == result.action_identity)
                .ok_or_else(|| {
                    error(format!(
                        "查询动作结果缺少动作键：{}",
                        result.action_identity
                    ))
                })?;
            let is_query_request = matches!(
                request,
                PendingDomainRequestV1::Order(_)
                    | PendingDomainRequestV1::Orders(_)
                    | PendingDomainRequestV1::Trades(_)
            );
            if is_query_request != result.query_projection_revision.is_some() {
                return Err(error(format!(
                    "查询动作结果与冻结修订号存在性不一致：{}",
                    result.action_identity
                )));
            }
            match (request, &result.part) {
                (
                    PendingDomainRequestV1::Order(_),
                    DirectResponsePartV1::Order { result: query, .. },
                ) => {
                    if result.query_projection_revision.is_none() {
                        return Err(error(format!(
                            "单笔订单查询动作结果缺少冻结修订号：{}",
                            result.action_identity
                        )));
                    }
                    if let OrderQueryResultV1::OutOfRetention(out_of_retention) = query {
                        self.validate_out_of_retention(
                            out_of_retention,
                            QueryRetentionScopeV1::OrderHistory,
                            context.query_retention.1,
                            None,
                            context.query_retention.0,
                            &result.action_identity,
                        )?;
                    }
                }
                (
                    PendingDomainRequestV1::Orders(request),
                    DirectResponsePartV1::Orders { result: query, .. },
                ) => {
                    let revision = result.query_projection_revision.ok_or_else(|| {
                        error(format!(
                            "订单列表查询动作结果缺少冻结修订号：{}",
                            result.action_identity
                        ))
                    })?;
                    match query {
                        OrdersQueryResultV1::Page(page) => {
                            if page.query_projection_revision != revision
                                || page.root_identity.len() != 64
                            {
                                return Err(error(format!(
                                    "订单列表查询响应与冻结修订号不一致：{}",
                                    result.action_identity
                                )));
                            }
                            if request.cursor.is_none() {
                                initial_pages.push((action.key, result));
                            } else {
                                continuation_pages.push(result);
                            }
                        }
                        OrdersQueryResultV1::OutOfRetention(_) if request.cursor.is_some() => {
                            continuation_pages.push(result);
                        }
                        OrdersQueryResultV1::OutOfRetention(_) => {
                            return Err(error(format!(
                                "初始订单列表查询不能返回根保留期外：{}",
                                result.action_identity
                            )));
                        }
                    }
                }
                (
                    PendingDomainRequestV1::Trades(request),
                    DirectResponsePartV1::Trades { result: query, .. },
                ) => {
                    let revision = result.query_projection_revision.ok_or_else(|| {
                        error(format!(
                            "成交列表查询动作结果缺少冻结修订号：{}",
                            result.action_identity
                        ))
                    })?;
                    match query {
                        TradesQueryResultV1::Page(page) => {
                            if page.query_projection_revision != revision
                                || page.root_identity.len() != 64
                            {
                                return Err(error(format!(
                                    "成交列表查询响应与冻结修订号不一致：{}",
                                    result.action_identity
                                )));
                            }
                            if request.cursor.is_none() {
                                initial_pages.push((action.key, result));
                            } else {
                                continuation_pages.push(result);
                            }
                        }
                        TradesQueryResultV1::OutOfRetention(out_of_retention)
                            if request.cursor.is_some() =>
                        {
                            continuation_pages.push(result);
                        }
                        TradesQueryResultV1::OutOfRetention(out_of_retention) => {
                            self.validate_out_of_retention(
                                out_of_retention,
                                QueryRetentionScopeV1::TradeHistory,
                                context.query_retention.2,
                                None,
                                context.query_retention.0,
                                &result.action_identity,
                            )?;
                        }
                    }
                }
                (
                    PendingDomainRequestV1::PostOrder(_)
                    | PendingDomainRequestV1::PostOrders(_)
                    | PendingDomainRequestV1::CancelOrder(_)
                    | PendingDomainRequestV1::CancelOrders(_)
                    | PendingDomainRequestV1::CancelAllOrders(_),
                    _,
                ) => {
                    if result.query_projection_revision.is_some() {
                        return Err(error(format!(
                            "变更动作结果错误携带查询修订号：{}",
                            result.action_identity
                        )));
                    }
                }
                _ => {
                    return Err(error(format!(
                        "查询动作结果类型与请求不一致：{}",
                        result.action_identity
                    )));
                }
            }
        }

        initial_pages.sort_by_key(|(key, _)| *key);
        let expected_next_root_sequence = u64::try_from(initial_pages.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| error("查询根序号数量溢出".to_owned()))?;
        if self.next_root_sequence != expected_next_root_sequence {
            return Err(error(
                "查询根下一序号与耐久初始列表查询结果不一致".to_owned(),
            ));
        }

        let mut expected_root_sequences = BTreeSet::new();
        for (index, (_, result)) in initial_pages.iter().enumerate() {
            let root_sequence =
                u64::try_from(index + 1).map_err(|_| error("查询根序号无法表示".to_owned()))?;
            match &result.part {
                DirectResponsePartV1::Orders {
                    result: OrdersQueryResultV1::Page(page),
                    ..
                } => {
                    self.validate_initial_orders_page(
                        spec,
                        root_sequence,
                        requests
                            .get(&result.global_request_id)
                            .and_then(|request| match request {
                                PendingDomainRequestV1::Orders(request) => Some(request),
                                _ => None,
                            })
                            .expect("初始订单列表结果已经核对请求类型"),
                        result
                            .query_projection_revision
                            .expect("初始订单列表结果已经核对冻结修订号"),
                        durable_contexts
                            .get(&result.action_identity)
                            .expect("初始订单列表结果已经核对形成事务上下文")
                            .query_retention
                            .0,
                        page,
                    )?;
                    if let Some(next_cursor) = page
                        .next_cursor
                        .as_ref()
                        .filter(|_| root_sequence >= self.earliest_retained_root_sequence)
                    {
                        expected_root_sequences.insert(root_sequence);
                        expected_cursor_tokens.insert(next_cursor.as_str().to_owned());
                    }
                }
                DirectResponsePartV1::Trades {
                    result: TradesQueryResultV1::Page(page),
                    ..
                } => {
                    self.validate_initial_trades_page(
                        spec,
                        root_sequence,
                        requests
                            .get(&result.global_request_id)
                            .and_then(|request| match request {
                                PendingDomainRequestV1::Trades(request) => Some(request),
                                _ => None,
                            })
                            .expect("初始成交列表结果已经核对请求类型"),
                        result
                            .query_projection_revision
                            .expect("初始成交列表结果已经核对冻结修订号"),
                        durable_contexts
                            .get(&result.action_identity)
                            .expect("初始成交列表结果已经核对形成事务上下文")
                            .query_retention
                            .0,
                        page,
                    )?;
                    if let Some(next_cursor) = page
                        .next_cursor
                        .as_ref()
                        .filter(|_| root_sequence >= self.earliest_retained_root_sequence)
                    {
                        expected_root_sequences.insert(root_sequence);
                        expected_cursor_tokens.insert(next_cursor.as_str().to_owned());
                    }
                }
                _ => unreachable!("初始查询页已经按类型筛选"),
            }
        }
        for result in continuation_pages {
            self.validate_continuation_page(
                requests,
                result,
                durable_contexts
                    .get(&result.action_identity)
                    .expect("续页查询结果已经核对形成事务上下文"),
            )?;
            let next_cursor = match &result.part {
                DirectResponsePartV1::Orders {
                    result: OrdersQueryResultV1::Page(page),
                    ..
                } => page.next_cursor.as_ref(),
                DirectResponsePartV1::Trades {
                    result: TradesQueryResultV1::Page(page),
                    ..
                } => page.next_cursor.as_ref(),
                _ => None,
            };
            if let Some(cursor) = next_cursor
                && self
                    .continuation_by_request
                    .get(&result.global_request_id)
                    .is_some_and(|binding| {
                        binding.root_sequence >= self.earliest_retained_root_sequence
                    })
            {
                expected_cursor_tokens.insert(cursor.as_str().to_owned());
            }
        }
        if self.roots.keys().copied().collect::<BTreeSet<_>>() != expected_root_sequences
            || self.issued_cursors.keys().cloned().collect::<BTreeSet<_>>()
                != expected_cursor_tokens
        {
            return Err(error(
                "查询根或已签发游标集合与耐久查询结果不一致".to_owned(),
            ));
        }
        Ok(())
    }

    fn retention_projection(&self) -> (u64, Option<i64>, Option<i64>) {
        (
            self.earliest_retained_root_sequence,
            self.order_history_retained_from_utc_ms,
            self.trade_history_retained_from_utc_ms,
        )
    }

    fn is_fully_pruned_for_session_retirement(&self) -> bool {
        self.earliest_retained_root_sequence == self.next_root_sequence
            && self.roots.is_empty()
            && self.issued_cursors.is_empty()
            && self.terminal_projections.orders.is_empty()
            && self.terminal_projections.trades.is_empty()
            && self.order_history_retained_from_utc_ms.is_some()
            && self.trade_history_retained_from_utc_ms.is_some()
    }

    fn retained_terminal_order(&self, platform_order_id: &str) -> Option<&OrderProjectionV1> {
        self.terminal_projections
            .orders
            .get(platform_order_id)
            .filter(|order| self.terminal_order_is_retained(order.last_updated_at_utc_ms))
    }

    fn terminal_order_is_retained(&self, last_updated_at_utc_ms: i64) -> bool {
        self.order_history_retained_from_utc_ms
            .is_none_or(|floor| last_updated_at_utc_ms >= floor)
    }

    fn trade_is_retained(&self, matched_at_utc_ms: i64) -> bool {
        self.trade_history_retained_from_utc_ms
            .is_none_or(|floor| matched_at_utc_ms >= floor)
    }

    fn hot_order_is_query_visible(
        &self,
        order: &SimOrderRecordV1,
    ) -> Result<bool, MatchingMathErrorV1> {
        let active = order
            .pending_maker_depletion_base_units
            .checked_add(order.open_base_units)
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
        Ok(active > 0 || self.terminal_order_is_retained(order.last_updated_at_utc_ms))
    }

    fn terminal_market_projection_matches(
        &self,
        condition_id: &str,
        execution: &ExecutionStateV1,
    ) -> Result<bool, MatchingMathErrorV1> {
        let expected_orders = execution
            .orders()
            .filter(|order| {
                order.condition_id == condition_id
                    && self.terminal_order_is_retained(order.last_updated_at_utc_ms)
            })
            .map(|order| {
                project_order(order)
                    .map(|projection| (projection.platform_order_id.clone(), projection))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let expected_trades = execution
            .fills()
            .filter(|fill| {
                fill.condition_id == condition_id && self.trade_is_retained(fill.matched_at_utc_ms)
            })
            .map(|fill| {
                project_trade(fill)
                    .map(|projection| (projection.simulated_trade_id.clone(), projection))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let actual_orders = self
            .terminal_projections
            .orders
            .iter()
            .filter(|(_, order)| order.condition_id.0 == condition_id)
            .map(|(key, order)| (key.clone(), order.clone()))
            .collect::<BTreeMap<_, _>>();
        let actual_trades = self
            .terminal_projections
            .trades
            .iter()
            .filter(|(_, trade)| trade.condition_id.0 == condition_id)
            .map(|(key, trade)| (key.clone(), trade.clone()))
            .collect::<BTreeMap<_, _>>();
        Ok(actual_orders == expected_orders && actual_trades == expected_trades)
    }

    fn terminal_market_projections(
        &self,
        condition_id: &str,
    ) -> (
        BTreeMap<String, OrderProjectionV1>,
        BTreeMap<String, TradeProjectionV1>,
    ) {
        let orders = self
            .terminal_projections
            .orders
            .iter()
            .filter(|(_, order)| order.condition_id.0 == condition_id)
            .map(|(key, order)| (key.clone(), order.clone()))
            .collect();
        let trades = self
            .terminal_projections
            .trades
            .iter()
            .filter(|(_, trade)| trade.condition_id.0 == condition_id)
            .map(|(key, trade)| (key.clone(), trade.clone()))
            .collect();
        (orders, trades)
    }

    fn validate_initial_orders_page(
        &self,
        spec: &SessionSpecV2,
        root_sequence: u64,
        request: &OrdersRequestV1,
        query_projection_revision: u64,
        retention_floor_at_issue: u64,
        page: &OrdersPageV1,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let error = |message: String| SimExecutionPersistenceErrorV1::Session(message);
        let filter_sha256 = orders_filter_sha256(&request.filter);
        let Some(cursor) = &page.next_cursor else {
            let projection_sha256 = orders_projection_sha256(&page.items);
            let expected_identity = query_root_identity(
                spec,
                ListQueryMethodV1::Orders,
                &filter_sha256,
                query_projection_revision,
                retention_floor_at_issue,
                root_sequence,
                &projection_sha256,
            );
            if page.items.len() > SIM_QUERY_PAGE_SIZE_V1
                || page.root_identity != expected_identity.0
            {
                return Err(error(format!(
                    "无后续游标的初始查询页身份或容量不一致：{root_sequence}"
                )));
            }
            return Ok(());
        };
        if root_sequence < self.earliest_retained_root_sequence {
            if self.roots.contains_key(&root_sequence)
                || self.issued_cursors.contains_key(cursor.as_str())
            {
                return Err(error(format!(
                    "已过期订单查询根仍保留热正文或普通游标：{root_sequence}"
                )));
            }
            return Ok(());
        }
        let binding = self
            .issued_cursors
            .get(cursor.as_str())
            .ok_or_else(|| error(format!("初始查询页引用未签发游标：{root_sequence}")))?;
        let root = self
            .roots
            .get(&root_sequence)
            .ok_or_else(|| error(format!("初始查询页缺少完整查询根：{root_sequence}")))?;
        let QueryRootRowsV1::Orders(rows) = &root.rows else {
            return Err(error(format!(
                "订单初始查询页引用了成交查询根：{root_sequence}"
            )));
        };
        if binding.root_sequence != root_sequence
            || binding.next_index != SIM_QUERY_PAGE_SIZE_V1
            || root.method != ListQueryMethodV1::Orders
            || root.filter_sha256 != filter_sha256
            || root.query_projection_revision != query_projection_revision
            || root.retention_floor_at_issue != retention_floor_at_issue
            || root.root_identity.0 != page.root_identity
            || binding.root_identity != root.root_identity
            || page.items.len() != SIM_QUERY_PAGE_SIZE_V1
            || rows.get(..page.items.len()) != Some(page.items.as_slice())
        {
            return Err(error(format!(
                "初始查询页与完整查询根不一致：{root_sequence}"
            )));
        }
        Ok(())
    }

    fn validate_initial_trades_page(
        &self,
        spec: &SessionSpecV2,
        root_sequence: u64,
        request: &TradesRequestV1,
        query_projection_revision: u64,
        retention_floor_at_issue: u64,
        page: &TradesPageV1,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let error = |message: String| SimExecutionPersistenceErrorV1::Session(message);
        let filter_sha256 = trades_filter_sha256(&request.filter);
        let Some(cursor) = &page.next_cursor else {
            let projection_sha256 = trades_projection_sha256(&page.items);
            let expected_identity = query_root_identity(
                spec,
                ListQueryMethodV1::Trades,
                &filter_sha256,
                query_projection_revision,
                retention_floor_at_issue,
                root_sequence,
                &projection_sha256,
            );
            if page.items.len() > SIM_QUERY_PAGE_SIZE_V1
                || page.root_identity != expected_identity.0
            {
                return Err(error(format!(
                    "无后续游标的初始成交查询页身份或容量不一致：{root_sequence}"
                )));
            }
            return Ok(());
        };
        if root_sequence < self.earliest_retained_root_sequence {
            if self.roots.contains_key(&root_sequence)
                || self.issued_cursors.contains_key(cursor.as_str())
            {
                return Err(error(format!(
                    "已过期成交查询根仍保留热正文或普通游标：{root_sequence}"
                )));
            }
            return Ok(());
        }
        let binding = self
            .issued_cursors
            .get(cursor.as_str())
            .ok_or_else(|| error(format!("成交初始查询页引用未签发游标：{root_sequence}")))?;
        let root = self
            .roots
            .get(&root_sequence)
            .ok_or_else(|| error(format!("成交初始查询页缺少完整查询根：{root_sequence}")))?;
        let QueryRootRowsV1::Trades(rows) = &root.rows else {
            return Err(error(format!(
                "成交初始查询页引用了订单查询根：{root_sequence}"
            )));
        };
        if binding.root_sequence != root_sequence
            || binding.next_index != SIM_QUERY_PAGE_SIZE_V1
            || root.method != ListQueryMethodV1::Trades
            || root.filter_sha256 != filter_sha256
            || root.query_projection_revision != query_projection_revision
            || root.retention_floor_at_issue != retention_floor_at_issue
            || root.root_identity.0 != page.root_identity
            || binding.root_identity != root.root_identity
            || page.items.len() != SIM_QUERY_PAGE_SIZE_V1
            || rows.get(..page.items.len()) != Some(page.items.as_slice())
        {
            return Err(error(format!(
                "成交初始查询页与完整查询根不一致：{root_sequence}"
            )));
        }
        Ok(())
    }

    fn validate_continuation_page(
        &self,
        requests: &BTreeMap<String, PendingDomainRequestV1>,
        result: &DurableLogicalActionResultV1,
        context: &DurableLogicalActionRecoveryContextV1,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        let error = |message: String| SimExecutionPersistenceErrorV1::Session(message);
        let request = requests
            .get(&result.global_request_id)
            .expect("续页动作结果已经核对请求存在");
        let (input_cursor, expected_method, expected_filter_sha256) = match request {
            PendingDomainRequestV1::Orders(request) => (
                request.cursor.as_ref(),
                ListQueryMethodV1::Orders,
                orders_filter_sha256(&request.filter),
            ),
            PendingDomainRequestV1::Trades(request) => (
                request.cursor.as_ref(),
                ListQueryMethodV1::Trades,
                trades_filter_sha256(&request.filter),
            ),
            _ => {
                return Err(error(format!(
                    "续页动作结果对应非列表查询请求：{}",
                    result.action_identity
                )));
            }
        };
        let input_cursor = input_cursor
            .ok_or_else(|| error(format!("续页查询缺少输入游标：{}", result.action_identity)))?;
        let binding = self
            .continuation_by_request
            .get(&result.global_request_id)
            .ok_or_else(|| {
                error(format!(
                    "续页查询缺少已登记游标绑定：{}",
                    result.action_identity
                ))
            })?;
        if cursor_token(binding) != input_cursor.as_str()
            || binding.method != expected_method
            || binding.filter_sha256 != expected_filter_sha256
            || result.query_projection_revision != Some(binding.query_projection_revision)
        {
            return Err(error(format!(
                "续页查询方法、过滤条件或冻结修订号与输入游标不一致：{}",
                result.action_identity
            )));
        }
        match &result.part {
            DirectResponsePartV1::Orders {
                result: OrdersQueryResultV1::OutOfRetention(out_of_retention),
                ..
            }
            | DirectResponsePartV1::Trades {
                result: TradesQueryResultV1::OutOfRetention(out_of_retention),
                ..
            } => {
                return self.validate_out_of_retention(
                    out_of_retention,
                    QueryRetentionScopeV1::RootSnapshot,
                    None,
                    Some(binding.root_sequence),
                    context.query_retention.0,
                    &result.action_identity,
                );
            }
            _ => {}
        }
        if context.query_retention.0 > binding.root_sequence {
            return Err(error(format!(
                "续页查询在根已越过保留水位后仍返回页面：{}",
                result.action_identity
            )));
        }
        if self.earliest_retained_root_sequence > binding.root_sequence {
            if self.roots.contains_key(&binding.root_sequence)
                || self.issued_cursors.contains_key(input_cursor.as_str())
            {
                return Err(error(format!(
                    "已过期续页根仍保留热正文或普通游标：{}",
                    result.action_identity
                )));
            }
            return Ok(());
        }
        let root = self.roots.get(&binding.root_sequence).ok_or_else(|| {
            error(format!(
                "续页查询缺少完整查询根：{}",
                result.action_identity
            ))
        })?;
        let end = match (&root.rows, &result.part) {
            (
                QueryRootRowsV1::Orders(rows),
                DirectResponsePartV1::Orders {
                    result: OrdersQueryResultV1::Page(page),
                    ..
                },
            ) => {
                let end = binding
                    .next_index
                    .saturating_add(SIM_QUERY_PAGE_SIZE_V1)
                    .min(rows.len());
                if rows.get(binding.next_index..end) != Some(page.items.as_slice())
                    || page.root_identity != binding.root_identity.0
                {
                    return Err(error(format!(
                        "订单续页正文与查询根不一致：{}",
                        result.action_identity
                    )));
                }
                (end, page.next_cursor.as_ref())
            }
            (
                QueryRootRowsV1::Trades(rows),
                DirectResponsePartV1::Trades {
                    result: TradesQueryResultV1::Page(page),
                    ..
                },
            ) => {
                let end = binding
                    .next_index
                    .saturating_add(SIM_QUERY_PAGE_SIZE_V1)
                    .min(rows.len());
                if rows.get(binding.next_index..end) != Some(page.items.as_slice())
                    || page.root_identity != binding.root_identity.0
                {
                    return Err(error(format!(
                        "成交续页正文与查询根不一致：{}",
                        result.action_identity
                    )));
                }
                (end, page.next_cursor.as_ref())
            }
            _ => {
                return Err(error(format!(
                    "续页查询方法与查询根不一致：{}",
                    result.action_identity
                )));
            }
        };
        let expected_next_cursor = (end.0
            < match &root.rows {
                QueryRootRowsV1::Orders(rows) => rows.len(),
                QueryRootRowsV1::Trades(rows) => rows.len(),
            })
        .then(|| {
            cursor_token(&IssuedCursorV1 {
                next_index: end.0,
                ..binding.clone()
            })
        });
        if end.1.map(OpaquePageCursorV1::as_str) != expected_next_cursor.as_deref() {
            return Err(error(format!(
                "续页查询后继游标与查询根不一致：{}",
                result.action_identity
            )));
        }
        Ok(())
    }

    fn validate_out_of_retention(
        &self,
        actual: &QueryOutOfRetentionV1,
        scope: QueryRetentionScopeV1,
        retained_from_utc_ms: Option<i64>,
        requested_root_sequence: Option<u64>,
        earliest_retained_root_sequence: u64,
        action_identity: &str,
    ) -> Result<(), SimExecutionPersistenceErrorV1> {
        if actual.scope != scope
            || actual.retained_from_utc_ms != retained_from_utc_ms
            || actual.requested_root_sequence != requested_root_sequence
            || actual.earliest_retained_root_sequence != earliest_retained_root_sequence
            || (scope == QueryRetentionScopeV1::RootSnapshot
                && requested_root_sequence
                    .is_none_or(|requested| requested >= earliest_retained_root_sequence))
            || (scope != QueryRetentionScopeV1::RootSnapshot && retained_from_utc_ms.is_none())
        {
            return Err(SimExecutionPersistenceErrorV1::Session(format!(
                "查询保留期外响应与形成事务水位不一致：{action_identity}"
            )));
        }
        Ok(())
    }

    fn prepare_pending_terminal_projections(
        &mut self,
        session: &mut SimSession,
    ) -> Result<(), SessionError> {
        let condition_ids = session.pending_terminal_projection_condition_ids();
        if condition_ids.is_empty() {
            return Ok(());
        }
        for condition_id in &condition_ids {
            let orders = session
                .execution_state()
                .orders()
                .filter(|order| {
                    &order.condition_id == condition_id
                        && self.terminal_order_is_retained(order.last_updated_at_utc_ms)
                })
                .map(project_order)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| SessionError::ExecutionState(format!("{error:?}")))?;
            let trades = session
                .execution_state()
                .fills()
                .filter(|fill| {
                    &fill.condition_id == condition_id
                        && self.trade_is_retained(fill.matched_at_utc_ms)
                })
                .map(project_trade)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| SessionError::ExecutionState(format!("{error:?}")))?;
            let terminal_projections = Arc::make_mut(&mut self.terminal_projections);
            for projection in orders {
                match terminal_projections
                    .orders
                    .get(&projection.platform_order_id)
                {
                    Some(existing) if existing != &projection => {
                        return Err(SessionError::QueryIdentityConflict(format!(
                            "compact-order:{}",
                            projection.platform_order_id
                        )));
                    }
                    Some(_) => {}
                    None => {
                        terminal_projections
                            .orders
                            .insert(projection.platform_order_id.clone(), projection);
                    }
                }
            }
            for projection in trades {
                match terminal_projections
                    .trades
                    .get(&projection.simulated_trade_id)
                {
                    Some(existing) if existing != &projection => {
                        return Err(SessionError::QueryIdentityConflict(format!(
                            "compact-trade:{}",
                            projection.simulated_trade_id
                        )));
                    }
                    Some(_) => {}
                    None => {
                        terminal_projections
                            .trades
                            .insert(projection.simulated_trade_id.clone(), projection);
                    }
                }
            }
        }
        session.commit_terminal_projections_prepared(&condition_ids);
        Ok(())
    }

    fn bind_continuation_for_request(
        &mut self,
        global_request_id: &str,
        request: &PendingDomainRequestV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let prepared = match request {
            PendingDomainRequestV1::Orders(request) => request
                .cursor
                .as_ref()
                .map(|cursor| {
                    self.validate_cursor(
                        cursor,
                        ListQueryMethodV1::Orders,
                        &orders_filter_sha256(&request.filter),
                    )
                })
                .transpose()?,
            PendingDomainRequestV1::Trades(request) => request
                .cursor
                .as_ref()
                .map(|cursor| {
                    self.validate_cursor(
                        cursor,
                        ListQueryMethodV1::Trades,
                        &trades_filter_sha256(&request.filter),
                    )
                })
                .transpose()?,
            _ => None,
        };
        if let Some(prepared) = prepared {
            if let Some(existing) = self.continuation_by_request.get(global_request_id) {
                if existing != &prepared {
                    return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                        global_request_id.to_owned(),
                    ));
                }
            } else {
                self.continuation_by_request
                    .insert(global_request_id.to_owned(), prepared);
            }
        }
        Ok(())
    }

    fn validate_cursor(
        &self,
        cursor: &OpaquePageCursorV1,
        method: ListQueryMethodV1,
        filter_sha256: &Sha256Hex,
    ) -> Result<IssuedCursorV1, SimExecutionContractErrorV1> {
        let binding = self.issued_cursors.get(cursor.as_str()).ok_or(
            SimExecutionContractErrorV1::InvalidRequest("列表游标未由本会话签发或已经损坏"),
        )?;
        if binding.method != method || &binding.filter_sha256 != filter_sha256 {
            return Err(SimExecutionContractErrorV1::InvalidRequest(
                "列表游标与方法或过滤条件不匹配",
            ));
        }
        Ok(binding.clone())
    }

    fn new_orders_page(
        &mut self,
        spec: &SessionSpecV2,
        filter_sha256: Sha256Hex,
        query_projection_revision: u64,
        rows: Vec<OrderProjectionV1>,
    ) -> Result<OrdersPageV1, SimExecutionContractErrorV1> {
        let projection_sha256 = orders_projection_sha256(&rows);
        let retention_floor_at_issue = self.earliest_retained_root_sequence;
        let (root_sequence, root_identity) = self.next_root_identity(
            spec,
            ListQueryMethodV1::Orders,
            &filter_sha256,
            query_projection_revision,
            retention_floor_at_issue,
            &projection_sha256,
        )?;
        let next_cursor = if rows.len() > SIM_QUERY_PAGE_SIZE_V1 {
            let root = QueryRootV1 {
                root_sequence,
                root_identity: root_identity.clone(),
                method: ListQueryMethodV1::Orders,
                filter_sha256: filter_sha256.clone(),
                query_projection_revision,
                retention_floor_at_issue,
                projection_sha256,
                rows: QueryRootRowsV1::Orders(Arc::from(rows.clone())),
            };
            self.roots.insert(root_sequence, root);
            Some(self.issue_cursor(IssuedCursorV1 {
                root_sequence,
                root_identity: root_identity.clone(),
                method: ListQueryMethodV1::Orders,
                filter_sha256,
                query_projection_revision,
                retention_floor_at_issue,
                next_index: SIM_QUERY_PAGE_SIZE_V1,
            }))
        } else {
            None
        };
        Ok(OrdersPageV1 {
            root_identity: root_identity.0,
            query_projection_revision,
            items: rows.into_iter().take(SIM_QUERY_PAGE_SIZE_V1).collect(),
            next_cursor,
        })
    }

    fn new_trades_page(
        &mut self,
        spec: &SessionSpecV2,
        filter_sha256: Sha256Hex,
        query_projection_revision: u64,
        rows: Vec<TradeProjectionV1>,
    ) -> Result<TradesPageV1, SimExecutionContractErrorV1> {
        let projection_sha256 = trades_projection_sha256(&rows);
        let retention_floor_at_issue = self.earliest_retained_root_sequence;
        let (root_sequence, root_identity) = self.next_root_identity(
            spec,
            ListQueryMethodV1::Trades,
            &filter_sha256,
            query_projection_revision,
            retention_floor_at_issue,
            &projection_sha256,
        )?;
        let next_cursor = if rows.len() > SIM_QUERY_PAGE_SIZE_V1 {
            let root = QueryRootV1 {
                root_sequence,
                root_identity: root_identity.clone(),
                method: ListQueryMethodV1::Trades,
                filter_sha256: filter_sha256.clone(),
                query_projection_revision,
                retention_floor_at_issue,
                projection_sha256,
                rows: QueryRootRowsV1::Trades(Arc::from(rows.clone())),
            };
            self.roots.insert(root_sequence, root);
            Some(self.issue_cursor(IssuedCursorV1 {
                root_sequence,
                root_identity: root_identity.clone(),
                method: ListQueryMethodV1::Trades,
                filter_sha256,
                query_projection_revision,
                retention_floor_at_issue,
                next_index: SIM_QUERY_PAGE_SIZE_V1,
            }))
        } else {
            None
        };
        Ok(TradesPageV1 {
            root_identity: root_identity.0,
            query_projection_revision,
            items: rows.into_iter().take(SIM_QUERY_PAGE_SIZE_V1).collect(),
            next_cursor,
        })
    }

    fn continue_orders(
        &mut self,
        global_request_id: &str,
    ) -> Result<OrdersQueryResultV1, SimExecutionContractErrorV1> {
        let binding = self.continuation(global_request_id, ListQueryMethodV1::Orders)?;
        if binding.root_sequence < self.earliest_retained_root_sequence {
            return self
                .root_out_of_retention(binding.root_sequence)
                .map(OrdersQueryResultV1::OutOfRetention);
        }
        let Some(root) = self.roots.get(&binding.root_sequence) else {
            return self
                .root_out_of_retention(binding.root_sequence)
                .map(OrdersQueryResultV1::OutOfRetention);
        };
        if root.root_identity != binding.root_identity
            || root.method != binding.method
            || root.filter_sha256 != binding.filter_sha256
            || root.query_projection_revision != binding.query_projection_revision
            || root.retention_floor_at_issue != binding.retention_floor_at_issue
        {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        let QueryRootRowsV1::Orders(rows) = &root.rows else {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ));
        };
        let end = binding
            .next_index
            .saturating_add(SIM_QUERY_PAGE_SIZE_V1)
            .min(rows.len());
        let items = rows
            .get(binding.next_index..end)
            .ok_or(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ))?
            .to_vec();
        let next_cursor = (end < rows.len()).then(|| {
            self.issue_cursor(IssuedCursorV1 {
                next_index: end,
                ..binding.clone()
            })
        });
        Ok(OrdersQueryResultV1::Page(OrdersPageV1 {
            root_identity: binding.root_identity.0,
            query_projection_revision: binding.query_projection_revision,
            items,
            next_cursor,
        }))
    }

    fn continue_trades(
        &mut self,
        global_request_id: &str,
    ) -> Result<TradesQueryResultV1, SimExecutionContractErrorV1> {
        let binding = self.continuation(global_request_id, ListQueryMethodV1::Trades)?;
        if binding.root_sequence < self.earliest_retained_root_sequence {
            return self
                .root_out_of_retention(binding.root_sequence)
                .map(TradesQueryResultV1::OutOfRetention);
        }
        let Some(root) = self.roots.get(&binding.root_sequence) else {
            return self
                .root_out_of_retention(binding.root_sequence)
                .map(TradesQueryResultV1::OutOfRetention);
        };
        if root.root_identity != binding.root_identity
            || root.method != binding.method
            || root.filter_sha256 != binding.filter_sha256
            || root.query_projection_revision != binding.query_projection_revision
            || root.retention_floor_at_issue != binding.retention_floor_at_issue
        {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        let QueryRootRowsV1::Trades(rows) = &root.rows else {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ));
        };
        let end = binding
            .next_index
            .saturating_add(SIM_QUERY_PAGE_SIZE_V1)
            .min(rows.len());
        let items = rows
            .get(binding.next_index..end)
            .ok_or(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ))?
            .to_vec();
        let next_cursor = (end < rows.len()).then(|| {
            self.issue_cursor(IssuedCursorV1 {
                next_index: end,
                ..binding.clone()
            })
        });
        Ok(TradesQueryResultV1::Page(TradesPageV1 {
            root_identity: binding.root_identity.0,
            query_projection_revision: binding.query_projection_revision,
            items,
            next_cursor,
        }))
    }

    fn continuation(
        &self,
        global_request_id: &str,
        method: ListQueryMethodV1,
    ) -> Result<IssuedCursorV1, SimExecutionContractErrorV1> {
        let binding = self
            .continuation_by_request
            .get(global_request_id)
            .ok_or_else(|| {
                SimExecutionContractErrorV1::QueryIdentityConflict(global_request_id.to_owned())
            })?;
        if binding.method != method {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        Ok(binding.clone())
    }

    fn root_out_of_retention(
        &self,
        requested_root_sequence: u64,
    ) -> Result<QueryOutOfRetentionV1, SimExecutionContractErrorV1> {
        if requested_root_sequence >= self.earliest_retained_root_sequence {
            return Err(SimExecutionContractErrorV1::QueryIdentityConflict(format!(
                "query-root:{requested_root_sequence}"
            )));
        }
        Ok(QueryOutOfRetentionV1 {
            scope: QueryRetentionScopeV1::RootSnapshot,
            retained_from_utc_ms: None,
            requested_root_sequence: Some(requested_root_sequence),
            earliest_retained_root_sequence: self.earliest_retained_root_sequence,
        })
    }

    fn next_root_identity(
        &mut self,
        spec: &SessionSpecV2,
        method: ListQueryMethodV1,
        filter_sha256: &Sha256Hex,
        query_projection_revision: u64,
        retention_floor_at_issue: u64,
        projection_sha256: &Sha256Hex,
    ) -> Result<(u64, Sha256Hex), SimExecutionContractErrorV1> {
        let root_sequence = self.next_root_sequence;
        self.next_root_sequence = self
            .next_root_sequence
            .checked_add(1)
            .ok_or(SimExecutionContractErrorV1::PlatformRevisionRegression)?;
        Ok((
            root_sequence,
            query_root_identity(
                spec,
                method,
                filter_sha256,
                query_projection_revision,
                retention_floor_at_issue,
                root_sequence,
                projection_sha256,
            ),
        ))
    }

    fn issue_cursor(&mut self, binding: IssuedCursorV1) -> OpaquePageCursorV1 {
        let token = cursor_token(&binding);
        self.issued_cursors.insert(token.clone(), binding);
        OpaquePageCursorV1(token)
    }

    fn advance_root_retention_floor(
        &mut self,
        new_floor: u64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        if new_floor < self.earliest_retained_root_sequence || new_floor > self.next_root_sequence {
            return Err(SimExecutionContractErrorV1::PlatformRevisionRegression);
        }
        self.earliest_retained_root_sequence = new_floor;
        Ok(())
    }

    fn prune_query_roots_before_retention_floor(&mut self) {
        let floor = self.earliest_retained_root_sequence;
        self.roots
            .retain(|root_sequence, _| *root_sequence >= floor);
        self.issued_cursors
            .retain(|_, binding| binding.root_sequence >= floor);
    }

    fn advance_order_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        if retained_from_utc_ms < 0
            || self
                .order_history_retained_from_utc_ms
                .is_some_and(|current| retained_from_utc_ms < current)
        {
            return Err(SimExecutionContractErrorV1::PlatformRevisionRegression);
        }
        self.order_history_retained_from_utc_ms = Some(retained_from_utc_ms);
        Ok(())
    }

    fn prune_order_history_before_retention_floor(&mut self) {
        let Some(floor) = self.order_history_retained_from_utc_ms else {
            return;
        };
        Arc::make_mut(&mut self.terminal_projections)
            .orders
            .retain(|_, order| order.last_updated_at_utc_ms >= floor);
    }

    fn advance_trade_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        if retained_from_utc_ms < 0
            || self
                .trade_history_retained_from_utc_ms
                .is_some_and(|current| retained_from_utc_ms < current)
        {
            return Err(SimExecutionContractErrorV1::PlatformRevisionRegression);
        }
        self.trade_history_retained_from_utc_ms = Some(retained_from_utc_ms);
        Ok(())
    }

    fn prune_trade_history_before_retention_floor(&mut self) {
        let Some(floor) = self.trade_history_retained_from_utc_ms else {
            return;
        };
        Arc::make_mut(&mut self.terminal_projections)
            .trades
            .retain(|_, trade| trade.matched_at_utc_ms >= floor);
    }
}

fn query_root_identity(
    spec: &SessionSpecV2,
    method: ListQueryMethodV1,
    filter_sha256: &Sha256Hex,
    query_projection_revision: u64,
    retention_floor_at_issue: u64,
    root_sequence: u64,
    projection_sha256: &Sha256Hex,
) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_QUERY_ROOT_V1");
    hash.text(SIM_EXECUTION_CONTRACT_V1);
    hash.text(&spec.session_id);
    hash.text(&spec.account_id);
    hash.text(method.identity());
    hash.text(&filter_sha256.0);
    hash.u64(query_projection_revision);
    hash.u64(retention_floor_at_issue);
    hash.u64(root_sequence);
    hash.text(SIM_QUERY_PAGE_POLICY_V1);
    hash.text(&projection_sha256.0);
    hash.finish()
}

fn cursor_token(binding: &IssuedCursorV1) -> String {
    let mut hash = CanonicalHasher::new("SIM_QUERY_CURSOR_V1");
    hash.text(&binding.root_identity.0);
    hash.text(binding.method.identity());
    hash.text(&binding.filter_sha256.0);
    hash.u64(binding.query_projection_revision);
    hash.u64(binding.retention_floor_at_issue);
    hash.u64(binding.root_sequence);
    hash.text(SIM_QUERY_PAGE_POLICY_V1);
    hash.u64(binding.next_index as u64);
    hash.finish().0
}

impl SimExecutionSessionV1 {
    #[must_use]
    pub fn session_spec(&self) -> &SessionSpecV2 {
        self.session.session_spec()
    }

    #[must_use]
    pub fn model_visible_cursor(&self) -> ModelVisibleCursorV1 {
        self.session.model_visible_cursor()
    }

    #[allow(dead_code)]
    pub(crate) fn apply_closed_market_event_bucket(
        &mut self,
        bucket: ClosedMarketEventBucketV1,
    ) -> Result<MarketBucketApplyDispositionV1, MarketEventErrorV1> {
        let physical_retry = self.session.last_applied_bucket_identity_matches(&bucket);
        let transient_input = if physical_retry {
            ResourceQuantityV1::default()
        } else {
            bucket.resource_input_quantity()?
        };
        if !physical_retry {
            self.reserve_transient_input(transient_input)
                .map_err(MarketEventErrorV1::from)?;
        }
        let result = self.apply_closed_market_event_bucket_inner(bucket, transient_input);
        if !physical_retry {
            let release = self
                .release_transient_input()
                .map_err(MarketEventErrorV1::from);
            if result.is_ok() {
                release?;
            } else {
                let _ = release;
            }
        }
        result
    }

    fn apply_closed_market_event_bucket_inner(
        &mut self,
        bucket: ClosedMarketEventBucketV1,
        transient_input: ResourceQuantityV1,
    ) -> Result<MarketBucketApplyDispositionV1, MarketEventErrorV1> {
        let expected_checkpoint = self.durable_checkpoint();
        let session_id = self.session.session_spec().session_id.clone();
        let bucket_reference = bucket.durable_reference();
        if !bucket.drives_execution_for(self.session.session_spec().caller_role) {
            let mut working_session = self.session.clone();
            let mut working_query_state = self.query_state.clone();
            let (disposition, feedbacks) =
                working_session.apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
            debug_assert!(feedbacks.trade_finality_effects.is_empty());
            debug_assert!(feedbacks.venue_dust_effects.is_empty());
            debug_assert!(feedbacks.market_resolution_order_effects.is_empty());
            debug_assert!(feedbacks.settlement_feedbacks.is_empty());
            prepare_pending_terminal_state(&mut working_session, &mut working_query_state)?;
            let mut candidate_state = self.runtime_state();
            candidate_state.session = working_session;
            candidate_state.query_state = working_query_state;
            let candidate_checkpoint =
                candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
            let mut evidence = DurableTransactionEvidenceV1::new(
                DurableTransactionFactV1::PlatformBucketApplied {
                    session_id: session_id.clone(),
                    bucket: bucket_reference.clone(),
                },
            );
            evidence.transient_input_session_id = Some(session_id.clone());
            evidence.transient_input = transient_input;
            self.commit_durable_transition(&expected_checkpoint, candidate_checkpoint, evidence)
                .map_err(MarketEventErrorV1::from)?;
            self.replace_runtime_state(candidate_state);
            return Ok(disposition);
        }

        if self.session.last_applied_bucket_coordinates_match(&bucket) {
            let mut working_session = self.session.clone();
            let (disposition, feedbacks) =
                working_session.apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
            debug_assert!(feedbacks.trade_finality_effects.is_empty());
            debug_assert!(feedbacks.venue_dust_effects.is_empty());
            debug_assert!(feedbacks.market_resolution_order_effects.is_empty());
            debug_assert!(feedbacks.settlement_feedbacks.is_empty());
            let mut candidate_state = self.runtime_state();
            candidate_state.session = working_session;
            let candidate_checkpoint =
                candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
            let mut evidence = DurableTransactionEvidenceV1::new(
                DurableTransactionFactV1::PlatformBucketApplied {
                    session_id: session_id.clone(),
                    bucket: bucket_reference.clone(),
                },
            );
            evidence.transient_input_session_id = Some(session_id.clone());
            evidence.transient_input = transient_input;
            self.commit_durable_transition(&expected_checkpoint, candidate_checkpoint, evidence)
                .map_err(MarketEventErrorV1::from)?;
            self.replace_runtime_state(candidate_state);
            return Ok(disposition);
        }

        let bucket_at_utc_ms = bucket.bucket_at_utc_ms();
        let bucket_work_identity = bucket.lifecycle_work_identity();
        let mut working_session = self.session.clone();
        let mut working_query_state = self.query_state.clone();
        let mut working_lifecycle_outbox = self.lifecycle_outbox.clone();
        let failed_input_seal = working_session.preview_failed_input_seal(&bucket)?;
        let query_projection_changed = if failed_input_seal.is_some() {
            false
        } else {
            drive_scheduled_platform_work_through(
                &mut working_session,
                &mut working_lifecycle_outbox,
                bucket_at_utc_ms,
                None,
            )
            .map_err(|error| match error {
                LogicalPlatformDriveErrorV1::Session(error) => MarketEventErrorV1::from(error),
                LogicalPlatformDriveErrorV1::Matching(error) => MarketEventErrorV1::from(error),
            })?
        };
        if query_projection_changed {
            working_session.advance_platform_state_revision_once()?;
        }
        let (disposition, lifecycle_effects) =
            working_session.apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
        if disposition == MarketBucketApplyDispositionV1::Applied && failed_input_seal.is_none() {
            working_session.commit_logical_platform_driven_through(bucket_at_utc_ms);
        }
        if !lifecycle_effects.trade_finality_effects.is_empty()
            || !lifecycle_effects.venue_dust_effects.is_empty()
            || !lifecycle_effects.market_resolution_order_effects.is_empty()
            || !lifecycle_effects.settlement_feedbacks.is_empty()
        {
            let reservation = working_lifecycle_outbox.reserve_platform_work(
                &format!("DPT:{bucket_work_identity}"),
                None,
                bucket_at_utc_ms,
            )?;
            record_initial_finality_lifecycle_feedbacks(
                &mut working_session,
                &mut working_lifecycle_outbox,
                Some(&reservation),
                lifecycle_effects.trade_finality_effects,
            )?;
            let dust_feedbacks = lifecycle_effects
                .venue_dust_effects
                .into_iter()
                .map(|effect| OrderLifecycleFeedbackSeedV1 {
                    platform_order_id: effect.platform_order_id,
                    condition_id: effect.condition_id,
                    token_id: effect.token_id,
                    event_kind: OrderLifecycleEventKindV1::VenueDustTerminal,
                    order_status: simulated_order_status("VENUE_DUST_TERMINAL"),
                    venue_dust_closed_base_units: effect.closed_base_units,
                    venue_dust_released_quote_units: effect.released_open_quote_units,
                    market_resolved_canceled_base_units: 0,
                    market_resolved_released_quote_units: 0,
                    account_delta: effect.account_delta,
                })
                .chain(
                    lifecycle_effects
                        .market_resolution_order_effects
                        .into_iter()
                        .map(|effect| OrderLifecycleFeedbackSeedV1 {
                            platform_order_id: effect.platform_order_id,
                            condition_id: effect.condition_id,
                            token_id: effect.token_id,
                            event_kind: OrderLifecycleEventKindV1::MarketResolvedCanceled,
                            order_status: simulated_order_status(
                                "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
                            ),
                            venue_dust_closed_base_units: 0,
                            venue_dust_released_quote_units: 0,
                            market_resolved_canceled_base_units: effect.canceled_base_units,
                            market_resolved_released_quote_units: effect.released_open_quote_units,
                            account_delta: effect.account_delta,
                        }),
                )
                .collect();
            record_order_lifecycle_feedbacks_in_batch(
                &mut working_session,
                &mut working_lifecycle_outbox,
                &reservation,
                dust_feedbacks,
            )?;
            let settlement_feedbacks = lifecycle_effects
                .settlement_feedbacks
                .into_iter()
                .map(settlement_lifecycle_feedback_seed)
                .collect::<Result<Vec<_>, _>>()?;
            record_settlement_lifecycle_feedbacks_in_batch(
                &mut working_session,
                &mut working_lifecycle_outbox,
                &reservation,
                settlement_feedbacks,
            )?;
            working_lifecycle_outbox.complete_platform_work(&reservation)?;
        }
        prepare_pending_terminal_state(&mut working_session, &mut working_query_state)?;
        let mut candidate_state = self.runtime_state();
        candidate_state.session = working_session;
        candidate_state.query_state = working_query_state;
        candidate_state.lifecycle_outbox = working_lifecycle_outbox;
        let candidate_checkpoint =
            candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
        let mut evidence =
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::PlatformBucketApplied {
                session_id: session_id.clone(),
                bucket: bucket_reference,
            });
        evidence.transient_input_session_id = Some(session_id);
        evidence.transient_input = transient_input;
        self.commit_durable_transition(&expected_checkpoint, candidate_checkpoint, evidence)
            .map_err(MarketEventErrorV1::from)?;
        self.replace_runtime_state(candidate_state);
        Ok(disposition)
    }

    #[allow(dead_code)]
    pub(crate) fn platform_market_state(&self, condition_id: &str) -> Option<&MarketStateV1> {
        Self::platform_market_state_for(&self.session, condition_id)
    }

    fn platform_market_state_for<'a>(
        session: &'a SimSession,
        condition_id: &str,
    ) -> Option<&'a MarketStateV1> {
        match session.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => session
                .hidden_platform_markets()
                .and_then(|markets| markets.market(condition_id)),
            SimCallerRoleV1::Paper => session.public_markets().market(condition_id),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn public_market_state(&self, condition_id: &str) -> Option<&MarketStateV1> {
        self.session.public_markets().market(condition_id)
    }

    #[allow(dead_code)]
    pub(crate) fn platform_derived_quantity(
        &self,
        condition_id: &str,
        token_id: &str,
        side: contracts::BookSide,
        price: Decimal,
    ) -> Option<Decimal> {
        let external = self
            .platform_market_state(condition_id)?
            .asset(token_id)?
            .external_quantity(side, price);
        external.checked_add(self.session.account_overlay().quantity(
            condition_id,
            token_id,
            side,
            price,
        ))
    }

    #[cfg(test)]
    pub(crate) fn take_next_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<QueuedLogicalActionV1>, SimExecutionContractErrorV1> {
        self.session
            .take_next_action_through(effective_processing_at_utc_ms)
    }

    #[cfg(test)]
    pub(crate) fn complete_logical_item(
        &mut self,
        action_identity: &str,
    ) -> Result<bool, SimExecutionContractErrorV1> {
        self.session.complete_logical_item(action_identity)
    }

    #[allow(dead_code)]
    pub(crate) fn prepare_submission(
        &self,
        action: &QueuedLogicalActionV1,
    ) -> Result<SubmissionAdmissionDecisionV1, SubmissionPreparationErrorV1> {
        self.prepare_submission_against(&self.session, action)
    }

    fn prepare_submission_against(
        &self,
        session: &SimSession,
        action: &QueuedLogicalActionV1,
    ) -> Result<SubmissionAdmissionDecisionV1, SubmissionPreparationErrorV1> {
        session.validate_in_flight_action(action)?;
        let item = self.post_item_for_action(action)?;
        let effective_at = action.key.effective_processing_at_utc_ms;
        let condition_id = item.order.condition_id.0.as_str();
        let token_id = item.order.token_id.0.as_str();
        if session.session_spec().caller_role == SimCallerRoleV1::Paper
            && session.public_markets().is_evicted(condition_id)
        {
            return Ok(rejected(SubmissionAdmissionRejectionV1::EndAtReached));
        }
        let Some(market) = Self::platform_market_state_for(session, condition_id) else {
            return Ok(rejected(
                SubmissionAdmissionRejectionV1::MarketNotInScenario,
            ));
        };
        if effective_at >= market.end_at_utc_ms() {
            return Ok(rejected(SubmissionAdmissionRejectionV1::EndAtReached));
        }
        if matches!(
            market.lifecycle_phase(),
            MarketLifecyclePhaseV1::PostEndPendingResolution
                | MarketLifecyclePhaseV1::ResolutionTerminal
        ) {
            return Ok(rejected(SubmissionAdmissionRejectionV1::EndAtReached));
        }
        let trading = market.trading_state();
        if trading.active == Some(false)
            || trading.closed == Some(true)
            || trading.accepting_orders == Some(false)
        {
            return Ok(rejected(
                SubmissionAdmissionRejectionV1::ExplicitTradingStateVeto,
            ));
        }
        let Some(asset) = market.asset(token_id) else {
            return Ok(rejected(SubmissionAdmissionRejectionV1::AssetNotInMarket));
        };
        if market.lifecycle_phase() == MarketLifecyclePhaseV1::Preloading {
            return Ok(SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(
                SubmissionEvidenceUnknownV1::Preloading,
            ));
        }
        if asset.quality() != MarketDataQuality::Fresh {
            return Ok(SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(
                SubmissionEvidenceUnknownV1::TargetAssetNotFresh(asset.quality()),
            ));
        }
        let tick = asset
            .current_tick_size_pusd_per_share()
            .ok_or_else(|| SubmissionPreparationErrorV1::MissingCurrentTick(token_id.to_owned()))?;
        let minimum_shares = session
            .rule_snapshot()
            .synchronized_rules()
            .minimum_order_size_shares
            .value();

        let order = match &item.order.kind {
            SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
            } => {
                let Some(encoding) =
                    limit_buy_encoding(*limit_price_pusd_per_share, *size_shares, tick)
                else {
                    return Ok(rejected(
                        SubmissionAdmissionRejectionV1::OrderEncodingRejected,
                    ));
                };
                if let Some(rejection) = Self::limit_buy_rejection(
                    session,
                    condition_id,
                    token_id,
                    *post_only,
                    &encoding,
                    minimum_shares,
                    session
                        .rule_snapshot()
                        .snapshot()
                        .documented_platform_rules
                        .immediate_gtc_buy_minimum
                        .value
                        .minimum_principal_pusd
                        .value(),
                ) {
                    return Ok(rejected(rejection));
                }
                PreparedBuyOrderV1::GtcLimit {
                    limit_price_pusd_per_share: encoding.price,
                    maker_amount_pusd: encoding.maker_amount,
                    taker_amount_shares: encoding.taker_amount,
                    post_only: *post_only,
                }
            }
            SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
                expires_at_utc_ms,
            } => {
                let Some(encoding) =
                    limit_buy_encoding(*limit_price_pusd_per_share, *size_shares, tick)
                else {
                    return Ok(rejected(
                        SubmissionAdmissionRejectionV1::OrderEncodingRejected,
                    ));
                };
                if let Some(rejection) = Self::limit_buy_rejection(
                    session,
                    condition_id,
                    token_id,
                    *post_only,
                    &encoding,
                    minimum_shares,
                    session
                        .rule_snapshot()
                        .snapshot()
                        .documented_platform_rules
                        .immediate_gtd_buy_minimum
                        .value
                        .minimum_principal_pusd
                        .value(),
                ) {
                    return Ok(rejected(rejection));
                }
                let Some(expires_at_utc_ms) =
                    self.normalized_gtd_expiration(session, *expires_at_utc_ms, effective_at)?
                else {
                    return Ok(rejected(
                        SubmissionAdmissionRejectionV1::GtdExpirationTooSoon,
                    ));
                };
                PreparedBuyOrderV1::GtdLimit {
                    limit_price_pusd_per_share: encoding.price,
                    maker_amount_pusd: encoding.maker_amount,
                    taker_amount_shares: encoding.taker_amount,
                    post_only: *post_only,
                    expires_at_utc_ms,
                }
            }
            SimBuyOrderKindV1::FokMarket {
                spend_pusd,
                worst_price_pusd_per_share,
            } => {
                let Some(encoding) =
                    market_buy_encoding(*worst_price_pusd_per_share, *spend_pusd, tick)
                else {
                    return Ok(rejected(
                        SubmissionAdmissionRejectionV1::OrderEncodingRejected,
                    ));
                };
                if encoding.taker_amount < minimum_shares {
                    return Ok(rejected(SubmissionAdmissionRejectionV1::BelowMinimumShares));
                }
                PreparedBuyOrderV1::FokMarket {
                    worst_price_pusd_per_share: encoding.price,
                    maker_amount_pusd: encoding.maker_amount,
                    taker_amount_shares: encoding.taker_amount,
                }
            }
            SimBuyOrderKindV1::FakMarket {
                spend_pusd,
                worst_price_pusd_per_share,
            } => {
                let Some(encoding) =
                    market_buy_encoding(*worst_price_pusd_per_share, *spend_pusd, tick)
                else {
                    return Ok(rejected(
                        SubmissionAdmissionRejectionV1::OrderEncodingRejected,
                    ));
                };
                if encoding.taker_amount < minimum_shares {
                    return Ok(rejected(SubmissionAdmissionRejectionV1::BelowMinimumShares));
                }
                PreparedBuyOrderV1::FakMarket {
                    worst_price_pusd_per_share: encoding.price,
                    maker_amount_pusd: encoding.maker_amount,
                    taker_amount_shares: encoding.taker_amount,
                }
            }
        };

        Ok(SubmissionAdmissionDecisionV1::Prepared(Box::new(
            PreparedSubmissionV1 {
                global_request_id: action.global_request_id.clone(),
                action_identity: action.action_identity.clone(),
                item_id: item.item_id.clone(),
                batch_item_ordinal: action.key.batch_item_ordinal,
                condition_id: item.order.condition_id.clone(),
                token_id: item.order.token_id.clone(),
                order,
                effective_processing_at_utc_ms: effective_at,
                authorized_platform_revision: Self::authorized_platform_revision_for(session),
            },
        )))
    }

    fn post_item_for_action(
        &self,
        action: &QueuedLogicalActionV1,
    ) -> Result<&PostOrderItemV1, SubmissionPreparationErrorV1> {
        let request = self
            .pending_requests
            .get(&action.global_request_id)
            .ok_or_else(|| {
                SubmissionPreparationErrorV1::RequestNotFound(action.global_request_id.clone())
            })?;
        let expected_identity = format!(
            "{}:{}:{}",
            request.method_tag(),
            action.global_request_id,
            action.key.batch_item_ordinal
        );
        if action.action_identity != expected_identity {
            return Err(SubmissionPreparationErrorV1::ActionMappingConflict(
                action.action_identity.clone(),
            ));
        }
        match request {
            PendingDomainRequestV1::PostOrder(request) => {
                if action.key.batch_item_ordinal != 0 {
                    return Err(SubmissionPreparationErrorV1::ActionMappingConflict(
                        action.action_identity.clone(),
                    ));
                }
                Ok(&request.item)
            }
            PendingDomainRequestV1::PostOrders(request) => request
                .items
                .get(usize::from(action.key.batch_item_ordinal))
                .ok_or_else(|| {
                    SubmissionPreparationErrorV1::ActionMappingConflict(
                        action.action_identity.clone(),
                    )
                }),
            _ => Err(SubmissionPreparationErrorV1::ActionIsNotSubmission(
                action.action_identity.clone(),
            )),
        }
    }

    fn limit_buy_rejection(
        session: &SimSession,
        condition_id: &str,
        token_id: &str,
        post_only: bool,
        encoding: &BuyOrderEncoding,
        minimum_shares: Decimal,
        minimum_principal_pusd: Decimal,
    ) -> Option<SubmissionAdmissionRejectionV1> {
        if encoding.taker_amount < minimum_shares {
            return Some(SubmissionAdmissionRejectionV1::BelowMinimumShares);
        }
        let marketable = Self::platform_best_ask_price_for(session, condition_id, token_id)
            .is_some_and(|best_ask| best_ask <= encoding.price);
        if post_only && marketable {
            return Some(SubmissionAdmissionRejectionV1::PostOnlyWouldTake);
        }
        if marketable && encoding.maker_amount < minimum_principal_pusd {
            return Some(SubmissionAdmissionRejectionV1::BelowMarketableLimitBuyMinimumPrincipal);
        }
        None
    }

    fn platform_best_ask_price_for(
        session: &SimSession,
        condition_id: &str,
        token_id: &str,
    ) -> Option<Decimal> {
        session
            .execution_state()
            .best_executable_ask(condition_id, token_id)
    }

    fn normalized_gtd_expiration(
        &self,
        session: &SimSession,
        expires_at_utc_ms: i64,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<i64>, SubmissionPreparationErrorV1> {
        let encoded_expires_sec = expires_at_utc_ms.div_euclid(1_000);
        let processing_sec = effective_processing_at_utc_ms
            .div_euclid(1_000)
            .checked_add(i64::from(
                effective_processing_at_utc_ms.rem_euclid(1_000) != 0,
            ))
            .ok_or(SubmissionPreparationErrorV1::TimeOverflow)?;
        let timing = &session
            .rule_snapshot()
            .snapshot()
            .documented_platform_rules
            .gtd_timing
            .value;
        let lead = timing
            .minimum_lead_seconds
            .checked_add(timing.safety_offset_seconds)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(SubmissionPreparationErrorV1::TimeOverflow)?;
        let minimum_expires_sec = processing_sec
            .checked_add(lead)
            .ok_or(SubmissionPreparationErrorV1::TimeOverflow)?;
        if encoded_expires_sec < minimum_expires_sec {
            return Ok(None);
        }
        encoded_expires_sec
            .checked_mul(1_000)
            .map(Some)
            .ok_or(SubmissionPreparationErrorV1::TimeOverflow)
    }

    fn authorized_platform_revision_for(session: &SimSession) -> AuthorizedPlatformRevisionV1 {
        match session.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                AuthorizedPlatformRevisionV1::EventTime(session.platform_prefix_cursor().clone())
            }
            SimCallerRoleV1::Paper => AuthorizedPlatformRevisionV1::ReceivedTime(
                session.public_visibility_cursor().clone(),
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn platform_event_index_position(&self) -> u64 {
        self.session
            .platform_prefix_cursor()
            .next_event_time_index_position()
    }

    #[cfg(test)]
    pub(crate) fn public_event_index_position(&self) -> u64 {
        self.session
            .public_visibility_cursor()
            .next_visibility_index_position()
    }

    #[cfg(test)]
    pub(crate) fn set_account_overlay_quantity(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: contracts::BookSide,
        price: Decimal,
        quantity: Decimal,
    ) {
        self.session.account_overlay_mut().set_quantity(
            condition_id,
            token_id,
            side,
            price,
            quantity,
        );
    }

    #[cfg(test)]
    pub(crate) fn account_overlay_quantity(
        &self,
        condition_id: &str,
        token_id: &str,
        side: contracts::BookSide,
        price: Decimal,
    ) -> Decimal {
        self.session
            .account_overlay()
            .quantity(condition_id, token_id, side, price)
    }

    #[cfg(test)]
    pub(crate) fn available_pusd(&self) -> Decimal {
        self.session
            .execution_state()
            .available_pusd()
            .expect("test execution balance must fit Decimal")
    }

    #[cfg(test)]
    pub(crate) fn principal_encumbered_pusd(&self) -> Decimal {
        self.session
            .execution_state()
            .principal_encumbered_pusd()
            .expect("test principal balance must fit Decimal")
    }

    #[cfg(test)]
    pub(crate) fn immediate_taker_fee_encumbered_pusd(&self) -> Decimal {
        self.session
            .execution_state()
            .immediate_taker_fee_encumbered_pusd()
            .expect("test fee balance must fit Decimal")
    }

    #[cfg(test)]
    pub(crate) fn account_summary_sha256(&self) -> &Sha256Hex {
        self.session
            .execution_state()
            .account()
            .account_summary_sha256()
    }

    #[cfg(test)]
    pub(crate) fn outcome_balances_sha256(&self) -> &Sha256Hex {
        self.session
            .execution_state()
            .account()
            .outcome_balances_sha256()
    }

    #[cfg(test)]
    pub(crate) fn next_account_transition_seq(&self) -> u64 {
        self.session
            .execution_state()
            .account()
            .next_account_transition_seq()
    }

    #[cfg(test)]
    pub(crate) fn account_snapshot_for_tests(&self) -> contracts::PlatformAccountSnapshotV1 {
        self.session.execution_state().account().snapshot()
    }

    #[cfg(test)]
    pub(crate) fn logical_platform_driven_through_for_tests(&self) -> Option<i64> {
        self.session.logical_platform_driven_through()
    }

    #[cfg(test)]
    pub(crate) fn task41_diagnostics_for_tests(&self, condition_id: &str) -> Task41DiagnosticsV1 {
        let market_external_book_level_count = self
            .platform_market_state(condition_id)
            .map_or(0, MarketStateV1::external_price_level_count);
        let (
            execution_queue_level_count,
            execution_queue_segment_count,
            execution_queue_debt_base_units,
        ) = self
            .session
            .execution_state()
            .market_matching_queue_footprint(condition_id)
            .unwrap_or_default();
        let authoritative_path = match self.session.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                crate::market::MarketEventPathV1::HiddenPlatform
            }
            SimCallerRoleV1::Paper => crate::market::MarketEventPathV1::PublicVisibility,
        };
        let input_seal = self
            .session
            .failed_input_seal()
            .map(|(path, outcome)| (path, outcome.clone()))
            .or_else(|| {
                self.session
                    .input_path_seal(authoritative_path)
                    .map(|outcome| (authoritative_path, outcome.clone()))
            });
        let hot_orders = self
            .session
            .execution_state()
            .orders()
            .filter(|order| order.condition_id == condition_id)
            .map(|order| project_order(order).expect("测试热订单必须可以形成查询投影"))
            .collect();
        let compact_orders = self
            .query_state
            .terminal_projections
            .orders
            .values()
            .filter(|order| order.condition_id.0 == condition_id)
            .cloned()
            .collect();
        let hot_trades = self
            .session
            .execution_state()
            .fills()
            .filter(|fill| fill.condition_id == condition_id)
            .map(|fill| project_trade(fill).expect("测试热成交必须可以形成查询投影"))
            .collect();
        let compact_trades = self
            .query_state
            .terminal_projections
            .trades
            .values()
            .filter(|trade| trade.condition_id.0 == condition_id)
            .cloned()
            .collect();
        Task41DiagnosticsV1 {
            market_external_book_level_count,
            execution_queue_level_count,
            execution_queue_segment_count,
            execution_queue_debt_base_units,
            input_seal,
            hot_orders,
            compact_orders,
            hot_trades,
            compact_trades,
            retained_rebate_fill_state_count: self
                .session
                .execution_state()
                .retained_rebate_fill_state_count(),
            query_root_body_count: self.query_state.roots.len(),
            issued_cursor_count: self.query_state.issued_cursors.len(),
            earliest_retained_root_sequence: self.query_state.earliest_retained_root_sequence,
            pending_direct_response_count: self.direct_response_outbox.entries.len(),
            pending_lifecycle_feedback_count: self.lifecycle_outbox.entries.len(),
            compacted_confirmed_private_delivery_count: self
                .compacted_confirmed_private_deliveries
                .len(),
            full_confirmed_private_delivery_count: self.confirmed_private_deliveries.len(),
            confirmed_direct_response_confirmations: self
                .compacted_confirmed_private_deliveries
                .values()
                .filter(|delivery| delivery.kind == CompactedPrivateDeliveryKindV1::Direct)
                .map(|delivery| delivery.confirmation.clone())
                .chain(
                    self.confirmed_private_deliveries.values().filter_map(
                        |delivery| match delivery {
                            SealedPrivateDeliveryV1::Direct(response) => {
                                Some(response.confirmation())
                            }
                            SealedPrivateDeliveryV1::Lifecycle(_) => None,
                        },
                    ),
                )
                .collect(),
            confirmed_lifecycle_feedback_confirmations: self
                .compacted_confirmed_private_deliveries
                .values()
                .filter(|delivery| delivery.kind == CompactedPrivateDeliveryKindV1::Lifecycle)
                .map(|delivery| delivery.confirmation.clone())
                .chain(
                    self.confirmed_private_deliveries.values().filter_map(
                        |delivery| match delivery {
                            SealedPrivateDeliveryV1::Lifecycle(feedback) => {
                                Some(feedback.confirmation())
                            }
                            SealedPrivateDeliveryV1::Direct(_) => None,
                        },
                    ),
                )
                .collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn cloned_session_shares_market_enrollment_storage(&self) -> bool {
        let cloned = self.session.clone();
        self.session.shares_market_enrollment_storage_with(&cloned)
    }

    #[cfg(test)]
    pub(crate) fn cloned_execution_state_shares_rebate_storage(&self) -> bool {
        self.session
            .execution_state()
            .cloned_state_shares_rebate_storage()
    }

    #[cfg(test)]
    pub(crate) fn cloned_execution_state_shares_market_resolution_storage(&self) -> bool {
        self.session
            .execution_state()
            .cloned_state_shares_market_resolution_storage()
    }

    #[cfg(test)]
    pub(crate) fn market_enrollment_storage_identity(&self) -> usize {
        self.session.market_enrollment_storage_identity()
    }

    #[cfg(test)]
    pub(crate) fn simulated_order(&self, platform_order_id: &str) -> Option<&SimOrderRecordV1> {
        self.session.execution_state().order(platform_order_id)
    }

    #[cfg(test)]
    pub(crate) fn simulated_order_count(&self) -> usize {
        self.session.execution_state().order_count()
    }

    #[cfg(test)]
    pub(crate) fn simulated_fill_count(&self, platform_order_id: &str) -> usize {
        self.session
            .execution_state()
            .fills_for_order(platform_order_id)
            .count()
    }

    #[cfg(test)]
    pub(crate) fn simulated_fill<'a>(
        &'a self,
        platform_order_id: &'a str,
    ) -> Option<&'a crate::matching::SimFillRecordV1> {
        self.session
            .execution_state()
            .fills_for_order(platform_order_id)
            .next()
    }

    #[cfg(test)]
    pub(crate) fn query_projection_revision(&self) -> u64 {
        self.session.platform_state_revision()
    }

    #[cfg(test)]
    pub(crate) fn exhaust_query_projection_revision_for_test(&mut self) {
        self.session
            .advance_platform_state_revision(u64::MAX)
            .expect("测试修订号只能单调推进到上界");
    }

    pub(crate) fn advance_query_root_retention_floor(
        &mut self,
        new_floor: u64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let expected = self.durable_checkpoint();
        let mut candidate_state = self.runtime_state();
        candidate_state
            .query_state
            .advance_root_retention_floor(new_floor)?;
        candidate_state
            .query_state
            .prune_query_roots_before_retention_floor();
        let candidate = candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
        self.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RetentionPrepared {
                session_id: self.session.session_spec().session_id.clone(),
                scope: QueryRetentionScopeV1::RootSnapshot,
                root_sequence_floor: Some(new_floor),
                retained_from_utc_ms: None,
            }),
        )?;
        self.replace_runtime_state(candidate_state);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn next_query_root_sequence(&self) -> u64 {
        self.query_state.next_root_sequence
    }

    #[cfg(test)]
    pub(crate) fn prepare_private_delivery_through_for_tests(&mut self, received_at_utc_ms: i64) {
        match self.session.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                if self
                    .session
                    .platform_prefix_cursor()
                    .closed_through_source_event_at_utc_ms()
                    .is_none_or(|closed| closed < received_at_utc_ms)
                {
                    let position = self
                        .session
                        .platform_prefix_cursor()
                        .next_event_time_index_position();
                    self.session
                        .close_platform_prefix_through(received_at_utc_ms, position)
                        .expect("测试必须先闭合模型训练平台输入前缀");
                }
            }
            SimCallerRoleV1::Paper => {
                if self
                    .session
                    .public_visibility_cursor()
                    .closed_through_received_at_utc_ms()
                    .is_none_or(|closed| closed < received_at_utc_ms)
                {
                    let position = self
                        .session
                        .public_visibility_cursor()
                        .next_visibility_index_position();
                    self.session
                        .advance_public_visibility_through(received_at_utc_ms, position)
                        .expect("测试必须先闭合模拟盘公开输入前缀");
                }
            }
        }
        if self
            .session
            .logical_platform_driven_through()
            .is_none_or(|closed| closed <= received_at_utc_ms)
        {
            assert_eq!(
                self.drive_next_execution_action_through(received_at_utc_ms),
                Ok(SubmissionDriveDispositionV1::NoAction),
                "测试交付私有事实前必须显式驱动统一逻辑平台水位"
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn drive_private_delivery_after_platform_through_for_tests(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<SubmissionDeliveryDispositionV1, SimExecutionContractErrorV1> {
        let mut delivered_any = false;
        loop {
            let next_private_at = self
                .session
                .next_private_fact_received_at_through(received_at_utc_ms);
            let platform_through = next_private_at.unwrap_or(received_at_utc_ms);
            self.prepare_private_delivery_through_for_tests(platform_through);
            match self.drive_private_delivery_through(platform_through)? {
                SubmissionDeliveryDispositionV1::Delivered => delivered_any = true,
                SubmissionDeliveryDispositionV1::NoVisibleResponse if next_private_at.is_none() => {
                    return Ok(if delivered_any {
                        SubmissionDeliveryDispositionV1::Delivered
                    } else {
                        SubmissionDeliveryDispositionV1::NoVisibleResponse
                    });
                }
                disposition @ (SubmissionDeliveryDispositionV1::Full
                | SubmissionDeliveryDispositionV1::Closed) => return Ok(disposition),
                SubmissionDeliveryDispositionV1::NoVisibleResponse => {
                    return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
                }
            }
        }
    }

    pub(crate) fn advance_order_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let expected = self.durable_checkpoint();
        let mut candidate_state = self.runtime_state();
        candidate_state
            .query_state
            .advance_order_history_retention_floor(retained_from_utc_ms)?;
        candidate_state
            .query_state
            .prune_order_history_before_retention_floor();
        let candidate = candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
        self.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RetentionPrepared {
                session_id: self.session.session_spec().session_id.clone(),
                scope: QueryRetentionScopeV1::OrderHistory,
                root_sequence_floor: None,
                retained_from_utc_ms: Some(retained_from_utc_ms),
            }),
        )?;
        self.replace_runtime_state(candidate_state);
        Ok(())
    }

    pub(crate) fn advance_trade_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let expected = self.durable_checkpoint();
        let mut candidate_state = self.runtime_state();
        candidate_state
            .query_state
            .advance_trade_history_retention_floor(retained_from_utc_ms)?;
        candidate_state
            .query_state
            .prune_trade_history_before_retention_floor();
        let candidate = candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
        self.commit_durable_transition(
            &expected,
            candidate,
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RetentionPrepared {
                session_id: self.session.session_spec().session_id.clone(),
                scope: QueryRetentionScopeV1::TradeHistory,
                root_sequence_floor: None,
                retained_from_utc_ms: Some(retained_from_utc_ms),
            }),
        )?;
        self.replace_runtime_state(candidate_state);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn next_order_deadline_at_utc_ms(&self) -> Option<i64> {
        self.session.execution_state().next_gtd_deadline_at_utc_ms()
    }

    #[cfg(test)]
    pub(crate) fn next_rebate_clock_at_utc_ms_for_tests(&self) -> Option<i64> {
        self.session.execution_state().next_rebate_clock_at_utc_ms()
    }

    pub(crate) fn drive_next_execution_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<SubmissionDriveDispositionV1, SubmissionProcessingErrorV1> {
        let before_state = self.runtime_state();
        let expected_checkpoint = self.durable_checkpoint();
        let mut result =
            self.drive_next_execution_action_through_working(effective_processing_at_utc_ms);
        if matches!(
            result,
            Err(SubmissionProcessingErrorV1::Preparation(_)
                | SubmissionProcessingErrorV1::Matching(_)
                | SubmissionProcessingErrorV1::Math(_)
                | SubmissionProcessingErrorV1::InternalInvariant(_))
        ) || matches!(
            &result,
            Err(SubmissionProcessingErrorV1::Session(error))
                if fatal_session_processing_error(error)
        ) {
            self.accepting_new_ingress = false;
        }
        let candidate_checkpoint = self.durable_checkpoint();
        let evidence_fact = if before_state.accepting_new_ingress && !self.accepting_new_ingress {
            DurableTransactionFactV1::IngressClosed {
                session_id: self.session.session_spec().session_id.clone(),
            }
        } else {
            DurableTransactionFactV1::ExecutionAdvanced {
                session_id: self.session.session_spec().session_id.clone(),
            }
        };
        if let Err(error) = self.commit_durable_transition(
            &expected_checkpoint,
            candidate_checkpoint,
            DurableTransactionEvidenceV1::new(evidence_fact),
        ) {
            self.replace_runtime_state(before_state);
            result = Err(SubmissionProcessingErrorV1::Session(error));
        }
        result
    }

    fn drive_next_execution_action_through_working(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<SubmissionDriveDispositionV1, SubmissionProcessingErrorV1> {
        self.session.require_input_not_failed()?;
        let mut working_session = self.session.clone();
        let mut working_query_state = self.query_state.clone();
        let mut working_lifecycle_outbox = self.lifecycle_outbox.clone();
        let original_execution = working_session.execution_state().clone();
        let Some(mut action) =
            working_session.take_next_action_through(effective_processing_at_utc_ms)?
        else {
            let query_projection_changed = drive_scheduled_platform_work_through(
                &mut working_session,
                &mut working_lifecycle_outbox,
                effective_processing_at_utc_ms,
                None,
            )?;
            if query_projection_changed {
                working_session.advance_platform_state_revision_once()?;
            }
            prepare_pending_terminal_state(&mut working_session, &mut working_query_state)?;
            self.session = working_session;
            self.query_state = working_query_state;
            self.lifecycle_outbox = working_lifecycle_outbox;
            return Ok(SubmissionDriveDispositionV1::NoAction);
        };
        let request = self
            .pending_requests
            .get(&action.global_request_id)
            .cloned()
            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                "逻辑动作对应的领域请求丢失",
            ))?;
        let lifecycle_request_reservation = mutation_binding(&request)
            .is_some()
            .then(|| {
                working_lifecycle_outbox.reserve_request(
                    &action.global_request_id,
                    action.key.effective_processing_at_utc_ms,
                )
            })
            .transpose()?;
        drive_scheduled_platform_work_through(
            &mut working_session,
            &mut working_lifecycle_outbox,
            action.key.effective_processing_at_utc_ms,
            Some(&action.global_request_id),
        )?;
        prepare_pending_terminal_state(&mut working_session, &mut working_query_state)?;
        let query_action = matches!(
            request,
            PendingDomainRequestV1::Order(_)
                | PendingDomainRequestV1::Orders(_)
                | PendingDomainRequestV1::Trades(_)
        );
        if query_action
            && working_session
                .execution_state()
                .query_projection_changed_from(&original_execution)
        {
            working_session.advance_platform_state_revision_once()?;
        }

        let mut working_submission_effect_records = self.submission_effect_records.clone();
        let mut working_cancel_effect_records = self.cancel_effect_records.clone();
        let mut working_outbox = self.direct_response_outbox.clone();
        let drain_cancel_batch = matches!(request, PendingDomainRequestV1::CancelOrders(_));
        let request_id = action.global_request_id.clone();
        let action_at = action.key.effective_processing_at_utc_ms;
        let caller_sequence = action.key.caller_sequence;
        let mut request_completed;
        loop {
            let (part, order_feedbacks, trade_feedbacks) = match &request {
                PendingDomainRequestV1::PostOrder(_) | PendingDomainRequestV1::PostOrders(_) => {
                    self.process_post_action(
                        &mut working_session,
                        &action,
                        &request,
                        &mut working_submission_effect_records,
                    )?
                }
                PendingDomainRequestV1::CancelOrder(_)
                | PendingDomainRequestV1::CancelOrders(_)
                | PendingDomainRequestV1::CancelAllOrders(_) => {
                    let (part, feedbacks) = self.process_cancel_action(
                        &mut working_session,
                        &action,
                        &request,
                        &mut working_cancel_effect_records,
                    )?;
                    (part, feedbacks, Vec::new())
                }
                PendingDomainRequestV1::Order(_)
                | PendingDomainRequestV1::Orders(_)
                | PendingDomainRequestV1::Trades(_) => {
                    let part = self.process_query_action(
                        &mut working_session,
                        &mut working_query_state,
                        &action,
                        &request,
                    )?;
                    (part, Vec::new(), Vec::new())
                }
            };
            record_order_lifecycle_feedbacks(
                &mut working_session,
                &mut working_lifecycle_outbox,
                lifecycle_request_reservation.as_ref(),
                order_feedbacks,
            )?;
            record_initial_finality_lifecycle_feedbacks(
                &mut working_session,
                &mut working_lifecycle_outbox,
                lifecycle_request_reservation.as_ref(),
                trade_feedbacks,
            )?;
            working_outbox.record_part(&request_id, part)?;
            request_completed = working_session.complete_logical_item(&action.action_identity)?;
            if !drain_cancel_batch || request_completed {
                break;
            }
            let expected_ordinal = action.key.batch_item_ordinal.checked_add(1).ok_or(
                SubmissionProcessingErrorV1::InternalInvariant("批撤动作序号溢出"),
            )?;
            let next_action = working_session.take_next_action_through(action_at)?.ok_or(
                SubmissionProcessingErrorV1::InternalInvariant("批撤请求尚未完成但后续动作缺失"),
            )?;
            if next_action.global_request_id != request_id
                || next_action.key.effective_processing_at_utc_ms != action_at
                || next_action.key.caller_sequence != caller_sequence
                || next_action.key.batch_item_ordinal != expected_ordinal
            {
                return Err(SubmissionProcessingErrorV1::InternalInvariant(
                    "批撤动作不是同一请求的连续唯一目标",
                ));
            }
            action = next_action;
        }
        if !query_action
            && working_session
                .execution_state()
                .query_projection_changed_from(&original_execution)
        {
            working_session.advance_platform_state_revision_once()?;
        }
        if request_completed {
            if let Some(reservation) = &lifecycle_request_reservation {
                working_lifecycle_outbox.complete_request(&request_id, reservation)?;
            }
            let platform_responded_at_utc_ms =
                working_outbox.platform_responded_at(&request_id).ok_or(
                    SubmissionProcessingErrorV1::InternalInvariant("直接响应缺少平台响应时点"),
                )?;
            let private_emission_sequence = working_session.next_private_emission_sequence()?;
            let transit = working_session.complete_direct_response(
                &request_id,
                platform_responded_at_utc_ms,
                private_emission_sequence,
            )?;
            let context = self
                .pending_requests
                .get(&request_id)
                .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                    "领域请求在响应密封前丢失",
                ))?
                .context()
                .clone();
            working_outbox.complete_draft(
                &request_id,
                context,
                &transit,
                working_session.rule_snapshot().file_sha256().clone(),
                working_session.scenario().scenario_sha256().clone(),
                fee_rule_identity(&working_session),
            )?;
        }
        self.session = working_session;
        self.submission_effect_records = working_submission_effect_records;
        self.cancel_effect_records = working_cancel_effect_records;
        self.query_state = working_query_state;
        self.direct_response_outbox = working_outbox;
        self.lifecycle_outbox = working_lifecycle_outbox;
        Ok(SubmissionDriveDispositionV1::ItemProcessed { request_completed })
    }

    fn process_post_action(
        &self,
        working_session: &mut SimSession,
        action: &QueuedLogicalActionV1,
        request: &PendingDomainRequestV1,
        working_effect_records: &mut BTreeMap<String, SubmissionEffectRecordV1>,
    ) -> Result<
        (
            DirectResponsePartV1,
            Vec<OrderLifecycleFeedbackSeedV1>,
            Vec<TradeFinalityLifecycleEffectsV1>,
        ),
        SubmissionProcessingErrorV1,
    > {
        let item = self.post_item_for_action(action)?.clone();
        let (idempotency_key, payload_sha256) = mutation_binding(request).ok_or(
            SubmissionProcessingErrorV1::InternalInvariant("提交动作缺少变更幂等绑定"),
        )?;
        let item_idempotency_identity = submission_item_idempotency_identity(
            working_session.session_spec(),
            &idempotency_key,
            action.key.batch_item_ordinal,
        );
        let effect_identity =
            submission_item_effect_identity(&item_idempotency_identity, &payload_sha256);
        let resolution = working_session.resolve_mutation_idempotency(
            &action.global_request_id,
            &action.action_identity,
            item_idempotency_identity.clone(),
            payload_sha256,
            effect_identity,
        )?;
        let (
            effect_record,
            effect_disposition,
            prepared_execution,
            order_feedbacks,
            trade_feedbacks,
        ) = match resolution.disposition {
            MutationResolutionDispositionV1::ExistingEffect => (
                working_effect_records
                    .get(&resolution.effect_identity)
                    .cloned()
                    .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                        "既有提交幂等效果缺少终局结果",
                    ))?,
                SubmissionEffectDispositionV1::ExistingEffect,
                None,
                Vec::new(),
                Vec::new(),
            ),
            MutationResolutionDispositionV1::NewEffect => {
                let decision = self.prepare_submission_against(working_session, action)?;
                let (outcome, prepared_execution, order_feedbacks, trade_feedbacks) = match decision
                {
                    SubmissionAdmissionDecisionV1::Prepared(prepared) => self
                        .process_prepared_submission(
                            working_session,
                            &prepared,
                            &resolution.effect_identity,
                            &item_idempotency_identity,
                        )?,
                    SubmissionAdmissionDecisionV1::RejectedNoEffect(reason) => (
                        SubmissionItemOutcomeV1::RejectedNoEffect(map_submission_rejection(reason)),
                        None,
                        Vec::new(),
                        Vec::new(),
                    ),
                    SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(reason) => (
                        SubmissionItemOutcomeV1::EvidenceUnknownNoEffect(map_submission_evidence(
                            reason,
                        )),
                        None,
                        Vec::new(),
                        Vec::new(),
                    ),
                };
                let effect_record = SubmissionEffectRecordV1 {
                    outcome,
                    formed_at_utc_ms: action.key.effective_processing_at_utc_ms,
                };
                if working_effect_records
                    .insert(resolution.effect_identity.clone(), effect_record.clone())
                    .is_some()
                {
                    return Err(SubmissionProcessingErrorV1::InternalInvariant(
                        "新提交幂等效果已存在终局结果",
                    ));
                }
                (
                    effect_record,
                    SubmissionEffectDispositionV1::NewEffect,
                    prepared_execution,
                    order_feedbacks,
                    trade_feedbacks,
                )
            }
        };
        if let Some(execution) = prepared_execution {
            working_session.replace_execution_state(execution);
        }
        Ok((
            DirectResponsePartV1::Submission(SubmissionResponseItemV1 {
                item_id: item.item_id,
                batch_item_ordinal: action.key.batch_item_ordinal,
                condition_id: item.order.condition_id,
                token_id: item.order.token_id,
                effective_processing_at_utc_ms: action.key.effective_processing_at_utc_ms,
                submission_effect_identity: resolution.effect_identity,
                submission_effect_formed_at_utc_ms: effect_record.formed_at_utc_ms,
                submission_effect_disposition: effect_disposition,
                outcome: effect_record.outcome,
            }),
            order_feedbacks,
            trade_feedbacks,
        ))
    }

    fn process_cancel_action(
        &self,
        working_session: &mut SimSession,
        action: &QueuedLogicalActionV1,
        request: &PendingDomainRequestV1,
        working_effect_records: &mut BTreeMap<String, CancelEffectRecordV1>,
    ) -> Result<
        (DirectResponsePartV1, Vec<OrderLifecycleFeedbackSeedV1>),
        SubmissionProcessingErrorV1,
    > {
        validate_action_mapping(request, action)?;
        let (idempotency_key, payload_sha256) = mutation_binding(request).ok_or(
            SubmissionProcessingErrorV1::InternalInvariant("撤单动作缺少变更幂等绑定"),
        )?;
        let request_idempotency_identity =
            cancel_request_idempotency_identity(working_session.session_spec(), &idempotency_key);
        let root_effect_identity =
            cancel_item_effect_identity(&request_idempotency_identity, &payload_sha256);
        let resolution = working_session.resolve_mutation_idempotency(
            &action.global_request_id,
            &action.action_identity,
            request_idempotency_identity,
            payload_sha256,
            root_effect_identity,
        )?;
        let formed_at = action.key.effective_processing_at_utc_ms;

        match request {
            PendingDomainRequestV1::CancelOrder(_) | PendingDomainRequestV1::CancelOrders(_) => {
                let groups = cancel_target_groups(request)?;
                let group = cancel_target_group_for_action(request, action)?;
                let (record, feedbacks) = match resolution.disposition {
                    MutationResolutionDispositionV1::ExistingEffect => (
                        working_effect_records
                            .get(&resolution.effect_identity)
                            .cloned()
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                                "既有撤单幂等效果缺少终局结果",
                            ))?,
                        Vec::new(),
                    ),
                    MutationResolutionDispositionV1::NewEffect => {
                        let mut execution = working_session.execution_state().clone();
                        let mut targets = Vec::with_capacity(groups.len());
                        let mut feedbacks = Vec::new();
                        for target_group in &groups {
                            let target_effect_identity = cancel_target_effect_identity(
                                &resolution.effect_identity,
                                &target_group.platform_order_id,
                                u64::from(target_group.unique_target_ordinal),
                            );
                            let order = execution.order(&target_group.platform_order_id).cloned();
                            let hot_order_visible = order
                                .as_ref()
                                .map(|order| self.query_state.hot_order_is_query_visible(order))
                                .transpose()?
                                .unwrap_or(false);
                            let disposition = if hot_order_visible {
                                execution.cancel_order_remainder(
                                    &target_group.platform_order_id,
                                    formed_at,
                                    &Sha256Hex(target_effect_identity.clone()),
                                )?
                            } else if self
                                .query_state
                                .retained_terminal_order(&target_group.platform_order_id)
                                .is_some()
                            {
                                CancelOrderStateDispositionV1::NoCancellableRemainder
                            } else {
                                CancelOrderStateDispositionV1::NotFound
                            };
                            let (outcome, feedback) =
                                cancel_target_outcome(disposition, order.as_ref())?;
                            feedbacks.extend(feedback);
                            targets.push(CancelTargetEffectV1 {
                                platform_order_id: target_group.platform_order_id.clone(),
                                effect_identity: target_effect_identity,
                                outcome,
                            });
                        }
                        working_session.replace_execution_state(execution);
                        let record = CancelEffectRecordV1 {
                            originating_global_request_id: action.global_request_id.clone(),
                            formed_at_utc_ms: formed_at,
                            targets,
                        };
                        if working_effect_records
                            .insert(resolution.effect_identity.clone(), record.clone())
                            .is_some()
                        {
                            return Err(SubmissionProcessingErrorV1::InternalInvariant(
                                "新撤单幂等效果已存在终局结果",
                            ));
                        }
                        (record, feedbacks)
                    }
                };
                if record.targets.len() != groups.len() {
                    return Err(SubmissionProcessingErrorV1::InternalInvariant(
                        "撤单幂等效果目标数量冲突",
                    ));
                }
                let target = record
                    .targets
                    .get(usize::from(group.unique_target_ordinal))
                    .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                        "撤单幂等效果缺少目标结果",
                    ))?;
                if target.platform_order_id != group.platform_order_id {
                    return Err(SubmissionProcessingErrorV1::InternalInvariant(
                        "撤单幂等效果目标冲突",
                    ));
                }
                let effect_disposition =
                    if record.originating_global_request_id == action.global_request_id {
                        CancelEffectDispositionV1::NewEffect
                    } else {
                        CancelEffectDispositionV1::ExistingEffect
                    };
                Ok((
                    DirectResponsePartV1::CancelTarget(CancelTargetResponseV1 {
                        platform_order_id: target.platform_order_id.clone(),
                        target_ordinal: u64::from(group.unique_target_ordinal),
                        input_item_refs: group.input_item_refs,
                        effective_processing_at_utc_ms: formed_at,
                        cancel_effect_identity: target.effect_identity.clone(),
                        cancel_effect_formed_at_utc_ms: record.formed_at_utc_ms,
                        cancel_effect_disposition: effect_disposition,
                        outcome: target.outcome.clone(),
                    }),
                    feedbacks,
                ))
            }
            PendingDomainRequestV1::CancelAllOrders(_) => {
                let (record, feedbacks) = match resolution.disposition {
                    MutationResolutionDispositionV1::ExistingEffect => (
                        working_effect_records
                            .get(&resolution.effect_identity)
                            .cloned()
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                                "既有全撤幂等效果缺少终局结果",
                            ))?,
                        Vec::new(),
                    ),
                    MutationResolutionDispositionV1::NewEffect => {
                        let mut execution = working_session.execution_state().clone();
                        let target_ids = execution.active_order_ids()?;
                        let mut targets = Vec::with_capacity(target_ids.len());
                        let mut feedbacks = Vec::new();
                        for (ordinal, platform_order_id) in target_ids.into_iter().enumerate() {
                            let ordinal = u64::try_from(ordinal).map_err(|_| {
                                SubmissionProcessingErrorV1::InternalInvariant(
                                    "全撤活动目标序号超出 u64 边界",
                                )
                            })?;
                            let effect_identity = cancel_target_effect_identity(
                                &resolution.effect_identity,
                                &platform_order_id,
                                ordinal,
                            );
                            let order = execution.order(&platform_order_id).cloned();
                            let (outcome, feedback) = cancel_target_outcome(
                                execution.cancel_order_remainder(
                                    &platform_order_id,
                                    formed_at,
                                    &Sha256Hex(effect_identity.clone()),
                                )?,
                                order.as_ref(),
                            )?;
                            match &outcome {
                                CancelTargetOutcomeV1::Canceled(_) => {}
                                _ => {
                                    return Err(SubmissionProcessingErrorV1::InternalInvariant(
                                        "全撤冻结活动目标在同一工作副本中失去可撤余量",
                                    ));
                                }
                            }
                            feedbacks.extend(feedback);
                            targets.push(CancelTargetEffectV1 {
                                platform_order_id,
                                effect_identity,
                                outcome,
                            });
                        }
                        working_session.replace_execution_state(execution);
                        let record = CancelEffectRecordV1 {
                            originating_global_request_id: action.global_request_id.clone(),
                            formed_at_utc_ms: formed_at,
                            targets,
                        };
                        if working_effect_records
                            .insert(resolution.effect_identity.clone(), record.clone())
                            .is_some()
                        {
                            return Err(SubmissionProcessingErrorV1::InternalInvariant(
                                "新全撤幂等效果已存在终局结果",
                            ));
                        }
                        (record, feedbacks)
                    }
                };
                let effect_disposition =
                    if record.originating_global_request_id == action.global_request_id {
                        CancelEffectDispositionV1::NewEffect
                    } else {
                        CancelEffectDispositionV1::ExistingEffect
                    };
                let targets = record
                    .targets
                    .iter()
                    .enumerate()
                    .map(|(ordinal, target)| {
                        Ok(CancelTargetResponseV1 {
                            platform_order_id: target.platform_order_id.clone(),
                            target_ordinal: u64::try_from(ordinal).map_err(|_| {
                                SubmissionProcessingErrorV1::InternalInvariant(
                                    "全撤响应目标序号超出 u64 边界",
                                )
                            })?,
                            input_item_refs: Vec::new(),
                            effective_processing_at_utc_ms: formed_at,
                            cancel_effect_identity: target.effect_identity.clone(),
                            cancel_effect_formed_at_utc_ms: record.formed_at_utc_ms,
                            cancel_effect_disposition: effect_disposition,
                            outcome: target.outcome.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, SubmissionProcessingErrorV1>>()?;
                Ok((
                    DirectResponsePartV1::CancelAll {
                        effective_processing_at_utc_ms: formed_at,
                        targets,
                    },
                    feedbacks,
                ))
            }
            _ => Err(SubmissionProcessingErrorV1::InternalInvariant(
                "非撤单请求进入撤单处理器",
            )),
        }
    }

    fn process_query_action(
        &self,
        working_session: &mut SimSession,
        working_query_state: &mut QueryStateV1,
        action: &QueuedLogicalActionV1,
        request: &PendingDomainRequestV1,
    ) -> Result<DirectResponsePartV1, SubmissionProcessingErrorV1> {
        validate_action_mapping(request, action)?;
        let query_sha256 = canonical_request_sha256(request);
        let effective_processing_at_utc_ms = action.key.effective_processing_at_utc_ms;
        match request {
            PendingDomainRequestV1::Order(request) => {
                working_session.resolve_query_revision(
                    &action.global_request_id,
                    &action.action_identity,
                    query_sha256,
                )?;
                let hot_order = match working_session
                    .execution_state()
                    .order(&request.platform_order_id)
                {
                    Some(order) if working_query_state.hot_order_is_query_visible(order)? => {
                        Some(order)
                    }
                    Some(_) | None => None,
                };
                let result = match hot_order {
                    Some(order) => OrderQueryResultV1::Found(Box::new(project_order(order)?)),
                    None => match working_query_state
                        .retained_terminal_order(&request.platform_order_id)
                    {
                        Some(order) => OrderQueryResultV1::Found(Box::new(order.clone())),
                        None => match working_query_state.order_history_retained_from_utc_ms {
                            Some(retained_from_utc_ms) => {
                                OrderQueryResultV1::OutOfRetention(QueryOutOfRetentionV1 {
                                    scope: QueryRetentionScopeV1::OrderHistory,
                                    retained_from_utc_ms: Some(retained_from_utc_ms),
                                    requested_root_sequence: None,
                                    earliest_retained_root_sequence: working_query_state
                                        .earliest_retained_root_sequence,
                                })
                            }
                            None => OrderQueryResultV1::NotFound,
                        },
                    },
                };
                Ok(DirectResponsePartV1::Order {
                    effective_processing_at_utc_ms,
                    result,
                })
            }
            PendingDomainRequestV1::Orders(request) => {
                let result = if request.cursor.is_some() {
                    let binding = working_query_state
                        .continuation(&action.global_request_id, ListQueryMethodV1::Orders)?;
                    working_session.resolve_query_revision_at(
                        &action.global_request_id,
                        &action.action_identity,
                        query_sha256,
                        binding.query_projection_revision,
                    )?;
                    working_query_state.continue_orders(&action.global_request_id)?
                } else {
                    let revision = working_session.resolve_query_revision(
                        &action.global_request_id,
                        &action.action_identity,
                        query_sha256,
                    )?;
                    let mut rows = working_session
                        .execution_state()
                        .orders()
                        .filter(|order| {
                            order
                                .pending_maker_depletion_base_units
                                .checked_add(order.open_base_units)
                                .is_some_and(|active| active > 0)
                        })
                        .filter(|order| {
                            request
                                .filter
                                .condition_id
                                .as_ref()
                                .is_none_or(|id| id.0 == order.condition_id)
                                && request
                                    .filter
                                    .token_id
                                    .as_ref()
                                    .is_none_or(|id| id.0 == order.token_id)
                        })
                        .map(project_order)
                        .collect::<Result<Vec<_>, _>>()?;
                    rows.sort_by(|left, right| {
                        (left.created_at_utc_ms, left.platform_order_id.as_str())
                            .cmp(&(right.created_at_utc_ms, right.platform_order_id.as_str()))
                    });
                    OrdersQueryResultV1::Page(working_query_state.new_orders_page(
                        working_session.session_spec(),
                        orders_filter_sha256(&request.filter),
                        revision.platform_state_revision,
                        rows,
                    )?)
                };
                Ok(DirectResponsePartV1::Orders {
                    effective_processing_at_utc_ms,
                    result,
                })
            }
            PendingDomainRequestV1::Trades(request) => {
                let result = if request.cursor.is_some() {
                    let binding = working_query_state
                        .continuation(&action.global_request_id, ListQueryMethodV1::Trades)?;
                    working_session.resolve_query_revision_at(
                        &action.global_request_id,
                        &action.action_identity,
                        query_sha256,
                        binding.query_projection_revision,
                    )?;
                    working_query_state.continue_trades(&action.global_request_id)?
                } else {
                    let revision = working_session.resolve_query_revision(
                        &action.global_request_id,
                        &action.action_identity,
                        query_sha256,
                    )?;
                    let normalized_interval = strict_millisecond_interval(&request.filter);
                    let empty_interval = normalized_interval.is_none();
                    let retained_exact_trade = request.filter.trade_id.as_ref().is_some_and(|id| {
                        working_session.execution_state().fills().any(|fill| {
                            &fill.trade_id == id
                                && working_query_state.trade_is_retained(fill.matched_at_utc_ms)
                        }) || working_query_state
                            .terminal_projections
                            .trades
                            .get(id)
                            .is_some_and(|trade| {
                                working_query_state.trade_is_retained(trade.matched_at_utc_ms)
                            })
                    });
                    if !empty_interval
                        && !retained_exact_trade
                        && working_query_state
                            .trade_history_retained_from_utc_ms
                            .is_some_and(|floor| {
                                normalized_interval
                                    .is_some_and(|(first_inclusive, _)| first_inclusive < floor)
                            })
                    {
                        TradesQueryResultV1::OutOfRetention(QueryOutOfRetentionV1 {
                            scope: QueryRetentionScopeV1::TradeHistory,
                            retained_from_utc_ms: working_query_state
                                .trade_history_retained_from_utc_ms,
                            requested_root_sequence: None,
                            earliest_retained_root_sequence: working_query_state
                                .earliest_retained_root_sequence,
                        })
                    } else {
                        let mut rows_by_id = BTreeMap::new();
                        if !empty_interval {
                            for fill in working_session.execution_state().fills().filter(|fill| {
                                working_query_state.trade_is_retained(fill.matched_at_utc_ms)
                                    && request
                                        .filter
                                        .trade_id
                                        .as_ref()
                                        .is_none_or(|id| id == &fill.trade_id)
                                    && request
                                        .filter
                                        .condition_id
                                        .as_ref()
                                        .is_none_or(|id| id.0 == fill.condition_id)
                                    && request
                                        .filter
                                        .token_id
                                        .as_ref()
                                        .is_none_or(|id| id.0 == fill.token_id)
                                    && request
                                        .filter
                                        .after_utc_ms
                                        .is_none_or(|after| after < fill.matched_at_utc_ms)
                                    && request
                                        .filter
                                        .before_utc_ms
                                        .is_none_or(|before| fill.matched_at_utc_ms < before)
                            }) {
                                let projection = project_trade(fill)?;
                                rows_by_id
                                    .insert(projection.simulated_trade_id.clone(), projection);
                            }
                            for trade in working_query_state
                                .terminal_projections
                                .trades
                                .values()
                                .filter(|trade| {
                                    working_query_state.trade_is_retained(trade.matched_at_utc_ms)
                                        && request
                                            .filter
                                            .trade_id
                                            .as_ref()
                                            .is_none_or(|id| id == &trade.simulated_trade_id)
                                        && request
                                            .filter
                                            .condition_id
                                            .as_ref()
                                            .is_none_or(|id| id == &trade.condition_id)
                                        && request
                                            .filter
                                            .token_id
                                            .as_ref()
                                            .is_none_or(|id| id == &trade.token_id)
                                        && request
                                            .filter
                                            .after_utc_ms
                                            .is_none_or(|after| after < trade.matched_at_utc_ms)
                                        && request
                                            .filter
                                            .before_utc_ms
                                            .is_none_or(|before| trade.matched_at_utc_ms < before)
                                })
                            {
                                rows_by_id
                                    .entry(trade.simulated_trade_id.clone())
                                    .or_insert_with(|| trade.clone());
                            }
                        }
                        let mut rows = rows_by_id.into_values().collect::<Vec<_>>();
                        rows.sort_by(|left, right| {
                            (left.matched_at_utc_ms, left.simulated_trade_id.as_str())
                                .cmp(&(right.matched_at_utc_ms, right.simulated_trade_id.as_str()))
                        });
                        TradesQueryResultV1::Page(working_query_state.new_trades_page(
                            working_session.session_spec(),
                            trades_filter_sha256(&request.filter),
                            revision.platform_state_revision,
                            rows,
                        )?)
                    }
                };
                Ok(DirectResponsePartV1::Trades {
                    effective_processing_at_utc_ms,
                    result,
                })
            }
            _ => Err(SubmissionProcessingErrorV1::InternalInvariant(
                "非查询请求进入查询处理器",
            )),
        }
    }

    fn process_prepared_submission(
        &self,
        session: &SimSession,
        prepared: &PreparedSubmissionV1,
        effect_identity: &str,
        item_idempotency_identity: &str,
    ) -> Result<PreparedSubmissionResultV1, SubmissionProcessingErrorV1> {
        let platform_order_id = stable_order_id(session.session_spec(), effect_identity);
        let mut execution = session.execution_state().clone();
        let (mut order, match_plan) = match &prepared.order {
            PreparedBuyOrderV1::GtcLimit {
                limit_price_pusd_per_share,
                maker_amount_pusd,
                taker_amount_shares,
                ..
            } => {
                let accepted_base_units = micro_units_v1(*taker_amount_shares)?;
                let accepted_quote_units = micro_units_v1(*maker_amount_pusd)?;
                let plan = execution.match_taker_buy_by_base(
                    &prepared.condition_id.0,
                    &prepared.token_id.0,
                    *limit_price_pusd_per_share,
                    accepted_base_units,
                    accepted_quote_units,
                )?;
                let allocated_quote = cumulative_segment_quote_units_v1(
                    accepted_base_units,
                    accepted_quote_units,
                    plan.matched_base_units,
                )?;
                let released_quote_units = allocated_quote
                    .checked_sub(plan.used_quote_units)
                    .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                        "限价价格改善 quote 超出接受本金",
                    ))?;
                (
                    SimOrderRecordV1 {
                        platform_order_id: platform_order_id.clone(),
                        global_request_id: prepared.global_request_id.clone(),
                        item_id: prepared.item_id.clone(),
                        item_idempotency_identity: item_idempotency_identity.to_owned(),
                        condition_id: prepared.condition_id.0.clone(),
                        token_id: prepared.token_id.0.clone(),
                        created_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        last_updated_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        kind: SimOrderKindStateV1::Gtc,
                        source_price_pusd_per_share: *limit_price_pusd_per_share,
                        accepted_base_units: Some(accepted_base_units),
                        matched_base_units: plan.matched_base_units,
                        pending_maker_depletion_base_units: 0,
                        pending_maker_depletion_evidence: None,
                        open_base_units: accepted_base_units
                            .checked_sub(plan.matched_base_units)
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                            "限价成交 base 超出接受数量",
                        ))?,
                        expired_base_units: 0,
                        expired_released_quote_units: 0,
                        expired_at_utc_ms: None,
                        gtd_expiration_identity: None,
                        venue_dust_closed_base_units: 0,
                        venue_dust_released_quote_units: 0,
                        venue_dust_at_utc_ms: None,
                        venue_dust_identity: None,
                        market_resolved_canceled_base_units: 0,
                        market_resolved_released_quote_units: 0,
                        market_resolved_at_utc_ms: None,
                        market_resolution_effect_identity: None,
                        canceled_base_units: 0,
                        canceled_released_quote_units: 0,
                        canceled_at_utc_ms: None,
                        caller_cancel_effect_identity: None,
                        terminalized_pending_maker_depletion_evidence: None,
                        accepted_quote_units,
                        matched_quote_units: plan.used_quote_units,
                        open_quote_units: accepted_quote_units.checked_sub(allocated_quote).ok_or(
                            SubmissionProcessingErrorV1::InternalInvariant(
                                "限价开放 quote 超出接受本金",
                            ),
                        )?,
                        released_quote_units,
                        sdk_taker_amount_audit_base_units: None,
                        immediate_taker_fee_encumbered_units: 0,
                        terminal_reason: None,
                    },
                    plan,
                )
            }
            PreparedBuyOrderV1::GtdLimit {
                limit_price_pusd_per_share,
                maker_amount_pusd,
                taker_amount_shares,
                expires_at_utc_ms,
                ..
            } => {
                let accepted_base_units = micro_units_v1(*taker_amount_shares)?;
                let accepted_quote_units = micro_units_v1(*maker_amount_pusd)?;
                let plan = execution.match_taker_buy_by_base(
                    &prepared.condition_id.0,
                    &prepared.token_id.0,
                    *limit_price_pusd_per_share,
                    accepted_base_units,
                    accepted_quote_units,
                )?;
                let allocated_quote = cumulative_segment_quote_units_v1(
                    accepted_base_units,
                    accepted_quote_units,
                    plan.matched_base_units,
                )?;
                let released_quote_units = allocated_quote
                    .checked_sub(plan.used_quote_units)
                    .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                        "GTD 价格改善 quote 超出接受本金",
                    ))?;
                (
                    SimOrderRecordV1 {
                        platform_order_id: platform_order_id.clone(),
                        global_request_id: prepared.global_request_id.clone(),
                        item_id: prepared.item_id.clone(),
                        item_idempotency_identity: item_idempotency_identity.to_owned(),
                        condition_id: prepared.condition_id.0.clone(),
                        token_id: prepared.token_id.0.clone(),
                        created_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        last_updated_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        kind: SimOrderKindStateV1::Gtd {
                            expires_at_utc_ms: *expires_at_utc_ms,
                        },
                        source_price_pusd_per_share: *limit_price_pusd_per_share,
                        accepted_base_units: Some(accepted_base_units),
                        matched_base_units: plan.matched_base_units,
                        pending_maker_depletion_base_units: 0,
                        pending_maker_depletion_evidence: None,
                        open_base_units: accepted_base_units
                            .checked_sub(plan.matched_base_units)
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                            "GTD 成交 base 超出接受数量",
                        ))?,
                        expired_base_units: 0,
                        expired_released_quote_units: 0,
                        expired_at_utc_ms: None,
                        gtd_expiration_identity: None,
                        venue_dust_closed_base_units: 0,
                        venue_dust_released_quote_units: 0,
                        venue_dust_at_utc_ms: None,
                        venue_dust_identity: None,
                        market_resolved_canceled_base_units: 0,
                        market_resolved_released_quote_units: 0,
                        market_resolved_at_utc_ms: None,
                        market_resolution_effect_identity: None,
                        canceled_base_units: 0,
                        canceled_released_quote_units: 0,
                        canceled_at_utc_ms: None,
                        caller_cancel_effect_identity: None,
                        terminalized_pending_maker_depletion_evidence: None,
                        accepted_quote_units,
                        matched_quote_units: plan.used_quote_units,
                        open_quote_units: accepted_quote_units.checked_sub(allocated_quote).ok_or(
                            SubmissionProcessingErrorV1::InternalInvariant(
                                "GTD 开放 quote 超出接受本金",
                            ),
                        )?,
                        released_quote_units,
                        sdk_taker_amount_audit_base_units: None,
                        immediate_taker_fee_encumbered_units: 0,
                        terminal_reason: None,
                    },
                    plan,
                )
            }
            PreparedBuyOrderV1::FokMarket {
                worst_price_pusd_per_share,
                maker_amount_pusd,
                taker_amount_shares,
            }
            | PreparedBuyOrderV1::FakMarket {
                worst_price_pusd_per_share,
                maker_amount_pusd,
                taker_amount_shares,
            } => {
                let accepted_quote_units = micro_units_v1(*maker_amount_pusd)?;
                let sdk_taker_amount_audit_base_units = micro_units_v1(*taker_amount_shares)?;
                let simulated = execution.match_taker_buy_by_quote(
                    &prepared.condition_id.0,
                    &prepared.token_id.0,
                    *worst_price_pusd_per_share,
                    accepted_quote_units,
                )?;
                let fok = matches!(&prepared.order, PreparedBuyOrderV1::FokMarket { .. });
                if fok && simulated.used_quote_units != accepted_quote_units {
                    return Ok((
                        SubmissionItemOutcomeV1::RejectedNoEffect(
                            SubmissionRejectedReasonV1::FokNotFullyFilled,
                        ),
                        None,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
                if !fok && simulated.fills.is_empty() {
                    return Ok((
                        SubmissionItemOutcomeV1::RejectedNoEffect(
                            SubmissionRejectedReasonV1::FakNoMatch,
                        ),
                        None,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
                let terminal_reason = (!fok).then_some(SimOrderTerminalReasonV1::FakCompleted);
                let plan = simulated;
                (
                    SimOrderRecordV1 {
                        platform_order_id: platform_order_id.clone(),
                        global_request_id: prepared.global_request_id.clone(),
                        item_id: prepared.item_id.clone(),
                        item_idempotency_identity: item_idempotency_identity.to_owned(),
                        condition_id: prepared.condition_id.0.clone(),
                        token_id: prepared.token_id.0.clone(),
                        created_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        last_updated_at_utc_ms: prepared.effective_processing_at_utc_ms,
                        kind: if fok {
                            SimOrderKindStateV1::Fok
                        } else {
                            SimOrderKindStateV1::Fak
                        },
                        source_price_pusd_per_share: *worst_price_pusd_per_share,
                        accepted_base_units: None,
                        matched_base_units: plan.matched_base_units,
                        pending_maker_depletion_base_units: 0,
                        pending_maker_depletion_evidence: None,
                        open_base_units: 0,
                        expired_base_units: 0,
                        expired_released_quote_units: 0,
                        expired_at_utc_ms: None,
                        gtd_expiration_identity: None,
                        venue_dust_closed_base_units: 0,
                        venue_dust_released_quote_units: 0,
                        venue_dust_at_utc_ms: None,
                        venue_dust_identity: None,
                        market_resolved_canceled_base_units: 0,
                        market_resolved_released_quote_units: 0,
                        market_resolved_at_utc_ms: None,
                        market_resolution_effect_identity: None,
                        canceled_base_units: 0,
                        canceled_released_quote_units: 0,
                        canceled_at_utc_ms: None,
                        caller_cancel_effect_identity: None,
                        terminalized_pending_maker_depletion_evidence: None,
                        accepted_quote_units,
                        matched_quote_units: plan.used_quote_units,
                        open_quote_units: 0,
                        released_quote_units: accepted_quote_units
                            .checked_sub(plan.used_quote_units)
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                                "市价 BUY 消耗 quote 超出预算",
                            ))?,
                        sdk_taker_amount_audit_base_units: Some(sdk_taker_amount_audit_base_units),
                        immediate_taker_fee_encumbered_units: 0,
                        terminal_reason,
                    },
                    plan,
                )
            }
        };
        let mut fill_records = taker_fill_records(
            &platform_order_id,
            &prepared.condition_id.0,
            &prepared.token_id.0,
            prepared.effective_processing_at_utc_ms,
            &match_plan,
        )?;
        let fee_units = allocate_immediate_taker_fee(session, &mut fill_records)?;
        order.immediate_taker_fee_encumbered_units = fee_units;
        let required_units = order.accepted_quote_units.checked_add(fee_units).ok_or(
            SubmissionProcessingErrorV1::InternalInvariant("订单本金与手续费之和溢出"),
        )?;
        if execution.available_pusd_units() < required_units {
            return Ok((
                SubmissionItemOutcomeV1::RejectedNoEffect(
                    SubmissionRejectedReasonV1::InsufficientAvailablePusd,
                ),
                None,
                Vec::new(),
                Vec::new(),
            ));
        }
        let commit_effects = execution.commit_new_order(order.clone(), fill_records.clone())?;
        let mut fill_ids = fill_records
            .iter()
            .map(|fill| fill.trade_id.clone())
            .collect::<Vec<_>>();
        fill_ids.sort();
        let mut trade_feedbacks = Vec::with_capacity(fill_ids.len());
        for trade_id in fill_ids {
            trade_feedbacks
                .push(execution.initialize_fill_finality(session.finality_policy(), &trade_id)?);
        }
        let lifecycle_account_rule = &session
            .rule_snapshot()
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;
        let venue_dust_effect = execution.apply_venue_dust_after_order_action(
            &order.platform_order_id,
            prepared.effective_processing_at_utc_ms,
            lifecycle_account_rule,
        )?;
        let mut account_delta_references = commit_effects
            .account_deltas
            .iter()
            .map(|delta| AccountDeltaReferenceV1::from(delta.as_ref()))
            .collect::<Vec<_>>();
        if let Some(effect) = &venue_dust_effect {
            account_delta_references
                .push(AccountDeltaReferenceV1::from(effect.account_delta.as_ref()));
        }
        let order_status = if order.open_base_units > 0 {
            simulated_order_status("LIVE")
        } else {
            simulated_order_status("MATCHED")
        };
        let mut feedbacks = commit_effects
            .account_deltas
            .iter()
            .map(|delta| OrderLifecycleFeedbackSeedV1 {
                platform_order_id: order.platform_order_id.clone(),
                condition_id: order.condition_id.clone(),
                token_id: order.token_id.clone(),
                event_kind: match delta.reason {
                    contracts::PlatformAccountDeltaReasonV1::OrderAcceptedEncumbranceApplied => {
                        OrderLifecycleEventKindV1::OrderAccepted
                    }
                    contracts::PlatformAccountDeltaReasonV1::FakUnspentEncumbranceReleased => {
                        OrderLifecycleEventKindV1::FakUnspentReleased
                    }
                    _ => unreachable!("订单提交只能形成接受或 FAK 未花释放变化"),
                },
                order_status: order_status.clone(),
                venue_dust_closed_base_units: 0,
                venue_dust_released_quote_units: 0,
                market_resolved_canceled_base_units: 0,
                market_resolved_released_quote_units: 0,
                account_delta: Arc::clone(delta),
            })
            .collect::<Vec<_>>();
        if let Some(effect) = venue_dust_effect {
            feedbacks.push(OrderLifecycleFeedbackSeedV1 {
                platform_order_id: effect.platform_order_id,
                condition_id: effect.condition_id,
                token_id: effect.token_id,
                event_kind: OrderLifecycleEventKindV1::VenueDustTerminal,
                order_status: simulated_order_status("VENUE_DUST_TERMINAL"),
                venue_dust_closed_base_units: effect.closed_base_units,
                venue_dust_released_quote_units: effect.released_open_quote_units,
                market_resolved_canceled_base_units: 0,
                market_resolved_released_quote_units: 0,
                account_delta: effect.account_delta,
            });
        }
        let committed_order = execution.order(&order.platform_order_id).ok_or(
            SubmissionProcessingErrorV1::InternalInvariant("订单提交后缺少规范订单记录"),
        )?;
        Ok((
            SubmissionItemOutcomeV1::Accepted(Box::new(accepted_submission_item(
                committed_order,
                &fill_records,
                account_delta_references,
            )?)),
            Some(execution),
            feedbacks,
            trade_feedbacks,
        ))
    }

    pub(crate) fn drive_private_delivery_through(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<SubmissionDeliveryDispositionV1, SimExecutionContractErrorV1> {
        self.require_durable_business_writes_open()?;
        let mut delivered_any = false;
        loop {
            let next = match self.drive_next_private_delivery_through(received_at_utc_ms) {
                Ok(next) => next,
                Err(SessionError::PrivateFactScheduleCrossesExpectedResponse(_))
                    if delivered_any =>
                {
                    return Ok(SubmissionDeliveryDispositionV1::Delivered);
                }
                Err(SessionError::PrivateFactScheduleCrossesPendingAction(_)) if delivered_any => {
                    return Ok(SubmissionDeliveryDispositionV1::Delivered);
                }
                Err(SessionError::PrivateFactScheduleCrossesPendingPlatformWork(_))
                    if delivered_any =>
                {
                    return Ok(SubmissionDeliveryDispositionV1::Delivered);
                }
                Err(error) => return Err(error),
            };
            match next {
                SubmissionDeliveryDispositionV1::Delivered => delivered_any = true,
                SubmissionDeliveryDispositionV1::NoVisibleResponse if delivered_any => {
                    return Ok(SubmissionDeliveryDispositionV1::Delivered);
                }
                disposition => return Ok(disposition),
            }
        }
    }

    fn drive_next_private_delivery_through(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<SubmissionDeliveryDispositionV1, SimExecutionContractErrorV1> {
        let process_confirmed_through = self.session.private_feedback_sequence();
        if self.bridge_replayed_through_private_feedback_sequence < process_confirmed_through {
            let next_sequence = self
                .bridge_replayed_through_private_feedback_sequence
                .checked_add(1)
                .ok_or(SessionError::PrivateEmissionSequenceConflict(u64::MAX))?;
            let delivery = self
                .confirmed_private_deliveries
                .get(&next_sequence)
                .cloned()
                .ok_or_else(|| {
                    SessionError::Durability(format!(
                        "恢复重放缺少已确认私有载荷序号 {next_sequence}"
                    ))
                })?;
            let expected_confirmation = delivery.confirmation();
            return match delivery.dispatch(&mut self.response_bridge) {
                SubmissionResponseAcceptanceV1::Accepted(actual)
                    if actual == expected_confirmation =>
                {
                    self.bridge_replayed_through_private_feedback_sequence = next_sequence;
                    Ok(SubmissionDeliveryDispositionV1::Delivered)
                }
                SubmissionResponseAcceptanceV1::Accepted(_) => {
                    self.close_ingress_durably()?;
                    Err(SessionError::ResponseDeliveryIdentityConflict)
                }
                SubmissionResponseAcceptanceV1::Full => Ok(SubmissionDeliveryDispositionV1::Full),
                SubmissionResponseAcceptanceV1::Disconnected => {
                    self.close_ingress_durably()?;
                    Ok(SubmissionDeliveryDispositionV1::Closed)
                }
            };
        }

        let before_state = self.runtime_state();
        let before_checkpoint = self.durable_checkpoint();
        let already_closed = self.session.private_fact_schedule_closed_through();
        if already_closed.is_some_and(|closed| received_at_utc_ms < closed) {
            self.session
                .close_private_fact_schedule_through(received_at_utc_ms)?;
        }
        let next_private_at = self
            .session
            .next_private_fact_received_at_through(received_at_utc_ms);
        let close_through = match (already_closed, next_private_at) {
            (Some(closed), Some(next)) if next <= closed => closed,
            (Some(_), Some(next)) => next,
            (Some(closed), None) if closed >= received_at_utc_ms => closed,
            (_, Some(next)) => next,
            _ => received_at_utc_ms,
        };
        if already_closed != Some(close_through) {
            self.session
                .close_private_fact_schedule_through(close_through)?;
        }
        let Some(candidate) = self
            .session
            .prepare_next_private_fact_through(close_through)?
        else {
            let candidate_checkpoint = self.durable_checkpoint();
            if let Err(error) = self.commit_durable_transition(
                &before_checkpoint,
                candidate_checkpoint,
                DurableTransactionEvidenceV1::new(DurableTransactionFactV1::ExecutionAdvanced {
                    session_id: self.session.session_spec().session_id.clone(),
                }),
            ) {
                self.replace_runtime_state(before_state);
                return Err(error);
            }
            return Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse);
        };
        self.session.validate_prepared_private_fact(&candidate)?;
        let delivery = match &candidate.evidence {
            crate::session::PrivateVisibilityEvidenceV1::DirectResponse { global_request_id } => {
                self.direct_response_outbox
                    .validate_remove_confirmed(global_request_id)?;
                if self
                    .direct_response_outbox
                    .sealed(global_request_id)
                    .is_none()
                {
                    let draft = self
                        .direct_response_outbox
                        .draft(global_request_id)
                        .ok_or_else(|| {
                            SimExecutionContractErrorV1::RequestNotFound(global_request_id.clone())
                        })?;
                    let response = seal_direct_response(draft, &candidate)?;
                    self.direct_response_outbox
                        .cache_sealed(global_request_id, response)?;
                }
                let sealed = self
                    .direct_response_outbox
                    .sealed(global_request_id)
                    .cloned()
                    .ok_or_else(|| {
                        SimExecutionContractErrorV1::RequestNotFound(global_request_id.clone())
                    })?;
                SealedPrivateDeliveryV1::Direct(sealed)
            }
            crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } => {
                self.lifecycle_outbox
                    .validate_remove_confirmed(feedback_id)?;
                if self.lifecycle_outbox.sealed(feedback_id).is_none() {
                    let draft = self.lifecycle_outbox.draft(feedback_id).ok_or_else(|| {
                        SimExecutionContractErrorV1::FeedbackIdentityConflict(feedback_id.clone())
                    })?;
                    let feedback = match draft {
                        LifecycleFeedbackDraftV1::Order(draft) => SealedLifecycleFeedbackV1::Order(
                            seal_order_lifecycle_feedback(draft, &candidate)?,
                        ),
                        LifecycleFeedbackDraftV1::Trade(draft) => SealedLifecycleFeedbackV1::Trade(
                            seal_trade_lifecycle_feedback(draft, &candidate)?,
                        ),
                        LifecycleFeedbackDraftV1::Settlement(draft) => {
                            SealedLifecycleFeedbackV1::Settlement(
                                seal_settlement_lifecycle_feedback(draft, &candidate)?,
                            )
                        }
                    };
                    self.lifecycle_outbox.cache_sealed(feedback_id, feedback)?;
                }
                let sealed = self
                    .lifecycle_outbox
                    .sealed(feedback_id)
                    .cloned()
                    .ok_or_else(|| {
                        SimExecutionContractErrorV1::FeedbackIdentityConflict(feedback_id.clone())
                    })?;
                SealedPrivateDeliveryV1::Lifecycle(sealed)
            }
        };
        let expected_confirmation = delivery.confirmation();
        let prepared_checkpoint = self.durable_checkpoint();
        if let Err(error) = self.commit_durable_transition(
            &before_checkpoint,
            prepared_checkpoint.clone(),
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::PrivateOutputPrepared {
                session_id: expected_confirmation.session_id.clone(),
                delivery: delivery.clone(),
            }),
        ) {
            self.replace_runtime_state(before_state);
            return Err(error);
        }
        let acceptance = delivery.dispatch(&mut self.response_bridge);
        match acceptance {
            SubmissionResponseAcceptanceV1::Accepted(actual) if actual == expected_confirmation => {
                let prepared_state = self.runtime_state();
                self.session.commit_prevalidated_private_fact(&candidate);
                match &candidate.evidence {
                    crate::session::PrivateVisibilityEvidenceV1::DirectResponse {
                        global_request_id,
                    } => {
                        self.direct_response_outbox
                            .remove_prevalidated_confirmed(global_request_id);
                    }
                    crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback {
                        feedback_id,
                    } => {
                        self.lifecycle_outbox
                            .remove_prevalidated_confirmed(feedback_id);
                    }
                }
                let sequence = expected_confirmation.private_feedback_sequence;
                if let Some(existing) = self
                    .confirmed_private_deliveries
                    .insert(sequence, delivery.clone())
                    && existing != delivery
                {
                    self.replace_runtime_state(prepared_state);
                    return Err(SessionError::PrivateEmissionSequenceConflict(sequence));
                }
                let confirmed_checkpoint = self.durable_checkpoint();
                if let Err(error) = self.commit_durable_transition(
                    &prepared_checkpoint,
                    confirmed_checkpoint,
                    DurableTransactionEvidenceV1::new(
                        DurableTransactionFactV1::PrivateOutputConfirmed {
                            session_id: expected_confirmation.session_id.clone(),
                            confirmation: expected_confirmation.clone(),
                        },
                    ),
                ) {
                    self.replace_runtime_state(prepared_state);
                    return Err(error);
                }
                self.bridge_replayed_through_private_feedback_sequence = sequence;
                Ok(SubmissionDeliveryDispositionV1::Delivered)
            }
            SubmissionResponseAcceptanceV1::Accepted(_) => {
                self.close_ingress_durably()?;
                Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict)
            }
            SubmissionResponseAcceptanceV1::Full => Ok(SubmissionDeliveryDispositionV1::Full),
            SubmissionResponseAcceptanceV1::Disconnected => {
                self.close_ingress_durably()?;
                Ok(SubmissionDeliveryDispositionV1::Closed)
            }
        }
    }

    pub fn post_order(
        &mut self,
        request: PostOrderRequestV1,
    ) -> Result<PostOrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_post_item(&request.item)?;
        let item_id = request.item.item_id.clone();
        let method_timing = request.item.order.kind.method_timing();
        let schedule = self.register(
            PendingDomainRequestV1::PostOrder(request),
            vec![method_action(&item_id, 0, method_timing)],
        )?;
        Ok(PostOrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
            item: registered_item(item_id, 0, &schedule),
        })
    }

    pub fn post_orders(
        &mut self,
        request: PostOrdersRequestV1,
    ) -> Result<PostOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_batch_len(
            request.items.len(),
            POST_ORDERS_MAX_ITEMS,
            "批量提交项数必须为 1 至 15",
        )?;
        validate_unique_item_ids(request.items.iter().map(|item| item.item_id.as_str()))?;
        for item in &request.items {
            validate_post_item(item)?;
        }
        let item_specs = request
            .items
            .iter()
            .enumerate()
            .map(|(ordinal, item)| {
                (
                    item.item_id.clone(),
                    u16::try_from(ordinal).expect("批量提交上限适合 u16"),
                    item.order.kind.method_timing(),
                )
            })
            .collect::<Vec<_>>();
        let actions = item_specs
            .iter()
            .map(|(item_id, ordinal, timing)| method_action(item_id, *ordinal, *timing))
            .collect();
        let schedule = self.register(PendingDomainRequestV1::PostOrders(request), actions)?;
        Ok(PostOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
            items: item_specs
                .into_iter()
                .map(|(item_id, ordinal, _)| registered_item(item_id, ordinal, &schedule))
                .collect(),
        })
    }

    pub fn cancel_order(
        &mut self,
        request: CancelOrderRequestV1,
    ) -> Result<CancelOrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_cancel_item(&request.item)?;
        let item_id = request.item.item_id.clone();
        let schedule = self.register(
            PendingDomainRequestV1::CancelOrder(request),
            vec![method_action(&item_id, 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(CancelOrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
            item: registered_item(item_id, 0, &schedule),
        })
    }

    pub fn cancel_orders(
        &mut self,
        request: CancelOrdersRequestV1,
    ) -> Result<CancelOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_batch_len(
            request.items.len(),
            CANCEL_ORDERS_MAX_ITEMS,
            "批量撤单项数必须为 1 至 1000",
        )?;
        validate_unique_item_ids(request.items.iter().map(|item| item.item_id.as_str()))?;
        for item in &request.items {
            validate_cancel_item(item)?;
        }

        let mut ordinal_by_order_id = BTreeMap::new();
        let mut actions = Vec::new();
        let mut item_mappings = Vec::with_capacity(request.items.len());
        for item in &request.items {
            let ordinal = if let Some(ordinal) = ordinal_by_order_id.get(&item.platform_order_id) {
                *ordinal
            } else {
                let ordinal = u16::try_from(actions.len()).expect("批量撤单上限适合 u16");
                ordinal_by_order_id.insert(item.platform_order_id.clone(), ordinal);
                actions.push(method_action(
                    &item.item_id,
                    ordinal,
                    ParsedMethodTimingV1::Ordinary,
                ));
                ordinal
            };
            item_mappings.push((item.item_id.clone(), ordinal));
        }
        let schedule = self.register(PendingDomainRequestV1::CancelOrders(request), actions)?;
        Ok(CancelOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
            items: item_mappings
                .into_iter()
                .map(|(item_id, ordinal)| registered_item(item_id, ordinal, &schedule))
                .collect(),
        })
    }

    pub fn cancel_all_orders(
        &mut self,
        request: CancelAllOrdersRequestV1,
    ) -> Result<CancelAllOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        let schedule = self.register(
            PendingDomainRequestV1::CancelAllOrders(request),
            vec![method_action(
                "cancel-all",
                0,
                ParsedMethodTimingV1::Ordinary,
            )],
        )?;
        Ok(CancelAllOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn order(
        &mut self,
        request: OrderRequestV1,
    ) -> Result<OrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_platform_order_id(&request.platform_order_id)?;
        let schedule = self.register(
            PendingDomainRequestV1::Order(request),
            vec![method_action("order", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(OrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn orders(
        &mut self,
        request: OrdersRequestV1,
    ) -> Result<OrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_orders_request(&request)?;
        let schedule = self.register(
            PendingDomainRequestV1::Orders(request),
            vec![method_action("orders", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(OrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn trades(
        &mut self,
        request: TradesRequestV1,
    ) -> Result<TradesRegistrationV1, SimExecutionContractErrorV1> {
        validate_trades_request(&request)?;
        let schedule = self.register(
            PendingDomainRequestV1::Trades(request),
            vec![method_action("trades", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(TradesRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    fn register(
        &mut self,
        request: PendingDomainRequestV1,
        mut actions: Vec<ParsedLogicalActionV1>,
    ) -> Result<RegisteredLogicalScheduleV1, SimExecutionContractErrorV1> {
        self.require_durable_business_writes_open()?;
        let expected_checkpoint = self.durable_checkpoint();
        self.session.require_active()?;
        let context = request.context().clone();
        let request_id = context.global_request_id.clone();
        let physical_retry = match self.pending_requests.get(&request_id) {
            Some(existing) if existing != &request => {
                return Err(SimExecutionContractErrorV1::RequestIdentityConflict(
                    request_id,
                ));
            }
            Some(_) => true,
            None => false,
        };
        if !physical_retry {
            self.require_new_ingress_open()?;
        }
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request)
            && let Some(existing) = self.mutation_payloads.get(&idempotency_key)
            && existing != &payload_sha256
        {
            return Err(SimExecutionContractErrorV1::MutationIdentityConflict(
                idempotency_key,
            ));
        }
        if !physical_retry {
            self.session.require_input_not_failed()?;
        }
        if !physical_retry && !self.accepting_new_ingress {
            return Err(SimExecutionContractErrorV1::ResponseBridgeClosed);
        }

        for action in &mut actions {
            action.action_identity = format!(
                "{}:{}:{}",
                request.method_tag(),
                request_id,
                action.batch_item_ordinal
            );
        }
        let canonical_request_sha256 = canonical_request_sha256(&request);
        let response_method = request.direct_response_method();
        let response_part_count = actions.len();
        let mut working_session = self.session.clone();
        let mut working_outbox = self.direct_response_outbox.clone();
        let mut working_query_state = self.query_state.clone();
        if !physical_retry {
            working_query_state.bind_continuation_for_request(&request_id, &request)?;
            working_outbox.reserve(
                &context.session_id,
                &request_id,
                response_method,
                response_part_count,
            )?;
        }
        let schedule = working_session.register_and_schedule_request(
            context,
            canonical_request_sha256,
            actions,
        )?;
        if !physical_retry {
            working_session.expect_direct_response(&request_id)?;
            let (candidate_condition_ids, candidate_token_ids) = request.candidate_market_use();
            working_session
                .record_successful_business_request(candidate_condition_ids, candidate_token_ids);
        }

        let mut working_pending_requests = self.pending_requests.clone();
        working_pending_requests
            .entry(request_id.clone())
            .or_insert(request.clone());
        let mut working_mutation_payloads = self.mutation_payloads.clone();
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request) {
            working_mutation_payloads
                .entry(idempotency_key)
                .or_insert(payload_sha256);
        }
        let candidate_state = ExecutionSessionStateV1 {
            session: working_session,
            pending_requests: working_pending_requests,
            mutation_payloads: working_mutation_payloads,
            submission_effect_records: self.submission_effect_records.clone(),
            cancel_effect_records: self.cancel_effect_records.clone(),
            query_state: working_query_state,
            direct_response_outbox: working_outbox,
            lifecycle_outbox: self.lifecycle_outbox.clone(),
            compacted_confirmed_private_deliveries: self
                .compacted_confirmed_private_deliveries
                .clone(),
            confirmed_private_deliveries: self.confirmed_private_deliveries.clone(),
            accepting_new_ingress: self.accepting_new_ingress,
        };
        let candidate_checkpoint =
            candidate_state.checkpoint(self.downstream_durable_confirmation.clone());
        let evidence =
            DurableTransactionEvidenceV1::new(DurableTransactionFactV1::RequestRegistered {
                session_id: self.session.session_spec().session_id.clone(),
                registration: DurableRequestRegistrationV1 {
                    request: request.clone(),
                    transit: schedule.request.transit.clone(),
                    actions: schedule.actions.clone(),
                },
            });
        if physical_retry {
            self.commit_durable_transition(&expected_checkpoint, candidate_checkpoint, evidence)?;
        } else {
            self.commit_durable_transition_new_ingress(
                &expected_checkpoint,
                candidate_checkpoint,
                evidence,
            )?;
        }
        self.replace_runtime_state(candidate_state);
        Ok(schedule)
    }
}

fn validate_action_mapping(
    request: &PendingDomainRequestV1,
    action: &QueuedLogicalActionV1,
) -> Result<(), SubmissionProcessingErrorV1> {
    let expected = format!(
        "{}:{}:{}",
        request.method_tag(),
        action.global_request_id,
        action.key.batch_item_ordinal
    );
    if action.action_identity != expected {
        return Err(SubmissionProcessingErrorV1::InternalInvariant(
            "逻辑动作与领域请求映射冲突",
        ));
    }
    Ok(())
}

fn cancel_target_groups(
    request: &PendingDomainRequestV1,
) -> Result<Vec<CancelTargetInputGroupV1>, SubmissionProcessingErrorV1> {
    match request {
        PendingDomainRequestV1::CancelOrder(request) => Ok(vec![CancelTargetInputGroupV1 {
            platform_order_id: request.item.platform_order_id.clone(),
            unique_target_ordinal: 0,
            input_item_refs: vec![CancelInputItemRefV1 {
                item_id: request.item.item_id.clone(),
                input_item_ordinal: 0,
            }],
        }]),
        PendingDomainRequestV1::CancelOrders(request) => {
            let mut group_index_by_order_id = BTreeMap::<String, usize>::new();
            let mut groups = Vec::<CancelTargetInputGroupV1>::new();
            for (input_ordinal, item) in request.items.iter().enumerate() {
                let input_ordinal = u16::try_from(input_ordinal).map_err(|_| {
                    SubmissionProcessingErrorV1::InternalInvariant("批量撤单输入序号超出首版边界")
                })?;
                if let Some(index) = group_index_by_order_id
                    .get(&item.platform_order_id)
                    .copied()
                {
                    groups[index].input_item_refs.push(CancelInputItemRefV1 {
                        item_id: item.item_id.clone(),
                        input_item_ordinal: input_ordinal,
                    });
                } else {
                    let unique_target_ordinal = u16::try_from(groups.len()).map_err(|_| {
                        SubmissionProcessingErrorV1::InternalInvariant(
                            "批量撤单唯一目标序号超出首版边界",
                        )
                    })?;
                    group_index_by_order_id.insert(item.platform_order_id.clone(), groups.len());
                    groups.push(CancelTargetInputGroupV1 {
                        platform_order_id: item.platform_order_id.clone(),
                        unique_target_ordinal,
                        input_item_refs: vec![CancelInputItemRefV1 {
                            item_id: item.item_id.clone(),
                            input_item_ordinal: input_ordinal,
                        }],
                    });
                }
            }
            Ok(groups)
        }
        _ => Err(SubmissionProcessingErrorV1::InternalInvariant(
            "非单撤或批撤请求没有目标分组",
        )),
    }
}

fn cancel_target_group_for_action(
    request: &PendingDomainRequestV1,
    action: &QueuedLogicalActionV1,
) -> Result<CancelTargetInputGroupV1, SubmissionProcessingErrorV1> {
    cancel_target_groups(request)?
        .into_iter()
        .find(|group| group.unique_target_ordinal == action.key.batch_item_ordinal)
        .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
            "撤单动作没有对应唯一目标",
        ))
}

fn project_order(order: &SimOrderRecordV1) -> Result<OrderProjectionV1, MatchingMathErrorV1> {
    let (kind, expires_at_utc_ms) = match order.kind {
        SimOrderKindStateV1::Gtc => (OrderKindProjectionV1::Gtc, None),
        SimOrderKindStateV1::Gtd { expires_at_utc_ms } => {
            (OrderKindProjectionV1::Gtd, Some(expires_at_utc_ms))
        }
        SimOrderKindStateV1::Fok => (OrderKindProjectionV1::Fok, None),
        SimOrderKindStateV1::Fak => (OrderKindProjectionV1::Fak, None),
    };
    let active = order
        .pending_maker_depletion_base_units
        .checked_add(order.open_base_units)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let status = if active > 0 {
        simulated_order_status("LIVE")
    } else if order.market_resolved_canceled_base_units > 0 {
        simulated_order_status("ORDER_STATUS_CANCELED_MARKET_RESOLVED")
    } else if order.venue_dust_closed_base_units > 0 {
        simulated_order_status("VENUE_DUST_TERMINAL")
    } else if order.canceled_base_units > 0 {
        simulated_order_status("CANCELED")
    } else if order.expired_base_units > 0 {
        simulated_order_status("EXPIRED")
    } else {
        simulated_order_status("MATCHED")
    };
    Ok(OrderProjectionV1 {
        platform_order_id: order.platform_order_id.clone(),
        global_request_id: order.global_request_id.clone(),
        item_id: order.item_id.clone(),
        condition_id: ConditionId(order.condition_id.clone()),
        token_id: TokenId(order.token_id.clone()),
        created_at_utc_ms: order.created_at_utc_ms,
        last_updated_at_utc_ms: order.last_updated_at_utc_ms,
        kind,
        expires_at_utc_ms,
        source_price_pusd_per_share: order.source_price_pusd_per_share,
        status,
        accepted_base_quantity_shares: order
            .accepted_base_units
            .map(decimal_from_micro_units_v1)
            .transpose()?,
        matched_base_quantity_shares: decimal_from_micro_units_v1(order.matched_base_units)?,
        pending_maker_depletion_base_quantity_shares: decimal_from_micro_units_v1(
            order.pending_maker_depletion_base_units,
        )?,
        open_base_quantity_shares: decimal_from_micro_units_v1(order.open_base_units)?,
        venue_dust_closed_base_quantity_shares: decimal_from_micro_units_v1(
            order.venue_dust_closed_base_units,
        )?,
        market_resolved_canceled_base_quantity_shares: decimal_from_micro_units_v1(
            order.market_resolved_canceled_base_units,
        )?,
        canceled_base_quantity_shares: decimal_from_micro_units_v1(order.canceled_base_units)?,
        expired_base_quantity_shares: decimal_from_micro_units_v1(order.expired_base_units)?,
        accepted_quote_amount_pusd: decimal_from_micro_units_v1(order.accepted_quote_units)?,
        matched_quote_amount_pusd: decimal_from_micro_units_v1(order.matched_quote_units)?,
        open_encumbered_quote_amount_pusd: decimal_from_micro_units_v1(order.open_quote_units)?,
        venue_dust_released_quote_amount_pusd: decimal_from_micro_units_v1(
            order.venue_dust_released_quote_units,
        )?,
        market_resolved_released_quote_amount_pusd: decimal_from_micro_units_v1(
            order.market_resolved_released_quote_units,
        )?,
        released_price_improvement_quote_amount_pusd: decimal_from_micro_units_v1(
            order.released_quote_units,
        )?,
        canceled_released_quote_amount_pusd: decimal_from_micro_units_v1(
            order.canceled_released_quote_units,
        )?,
        expired_released_quote_amount_pusd: decimal_from_micro_units_v1(
            order.expired_released_quote_units,
        )?,
        immediate_taker_fee_encumbered_pusd: decimal_from_micro_units_v1(
            order.immediate_taker_fee_encumbered_units,
        )?,
    })
}

fn project_trade(fill: &SimFillRecordV1) -> Result<TradeProjectionV1, MatchingMathErrorV1> {
    let finality_state = fill
        .finality_state()
        .expect("已提交成交必须已原子实例化最终性首态");
    Ok(TradeProjectionV1 {
        simulated_trade_id: fill.trade_id.clone(),
        platform_order_id: fill.platform_order_id.clone(),
        condition_id: ConditionId(fill.condition_id.clone()),
        token_id: TokenId(fill.token_id.clone()),
        matched_at_utc_ms: fill.matched_at_utc_ms,
        status: simulated_trade_status(trade_finality_state_identity(finality_state)),
        liquidity_role: match fill.liquidity_role {
            SimLiquidityRoleV1::Taker => TradeLiquidityRoleV1::Taker,
            SimLiquidityRoleV1::MakerInferred => TradeLiquidityRoleV1::MakerInferred,
        },
        source_price_pusd_per_share: fill.price_pusd_per_share,
        base_quantity_shares: decimal_from_micro_units_v1(fill.base_units)?,
        quote_amount_pusd: decimal_from_micro_units_v1(fill.quote_units)?,
        immediate_taker_fee_pusd: decimal_from_micro_units_v1(fill.immediate_taker_fee_units)?,
        economic_disposition: project_trade_economic_disposition(fill.economic_disposition()),
    })
}

fn project_trade_economic_disposition(
    disposition: TradeEconomicDispositionStateV1,
) -> TradeEconomicDispositionV1 {
    match disposition {
        TradeEconomicDispositionStateV1::PendingUnapplied => {
            TradeEconomicDispositionV1::PendingUnapplied
        }
        TradeEconomicDispositionStateV1::PendingApplied => {
            TradeEconomicDispositionV1::PendingApplied
        }
        TradeEconomicDispositionStateV1::FinalSuccess => TradeEconomicDispositionV1::FinalSuccess,
        TradeEconomicDispositionStateV1::FinalFailureUnapplied => {
            TradeEconomicDispositionV1::FinalFailureUnapplied
        }
        TradeEconomicDispositionStateV1::FinalFailureCompensated => {
            TradeEconomicDispositionV1::FinalFailureCompensated
        }
    }
}

fn trade_finality_state_identity(state: TradeFinalityStateV1) -> &'static str {
    match state {
        TradeFinalityStateV1::MatchedNotBroadcasted => "MATCHED_NOT_BROADCASTED",
        TradeFinalityStateV1::Matched => "MATCHED",
        TradeFinalityStateV1::Retrying => "RETRYING",
        TradeFinalityStateV1::Mined => "MINED",
        TradeFinalityStateV1::Confirmed => "CONFIRMED",
        TradeFinalityStateV1::Failed => "FAILED",
    }
}

fn simulated_order_status(raw: &str) -> SimulatedOrderStatusV1 {
    let known = match raw {
        "LIVE" => Some(SimulatedOrderKnownStatusV1::Live),
        "MATCHED" => Some(SimulatedOrderKnownStatusV1::Matched),
        "VENUE_DUST_TERMINAL" => Some(SimulatedOrderKnownStatusV1::VenueDustTerminal),
        "ORDER_STATUS_CANCELED_MARKET_RESOLVED" => {
            Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
        }
        "CANCELED" => Some(SimulatedOrderKnownStatusV1::Canceled),
        "EXPIRED" => Some(SimulatedOrderKnownStatusV1::Expired),
        "DELAYED" => Some(SimulatedOrderKnownStatusV1::Delayed),
        "UNMATCHED" => Some(SimulatedOrderKnownStatusV1::Unmatched),
        _ => None,
    };
    SimulatedOrderStatusV1 {
        raw: raw.to_owned(),
        known,
    }
}

fn simulated_trade_status(raw: &str) -> SimulatedTradeStatusV1 {
    let known = match raw {
        "MATCHED_NOT_BROADCASTED" => Some(SimulatedTradeKnownStatusV1::MatchedNotBroadcasted),
        "MATCHED" => Some(SimulatedTradeKnownStatusV1::Matched),
        "MINED" => Some(SimulatedTradeKnownStatusV1::Mined),
        "CONFIRMED" => Some(SimulatedTradeKnownStatusV1::Confirmed),
        "RETRYING" => Some(SimulatedTradeKnownStatusV1::Retrying),
        "FAILED" => Some(SimulatedTradeKnownStatusV1::Failed),
        _ => None,
    };
    SimulatedTradeStatusV1 {
        raw: raw.to_owned(),
        known,
    }
}

fn strict_millisecond_interval(filter: &TradesFilterV1) -> Option<(i64, i64)> {
    let first_inclusive = match filter.after_utc_ms {
        Some(after) => after.checked_add(1)?,
        None => 0,
    };
    let last_inclusive = match filter.before_utc_ms {
        Some(before) => before.checked_sub(1)?,
        None => i64::MAX,
    };
    (first_inclusive <= last_inclusive).then_some((first_inclusive, last_inclusive))
}

fn map_submission_rejection(value: SubmissionAdmissionRejectionV1) -> SubmissionRejectedReasonV1 {
    match value {
        SubmissionAdmissionRejectionV1::EndAtReached => SubmissionRejectedReasonV1::EndAtReached,
        SubmissionAdmissionRejectionV1::ExplicitTradingStateVeto => {
            SubmissionRejectedReasonV1::ExplicitTradingStateVeto
        }
        SubmissionAdmissionRejectionV1::MarketNotInScenario => {
            SubmissionRejectedReasonV1::MarketNotInScenario
        }
        SubmissionAdmissionRejectionV1::AssetNotInMarket => {
            SubmissionRejectedReasonV1::AssetNotInMarket
        }
        SubmissionAdmissionRejectionV1::OrderEncodingRejected => {
            SubmissionRejectedReasonV1::OrderEncodingRejected
        }
        SubmissionAdmissionRejectionV1::BelowMinimumShares => {
            SubmissionRejectedReasonV1::BelowMinimumShares
        }
        SubmissionAdmissionRejectionV1::PostOnlyWouldTake => {
            SubmissionRejectedReasonV1::PostOnlyWouldTake
        }
        SubmissionAdmissionRejectionV1::BelowMarketableLimitBuyMinimumPrincipal => {
            SubmissionRejectedReasonV1::BelowMarketableLimitBuyMinimumPrincipal
        }
        SubmissionAdmissionRejectionV1::GtdExpirationTooSoon => {
            SubmissionRejectedReasonV1::GtdExpirationTooSoon
        }
    }
}

fn map_submission_evidence(
    value: SubmissionEvidenceUnknownV1,
) -> SubmissionEvidenceUnknownReasonV1 {
    match value {
        SubmissionEvidenceUnknownV1::Preloading => SubmissionEvidenceUnknownReasonV1::Preloading,
        SubmissionEvidenceUnknownV1::TargetAssetNotFresh(quality) => {
            SubmissionEvidenceUnknownReasonV1::TargetAssetNotFresh(quality)
        }
    }
}

fn stable_order_id(spec: &SessionSpecV2, effect_identity: &str) -> String {
    let mut hash = CanonicalHasher::new("SIM_PLATFORM_ORDER_ID_V1");
    hash.text(&spec.session_id);
    hash.text(&spec.account_id);
    hash.text(effect_identity);
    hash.finish().0
}

fn submission_item_idempotency_identity(
    spec: &SessionSpecV2,
    idempotency_key: &str,
    batch_item_ordinal: u16,
) -> String {
    let mut hash = CanonicalHasher::new("SIM_SUBMISSION_ITEM_IDEMPOTENCY_V1");
    hash.text(&spec.session_id);
    hash.text(&spec.account_id);
    hash.text(idempotency_key);
    hash.u64(u64::from(batch_item_ordinal));
    hash.finish().0
}

fn submission_item_effect_identity(
    item_idempotency_identity: &str,
    payload_sha256: &Sha256Hex,
) -> String {
    let mut hash = CanonicalHasher::new("SIM_SUBMISSION_ITEM_EFFECT_V1");
    hash.text(item_idempotency_identity);
    hash.text(&payload_sha256.0);
    hash.finish().0
}

fn cancel_request_idempotency_identity(spec: &SessionSpecV2, idempotency_key: &str) -> String {
    let mut hash = CanonicalHasher::new("SIM_CANCEL_REQUEST_IDEMPOTENCY_V1");
    hash.text(&spec.session_id);
    hash.text(&spec.account_id);
    hash.text(idempotency_key);
    hash.finish().0
}

fn cancel_item_effect_identity(
    item_idempotency_identity: &str,
    payload_sha256: &Sha256Hex,
) -> String {
    let mut hash = CanonicalHasher::new("SIM_CANCEL_ITEM_EFFECT_V1");
    hash.text(item_idempotency_identity);
    hash.text(&payload_sha256.0);
    hash.finish().0
}

fn cancel_target_effect_identity(
    root_effect_identity: &str,
    platform_order_id: &str,
    target_ordinal: u64,
) -> String {
    let mut hash = CanonicalHasher::new("SIM_CANCEL_TARGET_EFFECT_V1");
    hash.text(root_effect_identity);
    hash.text(platform_order_id);
    hash.u64(target_ordinal);
    hash.finish().0
}

fn cancel_target_outcome(
    disposition: CancelOrderStateDispositionV1,
    order: Option<&SimOrderRecordV1>,
) -> Result<
    (CancelTargetOutcomeV1, Option<OrderLifecycleFeedbackSeedV1>),
    SubmissionProcessingErrorV1,
> {
    Ok(match disposition {
        CancelOrderStateDispositionV1::NotFound => (
            CancelTargetOutcomeV1::NotCanceledNoEffect(
                CancelNotCanceledReasonV1::NotFoundOrNotOwned,
            ),
            None,
        ),
        CancelOrderStateDispositionV1::NoCancellableRemainder => (
            CancelTargetOutcomeV1::NotCanceledNoEffect(
                CancelNotCanceledReasonV1::NoCancellableRemainder,
            ),
            None,
        ),
        CancelOrderStateDispositionV1::Canceled(effect) => {
            let order = order.ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                "成功撤单缺少原订单证据",
            ))?;
            let account_delta = effect
                .account_delta
                .as_ref()
                .map(|delta| AccountDeltaReferenceV1::from(delta.as_ref()));
            let feedback = effect
                .account_delta
                .map(|account_delta| OrderLifecycleFeedbackSeedV1 {
                    platform_order_id: order.platform_order_id.clone(),
                    condition_id: order.condition_id.clone(),
                    token_id: order.token_id.clone(),
                    event_kind: OrderLifecycleEventKindV1::CallerCanceled,
                    order_status: simulated_order_status("CANCELED"),
                    venue_dust_closed_base_units: 0,
                    venue_dust_released_quote_units: 0,
                    market_resolved_canceled_base_units: 0,
                    market_resolved_released_quote_units: 0,
                    account_delta,
                });
            (
                CancelTargetOutcomeV1::Canceled(CanceledTargetMeasurementV1 {
                    canceled_base_quantity_shares: decimal_from_micro_units_v1(
                        effect
                            .pending_base_units_before
                            .checked_add(effect.open_base_units_before)
                            .ok_or(SubmissionProcessingErrorV1::InternalInvariant(
                                "撤单 base 数量溢出",
                            ))?,
                    )?,
                    released_quote_amount_pusd: decimal_from_micro_units_v1(
                        effect.released_open_quote_units,
                    )?,
                    account_delta,
                }),
                feedback,
            )
        }
    })
}

fn allocate_immediate_taker_fee(
    session: &SimSession,
    fills: &mut [SimFillRecordV1],
) -> Result<u128, SubmissionProcessingErrorV1> {
    let rules = session.rule_snapshot().synchronized_rules();
    if !rules.fees_enabled || fills.is_empty() {
        return Ok(0);
    }
    let fee_inputs = fills
        .iter()
        .map(|fill| {
            (
                fill.trade_id.as_str(),
                fill.base_units,
                fill.price_pusd_per_share,
            )
        })
        .collect::<Vec<_>>();
    let (total_units, allocations) = allocate_taker_fee_micro_units_v1(
        &fee_inputs,
        rules.fee_schedule.rate.value(),
        rules.fee_schedule.exponent,
    )?;
    for (fill, fee_units) in fills.iter_mut().zip(allocations) {
        fill.immediate_taker_fee_units = fee_units;
    }
    Ok(total_units)
}

fn taker_fill_records(
    platform_order_id: &str,
    condition_id: &str,
    token_id: &str,
    matched_at_utc_ms: i64,
    plan: &QueueMatchPlanV1,
) -> Result<Vec<SimFillRecordV1>, SubmissionProcessingErrorV1> {
    let mut cumulative_base = 0_u128;
    let mut records = Vec::with_capacity(plan.fills.len());
    for fill in &plan.fills {
        let trade_id = stable_fill_id(
            "TAKER",
            platform_order_id,
            &fill.segment_id.0,
            cumulative_base,
        );
        records.push(SimFillRecordV1 {
            trade_id,
            platform_order_id: platform_order_id.to_owned(),
            condition_id: condition_id.to_owned(),
            token_id: token_id.to_owned(),
            matched_at_utc_ms,
            liquidity_role: SimLiquidityRoleV1::Taker,
            source_identity: fill.segment_id.0.clone(),
            maker_depletion_evidence: None,
            price_pusd_per_share: fill.price_pusd_per_share,
            base_units: fill.base_units,
            quote_units: fill.quote_units,
            immediate_taker_fee_units: 0,
            finality_path: None,
            finality_path_ordinal: None,
            finality_state: None,
            economic_application_delta: None,
        });
        cumulative_base = cumulative_base.checked_add(fill.base_units).ok_or(
            SubmissionProcessingErrorV1::InternalInvariant("成交 base 累计溢出"),
        )?;
    }
    Ok(records)
}

fn accepted_submission_item(
    order: &SimOrderRecordV1,
    fills: &[SimFillRecordV1],
    account_deltas: Vec<AccountDeltaReferenceV1>,
) -> Result<AcceptedSubmissionItemV1, SubmissionProcessingErrorV1> {
    let fills = fills
        .iter()
        .map(|fill| {
            Ok(SubmissionFillV1 {
                simulated_trade_id: fill.trade_id.clone(),
                source_price_pusd_per_share: fill.price_pusd_per_share,
                base_quantity_shares: decimal_from_micro_units_v1(fill.base_units)?,
                quote_amount_pusd: decimal_from_micro_units_v1(fill.quote_units)?,
            })
        })
        .collect::<Result<Vec<_>, MatchingMathErrorV1>>()?;
    let terminal_reason = match order.terminal_reason {
        Some(SimOrderTerminalReasonV1::FakCompleted) => {
            Some(SubmissionTerminalReasonV1::FakCompleted)
        }
        Some(SimOrderTerminalReasonV1::VenueDustTerminal) => {
            Some(SubmissionTerminalReasonV1::VenueDustTerminal)
        }
        Some(SimOrderTerminalReasonV1::MarketResolvedCanceled) => {
            return Err(SubmissionProcessingErrorV1::InternalInvariant(
                "新订单提交响应不得已处于判定关闭终态",
            ));
        }
        None => None,
    };
    let measurement = if let Some(accepted_base_units) = order.accepted_base_units {
        SubmissionMeasurementV1::Base(BaseSubmissionMeasurementV1 {
            accepted_base_quantity_shares: decimal_from_micro_units_v1(accepted_base_units)?,
            matched_base_quantity_shares: decimal_from_micro_units_v1(order.matched_base_units)?,
            open_base_quantity_shares: decimal_from_micro_units_v1(order.open_base_units)?,
            venue_dust_closed_base_quantity_shares: decimal_from_micro_units_v1(
                order.venue_dust_closed_base_units,
            )?,
            order_type_terminated_base_quantity_shares: Decimal::ZERO,
            accepted_quote_amount_pusd: decimal_from_micro_units_v1(order.accepted_quote_units)?,
            matched_quote_amount_pusd: decimal_from_micro_units_v1(order.matched_quote_units)?,
            open_encumbered_quote_amount_pusd: decimal_from_micro_units_v1(order.open_quote_units)?,
            venue_dust_released_quote_amount_pusd: decimal_from_micro_units_v1(
                order.venue_dust_released_quote_units,
            )?,
            released_price_improvement_quote_amount_pusd: decimal_from_micro_units_v1(
                order.released_quote_units,
            )?,
            immediate_taker_fee_encumbered_pusd: decimal_from_micro_units_v1(
                order.immediate_taker_fee_encumbered_units,
            )?,
        })
    } else {
        SubmissionMeasurementV1::QuoteBudget(QuoteBudgetSubmissionMeasurementV1 {
            accepted_quote_budget_pusd: decimal_from_micro_units_v1(order.accepted_quote_units)?,
            used_quote_amount_pusd: decimal_from_micro_units_v1(order.matched_quote_units)?,
            released_or_unspent_quote_amount_pusd: decimal_from_micro_units_v1(
                order.released_quote_units,
            )?,
            actual_matched_base_quantity_shares: decimal_from_micro_units_v1(
                order.matched_base_units,
            )?,
            sdk_taker_amount_audit_shares: decimal_from_micro_units_v1(
                order.sdk_taker_amount_audit_base_units.ok_or(
                    SubmissionProcessingErrorV1::InternalInvariant(
                        "quote 预算订单缺少 SDK base 审计投影",
                    ),
                )?,
            )?,
            immediate_taker_fee_encumbered_pusd: decimal_from_micro_units_v1(
                order.immediate_taker_fee_encumbered_units,
            )?,
        })
    };
    Ok(AcceptedSubmissionItemV1 {
        simulated_platform_order_id: order.platform_order_id.clone(),
        platform_order_status: if order.venue_dust_closed_base_units > 0 {
            SubmissionPlatformOrderStatusV1::VenueDustTerminal
        } else if order.open_base_units > 0 {
            SubmissionPlatformOrderStatusV1::Live
        } else if order.matched_base_units > 0 {
            SubmissionPlatformOrderStatusV1::Matched
        } else {
            return Err(SubmissionProcessingErrorV1::InternalInvariant(
                "无开放量、无成交量且无订单类型终态的订单状态非法",
            ));
        },
        fills,
        terminal_reason,
        measurement,
        account_deltas,
    })
}

fn fee_rule_identity(session: &SimSession) -> SubmissionFeeRuleIdentityV1 {
    let rule = &session
        .rule_snapshot()
        .snapshot()
        .documented_platform_rules
        .fee_computation
        .value;
    SubmissionFeeRuleIdentityV1 {
        formula_version: rule.formula_version.clone(),
        aggregation_version: SUBMISSION_FEE_AGGREGATION_V1.to_owned(),
        precision_decimal_places: rule.precision_decimal_places,
        minimum_nonzero_fee_pusd: rule.minimum_nonzero_fee_pusd.value(),
        builder_fee_pusd: Decimal::ZERO,
    }
}

enum ScheduledLifecycleFeedbackV1 {
    Order(OrderLifecycleFeedbackSeedV1),
    Trade(TradeFinalityTransitionEffectV1),
    Settlement(SettlementLifecycleFeedbackSeedV1),
}

impl ScheduledLifecycleFeedbackV1 {
    fn platform_effective_at_utc_ms(&self) -> i64 {
        match self {
            Self::Order(feedback) => feedback.account_delta.platform_effective_at_utc_ms,
            Self::Trade(feedback) => feedback.platform_event_at_utc_ms,
            Self::Settlement(feedback) => feedback.platform_event_at_utc_ms,
        }
    }
}

fn drive_scheduled_platform_work_through(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    logical_time_utc_ms: i64,
    preferred_global_request_id: Option<&str>,
) -> Result<bool, LogicalPlatformDriveErrorV1> {
    let mut execution = session.execution_state().clone();
    let mut first_due_at = None;
    let mut lifecycle_feedbacks = Vec::new();
    let mut redeemed_resolutions = Vec::new();
    loop {
        let next_gtd = execution
            .next_gtd_deadline_at_utc_ms()
            .filter(|at| *at <= logical_time_utc_ms);
        let next_finality = execution
            .next_scheduled_finality_at_utc_ms()
            .filter(|at| *at <= logical_time_utc_ms);
        let next_rebate = execution
            .next_rebate_clock_at_utc_ms()
            .filter(|at| *at <= logical_time_utc_ms);
        let Some(next_at) = [next_gtd, next_finality, next_rebate]
            .into_iter()
            .flatten()
            .min()
        else {
            break;
        };
        first_due_at.get_or_insert(next_at);
        if next_gtd == Some(next_at) {
            lifecycle_feedbacks.extend(
                execution
                    .advance_gtd_deadlines_through(next_at)?
                    .into_iter()
                    .filter_map(|effect| {
                        effect.account_delta.map(|account_delta| {
                            ScheduledLifecycleFeedbackV1::Order(OrderLifecycleFeedbackSeedV1 {
                                platform_order_id: effect.platform_order_id,
                                condition_id: effect.condition_id,
                                token_id: effect.token_id,
                                event_kind: OrderLifecycleEventKindV1::GtdExpired,
                                order_status: simulated_order_status("EXPIRED"),
                                venue_dust_closed_base_units: 0,
                                venue_dust_released_quote_units: 0,
                                market_resolved_canceled_base_units: 0,
                                market_resolved_released_quote_units: 0,
                                account_delta,
                            })
                        })
                    }),
            );
        }
        if next_finality == Some(next_at) {
            for effect in execution.advance_finality_at(next_at)? {
                lifecycle_feedbacks
                    .push(ScheduledLifecycleFeedbackV1::Trade(effect.trade_feedback));
                for settlement in effect.settlement_feedbacks {
                    if let Some((condition_id, resolution)) =
                        settlement.action.redeemed_resolution()
                    {
                        redeemed_resolutions.push((condition_id.to_owned(), resolution.clone()));
                    }
                    lifecycle_feedbacks.push(ScheduledLifecycleFeedbackV1::Settlement(
                        settlement_lifecycle_feedback_seed(settlement)?,
                    ));
                }
            }
        }
        if next_rebate == Some(next_at) {
            for settlement in execution.advance_rebate_clock_at(next_at)? {
                if let Some((condition_id, resolution)) = settlement.action.redeemed_resolution() {
                    redeemed_resolutions.push((condition_id.to_owned(), resolution.clone()));
                }
                lifecycle_feedbacks.push(ScheduledLifecycleFeedbackV1::Settlement(
                    settlement_lifecycle_feedback_seed(settlement)?,
                ));
            }
        }
    }
    let trailing_gtd = execution.advance_gtd_deadlines_through(logical_time_utc_ms)?;
    if !trailing_gtd.is_empty() {
        return Err(
            MatchingStateErrorV1::Finality("统一驱动遗漏了截止时点内的 GTD".to_owned()).into(),
        );
    }
    let query_projection_changed =
        execution.query_projection_changed_from(session.execution_state());
    let has_lifecycle_feedbacks = !lifecycle_feedbacks.is_empty();
    let reservation = lifecycle_feedbacks
        .iter()
        .map(ScheduledLifecycleFeedbackV1::platform_effective_at_utc_ms)
        .max()
        .map(|latest_feedback_at| {
            outbox.reserve_platform_work(
                &format!(
                    "SCHEDULED:{}:{latest_feedback_at}:{logical_time_utc_ms}",
                    first_due_at.expect("生命周期反馈必须来自已排定平台工作")
                ),
                preferred_global_request_id,
                latest_feedback_at,
            )
        })
        .transpose()?;
    if let Some(reservation) = reservation.as_ref() {
        for feedback in lifecycle_feedbacks {
            match feedback {
                ScheduledLifecycleFeedbackV1::Order(feedback) => {
                    record_order_lifecycle_feedbacks_in_batch(
                        session,
                        outbox,
                        reservation,
                        vec![feedback],
                    )?;
                }
                ScheduledLifecycleFeedbackV1::Trade(feedback) => {
                    record_trade_lifecycle_feedback_in_batch(
                        session,
                        outbox,
                        reservation,
                        feedback,
                    )?;
                }
                ScheduledLifecycleFeedbackV1::Settlement(feedback) => {
                    record_settlement_lifecycle_feedbacks_in_batch(
                        session,
                        outbox,
                        reservation,
                        vec![feedback],
                    )?;
                }
            }
        }
    } else if has_lifecycle_feedbacks {
        return Err(SessionError::ResponseDeliveryIdentityConflict.into());
    }
    for (condition_id, resolution) in redeemed_resolutions {
        session.mark_redeemed_resolution_terminal(&condition_id, &resolution);
    }
    session.replace_execution_state(execution);
    if let Some(reservation) = reservation {
        outbox.complete_platform_work(&reservation)?;
    }
    Ok(query_projection_changed)
}

fn prepare_pending_terminal_state(
    session: &mut SimSession,
    query_state: &mut QueryStateV1,
) -> Result<(), SessionError> {
    query_state.prepare_pending_terminal_projections(session)
}

fn record_order_lifecycle_feedbacks(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    reservation: Option<&LifecycleBatchReservationV1>,
    feedbacks: Vec<OrderLifecycleFeedbackSeedV1>,
) -> Result<(), SessionError> {
    if feedbacks.is_empty() {
        return Ok(());
    }
    let reservation = reservation.ok_or(SessionError::ResponseDeliveryIdentityConflict)?;
    record_order_lifecycle_feedbacks_in_batch(session, outbox, reservation, feedbacks)
}

fn record_order_lifecycle_feedbacks_in_batch(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    reservation: &LifecycleBatchReservationV1,
    feedbacks: Vec<OrderLifecycleFeedbackSeedV1>,
) -> Result<(), SessionError> {
    for feedback in feedbacks {
        let mut feedback_id_hash = CanonicalHasher::new("SIM_ORDER_LIFECYCLE_FEEDBACK_ID_V1");
        feedback_id_hash.text(&feedback.account_delta.delta_id);
        feedback_id_hash.text(order_lifecycle_event_identity(feedback.event_kind));
        let feedback_id = feedback_id_hash.finish().0;
        let private_emission_sequence = session.next_private_emission_sequence()?;
        let timing = session.bind_lifecycle_feedback_timing(
            feedback_id.clone(),
            feedback.account_delta.platform_effective_at_utc_ms,
            private_emission_sequence,
        )?;
        let mut delivery_id_hash = CanonicalHasher::new("SIM_ORDER_LIFECYCLE_DELIVERY_ID_V1");
        delivery_id_hash.text(&session.session_spec().session_id);
        delivery_id_hash.text(&feedback_id);
        let delivery_id = delivery_id_hash.finish().0;
        outbox.record_for_batch(
            reservation,
            LifecycleFeedbackDraftV1::Order(OrderLifecycleFeedbackDraftV1 {
                feedback_id,
                delivery_id,
                account_instance_key: feedback.account_delta.account_instance_key.clone(),
                session_id: session.session_spec().session_id.clone(),
                timing,
                platform_order_id: feedback.platform_order_id,
                condition_id: ConditionId(feedback.condition_id),
                token_id: TokenId(feedback.token_id),
                event_kind: feedback.event_kind,
                order_status: feedback.order_status,
                venue_dust_closed_base_units: feedback.venue_dust_closed_base_units,
                venue_dust_released_quote_units: feedback.venue_dust_released_quote_units,
                market_resolved_canceled_base_units: feedback.market_resolved_canceled_base_units,
                market_resolved_released_quote_units: feedback.market_resolved_released_quote_units,
                account_delta: feedback.account_delta,
            }),
        )?;
    }
    Ok(())
}

fn record_initial_finality_lifecycle_feedbacks(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    reservation: Option<&LifecycleBatchReservationV1>,
    effects: Vec<TradeFinalityLifecycleEffectsV1>,
) -> Result<(), SessionError> {
    if effects.is_empty() {
        return Ok(());
    }
    let reservation = reservation.ok_or(SessionError::ResponseDeliveryIdentityConflict)?;
    for effect in effects {
        record_trade_lifecycle_feedback_in_batch(
            session,
            outbox,
            reservation,
            effect.trade_feedback,
        )?;
        let settlement_feedbacks = effect
            .settlement_feedbacks
            .into_iter()
            .map(settlement_lifecycle_feedback_seed)
            .collect::<Result<Vec<_>, _>>()?;
        record_settlement_lifecycle_feedbacks_in_batch(
            session,
            outbox,
            reservation,
            settlement_feedbacks,
        )?;
    }
    Ok(())
}

fn settlement_lifecycle_feedback_seed(
    effect: SettlementLifecycleEffectV1,
) -> Result<SettlementLifecycleFeedbackSeedV1, MatchingStateErrorV1> {
    let settlement_identity = effect.action.reason_identity().to_owned();
    let platform_event_at_utc_ms = effect.action.platform_effective_at_utc_ms();
    let event = match effect.action {
        SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::MakerMarketFinalized(
            value,
        )) => SettlementLifecycleEventDraftV1::MakerRebateAccrualFinalized {
            condition_id: ConditionId(value.condition_id),
            status: match value.status {
                MakerRebateMarketResultStatusV1::Calculated => {
                    MakerRebateAccrualStatusV1::Calculated
                }
                MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill => {
                    MakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill
                }
            },
            contributions: value
                .contributions
                .into_iter()
                .map(|contribution| {
                    Ok(MakerRebateFillContributionV1 {
                        simulated_trade_id: contribution.trade_id,
                        matched_at_utc_ms: contribution.matched_at_utc_ms,
                        attribution_day_start_utc_ms: contribution.attribution_day_utc_ms,
                        base_quantity_shares: decimal_from_micro_units_v1(contribution.base_units)?,
                        price_pusd_per_share: contribution.price_pusd_per_share,
                        maker_fee_equivalent_pusd: contribution.fee_equivalent_pusd,
                        rebate_rate: contribution.rebate_rate,
                        rebate_contribution_pusd: contribution.contribution_pusd,
                    })
                })
                .collect::<Result<Vec<_>, MatchingMathErrorV1>>()?,
            accrual_pusd: value.accrual_pusd,
            account_delta: effect.account_delta,
        },
        SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::TakerAccrualApplied(
            value,
        )) => SettlementLifecycleEventDraftV1::TakerRebateAccrualApplied {
            simulated_trade_id: value.trade_id,
            condition_id: ConditionId(value.condition_id),
            matched_at_utc_ms: value.matched_at_utc_ms,
            attribution_day_start_utc_ms: value.attribution_day_utc_ms,
            actual_taker_fee_pusd: value.actual_taker_fee_pusd,
            rebate_rate: value.rebate_rate,
            accrual_pusd: value.accrual_pusd,
            account_delta: effect.account_delta.ok_or_else(|| {
                MatchingStateErrorV1::Rebate("taker 返佣应计缺少规范账户变化".to_owned())
            })?,
        },
        SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::DayFinalized(value)) => {
            match value.kind {
                RebateKindV1::Maker => SettlementLifecycleEventDraftV1::MakerRebateDayFinalized {
                    attribution_day_start_utc_ms: value.attribution_day_utc_ms,
                    accrual_pusd: value.accrued_pusd,
                    payable_pusd: value.payable_pusd,
                    locked_remainder_pusd: value.locked_remainder_pusd,
                    account_delta: effect.account_delta,
                },
                RebateKindV1::Taker => SettlementLifecycleEventDraftV1::TakerRebateDayFinalized {
                    attribution_day_start_utc_ms: value.attribution_day_utc_ms,
                    accrual_pusd: value.accrued_pusd,
                    payable_pusd: value.payable_pusd,
                    locked_remainder_pusd: value.locked_remainder_pusd,
                    account_delta: effect.account_delta,
                },
            }
        }
        SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::Paid(value)) => {
            let account_delta = effect.account_delta.ok_or_else(|| {
                MatchingStateErrorV1::Rebate("返佣付款缺少规范账户变化".to_owned())
            })?;
            match value.kind {
                RebateKindV1::Maker => SettlementLifecycleEventDraftV1::MakerRebatePaid {
                    attribution_day_start_utc_ms: value.attribution_day_utc_ms,
                    paid_pusd: value.paid_pusd,
                    account_delta,
                },
                RebateKindV1::Taker => SettlementLifecycleEventDraftV1::TakerRebatePaid {
                    attribution_day_start_utc_ms: value.attribution_day_utc_ms,
                    paid_pusd: value.paid_pusd,
                    account_delta,
                },
            }
        }
        SettlementLifecycleActionV1::AutoRedeem(value) => {
            SettlementLifecycleEventDraftV1::AutoRedeemApplied {
                condition_id: ConditionId(value.condition_id),
                resolution_event_id: value.resolution_event_id,
                resolution_source_event_at_utc_ms: value.resolution_source_event_at_utc_ms,
                resolution: value.resolution,
                redeemed_outcome_balances: value.redeemed_outcome_balances,
                redeemed_pusd: value.redeemed_pusd,
                account_delta: effect.account_delta,
            }
        }
    };
    Ok(SettlementLifecycleFeedbackSeedV1 {
        settlement_identity,
        platform_event_at_utc_ms,
        event,
    })
}

fn record_trade_lifecycle_feedback_in_batch(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    reservation: &LifecycleBatchReservationV1,
    feedback: TradeFinalityTransitionEffectV1,
) -> Result<(), SessionError> {
    let account_instance_key = session.execution_state().account_instance_key().clone();
    let mut feedback_id_hash = CanonicalHasher::new("SIM_TRADE_LIFECYCLE_FEEDBACK_ID_V1");
    feedback_id_hash.text(&feedback.trade_id);
    feedback_id_hash.text(trade_finality_state_identity(feedback.finality_state));
    feedback_id_hash.i64(feedback.platform_event_at_utc_ms);
    let feedback_id = feedback_id_hash.finish().0;
    let private_emission_sequence = session.next_private_emission_sequence()?;
    let timing = session.bind_lifecycle_feedback_timing(
        feedback_id.clone(),
        feedback.platform_event_at_utc_ms,
        private_emission_sequence,
    )?;
    let mut delivery_id_hash = CanonicalHasher::new("SIM_TRADE_LIFECYCLE_DELIVERY_ID_V1");
    delivery_id_hash.text(&session.session_spec().session_id);
    delivery_id_hash.text(&feedback_id);
    let delivery_id = delivery_id_hash.finish().0;
    outbox.record_for_batch(
        reservation,
        LifecycleFeedbackDraftV1::Trade(TradeLifecycleFeedbackDraftV1 {
            feedback_id,
            delivery_id,
            account_instance_key,
            session_id: session.session_spec().session_id.clone(),
            timing,
            simulated_trade_id: feedback.trade_id,
            platform_order_id: feedback.platform_order_id,
            condition_id: ConditionId(feedback.condition_id),
            token_id: TokenId(feedback.token_id),
            status: simulated_trade_status(trade_finality_state_identity(feedback.finality_state)),
            economic_disposition: project_trade_economic_disposition(feedback.economic_disposition),
            liquidity_role: match feedback.liquidity_role {
                SimLiquidityRoleV1::Taker => TradeLiquidityRoleV1::Taker,
                SimLiquidityRoleV1::MakerInferred => TradeLiquidityRoleV1::MakerInferred,
            },
            base_units: feedback.base_units,
            quote_units: feedback.quote_units,
            immediate_taker_fee_units: feedback.immediate_taker_fee_units,
            economic_application_delta: feedback
                .economic_application_delta
                .as_deref()
                .map(AccountDeltaReferenceV1::from),
            account_delta: feedback.account_delta,
        }),
    )?;
    Ok(())
}

fn record_settlement_lifecycle_feedbacks_in_batch(
    session: &mut SimSession,
    outbox: &mut LogicalLifecycleOutboxV1,
    reservation: &LifecycleBatchReservationV1,
    feedbacks: Vec<SettlementLifecycleFeedbackSeedV1>,
) -> Result<(), SessionError> {
    for feedback in feedbacks {
        let mut feedback_id_hash = CanonicalHasher::new("SIM_SETTLEMENT_LIFECYCLE_FEEDBACK_ID_V1");
        feedback_id_hash.text(&feedback.settlement_identity);
        feedback_id_hash.i64(feedback.platform_event_at_utc_ms);
        let feedback_id = feedback_id_hash.finish().0;
        let private_emission_sequence = session.next_private_emission_sequence()?;
        let timing = session.bind_lifecycle_feedback_timing(
            feedback_id.clone(),
            feedback.platform_event_at_utc_ms,
            private_emission_sequence,
        )?;
        let mut delivery_id_hash = CanonicalHasher::new("SIM_SETTLEMENT_LIFECYCLE_DELIVERY_ID_V1");
        delivery_id_hash.text(&session.session_spec().session_id);
        delivery_id_hash.text(&feedback_id);
        let delivery_id = delivery_id_hash.finish().0;
        outbox.record_for_batch(
            reservation,
            LifecycleFeedbackDraftV1::Settlement(SettlementLifecycleFeedbackDraftV1 {
                feedback_id,
                delivery_id,
                account_instance_key: session.execution_state().account_instance_key().clone(),
                session_id: session.session_spec().session_id.clone(),
                timing,
                settlement_identity: feedback.settlement_identity,
                event: feedback.event,
            }),
        )?;
    }
    Ok(())
}

fn order_lifecycle_event_identity(event: OrderLifecycleEventKindV1) -> &'static str {
    match event {
        OrderLifecycleEventKindV1::OrderAccepted => "ORDER_ACCEPTED",
        OrderLifecycleEventKindV1::FakUnspentReleased => "FAK_UNSPENT_RELEASED",
        OrderLifecycleEventKindV1::VenueDustTerminal => "VENUE_DUST_TERMINAL",
        OrderLifecycleEventKindV1::MarketResolvedCanceled => "MARKET_RESOLVED_CANCELED",
        OrderLifecycleEventKindV1::CallerCanceled => "CALLER_CANCELED",
        OrderLifecycleEventKindV1::GtdExpired => "GTD_EXPIRED",
    }
}

fn seal_order_lifecycle_feedback(
    draft: &OrderLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Result<Arc<OrderLifecycleFeedbackV1>, SimExecutionContractErrorV1> {
    let crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } =
        &candidate.evidence
    else {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    };
    if feedback_id != &draft.feedback_id
        || candidate.received_at_utc_ms != draft.timing.feedback_received_at_utc_ms
        || candidate.private_emission_sequence != draft.timing.private_emission_sequence
    {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    }
    let payload_sha256 = order_lifecycle_payload_sha256(draft, candidate);
    Ok(Arc::new(OrderLifecycleFeedbackV1 {
        metadata: OrderLifecycleFeedbackMetadataV1 {
            schema_version: ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
            evidence_source: SIMULATED_EXECUTION_EVIDENCE_V1.to_owned(),
            account_instance_key: draft.account_instance_key.clone(),
            session_id: draft.session_id.clone(),
            delivery_id: draft.delivery_id.clone(),
            platform_event_at_utc_ms: draft.timing.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: draft.timing.feedback_received_at_utc_ms,
            private_emission_sequence: draft.timing.private_emission_sequence,
            private_feedback_sequence: candidate.private_feedback_sequence,
            payload_sha256,
        },
        platform_order_id: draft.platform_order_id.clone(),
        condition_id: draft.condition_id.clone(),
        token_id: draft.token_id.clone(),
        event_kind: draft.event_kind,
        order_status: draft.order_status.clone(),
        venue_dust_closed_base_quantity_shares: decimal_from_micro_units_v1(
            draft.venue_dust_closed_base_units,
        )
        .map_err(MatchingStateErrorV1::from)?,
        venue_dust_released_quote_amount_pusd: decimal_from_micro_units_v1(
            draft.venue_dust_released_quote_units,
        )
        .map_err(MatchingStateErrorV1::from)?,
        market_resolved_canceled_base_quantity_shares: decimal_from_micro_units_v1(
            draft.market_resolved_canceled_base_units,
        )
        .map_err(MatchingStateErrorV1::from)?,
        market_resolved_released_quote_amount_pusd: decimal_from_micro_units_v1(
            draft.market_resolved_released_quote_units,
        )
        .map_err(MatchingStateErrorV1::from)?,
        account_delta: draft.account_delta.as_ref().clone(),
    }))
}

fn order_lifecycle_payload_sha256(
    draft: &OrderLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_ORDER_LIFECYCLE_FEEDBACK_PAYLOAD_V1");
    hash.text(ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1);
    hash.text(SIMULATED_EXECUTION_EVIDENCE_V1);
    hash.text(&draft.account_instance_key.run_id);
    hash.text(match draft.account_instance_key.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&draft.account_instance_key.caller_instance_id);
    hash.text(&draft.account_instance_key.account_id);
    hash.text(match draft.account_instance_key.platform_source {
        PlatformSourceV1::Simulated => "SIMULATED",
    });
    hash.text(&draft.session_id);
    hash.text(&draft.delivery_id);
    hash.i64(draft.timing.platform_event_at_utc_ms);
    hash.i64(draft.timing.feedback_received_at_utc_ms);
    hash.u64(draft.timing.private_emission_sequence);
    hash.u64(candidate.private_feedback_sequence);
    hash.text(&draft.platform_order_id);
    hash.text(&draft.condition_id.0);
    hash.text(&draft.token_id.0);
    hash.text(order_lifecycle_event_identity(draft.event_kind));
    hash.text(&draft.order_status.raw);
    hash.optional_text(draft.order_status.known.map(|status| match status {
        SimulatedOrderKnownStatusV1::Live => "LIVE",
        SimulatedOrderKnownStatusV1::Matched => "MATCHED",
        SimulatedOrderKnownStatusV1::VenueDustTerminal => "VENUE_DUST_TERMINAL",
        SimulatedOrderKnownStatusV1::CanceledMarketResolved => {
            "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
        }
        SimulatedOrderKnownStatusV1::Canceled => "CANCELED",
        SimulatedOrderKnownStatusV1::Expired => "EXPIRED",
        SimulatedOrderKnownStatusV1::Delayed => "DELAYED",
        SimulatedOrderKnownStatusV1::Unmatched => "UNMATCHED",
    }));
    hash.u128(draft.venue_dust_closed_base_units);
    hash.u128(draft.venue_dust_released_quote_units);
    hash.u128(draft.market_resolved_canceled_base_units);
    hash.u128(draft.market_resolved_released_quote_units);
    hash.u64(draft.account_delta.account_transition_seq);
    hash.text(&draft.account_delta.delta_id);
    hash.text(&draft.account_delta.delta_sha256.0);
    hash.finish()
}

fn seal_trade_lifecycle_feedback(
    draft: &TradeLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Result<Arc<TradeLifecycleFeedbackV1>, SimExecutionContractErrorV1> {
    let crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } =
        &candidate.evidence
    else {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    };
    if feedback_id != &draft.feedback_id
        || candidate.received_at_utc_ms != draft.timing.feedback_received_at_utc_ms
        || candidate.private_emission_sequence != draft.timing.private_emission_sequence
    {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    }
    let base_quantity_shares = decimal_from_micro_units_v1(draft.base_units)
        .map_err(|error| SimExecutionContractErrorV1::ExecutionState(error.to_string()))?;
    let quote_amount_pusd = decimal_from_micro_units_v1(draft.quote_units)
        .map_err(|error| SimExecutionContractErrorV1::ExecutionState(error.to_string()))?;
    let immediate_taker_fee_pusd = decimal_from_micro_units_v1(draft.immediate_taker_fee_units)
        .map_err(|error| SimExecutionContractErrorV1::ExecutionState(error.to_string()))?;
    let payload_sha256 = trade_lifecycle_payload_sha256(draft, candidate);
    Ok(Arc::new(TradeLifecycleFeedbackV1 {
        metadata: TradeLifecycleFeedbackMetadataV1 {
            schema_version: TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
            evidence_source: SIMULATED_EXECUTION_EVIDENCE_V1.to_owned(),
            account_instance_key: draft.account_instance_key.clone(),
            session_id: draft.session_id.clone(),
            delivery_id: draft.delivery_id.clone(),
            platform_event_at_utc_ms: draft.timing.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: draft.timing.feedback_received_at_utc_ms,
            private_emission_sequence: draft.timing.private_emission_sequence,
            private_feedback_sequence: candidate.private_feedback_sequence,
            payload_sha256,
        },
        simulated_trade_id: draft.simulated_trade_id.clone(),
        platform_order_id: draft.platform_order_id.clone(),
        condition_id: draft.condition_id.clone(),
        token_id: draft.token_id.clone(),
        status: draft.status.clone(),
        economic_disposition: draft.economic_disposition,
        liquidity_role: draft.liquidity_role,
        base_quantity_shares,
        quote_amount_pusd,
        immediate_taker_fee_pusd,
        economic_application_delta: draft.economic_application_delta.clone(),
        account_delta: draft.account_delta.as_deref().cloned(),
    }))
}

fn trade_lifecycle_payload_sha256(
    draft: &TradeLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_TRADE_LIFECYCLE_FEEDBACK_PAYLOAD_V1");
    hash.text(TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1);
    hash.text(SIMULATED_EXECUTION_EVIDENCE_V1);
    hash.text(&draft.account_instance_key.run_id);
    hash.text(match draft.account_instance_key.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&draft.account_instance_key.caller_instance_id);
    hash.text(&draft.account_instance_key.account_id);
    hash.text(match draft.account_instance_key.platform_source {
        PlatformSourceV1::Simulated => "SIMULATED",
    });
    hash.text(&draft.session_id);
    hash.text(&draft.delivery_id);
    hash.i64(draft.timing.platform_event_at_utc_ms);
    hash.i64(draft.timing.feedback_received_at_utc_ms);
    hash.u64(draft.timing.private_emission_sequence);
    hash.u64(candidate.private_feedback_sequence);
    hash.text(&draft.simulated_trade_id);
    hash.text(&draft.platform_order_id);
    hash.text(&draft.condition_id.0);
    hash.text(&draft.token_id.0);
    hash.text(&draft.status.raw);
    hash.optional_text(draft.status.known.map(trade_status_identity));
    hash.text(trade_economic_disposition_identity(
        draft.economic_disposition,
    ));
    hash.text(match draft.liquidity_role {
        TradeLiquidityRoleV1::Taker => "TAKER",
        TradeLiquidityRoleV1::MakerInferred => "MAKER_INFERRED",
    });
    hash.u128(draft.base_units);
    hash.u128(draft.quote_units);
    hash.u128(draft.immediate_taker_fee_units);
    match &draft.economic_application_delta {
        Some(delta) => {
            hash.text("ECONOMIC_APPLICATION_DELTA");
            hash.u64(delta.account_transition_seq);
            hash.text(&delta.delta_id);
            hash.text(&delta.delta_sha256.0);
        }
        None => hash.text("NO_ECONOMIC_APPLICATION_DELTA"),
    }
    match &draft.account_delta {
        Some(delta) => {
            hash.text("ACCOUNT_DELTA");
            hash.u64(delta.account_transition_seq);
            hash.text(&delta.delta_id);
            hash.text(&delta.delta_sha256.0);
        }
        None => hash.text("NO_ACCOUNT_DELTA"),
    }
    hash.finish()
}

fn seal_settlement_lifecycle_feedback(
    draft: &SettlementLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Result<Arc<SettlementLifecycleFeedbackV1>, SimExecutionContractErrorV1> {
    let crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } =
        &candidate.evidence
    else {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    };
    if feedback_id != &draft.feedback_id
        || candidate.received_at_utc_ms != draft.timing.feedback_received_at_utc_ms
        || candidate.private_emission_sequence != draft.timing.private_emission_sequence
    {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    }
    let event = match &draft.event {
        SettlementLifecycleEventDraftV1::MakerRebateAccrualFinalized {
            condition_id,
            status,
            contributions,
            accrual_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
            condition_id: condition_id.clone(),
            status: *status,
            contributions: contributions.clone(),
            accrual_pusd: *accrual_pusd,
            account_delta: account_delta.as_deref().cloned(),
        },
        SettlementLifecycleEventDraftV1::TakerRebateAccrualApplied {
            simulated_trade_id,
            condition_id,
            matched_at_utc_ms,
            attribution_day_start_utc_ms,
            actual_taker_fee_pusd,
            rebate_rate,
            accrual_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::TakerRebateAccrualApplied {
            simulated_trade_id: simulated_trade_id.clone(),
            condition_id: condition_id.clone(),
            matched_at_utc_ms: *matched_at_utc_ms,
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            actual_taker_fee_pusd: *actual_taker_fee_pusd,
            rebate_rate: *rebate_rate,
            accrual_pusd: *accrual_pusd,
            account_delta: account_delta.as_ref().clone(),
        },
        SettlementLifecycleEventDraftV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            accrual_pusd: *accrual_pusd,
            payable_pusd: *payable_pusd,
            locked_remainder_pusd: *locked_remainder_pusd,
            account_delta: account_delta.as_deref().cloned(),
        },
        SettlementLifecycleEventDraftV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            accrual_pusd: *accrual_pusd,
            payable_pusd: *payable_pusd,
            locked_remainder_pusd: *locked_remainder_pusd,
            account_delta: account_delta.as_deref().cloned(),
        },
        SettlementLifecycleEventDraftV1::MakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::MakerRebatePaid {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            paid_pusd: *paid_pusd,
            account_delta: account_delta.as_ref().clone(),
        },
        SettlementLifecycleEventDraftV1::TakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::TakerRebatePaid {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            paid_pusd: *paid_pusd,
            account_delta: account_delta.as_ref().clone(),
        },
        SettlementLifecycleEventDraftV1::AutoRedeemApplied {
            condition_id,
            resolution_event_id,
            resolution_source_event_at_utc_ms,
            resolution,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
        } => SettlementLifecycleEventV1::AutoRedeemApplied {
            condition_id: condition_id.clone(),
            resolution_event_id: resolution_event_id.clone(),
            resolution_source_event_at_utc_ms: *resolution_source_event_at_utc_ms,
            resolution: resolution.clone(),
            redeemed_outcome_balances: redeemed_outcome_balances.clone(),
            redeemed_pusd: *redeemed_pusd,
            account_delta: account_delta.as_deref().cloned(),
        },
    };
    Ok(Arc::new(SettlementLifecycleFeedbackV1 {
        metadata: SettlementLifecycleFeedbackMetadataV1 {
            schema_version: SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1.to_owned(),
            evidence_source: SIMULATED_EXECUTION_EVIDENCE_V1.to_owned(),
            account_instance_key: draft.account_instance_key.clone(),
            session_id: draft.session_id.clone(),
            delivery_id: draft.delivery_id.clone(),
            platform_event_at_utc_ms: draft.timing.platform_event_at_utc_ms,
            feedback_received_at_utc_ms: draft.timing.feedback_received_at_utc_ms,
            private_emission_sequence: draft.timing.private_emission_sequence,
            private_feedback_sequence: candidate.private_feedback_sequence,
            payload_sha256: settlement_lifecycle_payload_sha256(draft, candidate),
        },
        settlement_identity: draft.settlement_identity.clone(),
        event,
    }))
}

fn settlement_lifecycle_payload_sha256(
    draft: &SettlementLifecycleFeedbackDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_SETTLEMENT_LIFECYCLE_FEEDBACK_PAYLOAD_V1");
    hash.text(SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1);
    hash.text(SIMULATED_EXECUTION_EVIDENCE_V1);
    hash.text(&draft.account_instance_key.run_id);
    hash.text(match draft.account_instance_key.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&draft.account_instance_key.caller_instance_id);
    hash.text(&draft.account_instance_key.account_id);
    hash.text(match draft.account_instance_key.platform_source {
        PlatformSourceV1::Simulated => "SIMULATED",
    });
    hash.text(&draft.session_id);
    hash.text(&draft.delivery_id);
    hash.i64(draft.timing.platform_event_at_utc_ms);
    hash.i64(draft.timing.feedback_received_at_utc_ms);
    hash.u64(draft.timing.private_emission_sequence);
    hash.u64(candidate.private_feedback_sequence);
    hash.text(&draft.settlement_identity);
    hash_settlement_event(&mut hash, &draft.event);
    hash.finish()
}

fn recovered_lifecycle_timing(
    platform_event_at_utc_ms: i64,
    feedback_received_at_utc_ms: i64,
    private_emission_sequence: u64,
) -> crate::session::LifecycleFeedbackTimingV1 {
    crate::session::LifecycleFeedbackTimingV1 {
        feedback_id: String::new(),
        platform_event_at_utc_ms,
        private_emission_sequence,
        inbound_delay_ms: 0,
        feedback_received_at_utc_ms,
    }
}

fn recovered_lifecycle_candidate(
    feedback_received_at_utc_ms: i64,
    private_emission_sequence: u64,
    private_feedback_sequence: u64,
) -> crate::session::VisiblePrivateFactV1 {
    crate::session::VisiblePrivateFactV1 {
        evidence: crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback {
            feedback_id: String::new(),
        },
        received_at_utc_ms: feedback_received_at_utc_ms,
        private_emission_sequence,
        private_feedback_sequence,
    }
}

fn validate_recovered_lifecycle_metadata(
    schema_version: &str,
    expected_schema_version: &str,
    evidence_source: &str,
) -> Result<(), SessionCheckpointErrorV1> {
    if schema_version != expected_schema_version
        || evidence_source != SIMULATED_EXECUTION_EVIDENCE_V1
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "已确认生命周期反馈的固定合同身份不符".to_owned(),
        ));
    }
    Ok(())
}

fn recovered_micro_units(value: Decimal) -> Result<u128, SessionCheckpointErrorV1> {
    micro_units_v1(value).map_err(|error| {
        SessionCheckpointErrorV1::InvalidSessionState(format!(
            "已确认生命周期反馈数量无法还原为微单位：{error}"
        ))
    })
}

fn recompute_order_lifecycle_payload_sha256(
    feedback: &OrderLifecycleFeedbackV1,
) -> Result<Sha256Hex, SessionCheckpointErrorV1> {
    let metadata = &feedback.metadata;
    validate_recovered_lifecycle_metadata(
        &metadata.schema_version,
        ORDER_LIFECYCLE_FEEDBACK_SCHEMA_V1,
        &metadata.evidence_source,
    )?;
    let draft = OrderLifecycleFeedbackDraftV1 {
        feedback_id: String::new(),
        delivery_id: metadata.delivery_id.clone(),
        account_instance_key: metadata.account_instance_key.clone(),
        session_id: metadata.session_id.clone(),
        timing: recovered_lifecycle_timing(
            metadata.platform_event_at_utc_ms,
            metadata.feedback_received_at_utc_ms,
            metadata.private_emission_sequence,
        ),
        platform_order_id: feedback.platform_order_id.clone(),
        condition_id: feedback.condition_id.clone(),
        token_id: feedback.token_id.clone(),
        event_kind: feedback.event_kind,
        order_status: feedback.order_status.clone(),
        venue_dust_closed_base_units: recovered_micro_units(
            feedback.venue_dust_closed_base_quantity_shares,
        )?,
        venue_dust_released_quote_units: recovered_micro_units(
            feedback.venue_dust_released_quote_amount_pusd,
        )?,
        market_resolved_canceled_base_units: recovered_micro_units(
            feedback.market_resolved_canceled_base_quantity_shares,
        )?,
        market_resolved_released_quote_units: recovered_micro_units(
            feedback.market_resolved_released_quote_amount_pusd,
        )?,
        account_delta: Arc::new(feedback.account_delta.clone()),
    };
    let candidate = recovered_lifecycle_candidate(
        metadata.feedback_received_at_utc_ms,
        metadata.private_emission_sequence,
        metadata.private_feedback_sequence,
    );
    Ok(order_lifecycle_payload_sha256(&draft, &candidate))
}

fn recompute_trade_lifecycle_payload_sha256(
    feedback: &TradeLifecycleFeedbackV1,
) -> Result<Sha256Hex, SessionCheckpointErrorV1> {
    let metadata = &feedback.metadata;
    validate_recovered_lifecycle_metadata(
        &metadata.schema_version,
        TRADE_LIFECYCLE_FEEDBACK_SCHEMA_V1,
        &metadata.evidence_source,
    )?;
    let draft = TradeLifecycleFeedbackDraftV1 {
        feedback_id: String::new(),
        delivery_id: metadata.delivery_id.clone(),
        account_instance_key: metadata.account_instance_key.clone(),
        session_id: metadata.session_id.clone(),
        timing: recovered_lifecycle_timing(
            metadata.platform_event_at_utc_ms,
            metadata.feedback_received_at_utc_ms,
            metadata.private_emission_sequence,
        ),
        simulated_trade_id: feedback.simulated_trade_id.clone(),
        platform_order_id: feedback.platform_order_id.clone(),
        condition_id: feedback.condition_id.clone(),
        token_id: feedback.token_id.clone(),
        status: feedback.status.clone(),
        economic_disposition: feedback.economic_disposition,
        liquidity_role: feedback.liquidity_role,
        base_units: recovered_micro_units(feedback.base_quantity_shares)?,
        quote_units: recovered_micro_units(feedback.quote_amount_pusd)?,
        immediate_taker_fee_units: recovered_micro_units(feedback.immediate_taker_fee_pusd)?,
        economic_application_delta: feedback.economic_application_delta.clone(),
        account_delta: feedback.account_delta.clone().map(Arc::new),
    };
    let candidate = recovered_lifecycle_candidate(
        metadata.feedback_received_at_utc_ms,
        metadata.private_emission_sequence,
        metadata.private_feedback_sequence,
    );
    Ok(trade_lifecycle_payload_sha256(&draft, &candidate))
}

fn recompute_settlement_lifecycle_payload_sha256(
    feedback: &SettlementLifecycleFeedbackV1,
) -> Result<Sha256Hex, SessionCheckpointErrorV1> {
    let metadata = &feedback.metadata;
    validate_recovered_lifecycle_metadata(
        &metadata.schema_version,
        SETTLEMENT_LIFECYCLE_FEEDBACK_SCHEMA_V1,
        &metadata.evidence_source,
    )?;
    let event = match &feedback.event {
        SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
            condition_id,
            status,
            contributions,
            accrual_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::MakerRebateAccrualFinalized {
            condition_id: condition_id.clone(),
            status: *status,
            contributions: contributions.clone(),
            accrual_pusd: *accrual_pusd,
            account_delta: account_delta.clone().map(Arc::new),
        },
        SettlementLifecycleEventV1::TakerRebateAccrualApplied {
            simulated_trade_id,
            condition_id,
            matched_at_utc_ms,
            attribution_day_start_utc_ms,
            actual_taker_fee_pusd,
            rebate_rate,
            accrual_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::TakerRebateAccrualApplied {
            simulated_trade_id: simulated_trade_id.clone(),
            condition_id: condition_id.clone(),
            matched_at_utc_ms: *matched_at_utc_ms,
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            actual_taker_fee_pusd: *actual_taker_fee_pusd,
            rebate_rate: *rebate_rate,
            accrual_pusd: *accrual_pusd,
            account_delta: Arc::new(account_delta.clone()),
        },
        SettlementLifecycleEventV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            accrual_pusd: *accrual_pusd,
            payable_pusd: *payable_pusd,
            locked_remainder_pusd: *locked_remainder_pusd,
            account_delta: account_delta.clone().map(Arc::new),
        },
        SettlementLifecycleEventV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            accrual_pusd: *accrual_pusd,
            payable_pusd: *payable_pusd,
            locked_remainder_pusd: *locked_remainder_pusd,
            account_delta: account_delta.clone().map(Arc::new),
        },
        SettlementLifecycleEventV1::MakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::MakerRebatePaid {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            paid_pusd: *paid_pusd,
            account_delta: Arc::new(account_delta.clone()),
        },
        SettlementLifecycleEventV1::TakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::TakerRebatePaid {
            attribution_day_start_utc_ms: *attribution_day_start_utc_ms,
            paid_pusd: *paid_pusd,
            account_delta: Arc::new(account_delta.clone()),
        },
        SettlementLifecycleEventV1::AutoRedeemApplied {
            condition_id,
            resolution_event_id,
            resolution_source_event_at_utc_ms,
            resolution,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
        } => SettlementLifecycleEventDraftV1::AutoRedeemApplied {
            condition_id: condition_id.clone(),
            resolution_event_id: resolution_event_id.clone(),
            resolution_source_event_at_utc_ms: *resolution_source_event_at_utc_ms,
            resolution: resolution.clone(),
            redeemed_outcome_balances: redeemed_outcome_balances.clone(),
            redeemed_pusd: *redeemed_pusd,
            account_delta: account_delta.clone().map(Arc::new),
        },
    };
    let draft = SettlementLifecycleFeedbackDraftV1 {
        feedback_id: String::new(),
        delivery_id: metadata.delivery_id.clone(),
        account_instance_key: metadata.account_instance_key.clone(),
        session_id: metadata.session_id.clone(),
        timing: recovered_lifecycle_timing(
            metadata.platform_event_at_utc_ms,
            metadata.feedback_received_at_utc_ms,
            metadata.private_emission_sequence,
        ),
        settlement_identity: feedback.settlement_identity.clone(),
        event,
    };
    let candidate = recovered_lifecycle_candidate(
        metadata.feedback_received_at_utc_ms,
        metadata.private_emission_sequence,
        metadata.private_feedback_sequence,
    );
    Ok(settlement_lifecycle_payload_sha256(&draft, &candidate))
}

fn hash_settlement_event(hash: &mut CanonicalHasher, event: &SettlementLifecycleEventDraftV1) {
    match event {
        SettlementLifecycleEventDraftV1::MakerRebateAccrualFinalized {
            condition_id,
            status,
            contributions,
            accrual_pusd,
            account_delta,
        } => {
            hash.text("MAKER_REBATE_ACCRUAL_FINALIZED");
            hash.text(&condition_id.0);
            hash.text(match status {
                MakerRebateAccrualStatusV1::Calculated => "CALCULATED",
                MakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill => {
                    "ZERO_NO_ELIGIBLE_MAKER_FILL"
                }
                MakerRebateAccrualStatusV1::UnknownIncomplete => "UNKNOWN_INCOMPLETE",
            });
            hash.u64(contributions.len() as u64);
            for contribution in contributions {
                hash.text(&contribution.simulated_trade_id);
                hash.i64(contribution.matched_at_utc_ms);
                hash.i64(contribution.attribution_day_start_utc_ms);
                hash.decimal(contribution.base_quantity_shares);
                hash.decimal(contribution.price_pusd_per_share);
                hash.decimal(contribution.maker_fee_equivalent_pusd);
                hash.decimal(contribution.rebate_rate);
                hash.decimal(contribution.rebate_contribution_pusd);
            }
            hash.decimal(*accrual_pusd);
            hash_optional_account_delta(hash, account_delta.as_deref());
        }
        SettlementLifecycleEventDraftV1::TakerRebateAccrualApplied {
            simulated_trade_id,
            condition_id,
            matched_at_utc_ms,
            attribution_day_start_utc_ms,
            actual_taker_fee_pusd,
            rebate_rate,
            accrual_pusd,
            account_delta,
        } => {
            hash.text("TAKER_REBATE_ACCRUAL_APPLIED");
            hash.text(simulated_trade_id);
            hash.text(&condition_id.0);
            hash.i64(*matched_at_utc_ms);
            hash.i64(*attribution_day_start_utc_ms);
            hash.decimal(*actual_taker_fee_pusd);
            hash.decimal(*rebate_rate);
            hash.decimal(*accrual_pusd);
            hash_optional_account_delta(hash, Some(account_delta));
        }
        SettlementLifecycleEventDraftV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        }
        | SettlementLifecycleEventDraftV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms,
            accrual_pusd,
            payable_pusd,
            locked_remainder_pusd,
            account_delta,
        } => {
            hash.text(match event {
                SettlementLifecycleEventDraftV1::MakerRebateDayFinalized { .. } => {
                    "MAKER_REBATE_DAY_FINALIZED"
                }
                _ => "TAKER_REBATE_DAY_FINALIZED",
            });
            hash.i64(*attribution_day_start_utc_ms);
            hash.decimal(*accrual_pusd);
            hash.decimal(*payable_pusd);
            hash.decimal(*locked_remainder_pusd);
            hash_optional_account_delta(hash, account_delta.as_deref());
        }
        SettlementLifecycleEventDraftV1::MakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        }
        | SettlementLifecycleEventDraftV1::TakerRebatePaid {
            attribution_day_start_utc_ms,
            paid_pusd,
            account_delta,
        } => {
            hash.text(match event {
                SettlementLifecycleEventDraftV1::MakerRebatePaid { .. } => "MAKER_REBATE_PAID",
                _ => "TAKER_REBATE_PAID",
            });
            hash.i64(*attribution_day_start_utc_ms);
            hash.decimal(*paid_pusd);
            hash_optional_account_delta(hash, Some(account_delta));
        }
        SettlementLifecycleEventDraftV1::AutoRedeemApplied {
            condition_id,
            resolution_event_id,
            resolution_source_event_at_utc_ms,
            resolution,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
        } => {
            hash.text("AUTO_REDEEM_APPLIED");
            hash.text(&condition_id.0);
            hash.text(resolution_event_id);
            hash.i64(*resolution_source_event_at_utc_ms);
            hash_market_resolution(hash, resolution);
            hash.u64(redeemed_outcome_balances.len() as u64);
            for balance in redeemed_outcome_balances {
                hash.text(&balance.token_id.0);
                hash.text(&balance.balance_shares.0);
            }
            hash.decimal(*redeemed_pusd);
            hash_optional_account_delta(hash, account_delta.as_deref());
        }
    }
}

fn hash_market_resolution(hash: &mut CanonicalHasher, resolution: &MarketResolutionTerminalV3) {
    match resolution {
        MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
        } => {
            hash.text("RESOLVED");
            hash.text(winning_asset_id);
            hash.text(winning_outcome);
            hash.u64(payout_vector.len() as u64);
            for payout in payout_vector {
                hash.text(&payout.asset.token_id.0);
                hash.u64(u64::from(payout.asset.outcome.index));
                hash.text(&payout.asset.outcome.label);
                hash.text(&payout.payout_pusd_per_share.0);
            }
            hash.text(mapping_rule_version);
        }
    }
}

fn hash_optional_account_delta(
    hash: &mut CanonicalHasher,
    account_delta: Option<&PlatformAccountDeltaV1>,
) {
    match account_delta {
        Some(delta) => {
            hash.text("ACCOUNT_DELTA");
            hash.u64(delta.account_transition_seq);
            hash.text(&delta.delta_id);
            hash.text(&delta.delta_sha256.0);
        }
        None => hash.text("NO_ACCOUNT_DELTA"),
    }
}

fn trade_status_identity(status: SimulatedTradeKnownStatusV1) -> &'static str {
    match status {
        SimulatedTradeKnownStatusV1::MatchedNotBroadcasted => "MATCHED_NOT_BROADCASTED",
        SimulatedTradeKnownStatusV1::Matched => "MATCHED",
        SimulatedTradeKnownStatusV1::Mined => "MINED",
        SimulatedTradeKnownStatusV1::Confirmed => "CONFIRMED",
        SimulatedTradeKnownStatusV1::Retrying => "RETRYING",
        SimulatedTradeKnownStatusV1::Failed => "FAILED",
    }
}

fn trade_economic_disposition_identity(disposition: TradeEconomicDispositionV1) -> &'static str {
    match disposition {
        TradeEconomicDispositionV1::PendingUnapplied => "PENDING_UNAPPLIED",
        TradeEconomicDispositionV1::PendingApplied => "PENDING_APPLIED",
        TradeEconomicDispositionV1::FinalSuccess => "FINAL_SUCCESS",
        TradeEconomicDispositionV1::FinalFailureUnapplied => "FINAL_FAILURE_UNAPPLIED",
        TradeEconomicDispositionV1::FinalFailureCompensated => "FINAL_FAILURE_COMPENSATED",
    }
}

fn seal_direct_response(
    draft: &DirectResponseDraftV1,
    candidate: &crate::session::VisiblePrivateFactV1,
) -> Result<SealedDirectResponseV1, SimExecutionContractErrorV1> {
    if candidate.received_at_utc_ms != draft.response_received_at_utc_ms
        || candidate.private_emission_sequence != draft.private_emission_sequence
    {
        return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
    }
    let payload_sha256 = match draft.method {
        DirectResponseMethodV1::PostOrder | DirectResponseMethodV1::PostOrders => {
            hash_submission_response(draft, candidate.private_feedback_sequence)?
        }
        _ => hash_non_submission_response(draft, candidate.private_feedback_sequence),
    };
    let metadata = SubmissionResponseMetadataV1 {
        execution_contract_version: draft.context.execution_contract_version.clone(),
        evidence_source: SIMULATED_EXECUTION_EVIDENCE_V1.to_owned(),
        session_id: draft.context.session_id.clone(),
        account_id: draft.context.account_id.clone(),
        caller_role: draft.context.caller_role,
        caller_instance_id: draft.context.caller_instance_id.clone(),
        global_request_id: draft.context.global_request_id.clone(),
        caller_correlation_id: draft.context.caller_correlation_id.clone(),
        method_identity: draft.method.identity().to_owned(),
        platform_responded_at_utc_ms: draft.platform_responded_at_utc_ms,
        response_received_at_utc_ms: draft.response_received_at_utc_ms,
        private_emission_sequence: draft.private_emission_sequence,
        private_feedback_sequence: candidate.private_feedback_sequence,
        delivery_id: draft.delivery_id.clone(),
        rule_snapshot_sha256: draft.rule_snapshot_sha256.clone(),
        scenario_sha256: draft.scenario_sha256.clone(),
        queue_model_version: L2_BOOK_DELTA_FIFO_ESTIMATE_V1.to_owned(),
        segment_quantization_model_version: L2_AGGREGATE_SYNTHETIC_MAKER_V1.to_owned(),
        fee_rule: draft.fee_rule.clone(),
        payload_sha256,
    };
    match draft.method {
        DirectResponseMethodV1::PostOrder => {
            let [DirectResponsePartV1::Submission(item)] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::PostOrder(Arc::new(
                PostOrderDirectResponseV1 {
                    metadata,
                    item: item.clone(),
                },
            )))
        }
        DirectResponseMethodV1::PostOrders => Ok(SealedDirectResponseV1::PostOrders(Arc::new(
            PostOrdersDirectResponseV1 {
                metadata,
                items: draft
                    .parts
                    .iter()
                    .map(|part| match part {
                        DirectResponsePartV1::Submission(item) => Ok(item.clone()),
                        _ => Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            },
        ))),
        DirectResponseMethodV1::CancelOrder => {
            let [DirectResponsePartV1::CancelTarget(target)] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::CancelOrder(Arc::new(
                CancelOrderDirectResponseV1 {
                    metadata,
                    target: target.clone(),
                },
            )))
        }
        DirectResponseMethodV1::CancelOrders => {
            let targets = draft
                .parts
                .iter()
                .map(|part| match part {
                    DirectResponsePartV1::CancelTarget(target) => Ok(target.clone()),
                    _ => Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SealedDirectResponseV1::CancelOrders(Arc::new(
                CancelOrdersDirectResponseV1 { metadata, targets },
            )))
        }
        DirectResponseMethodV1::CancelAllOrders => {
            let [DirectResponsePartV1::CancelAll { targets, .. }] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::CancelAllOrders(Arc::new(
                CancelAllOrdersDirectResponseV1 {
                    metadata,
                    targets: targets.clone(),
                },
            )))
        }
        DirectResponseMethodV1::Order => {
            let [DirectResponsePartV1::Order { result, .. }] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::Order(Arc::new(
                OrderDirectResponseV1 {
                    metadata,
                    result: result.clone(),
                },
            )))
        }
        DirectResponseMethodV1::Orders => {
            let [DirectResponsePartV1::Orders { result, .. }] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::Orders(Arc::new(
                OrdersDirectResponseV1 {
                    metadata,
                    result: result.clone(),
                },
            )))
        }
        DirectResponseMethodV1::Trades => {
            let [DirectResponsePartV1::Trades { result, .. }] = draft.parts.as_slice() else {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            };
            Ok(SealedDirectResponseV1::Trades(Arc::new(
                TradesDirectResponseV1 {
                    metadata,
                    result: result.clone(),
                },
            )))
        }
    }
}

fn hash_submission_response(
    draft: &DirectResponseDraftV1,
    private_feedback_sequence: u64,
) -> Result<Sha256Hex, SimExecutionContractErrorV1> {
    let mut hash = CanonicalHasher::new("SIM_SUBMISSION_DIRECT_RESPONSE_PAYLOAD_V1");
    hash.text(draft.method.identity());
    hash.text(&draft.context.execution_contract_version);
    hash.text(SIMULATED_EXECUTION_EVIDENCE_V1);
    hash.text(&draft.context.session_id);
    hash.text(&draft.context.account_id);
    hash.text(match draft.context.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&draft.context.caller_instance_id);
    hash.text(&draft.context.global_request_id);
    hash.optional_text(draft.context.caller_correlation_id.as_deref());
    hash.i64(draft.platform_responded_at_utc_ms);
    hash.i64(draft.response_received_at_utc_ms);
    hash.u64(draft.private_emission_sequence);
    hash.u64(private_feedback_sequence);
    hash.text(&draft.delivery_id);
    hash.text(&draft.rule_snapshot_sha256.0);
    hash.text(&draft.scenario_sha256.0);
    hash.text(L2_BOOK_DELTA_FIFO_ESTIMATE_V1);
    hash.text(L2_AGGREGATE_SYNTHETIC_MAKER_V1);
    hash.text(&draft.fee_rule.formula_version);
    hash.text(&draft.fee_rule.aggregation_version);
    hash.u64(u64::from(draft.fee_rule.precision_decimal_places));
    hash.decimal(draft.fee_rule.minimum_nonzero_fee_pusd);
    hash.decimal(draft.fee_rule.builder_fee_pusd);
    hash.u64(draft.parts.len() as u64);
    for part in &draft.parts {
        let DirectResponsePartV1::Submission(item) = part else {
            return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
        };
        hash_submission_response_item(&mut hash, item);
    }
    Ok(hash.finish())
}

fn hash_non_submission_response(
    draft: &DirectResponseDraftV1,
    private_feedback_sequence: u64,
) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_DIRECT_RESPONSE_PAYLOAD_V1");
    hash.text(draft.method.identity());
    hash.text(&draft.context.execution_contract_version);
    hash.text(SIMULATED_EXECUTION_EVIDENCE_V1);
    hash.text(&draft.context.session_id);
    hash.text(&draft.context.account_id);
    hash.text(match draft.context.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&draft.context.caller_instance_id);
    hash.text(&draft.context.global_request_id);
    hash.optional_text(draft.context.caller_correlation_id.as_deref());
    hash.i64(draft.platform_responded_at_utc_ms);
    hash.i64(draft.response_received_at_utc_ms);
    hash.u64(draft.private_emission_sequence);
    hash.u64(private_feedback_sequence);
    hash.text(&draft.delivery_id);
    hash.text(&draft.rule_snapshot_sha256.0);
    hash.text(&draft.scenario_sha256.0);
    hash.text(L2_BOOK_DELTA_FIFO_ESTIMATE_V1);
    hash.text(L2_AGGREGATE_SYNTHETIC_MAKER_V1);
    hash.text(&draft.fee_rule.formula_version);
    hash.text(&draft.fee_rule.aggregation_version);
    hash.u64(u64::from(draft.fee_rule.precision_decimal_places));
    hash.decimal(draft.fee_rule.minimum_nonzero_fee_pusd);
    hash.decimal(draft.fee_rule.builder_fee_pusd);
    hash.u64(draft.parts.len() as u64);
    for part in &draft.parts {
        hash_direct_response_part(&mut hash, part);
    }
    hash.finish()
}

fn recompute_direct_response_payload_sha256(
    response: &SealedDirectResponseV1,
) -> Result<Sha256Hex, SessionCheckpointErrorV1> {
    let (method, metadata, parts) = match response {
        SealedDirectResponseV1::PostOrder(response) => (
            DirectResponseMethodV1::PostOrder,
            &response.metadata,
            vec![DirectResponsePartV1::Submission(response.item.clone())],
        ),
        SealedDirectResponseV1::PostOrders(response) => (
            DirectResponseMethodV1::PostOrders,
            &response.metadata,
            response
                .items
                .iter()
                .cloned()
                .map(DirectResponsePartV1::Submission)
                .collect(),
        ),
        SealedDirectResponseV1::CancelOrder(response) => (
            DirectResponseMethodV1::CancelOrder,
            &response.metadata,
            vec![DirectResponsePartV1::CancelTarget(response.target.clone())],
        ),
        SealedDirectResponseV1::CancelOrders(response) => (
            DirectResponseMethodV1::CancelOrders,
            &response.metadata,
            response
                .targets
                .iter()
                .cloned()
                .map(DirectResponsePartV1::CancelTarget)
                .collect(),
        ),
        SealedDirectResponseV1::CancelAllOrders(response) => (
            DirectResponseMethodV1::CancelAllOrders,
            &response.metadata,
            vec![DirectResponsePartV1::CancelAll {
                effective_processing_at_utc_ms: response.metadata.platform_responded_at_utc_ms,
                targets: response.targets.clone(),
            }],
        ),
        SealedDirectResponseV1::Order(response) => (
            DirectResponseMethodV1::Order,
            &response.metadata,
            vec![DirectResponsePartV1::Order {
                effective_processing_at_utc_ms: response.metadata.platform_responded_at_utc_ms,
                result: response.result.clone(),
            }],
        ),
        SealedDirectResponseV1::Orders(response) => (
            DirectResponseMethodV1::Orders,
            &response.metadata,
            vec![DirectResponsePartV1::Orders {
                effective_processing_at_utc_ms: response.metadata.platform_responded_at_utc_ms,
                result: response.result.clone(),
            }],
        ),
        SealedDirectResponseV1::Trades(response) => (
            DirectResponseMethodV1::Trades,
            &response.metadata,
            vec![DirectResponsePartV1::Trades {
                effective_processing_at_utc_ms: response.metadata.platform_responded_at_utc_ms,
                result: response.result.clone(),
            }],
        ),
    };
    if metadata.method_identity != method.identity()
        || metadata.evidence_source != SIMULATED_EXECUTION_EVIDENCE_V1
        || metadata.queue_model_version != L2_BOOK_DELTA_FIFO_ESTIMATE_V1
        || metadata.segment_quantization_model_version != L2_AGGREGATE_SYNTHETIC_MAKER_V1
    {
        return Err(SessionCheckpointErrorV1::InvalidSessionState(
            "已确认直接响应的固定合同身份不符".to_owned(),
        ));
    }
    let draft = DirectResponseDraftV1 {
        method,
        delivery_id: metadata.delivery_id.clone(),
        context: RequestContextV1 {
            execution_contract_version: metadata.execution_contract_version.clone(),
            session_id: metadata.session_id.clone(),
            account_id: metadata.account_id.clone(),
            caller_role: metadata.caller_role,
            caller_instance_id: metadata.caller_instance_id.clone(),
            global_request_id: metadata.global_request_id.clone(),
            caller_correlation_id: metadata.caller_correlation_id.clone(),
            caller_sequence: 0,
            request_sent_at_utc_ms: 0,
            model_visible_cursor: ModelVisibleCursorV1 {
                session_id: metadata.session_id.clone(),
                public_visibility: PublicVisibilityCursorV1::default(),
                private_feedback_sequence: 0,
            },
        },
        platform_responded_at_utc_ms: metadata.platform_responded_at_utc_ms,
        response_received_at_utc_ms: metadata.response_received_at_utc_ms,
        private_emission_sequence: metadata.private_emission_sequence,
        rule_snapshot_sha256: metadata.rule_snapshot_sha256.clone(),
        scenario_sha256: metadata.scenario_sha256.clone(),
        fee_rule: metadata.fee_rule.clone(),
        parts,
    };
    match method {
        DirectResponseMethodV1::PostOrder | DirectResponseMethodV1::PostOrders => {
            hash_submission_response(&draft, metadata.private_feedback_sequence)
                .map_err(|error| SessionCheckpointErrorV1::InvalidSessionState(error.to_string()))
        }
        _ => Ok(hash_non_submission_response(
            &draft,
            metadata.private_feedback_sequence,
        )),
    }
}

fn hash_direct_response_part(hash: &mut CanonicalHasher, part: &DirectResponsePartV1) {
    match part {
        DirectResponsePartV1::Submission(item) => {
            hash.text("SUBMISSION");
            hash_submission_response_item(hash, item);
        }
        DirectResponsePartV1::CancelTarget(target) => {
            hash.text("CANCEL_TARGET");
            hash_cancel_target(hash, target);
        }
        DirectResponsePartV1::CancelAll {
            effective_processing_at_utc_ms,
            targets,
        } => {
            hash.text("CANCEL_ALL");
            hash.i64(*effective_processing_at_utc_ms);
            hash.u64(targets.len() as u64);
            for target in targets {
                hash_cancel_target(hash, target);
            }
        }
        DirectResponsePartV1::Order {
            effective_processing_at_utc_ms,
            result,
        } => {
            hash.text("ORDER");
            hash.i64(*effective_processing_at_utc_ms);
            hash_order_result(hash, result);
        }
        DirectResponsePartV1::Orders {
            effective_processing_at_utc_ms,
            result,
        } => {
            hash.text("ORDERS");
            hash.i64(*effective_processing_at_utc_ms);
            hash_orders_result(hash, result);
        }
        DirectResponsePartV1::Trades {
            effective_processing_at_utc_ms,
            result,
        } => {
            hash.text("TRADES");
            hash.i64(*effective_processing_at_utc_ms);
            hash_trades_result(hash, result);
        }
    }
}

fn hash_cancel_target(hash: &mut CanonicalHasher, target: &CancelTargetResponseV1) {
    hash.text(&target.platform_order_id);
    hash.u64(target.target_ordinal);
    hash.u64(target.input_item_refs.len() as u64);
    for item_ref in &target.input_item_refs {
        hash.text(&item_ref.item_id);
        hash.u64(u64::from(item_ref.input_item_ordinal));
    }
    hash.i64(target.effective_processing_at_utc_ms);
    hash.text(&target.cancel_effect_identity);
    hash.i64(target.cancel_effect_formed_at_utc_ms);
    hash.text(match target.cancel_effect_disposition {
        CancelEffectDispositionV1::NewEffect => "NEW_EFFECT",
        CancelEffectDispositionV1::ExistingEffect => "EXISTING_EFFECT",
    });
    match &target.outcome {
        CancelTargetOutcomeV1::Canceled(measurement) => {
            hash.text("CANCELED");
            hash.decimal(measurement.canceled_base_quantity_shares);
            hash.decimal(measurement.released_quote_amount_pusd);
            match &measurement.account_delta {
                Some(delta) => {
                    hash.text("ACCOUNT_DELTA");
                    hash.u64(delta.account_transition_seq);
                    hash.text(&delta.delta_id);
                    hash.text(&delta.delta_sha256.0);
                }
                None => hash.text("NO_ACCOUNT_DELTA"),
            }
        }
        CancelTargetOutcomeV1::NotCanceledNoEffect(reason) => {
            hash.text("NOT_CANCELED_NO_EFFECT");
            hash.text(match reason {
                CancelNotCanceledReasonV1::NotFoundOrNotOwned => "NOT_FOUND_OR_NOT_OWNED",
                CancelNotCanceledReasonV1::NoCancellableRemainder => "NO_CANCELLABLE_REMAINDER",
            });
        }
        CancelTargetOutcomeV1::EvidenceUnknownNoEffect(reason) => {
            hash.text("EVIDENCE_UNKNOWN_NO_EFFECT");
            hash.text(match reason {
                CancelEvidenceUnknownReasonV1::RetainedEvidenceIncomplete => {
                    "RETAINED_EVIDENCE_INCOMPLETE"
                }
            });
        }
    }
}

fn hash_order_result(hash: &mut CanonicalHasher, result: &OrderQueryResultV1) {
    match result {
        OrderQueryResultV1::Found(order) => {
            hash.text("FOUND");
            hash_order_projection(hash, order);
        }
        OrderQueryResultV1::NotFound => hash.text("NOT_FOUND"),
        OrderQueryResultV1::OutOfRetention(value) => {
            hash.text("OUT_OF_RETENTION");
            hash_out_of_retention(hash, value);
        }
    }
}

fn hash_orders_result(hash: &mut CanonicalHasher, result: &OrdersQueryResultV1) {
    match result {
        OrdersQueryResultV1::Page(page) => {
            hash.text("PAGE");
            hash.text(&page.root_identity);
            hash.u64(page.query_projection_revision);
            hash.u64(page.items.len() as u64);
            for item in &page.items {
                hash_order_projection(hash, item);
            }
            hash.optional_text(page.next_cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
        OrdersQueryResultV1::OutOfRetention(value) => {
            hash.text("OUT_OF_RETENTION");
            hash_out_of_retention(hash, value);
        }
    }
}

fn hash_trades_result(hash: &mut CanonicalHasher, result: &TradesQueryResultV1) {
    match result {
        TradesQueryResultV1::Page(page) => {
            hash.text("PAGE");
            hash.text(&page.root_identity);
            hash.u64(page.query_projection_revision);
            hash.u64(page.items.len() as u64);
            for item in &page.items {
                hash_trade_projection(hash, item);
            }
            hash.optional_text(page.next_cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
        TradesQueryResultV1::OutOfRetention(value) => {
            hash.text("OUT_OF_RETENTION");
            hash_out_of_retention(hash, value);
        }
    }
}

fn hash_out_of_retention(hash: &mut CanonicalHasher, value: &QueryOutOfRetentionV1) {
    hash.text(match value.scope {
        QueryRetentionScopeV1::OrderHistory => "ORDER_HISTORY",
        QueryRetentionScopeV1::TradeHistory => "TRADE_HISTORY",
        QueryRetentionScopeV1::RootSnapshot => "ROOT_SNAPSHOT",
    });
    hash.optional_i64(value.retained_from_utc_ms);
    match value.requested_root_sequence {
        Some(sequence) => {
            hash.boolean(true);
            hash.u64(sequence);
        }
        None => hash.boolean(false),
    }
    hash.u64(value.earliest_retained_root_sequence);
}

fn hash_order_projection(hash: &mut CanonicalHasher, order: &OrderProjectionV1) {
    hash.text(&order.platform_order_id);
    hash.text(&order.global_request_id);
    hash.text(&order.item_id);
    hash.text(&order.condition_id.0);
    hash.text(&order.token_id.0);
    hash.i64(order.created_at_utc_ms);
    hash.i64(order.last_updated_at_utc_ms);
    hash.text(match order.kind {
        OrderKindProjectionV1::Gtc => "GTC",
        OrderKindProjectionV1::Gtd => "GTD",
        OrderKindProjectionV1::Fok => "FOK",
        OrderKindProjectionV1::Fak => "FAK",
    });
    hash.optional_i64(order.expires_at_utc_ms);
    hash.decimal(order.source_price_pusd_per_share);
    hash.text(&order.status.raw);
    hash.optional_text(order.status.known.map(|known| match known {
        SimulatedOrderKnownStatusV1::Live => "LIVE",
        SimulatedOrderKnownStatusV1::Matched => "MATCHED",
        SimulatedOrderKnownStatusV1::VenueDustTerminal => "VENUE_DUST_TERMINAL",
        SimulatedOrderKnownStatusV1::CanceledMarketResolved => {
            "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
        }
        SimulatedOrderKnownStatusV1::Canceled => "CANCELED",
        SimulatedOrderKnownStatusV1::Expired => "EXPIRED",
        SimulatedOrderKnownStatusV1::Delayed => "DELAYED",
        SimulatedOrderKnownStatusV1::Unmatched => "UNMATCHED",
    }));
    match order.accepted_base_quantity_shares {
        Some(value) => {
            hash.boolean(true);
            hash.decimal(value);
        }
        None => hash.boolean(false),
    }
    hash.decimal(order.matched_base_quantity_shares);
    hash.decimal(order.pending_maker_depletion_base_quantity_shares);
    hash.decimal(order.open_base_quantity_shares);
    hash.decimal(order.venue_dust_closed_base_quantity_shares);
    hash.decimal(order.market_resolved_canceled_base_quantity_shares);
    hash.decimal(order.canceled_base_quantity_shares);
    hash.decimal(order.expired_base_quantity_shares);
    hash.decimal(order.accepted_quote_amount_pusd);
    hash.decimal(order.matched_quote_amount_pusd);
    hash.decimal(order.open_encumbered_quote_amount_pusd);
    hash.decimal(order.venue_dust_released_quote_amount_pusd);
    hash.decimal(order.market_resolved_released_quote_amount_pusd);
    hash.decimal(order.released_price_improvement_quote_amount_pusd);
    hash.decimal(order.canceled_released_quote_amount_pusd);
    hash.decimal(order.expired_released_quote_amount_pusd);
    hash.decimal(order.immediate_taker_fee_encumbered_pusd);
}

fn hash_trade_projection(hash: &mut CanonicalHasher, trade: &TradeProjectionV1) {
    hash.text(&trade.simulated_trade_id);
    hash.text(&trade.platform_order_id);
    hash.text(&trade.condition_id.0);
    hash.text(&trade.token_id.0);
    hash.i64(trade.matched_at_utc_ms);
    hash.text(&trade.status.raw);
    hash.optional_text(trade.status.known.map(trade_status_identity));
    hash.text(match trade.liquidity_role {
        TradeLiquidityRoleV1::Taker => "TAKER",
        TradeLiquidityRoleV1::MakerInferred => "MAKER_INFERRED",
    });
    hash.decimal(trade.source_price_pusd_per_share);
    hash.decimal(trade.base_quantity_shares);
    hash.decimal(trade.quote_amount_pusd);
    hash.decimal(trade.immediate_taker_fee_pusd);
    hash.text(trade_economic_disposition_identity(
        trade.economic_disposition,
    ));
}

fn orders_filter_sha256(filter: &OrdersFilterV1) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_ORDERS_FILTER_V1");
    hash.optional_text(filter.condition_id.as_ref().map(|id| id.0.as_str()));
    hash.optional_text(filter.token_id.as_ref().map(|id| id.0.as_str()));
    hash.finish()
}

fn trades_filter_sha256(filter: &TradesFilterV1) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_TRADES_FILTER_V1");
    hash.optional_text(filter.trade_id.as_deref());
    hash.optional_text(filter.condition_id.as_ref().map(|id| id.0.as_str()));
    hash.optional_text(filter.token_id.as_ref().map(|id| id.0.as_str()));
    hash.optional_i64(filter.after_utc_ms);
    hash.optional_i64(filter.before_utc_ms);
    hash.finish()
}

fn orders_projection_sha256(rows: &[OrderProjectionV1]) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_ORDERS_ROOT_PROJECTION_V1");
    hash.u64(rows.len() as u64);
    for row in rows {
        hash_order_projection(&mut hash, row);
    }
    hash.finish()
}

#[cfg(test)]
pub(crate) fn orders_projection_canonical_byte_len(rows: &[OrderProjectionV1]) -> u64 {
    let mut hash = CanonicalHasher::new("SIM_ORDERS_ROOT_PROJECTION_V1");
    hash.u64(rows.len() as u64);
    for row in rows {
        hash_order_projection(&mut hash, row);
    }
    hash.encoded_len()
}

fn trades_projection_sha256(rows: &[TradeProjectionV1]) -> Sha256Hex {
    let mut hash = CanonicalHasher::new("SIM_TRADES_ROOT_PROJECTION_V1");
    hash.u64(rows.len() as u64);
    for row in rows {
        hash_trade_projection(&mut hash, row);
    }
    hash.finish()
}

fn hash_submission_response_item(hash: &mut CanonicalHasher, item: &SubmissionResponseItemV1) {
    hash.text(&item.item_id);
    hash.u64(u64::from(item.batch_item_ordinal));
    hash.text(&item.condition_id.0);
    hash.text(&item.token_id.0);
    hash.i64(item.effective_processing_at_utc_ms);
    hash.text(&item.submission_effect_identity);
    hash.i64(item.submission_effect_formed_at_utc_ms);
    hash.text(match item.submission_effect_disposition {
        SubmissionEffectDispositionV1::NewEffect => "NEW_EFFECT",
        SubmissionEffectDispositionV1::ExistingEffect => "EXISTING_EFFECT",
    });
    match &item.outcome {
        SubmissionItemOutcomeV1::Accepted(accepted) => {
            hash.text("ACCEPTED");
            hash.text(&accepted.simulated_platform_order_id);
            hash_submission_platform_order_status(hash, &accepted.platform_order_status);
            hash.optional_text(accepted.terminal_reason.map(|reason| match reason {
                SubmissionTerminalReasonV1::FakCompleted => "FAK_COMPLETED",
                SubmissionTerminalReasonV1::VenueDustTerminal => "VENUE_DUST_TERMINAL",
            }));
            hash.u64(accepted.fills.len() as u64);
            for fill in &accepted.fills {
                hash.text(&fill.simulated_trade_id);
                hash.decimal(fill.source_price_pusd_per_share);
                hash.decimal(fill.base_quantity_shares);
                hash.decimal(fill.quote_amount_pusd);
            }
            match &accepted.measurement {
                SubmissionMeasurementV1::Base(value) => {
                    hash.text("BASE");
                    hash.decimal(value.accepted_base_quantity_shares);
                    hash.decimal(value.matched_base_quantity_shares);
                    hash.decimal(value.open_base_quantity_shares);
                    hash.decimal(value.venue_dust_closed_base_quantity_shares);
                    hash.decimal(value.order_type_terminated_base_quantity_shares);
                    hash.decimal(value.accepted_quote_amount_pusd);
                    hash.decimal(value.matched_quote_amount_pusd);
                    hash.decimal(value.open_encumbered_quote_amount_pusd);
                    hash.decimal(value.venue_dust_released_quote_amount_pusd);
                    hash.decimal(value.released_price_improvement_quote_amount_pusd);
                    hash.decimal(value.immediate_taker_fee_encumbered_pusd);
                }
                SubmissionMeasurementV1::QuoteBudget(value) => {
                    hash.text("QUOTE_BUDGET");
                    hash.decimal(value.accepted_quote_budget_pusd);
                    hash.decimal(value.used_quote_amount_pusd);
                    hash.decimal(value.released_or_unspent_quote_amount_pusd);
                    hash.decimal(value.actual_matched_base_quantity_shares);
                    hash.decimal(value.sdk_taker_amount_audit_shares);
                    hash.decimal(value.immediate_taker_fee_encumbered_pusd);
                }
            }
            hash.u64(accepted.account_deltas.len() as u64);
            for delta in &accepted.account_deltas {
                hash.u64(delta.account_transition_seq);
                hash.text(&delta.delta_id);
                hash.text(&delta.delta_sha256.0);
            }
        }
        SubmissionItemOutcomeV1::RejectedNoEffect(reason) => {
            hash.text("REJECTED_NO_EFFECT");
            hash.text(submission_rejection_identity(*reason));
        }
        SubmissionItemOutcomeV1::EvidenceUnknownNoEffect(reason) => {
            hash.text("EVIDENCE_UNKNOWN_NO_EFFECT");
            match reason {
                SubmissionEvidenceUnknownReasonV1::Preloading => hash.text("PRELOADING"),
                SubmissionEvidenceUnknownReasonV1::TargetAssetNotFresh(quality) => {
                    hash.text("TARGET_ASSET_NOT_FRESH");
                    hash.text(market_data_quality_identity(*quality));
                }
            }
        }
    }
}

fn hash_submission_platform_order_status(
    hash: &mut CanonicalHasher,
    status: &SubmissionPlatformOrderStatusV1,
) {
    match status {
        SubmissionPlatformOrderStatusV1::Live => hash.text("LIVE"),
        SubmissionPlatformOrderStatusV1::Matched => hash.text("MATCHED"),
        SubmissionPlatformOrderStatusV1::VenueDustTerminal => hash.text("VENUE_DUST_TERMINAL"),
        SubmissionPlatformOrderStatusV1::Delayed => hash.text("DELAYED"),
        SubmissionPlatformOrderStatusV1::Unmatched => hash.text("UNMATCHED"),
        SubmissionPlatformOrderStatusV1::Unknown(raw) => {
            hash.text("UNKNOWN");
            hash.text(raw);
        }
    }
}

fn submission_rejection_identity(reason: SubmissionRejectedReasonV1) -> &'static str {
    match reason {
        SubmissionRejectedReasonV1::EndAtReached => "END_AT_REACHED",
        SubmissionRejectedReasonV1::ExplicitTradingStateVeto => "EXPLICIT_TRADING_STATE_VETO",
        SubmissionRejectedReasonV1::MarketNotInScenario => "MARKET_NOT_IN_SCENARIO",
        SubmissionRejectedReasonV1::AssetNotInMarket => "ASSET_NOT_IN_MARKET",
        SubmissionRejectedReasonV1::OrderEncodingRejected => "ORDER_ENCODING_REJECTED",
        SubmissionRejectedReasonV1::BelowMinimumShares => "BELOW_MINIMUM_SHARES",
        SubmissionRejectedReasonV1::PostOnlyWouldTake => "POST_ONLY_WOULD_TAKE",
        SubmissionRejectedReasonV1::BelowMarketableLimitBuyMinimumPrincipal => {
            "BELOW_MARKETABLE_LIMIT_BUY_MINIMUM_PRINCIPAL"
        }
        SubmissionRejectedReasonV1::GtdExpirationTooSoon => "GTD_EXPIRATION_TOO_SOON",
        SubmissionRejectedReasonV1::InsufficientAvailablePusd => "INSUFFICIENT_AVAILABLE_PUSD",
        SubmissionRejectedReasonV1::FokNotFullyFilled => "FOK_NOT_FULLY_FILLED",
        SubmissionRejectedReasonV1::FakNoMatch => "FAK_NO_MATCH",
    }
}

fn market_data_quality_identity(quality: MarketDataQuality) -> &'static str {
    match quality {
        MarketDataQuality::Fresh => "FRESH",
        MarketDataQuality::Stale => "STALE",
        MarketDataQuality::Missing => "MISSING",
        MarketDataQuality::UnknownNotObserved => "UNKNOWN_NOT_OBSERVED",
        MarketDataQuality::ConfirmedGap => "CONFIRMED_GAP",
        MarketDataQuality::Invalid => "INVALID",
    }
}

fn rejected(rejection: SubmissionAdmissionRejectionV1) -> SubmissionAdmissionDecisionV1 {
    SubmissionAdmissionDecisionV1::RejectedNoEffect(rejection)
}

fn limit_buy_encoding(
    price: Decimal,
    size: Decimal,
    minimum_tick_size: Decimal,
) -> Option<BuyOrderEncoding> {
    encode_limit_buy_order(price, size, minimum_tick_size).ok()
}

fn market_buy_encoding(
    price: Decimal,
    spend: Decimal,
    minimum_tick_size: Decimal,
) -> Option<BuyOrderEncoding> {
    encode_market_buy_order(price, spend, minimum_tick_size).ok()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum PendingDomainRequestV1 {
    PostOrder(PostOrderRequestV1),
    PostOrders(PostOrdersRequestV1),
    CancelOrder(CancelOrderRequestV1),
    CancelOrders(CancelOrdersRequestV1),
    CancelAllOrders(CancelAllOrdersRequestV1),
    Order(OrderRequestV1),
    Orders(OrdersRequestV1),
    Trades(TradesRequestV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum DirectResponseMethodV1 {
    PostOrder,
    PostOrders,
    CancelOrder,
    CancelOrders,
    CancelAllOrders,
    Order,
    Orders,
    Trades,
}

impl DirectResponseMethodV1 {
    fn identity(self) -> &'static str {
        match self {
            Self::PostOrder => "post_order",
            Self::PostOrders => "post_orders",
            Self::CancelOrder => "cancel_order",
            Self::CancelOrders => "cancel_orders",
            Self::CancelAllOrders => "cancel_all_orders",
            Self::Order => "order",
            Self::Orders => "orders",
            Self::Trades => "trades",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum DirectResponsePartV1 {
    Submission(SubmissionResponseItemV1),
    CancelTarget(CancelTargetResponseV1),
    CancelAll {
        effective_processing_at_utc_ms: i64,
        targets: Vec<CancelTargetResponseV1>,
    },
    Order {
        effective_processing_at_utc_ms: i64,
        result: OrderQueryResultV1,
    },
    Orders {
        effective_processing_at_utc_ms: i64,
        result: OrdersQueryResultV1,
    },
    Trades {
        effective_processing_at_utc_ms: i64,
        result: TradesQueryResultV1,
    },
}

impl DirectResponsePartV1 {
    fn ordinal(&self) -> usize {
        match self {
            Self::Submission(item) => usize::from(item.batch_item_ordinal),
            Self::CancelTarget(target) => {
                usize::try_from(target.target_ordinal).expect("批撤目标序号受 1,000 项请求上限约束")
            }
            Self::CancelAll { .. }
            | Self::Order { .. }
            | Self::Orders { .. }
            | Self::Trades { .. } => 0,
        }
    }

    fn effective_processing_at_utc_ms(&self) -> i64 {
        match self {
            Self::Submission(item) => item.effective_processing_at_utc_ms,
            Self::CancelTarget(target) => target.effective_processing_at_utc_ms,
            Self::CancelAll {
                effective_processing_at_utc_ms,
                ..
            }
            | Self::Order {
                effective_processing_at_utc_ms,
                ..
            }
            | Self::Orders {
                effective_processing_at_utc_ms,
                ..
            }
            | Self::Trades {
                effective_processing_at_utc_ms,
                ..
            } => *effective_processing_at_utc_ms,
        }
    }

    fn method_compatible(&self, method: DirectResponseMethodV1) -> bool {
        matches!(
            (method, self),
            (
                DirectResponseMethodV1::PostOrder | DirectResponseMethodV1::PostOrders,
                Self::Submission(_)
            ) | (
                DirectResponseMethodV1::CancelOrder | DirectResponseMethodV1::CancelOrders,
                Self::CancelTarget(_)
            ) | (
                DirectResponseMethodV1::CancelAllOrders,
                Self::CancelAll { .. }
            ) | (DirectResponseMethodV1::Order, Self::Order { .. })
                | (DirectResponseMethodV1::Orders, Self::Orders { .. })
                | (DirectResponseMethodV1::Trades, Self::Trades { .. })
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DirectResponseDraftV1 {
    method: DirectResponseMethodV1,
    delivery_id: String,
    context: RequestContextV1,
    platform_responded_at_utc_ms: i64,
    response_received_at_utc_ms: i64,
    private_emission_sequence: u64,
    rule_snapshot_sha256: Sha256Hex,
    scenario_sha256: Sha256Hex,
    fee_rule: SubmissionFeeRuleIdentityV1,
    parts: Vec<DirectResponsePartV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SealedDirectResponseV1 {
    PostOrder(Arc<PostOrderDirectResponseV1>),
    PostOrders(Arc<PostOrdersDirectResponseV1>),
    CancelOrder(Arc<CancelOrderDirectResponseV1>),
    CancelOrders(Arc<CancelOrdersDirectResponseV1>),
    CancelAllOrders(Arc<CancelAllOrdersDirectResponseV1>),
    Order(Arc<OrderDirectResponseV1>),
    Orders(Arc<OrdersDirectResponseV1>),
    Trades(Arc<TradesDirectResponseV1>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SealedPrivateDeliveryV1 {
    Direct(SealedDirectResponseV1),
    Lifecycle(SealedLifecycleFeedbackV1),
}

impl SealedPrivateDeliveryV1 {
    fn confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        match self {
            Self::Direct(response) => response.confirmation(),
            Self::Lifecycle(feedback) => feedback.confirmation(),
        }
    }

    fn recompute_payload_sha256(&self) -> Result<Sha256Hex, SessionCheckpointErrorV1> {
        match self {
            Self::Direct(response) => recompute_direct_response_payload_sha256(response),
            Self::Lifecycle(SealedLifecycleFeedbackV1::Order(feedback)) => {
                recompute_order_lifecycle_payload_sha256(feedback)
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Trade(feedback)) => {
                recompute_trade_lifecycle_payload_sha256(feedback)
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Settlement(feedback)) => {
                recompute_settlement_lifecycle_payload_sha256(feedback)
            }
        }
    }

    fn received_at_and_emission_sequence(&self) -> (i64, u64) {
        match self {
            Self::Direct(response) => {
                let metadata = response.metadata();
                (
                    metadata.response_received_at_utc_ms,
                    metadata.private_emission_sequence,
                )
            }
            Self::Lifecycle(feedback) => feedback.received_at_and_emission_sequence(),
        }
    }

    fn validate_account_deltas(&self) -> Result<(), SessionCheckpointErrorV1> {
        let validate = |delta: &PlatformAccountDeltaV1| {
            validate_platform_account_delta_integrity(delta).map_err(|error| {
                SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "恢复私有载荷包含损坏的平台账户变化：{error}"
                ))
            })
        };
        match self {
            Self::Direct(_) => Ok(()),
            Self::Lifecycle(SealedLifecycleFeedbackV1::Order(feedback)) => {
                validate(&feedback.account_delta)
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Trade(feedback)) => {
                if let Some(delta) = &feedback.account_delta {
                    validate(delta)?;
                }
                Ok(())
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Settlement(feedback)) => {
                let delta = match &feedback.event {
                    SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventV1::MakerRebateDayFinalized {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::TakerRebateDayFinalized {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::AutoRedeemApplied { account_delta, .. } => {
                        account_delta.as_ref()
                    }
                    SettlementLifecycleEventV1::TakerRebateAccrualApplied {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::MakerRebatePaid { account_delta, .. }
                    | SettlementLifecycleEventV1::TakerRebatePaid { account_delta, .. } => {
                        Some(account_delta)
                    }
                };
                if let Some(delta) = delta {
                    validate(delta)?;
                }
                Ok(())
            }
        }
    }

    fn visit_account_deltas(
        &self,
        visitor: &mut impl FnMut(&PlatformAccountDeltaV1) -> Result<(), SessionCheckpointErrorV1>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        match self {
            Self::Direct(_) => Ok(()),
            Self::Lifecycle(SealedLifecycleFeedbackV1::Order(feedback)) => {
                visitor(&feedback.account_delta)
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Trade(feedback)) => {
                if let Some(delta) = &feedback.account_delta {
                    visitor(delta)?;
                }
                Ok(())
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Settlement(feedback)) => {
                let delta = match &feedback.event {
                    SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventV1::MakerRebateDayFinalized {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::TakerRebateDayFinalized {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::AutoRedeemApplied { account_delta, .. } => {
                        account_delta.as_ref()
                    }
                    SettlementLifecycleEventV1::TakerRebateAccrualApplied {
                        account_delta, ..
                    }
                    | SettlementLifecycleEventV1::MakerRebatePaid { account_delta, .. }
                    | SettlementLifecycleEventV1::TakerRebatePaid { account_delta, .. } => {
                        Some(account_delta)
                    }
                };
                if let Some(delta) = delta {
                    visitor(delta)?;
                }
                Ok(())
            }
        }
    }

    fn dispatch(
        &self,
        response_bridge: &mut SubmissionResponseBridgeV1,
    ) -> SubmissionResponseAcceptanceV1 {
        match self {
            Self::Direct(SealedDirectResponseV1::PostOrder(response)) => {
                response_bridge.accept_post_order(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::PostOrders(response)) => {
                response_bridge.accept_post_orders(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::CancelOrder(response)) => {
                response_bridge.accept_cancel_order(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::CancelOrders(response)) => {
                response_bridge.accept_cancel_orders(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::CancelAllOrders(response)) => {
                response_bridge.accept_cancel_all_orders(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::Order(response)) => {
                response_bridge.accept_order(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::Orders(response)) => {
                response_bridge.accept_orders(Arc::clone(response))
            }
            Self::Direct(SealedDirectResponseV1::Trades(response)) => {
                response_bridge.accept_trades(Arc::clone(response))
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Order(feedback)) => {
                response_bridge.accept_order_lifecycle_feedback(Arc::clone(feedback))
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Trade(feedback)) => {
                response_bridge.accept_trade_lifecycle_feedback(Arc::clone(feedback))
            }
            Self::Lifecycle(SealedLifecycleFeedbackV1::Settlement(feedback)) => {
                response_bridge.accept_settlement_lifecycle_feedback(Arc::clone(feedback))
            }
        }
    }
}

impl SealedDirectResponseV1 {
    fn metadata(&self) -> &SubmissionResponseMetadataV1 {
        match self {
            Self::PostOrder(response) => &response.metadata,
            Self::PostOrders(response) => &response.metadata,
            Self::CancelOrder(response) => &response.metadata,
            Self::CancelOrders(response) => &response.metadata,
            Self::CancelAllOrders(response) => &response.metadata,
            Self::Order(response) => &response.metadata,
            Self::Orders(response) => &response.metadata,
            Self::Trades(response) => &response.metadata,
        }
    }

    fn confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        self.metadata().delivery_confirmation()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DirectResponseOutboxEntryV1 {
    method: DirectResponseMethodV1,
    delivery_id: String,
    parts: Vec<Option<DirectResponsePartV1>>,
    platform_responded_at_utc_ms: Option<i64>,
    draft: Option<DirectResponseDraftV1>,
    sealed: Option<SealedDirectResponseV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LogicalDirectResponseOutboxV1 {
    capacity: usize,
    entries: BTreeMap<String, DirectResponseOutboxEntryV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OrderLifecycleFeedbackDraftV1 {
    feedback_id: String,
    delivery_id: String,
    account_instance_key: AccountInstanceKeyV1,
    session_id: String,
    timing: crate::session::LifecycleFeedbackTimingV1,
    platform_order_id: String,
    condition_id: ConditionId,
    token_id: TokenId,
    event_kind: OrderLifecycleEventKindV1,
    order_status: SimulatedOrderStatusV1,
    venue_dust_closed_base_units: u128,
    venue_dust_released_quote_units: u128,
    market_resolved_canceled_base_units: u128,
    market_resolved_released_quote_units: u128,
    account_delta: Arc<PlatformAccountDeltaV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TradeLifecycleFeedbackDraftV1 {
    feedback_id: String,
    delivery_id: String,
    account_instance_key: AccountInstanceKeyV1,
    session_id: String,
    timing: crate::session::LifecycleFeedbackTimingV1,
    simulated_trade_id: String,
    platform_order_id: String,
    condition_id: ConditionId,
    token_id: TokenId,
    status: SimulatedTradeStatusV1,
    economic_disposition: TradeEconomicDispositionV1,
    liquidity_role: TradeLiquidityRoleV1,
    base_units: u128,
    quote_units: u128,
    immediate_taker_fee_units: u128,
    economic_application_delta: Option<AccountDeltaReferenceV1>,
    account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SettlementLifecycleEventDraftV1 {
    MakerRebateAccrualFinalized {
        condition_id: ConditionId,
        status: MakerRebateAccrualStatusV1,
        contributions: Vec<MakerRebateFillContributionV1>,
        accrual_pusd: Decimal,
        account_delta: Option<Arc<PlatformAccountDeltaV1>>,
    },
    TakerRebateAccrualApplied {
        simulated_trade_id: String,
        condition_id: ConditionId,
        matched_at_utc_ms: i64,
        attribution_day_start_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
        rebate_rate: Decimal,
        accrual_pusd: Decimal,
        account_delta: Arc<PlatformAccountDeltaV1>,
    },
    MakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: Decimal,
        payable_pusd: Decimal,
        locked_remainder_pusd: Decimal,
        account_delta: Option<Arc<PlatformAccountDeltaV1>>,
    },
    TakerRebateDayFinalized {
        attribution_day_start_utc_ms: i64,
        accrual_pusd: Decimal,
        payable_pusd: Decimal,
        locked_remainder_pusd: Decimal,
        account_delta: Option<Arc<PlatformAccountDeltaV1>>,
    },
    MakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: Decimal,
        account_delta: Arc<PlatformAccountDeltaV1>,
    },
    TakerRebatePaid {
        attribution_day_start_utc_ms: i64,
        paid_pusd: Decimal,
        account_delta: Arc<PlatformAccountDeltaV1>,
    },
    AutoRedeemApplied {
        condition_id: ConditionId,
        resolution_event_id: String,
        resolution_source_event_at_utc_ms: i64,
        resolution: MarketResolutionTerminalV3,
        redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
        redeemed_pusd: Decimal,
        account_delta: Option<Arc<PlatformAccountDeltaV1>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SettlementLifecycleFeedbackDraftV1 {
    feedback_id: String,
    delivery_id: String,
    account_instance_key: AccountInstanceKeyV1,
    session_id: String,
    timing: crate::session::LifecycleFeedbackTimingV1,
    settlement_identity: String,
    event: SettlementLifecycleEventDraftV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LifecycleFeedbackDraftV1 {
    Order(OrderLifecycleFeedbackDraftV1),
    Trade(TradeLifecycleFeedbackDraftV1),
    Settlement(SettlementLifecycleFeedbackDraftV1),
}

impl LifecycleFeedbackDraftV1 {
    fn feedback_id(&self) -> &str {
        match self {
            Self::Order(draft) => &draft.feedback_id,
            Self::Trade(draft) => &draft.feedback_id,
            Self::Settlement(draft) => &draft.feedback_id,
        }
    }

    fn delivery_id(&self) -> &str {
        match self {
            Self::Order(draft) => &draft.delivery_id,
            Self::Trade(draft) => &draft.delivery_id,
            Self::Settlement(draft) => &draft.delivery_id,
        }
    }

    fn session_id(&self) -> &str {
        match self {
            Self::Order(draft) => &draft.session_id,
            Self::Trade(draft) => &draft.session_id,
            Self::Settlement(draft) => &draft.session_id,
        }
    }

    fn timing(&self) -> &crate::session::LifecycleFeedbackTimingV1 {
        match self {
            Self::Order(draft) => &draft.timing,
            Self::Trade(draft) => &draft.timing,
            Self::Settlement(draft) => &draft.timing,
        }
    }

    fn visit_account_deltas(
        &self,
        visitor: &mut impl FnMut(&PlatformAccountDeltaV1) -> Result<(), SessionCheckpointErrorV1>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        match self {
            Self::Order(draft) => visitor(&draft.account_delta),
            Self::Trade(draft) => {
                if let Some(delta) = draft.account_delta.as_deref() {
                    visitor(delta)?;
                }
                Ok(())
            }
            Self::Settlement(draft) => {
                let delta = match &draft.event {
                    SettlementLifecycleEventDraftV1::MakerRebateAccrualFinalized {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventDraftV1::MakerRebateDayFinalized {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventDraftV1::TakerRebateDayFinalized {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventDraftV1::AutoRedeemApplied {
                        account_delta, ..
                    } => account_delta.as_deref(),
                    SettlementLifecycleEventDraftV1::TakerRebateAccrualApplied {
                        account_delta,
                        ..
                    }
                    | SettlementLifecycleEventDraftV1::MakerRebatePaid { account_delta, .. }
                    | SettlementLifecycleEventDraftV1::TakerRebatePaid { account_delta, .. } => {
                        Some(account_delta.as_ref())
                    }
                };
                if let Some(delta) = delta {
                    visitor(delta)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum SealedLifecycleFeedbackV1 {
    Order(Arc<OrderLifecycleFeedbackV1>),
    Trade(Arc<TradeLifecycleFeedbackV1>),
    Settlement(Arc<SettlementLifecycleFeedbackV1>),
}

impl SealedLifecycleFeedbackV1 {
    fn received_at_and_emission_sequence(&self) -> (i64, u64) {
        match self {
            Self::Order(feedback) => (
                feedback.metadata.feedback_received_at_utc_ms,
                feedback.metadata.private_emission_sequence,
            ),
            Self::Trade(feedback) => (
                feedback.metadata.feedback_received_at_utc_ms,
                feedback.metadata.private_emission_sequence,
            ),
            Self::Settlement(feedback) => (
                feedback.metadata.feedback_received_at_utc_ms,
                feedback.metadata.private_emission_sequence,
            ),
        }
    }

    fn confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        match self {
            Self::Order(feedback) => feedback.metadata.delivery_confirmation(),
            Self::Trade(feedback) => feedback.metadata.delivery_confirmation(),
            Self::Settlement(feedback) => feedback.metadata.delivery_confirmation(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LifecycleOutboxEntryV1 {
    batch_id: String,
    draft: LifecycleFeedbackDraftV1,
    sealed: Option<SealedLifecycleFeedbackV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LifecycleBatchV1 {
    open_work_ids: BTreeSet<String>,
    feedback_ids: BTreeSet<String>,
    feedback_received_at_counts: BTreeMap<i64, usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecycleBatchReservationV1 {
    batch_id: String,
    work_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LogicalLifecycleOutboxV1 {
    capacity: usize,
    batches: BTreeMap<String, LifecycleBatchV1>,
    entries: BTreeMap<String, LifecycleOutboxEntryV1>,
    work_batch_ids: BTreeMap<String, String>,
}

impl LogicalLifecycleOutboxV1 {
    fn new(capacity: std::num::NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            batches: BTreeMap::new(),
            entries: BTreeMap::new(),
            work_batch_ids: BTreeMap::new(),
        }
    }

    fn request_batch_id(global_request_id: &str) -> String {
        format!("REQUEST:{global_request_id}")
    }

    fn reserve_request(
        &mut self,
        global_request_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<LifecycleBatchReservationV1, SessionError> {
        let work_id = Self::request_batch_id(global_request_id);
        if let Some(batch_id) = self.work_batch_ids.get(&work_id).cloned() {
            let batch = self.batches.get(&batch_id).ok_or_else(|| {
                SessionError::RequestNotFound(format!("lifecycle batch:{batch_id}"))
            })?;
            if !batch.open_work_ids.contains(&work_id) {
                return Err(SessionError::ResponseDeliveryIdentityConflict);
            }
            if self
                .earliest_feedback_received_at(batch)?
                .is_some_and(|received_at| platform_effective_at_utc_ms > received_at)
            {
                return Err(SessionError::ResponseOutboxFull);
            }
            return Ok(LifecycleBatchReservationV1 { batch_id, work_id });
        }
        self.reserve_visibility_work(work_id, platform_effective_at_utc_ms)
    }

    fn reserve_platform_work(
        &mut self,
        causal_identity: &str,
        preferred_global_request_id: Option<&str>,
        platform_effective_at_utc_ms: i64,
    ) -> Result<LifecycleBatchReservationV1, SessionError> {
        let work_id = format!("PLATFORM:{causal_identity}");
        if self.work_batch_ids.contains_key(&work_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        if let Some(global_request_id) = preferred_global_request_id {
            let request_work_id = Self::request_batch_id(global_request_id);
            if let Some(batch_id) = self.work_batch_ids.get(&request_work_id).cloned() {
                let batch = self.batches.get(&batch_id).ok_or_else(|| {
                    SessionError::RequestNotFound(format!("lifecycle batch:{batch_id}"))
                })?;
                if !batch.open_work_ids.contains(&request_work_id) {
                    return Err(SessionError::ResponseDeliveryIdentityConflict);
                }
                if self
                    .earliest_feedback_received_at(batch)?
                    .is_some_and(|received_at| platform_effective_at_utc_ms > received_at)
                {
                    return Err(SessionError::ResponseOutboxFull);
                }
                let batch = self.batches.get_mut(&batch_id).ok_or_else(|| {
                    SessionError::RequestNotFound(format!("lifecycle batch:{batch_id}"))
                })?;
                if !batch.open_work_ids.insert(work_id.clone()) {
                    return Err(SessionError::ResponseDeliveryIdentityConflict);
                }
                self.work_batch_ids
                    .insert(work_id.clone(), batch_id.clone());
                return Ok(LifecycleBatchReservationV1 { batch_id, work_id });
            }
        }
        self.reserve_visibility_work(work_id, platform_effective_at_utc_ms)
    }

    fn reserve_visibility_work(
        &mut self,
        work_id: String,
        platform_effective_at_utc_ms: i64,
    ) -> Result<LifecycleBatchReservationV1, SessionError> {
        if self.work_batch_ids.contains_key(&work_id) || self.batches.contains_key(&work_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        if self.batches.len() < self.capacity {
            let batch_id = work_id.clone();
            self.batches.insert(
                batch_id.clone(),
                LifecycleBatchV1 {
                    open_work_ids: BTreeSet::from([work_id.clone()]),
                    feedback_ids: BTreeSet::new(),
                    feedback_received_at_counts: BTreeMap::new(),
                },
            );
            self.work_batch_ids
                .insert(work_id.clone(), batch_id.clone());
            return Ok(LifecycleBatchReservationV1 { batch_id, work_id });
        }

        let mut reusable = None;
        for (batch_id, batch) in &self.batches {
            let threshold = match self.earliest_feedback_received_at(batch)? {
                Some(received_at) if platform_effective_at_utc_ms <= received_at => received_at,
                Some(_) => continue,
                None if batch
                    .open_work_ids
                    .iter()
                    .any(|work_id| work_id.starts_with("REQUEST:")) =>
                {
                    i64::MAX
                }
                None => continue,
            };
            let candidate = (threshold, batch_id.clone());
            if reusable.as_ref().is_none_or(|current| candidate < *current) {
                reusable = Some(candidate);
            }
        }
        let Some((_, batch_id)) = reusable else {
            return Err(SessionError::ResponseOutboxFull);
        };
        let inserted = self
            .batches
            .get_mut(&batch_id)
            .ok_or(SessionError::ResponseDeliveryIdentityConflict)?
            .open_work_ids
            .insert(work_id.clone());
        if !inserted {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        self.work_batch_ids
            .insert(work_id.clone(), batch_id.clone());
        Ok(LifecycleBatchReservationV1 { batch_id, work_id })
    }

    fn earliest_feedback_received_at(
        &self,
        batch: &LifecycleBatchV1,
    ) -> Result<Option<i64>, SessionError> {
        if batch.feedback_ids.is_empty() != batch.feedback_received_at_counts.is_empty() {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        Ok(batch
            .feedback_received_at_counts
            .first_key_value()
            .map(|(received_at, _)| *received_at))
    }

    fn record_for_batch(
        &mut self,
        reservation: &LifecycleBatchReservationV1,
        draft: LifecycleFeedbackDraftV1,
    ) -> Result<(), SessionError> {
        let batch = self.batches.get_mut(&reservation.batch_id).ok_or_else(|| {
            SessionError::RequestNotFound(format!("lifecycle batch:{}", reservation.batch_id))
        })?;
        if !batch.open_work_ids.contains(&reservation.work_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        let feedback_id = draft.feedback_id().to_owned();
        if let Some(existing) = self.entries.get(&feedback_id) {
            if existing.batch_id == reservation.batch_id && existing.draft == draft {
                return Ok(());
            }
            return Err(SessionError::FeedbackIdentityConflict(feedback_id));
        }
        if batch.feedback_ids.contains(&feedback_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        let received_at = draft.timing().feedback_received_at_utc_ms;
        let next_received_at_count = batch
            .feedback_received_at_counts
            .get(&received_at)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(SessionError::ResponseDeliveryIdentityConflict)?;
        batch.feedback_ids.insert(feedback_id.clone());
        batch
            .feedback_received_at_counts
            .insert(received_at, next_received_at_count);
        self.entries.insert(
            feedback_id,
            LifecycleOutboxEntryV1 {
                batch_id: reservation.batch_id.clone(),
                draft,
                sealed: None,
            },
        );
        Ok(())
    }

    fn complete_platform_work(
        &mut self,
        reservation: &LifecycleBatchReservationV1,
    ) -> Result<(), SessionError> {
        self.complete_work(reservation)
    }

    fn complete_request(
        &mut self,
        global_request_id: &str,
        reservation: &LifecycleBatchReservationV1,
    ) -> Result<(), SessionError> {
        let work_id = Self::request_batch_id(global_request_id);
        if self.work_batch_ids.get(&work_id) != Some(&reservation.batch_id)
            || reservation.work_id != work_id
        {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        self.complete_work(reservation)
    }

    fn complete_work(
        &mut self,
        reservation: &LifecycleBatchReservationV1,
    ) -> Result<(), SessionError> {
        if self.work_batch_ids.get(&reservation.work_id) != Some(&reservation.batch_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        let remove_batch = {
            let batch = self.batches.get_mut(&reservation.batch_id).ok_or_else(|| {
                SessionError::RequestNotFound(format!("lifecycle batch:{}", reservation.batch_id))
            })?;
            if !batch.open_work_ids.remove(&reservation.work_id) {
                return Err(SessionError::ResponseDeliveryIdentityConflict);
            }
            batch.open_work_ids.is_empty()
                && batch.feedback_ids.is_empty()
                && batch.feedback_received_at_counts.is_empty()
        };
        self.work_batch_ids.remove(&reservation.work_id);
        if remove_batch {
            self.batches.remove(&reservation.batch_id);
        }
        Ok(())
    }

    fn draft(&self, feedback_id: &str) -> Option<&LifecycleFeedbackDraftV1> {
        self.entries.get(feedback_id).map(|entry| &entry.draft)
    }

    fn sealed(&self, feedback_id: &str) -> Option<&SealedLifecycleFeedbackV1> {
        self.entries.get(feedback_id)?.sealed.as_ref()
    }

    fn cache_sealed(
        &mut self,
        feedback_id: &str,
        feedback: SealedLifecycleFeedbackV1,
    ) -> Result<(), SessionError> {
        let entry = self
            .entries
            .get_mut(feedback_id)
            .ok_or_else(|| SessionError::FeedbackIdentityConflict(feedback_id.to_owned()))?;
        if let Some(existing) = &entry.sealed {
            if existing != &feedback {
                return Err(SessionError::ResponseDeliveryIdentityConflict);
            }
            return Ok(());
        }
        entry.sealed = Some(feedback);
        Ok(())
    }

    fn validate_remove_confirmed(&self, feedback_id: &str) -> Result<(), SessionError> {
        let entry = self
            .entries
            .get(feedback_id)
            .ok_or_else(|| SessionError::FeedbackIdentityConflict(feedback_id.to_owned()))?;
        let batch = self
            .batches
            .get(&entry.batch_id)
            .ok_or(SessionError::ResponseDeliveryIdentityConflict)?;
        if !batch.feedback_ids.contains(feedback_id) {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        if batch
            .feedback_received_at_counts
            .get(&entry.draft.timing().feedback_received_at_utc_ms)
            .is_none_or(|count| *count == 0)
        {
            return Err(SessionError::ResponseDeliveryIdentityConflict);
        }
        Ok(())
    }

    fn remove_prevalidated_confirmed(&mut self, feedback_id: &str) {
        debug_assert!(self.validate_remove_confirmed(feedback_id).is_ok());
        let entry = self
            .entries
            .remove(feedback_id)
            .expect("已验证生命周期反馈必须仍在载荷池中");
        let batch = self
            .batches
            .get_mut(&entry.batch_id)
            .expect("已验证生命周期反馈必须仍绑定可见性槽");
        let removed = batch.feedback_ids.remove(feedback_id);
        debug_assert!(removed);
        let received_at = entry.draft.timing().feedback_received_at_utc_ms;
        let remove_received_at = {
            let count = batch
                .feedback_received_at_counts
                .get_mut(&received_at)
                .expect("已验证生命周期反馈必须仍计入接收时点索引");
            *count -= 1;
            *count == 0
        };
        if remove_received_at {
            batch.feedback_received_at_counts.remove(&received_at);
        }
        if batch.open_work_ids.is_empty()
            && batch.feedback_ids.is_empty()
            && batch.feedback_received_at_counts.is_empty()
        {
            self.batches.remove(&entry.batch_id);
        }
    }
}

impl LogicalDirectResponseOutboxV1 {
    fn new(capacity: std::num::NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            entries: BTreeMap::new(),
        }
    }

    fn delivery_id(
        session_id: &str,
        global_request_id: &str,
        method: DirectResponseMethodV1,
    ) -> String {
        let mut hash = CanonicalHasher::new("SIM_SUBMISSION_DELIVERY_ID_V1");
        hash.text(session_id);
        hash.text(global_request_id);
        hash.text(method.identity());
        hash.finish().0
    }

    fn reserve(
        &mut self,
        session_id: &str,
        global_request_id: &str,
        method: DirectResponseMethodV1,
        item_count: usize,
    ) -> Result<(), SimExecutionContractErrorV1> {
        if let Some(existing) = self.entries.get(global_request_id) {
            if existing.method == method && existing.parts.len() == item_count {
                return Ok(());
            }
            return Err(SimExecutionContractErrorV1::RequestIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        if self.entries.len() >= self.capacity {
            return Err(SimExecutionContractErrorV1::ResponseOutboxFull);
        }
        let delivery_id = Self::delivery_id(session_id, global_request_id, method);
        self.entries.insert(
            global_request_id.to_owned(),
            DirectResponseOutboxEntryV1 {
                method,
                delivery_id,
                parts: vec![None; item_count],
                platform_responded_at_utc_ms: None,
                draft: None,
                sealed: None,
            },
        );
        Ok(())
    }

    fn record_part(
        &mut self,
        global_request_id: &str,
        part: DirectResponsePartV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let entry = self.entries.get_mut(global_request_id).ok_or_else(|| {
            SimExecutionContractErrorV1::RequestNotFound(global_request_id.to_owned())
        })?;
        if !part.method_compatible(entry.method) {
            return Err(SimExecutionContractErrorV1::ActionIdentityConflict(
                format!("{global_request_id}:method"),
            ));
        }
        let ordinal = part.ordinal();
        let slot = entry.parts.get_mut(ordinal).ok_or_else(|| {
            SimExecutionContractErrorV1::ActionIdentityConflict(format!(
                "{global_request_id}:{ordinal}"
            ))
        })?;
        if let Some(existing) = slot {
            if existing == &part {
                return Ok(());
            }
            return Err(SimExecutionContractErrorV1::ActionIdentityConflict(
                format!("{global_request_id}:{ordinal}"),
            ));
        }
        entry.platform_responded_at_utc_ms = Some(
            entry
                .platform_responded_at_utc_ms
                .map_or(part.effective_processing_at_utc_ms(), |at| {
                    at.max(part.effective_processing_at_utc_ms())
                }),
        );
        *slot = Some(part);
        Ok(())
    }

    fn complete_draft(
        &mut self,
        global_request_id: &str,
        context: RequestContextV1,
        transit: &RequestTransitV1,
        rule_snapshot_sha256: Sha256Hex,
        scenario_sha256: Sha256Hex,
        fee_rule: SubmissionFeeRuleIdentityV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let entry = self.entries.get_mut(global_request_id).ok_or_else(|| {
            SimExecutionContractErrorV1::RequestNotFound(global_request_id.to_owned())
        })?;
        if entry.draft.is_some() {
            return Ok(());
        }
        let platform_responded_at_utc_ms = transit
            .platform_responded_at_utc_ms
            .ok_or(SimExecutionContractErrorV1::ResponseBeforeLogicalCompletion)?;
        let response_received_at_utc_ms = transit
            .response_received_at_utc_ms
            .ok_or(SimExecutionContractErrorV1::ResponseBeforeLogicalCompletion)?;
        let private_emission_sequence = transit
            .private_emission_sequence
            .ok_or(SimExecutionContractErrorV1::ResponseBeforeLogicalCompletion)?;
        if entry.platform_responded_at_utc_ms != Some(platform_responded_at_utc_ms)
            || entry.parts.iter().any(Option::is_none)
        {
            return Err(SimExecutionContractErrorV1::ResponseBeforeLogicalCompletion);
        }
        let parts = entry
            .parts
            .iter()
            .cloned()
            .map(|part| part.expect("all response part slots were checked above"))
            .collect();
        entry.draft = Some(DirectResponseDraftV1 {
            method: entry.method,
            delivery_id: entry.delivery_id.clone(),
            context,
            platform_responded_at_utc_ms,
            response_received_at_utc_ms,
            private_emission_sequence,
            rule_snapshot_sha256,
            scenario_sha256,
            fee_rule,
            parts,
        });
        Ok(())
    }

    fn draft(&self, global_request_id: &str) -> Option<&DirectResponseDraftV1> {
        self.entries.get(global_request_id)?.draft.as_ref()
    }

    fn platform_responded_at(&self, global_request_id: &str) -> Option<i64> {
        self.entries
            .get(global_request_id)?
            .platform_responded_at_utc_ms
    }

    fn sealed(&self, global_request_id: &str) -> Option<&SealedDirectResponseV1> {
        self.entries.get(global_request_id)?.sealed.as_ref()
    }

    fn cache_sealed(
        &mut self,
        global_request_id: &str,
        response: SealedDirectResponseV1,
    ) -> Result<(), SimExecutionContractErrorV1> {
        let entry = self.entries.get_mut(global_request_id).ok_or_else(|| {
            SimExecutionContractErrorV1::RequestNotFound(global_request_id.to_owned())
        })?;
        if let Some(existing) = &entry.sealed {
            if existing != &response {
                return Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict);
            }
            return Ok(());
        }
        entry.sealed = Some(response);
        Ok(())
    }

    fn validate_remove_confirmed(&self, global_request_id: &str) -> Result<(), SessionError> {
        self.entries
            .contains_key(global_request_id)
            .then_some(())
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.to_owned()))
    }

    fn remove_prevalidated_confirmed(&mut self, global_request_id: &str) {
        debug_assert!(self.validate_remove_confirmed(global_request_id).is_ok());
        self.entries
            .remove(global_request_id)
            .expect("已验证直接响应必须仍在载荷池中");
    }
}

impl PendingDomainRequestV1 {
    fn context(&self) -> &RequestContextV1 {
        match self {
            Self::PostOrder(request) => &request.context,
            Self::PostOrders(request) => &request.context,
            Self::CancelOrder(request) => &request.context,
            Self::CancelOrders(request) => &request.context,
            Self::CancelAllOrders(request) => &request.context,
            Self::Order(request) => &request.context,
            Self::Orders(request) => &request.context,
            Self::Trades(request) => &request.context,
        }
    }

    fn direct_response_method(&self) -> DirectResponseMethodV1 {
        match self {
            Self::PostOrder(_) => DirectResponseMethodV1::PostOrder,
            Self::PostOrders(_) => DirectResponseMethodV1::PostOrders,
            Self::CancelOrder(_) => DirectResponseMethodV1::CancelOrder,
            Self::CancelOrders(_) => DirectResponseMethodV1::CancelOrders,
            Self::CancelAllOrders(_) => DirectResponseMethodV1::CancelAllOrders,
            Self::Order(_) => DirectResponseMethodV1::Order,
            Self::Orders(_) => DirectResponseMethodV1::Orders,
            Self::Trades(_) => DirectResponseMethodV1::Trades,
        }
    }

    fn action_count(&self) -> usize {
        match self {
            Self::PostOrders(request) => request.items.len(),
            Self::CancelOrders(request) => request
                .items
                .iter()
                .map(|item| item.platform_order_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            Self::PostOrder(_)
            | Self::CancelOrder(_)
            | Self::CancelAllOrders(_)
            | Self::Order(_)
            | Self::Orders(_)
            | Self::Trades(_) => 1,
        }
    }

    fn method_tag(&self) -> &'static str {
        match self {
            Self::PostOrder(_) => "post_order",
            Self::PostOrders(_) => "post_orders",
            Self::CancelOrder(_) => "cancel_order",
            Self::CancelOrders(_) => "cancel_orders",
            Self::CancelAllOrders(_) => "cancel_all_orders",
            Self::Order(_) => "order",
            Self::Orders(_) => "orders",
            Self::Trades(_) => "trades",
        }
    }

    fn candidate_market_use(&self) -> (BTreeSet<String>, BTreeSet<String>) {
        let mut condition_ids = BTreeSet::new();
        let mut token_ids = BTreeSet::new();
        match self {
            Self::PostOrder(request) => {
                condition_ids.insert(request.item.order.condition_id.0.clone());
            }
            Self::PostOrders(request) => {
                condition_ids.extend(
                    request
                        .items
                        .iter()
                        .map(|item| item.order.condition_id.0.clone()),
                );
            }
            Self::Orders(request) => {
                if let Some(condition_id) = &request.filter.condition_id {
                    condition_ids.insert(condition_id.0.clone());
                } else if let Some(token_id) = &request.filter.token_id {
                    token_ids.insert(token_id.0.clone());
                }
            }
            Self::Trades(request) => {
                if let Some(condition_id) = &request.filter.condition_id {
                    condition_ids.insert(condition_id.0.clone());
                } else if let Some(token_id) = &request.filter.token_id {
                    token_ids.insert(token_id.0.clone());
                }
            }
            Self::CancelOrder(_)
            | Self::CancelOrders(_)
            | Self::CancelAllOrders(_)
            | Self::Order(_) => {}
        }
        (condition_ids, token_ids)
    }
}

fn method_action(
    stable_item_identity: &str,
    batch_item_ordinal: u16,
    method_timing: ParsedMethodTimingV1,
) -> ParsedLogicalActionV1 {
    ParsedLogicalActionV1 {
        action_identity: stable_item_identity.to_owned(),
        batch_item_ordinal,
        method_timing,
    }
}

fn registration_receipt(schedule: &RegisteredLogicalScheduleV1) -> RequestRegistrationReceiptV1 {
    RequestRegistrationReceiptV1 {
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        global_request_id: schedule
            .actions
            .first()
            .expect("登记调度至少包含一个动作")
            .global_request_id
            .clone(),
        disposition: schedule.request.disposition,
        transit: schedule.request.transit.clone(),
        scheduled_action_count: u16::try_from(schedule.actions.len())
            .expect("领域入口批量上限适合 u16"),
    }
}

fn registered_item(
    item_id: String,
    scheduled_action_ordinal: u16,
    schedule: &RegisteredLogicalScheduleV1,
) -> RegisteredItemV1 {
    let action = schedule
        .actions
        .get(usize::from(scheduled_action_ordinal))
        .expect("登记项映射必须指向已调度动作");
    RegisteredItemV1 {
        item_id,
        scheduled_action_ordinal,
        effective_processing_at_utc_ms: action.key.effective_processing_at_utc_ms,
    }
}

fn validate_idempotency_key(key: &str) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(key, "变更幂等键为空")
}

fn validate_post_item(item: &PostOrderItemV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(&item.item_id, "提交稳定项身份为空")?;
    validate_nonempty(&item.order.condition_id.0, "提交市场身份为空")?;
    validate_nonempty(&item.order.token_id.0, "提交资产身份为空")?;
    match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            ..
        } => validate_positive_order_amounts(*limit_price_pusd_per_share, *size_shares),
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            expires_at_utc_ms,
            ..
        } => {
            validate_positive_order_amounts(*limit_price_pusd_per_share, *size_shares)?;
            if *expires_at_utc_ms <= 0 {
                return Err(SimExecutionContractErrorV1::InvalidRequest(
                    "GTD 到期时点必须为正整数毫秒",
                ));
            }
            Ok(())
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        }
        | SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => validate_positive_order_amounts(*spend_pusd, *worst_price_pusd_per_share),
    }
}

fn validate_positive_order_amounts(
    primary: Decimal,
    price: Decimal,
) -> Result<(), SimExecutionContractErrorV1> {
    if primary <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "订单金额、份额和价格必须为正数",
        ));
    }
    Ok(())
}

fn validate_cancel_item(item: &CancelOrderItemV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(&item.item_id, "撤单稳定项身份为空")?;
    validate_platform_order_id(&item.platform_order_id)
}

fn validate_platform_order_id(value: &str) -> Result<(), SimExecutionContractErrorV1> {
    if value.len() != 64
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "模拟平台订单 ID 必须为 64 位小写十六进制",
        ));
    }
    Ok(())
}

fn validate_batch_len(
    actual: usize,
    maximum: usize,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if actual == 0 || actual > maximum {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn validate_unique_item_ids<'a>(
    item_ids: impl Iterator<Item = &'a str>,
) -> Result<(), SimExecutionContractErrorV1> {
    let mut seen = BTreeSet::new();
    for item_id in item_ids {
        validate_nonempty(item_id, "批量稳定项身份为空")?;
        if !seen.insert(item_id) {
            return Err(SimExecutionContractErrorV1::InvalidRequest(
                "批量稳定项身份重复",
            ));
        }
    }
    Ok(())
}

fn validate_orders_request(request: &OrdersRequestV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_optional_identity(
        request.filter.condition_id.as_ref().map(|id| id.0.as_str()),
        "订单列表市场过滤身份为空",
    )?;
    validate_optional_identity(
        request.filter.token_id.as_ref().map(|id| id.0.as_str()),
        "订单列表资产过滤身份为空",
    )?;
    validate_optional_identity(
        request.cursor.as_ref().map(OpaquePageCursorV1::as_str),
        "订单列表游标为空",
    )
}

fn validate_trades_request(request: &TradesRequestV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_optional_identity(request.filter.trade_id.as_deref(), "成交过滤身份为空")?;
    validate_optional_identity(
        request.filter.condition_id.as_ref().map(|id| id.0.as_str()),
        "成交列表市场过滤身份为空",
    )?;
    validate_optional_identity(
        request.filter.token_id.as_ref().map(|id| id.0.as_str()),
        "成交列表资产过滤身份为空",
    )?;
    validate_optional_identity(
        request.cursor.as_ref().map(OpaquePageCursorV1::as_str),
        "成交列表游标为空",
    )?;
    if request
        .filter
        .after_utc_ms
        .into_iter()
        .chain(request.filter.before_utc_ms)
        .any(|value| value < 0)
    {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "成交过滤时点不得为负数",
        ));
    }
    if matches!(
        (request.filter.after_utc_ms, request.filter.before_utc_ms),
        (Some(after), Some(before)) if after > before
    ) {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "成交过滤起点不得晚于终点",
        ));
    }
    Ok(())
}

fn validate_optional_identity(
    value: Option<&str>,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if value.is_some_and(str::is_empty) {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn validate_nonempty(
    value: &str,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if value.is_empty() {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn canonical_request_sha256(request: &PendingDomainRequestV1) -> Sha256Hex {
    let mut hash = CanonicalHasher::new(CANONICAL_REQUEST_HASH_DOMAIN);
    hash.text(request.method_tag());
    hash_context(&mut hash, request.context());
    hash_request_payload(&mut hash, request, true);
    hash.finish()
}

fn mutation_binding(request: &PendingDomainRequestV1) -> Option<(String, Sha256Hex)> {
    let idempotency_key = match request {
        PendingDomainRequestV1::PostOrder(request) => &request.idempotency_key,
        PendingDomainRequestV1::PostOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelOrder(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelAllOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::Order(_)
        | PendingDomainRequestV1::Orders(_)
        | PendingDomainRequestV1::Trades(_) => return None,
    };
    let mut hash = CanonicalHasher::new(MUTATION_PAYLOAD_HASH_DOMAIN);
    hash.text(request.method_tag());
    hash_request_payload(&mut hash, request, false);
    Some((idempotency_key.clone(), hash.finish()))
}

fn hash_context(hash: &mut CanonicalHasher, context: &RequestContextV1) {
    hash.text(&context.execution_contract_version);
    hash.text(&context.session_id);
    hash.text(&context.account_id);
    hash.text(match context.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&context.caller_instance_id);
    hash.text(&context.global_request_id);
    hash.optional_text(context.caller_correlation_id.as_deref());
    hash.u64(context.caller_sequence);
    hash.i64(context.request_sent_at_utc_ms);
    hash.text(&context.model_visible_cursor.session_id);
    hash.optional_i64(
        context
            .model_visible_cursor
            .public_visibility
            .closed_through_received_at_utc_ms(),
    );
    hash.u64(
        context
            .model_visible_cursor
            .public_visibility
            .next_visibility_index_position(),
    );
    hash.u64(context.model_visible_cursor.private_feedback_sequence);
}

fn hash_request_payload(
    hash: &mut CanonicalHasher,
    request: &PendingDomainRequestV1,
    include_idempotency_key: bool,
) {
    match request {
        PendingDomainRequestV1::PostOrder(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash_post_item(hash, &request.item);
        }
        PendingDomainRequestV1::PostOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash.u64(request.items.len() as u64);
            for item in &request.items {
                hash_post_item(hash, item);
            }
        }
        PendingDomainRequestV1::CancelOrder(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash_cancel_item(hash, &request.item);
        }
        PendingDomainRequestV1::CancelOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash.u64(request.items.len() as u64);
            for item in &request.items {
                hash_cancel_item(hash, item);
            }
        }
        PendingDomainRequestV1::CancelAllOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
        }
        PendingDomainRequestV1::Order(request) => hash.text(&request.platform_order_id),
        PendingDomainRequestV1::Orders(request) => {
            hash.optional_text(request.filter.condition_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.filter.token_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
        PendingDomainRequestV1::Trades(request) => {
            hash.optional_text(request.filter.trade_id.as_deref());
            hash.optional_text(request.filter.condition_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.filter.token_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_i64(request.filter.after_utc_ms);
            hash.optional_i64(request.filter.before_utc_ms);
            hash.optional_text(request.cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
    }
}

fn hash_optional_idempotency(hash: &mut CanonicalHasher, key: &str, include: bool) {
    if include {
        hash.text(key);
    }
}

fn hash_post_item(hash: &mut CanonicalHasher, item: &PostOrderItemV1) {
    hash.text(&item.item_id);
    hash.text(&item.order.condition_id.0);
    hash.text(&item.order.token_id.0);
    match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => {
            hash.text("GTC_LIMIT");
            hash.decimal(*limit_price_pusd_per_share);
            hash.decimal(*size_shares);
            hash.boolean(*post_only);
        }
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            expires_at_utc_ms,
        } => {
            hash.text("GTD_LIMIT");
            hash.decimal(*limit_price_pusd_per_share);
            hash.decimal(*size_shares);
            hash.boolean(*post_only);
            hash.i64(*expires_at_utc_ms);
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            hash.text("FOK_MARKET");
            hash.decimal(*spend_pusd);
            hash.decimal(*worst_price_pusd_per_share);
        }
        SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            hash.text("FAK_MARKET");
            hash.decimal(*spend_pusd);
            hash.decimal(*worst_price_pusd_per_share);
        }
    }
}

fn hash_cancel_item(hash: &mut CanonicalHasher, item: &CancelOrderItemV1) {
    hash.text(&item.item_id);
    hash.text(&item.platform_order_id);
}

struct CanonicalHasher {
    digest: Sha256,
    encoded_len: u64,
}

impl CanonicalHasher {
    fn new(domain: &str) -> Self {
        let mut hash = Self {
            digest: Sha256::new(),
            encoded_len: 0,
        };
        hash.text(domain);
        hash
    }

    fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.encoded_len = self
            .encoded_len
            .checked_add(8)
            .and_then(|length| length.checked_add(value.len() as u64))
            .expect("规范哈希输入长度必须适合 u64");
        self.digest.update((value.len() as u64).to_be_bytes());
        self.digest.update(value);
    }

    fn boolean(&mut self, value: bool) {
        self.bytes(&[u8::from(value)]);
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_be_bytes());
    }

    fn decimal(&mut self, value: Decimal) {
        self.text(&value.normalize().to_string());
    }

    fn optional_text(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.boolean(true);
                self.text(value);
            }
            None => self.boolean(false),
        }
    }

    fn optional_i64(&mut self, value: Option<i64>) {
        match value {
            Some(value) => {
                self.boolean(true);
                self.i64(value);
            }
            None => self.boolean(false),
        }
    }

    fn encoded_len(&self) -> u64 {
        self.encoded_len
    }

    fn finish(self) -> Sha256Hex {
        let mut encoded = String::with_capacity(64);
        for byte in self.digest.finalize() {
            write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
        }
        Sha256Hex(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{
        SimulationDeterministicScriptContractV1, SimulationRunArtifactErrorV1,
        SimulationWorkloadKindV1,
    };
    use crate::finality::successful_policy_for_tests;
    use crate::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};
    use crate::platform_rules::tests::successful_snapshot_for_sim_tests;

    fn resource_policy_for_tests() -> (SimulationResourcePolicyV1, Sha256Hex) {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let sha256 = policy.content_sha256().expect("计算测试资源策略摘要");
        (policy, sha256)
    }

    fn projection_evidence_for_tests(
        fact: DurableTransactionFactV1,
    ) -> DurableTransactionEvidenceV1 {
        let mut evidence = DurableTransactionEvidenceV1::new(fact);
        evidence.resource_usage = Some(SimulationResourceUsageV1::default());
        evidence
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_completion_facts_bind_role_and_cannot_certify_missing_run_closure() {
        let rules = successful_snapshot_for_sim_tests().await;
        let implementation_identity = rules.snapshot().provenance.implementation_identity.clone();
        let run_intent_binding = SimulationRunIntentBindingV1::try_new(
            implementation_identity,
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造组件测试运行意图");
        let source_identity = Sha256Hex("d".repeat(64));
        let (resource_policy, resource_policy_sha256) = resource_policy_for_tests();
        let scenario = scenario_for_sim_tests();
        let mut process = SimExecutionProcessV1::try_new(
            "component-test-run".to_owned(),
            successful_policy_for_tests(),
            Arc::clone(&rules),
            load_embedded_proxy_latency_profile().expect("加载组件时延配置档"),
            Arc::clone(&scenario),
        )
        .expect("创建组件测试运行根");
        let spec = session_spec();
        let prepared = process
            .create_session(spec.clone(), accepting_submission_response_bridge())
            .expect("创建训练会话");
        let activation = ActivateSessionRequestV1 {
            session_id: spec.session_id.clone(),
            account_instance_key: prepared.control.account_instance_key.clone(),
            opening_anchor_id: prepared.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: prepared.control.opening_anchor.anchor_sha256.clone(),
            activation_id: "component-test-artifact-activation".to_owned(),
        };
        let activated = process
            .activate_session(activation.clone())
            .expect("激活训练会话");

        let mut projection = DurableEvidenceProjectionV1::for_artifacts();
        projection
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::RunCreated {
                    run_id: process.run_id().to_owned(),
                    market_event_source_identity_sha256: source_identity.clone(),
                    run_intent_binding: run_intent_binding.clone(),
                    resource_policy: resource_policy.clone(),
                    resource_policy_sha256: resource_policy_sha256.clone(),
                },
            ))
            .expect("投影运行创建事实");
        projection
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::SessionCreated {
                    session_spec: spec.clone(),
                    opening_anchor: Box::new(prepared.control.opening_anchor.clone()),
                    initialization: prepared.control.initialization.clone(),
                },
            ))
            .expect("投影会话创建事实");
        projection
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::SessionActivated {
                    request: activation,
                    result: activated.snapshot,
                },
            ))
            .expect("投影会话激活事实");
        let mut role_mismatch = projection;
        assert!(matches!(
            role_mismatch.apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::SessionRetentionExpired {
                    session_id: spec.session_id.clone(),
                },
            )),
            Err(SimExecutionPersistenceErrorV1::Session(message))
                if message.contains("角色或初始化终态冲突")
        ));

        let mut incomplete = role_mismatch;
        incomplete
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::SessionCleanRunCompleted {
                    session_id: spec.session_id.clone(),
                },
            ))
            .expect("角色匹配的完成事实可以进入耐久投影");
        process
            .inner
            .retire_session(&spec.session_id)
            .expect("构造已删除会话正文的错误写入器结果");
        let checkpoint = DurableRunCheckpointV1 {
            schema_version: DURABLE_RUN_CHECKPOINT_SCHEMA_V1.to_owned(),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_source_identity_sha256: source_identity,
            run_intent_binding,
            resource_policy,
            resource_policy_sha256,
            component_golden_measurement_sha256: None,
            resource_high_water: SimulationResourceHighWaterV1::default(),
            run_resource_admission: ResourceAdmissionStateV1::default(),
            session_resource_admission: BTreeMap::new(),
            lifecycle: DurableRunLifecycleV1::Open,
            process_root: process.inner.checkpoint().expect("形成错误检查点"),
            sessions: BTreeMap::new(),
        };
        assert!(matches!(
            validate_run_artifact_seal_readiness(&process.inner, &checkpoint, &incomplete),
            Err(SimulationRunArtifactErrorV1::Integrity(message))
                if message.contains("输入路径")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paper_completion_fact_cannot_certify_unpruned_query_retention() {
        let rules = successful_snapshot_for_sim_tests().await;
        let implementation_identity = rules.snapshot().provenance.implementation_identity.clone();
        let run_intent_binding = SimulationRunIntentBindingV1::try_new(
            implementation_identity,
            SimulationWorkloadKindV1::RealVertical,
            SimulationDeterministicScriptContractV1::RealVerticalV1,
        )
        .expect("构造组件测试运行意图");
        let source_identity = Sha256Hex("d".repeat(64));
        let (resource_policy, resource_policy_sha256) = resource_policy_for_tests();
        let scenario = scenario_for_sim_tests();
        let mut process = SimExecutionProcessV1::try_new(
            "component-test-run".to_owned(),
            successful_policy_for_tests(),
            Arc::clone(&rules),
            load_embedded_proxy_latency_profile().expect("加载组件时延配置档"),
            Arc::clone(&scenario),
        )
        .expect("创建组件测试运行根");
        let mut spec = session_spec();
        spec.caller_role = SimCallerRoleV1::Paper;
        let prepared = process
            .create_session(spec.clone(), accepting_submission_response_bridge())
            .expect("创建模拟盘会话");
        let activation = ActivateSessionRequestV1 {
            session_id: spec.session_id.clone(),
            account_instance_key: prepared.control.account_instance_key.clone(),
            opening_anchor_id: prepared.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: prepared.control.opening_anchor.anchor_sha256.clone(),
            activation_id: "component-test-paper-artifact-activation".to_owned(),
        };
        let activated = process
            .activate_session(activation.clone())
            .expect("激活模拟盘会话");

        let mut projection = DurableEvidenceProjectionV1::for_artifacts();
        for fact in [
            DurableTransactionFactV1::RunCreated {
                run_id: process.run_id().to_owned(),
                market_event_source_identity_sha256: source_identity.clone(),
                run_intent_binding: run_intent_binding.clone(),
                resource_policy: resource_policy.clone(),
                resource_policy_sha256: resource_policy_sha256.clone(),
            },
            DurableTransactionFactV1::SessionCreated {
                session_spec: spec.clone(),
                opening_anchor: Box::new(prepared.control.opening_anchor.clone()),
                initialization: prepared.control.initialization.clone(),
            },
            DurableTransactionFactV1::SessionActivated {
                request: activation,
                result: activated.snapshot,
            },
        ] {
            projection
                .apply(&projection_evidence_for_tests(fact))
                .expect("投影模拟盘前驱事实");
        }
        let session_id = spec.session_id.clone();
        projection.last_platform_buckets.insert(
            session_id.clone(),
            BTreeMap::from([(
                MarketEventPathV1::PublicVisibility,
                DurableMarketBucketReferenceV1 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 2_000,
                    start_index_position: 0,
                    next_index_position: 0,
                    closure: MillisecondBucketClosureV1::EndOfInput,
                    bucket_sha256: Sha256Hex("a".repeat(64)),
                },
            )]),
        );
        let condition_id = scenario.market_set().markets()[0]
            .market()
            .condition_id
            .0
            .clone();
        projection.evicted_markets.insert(
            session_id.clone(),
            BTreeMap::from([(
                condition_id.clone(),
                (
                    BTreeMap::new(),
                    BTreeMap::new(),
                    TerminalExecutionMarketEvictionV1 {
                        condition_id,
                        fills: BTreeMap::new(),
                    },
                ),
            )]),
        );
        projection
            .query_retention
            .insert(session_id.clone(), (2, Some(0), Some(0)));
        projection
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::SessionRetentionExpired {
                    session_id: session_id.clone(),
                },
            ))
            .expect("错误写入器形成角色匹配的完成事实");
        process
            .inner
            .retire_session(&session_id)
            .expect("构造已删除会话正文的错误写入器结果");
        let checkpoint = DurableRunCheckpointV1 {
            schema_version: DURABLE_RUN_CHECKPOINT_SCHEMA_V1.to_owned(),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_source_identity_sha256: source_identity,
            run_intent_binding,
            resource_policy,
            resource_policy_sha256,
            component_golden_measurement_sha256: None,
            resource_high_water: SimulationResourceHighWaterV1::default(),
            run_resource_admission: ResourceAdmissionStateV1::default(),
            session_resource_admission: BTreeMap::new(),
            lifecycle: DurableRunLifecycleV1::Open,
            process_root: process.inner.checkpoint().expect("形成错误检查点"),
            sessions: BTreeMap::new(),
        };

        let error = validate_run_artifact_seal_readiness(&process.inner, &checkpoint, &projection)
            .expect_err("未完全裁剪查询状态的完成事实不能生成产物");
        assert!(
            matches!(&error, SimulationRunArtifactErrorV1::Integrity(message)
                if message.contains("查询保留水位")),
            "实际错误：{error}"
        );
    }

    #[test]
    fn unknown_platform_status_is_hash_domain_separated_from_known_status() {
        let mut known = CanonicalHasher::new("TEST_SUBMISSION_STATUS_V1");
        hash_submission_platform_order_status(&mut known, &SubmissionPlatformOrderStatusV1::Live);
        let mut unknown = CanonicalHasher::new("TEST_SUBMISSION_STATUS_V1");
        hash_submission_platform_order_status(
            &mut unknown,
            &SubmissionPlatformOrderStatusV1::Unknown("LIVE".to_owned()),
        );

        assert_ne!(known.finish(), unknown.finish());
    }

    #[test]
    fn query_status_mapping_preserves_unknown_raw_values() {
        assert_eq!(
            simulated_order_status("ORDER_STATUS_NEW_FROM_PLATFORM"),
            SimulatedOrderStatusV1 {
                raw: "ORDER_STATUS_NEW_FROM_PLATFORM".to_owned(),
                known: None,
            }
        );
        assert_eq!(
            simulated_trade_status("TRADE_STATUS_NEW_FROM_PLATFORM"),
            SimulatedTradeStatusV1 {
                raw: "TRADE_STATUS_NEW_FROM_PLATFORM".to_owned(),
                known: None,
            }
        );
        assert_eq!(
            simulated_order_status("LIVE").known,
            Some(SimulatedOrderKnownStatusV1::Live)
        );
        assert_eq!(
            simulated_trade_status("MATCHED").known,
            Some(SimulatedTradeKnownStatusV1::Matched)
        );
    }

    #[test]
    fn non_submission_response_hash_binds_the_result_and_fee_rule() {
        let draft = DirectResponseDraftV1 {
            method: DirectResponseMethodV1::Order,
            delivery_id: "delivery".to_owned(),
            context: RequestContextV1 {
                execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                session_id: "session".to_owned(),
                account_id: "account".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller".to_owned(),
                global_request_id: "request".to_owned(),
                caller_correlation_id: Some("correlation".to_owned()),
                caller_sequence: 1,
                request_sent_at_utc_ms: 10,
                model_visible_cursor: ModelVisibleCursorV1 {
                    session_id: "session".to_owned(),
                    public_visibility: Default::default(),
                    private_feedback_sequence: 0,
                },
            },
            platform_responded_at_utc_ms: 20,
            response_received_at_utc_ms: 30,
            private_emission_sequence: 1,
            rule_snapshot_sha256: Sha256Hex("a".repeat(64)),
            scenario_sha256: Sha256Hex("b".repeat(64)),
            fee_rule: SubmissionFeeRuleIdentityV1 {
                formula_version: "fee-v1".to_owned(),
                aggregation_version: SUBMISSION_FEE_AGGREGATION_V1.to_owned(),
                precision_decimal_places: 5,
                minimum_nonzero_fee_pusd: Decimal::new(1, 5),
                builder_fee_pusd: Decimal::ZERO,
            },
            parts: vec![DirectResponsePartV1::Order {
                effective_processing_at_utc_ms: 20,
                result: OrderQueryResultV1::NotFound,
            }],
        };
        let baseline = hash_non_submission_response(&draft, 1);
        let mut changed_result = draft.clone();
        changed_result.parts = vec![DirectResponsePartV1::Order {
            effective_processing_at_utc_ms: 20,
            result: OrderQueryResultV1::OutOfRetention(QueryOutOfRetentionV1 {
                scope: QueryRetentionScopeV1::OrderHistory,
                retained_from_utc_ms: Some(10),
                requested_root_sequence: None,
                earliest_retained_root_sequence: 1,
            }),
        }];
        let mut changed_fee = draft.clone();
        changed_fee.fee_rule.precision_decimal_places = 6;

        assert_ne!(baseline, hash_non_submission_response(&changed_result, 1));
        assert_ne!(baseline, hash_non_submission_response(&changed_fee, 1));

        let order = OrderProjectionV1 {
            platform_order_id: "1".repeat(64),
            global_request_id: "source-request".to_owned(),
            item_id: "source-item".to_owned(),
            condition_id: ConditionId("condition".to_owned()),
            token_id: TokenId("token".to_owned()),
            created_at_utc_ms: 10,
            last_updated_at_utc_ms: 10,
            kind: OrderKindProjectionV1::Gtc,
            expires_at_utc_ms: None,
            source_price_pusd_per_share: Decimal::new(4, 1),
            status: simulated_order_status("LIVE"),
            accepted_base_quantity_shares: Some(Decimal::ONE),
            matched_base_quantity_shares: Decimal::ZERO,
            pending_maker_depletion_base_quantity_shares: Decimal::ZERO,
            open_base_quantity_shares: Decimal::ONE,
            venue_dust_closed_base_quantity_shares: Decimal::ZERO,
            market_resolved_canceled_base_quantity_shares: Decimal::ZERO,
            canceled_base_quantity_shares: Decimal::ZERO,
            expired_base_quantity_shares: Decimal::ZERO,
            accepted_quote_amount_pusd: Decimal::new(4, 1),
            matched_quote_amount_pusd: Decimal::ZERO,
            open_encumbered_quote_amount_pusd: Decimal::new(4, 1),
            venue_dust_released_quote_amount_pusd: Decimal::ZERO,
            market_resolved_released_quote_amount_pusd: Decimal::ZERO,
            released_price_improvement_quote_amount_pusd: Decimal::ZERO,
            canceled_released_quote_amount_pusd: Decimal::ZERO,
            expired_released_quote_amount_pusd: Decimal::ZERO,
            immediate_taker_fee_encumbered_pusd: Decimal::ZERO,
        };
        let mut orders_draft = draft.clone();
        orders_draft.method = DirectResponseMethodV1::Orders;
        orders_draft.parts = vec![DirectResponsePartV1::Orders {
            effective_processing_at_utc_ms: 20,
            result: OrdersQueryResultV1::Page(OrdersPageV1 {
                root_identity: "root".to_owned(),
                query_projection_revision: 7,
                items: vec![order],
                next_cursor: Some(OpaquePageCursorV1("cursor-a".to_owned())),
            }),
        }];
        let orders_baseline = hash_non_submission_response(&orders_draft, 1);
        let mut changed_item = orders_draft.clone();
        let DirectResponsePartV1::Orders {
            result: OrdersQueryResultV1::Page(page),
            ..
        } = &mut changed_item.parts[0]
        else {
            unreachable!("测试订单页形状固定")
        };
        page.items[0].item_id = "different-source-item".to_owned();
        let mut changed_cursor = orders_draft.clone();
        let DirectResponsePartV1::Orders {
            result: OrdersQueryResultV1::Page(page),
            ..
        } = &mut changed_cursor.parts[0]
        else {
            unreachable!("测试订单页形状固定")
        };
        page.next_cursor = Some(OpaquePageCursorV1("cursor-b".to_owned()));
        let mut changed_revision = orders_draft.clone();
        let DirectResponsePartV1::Orders {
            result: OrdersQueryResultV1::Page(page),
            ..
        } = &mut changed_revision.parts[0]
        else {
            unreachable!("测试订单页形状固定")
        };
        page.query_projection_revision += 1;
        let mut changed_root = orders_draft.clone();
        let DirectResponsePartV1::Orders {
            result: OrdersQueryResultV1::Page(page),
            ..
        } = &mut changed_root.parts[0]
        else {
            unreachable!("测试订单页形状固定")
        };
        page.root_identity = "different-root".to_owned();

        assert_ne!(
            orders_baseline,
            hash_non_submission_response(&changed_item, 1)
        );
        assert_ne!(
            orders_baseline,
            hash_non_submission_response(&changed_cursor, 1)
        );
        assert_ne!(
            orders_baseline,
            hash_non_submission_response(&changed_revision, 1)
        );
        assert_ne!(
            orders_baseline,
            hash_non_submission_response(&changed_root, 1)
        );

        let mut trades_draft = draft.clone();
        trades_draft.method = DirectResponseMethodV1::Trades;
        trades_draft.parts = vec![DirectResponsePartV1::Trades {
            effective_processing_at_utc_ms: 20,
            result: TradesQueryResultV1::Page(TradesPageV1 {
                root_identity: "trade-root".to_owned(),
                query_projection_revision: 8,
                items: vec![TradeProjectionV1 {
                    simulated_trade_id: "2".repeat(64),
                    platform_order_id: "1".repeat(64),
                    condition_id: ConditionId("condition".to_owned()),
                    token_id: TokenId("token".to_owned()),
                    matched_at_utc_ms: 11,
                    status: simulated_trade_status("MATCHED"),
                    liquidity_role: TradeLiquidityRoleV1::Taker,
                    source_price_pusd_per_share: Decimal::new(4, 1),
                    base_quantity_shares: Decimal::ONE,
                    quote_amount_pusd: Decimal::new(4, 1),
                    immediate_taker_fee_pusd: Decimal::ZERO,
                    economic_disposition: TradeEconomicDispositionV1::PendingUnapplied,
                }],
                next_cursor: None,
            }),
        }];
        let trades_baseline = hash_non_submission_response(&trades_draft, 1);
        let mut changed_trade = trades_draft.clone();
        let DirectResponsePartV1::Trades {
            result: TradesQueryResultV1::Page(page),
            ..
        } = &mut changed_trade.parts[0]
        else {
            unreachable!("测试成交页形状固定")
        };
        page.items[0].quote_amount_pusd = Decimal::new(5, 1);
        assert_ne!(
            trades_baseline,
            hash_non_submission_response(&changed_trade, 1)
        );
    }

    #[test]
    fn query_root_and_cursor_identities_bind_the_retention_floor_at_issue() {
        let spec = session_spec();
        let filter_sha256 = orders_filter_sha256(&OrdersFilterV1::default());
        let mut first = QueryStateV1::new();
        first.next_root_sequence = 3;
        first.earliest_retained_root_sequence = 1;
        let first_page = first
            .new_orders_page(&spec, filter_sha256.clone(), 7, Vec::new())
            .expect("第一保留水位根");
        let mut second = QueryStateV1::new();
        second.next_root_sequence = 3;
        second.earliest_retained_root_sequence = 2;
        let second_page = second
            .new_orders_page(&spec, filter_sha256.clone(), 7, Vec::new())
            .expect("第二保留水位根");
        assert_ne!(first_page.root_identity, second_page.root_identity);

        let binding = IssuedCursorV1 {
            root_sequence: 3,
            root_identity: Sha256Hex("a".repeat(64)),
            method: ListQueryMethodV1::Orders,
            filter_sha256,
            query_projection_revision: 7,
            retention_floor_at_issue: 1,
            next_index: SIM_QUERY_PAGE_SIZE_V1,
        };
        let first_cursor = QueryStateV1::new().issue_cursor(binding.clone());
        let second_cursor = QueryStateV1::new().issue_cursor(IssuedCursorV1 {
            retention_floor_at_issue: 2,
            ..binding
        });
        assert_ne!(first_cursor, second_cursor);
    }

    #[test]
    fn recovered_query_sources_bind_small_roots_and_out_of_retention_pages() {
        let spec = session_spec();
        let context = RequestContextV1 {
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            session_id: spec.session_id.clone(),
            account_id: spec.account_id.clone(),
            caller_role: spec.caller_role,
            caller_instance_id: spec.caller_instance_id.clone(),
            global_request_id: "query-source-request".to_owned(),
            caller_correlation_id: None,
            caller_sequence: 1,
            request_sent_at_utc_ms: 10,
            model_visible_cursor: ModelVisibleCursorV1 {
                session_id: spec.session_id.clone(),
                public_visibility: Default::default(),
                private_feedback_sequence: 0,
            },
        };
        let initial_request = OrdersRequestV1 {
            context: context.clone(),
            filter: OrdersFilterV1::default(),
            cursor: None,
        };
        let mut query_state = QueryStateV1::new();
        let page = query_state
            .new_orders_page(
                &spec,
                orders_filter_sha256(&initial_request.filter),
                7,
                Vec::new(),
            )
            .expect("形成无后续游标的小查询页");
        assert!(
            query_state
                .validate_initial_orders_page(&spec, 1, &initial_request, 7, 1, &page,)
                .is_ok()
        );
        let mut changed_page = page.clone();
        changed_page.root_identity = "f".repeat(64);
        assert!(
            query_state
                .validate_initial_orders_page(&spec, 1, &initial_request, 7, 1, &changed_page,)
                .is_err()
        );

        let binding = IssuedCursorV1 {
            root_sequence: 1,
            root_identity: Sha256Hex("a".repeat(64)),
            method: ListQueryMethodV1::Orders,
            filter_sha256: orders_filter_sha256(&OrdersFilterV1::default()),
            query_projection_revision: 7,
            retention_floor_at_issue: 1,
            next_index: SIM_QUERY_PAGE_SIZE_V1,
        };
        let input_cursor = query_state.issue_cursor(binding.clone());
        query_state
            .continuation_by_request
            .insert("query-source-request".to_owned(), binding);
        let continuation_request = PendingDomainRequestV1::Orders(OrdersRequestV1 {
            context,
            filter: OrdersFilterV1::default(),
            cursor: Some(input_cursor),
        });
        let requests = BTreeMap::from([("query-source-request".to_owned(), continuation_request)]);
        let result = DurableLogicalActionResultV1 {
            action_identity: "query-source-action".to_owned(),
            global_request_id: "query-source-request".to_owned(),
            batch_item_ordinal: 0,
            query_projection_revision: Some(7),
            part: DirectResponsePartV1::Orders {
                effective_processing_at_utc_ms: 20,
                result: OrdersQueryResultV1::OutOfRetention(QueryOutOfRetentionV1 {
                    scope: QueryRetentionScopeV1::RootSnapshot,
                    retained_from_utc_ms: None,
                    requested_root_sequence: Some(1),
                    earliest_retained_root_sequence: 2,
                }),
            },
        };
        let recovery_context = DurableLogicalActionRecoveryContextV1 {
            query_projection_revision: 9,
            query_retention: (2, None, None),
        };
        assert!(
            query_state
                .validate_continuation_page(&requests, &result, &recovery_context)
                .is_ok()
        );
        let mut changed_requests = requests.clone();
        let Some(PendingDomainRequestV1::Orders(changed_request)) =
            changed_requests.get_mut("query-source-request")
        else {
            unreachable!("测试续页请求形状固定")
        };
        changed_request.filter.condition_id = Some(ConditionId("changed-condition".to_owned()));
        assert!(
            query_state
                .validate_continuation_page(&changed_requests, &result, &recovery_context)
                .is_err()
        );
        let mut changed_result = result;
        changed_result.query_projection_revision = Some(8);
        assert!(
            query_state
                .validate_continuation_page(&requests, &changed_result, &recovery_context)
                .is_err()
        );
    }

    #[test]
    fn durable_platform_bucket_projection_rejects_hidden_paper_and_conflicting_same_time_tail() {
        let session_id = "paper-durable-bucket".to_owned();
        let mut projection = DurableEvidenceProjectionV1::default();
        let (resource_policy, resource_policy_sha256) = resource_policy_for_tests();
        projection
            .apply(&projection_evidence_for_tests(
                DurableTransactionFactV1::RunCreated {
                    run_id: "component-test-run".to_owned(),
                    market_event_source_identity_sha256: Sha256Hex("2".repeat(64)),
                    run_intent_binding: SimulationRunIntentBindingV1::try_new(
                        crate::platform_rules::ImplementationIdentityV1 {
                            git_commit: "1".repeat(40),
                            source_identity_sha256: Sha256Hex("2".repeat(64)),
                            cargo_lock_sha256: Sha256Hex("3".repeat(64)),
                        },
                        SimulationWorkloadKindV1::ComponentGolden,
                        SimulationDeterministicScriptContractV1::ComponentGoldenV1,
                    )
                    .expect("构造测试运行意图"),
                    resource_policy,
                    resource_policy_sha256,
                },
            ))
            .expect("先投影真实运行创建前缀");
        projection.session_creations.insert(
            session_id.clone(),
            (
                SessionSpecV2 {
                    caller_role: SimCallerRoleV1::Paper,
                    session_id: session_id.clone(),
                    ..session_spec()
                },
                funded_opening_anchor_for_projection_test(&session_id),
                SessionInitializationSnapshotV1 {
                    state: SessionInitializationStateV1::PreparedLedgerInit,
                    activation_id: None,
                    failure_id: None,
                },
            ),
        );
        let evidence = |bucket| {
            projection_evidence_for_tests(DurableTransactionFactV1::PlatformBucketApplied {
                session_id: session_id.clone(),
                bucket,
            })
        };
        projection
            .apply(&evidence(DurableMarketBucketReferenceV1 {
                path: MarketEventPathV1::PublicVisibility,
                bucket_at_utc_ms: 10,
                start_index_position: 0,
                next_index_position: 1,
                closure: MillisecondBucketClosureV1::NextEventAt(20),
                bucket_sha256: Sha256Hex("a".repeat(64)),
            }))
            .expect("首个公开平台桶事实合法");
        assert!(
            projection
                .apply(&evidence(DurableMarketBucketReferenceV1 {
                    path: MarketEventPathV1::PublicVisibility,
                    bucket_at_utc_ms: 10,
                    start_index_position: 1,
                    next_index_position: 2,
                    closure: MillisecondBucketClosureV1::NextEventAt(30),
                    bucket_sha256: Sha256Hex("b".repeat(64)),
                }))
                .is_err()
        );
        projection
            .apply(&evidence(DurableMarketBucketReferenceV1 {
                path: MarketEventPathV1::PublicVisibility,
                bucket_at_utc_ms: 10,
                start_index_position: 0,
                next_index_position: 1,
                closure: MillisecondBucketClosureV1::EndOfInput,
                bucket_sha256: Sha256Hex("a".repeat(64)),
            }))
            .expect("同一桶正文从下一事件见证升级为输入结束合法");

        let hidden = evidence(DurableMarketBucketReferenceV1 {
            path: MarketEventPathV1::HiddenPlatform,
            bucket_at_utc_ms: 10,
            start_index_position: 0,
            next_index_position: 0,
            closure: MillisecondBucketClosureV1::EndOfInput,
            bucket_sha256: Sha256Hex("c".repeat(64)),
        });
        projection.apply(&hidden).expect("证据投影先保留路径事实");
        assert!(
            validate_durable_bucket_paths_for_recovery(
                &session_id,
                SimCallerRoleV1::Paper,
                projection.last_platform_buckets.get(&session_id),
            )
            .is_err()
        );
    }

    fn funded_opening_anchor_for_projection_test(session_id: &str) -> AccountOpeningAnchorV1 {
        AccountOpeningAnchorV1 {
            schema_version: "sim_account_opening_anchor_v1".to_owned(),
            anchor_id: "projection-anchor".to_owned(),
            anchor_sha256: Sha256Hex("d".repeat(64)),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "component-test-run".to_owned(),
                caller_role: SimCallerRoleV1::Paper,
                caller_instance_id: "caller-task13".to_owned(),
                account_id: "account-task13".to_owned(),
                platform_source: PlatformSourceV1::Simulated,
            },
            session_id: session_id.to_owned(),
            initial_scenario_sha256: Sha256Hex("e".repeat(64)),
            initial_market_set_sha256: Sha256Hex("f".repeat(64)),
            initial_account_state_sha256: Sha256Hex("1".repeat(64)),
            initial_platform_account: contracts::PlatformAccountSnapshotV1 {
                cash_pusd: contracts::ExactDecimal("1000".to_owned()),
                available_pusd: contracts::ExactDecimal("1000".to_owned()),
                principal_encumbrance_pusd: contracts::ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbrance_pusd: contracts::ExactDecimal("0".to_owned()),
                outcome_balances: Vec::new(),
                maker_rebate_receivable_pusd: contracts::ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd: contracts::ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd: contracts::ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd: contracts::ExactDecimal("0".to_owned()),
            },
        }
    }

    #[test]
    fn same_time_autonomous_work_reuses_a_started_incomplete_request_visibility_slot() {
        let mut outbox = LogicalLifecycleOutboxV1::new(
            std::num::NonZeroUsize::new(1).expect("positive capacity"),
        );
        let request = outbox
            .reserve_request("started-request-r", 200)
            .expect("the first request item starts the only visibility slot");

        let platform = outbox
            .reserve_platform_work("GTD:200", None, 200)
            .expect("same-time platform work must not deadlock a started request");

        assert_eq!(platform.batch_id, request.batch_id);
        assert_eq!(
            outbox.work_batch_ids.get("REQUEST:started-request-r"),
            Some(&request.batch_id)
        );
        assert_eq!(
            outbox.work_batch_ids.get("PLATFORM:GTD:200"),
            Some(&request.batch_id)
        );
        assert!(outbox.batches.get(&request.batch_id).is_some_and(|batch| {
            batch.open_work_ids
                == BTreeSet::from([
                    "PLATFORM:GTD:200".to_owned(),
                    "REQUEST:started-request-r".to_owned(),
                ])
                && batch.feedback_ids.is_empty()
                && batch.feedback_received_at_counts.is_empty()
        }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn autonomous_gtd_full_is_retryable_and_private_delivery_follows_the_unified_head() {
        use std::sync::Mutex;

        use crate::task22_tests::{
            asset as long_asset, context_for_arrival, load_initial_market, market as long_market,
            scenario as long_scenario, session_spec as long_session_spec,
        };

        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct DeliveredPrivateProbe {
            received_at_utc_ms: i64,
            private_emission_sequence: u64,
            private_feedback_sequence: u64,
            kind: &'static str,
        }

        let delivered = Arc::new(Mutex::new(Vec::<DeliveredPrivateProbe>::new()));
        let direct_delivered = Arc::clone(&delivered);
        let lifecycle_delivered = Arc::clone(&delivered);
        let bridge = SubmissionResponseBridgeV1::new(
            move |response| {
                direct_delivered
                    .lock()
                    .expect("direct probe lock")
                    .push(DeliveredPrivateProbe {
                        received_at_utc_ms: response.metadata.response_received_at_utc_ms,
                        private_emission_sequence: response.metadata.private_emission_sequence,
                        private_feedback_sequence: response.metadata.private_feedback_sequence,
                        kind: "DIRECT",
                    });
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |feedback| {
                lifecycle_delivered
                    .lock()
                    .expect("lifecycle probe lock")
                    .push(DeliveredPrivateProbe {
                        received_at_utc_ms: feedback.metadata.feedback_received_at_utc_ms,
                        private_emission_sequence: feedback.metadata.private_emission_sequence,
                        private_feedback_sequence: feedback.metadata.private_feedback_sequence,
                        kind: "LIFECYCLE",
                    });
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        );

        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let scenario = long_scenario();
        let mut spec = long_session_spec(
            &scenario,
            SimCallerRoleV1::ModelTrain,
            "autonomous-gtd-full-retry",
        );
        spec.response_outbox_capacity = std::num::NonZeroUsize::new(1).expect("positive capacity");
        let mut session = execution_process_for_tests(rules, Arc::clone(&profile), scenario)
            .create_active_session_for_tests(spec, bridge);
        load_initial_market(&mut session, "0.00001", "0.6", "5");

        let gtd = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(&session, &profile, "gtd-source", 1, 150_000),
                idempotency_key: "gtd-source-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "gtd-source-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtdLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                            expires_at_utc_ms: 391_999,
                        },
                    },
                },
            })
            .expect("register GTD source");
        let gtd_effective_at = gtd.item.effective_processing_at_utc_ms;
        let position = session.platform_event_index_position();
        session
            .session
            .close_platform_prefix_through(gtd_effective_at, position)
            .expect("close through GTD acceptance");
        assert_eq!(
            session.drive_next_execution_action_through(gtd_effective_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let gtd_deadline = session
            .next_order_deadline_at_utc_ms()
            .expect("GTD deadline");

        let initial_delivery_cutoff = 200_000;
        session
            .session
            .close_platform_prefix_through(initial_delivery_cutoff, position)
            .expect("close through initial private delivery");
        assert_eq!(
            session.drive_next_execution_action_through(initial_delivery_cutoff),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert_eq!(
            session.drive_private_delivery_through(initial_delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(initial_delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        delivered.lock().expect("delivery probe lock").clear();
        assert!(session.lifecycle_outbox.batches.is_empty());

        let unrelated = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(&session, &profile, "future-unrelated-r", 2, 250_000),
                idempotency_key: "future-unrelated-r-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "future-unrelated-r-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                        },
                    },
                },
            })
            .expect("register unrelated mutation R");
        let unrelated_at = unrelated.item.effective_processing_at_utc_ms;
        session
            .session
            .close_platform_prefix_through(unrelated_at, position)
            .expect("close through unrelated mutation");
        assert_eq!(
            session.drive_next_execution_action_through(unrelated_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );

        let before_deadline = gtd_deadline - 1;
        session
            .session
            .close_platform_prefix_through(before_deadline, position)
            .expect("close just before GTD deadline");
        assert_eq!(
            session.drive_next_execution_action_through(before_deadline),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert!(
            session
                .lifecycle_outbox
                .batches
                .get("REQUEST:future-unrelated-r")
                .is_some_and(|batch| {
                    batch.open_work_ids.is_empty() && !batch.feedback_ids.is_empty()
                })
        );

        let available_before = session.available_pusd();
        let principal_before = session.principal_encumbered_pusd();
        let next_account_seq_before = session
            .session
            .execution_state()
            .account()
            .next_account_transition_seq();
        let lifecycle_before = session.lifecycle_outbox.clone();
        let logical_platform_before = session.session.logical_platform_driven_through();
        session
            .session
            .close_platform_prefix_through(gtd_deadline, position)
            .expect("close platform input through GTD deadline");

        assert_eq!(
            session.drive_next_execution_action_through(gtd_deadline),
            Err(SubmissionProcessingErrorV1::Session(
                SessionError::ResponseOutboxFull
            ))
        );
        assert_eq!(session.available_pusd(), available_before);
        assert_eq!(session.principal_encumbered_pusd(), principal_before);
        assert_eq!(
            session
                .session
                .execution_state()
                .account()
                .next_account_transition_seq(),
            next_account_seq_before
        );
        assert_eq!(session.next_order_deadline_at_utc_ms(), Some(gtd_deadline));
        assert_eq!(session.lifecycle_outbox, lifecycle_before);
        assert_eq!(
            session.session.logical_platform_driven_through(),
            logical_platform_before
        );
        assert!(session.accepting_new_ingress);
        assert!(delivered.lock().expect("delivery probe lock").is_empty());

        assert_eq!(
            session.drive_private_delivery_through(before_deadline),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(before_deadline),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        let actual_delivery_order = delivered.lock().expect("delivery probe lock").clone();
        assert_eq!(actual_delivery_order.len(), 2);
        let mut sorted_by_unified_private_key = actual_delivery_order.clone();
        sorted_by_unified_private_key
            .sort_by_key(|probe| (probe.received_at_utc_ms, probe.private_emission_sequence));
        assert_eq!(actual_delivery_order, sorted_by_unified_private_key);
        assert!(
            actual_delivery_order
                .windows(2)
                .all(|pair| pair[0].private_feedback_sequence + 1
                    == pair[1].private_feedback_sequence)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());

        session
            .order(OrderRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    "ingress-after-retryable-full",
                    3,
                    gtd_deadline + 10_000,
                ),
                platform_order_id: "f".repeat(64),
            })
            .expect("retryable capacity pressure must not close new ingress");

        assert_eq!(
            session.drive_next_execution_action_through(gtd_deadline),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert_eq!(session.next_order_deadline_at_utc_ms(), None);
        assert_eq!(
            session
                .session
                .execution_state()
                .account()
                .next_account_transition_seq(),
            next_account_seq_before + 1
        );
        assert!(session.available_pusd() > available_before);
        assert!(session.principal_encumbered_pusd() < principal_before);
        assert_eq!(
            session.session.logical_platform_driven_through(),
            Some(gtd_deadline)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn future_mutation_does_not_preempt_capacity_from_an_earlier_autonomous_gtd() {
        use crate::task22_tests::{
            asset as long_asset, context_for_arrival, hidden_bucket as long_hidden_bucket,
            load_initial_market, market as long_market, scenario as long_scenario,
            session_spec as long_session_spec,
        };

        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let scenario = long_scenario();
        let mut spec = long_session_spec(
            &scenario,
            SimCallerRoleV1::ModelTrain,
            "future-mutation-after-gtd",
        );
        spec.response_outbox_capacity = std::num::NonZeroUsize::new(1).expect("positive capacity");
        let mut session =
            execution_process_for_tests(rules, Arc::clone(&profile), Arc::clone(&scenario))
                .create_active_session_for_tests(spec, accepting_submission_response_bridge());
        load_initial_market(&mut session, "0.00001", "0.6", "5");

        let gtd = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(&session, &profile, "earlier-gtd", 1, 150_000),
                idempotency_key: "earlier-gtd-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "earlier-gtd-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtdLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                            expires_at_utc_ms: 391_999,
                        },
                    },
                },
            })
            .expect("register earlier GTD");
        let gtd_at = gtd.item.effective_processing_at_utc_ms;
        let position = session.platform_event_index_position();
        session
            .session
            .close_platform_prefix_through(gtd_at, position)
            .expect("close through earlier GTD acceptance");
        assert_eq!(
            session.drive_next_execution_action_through(gtd_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let gtd_deadline = session
            .next_order_deadline_at_utc_ms()
            .expect("GTD deadline");

        let accepted_delivery_cutoff = 200_000;
        session
            .session
            .close_platform_prefix_through(accepted_delivery_cutoff, position)
            .expect("close through accepted delivery");
        assert_eq!(
            session.drive_next_execution_action_through(accepted_delivery_cutoff),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert_eq!(
            session.drive_private_delivery_through(accepted_delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(accepted_delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());

        let future = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    "later-mutation-r",
                    2,
                    gtd_deadline + 1,
                ),
                idempotency_key: "later-mutation-r-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "later-mutation-r-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                        },
                    },
                },
            })
            .expect("register later mutation R without lifecycle pre-reservation");
        let future_at = future.item.effective_processing_at_utc_ms;
        assert!(future_at > gtd_deadline);
        assert!(session.lifecycle_outbox.batches.is_empty());
        assert!(session.lifecycle_outbox.work_batch_ids.is_empty());

        let position = session.platform_event_index_position();
        assert_eq!(
            session.apply_closed_market_event_bucket(long_hidden_bucket(
                gtd_deadline,
                position,
                position,
                &[],
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        assert_eq!(
            session
                .session
                .platform_prefix_cursor()
                .closed_through_source_event_at_utc_ms(),
            Some(gtd_deadline)
        );
        assert_eq!(session.next_order_deadline_at_utc_ms(), None);
        assert_eq!(session.lifecycle_outbox.batches.len(), 1);
        assert!(session.lifecycle_outbox.batches.values().all(|batch| {
            batch.open_work_ids.is_empty()
                && batch.feedback_ids.len() == 1
                && batch.feedback_received_at_counts.values().sum::<usize>() == 1
        }));
        assert_eq!(session.lifecycle_outbox.work_batch_ids.len(), 0);

        assert_eq!(
            session.drive_next_execution_action_through(gtd_deadline),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        let gtd_feedback_at = session
            .session
            .next_private_fact_received_at_through(i64::MAX)
            .expect("GTD feedback timing");
        assert!(future_at < gtd_feedback_at);
        session
            .session
            .close_platform_prefix_through(future_at, position)
            .expect("close through mutation before GTD feedback becomes visible");
        assert_eq!(
            session.drive_next_execution_action_through(future_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let lifecycle_batch = session
            .lifecycle_outbox
            .batches
            .values()
            .next()
            .expect("GTD 与更早可见的 mutation 反馈共享唯一槽");
        assert!(lifecycle_batch.open_work_ids.is_empty());
        assert_eq!(
            lifecycle_batch
                .feedback_received_at_counts
                .values()
                .sum::<usize>(),
            lifecycle_batch.feedback_ids.len()
        );
        assert_eq!(
            lifecycle_batch
                .feedback_received_at_counts
                .first_key_value()
                .map(|(received_at, _)| *received_at),
            lifecycle_batch
                .feedback_ids
                .iter()
                .map(|feedback_id| {
                    session
                        .lifecycle_outbox
                        .entries
                        .get(feedback_id)
                        .expect("feedback payload indexed by identity")
                        .draft
                        .timing()
                        .feedback_received_at_utc_ms
                })
                .min()
        );
        assert!(session.lifecycle_outbox.work_batch_ids.is_empty());
        let delivery_cutoff = gtd_feedback_at.max(future_at + 10_000);
        session
            .session
            .close_platform_prefix_through(delivery_cutoff, position)
            .expect("close through both lifecycle feedback receive times");
        assert_eq!(
            session.drive_next_execution_action_through(delivery_cutoff),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert_eq!(
            session.drive_private_delivery_through(delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn started_ordinary_then_itode_batch_does_not_deadlock_an_intermediate_gtd_at_capacity_one()
     {
        use crate::task22_tests::{
            asset as long_asset, context_for_arrival, hidden_bucket as long_hidden_bucket,
            load_initial_market, market as long_market, scenario as long_scenario,
            session_spec as long_session_spec,
        };

        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let scenario = long_scenario();
        let mut spec = long_session_spec(
            &scenario,
            SimCallerRoleV1::ModelTrain,
            "started-batch-intermediate-gtd",
        );
        spec.response_outbox_capacity = std::num::NonZeroUsize::new(1).expect("positive capacity");
        let mut session =
            execution_process_for_tests(rules, Arc::clone(&profile), Arc::clone(&scenario))
                .create_active_session_for_tests(spec, accepting_submission_response_bridge());
        let position = load_initial_market(&mut session, "0.00001", "0.6", "5");

        let gtd = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(&session, &profile, "batch-gap-gtd", 1, 150_000),
                idempotency_key: "batch-gap-gtd-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "batch-gap-gtd-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtdLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                            expires_at_utc_ms: 391_999,
                        },
                    },
                },
            })
            .expect("register GTD source");
        let gtd_at = gtd.item.effective_processing_at_utc_ms;
        session
            .session
            .close_platform_prefix_through(gtd_at, position)
            .expect("close through GTD acceptance");
        assert_eq!(
            session.drive_next_execution_action_through(gtd_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let gtd_deadline = session
            .next_order_deadline_at_utc_ms()
            .expect("GTD deadline");
        session.prepare_private_delivery_through_for_tests(200_000);
        assert_eq!(
            session.drive_private_delivery_through(200_000),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(200_000),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());

        let batch = session
            .post_orders(PostOrdersRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    "ordinary-itode-around-gtd",
                    2,
                    gtd_deadline - 1,
                ),
                idempotency_key: "ordinary-itode-around-gtd-idem".to_owned(),
                items: vec![
                    PostOrderItemV1 {
                        item_id: "ordinary-rejected-without-delta".to_owned(),
                        order: SimBuyOrderV1 {
                            condition_id: long_market().condition_id.clone(),
                            token_id: long_asset("1", 0, "Up").token_id,
                            kind: SimBuyOrderKindV1::GtcLimit {
                                limit_price_pusd_per_share: Decimal::new(6, 1),
                                size_shares: Decimal::new(5, 0),
                                post_only: true,
                            },
                        },
                    },
                    PostOrderItemV1 {
                        item_id: "itode-fak-after-gtd".to_owned(),
                        order: SimBuyOrderV1 {
                            condition_id: long_market().condition_id,
                            token_id: long_asset("1", 0, "Up").token_id,
                            kind: SimBuyOrderKindV1::FakMarket {
                                spend_pusd: Decimal::ONE,
                                worst_price_pusd_per_share: Decimal::new(6, 1),
                            },
                        },
                    },
                ],
            })
            .expect("register ordinary plus itode batch without lifecycle pre-reservation");
        let first_at = batch.items[0].effective_processing_at_utc_ms;
        let second_at = batch.items[1].effective_processing_at_utc_ms;
        assert_eq!(first_at, gtd_deadline - 1);
        assert_eq!(second_at - first_at, 250);
        assert!(session.lifecycle_outbox.batches.is_empty());

        session
            .session
            .close_platform_prefix_through(first_at, position)
            .expect("close through ordinary first item");
        let account_sequence_before_first = session
            .session
            .execution_state()
            .account()
            .next_account_transition_seq();
        assert_eq!(
            session.drive_next_execution_action_through(first_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: false,
            })
        );
        assert_eq!(
            session
                .session
                .execution_state()
                .account()
                .next_account_transition_seq(),
            account_sequence_before_first
        );
        let request_work_id =
            LogicalLifecycleOutboxV1::request_batch_id("ordinary-itode-around-gtd");
        let request_batch_id = session
            .lifecycle_outbox
            .work_batch_ids
            .get(&request_work_id)
            .expect("first item starts one request visibility slot")
            .clone();
        assert!(
            session
                .lifecycle_outbox
                .batches
                .get(&request_batch_id)
                .is_some_and(|slot| {
                    slot.open_work_ids == BTreeSet::from([request_work_id.clone()])
                        && slot.feedback_ids.is_empty()
                        && slot.feedback_received_at_counts.is_empty()
                })
        );

        assert_eq!(
            session.apply_closed_market_event_bucket(long_hidden_bucket(
                gtd_deadline,
                position,
                position,
                &[],
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        assert_eq!(session.next_order_deadline_at_utc_ms(), None);
        let gtd_feedback_at = session
            .session
            .next_private_fact_received_at_through(i64::MAX)
            .expect("GTD release feedback timing");
        assert!(second_at > gtd_feedback_at);
        let slot_after_gtd = session
            .lifecycle_outbox
            .batches
            .get(&request_batch_id)
            .expect("GTD reuses the started request slot");
        assert_eq!(
            slot_after_gtd.open_work_ids,
            BTreeSet::from([request_work_id.clone()])
        );
        assert_eq!(slot_after_gtd.feedback_ids.len(), 1);
        assert_eq!(
            slot_after_gtd
                .feedback_received_at_counts
                .first_key_value()
                .map(|(received_at, count)| (*received_at, *count)),
            Some((gtd_feedback_at, 1))
        );

        session
            .session
            .close_platform_prefix_through(second_at, position)
            .expect("close input through delayed item");
        let before_late_action = session.lifecycle_outbox.clone();
        let account_sequence_before_late_action = session
            .session
            .execution_state()
            .account()
            .next_account_transition_seq();
        assert_eq!(
            session.drive_next_execution_action_through(second_at),
            Err(SubmissionProcessingErrorV1::Session(
                SessionError::ResponseOutboxFull
            ))
        );
        assert_eq!(session.lifecycle_outbox, before_late_action);
        assert_eq!(
            session
                .session
                .execution_state()
                .account()
                .next_account_transition_seq(),
            account_sequence_before_late_action
        );
        assert!(session.accepting_new_ingress);

        assert_eq!(
            session.drive_next_execution_action_through(gtd_feedback_at),
            Ok(SubmissionDriveDispositionV1::NoAction)
        );
        assert_eq!(
            session.drive_private_delivery_through(gtd_feedback_at),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert!(
            session
                .lifecycle_outbox
                .batches
                .get(&request_batch_id)
                .is_some_and(|slot| {
                    slot.open_work_ids == BTreeSet::from([request_work_id.clone()])
                        && slot.feedback_ids.is_empty()
                        && slot.feedback_received_at_counts.is_empty()
                })
        );

        assert_eq!(
            session.drive_next_execution_action_through(second_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let delivery_cutoff = second_at + 40_000;
        session.prepare_private_delivery_through_for_tests(delivery_cutoff);
        assert_eq!(
            session.drive_private_delivery_through(delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());
        assert!(session.lifecycle_outbox.work_batch_ids.is_empty());
        assert!(session.direct_response_outbox.entries.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn earliest_due_gtd_reuses_a_slot_when_its_deadline_precedes_old_feedback_and_bucket_time()
     {
        use crate::task22_tests::{
            asset as long_asset, context_for_arrival, hidden_bucket as long_hidden_bucket,
            load_initial_market, market as long_market, scenario as long_scenario,
            session_spec as long_session_spec,
        };

        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let scenario = long_scenario();
        let mut spec = long_session_spec(
            &scenario,
            SimCallerRoleV1::ModelTrain,
            "gtd-deadline-before-old-feedback",
        );
        spec.response_outbox_capacity = std::num::NonZeroUsize::new(1).expect("positive capacity");
        let mut session =
            execution_process_for_tests(rules, Arc::clone(&profile), Arc::clone(&scenario))
                .create_active_session_for_tests(spec, accepting_submission_response_bridge());
        let position = load_initial_market(&mut session, "0.00001", "0.6", "5");

        let gtd = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    "earliest-deadline-gtd",
                    1,
                    150_000,
                ),
                idempotency_key: "earliest-deadline-gtd-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "earliest-deadline-gtd-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtdLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                            expires_at_utc_ms: 391_999,
                        },
                    },
                },
            })
            .expect("register GTD source");
        let gtd_at = gtd.item.effective_processing_at_utc_ms;
        session
            .session
            .close_platform_prefix_through(gtd_at, position)
            .expect("close through GTD acceptance");
        assert_eq!(
            session.drive_next_execution_action_through(gtd_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let deadline = session
            .next_order_deadline_at_utc_ms()
            .expect("GTD deadline");
        session.prepare_private_delivery_through_for_tests(200_000);
        assert_eq!(
            session.drive_private_delivery_through(200_000),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());

        let older = session
            .post_order(PostOrderRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    "feedback-crossed-by-later-bucket",
                    2,
                    deadline - 1,
                ),
                idempotency_key: "feedback-crossed-by-later-bucket-idem".to_owned(),
                item: PostOrderItemV1 {
                    item_id: "feedback-crossed-by-later-bucket-item".to_owned(),
                    order: SimBuyOrderV1 {
                        condition_id: long_market().condition_id,
                        token_id: long_asset("1", 0, "Up").token_id,
                        kind: SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(1, 5),
                            size_shares: Decimal::new(5, 0),
                            post_only: true,
                        },
                    },
                },
            })
            .expect("register mutation immediately before the deadline");
        let older_at = older.item.effective_processing_at_utc_ms;
        assert_eq!(older_at, deadline - 1);
        session
            .session
            .close_platform_prefix_through(older_at, position)
            .expect("close through older mutation");
        assert_eq!(
            session.drive_next_execution_action_through(older_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: true,
            })
        );
        let old_feedback_at = session
            .lifecycle_outbox
            .batches
            .values()
            .next()
            .and_then(|slot| slot.feedback_received_at_counts.first_key_value())
            .map(|(received_at, _)| *received_at)
            .expect("older mutation occupies the only slot with feedback");
        assert!(deadline < old_feedback_at);
        let later_bucket_at = old_feedback_at + 1;

        assert_eq!(
            session.apply_closed_market_event_bucket(long_hidden_bucket(
                later_bucket_at,
                position,
                position,
                &[],
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        assert_eq!(session.next_order_deadline_at_utc_ms(), None);
        assert_eq!(session.lifecycle_outbox.batches.len(), 1);
        let shared_slot = session
            .lifecycle_outbox
            .batches
            .values()
            .next()
            .expect("older feedback and GTD release share the visibility slot");
        assert!(shared_slot.open_work_ids.is_empty());
        assert_eq!(shared_slot.feedback_ids.len(), 2);
        assert_eq!(
            shared_slot
                .feedback_received_at_counts
                .values()
                .sum::<usize>(),
            2
        );

        let delivery_cutoff = later_bucket_at + 40_000;
        session.prepare_private_delivery_through_for_tests(delivery_cutoff);
        assert_eq!(
            session.drive_private_delivery_through(delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::Delivered)
        );
        assert_eq!(
            session.drive_private_delivery_through(delivery_cutoff),
            Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
        );
        assert!(session.lifecycle_outbox.batches.is_empty());
        assert!(session.lifecycle_outbox.entries.is_empty());
    }

    #[test]
    fn only_fatal_session_processing_errors_close_new_ingress() {
        assert!(fatal_session_processing_error(
            &SessionError::PlatformRevisionRegression
        ));
        assert!(fatal_session_processing_error(
            &SessionError::ExecutionState("broken".to_owned())
        ));
        assert!(fatal_session_processing_error(
            &SessionError::ActionNotLogicallyReleased("action".to_owned())
        ));
        assert!(fatal_session_processing_error(
            &SessionError::RequestNotLogicallyReleased("request".to_owned())
        ));
        assert!(!fatal_session_processing_error(
            &SessionError::PlatformPrefixNotClosed
        ));
        assert!(!fatal_session_processing_error(
            &SessionError::PlatformPrefixCrossesPendingAction
        ));
    }

    #[test]
    fn submission_effect_evidence_is_independently_bound_into_the_item_hash() {
        fn item_hash(item: &SubmissionResponseItemV1) -> Sha256Hex {
            let mut hash = CanonicalHasher::new("TEST_SUBMISSION_RESPONSE_ITEM_V1");
            hash_submission_response_item(&mut hash, item);
            hash.finish()
        }

        let item = SubmissionResponseItemV1 {
            item_id: "item-effect-hash".to_owned(),
            batch_item_ordinal: 0,
            condition_id: ConditionId("condition-effect-hash".to_owned()),
            token_id: TokenId("token-effect-hash".to_owned()),
            effective_processing_at_utc_ms: 2_000,
            submission_effect_identity: "effect-a".to_owned(),
            submission_effect_formed_at_utc_ms: 1_000,
            submission_effect_disposition: SubmissionEffectDispositionV1::ExistingEffect,
            outcome: SubmissionItemOutcomeV1::RejectedNoEffect(
                SubmissionRejectedReasonV1::EndAtReached,
            ),
        };
        let baseline = item_hash(&item);

        let mut changed_identity = item.clone();
        changed_identity.submission_effect_identity = "effect-b".to_owned();
        assert_ne!(baseline, item_hash(&changed_identity));

        let mut changed_formed_at = item.clone();
        changed_formed_at.submission_effect_formed_at_utc_ms += 1;
        assert_ne!(baseline, item_hash(&changed_formed_at));

        let mut changed_disposition = item;
        changed_disposition.submission_effect_disposition =
            SubmissionEffectDispositionV1::NewEffect;
        assert_ne!(baseline, item_hash(&changed_disposition));
    }

    use crate::scenario::scenario_for_sim_tests;
    use contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3;

    fn sha(byte: &str) -> Sha256Hex {
        Sha256Hex(byte.repeat(64))
    }

    fn session_spec() -> SessionSpecV2 {
        let scenario = scenario_for_sim_tests();
        SessionSpecV2 {
            run_id: "component-test-run".to_owned(),
            session_id: "session-task13".to_owned(),
            account_id: "account-task13".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-task13".to_owned(),
            creation_request_id: "create-session-task13".to_owned(),
            response_bridge_binding_id: "bridge-session-task13".to_owned(),
            initial_account_state: InitialAccountStateV1::try_new(Decimal::new(1_000, 0))
                .expect("initial account"),
            response_outbox_capacity: std::num::NonZeroUsize::new(8)
                .expect("positive test capacity"),
            market_set_sha256: scenario.market_set_sha256().clone(),
            scenario_sha256: scenario.scenario_sha256().clone(),
            session_seed: sha("c"),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        }
    }

    fn context(
        session: &SimExecutionSessionV1,
        request_id: &str,
        caller_sequence: u64,
        request_sent_at_utc_ms: i64,
    ) -> RequestContextV1 {
        RequestContextV1 {
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            session_id: session.session_spec().session_id.clone(),
            account_id: session.session_spec().account_id.clone(),
            caller_role: session.session_spec().caller_role,
            caller_instance_id: session.session_spec().caller_instance_id.clone(),
            global_request_id: request_id.to_owned(),
            caller_correlation_id: Some("audit-only".to_owned()),
            caller_sequence,
            request_sent_at_utc_ms,
            model_visible_cursor: session.model_visible_cursor(),
        }
    }

    fn gtc_item(item_id: &str, price: i64) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(price, 2),
                    size_shares: Decimal::new(200, 2),
                    post_only: false,
                },
            },
        }
    }

    fn fok_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::FokMarket {
                    spend_pusd: Decimal::new(100, 2),
                    worst_price_pusd_per_share: Decimal::new(60, 2),
                },
            },
        }
    }

    fn gtd_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: Decimal::new(50, 2),
                    size_shares: Decimal::new(200, 2),
                    post_only: true,
                    expires_at_utc_ms: 1_000_000,
                },
            },
        }
    }

    fn fak_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: Decimal::new(100, 2),
                    worst_price_pusd_per_share: Decimal::new(60, 2),
                },
            },
        }
    }

    async fn execution_session() -> SimExecutionSessionV1 {
        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut process = execution_process_for_tests(rules, profile, scenario_for_sim_tests());
        process
            .create_active_session_for_tests(session_spec(), accepting_submission_response_bridge())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn all_eight_methods_return_only_method_specific_registration_receipts() {
        let mut session = execution_session().await;

        let post_order = session
            .post_order(PostOrderRequestV1 {
                context: context(&session, "request-1", 1, 1_000),
                idempotency_key: "idem-1".to_owned(),
                item: gtc_item("item-1", 55),
            })
            .expect("post_order registration");
        let post_orders = session
            .post_orders(PostOrdersRequestV1 {
                context: context(&session, "request-2", 2, 1_001),
                idempotency_key: "idem-2".to_owned(),
                items: vec![gtc_item("item-2a", 54), fok_item("item-2b")],
            })
            .expect("post_orders registration");
        let cancel_order = session
            .cancel_order(CancelOrderRequestV1 {
                context: context(&session, "request-3", 3, 1_002),
                idempotency_key: "idem-3".to_owned(),
                item: CancelOrderItemV1 {
                    item_id: "item-3".to_owned(),
                    platform_order_id: "3".repeat(64),
                },
            })
            .expect("cancel_order registration");
        let cancel_orders = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: context(&session, "request-4", 4, 1_003),
                idempotency_key: "idem-4".to_owned(),
                items: vec![CancelOrderItemV1 {
                    item_id: "item-4".to_owned(),
                    platform_order_id: "4".repeat(64),
                }],
            })
            .expect("cancel_orders registration");
        let cancel_all_orders = session
            .cancel_all_orders(CancelAllOrdersRequestV1 {
                context: context(&session, "request-5", 5, 1_004),
                idempotency_key: "idem-5".to_owned(),
            })
            .expect("cancel_all_orders registration");
        let order = session
            .order(OrderRequestV1 {
                context: context(&session, "request-6", 6, 1_005),
                platform_order_id: "6".repeat(64),
            })
            .expect("order registration");
        let orders = session
            .orders(OrdersRequestV1 {
                context: context(&session, "request-7", 7, 1_006),
                filter: OrdersFilterV1::default(),
                cursor: None,
            })
            .expect("orders registration");
        let trades = session
            .trades(TradesRequestV1 {
                context: context(&session, "request-8", 8, 1_007),
                filter: TradesFilterV1::default(),
                cursor: None,
            })
            .expect("trades registration");

        assert_eq!(post_order.receipt.scheduled_action_count, 1);
        assert_eq!(post_orders.receipt.scheduled_action_count, 2);
        assert_eq!(cancel_order.receipt.scheduled_action_count, 1);
        assert_eq!(cancel_orders.receipt.scheduled_action_count, 1);
        assert_eq!(cancel_all_orders.receipt.scheduled_action_count, 1);
        assert_eq!(order.receipt.scheduled_action_count, 1);
        assert_eq!(orders.receipt.scheduled_action_count, 1);
        assert_eq!(trades.receipt.scheduled_action_count, 1);
        for receipt in [
            &post_order.receipt,
            &post_orders.receipt,
            &cancel_order.receipt,
            &cancel_orders.receipt,
            &cancel_all_orders.receipt,
            &order.receipt,
            &orders.receipt,
            &trades.receipt,
        ] {
            assert_eq!(
                receipt.disposition,
                RequestRegistrationDispositionV1::NewLogicalRequest
            );
            assert_eq!(
                receipt.execution_contract_version,
                SIM_EXECUTION_CONTRACT_V1
            );
            assert_eq!(receipt.transit.platform_responded_at_utc_ms, None);
            assert_eq!(receipt.transit.response_received_at_utc_ms, None);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn physical_retry_reuses_timing_and_never_requeues_the_action() {
        let mut session = execution_session().await;
        let request = PostOrderRequestV1 {
            context: context(&session, "retry-request", 1, 2_000),
            idempotency_key: "retry-idem".to_owned(),
            item: gtc_item("retry-item", 50),
        };
        let first = session.post_order(request.clone()).expect("first");
        let pending_retry = session
            .post_order(request.clone())
            .expect("pending physical retry");

        assert_eq!(
            first.receipt.disposition,
            RequestRegistrationDispositionV1::NewLogicalRequest
        );
        assert_eq!(
            pending_retry.receipt.disposition,
            RequestRegistrationDispositionV1::PhysicalRetry
        );
        assert_eq!(first.receipt.transit, pending_retry.receipt.transit);
        assert_eq!(first.item, pending_retry.item);
        assert_eq!(session.pending_requests.len(), 1);

        let effective_at = first.item.effective_processing_at_utc_ms;
        session
            .session
            .close_platform_prefix_through(effective_at, 1)
            .expect("close platform prefix");
        let action = session
            .session
            .take_next_action_through(effective_at)
            .expect("take action")
            .expect("one action");
        let in_flight_retry = session
            .post_order(request.clone())
            .expect("in-flight physical retry");
        assert_eq!(first.receipt.transit, in_flight_retry.receipt.transit);
        assert_eq!(first.item, in_flight_retry.item);
        session
            .session
            .complete_logical_item(&action.action_identity)
            .expect("complete action");
        let completed_retry = session
            .post_order(request)
            .expect("completed physical retry");
        assert_eq!(first.receipt.transit, completed_retry.receipt.transit);
        assert_eq!(first.item, completed_retry.item);
        assert!(
            session
                .session
                .take_next_action_through(effective_at)
                .expect("no duplicate action")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn order_kind_alone_selects_the_frozen_itode_processing_delay() {
        let mut session = execution_session().await;
        let registrations = [
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-gtc", 1, 4_000),
                    idempotency_key: "timing-gtc".to_owned(),
                    item: gtc_item("timing-gtc", 50),
                })
                .expect("GTC"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-gtd", 2, 4_001),
                    idempotency_key: "timing-gtd".to_owned(),
                    item: gtd_item("timing-gtd"),
                })
                .expect("GTD"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-fok", 3, 4_002),
                    idempotency_key: "timing-fok".to_owned(),
                    item: fok_item("timing-fok"),
                })
                .expect("FOK"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-fak", 4, 4_003),
                    idempotency_key: "timing-fak".to_owned(),
                    item: fak_item("timing-fak"),
                })
                .expect("FAK"),
        ];

        let processing_delays = registrations.map(|registration| {
            registration.item.effective_processing_at_utc_ms
                - registration.receipt.transit.platform_arrived_at_utc_ms
        });
        assert_eq!(processing_delays, [0, 0, 250, 250]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_later_batch_item_leaves_the_request_identity_reusable() {
        let mut session = execution_session().await;
        let request_context = context(&session, "invalid-later-item", 1, 4_500);
        let mut invalid_item = gtc_item("item-b", 50);
        invalid_item.order.token_id = TokenId(String::new());
        assert!(matches!(
            session.post_orders(PostOrdersRequestV1 {
                context: request_context.clone(),
                idempotency_key: "batch-idem".to_owned(),
                items: vec![gtc_item("item-a", 50), invalid_item],
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        assert!(session.pending_requests.is_empty());
        assert!(session.mutation_payloads.is_empty());

        let corrected = session
            .post_orders(PostOrdersRequestV1 {
                context: request_context,
                idempotency_key: "batch-idem".to_owned(),
                items: vec![gtc_item("item-a", 50), gtc_item("item-b", 50)],
            })
            .expect("same request identity remains usable");
        assert_eq!(corrected.receipt.scheduled_action_count, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batch_limits_accept_the_frozen_maxima_and_reject_the_next_item_atomically() {
        let mut session = execution_session().await;
        let post_context = context(&session, "post-limit", 1, 4_750);
        let post_items = (0..=POST_ORDERS_MAX_ITEMS)
            .map(|index| gtc_item(&format!("post-{index}"), 50))
            .collect::<Vec<_>>();
        assert!(matches!(
            session.post_orders(PostOrdersRequestV1 {
                context: post_context.clone(),
                idempotency_key: "post-limit".to_owned(),
                items: post_items.clone(),
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        let posts = session
            .post_orders(PostOrdersRequestV1 {
                context: post_context,
                idempotency_key: "post-limit".to_owned(),
                items: post_items[..POST_ORDERS_MAX_ITEMS].to_vec(),
            })
            .expect("15 post items");
        assert_eq!(posts.receipt.scheduled_action_count, 15);

        let cancel_context = context(&session, "cancel-limit", 2, 4_751);
        let cancel_items = (0..=CANCEL_ORDERS_MAX_ITEMS)
            .map(|index| CancelOrderItemV1 {
                item_id: format!("cancel-{index}"),
                platform_order_id: format!("{index:064x}"),
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            session.cancel_orders(CancelOrdersRequestV1 {
                context: cancel_context.clone(),
                idempotency_key: "cancel-limit".to_owned(),
                items: cancel_items.clone(),
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        let cancellations = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: cancel_context,
                idempotency_key: "cancel-limit".to_owned(),
                items: cancel_items[..CANCEL_ORDERS_MAX_ITEMS].to_vec(),
            })
            .expect("1000 cancel items");
        assert_eq!(cancellations.receipt.scheduled_action_count, 1_000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_item_overflow_and_idempotency_conflict_leave_no_partial_registration() {
        let rules = successful_snapshot_for_sim_tests().await;
        assert!(rules.synchronized_rules().taker_order_delay_enabled);
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let spec = session_spec();
        let request_id = "overflow-request";
        let outbound_delay_ms = profile.sample_delay_ms(
            spec.session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let sent_at = i64::MAX - i64::try_from(outbound_delay_ms).expect("delay fits i64") - 100;
        let mut process = execution_process_for_tests(rules, profile, scenario_for_sim_tests());
        let mut session =
            process.create_active_session_for_tests(spec, accepting_submission_response_bridge());
        let overflow_context = context(&session, request_id, 1, sent_at);
        assert_eq!(
            session.post_order(PostOrderRequestV1 {
                context: overflow_context.clone(),
                idempotency_key: "overflow-idem".to_owned(),
                item: fok_item("overflow-item"),
            }),
            Err(SimExecutionContractErrorV1::TimeOverflow)
        );
        assert!(session.pending_requests.is_empty());
        assert!(session.mutation_payloads.is_empty());

        session
            .post_order(PostOrderRequestV1 {
                context: overflow_context,
                idempotency_key: "overflow-idem".to_owned(),
                item: gtc_item("overflow-item", 50),
            })
            .expect("same identity succeeds after zero-mutation failure");

        assert!(matches!(
            session.cancel_all_orders(CancelAllOrdersRequestV1 {
                context: context(&session, request_id, 1, sent_at),
                idempotency_key: "cross-method-idem".to_owned(),
            }),
            Err(SimExecutionContractErrorV1::RequestIdentityConflict(_))
        ));

        let conflicting_context = context(&session, "conflicting-request", 2, sent_at);
        assert!(matches!(
            session.post_order(PostOrderRequestV1 {
                context: conflicting_context.clone(),
                idempotency_key: "overflow-idem".to_owned(),
                item: gtc_item("different-item", 51),
            }),
            Err(SimExecutionContractErrorV1::MutationIdentityConflict(_))
        ));
        session
            .post_order(PostOrderRequestV1 {
                context: conflicting_context,
                idempotency_key: "corrected-idem".to_owned(),
                item: gtc_item("different-item", 51),
            })
            .expect("failed idempotency conflict does not consume sequence");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_cancel_targets_schedule_once_but_keep_each_stable_item_mapping() {
        let mut session = execution_session().await;
        let registration = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: context(&session, "duplicate-cancel", 1, 3_000),
                idempotency_key: "duplicate-cancel-idem".to_owned(),
                items: vec![
                    CancelOrderItemV1 {
                        item_id: "cancel-item-a".to_owned(),
                        platform_order_id: "a".repeat(64),
                    },
                    CancelOrderItemV1 {
                        item_id: "cancel-item-b".to_owned(),
                        platform_order_id: "a".repeat(64),
                    },
                ],
            })
            .expect("duplicate target registration");

        assert_eq!(registration.receipt.scheduled_action_count, 1);
        assert_eq!(registration.items.len(), 2);
        assert_eq!(registration.items[0].scheduled_action_ordinal, 0);
        assert_eq!(registration.items[1].scheduled_action_ordinal, 0);
        assert_eq!(
            registration.items[0].effective_processing_at_utc_ms,
            registration.items[1].effective_processing_at_utc_ms
        );
    }
}
