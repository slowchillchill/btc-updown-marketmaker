use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

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
use sha2::{Digest, Sha256};

pub use crate::account::{
    INITIAL_ACCOUNT_STATE_SCHEMA_V1, InitialAccountStateErrorV1, InitialAccountStateV1,
};
use crate::finality::{TradeFinalityPolicyV1, TradeFinalityStateV1};
use crate::latency_profile::EmpiricalDelayProfile;
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventErrorV1,
    MarketLifecyclePhaseV1, MarketStateV1,
};
use crate::matching::{
    CancelOrderStateDispositionV1, ExecutionStateV1, MatchingMathErrorV1, MatchingStateErrorV1,
    QueueMatchPlanV1, SettlementLifecycleActionV1, SettlementLifecycleEffectV1, SimFillRecordV1,
    SimLiquidityRoleV1, SimOrderKindStateV1, SimOrderRecordV1, SimOrderTerminalReasonV1,
    TradeEconomicDispositionStateV1, TradeFinalityLifecycleEffectsV1,
    TradeFinalityTransitionEffectV1, allocate_taker_fee_micro_units_v1,
    cumulative_segment_quote_units_v1, decimal_from_micro_units_v1, micro_units_v1, stable_fill_id,
};
use crate::platform_rules::SealedProcessRuleSnapshot;
use crate::rebate::{MakerRebateMarketResultStatusV1, RebateKindV1, RebateSettlementActionV1};
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
    MutationResolutionDispositionV1, ParsedLogicalActionV1, ParsedMethodTimingV1,
    PlatformPrefixCursorV1, PreparedSimSessionV2, PublicVisibilityCursorV1, QueuedLogicalActionV1,
    RegisteredLogicalScheduleV1, SessionError, SimProcessRoot, SimSession,
};

const POST_ORDERS_MAX_ITEMS: usize = 15;
const CANCEL_ORDERS_MAX_ITEMS: usize = 1_000;
pub const SIM_QUERY_PAGE_SIZE_V1: usize = 100;
const SIM_QUERY_PAGE_POLICY_V1: &str = "PROJECT_QUERY_PAGE_100_V1";
const CANONICAL_REQUEST_HASH_DOMAIN: &str = "SIM_EXECUTION_CANONICAL_REQUEST_V1";
const MUTATION_PAYLOAD_HASH_DOMAIN: &str = "SIM_EXECUTION_MUTATION_PAYLOAD_V1";

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimBuyOrderV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub kind: SimBuyOrderKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderItemV1 {
    pub item_id: String,
    pub order: SimBuyOrderV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderItemV1 {
    pub item_id: String,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<PostOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<CancelOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelAllOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRequestV1 {
    pub context: RequestContextV1,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrdersFilterV1 {
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TradesFilterV1 {
    pub trade_id: Option<String>,
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
    pub after_utc_ms: Option<i64>,
    pub before_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaquePageCursorV1(String);

impl OpaquePageCursorV1 {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdersRequestV1 {
    pub context: RequestContextV1,
    pub filter: OrdersFilterV1,
    pub cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmissionFeeRuleIdentityV1 {
    pub formula_version: String,
    pub aggregation_version: String,
    pub precision_decimal_places: u32,
    pub minimum_nonzero_fee_pusd: Decimal,
    pub builder_fee_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionPlatformOrderStatusV1 {
    Live,
    Matched,
    VenueDustTerminal,
    Delayed,
    Unmatched,
    Unknown(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionTerminalReasonV1 {
    FakCompleted,
    VenueDustTerminal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmissionFillV1 {
    pub simulated_trade_id: String,
    pub source_price_pusd_per_share: Decimal,
    pub base_quantity_shares: Decimal,
    pub quote_amount_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuoteBudgetSubmissionMeasurementV1 {
    pub accepted_quote_budget_pusd: Decimal,
    pub used_quote_amount_pusd: Decimal,
    pub released_or_unspent_quote_amount_pusd: Decimal,
    pub actual_matched_base_quantity_shares: Decimal,
    pub sdk_taker_amount_audit_shares: Decimal,
    pub immediate_taker_fee_encumbered_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionMeasurementV1 {
    Base(BaseSubmissionMeasurementV1),
    QuoteBudget(QuoteBudgetSubmissionMeasurementV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedSubmissionItemV1 {
    pub simulated_platform_order_id: String,
    pub platform_order_status: SubmissionPlatformOrderStatusV1,
    pub fills: Vec<SubmissionFillV1>,
    pub terminal_reason: Option<SubmissionTerminalReasonV1>,
    pub measurement: SubmissionMeasurementV1,
    pub account_deltas: Vec<AccountDeltaReferenceV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionEvidenceUnknownReasonV1 {
    Preloading,
    TargetAssetNotFresh(MarketDataQuality),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionItemOutcomeV1 {
    Accepted(Box<AcceptedSubmissionItemV1>),
    RejectedNoEffect(SubmissionRejectedReasonV1),
    EvidenceUnknownNoEffect(SubmissionEvidenceUnknownReasonV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionEffectDispositionV1 {
    NewEffect,
    ExistingEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub item: SubmissionResponseItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub items: Vec<SubmissionResponseItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelInputItemRefV1 {
    pub item_id: String,
    pub input_item_ordinal: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelNotCanceledReasonV1 {
    NotFoundOrNotOwned,
    NoCancellableRemainder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelEvidenceUnknownReasonV1 {
    RetainedEvidenceIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanceledTargetMeasurementV1 {
    pub canceled_base_quantity_shares: Decimal,
    pub released_quote_amount_pusd: Decimal,
    pub account_delta: Option<AccountDeltaReferenceV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancelTargetOutcomeV1 {
    Canceled(CanceledTargetMeasurementV1),
    NotCanceledNoEffect(CancelNotCanceledReasonV1),
    EvidenceUnknownNoEffect(CancelEvidenceUnknownReasonV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelEffectDispositionV1 {
    NewEffect,
    ExistingEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub target: CancelTargetResponseV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub targets: Vec<CancelTargetResponseV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelAllOrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub targets: Vec<CancelTargetResponseV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedOrderStatusV1 {
    pub raw: String,
    pub known: Option<SimulatedOrderKnownStatusV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimulatedTradeKnownStatusV1 {
    MatchedNotBroadcasted,
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedTradeStatusV1 {
    pub raw: String,
    pub known: Option<SimulatedTradeKnownStatusV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKindProjectionV1 {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeLiquidityRoleV1 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeEconomicDispositionV1 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryRetentionScopeV1 {
    OrderHistory,
    TradeHistory,
    RootSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOutOfRetentionV1 {
    pub scope: QueryRetentionScopeV1,
    pub retained_from_utc_ms: Option<i64>,
    pub requested_root_sequence: Option<u64>,
    pub earliest_retained_root_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderQueryResultV1 {
    Found(Box<OrderProjectionV1>),
    NotFound,
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdersPageV1 {
    pub root_identity: String,
    pub query_projection_revision: u64,
    pub items: Vec<OrderProjectionV1>,
    pub next_cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradesPageV1 {
    pub root_identity: String,
    pub query_projection_revision: u64,
    pub items: Vec<TradeProjectionV1>,
    pub next_cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrdersQueryResultV1 {
    Page(OrdersPageV1),
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TradesQueryResultV1 {
    Page(TradesPageV1),
    OutOfRetention(QueryOutOfRetentionV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: OrderQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdersDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: OrdersQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradesDirectResponseV1 {
    pub metadata: SubmissionResponseMetadataV1,
    pub result: TradesQueryResultV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseDeliveryConfirmationV1 {
    pub session_id: String,
    pub delivery_id: String,
    pub private_feedback_sequence: u64,
    pub payload_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderLifecycleEventKindV1 {
    OrderAccepted,
    FakUnspentReleased,
    VenueDustTerminal,
    MarketResolvedCanceled,
    CallerCanceled,
    GtdExpired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MakerRebateAccrualStatusV1 {
    Calculated,
    ZeroNoEligibleMakerFill,
    UnknownIncomplete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
}

pub struct NewPreparedSessionV2 {
    pub control: SessionCreationControlResultV2,
    pub session: SimExecutionSessionV1,
}

impl SimExecutionProcessV1 {
    pub fn try_new(
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
        })
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        self.inner.run_id()
    }

    #[must_use]
    pub fn finality_policy(&self) -> &Arc<TradeFinalityPolicyV1> {
        self.inner.finality_policy()
    }

    pub fn create_session(
        &mut self,
        spec: SessionSpecV2,
        response_bridge: SubmissionResponseBridgeV1,
    ) -> Result<NewPreparedSessionV2, SimExecutionContractErrorV1> {
        let response_outbox_capacity = spec.response_outbox_capacity;
        let PreparedSimSessionV2 { session, control } = self.inner.create_session(spec)?;
        Ok(NewPreparedSessionV2 {
            control,
            session: SimExecutionSessionV1 {
                session,
                pending_requests: BTreeMap::new(),
                mutation_payloads: BTreeMap::new(),
                submission_effect_records: BTreeMap::new(),
                cancel_effect_records: BTreeMap::new(),
                query_state: QueryStateV1::new(),
                direct_response_outbox: LogicalDirectResponseOutboxV1::new(
                    response_outbox_capacity,
                ),
                lifecycle_outbox: LogicalLifecycleOutboxV1::new(response_outbox_capacity),
                accepting_new_ingress: true,
                response_bridge,
            },
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
        self.inner.activate_session(request)
    }

    pub fn fail_session_initialization(
        &mut self,
        request: FailSessionInitializationRequestV1,
    ) -> Result<SessionInitializationControlResultV1, SimExecutionContractErrorV1> {
        self.inner.fail_session_initialization(request)
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
        self.inner.enroll_market(&mut session.session, request)
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
    accepting_new_ingress: bool,
    response_bridge: SubmissionResponseBridgeV1,
}

#[derive(Clone)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct CancelTargetEffectV1 {
    platform_order_id: String,
    effect_identity: String,
    outcome: CancelTargetOutcomeV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueryRootRowsV1 {
    Orders(Arc<[OrderProjectionV1]>),
    Trades(Arc<[TradeProjectionV1]>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct IssuedCursorV1 {
    root_sequence: u64,
    root_identity: Sha256Hex,
    method: ListQueryMethodV1,
    filter_sha256: Sha256Hex,
    query_projection_revision: u64,
    retention_floor_at_issue: u64,
    next_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryStateV1 {
    next_root_sequence: u64,
    earliest_retained_root_sequence: u64,
    roots: BTreeMap<u64, QueryRootV1>,
    issued_cursors: BTreeMap<String, IssuedCursorV1>,
    continuation_by_request: BTreeMap<String, IssuedCursorV1>,
    order_history_retained_from_utc_ms: Option<i64>,
    trade_history_retained_from_utc_ms: Option<i64>,
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
        }
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
        Ok((root_sequence, hash.finish()))
    }

    fn issue_cursor(&mut self, binding: IssuedCursorV1) -> OpaquePageCursorV1 {
        let mut hash = CanonicalHasher::new("SIM_QUERY_CURSOR_V1");
        hash.text(&binding.root_identity.0);
        hash.text(binding.method.identity());
        hash.text(&binding.filter_sha256.0);
        hash.u64(binding.query_projection_revision);
        hash.u64(binding.retention_floor_at_issue);
        hash.u64(binding.root_sequence);
        hash.text(SIM_QUERY_PAGE_POLICY_V1);
        hash.u64(binding.next_index as u64);
        let token = hash.finish().0;
        self.issued_cursors.insert(token.clone(), binding);
        OpaquePageCursorV1(token)
    }

    #[cfg(test)]
    fn advance_root_retention_floor(
        &mut self,
        new_floor: u64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        if new_floor < self.earliest_retained_root_sequence || new_floor > self.next_root_sequence {
            return Err(SimExecutionContractErrorV1::PlatformRevisionRegression);
        }
        self.roots.retain(|sequence, _| *sequence >= new_floor);
        self.earliest_retained_root_sequence = new_floor;
        Ok(())
    }

    #[cfg(test)]
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

    #[cfg(test)]
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
        if !bucket.drives_execution_for(self.session.session_spec().caller_role) {
            let (disposition, feedbacks) = self
                .session
                .apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
            debug_assert!(feedbacks.trade_finality_effects.is_empty());
            debug_assert!(feedbacks.venue_dust_effects.is_empty());
            debug_assert!(feedbacks.market_resolution_order_effects.is_empty());
            debug_assert!(feedbacks.settlement_feedbacks.is_empty());
            return Ok(disposition);
        }

        if self.session.last_applied_bucket_coordinates_match(&bucket) {
            let (disposition, feedbacks) = self
                .session
                .apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
            debug_assert!(feedbacks.trade_finality_effects.is_empty());
            debug_assert!(feedbacks.venue_dust_effects.is_empty());
            debug_assert!(feedbacks.market_resolution_order_effects.is_empty());
            debug_assert!(feedbacks.settlement_feedbacks.is_empty());
            return Ok(disposition);
        }

        let bucket_at_utc_ms = bucket.bucket_at_utc_ms();
        let bucket_work_identity = bucket.lifecycle_work_identity();
        let mut working_session = self.session.clone();
        let mut working_lifecycle_outbox = self.lifecycle_outbox.clone();
        let query_projection_changed = drive_scheduled_platform_work_through(
            &mut working_session,
            &mut working_lifecycle_outbox,
            bucket_at_utc_ms,
            None,
        )
        .map_err(|error| match error {
            LogicalPlatformDriveErrorV1::Session(error) => MarketEventErrorV1::from(error),
            LogicalPlatformDriveErrorV1::Matching(error) => MarketEventErrorV1::from(error),
        })?;
        if query_projection_changed {
            working_session.advance_platform_state_revision_once()?;
        }
        let (disposition, lifecycle_effects) =
            working_session.apply_closed_market_event_bucket_with_lifecycle_effects(bucket)?;
        if disposition == MarketBucketApplyDispositionV1::Applied {
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
        self.session = working_session;
        self.lifecycle_outbox = working_lifecycle_outbox;
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

    #[allow(dead_code)]
    pub(crate) fn take_next_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<QueuedLogicalActionV1>, SimExecutionContractErrorV1> {
        self.session
            .take_next_action_through(effective_processing_at_utc_ms)
    }

    #[allow(dead_code)]
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

    #[cfg(test)]
    pub(crate) fn advance_query_root_retention_floor(
        &mut self,
        new_floor: u64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        self.query_state.advance_root_retention_floor(new_floor)
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

    #[cfg(test)]
    pub(crate) fn advance_order_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        self.query_state
            .advance_order_history_retention_floor(retained_from_utc_ms)
    }

    #[cfg(test)]
    pub(crate) fn advance_trade_history_retention_floor(
        &mut self,
        retained_from_utc_ms: i64,
    ) -> Result<(), SimExecutionContractErrorV1> {
        self.query_state
            .advance_trade_history_retention_floor(retained_from_utc_ms)
    }

    #[allow(dead_code)]
    pub(crate) fn next_order_deadline_at_utc_ms(&self) -> Option<i64> {
        self.session.execution_state().next_gtd_deadline_at_utc_ms()
    }

    pub(crate) fn drive_next_execution_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<SubmissionDriveDispositionV1, SubmissionProcessingErrorV1> {
        let result =
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
        result
    }

    fn drive_next_execution_action_through_working(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<SubmissionDriveDispositionV1, SubmissionProcessingErrorV1> {
        let mut working_session = self.session.clone();
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
            self.session = working_session;
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
        let mut working_query_state = self.query_state.clone();
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
                            let (outcome, feedback) = cancel_target_outcome(
                                execution.cancel_order_remainder(
                                    &target_group.platform_order_id,
                                    formed_at,
                                    &Sha256Hex(target_effect_identity.clone()),
                                )?,
                                order.as_ref(),
                            )?;
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
                let result = match working_session
                    .execution_state()
                    .order(&request.platform_order_id)
                {
                    Some(order) => OrderQueryResultV1::Found(Box::new(project_order(order)?)),
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
                        working_session
                            .execution_state()
                            .fills()
                            .any(|fill| &fill.trade_id == id)
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
                        let mut rows = if empty_interval {
                            Vec::new()
                        } else {
                            working_session
                                .execution_state()
                                .fills()
                                .filter(|fill| {
                                    request
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
                                })
                                .map(project_trade)
                                .collect::<Result<Vec<_>, _>>()?
                        };
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
            return Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse);
        };
        self.session.validate_prepared_private_fact(&candidate)?;
        let (expected_confirmation, acceptance) = match &candidate.evidence {
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
                let expected_confirmation = sealed.confirmation();
                let acceptance = match &sealed {
                    SealedDirectResponseV1::PostOrder(response) => {
                        self.response_bridge.accept_post_order(Arc::clone(response))
                    }
                    SealedDirectResponseV1::PostOrders(response) => self
                        .response_bridge
                        .accept_post_orders(Arc::clone(response)),
                    SealedDirectResponseV1::CancelOrder(response) => self
                        .response_bridge
                        .accept_cancel_order(Arc::clone(response)),
                    SealedDirectResponseV1::CancelOrders(response) => self
                        .response_bridge
                        .accept_cancel_orders(Arc::clone(response)),
                    SealedDirectResponseV1::CancelAllOrders(response) => self
                        .response_bridge
                        .accept_cancel_all_orders(Arc::clone(response)),
                    SealedDirectResponseV1::Order(response) => {
                        self.response_bridge.accept_order(Arc::clone(response))
                    }
                    SealedDirectResponseV1::Orders(response) => {
                        self.response_bridge.accept_orders(Arc::clone(response))
                    }
                    SealedDirectResponseV1::Trades(response) => {
                        self.response_bridge.accept_trades(Arc::clone(response))
                    }
                };
                (expected_confirmation, acceptance)
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
                let expected_confirmation = sealed.confirmation();
                let acceptance = match &sealed {
                    SealedLifecycleFeedbackV1::Order(feedback) => self
                        .response_bridge
                        .accept_order_lifecycle_feedback(Arc::clone(feedback)),
                    SealedLifecycleFeedbackV1::Trade(feedback) => self
                        .response_bridge
                        .accept_trade_lifecycle_feedback(Arc::clone(feedback)),
                    SealedLifecycleFeedbackV1::Settlement(feedback) => self
                        .response_bridge
                        .accept_settlement_lifecycle_feedback(Arc::clone(feedback)),
                };
                (expected_confirmation, acceptance)
            }
        };
        match acceptance {
            SubmissionResponseAcceptanceV1::Accepted(actual) if actual == expected_confirmation => {
                self.session.commit_prevalidated_private_fact(&candidate);
                match &candidate.evidence {
                    crate::session::PrivateVisibilityEvidenceV1::DirectResponse {
                        global_request_id,
                    } => self
                        .direct_response_outbox
                        .remove_prevalidated_confirmed(global_request_id),
                    crate::session::PrivateVisibilityEvidenceV1::LifecycleFeedback {
                        feedback_id,
                    } => self
                        .lifecycle_outbox
                        .remove_prevalidated_confirmed(feedback_id),
                }
                Ok(SubmissionDeliveryDispositionV1::Delivered)
            }
            SubmissionResponseAcceptanceV1::Accepted(_) => {
                self.accepting_new_ingress = false;
                Err(SimExecutionContractErrorV1::ResponseDeliveryIdentityConflict)
            }
            SubmissionResponseAcceptanceV1::Full => Ok(SubmissionDeliveryDispositionV1::Full),
            SubmissionResponseAcceptanceV1::Disconnected => {
                self.accepting_new_ingress = false;
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
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request)
            && let Some(existing) = self.mutation_payloads.get(&idempotency_key)
            && existing != &payload_sha256
        {
            return Err(SimExecutionContractErrorV1::MutationIdentityConflict(
                idempotency_key,
            ));
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

        self.pending_requests
            .entry(request_id)
            .or_insert(request.clone());
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request) {
            self.mutation_payloads
                .entry(idempotency_key)
                .or_insert(payload_sha256);
        }
        self.session = working_session;
        self.query_state = working_query_state;
        self.direct_response_outbox = working_outbox;
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

impl SealedDirectResponseV1 {
    fn confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        match self {
            Self::PostOrder(response) => response.metadata.delivery_confirmation(),
            Self::PostOrders(response) => response.metadata.delivery_confirmation(),
            Self::CancelOrder(response) => response.metadata.delivery_confirmation(),
            Self::CancelOrders(response) => response.metadata.delivery_confirmation(),
            Self::CancelAllOrders(response) => response.metadata.delivery_confirmation(),
            Self::Order(response) => response.metadata.delivery_confirmation(),
            Self::Orders(response) => response.metadata.delivery_confirmation(),
            Self::Trades(response) => response.metadata.delivery_confirmation(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DirectResponseOutboxEntryV1 {
    method: DirectResponseMethodV1,
    delivery_id: String,
    parts: Vec<Option<DirectResponsePartV1>>,
    platform_responded_at_utc_ms: Option<i64>,
    draft: Option<DirectResponseDraftV1>,
    sealed: Option<SealedDirectResponseV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogicalDirectResponseOutboxV1 {
    capacity: usize,
    entries: BTreeMap<String, DirectResponseOutboxEntryV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct SettlementLifecycleFeedbackDraftV1 {
    feedback_id: String,
    delivery_id: String,
    account_instance_key: AccountInstanceKeyV1,
    session_id: String,
    timing: crate::session::LifecycleFeedbackTimingV1,
    settlement_identity: String,
    event: SettlementLifecycleEventDraftV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

    fn timing(&self) -> &crate::session::LifecycleFeedbackTimingV1 {
        match self {
            Self::Order(draft) => &draft.timing,
            Self::Trade(draft) => &draft.timing,
            Self::Settlement(draft) => &draft.timing,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SealedLifecycleFeedbackV1 {
    Order(Arc<OrderLifecycleFeedbackV1>),
    Trade(Arc<TradeLifecycleFeedbackV1>),
    Settlement(Arc<SettlementLifecycleFeedbackV1>),
}

impl SealedLifecycleFeedbackV1 {
    fn confirmation(&self) -> ResponseDeliveryConfirmationV1 {
        match self {
            Self::Order(feedback) => feedback.metadata.delivery_confirmation(),
            Self::Trade(feedback) => feedback.metadata.delivery_confirmation(),
            Self::Settlement(feedback) => feedback.metadata.delivery_confirmation(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecycleOutboxEntryV1 {
    batch_id: String,
    draft: LifecycleFeedbackDraftV1,
    sealed: Option<SealedLifecycleFeedbackV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
        let mut hash = CanonicalHasher::new("SIM_SUBMISSION_DELIVERY_ID_V1");
        hash.text(session_id);
        hash.text(global_request_id);
        hash.text(method.identity());
        let delivery_id = hash.finish().0;
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
    use crate::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};
    use crate::platform_rules::tests::successful_snapshot_for_sim_tests;

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
