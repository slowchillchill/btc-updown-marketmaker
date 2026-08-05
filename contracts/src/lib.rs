use serde::{Deserialize, Serialize};

pub const EXECUTION_MARKET_EVENT_SCHEMA_V2: &str = "execution_market_event_v2";
pub const MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V2: &str = "market_observation_causal_shell_v2";
pub const GATE_A_CAPABILITY_MATRIX_SCHEMA_V2: &str = "gate_a_capability_matrix_v2";
pub const GATE_A_RECEIPT_SCHEMA_V2: &str = "real_execution_tape_receipt_v2";
pub const EVENT_TIME_INDEX_SCHEMA_V2: &str = "event_time_index_v2";
pub const VISIBILITY_INDEX_SCHEMA_V2: &str = "visibility_index_v2";
pub const DATASET_LINEAGE_ROOT: &str = "/home/chillchill/new-datasets";
pub const DATASET_DATE_FLOOR: &str = "2026-07-30";

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
pub struct MarketWindowV1 {
    pub window_id: Sha256Hex,
    pub start_at_utc_ms: i64,
    pub end_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketIdentityV1 {
    pub condition_id: ConditionId,
    pub clob_market_id: ClobMarketId,
    pub window: MarketWindowV1,
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
pub struct MarketLifecycleTransitionV1 {
    pub previous_state: Option<MappedMarketLifecycleStateV1>,
    pub current_state: MappedMarketLifecycleStateV1,
    pub assets: Vec<AssetIdentityV1>,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTradingStateV1 {
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialL2SnapshotV1 {
    pub asset: AssetIdentityV1,
    pub initial_tick_size_pusd_per_share: ExactDecimal,
    pub source_sequence_epoch_id: String,
    pub source_cursor: u64,
    pub book_digest_sha256: Sha256Hex,
    pub bids: Vec<PriceLevelV1>,
    pub asks: Vec<PriceLevelV1>,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BestBidAskV1 {
    pub asset: AssetIdentityV1,
    pub best_bid_pusd_per_share: Option<ExactDecimal>,
    pub best_ask_pusd_per_share: Option<ExactDecimal>,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickSizeChangeV1 {
    pub asset: AssetIdentityV1,
    pub old_tick_size_pusd_per_share: Option<ExactDecimal>,
    pub new_tick_size_pusd_per_share: ExactDecimal,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicTradeIdSourceV1 {
    PlatformTransactionHash,
    DerivedSourceEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicTradeV1 {
    pub asset: AssetIdentityV1,
    pub trade_id: String,
    pub trade_id_source: PublicTradeIdSourceV1,
    pub price_pusd_per_share: ExactDecimal,
    pub quantity_shares: ExactDecimal,
    pub aggressor_side: TradeSide,
    pub fee_rate_bps: Option<ExactDecimal>,
    pub source_event_ids: Vec<CanonicalEventId>,
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
pub struct LevelExecutionTransitionV2 {
    pub causal_group_id: Sha256Hex,
    pub market: MarketIdentityV1,
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
    pub source_event_ids: Vec<CanonicalEventId>,
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
pub struct QualityBoundaryV1 {
    pub scope: QualityScopeV1,
    pub previous_quality: MarketDataQuality,
    pub current_quality: MarketDataQuality,
    pub reason_code: String,
    pub source_event_ids: Vec<CanonicalEventId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayoutEntryV1 {
    pub asset: AssetIdentityV1,
    pub payout_pusd_per_share: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalWatermarkV2 {
    pub last_source_event_at_utc_ms: Option<i64>,
    pub last_received_at_utc_ms: i64,
    pub canonical_event_root_sha256: Sha256Hex,
    pub lineage_identity_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum MarketResolutionTerminalV2 {
    Resolved {
        winning_asset_id: String,
        winning_outcome: String,
        payout_vector: Vec<PayoutEntryV1>,
        mapping_rule_version: String,
        source_event_ids: Vec<CanonicalEventId>,
        terminal_watermark: TerminalWatermarkV2,
    },
    WaitingExhaustedQuarantine {
        reason_code: String,
        source_event_ids: Vec<CanonicalEventId>,
        terminal_watermark: TerminalWatermarkV2,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "event_type",
    content = "event",
    rename_all = "snake_case"
)]
pub enum ExecutionMarketEventPayloadV2 {
    MarketLifecycleTransition(MarketLifecycleTransitionV1),
    MarketTradingState(MarketTradingStateV1),
    InitialL2Snapshot(InitialL2SnapshotV1),
    BestBidAsk(BestBidAskV1),
    TickSizeChange(TickSizeChangeV1),
    PublicTrade(PublicTradeV1),
    LevelExecutionTransition(LevelExecutionTransitionV2),
    QualityBoundary(QualityBoundaryV1),
    MarketResolutionTerminal(MarketResolutionTerminalV2),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionMarketEventV2 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub market: MarketIdentityV1,
    pub time: EventTimeV2,
    pub lineage: EventLineageV2,
    pub quality: EventQualityV2,
    pub payload: ExecutionMarketEventPayloadV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationLineageV2 {
    pub execution_event_contract_version: String,
    pub input_set_identity_sha256: Sha256Hex,
    pub visibility_index_manifest_sha256: Sha256Hex,
    pub contributing_event_ids: Vec<CanonicalEventId>,
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
pub struct MarketObservationCausalShellV2 {
    pub schema_version: String,
    pub observation_id: Sha256Hex,
    pub market: MarketIdentityV1,
    pub lineage: ObservationLineageV2,
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
pub struct GateAReceiptV2 {
    pub schema_version: String,
    pub receipt_id: Sha256Hex,
    pub requirement_id: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub execution_event_contract_version: String,
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
