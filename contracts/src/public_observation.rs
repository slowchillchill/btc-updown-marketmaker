//! 公共参考事实、逐事实业务观测与串行流引用。

use serde::{Deserialize, Serialize};

use crate::{
    BinanceAggregateTradeReferenceV1, BinanceDepthStateReferenceV1, CanonicalEventId,
    ChainlinkBtcUsdReferenceV1, CollectorAdmissionCoordinateV1, EventQualityV2, EventTimeV2,
    ExactDecimal, MarketIdentityV2, Sha256Hex, SourceNativeIdentityV1,
};

pub const CANONICAL_REFERENCE_EVENT_SCHEMA_V2: &str = "canonical_reference_event_v2";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PublicFactPositionV1 {
    pub input_stream_id: String,
    pub closed_at_input_position: u64,
    pub fact_ordinal: u32,
}

/// 消费流绑定同一运行、路径、初态及规则；不是规范来源身份。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PmDerivedStepPositionV1 {
    pub consumer_stream_id: String,
    pub canonical_record_position: u64,
    pub member_index: Option<u32>,
    pub step_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedPmStepIdV1(pub Sha256Hex);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PmDerivedStepBindingV1 {
    pub position: PmDerivedStepPositionV1,
    pub derived_id: DerivedPmStepIdV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalSourceKindV2 {
    BinanceAggtrade,
    BinanceDepthSnapshot,
    BinanceDepthUpdate,
    PolymarketMarketMetadata,
    PolymarketOrderbook,
    PolymarketRtdsChainlinkBtcUsd,
    PolymarketRtdsChainlinkTwap,
}

/// 真实来源消息身份不依赖最终 Parquet 或 manifest 的物理摘要。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEvidenceRefV3 {
    pub source_kind: CanonicalSourceKindV2,
    pub source_message_id: Sha256Hex,
    pub source_native_identity: Option<SourceNativeIdentityV1>,
    pub source_event_at_utc_ms: Option<i64>,
    pub received_at_utc_ms: i64,
    pub collector_admission: Option<CollectorAdmissionCoordinateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLineageV3 {
    pub canonical_rule_version: String,
    pub source_evidence: Vec<SourceEvidenceRefV3>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BtcSettlementReferenceV1 {
    ChainlinkSpot,
    ChainlinkTwapThirtySeconds,
    ChainlinkTwapSixtySeconds,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAnchorSourceV1 {
    GammaEventMetadataPriceToBeat,
    OfficialCryptoPriceOpenPrice,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum PriceAnchorApplicationV1 {
    Observed,
    HistoricalRepair {
        policy_version: String,
        historical_identity_ready_at_utc_ms: i64,
        effective_from_utc_ms: i64,
        historical_identity_evidence: Vec<SourceEvidenceRefV3>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketPriceAnchorV1 {
    pub price_usd_per_btc: ExactDecimal,
    pub source: MarketAnchorSourceV1,
    pub application: PriceAnchorApplicationV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketPriceAnchorUpdateV1 {
    pub market: MarketIdentityV2,
    pub anchor: MarketPriceAnchorV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSettlementReferenceUpdateV1 {
    pub market: MarketIdentityV2,
    pub reference: BtcSettlementReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainlinkBtcUsdTwapReferenceV1 {
    pub symbol: String,
    pub window_s: u64,
    pub price_usd_per_btc: ExactDecimal,
    pub payload_timestamp_utc_ms: i64,
    pub message_timestamp_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalPriceAnchorRepairBatchV1 {
    pub anchors: Vec<MarketPriceAnchorUpdateV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RtdsObservedAnomalyClassificationV1 {
    InvalidType,
    InvalidTopic,
    InvalidSymbol,
    InvalidTimestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RtdsObservedAnomalyV1 {
    pub classification: RtdsObservedAnomalyClassificationV1,
    pub observed_topic: String,
    pub observed_message_type: String,
    pub observed_symbol: String,
    pub observed_value_raw: String,
    pub observed_message_timestamp_raw: i64,
    pub observed_payload_timestamp_raw: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "reference_type",
    content = "reference",
    rename_all = "snake_case"
)]
pub enum CanonicalReferenceEventPayloadV2 {
    ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1),
    BinanceDepthState(BinanceDepthStateReferenceV1),
    BinanceAggregateTrade(BinanceAggregateTradeReferenceV1),
    MarketPriceAnchorUpdate(MarketPriceAnchorUpdateV1),
    MarketSettlementReferenceUpdate(MarketSettlementReferenceUpdateV1),
    ChainlinkBtcUsdTwap(ChainlinkBtcUsdTwapReferenceV1),
    RtdsObservedAnomaly(RtdsObservedAnomalyV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalReferenceEventV2 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub time: EventTimeV2,
    pub lineage: EventLineageV3,
    pub quality: EventQualityV2,
    pub payload: CanonicalReferenceEventPayloadV2,
}

pub const CANONICAL_MARKET_FACT_SCHEMA_V1: &str = "canonical_market_fact_v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalMarketFactPayloadV1 {
    FixedIdentity {
        assets: Vec<crate::AssetIdentityV1>,
    },
    InitialTick {
        asset: crate::AssetIdentityV1,
        tick_size_pusd_per_share: ExactDecimal,
    },
    LifecycleTransition(crate::MarketLifecycleTransitionV2),
    MarketLifecycleSnapshot {
        current_state: crate::MappedMarketLifecycleStateV1,
        assets: Vec<crate::AssetIdentityV1>,
    },
    TradingState(crate::MarketTradingStateV2),
    Terminal(crate::MarketResolutionTerminalV3),
    QualityBoundary(crate::QualityBoundaryV2),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicBusinessPayloadV1 {
    PolymarketMessage(crate::CanonicalPmMessageV1),
    MarketFact {
        market: MarketIdentityV2,
        payload: CanonicalMarketFactPayloadV1,
    },
    ReferenceFact(CanonicalReferenceEventPayloadV2),
}

/// 实际归约结果，不需要耐久身份即可消费。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicBusinessFactV1 {
    pub position: PublicFactPositionV1,
    pub payload: PublicBusinessPayloadV1,
    pub source_refs: Vec<crate::CanonicalInputRecordRefV1>,
    pub lineage: EventLineageV3,
    pub direct_time: EventTimeV2,
    pub dependency_time: EventTimeV2,
    pub quality: EventQualityV2,
    pub direct_sources: Vec<SourceEvidenceRefV3>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalMarketFactV1 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub market: MarketIdentityV2,
    pub group_id: Sha256Hex,
    pub source_refs: Vec<crate::CanonicalInputRecordRefV1>,
    pub direct_source_message_ids: Vec<Sha256Hex>,
    pub lineage: EventLineageV3,
    pub direct_time: EventTimeV2,
    pub dependency_time: EventTimeV2,
    pub quality: EventQualityV2,
    pub payload: CanonicalMarketFactPayloadV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalEventRecordV2 {
    PolymarketMessage(crate::CanonicalPmMessageV1),
    ReferenceEvent(CanonicalReferenceEventV2),
    MarketFact(CanonicalMarketFactV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicFactBindingV1 {
    pub position: PublicFactPositionV1,
    pub event_id: CanonicalEventId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedIngressPrefixV1 {
    pub input_stream_id: String,
    pub next_input_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectObservationAnchorV1 {
    pub field_path: String,
    pub position: PublicFactPositionV1,
    pub group_id: Sha256Hex,
    pub source_message_id: Sha256Hex,
    pub source_event_at_utc_ms: Option<i64>,
    pub received_at_utc_ms: i64,
}

/// 来源准备阶段由核心纯函数产生的唯一选择结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedReferenceSelectionV1 {
    pub selected_record_ref: Option<crate::CanonicalInputRecordRefV1>,
    pub selected_value: Option<ChainlinkBtcUsdTwapReferenceV1>,
    pub direct_anchor: Option<DirectObservationAnchorV1>,
    pub dependency_input_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicReportPositionV1 {
    pub input_stream_id: String,
    pub closed_at_input_position: u64,
    pub report_ordinal: u32,
}

/// 一次分配并由相同原序重放复用的流头；初态始终为空。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicInputStreamHeaderV1 {
    pub input_stream_id: String,
    pub canonical_rule_version: String,
    pub observation_version: String,
    pub ordering_version: String,
    pub input_scope: String,
    pub max_pending_ws_terminals: usize,
    pub max_retained_reducer_state_entries: usize,
    pub initial_state: PublicInitialStateV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedFactV1<T> {
    pub value: Option<T>,
    pub quality: crate::MarketDataQuality,
    pub direct_anchors: Vec<DirectObservationAnchorV1>,
    pub dependency_frontier: Option<AppliedIngressPrefixV1>,
}
impl<T> Default for ObservedFactV1<T> {
    fn default() -> Self {
        Self {
            value: None,
            quality: crate::MarketDataQuality::UnknownNotObserved,
            direct_anchors: Vec::new(),
            dependency_frontier: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporaryTwapReferenceV1 {
    pub value: ChainlinkBtcUsdTwapReferenceV1,
    pub selected_position: PublicFactPositionV1,
    pub direct_anchor: DirectObservationAnchorV1,
    pub dependency_frontier: AppliedIngressPrefixV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectiveMarketReferenceV1 {
    Missing,
    TemporaryTwap(TemporaryTwapReferenceV1),
    OfficialAnchor(ObservedFactV1<MarketPriceAnchorV1>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetBusinessObservationV1 {
    pub asset: crate::AssetIdentityV1,
    pub bids: ObservedFactV1<Vec<crate::PriceLevelV1>>,
    pub asks: ObservedFactV1<Vec<crate::PriceLevelV1>>,
    pub best_bid_ask: ObservedFactV1<crate::CanonicalBbaFactV1>,
    pub tick: ObservedFactV1<ExactDecimal>,
    pub public_trade: ObservedFactV1<crate::PublicTradeV2>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketBusinessObservationV1 {
    pub schema_version: String,
    pub market: MarketIdentityV2,
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub visible_prefix: AppliedIngressPrefixV1,
    pub assets: Vec<AssetBusinessObservationV1>,
    pub lifecycle: ObservedFactV1<crate::MappedMarketLifecycleStateV1>,
    pub active: ObservedFactV1<bool>,
    pub closed: ObservedFactV1<bool>,
    pub accepting_orders: ObservedFactV1<bool>,
    pub terminal: ObservedFactV1<crate::MarketResolutionTerminalV3>,
    pub official_anchor: ObservedFactV1<MarketPriceAnchorV1>,
    pub settlement_reference: ObservedFactV1<BtcSettlementReferenceV1>,
    pub temporary_reference: Option<TemporaryTwapReferenceV1>,
    pub effective_reference: EffectiveMarketReferenceV1,
    pub chainlink_spot: ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
    pub chainlink_twap_thirty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    pub chainlink_twap_sixty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    pub binance_depth: ObservedFactV1<BinanceDepthStateReferenceV1>,
    pub binance_trade: ObservedFactV1<BinanceAggregateTradeReferenceV1>,
    pub group_events: Vec<PublicBusinessFactV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedBusinessGroupV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub records: Vec<PublicBusinessFactV1>,
    pub observations: Vec<MarketBusinessObservationV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedIngressV1 {
    pub applied_prefix: AppliedIngressPrefixV1,
    pub closed_groups: Vec<ClosedBusinessGroupV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicInitialStateV1 {
    Empty,
}
