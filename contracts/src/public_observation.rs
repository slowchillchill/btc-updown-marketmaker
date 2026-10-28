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
    BinanceDepthState(std::sync::Arc<BinanceDepthStateReferenceV1>),
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
    PolymarketMessage(std::sync::Arc<crate::CanonicalPmMessageV1>),
    MarketFact {
        market: MarketIdentityV2,
        payload: CanonicalMarketFactPayloadV1,
    },
    ReferenceFact(CanonicalReferenceEventPayloadV2),
    /// 深度正文由原输入重建；位置、身份和事实时间仍在原组内。
    BinanceDepthReference(PublicFactBindingV1),
    /// 日内状态事实的位置引用；数量按共同需求从观察投影交付。
    BinanceDepthPositionReference {
        quantities: Option<std::sync::Arc<BinanceDepthStateReferenceV1>>,
    },
}

/// 完整来源谱系与日内状态位置是不同表示；位置不冒充内容身份。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PublicBusinessLineageV1 {
    Canonical(EventLineageV3),
    StateReference { canonical_rule_version: String },
}

/// 实际归约结果，不需要耐久身份即可消费。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicBusinessFactV1 {
    pub position: PublicFactPositionV1,
    pub payload: PublicBusinessPayloadV1,
    pub source_refs: Vec<crate::CanonicalInputRecordRefV1>,
    pub lineage: PublicBusinessLineageV1,
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
    pub bids: std::sync::Arc<ObservedFactV1<Vec<crate::PriceLevelV1>>>,
    pub asks: std::sync::Arc<ObservedFactV1<Vec<crate::PriceLevelV1>>>,
    pub best_bid_ask: ObservedFactV1<crate::CanonicalBbaFactV1>,
    /// 同来源版本选择的最佳买价；Some(None) 是有效空买盘。直接 BBA 保持独立语义。
    #[serde(default)]
    pub valuation_best_bid: std::sync::Arc<ObservedFactV1<Option<ExactDecimal>>>,
    pub tick: ObservedFactV1<ExactDecimal>,
    pub public_trade: ObservedFactV1<crate::PublicTradeV2>,
}
/// 持续业务流的进展；固定市场锚点与已判定结果不属于此类型。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStreamScopeV1 {
    pub source: SourceStreamKindV1,
    pub condition_id: Option<crate::ConditionId>,
    pub asset_or_symbol: String,
}
impl PartialOrd for SourceStreamScopeV1 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SourceStreamScopeV1 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            &self.source,
            self.condition_id.as_ref().map(|v| &v.0),
            &self.asset_or_symbol,
        )
            .cmp(&(
                &other.source,
                other.condition_id.as_ref().map(|v| &v.0),
                &other.asset_or_symbol,
            ))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceStreamKindV1 {
    BinanceDepth,
    BinanceAggregateTrade,
    PolymarketBook,
    ChainlinkSpot,
    ChainlinkTwapThirty,
    ChainlinkTwapSixty,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceSynchronizationV1 {
    Unknown,
    SnapshotRequired,
    SnapshotObserved { source_message_id: Sha256Hex },
}
/// 唯一归约输入产生的流投影。连接代次存在并不证明当前连接存活。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStreamProgressV1 {
    pub scope: SourceStreamScopeV1,
    pub last_valid_business_message: Option<SourceEvidenceRefV3>,
    pub last_observed_message: SourceEvidenceRefV3,
    pub connection_generation: Option<u64>,
    pub synchronization: SourceSynchronizationV1,
    pub quality: EventQualityV2,
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
    /// 同一关闭组的市场共享一次完整投影；后续组独立生成快照。
    pub binance_depth: std::sync::Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    pub binance_trade: ObservedFactV1<BinanceAggregateTradeReferenceV1>,
    /// 与关闭组共享全部事实；跨市场事件仍参与组统计与时序。
    pub group_events: std::sync::Arc<[PublicBusinessFactV1]>,
    /// 显式选用的来源进展投影；空集合表示没有此证据，不能从字段锚点补造。
    #[serde(default, skip_serializing_if = "source_progress_is_empty")]
    pub source_progress: SharedSourceProgressV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedBusinessGroupV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub records: std::sync::Arc<[PublicBusinessFactV1]>,
    pub observations: MarketObservationSetV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedIngressV1 {
    pub applied_prefix: AppliedIngressPrefixV1,
    pub closed_groups: Vec<std::sync::Arc<ClosedBusinessGroupV1>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicInitialStateV1 {
    Empty,
}

/// 同批记录及其引用都已稳定的前缀；与内存已应用凭据分开。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedIngressPrefixV1 {
    pub input_stream_id: String,
    pub next_input_position: u64,
    pub next_canonical_position: u64,
    pub next_group_position: u64,
    pub next_observation_position: u64,
    pub next_public_index_position: u64,
    pub stable_storage_ref: String,
    pub normal_end: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceArtifactBindingV1 {
    pub source_message_id: Sha256Hex,
    pub manifest_sha256: Sha256Hex,
    pub file_sha256: Sha256Hex,
    pub relative_path: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicGroupIndexV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub records: Vec<PublicFactPositionV1>,
    pub observation_positions: Vec<u64>,
}
/// 状态由唯一输入流重建；索引不复制整棵观测或其内嵌来源事实。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicObservationIndexV1 {
    pub observation_position: u64,
    pub market: MarketIdentityV2,
    pub group_position: u64,
    pub visible_prefix: AppliedIngressPrefixV1,
    pub group_events: Vec<PublicFactPositionV1>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayIndexClosureV1 {
    NextGlobalEventAt(i64),
    EndOfInput,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiddenDerivedStepRefV1 {
    pub canonical_fact_id: crate::CanonicalEventId,
    pub canonical_record_position: u64,
    pub member_index: Option<u32>,
    pub step_index: u32,
    pub derivation_position: u64,
    pub position: PmDerivedStepPositionV1,
    pub derived_id: DerivedPmStepIdV1,
    pub dependency_time: EventTimeV2,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedHiddenStepBucketV1 {
    pub committed_prefix: CommittedIngressPrefixV1,
    pub bucket_time_utc_ms: i64,
    pub index_start: u64,
    pub index_end: u64,
    pub steps: Vec<HiddenDerivedStepRefV1>,
    /// 完整隐藏外排索引中的全局位置；通过索引项再定位唯一规范记录。
    pub reference_positions: Vec<u64>,
    pub closure: ReplayIndexClosureV1,
}

/// 原始日流的完整隐藏桶来源；不声称完整录制材料已经耐久发布。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingHiddenSourceBindingV1 {
    pub input_stream_id: String,
    pub input_set_identity_sha256: Sha256Hex,
    pub applied_prefix: AppliedIngressPrefixV1,
    /// 同刻先参考，再按 epoch／来源时刻／准入／规范位置的 PM 派生先后。
    pub ordering_version: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedStreamingHiddenStepBucketV1 {
    pub source: StreamingHiddenSourceBindingV1,
    pub bucket_time_utc_ms: i64,
    pub index_start: u64,
    pub index_end: u64,
    pub closure: ReplayIndexClosureV1,
}

/// DPT 已应用组的原位置，不代表磁盘稳定或受信 UTC。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicGroupReferenceV1 {
    pub input_stream_id: String,
    pub applied_prefix: AppliedIngressPrefixV1,
    pub group_position: u64,
    pub group_id: Sha256Hex,
    pub closed_at_input_position: u64,
}

/// 公共规则的精确值与其原来源；历史市场事实仍由同帧观测承载。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicTradingRuleContextV1 {
    pub version: String,
    pub snapshot_file_sha256: Sha256Hex,
    pub market: MarketIdentityV2,
    pub query_started_at_utc_ms: i64,
    pub query_completed_at_utc_ms: i64,
    pub frozen_at_utc_ms: i64,
    pub queried_minimum_order_size_shares: ExactDecimal,
    /// 同一快照的真实平台最低份数；模型条件输入与执行规则，不是Actor最低数量掩码。
    pub limit_min_quantity_shares: ExactDecimal,
    pub market_buy_min_spend_pusd: ExactDecimal,
    pub taker_delay_enabled: bool,
    pub taker_delay_ms: u64,
    pub delay_requires_itode: bool,
    pub delay_only_fok_fak_market_buy: bool,
    pub fees_enabled: bool,
    pub fee_rate: ExactDecimal,
    pub fee_exponent: u32,
    pub fee_taker_only: bool,
    pub fee_formula_version: String,
    pub fee_precision_decimal_places: u32,
    pub minimum_nonzero_fee_pusd: ExactDecimal,
    pub gtd_lifetime_seconds: [u64; 3],
    pub gtd_safety_offset_seconds: u64,
    pub maker_rebate_rate: ExactDecimal,
    pub taker_rebate_rate: ExactDecimal,
    pub encoding_contract_reference: Option<String>,
    pub queried_rules_source: PublicRuleSourceV1,
    pub delay_rule_source: PublicRuleSourceV1,
    pub fee_formula_source: PublicRuleSourceV1,
    pub rebate_source: PublicRuleSourceV1,
    pub project_rules_source: PublicRuleSourceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicRuleSourceCategoryV1 {
    QueriedPlatform,
    PublishedPlatformRule,
    ApprovedProjectRule,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicRuleSourceV1 {
    pub category: PublicRuleSourceCategoryV1,
    pub reference: String,
}

fn source_progress_is_empty(v: &SharedSourceProgressV1) -> bool {
    v.is_empty()
}

/// 消费者需求只选择数量正文，不改变来源、质量或可见前沿。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicObservationRequirementsV1 {
    pub binance_depth_quantities: bool,
}
impl PublicObservationRequirementsV1 {
    pub const FULL: Self = Self {
        binance_depth_quantities: true,
    };
}

/// None 仍表示尚未观察；NotMaterialized 表示已知深度但本次消费者不需要数量。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceDepthObservationV1 {
    Full(std::sync::Arc<BinanceDepthStateReferenceV1>),
    NotMaterialized,
}
impl BinanceDepthObservationV1 {
    pub fn full(&self) -> Option<&BinanceDepthStateReferenceV1> {
        match self {
            Self::Full(value) => Some(value),
            Self::NotMaterialized => None,
        }
    }
}

// 默认完整观察沿原 JSON/CBOR 对象编码；省略正文显式编码为描述对象。
mod depth_observation_encoding {
    use super::*;
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum QuantityDisposition {
        NotMaterialized,
    }
    #[derive(Serialize, Deserialize)]
    #[serde(untagged, deny_unknown_fields)]
    enum Representation<T> {
        Full(T),
        Description {
            binance_depth_quantities: QuantityDisposition,
        },
    }
    impl Serialize for BinanceDepthObservationV1 {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self {
                Self::Full(value) => Representation::Full(value),
                Self::NotMaterialized => {
                    Representation::<&std::sync::Arc<BinanceDepthStateReferenceV1>>::Description {
                        binance_depth_quantities: QuantityDisposition::NotMaterialized,
                    }
                }
            }
            .serialize(serializer)
        }
    }
    impl<'de> Deserialize<'de> for BinanceDepthObservationV1 {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            Ok(
                match Representation::<std::sync::Arc<BinanceDepthStateReferenceV1>>::deserialize(
                    deserializer,
                )? {
                    Representation::Full(value) => Self::Full(value),
                    Representation::Description { .. } => Self::NotMaterialized,
                },
            )
        }
    }
}

impl Default for PublicObservationRequirementsV1 {
    fn default() -> Self {
        Self::FULL
    }
}

/// 市场状态按变化共享；本组公共参考与可见前缀仅保存一次。
pub type MarketObservationSnapshotsV1 =
    imbl::OrdMap<String, std::sync::Arc<MarketObservationSnapshotV1>>;
/// 仅包含市场事实；完整正式观察由读取时结合本组事实产生。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketObservationSnapshotV1 {
    pub market: MarketIdentityV2,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketObservationGroupV1 {
    pub schema_version: String,
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub visible_prefix: AppliedIngressPrefixV1,
    pub chainlink_spot: ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
    pub chainlink_twap_thirty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    pub chainlink_twap_sixty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
    pub binance_depth: std::sync::Arc<ObservedFactV1<BinanceDepthObservationV1>>,
    pub binance_trade: ObservedFactV1<BinanceAggregateTradeReferenceV1>,
    pub group_events: std::sync::Arc<[PublicBusinessFactV1]>,
    pub source_progress: SharedSourceProgressV1,
}
#[derive(Clone)]
enum ObservationSource {
    Complete(Vec<MarketBusinessObservationV1>),
    Shared {
        markets: MarketObservationSnapshotsV1,
        group: std::sync::Arc<MarketObservationGroupV1>,
    },
}
/// 保留完整观察读取；必要市场读取不展开其他市场。缓存只归本组。
pub struct MarketObservationSetV1 {
    source: ObservationSource,
    selected: elsa::sync::FrozenBTreeMap<String, Box<MarketBusinessObservationV1>>,
    complete: std::sync::OnceLock<Vec<MarketBusinessObservationV1>>,
}
impl MarketObservationSetV1 {
    pub fn shared(markets: MarketObservationSnapshotsV1, group: MarketObservationGroupV1) -> Self {
        Self {
            source: ObservationSource::Shared {
                markets,
                group: std::sync::Arc::new(group),
            },
            selected: elsa::sync::FrozenBTreeMap::new(),
            complete: std::sync::OnceLock::new(),
        }
    }
    pub fn shared_parts(
        &self,
    ) -> Option<(
        &MarketObservationSnapshotsV1,
        &std::sync::Arc<MarketObservationGroupV1>,
    )> {
        match &self.source {
            ObservationSource::Shared { markets, group } => Some((markets, group)),
            _ => None,
        }
    }
    pub fn get_market(&self, id: &str) -> Option<&MarketBusinessObservationV1> {
        match &self.source {
            ObservationSource::Complete(v) => v.iter().find(|o| o.market.condition_id.0 == id),
            ObservationSource::Shared { markets, group } => {
                if let Some(o) = self.selected.get(id) {
                    return Some(o);
                }
                let market = markets.get(id)?;
                let o = MarketBusinessObservationV1 {
                    schema_version: group.schema_version.clone(),
                    group_id: group.group_id.clone(),
                    group_position: group.group_position,
                    visible_prefix: group.visible_prefix.clone(),
                    market: market.market.clone(),
                    assets: market.assets.clone(),
                    lifecycle: market.lifecycle.clone(),
                    active: market.active.clone(),
                    closed: market.closed.clone(),
                    accepting_orders: market.accepting_orders.clone(),
                    terminal: market.terminal.clone(),
                    official_anchor: market.official_anchor.clone(),
                    settlement_reference: market.settlement_reference.clone(),
                    temporary_reference: market.temporary_reference.clone(),
                    effective_reference: market.effective_reference.clone(),
                    chainlink_spot: group.chainlink_spot.clone(),
                    chainlink_twap_thirty: group.chainlink_twap_thirty.clone(),
                    chainlink_twap_sixty: group.chainlink_twap_sixty.clone(),
                    binance_depth: group.binance_depth.clone(),
                    binance_trade: group.binance_trade.clone(),
                    group_events: group.group_events.clone(),
                    source_progress: group.source_progress.clone(),
                };
                Some(self.selected.insert(id.into(), Box::new(o)))
            }
        }
    }
    pub fn len(&self) -> usize {
        match &self.source {
            ObservationSource::Complete(v) => v.len(),
            ObservationSource::Shared { markets, .. } => markets.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn iter(&self) -> Box<dyn Iterator<Item = &MarketBusinessObservationV1> + '_> {
        match &self.source {
            ObservationSource::Complete(v) => Box::new(v.iter()),
            ObservationSource::Shared { markets, .. } => Box::new(
                markets
                    .keys()
                    .map(|id| self.get_market(id).expect("原市场快照")),
            ),
        }
    }
    fn as_vec(&self) -> &Vec<MarketBusinessObservationV1> {
        match &self.source {
            ObservationSource::Complete(v) => v,
            _ => self.complete.get_or_init(|| self.iter().cloned().collect()),
        }
    }
    pub fn as_slice(&self) -> &[MarketBusinessObservationV1] {
        self.as_vec()
    }
    pub fn replace_group_events(&mut self, records: std::sync::Arc<[PublicBusinessFactV1]>) {
        match &mut self.source {
            ObservationSource::Complete(v) => {
                for o in v {
                    o.group_events = records.clone();
                }
            }
            ObservationSource::Shared { group, .. } => {
                std::sync::Arc::make_mut(group).group_events = records
            }
        }
        self.selected = elsa::sync::FrozenBTreeMap::new();
        self.complete.take();
    }
}
impl From<Vec<MarketBusinessObservationV1>> for MarketObservationSetV1 {
    fn from(v: Vec<MarketBusinessObservationV1>) -> Self {
        Self {
            source: ObservationSource::Complete(v),
            selected: elsa::sync::FrozenBTreeMap::new(),
            complete: std::sync::OnceLock::new(),
        }
    }
}
impl Clone for MarketObservationSetV1 {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            selected: elsa::sync::FrozenBTreeMap::new(),
            complete: std::sync::OnceLock::new(),
        }
    }
}
impl std::ops::Index<usize> for MarketObservationSetV1 {
    type Output = MarketBusinessObservationV1;
    fn index(&self, index: usize) -> &Self::Output {
        self.iter().nth(index).expect("原观察位置越界")
    }
}
impl std::ops::IndexMut<usize> for MarketObservationSetV1 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut std::ops::DerefMut::deref_mut(self)[index]
    }
}
impl std::ops::Deref for MarketObservationSetV1 {
    type Target = Vec<MarketBusinessObservationV1>;
    fn deref(&self) -> &Self::Target {
        self.as_vec()
    }
}
impl std::ops::DerefMut for MarketObservationSetV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if !matches!(self.source, ObservationSource::Complete(_)) {
            self.source = ObservationSource::Complete(self.iter().cloned().collect());
        }
        match &mut self.source {
            ObservationSource::Complete(v) => v,
            _ => unreachable!(),
        }
    }
}
impl<'a> IntoIterator for &'a MarketObservationSetV1 {
    type Item = &'a MarketBusinessObservationV1;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut MarketObservationSetV1 {
    type Item = &'a mut MarketBusinessObservationV1;
    type IntoIter = std::slice::IterMut<'a, MarketBusinessObservationV1>;
    fn into_iter(self) -> Self::IntoIter {
        std::ops::DerefMut::deref_mut(self).iter_mut()
    }
}
impl std::fmt::Debug for MarketObservationSetV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for MarketObservationSetV1 {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for MarketObservationSetV1 {}
impl Serialize for MarketObservationSetV1 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for o in self.iter() {
            seq.serialize_element(o)?;
        }
        seq.end()
    }
}
impl<'de> Deserialize<'de> for MarketObservationSetV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Vec::<MarketBusinessObservationV1>::deserialize(d).map(Into::into)
    }
}

/// 已归约来源进展的共享快照；同组健康查询只读所需 scope。
pub type SourceProgressSnapshotsV1 =
    imbl::OrdMap<SourceStreamScopeV1, std::sync::Arc<SourceStreamProgressV1>>;
#[derive(Clone, Default)]
pub struct SharedSourceProgressV1 {
    values: SourceProgressSnapshotsV1,
    original: Option<std::sync::Arc<[SourceStreamProgressV1]>>,
    complete: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<[SourceStreamProgressV1]>>>,
}
impl SharedSourceProgressV1 {
    pub fn new(values: SourceProgressSnapshotsV1) -> Self {
        Self {
            values,
            original: None,
            complete: Default::default(),
        }
    }
    pub fn shared_values(&self) -> Option<&SourceProgressSnapshotsV1> {
        self.original.is_none().then_some(&self.values)
    }
    pub fn get_scope(&self, scope: &SourceStreamScopeV1) -> Option<&SourceStreamProgressV1> {
        if let Some(v) = &self.original {
            v.iter().find(|p| p.scope == *scope)
        } else {
            self.values.get(scope).map(AsRef::as_ref)
        }
    }
    pub fn iter(&self) -> Box<dyn Iterator<Item = &SourceStreamProgressV1> + '_> {
        if let Some(v) = &self.original {
            Box::new(v.iter())
        } else {
            Box::new(self.values.values().map(AsRef::as_ref))
        }
    }
    pub fn len(&self) -> usize {
        self.original
            .as_ref()
            .map_or(self.values.len(), |v| v.len())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_arc(&self) -> &std::sync::Arc<[SourceStreamProgressV1]> {
        if let Some(v) = &self.original {
            v
        } else {
            self.complete.get_or_init(|| self.iter().cloned().collect())
        }
    }
}
impl From<std::sync::Arc<[SourceStreamProgressV1]>> for SharedSourceProgressV1 {
    fn from(v: std::sync::Arc<[SourceStreamProgressV1]>) -> Self {
        Self {
            values: Default::default(),
            original: Some(v),
            complete: Default::default(),
        }
    }
}
impl From<Vec<SourceStreamProgressV1>> for SharedSourceProgressV1 {
    fn from(v: Vec<SourceStreamProgressV1>) -> Self {
        Self::from(std::sync::Arc::<[SourceStreamProgressV1]>::from(v))
    }
}
impl std::ops::Deref for SharedSourceProgressV1 {
    type Target = [SourceStreamProgressV1];
    fn deref(&self) -> &Self::Target {
        self.as_arc()
    }
}
impl std::fmt::Debug for SharedSourceProgressV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for SharedSourceProgressV1 {
    fn eq(&self, o: &Self) -> bool {
        self.iter().eq(o.iter())
    }
}
impl Eq for SharedSourceProgressV1 {}
impl Serialize for SharedSourceProgressV1 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for p in self.iter() {
            seq.serialize_element(p)?;
        }
        seq.end()
    }
}
impl<'de> Deserialize<'de> for SharedSourceProgressV1 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Vec::<SourceStreamProgressV1>::deserialize(d).map(Into::into)
    }
}
