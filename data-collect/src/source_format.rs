use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ValidationVerdict;
use crate::event::{EventEnvelope, SourceKind};

const MIN_PLAUSIBLE_UNIX_TIMESTAMP_MS: i64 = 1_000_000_000_000;
const MAX_PLAUSIBLE_UNIX_TIMESTAMP_MS: i64 = 9_999_999_999_999;

pub(crate) fn plausible_unix_timestamp_ms(timestamp: i64) -> bool {
    (MIN_PLAUSIBLE_UNIX_TIMESTAMP_MS..=MAX_PLAUSIBLE_UNIX_TIMESTAMP_MS).contains(&timestamp)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatValidationReport {
    pub verdict: ValidationVerdict,
    pub missing_fields: Vec<String>,
    pub source_gaps: Vec<String>,
}

impl FormatValidationReport {
    fn new() -> Self {
        Self {
            verdict: ValidationVerdict::Complete,
            missing_fields: Vec::new(),
            source_gaps: Vec::new(),
        }
    }

    fn require_non_empty(&mut self, field: &str, value: &str) {
        if value.trim().is_empty() {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn require_some<T>(&mut self, field: &str, value: &Option<T>) {
        if value.is_none() {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn require_non_empty_vec<T>(&mut self, field: &str, value: &[T]) {
        if value.is_empty() {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn merge_envelope(&mut self, envelope: &EventEnvelope) {
        if let Err(error) = envelope.validate() {
            self.missing_fields.extend(error.missing_fields);
        }
    }

    fn finish(mut self) -> Self {
        if !self.missing_fields.is_empty() || !self.source_gaps.is_empty() {
            self.verdict = ValidationVerdict::Incomplete;
        }
        self
    }
}

fn raw_json_is_parseable(raw_json: &str) -> bool {
    serde_json::from_str::<Value>(raw_json).is_ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionConfirmation {
    Ack { message: String },
    FirstBusinessEvent { event_type: String },
    Unavailable { reason: String },
}

impl SubscriptionConfirmation {
    fn has_evidence(&self) -> bool {
        match self {
            Self::Ack { message } => !message.trim().is_empty(),
            Self::FirstBusinessEvent { event_type } => !event_type.trim().is_empty(),
            Self::Unavailable { .. } => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketSubscriptionEvidence {
    pub raw_json: String,
    pub assets_ids: Vec<String>,
    pub custom_feature_enabled: bool,
    pub confirmation: SubscriptionConfirmation,
    pub sdk_support_status: String,
}

impl PolymarketSubscriptionEvidence {
    fn validate_into(&self, report: &mut FormatValidationReport) {
        if !raw_json_is_parseable(&self.raw_json) {
            report
                .missing_fields
                .push("subscription_raw_json".to_owned());
        } else if !polymarket_market_subscription_matches(
            &self.raw_json,
            &self.assets_ids,
            self.custom_feature_enabled,
        ) {
            report
                .source_gaps
                .push("subscription_payload_mismatch".to_owned());
        }
        report.require_non_empty_vec("subscription_assets_ids", &self.assets_ids);
        if !self.custom_feature_enabled {
            report
                .source_gaps
                .push("custom_feature_enabled_missing".to_owned());
        }
        if !self.confirmation.has_evidence() {
            report.source_gaps.push("subscription_unproven".to_owned());
        }
        report.require_non_empty("sdk_support_status", &self.sdk_support_status);
    }
}

fn polymarket_market_subscription_matches(
    raw_json: &str,
    assets_ids: &[String],
    custom_feature_enabled: bool,
) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(raw_json) else {
        return false;
    };
    let has_market_type = value.get("type").and_then(Value::as_str) == Some("market");
    let has_custom_feature = value.get("custom_feature_enabled").and_then(Value::as_bool)
        == Some(true)
        && custom_feature_enabled;
    let Some(raw_assets) = value.get("assets_ids").and_then(Value::as_array) else {
        return false;
    };
    let raw_asset_ids = raw_assets
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    has_market_type
        && has_custom_feature
        && !assets_ids.is_empty()
        && assets_ids
            .iter()
            .all(|asset_id| raw_asset_ids.contains(&asset_id.as_str()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketRaw {
    pub envelope: EventEnvelope,
    pub event_type: String,
    pub platform_timestamp: String,
    pub market_id: Option<String>,
    pub condition_id: Option<String>,
    pub asset_ids: Vec<String>,
    pub subscription: PolymarketSubscriptionEvidence,
}

impl PolymarketMarketRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::PolymarketMarket {
            report.missing_fields.push("source_kind".to_owned());
        }
        report.require_non_empty("event_type", &self.event_type);
        report.require_non_empty("platform_timestamp", &self.platform_timestamp);
        if self.market_id.is_none() && self.condition_id.is_none() {
            report
                .missing_fields
                .push("market_or_condition_id".to_owned());
        }
        report.require_non_empty_vec("asset_ids", &self.asset_ids);
        self.subscription.validate_into(&mut report);
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketResolutionRaw {
    pub envelope: EventEnvelope,
    pub condition_id: String,
    pub market_id: Option<String>,
    pub subscribed_asset_ids_for_market: Vec<String>,
    pub resolved_event_asset_ids: Vec<String>,
    pub winning_asset_id: String,
    pub winning_outcome: String,
    pub close_reason_if_present: Option<String>,
}

impl PolymarketMarketResolutionRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::PolymarketMarket {
            report.missing_fields.push("source_kind".to_owned());
        }
        if self.envelope.source_event_type != "market_resolved" {
            report
                .missing_fields
                .push("source_event_type=market_resolved".to_owned());
        }
        report.require_non_empty("condition_id", &self.condition_id);
        report.require_non_empty_vec(
            "subscribed_asset_ids_for_market",
            &self.subscribed_asset_ids_for_market,
        );
        report.require_non_empty_vec("resolved_event_asset_ids", &self.resolved_event_asset_ids);
        report.require_non_empty("winning_asset_id", &self.winning_asset_id);
        report.require_non_empty("winning_outcome", &self.winning_outcome);
        let resolved_covers_subscribed = self
            .subscribed_asset_ids_for_market
            .iter()
            .all(|asset_id| self.resolved_event_asset_ids.contains(asset_id));
        if !resolved_covers_subscribed {
            report
                .source_gaps
                .push("resolved_assets_do_not_cover_subscription".to_owned());
        }
        if !self
            .resolved_event_asset_ids
            .contains(&self.winning_asset_id)
        {
            report
                .source_gaps
                .push("winning_asset_not_in_resolved_assets".to_owned());
        }
        if !self
            .subscribed_asset_ids_for_market
            .contains(&self.winning_asset_id)
        {
            report
                .source_gaps
                .push("winning_asset_not_in_subscribed_assets".to_owned());
        }
        report.finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketLifecycleState {
    Candidate,
    Prefetched,
    Active,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataRaw {
    pub envelope: EventEnvelope,
    pub condition_id: String,
    pub asset_ids: Vec<String>,
    pub clob_market_id: String,
    pub slug: String,
    pub start_time_utc_ms: i64,
    pub end_time_utc_ms: i64,
    pub active: bool,
    pub accepting_orders: bool,
    pub market_lifecycle_state: MarketLifecycleState,
    pub tick_size: String,
    pub min_size: String,
    pub fee_raw: Option<String>,
    pub reward_raw: Option<String>,
    pub candidate_slug: String,
    pub active_scan_evidence: Option<String>,
    pub metadata_snapshot_evidence: Option<String>,
    pub clock_drift_evidence: Option<String>,
}

impl PolymarketMarketMetadataRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::PolymarketMarketMetadata {
            report.missing_fields.push("source_kind".to_owned());
        }
        report.require_non_empty("condition_id", &self.condition_id);
        report.require_non_empty_vec("asset_ids", &self.asset_ids);
        report.require_non_empty("clob_market_id", &self.clob_market_id);
        report.require_non_empty("slug", &self.slug);
        report.require_non_empty("tick_size", &self.tick_size);
        report.require_non_empty("min_size", &self.min_size);
        report.require_some("fee_raw", &self.fee_raw);
        report.require_some("reward_raw", &self.reward_raw);
        report.require_non_empty("candidate_slug", &self.candidate_slug);
        report.require_some("clock_drift_evidence", &self.clock_drift_evidence);
        if self.active_scan_evidence.is_none() && self.metadata_snapshot_evidence.is_none() {
            report.source_gaps.push("slug_only_discovery".to_owned());
        }
        if self.start_time_utc_ms >= self.end_time_utc_ms {
            report.missing_fields.push("start_end_window".to_owned());
        }
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthLevel {
    pub price: String,
    pub quantity: String,
}

fn depth_levels_well_formed(levels: &[BinanceDepthLevel]) -> bool {
    levels
        .iter()
        .all(|level| !level.price.trim().is_empty() && !level.quantity.trim().is_empty())
}

fn depth_levels_non_empty_complete(levels: &[BinanceDepthLevel]) -> bool {
    !levels.is_empty() && depth_levels_well_formed(levels)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthOrdering {
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub depth_ingress_sequence: u64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

impl BinanceDepthOrdering {
    pub fn validate_contract(&self) -> Result<(), String> {
        if self.source_sequence_epoch_id.trim().is_empty() {
            return Err("binance_depth_source_sequence_epoch_id_empty".to_owned());
        }
        if self.connection_generation == 0 {
            return Err("binance_depth_connection_generation_is_zero".to_owned());
        }
        if self.depth_ingress_sequence == 0 {
            return Err("binance_depth_ingress_sequence_is_zero".to_owned());
        }
        if self.collector_admission_sequence == 0 {
            return Err("binance_depth_collector_admission_sequence_is_zero".to_owned());
        }
        if self.collector_admission_time_utc_ms <= 0 {
            return Err("binance_depth_collector_admission_time_not_positive".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthUpdateRaw {
    pub envelope: EventEnvelope,
    pub event_type: String,
    pub event_time_ms: i64,
    pub symbol: String,
    pub stream_name: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<BinanceDepthLevel>,
    pub asks: Vec<BinanceDepthLevel>,
    pub combined_stream: Option<String>,
}

impl BinanceDepthUpdateRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::BinanceDepthUpdate {
            report.missing_fields.push("source_kind".to_owned());
        }
        report.require_non_empty("e", &self.event_type);
        report.require_non_empty("s", &self.symbol);
        report.require_non_empty("stream_name", &self.stream_name);
        if self.event_type != "depthUpdate" {
            report.missing_fields.push("e_valid".to_owned());
        }
        if self.symbol != "BTCUSDT" {
            report.missing_fields.push("s_valid".to_owned());
        }
        if self.stream_name != "btcusdt@depth" {
            report.missing_fields.push("stream_name_valid".to_owned());
        }
        if self.first_update_id == 0 {
            report.missing_fields.push("U".to_owned());
        }
        if self.final_update_id < self.first_update_id {
            report.missing_fields.push("u".to_owned());
        }
        if !depth_levels_well_formed(&self.bids) {
            report.missing_fields.push("b".to_owned());
        }
        if !depth_levels_well_formed(&self.asks) {
            report.missing_fields.push("a".to_owned());
        }
        if self.bids.is_empty() && self.asks.is_empty() {
            report.missing_fields.push("b_or_a".to_owned());
        }
        if self
            .combined_stream
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            report.missing_fields.push("combined_stream".to_owned());
        }
        if self.event_time_ms <= 0 {
            report.missing_fields.push("E".to_owned());
        }
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthSnapshotRaw {
    pub envelope: EventEnvelope,
    pub last_update_id: u64,
    pub bids: Vec<BinanceDepthLevel>,
    pub asks: Vec<BinanceDepthLevel>,
    pub snapshot_fetch_start_utc_ms: i64,
    pub snapshot_recv_time_utc_ms: i64,
    pub official_rest_endpoint: String,
    pub no_exchange_timestamp_evidence: String,
}

impl BinanceDepthSnapshotRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::BinanceDepthSnapshot {
            report.missing_fields.push("source_kind".to_owned());
        }
        if self.last_update_id == 0 {
            report.missing_fields.push("lastUpdateId".to_owned());
        }
        if !depth_levels_non_empty_complete(&self.bids) {
            report.missing_fields.push("bids".to_owned());
        }
        if !depth_levels_non_empty_complete(&self.asks) {
            report.missing_fields.push("asks".to_owned());
        }
        if self.snapshot_fetch_start_utc_ms <= 0 {
            report
                .missing_fields
                .push("snapshot_fetch_start_utc".to_owned());
        }
        if self.snapshot_recv_time_utc_ms <= 0 {
            report
                .missing_fields
                .push("snapshot_recv_time_utc".to_owned());
        }
        report.require_non_empty("official_rest_endpoint", &self.official_rest_endpoint);
        report.require_non_empty(
            "no_exchange_timestamp_evidence",
            &self.no_exchange_timestamp_evidence,
        );
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeRaw {
    pub envelope: EventEnvelope,
    pub event_type: String,
    pub event_time_ms: i64,
    pub symbol: String,
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub trade_time_ms: i64,
    pub buyer_is_maker: bool,
    pub ws_ignore: bool,
    pub stream_name: String,
}

impl BinanceAggTradeRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::BinanceAggTrade {
            report.missing_fields.push("source_kind".to_owned());
        }
        report.require_non_empty("e", &self.event_type);
        report.require_non_empty("s", &self.symbol);
        report.require_non_empty("p", &self.price);
        report.require_non_empty("q", &self.quantity);
        report.require_non_empty("stream_name", &self.stream_name);
        if self.event_time_ms <= 0 {
            report.missing_fields.push("E".to_owned());
        }
        if self.aggregate_trade_id == 0 {
            report.missing_fields.push("a".to_owned());
        }
        if self.first_trade_id == 0 {
            report.missing_fields.push("f".to_owned());
        }
        if self.last_trade_id < self.first_trade_id {
            report.missing_fields.push("l".to_owned());
        }
        if self.trade_time_ms <= 0 {
            report.missing_fields.push("T".to_owned());
        }
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtdsChainlinkBtcUsdRaw {
    pub envelope: EventEnvelope,
    pub message_timestamp_ms: i64,
    pub payload_timestamp_ms: i64,
    pub symbol: String,
    pub value: String,
    pub source_topic: String,
    pub message_type: String,
    pub age_ms: Option<i64>,
    pub freshness_status: String,
    pub subscription_raw_json: String,
    pub ack_or_first_message_evidence: Option<String>,
    pub close_or_error_evidence: Option<String>,
}

impl RtdsChainlinkBtcUsdRaw {
    pub fn validate_contract(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.merge_envelope(&self.envelope);
        if self.envelope.source_kind != SourceKind::PolymarketRtdsChainlinkBtcUsd {
            report.missing_fields.push("source_kind".to_owned());
        }
        if self.message_timestamp_ms <= 0 && self.freshness_status != "invalid_timestamp" {
            report.missing_fields.push("message_timestamp".to_owned());
        }
        if self.payload_timestamp_ms <= 0 && self.freshness_status != "invalid_timestamp" {
            report.missing_fields.push("payload_timestamp".to_owned());
        }
        report.require_non_empty("symbol", &self.symbol);
        report.require_non_empty("value", &self.value);
        report.require_non_empty("source_topic", &self.source_topic);
        report.require_non_empty("message_type", &self.message_type);
        if !matches!(
            self.freshness_status.as_str(),
            "fresh"
                | "stale"
                | "invalid_type"
                | "invalid_topic"
                | "invalid_symbol"
                | "invalid_timestamp"
        ) {
            report.missing_fields.push("freshness_status".to_owned());
        }
        if self.freshness_status != "invalid_timestamp" && self.age_ms.is_none() {
            report.missing_fields.push("age_ms".to_owned());
        }
        if !raw_json_is_parseable(&self.subscription_raw_json) {
            report
                .missing_fields
                .push("subscription_raw_json".to_owned());
        } else if !rtds_subscription_matches(&self.subscription_raw_json, &self.symbol) {
            report
                .source_gaps
                .push("rtds_subscription_payload_mismatch".to_owned());
        }
        if self.ack_or_first_message_evidence.is_none() && self.close_or_error_evidence.is_none() {
            report
                .source_gaps
                .push("rtds_subscription_unproven".to_owned());
        }
        report.finish()
    }
}

fn rtds_subscription_matches(raw_json: &str, symbol: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(raw_json) else {
        return false;
    };
    if value.get("action").and_then(Value::as_str) != Some("subscribe") {
        return false;
    }
    let subscription = value
        .get("subscriptions")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .unwrap_or(&value);
    if subscription.get("topic").and_then(Value::as_str) != Some("crypto_prices_chainlink")
        || subscription.get("type").and_then(Value::as_str) != Some("*")
    {
        return false;
    }
    let Some(filters) = subscription.get("filters").and_then(Value::as_str) else {
        return false;
    };
    let Ok(filters) = serde_json::from_str::<Value>(filters) else {
        return false;
    };
    filters.get("symbol").and_then(Value::as_str) == Some(symbol)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerKind {
    DataPipeline,
    /// 历史兼容标识，表示下游 `model-train` 消费方。
    Training,
    PaperLive,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerBoundaryContract {
    pub consumer: ConsumerKind,
    pub consumes_typed_raw_parquet: bool,
    pub requires_data_pipeline_outputs: bool,
    pub data_collect_generates_labels: bool,
    pub data_collect_generates_features: bool,
    pub data_collect_generates_tick_stream: bool,
    pub data_collect_generates_training_split: bool,
    pub data_collect_output_is_training_approved: bool,
    pub may_replace_append_only_log: bool,
    pub requires_realtime_handoff: bool,
    pub requires_runtime_receive_timestamp: bool,
    pub may_generate_noe_or_ledger: bool,
    pub may_emit_trading_decision: bool,
}

impl ConsumerBoundaryContract {
    pub fn data_pipeline() -> Self {
        Self {
            consumer: ConsumerKind::DataPipeline,
            consumes_typed_raw_parquet: true,
            requires_data_pipeline_outputs: false,
            data_collect_generates_labels: false,
            data_collect_generates_features: false,
            data_collect_generates_tick_stream: false,
            data_collect_generates_training_split: false,
            data_collect_output_is_training_approved: false,
            may_replace_append_only_log: false,
            requires_realtime_handoff: false,
            requires_runtime_receive_timestamp: false,
            may_generate_noe_or_ledger: false,
            may_emit_trading_decision: false,
        }
    }

    pub fn training() -> Self {
        Self {
            consumer: ConsumerKind::Training,
            consumes_typed_raw_parquet: false,
            requires_data_pipeline_outputs: true,
            data_collect_generates_labels: false,
            data_collect_generates_features: false,
            data_collect_generates_tick_stream: false,
            data_collect_generates_training_split: false,
            data_collect_output_is_training_approved: false,
            may_replace_append_only_log: false,
            requires_realtime_handoff: false,
            requires_runtime_receive_timestamp: false,
            may_generate_noe_or_ledger: false,
            may_emit_trading_decision: false,
        }
    }

    pub fn paper_live() -> Self {
        Self {
            consumer: ConsumerKind::PaperLive,
            consumes_typed_raw_parquet: true,
            requires_data_pipeline_outputs: false,
            data_collect_generates_labels: false,
            data_collect_generates_features: false,
            data_collect_generates_tick_stream: false,
            data_collect_generates_training_split: false,
            data_collect_output_is_training_approved: false,
            may_replace_append_only_log: false,
            requires_realtime_handoff: true,
            requires_runtime_receive_timestamp: true,
            may_generate_noe_or_ledger: false,
            may_emit_trading_decision: false,
        }
    }

    pub fn validate(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        match self.consumer {
            ConsumerKind::DataPipeline | ConsumerKind::PaperLive
                if !self.consumes_typed_raw_parquet =>
            {
                report
                    .missing_fields
                    .push("typed_raw_parquet_consumer_boundary".to_owned());
            }
            ConsumerKind::Training if self.consumes_typed_raw_parquet => {
                report
                    .source_gaps
                    .push("model_train_may_not_consume_typed_raw_parquet".to_owned());
            }
            _ => {}
        }
        if self.data_collect_generates_labels
            || self.data_collect_generates_features
            || self.data_collect_generates_tick_stream
            || self.data_collect_generates_training_split
            || self.data_collect_output_is_training_approved
            || self.may_replace_append_only_log
            || self.may_generate_noe_or_ledger
            || self.may_emit_trading_decision
        {
            report
                .source_gaps
                .push("consumer_boundary_violation".to_owned());
        }
        if self.consumer == ConsumerKind::Training && !self.requires_data_pipeline_outputs {
            report
                .source_gaps
                .push("training_without_data_pipeline_outputs".to_owned());
        }
        if self.consumer == ConsumerKind::PaperLive && !self.requires_runtime_receive_timestamp {
            report
                .source_gaps
                .push("paper_live_runtime_receive_timestamp_missing".to_owned());
        }
        if self.consumer == ConsumerKind::PaperLive && !self.requires_realtime_handoff {
            report
                .source_gaps
                .push("paper_live_realtime_handoff_missing".to_owned());
        }
        report.finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimestampNamespace {
    pub source_ts_name: String,
    pub collector_recv_time_name: String,
    pub runtime_receive_time_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedConsumerSchema {
    pub source_kind: String,
    pub schema_version: String,
    pub manifest_required: bool,
    pub coverage_window_start_utc_ms: Option<i64>,
    pub coverage_window_end_utc_ms: Option<i64>,
    pub timestamp_namespace: TimestampNamespace,
    pub nullable_fields_have_reason: bool,
}

impl SharedConsumerSchema {
    pub fn validate(&self) -> FormatValidationReport {
        let mut report = FormatValidationReport::new();
        report.require_non_empty("source_kind", &self.source_kind);
        report.require_non_empty("schema_version", &self.schema_version);
        if !self.manifest_required {
            report.missing_fields.push("manifest".to_owned());
        }
        if self.coverage_window_start_utc_ms.is_none() || self.coverage_window_end_utc_ms.is_none()
        {
            report.missing_fields.push("coverage_window".to_owned());
        }
        if self.timestamp_namespace.source_ts_name.trim().is_empty() {
            report.missing_fields.push("source_ts_namespace".to_owned());
        }
        if self.timestamp_namespace.collector_recv_time_name != "recv_time_utc" {
            report
                .source_gaps
                .push("collector_recv_time_namespace_invalid".to_owned());
        }
        if !self.nullable_fields_have_reason {
            report
                .source_gaps
                .push("nullable_reason_missing".to_owned());
        }
        report.finish()
    }
}
