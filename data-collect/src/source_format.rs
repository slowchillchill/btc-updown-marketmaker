use serde_json::Value;

use crate::event::{EventEnvelope, SourceKind};

const MIN_PLAUSIBLE_UNIX_TIMESTAMP_MS: i64 = 1_000_000_000_000;
const MAX_PLAUSIBLE_UNIX_TIMESTAMP_MS: i64 = 9_999_999_999_999;

pub(crate) fn plausible_unix_timestamp_ms(timestamp: i64) -> bool {
    (MIN_PLAUSIBLE_UNIX_TIMESTAMP_MS..=MAX_PLAUSIBLE_UNIX_TIMESTAMP_MS).contains(&timestamp)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatValidationReport {
    pub missing_fields: Vec<String>,
    pub source_gaps: Vec<String>,
}

impl FormatValidationReport {
    fn new() -> Self {
        Self {
            missing_fields: Vec::new(),
            source_gaps: Vec::new(),
        }
    }

    fn require_non_empty(&mut self, field: &str, value: &str) {
        if value.trim().is_empty() {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn merge_envelope(&mut self, envelope: &EventEnvelope) {
        if let Err(error) = envelope.validate() {
            self.missing_fields.extend(error.missing_fields);
        }
    }

    pub fn is_complete(&self) -> bool {
        self.missing_fields.is_empty() && self.source_gaps.is_empty()
    }

    fn finish(self) -> Self {
        self
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
        match serde_json::from_str::<Value>(&self.subscription_raw_json) {
            Err(_) => report
                .missing_fields
                .push("subscription_raw_json".to_owned()),
            Ok(value) if !rtds_subscription_matches(&value, &self.symbol) => report
                .source_gaps
                .push("rtds_subscription_payload_mismatch".to_owned()),
            Ok(_) => {}
        }
        if self.ack_or_first_message_evidence.is_none() && self.close_or_error_evidence.is_none() {
            report
                .source_gaps
                .push("rtds_subscription_unproven".to_owned());
        }
        report.finish()
    }
}

fn rtds_subscription_matches(value: &Value, symbol: &str) -> bool {
    if value.get("action").and_then(Value::as_str) != Some("subscribe") {
        return false;
    }
    let subscription = value
        .get("subscriptions")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .unwrap_or(value);
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
