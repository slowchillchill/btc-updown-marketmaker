use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::event::EventEnvelope;
use crate::source_format::{
    BinanceAggTradeRaw, BinanceDepthLevel, BinanceDepthSnapshotRaw, BinanceDepthUpdateRaw,
    RtdsChainlinkBtcUsdRaw,
};
use crate::sources::binance::DepthUpdateDecision;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceHourlyValidation {
    pub missing_fields: Vec<String>,
}

impl SourceHourlyValidation {
    pub fn is_complete(&self) -> bool {
        self.missing_fields.is_empty()
    }

    fn require_non_empty(&mut self, field: &str, value: &str) {
        if value.trim().is_empty() {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn require_some<T>(&mut self, field: &str, value: Option<T>) {
        if value.is_none() {
            self.missing_fields.push(field.to_owned());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthSnapshotHourlyRow {
    pub timestamp_received: i64,
    pub snapshot_fetch_start_utc: i64,
    pub snapshot_recv_time_utc: i64,
    pub recv_time_utc: i64,
    pub last_update_id: u64,
    pub bids: String,
    pub asks: String,
    pub symbol: String,
    pub source_endpoint: String,
    pub snapshot_status: String,
    pub collector_host_id: String,
    pub collector_region: String,
    pub schema_version: String,
}

impl BinanceDepthSnapshotHourlyRow {
    pub fn from_raw(raw: &BinanceDepthSnapshotRaw) -> Self {
        let timestamp_received = recv_time(&raw.envelope);
        Self {
            timestamp_received,
            snapshot_fetch_start_utc: raw.snapshot_fetch_start_utc_ms,
            snapshot_recv_time_utc: raw.snapshot_recv_time_utc_ms,
            recv_time_utc: timestamp_received,
            last_update_id: raw.last_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            symbol: raw.envelope.source_identifier.clone(),
            source_endpoint: raw.official_rest_endpoint.clone(),
            snapshot_status: "complete".to_owned(),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            schema_version: "binance_depth_snapshot_v1".to_owned(),
        }
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        require_positive_i64(
            "snapshot_fetch_start_utc",
            self.snapshot_fetch_start_utc,
            &mut report,
        );
        require_positive_i64(
            "snapshot_recv_time_utc",
            self.snapshot_recv_time_utc,
            &mut report,
        );
        if self.last_update_id == 0 {
            report.missing_fields.push("last_update_id".to_owned());
        }
        require_depth_json("bids", &self.bids, false, &mut report);
        require_depth_json("asks", &self.asks, false, &mut report);
        report.require_non_empty("symbol", &self.symbol);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("snapshot_status", &self.snapshot_status);
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthUpdateHourlyRow {
    pub timestamp_received: i64,
    pub recv_time_utc: i64,
    pub event_type: String,
    pub event_time: i64,
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: String,
    pub asks: String,
    pub stream: String,
    pub snapshot_last_update_id: u64,
    pub local_u_before: Option<u64>,
    pub local_u_after: Option<u64>,
    pub stitch_verdict: String,
    pub stitch_gap_reason: Option<String>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl BinanceDepthUpdateHourlyRow {
    pub fn from_raw(
        raw: &BinanceDepthUpdateRaw,
        snapshot_last_update_id: u64,
        local_u_before: Option<u64>,
    ) -> Self {
        let timestamp_received = recv_time(&raw.envelope);
        Self {
            timestamp_received,
            recv_time_utc: timestamp_received,
            event_type: raw.event_type.clone(),
            event_time: raw.event_time_ms,
            symbol: raw.symbol.clone(),
            first_update_id: raw.first_update_id,
            final_update_id: raw.final_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            stream: raw.stream_name.clone(),
            snapshot_last_update_id,
            local_u_before,
            local_u_after: raw.local_u,
            stitch_verdict: raw.stitch_evidence.clone().unwrap_or_default(),
            stitch_gap_reason: None,
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: "binance_depth_update_v1".to_owned(),
        }
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        report.require_non_empty("event_type", &self.event_type);
        require_positive_i64("event_time", self.event_time, &mut report);
        report.require_non_empty("symbol", &self.symbol);
        if self.first_update_id == 0 {
            report.missing_fields.push("first_update_id".to_owned());
        }
        if self.final_update_id < self.first_update_id {
            report.missing_fields.push("final_update_id".to_owned());
        }
        require_depth_json("bids", &self.bids, true, &mut report);
        require_depth_json("asks", &self.asks, true, &mut report);
        if depth_is_empty(&self.bids) && depth_is_empty(&self.asks) {
            report.missing_fields.push("bids_or_asks".to_owned());
        }
        report.require_non_empty("stream", &self.stream);
        if self.snapshot_last_update_id == 0 {
            report
                .missing_fields
                .push("snapshot_last_update_id".to_owned());
        }
        report.require_some("local_u_after", self.local_u_after);
        report.require_non_empty("stitch_verdict", &self.stitch_verdict);
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeHourlyRow {
    pub timestamp_received: i64,
    pub recv_time_utc: i64,
    pub event_type: String,
    pub event_time: i64,
    pub trade_time: i64,
    pub symbol: String,
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub buyer_is_maker: bool,
    pub stream: String,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl BinanceAggTradeHourlyRow {
    pub fn from_raw(raw: &BinanceAggTradeRaw) -> Self {
        let timestamp_received = recv_time(&raw.envelope);
        Self {
            timestamp_received,
            recv_time_utc: timestamp_received,
            event_type: raw.event_type.clone(),
            event_time: raw.event_time_ms,
            trade_time: raw.trade_time_ms,
            symbol: raw.symbol.clone(),
            aggregate_trade_id: raw.aggregate_trade_id,
            price: raw.price.clone(),
            quantity: raw.quantity.clone(),
            first_trade_id: raw.first_trade_id,
            last_trade_id: raw.last_trade_id,
            buyer_is_maker: raw.buyer_is_maker,
            stream: raw.stream_name.clone(),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: "binance_aggtrade_v1".to_owned(),
        }
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        report.require_non_empty("event_type", &self.event_type);
        require_positive_i64("event_time", self.event_time, &mut report);
        require_positive_i64("trade_time", self.trade_time, &mut report);
        report.require_non_empty("symbol", &self.symbol);
        if self.aggregate_trade_id == 0 {
            report.missing_fields.push("aggregate_trade_id".to_owned());
        }
        report.require_non_empty("price", &self.price);
        report.require_non_empty("quantity", &self.quantity);
        if self.first_trade_id == 0 {
            report.missing_fields.push("first_trade_id".to_owned());
        }
        if self.last_trade_id < self.first_trade_id {
            report.missing_fields.push("last_trade_id".to_owned());
        }
        report.require_non_empty("stream", &self.stream);
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtdsChainlinkBtcUsdHourlyRow {
    pub timestamp_received: i64,
    pub recv_time_utc: i64,
    pub message_timestamp: i64,
    pub payload_timestamp: i64,
    pub symbol: String,
    pub value: String,
    pub topic: String,
    pub subscription_filter: String,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl RtdsChainlinkBtcUsdHourlyRow {
    pub fn from_raw(raw: &RtdsChainlinkBtcUsdRaw) -> Self {
        let timestamp_received = recv_time(&raw.envelope);
        Self {
            timestamp_received,
            recv_time_utc: timestamp_received,
            message_timestamp: raw.message_timestamp_ms,
            payload_timestamp: raw.payload_timestamp_ms,
            symbol: raw.symbol.clone(),
            value: raw.value.clone(),
            topic: raw.source_topic.clone(),
            subscription_filter: raw.subscription_raw_json.clone(),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: "polymarket_rtds_chainlink_btc_usd_v1".to_owned(),
        }
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        require_positive_i64("message_timestamp", self.message_timestamp, &mut report);
        require_positive_i64("payload_timestamp", self.payload_timestamp, &mut report);
        report.require_non_empty("symbol", &self.symbol);
        report.require_non_empty("value", &self.value);
        report.require_non_empty("topic", &self.topic);
        report.require_non_empty("subscription_filter", &self.subscription_filter);
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthStitchReceipt {
    pub snapshot_last_update_id: u64,
    pub stale_update_rows: u64,
    pub stitch_applied_rows: u64,
    pub gap_rows: u64,
    pub resnapshot_count: u64,
    pub local_u_min: Option<u64>,
    pub local_u_max: Option<u64>,
    pub first_update_bridged_snapshot: bool,
    pub gap_reasons: Vec<String>,
}

impl BinanceDepthStitchReceipt {
    pub fn from_decisions(snapshot_last_update_id: u64, decisions: &[DepthUpdateDecision]) -> Self {
        let mut receipt = Self {
            snapshot_last_update_id,
            stale_update_rows: 0,
            stitch_applied_rows: 0,
            gap_rows: 0,
            resnapshot_count: 0,
            local_u_min: None,
            local_u_max: None,
            first_update_bridged_snapshot: false,
            gap_reasons: Vec::new(),
        };

        for decision in decisions {
            match decision {
                DepthUpdateDecision::DropStaleOrDuplicate { .. } => {
                    receipt.stale_update_rows = receipt.stale_update_rows.saturating_add(1);
                }
                DepthUpdateDecision::Apply {
                    local_u,
                    stitch_evidence,
                } => {
                    receipt.stitch_applied_rows = receipt.stitch_applied_rows.saturating_add(1);
                    receipt.local_u_min = Some(
                        receipt
                            .local_u_min
                            .map_or(*local_u, |current| current.min(*local_u)),
                    );
                    receipt.local_u_max = Some(
                        receipt
                            .local_u_max
                            .map_or(*local_u, |current| current.max(*local_u)),
                    );
                    if stitch_evidence == "U <= lastUpdateId + 1 <= u" {
                        receipt.first_update_bridged_snapshot = true;
                    }
                }
                DepthUpdateDecision::SourceGapResnapshot { reason } => {
                    receipt.gap_rows = receipt.gap_rows.saturating_add(1);
                    receipt.resnapshot_count = receipt.resnapshot_count.saturating_add(1);
                    receipt.gap_reasons.push(reason.clone());
                }
            }
        }

        receipt
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        if self.snapshot_last_update_id == 0 {
            report
                .missing_fields
                .push("snapshot_last_update_id".to_owned());
        }
        if self.stitch_applied_rows == 0 {
            report.missing_fields.push("stitch_applied_rows".to_owned());
        }
        if !self.first_update_bridged_snapshot {
            report
                .missing_fields
                .push("first_update_bridged_snapshot".to_owned());
        }
        report.require_some("local_u_min", self.local_u_min);
        report.require_some("local_u_max", self.local_u_max);
        if self.gap_rows > 0 || self.resnapshot_count > 0 || !self.gap_reasons.is_empty() {
            report.missing_fields.push("source_gap_absent".to_owned());
        }
        report
    }
}

pub fn reject_opaque_raw_json_only(source_kind: &str, _raw_json: &str) -> Result<(), String> {
    Err(format!(
        "{source_kind} hourly layout requires source-specific required typed fields"
    ))
}

pub fn binance_depth_snapshot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("snapshot_fetch_start_utc", false),
        timestamp_field("snapshot_recv_time_utc", false),
        timestamp_field("recv_time_utc", false),
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("snapshot_status", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn binance_depth_update_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("recv_time_utc", false),
        Field::new("event_type", DataType::Utf8, false),
        timestamp_field("event_time", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("snapshot_last_update_id", DataType::UInt64, false),
        Field::new("local_u_before", DataType::UInt64, true),
        Field::new("local_u_after", DataType::UInt64, true),
        Field::new("stitch_verdict", DataType::Utf8, false),
        Field::new("stitch_gap_reason", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn binance_aggtrade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("recv_time_utc", false),
        Field::new("event_type", DataType::Utf8, false),
        timestamp_field("event_time", false),
        timestamp_field("trade_time", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("aggregate_trade_id", DataType::UInt64, false),
        Field::new("price", DataType::Utf8, false),
        Field::new("quantity", DataType::Utf8, false),
        Field::new("first_trade_id", DataType::UInt64, false),
        Field::new("last_trade_id", DataType::UInt64, false),
        Field::new("buyer_is_maker", DataType::Boolean, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn rtds_chainlink_btc_usd_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("recv_time_utc", false),
        timestamp_field("message_timestamp", false),
        timestamp_field("payload_timestamp", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("subscription_filter", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}

fn recv_time(envelope: &EventEnvelope) -> i64 {
    envelope.recv_time_utc_ms.unwrap_or_default()
}

fn depth_levels_to_json(levels: &[BinanceDepthLevel]) -> String {
    let values = levels
        .iter()
        .map(|level| [level.price.clone(), level.quantity.clone()])
        .collect::<Vec<_>>();
    serde_json::to_string(&values).expect("depth levels serialize")
}

fn require_depth_json(
    field: &str,
    value: &str,
    allow_empty: bool,
    report: &mut SourceHourlyValidation,
) {
    if !depth_json_well_formed(value, allow_empty) {
        report.missing_fields.push(field.to_owned());
    }
}

fn depth_json_well_formed(value: &str, allow_empty: bool) -> bool {
    let Ok(values) = serde_json::from_str::<Vec<[String; 2]>>(value) else {
        return false;
    };
    (allow_empty || !values.is_empty())
        && values
            .iter()
            .all(|[price, quantity]| !price.trim().is_empty() && !quantity.trim().is_empty())
}

fn depth_is_empty(value: &str) -> bool {
    serde_json::from_str::<Vec<[String; 2]>>(value)
        .map(|values| values.is_empty())
        .unwrap_or(true)
}

fn require_positive_i64(field: &str, value: i64, report: &mut SourceHourlyValidation) {
    if value <= 0 {
        report.missing_fields.push(field.to_owned());
    }
}
