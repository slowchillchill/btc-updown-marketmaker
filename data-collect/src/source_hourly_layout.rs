use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use crate::binance_aggtrade::{
    AggTradeIngestSource, BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES,
    BinanceAggTradeIngressCoordinate, BinanceAggTradeOutput,
};
use crate::event::{EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp};
use crate::source_format::{
    BinanceAggTradeRaw, BinanceDepthLevel, BinanceDepthOrdering, BinanceDepthSnapshotRaw,
    BinanceDepthUpdateRaw, RtdsChainlinkBtcUsdRaw, plausible_unix_timestamp_ms,
};
use crate::sources::binance::{
    BINANCE_DEPTH_STREAM, DepthUpdateAssessment, DepthUpdateVerdict, assess_depth_update,
};

pub const BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION: &str = "binance_depth_snapshot_v3";
pub const BINANCE_DEPTH_UPDATE_SCHEMA_VERSION: &str = "binance_depth_update_v4";
pub const BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION: &str = "binance_depth_snapshot_v2";
pub const BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION: &str = "binance_depth_update_v3";
pub const BINANCE_DEPTH_VERDICT_APPLIED: &str = "applied";
pub const BINANCE_DEPTH_VERDICT_NOT_APPLIED_STALE_DUPLICATE: &str = "not_applied_stale_duplicate";
pub const BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE: &str = "not_applied_confirmed_hole";
pub const BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE: &str =
    "not_applied_snapshot_unavailable";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceDepthOrderingClassification {
    Exact,
    LegacyOrderingUnknown,
}

impl BinanceDepthOrderingClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::LegacyOrderingUnknown => "legacy_ordering_unknown",
        }
    }
}

fn classify_depth_ordering(
    schema_version: &str,
    current_schema_version: &str,
    legacy_schema_version: &str,
    ordering_presence: [bool; 5],
) -> Result<BinanceDepthOrderingClassification, String> {
    let all_present = ordering_presence.iter().all(|present| *present);
    let all_absent = ordering_presence.iter().all(|present| !*present);
    if schema_version == current_schema_version {
        return if all_present {
            Ok(BinanceDepthOrderingClassification::Exact)
        } else {
            Err("partial_or_missing_depth_ordering_coordinates".to_owned())
        };
    }
    if schema_version == legacy_schema_version {
        return if all_absent {
            Ok(BinanceDepthOrderingClassification::LegacyOrderingUnknown)
        } else {
            Err("legacy_depth_schema_must_not_claim_ordering_coordinates".to_owned())
        };
    }
    Err(format!("unsupported_depth_schema_version:{schema_version}"))
}

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
    pub last_update_id: u64,
    pub bids: String,
    pub asks: String,
    pub symbol: String,
    pub source_endpoint: String,
    pub snapshot_status: String,
    pub source_sequence_epoch_id: Option<String>,
    pub connection_generation: Option<u64>,
    pub depth_ingress_sequence: Option<u64>,
    pub collector_admission_sequence: Option<u64>,
    pub collector_admission_time_utc_ms: Option<i64>,
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
            last_update_id: raw.last_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            symbol: raw.envelope.source_identifier.clone(),
            source_endpoint: raw.official_rest_endpoint.clone(),
            snapshot_status: "complete".to_owned(),
            source_sequence_epoch_id: None,
            connection_generation: None,
            depth_ingress_sequence: None,
            collector_admission_sequence: None,
            collector_admission_time_utc_ms: None,
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION.to_owned(),
        }
    }

    pub fn from_raw_with_ordering(
        raw: &BinanceDepthSnapshotRaw,
        ordering: BinanceDepthOrdering,
    ) -> Result<Self, String> {
        if raw.validate_contract().verdict != crate::ValidationVerdict::Complete {
            return Err("invalid_binance_depth_snapshot_contract".to_owned());
        }
        ordering.validate_contract()?;
        let timestamp_received = recv_time(&raw.envelope);
        let row = Self {
            timestamp_received,
            snapshot_fetch_start_utc: raw.snapshot_fetch_start_utc_ms,
            last_update_id: raw.last_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            symbol: raw.envelope.source_identifier.clone(),
            source_endpoint: raw.official_rest_endpoint.clone(),
            snapshot_status: "complete".to_owned(),
            source_sequence_epoch_id: Some(ordering.source_sequence_epoch_id),
            connection_generation: Some(ordering.connection_generation),
            depth_ingress_sequence: Some(ordering.depth_ingress_sequence),
            collector_admission_sequence: Some(ordering.collector_admission_sequence),
            collector_admission_time_utc_ms: Some(ordering.collector_admission_time_utc_ms),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        };
        let validation = row.validate();
        if validation.is_complete() {
            Ok(row)
        } else {
            Err(format!(
                "binance_depth_snapshot_v3_validation_failed:{}",
                validation.missing_fields.join(",")
            ))
        }
    }

    pub fn ordering_classification(&self) -> Result<BinanceDepthOrderingClassification, String> {
        classify_depth_ordering(
            &self.schema_version,
            BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
            BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
            [
                self.source_sequence_epoch_id.is_some(),
                self.connection_generation.is_some(),
                self.depth_ingress_sequence.is_some(),
                self.collector_admission_sequence.is_some(),
                self.collector_admission_time_utc_ms.is_some(),
            ],
        )
    }

    fn exact_ordering(&self) -> Option<BinanceDepthOrdering> {
        Some(BinanceDepthOrdering {
            source_sequence_epoch_id: self.source_sequence_epoch_id.clone()?,
            connection_generation: self.connection_generation?,
            depth_ingress_sequence: self.depth_ingress_sequence?,
            collector_admission_sequence: self.collector_admission_sequence?,
            collector_admission_time_utc_ms: self.collector_admission_time_utc_ms?,
        })
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
        if self.last_update_id == 0 {
            report.missing_fields.push("last_update_id".to_owned());
        }
        require_depth_json("bids", &self.bids, false, &mut report);
        require_depth_json("asks", &self.asks, false, &mut report);
        report.require_non_empty("symbol", &self.symbol);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("snapshot_status", &self.snapshot_status);
        if self.symbol != "BTCUSDT" {
            report.missing_fields.push("symbol_valid".to_owned());
        }
        if self.snapshot_status != "complete" {
            report
                .missing_fields
                .push("snapshot_status_valid".to_owned());
        }
        match self.ordering_classification() {
            Ok(BinanceDepthOrderingClassification::Exact) => {
                if self
                    .exact_ordering()
                    .is_none_or(|ordering| ordering.validate_contract().is_err())
                {
                    report
                        .missing_fields
                        .push("depth_ordering_coordinates_valid".to_owned());
                }
            }
            Ok(BinanceDepthOrderingClassification::LegacyOrderingUnknown) => {}
            Err(_) => report
                .missing_fields
                .push("depth_ordering_classification".to_owned()),
        }
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceDepthUpdateHourlyRow {
    pub timestamp_received: i64,
    pub event_type: String,
    pub event_time: i64,
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: String,
    pub asks: String,
    pub stream: String,
    pub snapshot_last_update_id: Option<u64>,
    pub local_u_before: Option<u64>,
    pub stitch_verdict: String,
    pub stitch_evidence: Option<String>,
    pub stitch_gap_reason: Option<String>,
    pub source_sequence_epoch_id: Option<String>,
    pub connection_generation: Option<u64>,
    pub depth_ingress_sequence: Option<u64>,
    pub collector_admission_sequence: Option<u64>,
    pub collector_admission_time_utc_ms: Option<i64>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl BinanceDepthUpdateHourlyRow {
    pub fn from_legacy_raw(
        raw: &BinanceDepthUpdateRaw,
        snapshot_last_update_id: u64,
        local_u_before: Option<u64>,
        stitch_verdict: impl Into<String>,
    ) -> Self {
        let timestamp_received = recv_time(&raw.envelope);
        Self {
            timestamp_received,
            event_type: raw.event_type.clone(),
            event_time: raw.event_time_ms,
            symbol: raw.symbol.clone(),
            first_update_id: raw.first_update_id,
            final_update_id: raw.final_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            stream: raw.stream_name.clone(),
            snapshot_last_update_id: Some(snapshot_last_update_id),
            local_u_before,
            stitch_verdict: stitch_verdict.into(),
            stitch_evidence: None,
            stitch_gap_reason: None,
            source_sequence_epoch_id: None,
            connection_generation: None,
            depth_ingress_sequence: None,
            collector_admission_sequence: None,
            collector_admission_time_utc_ms: None,
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION.to_owned(),
        }
    }

    pub fn from_assessment(
        raw: &BinanceDepthUpdateRaw,
        assessment: &DepthUpdateAssessment,
        ordering: BinanceDepthOrdering,
    ) -> Result<Self, String> {
        if raw.validate_contract().verdict != crate::ValidationVerdict::Complete {
            return Err("invalid_binance_depth_update_contract".to_owned());
        }
        validate_canonical_depth_assessment(raw, assessment)?;
        ordering.validate_contract()?;
        let stitch_verdict = match assessment.verdict {
            DepthUpdateVerdict::Applied => BINANCE_DEPTH_VERDICT_APPLIED,
            DepthUpdateVerdict::NotAppliedStaleDuplicate => {
                BINANCE_DEPTH_VERDICT_NOT_APPLIED_STALE_DUPLICATE
            }
            DepthUpdateVerdict::NotAppliedConfirmedHole => {
                BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE
            }
            DepthUpdateVerdict::NotAppliedSnapshotUnavailable => {
                BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
            }
        };
        let row = Self {
            timestamp_received: recv_time(&raw.envelope),
            event_type: raw.event_type.clone(),
            event_time: raw.event_time_ms,
            symbol: raw.symbol.clone(),
            first_update_id: raw.first_update_id,
            final_update_id: raw.final_update_id,
            bids: depth_levels_to_json(&raw.bids),
            asks: depth_levels_to_json(&raw.asks),
            stream: raw.stream_name.clone(),
            snapshot_last_update_id: assessment.snapshot_last_update_id,
            local_u_before: assessment.local_u_before,
            stitch_verdict: stitch_verdict.to_owned(),
            stitch_evidence: assessment.stitch_evidence.clone(),
            stitch_gap_reason: assessment.gap_reason.clone(),
            source_sequence_epoch_id: Some(ordering.source_sequence_epoch_id),
            connection_generation: Some(ordering.connection_generation),
            depth_ingress_sequence: Some(ordering.depth_ingress_sequence),
            collector_admission_sequence: Some(ordering.collector_admission_sequence),
            collector_admission_time_utc_ms: Some(ordering.collector_admission_time_utc_ms),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.to_owned(),
        };
        let validation = row.validate();
        if validation.is_complete() {
            Ok(row)
        } else {
            Err(format!(
                "binance_depth_update_v4_validation_failed:{}",
                validation.missing_fields.join(",")
            ))
        }
    }

    pub fn ordering_classification(&self) -> Result<BinanceDepthOrderingClassification, String> {
        classify_depth_ordering(
            &self.schema_version,
            BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
            [
                self.source_sequence_epoch_id.is_some(),
                self.connection_generation.is_some(),
                self.depth_ingress_sequence.is_some(),
                self.collector_admission_sequence.is_some(),
                self.collector_admission_time_utc_ms.is_some(),
            ],
        )
    }

    fn exact_ordering(&self) -> Option<BinanceDepthOrdering> {
        Some(BinanceDepthOrdering {
            source_sequence_epoch_id: self.source_sequence_epoch_id.clone()?,
            connection_generation: self.connection_generation?,
            depth_ingress_sequence: self.depth_ingress_sequence?,
            collector_admission_sequence: self.collector_admission_sequence?,
            collector_admission_time_utc_ms: self.collector_admission_time_utc_ms?,
        })
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
        if self.event_type != "depthUpdate" {
            report.missing_fields.push("event_type_valid".to_owned());
        }
        if self.symbol != "BTCUSDT" {
            report.missing_fields.push("symbol_valid".to_owned());
        }
        if self.stream != BINANCE_DEPTH_STREAM {
            report.missing_fields.push("stream_valid".to_owned());
        }
        match self.ordering_classification() {
            Ok(BinanceDepthOrderingClassification::Exact) => {
                if self
                    .exact_ordering()
                    .is_none_or(|ordering| ordering.validate_contract().is_err())
                {
                    report
                        .missing_fields
                        .push("depth_ordering_coordinates_valid".to_owned());
                }
                self.validate_current_verdict(&mut report);
            }
            Ok(BinanceDepthOrderingClassification::LegacyOrderingUnknown) => {
                if self.snapshot_last_update_id.is_none_or(|value| value == 0) {
                    report
                        .missing_fields
                        .push("snapshot_last_update_id".to_owned());
                }
                report.require_non_empty("stitch_verdict", &self.stitch_verdict);
            }
            Err(_) => report
                .missing_fields
                .push("depth_ordering_classification".to_owned()),
        }
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        report.require_non_empty("schema_version", &self.schema_version);
        report
    }

    fn validate_current_verdict(&self, report: &mut SourceHourlyValidation) {
        let verdict = match self.stitch_verdict.as_str() {
            BINANCE_DEPTH_VERDICT_APPLIED => DepthUpdateVerdict::Applied,
            BINANCE_DEPTH_VERDICT_NOT_APPLIED_STALE_DUPLICATE => {
                DepthUpdateVerdict::NotAppliedStaleDuplicate
            }
            BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE => {
                DepthUpdateVerdict::NotAppliedConfirmedHole
            }
            BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE => {
                DepthUpdateVerdict::NotAppliedSnapshotUnavailable
            }
            _ => {
                report
                    .missing_fields
                    .push("stitch_verdict_valid".to_owned());
                return;
            }
        };
        let snapshot_is_present = self.snapshot_last_update_id.is_some_and(|value| value > 0);
        if self.stitch_verdict == BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE {
            if self.snapshot_last_update_id.is_some() {
                report
                    .missing_fields
                    .push("snapshot_unavailable_snapshot_last_update_id_null".to_owned());
            }
            if self.local_u_before.is_some() {
                report
                    .missing_fields
                    .push("snapshot_unavailable_local_u_before_null".to_owned());
            }
            if self.stitch_gap_reason.is_some() {
                report
                    .missing_fields
                    .push("snapshot_unavailable_gap_absent".to_owned());
            }
        } else {
            if !snapshot_is_present {
                report
                    .missing_fields
                    .push("snapshot_last_update_id".to_owned());
            }
            if self.stitch_verdict == BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE {
                if self
                    .stitch_gap_reason
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    report
                        .missing_fields
                        .push("confirmed_hole_gap_reason".to_owned());
                }
            } else if self.stitch_gap_reason.is_some() {
                report
                    .missing_fields
                    .push("non_gap_reason_absent".to_owned());
            }
            if self
                .stitch_evidence
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                report.missing_fields.push("stitch_evidence".to_owned());
            }
        }

        let claimed = DepthUpdateAssessment {
            snapshot_last_update_id: self.snapshot_last_update_id,
            local_u_before: self.local_u_before,
            verdict,
            local_u_after: (verdict == DepthUpdateVerdict::Applied).then_some(self.final_update_id),
            stitch_evidence: self.stitch_evidence.clone(),
            gap_reason: self.stitch_gap_reason.clone(),
        };
        if self
            .as_depth_update_raw()
            .and_then(|raw| validate_canonical_depth_assessment(&raw, &claimed))
            .is_err()
        {
            report
                .missing_fields
                .push("stitch_assessment_canonical".to_owned());
        }
    }

    fn as_depth_update_raw(&self) -> Result<BinanceDepthUpdateRaw, String> {
        let parse_levels = |value: &str| {
            serde_json::from_str::<Vec<[String; 2]>>(value)
                .map(|levels| {
                    levels
                        .into_iter()
                        .map(|[price, quantity]| BinanceDepthLevel { price, quantity })
                        .collect::<Vec<_>>()
                })
                .map_err(|error| format!("invalid_depth_levels_json:{error}"))
        };
        Ok(BinanceDepthUpdateRaw {
            envelope: EventEnvelope {
                source_kind: SourceKind::BinanceDepthUpdate,
                source_event_type: self.event_type.clone(),
                source_identifier: self.symbol.clone(),
                source_timestamp: SourceTimestamp::Timestamp {
                    value: self.event_time.to_string(),
                },
                recv_time_utc_ms: Some(self.timestamp_received),
                collector_host_id: self.collector_host_id.clone(),
                collector_region: self.collector_region.clone(),
                source_endpoint: self.source_endpoint.clone(),
                host_clock_sync_status: HostClockSyncStatus::Unavailable {
                    reason: "typed_hourly_row_validation".to_owned(),
                },
                raw_evidence: RawEvidence::Json("{}".to_owned()),
                parquet_raw_layout_version: 1,
                parquet_raw_source_kind: SourceKind::BinanceDepthUpdate,
            },
            event_type: self.event_type.clone(),
            event_time_ms: self.event_time,
            symbol: self.symbol.clone(),
            stream_name: self.stream.clone(),
            first_update_id: self.first_update_id,
            final_update_id: self.final_update_id,
            bids: parse_levels(&self.bids)?,
            asks: parse_levels(&self.asks)?,
            combined_stream: None,
        })
    }
}

fn validate_canonical_depth_assessment(
    raw: &BinanceDepthUpdateRaw,
    assessment: &DepthUpdateAssessment,
) -> Result<(), String> {
    assessment.validate_contract()?;
    let canonical = assess_depth_update(
        assessment.snapshot_last_update_id,
        assessment.local_u_before,
        raw,
    )?;
    if canonical != *assessment {
        return Err("depth_update_assessment_not_canonical".to_owned());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeOrdering {
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub websocket_ingress_sequence: u64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

impl BinanceAggTradeOrdering {
    fn validate(&self) -> Result<(), String> {
        if self.source_sequence_epoch_id.trim().is_empty() {
            return Err("binance_aggtrade_source_sequence_epoch_id_empty".to_owned());
        }
        if self.connection_generation == 0 {
            return Err("binance_aggtrade_connection_generation_is_zero".to_owned());
        }
        if self.websocket_ingress_sequence == 0 {
            return Err("binance_aggtrade_websocket_ingress_sequence_is_zero".to_owned());
        }
        if self.collector_admission_sequence == 0 {
            return Err("binance_aggtrade_collector_admission_sequence_is_zero".to_owned());
        }
        if self.collector_admission_time_utc_ms <= 0 {
            return Err("binance_aggtrade_collector_admission_time_not_positive".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeInvalidEvidence {
    pub timestamp_received: i64,
    pub ingest_source: String,
    pub source_native_payload_json: String,
    pub source_endpoint: String,
    pub invalid_reason_code: String,
    pub invalid_reason_detail: Option<String>,
    pub payload_encoding: Option<String>,
    pub payload_length_bytes: Option<u64>,
    pub payload_sha256: Option<String>,
    pub payload_capture_status: String,
}

pub const BINANCE_AGGTRADE_SCHEMA_VERSION: &str = "binance_aggtrade_v6";
pub const BINANCE_AGGTRADE_RECORD_TRADE: &str = "trade";
pub const BINANCE_AGGTRADE_RECORD_INVALID_INPUT: &str = "invalid_input";
pub const BINANCE_AGGTRADE_CAPTURE_COMPLETE: &str = "complete";
pub const BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED: &str = "oversize_not_captured";
pub const BINANCE_AGGTRADE_INVALID_REASON_CODE_MAX_BYTES: usize = 128;
pub const BINANCE_AGGTRADE_INVALID_REASON_DETAIL_MAX_BYTES: usize = 2_048;

#[derive(serde::Deserialize)]
struct BinanceAggTradeInvalidEvidenceEnvelope {
    evidence_schema: String,
    transport: String,
    endpoint: String,
    receive_time_utc_ms: i64,
    invalid_reason_code: String,
    payload_encoding: Option<String>,
    payload_length_bytes: Option<u64>,
    payload_sha256: Option<String>,
    payload_capture_status: String,
    payload_base64: Option<String>,
    observed_bytes_at_least: u64,
    declared_content_length: Option<u64>,
    capture_limit_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeHourlyRow {
    pub record_kind: String,
    pub timestamp_received: i64,
    pub websocket_timestamp_received: Option<i64>,
    pub rest_backfill_timestamp_received: Option<i64>,
    pub ingest_source: String,
    pub event_type: Option<String>,
    pub event_time: Option<i64>,
    pub trade_time: Option<i64>,
    pub symbol: Option<String>,
    pub aggregate_trade_id: Option<u64>,
    pub price: Option<String>,
    pub quantity: Option<String>,
    pub first_trade_id: Option<u64>,
    pub last_trade_id: Option<u64>,
    pub buyer_is_maker: Option<bool>,
    pub stream: Option<String>,
    pub ws_ignore: Option<bool>,
    pub rest_was_best_price_match: Option<bool>,
    pub source_native_payload_json: String,
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub websocket_ingress_sequence: Option<u64>,
    pub rest_response_ingress_sequence: Option<u64>,
    pub rest_response_item_index: Option<u64>,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub invalid_reason_code: Option<String>,
    pub invalid_reason_detail: Option<String>,
    pub payload_encoding: Option<String>,
    pub payload_length_bytes: Option<u64>,
    pub payload_sha256: Option<String>,
    pub payload_capture_status: Option<String>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl BinanceAggTradeHourlyRow {
    pub fn from_raw(
        raw: &BinanceAggTradeRaw,
        ordering: BinanceAggTradeOrdering,
    ) -> Result<Self, String> {
        ordering.validate()?;
        let timestamp_received = recv_time(&raw.envelope);
        Ok(Self {
            record_kind: BINANCE_AGGTRADE_RECORD_TRADE.to_owned(),
            timestamp_received,
            websocket_timestamp_received: Some(timestamp_received),
            rest_backfill_timestamp_received: None,
            ingest_source: AggTradeIngestSource::Websocket.as_str().to_owned(),
            event_type: Some(raw.event_type.clone()),
            event_time: Some(raw.event_time_ms),
            trade_time: Some(raw.trade_time_ms),
            symbol: Some(raw.symbol.clone()),
            aggregate_trade_id: Some(raw.aggregate_trade_id),
            price: Some(raw.price.clone()),
            quantity: Some(raw.quantity.clone()),
            first_trade_id: Some(raw.first_trade_id),
            last_trade_id: Some(raw.last_trade_id),
            buyer_is_maker: Some(raw.buyer_is_maker),
            stream: Some(raw.stream_name.clone()),
            ws_ignore: Some(raw.ws_ignore),
            rest_was_best_price_match: None,
            source_native_payload_json: serde_json::json!({
                "websocket": serde_json::json!({
                    "source": "binance_aggtrade_websocket",
                    "stream": raw.stream_name,
                    "payload": {
                        "e": raw.event_type,
                        "E": raw.event_time_ms,
                        "s": raw.symbol,
                        "a": raw.aggregate_trade_id,
                        "p": raw.price,
                        "q": raw.quantity,
                        "f": raw.first_trade_id,
                        "l": raw.last_trade_id,
                        "T": raw.trade_time_ms,
                        "m": raw.buyer_is_maker,
                        "M": raw.ws_ignore,
                    }
                }).to_string(),
                "rest_backfill": null,
            })
            .to_string(),
            source_sequence_epoch_id: ordering.source_sequence_epoch_id,
            connection_generation: ordering.connection_generation,
            websocket_ingress_sequence: Some(ordering.websocket_ingress_sequence),
            rest_response_ingress_sequence: None,
            rest_response_item_index: None,
            collector_admission_sequence: ordering.collector_admission_sequence,
            collector_admission_time_utc_ms: ordering.collector_admission_time_utc_ms,
            invalid_reason_code: None,
            invalid_reason_detail: None,
            payload_encoding: None,
            payload_length_bytes: None,
            payload_sha256: None,
            payload_capture_status: None,
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
        })
    }

    pub fn from_output(
        output: &BinanceAggTradeOutput,
        collector_host_id: &str,
        collector_region: &str,
        websocket_endpoint: &str,
        ordering: BinanceAggTradeOrdering,
    ) -> Result<Self, String> {
        ordering.validate()?;
        let BinanceAggTradeOutput::Websocket(websocket) = output;
        let BinanceAggTradeIngressCoordinate::Websocket { sequence } =
            &websocket.ingress_coordinate;
        if ordering.websocket_ingress_sequence != *sequence {
            return Err("binance_aggtrade_ordering_does_not_match_source_evidence".to_owned());
        }
        let shared = &websocket.shared;
        Ok(Self {
            record_kind: BINANCE_AGGTRADE_RECORD_TRADE.to_owned(),
            timestamp_received: websocket.timestamp_received,
            websocket_timestamp_received: Some(websocket.timestamp_received),
            rest_backfill_timestamp_received: None,
            ingest_source: AggTradeIngestSource::Websocket.as_str().to_owned(),
            event_type: Some(websocket.event_type.clone()),
            event_time: Some(websocket.event_time_ms),
            trade_time: Some(shared.trade_time_ms),
            symbol: Some(websocket.symbol.clone()),
            aggregate_trade_id: Some(shared.aggregate_trade_id),
            price: Some(shared.price.clone()),
            quantity: Some(shared.quantity.clone()),
            first_trade_id: Some(shared.first_trade_id),
            last_trade_id: Some(shared.last_trade_id),
            buyer_is_maker: Some(shared.buyer_is_maker),
            stream: Some(websocket.stream.clone()),
            ws_ignore: Some(websocket.ws_ignore),
            rest_was_best_price_match: None,
            source_native_payload_json: output.source_native_payload_json()?,
            source_sequence_epoch_id: ordering.source_sequence_epoch_id,
            connection_generation: ordering.connection_generation,
            websocket_ingress_sequence: Some(ordering.websocket_ingress_sequence),
            rest_response_ingress_sequence: None,
            rest_response_item_index: None,
            collector_admission_sequence: ordering.collector_admission_sequence,
            collector_admission_time_utc_ms: ordering.collector_admission_time_utc_ms,
            invalid_reason_code: None,
            invalid_reason_detail: None,
            payload_encoding: None,
            payload_length_bytes: None,
            payload_sha256: None,
            payload_capture_status: None,
            collector_host_id: collector_host_id.to_owned(),
            collector_region: collector_region.to_owned(),
            source_endpoint: websocket_endpoint.to_owned(),
            schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
        })
    }

    pub fn from_invalid_evidence(
        evidence: BinanceAggTradeInvalidEvidence,
        ordering: BinanceAggTradeOrdering,
        collector_host_id: &str,
        collector_region: &str,
    ) -> Result<Self, String> {
        ordering.validate()?;
        if evidence.ingest_source != AggTradeIngestSource::Websocket.as_str() {
            return Err("binance_aggtrade_invalid_evidence_must_be_websocket".to_owned());
        }
        Ok(Self {
            record_kind: BINANCE_AGGTRADE_RECORD_INVALID_INPUT.to_owned(),
            timestamp_received: evidence.timestamp_received,
            websocket_timestamp_received: Some(evidence.timestamp_received),
            rest_backfill_timestamp_received: None,
            ingest_source: AggTradeIngestSource::Websocket.as_str().to_owned(),
            event_type: None,
            event_time: None,
            trade_time: None,
            symbol: None,
            aggregate_trade_id: None,
            price: None,
            quantity: None,
            first_trade_id: None,
            last_trade_id: None,
            buyer_is_maker: None,
            stream: None,
            ws_ignore: None,
            rest_was_best_price_match: None,
            source_native_payload_json: evidence.source_native_payload_json,
            source_sequence_epoch_id: ordering.source_sequence_epoch_id,
            connection_generation: ordering.connection_generation,
            websocket_ingress_sequence: Some(ordering.websocket_ingress_sequence),
            rest_response_ingress_sequence: None,
            rest_response_item_index: None,
            collector_admission_sequence: ordering.collector_admission_sequence,
            collector_admission_time_utc_ms: ordering.collector_admission_time_utc_ms,
            invalid_reason_code: Some(evidence.invalid_reason_code),
            invalid_reason_detail: evidence.invalid_reason_detail,
            payload_encoding: evidence.payload_encoding,
            payload_length_bytes: evidence.payload_length_bytes,
            payload_sha256: evidence.payload_sha256,
            payload_capture_status: Some(evidence.payload_capture_status),
            collector_host_id: collector_host_id.to_owned(),
            collector_region: collector_region.to_owned(),
            source_endpoint: evidence.source_endpoint,
            schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
        })
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        if let Some(timestamp) = self.websocket_timestamp_received {
            require_positive_i64("websocket_timestamp_received", timestamp, &mut report);
        }
        if let Some(timestamp) = self.rest_backfill_timestamp_received {
            require_positive_i64("rest_backfill_timestamp_received", timestamp, &mut report);
        }
        if let Some(timestamp) = self.event_time {
            require_positive_i64("event_time", timestamp, &mut report);
        }
        require_positive_i64(
            "collector_admission_time_utc_ms",
            self.collector_admission_time_utc_ms,
            &mut report,
        );
        report.require_non_empty("record_kind", &self.record_kind);
        report.require_non_empty("ingest_source", &self.ingest_source);
        report.require_non_empty("source_sequence_epoch_id", &self.source_sequence_epoch_id);
        if self.connection_generation == 0 {
            report
                .missing_fields
                .push("connection_generation".to_owned());
        }
        if self.collector_admission_sequence == 0 {
            report
                .missing_fields
                .push("collector_admission_sequence".to_owned());
        }
        if self.websocket_ingress_sequence == Some(0) {
            report
                .missing_fields
                .push("websocket_ingress_sequence".to_owned());
        }
        let websocket = self.ingest_source == AggTradeIngestSource::Websocket.as_str();
        if !websocket {
            report.missing_fields.push("ingest_source_valid".to_owned());
        }
        report.require_some(
            "websocket_timestamp_received",
            self.websocket_timestamp_received,
        );
        report.require_some(
            "websocket_ingress_sequence",
            self.websocket_ingress_sequence,
        );
        if self.rest_backfill_timestamp_received.is_some()
            || self.rest_response_ingress_sequence.is_some()
            || self.rest_response_item_index.is_some()
            || self.rest_was_best_price_match.is_some()
        {
            report
                .missing_fields
                .push("websocket_has_rest_only_fields".to_owned());
        }
        let expected_availability = self.websocket_timestamp_received.unwrap_or_default();
        if self.timestamp_received != expected_availability {
            report
                .missing_fields
                .push("timestamp_received_is_full_row_availability".to_owned());
        }
        report.require_non_empty(
            "source_native_payload_json",
            &self.source_native_payload_json,
        );
        if serde_json::from_str::<serde_json::Value>(&self.source_native_payload_json).is_err() {
            report
                .missing_fields
                .push("source_native_payload_json_parseable".to_owned());
        }
        match self.record_kind.as_str() {
            BINANCE_AGGTRADE_RECORD_TRADE => self.validate_trade(&mut report),
            BINANCE_AGGTRADE_RECORD_INVALID_INPUT => self.validate_invalid_input(&mut report),
            _ => report.missing_fields.push("record_kind_valid".to_owned()),
        }
        report.require_non_empty("collector_host_id", &self.collector_host_id);
        report.require_non_empty("collector_region", &self.collector_region);
        report.require_non_empty("source_endpoint", &self.source_endpoint);
        if self.schema_version != BINANCE_AGGTRADE_SCHEMA_VERSION {
            report
                .missing_fields
                .push("schema_version_valid".to_owned());
        }
        report
    }

    fn validate_trade(&self, report: &mut SourceHourlyValidation) {
        report.require_some("trade_time", self.trade_time);
        if let Some(trade_time) = self.trade_time {
            require_positive_i64("trade_time", trade_time, report);
        }
        report.require_some("symbol", self.symbol.as_ref());
        if let Some(symbol) = self.symbol.as_deref() {
            report.require_non_empty("symbol", symbol);
        }
        report.require_some("aggregate_trade_id", self.aggregate_trade_id);
        if self.aggregate_trade_id == Some(0) {
            report.missing_fields.push("aggregate_trade_id".to_owned());
        }
        report.require_some("price", self.price.as_ref());
        if let Some(price) = self.price.as_deref() {
            report.require_non_empty("price", price);
        }
        report.require_some("quantity", self.quantity.as_ref());
        if let Some(quantity) = self.quantity.as_deref() {
            report.require_non_empty("quantity", quantity);
        }
        report.require_some("first_trade_id", self.first_trade_id);
        if self.first_trade_id == Some(0) {
            report.missing_fields.push("first_trade_id".to_owned());
        }
        report.require_some("last_trade_id", self.last_trade_id);
        if matches!((self.first_trade_id, self.last_trade_id), (Some(first), Some(last)) if last < first)
        {
            report.missing_fields.push("last_trade_id".to_owned());
        }
        report.require_some("buyer_is_maker", self.buyer_is_maker);
        report.require_some("event_type", self.event_type.as_ref());
        report.require_some("event_time", self.event_time);
        report.require_some("stream", self.stream.as_ref());
        report.require_some("ws_ignore", self.ws_ignore);
        if self.invalid_reason_code.is_some()
            || self.invalid_reason_detail.is_some()
            || self.payload_encoding.is_some()
            || self.payload_length_bytes.is_some()
            || self.payload_sha256.is_some()
            || self.payload_capture_status.is_some()
        {
            report
                .missing_fields
                .push("trade_has_invalid_only_fields".to_owned());
        }
    }

    fn validate_invalid_input(&self, report: &mut SourceHourlyValidation) {
        if self.event_type.is_some()
            || self.event_time.is_some()
            || self.trade_time.is_some()
            || self.symbol.is_some()
            || self.aggregate_trade_id.is_some()
            || self.price.is_some()
            || self.quantity.is_some()
            || self.first_trade_id.is_some()
            || self.last_trade_id.is_some()
            || self.buyer_is_maker.is_some()
            || self.stream.is_some()
            || self.ws_ignore.is_some()
            || self.rest_was_best_price_match.is_some()
            || self.rest_response_item_index.is_some()
        {
            report
                .missing_fields
                .push("invalid_input_has_trade_only_fields".to_owned());
        }
        match self.invalid_reason_code.as_deref() {
            Some(code) if valid_invalid_reason_code(code) => {}
            _ => report.missing_fields.push("invalid_reason_code".to_owned()),
        }
        if self.invalid_reason_detail.as_deref().is_some_and(|detail| {
            detail.is_empty() || detail.len() > BINANCE_AGGTRADE_INVALID_REASON_DETAIL_MAX_BYTES
        }) {
            report
                .missing_fields
                .push("invalid_reason_detail".to_owned());
        }
        match self.payload_capture_status.as_deref() {
            Some(BINANCE_AGGTRADE_CAPTURE_COMPLETE) => {
                report.require_some("payload_encoding", self.payload_encoding.as_ref());
                if self
                    .payload_encoding
                    .as_deref()
                    .is_some_and(|encoding| encoding.trim().is_empty())
                {
                    report.missing_fields.push("payload_encoding".to_owned());
                }
                report.require_some("payload_length_bytes", self.payload_length_bytes);
                if !self.payload_sha256.as_deref().is_some_and(valid_sha256_hex) {
                    report.missing_fields.push("payload_sha256".to_owned());
                }
            }
            Some(BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED) => {
                if self.payload_encoding.is_some() || self.payload_sha256.is_some() {
                    report
                        .missing_fields
                        .push("oversize_has_uncaptured_payload_fact".to_owned());
                }
            }
            _ => report
                .missing_fields
                .push("payload_capture_status".to_owned()),
        }
        if let Err(field) = validate_invalid_evidence_envelope(self) {
            report.missing_fields.push(field.to_owned());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtdsChainlinkBtcUsdHourlyRow {
    pub timestamp_received: i64,
    pub message_timestamp: i64,
    pub payload_timestamp: i64,
    pub age_ms: Option<i64>,
    pub symbol: String,
    pub value: String,
    pub topic: String,
    pub message_type: String,
    pub freshness_status: String,
    pub subscription_filter: String,
    pub source_native_payload_json: String,
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
            message_timestamp: raw.message_timestamp_ms,
            payload_timestamp: raw.payload_timestamp_ms,
            age_ms: raw.age_ms,
            symbol: raw.symbol.clone(),
            value: raw.value.clone(),
            topic: raw.source_topic.clone(),
            message_type: raw.message_type.clone(),
            freshness_status: raw.freshness_status.clone(),
            subscription_filter: raw.subscription_raw_json.clone(),
            source_native_payload_json: serde_json::json!({
                "source": "polymarket_rtds_chainlink_btc_usd",
                "topic": raw.source_topic,
                "type": raw.message_type,
                "timestamp": raw.message_timestamp_ms,
                "payload": {
                    "symbol": raw.symbol,
                    "timestamp": raw.payload_timestamp_ms,
                    "value": raw.value,
                },
            })
            .to_string(),
            collector_host_id: raw.envelope.collector_host_id.clone(),
            collector_region: raw.envelope.collector_region.clone(),
            source_endpoint: raw.envelope.source_endpoint.clone(),
            schema_version: "polymarket_rtds_chainlink_btc_usd_v4".to_owned(),
        }
    }

    pub fn validate(&self) -> SourceHourlyValidation {
        let mut report = SourceHourlyValidation {
            missing_fields: Vec::new(),
        };
        require_positive_i64("timestamp_received", self.timestamp_received, &mut report);
        if self.freshness_status != "invalid_timestamp" {
            require_positive_i64("message_timestamp", self.message_timestamp, &mut report);
            require_positive_i64("payload_timestamp", self.payload_timestamp, &mut report);
            report.require_some("age_ms", self.age_ms);
        }
        report.require_non_empty("symbol", &self.symbol);
        report.require_non_empty("value", &self.value);
        report.require_non_empty("topic", &self.topic);
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
        let computed_age_ms = self.timestamp_received.checked_sub(self.payload_timestamp);
        let timestamps_valid = plausible_unix_timestamp_ms(self.message_timestamp)
            && plausible_unix_timestamp_ms(self.payload_timestamp)
            && computed_age_ms.is_some();
        let fresh_age = computed_age_ms.is_some_and(|age| (-5_000..=30_000).contains(&age));
        let status_consistent = match self.freshness_status.as_str() {
            "invalid_timestamp" => !timestamps_valid,
            "invalid_topic" => timestamps_valid && self.topic != "crypto_prices_chainlink",
            "invalid_type" => {
                timestamps_valid
                    && self.topic == "crypto_prices_chainlink"
                    && self.message_type != "update"
            }
            "invalid_symbol" => {
                timestamps_valid
                    && self.topic == "crypto_prices_chainlink"
                    && self.message_type == "update"
                    && self.symbol != "btc/usd"
            }
            "fresh" => {
                timestamps_valid
                    && self.topic == "crypto_prices_chainlink"
                    && self.message_type == "update"
                    && self.symbol == "btc/usd"
                    && fresh_age
            }
            "stale" => {
                timestamps_valid
                    && self.topic == "crypto_prices_chainlink"
                    && self.message_type == "update"
                    && self.symbol == "btc/usd"
                    && !fresh_age
            }
            _ => false,
        };
        if self.age_ms != computed_age_ms {
            report.missing_fields.push("age_ms_consistent".to_owned());
        }
        if !status_consistent {
            report
                .missing_fields
                .push("freshness_status_consistent".to_owned());
        }
        report.require_non_empty("subscription_filter", &self.subscription_filter);
        report.require_non_empty(
            "source_native_payload_json",
            &self.source_native_payload_json,
        );
        if serde_json::from_str::<serde_json::Value>(&self.source_native_payload_json).is_err() {
            report
                .missing_fields
                .push("source_native_payload_json_parseable".to_owned());
        }
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
    pub fn from_assessments(
        snapshot_last_update_id: u64,
        assessments: &[DepthUpdateAssessment],
    ) -> Result<Self, String> {
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

        for assessment in assessments {
            assessment.validate_contract()?;
            match assessment.verdict {
                DepthUpdateVerdict::NotAppliedStaleDuplicate => {
                    receipt.stale_update_rows = receipt
                        .stale_update_rows
                        .checked_add(1)
                        .ok_or_else(|| "depth_stale_update_rows_overflow".to_owned())?;
                }
                DepthUpdateVerdict::Applied => {
                    let local_u = assessment
                        .local_u_after
                        .ok_or_else(|| "applied_depth_update_missing_local_u_after".to_owned())?;
                    receipt.stitch_applied_rows = receipt
                        .stitch_applied_rows
                        .checked_add(1)
                        .ok_or_else(|| "depth_stitch_applied_rows_overflow".to_owned())?;
                    receipt.local_u_min = Some(
                        receipt
                            .local_u_min
                            .map_or(local_u, |current| current.min(local_u)),
                    );
                    receipt.local_u_max = Some(
                        receipt
                            .local_u_max
                            .map_or(local_u, |current| current.max(local_u)),
                    );
                    if assessment.stitch_evidence.as_deref() == Some("U <= lastUpdateId + 1 <= u") {
                        receipt.first_update_bridged_snapshot = true;
                    }
                }
                DepthUpdateVerdict::NotAppliedConfirmedHole => {
                    receipt.gap_rows = receipt
                        .gap_rows
                        .checked_add(1)
                        .ok_or_else(|| "depth_gap_rows_overflow".to_owned())?;
                    receipt.resnapshot_count = receipt
                        .resnapshot_count
                        .checked_add(1)
                        .ok_or_else(|| "depth_resnapshot_count_overflow".to_owned())?;
                    receipt.gap_reasons.push(
                        assessment
                            .gap_reason
                            .clone()
                            .ok_or_else(|| "confirmed_hole_missing_gap_reason".to_owned())?,
                    );
                }
                DepthUpdateVerdict::NotAppliedSnapshotUnavailable => {}
            }
        }

        Ok(receipt)
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
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("snapshot_status", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("depth_ingress_sequence", DataType::UInt64, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn binance_depth_update_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        Field::new("event_type", DataType::Utf8, false),
        timestamp_field("event_time", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("snapshot_last_update_id", DataType::UInt64, true),
        Field::new("local_u_before", DataType::UInt64, true),
        Field::new("stitch_verdict", DataType::Utf8, false),
        Field::new("stitch_evidence", DataType::Utf8, true),
        Field::new("stitch_gap_reason", DataType::Utf8, true),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("depth_ingress_sequence", DataType::UInt64, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn binance_aggtrade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("record_kind", DataType::Utf8, false),
        timestamp_field("timestamp_received", false),
        timestamp_field("websocket_timestamp_received", true),
        timestamp_field("rest_backfill_timestamp_received", true),
        Field::new("ingest_source", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        timestamp_field("event_time", true),
        timestamp_field("trade_time", true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("aggregate_trade_id", DataType::UInt64, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("quantity", DataType::Utf8, true),
        Field::new("first_trade_id", DataType::UInt64, true),
        Field::new("last_trade_id", DataType::UInt64, true),
        Field::new("buyer_is_maker", DataType::Boolean, true),
        Field::new("stream", DataType::Utf8, true),
        Field::new("ws_ignore", DataType::Boolean, true),
        Field::new("rest_was_best_price_match", DataType::Boolean, true),
        Field::new("source_native_payload_json", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("websocket_ingress_sequence", DataType::UInt64, true),
        Field::new("rest_response_ingress_sequence", DataType::UInt64, true),
        Field::new("rest_response_item_index", DataType::UInt64, true),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("invalid_reason_code", DataType::Utf8, true),
        Field::new("invalid_reason_detail", DataType::Utf8, true),
        Field::new("payload_encoding", DataType::Utf8, true),
        Field::new("payload_length_bytes", DataType::UInt64, true),
        Field::new("payload_sha256", DataType::Utf8, true),
        Field::new("payload_capture_status", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

pub fn rtds_chainlink_btc_usd_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("message_timestamp", false),
        timestamp_field("payload_timestamp", false),
        Field::new("age_ms", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("message_type", DataType::Utf8, false),
        Field::new("freshness_status", DataType::Utf8, false),
        Field::new("subscription_filter", DataType::Utf8, false),
        Field::new("source_native_payload_json", DataType::Utf8, false),
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

fn valid_invalid_reason_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= BINANCE_AGGTRADE_INVALID_REASON_CODE_MAX_BYTES
        && code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_invalid_evidence_envelope(row: &BinanceAggTradeHourlyRow) -> Result<(), &'static str> {
    const REQUIRED_FIELDS: [&str; 13] = [
        "evidence_schema",
        "transport",
        "endpoint",
        "receive_time_utc_ms",
        "invalid_reason_code",
        "payload_encoding",
        "payload_length_bytes",
        "payload_sha256",
        "payload_capture_status",
        "payload_base64",
        "observed_bytes_at_least",
        "declared_content_length",
        "capture_limit_bytes",
    ];

    let value = serde_json::from_str::<serde_json::Value>(&row.source_native_payload_json)
        .map_err(|_| "invalid_evidence_envelope_parseable")?;
    let object = value
        .as_object()
        .ok_or("invalid_evidence_envelope_object")?;
    if REQUIRED_FIELDS
        .iter()
        .any(|field| !object.contains_key(*field))
    {
        return Err("invalid_evidence_required_fields");
    }
    let evidence = serde_json::from_value::<BinanceAggTradeInvalidEvidenceEnvelope>(value)
        .map_err(|_| "invalid_evidence_field_types")?;

    if evidence.evidence_schema != "binance_aggtrade_invalid_evidence_v1" {
        return Err("invalid_evidence_schema");
    }
    if evidence.transport != row.ingest_source
        || evidence.endpoint != row.source_endpoint
        || evidence.receive_time_utc_ms != row.timestamp_received
        || Some(evidence.invalid_reason_code.as_str()) != row.invalid_reason_code.as_deref()
        || Some(evidence.payload_capture_status.as_str()) != row.payload_capture_status.as_deref()
        || evidence.payload_encoding.as_deref() != row.payload_encoding.as_deref()
        || evidence.payload_length_bytes != row.payload_length_bytes
        || evidence.payload_sha256.as_deref() != row.payload_sha256.as_deref()
    {
        return Err("invalid_evidence_row_consistency");
    }

    let limit = u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES)
        .map_err(|_| "invalid_evidence_capture_limit")?;
    if evidence.capture_limit_bytes != limit {
        return Err("invalid_evidence_capture_limit");
    }

    match evidence.payload_capture_status.as_str() {
        BINANCE_AGGTRADE_CAPTURE_COMPLETE => {
            if evidence.payload_encoding.as_deref() != Some("base64") {
                return Err("invalid_evidence_complete_payload");
            }
            let encoded = evidence
                .payload_base64
                .as_deref()
                .ok_or("invalid_evidence_complete_payload")?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| "invalid_evidence_complete_payload")?;
            let decoded_length =
                u64::try_from(decoded.len()).map_err(|_| "invalid_evidence_complete_payload")?;
            if decoded_length > limit
                || evidence.payload_length_bytes != Some(decoded_length)
                || evidence.observed_bytes_at_least != decoded_length
                || evidence
                    .declared_content_length
                    .is_some_and(|declared| declared > limit)
            {
                return Err("invalid_evidence_complete_payload");
            }
            let expected_sha256 = format!("{:x}", Sha256::digest(&decoded));
            if !evidence
                .payload_sha256
                .as_deref()
                .is_some_and(valid_sha256_hex)
                || evidence.payload_sha256.as_deref() != Some(expected_sha256.as_str())
            {
                return Err("invalid_evidence_complete_payload");
            }
        }
        BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED => {
            if evidence.payload_encoding.is_some()
                || evidence.payload_base64.is_some()
                || evidence.payload_sha256.is_some()
                || evidence.payload_length_bytes != evidence.declared_content_length
                || (evidence.observed_bytes_at_least <= limit
                    && evidence
                        .declared_content_length
                        .is_none_or(|declared| declared <= limit))
            {
                return Err("invalid_evidence_oversize_payload");
            }
        }
        _ => return Err("invalid_evidence_capture_status"),
    }
    Ok(())
}
