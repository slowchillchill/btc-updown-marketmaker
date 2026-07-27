use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use arrow_array::{
    Array as _, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::KeyValue;
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;

use crate::market_discovery::{
    MarketWindow, validate_recovered_btc_updown_market_from_sdk_payload,
};
use crate::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
    POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
    PolymarketMarketMetadataV3Row, PolymarketMarketMetadataV4Row, PolymarketMarketMetadataV5Row,
    PolymarketMetadataV3ReferenceRecord, ResolutionPollResult, SourceEvidenceOrigin,
    polymarket_market_metadata_v3_schema, polymarket_market_metadata_v4_schema,
    polymarket_market_metadata_v5_schema, validate_polymarket_metadata_v3_local_references,
};
use crate::polymarket_orderbook_v3::{
    POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketOrderbookV3Event,
    PolymarketOrderbookV3Message, polymarket_orderbook_v3_messages_from_batch,
    polymarket_orderbook_v3_schema, same_outcome_label,
};
use crate::sources::polymarket::{
    GammaTerminalAttemptContext, GammaTerminalFixedIdentity, GammaTerminalResponse,
    evaluate_gamma_terminal_response,
};

const UTC_DAY_MS: i64 = 86_400_000;
const RESOLUTION_ATTEMPT_COUNT: u8 = 6;
const FIRST_SLOT_DELAY_MS: i64 = 3 * 60_000;
const SLOT_INTERVAL_MS: i64 = 60_000;
#[cfg(test)]
const BUDGET_HORIZON_MS: i64 = 8 * 60_000;
const MAX_RECOVERED_PENDING: usize = 2;
const POLYMARKET_ORDERBOOK_V2_SCHEMA_VERSION: &str = "polymarket_orderbook_v2";

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const LEGACY_V2_ROUTING_PROJECTION: [&str; 5] = [
    "timestamp_received",
    "market",
    "event_type",
    "slug",
    "schema_version",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataLayout {
    V2,
    V3,
    V4,
    V5,
}

impl MetadataLayout {
    fn schema(self) -> Option<std::sync::Arc<Schema>> {
        match self {
            Self::V2 => None,
            Self::V3 => Some(polymarket_market_metadata_v3_schema()),
            Self::V4 => Some(polymarket_market_metadata_v4_schema()),
            Self::V5 => Some(polymarket_market_metadata_v5_schema()),
        }
    }

    const fn version(self) -> &'static str {
        match self {
            Self::V2 => POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
            Self::V3 => POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            Self::V4 => POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
            Self::V5 => POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketRestartRecovery {
    pub pending: Vec<RecoveredPolymarketPending>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredPolymarketPending {
    pub slug: String,
    pub clob_market_id: String,
    pub condition_id: String,
    pub up_asset_id: String,
    pub down_asset_id: String,
    pub market_start_utc_ms: i64,
    pub market_end_utc_ms: i64,
    pub fixed_identity_verified_at_utc_ms: i64,
    pub discovery_source_received_at_utc_ms: i64,
    pub discovery_source_raw_sha256: String,
    pub finalized_attempt_ordinals: Vec<u8>,
    pub missed_attempt_ordinals: Vec<u8>,
    pub next_attempt_ordinal: u8,
    pub attempts_recorded: u32,
    pub valid_response_count: u32,
    pub transport_unavailable_count: u32,
    pub missed_slot_count: u32,
    pub last_valid_metadata_received_at_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FixedIdentity {
    slug: String,
    condition_id: String,
    clob_market_id: String,
    asset_ids_json: String,
    up_asset_id: String,
    down_asset_id: String,
    market_start_utc_ms: i64,
    market_end_utc_ms: i64,
    fixed_identity_verified_at_utc_ms: i64,
}

impl FixedIdentity {
    fn from_record(record: &MetadataRecord) -> Result<Self> {
        let asset_ids =
            serde_json::from_str::<Vec<String>>(&record.asset_ids_json).map_err(|error| {
                invalid_data(format!(
                    "Polymarket restart asset_ids decode failed:slug={}:error={error}",
                    record.slug
                ))
            })?;
        if asset_ids.len() != 2
            || asset_ids[0].trim().is_empty()
            || asset_ids[1].trim().is_empty()
            || asset_ids[0] == asset_ids[1]
            || serde_json::to_string(&asset_ids)? != record.asset_ids_json
        {
            return Err(invalid_data(format!(
                "Polymarket restart fixed asset mapping is invalid:slug={}",
                record.slug
            ))
            .into());
        }
        if record.slug.trim().is_empty()
            || record.condition_id.trim().is_empty()
            || record.clob_market_id.trim().is_empty()
            || record.market_start_utc_ms <= 0
            || record.market_start_utc_ms >= record.market_end_utc_ms
            || record.fixed_identity_verified_at_utc_ms <= 0
            || record.fixed_identity_verified_at_utc_ms >= record.market_end_utc_ms
        {
            return Err(invalid_data(format!(
                "Polymarket restart fixed identity is invalid:slug={}",
                record.slug
            ))
            .into());
        }
        Ok(Self {
            slug: record.slug.clone(),
            condition_id: record.condition_id.clone(),
            clob_market_id: record.clob_market_id.clone(),
            asset_ids_json: record.asset_ids_json.clone(),
            up_asset_id: asset_ids[0].clone(),
            down_asset_id: asset_ids[1].clone(),
            market_start_utc_ms: record.market_start_utc_ms,
            market_end_utc_ms: record.market_end_utc_ms,
            fixed_identity_verified_at_utc_ms: record.fixed_identity_verified_at_utc_ms,
        })
    }

    fn slot_utc_ms(&self, ordinal: u8) -> i64 {
        self.market_end_utc_ms
            .saturating_add(FIRST_SLOT_DELAY_MS)
            .saturating_add(i64::from(ordinal.saturating_sub(1)).saturating_mul(SLOT_INTERVAL_MS))
    }
}

#[derive(Clone, Debug)]
struct MetadataRecord {
    artifact_position: FinalizedArtifactPosition,
    kind: String,
    timestamp_received: i64,
    admission_sequence: u64,
    admission_time_utc_ms: i64,
    metadata_origin: String,
    slug: String,
    condition_id: String,
    clob_market_id: String,
    asset_ids_json: String,
    market_start_utc_ms: i64,
    market_end_utc_ms: i64,
    fixed_identity_verified_at_utc_ms: i64,
    lifecycle_state: String,
    runtime_role: String,
    source_origin: Option<String>,
    source_received_at_utc_ms: Option<i64>,
    source_payload_json: Option<String>,
    source_raw_sha256: Option<String>,
    #[cfg(test)]
    lifecycle_transition: Option<String>,
    #[cfg(test)]
    lifecycle_applied_at_utc_ms: Option<i64>,
    runtime_role_transition: Option<String>,
    runtime_role_applied_at_utc_ms: Option<i64>,
    winning_asset_id: Option<String>,
    winning_outcome: Option<String>,
    #[cfg(test)]
    resolution_evidence_source: Option<String>,
    #[cfg(test)]
    resolution_evidence_raw_sha256: Option<String>,
    #[cfg(test)]
    terminal_claim_owner: Option<String>,
    attempt_ordinal: Option<u8>,
    #[cfg(test)]
    attempt_scheduled_utc_ms: Option<i64>,
    attempt_result: Option<String>,
    integrity_error_class: Option<String>,
    #[cfg(test)]
    budget_horizon_utc_ms: Option<i64>,
    #[cfg(test)]
    followup_status: Option<String>,
    attempts_recorded: Option<u32>,
    valid_response_count: Option<u32>,
    transport_unavailable_count: Option<u32>,
    missed_slot_count: Option<u32>,
    last_valid_metadata_received_at_utc_ms: Option<i64>,
    #[cfg(test)]
    terminal_predicate_passed: Option<bool>,
    ws_claimant_source_sequence_epoch_id: Option<String>,
    ws_claimant_collector_admission_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FinalizedArtifactPosition {
    path: PathBuf,
    file_order: usize,
    file_row: u64,
}

impl FinalizedArtifactPosition {
    fn precedes(&self, other: &Self) -> bool {
        self.file_order < other.file_order
            || (self.file_order == other.file_order && self.file_row < other.file_row)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataReferenceLookup {
    slug: String,
    condition_id: String,
    clob_market_id: String,
    asset_ids_json: String,
    market_start_utc_ms: i64,
    market_end_utc_ms: i64,
    fixed_identity_verified_at_utc_ms: i64,
    source_origin: String,
    source_received_at_utc_ms: i64,
}

impl MetadataReferenceLookup {
    fn from_local_reference(record: &MetadataRecord) -> Option<Self> {
        if record.kind != "market_state_transition" || record.source_payload_json.is_some() {
            return None;
        }
        Some(Self {
            slug: record.slug.clone(),
            condition_id: record.condition_id.clone(),
            clob_market_id: record.clob_market_id.clone(),
            asset_ids_json: record.asset_ids_json.clone(),
            market_start_utc_ms: record.market_start_utc_ms,
            market_end_utc_ms: record.market_end_utc_ms,
            fixed_identity_verified_at_utc_ms: record.fixed_identity_verified_at_utc_ms,
            source_origin: record.source_origin.clone()?,
            source_received_at_utc_ms: record.source_received_at_utc_ms?,
        })
    }

    fn matches_source_authority(&self, record: &MetadataRecord) -> bool {
        record.source_payload_json.is_some()
            && record.slug == self.slug
            && record.condition_id == self.condition_id
            && record.clob_market_id == self.clob_market_id
            && record.asset_ids_json == self.asset_ids_json
            && record.market_start_utc_ms == self.market_start_utc_ms
            && record.market_end_utc_ms == self.market_end_utc_ms
            && record.fixed_identity_verified_at_utc_ms == self.fixed_identity_verified_at_utc_ms
            && record.source_origin.as_deref() == Some(self.source_origin.as_str())
            && record.source_received_at_utc_ms == Some(self.source_received_at_utc_ms)
    }
}

fn require_canonical_metadata_schema(
    schema: &Schema,
    path: &Path,
    layout: MetadataLayout,
) -> Result<()> {
    let expected = layout
        .schema()
        .ok_or_else(|| invalid_data("metadata v2 has no current canonical recovery schema"))?;
    if let Some(missing) = expected
        .fields()
        .iter()
        .find(|field| schema.index_of(field.name()).is_err())
    {
        return Err(invalid_data(format!(
            "Polymarket restart {} projection column missing:file={}:column={}",
            layout.version(),
            path.display(),
            missing.name()
        ))
        .into());
    }
    if schema.fields().len() != expected.fields().len() {
        return Err(invalid_data(format!(
            "Polymarket restart finalized {} schema field count mismatch:file={}:expected={}:actual={}",
            layout.version(),
            path.display(),
            expected.fields().len(),
            schema.fields().len()
        ))
        .into());
    }
    for (index, (actual, expected)) in schema.fields().iter().zip(expected.fields()).enumerate() {
        if actual.name() != expected.name()
            || actual.data_type() != expected.data_type()
            || actual.is_nullable() != expected.is_nullable()
        {
            return Err(invalid_data(format!(
                "Polymarket restart finalized {} schema mismatch:file={}:index={index}:expected={expected:?}:actual={actual:?}",
                layout.version(),
                path.display()
            ))
            .into());
        }
    }
    Ok(())
}

fn finalized_metadata_v3_row_from_batch(
    batch: &RecordBatch,
    row: usize,
    layout: MetadataLayout,
) -> Result<PolymarketMarketMetadataV3Row> {
    let admission_sequence = required_u64(batch, row, "collector_admission_sequence")?;
    let admission_time_utc_ms = required_timestamp(batch, row, "collector_admission_time_utc_ms")?;
    let schema = layout
        .schema()
        .ok_or_else(|| invalid_data("metadata v2 cannot decode a typed metadata row"))?;
    let mut object = serde_json::Map::new();
    for field in schema.fields() {
        if matches!(
            field.name().as_str(),
            "collector_admission_sequence" | "collector_admission_time_utc_ms"
        ) {
            continue;
        }
        object.insert(
            field.name().clone(),
            metadata_v3_json_value(batch, row, field)?,
        );
    }
    if layout == MetadataLayout::V5 {
        object.insert("resolution_evidence_raw_sha256".to_owned(), Value::Null);
    }
    let draft = serde_json::to_string(&Value::Object(object))?;
    PolymarketMarketMetadataV3Row::from_draft_json_with_admission(
        &draft,
        admission_sequence,
        admission_time_utc_ms,
    )
    .map_err(|error| {
        invalid_data(format!(
            "Polymarket restart finalized {} row decode failed:row={row}:error={error}",
            layout.version()
        ))
        .into()
    })
}

fn metadata_v3_json_value(batch: &RecordBatch, row: usize, field: &Field) -> Result<Value> {
    let index = batch.schema().index_of(field.name())?;
    let column = batch.column(index);
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    let value = match field.data_type() {
        DataType::Utf8 => Value::String(
            typed_column::<StringArray>(batch, field.name())?
                .value(row)
                .to_owned(),
        ),
        DataType::Boolean => {
            Value::Bool(typed_column::<BooleanArray>(batch, field.name())?.value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Value::from(typed_column::<TimestampMillisecondArray>(batch, field.name())?.value(row))
        }
        DataType::UInt8 => Value::from(typed_column::<UInt8Array>(batch, field.name())?.value(row)),
        DataType::UInt16 => {
            Value::from(typed_column::<UInt16Array>(batch, field.name())?.value(row))
        }
        DataType::UInt32 => {
            Value::from(typed_column::<UInt32Array>(batch, field.name())?.value(row))
        }
        DataType::UInt64 => {
            Value::from(typed_column::<UInt64Array>(batch, field.name())?.value(row))
        }
        other => {
            return Err(invalid_data(format!(
                "Polymarket restart finalized v3 column type is unsupported:column={}:type={other:?}",
                field.name()
            ))
            .into());
        }
    };
    Ok(value)
}

fn validate_finalized_metadata_v3_payload_semantics(
    row: &PolymarketMarketMetadataV3Row,
) -> Result<()> {
    match (
        row.source_evidence_origin,
        row.source_evidence_payload_json.as_deref(),
    ) {
        (Some(SourceEvidenceOrigin::GammaMarketsDiscovery), Some(payload)) => {
            validate_finalized_discovery_payload(row, payload)
        }
        (Some(SourceEvidenceOrigin::GammaBySlug), Some(payload)) => {
            validate_finalized_gamma_attempt_payload(row, payload)
        }
        (Some(SourceEvidenceOrigin::MarketResolvedWs), Some(payload)) => {
            validate_finalized_ws_payload_platform_timestamp(row, payload)
        }
        _ => Ok(()),
    }
}

fn validate_finalized_ws_payload_platform_timestamp(
    row: &PolymarketMarketMetadataV3Row,
    payload: &str,
) -> Result<()> {
    let payload = serde_json::from_str::<Value>(payload).map_err(|error| {
        invalid_data(format!(
            "market_resolved payload decode failed:slug={}:error={error}",
            row.slug
        ))
    })?;
    let payload = payload.as_object().ok_or_else(|| {
        invalid_data(format!(
            "market_resolved source payload is not an object:slug={}",
            row.slug
        ))
    })?;
    if payload.get("event_type").and_then(Value::as_str) != Some("market_resolved") {
        return Err(invalid_data(format!(
            "market_resolved source payload has wrong event type:slug={}",
            row.slug
        ))
        .into());
    }
    let platform_timestamp_raw = payload
        .get("timestamp")
        .and_then(json_scalar_string)
        .ok_or_else(|| {
            invalid_data(format!(
                "market_resolved source payload timestamp is missing:slug={}",
                row.slug
            ))
        })?;
    let platform_timestamp_utc = platform_timestamp_raw.parse::<i64>().map_err(|error| {
        invalid_data(format!(
            "market_resolved source payload timestamp is invalid:slug={}:error={error}",
            row.slug
        ))
    })?;
    if row.ws_platform_timestamp_raw.as_deref() != Some(platform_timestamp_raw.as_str())
        || row.ws_platform_timestamp_utc != Some(platform_timestamp_utc)
    {
        return Err(invalid_data(format!(
            "market_resolved payload timestamp contradicts persisted platform timestamp:slug={}",
            row.slug
        ))
        .into());
    }
    Ok(())
}

fn validate_finalized_discovery_payload(
    row: &PolymarketMarketMetadataV3Row,
    payload: &str,
) -> Result<()> {
    let market = serde_json::from_str::<SdkGammaMarket>(payload).map_err(|error| {
        invalid_data(format!(
            "discovery SDK payload decode failed:slug={}:error={error}",
            row.slug
        ))
    })?;
    if row.market_start_utc.rem_euclid(1_000) != 0 || row.market_end_utc.rem_euclid(1_000) != 0 {
        return Err(invalid_data(format!(
            "discovery fixed window is not second-aligned:slug={}",
            row.slug
        ))
        .into());
    }
    let window = MarketWindow {
        target_ts_s: row.market_start_utc.div_euclid(1_000),
        end_ts_s: row.market_end_utc.div_euclid(1_000),
        slug: row.slug.clone(),
    };
    let discovered = validate_recovered_btc_updown_market_from_sdk_payload(
        &market,
        &window,
        row.fixed_identity_verified_at_utc,
    )
    .map_err(|reason| {
        invalid_data(format!(
            "discovery SDK payload failed fixed-identity validation:slug={}:reason={reason:?}",
            row.slug
        ))
    })?;
    let persisted_assets = serde_json::from_str::<Vec<String>>(&row.asset_ids)?;
    if discovered.id != row.clob_market_id
        || discovered.condition_id != row.condition_id
        || persisted_assets
            != [
                discovered.up_asset_id.clone(),
                discovered.down_asset_id.clone(),
            ]
        || discovered.start_ts_s.saturating_mul(1_000) != row.market_start_utc
        || discovered.end_ts_s.saturating_mul(1_000) != row.market_end_utc
    {
        return Err(invalid_data(format!(
            "discovery SDK payload contradicts persisted fixed identity:slug={}",
            row.slug
        ))
        .into());
    }
    validate_persisted_gamma_response_fields(row, &market)
}

fn validate_finalized_gamma_attempt_payload(
    row: &PolymarketMarketMetadataV3Row,
    payload: &str,
) -> Result<()> {
    let market = serde_json::from_str::<SdkGammaMarket>(payload).map_err(|error| {
        invalid_data(format!(
            "Gamma attempt SDK payload decode failed:slug={}:error={error}",
            row.slug
        ))
    })?;
    let asset_ids = serde_json::from_str::<Vec<String>>(&row.asset_ids)?;
    let [up_asset_id, down_asset_id] = asset_ids.as_slice() else {
        return Err(invalid_data(format!(
            "Gamma attempt persisted fixed assets are not binary:slug={}",
            row.slug
        ))
        .into());
    };
    let scheduled_at_utc_ms = row.resolution_poll_scheduled_utc.ok_or_else(|| {
        invalid_data(format!(
            "Gamma attempt scheduled time is missing after row validation:slug={}",
            row.slug
        ))
    })?;
    let fetch_start_utc_ms = row.metadata_fetch_start_utc.ok_or_else(|| {
        invalid_data(format!(
            "Gamma attempt fetch time is missing after row validation:slug={}",
            row.slug
        ))
    })?;
    let evaluation = evaluate_gamma_terminal_response(
        &GammaTerminalFixedIdentity {
            slug: row.slug.clone(),
            clob_market_id: row.clob_market_id.clone(),
            condition_id: row.condition_id.clone(),
            up_asset_id: up_asset_id.clone(),
            down_asset_id: down_asset_id.clone(),
            market_end_utc_ms: row.market_end_utc,
        },
        GammaTerminalAttemptContext {
            scheduled_at_utc_ms,
            fetch_start_utc_ms,
            received_at_utc_ms: row.timestamp_received,
            response_bound_to_attempt: true,
        },
        GammaTerminalResponse::Decoded(&market),
    );
    let persisted_result = row.resolution_poll_result.ok_or_else(|| {
        invalid_data(format!(
            "Gamma attempt result is missing after row validation:slug={}",
            row.slug
        ))
    })?;
    let result_matches = if persisted_result == ResolutionPollResult::PostTerminalClaimObservation {
        matches!(
            evaluation.resolution_poll_result,
            ResolutionPollResult::Terminal | ResolutionPollResult::ValidNonterminal
        )
    } else {
        persisted_result == evaluation.resolution_poll_result
    };
    if !result_matches
        || row.resolution_integrity_error_class != evaluation.resolution_integrity_error_class
        || row.terminal_predicate_passed != evaluation.terminal_predicate_passed
        || row.terminal_predicate_failure_reason != evaluation.terminal_predicate_failure_reason
        || row.active != evaluation.observed_active
        || row.closed_time_raw != evaluation.closed_time_raw
        || row.closed_time_utc != evaluation.closed_time_utc_ms
    {
        return Err(invalid_data(format!(
            "Gamma attempt payload evaluation contradicts persisted result:slug={}:ordinal={:?}",
            row.slug, row.resolution_poll_attempt_ordinal
        ))
        .into());
    }
    if persisted_result == ResolutionPollResult::Terminal {
        let winner = evaluation.winner.ok_or_else(|| {
            invalid_data(format!(
                "Gamma terminal payload produced no winner:slug={}",
                row.slug
            ))
        })?;
        if row.winning_asset_id.as_deref() != Some(winner.winning_asset_id.as_str())
            || row.winning_outcome.as_deref() != Some(winner.winning_outcome.as_str())
        {
            return Err(invalid_data(format!(
                "Gamma terminal payload winner contradicts persisted winner:slug={}",
                row.slug
            ))
            .into());
        }
    }
    validate_persisted_gamma_response_fields(row, &market)
}

fn validate_persisted_gamma_response_fields(
    row: &PolymarketMarketMetadataV3Row,
    market: &SdkGammaMarket,
) -> Result<()> {
    let outcomes_raw = market
        .outcomes
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let outcome_prices_raw = market
        .outcome_prices
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    if row.closed != market.closed
        || row.closed_time_raw != market.closed_time
        || row.automatically_resolved != market.automatically_resolved
        || row.uma_resolution_status != market.uma_resolution_status
        || row.outcomes_raw != outcomes_raw
        || row.outcome_prices_raw != outcome_prices_raw
        || row.active != market.active
        || row.accepting_orders != market.accepting_orders
    {
        return Err(invalid_data(format!(
            "Gamma payload response fields contradict persisted row:slug={}",
            row.slug
        ))
        .into());
    }
    Ok(())
}

impl MetadataRecord {
    fn from_batch(
        batch: &RecordBatch,
        row: usize,
        artifact_position: FinalizedArtifactPosition,
        layout: MetadataLayout,
    ) -> Result<Self> {
        let full_row = finalized_metadata_v3_row_from_batch(batch, row, layout)?;
        let (
            validation,
            ws_claimant_source_sequence_epoch_id,
            ws_claimant_collector_admission_sequence,
        ) = match layout {
            MetadataLayout::V3 => (full_row.validate(), None, None),
            MetadataLayout::V4 => {
                let v4 = PolymarketMarketMetadataV4Row {
                    v3_prefix: full_row.clone(),
                    ws_claimant_source_sequence_epoch_id: optional_string(
                        batch,
                        row,
                        "ws_claimant_source_sequence_epoch_id",
                    )?,
                    ws_claimant_collector_admission_sequence: optional_u64(
                        batch,
                        row,
                        "ws_claimant_collector_admission_sequence",
                    )?,
                };
                (
                    v4.validate(),
                    v4.ws_claimant_source_sequence_epoch_id,
                    v4.ws_claimant_collector_admission_sequence,
                )
            }
            MetadataLayout::V5 => {
                if full_row.schema_version != POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION {
                    return Err(invalid_data(format!(
                        "Polymarket restart finalized metadata row schema version mismatch:file={}:row={row}:actual={}:expected={POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION}",
                        artifact_position.path.display(),
                        full_row.schema_version,
                    ))
                    .into());
                }
                let v5 = PolymarketMarketMetadataV5Row::from_v3_prefix(
                    full_row.clone(),
                    optional_string(batch, row, "ws_claimant_source_sequence_epoch_id")?,
                    optional_u64(batch, row, "ws_claimant_collector_admission_sequence")?,
                );
                (
                    v5.validate(),
                    v5.ws_claimant_source_sequence_epoch_id,
                    v5.ws_claimant_collector_admission_sequence,
                )
            }
            MetadataLayout::V2 => {
                return Err(invalid_data("metadata v2 cannot produce a recovery record").into());
            }
        };
        if !validation.is_complete() {
            return Err(invalid_data(format!(
                "Polymarket restart finalized {} row violates canonical schema:file={}:row={row}:violations={:?}",
                layout.version(),
                artifact_position.path.display(),
                validation.violations
            ))
            .into());
        }
        validate_finalized_metadata_v3_payload_semantics(&full_row).map_err(|error| {
            invalid_data(format!(
                "Polymarket restart finalized metadata payload semantics failed:file={}:row={row}:error={error}",
                artifact_position.path.display()
            ))
        })?;
        Ok(Self {
            artifact_position,
            kind: required_string(batch, row, "metadata_row_kind")?,
            timestamp_received: required_timestamp(batch, row, "timestamp_received")?,
            admission_sequence: required_u64(batch, row, "collector_admission_sequence")?,
            admission_time_utc_ms: required_timestamp(
                batch,
                row,
                "collector_admission_time_utc_ms",
            )?,
            metadata_origin: required_string(batch, row, "metadata_origin")?,
            slug: required_string(batch, row, "slug")?,
            condition_id: required_string(batch, row, "condition_id")?,
            clob_market_id: required_string(batch, row, "clob_market_id")?,
            asset_ids_json: required_string(batch, row, "asset_ids")?,
            market_start_utc_ms: required_timestamp(batch, row, "market_start_utc")?,
            market_end_utc_ms: required_timestamp(batch, row, "market_end_utc")?,
            fixed_identity_verified_at_utc_ms: required_timestamp(
                batch,
                row,
                "fixed_identity_verified_at_utc",
            )?,
            lifecycle_state: required_string(batch, row, "market_lifecycle_state")?,
            runtime_role: required_string(batch, row, "market_runtime_role")?,
            source_origin: optional_string(batch, row, "source_evidence_origin")?,
            source_received_at_utc_ms: optional_timestamp(
                batch,
                row,
                "source_evidence_received_at_utc",
            )?,
            source_payload_json: optional_string(batch, row, "source_evidence_payload_json")?,
            source_raw_sha256: optional_string(batch, row, "source_evidence_raw_sha256")?,
            #[cfg(test)]
            lifecycle_transition: optional_string(batch, row, "market_lifecycle_transition")?,
            #[cfg(test)]
            lifecycle_applied_at_utc_ms: optional_timestamp(
                batch,
                row,
                "lifecycle_applied_at_utc",
            )?,
            runtime_role_transition: optional_string(batch, row, "market_runtime_role_transition")?,
            runtime_role_applied_at_utc_ms: optional_timestamp(
                batch,
                row,
                "runtime_role_applied_at_utc",
            )?,
            winning_asset_id: optional_string(batch, row, "winning_asset_id")?,
            winning_outcome: optional_string(batch, row, "winning_outcome")?,
            #[cfg(test)]
            resolution_evidence_source: optional_string(batch, row, "resolution_evidence_source")?,
            #[cfg(test)]
            resolution_evidence_raw_sha256: if layout == MetadataLayout::V5 {
                None
            } else {
                optional_string(batch, row, "resolution_evidence_raw_sha256")?
            },
            #[cfg(test)]
            terminal_claim_owner: optional_string(batch, row, "terminal_claim_owner")?,
            attempt_ordinal: optional_u8(batch, row, "resolution_poll_attempt_ordinal")?,
            #[cfg(test)]
            attempt_scheduled_utc_ms: optional_timestamp(
                batch,
                row,
                "resolution_poll_scheduled_utc",
            )?,
            attempt_result: optional_string(batch, row, "resolution_poll_result")?,
            integrity_error_class: optional_string(batch, row, "resolution_integrity_error_class")?,
            #[cfg(test)]
            budget_horizon_utc_ms: optional_timestamp(batch, row, "resolution_budget_horizon_utc")?,
            #[cfg(test)]
            followup_status: optional_string(batch, row, "resolution_followup_status")?,
            attempts_recorded: optional_u32(batch, row, "resolution_poll_attempts_recorded")?,
            valid_response_count: optional_u32(batch, row, "resolution_valid_response_count")?,
            transport_unavailable_count: optional_u32(
                batch,
                row,
                "resolution_transport_unavailable_count",
            )?,
            missed_slot_count: optional_u32(batch, row, "resolution_missed_slot_count")?,
            last_valid_metadata_received_at_utc_ms: optional_timestamp(
                batch,
                row,
                "last_valid_metadata_received_utc",
            )?,
            #[cfg(test)]
            terminal_predicate_passed: optional_bool(batch, row, "terminal_predicate_passed")?,
            ws_claimant_source_sequence_epoch_id,
            ws_claimant_collector_admission_sequence,
        })
    }

    fn fixed_identity(&self) -> Result<FixedIdentity> {
        FixedIdentity::from_record(self)
    }

    fn validate_recovery_eligibility(&self) -> Result<()> {
        self.fixed_identity()?;
        if self.attempt_result.as_deref() == Some("integrity_error")
            || self.integrity_error_class.is_some()
        {
            return Err(invalid_data(format!(
                "Polymarket restart encountered finalized integrity_error:slug={}:ordinal={:?}",
                self.slug, self.attempt_ordinal
            ))
            .into());
        }
        Ok(())
    }

    fn reference_record(&self) -> PolymarketMetadataV3ReferenceRecord {
        PolymarketMetadataV3ReferenceRecord {
            metadata_row_kind: self.kind.clone(),
            slug: self.slug.clone(),
            condition_id: self.condition_id.clone(),
            clob_market_id: self.clob_market_id.clone(),
            asset_ids: self.asset_ids_json.clone(),
            market_start_utc: self.market_start_utc_ms,
            market_end_utc: self.market_end_utc_ms,
            fixed_identity_verified_at_utc: self.fixed_identity_verified_at_utc_ms,
            source_evidence_origin: self.source_origin.clone(),
            source_evidence_received_at_utc: self.source_received_at_utc_ms,
            source_evidence_raw_sha256: self.source_raw_sha256.clone(),
            source_evidence_payload_present: self.source_payload_json.is_some(),
        }
    }

    fn is_current_authority(&self) -> bool {
        self.runtime_role == "current"
            && matches!(
                (self.kind.as_str(), self.metadata_origin.as_str()),
                ("metadata_snapshot", "gamma_markets_discovery")
                    | ("market_state_transition", "local_market_state_timer")
            )
    }

    fn is_pending_transition(&self) -> bool {
        self.kind == "market_state_transition"
            && self.lifecycle_state == "active"
            && self.runtime_role == "resolution_pending"
            && self.metadata_origin == "local_market_state_timer"
            && self.runtime_role_transition.as_deref() == Some("current_to_resolution_pending")
            && self.source_origin.as_deref() == Some("gamma_markets_discovery")
            && self.source_received_at_utc_ms.is_some()
            && self.source_raw_sha256.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QualifiedTypedSource {
    timestamp_received: i64,
    collector_admission_time_utc_ms: i64,
    source_sequence_epoch_id: String,
    collector_admission_sequence: u64,
    winning_asset_id: String,
    winning_outcome: String,
}

#[derive(Default)]
struct AttemptCounters {
    ordinals: BTreeSet<u8>,
    attempts: u32,
    valid: u32,
    transport: u32,
    missed: u32,
    last_valid_received: Option<i64>,
}

pub fn restore_polymarket_resolution_recovery(
    dataset_root: &Path,
    recovery_time_ms: i64,
) -> Result<PolymarketRestartRecovery> {
    if recovery_time_ms <= 0 {
        return Err(invalid_data("Polymarket restart recovery time must be positive").into());
    }
    let days = recovery_days(recovery_time_ms)?;
    let metadata = read_finalized_metadata_for_validation(dataset_root, &days)?;
    if metadata.is_empty() {
        read_qualified_typed_market_sources(
            dataset_root,
            &days,
            &BTreeMap::new(),
            recovery_time_ms,
        )?;
        return Ok(PolymarketRestartRecovery {
            pending: Vec::new(),
        });
    }

    let mut grouped = BTreeMap::<String, Vec<MetadataRecord>>::new();
    for record in metadata {
        record.validate_recovery_eligibility()?;
        grouped.entry(record.slug.clone()).or_default().push(record);
    }

    let mut identities = BTreeMap::new();
    for (slug, records) in &mut grouped {
        records.sort_by_key(|record| {
            (
                record.timestamp_received,
                record.admission_sequence,
                record.kind.clone(),
            )
        });
        let identity = records[0].fixed_identity()?;
        if records.iter().any(|record| {
            !record
                .fixed_identity()
                .is_ok_and(|candidate| candidate == identity)
        }) {
            return Err(invalid_data(format!(
                "Polymarket restart fixed identity conflict:slug={slug}"
            ))
            .into());
        }
        identities.insert(slug.clone(), identity);
    }
    reject_dual_metadata_layout_for_current_lifecycles(
        dataset_root,
        &days,
        grouped.keys().map(String::as_str).collect(),
    )?;

    let reference_authorities =
        read_precise_metadata_reference_authorities(dataset_root, &days, &grouped)?;
    for authority in &reference_authorities {
        authority.validate_recovery_eligibility()?;
    }
    validate_polymarket_metadata_v3_local_references(
        &grouped
            .values()
            .flatten()
            .chain(reference_authorities.iter())
            .map(MetadataRecord::reference_record)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| {
        invalid_data(format!(
            "Polymarket restart source reference integrity:{error}"
        ))
    })?;

    let qualified_typed =
        read_qualified_typed_market_sources(dataset_root, &days, &identities, recovery_time_ms)?;

    let mut pending_counters = BTreeMap::new();
    for (slug, records) in &grouped {
        let identity = identities
            .get(slug)
            .expect("grouped validation identity has fixed identity");
        let matching_typed = qualified_typed
            .get(slug)
            .map(Vec::as_slice)
            .unwrap_or_default();

        let terminals = terminal_rows(records);
        let summaries = records
            .iter()
            .filter(|record| record.kind == "resolution_followup_summary")
            .collect::<Vec<_>>();
        if terminals.len() > 1 {
            return Err(invalid_data(format!(
                "Polymarket restart has duplicate terminal rows:slug={slug}:count={}",
                terminals.len()
            ))
            .into());
        }
        if summaries.len() > 1 {
            return Err(invalid_data(format!(
                "Polymarket restart has duplicate followup summaries:slug={slug}:count={}",
                summaries.len()
            ))
            .into());
        }
        if !terminals.is_empty() && !summaries.is_empty() {
            return Err(invalid_data(format!(
                "Polymarket restart terminal and summary coexist:slug={slug}"
            ))
            .into());
        }
        validate_post_rows(records, terminals.first().copied())?;

        let counters = attempt_counters(records)?;
        if let Some(terminal) = terminals.first().copied() {
            validate_terminal_typed_sources(terminal, matching_typed, identity)?;
            continue;
        }
        if let Some(summary) = summaries.first().copied() {
            validate_typed_summary(summary, &counters, matching_typed)?;
            continue;
        }
        if !matching_typed.is_empty() {
            return Err(invalid_data(format!(
                "Polymarket restart matching source has no terminal or summary:slug={slug}"
            ))
            .into());
        }
        pending_counters.insert(slug.clone(), counters);
    }

    let mut recovered = Vec::new();
    for (slug, counters) in pending_counters {
        let identity = identities
            .remove(&slug)
            .expect("validated pending identity has fixed identity");
        let records = grouped
            .remove(&slug)
            .expect("validated pending identity has metadata records");
        if recovery_time_ms < identity.market_end_utc_ms {
            continue;
        }
        let Some(authority) = durable_current_authority(&records) else {
            continue;
        };
        let mut missed_attempt_ordinals = Vec::new();
        let mut next_attempt_ordinal = None;
        for ordinal in 1..=RESOLUTION_ATTEMPT_COUNT {
            if counters.ordinals.contains(&ordinal) {
                continue;
            }
            if identity.slot_utc_ms(ordinal) <= recovery_time_ms {
                missed_attempt_ordinals.push(ordinal);
            } else if next_attempt_ordinal.is_none() {
                next_attempt_ordinal = Some(ordinal);
            }
        }
        let Some(next_attempt_ordinal) = next_attempt_ordinal else {
            continue;
        };
        recovered.push(RecoveredPolymarketPending {
            slug: identity.slug.clone(),
            clob_market_id: identity.clob_market_id.clone(),
            condition_id: identity.condition_id.clone(),
            up_asset_id: identity.up_asset_id.clone(),
            down_asset_id: identity.down_asset_id.clone(),
            market_start_utc_ms: identity.market_start_utc_ms,
            market_end_utc_ms: identity.market_end_utc_ms,
            fixed_identity_verified_at_utc_ms: identity.fixed_identity_verified_at_utc_ms,
            discovery_source_received_at_utc_ms: authority
                .source_received_at_utc_ms
                .expect("durable authority has source receive"),
            discovery_source_raw_sha256: authority
                .source_raw_sha256
                .clone()
                .expect("durable authority has source digest"),
            finalized_attempt_ordinals: counters.ordinals.iter().copied().collect(),
            missed_attempt_ordinals,
            next_attempt_ordinal,
            attempts_recorded: counters.attempts,
            valid_response_count: counters.valid,
            transport_unavailable_count: counters.transport,
            missed_slot_count: counters.missed,
            last_valid_metadata_received_at_utc_ms: counters.last_valid_received,
        });
    }
    if recovered.len() > MAX_RECOVERED_PENDING {
        return Err(invalid_data(format!(
            "Polymarket restart pending bound exceeded:count={}:limit={MAX_RECOVERED_PENDING}",
            recovered.len()
        ))
        .into());
    }
    Ok(PolymarketRestartRecovery { pending: recovered })
}

fn terminal_rows(records: &[MetadataRecord]) -> Vec<&MetadataRecord> {
    records
        .iter()
        .filter(|record| {
            record.kind == "resolution_terminal_ws"
                || (record.kind == "resolution_poll_attempt"
                    && record.attempt_result.as_deref() == Some("terminal"))
        })
        .collect()
}

fn validate_post_rows(records: &[MetadataRecord], terminal: Option<&MetadataRecord>) -> Result<()> {
    for post in records.iter().filter(|record| {
        record.kind == "resolution_poll_attempt"
            && record.attempt_result.as_deref() == Some("post_terminal_claim_observation")
    }) {
        let Some(terminal) = terminal else {
            return Err(invalid_data(format!(
                "Polymarket restart orphan post-terminal observation:slug={}",
                post.slug
            ))
            .into());
        };
        if terminal.kind != "resolution_terminal_ws"
            || !terminal.artifact_position.precedes(&post.artifact_position)
            || terminal.admission_sequence >= post.admission_sequence
        {
            return Err(invalid_data(format!(
                "Polymarket restart post-terminal prefix is invalid:slug={}:ordinal={}:terminal={}:{}:post={}:{}",
                post.slug,
                post.attempt_ordinal.unwrap_or_default(),
                terminal.artifact_position.path.display(),
                terminal.artifact_position.file_row,
                post.artifact_position.path.display(),
                post.artifact_position.file_row,
            ))
            .into());
        }
    }
    Ok(())
}

fn attempt_counters(records: &[MetadataRecord]) -> Result<AttemptCounters> {
    let mut counters = AttemptCounters::default();
    for record in records
        .iter()
        .filter(|record| record.kind == "resolution_poll_attempt")
    {
        let ordinal = record
            .attempt_ordinal
            .expect("validated attempt has ordinal");
        if !counters.ordinals.insert(ordinal) {
            return Err(invalid_data(format!(
                "Polymarket restart has duplicate attempt ordinal:slug={}:ordinal={ordinal}",
                record.slug
            ))
            .into());
        }
        counters.attempts = counters.attempts.saturating_add(1);
        match record.attempt_result.as_deref() {
            Some("valid_nonterminal") => {
                let received = record.source_received_at_utc_ms.ok_or_else(|| {
                    invalid_data(format!(
                        "Polymarket restart valid response has no source receive:slug={}:ordinal={ordinal}",
                        record.slug
                    ))
                })?;
                counters.valid = counters.valid.saturating_add(1);
                counters.last_valid_received = Some(
                    counters
                        .last_valid_received
                        .map_or(received, |last| last.max(received)),
                );
            }
            Some("transport_unavailable") => {
                counters.transport = counters.transport.saturating_add(1);
            }
            Some("missed_due_to_downtime") => {
                counters.missed = counters.missed.saturating_add(1);
            }
            Some("terminal" | "post_terminal_claim_observation") => {}
            other => {
                return Err(invalid_data(format!(
                    "Polymarket restart cannot count attempt result:slug={}:result={other:?}",
                    record.slug
                ))
                .into());
            }
        }
    }
    let durable_prefix = (1..=u8::try_from(counters.ordinals.len())
        .map_err(|_| invalid_data("Polymarket restart attempt prefix length exceeds u8"))?)
        .collect::<BTreeSet<_>>();
    if counters.ordinals != durable_prefix {
        let slug = records
            .first()
            .map(|record| record.slug.as_str())
            .unwrap_or("<unknown>");
        return Err(invalid_data(format!(
            "Polymarket restart durable attempt ordinals are not a prefix:slug={slug}:ordinals={:?}",
            counters.ordinals
        ))
        .into());
    }
    Ok(counters)
}

fn validate_terminal_typed_sources(
    terminal: &MetadataRecord,
    sources: &[QualifiedTypedSource],
    identity: &FixedIdentity,
) -> Result<()> {
    let winning_asset = terminal
        .winning_asset_id
        .as_deref()
        .expect("validated terminal has winner");
    let winning_outcome = terminal
        .winning_outcome
        .as_deref()
        .expect("validated terminal has outcome");
    if !winner_matches(identity, winning_asset, winning_outcome)
        || sources.iter().any(|source| {
            source.winning_asset_id != winning_asset
                || !same_outcome_label(&source.winning_outcome, winning_outcome)
        })
    {
        return Err(invalid_data(format!(
            "Polymarket restart typed terminal/source winner mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    let role_exit = terminal.runtime_role_applied_at_utc_ms.ok_or_else(|| {
        invalid_data(format!(
            "Polymarket restart typed terminal has no role-exit time:slug={}",
            identity.slug
        ))
    })?;
    if sources.iter().any(|source| {
        source.timestamp_received > role_exit
            || source.collector_admission_time_utc_ms > terminal.admission_time_utc_ms
    }) {
        return Err(invalid_data(format!(
            "Polymarket restart typed source is later than terminal role exit/admission:slug={}",
            identity.slug
        ))
        .into());
    }

    if terminal.kind == "resolution_terminal_ws" {
        let claimant_epoch = terminal
            .ws_claimant_source_sequence_epoch_id
            .as_deref()
            .expect("validated metadata WS terminal has claimant epoch");
        let claimant_sequence = terminal
            .ws_claimant_collector_admission_sequence
            .expect("validated metadata WS terminal has claimant sequence");
        let matches = sources
            .iter()
            .filter(|source| {
                source.source_sequence_epoch_id == claimant_epoch
                    && source.collector_admission_sequence == claimant_sequence
            })
            .collect::<Vec<_>>();
        let [claimant] = matches.as_slice() else {
            return Err(invalid_data(format!(
                "Polymarket restart WS claimant does not resolve exactly once:slug={}:matches={}",
                identity.slug,
                matches.len()
            ))
            .into());
        };
        if terminal.source_received_at_utc_ms != Some(claimant.timestamp_received) {
            return Err(invalid_data(format!(
                "Polymarket restart WS claimant receive time mismatch:slug={}",
                identity.slug
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_typed_summary(
    summary: &MetadataRecord,
    counters: &AttemptCounters,
    sources: &[QualifiedTypedSource],
) -> Result<()> {
    validate_summary_counters(summary, counters)?;
    if !sources.is_empty() {
        return Err(invalid_data(format!(
            "Polymarket restart exhausted summary coexists with typed market_resolved source:slug={}",
            summary.slug
        ))
        .into());
    }
    Ok(())
}

fn validate_summary_counters(summary: &MetadataRecord, counters: &AttemptCounters) -> Result<()> {
    if counters.ordinals != BTreeSet::from([1, 2, 3, 4, 5, 6])
        || counters.attempts != u32::from(RESOLUTION_ATTEMPT_COUNT)
        || summary.attempts_recorded != Some(counters.attempts)
        || summary.valid_response_count != Some(counters.valid)
        || summary.transport_unavailable_count != Some(counters.transport)
        || summary.missed_slot_count != Some(counters.missed)
        || counters
            .valid
            .saturating_add(counters.transport)
            .saturating_add(counters.missed)
            != u32::from(RESOLUTION_ATTEMPT_COUNT)
        || summary.last_valid_metadata_received_at_utc_ms != counters.last_valid_received
    {
        return Err(invalid_data(format!(
            "Polymarket restart summary counters do not reconstruct six slots:slug={}",
            summary.slug
        ))
        .into());
    }
    Ok(())
}

fn durable_current_authority(records: &[MetadataRecord]) -> Option<&MetadataRecord> {
    let current = records
        .iter()
        .filter(|record| record.is_current_authority())
        .max_by_key(|record| (record.timestamp_received, record.admission_sequence))?;
    let basis = records
        .iter()
        .filter(|record| record.is_pending_transition())
        .max_by_key(|record| (record.timestamp_received, record.admission_sequence))
        .unwrap_or(current);
    Some(basis)
}

fn relevant_raw_error(identity: &FixedIdentity, detail: impl std::fmt::Display) -> io::Error {
    invalid_data(format!(
        "Polymarket restart relevant raw is invalid:slug={}:detail={detail}",
        identity.slug
    ))
}

fn asset_set_matches(asset_ids: &[String], identity: &FixedIdentity) -> bool {
    asset_ids.len() == 2
        && asset_ids.iter().collect::<BTreeSet<_>>()
            == BTreeSet::from([&identity.up_asset_id, &identity.down_asset_id])
}

fn winner_matches(identity: &FixedIdentity, asset_id: &str, outcome: &str) -> bool {
    if asset_id == identity.up_asset_id {
        outcome.trim().eq_ignore_ascii_case("up")
    } else if asset_id == identity.down_asset_id {
        outcome.trim().eq_ignore_ascii_case("down")
    } else {
        false
    }
}

fn read_finalized_metadata_for_validation(
    dataset_root: &Path,
    days: &BTreeSet<String>,
) -> Result<Vec<MetadataRecord>> {
    let mut records = Vec::new();
    for (file_order, path) in
        finalized_source_paths(dataset_root, days, "polymarket_market_metadata")?
            .into_iter()
            .enumerate()
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        let layout = metadata_layout(
            builder.metadata().file_metadata().key_value_metadata(),
            &path,
        )?;
        if !matches!(layout, MetadataLayout::V4 | MetadataLayout::V5) {
            continue;
        }
        require_canonical_metadata_schema(builder.schema().as_ref(), &path, layout)?;
        let mut reader = builder.with_batch_size(64).build()?;
        let mut file_row = 0_u64;
        for batch in &mut reader {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let record = MetadataRecord::from_batch(
                    &batch,
                    row,
                    FinalizedArtifactPosition {
                        path: path.clone(),
                        file_order,
                        file_row,
                    },
                    layout,
                )?;
                records.push(record);
                file_row = file_row.saturating_add(1);
            }
        }
    }
    Ok(records)
}

fn reject_dual_metadata_layout_for_current_lifecycles(
    dataset_root: &Path,
    days: &BTreeSet<String>,
    current_slugs: BTreeSet<&str>,
) -> Result<()> {
    if current_slugs.is_empty() {
        return Ok(());
    }
    let forbidden_layout = MetadataLayout::V3;
    for path in finalized_source_paths(dataset_root, days, "polymarket_market_metadata")? {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        let layout = metadata_layout(
            builder.metadata().file_metadata().key_value_metadata(),
            &path,
        )?;
        if layout != forbidden_layout {
            continue;
        }
        require_canonical_metadata_schema(builder.schema().as_ref(), &path, layout)?;
        let mut reader = builder.with_batch_size(64).build()?;
        for batch in &mut reader {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let slug = required_string(&batch, row, "slug")?;
                if current_slugs.contains(slug.as_str()) {
                    return Err(invalid_data(format!(
                        "Polymarket restart detected dual metadata layouts for slug={slug}:forbidden_layout={}",
                        layout.version()
                    ))
                    .into());
                }
            }
        }
    }
    Ok(())
}

fn read_precise_metadata_reference_authorities(
    dataset_root: &Path,
    default_days: &BTreeSet<String>,
    grouped: &BTreeMap<String, Vec<MetadataRecord>>,
) -> Result<Vec<MetadataRecord>> {
    let mut lookups_by_day = BTreeMap::<String, BTreeSet<MetadataReferenceLookup>>::new();
    for lookup in grouped
        .values()
        .flatten()
        .filter_map(MetadataReferenceLookup::from_local_reference)
    {
        let day = utc_day(lookup.source_received_at_utc_ms)?;
        if !default_days.contains(&day) {
            lookups_by_day.entry(day).or_default().insert(lookup);
        }
    }

    let mut records = Vec::new();
    for (day, lookups) in lookups_by_day {
        let days = BTreeSet::from([day]);
        for (file_order, path) in
            finalized_source_paths(dataset_root, &days, "polymarket_market_metadata")?
                .into_iter()
                .enumerate()
        {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
            let layout = metadata_layout(
                builder.metadata().file_metadata().key_value_metadata(),
                &path,
            )?;
            if !matches!(layout, MetadataLayout::V4 | MetadataLayout::V5) {
                continue;
            }
            require_canonical_metadata_schema(builder.schema().as_ref(), &path, layout)?;
            let mut reader = builder.with_batch_size(64).build()?;
            let mut file_row = 0_u64;
            for batch in &mut reader {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let slug = required_string(&batch, row, "slug")?;
                    let source_received_at_utc_ms =
                        optional_timestamp(&batch, row, "source_evidence_received_at_utc")?;
                    if !lookups.iter().any(|lookup| {
                        lookup.slug == slug
                            && source_received_at_utc_ms == Some(lookup.source_received_at_utc_ms)
                    }) {
                        file_row = file_row.saturating_add(1);
                        continue;
                    }
                    let record = MetadataRecord::from_batch(
                        &batch,
                        row,
                        FinalizedArtifactPosition {
                            path: path.clone(),
                            file_order,
                            file_row,
                        },
                        layout,
                    )?;
                    if lookups
                        .iter()
                        .any(|lookup| lookup.matches_source_authority(&record))
                    {
                        records.push(record);
                    }
                    file_row = file_row.saturating_add(1);
                }
            }
        }
    }
    Ok(records)
}

fn parquet_layout_version<'a>(
    metadata: Option<&'a Vec<KeyValue>>,
    path: &Path,
) -> Result<Option<&'a str>> {
    let mut values = metadata
        .into_iter()
        .flatten()
        .filter(|entry| entry.key == "parquet_raw_layout_version");
    let Some(entry) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid_data(format!(
            "Polymarket restart layout footer is duplicated:file={}",
            path.display()
        ))
        .into());
    }
    let version = entry.value.as_deref().ok_or_else(|| {
        invalid_data(format!(
            "Polymarket restart layout footer has no value:file={}",
            path.display()
        ))
    })?;
    Ok(Some(version))
}

fn metadata_layout(metadata: Option<&Vec<KeyValue>>, path: &Path) -> Result<MetadataLayout> {
    let version = parquet_layout_version(metadata, path)?.ok_or_else(|| {
        invalid_data(format!(
            "Polymarket restart metadata layout footer is missing:file={}",
            path.display()
        ))
    })?;
    match version {
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION => Ok(MetadataLayout::V2),
        POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION => Ok(MetadataLayout::V3),
        POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION => Ok(MetadataLayout::V4),
        POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION => Ok(MetadataLayout::V5),
        _ => Err(invalid_data(format!(
            "Polymarket restart metadata layout footer is unsupported:file={}:version={version}",
            path.display()
        ))
        .into()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceLayout {
    V2,
    V3,
}

fn source_layout(
    metadata: Option<&Vec<KeyValue>>,
    schema: &Schema,
    path: &Path,
) -> Result<SourceLayout> {
    match parquet_layout_version(metadata, path)? {
        Some(POLYMARKET_ORDERBOOK_V2_SCHEMA_VERSION) => Ok(SourceLayout::V2),
        Some(POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION) => Ok(SourceLayout::V3),
        Some(version) => Err(invalid_data(format!(
            "Polymarket restart source layout footer is unsupported:file={}:version={version}",
            path.display()
        ))
        .into()),
        None if schema.index_of("source_native_payload_json").is_ok() => Ok(SourceLayout::V2),
        None => Err(invalid_data(format!(
            "Polymarket restart source layout footer is missing:file={}",
            path.display()
        ))
        .into()),
    }
}

fn read_qualified_typed_market_sources(
    dataset_root: &Path,
    days: &BTreeSet<String>,
    identities: &BTreeMap<String, FixedIdentity>,
    recovery_time_ms: i64,
) -> Result<BTreeMap<String, Vec<QualifiedTypedSource>>> {
    let mut qualified = BTreeMap::<String, Vec<QualifiedTypedSource>>::new();
    let mut source_keys = BTreeSet::<(String, u64)>::new();
    let mut last_sequence_by_epoch = BTreeMap::<String, u64>::new();
    for path in finalized_source_paths(dataset_root, days, "polymarket_market")? {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        match source_layout(
            builder.metadata().file_metadata().key_value_metadata(),
            builder.schema().as_ref(),
            &path,
        )? {
            SourceLayout::V2 => {
                reject_relevant_legacy_v2_source(builder, &path, identities, recovery_time_ms)?;
            }
            SourceLayout::V3 => {
                let expected = polymarket_orderbook_v3_schema();
                if builder.schema().fields() != expected.fields() {
                    return Err(invalid_data(format!(
                        "Polymarket restart finalized v3 source schema mismatch:file={}",
                        path.display()
                    ))
                    .into());
                }
                let mut reader = builder.with_batch_size(64).build()?;
                for batch in &mut reader {
                    let batch = batch?;
                    for message in polymarket_orderbook_v3_messages_from_batch(&batch)? {
                        let key = (
                            message.envelope.source_sequence_epoch_id.clone(),
                            message.envelope.collector_admission_sequence,
                        );
                        if !source_keys.insert(key.clone()) {
                            return Err(invalid_data(format!(
                                "Polymarket restart duplicate v3 source key:epoch={}:sequence={}",
                                key.0, key.1
                            ))
                            .into());
                        }
                        let previous = last_sequence_by_epoch.entry(key.0.clone()).or_insert(0);
                        if key.1 <= *previous {
                            return Err(invalid_data(format!(
                                "Polymarket restart v3 source sequence regressed:epoch={}:previous={}:current={}",
                                key.0, *previous, key.1
                            ))
                            .into());
                        }
                        *previous = key.1;
                        let PolymarketOrderbookV3Event::MarketResolved(resolved) = &message.event
                        else {
                            continue;
                        };
                        if message.envelope.timestamp_received > recovery_time_ms {
                            return Err(invalid_data(format!(
                                "Polymarket restart v3 source receive exceeds recovery time:file={}:received={}:recovery={recovery_time_ms}",
                                path.display(),
                                message.envelope.timestamp_received
                            ))
                            .into());
                        }
                        let candidates = identities
                            .values()
                            .filter(|identity| {
                                message.envelope.timestamp_received >= identity.market_start_utc_ms
                                    && (message.envelope.market == identity.condition_id
                                        || message.envelope.slug.as_deref()
                                            == Some(identity.slug.as_str())
                                        || resolved.clob_market_id == identity.clob_market_id)
                            })
                            .collect::<Vec<_>>();
                        let [identity] = candidates.as_slice() else {
                            let detail = if candidates.is_empty() {
                                "no current metadata identity"
                            } else {
                                "multiple current metadata identities"
                            };
                            return Err(invalid_data(format!(
                                "Polymarket restart v3 market_resolved has {detail}:market={}:slug={:?}:clob_market_id={}",
                                message.envelope.market,
                                message.envelope.slug,
                                resolved.clob_market_id
                            ))
                            .into());
                        };
                        let source = validate_typed_market_source(&message, resolved, identity)?;
                        qualified
                            .entry(identity.slug.clone())
                            .or_default()
                            .push(source);
                    }
                }
            }
        }
    }
    Ok(qualified)
}

fn reject_relevant_legacy_v2_source(
    builder: ParquetRecordBatchReaderBuilder<File>,
    path: &Path,
    identities: &BTreeMap<String, FixedIdentity>,
    recovery_time_ms: i64,
) -> Result<()> {
    if identities.is_empty() {
        return Ok(());
    }
    require_columns(
        builder.schema().as_ref(),
        &LEGACY_V2_ROUTING_PROJECTION,
        path,
    )?;
    let projection =
        ProjectionMask::columns(builder.parquet_schema(), LEGACY_V2_ROUTING_PROJECTION);
    let mut reader = builder
        .with_projection(projection)
        .with_batch_size(64)
        .build()?;
    for batch in &mut reader {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            let timestamp_received = required_timestamp(&batch, row, "timestamp_received")?;
            if timestamp_received > recovery_time_ms
                || required_string(&batch, row, "event_type")? != "market_resolved"
            {
                continue;
            }
            let market = required_string(&batch, row, "market")?;
            let slug = optional_string(&batch, row, "slug")?;
            if identities.values().any(|identity| {
                timestamp_received >= identity.market_start_utc_ms
                    && (market == identity.condition_id
                        || slug.as_deref() == Some(identity.slug.as_str()))
            }) {
                return Err(invalid_data(format!(
                    "Polymarket restart source v2 cannot pair with current metadata:file={}:market={market}:slug={slug:?}",
                    path.display()
                ))
                .into());
            }
        }
    }
    Ok(())
}

fn validate_typed_market_source(
    message: &PolymarketOrderbookV3Message,
    resolved: &crate::polymarket_orderbook_v3::PolymarketMarketResolvedV3,
    identity: &FixedIdentity,
) -> Result<QualifiedTypedSource> {
    if message.envelope.market != identity.condition_id
        || message.envelope.slug.as_deref() != Some(identity.slug.as_str())
        || resolved.clob_market_id != identity.clob_market_id
        || !asset_set_matches(&resolved.asset_ids, identity)
        || !winner_matches(
            identity,
            &resolved.winning_asset_id,
            &resolved.winning_outcome,
        )
        || (!resolved.outcomes.is_empty()
            && (resolved.outcomes.len() != resolved.asset_ids.len()
                || resolved
                    .asset_ids
                    .iter()
                    .zip(&resolved.outcomes)
                    .any(|(asset, outcome)| {
                        (asset == &identity.up_asset_id && !same_outcome_label(outcome, "up"))
                            || (asset == &identity.down_asset_id
                                && !same_outcome_label(outcome, "down"))
                    })))
    {
        return Err(relevant_raw_error(identity, "typed v3 identity/winner contradiction").into());
    }
    Ok(QualifiedTypedSource {
        timestamp_received: message.envelope.timestamp_received,
        collector_admission_time_utc_ms: message.envelope.collector_admission_time_utc_ms,
        source_sequence_epoch_id: message.envelope.source_sequence_epoch_id.clone(),
        collector_admission_sequence: message.envelope.collector_admission_sequence,
        winning_asset_id: resolved.winning_asset_id.clone(),
        winning_outcome: resolved.winning_outcome.clone(),
    })
}

fn finalized_source_paths(
    dataset_root: &Path,
    days: &BTreeSet<String>,
    source_kind: &str,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for day in days {
        let source_dir = dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join(format!("source_kind={source_kind}"));
        let entries = match fs::read_dir(&source_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".parquet")
                && !name.ends_with("-running.parquet")
                && !name.ends_with(".running.parquet")
                && !name.ends_with("-digesting.parquet")
            {
                paths.push(entry.path());
            }
        }
    }
    paths.sort_by_key(|path| {
        (
            path.parent()
                .map(|parent| parent.to_string_lossy().into_owned())
                .unwrap_or_default(),
            finalized_parquet_order_key(path),
        )
    });
    Ok(paths)
}

fn finalized_parquet_order_key(path: &Path) -> (String, u32) {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    stem.rsplit_once("-part-").map_or_else(
        || (stem.to_owned(), 0),
        |(base, part)| (base.to_owned(), part.parse::<u32>().unwrap_or(u32::MAX)),
    )
}

fn recovery_days(recovery_time_ms: i64) -> Result<BTreeSet<String>> {
    Ok(BTreeSet::from([
        utc_day(recovery_time_ms.saturating_sub(UTC_DAY_MS))?,
        utc_day(recovery_time_ms)?,
    ]))
}

fn utc_day(utc_ms: i64) -> Result<String> {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(utc_ms.div_euclid(1_000)).map_err(|error| {
            invalid_data(format!(
                "Polymarket restart UTC day is out of range:{error}"
            ))
        })?;
    Ok(format!(
        "{:04}-{:02}-{:02}",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day()
    ))
}

fn require_columns(schema: &arrow_schema::Schema, columns: &[&str], path: &Path) -> Result<()> {
    if let Some(missing) = columns
        .iter()
        .find(|column| schema.index_of(column).is_err())
    {
        return Err(invalid_data(format!(
            "Polymarket restart v3 projection column missing:file={}:column={missing}",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn required_string(batch: &RecordBatch, row: usize, name: &str) -> Result<String> {
    let values = typed_column::<StringArray>(batch, name)?;
    if values.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket restart required string is null:column={name}:row={row}"
        ))
        .into());
    }
    Ok(values.value(row).to_owned())
}

fn optional_string(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<String>> {
    let values = typed_column::<StringArray>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row).to_owned()))
}

fn required_timestamp(batch: &RecordBatch, row: usize, name: &str) -> Result<i64> {
    let values = typed_column::<TimestampMillisecondArray>(batch, name)?;
    if values.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket restart required timestamp is null:column={name}:row={row}"
        ))
        .into());
    }
    Ok(values.value(row))
}

fn optional_timestamp(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<i64>> {
    let values = typed_column::<TimestampMillisecondArray>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row)))
}

fn required_u64(batch: &RecordBatch, row: usize, name: &str) -> Result<u64> {
    let values = typed_column::<UInt64Array>(batch, name)?;
    if values.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket restart required u64 is null:column={name}:row={row}"
        ))
        .into());
    }
    Ok(values.value(row))
}

fn optional_u64(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<u64>> {
    let values = typed_column::<UInt64Array>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row)))
}

fn optional_u8(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<u8>> {
    let values = typed_column::<UInt8Array>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row)))
}

fn optional_u32(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<u32>> {
    let values = typed_column::<UInt32Array>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row)))
}

#[cfg(test)]
fn optional_bool(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<bool>> {
    let values = typed_column::<arrow_array::BooleanArray>(batch, name)?;
    Ok(values.is_valid(row).then(|| values.value(row)))
}

fn typed_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch.schema().index_of(name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            invalid_data(format!("Polymarket restart column type mismatch:{name}")).into()
        })
}

fn json_scalar_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use serde::de::DeserializeOwned;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::polymarket_market_metadata_layout::{
        ActiveUnavailableReason, ClosedTimeCheckFailureReason, MarketLifecycleTransition,
        MarketRuntimeRoleTransition, PolymarketMetadataOrigin, PolymarketMetadataRowKind,
        ResolutionTransportErrorClass, ResponseFieldsUnavailableReason,
        SourceEndpointUnavailableReason, SourceEvidenceChannel, SourceEvidenceEventType,
        TerminalPredicateFailureReason, polymarket_market_metadata_v3_schema,
        polymarket_market_metadata_v4_schema, polymarket_market_metadata_v5_schema,
    };
    use crate::polymarket_orderbook_layout::{PolymarketOrderbookContext, PolymarketOrderbookRow};
    use crate::polymarket_orderbook_v3::{
        PolymarketMarketResolvedV3, PolymarketOrderbookV3Envelope,
    };
    use crate::writer::{
        PolymarketOrderbookV3WriteSequenceState, TypedParquetSegmentWriter,
        polymarket_market_metadata_v3_rows_to_batch, polymarket_market_metadata_v4_rows_to_batch,
        polymarket_market_metadata_v5_rows_to_batch,
    };

    const T: i64 = 1_783_392_300_000;
    const E: i64 = T + 300_000;
    const CONDITION: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    const CONDITION_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
    const CONDITION_3: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
    const CONDITION_4: &str = "0x0000000000000000000000000000000000000000000000000000000000000004";
    const CONDITION_5: &str = "0x0000000000000000000000000000000000000000000000000000000000000005";
    const CONDITION_6: &str = "0x0000000000000000000000000000000000000000000000000000000000000006";
    const SLUG: &str = "btc-updown-5m-1783392300";
    const MARKET_ID: &str = "clob-market-1";

    fn gamma_market_payload(
        slug: &str,
        condition_id: &str,
        clob_market_id: &str,
        market_start_utc_ms: i64,
        market_end_utc_ms: i64,
        terminal: bool,
    ) -> String {
        let start_date =
            polymarket_client_sdk_v2::types::DateTime::from_timestamp_millis(market_start_utc_ms)
                .unwrap()
                .to_rfc3339();
        let end_date =
            polymarket_client_sdk_v2::types::DateTime::from_timestamp_millis(market_end_utc_ms)
                .unwrap()
                .to_rfc3339();
        json!({
            "id": clob_market_id,
            "conditionId": condition_id,
            "slug": slug,
            "question": "Bitcoin Up or Down",
            "startDate": start_date,
            "endDate": end_date,
            "clobTokenIds": "[\"101\",\"102\"]",
            "outcomes": "[\"Up\",\"Down\"]",
            "outcomePrices": if terminal {
                "[\"1\",\"0\"]"
            } else {
                "[\"0.5\",\"0.5\"]"
            },
            "active": true,
            "closed": terminal,
            "archived": false,
            "enableOrderBook": true,
            "acceptingOrders": !terminal,
            "closedTime": terminal.then_some(end_date),
            "umaResolutionStatus": terminal.then_some("resolved")
        })
        .to_string()
    }

    fn discovery_payload() -> String {
        gamma_market_payload(SLUG, CONDITION, MARKET_ID, T, E, false)
    }

    fn fixture_slug(market_start_utc_ms: i64) -> String {
        format!("btc-updown-5m-{}", market_start_utc_ms.div_euclid(1_000))
    }

    fn ws_payload() -> String {
        json!({
            "source": "polymarket_market",
            "event_type": "market_resolved",
            "id": MARKET_ID,
            "market": CONDITION,
            "slug": SLUG,
            "asset_ids": ["101", "102"],
            "outcomes": ["Up", "Down"],
            "winning_asset_id": "101",
            "winning_outcome": "Up",
            "timestamp": E + 240_000
        })
        .to_string()
    }

    fn base_record(sequence: u64) -> MetadataRecord {
        let payload = discovery_payload();
        MetadataRecord {
            artifact_position: FinalizedArtifactPosition {
                path: PathBuf::from("<fixture>"),
                file_order: 0,
                file_row: 0,
            },
            kind: "metadata_snapshot".to_owned(),
            timestamp_received: T - 120_000,
            admission_sequence: sequence,
            admission_time_utc_ms: T - 119_990,
            metadata_origin: "gamma_markets_discovery".to_owned(),
            slug: SLUG.to_owned(),
            condition_id: CONDITION.to_owned(),
            clob_market_id: MARKET_ID.to_owned(),
            asset_ids_json: r#"["101","102"]"#.to_owned(),
            market_start_utc_ms: T,
            market_end_utc_ms: E,
            fixed_identity_verified_at_utc_ms: T - 119_995,
            lifecycle_state: "active".to_owned(),
            runtime_role: "current".to_owned(),
            source_origin: Some("gamma_markets_discovery".to_owned()),
            source_received_at_utc_ms: Some(T - 120_000),
            source_payload_json: Some(payload.clone()),
            source_raw_sha256: Some(sha256_hex(payload.as_bytes())),
            lifecycle_transition: None,
            lifecycle_applied_at_utc_ms: None,
            runtime_role_transition: None,
            runtime_role_applied_at_utc_ms: None,
            winning_asset_id: None,
            winning_outcome: None,
            resolution_evidence_source: None,
            resolution_evidence_raw_sha256: None,
            terminal_claim_owner: None,
            attempt_ordinal: None,
            attempt_scheduled_utc_ms: None,
            attempt_result: None,
            integrity_error_class: None,
            budget_horizon_utc_ms: None,
            followup_status: None,
            attempts_recorded: None,
            valid_response_count: None,
            transport_unavailable_count: None,
            missed_slot_count: None,
            last_valid_metadata_received_at_utc_ms: None,
            terminal_predicate_passed: None,
            ws_claimant_source_sequence_epoch_id: None,
            ws_claimant_collector_admission_sequence: None,
        }
    }

    fn discovery_authority_record(
        sequence: u64,
        slug: &str,
        condition_id: &str,
        clob_market_id: &str,
        market_start_utc_ms: i64,
        market_end_utc_ms: i64,
        source_received_at_utc_ms: i64,
    ) -> MetadataRecord {
        let mut record = base_record(sequence);
        let payload = gamma_market_payload(
            slug,
            condition_id,
            clob_market_id,
            market_start_utc_ms,
            market_end_utc_ms,
            false,
        );
        record.timestamp_received = source_received_at_utc_ms;
        record.admission_time_utc_ms = source_received_at_utc_ms + 10;
        record.slug = slug.to_owned();
        record.condition_id = condition_id.to_owned();
        record.clob_market_id = clob_market_id.to_owned();
        record.market_start_utc_ms = market_start_utc_ms;
        record.market_end_utc_ms = market_end_utc_ms;
        record.fixed_identity_verified_at_utc_ms = source_received_at_utc_ms + 5;
        record.lifecycle_state = "prefetched".to_owned();
        record.runtime_role = "next".to_owned();
        record.source_received_at_utc_ms = Some(source_received_at_utc_ms);
        record.source_payload_json = Some(payload.clone());
        record.source_raw_sha256 = Some(sha256_hex(payload.as_bytes()));
        record
    }

    fn next_to_current_transition(
        authority: &MetadataRecord,
        sequence: u64,
        applied_at_utc_ms: i64,
    ) -> MetadataRecord {
        let mut transition = authority.clone();
        transition.kind = "market_state_transition".to_owned();
        transition.timestamp_received = applied_at_utc_ms;
        transition.admission_sequence = sequence;
        transition.admission_time_utc_ms = applied_at_utc_ms + 10;
        transition.metadata_origin = "local_market_state_timer".to_owned();
        transition.lifecycle_state = "active".to_owned();
        transition.runtime_role = "current".to_owned();
        transition.source_payload_json = None;
        transition.lifecycle_transition = Some("prefetched_to_active".to_owned());
        transition.lifecycle_applied_at_utc_ms = Some(applied_at_utc_ms);
        transition.runtime_role_transition = Some("next_to_current".to_owned());
        transition.runtime_role_applied_at_utc_ms = Some(applied_at_utc_ms);
        transition
    }

    fn attempt(sequence: u64, ordinal: u8, result: &str) -> MetadataRecord {
        let mut record = base_record(sequence);
        let scheduled = record.fixed_identity().unwrap().slot_utc_ms(ordinal);
        record.kind = "resolution_poll_attempt".to_owned();
        record.timestamp_received = scheduled + 10;
        record.admission_time_utc_ms = scheduled + 20;
        record.metadata_origin = if result == "missed_due_to_downtime" {
            "local_followup_scheduler".to_owned()
        } else {
            "gamma_by_slug".to_owned()
        };
        record.lifecycle_state = "active".to_owned();
        record.runtime_role = "resolution_pending".to_owned();
        record.source_origin = None;
        record.source_received_at_utc_ms = None;
        record.source_payload_json = None;
        record.source_raw_sha256 = None;
        record.attempt_ordinal = Some(ordinal);
        record.attempt_scheduled_utc_ms = Some(scheduled);
        record.attempt_result = Some(result.to_owned());
        record.budget_horizon_utc_ms = Some(E + BUDGET_HORIZON_MS);
        record.followup_status = Some("pending".to_owned());
        if matches!(
            result,
            "valid_nonterminal" | "terminal" | "post_terminal_claim_observation"
        ) {
            let payload =
                gamma_market_payload(SLUG, CONDITION, MARKET_ID, T, E, result == "terminal");
            record.source_origin = Some("gamma_by_slug".to_owned());
            record.source_received_at_utc_ms = Some(record.timestamp_received);
            record.source_payload_json = Some(payload.clone());
            record.source_raw_sha256 = Some(sha256_hex(payload.as_bytes()));
        }
        record
    }

    fn gamma_terminal(sequence: u64, ordinal: u8) -> MetadataRecord {
        let mut record = attempt(sequence, ordinal, "terminal");
        record.lifecycle_state = "retired".to_owned();
        record.runtime_role = "none".to_owned();
        record.lifecycle_transition = Some("active_to_retired".to_owned());
        record.lifecycle_applied_at_utc_ms = Some(record.timestamp_received + 1);
        record.runtime_role_transition = Some("resolution_pending_to_none".to_owned());
        record.runtime_role_applied_at_utc_ms = Some(record.timestamp_received + 1);
        record.winning_asset_id = Some("101".to_owned());
        record.winning_outcome = Some("Up".to_owned());
        record.resolution_evidence_source = Some("gamma_by_slug".to_owned());
        record.resolution_evidence_raw_sha256 = record.source_raw_sha256.clone();
        record.terminal_claim_owner = Some("gamma_by_slug".to_owned());
        record.followup_status = Some("terminal_gamma_fallback".to_owned());
        record.terminal_predicate_passed = Some(true);
        record
    }

    fn ws_terminal(sequence: u64, receive: i64) -> MetadataRecord {
        let mut record = base_record(sequence);
        let payload = ws_payload();
        record.kind = "resolution_terminal_ws".to_owned();
        record.timestamp_received = receive;
        record.admission_time_utc_ms = receive + 20;
        record.metadata_origin = "market_resolved_ws".to_owned();
        record.lifecycle_state = "retired".to_owned();
        record.runtime_role = "none".to_owned();
        record.source_origin = Some("market_resolved_ws".to_owned());
        record.source_received_at_utc_ms = Some(receive);
        record.source_payload_json = Some(payload.clone());
        record.source_raw_sha256 = Some(sha256_hex(payload.as_bytes()));
        record.lifecycle_transition = Some("active_to_retired".to_owned());
        record.lifecycle_applied_at_utc_ms = Some(receive + 10);
        record.runtime_role_transition = Some("resolution_pending_to_none".to_owned());
        record.runtime_role_applied_at_utc_ms = Some(receive + 10);
        record.winning_asset_id = Some("101".to_owned());
        record.winning_outcome = Some("Up".to_owned());
        record.resolution_evidence_source = Some("market_resolved_ws".to_owned());
        record.resolution_evidence_raw_sha256 = record.source_raw_sha256.clone();
        record.terminal_claim_owner = Some("market_resolved_ws".to_owned());
        record.budget_horizon_utc_ms = Some(E + BUDGET_HORIZON_MS);
        record.followup_status = Some("terminal_ws".to_owned());
        record.terminal_predicate_passed = Some(true);
        record
    }

    fn post_observation(sequence: u64, ordinal: u8) -> MetadataRecord {
        let mut record = attempt(sequence, ordinal, "post_terminal_claim_observation");
        record.terminal_claim_owner = Some("market_resolved_ws".to_owned());
        record.terminal_predicate_passed = Some(false);
        record
    }

    fn summary(sequence: u64, attempts: &[MetadataRecord]) -> MetadataRecord {
        let valid = attempts
            .iter()
            .filter(|record| record.attempt_result.as_deref() == Some("valid_nonterminal"))
            .count() as u32;
        let transport = attempts
            .iter()
            .filter(|record| record.attempt_result.as_deref() == Some("transport_unavailable"))
            .count() as u32;
        let missed = attempts
            .iter()
            .filter(|record| record.attempt_result.as_deref() == Some("missed_due_to_downtime"))
            .count() as u32;
        let last_valid = attempts
            .iter()
            .filter(|record| record.attempt_result.as_deref() == Some("valid_nonterminal"))
            .filter_map(|record| record.source_received_at_utc_ms)
            .max();
        let mut record = base_record(sequence);
        let role_exit = E + BUDGET_HORIZON_MS + 10;
        record.kind = "resolution_followup_summary".to_owned();
        record.timestamp_received = role_exit + 1;
        record.admission_time_utc_ms = role_exit + 2;
        record.metadata_origin = "local_followup_scheduler".to_owned();
        record.lifecycle_state = "active".to_owned();
        record.runtime_role = "none".to_owned();
        record.source_origin = None;
        record.source_received_at_utc_ms = None;
        record.source_payload_json = None;
        record.source_raw_sha256 = None;
        record.runtime_role_transition = Some("resolution_pending_to_none".to_owned());
        record.runtime_role_applied_at_utc_ms = Some(role_exit);
        record.budget_horizon_utc_ms = Some(E + BUDGET_HORIZON_MS);
        record.followup_status = Some(if valid > 0 {
            "budget_exhausted_after_valid_nonterminal".to_owned()
        } else {
            "budget_exhausted_without_valid_response".to_owned()
        });
        record.attempts_recorded = Some(attempts.len() as u32);
        record.valid_response_count = Some(valid);
        record.transport_unavailable_count = Some(transport);
        record.missed_slot_count = Some(missed);
        record.last_valid_metadata_received_at_utc_ms = last_valid;
        record
    }

    fn day_dir(root: &Path, recovery_time: i64, source: &str) -> PathBuf {
        root.join("training")
            .join("typed_raw")
            .join(format!("utc_day={}", utc_day(recovery_time).unwrap()))
            .join(format!("source_kind={source}"))
    }

    fn metadata_writer_properties(layout_version: &str) -> WriterProperties {
        WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "parquet_raw_layout_version".to_owned(),
                layout_version.to_owned(),
            )]))
            .build()
    }

    fn fixture_enum<T: DeserializeOwned>(value: &str) -> T {
        serde_json::from_value(Value::String(value.to_owned())).unwrap()
    }

    fn fixture_optional_enum<T: DeserializeOwned>(value: Option<&str>) -> Option<T> {
        value.map(fixture_enum)
    }

    fn metadata_fixture_row(record: &MetadataRecord) -> PolymarketMarketMetadataV3Row {
        let metadata_row_kind = fixture_enum(&record.kind);
        let metadata_origin = fixture_enum(&record.metadata_origin);
        let source_evidence_origin = fixture_optional_enum(record.source_origin.as_deref());
        let source_endpoint = match metadata_origin {
            PolymarketMetadataOrigin::GammaMarketsDiscovery
            | PolymarketMetadataOrigin::LocalMarketStateTimer
                if source_evidence_origin == Some(SourceEvidenceOrigin::GammaMarketsDiscovery) =>
            {
                Some("https://gamma-api.polymarket.com/markets".to_owned())
            }
            PolymarketMetadataOrigin::GammaBySlug => Some(format!(
                "https://gamma-api.polymarket.com/markets/slug/{}",
                record.slug
            )),
            PolymarketMetadataOrigin::MarketResolvedWs => {
                Some("wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned())
            }
            _ => None,
        };
        let source_evidence_event_type = match source_evidence_origin {
            Some(SourceEvidenceOrigin::GammaMarketsDiscovery) => {
                Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse)
            }
            Some(SourceEvidenceOrigin::GammaBySlug) => {
                Some(SourceEvidenceEventType::GammaMarketBySlugResponse)
            }
            Some(SourceEvidenceOrigin::MarketResolvedWs) => {
                Some(SourceEvidenceEventType::MarketResolved)
            }
            None => None,
        };
        let source_evidence_channel = (source_evidence_origin
            == Some(SourceEvidenceOrigin::MarketResolvedWs))
        .then_some(SourceEvidenceChannel::Market);
        let lifecycle_transition = fixture_optional_enum(record.lifecycle_transition.as_deref());
        let runtime_role_transition =
            fixture_optional_enum(record.runtime_role_transition.as_deref());
        let lifecycle_scheduled_at_utc = match lifecycle_transition {
            Some(
                MarketLifecycleTransition::PrefetchedToActive
                | MarketLifecycleTransition::CandidateToActive,
            ) => Some(record.market_start_utc_ms),
            Some(MarketLifecycleTransition::ActiveToRetired) | None => None,
        };
        let runtime_role_scheduled_at_utc = match runtime_role_transition {
            Some(
                MarketRuntimeRoleTransition::NoneToCurrent
                | MarketRuntimeRoleTransition::NextToCurrent,
            ) => Some(record.market_start_utc_ms),
            Some(MarketRuntimeRoleTransition::CurrentToResolutionPending) => {
                Some(record.market_end_utc_ms)
            }
            Some(MarketRuntimeRoleTransition::ResolutionPendingToNone) | None => None,
        };
        let metadata_fetch_start_utc = match metadata_origin {
            PolymarketMetadataOrigin::GammaMarketsDiscovery => record
                .source_received_at_utc_ms
                .map(|received| received.saturating_sub(1)),
            PolymarketMetadataOrigin::LocalMarketStateTimer
                if record.source_payload_json.is_some() =>
            {
                record
                    .source_received_at_utc_ms
                    .map(|received| received.saturating_sub(1))
            }
            PolymarketMetadataOrigin::GammaBySlug => record.attempt_scheduled_utc_ms,
            _ => None,
        };
        let row_evidence_validation_completed_at_utc =
            record.source_payload_json.as_ref().map(|_| {
                if source_evidence_origin == Some(SourceEvidenceOrigin::GammaMarketsDiscovery) {
                    record.fixed_identity_verified_at_utc_ms
                } else {
                    record.timestamp_received.saturating_add(1)
                }
            });
        let source_endpoint_unavailable_reason = source_endpoint
            .is_none()
            .then_some(SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
        let mut row = PolymarketMarketMetadataV3Row {
            metadata_row_kind,
            timestamp_received: record.timestamp_received,
            collector_admission_sequence: record.admission_sequence,
            collector_admission_time_utc_ms: record.admission_time_utc_ms,
            metadata_origin,
            collector_host_id: "recovery-fixture-host".to_owned(),
            collector_region: "recovery-fixture-region".to_owned(),
            schema_version: POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION.to_owned(),
            slug: record.slug.clone(),
            condition_id: record.condition_id.clone(),
            clob_market_id: record.clob_market_id.clone(),
            asset_ids: record.asset_ids_json.clone(),
            market_start_utc: record.market_start_utc_ms,
            market_end_utc: record.market_end_utc_ms,
            fixed_identity_verified_at_utc: record.fixed_identity_verified_at_utc_ms,
            market_lifecycle_state: fixture_enum(&record.lifecycle_state),
            market_runtime_role: fixture_enum(&record.runtime_role),
            source_endpoint,
            source_endpoint_unavailable_reason,
            source_evidence_origin,
            source_evidence_event_type,
            source_evidence_channel,
            source_evidence_received_at_utc: record.source_received_at_utc_ms,
            row_evidence_validation_completed_at_utc,
            source_evidence_payload_json: record.source_payload_json.clone(),
            source_evidence_raw_sha256: record.source_raw_sha256.clone(),
            market_lifecycle_transition: lifecycle_transition,
            lifecycle_scheduled_at_utc,
            lifecycle_applied_at_utc: record.lifecycle_applied_at_utc_ms,
            market_runtime_role_transition: runtime_role_transition,
            runtime_role_scheduled_at_utc,
            runtime_role_applied_at_utc: record.runtime_role_applied_at_utc_ms,
            metadata_fetch_start_utc,
            closed: None,
            closed_time_raw: None,
            closed_time_utc: None,
            closed_time_check_passed: None,
            closed_time_check_failure_reason: None,
            automatically_resolved: None,
            uma_resolution_status: None,
            outcomes_raw: None,
            outcome_prices_raw: None,
            active: None,
            active_unavailable_reason: Some(ActiveUnavailableReason::NotApplicableForRowKind),
            accepting_orders: None,
            ws_platform_timestamp_raw: None,
            ws_platform_timestamp_utc: None,
            winning_asset_id: record.winning_asset_id.clone(),
            winning_outcome: record.winning_outcome.clone(),
            resolution_evidence_source: fixture_optional_enum(
                record.resolution_evidence_source.as_deref(),
            ),
            resolution_evidence_raw_sha256: record.resolution_evidence_raw_sha256.clone(),
            terminal_claim_owner: fixture_optional_enum(record.terminal_claim_owner.as_deref()),
            resolution_poll_attempt_ordinal: record.attempt_ordinal,
            resolution_poll_scheduled_utc: record.attempt_scheduled_utc_ms,
            resolution_poll_result: fixture_optional_enum(record.attempt_result.as_deref()),
            resolution_transport_error_class: None,
            resolution_transport_http_status: None,
            resolution_integrity_error_class: fixture_optional_enum(
                record.integrity_error_class.as_deref(),
            ),
            resolution_budget_horizon_utc: record.budget_horizon_utc_ms,
            resolution_followup_status: fixture_optional_enum(record.followup_status.as_deref()),
            resolution_poll_attempts_recorded: record.attempts_recorded,
            resolution_valid_response_count: record.valid_response_count,
            resolution_transport_unavailable_count: record.transport_unavailable_count,
            resolution_missed_slot_count: record.missed_slot_count,
            last_valid_metadata_received_utc: record.last_valid_metadata_received_at_utc_ms,
            terminal_predicate_passed: record.terminal_predicate_passed,
            terminal_predicate_failure_reason: None,
            response_fields_unavailable_reason: Some(
                ResponseFieldsUnavailableReason::NotApplicableForRowKind,
            ),
        };

        if let Some(payload) = row.source_evidence_payload_json.as_deref()
            && matches!(
                row.source_evidence_origin,
                Some(
                    SourceEvidenceOrigin::GammaMarketsDiscovery | SourceEvidenceOrigin::GammaBySlug
                )
            )
        {
            let market = serde_json::from_str::<SdkGammaMarket>(payload).unwrap_or_else(|_| {
                serde_json::from_str::<SdkGammaMarket>(&gamma_market_payload(
                    SLUG,
                    CONDITION,
                    MARKET_ID,
                    T,
                    E,
                    record.attempt_result.as_deref() == Some("terminal"),
                ))
                .unwrap()
            });
            row.closed = market.closed;
            row.closed_time_raw = market.closed_time.clone();
            row.automatically_resolved = market.automatically_resolved;
            row.uma_resolution_status = market.uma_resolution_status.clone();
            row.outcomes_raw = market
                .outcomes
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .unwrap();
            row.outcome_prices_raw = market
                .outcome_prices
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .unwrap();
            row.active = market.active;
            row.active_unavailable_reason = market
                .active
                .is_none()
                .then_some(ActiveUnavailableReason::SourceFieldAbsent);
            row.accepting_orders = market.accepting_orders;
            row.response_fields_unavailable_reason = None;

            if row.source_evidence_origin == Some(SourceEvidenceOrigin::GammaBySlug) {
                let assets = serde_json::from_str::<Vec<String>>(&row.asset_ids).unwrap();
                let evaluation = evaluate_gamma_terminal_response(
                    &GammaTerminalFixedIdentity {
                        slug: row.slug.clone(),
                        clob_market_id: row.clob_market_id.clone(),
                        condition_id: row.condition_id.clone(),
                        up_asset_id: assets[0].clone(),
                        down_asset_id: assets[1].clone(),
                        market_end_utc_ms: row.market_end_utc,
                    },
                    GammaTerminalAttemptContext {
                        scheduled_at_utc_ms: row.resolution_poll_scheduled_utc.unwrap(),
                        fetch_start_utc_ms: row.metadata_fetch_start_utc.unwrap(),
                        received_at_utc_ms: row.timestamp_received,
                        response_bound_to_attempt: true,
                    },
                    GammaTerminalResponse::Decoded(&market),
                );
                row.closed_time_raw = evaluation.closed_time_raw.clone();
                row.closed_time_utc = evaluation.closed_time_utc_ms;
                match evaluation.terminal_predicate_failure_reason {
                    Some(TerminalPredicateFailureReason::ClosedTimeMissing) => {
                        row.closed_time_check_passed = Some(false);
                        row.closed_time_check_failure_reason =
                            Some(ClosedTimeCheckFailureReason::Missing);
                    }
                    Some(TerminalPredicateFailureReason::ClosedTimeParseError) => {
                        row.closed_time_check_passed = Some(false);
                        row.closed_time_check_failure_reason =
                            Some(ClosedTimeCheckFailureReason::ParseError);
                    }
                    Some(TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd) => {
                        row.closed_time_check_passed = Some(false);
                        row.closed_time_check_failure_reason =
                            Some(ClosedTimeCheckFailureReason::BeforeMarketEnd);
                    }
                    _ if evaluation.closed_time_utc_ms.is_some() => {
                        row.closed_time_check_passed = Some(true);
                    }
                    _ => {}
                }
                row.resolution_integrity_error_class = evaluation.resolution_integrity_error_class;
                row.terminal_predicate_passed = evaluation.terminal_predicate_passed;
                row.terminal_predicate_failure_reason =
                    evaluation.terminal_predicate_failure_reason;
            }
        }

        match row.resolution_poll_result {
            Some(ResolutionPollResult::TransportUnavailable) => {
                row.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
                row.resolution_transport_error_class = Some(ResolutionTransportErrorClass::Timeout);
                row.response_fields_unavailable_reason =
                    Some(ResponseFieldsUnavailableReason::TransportUnavailable);
            }
            Some(ResolutionPollResult::MissedDueToDowntime) => {
                row.response_fields_unavailable_reason =
                    Some(ResponseFieldsUnavailableReason::MissedDueToDowntime);
            }
            Some(ResolutionPollResult::IntegrityError)
                if row.source_evidence_payload_json.is_none() =>
            {
                row.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
                row.response_fields_unavailable_reason =
                    Some(ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError);
            }
            _ => {}
        }
        if row.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs {
            row.active_unavailable_reason = Some(ActiveUnavailableReason::NotApplicableForRowKind);
            row.response_fields_unavailable_reason = None;
            row.ws_platform_timestamp_utc = row
                .source_evidence_payload_json
                .as_deref()
                .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
                .and_then(|payload| payload.get("timestamp").and_then(Value::as_i64));
            row.ws_platform_timestamp_raw = row
                .ws_platform_timestamp_utc
                .map(|timestamp| timestamp.to_string());
        }
        row
    }

    fn write_metadata_v3_rows(path: &Path, rows: &[PolymarketMarketMetadataV3Row]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = polymarket_market_metadata_v3_schema();
        let batch = polymarket_market_metadata_v3_rows_to_batch(Arc::clone(&schema), rows).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema,
            Some(metadata_writer_properties(
                POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            )),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn metadata_v4_fixture_row(
        record: &MetadataRecord,
        claimant: Option<(&str, u64)>,
    ) -> PolymarketMarketMetadataV4Row {
        metadata_v4_from_v3_fixture(metadata_fixture_row(record), claimant)
    }

    fn metadata_v4_from_v3_fixture(
        row: PolymarketMarketMetadataV3Row,
        claimant: Option<(&str, u64)>,
    ) -> PolymarketMarketMetadataV4Row {
        let (claimant_epoch, claimant_sequence) = claimant
            .map(|(epoch, sequence)| (Some(epoch.to_owned()), Some(sequence)))
            .unwrap_or((None, None));
        PolymarketMarketMetadataV4Row::from_v3_prefix(row, claimant_epoch, claimant_sequence)
    }

    fn write_metadata_v4_rows(path: &Path, rows: &[PolymarketMarketMetadataV4Row]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = polymarket_market_metadata_v4_schema();
        let batch = polymarket_market_metadata_v4_rows_to_batch(Arc::clone(&schema), rows).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema,
            Some(metadata_writer_properties(
                POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
            )),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn metadata_v5_fixture_row(
        record: &MetadataRecord,
        claimant: Option<(&str, u64)>,
    ) -> PolymarketMarketMetadataV5Row {
        metadata_v5_from_v3_fixture(metadata_fixture_row(record), claimant)
    }

    fn metadata_v5_from_v3_fixture(
        row: PolymarketMarketMetadataV3Row,
        claimant: Option<(&str, u64)>,
    ) -> PolymarketMarketMetadataV5Row {
        let (claimant_epoch, claimant_sequence) = claimant
            .map(|(epoch, sequence)| (Some(epoch.to_owned()), Some(sequence)))
            .unwrap_or((None, None));
        PolymarketMarketMetadataV5Row::from_v3_prefix(row, claimant_epoch, claimant_sequence)
    }

    fn write_metadata_v5_rows(path: &Path, rows: &[PolymarketMarketMetadataV5Row]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = polymarket_market_metadata_v5_schema();
        let batch = polymarket_market_metadata_v5_rows_to_batch(Arc::clone(&schema), rows).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema,
            Some(metadata_writer_properties(
                POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
            )),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn typed_market_resolved_source(
        source_sequence_epoch_id: &str,
        collector_admission_sequence: u64,
        timestamp_received: i64,
        collector_admission_time_utc_ms: i64,
        winning_asset_id: &str,
        winning_outcome: &str,
    ) -> PolymarketOrderbookV3Message {
        PolymarketOrderbookV3Message {
            envelope: PolymarketOrderbookV3Envelope {
                timestamp_received,
                timestamp: Some(timestamp_received),
                market: CONDITION.to_owned(),
                slug: Some(SLUG.to_owned()),
                collector_host_id: "recovery-fixture-host".to_owned(),
                collector_region: "recovery-fixture-region".to_owned(),
                source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
                schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
                source_sequence_epoch_id: source_sequence_epoch_id.to_owned(),
                collector_admission_sequence,
                collector_admission_time_utc_ms,
            },
            event: PolymarketOrderbookV3Event::MarketResolved(PolymarketMarketResolvedV3 {
                clob_market_id: MARKET_ID.to_owned(),
                question: None,
                description: None,
                asset_ids: vec!["101".to_owned(), "102".to_owned()],
                outcomes: vec!["Up".to_owned(), "Down".to_owned()],
                event_message: None,
                winning_asset_id: winning_asset_id.to_owned(),
                winning_outcome: winning_outcome.to_owned(),
            }),
        }
    }

    fn write_typed_v3_sources(path: &Path, sources: &[PolymarketOrderbookV3Message]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            path.parent().unwrap(),
            stem,
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            "polymarket_market",
        )
        .unwrap();
        let mut sequence_state = PolymarketOrderbookV3WriteSequenceState::default();
        writer
            .write_polymarket_orderbook_v3_messages(sources, &mut sequence_state)
            .unwrap();
        let published = writer.finalize().unwrap();
        assert_eq!(published.final_path, path);
    }

    fn write_current_fixture(
        root: &Path,
        recovery_time: i64,
        metadata: &[PolymarketMarketMetadataV5Row],
        sources: &[PolymarketOrderbookV3Message],
    ) {
        if !metadata.is_empty() {
            write_metadata_v5_rows(
                &day_dir(root, recovery_time, "polymarket_market_metadata")
                    .join("polymarket_market_metadata_02.parquet"),
                metadata,
            );
        }
        if !sources.is_empty() {
            write_typed_v3_sources(
                &day_dir(root, recovery_time, "polymarket_market")
                    .join("polymarket_market_02.parquet"),
                sources,
            );
        }
    }

    fn write_metadata_v4_rows_without_field(
        path: &Path,
        rows: &[PolymarketMarketMetadataV4Row],
        omitted_field: &str,
    ) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let canonical_schema = polymarket_market_metadata_v4_schema();
        let canonical_batch =
            polymarket_market_metadata_v4_rows_to_batch(Arc::clone(&canonical_schema), rows)
                .unwrap();
        let omitted_index = canonical_schema.index_of(omitted_field).unwrap();
        let fields = canonical_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != omitted_index)
            .map(|(_, field)| Arc::clone(field))
            .collect::<Vec<_>>();
        let columns = canonical_batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != omitted_index)
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            canonical_schema.metadata().clone(),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema,
            Some(metadata_writer_properties(
                POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
            )),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_metadata(path: &Path, records: &[MetadataRecord]) {
        let rows = records.iter().map(metadata_fixture_row).collect::<Vec<_>>();
        write_metadata_v3_rows(path, &rows);
    }

    fn write_metadata_v4_records(path: &Path, records: &[MetadataRecord]) {
        let rows = records
            .iter()
            .map(|record| {
                let claimant = (record.kind == "resolution_terminal_ws")
                    .then_some(("fixture-source-epoch", 1));
                metadata_v4_fixture_row(record, claimant)
            })
            .collect::<Vec<_>>();
        write_metadata_v4_rows(path, &rows);
    }

    fn write_metadata_v5_records(path: &Path, records: &[MetadataRecord]) {
        let rows = records
            .iter()
            .map(|record| {
                let claimant = (record.kind == "resolution_terminal_ws")
                    .then_some(("fixture-source-epoch", 1));
                metadata_v5_fixture_row(record, claimant)
            })
            .collect::<Vec<_>>();
        write_metadata_v5_rows(path, &rows);
    }

    fn write_relevant_legacy_v2_source(path: &Path, receive: i64) {
        let row = PolymarketOrderbookRow::market_resolved(
            CONDITION,
            receive,
            "101",
            "Up",
            None,
            &["101".to_owned(), "102".to_owned()],
            PolymarketOrderbookContext {
                timestamp_received_ms: receive,
                slug: Some(SLUG.to_owned()),
                source_native_payload_json: ws_payload(),
                collector_host_id: "recovery-fixture-host".to_owned(),
                collector_region: "recovery-fixture-region".to_owned(),
                source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
                schema_version: POLYMARKET_ORDERBOOK_V2_SCHEMA_VERSION.to_owned(),
            },
        );
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let mut writer = TypedParquetSegmentWriter::create_polymarket_orderbook(
            path.parent().unwrap(),
            stem,
            POLYMARKET_ORDERBOOK_V2_SCHEMA_VERSION,
        )
        .unwrap();
        writer.write_polymarket_orderbook_rows(&[row]).unwrap();
        assert_eq!(writer.finalize().unwrap().final_path, path);
    }

    fn write_metadata_without_row_kind(path: &Path, layout_version: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_version", DataType::Utf8, false),
            Field::new("slug", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![layout_version])),
                Arc::new(StringArray::from(vec![SLUG])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(path).unwrap(),
            schema,
            Some(metadata_writer_properties(layout_version)),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_v2(path: &Path) {
        write_metadata_without_row_kind(path, POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION);
    }

    fn write_finalized_fixture(root: &Path, recovery_time: i64, metadata: &[MetadataRecord]) {
        let rows = metadata
            .iter()
            .map(|record| {
                let claimant = (record.kind == "resolution_terminal_ws")
                    .then_some(("fixture-source-epoch", 1));
                metadata_v5_fixture_row(record, claimant)
            })
            .collect::<Vec<_>>();
        write_metadata_v5_rows(
            &day_dir(root, recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &rows,
        );
    }

    #[test]
    fn legacy_v3_exact_row_decode_and_validation_remain_available() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("metadata-v3.parquet");
        write_metadata_v3_rows(&path, &[metadata_fixture_row(&base_record(1))]);

        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let layout = metadata_layout(
            builder.metadata().file_metadata().key_value_metadata(),
            &path,
        )
        .unwrap();
        assert_eq!(layout, MetadataLayout::V3);
        require_canonical_metadata_schema(builder.schema().as_ref(), &path, layout).unwrap();
        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let record = MetadataRecord::from_batch(
            &batch,
            0,
            FinalizedArtifactPosition {
                path,
                file_order: 0,
                file_row: 0,
            },
            layout,
        )
        .unwrap();
        assert_eq!(record.slug, SLUG);
        assert!(record.source_raw_sha256.is_some());
    }

    #[test]
    fn legacy_v4_pending_fixture_still_recovers() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + 4 * 60_000;
        write_metadata_v4_records(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[base_record(1), attempt(2, 1, "valid_nonterminal")],
        );

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert_eq!(recovery.pending.len(), 1);
        assert_eq!(recovery.pending[0].finalized_attempt_ordinals, [1]);
    }

    #[test]
    fn legacy_v4_ws_claimant_terminal_still_recovers() {
        let tmp = tempdir().unwrap();
        let epoch = "source-epoch-legacy-v4";
        let receive = E + 240_000;
        let recovery_time = receive + 30;
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[
                metadata_v4_fixture_row(&base_record(1), None),
                metadata_v4_fixture_row(&ws_terminal(2, receive), Some((epoch, 1))),
            ],
        );
        write_typed_v3_sources(
            &day_dir(tmp.path(), recovery_time, "polymarket_market")
                .join("polymarket_market_02.parquet"),
            &[typed_market_resolved_source(
                epoch,
                1,
                receive,
                receive + 1,
                "101",
                "Up",
            )],
        );

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert!(recovery.pending.is_empty());
    }

    #[test]
    fn coherent_v4_and_v5_files_can_share_one_lifecycle() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + 4 * 60_000;
        let metadata_dir = day_dir(tmp.path(), recovery_time, "polymarket_market_metadata");
        write_metadata_v4_records(
            &metadata_dir.join("polymarket_market_metadata_02.parquet"),
            &[base_record(1)],
        );
        write_metadata_v5_records(
            &metadata_dir.join("polymarket_market_metadata_03.parquet"),
            &[attempt(2, 1, "valid_nonterminal")],
        );

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert_eq!(recovery.pending.len(), 1);
        assert_eq!(recovery.pending[0].finalized_attempt_ordinals, [1]);
    }

    #[test]
    fn rejects_v5_row_with_mismatched_embedded_schema_version() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + 4 * 60_000;
        let mut row = metadata_v5_fixture_row(&base_record(1), None);
        row.compatibility_prefix_mut().schema_version =
            POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION.to_owned();
        write_metadata_v5_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[row],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("metadata row schema version mismatch")
                && error.contains(POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION)
                && error.contains(POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION),
            "{error}"
        );
    }

    #[test]
    fn current_source_v3_metadata_v5_ws_claimant_and_agreeing_duplicate_are_terminal_complete() {
        let tmp = tempdir().unwrap();
        let epoch = "source-epoch-current";
        let receive = E + 240_000;
        let recovery_time = receive + 30;
        let terminal = ws_terminal(2, receive);
        let metadata = vec![
            metadata_v5_fixture_row(&base_record(1), None),
            metadata_v5_fixture_row(&terminal, Some((epoch, 1))),
        ];
        let sources = vec![
            typed_market_resolved_source(epoch, 1, receive, receive + 1, "101", "Up"),
            typed_market_resolved_source(epoch, 2, receive, receive + 2, "101", "up"),
        ];
        write_current_fixture(tmp.path(), recovery_time, &metadata, &sources);

        let recovery =
            super::restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert!(recovery.pending.is_empty());
    }

    #[test]
    fn current_source_v3_metadata_v5_ws_missing_or_wrong_claimant_fails_closed() {
        let epoch = "source-epoch-current";
        let receive = E + 240_000;
        let recovery_time = receive + 30;
        for (claimant, expected) in [
            (None, "ws_claimant_source_sequence_epoch_id"),
            (
                Some(("wrong-source-epoch", 99)),
                "WS claimant does not resolve exactly once",
            ),
        ] {
            let tmp = tempdir().unwrap();
            let terminal = ws_terminal(2, receive);
            let metadata = vec![
                metadata_v5_fixture_row(&base_record(1), None),
                metadata_v5_fixture_row(&terminal, claimant),
            ];
            let sources = vec![typed_market_resolved_source(
                epoch,
                1,
                receive,
                receive + 1,
                "101",
                "Up",
            )];
            write_current_fixture(tmp.path(), recovery_time, &metadata, &sources);

            let error = super::restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn current_source_v3_metadata_v5_gamma_accepts_zero_or_pre_exit_agreeing_source() {
        let epoch = "source-epoch-current";
        let terminal = gamma_terminal(2, 1);
        let role_exit = terminal.runtime_role_applied_at_utc_ms.unwrap();
        let recovery_time = role_exit + 30;
        let metadata = vec![
            metadata_v5_fixture_row(&base_record(1), None),
            metadata_v5_fixture_row(&terminal, None),
        ];

        let zero_source = tempdir().unwrap();
        write_current_fixture(zero_source.path(), recovery_time, &metadata, &[]);
        assert!(
            super::restore_polymarket_resolution_recovery(zero_source.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let agreeing_source = tempdir().unwrap();
        let sources = vec![typed_market_resolved_source(
            epoch,
            1,
            terminal.timestamp_received,
            role_exit,
            "101",
            "Up",
        )];
        write_current_fixture(agreeing_source.path(), recovery_time, &metadata, &sources);
        assert!(
            super::restore_polymarket_resolution_recovery(agreeing_source.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );
    }

    #[test]
    fn current_source_v3_metadata_v5_gamma_rejects_late_or_contradictory_source() {
        let epoch = "source-epoch-current";
        let terminal = gamma_terminal(2, 1);
        let role_exit = terminal.runtime_role_applied_at_utc_ms.unwrap();
        let recovery_time = role_exit + 30;
        let metadata = vec![
            metadata_v5_fixture_row(&base_record(1), None),
            metadata_v5_fixture_row(&terminal, None),
        ];
        let cases = [
            (
                typed_market_resolved_source(epoch, 1, role_exit + 1, role_exit + 2, "101", "Up"),
                "later than terminal role exit/admission",
            ),
            (
                typed_market_resolved_source(
                    epoch,
                    1,
                    terminal.timestamp_received,
                    role_exit,
                    "102",
                    "Down",
                ),
                "terminal/source winner mismatch",
            ),
        ];

        for (source, expected) in cases {
            let tmp = tempdir().unwrap();
            write_current_fixture(tmp.path(), recovery_time, &metadata, &[source]);
            let error = super::restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn current_source_v3_metadata_v5_exhausted_summary_requires_zero_source() {
        let epoch = "source-epoch-current";
        let attempts = (1..=RESOLUTION_ATTEMPT_COUNT)
            .map(|ordinal| attempt(u64::from(ordinal) + 1, ordinal, "transport_unavailable"))
            .collect::<Vec<_>>();
        let summary = summary(8, &attempts);
        let recovery_time = summary.admission_time_utc_ms + 30;
        let mut records = vec![base_record(1)];
        records.extend(attempts);
        records.push(summary);
        let metadata = records
            .iter()
            .map(|record| metadata_v5_fixture_row(record, None))
            .collect::<Vec<_>>();

        let zero_source = tempdir().unwrap();
        write_current_fixture(zero_source.path(), recovery_time, &metadata, &[]);
        assert!(
            super::restore_polymarket_resolution_recovery(zero_source.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let with_source = tempdir().unwrap();
        let source_receive = E + 240_000;
        let sources = vec![typed_market_resolved_source(
            epoch,
            1,
            source_receive,
            source_receive + 1,
            "101",
            "Up",
        )];
        write_current_fixture(with_source.path(), recovery_time, &metadata, &sources);
        let error =
            super::restore_polymarket_resolution_recovery(with_source.path(), recovery_time)
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("exhausted summary coexists with typed market_resolved source"),
            "{error}"
        );
    }

    #[test]
    fn current_source_v3_metadata_v5_source_only_and_cross_pair_sources_fail_closed() {
        let epoch = "source-epoch-current";
        let receive = E + 240_000;
        let recovery_time = receive + 30;
        let sources = vec![typed_market_resolved_source(
            epoch,
            1,
            receive,
            receive + 1,
            "101",
            "Up",
        )];

        let source_only = tempdir().unwrap();
        write_current_fixture(source_only.path(), recovery_time, &[], &sources);
        let error =
            super::restore_polymarket_resolution_recovery(source_only.path(), recovery_time)
                .unwrap_err()
                .to_string();
        assert!(error.contains("no current metadata identity"), "{error}");

        let v4_with_v2_source = tempdir().unwrap();
        write_current_fixture(
            v4_with_v2_source.path(),
            recovery_time,
            &[metadata_v5_fixture_row(&base_record(1), None)],
            &[],
        );
        write_relevant_legacy_v2_source(
            &day_dir(v4_with_v2_source.path(), recovery_time, "polymarket_market")
                .join("polymarket_market_02.parquet"),
            receive,
        );
        let error =
            super::restore_polymarket_resolution_recovery(v4_with_v2_source.path(), recovery_time)
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("source v2 cannot pair with current metadata"),
            "{error}"
        );

        let v3_metadata_with_v3_source = tempdir().unwrap();
        write_metadata(
            &day_dir(
                v3_metadata_with_v3_source.path(),
                recovery_time,
                "polymarket_market_metadata",
            )
            .join("polymarket_market_metadata_02.parquet"),
            &[base_record(1)],
        );
        write_typed_v3_sources(
            &day_dir(
                v3_metadata_with_v3_source.path(),
                recovery_time,
                "polymarket_market",
            )
            .join("polymarket_market_02.parquet"),
            &sources,
        );
        let error = super::restore_polymarket_resolution_recovery(
            v3_metadata_with_v3_source.path(),
            recovery_time,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("no current metadata identity"), "{error}");
    }

    #[test]
    fn current_source_v3_metadata_v5_rejects_same_slug_v3_metadata_layout() {
        let tmp = tempdir().unwrap();
        let epoch = "source-epoch-current";
        let receive = E + 240_000;
        let recovery_time = receive + 30;
        let terminal = ws_terminal(2, receive);
        let metadata_v5 = vec![
            metadata_v5_fixture_row(&base_record(1), None),
            metadata_v5_fixture_row(&terminal, Some((epoch, 1))),
        ];
        let sources = vec![typed_market_resolved_source(
            epoch,
            1,
            receive,
            receive + 1,
            "101",
            "Up",
        )];
        write_current_fixture(tmp.path(), recovery_time, &metadata_v5, &sources);
        write_metadata(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_03.parquet"),
            &[base_record(1)],
        );

        let error = super::restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("detected dual metadata layouts")
                && error.contains("forbidden_layout=polymarket_market_metadata_v3"),
            "{error}"
        );
    }

    #[test]
    fn restores_only_future_slots_and_reports_finalized_and_missing_ordinals() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + 4 * 60_000;
        let metadata = vec![base_record(1), attempt(2, 1, "valid_nonterminal")];
        write_finalized_fixture(tmp.path(), recovery_time, &metadata);

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert_eq!(recovery.pending.len(), 1);
        let pending = &recovery.pending[0];
        assert_eq!(pending.slug, SLUG);
        assert_eq!(pending.up_asset_id, "101");
        assert_eq!(pending.down_asset_id, "102");
        assert_eq!(pending.finalized_attempt_ordinals, vec![1]);
        assert_eq!(pending.missed_attempt_ordinals, vec![2]);
        assert_eq!(pending.next_attempt_ordinal, 3);
        assert_eq!(pending.attempts_recorded, 1);
        assert_eq!(pending.valid_response_count, 1);
        assert_eq!(
            pending.last_valid_metadata_received_at_utc_ms,
            Some(E + 180_010)
        );
        assert_eq!(
            pending.discovery_source_raw_sha256,
            sha256_hex(discovery_payload().as_bytes())
        );
    }

    #[test]
    fn recovery_boundaries_treat_e_as_pending_and_slot_equality_as_missed() {
        for (recovery_time, expected_next, expected_missed) in [
            (E - 1, None, Vec::new()),
            (E, Some(1), Vec::new()),
            (E + FIRST_SLOT_DELAY_MS - 1, Some(1), Vec::new()),
            (E + FIRST_SLOT_DELAY_MS, Some(2), vec![1]),
            (E + BUDGET_HORIZON_MS - 1, Some(6), vec![1, 2, 3, 4, 5]),
            (E + BUDGET_HORIZON_MS, None, Vec::new()),
            (E + BUDGET_HORIZON_MS + 1, None, Vec::new()),
        ] {
            let tmp = tempdir().unwrap();
            write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1)]);
            let recovery =
                restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
            match expected_next {
                Some(expected_next) => {
                    assert_eq!(recovery.pending.len(), 1, "time={recovery_time}");
                    assert_eq!(
                        recovery.pending[0].next_attempt_ordinal, expected_next,
                        "time={recovery_time}"
                    );
                    assert_eq!(
                        recovery.pending[0].missed_attempt_ordinals, expected_missed,
                        "time={recovery_time}"
                    );
                }
                None => assert!(recovery.pending.is_empty(), "time={recovery_time}"),
            }
        }
    }

    #[test]
    fn ignores_running_digesting_and_finalized_v2_files() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + 4 * 60_000;
        let metadata_dir = day_dir(tmp.path(), recovery_time, "polymarket_market_metadata");
        write_metadata(
            &metadata_dir.join("polymarket_market_metadata_02.parquet.running"),
            &[base_record(1)],
        );
        write_metadata(
            &metadata_dir.join("polymarket_market_metadata_02.running.parquet"),
            &[base_record(1)],
        );
        write_metadata(
            &metadata_dir.join("polymarket_market_metadata_02-running.parquet"),
            &[base_record(1)],
        );
        write_metadata(
            &metadata_dir.join("polymarket_market_metadata_02-digesting.parquet"),
            &[base_record(1)],
        );
        write_v2(&metadata_dir.join("polymarket_market_metadata_01.parquet"));

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert!(recovery.pending.is_empty());
    }

    #[test]
    fn rejects_v5_missing_row_kind_in_default_and_precise_reference_days() {
        let recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        write_metadata_without_row_kind(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
        );
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("projection column missing") && error.contains("metadata_row_kind"),
            "{error}"
        );

        let day_start = T.div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
        let recovery_time = day_start + 10 * 60_000;
        let source_receive = day_start - 2 * UTC_DAY_MS - 5 * 60_000;
        let precise_slug = fixture_slug(day_start);
        let authority = discovery_authority_record(
            1,
            &precise_slug,
            CONDITION_2,
            "precise-reference-clob",
            day_start,
            day_start + 5 * 60_000,
            source_receive,
        );
        let transition = next_to_current_transition(&authority, 2, day_start);
        let tmp = tempdir().unwrap();
        write_metadata_v5_records(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[transition],
        );
        write_metadata_without_row_kind(
            &day_dir(tmp.path(), source_receive, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
        );
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("projection column missing") && error.contains("metadata_row_kind"),
            "{error}"
        );
    }

    #[test]
    fn cross_day_candidate_loads_only_its_precise_older_source_authority() {
        let day_start = T.div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
        let recovery_time = day_start + 10 * 60_000;
        let source_day_receive = day_start - UTC_DAY_MS - 5 * 60_000;
        let old_slug = fixture_slug(day_start - UTC_DAY_MS);

        let old_authority = discovery_authority_record(
            1,
            &old_slug,
            CONDITION_2,
            "old-cross-day-clob",
            day_start - UTC_DAY_MS,
            day_start - UTC_DAY_MS + 5 * 60_000,
            source_day_receive,
        );
        let old_transition = next_to_current_transition(&old_authority, 2, day_start - UTC_DAY_MS);
        let candidate_slug = fixture_slug(day_start + 4 * 60_000);

        let candidate_authority = discovery_authority_record(
            3,
            &candidate_slug,
            CONDITION_3,
            "candidate-cross-day-clob",
            day_start + 4 * 60_000,
            day_start + 9 * 60_000,
            source_day_receive + 1_000,
        );
        let candidate_transition =
            next_to_current_transition(&candidate_authority, 4, day_start + 4 * 60_000);

        let tmp = tempdir().unwrap();
        write_metadata_v5_records(
            &day_dir(tmp.path(), source_day_receive, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[old_authority, candidate_authority.clone()],
        );
        write_metadata_v5_records(
            &day_dir(
                tmp.path(),
                day_start - UTC_DAY_MS,
                "polymarket_market_metadata",
            )
            .join("polymarket_market_metadata_02.parquet"),
            &[old_transition],
        );
        write_metadata_v5_records(
            &day_dir(tmp.path(), day_start, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[candidate_transition],
        );

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert_eq!(recovery.pending.len(), 1);
        assert_eq!(recovery.pending[0].slug, candidate_slug);
        assert_eq!(
            recovery.pending[0].discovery_source_raw_sha256,
            candidate_authority.source_raw_sha256.unwrap()
        );
    }

    #[test]
    fn completed_recent_market_does_not_consume_a_recovery_candidate_slot() {
        let recovery_time = E + 10 * 60_000;
        let second_slug = fixture_slug(E);
        let second_authority = discovery_authority_record(
            3,
            &second_slug,
            CONDITION_4,
            "second-recoverable-clob",
            E,
            E + 5 * 60_000,
            E - 2 * 60_000,
        );
        let second_transition = next_to_current_transition(&second_authority, 4, E);
        let third_slug = fixture_slug(E + 5 * 60_000);
        let third_authority = discovery_authority_record(
            5,
            &third_slug,
            CONDITION_5,
            "third-recoverable-clob",
            E + 5 * 60_000,
            E + 10 * 60_000,
            E + 3 * 60_000,
        );
        let third_transition = next_to_current_transition(&third_authority, 6, E + 5 * 60_000);

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[
                base_record(1),
                gamma_terminal(2, 1),
                second_authority,
                second_transition,
                third_authority,
                third_transition,
            ],
        );

        let recovery = restore_polymarket_resolution_recovery(tmp.path(), recovery_time).unwrap();
        assert_eq!(
            recovery
                .pending
                .iter()
                .map(|pending| pending.slug.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([second_slug.as_str(), third_slug.as_str()])
        );
    }

    #[test]
    fn authoritative_schema_covers_full_v4_metadata() {
        let metadata = polymarket_market_metadata_v4_schema();
        require_canonical_metadata_schema(
            metadata.as_ref(),
            Path::new("metadata"),
            MetadataLayout::V4,
        )
        .unwrap();

        assert_eq!(
            metadata
                .field_with_name("collector_admission_sequence")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            metadata
                .field_with_name("resolution_poll_attempt_ordinal")
                .unwrap()
                .data_type(),
            &DataType::UInt8
        );
    }

    #[test]
    fn rejects_full_v4_schema_missing_required_column() {
        let recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        write_metadata_v4_rows_without_field(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[metadata_v4_fixture_row(&base_record(1), None)],
            "resolution_transport_http_status",
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("projection column missing")
                && error.contains("resolution_transport_http_status"),
            "{error}"
        );
    }

    #[test]
    fn rejects_v4_required_null_matrix_contradiction() {
        let recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        let mut row = metadata_fixture_row(&base_record(1));
        row.source_evidence_event_type = Some(SourceEvidenceEventType::MarketResolved);
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[metadata_v4_from_v3_fixture(row, None)],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("source_evidence_event_type:expected_discovery_response"),
            "{error}"
        );
    }

    #[test]
    fn rejects_attempt_missing_previously_unprojected_budget_horizon() {
        let recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        let mut attempt_row = metadata_fixture_row(&attempt(2, 1, "valid_nonterminal"));
        attempt_row.resolution_budget_horizon_utc = None;
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[
                metadata_v4_fixture_row(&base_record(1), None),
                metadata_v4_from_v3_fixture(attempt_row, None),
            ],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("resolution_budget_horizon_utc:required"),
            "{error}"
        );
    }

    #[test]
    fn rejects_discovery_payload_fixed_identity_contradiction() {
        let recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        let mut row = metadata_fixture_row(&base_record(1));
        let mut payload =
            serde_json::from_str::<Value>(row.source_evidence_payload_json.as_deref().unwrap())
                .unwrap();
        payload["conditionId"] = Value::String(CONDITION_6.to_owned());
        let payload = payload.to_string();
        row.source_evidence_payload_json = Some(payload.clone());
        row.source_evidence_raw_sha256 = Some(sha256_hex(payload.as_bytes()));
        assert!(row.validate().is_complete());
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[metadata_v4_from_v3_fixture(row, None)],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("discovery SDK payload contradicts persisted fixed identity"),
            "{error}"
        );
    }

    #[test]
    fn rejects_gamma_terminal_payload_predicate_contradiction() {
        let recovery_time = E + 5 * 60_000;
        let tmp = tempdir().unwrap();
        let mut terminal_row = metadata_fixture_row(&gamma_terminal(2, 1));
        let mut payload = serde_json::from_str::<Value>(
            terminal_row
                .source_evidence_payload_json
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        payload["acceptingOrders"] = Value::Bool(true);
        let payload = payload.to_string();
        let digest = sha256_hex(payload.as_bytes());
        terminal_row.source_evidence_payload_json = Some(payload);
        terminal_row.source_evidence_raw_sha256 = Some(digest.clone());
        terminal_row.resolution_evidence_raw_sha256 = Some(digest);
        assert!(terminal_row.validate().is_complete());
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[
                metadata_v4_fixture_row(&base_record(1), None),
                metadata_v4_from_v3_fixture(terminal_row, None),
            ],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Gamma attempt payload evaluation contradicts persisted result"),
            "{error}"
        );
    }

    #[test]
    fn rejects_ws_platform_timestamp_that_does_not_match_payload() {
        let recovery_time = E + 5 * 60_000;
        let receive = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        let mut terminal_row = metadata_fixture_row(&ws_terminal(2, receive));
        terminal_row.ws_platform_timestamp_raw = Some((receive + 1).to_string());
        terminal_row.ws_platform_timestamp_utc = Some(receive + 1);
        assert!(terminal_row.validate().is_complete());
        write_metadata_v4_rows(
            &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                .join("polymarket_market_metadata_02.parquet"),
            &[
                metadata_v4_fixture_row(&base_record(1), None),
                metadata_v4_from_v3_fixture(terminal_row, Some(("fixture-source-epoch", 1))),
            ],
        );

        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(
                "market_resolved payload timestamp contradicts persisted platform timestamp"
            ),
            "{error}"
        );
    }

    #[test]
    fn rejects_fixed_identity_digest_and_local_reference_conflicts() {
        let recovery_time = E + 4 * 60_000;

        let tmp = tempdir().unwrap();
        let mut conflict = base_record(2);
        conflict.condition_id = format!("{CONDITION}ff");
        write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1), conflict]);
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("discovery SDK payload contradicts persisted fixed identity"),
            "{error}"
        );

        let tmp = tempdir().unwrap();
        let mut digest = base_record(1);
        digest.source_raw_sha256 = Some("0".repeat(64));
        write_finalized_fixture(tmp.path(), recovery_time, &[digest]);
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("source_evidence_raw_sha256:payload_mismatch"),
            "{error}"
        );

        let tmp = tempdir().unwrap();
        let mut transition = base_record(2);
        transition.kind = "market_state_transition".to_owned();
        transition.timestamp_received = T + 1;
        transition.admission_time_utc_ms = T + 3;
        transition.metadata_origin = "local_market_state_timer".to_owned();
        transition.source_payload_json = None;
        transition.lifecycle_transition = Some("prefetched_to_active".to_owned());
        transition.lifecycle_applied_at_utc_ms = Some(T + 1);
        transition.runtime_role_transition = Some("next_to_current".to_owned());
        transition.runtime_role_applied_at_utc_ms = Some(T + 1);
        let mut duplicate = base_record(3);
        duplicate.admission_time_utc_ms += 1;
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), transition, duplicate],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("multiple metadata source authorities")
        );
    }

    #[test]
    fn rejects_duplicate_misbound_and_integrity_attempts() {
        let recovery_time = E + 4 * 60_000;

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[
                base_record(1),
                attempt(2, 1, "valid_nonterminal"),
                attempt(3, 1, "transport_unavailable"),
            ],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("duplicate attempt ordinal")
        );

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), attempt(2, 5, "transport_unavailable")],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("durable attempt ordinals are not a prefix")
        );

        let tmp = tempdir().unwrap();
        let mut misbound = attempt(2, 1, "valid_nonterminal");
        misbound.attempt_scheduled_utc_ms = Some(E + 180_001);
        write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1), misbound]);
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("resolution_poll_scheduled_utc:not_absolute_slot"),
            "{error}"
        );

        let tmp = tempdir().unwrap();
        let mut integrity = attempt(2, 1, "integrity_error");
        integrity.integrity_error_class = Some("sdk_decode_or_schema".to_owned());
        write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1), integrity]);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("integrity_error")
        );
    }

    #[test]
    fn rejects_restart_relevant_attempt_source_shape_conflicts() {
        let recovery_time = E + 4 * 60_000;

        let mut decoded_receive_mismatch = attempt(2, 1, "valid_nonterminal");
        decoded_receive_mismatch.source_received_at_utc_ms =
            Some(decoded_receive_mismatch.timestamp_received + 1);

        let mut decoded_missing_source = attempt(2, 1, "valid_nonterminal");
        decoded_missing_source.source_origin = None;
        decoded_missing_source.source_received_at_utc_ms = None;
        decoded_missing_source.source_payload_json = None;
        decoded_missing_source.source_raw_sha256 = None;

        let mut transport_with_source = attempt(2, 1, "transport_unavailable");
        let payload = json!({"slug": SLUG, "unexpected": "transport_source"}).to_string();
        transport_with_source.source_origin = Some("gamma_by_slug".to_owned());
        transport_with_source.source_received_at_utc_ms =
            Some(transport_with_source.timestamp_received);
        transport_with_source.source_payload_json = Some(payload.clone());
        transport_with_source.source_raw_sha256 = Some(sha256_hex(payload.as_bytes()));

        let mut missed_wrong_origin = attempt(2, 1, "missed_due_to_downtime");
        missed_wrong_origin.metadata_origin = "gamma_by_slug".to_owned();

        for (case, invalid_attempt, expected) in [
            (
                "decoded_receive_mismatch",
                decoded_receive_mismatch,
                "resolution_poll_attempt:timestamp_not_source_receive",
            ),
            (
                "decoded_missing_source",
                decoded_missing_source,
                "source_evidence_origin:expected_gamma_by_slug",
            ),
            (
                "transport_with_source",
                transport_with_source,
                "source_evidence_origin:must_be_null",
            ),
            (
                "missed_wrong_origin",
                missed_wrong_origin,
                "metadata_origin:missed_attempt",
            ),
        ] {
            let tmp = tempdir().unwrap();
            write_finalized_fixture(
                tmp.path(),
                recovery_time,
                &[base_record(1), invalid_attempt],
            );
            let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "case={case}:{error}");
        }
    }

    #[test]
    fn ws_terminal_uses_current_typed_claimant_and_prefixes_post_observations() {
        let recovery_time = E + 5 * 60_000;
        let receive = E + 240_000;

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[
                base_record(1),
                ws_terminal(2, receive),
                post_observation(3, 1),
            ],
        );
        write_typed_v3_sources(
            &day_dir(tmp.path(), recovery_time, "polymarket_market")
                .join("polymarket_market_02.parquet"),
            &[typed_market_resolved_source(
                "fixture-source-epoch",
                1,
                receive,
                receive + 1,
                "101",
                "Up",
            )],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[
                base_record(1),
                ws_terminal(3, receive),
                post_observation(2, 1),
            ],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("post-terminal prefix")
        );

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), post_observation(2, 1)],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("orphan post-terminal")
        );
    }

    #[test]
    fn post_terminal_observation_requires_an_earlier_finalized_artifact_position() {
        let recovery_time = E + 5 * 60_000;
        let receive = E + 4 * 60_000;

        let tmp = tempdir().unwrap();
        let metadata_dir = day_dir(tmp.path(), recovery_time, "polymarket_market_metadata");
        write_metadata_v5_records(
            &metadata_dir.join("polymarket_market_metadata_02.parquet"),
            &[base_record(1), ws_terminal(2, receive)],
        );
        write_metadata_v5_records(
            &metadata_dir.join("polymarket_market_metadata_03.parquet"),
            &[post_observation(3, 1)],
        );
        write_typed_v3_sources(
            &day_dir(tmp.path(), recovery_time, "polymarket_market")
                .join("polymarket_market_02.parquet"),
            &[typed_market_resolved_source(
                "fixture-source-epoch",
                1,
                receive,
                receive + 1,
                "101",
                "Up",
            )],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let tmp = tempdir().unwrap();
        let metadata_dir = day_dir(tmp.path(), recovery_time, "polymarket_market_metadata");
        write_metadata_v5_records(
            &metadata_dir.join("polymarket_market_metadata_02.parquet"),
            &[base_record(1), post_observation(3, 1)],
        );
        write_metadata_v5_records(
            &metadata_dir.join("polymarket_market_metadata_03.parquet"),
            &[ws_terminal(2, receive)],
        );
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(error.contains("post-terminal prefix"), "{error}");
    }

    #[test]
    fn legacy_v4_owned_terminals_require_receive_and_matching_source_and_resolution_digests() {
        let recovery_time = E + 5 * 60_000;
        let receive = E + 4 * 60_000;
        for (owner, terminal) in [
            ("ws", ws_terminal(2, receive)),
            ("gamma", gamma_terminal(2, 1)),
        ] {
            let mut missing_receive = terminal.clone();
            missing_receive.source_received_at_utc_ms = None;
            let mut missing_source_digest = terminal.clone();
            missing_source_digest.source_raw_sha256 = None;
            let mut missing_resolution_digest = terminal.clone();
            missing_resolution_digest.resolution_evidence_raw_sha256 = None;
            let mut mismatched_digest = terminal;
            mismatched_digest.resolution_evidence_raw_sha256 = Some("f".repeat(64));

            for (case, invalid_terminal, expected) in [
                (
                    "missing_receive",
                    missing_receive,
                    "source_evidence_received_at_utc:required",
                ),
                (
                    "missing_source_digest",
                    missing_source_digest,
                    "source_evidence_raw_sha256:required",
                ),
                (
                    "missing_resolution_digest",
                    missing_resolution_digest,
                    "resolution_evidence_raw_sha256:required",
                ),
                (
                    "mismatched_digest",
                    mismatched_digest,
                    "resolution_evidence_raw_sha256:source_digest_mismatch",
                ),
            ] {
                let tmp = tempdir().unwrap();
                write_metadata_v4_records(
                    &day_dir(tmp.path(), recovery_time, "polymarket_market_metadata")
                        .join("polymarket_market_metadata_02.parquet"),
                    &[base_record(1), invalid_terminal],
                );
                let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(expected),
                    "owner={owner}:case={case}:{error}"
                );
            }
        }
    }

    #[test]
    fn legal_terminal_and_summary_after_horizon_validate_without_recovery() {
        let recovery_time = E + 30 * 60_000;

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), gamma_terminal(2, 1)],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let attempts = (1..=RESOLUTION_ATTEMPT_COUNT)
            .map(|ordinal| attempt(u64::from(ordinal) + 1, ordinal, "transport_unavailable"))
            .collect::<Vec<_>>();
        let mut metadata = vec![base_record(1)];
        metadata.extend(attempts.clone());
        metadata.push(summary(8, &attempts));
        let tmp = tempdir().unwrap();
        write_finalized_fixture(tmp.path(), recovery_time, &metadata);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );
    }

    #[test]
    fn summary_reconstructs_six_ordinals_and_requires_zero_typed_source() {
        let attempts = (1..=6)
            .map(|ordinal| {
                let result = match ordinal {
                    1..=4 => "valid_nonterminal",
                    5 => "transport_unavailable",
                    _ => "missed_due_to_downtime",
                };
                attempt(u64::from(ordinal) + 1, ordinal, result)
            })
            .collect::<Vec<_>>();
        let summary = summary(8, &attempts);
        summary.validate_recovery_eligibility().unwrap();
        let counters = attempt_counters(&attempts).unwrap();
        validate_typed_summary(&summary, &counters, &[]).unwrap();

        let mut bad_summary = summary;
        bad_summary.valid_response_count = Some(3);
        assert!(
            validate_typed_summary(&bad_summary, &counters, &[])
                .unwrap_err()
                .to_string()
                .contains("do not reconstruct six slots")
        );
    }

    #[test]
    fn rejects_restart_decision_row_shape_contradictions() {
        let recovery_time = E + 7 * 60_000;
        let receive = E + 4 * 60_000;

        let mut bad_terminal = ws_terminal(2, receive);
        bad_terminal.lifecycle_state = "active".to_owned();
        let tmp = tempdir().unwrap();
        write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1), bad_terminal]);
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("terminal_transition:invalid_state_role"),
            "{error}"
        );

        let mut late_applied_terminal = ws_terminal(2, receive);
        late_applied_terminal.lifecycle_applied_at_utc_ms =
            Some(late_applied_terminal.admission_time_utc_ms + 1);
        late_applied_terminal.runtime_role_applied_at_utc_ms =
            late_applied_terminal.lifecycle_applied_at_utc_ms;
        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), late_applied_terminal],
        );
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("lifecycle_applied_at_utc:after_admission"),
            "{error}"
        );

        let attempts = (1..=6)
            .map(|ordinal| attempt(u64::from(ordinal) + 1, ordinal, "transport_unavailable"))
            .collect::<Vec<_>>();
        let mut bad_summary = summary(8, &attempts);
        bad_summary.terminal_claim_owner = Some("market_resolved_ws".to_owned());
        let mut metadata = vec![base_record(1)];
        metadata.extend(attempts);
        metadata.push(bad_summary);
        let tmp = tempdir().unwrap();
        write_finalized_fixture(tmp.path(), recovery_time, &metadata);
        let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("terminal_claim_owner:must_be_null"),
            "{error}"
        );

        for (case, post, expected) in [
            {
                let mut post = post_observation(3, 1);
                post.winning_asset_id = Some("101".to_owned());
                ("winner", post, "winning_asset_id:must_be_null")
            },
            {
                let mut post = post_observation(3, 1);
                post.runtime_role_transition = Some("resolution_pending_to_none".to_owned());
                post.runtime_role_applied_at_utc_ms = Some(post.timestamp_received + 1);
                (
                    "transition",
                    post,
                    "market_runtime_role_transition:must_be_null",
                )
            },
        ] {
            let tmp = tempdir().unwrap();
            write_finalized_fixture(
                tmp.path(),
                recovery_time,
                &[base_record(1), ws_terminal(2, receive), post.clone()],
            );
            let error = restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "case={case}:{error}");
        }

        let mut bad_snapshot = base_record(1);
        bad_snapshot.metadata_origin = "local_market_state_timer".to_owned();
        let authority_recovery_time = E + 4 * 60_000;
        let tmp = tempdir().unwrap();
        write_finalized_fixture(tmp.path(), authority_recovery_time, &[bad_snapshot]);
        let error = restore_polymarket_resolution_recovery(tmp.path(), authority_recovery_time)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("metadata_origin:metadata_snapshot"),
            "{error}"
        );

        let mut pending_transition = base_record(2);
        pending_transition.kind = "market_state_transition".to_owned();
        pending_transition.timestamp_received = E + 1;
        pending_transition.admission_time_utc_ms = E + 2;
        pending_transition.metadata_origin = "local_market_state_timer".to_owned();
        pending_transition.runtime_role = "resolution_pending".to_owned();
        pending_transition.source_payload_json = None;
        pending_transition.runtime_role_transition =
            Some("current_to_resolution_pending".to_owned());
        pending_transition.runtime_role_applied_at_utc_ms = Some(E + 1);
        pending_transition.budget_horizon_utc_ms = Some(E + BUDGET_HORIZON_MS);
        pending_transition.followup_status = Some("pending".to_owned());
        for (case, transition, expected) in [
            {
                let mut transition = pending_transition.clone();
                transition.runtime_role_transition = Some("next_to_current".to_owned());
                (
                    "subtype",
                    transition,
                    "market_state_transition:unsupported_subtype",
                )
            },
            {
                let mut transition = pending_transition.clone();
                transition.metadata_origin = "gamma_markets_discovery".to_owned();
                (
                    "origin",
                    transition,
                    "metadata_origin:market_state_transition",
                )
            },
        ] {
            let tmp = tempdir().unwrap();
            write_finalized_fixture(
                tmp.path(),
                authority_recovery_time,
                &[base_record(1), transition],
            );
            let error = restore_polymarket_resolution_recovery(tmp.path(), authority_recovery_time)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "case={case}:{error}");
        }
    }

    #[test]
    fn rejects_duplicate_or_mutually_exclusive_terminal_states() {
        let recovery_time = E + 7 * 60_000;

        let tmp = tempdir().unwrap();
        write_finalized_fixture(
            tmp.path(),
            recovery_time,
            &[base_record(1), gamma_terminal(2, 1), gamma_terminal(3, 2)],
        );
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("duplicate terminal")
        );

        let attempts = (1..=6)
            .map(|ordinal| attempt(u64::from(ordinal) + 10, ordinal, "transport_unavailable"))
            .collect::<Vec<_>>();
        let tmp = tempdir().unwrap();
        let mut metadata = vec![base_record(1), gamma_terminal(2, 1)];
        metadata.push(summary(30, &attempts));
        write_finalized_fixture(tmp.path(), recovery_time, &metadata);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("terminal and summary coexist")
        );

        let tmp = tempdir().unwrap();
        let first = summary(30, &attempts);
        let mut second = first.clone();
        second.admission_sequence = 31;
        second.admission_time_utc_ms += 1;
        let mut metadata = vec![base_record(1)];
        metadata.extend(attempts);
        metadata.extend([first, second]);
        write_finalized_fixture(tmp.path(), recovery_time, &metadata);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("duplicate followup summaries")
        );
    }

    #[test]
    fn never_recovers_at_or_after_last_slot_and_enforces_two_pending_bound() {
        let tmp = tempdir().unwrap();
        let recovery_time = E + BUDGET_HORIZON_MS;
        write_finalized_fixture(tmp.path(), recovery_time, &[base_record(1)]);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap()
                .pending
                .is_empty()
        );

        let tmp = tempdir().unwrap();
        let recovery_time = E + 2 * 60_000;
        let conditions = [CONDITION_2, CONDITION_3, CONDITION_4];
        let records = (0..3)
            .map(|index| {
                let mut record = base_record(index + 1);
                record.market_start_utc_ms = T + i64::try_from(index).unwrap() * 1_000;
                record.market_end_utc_ms = record.market_start_utc_ms + 5 * 60_000;
                record.slug = fixture_slug(record.market_start_utc_ms);
                record.condition_id = conditions[usize::try_from(index).unwrap()].to_owned();
                record.clob_market_id = format!("{MARKET_ID}-{index}");
                let payload = gamma_market_payload(
                    &record.slug,
                    &record.condition_id,
                    &record.clob_market_id,
                    record.market_start_utc_ms,
                    record.market_end_utc_ms,
                    false,
                );
                record.source_payload_json = Some(payload.clone());
                record.source_raw_sha256 = Some(sha256_hex(payload.as_bytes()));
                record
            })
            .collect::<Vec<_>>();
        write_finalized_fixture(tmp.path(), recovery_time, &records);
        assert!(
            restore_polymarket_resolution_recovery(tmp.path(), recovery_time)
                .unwrap_err()
                .to_string()
                .contains("pending bound exceeded")
        );
    }
}
