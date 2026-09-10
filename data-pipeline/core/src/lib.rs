pub mod pm_message;
pub mod pm_reduction;
pub mod public_input;
pub mod public_reduction;
pub mod public_reference;
pub mod reduction;
pub mod typed_raw;

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use contracts::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const EXECUTION_EVENT_ID_DOMAIN_V3: &[u8] =
    b"data-pipeline/canonical-event-id/execution-market-event/v3\0";
const REFERENCE_EVENT_ID_DOMAIN_V1: &[u8] =
    b"data-pipeline/canonical-event-id/reference-event/v1\0";
const OBSERVATION_ID_DOMAIN_V3: &[u8] = b"data-pipeline/market-observation-id/v3\0";
const OBSERVATION_HASH_DOMAIN_V3: &[u8] = b"data-pipeline/market-observation-hash/v3\0";
const CONTRACT_DIGEST_DOMAIN: &[u8] = b"data-pipeline/contract-digest/v2\0";
const GATE_A_RECEIPT_ID_DOMAIN_V3: &[u8] = b"data-pipeline/gate-a-receipt-id/v3\0";
const METADATA_SEMANTIC_LOCATOR_DOMAIN_V1: &[u8] =
    b"data-pipeline/source-evidence/metadata-semantic-locator/v1\0";
const RTDS_NATIVE_PAYLOAD_DOMAIN_V1: &[u8] =
    b"data-pipeline/source-evidence/rtds-native-payload/v1\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractError {
    Json(String),
    UnsupportedSchema {
        contract: &'static str,
        actual: String,
    },
    InvalidField {
        field: String,
        reason: String,
    },
    DuplicateValue {
        field: String,
        value: String,
    },
    NonCanonicalOrder {
        field: String,
    },
}

impl Display for ContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(message) => write!(formatter, "contract JSON error: {message}"),
            Self::UnsupportedSchema { contract, actual } => {
                write!(formatter, "unsupported {contract} schema version: {actual}")
            }
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid contract field {field}: {reason}")
            }
            Self::DuplicateValue { field, value } => {
                write!(formatter, "duplicate contract value {field}: {value}")
            }
            Self::NonCanonicalOrder { field } => {
                write!(
                    formatter,
                    "contract collection is not canonically ordered: {field}"
                )
            }
        }
    }
}

impl std::error::Error for ContractError {}

fn invalid(field: impl Into<String>, reason: impl Into<String>) -> ContractError {
    ContractError::InvalidField {
        field: field.into(),
        reason: reason.into(),
    }
}

fn validate_iso_date(date: &str, field: &str) -> Result<(), ContractError> {
    let parts = date.split('-').collect::<Vec<_>>();
    if parts.len() != 3
        || parts[0].len() != 4
        || parts[1].len() != 2
        || parts[2].len() != 2
        || parts
            .iter()
            .any(|part| !part.as_bytes().iter().all(u8::is_ascii_digit))
    {
        return Err(invalid(field, "must use YYYY-MM-DD"));
    }
    let year = parts[0]
        .parse::<u32>()
        .map_err(|error| invalid(field, error.to_string()))?;
    let month = parts[1]
        .parse::<u32>()
        .map_err(|error| invalid(field, error.to_string()))?;
    let day = parts[2]
        .parse::<u32>()
        .map_err(|error| invalid(field, error.to_string()))?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if day == 0 || day > max_day {
        return Err(invalid(field, "is not a real calendar date"));
    }
    Ok(())
}

pub fn classify_dataset_date(date: &str) -> Result<DatasetDateEligibility, ContractError> {
    validate_iso_date(date, "dataset_date")?;
    Ok(if date < DATASET_DATE_FLOOR {
        DatasetDateEligibility::IneligibleBeforeDatasetFloor
    } else {
        DatasetDateEligibility::Unassigned
    })
}

fn stable_json<T: Serialize>(value: &T) -> Result<Vec<u8>, ContractError> {
    #[cfg(test)]
    assert!(
        !FORBID_IDENTITY_ENCODING.get(),
        "热业务调用了 JSON 身份编码"
    );
    serde_json::to_vec(value).map_err(|error| ContractError::Json(error.to_string()))
}

#[derive(Deserialize)]
struct SchemaVersionEnvelope {
    schema_version: String,
}

fn require_schema_version(
    bytes: &[u8],
    expected: &str,
    contract: &'static str,
) -> Result<(), ContractError> {
    let envelope: SchemaVersionEnvelope =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    if envelope.schema_version != expected {
        return Err(ContractError::UnsupportedSchema {
            contract,
            actual: envelope.schema_version,
        });
    }
    Ok(())
}

fn hash_domain_bytes(domain: &[u8], bytes: &[u8]) -> Sha256Hex {
    #[cfg(test)]
    assert!(!FORBID_IDENTITY_ENCODING.get(), "热业务调用了 SHA 身份编码");
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

fn typed_raw_contract_error(error: typed_raw::TypedRawError) -> ContractError {
    invalid("typed_raw", error.to_string())
}

const fn metadata_row_kind_name(kind: typed_raw::PolymarketMetadataRowKind) -> &'static str {
    match kind {
        typed_raw::PolymarketMetadataRowKind::MetadataSnapshot => "metadata_snapshot",
        typed_raw::PolymarketMetadataRowKind::MarketStateTransition => "market_state_transition",
        typed_raw::PolymarketMetadataRowKind::ResolutionPollAttempt => "resolution_poll_attempt",
        typed_raw::PolymarketMetadataRowKind::ResolutionTerminalWs => "resolution_terminal_ws",
        typed_raw::PolymarketMetadataRowKind::ResolutionFollowupSummary => {
            "resolution_followup_summary"
        }
    }
}

const fn metadata_source_origin_name(
    origin: typed_raw::MetadataSourceEvidenceOrigin,
) -> &'static str {
    match origin {
        typed_raw::MetadataSourceEvidenceOrigin::GammaMarketsDiscovery => "gamma_markets_discovery",
        typed_raw::MetadataSourceEvidenceOrigin::GammaBySlug => "gamma_by_slug",
        typed_raw::MetadataSourceEvidenceOrigin::MarketResolvedWs => "market_resolved_ws",
    }
}

#[derive(Serialize)]
struct MetadataSemanticLocatorV1<'a> {
    schema_version: &'a str,
    metadata_row_kind: &'static str,
    timestamp_received: i64,
    collector_host_id: &'a str,
    collector_admission_sequence: u64,
    collector_admission_time_utc_ms: i64,
    slug: &'a str,
    condition_id: &'a str,
    clob_market_id: &'a str,
    source_evidence_origin: Option<&'static str>,
    source_evidence_received_at_utc: Option<i64>,
    ws_platform_timestamp_utc: Option<i64>,
    ws_claimant_source_sequence_epoch_id: Option<&'a str>,
    ws_claimant_collector_admission_sequence: Option<u64>,
}

fn metadata_semantic_identity(
    row: &typed_raw::PolymarketMarketMetadataRow,
) -> Result<SourceNativeIdentityV1, ContractError> {
    let sha256 = match &row.source_evidence_raw_sha256 {
        Some(value) => Sha256Hex(value.clone()),
        None => {
            let locator = MetadataSemanticLocatorV1 {
                schema_version: &row.schema_version,
                metadata_row_kind: metadata_row_kind_name(row.metadata_row_kind),
                timestamp_received: row.timestamp_received,
                collector_host_id: &row.collector_host_id,
                collector_admission_sequence: row.collector_admission_sequence,
                collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
                slug: &row.slug,
                condition_id: &row.condition_id,
                clob_market_id: &row.clob_market_id,
                source_evidence_origin: row.source_evidence_origin.map(metadata_source_origin_name),
                source_evidence_received_at_utc: row.source_evidence_received_at_utc,
                ws_platform_timestamp_utc: row.ws_platform_timestamp_utc,
                ws_claimant_source_sequence_epoch_id: row
                    .ws_claimant_source_sequence_epoch_id
                    .as_deref(),
                ws_claimant_collector_admission_sequence: row
                    .ws_claimant_collector_admission_sequence,
            };
            hash_domain_bytes(METADATA_SEMANTIC_LOCATOR_DOMAIN_V1, &stable_json(&locator)?)
        }
    };
    Ok(SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256 })
}

fn collector_admission(
    source_sequence_epoch_id: &str,
    collector_admission_sequence: u64,
    collector_admission_time_utc_ms: i64,
) -> CollectorAdmissionCoordinateV1 {
    CollectorAdmissionCoordinateV1 {
        source_sequence_epoch_id: source_sequence_epoch_id.to_owned(),
        collector_admission_sequence,
        admitted_at_utc_ms: collector_admission_time_utc_ms,
    }
}

pub fn source_evidence_from_typed_raw(
    row: &typed_raw::TypedRawRow,
    input_manifest_sha256: &Sha256Hex,
    input_file_sha256: &Sha256Hex,
) -> Result<SourceEvidenceRefV2, ContractError> {
    row.validate().map_err(typed_raw_contract_error)?;
    decode_sha256(input_manifest_sha256, "input_manifest_sha256")?;
    decode_sha256(input_file_sha256, "input_file_sha256")?;

    let (
        source_kind,
        source_native_identity,
        source_event_at_utc_ms,
        received_at_utc_ms,
        admission,
    ) = source_evidence_parts_from_typed_raw(row)?;

    let evidence = SourceEvidenceRefV2 {
        source_kind,
        source_native_identity,
        source_event_at_utc_ms,
        received_at_utc_ms,
        collector_admission: admission,
        input_manifest_sha256: input_manifest_sha256.clone(),
        input_file_sha256: input_file_sha256.clone(),
    };
    validate_source_evidence(std::slice::from_ref(&evidence))?;
    Ok(evidence)
}

type TypedSourceEvidenceParts = (
    CanonicalSourceKind,
    Option<SourceNativeIdentityV1>,
    Option<i64>,
    i64,
    Option<CollectorAdmissionCoordinateV1>,
);

fn source_evidence_parts_from_typed_raw(
    row: &typed_raw::TypedRawRow,
) -> Result<TypedSourceEvidenceParts, ContractError> {
    Ok(match row {
        typed_raw::TypedRawRow::PolymarketOrderbook(row) => (
            CanonicalSourceKind::PolymarketOrderbook,
            None,
            row.timestamp,
            row.timestamp_received,
            Some(collector_admission(
                &row.source_sequence_epoch_id,
                row.collector_admission_sequence,
                row.collector_admission_time_utc_ms,
            )),
        ),
        typed_raw::TypedRawRow::PolymarketMarketMetadata(row) => {
            metadata_source_evidence_parts(row)?
        }
        typed_raw::TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => (
            CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
            Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                sha256: hash_domain_bytes(
                    RTDS_NATIVE_PAYLOAD_DOMAIN_V1,
                    row.source_native_payload_json.as_bytes(),
                ),
            }),
            (row.freshness_status != typed_raw::RtdsFreshnessStatus::InvalidTimestamp)
                .then_some(row.payload_timestamp),
            row.timestamp_received,
            None,
        ),
        typed_raw::TypedRawRow::BinanceDepthSnapshot(row) => (
            CanonicalSourceKind::BinanceDepthSnapshot,
            Some(SourceNativeIdentityV1::BinanceDepthSnapshot {
                last_update_id: row.last_update_id,
            }),
            None,
            row.timestamp_received,
            Some(collector_admission(
                &row.source_sequence_epoch_id,
                row.collector_admission_sequence,
                row.collector_admission_time_utc_ms,
            )),
        ),
        typed_raw::TypedRawRow::BinanceDepthUpdate(row) => (
            CanonicalSourceKind::BinanceDepthUpdate,
            Some(SourceNativeIdentityV1::BinanceDepthUpdate {
                first_update_id: row.first_update_id,
                final_update_id: row.final_update_id,
            }),
            Some(row.event_time),
            row.timestamp_received,
            Some(collector_admission(
                &row.source_sequence_epoch_id,
                row.collector_admission_sequence,
                row.collector_admission_time_utc_ms,
            )),
        ),
        typed_raw::TypedRawRow::BinanceAggTrade(row) => {
            let (source_native_identity, source_event_at_utc_ms) = match &row.record {
                typed_raw::BinanceAggTradeRecord::Trade {
                    aggregate_trade_id,
                    trade_time,
                    ..
                } => (
                    Some(SourceNativeIdentityV1::BinanceAggregateTrade {
                        aggregate_trade_id: *aggregate_trade_id,
                    }),
                    Some(*trade_time),
                ),
                typed_raw::BinanceAggTradeRecord::InvalidInput { .. } => (None, None),
            };
            (
                CanonicalSourceKind::BinanceAggtrade,
                source_native_identity,
                source_event_at_utc_ms,
                row.timestamp_received,
                Some(collector_admission(
                    &row.source_sequence_epoch_id,
                    row.collector_admission_sequence,
                    row.collector_admission_time_utc_ms,
                )),
            )
        }
    })
}

fn metadata_source_evidence_parts(
    row: &typed_raw::PolymarketMarketMetadataRow,
) -> Result<TypedSourceEvidenceParts, ContractError> {
    Ok((
        CanonicalSourceKind::PolymarketMarketMetadata,
        Some(metadata_semantic_identity(row)?),
        match row.source_evidence_origin {
            Some(typed_raw::MetadataSourceEvidenceOrigin::MarketResolvedWs) => {
                row.ws_platform_timestamp_utc
            }
            _ => None,
        },
        row.timestamp_received,
        None,
    ))
}

pub fn gate_a_capability_matrix_digest(
    matrix: &GateACapabilityMatrixV2,
) -> Result<Sha256Hex, ContractError> {
    validate_gate_a_capability_matrix(matrix)?;
    Ok(hash_domain_bytes(
        CONTRACT_DIGEST_DOMAIN,
        &stable_json(matrix)?,
    ))
}

#[derive(Serialize)]
struct SemanticSourceEvidence<'a> {
    source_kind: CanonicalSourceKind,
    source_native_identity: &'a Option<SourceNativeIdentityV1>,
    source_event_at_utc_ms: Option<i64>,
    received_at_utc_ms: i64,
    collector_admission: &'a Option<CollectorAdmissionCoordinateV1>,
}

#[derive(Serialize)]
struct ExecutionEventIdentityMaterialV3<'a> {
    schema_version: &'a str,
    market: &'a MarketIdentityV2,
    time: &'a EventTimeV2,
    canonical_rule_version: &'a str,
    source_evidence: Vec<SemanticSourceEvidence<'a>>,
    quality: &'a EventQualityV2,
    payload: &'a ExecutionMarketEventPayloadV3,
}

fn execution_event_identity_material_v3(
    event: &ExecutionMarketEventV3,
) -> ExecutionEventIdentityMaterialV3<'_> {
    ExecutionEventIdentityMaterialV3 {
        schema_version: &event.schema_version,
        market: &event.market,
        time: &event.time,
        canonical_rule_version: &event.lineage.canonical_rule_version,
        source_evidence: event
            .lineage
            .source_evidence
            .iter()
            .map(|source| SemanticSourceEvidence {
                source_kind: source.source_kind,
                source_native_identity: &source.source_native_identity,
                source_event_at_utc_ms: source.source_event_at_utc_ms,
                received_at_utc_ms: source.received_at_utc_ms,
                collector_admission: &source.collector_admission,
            })
            .collect(),
        quality: &event.quality,
        payload: &event.payload,
    }
}

pub fn derive_execution_market_event_v3_id(
    event: &ExecutionMarketEventV3,
) -> Result<CanonicalEventId, ContractError> {
    let bytes = stable_json(&execution_event_identity_material_v3(event))?;
    Ok(CanonicalEventId(
        hash_domain_bytes(EXECUTION_EVENT_ID_DOMAIN_V3, &bytes).0,
    ))
}

pub fn validate_event_time_index_entry(entry: &EventTimeIndexEntryV2) -> Result<(), ContractError> {
    if entry.schema_version != EVENT_TIME_INDEX_SCHEMA_V2 {
        return Err(ContractError::UnsupportedSchema {
            contract: "event-time index entry",
            actual: entry.schema_version.clone(),
        });
    }
    validate_event_id(&entry.event_id, "event_id")?;
    if entry.source_event_at_utc_ms <= 0 {
        return Err(invalid("source_event_at_utc_ms", "must be positive"));
    }
    Ok(())
}

pub fn encode_event_time_index_entry(
    entry: &EventTimeIndexEntryV2,
) -> Result<Vec<u8>, ContractError> {
    validate_event_time_index_entry(entry)?;
    stable_json(entry)
}

pub fn decode_event_time_index_entry(bytes: &[u8]) -> Result<EventTimeIndexEntryV2, ContractError> {
    require_schema_version(bytes, EVENT_TIME_INDEX_SCHEMA_V2, "event-time index entry")?;
    let entry =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_event_time_index_entry(&entry)?;
    Ok(entry)
}

pub fn validate_visibility_index_entry(
    entry: &VisibilityIndexEntryV2,
) -> Result<(), ContractError> {
    if entry.schema_version != VISIBILITY_INDEX_SCHEMA_V2 {
        return Err(ContractError::UnsupportedSchema {
            contract: "visibility index entry",
            actual: entry.schema_version.clone(),
        });
    }
    validate_event_id(&entry.event_id, "event_id")?;
    if entry.received_at_utc_ms <= 0 {
        return Err(invalid("received_at_utc_ms", "must be positive"));
    }
    Ok(())
}

pub fn encode_visibility_index_entry(
    entry: &VisibilityIndexEntryV2,
) -> Result<Vec<u8>, ContractError> {
    validate_visibility_index_entry(entry)?;
    stable_json(entry)
}

pub fn decode_visibility_index_entry(
    bytes: &[u8],
) -> Result<VisibilityIndexEntryV2, ContractError> {
    require_schema_version(bytes, VISIBILITY_INDEX_SCHEMA_V2, "visibility index entry")?;
    let entry =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_visibility_index_entry(&entry)?;
    Ok(entry)
}

fn decode_sha256(value: &Sha256Hex, field: &str) -> Result<[u8; 32], ContractError> {
    let bytes = value.0.as_bytes();
    if bytes.len() != 64
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid(
            field,
            "must be 64 lowercase hexadecimal characters",
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).expect("validated ASCII");
        decoded[index] = u8::from_str_radix(text, 16).expect("validated hexadecimal");
    }
    Ok(decoded)
}

fn validate_event_id(value: &CanonicalEventId, field: &str) -> Result<(), ContractError> {
    decode_sha256(&Sha256Hex(value.0.clone()), field).map(|_| ())
}

fn validate_condition_id(value: &str, field: &str) -> Result<(), ContractError> {
    let Some(hex) = value.strip_prefix("0x") else {
        return Err(invalid(field, "must begin with lowercase 0x"));
    };
    if hex.len() != 64
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid(
            field,
            "must contain exactly 32 bytes of lowercase hexadecimal",
        ));
    }
    Ok(())
}

fn validate_unsigned_decimal_id(value: &str, field: &str) -> Result<(), ContractError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || !bytes.iter().all(u8::is_ascii_digit)
        || (bytes.len() > 1 && bytes[0] == b'0')
    {
        return Err(invalid(
            field,
            "must be an unsigned decimal string without leading zeroes",
        ));
    }
    Ok(())
}

fn validate_token_id(value: &TokenId, field: &str) -> Result<(), ContractError> {
    validate_unsigned_decimal_id(&value.0, field)
}

pub fn parse_canonical_decimal(
    value: &ExactDecimal,
    field: &str,
) -> Result<Decimal, ContractError> {
    let text = value.0.as_str();
    if text.is_empty() || text.starts_with('+') || text.contains('e') || text.contains('E') {
        return Err(invalid(
            field,
            "must be a plain base-10 decimal without sign plus or exponent",
        ));
    }
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.as_bytes().iter().all(u8::is_ascii_digit)
        || (integer.len() > 1 && integer.starts_with('0'))
        || fraction.is_some_and(|part| {
            part.is_empty()
                || !part.as_bytes().iter().all(u8::is_ascii_digit)
                || part.ends_with('0')
        })
    {
        return Err(invalid(
            field,
            "must use canonical decimal digits and scale",
        ));
    }
    if text.starts_with('-')
        && unsigned
            .chars()
            .all(|character| character == '0' || character == '.')
    {
        return Err(invalid(field, "negative zero is not canonical"));
    }
    Decimal::from_str(text).map_err(|error| invalid(field, error.to_string()))
}

fn validate_nonnegative_decimal(
    value: &ExactDecimal,
    field: &str,
) -> Result<Decimal, ContractError> {
    let decimal = parse_canonical_decimal(value, field)?;
    if decimal.is_sign_negative() {
        return Err(invalid(field, "must be non-negative"));
    }
    Ok(decimal)
}

fn validate_positive_decimal(value: &ExactDecimal, field: &str) -> Result<Decimal, ContractError> {
    let decimal = validate_nonnegative_decimal(value, field)?;
    if decimal.is_zero() {
        return Err(invalid(field, "must be positive"));
    }
    Ok(decimal)
}

fn validate_asset(asset: &AssetIdentityV1, field: &str) -> Result<(), ContractError> {
    validate_token_id(&asset.token_id, &format!("{field}.token_id"))?;
    if asset.outcome.label.is_empty() {
        return Err(invalid(
            format!("{field}.outcome.label"),
            "must not be empty",
        ));
    }
    Ok(())
}

fn compare_token_ids(left: &TokenId, right: &TokenId) -> Ordering {
    left.0
        .len()
        .cmp(&right.0.len())
        .then_with(|| left.0.cmp(&right.0))
}

fn validate_sorted_assets(values: &[AssetIdentityV1], field: &str) -> Result<(), ContractError> {
    if values.is_empty() {
        return Err(invalid(field, "must contain at least one asset"));
    }
    let mut previous: Option<&TokenId> = None;
    let mut outcome_indices = BTreeSet::new();
    for (index, asset) in values.iter().enumerate() {
        validate_asset(asset, &format!("{field}[{index}]"))?;
        if !outcome_indices.insert(asset.outcome.index) {
            return Err(invalid(
                format!("{field}[{index}].outcome.index"),
                "must be unique across lifecycle assets",
            ));
        }
        if previous.is_some_and(|prior| compare_token_ids(prior, &asset.token_id) != Ordering::Less)
        {
            return Err(ContractError::NonCanonicalOrder {
                field: field.to_owned(),
            });
        }
        previous = Some(&asset.token_id);
    }
    Ok(())
}

fn validate_market(market: &MarketIdentityV2, field: &str) -> Result<(), ContractError> {
    validate_condition_id(&market.condition_id.0, &format!("{field}.condition_id"))?;
    validate_unsigned_decimal_id(&market.clob_market_id.0, &format!("{field}.clob_market_id"))?;
    if market.window.start_at_utc_ms >= market.window.end_at_utc_ms {
        return Err(invalid(
            format!("{field}.window"),
            "start_at_utc_ms must be earlier than end_at_utc_ms",
        ));
    }
    Ok(())
}

fn validate_mapped_lifecycle_state(
    value: &MappedMarketLifecycleStateV1,
    field: &str,
) -> Result<(), ContractError> {
    if let MappedMarketLifecycleStateV1::UnknownUnmodeled { raw_value } = value
        && raw_value.is_empty()
    {
        return Err(invalid(field, "unknown lifecycle code must not be empty"));
    }
    Ok(())
}

fn validate_sorted_event_ids(
    values: &[CanonicalEventId],
    field: &str,
) -> Result<(), ContractError> {
    if values.is_empty() {
        return Err(invalid(field, "must contain at least one source event ID"));
    }
    let mut previous: Option<&str> = None;
    for value in values {
        validate_event_id(value, field)?;
        if let Some(prior) = previous {
            match prior.cmp(&value.0) {
                Ordering::Greater => {
                    return Err(ContractError::NonCanonicalOrder {
                        field: field.to_owned(),
                    });
                }
                Ordering::Equal => {
                    return Err(ContractError::DuplicateValue {
                        field: field.to_owned(),
                        value: value.0.clone(),
                    });
                }
                Ordering::Less => {}
            }
        }
        previous = Some(&value.0);
    }
    Ok(())
}

fn validate_source_native_identity(
    source_kind: CanonicalSourceKind,
    identity: &Option<SourceNativeIdentityV1>,
    field: &str,
) -> Result<(), ContractError> {
    let Some(identity) = identity else {
        return Ok(());
    };
    match (source_kind, identity) {
        (
            CanonicalSourceKind::BinanceAggtrade,
            SourceNativeIdentityV1::BinanceAggregateTrade { aggregate_trade_id },
        ) if *aggregate_trade_id > 0 => {}
        (
            CanonicalSourceKind::BinanceDepthSnapshot,
            SourceNativeIdentityV1::BinanceDepthSnapshot { last_update_id },
        ) if *last_update_id > 0 => {}
        (
            CanonicalSourceKind::BinanceDepthUpdate,
            SourceNativeIdentityV1::BinanceDepthUpdate {
                first_update_id,
                final_update_id,
            },
        ) if *first_update_id > 0 && *final_update_id >= *first_update_id => {}
        (
            CanonicalSourceKind::PolymarketMarketMetadata
            | CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
            SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256 },
        ) => {
            decode_sha256(sha256, &format!("{field}.sha256"))?;
        }
        _ => {
            return Err(invalid(
                field,
                "source-native identity kind or value does not match source_kind",
            ));
        }
    }
    Ok(())
}

fn validate_collector_admission(
    admission: &CollectorAdmissionCoordinateV1,
    field: &str,
) -> Result<(), ContractError> {
    if admission.source_sequence_epoch_id.is_empty() {
        return Err(invalid(
            format!("{field}.source_sequence_epoch_id"),
            "must not be empty",
        ));
    }
    if admission.collector_admission_sequence == 0 {
        return Err(invalid(
            format!("{field}.collector_admission_sequence"),
            "must be positive",
        ));
    }
    if admission.admitted_at_utc_ms <= 0 {
        return Err(invalid(
            format!("{field}.admitted_at_utc_ms"),
            "must be positive",
        ));
    }
    Ok(())
}

fn validate_event_time(time: &EventTimeV2) -> Result<(), ContractError> {
    if time.source_event_at_utc_ms.is_some_and(|value| value <= 0) {
        return Err(invalid(
            "time.source_event_at_utc_ms",
            "must be positive when present",
        ));
    }
    if time.received_at_utc_ms <= 0 {
        return Err(invalid("time.received_at_utc_ms", "must be positive"));
    }
    Ok(())
}

fn semantic_source_evidence(value: &SourceEvidenceRefV2) -> SemanticSourceEvidence<'_> {
    SemanticSourceEvidence {
        source_kind: value.source_kind,
        source_native_identity: &value.source_native_identity,
        source_event_at_utc_ms: value.source_event_at_utc_ms,
        received_at_utc_ms: value.received_at_utc_ms,
        collector_admission: &value.collector_admission,
    }
}

fn validate_semantic_source_evidence(
    value: &SemanticSourceEvidence<'_>,
    index: usize,
) -> Result<(), ContractError> {
    if value.source_event_at_utc_ms.is_some_and(|time| time <= 0) {
        return Err(invalid(
            format!("lineage.source_evidence[{index}].source_event_at_utc_ms"),
            "must be positive when present",
        ));
    }
    if value.source_kind == CanonicalSourceKind::PolymarketOrderbook
        && value.source_event_at_utc_ms.is_none()
    {
        return Err(invalid(
            format!("lineage.source_evidence[{index}].source_event_at_utc_ms"),
            "Polymarket orderbook evidence requires source event time",
        ));
    }
    if value.received_at_utc_ms <= 0 {
        return Err(invalid(
            format!("lineage.source_evidence[{index}].received_at_utc_ms"),
            "must be positive",
        ));
    }
    if let Some(admission) = &value.collector_admission {
        let field = format!("lineage.source_evidence[{index}].collector_admission");
        validate_collector_admission(admission, &field)?;
        if value.received_at_utc_ms > admission.admitted_at_utc_ms {
            return Err(invalid(
                format!("lineage.source_evidence[{index}].received_at_utc_ms"),
                "must not follow collector admission",
            ));
        }
    }
    if value.source_native_identity.is_none() && value.collector_admission.is_none() {
        return Err(invalid(
            format!("lineage.source_evidence[{index}]"),
            "requires a source-native identity or complete collector admission coordinate",
        ));
    }
    validate_source_native_identity(
        value.source_kind,
        &value.source_native_identity,
        &format!("lineage.source_evidence[{index}].source_native_identity"),
    )?;
    Ok(())
}

fn require_source_evidence<T>(values: &[T]) -> Result<(), ContractError> {
    if values.is_empty() {
        return Err(invalid(
            "lineage.source_evidence",
            "must contain at least one source evidence reference",
        ));
    }
    Ok(())
}

fn validate_source_evidence(values: &[SourceEvidenceRefV2]) -> Result<(), ContractError> {
    require_source_evidence(values)?;
    let mut previous: Option<Vec<u8>> = None;
    for (index, value) in values.iter().enumerate() {
        validate_semantic_source_evidence(&semantic_source_evidence(value), index)?;
        decode_sha256(
            &value.input_manifest_sha256,
            &format!("lineage.source_evidence[{index}].input_manifest_sha256"),
        )?;
        decode_sha256(
            &value.input_file_sha256,
            &format!("lineage.source_evidence[{index}].input_file_sha256"),
        )?;
        let current = stable_json(&semantic_source_evidence(value))?;
        if let Some(prior) = &previous {
            match prior.cmp(&current) {
                Ordering::Greater => {
                    return Err(ContractError::NonCanonicalOrder {
                        field: "lineage.source_evidence".to_owned(),
                    });
                }
                Ordering::Equal => {
                    return Err(ContractError::DuplicateValue {
                        field: "lineage.source_evidence".to_owned(),
                        value: format!("entry {index}"),
                    });
                }
                Ordering::Less => {}
            }
        }
        previous = Some(current);
    }
    Ok(())
}

fn validate_event_lineage_and_time(
    time: &EventTimeV2,
    lineage: &EventLineageV2,
) -> Result<(), ContractError> {
    validate_event_time(time)?;
    if lineage.canonical_rule_version.is_empty() {
        return Err(invalid(
            "lineage.canonical_rule_version",
            "must not be empty",
        ));
    }
    validate_source_evidence(&lineage.source_evidence)?;
    let expected_source_event_at = lineage
        .source_evidence
        .iter()
        .filter_map(|source| source.source_event_at_utc_ms)
        .max();
    if time.source_event_at_utc_ms != expected_source_event_at {
        return Err(invalid(
            "time.source_event_at_utc_ms",
            "must equal the latest source event time in lineage evidence",
        ));
    }
    let expected_received_at = lineage
        .source_evidence
        .iter()
        .map(|source| source.received_at_utc_ms)
        .max()
        .expect("source evidence is non-empty after validation");
    if time.received_at_utc_ms != expected_received_at {
        return Err(invalid(
            "time.received_at_utc_ms",
            "must equal the latest receive time in lineage evidence",
        ));
    }
    Ok(())
}

fn validate_snapshot(snapshot: &InitialL2SnapshotV2) -> Result<(), ContractError> {
    validate_asset(&snapshot.asset, "payload.event.asset")?;
    validate_positive_decimal(
        &snapshot.initial_tick_size_pusd_per_share,
        "payload.event.initial_tick_size_pusd_per_share",
    )?;
    if snapshot.source_sequence_epoch_id.is_empty() {
        return Err(invalid(
            "payload.event.source_sequence_epoch_id",
            "must not be empty",
        ));
    }
    decode_sha256(
        &snapshot.book_digest_sha256,
        "payload.event.book_digest_sha256",
    )?;
    let mut prior_bid: Option<Decimal> = None;
    for (index, level) in snapshot.bids.iter().enumerate() {
        let price = validate_positive_decimal(
            &level.price_pusd_per_share,
            &format!("payload.event.bids[{index}].price_pusd_per_share"),
        )?;
        validate_nonnegative_decimal(
            &level.quantity_shares,
            &format!("payload.event.bids[{index}].quantity_shares"),
        )?;
        if prior_bid.is_some_and(|prior| prior <= price) {
            return Err(ContractError::NonCanonicalOrder {
                field: "payload.event.bids".to_owned(),
            });
        }
        prior_bid = Some(price);
    }
    let mut prior_ask: Option<Decimal> = None;
    for (index, level) in snapshot.asks.iter().enumerate() {
        let price = validate_positive_decimal(
            &level.price_pusd_per_share,
            &format!("payload.event.asks[{index}].price_pusd_per_share"),
        )?;
        validate_nonnegative_decimal(
            &level.quantity_shares,
            &format!("payload.event.asks[{index}].quantity_shares"),
        )?;
        if prior_ask.is_some_and(|prior| prior >= price) {
            return Err(ContractError::NonCanonicalOrder {
                field: "payload.event.asks".to_owned(),
            });
        }
        prior_ask = Some(price);
    }
    Ok(())
}

fn validate_level_transition(
    outer_market: &MarketIdentityV2,
    outer_time: &EventTimeV2,
    transition: &LevelExecutionTransitionV3,
) -> Result<(), ContractError> {
    if transition.market != *outer_market {
        return Err(invalid(
            "payload.event.market",
            "must equal the event envelope market identity",
        ));
    }
    validate_asset(&transition.asset, "payload.event.asset")?;
    decode_sha256(&transition.causal_group_id, "payload.event.causal_group_id")?;
    validate_positive_decimal(
        &transition.price_pusd_per_share,
        "payload.event.price_pusd_per_share",
    )?;
    let before = validate_nonnegative_decimal(
        &transition.quantity_before_shares,
        "payload.event.quantity_before_shares",
    )?;
    let after = validate_nonnegative_decimal(
        &transition.quantity_after_shares,
        "payload.event.quantity_after_shares",
    )?;
    validate_nonnegative_decimal(
        &transition.attributed_trade_quantity_shares,
        "payload.event.attributed_trade_quantity_shares",
    )?;
    validate_nonnegative_decimal(
        &transition.already_covered_quantity_shares,
        "payload.event.already_covered_quantity_shares",
    )?;
    let added = validate_nonnegative_decimal(
        &transition.net_added_quantity_shares,
        "payload.event.net_added_quantity_shares",
    )?;
    let reduced = validate_nonnegative_decimal(
        &transition.net_reduced_quantity_shares,
        "payload.event.net_reduced_quantity_shares",
    )?;
    let queue = validate_nonnegative_decimal(
        &transition.queue_consumption_quantity_shares,
        "payload.event.queue_consumption_quantity_shares",
    )?;
    let before_after_added = before
        .checked_add(added)
        .ok_or_else(|| invalid("payload.event", "quantity conservation addition overflowed"))?;
    let after_after_reduced = after
        .checked_add(reduced)
        .ok_or_else(|| invalid("payload.event", "quantity conservation addition overflowed"))?;
    if before_after_added != after_after_reduced {
        return Err(invalid(
            "payload.event",
            "quantity conservation requires before + net_added == after + net_reduced",
        ));
    }
    let (expected_added, expected_reduced) = match after.cmp(&before) {
        Ordering::Greater => (after - before, Decimal::ZERO),
        Ordering::Less => (Decimal::ZERO, before - after),
        Ordering::Equal => (Decimal::ZERO, Decimal::ZERO),
    };
    if added != expected_added || reduced != expected_reduced {
        return Err(invalid(
            "payload.event",
            "net_added and net_reduced must be the unique non-overlapping difference between before and after",
        ));
    }
    if queue > reduced {
        return Err(invalid(
            "payload.event.queue_consumption_quantity_shares",
            "must not exceed net_reduced_quantity_shares",
        ));
    }
    if transition.platform_effective_at_utc_ms <= 0 {
        return Err(invalid(
            "payload.event.platform_effective_at_utc_ms",
            "must be positive",
        ));
    }
    if transition.visible_at_utc_ms <= 0 {
        return Err(invalid(
            "payload.event.visible_at_utc_ms",
            "must be positive",
        ));
    }
    if outer_time.source_event_at_utc_ms != Some(transition.platform_effective_at_utc_ms) {
        return Err(invalid(
            "payload.event.platform_effective_at_utc_ms",
            "must equal the event envelope source event time",
        ));
    }
    if outer_time.received_at_utc_ms != transition.visible_at_utc_ms {
        return Err(invalid(
            "payload.event.visible_at_utc_ms",
            "must equal the event envelope receive time",
        ));
    }
    if transition.basis != LevelExecutionBasis::TradeEvidence
        && transition.source_attribution_quality == SourceAttributionQuality::Exact
    {
        return Err(invalid(
            "payload.event.source_attribution_quality",
            "inference basis cannot claim exact source attribution",
        ));
    }
    if transition.basis != LevelExecutionBasis::TradeEvidence
        && transition.queue_result_quality == QueueResultQuality::Exact
    {
        return Err(invalid(
            "payload.event.queue_result_quality",
            "inference basis cannot claim an exact queue result",
        ));
    }
    Ok(())
}

fn validate_terminal(
    terminal: &MarketResolutionTerminalV3,
    _event_time: &EventTimeV2,
) -> Result<(), ContractError> {
    match terminal {
        MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
        } => {
            if winning_asset_id.is_empty()
                || winning_outcome.is_empty()
                || mapping_rule_version.is_empty()
            {
                return Err(invalid(
                    "payload.event",
                    "resolved identity and mapping rule must not be empty",
                ));
            }
            if payout_vector.len() != 2 {
                return Err(invalid(
                    "payload.event.payout_vector",
                    "btc-updown binary resolution requires exactly two assets",
                ));
            }
            let mut previous_token: Option<&TokenId> = None;
            let mut outcome_indices = BTreeSet::new();
            let mut payout_total = Decimal::ZERO;
            let mut winning_entry_found = false;
            for (index, entry) in payout_vector.iter().enumerate() {
                validate_asset(
                    &entry.asset,
                    &format!("payload.event.payout_vector[{index}].asset"),
                )?;
                if !outcome_indices.insert(entry.asset.outcome.index) {
                    return Err(invalid(
                        format!("payload.event.payout_vector[{index}].asset.outcome.index"),
                        "must be unique across payout assets",
                    ));
                }
                let payout = validate_nonnegative_decimal(
                    &entry.payout_pusd_per_share,
                    &format!("payload.event.payout_vector[{index}].payout_pusd_per_share"),
                )?;
                if previous_token.is_some_and(|prior| {
                    compare_token_ids(prior, &entry.asset.token_id) != Ordering::Less
                }) {
                    return Err(ContractError::NonCanonicalOrder {
                        field: "payload.event.payout_vector".to_owned(),
                    });
                }
                payout_total = payout_total.checked_add(payout).ok_or_else(|| {
                    invalid("payload.event.payout_vector", "payout sum overflowed")
                })?;
                if entry.asset.token_id.0 == *winning_asset_id
                    && entry.asset.outcome.label == *winning_outcome
                    && payout == Decimal::ONE
                {
                    winning_entry_found = true;
                }
                previous_token = Some(&entry.asset.token_id);
            }
            if payout_total != Decimal::ONE || !winning_entry_found {
                return Err(invalid(
                    "payload.event.payout_vector",
                    "binary payout must sum to one and identify the unit winning asset",
                ));
            }
        }
    }
    Ok(())
}

fn validate_payload(event: &ExecutionMarketEventV3) -> Result<(), ContractError> {
    match &event.payload {
        ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) => {
            if let Some(previous) = &transition.previous_state {
                validate_mapped_lifecycle_state(previous, "payload.event.previous_state")?;
            }
            validate_mapped_lifecycle_state(
                &transition.current_state,
                "payload.event.current_state",
            )?;
            validate_sorted_assets(&transition.assets, "payload.event.assets")?;
        }
        ExecutionMarketEventPayloadV3::MarketTradingState(_) => {}
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
            validate_snapshot(snapshot)?;
        }
        ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
            validate_asset(&best.asset, "payload.event.asset")?;
            let bid = best
                .best_bid_pusd_per_share
                .as_ref()
                .map(|value| {
                    validate_positive_decimal(value, "payload.event.best_bid_pusd_per_share")
                })
                .transpose()?;
            let ask = best
                .best_ask_pusd_per_share
                .as_ref()
                .map(|value| {
                    validate_positive_decimal(value, "payload.event.best_ask_pusd_per_share")
                })
                .transpose()?;
            if bid.zip(ask).is_some_and(|(bid, ask)| bid > ask) {
                return Err(invalid(
                    "payload.event",
                    "best bid must not exceed best ask",
                ));
            }
        }
        ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
            validate_asset(&change.asset, "payload.event.asset")?;
            if let Some(old_tick_size) = &change.old_tick_size_pusd_per_share {
                validate_positive_decimal(
                    old_tick_size,
                    "payload.event.old_tick_size_pusd_per_share",
                )?;
            }
            validate_positive_decimal(
                &change.new_tick_size_pusd_per_share,
                "payload.event.new_tick_size_pusd_per_share",
            )?;
        }
        ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
            validate_asset(&trade.asset, "payload.event.asset")?;
            if trade.trade_id.is_empty() {
                return Err(invalid("payload.event.trade_id", "must not be empty"));
            }
            validate_positive_decimal(
                &trade.price_pusd_per_share,
                "payload.event.price_pusd_per_share",
            )?;
            validate_positive_decimal(&trade.quantity_shares, "payload.event.quantity_shares")?;
            if let Some(fee_rate) = &trade.fee_rate_bps {
                validate_nonnegative_decimal(fee_rate, "payload.event.fee_rate_bps")?;
            }
        }
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
            validate_level_transition(&event.market, &event.time, transition)?;
        }
        ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => {
            if let QualityScopeV1::Asset(asset) = &boundary.scope {
                validate_asset(asset, "payload.event.scope.asset")?;
            }
            if boundary.current_quality != event.quality.availability {
                return Err(invalid(
                    "payload.event.current_quality",
                    "must equal the event envelope availability",
                ));
            }
            if boundary.reason_code.is_empty() {
                return Err(invalid("payload.event.reason_code", "must not be empty"));
            }
        }
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => {
            validate_terminal(terminal, &event.time)?;
        }
    }
    Ok(())
}

pub fn validate_execution_market_event_v3(
    event: &ExecutionMarketEventV3,
) -> Result<(), ContractError> {
    if event.schema_version != EXECUTION_MARKET_EVENT_SCHEMA_V3 {
        return Err(ContractError::UnsupportedSchema {
            contract: "execution market event",
            actual: event.schema_version.clone(),
        });
    }
    validate_event_id(&event.event_id, "event_id")?;
    validate_market(&event.market, "market")?;
    validate_event_lineage_and_time(&event.time, &event.lineage)?;
    if event.lineage.source_evidence.iter().any(|source| {
        !matches!(
            source.source_kind,
            CanonicalSourceKind::PolymarketMarketMetadata
                | CanonicalSourceKind::PolymarketOrderbook
        )
    }) {
        return Err(invalid(
            "lineage.source_evidence.source_kind",
            "execution market events only accept Polymarket market evidence",
        ));
    }
    for (index, source) in event.lineage.source_evidence.iter().enumerate() {
        let provenance_matches = match source.source_kind {
            CanonicalSourceKind::PolymarketOrderbook => {
                source.source_native_identity.is_none() && source.collector_admission.is_some()
            }
            CanonicalSourceKind::PolymarketMarketMetadata => {
                matches!(
                    source.source_native_identity.as_ref(),
                    Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { .. })
                ) && source.collector_admission.is_none()
            }
            _ => false,
        };
        if !provenance_matches {
            return Err(invalid(
                format!("lineage.source_evidence[{index}]"),
                "does not match the frozen Polymarket source provenance mapping",
            ));
        }
    }
    validate_payload(event)?;
    if derive_execution_market_event_v3_id(event)? != event.event_id {
        return Err(invalid(
            "event_id",
            "does not match the canonical semantic identity",
        ));
    }
    Ok(())
}

pub fn encode_execution_market_event_v3(
    event: &ExecutionMarketEventV3,
) -> Result<Vec<u8>, ContractError> {
    validate_execution_market_event_v3(event)?;
    stable_json(event)
}

pub fn decode_execution_market_event_v3(
    bytes: &[u8],
) -> Result<ExecutionMarketEventV3, ContractError> {
    require_schema_version(
        bytes,
        EXECUTION_MARKET_EVENT_SCHEMA_V3,
        "execution market event",
    )?;
    let event =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_execution_market_event_v3(&event)?;
    Ok(event)
}

#[derive(Serialize)]
struct ReferenceEventIdentityMaterialV1<'a> {
    schema_version: &'a str,
    time: &'a EventTimeV2,
    canonical_rule_version: &'a str,
    source_evidence: Vec<SemanticSourceEvidence<'a>>,
    quality: &'a EventQualityV2,
    payload: &'a CanonicalReferenceEventPayloadV1,
}

fn reference_event_identity_material_v1(
    event: &CanonicalReferenceEventV1,
) -> ReferenceEventIdentityMaterialV1<'_> {
    ReferenceEventIdentityMaterialV1 {
        schema_version: &event.schema_version,
        time: &event.time,
        canonical_rule_version: &event.lineage.canonical_rule_version,
        source_evidence: event
            .lineage
            .source_evidence
            .iter()
            .map(semantic_source_evidence)
            .collect(),
        quality: &event.quality,
        payload: &event.payload,
    }
}

pub fn derive_reference_event_v1_id(
    event: &CanonicalReferenceEventV1,
) -> Result<CanonicalEventId, ContractError> {
    let bytes = stable_json(&reference_event_identity_material_v1(event))?;
    Ok(CanonicalEventId(
        hash_domain_bytes(REFERENCE_EVENT_ID_DOMAIN_V1, &bytes).0,
    ))
}

fn finalize_reference_event_v1(
    evidence: SourceEvidenceRefV2,
    canonical_rule_version: &str,
    quality: EventQualityV2,
    payload: CanonicalReferenceEventPayloadV1,
) -> Result<CanonicalReferenceEventV1, ContractError> {
    let mut event = CanonicalReferenceEventV1 {
        schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        time: EventTimeV2 {
            source_event_at_utc_ms: evidence.source_event_at_utc_ms,
            received_at_utc_ms: evidence.received_at_utc_ms,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: canonical_rule_version.to_owned(),
            source_evidence: vec![evidence],
        },
        quality,
        payload,
    };
    event.event_id = derive_reference_event_v1_id(&event)?;
    validate_reference_event_v1(&event)?;
    Ok(event)
}

pub fn build_chainlink_reference_event_v1(
    row: &typed_raw::PolymarketRtdsChainlinkBtcUsdRow,
    input_manifest_sha256: &Sha256Hex,
    input_file_sha256: &Sha256Hex,
    canonical_rule_version: &str,
) -> Result<CanonicalReferenceEventV1, ContractError> {
    let availability = match row.freshness_status {
        typed_raw::RtdsFreshnessStatus::Fresh => MarketDataQuality::Fresh,
        typed_raw::RtdsFreshnessStatus::Stale => MarketDataQuality::Stale,
        _ => {
            return Err(invalid(
                "typed_raw.freshness_status",
                "invalid RTDS rows cannot form a Chainlink reference update",
            ));
        }
    };
    let evidence = source_evidence_from_typed_raw(
        &typed_raw::TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row.clone()),
        input_manifest_sha256,
        input_file_sha256,
    )?;
    let price_usd_per_btc =
        typed_raw::normalize_decimal_text(&row.value, "value").map_err(typed_raw_contract_error)?;
    finalize_reference_event_v1(
        evidence,
        canonical_rule_version,
        EventQualityV2 {
            availability,
            resync: ResyncQuality::NotRequired,
        },
        CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1 {
            symbol: row.symbol.clone(),
            price_usd_per_btc,
            message_timestamp_utc_ms: row.message_timestamp,
        }),
    )
}

pub fn build_binance_aggregate_trade_reference_event_v1(
    row: &typed_raw::BinanceAggTradeRow,
    input_manifest_sha256: &Sha256Hex,
    input_file_sha256: &Sha256Hex,
    canonical_rule_version: &str,
) -> Result<CanonicalReferenceEventV1, ContractError> {
    if !matches!(row.record, typed_raw::BinanceAggTradeRecord::Trade { .. }) {
        return Err(invalid(
            "typed_raw.record",
            "invalid aggTrade rows cannot form aggregate-trade reference updates",
        ));
    }
    let evidence = source_evidence_from_typed_raw(
        &typed_raw::TypedRawRow::BinanceAggTrade(row.clone()),
        input_manifest_sha256,
        input_file_sha256,
    )?;
    finalize_reference_event_v1(
        evidence,
        canonical_rule_version,
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::NotRequired,
        },
        CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(
            prepare_binance_aggregate_trade_payload(row)?,
        ),
    )
}

pub(crate) fn prepare_binance_aggregate_trade_payload(
    row: &typed_raw::BinanceAggTradeRow,
) -> Result<BinanceAggregateTradeReferenceV1, ContractError> {
    let typed_raw::BinanceAggTradeRecord::Trade {
        event_time,
        symbol,
        aggregate_trade_id,
        price,
        quantity,
        first_trade_id,
        last_trade_id,
        buyer_is_maker,
        ..
    } = &row.record
    else {
        return Err(invalid(
            "typed_raw.record",
            "invalid aggTrade rows cannot form aggregate-trade reference updates",
        ));
    };
    let price_usdt_per_btc =
        typed_raw::normalize_decimal_text(price, "price").map_err(typed_raw_contract_error)?;
    let quantity_btc = typed_raw::normalize_decimal_text(quantity, "quantity")
        .map_err(typed_raw_contract_error)?;
    Ok(BinanceAggregateTradeReferenceV1 {
        symbol: symbol.clone(),
        aggregate_trade_id: *aggregate_trade_id,
        first_trade_id: *first_trade_id,
        last_trade_id: *last_trade_id,
        price_usdt_per_btc,
        quantity_btc,
        buyer_is_maker: *buyer_is_maker,
        event_time_utc_ms: *event_time,
    })
}

fn validate_reference_levels(
    bids: &[ReferencePriceLevelV1],
    asks: &[ReferencePriceLevelV1],
) -> Result<(), ContractError> {
    let mut previous_bid: Option<Decimal> = None;
    for (index, level) in bids.iter().enumerate() {
        let price = validate_positive_decimal(
            &level.price_usdt_per_btc,
            &format!("payload.reference.bids[{index}].price_usdt_per_btc"),
        )?;
        validate_nonnegative_decimal(
            &level.quantity_btc,
            &format!("payload.reference.bids[{index}].quantity_btc"),
        )?;
        if previous_bid.is_some_and(|previous| previous <= price) {
            return Err(ContractError::NonCanonicalOrder {
                field: "payload.reference.bids".to_owned(),
            });
        }
        previous_bid = Some(price);
    }
    let mut previous_ask: Option<Decimal> = None;
    for (index, level) in asks.iter().enumerate() {
        let price = validate_positive_decimal(
            &level.price_usdt_per_btc,
            &format!("payload.reference.asks[{index}].price_usdt_per_btc"),
        )?;
        validate_nonnegative_decimal(
            &level.quantity_btc,
            &format!("payload.reference.asks[{index}].quantity_btc"),
        )?;
        if previous_ask.is_some_and(|previous| previous >= price) {
            return Err(ContractError::NonCanonicalOrder {
                field: "payload.reference.asks".to_owned(),
            });
        }
        previous_ask = Some(price);
    }
    Ok(())
}

fn validate_reference_payload(event: &CanonicalReferenceEventV1) -> Result<(), ContractError> {
    match &event.payload {
        CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(reference) => {
            if reference.symbol.is_empty() {
                return Err(invalid("payload.reference.symbol", "must not be empty"));
            }
            validate_positive_decimal(
                &reference.price_usd_per_btc,
                "payload.reference.price_usd_per_btc",
            )?;
            if reference.message_timestamp_utc_ms <= 0 {
                return Err(invalid(
                    "payload.reference.message_timestamp_utc_ms",
                    "must be positive",
                ));
            }
            let [source] = event.lineage.source_evidence.as_slice() else {
                return Err(invalid(
                    "lineage.source_evidence",
                    "a Chainlink reference update requires exactly one direct RTDS evidence item",
                ));
            };
            if source.source_kind != CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd
                || !matches!(
                    source.source_native_identity.as_ref(),
                    Some(SourceNativeIdentityV1::SemanticEvidenceSha256 { .. })
                )
                || source.collector_admission.is_some()
                || source.source_event_at_utc_ms.is_none()
            {
                return Err(invalid(
                    "lineage.source_evidence[0]",
                    "Chainlink reference evidence requires its semantic payload identity, real payload time, and no fabricated collector admission coordinate",
                ));
            }
        }
        CanonicalReferenceEventPayloadV1::BinanceDepthState(reference) => {
            if reference.symbol.is_empty() || reference.last_update_id == 0 {
                return Err(invalid(
                    "payload.reference",
                    "Binance depth symbol and last_update_id are required",
                ));
            }
            validate_reference_levels(&reference.bids, &reference.asks)?;
            let mut terminal_update_id_is_present = false;
            for (index, source) in event.lineage.source_evidence.iter().enumerate() {
                let source_matches =
                    match (source.source_kind, source.source_native_identity.as_ref()) {
                        (
                            CanonicalSourceKind::BinanceDepthSnapshot,
                            Some(SourceNativeIdentityV1::BinanceDepthSnapshot { last_update_id }),
                        ) => {
                            terminal_update_id_is_present |=
                                *last_update_id == reference.last_update_id;
                            source.source_event_at_utc_ms.is_none()
                                && source.collector_admission.is_some()
                        }
                        (
                            CanonicalSourceKind::BinanceDepthUpdate,
                            Some(SourceNativeIdentityV1::BinanceDepthUpdate {
                                final_update_id,
                                ..
                            }),
                        ) => {
                            terminal_update_id_is_present |=
                                *final_update_id == reference.last_update_id;
                            source.source_event_at_utc_ms.is_some()
                                && source.collector_admission.is_some()
                        }
                        _ => false,
                    };
                if !source_matches {
                    return Err(invalid(
                        format!("lineage.source_evidence[{index}]"),
                        "does not match the frozen Binance depth source provenance and time mapping",
                    ));
                }
            }
            if !terminal_update_id_is_present {
                return Err(invalid(
                    "payload.reference.last_update_id",
                    "must equal a directly evidenced snapshot or update terminal ID",
                ));
            }
        }
        CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(reference) => {
            if reference.symbol.is_empty()
                || reference.aggregate_trade_id == 0
                || reference.first_trade_id == 0
                || reference.last_trade_id < reference.first_trade_id
                || reference.event_time_utc_ms <= 0
            {
                return Err(invalid(
                    "payload.reference",
                    "Binance aggregate trade identity and event time are invalid",
                ));
            }
            validate_positive_decimal(
                &reference.price_usdt_per_btc,
                "payload.reference.price_usdt_per_btc",
            )?;
            validate_positive_decimal(&reference.quantity_btc, "payload.reference.quantity_btc")?;
            let [source] = event.lineage.source_evidence.as_slice() else {
                return Err(invalid(
                    "lineage.source_evidence",
                    "a Binance aggregate trade reference requires exactly one direct evidence item",
                ));
            };
            if source.source_kind != CanonicalSourceKind::BinanceAggtrade
                || !matches!(
                    source.source_native_identity.as_ref(),
                    Some(SourceNativeIdentityV1::BinanceAggregateTrade { aggregate_trade_id })
                        if *aggregate_trade_id == reference.aggregate_trade_id
                )
                || source.source_event_at_utc_ms.is_none()
                || source.collector_admission.is_none()
            {
                return Err(invalid(
                    "lineage.source_evidence[0]",
                    "must carry the matching Binance aggregate_trade_id, real trade time, and collector admission coordinate",
                ));
            }
        }
    }
    Ok(())
}

pub fn validate_reference_event_v1(event: &CanonicalReferenceEventV1) -> Result<(), ContractError> {
    if event.schema_version != CANONICAL_REFERENCE_EVENT_SCHEMA_V1 {
        return Err(ContractError::UnsupportedSchema {
            contract: "canonical reference event",
            actual: event.schema_version.clone(),
        });
    }
    validate_event_id(&event.event_id, "event_id")?;
    validate_event_lineage_and_time(&event.time, &event.lineage)?;
    validate_reference_payload(event)?;
    if derive_reference_event_v1_id(event)? != event.event_id {
        return Err(invalid(
            "event_id",
            "does not match the canonical reference identity",
        ));
    }
    Ok(())
}

pub fn encode_reference_event_v1(
    event: &CanonicalReferenceEventV1,
) -> Result<Vec<u8>, ContractError> {
    validate_reference_event_v1(event)?;
    stable_json(event)
}

pub fn decode_reference_event_v1(bytes: &[u8]) -> Result<CanonicalReferenceEventV1, ContractError> {
    require_schema_version(
        bytes,
        CANONICAL_REFERENCE_EVENT_SCHEMA_V1,
        "canonical reference event",
    )?;
    let event =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_reference_event_v1(&event)?;
    Ok(event)
}

pub fn validate_canonical_event_record_v1(
    record: &CanonicalEventRecordV1,
) -> Result<(), ContractError> {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
            validate_execution_market_event_v3(event)
        }
        CanonicalEventRecordV1::ReferenceEvent(event) => validate_reference_event_v1(event),
    }
}

pub fn encode_canonical_event_record_v1(
    record: &CanonicalEventRecordV1,
) -> Result<Vec<u8>, ContractError> {
    validate_canonical_event_record_v1(record)?;
    stable_json(record)
}

pub fn decode_canonical_event_record_v1(
    bytes: &[u8],
) -> Result<CanonicalEventRecordV1, ContractError> {
    let record =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_canonical_event_record_v1(&record)?;
    Ok(record)
}

fn record_identity_and_time(record: &CanonicalEventRecordV1) -> (&CanonicalEventId, &EventTimeV2) {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => (&event.event_id, &event.time),
        CanonicalEventRecordV1::ReferenceEvent(event) => (&event.event_id, &event.time),
    }
}

pub fn project_record_event_time_index_entry(
    record: &CanonicalEventRecordV1,
) -> Result<Option<EventTimeIndexEntryV2>, ContractError> {
    validate_canonical_event_record_v1(record)?;
    let (event_id, time) = record_identity_and_time(record);
    Ok(time
        .source_event_at_utc_ms
        .map(|source_event_at_utc_ms| EventTimeIndexEntryV2 {
            schema_version: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
            event_id: event_id.clone(),
            source_event_at_utc_ms,
        }))
}

pub fn project_record_visibility_index_entry(
    record: &CanonicalEventRecordV1,
) -> Result<VisibilityIndexEntryV2, ContractError> {
    validate_canonical_event_record_v1(record)?;
    let (event_id, time) = record_identity_and_time(record);
    Ok(VisibilityIndexEntryV2 {
        schema_version: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        event_id: event_id.clone(),
        received_at_utc_ms: time.received_at_utc_ms,
    })
}

#[derive(Serialize)]
struct ObservationIdentityMaterial<'a> {
    schema_version: &'a str,
    market: &'a MarketIdentityV2,
    lineage: &'a ObservationLineageV3,
    visibility_cutoff: &'a ObservationVisibilityCutoffV2,
}

#[derive(Serialize)]
struct ObservationHashMaterial<'a> {
    schema_version: &'a str,
    observation_id: &'a Sha256Hex,
    market: &'a MarketIdentityV2,
    lineage: &'a ObservationLineageV3,
    visibility_cutoff: &'a ObservationVisibilityCutoffV2,
    market_state_digest_sha256: &'a Sha256Hex,
    component_states: &'a [ObservationComponentStateV2],
    missing_mask: &'a [ObservationComponentId],
}

pub fn derive_observation_v3_id(
    observation: &MarketObservationCausalShellV3,
) -> Result<Sha256Hex, ContractError> {
    let material = ObservationIdentityMaterial {
        schema_version: &observation.schema_version,
        market: &observation.market,
        lineage: &observation.lineage,
        visibility_cutoff: &observation.visibility_cutoff,
    };
    Ok(hash_domain_bytes(
        OBSERVATION_ID_DOMAIN_V3,
        &stable_json(&material)?,
    ))
}

pub fn derive_observation_v3_hash(
    observation: &MarketObservationCausalShellV3,
) -> Result<Sha256Hex, ContractError> {
    let material = ObservationHashMaterial {
        schema_version: &observation.schema_version,
        observation_id: &observation.observation_id,
        market: &observation.market,
        lineage: &observation.lineage,
        visibility_cutoff: &observation.visibility_cutoff,
        market_state_digest_sha256: &observation.market_state_digest_sha256,
        component_states: &observation.component_states,
        missing_mask: &observation.missing_mask,
    };
    Ok(hash_domain_bytes(
        OBSERVATION_HASH_DOMAIN_V3,
        &stable_json(&material)?,
    ))
}

fn validate_sorted_unique_component_ids(
    values: &[ObservationComponentId],
    field: &str,
) -> Result<(), ContractError> {
    let mut previous = None;
    for value in values {
        if previous.is_some_and(|prior| prior >= *value) {
            return Err(ContractError::NonCanonicalOrder {
                field: field.to_owned(),
            });
        }
        previous = Some(*value);
    }
    Ok(())
}

pub const REQUIRED_OBSERVATION_COMPONENTS: [ObservationComponentId; 9] = [
    ObservationComponentId::PolymarketL2,
    ObservationComponentId::BestBidAsk,
    ObservationComponentId::CurrentTick,
    ObservationComponentId::PublicTrade,
    ObservationComponentId::MarketLifecycle,
    ObservationComponentId::MarketResolution,
    ObservationComponentId::ChainlinkBtcUsd,
    ObservationComponentId::BinanceBtcUsdtDepth,
    ObservationComponentId::BinanceBtcUsdtTrade,
];

pub fn validate_market_observation_causal_shell_v3(
    observation: &MarketObservationCausalShellV3,
) -> Result<(), ContractError> {
    if observation.schema_version != MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3 {
        return Err(ContractError::UnsupportedSchema {
            contract: "market observation causal shell",
            actual: observation.schema_version.clone(),
        });
    }
    decode_sha256(&observation.observation_id, "observation_id")?;
    validate_market(&observation.market, "market")?;
    if observation.lineage.canonical_event_record_contract_version
        != CANONICAL_EVENT_RECORD_SCHEMA_V1
        || observation.lineage.execution_event_contract_version != EXECUTION_MARKET_EVENT_SCHEMA_V3
        || observation.lineage.reference_event_contract_version
            != CANONICAL_REFERENCE_EVENT_SCHEMA_V1
    {
        return Err(invalid(
            "lineage.contract_version",
            "must bind canonical record V1, execution event V3, and reference event V1",
        ));
    }
    decode_sha256(
        &observation.lineage.input_set_identity_sha256,
        "lineage.input_set_identity_sha256",
    )?;
    decode_sha256(
        &observation.lineage.visibility_index_manifest_sha256,
        "lineage.visibility_index_manifest_sha256",
    )?;
    validate_sorted_event_ids_allow_empty(
        &observation.lineage.contributing_event_ids,
        "lineage.contributing_event_ids",
    )?;
    if observation.visibility_cutoff.received_at_utc_ms <= 0 {
        return Err(invalid(
            "visibility_cutoff.received_at_utc_ms",
            "must be positive",
        ));
    }
    decode_sha256(
        &observation.market_state_digest_sha256,
        "market_state_digest_sha256",
    )?;
    decode_sha256(&observation.canonical_hash_sha256, "canonical_hash_sha256")?;
    validate_sorted_unique_component_ids(&observation.missing_mask, "missing_mask")?;
    if observation.component_states.len() != REQUIRED_OBSERVATION_COMPONENTS.len() {
        return Err(invalid(
            "component_states",
            "must contain each frozen market-side component exactly once",
        ));
    }
    let missing = observation
        .missing_mask
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut previous_component = None;
    for (index, (component, required)) in observation
        .component_states
        .iter()
        .zip(REQUIRED_OBSERVATION_COMPONENTS)
        .enumerate()
    {
        if component.component != required
            || previous_component.is_some_and(|previous| previous >= component.component)
        {
            return Err(ContractError::NonCanonicalOrder {
                field: "component_states".to_owned(),
            });
        }
        previous_component = Some(component.component);
        let is_visible = matches!(
            component.quality,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        );
        if is_visible {
            let Some(received_at) = component.received_at_utc_ms else {
                return Err(invalid(
                    format!("component_states[{index}].received_at_utc_ms"),
                    "fresh or stale component requires a receive time",
                ));
            };
            if received_at <= 0 {
                return Err(invalid(
                    format!("component_states[{index}].received_at_utc_ms"),
                    "must be positive",
                ));
            }
            if received_at > observation.visibility_cutoff.received_at_utc_ms {
                return Err(invalid(
                    format!("component_states[{index}].received_at_utc_ms"),
                    "must not be later than the visibility cutoff",
                ));
            }
            let metrics_are_valid = if let Some(source_event_at) = component.source_event_at_utc_ms
            {
                if source_event_at <= 0 {
                    return Err(invalid(
                        format!("component_states[{index}].source_event_at_utc_ms"),
                        "must be positive when present",
                    ));
                }
                optional_delta_is_valid(
                    component.information_age_ms,
                    observation.visibility_cutoff.received_at_utc_ms,
                    source_event_at,
                ) && optional_delta_is_valid(
                    component.observed_transport_delay_ms,
                    received_at,
                    source_event_at,
                )
            } else {
                component.information_age_ms.is_none()
                    && component.observed_transport_delay_ms.is_none()
            };
            if !metrics_are_valid
                || component.last_visible_event_id.is_none()
                || component.state_digest_sha256.is_none()
                || missing.contains(&component.component)
            {
                return Err(invalid(
                    format!("component_states[{index}]"),
                    "visible component requires consistent time evidence, event, digest, and no missing mask",
                ));
            }
            if !observation.lineage.contributing_event_ids.contains(
                component
                    .last_visible_event_id
                    .as_ref()
                    .expect("checked above"),
            ) {
                return Err(invalid(
                    format!("component_states[{index}].last_visible_event_id"),
                    "must belong to lineage.contributing_event_ids",
                ));
            }
        } else {
            if !missing.contains(&component.component) {
                return Err(invalid(
                    format!("component_states[{index}]"),
                    "non-visible component must be present in missing_mask",
                ));
            }
            if component.source_event_at_utc_ms.is_some()
                || component.received_at_utc_ms.is_some()
                || component.information_age_ms.is_some()
                || component.observed_transport_delay_ms.is_some()
                || component.last_visible_event_id.is_some()
                || component.state_digest_sha256.is_some()
            {
                return Err(invalid(
                    format!("component_states[{index}]"),
                    "non-visible component cannot carry hidden state or future evidence",
                ));
            }
        }
        if let Some(event_id) = &component.last_visible_event_id {
            validate_event_id(
                event_id,
                &format!("component_states[{index}].last_visible_event_id"),
            )?;
        }
        if let Some(digest) = &component.state_digest_sha256 {
            decode_sha256(
                digest,
                &format!("component_states[{index}].state_digest_sha256"),
            )?;
        }
    }
    if derive_observation_v3_id(observation)? != observation.observation_id {
        return Err(invalid(
            "observation_id",
            "does not match the causal observation identity",
        ));
    }
    if derive_observation_v3_hash(observation)? != observation.canonical_hash_sha256 {
        return Err(invalid(
            "canonical_hash_sha256",
            "does not match the canonical observation content",
        ));
    }
    Ok(())
}

pub fn validate_market_observation_v3_visibility_index_entries(
    observation: &MarketObservationCausalShellV3,
    visibility_entries: &[VisibilityIndexEntryV2],
) -> Result<(), ContractError> {
    validate_market_observation_causal_shell_v3(observation)?;
    if visibility_entries.len() != observation.lineage.contributing_event_ids.len() {
        return Err(invalid(
            "visibility_index_entries",
            "must contain exactly one entry for each contributing event",
        ));
    }

    let contributing_event_ids = observation
        .lineage
        .contributing_event_ids
        .iter()
        .map(|event_id| event_id.0.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_event_ids = BTreeSet::new();
    for (index, entry) in visibility_entries.iter().enumerate() {
        validate_visibility_index_entry(entry)?;
        if !contributing_event_ids.contains(entry.event_id.0.as_str()) {
            return Err(invalid(
                format!("visibility_index_entries[{index}].event_id"),
                "must belong to lineage.contributing_event_ids",
            ));
        }
        if !seen_event_ids.insert(entry.event_id.0.as_str()) {
            return Err(ContractError::DuplicateValue {
                field: "visibility_index_entries.event_id".to_owned(),
                value: entry.event_id.0.clone(),
            });
        }
        if entry.received_at_utc_ms > observation.visibility_cutoff.received_at_utc_ms {
            return Err(invalid(
                format!("visibility_index_entries[{index}].received_at_utc_ms"),
                "must not be later than the visibility cutoff",
            ));
        }
    }
    Ok(())
}

fn optional_delta_is_valid(actual: Option<u64>, later: i64, earlier: i64) -> bool {
    let expected = later
        .checked_sub(earlier)
        .and_then(|delta| u64::try_from(delta).ok());
    actual.is_none() || actual == expected
}

fn validate_sorted_event_ids_allow_empty(
    values: &[CanonicalEventId],
    field: &str,
) -> Result<(), ContractError> {
    if values.is_empty() {
        return Ok(());
    }
    validate_sorted_event_ids(values, field)
}

pub fn encode_market_observation_causal_shell_v3(
    observation: &MarketObservationCausalShellV3,
) -> Result<Vec<u8>, ContractError> {
    validate_market_observation_causal_shell_v3(observation)?;
    stable_json(observation)
}

pub fn decode_market_observation_causal_shell_v3(
    bytes: &[u8],
) -> Result<MarketObservationCausalShellV3, ContractError> {
    require_schema_version(
        bytes,
        MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3,
        "market observation causal shell",
    )?;
    let observation =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_market_observation_causal_shell_v3(&observation)?;
    Ok(observation)
}

pub const REQUIRED_GATE_A_CAPABILITIES: [GateACapabilityId; 16] = [
    GateACapabilityId::InputAdmissionAndSixSchemaMapping,
    GateACapabilityId::CanonicalIdentityAndDualTimeProjection,
    GateACapabilityId::MarketIdentityWindowAndLifecycle,
    GateACapabilityId::InitialL2SnapshotEpochCursorAndDigest,
    GateACapabilityId::PriceLevelQuantityTransition,
    GateACapabilityId::BestBidAsk,
    GateACapabilityId::DynamicTickSize,
    GateACapabilityId::PublicTradeEvidence,
    GateACapabilityId::LevelExecutionTransition,
    GateACapabilityId::QualityGapResyncAndWatermark,
    GateACapabilityId::MarketResolutionTerminal,
    GateACapabilityId::ObservationSharedLineageAndVisibilityCutoff,
    GateACapabilityId::ImmutableEventStoreAndDualIndexes,
    GateACapabilityId::DeterminismAndSourceTraceability,
    GateACapabilityId::BoundedResourcesAndAtomicPublication,
    GateACapabilityId::ForbiddenLowFrequencyFields,
];

fn validate_sorted_unique_strings(
    values: &[String],
    field: &str,
    allow_empty: bool,
) -> Result<(), ContractError> {
    if !allow_empty && values.is_empty() {
        return Err(invalid(field, "must not be empty"));
    }
    let mut previous: Option<&str> = None;
    for value in values {
        if value.trim().is_empty() {
            return Err(invalid(field, "entries must not be empty"));
        }
        if previous.is_some_and(|prior| prior >= value.as_str()) {
            return Err(ContractError::NonCanonicalOrder {
                field: field.to_owned(),
            });
        }
        previous = Some(value);
    }
    Ok(())
}

pub fn validate_gate_a_capability_matrix(
    matrix: &GateACapabilityMatrixV2,
) -> Result<(), ContractError> {
    if matrix.schema_version != GATE_A_CAPABILITY_MATRIX_SCHEMA_V2 {
        return Err(ContractError::UnsupportedSchema {
            contract: "Gate A capability matrix",
            actual: matrix.schema_version.clone(),
        });
    }
    if matrix.entries.len() != REQUIRED_GATE_A_CAPABILITIES.len() {
        return Err(invalid(
            "entries",
            "must contain each required Gate A capability exactly once",
        ));
    }
    for (index, (entry, required)) in matrix
        .entries
        .iter()
        .zip(REQUIRED_GATE_A_CAPABILITIES)
        .enumerate()
    {
        if entry.capability != required {
            return Err(invalid(
                format!("entries[{index}].capability"),
                "capabilities must use the frozen complete order",
            ));
        }
        validate_sorted_unique_strings(
            &entry.contract_field_paths,
            &format!("entries[{index}].contract_field_paths"),
            false,
        )?;
        validate_sorted_unique_strings(
            &entry.evidence_ids,
            &format!("entries[{index}].evidence_ids"),
            entry.status != CapabilityStatus::Pass,
        )?;
        match entry.status {
            CapabilityStatus::Pass => {
                if entry.evidence_ids.is_empty() {
                    return Err(invalid(
                        format!("entries[{index}].evidence_ids"),
                        "PASS capability requires evidence",
                    ));
                }
                if entry.blocking_reason.is_some() {
                    return Err(invalid(
                        format!("entries[{index}].blocking_reason"),
                        "PASS capability cannot have a blocking reason",
                    ));
                }
            }
            CapabilityStatus::Fail => {
                if entry
                    .blocking_reason
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
                {
                    return Err(invalid(
                        format!("entries[{index}].blocking_reason"),
                        "FAIL capability requires a blocking reason",
                    ));
                }
            }
        }
    }
    Ok(())
}

pub fn encode_gate_a_capability_matrix(
    matrix: &GateACapabilityMatrixV2,
) -> Result<Vec<u8>, ContractError> {
    validate_gate_a_capability_matrix(matrix)?;
    stable_json(matrix)
}

pub fn decode_gate_a_capability_matrix(
    bytes: &[u8],
) -> Result<GateACapabilityMatrixV2, ContractError> {
    require_schema_version(
        bytes,
        GATE_A_CAPABILITY_MATRIX_SCHEMA_V2,
        "Gate A capability matrix",
    )?;
    let matrix =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_gate_a_capability_matrix(&matrix)?;
    Ok(matrix)
}

fn validate_receipt_digest(value: &Sha256Hex, field: &str) -> Result<(), ContractError> {
    decode_sha256(value, field).map(|_| ())
}

pub fn derive_gate_a_receipt_v3_id(receipt: &GateAReceiptV3) -> Result<Sha256Hex, ContractError> {
    let mut material = receipt.clone();
    material.receipt_id = Sha256Hex("0".repeat(64));
    Ok(hash_domain_bytes(
        GATE_A_RECEIPT_ID_DOMAIN_V3,
        &stable_json(&material)?,
    ))
}

pub fn derive_dpt_observation_receipt_v1_id(
    receipt: &DptObservationReceiptV1,
) -> Result<Sha256Hex, ContractError> {
    let mut material = receipt.clone();
    material.receipt_id = Sha256Hex("0".repeat(64));
    Ok(hash_domain_bytes(
        b"dpt-observation-receipt-v1\0",
        &stable_json(&material)?,
    ))
}

pub fn validate_dpt_observation_receipt_v1(
    receipt: &DptObservationReceiptV1,
) -> Result<(), ContractError> {
    if receipt.schema_version != DPT_OBSERVATION_RECEIPT_SCHEMA_V1 {
        return Err(ContractError::UnsupportedSchema {
            contract: "DPT observation receipt",
            actual: receipt.schema_version.clone(),
        });
    }
    for (field, digest) in [
        ("receipt_id", &receipt.receipt_id),
        (
            "pipeline_core_identity_sha256",
            &receipt.pipeline_core_identity_sha256,
        ),
        (
            "input_set_identity_sha256",
            &receipt.input_set_identity_sha256,
        ),
        (
            "input_manifest_set_sha256",
            &receipt.input_manifest_set_sha256,
        ),
        (
            "observation_run_manifest_sha256",
            &receipt.observation_run_manifest_sha256,
        ),
    ] {
        validate_receipt_digest(digest, field)?;
    }
    if receipt.training_dataset_ready {
        return Err(invalid(
            "training_dataset_ready",
            "观测凭据不能签发训练数据集就绪",
        ));
    }
    if derive_dpt_observation_receipt_v1_id(receipt)? != receipt.receipt_id {
        return Err(invalid("receipt_id", "与观测凭据的内容摘要不一致"));
    }
    Ok(())
}

pub fn encode_dpt_observation_receipt_v1(
    receipt: &DptObservationReceiptV1,
) -> Result<Vec<u8>, ContractError> {
    validate_dpt_observation_receipt_v1(receipt)?;
    stable_json(receipt)
}

pub fn decode_dpt_observation_receipt_v1(
    bytes: &[u8],
) -> Result<DptObservationReceiptV1, ContractError> {
    require_schema_version(
        bytes,
        DPT_OBSERVATION_RECEIPT_SCHEMA_V1,
        "DPT observation receipt",
    )?;
    let receipt =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_dpt_observation_receipt_v1(&receipt)?;
    Ok(receipt)
}

pub fn validate_gate_a_receipt_v3(
    receipt: &GateAReceiptV3,
    matrix: &GateACapabilityMatrixV2,
) -> Result<(), ContractError> {
    validate_gate_a_capability_matrix(matrix)?;
    if receipt.schema_version != GATE_A_RECEIPT_SCHEMA_V3 {
        return Err(ContractError::UnsupportedSchema {
            contract: "Gate A receipt",
            actual: receipt.schema_version.clone(),
        });
    }
    validate_receipt_digest(&receipt.receipt_id, "receipt_id")?;
    if receipt.requirement_id != "DATA-PIPELINE-TRAINING-20260803" {
        return Err(invalid(
            "requirement_id",
            "must bind DATA-PIPELINE-TRAINING-20260803",
        ));
    }
    if receipt.canonical_event_record_contract_version != CANONICAL_EVENT_RECORD_SCHEMA_V1
        || receipt.execution_event_contract_version != EXECUTION_MARKET_EVENT_SCHEMA_V3
        || receipt.reference_event_contract_version != CANONICAL_REFERENCE_EVENT_SCHEMA_V1
        || receipt.observation_contract_version != MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3
        || receipt.event_time_index_contract_version != EVENT_TIME_INDEX_SCHEMA_V2
        || receipt.visibility_index_contract_version != VISIBILITY_INDEX_SCHEMA_V2
    {
        return Err(invalid(
            "contract_version",
            "must bind the current record, event, observation, and dual-time index contracts",
        ));
    }
    if receipt.dataset_lineage_root != DATASET_LINEAGE_ROOT
        || receipt.dataset_date_floor != DATASET_DATE_FLOOR
    {
        return Err(invalid(
            "dataset_lineage",
            "must bind the approved root and date floor",
        ));
    }
    validate_iso_date(&receipt.dataset_date_floor, "dataset_date_floor")?;
    for date in &receipt.covered_utc_dates {
        validate_iso_date(date, "covered_utc_dates")?;
    }
    if receipt
        .covered_utc_dates
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
        || receipt
            .covered_utc_dates
            .iter()
            .any(|date| date.as_str() < receipt.dataset_date_floor.as_str())
    {
        return Err(invalid(
            "covered_utc_dates",
            "must be unique, sorted, and not earlier than the dataset floor",
        ));
    }
    if receipt.covered_utc_dates.is_empty() {
        return Err(invalid(
            "covered_utc_dates",
            "requires at least one covered UTC date",
        ));
    }
    if receipt.training_dataset_ready {
        return Err(invalid(
            "training_dataset_ready",
            "Gate A must remain false",
        ));
    }
    if receipt.training_split_manifest_id != "N/A"
        || receipt.training_split_manifest_sha256 != "N/A"
    {
        return Err(invalid(
            "training_split_manifest",
            "Gate A split identity and hash must both be N/A",
        ));
    }
    for (field, digest) in [
        (
            "pipeline_core_identity_sha256",
            &receipt.pipeline_core_identity_sha256,
        ),
        (
            "input_set_identity_sha256",
            &receipt.input_set_identity_sha256,
        ),
        (
            "supported_schema_set_identity_sha256",
            &receipt.supported_schema_set_identity_sha256,
        ),
        (
            "input_manifest_set_sha256",
            &receipt.input_manifest_set_sha256,
        ),
        (
            "canonical_event_store_manifest_sha256",
            &receipt.canonical_event_store_manifest_sha256,
        ),
        (
            "event_time_index_manifest_sha256",
            &receipt.event_time_index_manifest_sha256,
        ),
        (
            "visibility_index_manifest_sha256",
            &receipt.visibility_index_manifest_sha256,
        ),
        (
            "observation_manifest_sha256",
            &receipt.observation_manifest_sha256,
        ),
        (
            "capability_matrix_sha256",
            &receipt.capability_matrix_sha256,
        ),
        ("run_manifest_sha256", &receipt.run_manifest_sha256),
    ] {
        validate_receipt_digest(digest, field)?;
    }
    if gate_a_capability_matrix_digest(matrix)? != receipt.capability_matrix_sha256 {
        return Err(invalid(
            "capability_matrix_sha256",
            "does not match the supplied capability matrix",
        ));
    }
    if matrix
        .entries
        .iter()
        .any(|entry| entry.status != CapabilityStatus::Pass)
    {
        return Err(invalid(
            "capability_matrix",
            "Gate A receipt requires every capability to PASS",
        ));
    }
    if derive_gate_a_receipt_v3_id(receipt)? != receipt.receipt_id {
        return Err(invalid(
            "receipt_id",
            "does not match the content-addressed Gate A receipt identity",
        ));
    }
    Ok(())
}

pub fn encode_gate_a_receipt_v3(
    receipt: &GateAReceiptV3,
    matrix: &GateACapabilityMatrixV2,
) -> Result<Vec<u8>, ContractError> {
    validate_gate_a_receipt_v3(receipt, matrix)?;
    stable_json(receipt)
}

pub fn decode_gate_a_receipt_v3(
    bytes: &[u8],
    matrix: &GateACapabilityMatrixV2,
) -> Result<GateAReceiptV3, ContractError> {
    require_schema_version(bytes, GATE_A_RECEIPT_SCHEMA_V3, "Gate A receipt")?;
    let receipt =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_gate_a_receipt_v3(&receipt, matrix)?;
    Ok(receipt)
}

#[cfg(test)]
std::thread_local! { static FORBID_IDENTITY_ENCODING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
fn without_identity_encoding<T>(work: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FORBID_IDENTITY_ENCODING.set(false);
        }
    }
    FORBID_IDENTITY_ENCODING.set(true);
    let _reset = Reset;
    work()
}
