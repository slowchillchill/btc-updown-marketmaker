use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use contracts::*;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};

const EVENT_ID_DOMAIN: &[u8] = b"data-pipeline/canonical-event-id/v1\0";
const SEMANTIC_TIEBREAK_DOMAIN: &[u8] = b"data-pipeline/semantic-tiebreak/v1\0";
const OBSERVATION_ID_DOMAIN: &[u8] = b"data-pipeline/market-observation-id/v1\0";
const OBSERVATION_HASH_DOMAIN: &[u8] = b"data-pipeline/market-observation-hash/v1\0";
const CONTRACT_DIGEST_DOMAIN: &[u8] = b"data-pipeline/contract-digest/v1\0";
const GATE_A_RECEIPT_ID_DOMAIN: &[u8] = b"data-pipeline/gate-a-receipt-id/v1\0";

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
    serde_json::to_vec(value).map_err(|error| ContractError::Json(error.to_string()))
}

fn hash_domain_bytes(domain: &[u8], bytes: &[u8]) -> Sha256Hex {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

pub fn gate_a_capability_matrix_digest(
    matrix: &GateACapabilityMatrixV1,
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
    ordering_coordinate: &'a OrderingCoordinateV1,
}

#[derive(Serialize)]
struct EventIdentityMaterial<'a> {
    schema_version: &'a str,
    market: &'a MarketIdentityV1,
    time: &'a EventTimeV1,
    canonical_rule_version: &'a str,
    source_evidence: Vec<SemanticSourceEvidence<'a>>,
    ordering_source_evidence_index: u32,
    quality: &'a EventQualityV1,
    payload: &'a ExecutionMarketEventPayloadV1,
}

fn event_identity_material(event: &ExecutionMarketEventV1) -> EventIdentityMaterial<'_> {
    EventIdentityMaterial {
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
                ordering_coordinate: &source.ordering_coordinate,
            })
            .collect(),
        ordering_source_evidence_index: event.lineage.ordering_source_evidence_index,
        quality: &event.quality,
        payload: &event.payload,
    }
}

pub fn derive_canonical_event_id(
    event: &ExecutionMarketEventV1,
) -> Result<CanonicalEventId, ContractError> {
    let bytes = stable_json(&event_identity_material(event))?;
    Ok(CanonicalEventId(
        hash_domain_bytes(EVENT_ID_DOMAIN, &bytes).0,
    ))
}

pub fn derive_semantic_tiebreak(
    event: &ExecutionMarketEventV1,
) -> Result<Sha256Hex, ContractError> {
    let bytes = stable_json(&event_identity_material(event))?;
    Ok(hash_domain_bytes(SEMANTIC_TIEBREAK_DOMAIN, &bytes))
}

fn append_memcomparable_bytes(output: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            output.extend_from_slice(&[0, 255]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

fn append_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
}

fn source_kind_rank(source_kind: CanonicalSourceKind) -> u8 {
    match source_kind {
        CanonicalSourceKind::BinanceAggtrade => 0,
        CanonicalSourceKind::BinanceDepthSnapshot => 1,
        CanonicalSourceKind::BinanceDepthUpdate => 2,
        CanonicalSourceKind::PolymarketMarketMetadata => 3,
        CanonicalSourceKind::PolymarketOrderbook => 4,
        CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd => 5,
    }
}

fn append_source_native_identity(
    output: &mut Vec<u8>,
    identity: &SourceNativeIdentityV1,
) -> Result<(), ContractError> {
    match identity {
        SourceNativeIdentityV1::BinanceAggregateTrade { aggregate_trade_id } => {
            output.push(0);
            output.extend_from_slice(&aggregate_trade_id.to_be_bytes());
        }
        SourceNativeIdentityV1::BinanceDepthSnapshot { last_update_id } => {
            output.push(1);
            output.extend_from_slice(&last_update_id.to_be_bytes());
        }
        SourceNativeIdentityV1::BinanceDepthUpdate {
            first_update_id,
            final_update_id,
        } => {
            output.push(2);
            output.extend_from_slice(&first_update_id.to_be_bytes());
            output.extend_from_slice(&final_update_id.to_be_bytes());
        }
        SourceNativeIdentityV1::SemanticEvidenceSha256 { sha256 } => {
            output.push(3);
            output.extend_from_slice(&decode_sha256(sha256, "source_native_identity.sha256")?);
        }
    }
    Ok(())
}

pub fn total_order_key_bytes(key: &TotalOrderKeyV1) -> Result<Vec<u8>, ContractError> {
    validate_total_order_key(key)?;
    let mut output = Vec::with_capacity(192);
    append_i64(&mut output, key.available_at_utc_ms);
    output.push(source_kind_rank(key.source_kind));
    match &key.ordering_coordinate {
        OrderingCoordinateV1::CollectorAdmission {
            source_sequence_epoch_id,
            collector_admission_sequence,
        } => {
            output.push(0);
            append_memcomparable_bytes(&mut output, source_sequence_epoch_id.as_bytes());
            output.extend_from_slice(&collector_admission_sequence.to_be_bytes());
        }
        OrderingCoordinateV1::ReceivedOnly {
            received_at_utc_ms,
            stable_evidence_key_sha256,
        } => {
            output.push(1);
            append_i64(&mut output, *received_at_utc_ms);
            output.extend_from_slice(&decode_sha256(
                stable_evidence_key_sha256,
                "total_order_key.ordering_coordinate.stable_evidence_key_sha256",
            )?);
        }
    }
    match &key.source_native_identity {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            append_source_native_identity(&mut output, value)?;
        }
    }
    output.extend_from_slice(&decode_sha256(
        &key.semantic_tiebreak_sha256,
        "total_order_key.semantic_tiebreak_sha256",
    )?);
    Ok(output)
}

pub fn compare_total_order_keys(
    left: &TotalOrderKeyV1,
    right: &TotalOrderKeyV1,
) -> Result<Ordering, ContractError> {
    Ok(total_order_key_bytes(left)?.cmp(&total_order_key_bytes(right)?))
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

fn validate_market(market: &MarketIdentityV1, field: &str) -> Result<(), ContractError> {
    validate_condition_id(&market.condition_id.0, &format!("{field}.condition_id"))?;
    validate_unsigned_decimal_id(&market.clob_market_id.0, &format!("{field}.clob_market_id"))?;
    decode_sha256(
        &market.window.window_id,
        &format!("{field}.window.window_id"),
    )?;
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

fn validate_ordering_coordinate_for_source(
    source_kind: CanonicalSourceKind,
    coordinate: &OrderingCoordinateV1,
    field: &str,
) -> Result<(), ContractError> {
    match (source_kind, coordinate) {
        (
            CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
            OrderingCoordinateV1::ReceivedOnly {
                stable_evidence_key_sha256,
                ..
            },
        ) => {
            decode_sha256(
                stable_evidence_key_sha256,
                &format!("{field}.stable_evidence_key_sha256"),
            )?;
        }
        (
            CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
            OrderingCoordinateV1::CollectorAdmission { .. },
        ) => {
            return Err(invalid(
                field,
                "RTDS v4 cannot claim a collector admission coordinate",
            ));
        }
        (
            _,
            OrderingCoordinateV1::CollectorAdmission {
                source_sequence_epoch_id,
                collector_admission_sequence,
            },
        ) => {
            if source_sequence_epoch_id.is_empty() {
                return Err(invalid(
                    format!("{field}.source_sequence_epoch_id"),
                    "must not be empty",
                ));
            }
            if *collector_admission_sequence == 0 {
                return Err(invalid(
                    format!("{field}.collector_admission_sequence"),
                    "must be positive",
                ));
            }
        }
        (_, OrderingCoordinateV1::ReceivedOnly { .. }) => {
            return Err(invalid(
                field,
                "this source schema requires its collector admission coordinate",
            ));
        }
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
            | CanonicalSourceKind::PolymarketOrderbook
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

fn validate_total_order_key(key: &TotalOrderKeyV1) -> Result<(), ContractError> {
    if key.version != TOTAL_ORDER_KEY_VERSION_V1 {
        return Err(ContractError::UnsupportedSchema {
            contract: "total order key",
            actual: key.version.clone(),
        });
    }
    validate_ordering_coordinate_for_source(
        key.source_kind,
        &key.ordering_coordinate,
        "total_order_key.ordering_coordinate",
    )?;
    validate_source_native_identity(
        key.source_kind,
        &key.source_native_identity,
        "total_order_key.source_native_identity",
    )?;
    match &key.ordering_coordinate {
        OrderingCoordinateV1::CollectorAdmission {
            source_sequence_epoch_id,
            ..
        } if source_sequence_epoch_id.is_empty() => {
            return Err(invalid(
                "total_order_key.ordering_coordinate.source_sequence_epoch_id",
                "must not be empty",
            ));
        }
        OrderingCoordinateV1::ReceivedOnly {
            received_at_utc_ms,
            stable_evidence_key_sha256,
        } => {
            if received_at_utc_ms > &key.available_at_utc_ms {
                return Err(invalid(
                    "total_order_key.ordering_coordinate.received_at_utc_ms",
                    "must not follow total-order key availability",
                ));
            }
            decode_sha256(
                stable_evidence_key_sha256,
                "total_order_key.ordering_coordinate.stable_evidence_key_sha256",
            )?;
        }
        OrderingCoordinateV1::CollectorAdmission { .. } => {}
    }
    decode_sha256(
        &key.semantic_tiebreak_sha256,
        "total_order_key.semantic_tiebreak_sha256",
    )?;
    Ok(())
}

fn validate_event_time_and_order(event: &ExecutionMarketEventV1) -> Result<(), ContractError> {
    if event.time.available_at_utc_ms != event.total_order_key.available_at_utc_ms {
        return Err(invalid(
            "total_order_key.available_at_utc_ms",
            "must equal time.available_at_utc_ms",
        ));
    }
    match (
        &event.time.collector_admission,
        &event.time.ordering_evidence_quality,
        &event.total_order_key.ordering_coordinate,
    ) {
        (
            Some(admission),
            OrderingEvidenceQuality::CollectorAdmissionCoordinate,
            OrderingCoordinateV1::CollectorAdmission {
                source_sequence_epoch_id,
                collector_admission_sequence,
            },
        ) if admission.source_sequence_epoch_id == source_sequence_epoch_id.as_str()
            && admission.collector_admission_sequence == *collector_admission_sequence =>
        {
            if event.time.received_at_utc_ms > admission.admitted_at_utc_ms {
                return Err(invalid(
                    "time.received_at_utc_ms",
                    "must not follow collector admission",
                ));
            }
            if event.time.available_at_utc_ms < admission.admitted_at_utc_ms {
                return Err(invalid(
                    "time.available_at_utc_ms",
                    "must not precede collector admission",
                ));
            }
        }
        (
            None,
            OrderingEvidenceQuality::ReceivedTimeStableEvidence,
            OrderingCoordinateV1::ReceivedOnly {
                received_at_utc_ms, ..
            },
        ) if event.time.received_at_utc_ms == *received_at_utc_ms => {
            if event.time.available_at_utc_ms < event.time.received_at_utc_ms {
                return Err(invalid(
                    "time.available_at_utc_ms",
                    "must not precede collector receive time",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "time.ordering_evidence_quality",
                "time evidence and total-order coordinate do not match",
            ));
        }
    }
    if event.quality.ordering != event.time.ordering_evidence_quality {
        return Err(invalid(
            "quality.ordering",
            "must equal time.ordering_evidence_quality",
        ));
    }
    Ok(())
}

fn validate_source_evidence(
    values: &[SourceEvidenceRefV1],
    event_available_at_utc_ms: i64,
) -> Result<(), ContractError> {
    if values.is_empty() {
        return Err(invalid(
            "lineage.source_evidence",
            "must contain at least one source evidence reference",
        ));
    }
    let mut previous: Option<(
        CanonicalSourceKind,
        &Option<SourceNativeIdentityV1>,
        &OrderingCoordinateV1,
    )> = None;
    for (index, value) in values.iter().enumerate() {
        validate_ordering_coordinate_for_source(
            value.source_kind,
            &value.ordering_coordinate,
            &format!("lineage.source_evidence[{index}].ordering_coordinate"),
        )?;
        if let OrderingCoordinateV1::ReceivedOnly {
            received_at_utc_ms, ..
        } = &value.ordering_coordinate
            && *received_at_utc_ms > event_available_at_utc_ms
        {
            return Err(invalid(
                format!("lineage.source_evidence[{index}].ordering_coordinate.received_at_utc_ms"),
                "must not follow event availability",
            ));
        }
        validate_source_native_identity(
            value.source_kind,
            &value.source_native_identity,
            &format!("lineage.source_evidence[{index}].source_native_identity"),
        )?;
        decode_sha256(
            &value.input_manifest_sha256,
            &format!("lineage.source_evidence[{index}].input_manifest_sha256"),
        )?;
        decode_sha256(
            &value.input_file_sha256,
            &format!("lineage.source_evidence[{index}].input_file_sha256"),
        )?;
        let current = (
            value.source_kind,
            &value.source_native_identity,
            &value.ordering_coordinate,
        );
        if previous.is_some_and(|prior| prior >= current) {
            return Err(ContractError::NonCanonicalOrder {
                field: "lineage.source_evidence".to_owned(),
            });
        }
        previous = Some(current);
    }
    Ok(())
}

fn validate_snapshot(snapshot: &InitialL2SnapshotV1) -> Result<(), ContractError> {
    validate_asset(&snapshot.asset, "payload.event.asset")?;
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
    validate_sorted_event_ids(&snapshot.source_event_ids, "payload.event.source_event_ids")?;
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
    outer_market: &MarketIdentityV1,
    outer_available_at: i64,
    transition: &LevelExecutionTransitionV1,
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
    if queue > reduced {
        return Err(invalid(
            "payload.event.queue_consumption_quantity_shares",
            "must not exceed net_reduced_quantity_shares",
        ));
    }
    if transition.group_available_at_utc_ms != outer_available_at {
        return Err(invalid(
            "payload.event.group_available_at_utc_ms",
            "must equal the finalized event available_at_utc_ms",
        ));
    }
    validate_sorted_event_ids(
        &transition.source_event_ids,
        "payload.event.source_event_ids",
    )?;
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
    terminal: &MarketResolutionTerminalV1,
    event_total_order_key: &TotalOrderKeyV1,
) -> Result<(), ContractError> {
    let (source_event_ids, watermark) = match terminal {
        MarketResolutionTerminalV1::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
            source_event_ids,
            terminal_watermark,
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
            (source_event_ids, terminal_watermark)
        }
        MarketResolutionTerminalV1::WaitingExhaustedQuarantine {
            reason_code,
            source_event_ids,
            terminal_watermark,
        } => {
            if reason_code.is_empty() {
                return Err(invalid("payload.event.reason_code", "must not be empty"));
            }
            (source_event_ids, terminal_watermark)
        }
    };
    validate_sorted_event_ids(source_event_ids, "payload.event.source_event_ids")?;
    validate_total_order_key(&watermark.last_total_order_key)?;
    if compare_total_order_keys(&watermark.last_total_order_key, event_total_order_key)?
        == Ordering::Greater
    {
        return Err(invalid(
            "payload.event.terminal_watermark.last_total_order_key",
            "must not be later than the terminal event",
        ));
    }
    decode_sha256(
        &watermark.canonical_event_root_sha256,
        "payload.event.terminal_watermark.canonical_event_root_sha256",
    )?;
    decode_sha256(
        &watermark.lineage_identity_sha256,
        "payload.event.terminal_watermark.lineage_identity_sha256",
    )?;
    Ok(())
}

fn validate_payload(event: &ExecutionMarketEventV1) -> Result<(), ContractError> {
    match &event.payload {
        ExecutionMarketEventPayloadV1::MarketLifecycleTransition(transition) => {
            if let Some(previous) = &transition.previous_state {
                validate_mapped_lifecycle_state(previous, "payload.event.previous_state")?;
            }
            validate_mapped_lifecycle_state(
                &transition.current_state,
                "payload.event.current_state",
            )?;
            validate_sorted_assets(&transition.assets, "payload.event.assets")?;
            validate_sorted_event_ids(
                &transition.source_event_ids,
                "payload.event.source_event_ids",
            )?;
        }
        ExecutionMarketEventPayloadV1::InitialL2Snapshot(snapshot) => {
            validate_snapshot(snapshot)?;
        }
        ExecutionMarketEventPayloadV1::BestBidAsk(best) => {
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
            validate_sorted_event_ids(&best.source_event_ids, "payload.event.source_event_ids")?;
        }
        ExecutionMarketEventPayloadV1::TickSizeChange(change) => {
            validate_asset(&change.asset, "payload.event.asset")?;
            validate_positive_decimal(
                &change.old_tick_size_pusd_per_share,
                "payload.event.old_tick_size_pusd_per_share",
            )?;
            validate_positive_decimal(
                &change.new_tick_size_pusd_per_share,
                "payload.event.new_tick_size_pusd_per_share",
            )?;
            validate_sorted_event_ids(&change.source_event_ids, "payload.event.source_event_ids")?;
        }
        ExecutionMarketEventPayloadV1::PublicTrade(trade) => {
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
            validate_sorted_event_ids(&trade.source_event_ids, "payload.event.source_event_ids")?;
        }
        ExecutionMarketEventPayloadV1::LevelExecutionTransition(transition) => {
            validate_level_transition(&event.market, event.time.available_at_utc_ms, transition)?;
        }
        ExecutionMarketEventPayloadV1::QualityBoundary(boundary) => {
            if let QualityScopeV1::Asset(asset) = &boundary.scope {
                validate_asset(asset, "payload.event.scope.asset")?;
            }
            if boundary.reason_code.is_empty() {
                return Err(invalid("payload.event.reason_code", "must not be empty"));
            }
            validate_sorted_event_ids(
                &boundary.source_event_ids,
                "payload.event.source_event_ids",
            )?;
        }
        ExecutionMarketEventPayloadV1::MarketResolutionTerminal(terminal) => {
            validate_terminal(terminal, &event.total_order_key)?;
        }
    }
    Ok(())
}

pub fn validate_execution_market_event(
    event: &ExecutionMarketEventV1,
) -> Result<(), ContractError> {
    if event.schema_version != EXECUTION_MARKET_EVENT_SCHEMA_V1 {
        return Err(ContractError::UnsupportedSchema {
            contract: "execution market event",
            actual: event.schema_version.clone(),
        });
    }
    validate_event_id(&event.event_id, "event_id")?;
    validate_market(&event.market, "market")?;
    validate_total_order_key(&event.total_order_key)?;
    validate_event_time_and_order(event)?;
    if event.lineage.canonical_rule_version.is_empty() {
        return Err(invalid(
            "lineage.canonical_rule_version",
            "must not be empty",
        ));
    }
    validate_source_evidence(
        &event.lineage.source_evidence,
        event.time.available_at_utc_ms,
    )?;
    let ordering_source = event
        .lineage
        .source_evidence
        .get(event.lineage.ordering_source_evidence_index as usize)
        .ok_or_else(|| {
            invalid(
                "lineage.ordering_source_evidence_index",
                "must select one source evidence entry",
            )
        })?;
    if ordering_source.source_kind != event.total_order_key.source_kind
        || ordering_source.source_native_identity != event.total_order_key.source_native_identity
        || ordering_source.ordering_coordinate != event.total_order_key.ordering_coordinate
    {
        return Err(invalid(
            "lineage.ordering_source_evidence_index",
            "selected source kind, identity, and coordinate must match total_order_key",
        ));
    }
    validate_payload(event)?;
    if derive_canonical_event_id(event)? != event.event_id {
        return Err(invalid(
            "event_id",
            "does not match the canonical semantic identity",
        ));
    }
    if derive_semantic_tiebreak(event)? != event.total_order_key.semantic_tiebreak_sha256 {
        return Err(invalid(
            "total_order_key.semantic_tiebreak_sha256",
            "does not match the canonical semantic tiebreak",
        ));
    }
    Ok(())
}

pub fn encode_execution_market_event(
    event: &ExecutionMarketEventV1,
) -> Result<Vec<u8>, ContractError> {
    validate_execution_market_event(event)?;
    stable_json(event)
}

pub fn decode_execution_market_event(
    bytes: &[u8],
) -> Result<ExecutionMarketEventV1, ContractError> {
    let event =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_execution_market_event(&event)?;
    Ok(event)
}

#[derive(Serialize)]
struct ObservationIdentityMaterial<'a> {
    schema_version: &'a str,
    market: &'a MarketIdentityV1,
    lineage: &'a ObservationLineageV1,
    causal_cutoff: &'a ObservationCausalCutoffV1,
}

#[derive(Serialize)]
struct ObservationHashMaterial<'a> {
    schema_version: &'a str,
    observation_id: &'a Sha256Hex,
    market: &'a MarketIdentityV1,
    lineage: &'a ObservationLineageV1,
    causal_cutoff: &'a ObservationCausalCutoffV1,
    market_state_digest_sha256: &'a Sha256Hex,
    component_states: &'a [ObservationComponentStateV1],
    missing_mask: &'a [ObservationComponentId],
}

pub fn derive_observation_id(
    observation: &MarketObservationCausalShellV1,
) -> Result<Sha256Hex, ContractError> {
    let material = ObservationIdentityMaterial {
        schema_version: &observation.schema_version,
        market: &observation.market,
        lineage: &observation.lineage,
        causal_cutoff: &observation.causal_cutoff,
    };
    Ok(hash_domain_bytes(
        OBSERVATION_ID_DOMAIN,
        &stable_json(&material)?,
    ))
}

pub fn derive_observation_hash(
    observation: &MarketObservationCausalShellV1,
) -> Result<Sha256Hex, ContractError> {
    let material = ObservationHashMaterial {
        schema_version: &observation.schema_version,
        observation_id: &observation.observation_id,
        market: &observation.market,
        lineage: &observation.lineage,
        causal_cutoff: &observation.causal_cutoff,
        market_state_digest_sha256: &observation.market_state_digest_sha256,
        component_states: &observation.component_states,
        missing_mask: &observation.missing_mask,
    };
    Ok(hash_domain_bytes(
        OBSERVATION_HASH_DOMAIN,
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

pub fn validate_market_observation_causal_shell(
    observation: &MarketObservationCausalShellV1,
) -> Result<(), ContractError> {
    if observation.schema_version != MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V1 {
        return Err(ContractError::UnsupportedSchema {
            contract: "market observation causal shell",
            actual: observation.schema_version.clone(),
        });
    }
    decode_sha256(&observation.observation_id, "observation_id")?;
    validate_market(&observation.market, "market")?;
    if observation.lineage.execution_event_contract_version != EXECUTION_MARKET_EVENT_SCHEMA_V1 {
        return Err(invalid(
            "lineage.execution_event_contract_version",
            "must reference execution_market_event_v1",
        ));
    }
    decode_sha256(
        &observation.lineage.input_set_identity_sha256,
        "lineage.input_set_identity_sha256",
    )?;
    validate_sorted_event_ids_allow_empty(
        &observation.lineage.contributing_event_ids,
        "lineage.contributing_event_ids",
    )?;
    validate_total_order_key(&observation.lineage.last_finalized_order_key)?;
    validate_total_order_key(&observation.causal_cutoff.through_total_order_key)?;
    if observation.lineage.last_finalized_order_key
        != observation.causal_cutoff.through_total_order_key
    {
        return Err(invalid(
            "causal_cutoff.through_total_order_key",
            "must equal lineage.last_finalized_order_key",
        ));
    }
    if observation.causal_cutoff.available_at_utc_ms
        < observation
            .causal_cutoff
            .through_total_order_key
            .available_at_utc_ms
    {
        return Err(invalid(
            "causal_cutoff.available_at_utc_ms",
            "must not precede the finalized total-order key availability",
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
            let Some(as_of) = component.as_of_available_at_utc_ms else {
                return Err(invalid(
                    format!("component_states[{index}].as_of_available_at_utc_ms"),
                    "fresh or stale component requires an as-of time",
                ));
            };
            if as_of > observation.causal_cutoff.available_at_utc_ms {
                return Err(invalid(
                    format!("component_states[{index}].as_of_available_at_utc_ms"),
                    "must not be later than the causal cutoff",
                ));
            }
            if as_of
                > observation
                    .causal_cutoff
                    .through_total_order_key
                    .available_at_utc_ms
            {
                return Err(invalid(
                    format!("component_states[{index}].as_of_available_at_utc_ms"),
                    "must not be later than the last finalized total-order key",
                ));
            }
            let age_ms = observation
                .causal_cutoff
                .available_at_utc_ms
                .checked_sub(as_of)
                .ok_or_else(|| invalid("component_states", "information age overflow"))?;
            let expected_age = u64::try_from(age_ms)
                .map_err(|_| invalid("component_states", "information age overflow"))?;
            if component.information_age_ms != Some(expected_age)
                || component.last_visible_event_id.is_none()
                || component.state_digest_sha256.is_none()
                || missing.contains(&component.component)
            {
                return Err(invalid(
                    format!("component_states[{index}]"),
                    "visible component requires event, digest, exact age, and no missing mask",
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
            if component.as_of_available_at_utc_ms.is_some()
                || component.information_age_ms.is_some()
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
    if derive_observation_id(observation)? != observation.observation_id {
        return Err(invalid(
            "observation_id",
            "does not match the causal observation identity",
        ));
    }
    if derive_observation_hash(observation)? != observation.canonical_hash_sha256 {
        return Err(invalid(
            "canonical_hash_sha256",
            "does not match the canonical observation content",
        ));
    }
    Ok(())
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

pub fn encode_market_observation_causal_shell(
    observation: &MarketObservationCausalShellV1,
) -> Result<Vec<u8>, ContractError> {
    validate_market_observation_causal_shell(observation)?;
    stable_json(observation)
}

pub fn decode_market_observation_causal_shell(
    bytes: &[u8],
) -> Result<MarketObservationCausalShellV1, ContractError> {
    let observation =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_market_observation_causal_shell(&observation)?;
    Ok(observation)
}

pub const REQUIRED_GATE_A_CAPABILITIES: [GateACapabilityId; 16] = [
    GateACapabilityId::InputAdmissionAndSixSchemaMapping,
    GateACapabilityId::CanonicalIdentityTimeAndOrder,
    GateACapabilityId::MarketIdentityWindowAndLifecycle,
    GateACapabilityId::InitialL2SnapshotEpochCursorAndDigest,
    GateACapabilityId::PriceLevelQuantityTransition,
    GateACapabilityId::BestBidAsk,
    GateACapabilityId::DynamicTickSize,
    GateACapabilityId::PublicTradeEvidence,
    GateACapabilityId::LevelExecutionTransition,
    GateACapabilityId::QualityGapResyncAndWatermark,
    GateACapabilityId::MarketResolutionTerminal,
    GateACapabilityId::ObservationSharedLineageAndCutoff,
    GateACapabilityId::ImmutablePagedMarketAndCausalRangeTape,
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
    matrix: &GateACapabilityMatrixV1,
) -> Result<(), ContractError> {
    if matrix.schema_version != GATE_A_CAPABILITY_MATRIX_SCHEMA_V1 {
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
    matrix: &GateACapabilityMatrixV1,
) -> Result<Vec<u8>, ContractError> {
    validate_gate_a_capability_matrix(matrix)?;
    stable_json(matrix)
}

pub fn decode_gate_a_capability_matrix(
    bytes: &[u8],
) -> Result<GateACapabilityMatrixV1, ContractError> {
    let matrix =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_gate_a_capability_matrix(&matrix)?;
    Ok(matrix)
}

fn validate_receipt_digest(value: &Sha256Hex, field: &str) -> Result<(), ContractError> {
    decode_sha256(value, field).map(|_| ())
}

pub fn derive_gate_a_receipt_id(receipt: &GateAReceiptV1) -> Result<Sha256Hex, ContractError> {
    let mut material = receipt.clone();
    material.receipt_id = Sha256Hex("0".repeat(64));
    Ok(hash_domain_bytes(
        GATE_A_RECEIPT_ID_DOMAIN,
        &stable_json(&material)?,
    ))
}

pub fn validate_gate_a_receipt(
    receipt: &GateAReceiptV1,
    matrix: &GateACapabilityMatrixV1,
) -> Result<(), ContractError> {
    validate_gate_a_capability_matrix(matrix)?;
    if receipt.schema_version != GATE_A_RECEIPT_SCHEMA_V1 {
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
    if receipt.execution_event_contract_version != EXECUTION_MARKET_EVENT_SCHEMA_V1
        || receipt.observation_contract_version != MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V1
    {
        return Err(invalid(
            "contract_version",
            "must bind the frozen Phase 0 event and observation contracts",
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
            "canonical_event_tape_manifest_sha256",
            &receipt.canonical_event_tape_manifest_sha256,
        ),
        (
            "execution_event_tape_manifest_sha256",
            &receipt.execution_event_tape_manifest_sha256,
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
    if derive_gate_a_receipt_id(receipt)? != receipt.receipt_id {
        return Err(invalid(
            "receipt_id",
            "does not match the content-addressed Gate A receipt identity",
        ));
    }
    Ok(())
}

pub fn encode_gate_a_receipt(
    receipt: &GateAReceiptV1,
    matrix: &GateACapabilityMatrixV1,
) -> Result<Vec<u8>, ContractError> {
    validate_gate_a_receipt(receipt, matrix)?;
    stable_json(receipt)
}

pub fn decode_gate_a_receipt(
    bytes: &[u8],
    matrix: &GateACapabilityMatrixV1,
) -> Result<GateAReceiptV1, ContractError> {
    let receipt =
        serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))?;
    validate_gate_a_receipt(&receipt, matrix)?;
    Ok(receipt)
}
