//! 六类当前 typed_raw schema 的无损业务边界。
//!
//! 物理列名由训练适配器校验；本模块保留原始十进制文本。
//! 经济单位与原生/派生属性由 [`TypedRawValueMapping`] 明确表达，来源原生标识、
//! 采集器派生标识和证据摘要由 [`TypedRawIdentifierMapping`] 明确区分。

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

use base64::Engine as _;
use contracts::ExactDecimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const POLYMARKET_ORDERBOOK_SCHEMA: &str = "polymarket_orderbook_v3";
pub const POLYMARKET_MARKET_METADATA_SCHEMA: &str = "polymarket_market_metadata_v5";
pub const POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA: &str = "polymarket_rtds_chainlink_btc_usd_v4";
pub const BINANCE_DEPTH_SNAPSHOT_SCHEMA: &str = "binance_depth_snapshot_v3";
pub const BINANCE_DEPTH_UPDATE_SCHEMA: &str = "binance_depth_update_v4";
pub const BINANCE_AGGTRADE_SCHEMA: &str = "binance_aggtrade_v7";
const BINANCE_AGGTRADE_INVALID_EVIDENCE_SCHEMA: &str = "binance_aggtrade_invalid_evidence_v1";
const BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES: u64 = 524_288;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypedRawSourceKind {
    PolymarketOrderbook,
    PolymarketMarketMetadata,
    PolymarketRtdsChainlinkBtcUsd,
    BinanceDepthSnapshot,
    BinanceDepthUpdate,
    BinanceAggTrade,
}

impl TypedRawSourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolymarketOrderbook => "polymarket_market",
            Self::PolymarketMarketMetadata => "polymarket_market_metadata",
            Self::PolymarketRtdsChainlinkBtcUsd => "polymarket_rtds_chainlink_btc_usd",
            Self::BinanceDepthSnapshot => "binance_depth_snapshot",
            Self::BinanceDepthUpdate => "binance_depth_update",
            Self::BinanceAggTrade => "binance_aggtrade",
        }
    }

    pub const fn identifier_mappings(self) -> &'static [TypedRawIdentifierMapping] {
        match self {
            Self::PolymarketOrderbook => POLYMARKET_ORDERBOOK_IDENTIFIERS,
            Self::PolymarketMarketMetadata => POLYMARKET_METADATA_IDENTIFIERS,
            Self::PolymarketRtdsChainlinkBtcUsd => POLYMARKET_RTDS_IDENTIFIERS,
            Self::BinanceDepthSnapshot => BINANCE_DEPTH_SNAPSHOT_IDENTIFIERS,
            Self::BinanceDepthUpdate => BINANCE_DEPTH_UPDATE_IDENTIFIERS,
            Self::BinanceAggTrade => BINANCE_AGGTRADE_IDENTIFIERS,
        }
    }

    pub const fn value_mappings(self) -> &'static [TypedRawValueMapping] {
        match self {
            Self::PolymarketOrderbook => POLYMARKET_ORDERBOOK_VALUES,
            Self::PolymarketMarketMetadata => POLYMARKET_METADATA_VALUES,
            Self::PolymarketRtdsChainlinkBtcUsd => POLYMARKET_RTDS_VALUES,
            Self::BinanceDepthSnapshot => BINANCE_DEPTH_SNAPSHOT_VALUES,
            Self::BinanceDepthUpdate => BINANCE_DEPTH_UPDATE_VALUES,
            Self::BinanceAggTrade => BINANCE_AGGTRADE_VALUES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypedRawFieldOrigin {
    /// 上游源证据直接提供，或由采集器从该证据无改动携带。
    SourceNative,
    /// 采集器计算、分类、注入或维护的值。
    CollectorDerived,
    /// 对另一份证据字节计算的摘要。
    EvidenceDigest,
    /// 必须结合当前行的证据来源再解析，不能静态归类。
    RowEvidenceDependent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypedRawIdentifierMapping {
    pub field: &'static str,
    pub origin: TypedRawFieldOrigin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypedRawUnit {
    PusdPerShare,
    Shares,
    BasisPoints,
    UsdPerBtc,
    UsdtPerBtc,
    Btc,
    Milliseconds,
    Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypedRawValueMapping {
    pub field: &'static str,
    pub origin: TypedRawFieldOrigin,
    pub unit: Option<TypedRawUnit>,
}

macro_rules! identifier {
    ($field:literal, $origin:ident) => {
        TypedRawIdentifierMapping {
            field: $field,
            origin: TypedRawFieldOrigin::$origin,
        }
    };
}

macro_rules! value {
    ($field:literal, $origin:ident, $unit:ident) => {
        TypedRawValueMapping {
            field: $field,
            origin: TypedRawFieldOrigin::$origin,
            unit: Some(TypedRawUnit::$unit),
        }
    };
    ($field:literal, $origin:ident) => {
        TypedRawValueMapping {
            field: $field,
            origin: TypedRawFieldOrigin::$origin,
            unit: None,
        }
    };
}

const POLYMARKET_ORDERBOOK_IDENTIFIERS: &[TypedRawIdentifierMapping] = &[
    identifier!("market", SourceNative),
    identifier!("event.book.asset_id", SourceNative),
    identifier!("event.price_change.price_changes[].asset_id", SourceNative),
    identifier!("event.tick_size_change.asset_id", SourceNative),
    identifier!("event.last_trade_price.asset_id", SourceNative),
    identifier!("event.best_bid_ask.asset_id", SourceNative),
    identifier!("event.new_market.clob_market_id", SourceNative),
    identifier!("event.new_market.asset_ids[]", SourceNative),
    identifier!("event.new_market.event_message.id", SourceNative),
    identifier!("event.market_resolved.clob_market_id", SourceNative),
    identifier!("event.market_resolved.asset_ids[]", SourceNative),
    identifier!("event.market_resolved.event_message.id", SourceNative),
    identifier!("event.market_resolved.winning_asset_id", SourceNative),
    identifier!("source_sequence_epoch_id", CollectorDerived),
    identifier!("collector_admission_sequence", CollectorDerived),
];
const POLYMARKET_METADATA_IDENTIFIERS: &[TypedRawIdentifierMapping] = &[
    identifier!("slug", SourceNative),
    identifier!("condition_id", SourceNative),
    identifier!("clob_market_id", SourceNative),
    identifier!("asset_ids[]", SourceNative),
    identifier!("winning_asset_id", RowEvidenceDependent),
    identifier!("source_evidence_raw_sha256", EvidenceDigest),
    identifier!("collector_admission_sequence", CollectorDerived),
    identifier!("ws_claimant_source_sequence_epoch_id", CollectorDerived),
    identifier!("ws_claimant_collector_admission_sequence", CollectorDerived),
];
const POLYMARKET_RTDS_IDENTIFIERS: &[TypedRawIdentifierMapping] =
    &[identifier!("symbol", SourceNative)];
const BINANCE_DEPTH_SNAPSHOT_IDENTIFIERS: &[TypedRawIdentifierMapping] = &[
    identifier!("symbol", CollectorDerived),
    identifier!("last_update_id", SourceNative),
    identifier!("source_sequence_epoch_id", CollectorDerived),
    identifier!("connection_generation", CollectorDerived),
    identifier!("depth_ingress_sequence", CollectorDerived),
    identifier!("collector_admission_sequence", CollectorDerived),
];
const BINANCE_DEPTH_UPDATE_IDENTIFIERS: &[TypedRawIdentifierMapping] = &[
    identifier!("symbol", SourceNative),
    identifier!("first_update_id", SourceNative),
    identifier!("final_update_id", SourceNative),
    identifier!("snapshot_last_update_id", SourceNative),
    identifier!("local_u_before", CollectorDerived),
    identifier!("source_sequence_epoch_id", CollectorDerived),
    identifier!("connection_generation", CollectorDerived),
    identifier!("depth_ingress_sequence", CollectorDerived),
    identifier!("collector_admission_sequence", CollectorDerived),
];
const BINANCE_AGGTRADE_IDENTIFIERS: &[TypedRawIdentifierMapping] = &[
    identifier!("record.trade.symbol", SourceNative),
    identifier!("record.trade.aggregate_trade_id", SourceNative),
    identifier!("record.trade.first_trade_id", SourceNative),
    identifier!("record.trade.last_trade_id", SourceNative),
    identifier!("record.invalid_input.payload_sha256", EvidenceDigest),
    identifier!("source_sequence_epoch_id", CollectorDerived),
    identifier!("connection_generation", CollectorDerived),
    identifier!("websocket_ingress_sequence", CollectorDerived),
    identifier!("collector_admission_sequence", CollectorDerived),
];

const POLYMARKET_ORDERBOOK_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!("timestamp", SourceNative, Milliseconds),
    value!(
        "collector_admission_time_utc_ms",
        CollectorDerived,
        Milliseconds
    ),
    value!("event.book.bids[].price", SourceNative, PusdPerShare),
    value!("event.book.bids[].size", SourceNative, Shares),
    value!("event.book.asks[].price", SourceNative, PusdPerShare),
    value!("event.book.asks[].size", SourceNative, Shares),
    value!("event.book.last_trade_price", SourceNative, PusdPerShare),
    value!(
        "event.price_change.price_changes[].price",
        SourceNative,
        PusdPerShare
    ),
    value!(
        "event.price_change.price_changes[].size",
        SourceNative,
        Shares
    ),
    value!(
        "event.price_change.price_changes[].best_bid",
        SourceNative,
        PusdPerShare
    ),
    value!(
        "event.price_change.price_changes[].best_ask",
        SourceNative,
        PusdPerShare
    ),
    value!("event.price_change.price_changes[].side", SourceNative),
    value!(
        "event.tick_size_change.old_tick_size",
        SourceNative,
        PusdPerShare
    ),
    value!(
        "event.tick_size_change.new_tick_size",
        SourceNative,
        PusdPerShare
    ),
    value!("event.last_trade_price.price", SourceNative, PusdPerShare),
    value!("event.last_trade_price.size", SourceNative, Shares),
    value!("event.last_trade_price.side", SourceNative),
    value!(
        "event.last_trade_price.fee_rate_bps",
        SourceNative,
        BasisPoints
    ),
    value!("event.best_bid_ask.best_bid", SourceNative, PusdPerShare),
    value!("event.best_bid_ask.best_ask", SourceNative, PusdPerShare),
    value!("event.best_bid_ask.spread", SourceNative, PusdPerShare),
    value!("event.new_market.outcomes[]", SourceNative),
    value!("event.market_resolved.outcomes[]", SourceNative),
    value!("event.market_resolved.winning_outcome", SourceNative),
];
const POLYMARKET_METADATA_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!(
        "collector_admission_time_utc_ms",
        CollectorDerived,
        Milliseconds
    ),
    value!("market_start_utc", SourceNative, Milliseconds),
    value!("market_end_utc", SourceNative, Milliseconds),
    value!(
        "fixed_identity_verified_at_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!(
        "source_evidence_received_at_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!(
        "source_evidence_payload_json.orderPriceMinTickSize",
        SourceNative,
        PusdPerShare
    ),
    value!("outcome_prices_raw[]", SourceNative, PusdPerShare),
    value!(
        "row_evidence_validation_completed_at_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!("lifecycle_scheduled_at_utc", CollectorDerived, Milliseconds),
    value!("lifecycle_applied_at_utc", CollectorDerived, Milliseconds),
    value!(
        "runtime_role_scheduled_at_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!(
        "runtime_role_applied_at_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!("metadata_fetch_start_utc", CollectorDerived, Milliseconds),
    value!("closed_time_utc", CollectorDerived, Milliseconds),
    value!("closed", SourceNative),
    value!("active", SourceNative),
    value!("accepting_orders", SourceNative),
    value!("ws_platform_timestamp_utc", SourceNative, Milliseconds),
    value!("winning_outcome", RowEvidenceDependent),
    value!(
        "resolution_poll_scheduled_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!(
        "resolution_budget_horizon_utc",
        CollectorDerived,
        Milliseconds
    ),
    value!(
        "last_valid_metadata_received_utc",
        CollectorDerived,
        Milliseconds
    ),
];
const POLYMARKET_RTDS_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!("message_timestamp", SourceNative, Milliseconds),
    value!("payload_timestamp", SourceNative, Milliseconds),
    value!("value", SourceNative, UsdPerBtc),
    value!("age_ms", CollectorDerived, Milliseconds),
    value!("freshness_status", CollectorDerived),
];
const BINANCE_DEPTH_SNAPSHOT_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!("snapshot_fetch_start_utc", CollectorDerived, Milliseconds),
    value!(
        "collector_admission_time_utc_ms",
        CollectorDerived,
        Milliseconds
    ),
    value!("bids[].price", SourceNative, UsdtPerBtc),
    value!("bids[].quantity", SourceNative, Btc),
    value!("asks[].price", SourceNative, UsdtPerBtc),
    value!("asks[].quantity", SourceNative, Btc),
    value!("snapshot_status", CollectorDerived),
];
const BINANCE_DEPTH_UPDATE_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!("event_time", SourceNative, Milliseconds),
    value!(
        "collector_admission_time_utc_ms",
        CollectorDerived,
        Milliseconds
    ),
    value!("bids[].price", SourceNative, UsdtPerBtc),
    value!("bids[].quantity", SourceNative, Btc),
    value!("asks[].price", SourceNative, UsdtPerBtc),
    value!("asks[].quantity", SourceNative, Btc),
    value!("stitch_verdict", CollectorDerived),
    value!("stitch_evidence", CollectorDerived),
    value!("stitch_gap_reason", CollectorDerived),
];
const BINANCE_AGGTRADE_VALUES: &[TypedRawValueMapping] = &[
    value!("timestamp_received", CollectorDerived, Milliseconds),
    value!(
        "rest_backfill_timestamp_received",
        CollectorDerived,
        Milliseconds
    ),
    value!("record.trade.event_time", SourceNative, Milliseconds),
    value!("record.trade.trade_time", SourceNative, Milliseconds),
    value!("record.trade.price", SourceNative, UsdtPerBtc),
    value!("record.trade.quantity", SourceNative, Btc),
    value!("record.trade.buyer_is_maker", SourceNative),
    value!("record.invalid_input.invalid_reason_code", CollectorDerived),
    value!(
        "record.invalid_input.invalid_reason_detail",
        CollectorDerived
    ),
    value!(
        "record.invalid_input.payload_capture_status",
        CollectorDerived
    ),
    value!(
        "record.invalid_input.payload_length_bytes",
        CollectorDerived,
        Bytes
    ),
    value!(
        "collector_admission_time_utc_ms",
        CollectorDerived,
        Milliseconds
    ),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypedRawError {
    InvalidField { field: String, reason: String },
    Json { field: String, message: String },
}

impl Display for TypedRawError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid typed_raw field {field}: {reason}")
            }
            Self::Json { field, message } => {
                write!(formatter, "invalid typed_raw JSON field {field}: {message}")
            }
        }
    }
}

impl std::error::Error for TypedRawError {}

fn invalid(field: impl Into<String>, reason: impl Into<String>) -> TypedRawError {
    TypedRawError::InvalidField {
        field: field.into(),
        reason: reason.into(),
    }
}

fn require_non_empty(value: &str, field: &str) -> Result<(), TypedRawError> {
    if value.trim().is_empty() {
        return Err(invalid(field, "must not be empty"));
    }
    Ok(())
}

fn require_positive(value: i64, field: &str) -> Result<(), TypedRawError> {
    if value <= 0 {
        return Err(invalid(field, "must be positive"));
    }
    Ok(())
}

fn require_positive_u64(value: u64, field: &str) -> Result<(), TypedRawError> {
    if value == 0 {
        return Err(invalid(field, "must be positive"));
    }
    Ok(())
}

fn require_schema(actual: &str, expected: &str) -> Result<(), TypedRawError> {
    if actual != expected {
        return Err(invalid(
            "schema_version",
            format!("expected {expected}, got {actual}"),
        ));
    }
    Ok(())
}

fn require_sha256(value: &str, field: &str) -> Result<(), TypedRawError> {
    if value.len() != 64
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid(
            field,
            "must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn verify_sha256(payload: &str, expected: &str, field: &str) -> Result<(), TypedRawError> {
    require_sha256(expected, field)?;
    let actual = format!("{:x}", Sha256::digest(payload.as_bytes()));
    if actual != expected {
        return Err(invalid(
            field,
            format!("payload digest mismatch: expected {expected}, got {actual}"),
        ));
    }
    Ok(())
}

fn require_decimal_id(value: &str, field: &str) -> Result<(), TypedRawError> {
    if value.is_empty() || !value.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(invalid(field, "must be an unsigned decimal identifier"));
    }
    Ok(())
}

pub fn normalize_decimal_text(raw: &str, field: &str) -> Result<ExactDecimal, TypedRawError> {
    if raw.is_empty() || raw.trim() != raw || raw.starts_with('+') || raw.contains(['e', 'E']) {
        return Err(invalid(
            field,
            "must be plain base-10 decimal text without whitespace, plus sign, or exponent",
        ));
    }

    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, raw),
    };
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.as_bytes().iter().all(u8::is_ascii_digit)
        || fraction.is_some_and(|value| {
            value.is_empty() || !value.as_bytes().iter().all(u8::is_ascii_digit)
        })
    {
        return Err(invalid(
            field,
            "must contain decimal digits with at most one point",
        ));
    }

    let canonical_integer = integer.trim_start_matches('0');
    let canonical_integer = if canonical_integer.is_empty() {
        "0"
    } else {
        canonical_integer
    };
    let canonical_fraction = fraction.map(|value| value.trim_end_matches('0'));
    let is_zero =
        canonical_integer == "0" && canonical_fraction.is_none_or(|value| value.is_empty());
    let mut canonical = String::new();
    if negative && !is_zero {
        canonical.push('-');
    }
    canonical.push_str(canonical_integer);
    if let Some(fraction) = canonical_fraction.filter(|value| !value.is_empty()) {
        canonical.push('.');
        canonical.push_str(fraction);
    }
    let exact = ExactDecimal(canonical);
    crate::parse_canonical_decimal(&exact, field)
        .map_err(|error| invalid(field, error.to_string()))?;
    Ok(exact)
}

fn require_nonnegative_decimal(raw: &str, field: &str) -> Result<(), TypedRawError> {
    let value = normalize_decimal_text(raw, field)?;
    if value.0.starts_with('-') {
        return Err(invalid(field, "must be non-negative"));
    }
    Ok(())
}

fn require_positive_decimal(raw: &str, field: &str) -> Result<(), TypedRawError> {
    let value = normalize_decimal_text(raw, field)?;
    if value.0 == "0" || value.0.starts_with('-') {
        return Err(invalid(field, "must be positive"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price: String,
    pub quantity: String,
}

pub fn parse_depth_levels_json(
    raw: &str,
    field: &str,
    allow_empty: bool,
) -> Result<Vec<DepthLevel>, TypedRawError> {
    let values =
        serde_json::from_str::<Vec<[String; 2]>>(raw).map_err(|error| TypedRawError::Json {
            field: field.to_owned(),
            message: error.to_string(),
        })?;
    if !allow_empty && values.is_empty() {
        return Err(invalid(field, "must contain at least one depth level"));
    }
    values
        .into_iter()
        .enumerate()
        .map(|(index, [price, quantity])| {
            require_positive_decimal(&price, &format!("{field}[{index}].price"))?;
            require_nonnegative_decimal(&quantity, &format!("{field}[{index}].quantity"))?;
            Ok(DepthLevel { price, quantity })
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataInitialTickEvidence {
    pub raw_text: String,
    pub exact: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataOutcomePriceEvidence {
    pub raw_text: String,
    pub exact: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataOutcomeEvidence {
    pub asset_id: String,
    pub outcome: Option<String>,
    pub price: Option<MetadataOutcomePriceEvidence>,
}

pub fn map_metadata_outcomes(
    asset_ids_raw: &str,
    outcomes_raw: Option<&str>,
    outcome_prices_raw: Option<&str>,
) -> Result<Vec<MetadataOutcomeEvidence>, TypedRawError> {
    let asset_ids = serde_json::from_str::<Vec<String>>(asset_ids_raw).map_err(|error| {
        TypedRawError::Json {
            field: "asset_ids".to_owned(),
            message: error.to_string(),
        }
    })?;
    if asset_ids.len() != 2
        || asset_ids[0] == asset_ids[1]
        || !serde_json::to_string(&asset_ids).is_ok_and(|encoded| encoded == asset_ids_raw)
    {
        return Err(invalid(
            "asset_ids",
            "must be a canonical JSON array of two distinct IDs",
        ));
    }
    for (index, asset_id) in asset_ids.iter().enumerate() {
        require_decimal_id(asset_id, &format!("asset_ids[{index}]"))?;
    }

    let outcomes = outcomes_raw
        .map(|raw| {
            serde_json::from_str::<Vec<String>>(raw).map_err(|error| TypedRawError::Json {
                field: "outcomes_raw".to_owned(),
                message: error.to_string(),
            })
        })
        .transpose()?;
    if let Some(outcomes) = &outcomes
        && (outcomes.len() != asset_ids.len()
            || outcomes.iter().any(|outcome| outcome.trim().is_empty()))
    {
        return Err(invalid(
            "outcomes_raw",
            "must map non-empty outcomes one-to-one to asset_ids",
        ));
    }

    let prices = outcome_prices_raw
        .map(|raw| {
            let prices =
                serde_json::from_str::<Vec<String>>(raw).map_err(|error| TypedRawError::Json {
                    field: "outcome_prices_raw".to_owned(),
                    message: error.to_string(),
                })?;
            if prices.len() != asset_ids.len() {
                return Err(invalid(
                    "outcome_prices_raw",
                    "must map prices one-to-one to asset_ids",
                ));
            }
            prices
                .into_iter()
                .enumerate()
                .map(|(index, raw_text)| {
                    let field = format!("outcome_prices_raw[{index}]");
                    let exact = normalize_decimal_text(&raw_text, &field)?;
                    if exact.0.starts_with('-') {
                        return Err(invalid(field, "must be non-negative"));
                    }
                    Ok(MetadataOutcomePriceEvidence { raw_text, exact })
                })
                .collect::<Result<Vec<_>, TypedRawError>>()
        })
        .transpose()?;

    Ok(asset_ids
        .into_iter()
        .enumerate()
        .map(|(index, asset_id)| MetadataOutcomeEvidence {
            asset_id,
            outcome: outcomes.as_ref().map(|values| values[index].clone()),
            price: prices.as_ref().map(|values| values[index].clone()),
        })
        .collect())
}

#[derive(Deserialize)]
struct MetadataInitialTickPayload {
    #[serde(rename = "orderPriceMinTickSize")]
    order_price_min_tick_size: Option<String>,
}

pub fn extract_metadata_initial_tick(
    source_evidence_payload_json: &str,
) -> Result<Option<MetadataInitialTickEvidence>, TypedRawError> {
    let payload = serde_json::from_str::<MetadataInitialTickPayload>(source_evidence_payload_json)
        .map_err(|error| TypedRawError::Json {
            field: "source_evidence_payload_json".to_owned(),
            message: error.to_string(),
        })?;
    payload
        .order_price_min_tick_size
        .map(|raw_text| {
            let exact = normalize_decimal_text(
                &raw_text,
                "source_evidence_payload_json.orderPriceMinTickSize",
            )?;
            if exact.0 == "0" || exact.0.starts_with('-') {
                return Err(invalid(
                    "source_evidence_payload_json.orderPriceMinTickSize",
                    "must be positive",
                ));
            }
            Ok(MetadataInitialTickEvidence { raw_text, exact })
        })
        .transpose()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketSide {
    Buy,
    Sell,
    Unknown,
}

impl TryFrom<&str> for PolymarketSide {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            "UNKNOWN" => Ok(Self::Unknown),
            _ => Err(invalid(
                "side",
                format!("unsupported Polymarket side {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketPriceLevel {
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketPriceChangeEntry {
    pub asset_id: String,
    pub price: String,
    pub size: Option<String>,
    pub side: PolymarketSide,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketEventMessage {
    pub id: String,
    pub ticker: Option<String>,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketOrderbookEvent {
    Book {
        asset_id: String,
        bids: Vec<PolymarketPriceLevel>,
        asks: Vec<PolymarketPriceLevel>,
        last_trade_price: Option<String>,
    },
    PriceChange {
        price_changes: Vec<PolymarketPriceChangeEntry>,
    },
    TickSizeChange {
        asset_id: String,
        old_tick_size: Option<String>,
        new_tick_size: String,
    },
    LastTradePrice {
        asset_id: String,
        price: String,
        size: Option<String>,
        side: Option<PolymarketSide>,
        fee_rate_bps: Option<String>,
    },
    BestBidAsk {
        asset_id: String,
        best_bid: Option<String>,
        best_ask: Option<String>,
        spread: Option<String>,
    },
    NewMarket {
        clob_market_id: String,
        question: Option<String>,
        description: Option<String>,
        asset_ids: Vec<String>,
        outcomes: Vec<String>,
        event_message: Option<PolymarketEventMessage>,
    },
    MarketResolved {
        clob_market_id: String,
        question: Option<String>,
        description: Option<String>,
        asset_ids: Vec<String>,
        outcomes: Vec<String>,
        event_message: Option<PolymarketEventMessage>,
        winning_asset_id: String,
        winning_outcome: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketOrderbookRow {
    pub timestamp_received: i64,
    pub timestamp: Option<i64>,
    pub market: String,
    pub slug: Option<String>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub event: PolymarketOrderbookEvent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolymarketMetadataRowKind {
    MetadataSnapshot,
    MarketStateTransition,
    ResolutionPollAttempt,
    ResolutionTerminalWs,
    ResolutionFollowupSummary,
}

impl TryFrom<&str> for PolymarketMetadataRowKind {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "metadata_snapshot" => Ok(Self::MetadataSnapshot),
            "market_state_transition" => Ok(Self::MarketStateTransition),
            "resolution_poll_attempt" => Ok(Self::ResolutionPollAttempt),
            "resolution_terminal_ws" => Ok(Self::ResolutionTerminalWs),
            "resolution_followup_summary" => Ok(Self::ResolutionFollowupSummary),
            _ => Err(invalid(
                "metadata_row_kind",
                format!("unsupported metadata row kind {value}"),
            )),
        }
    }
}

macro_rules! closed_string_enum {
    ($name:ident, $field:literal, {$($variant:ident => $raw:literal),+ $(,)?}) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            $($variant),+
        }

        impl TryFrom<&str> for $name {
            type Error = TypedRawError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                match value {
                    $($raw => Ok(Self::$variant),)+
                    _ => Err(invalid($field, format!("unsupported value {value}"))),
                }
            }
        }
    };
}

closed_string_enum!(PolymarketMetadataOrigin, "metadata_origin", {
    GammaMarketsDiscovery => "gamma_markets_discovery",
    GammaBySlug => "gamma_by_slug",
    MarketResolvedWs => "market_resolved_ws",
    LocalMarketStateTimer => "local_market_state_timer",
    LocalFollowupScheduler => "local_followup_scheduler",
});
closed_string_enum!(MetadataMarketLifecycleState, "market_lifecycle_state", {
    Candidate => "candidate",
    Prefetched => "prefetched",
    Active => "active",
    Retired => "retired",
});
closed_string_enum!(MetadataMarketRuntimeRole, "market_runtime_role", {
    Current => "current",
    Next => "next",
    ResolutionPending => "resolution_pending",
    None => "none",
});
closed_string_enum!(
    MetadataSourceEndpointUnavailableReason,
    "source_endpoint_unavailable_reason",
    { NotApplicableNoSourceEvidence => "not_applicable_no_source_evidence" }
);
closed_string_enum!(MetadataSourceEvidenceOrigin, "source_evidence_origin", {
    GammaMarketsDiscovery => "gamma_markets_discovery",
    GammaBySlug => "gamma_by_slug",
    MarketResolvedWs => "market_resolved_ws",
});
closed_string_enum!(MetadataSourceEvidenceEventType, "source_evidence_event_type", {
    GammaMarketsDiscoveryResponse => "gamma_markets_discovery_response",
    GammaMarketBySlugResponse => "gamma_market_by_slug_response",
    MarketResolved => "market_resolved",
});
closed_string_enum!(MetadataSourceEvidenceChannel, "source_evidence_channel", {
    Market => "market",
});
closed_string_enum!(MetadataMarketLifecycleTransition, "market_lifecycle_transition", {
    PrefetchedToActive => "prefetched_to_active",
    CandidateToActive => "candidate_to_active",
    ActiveToRetired => "active_to_retired",
});
closed_string_enum!(
    MetadataMarketRuntimeRoleTransition,
    "market_runtime_role_transition",
    {
        NoneToCurrent => "none_to_current",
        NextToCurrent => "next_to_current",
        CurrentToResolutionPending => "current_to_resolution_pending",
        ResolutionPendingToNone => "resolution_pending_to_none",
    }
);
closed_string_enum!(MetadataClosedTimeCheckFailureReason, "closed_time_check_failure_reason", {
    Missing => "missing",
    ParseError => "parse_error",
    BeforeMarketEnd => "before_market_end",
});
closed_string_enum!(MetadataActiveUnavailableReason, "active_unavailable_reason", {
    SourceFieldAbsent => "source_field_absent",
    NotApplicableForRowKind => "not_applicable_for_row_kind",
});
closed_string_enum!(MetadataResolutionEvidenceSource, "resolution_evidence_source", {
    MarketResolvedWs => "market_resolved_ws",
    GammaBySlug => "gamma_by_slug",
});

pub const fn metadata_winning_asset_id_origin(
    source: MetadataResolutionEvidenceSource,
) -> TypedRawFieldOrigin {
    match source {
        MetadataResolutionEvidenceSource::MarketResolvedWs => TypedRawFieldOrigin::SourceNative,
        MetadataResolutionEvidenceSource::GammaBySlug => TypedRawFieldOrigin::CollectorDerived,
    }
}

closed_string_enum!(MetadataTerminalClaimOwner, "terminal_claim_owner", {
    MarketResolvedWs => "market_resolved_ws",
    GammaBySlug => "gamma_by_slug",
});
closed_string_enum!(MetadataResolutionPollResult, "resolution_poll_result", {
    Terminal => "terminal",
    ValidNonterminal => "valid_nonterminal",
    TransportUnavailable => "transport_unavailable",
    IntegrityError => "integrity_error",
    MissedDueToDowntime => "missed_due_to_downtime",
    PostTerminalClaimObservation => "post_terminal_claim_observation",
});
closed_string_enum!(
    MetadataResolutionTransportErrorClass,
    "resolution_transport_error_class",
    {
        Connect => "connect",
        Timeout => "timeout",
        HttpStatus => "http_status",
        RateLimited => "rate_limited",
        SdkTransport => "sdk_transport",
    }
);
closed_string_enum!(
    MetadataResolutionIntegrityErrorClass,
    "resolution_integrity_error_class",
    {
        AttemptBindingInvariant => "attempt_binding_invariant",
        SdkDecodeOrSchema => "sdk_decode_or_schema",
        SourceIdentityOrShapeContradiction => "source_identity_or_shape_contradiction",
        ClosedTimeInvalid => "closed_time_invalid",
        DerivedWinnerInvariant => "derived_winner_invariant",
    }
);
closed_string_enum!(MetadataResolutionFollowupStatus, "resolution_followup_status", {
    Pending => "pending",
    TerminalWs => "terminal_ws",
    TerminalGammaFallback => "terminal_gamma_fallback",
    BudgetExhaustedAfterValidNonterminal => "budget_exhausted_after_valid_nonterminal",
    BudgetExhaustedWithoutValidResponse => "budget_exhausted_without_valid_response",
});
closed_string_enum!(
    MetadataTerminalPredicateFailureReason,
    "terminal_predicate_failure_reason",
    {
        SlugMismatch => "slug_mismatch",
        MarketIdMismatch => "market_id_mismatch",
        ConditionIdMismatch => "condition_id_mismatch",
        AssetCountNotTwo => "asset_count_not_two",
        AssetDuplicate => "asset_duplicate",
        AssetSetMismatch => "asset_set_mismatch",
        OutcomesCountNotTwo => "outcomes_count_not_two",
        OutcomeMappingInvalid => "outcome_mapping_invalid",
        OutcomePricesCountNotTwo => "outcome_prices_count_not_two",
        ClosedMissing => "closed_missing",
        ClosedFalse => "closed_false",
        AcceptingOrdersMissing => "accepting_orders_missing",
        AcceptingOrdersTrue => "accepting_orders_true",
        ClosedTimeMissing => "closed_time_missing",
        ClosedTimeParseError => "closed_time_parse_error",
        ClosedTimeBeforeMarketEnd => "closed_time_before_market_end",
        UmaResolutionStatusMissing => "uma_resolution_status_missing",
        UmaResolutionStatusNotResolved => "uma_resolution_status_not_resolved",
        OutcomePricesNotOneHot => "outcome_prices_not_one_hot",
    }
);
closed_string_enum!(
    MetadataResponseFieldsUnavailableReason,
    "response_fields_unavailable_reason",
    {
        NotApplicableForRowKind => "not_applicable_for_row_kind",
        TransportUnavailable => "transport_unavailable",
        MissedDueToDowntime => "missed_due_to_downtime",
        SdkDecodeOrSchemaError => "sdk_decode_or_schema_error",
    }
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketMarketMetadataRow {
    pub metadata_row_kind: PolymarketMetadataRowKind,
    pub timestamp_received: i64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub metadata_origin: PolymarketMetadataOrigin,
    pub collector_host_id: String,
    pub collector_region: String,
    pub schema_version: String,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub asset_ids: String,
    pub market_start_utc: i64,
    pub market_end_utc: i64,
    pub fixed_identity_verified_at_utc: i64,
    pub market_lifecycle_state: MetadataMarketLifecycleState,
    pub market_runtime_role: MetadataMarketRuntimeRole,
    pub source_endpoint: Option<String>,
    pub source_endpoint_unavailable_reason: Option<MetadataSourceEndpointUnavailableReason>,
    pub source_evidence_origin: Option<MetadataSourceEvidenceOrigin>,
    pub source_evidence_event_type: Option<MetadataSourceEvidenceEventType>,
    pub source_evidence_channel: Option<MetadataSourceEvidenceChannel>,
    pub source_evidence_received_at_utc: Option<i64>,
    pub row_evidence_validation_completed_at_utc: Option<i64>,
    pub source_evidence_payload_json: Option<String>,
    pub source_evidence_raw_sha256: Option<String>,
    pub market_lifecycle_transition: Option<MetadataMarketLifecycleTransition>,
    pub lifecycle_scheduled_at_utc: Option<i64>,
    pub lifecycle_applied_at_utc: Option<i64>,
    pub market_runtime_role_transition: Option<MetadataMarketRuntimeRoleTransition>,
    pub runtime_role_scheduled_at_utc: Option<i64>,
    pub runtime_role_applied_at_utc: Option<i64>,
    pub metadata_fetch_start_utc: Option<i64>,
    pub closed: Option<bool>,
    pub closed_time_raw: Option<String>,
    pub closed_time_utc: Option<i64>,
    pub closed_time_check_passed: Option<bool>,
    pub closed_time_check_failure_reason: Option<MetadataClosedTimeCheckFailureReason>,
    pub automatically_resolved: Option<bool>,
    pub uma_resolution_status: Option<String>,
    pub outcomes_raw: Option<String>,
    pub outcome_prices_raw: Option<String>,
    pub active: Option<bool>,
    pub active_unavailable_reason: Option<MetadataActiveUnavailableReason>,
    pub accepting_orders: Option<bool>,
    pub ws_platform_timestamp_raw: Option<String>,
    pub ws_platform_timestamp_utc: Option<i64>,
    pub winning_asset_id: Option<String>,
    pub winning_outcome: Option<String>,
    pub resolution_evidence_source: Option<MetadataResolutionEvidenceSource>,
    pub terminal_claim_owner: Option<MetadataTerminalClaimOwner>,
    pub resolution_poll_attempt_ordinal: Option<u8>,
    pub resolution_poll_scheduled_utc: Option<i64>,
    pub resolution_poll_result: Option<MetadataResolutionPollResult>,
    pub resolution_transport_error_class: Option<MetadataResolutionTransportErrorClass>,
    pub resolution_transport_http_status: Option<u16>,
    pub resolution_integrity_error_class: Option<MetadataResolutionIntegrityErrorClass>,
    pub resolution_budget_horizon_utc: Option<i64>,
    pub resolution_followup_status: Option<MetadataResolutionFollowupStatus>,
    pub resolution_poll_attempts_recorded: Option<u32>,
    pub resolution_valid_response_count: Option<u32>,
    pub resolution_transport_unavailable_count: Option<u32>,
    pub resolution_missed_slot_count: Option<u32>,
    pub last_valid_metadata_received_utc: Option<i64>,
    pub terminal_predicate_passed: Option<bool>,
    pub terminal_predicate_failure_reason: Option<MetadataTerminalPredicateFailureReason>,
    pub response_fields_unavailable_reason: Option<MetadataResponseFieldsUnavailableReason>,
    pub ws_claimant_source_sequence_epoch_id: Option<String>,
    pub ws_claimant_collector_admission_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RtdsFreshnessStatus {
    Fresh,
    Stale,
    InvalidType,
    InvalidTopic,
    InvalidSymbol,
    InvalidTimestamp,
}

impl TryFrom<&str> for RtdsFreshnessStatus {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "fresh" => Ok(Self::Fresh),
            "stale" => Ok(Self::Stale),
            "invalid_type" => Ok(Self::InvalidType),
            "invalid_topic" => Ok(Self::InvalidTopic),
            "invalid_symbol" => Ok(Self::InvalidSymbol),
            "invalid_timestamp" => Ok(Self::InvalidTimestamp),
            _ => Err(invalid(
                "freshness_status",
                format!("unsupported RTDS freshness status {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketRtdsChainlinkBtcUsdRow {
    pub timestamp_received: i64,
    pub message_timestamp: i64,
    pub payload_timestamp: i64,
    pub age_ms: Option<i64>,
    pub symbol: String,
    pub value: String,
    pub topic: String,
    pub message_type: String,
    pub freshness_status: RtdsFreshnessStatus,
    pub subscription_filter: String,
    pub source_native_payload_json: String,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceDepthSnapshotRow {
    pub timestamp_received: i64,
    pub snapshot_fetch_start_utc: i64,
    pub last_update_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
    pub symbol: String,
    pub source_endpoint: String,
    pub snapshot_status: String,
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub depth_ingress_sequence: u64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub schema_version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceDepthStitchVerdict {
    Applied,
    NotAppliedStaleDuplicate,
    NotAppliedConfirmedHole,
    NotAppliedSnapshotUnavailable,
}

impl TryFrom<&str> for BinanceDepthStitchVerdict {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "applied" => Ok(Self::Applied),
            "not_applied_stale_duplicate" => Ok(Self::NotAppliedStaleDuplicate),
            "not_applied_confirmed_hole" => Ok(Self::NotAppliedConfirmedHole),
            "not_applied_snapshot_unavailable" => Ok(Self::NotAppliedSnapshotUnavailable),
            _ => Err(invalid(
                "stitch_verdict",
                format!("unsupported Binance depth stitch verdict {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceDepthUpdateRow {
    pub timestamp_received: i64,
    pub event_type: String,
    pub event_time: i64,
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
    pub stream: String,
    pub snapshot_last_update_id: Option<u64>,
    pub local_u_before: Option<u64>,
    pub stitch_verdict: BinanceDepthStitchVerdict,
    pub stitch_evidence: Option<String>,
    pub stitch_gap_reason: Option<String>,
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub depth_ingress_sequence: u64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceAggTradeCaptureStatus {
    Complete,
    OversizeNotCaptured,
}

impl BinanceAggTradeCaptureStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::OversizeNotCaptured => "oversize_not_captured",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceAggTradeInvalidReason {
    BodyTooLarge,
    InvalidJson,
    InvalidPayload,
}

impl BinanceAggTradeInvalidReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "body_too_large",
            Self::InvalidJson => "invalid_json",
            Self::InvalidPayload => "invalid_payload",
        }
    }
}

impl TryFrom<&str> for BinanceAggTradeInvalidReason {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "body_too_large" => Ok(Self::BodyTooLarge),
            "invalid_json" => Ok(Self::InvalidJson),
            "invalid_payload" => Ok(Self::InvalidPayload),
            _ => Err(invalid(
                "invalid_reason_code",
                format!("unsupported Binance aggTrade invalid reason {value}"),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceAggTradePayloadEncoding {
    Base64,
}

impl BinanceAggTradePayloadEncoding {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Base64 => "base64",
        }
    }
}

impl TryFrom<&str> for BinanceAggTradePayloadEncoding {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "base64" => Ok(Self::Base64),
            _ => Err(invalid(
                "payload_encoding",
                format!("unsupported Binance aggTrade payload encoding {value}"),
            )),
        }
    }
}

impl TryFrom<&str> for BinanceAggTradeCaptureStatus {
    type Error = TypedRawError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "complete" => Ok(Self::Complete),
            "oversize_not_captured" => Ok(Self::OversizeNotCaptured),
            _ => Err(invalid(
                "payload_capture_status",
                format!("unsupported payload capture status {value}"),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceAggTradeRecord {
    Trade {
        event_type: String,
        event_time: i64,
        trade_time: i64,
        symbol: String,
        aggregate_trade_id: u64,
        price: String,
        quantity: String,
        first_trade_id: u64,
        last_trade_id: u64,
        buyer_is_maker: bool,
        stream: String,
        ws_ignore: bool,
    },
    InvalidInput {
        invalid_reason_code: BinanceAggTradeInvalidReason,
        invalid_reason_detail: Option<String>,
        payload_encoding: Option<BinanceAggTradePayloadEncoding>,
        payload_length_bytes: Option<u64>,
        payload_sha256: Option<String>,
        payload_capture_status: BinanceAggTradeCaptureStatus,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeRow {
    pub timestamp_received: i64,
    pub rest_backfill_timestamp_received: Option<i64>,
    pub ingest_source: String,
    pub rest_was_best_price_match: Option<bool>,
    pub source_native_payload_json: String,
    pub source_sequence_epoch_id: String,
    pub connection_generation: u64,
    pub websocket_ingress_sequence: u64,
    pub rest_response_ingress_sequence: Option<u64>,
    pub rest_response_item_index: Option<u64>,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
    pub record: BinanceAggTradeRecord,
}

#[derive(Deserialize)]
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

fn validate_binance_aggtrade_invalid_evidence(
    row: &BinanceAggTradeRow,
    invalid_reason_code: BinanceAggTradeInvalidReason,
    payload_encoding: Option<BinanceAggTradePayloadEncoding>,
    payload_length_bytes: Option<u64>,
    payload_sha256: Option<&str>,
    payload_capture_status: BinanceAggTradeCaptureStatus,
) -> Result<(), TypedRawError> {
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
        .map_err(|error| TypedRawError::Json {
            field: "source_native_payload_json".to_owned(),
            message: error.to_string(),
        })?;
    let object = value.as_object().ok_or_else(|| {
        invalid(
            "source_native_payload_json",
            "invalid evidence envelope must be a JSON object",
        )
    })?;
    if REQUIRED_FIELDS
        .iter()
        .any(|field| !object.contains_key(*field))
    {
        return Err(invalid(
            "source_native_payload_json",
            "invalid evidence envelope is missing a required field",
        ));
    }
    let evidence = serde_json::from_value::<BinanceAggTradeInvalidEvidenceEnvelope>(value)
        .map_err(|error| TypedRawError::Json {
            field: "source_native_payload_json".to_owned(),
            message: error.to_string(),
        })?;

    if evidence.evidence_schema != BINANCE_AGGTRADE_INVALID_EVIDENCE_SCHEMA {
        return Err(invalid(
            "source_native_payload_json",
            "invalid evidence schema does not match the frozen v1 envelope",
        ));
    }
    if evidence.transport != row.ingest_source
        || evidence.endpoint != row.source_endpoint
        || evidence.receive_time_utc_ms != row.timestamp_received
        || evidence.invalid_reason_code != invalid_reason_code.as_str()
        || evidence.payload_capture_status != payload_capture_status.as_str()
        || evidence.payload_encoding.as_deref() != payload_encoding.map(|value| value.as_str())
        || evidence.payload_length_bytes != payload_length_bytes
        || evidence.payload_sha256.as_deref() != payload_sha256
    {
        return Err(invalid(
            "source_native_payload_json",
            "invalid evidence envelope disagrees with its typed_raw row",
        ));
    }
    if evidence.capture_limit_bytes != BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES {
        return Err(invalid(
            "source_native_payload_json",
            "invalid evidence capture limit does not match the frozen boundary",
        ));
    }

    match payload_capture_status {
        BinanceAggTradeCaptureStatus::Complete => {
            if evidence.payload_encoding.as_deref() != Some("base64") {
                return Err(invalid(
                    "source_native_payload_json",
                    "complete invalid evidence must use base64",
                ));
            }
            let encoded = evidence.payload_base64.as_deref().ok_or_else(|| {
                invalid(
                    "source_native_payload_json",
                    "complete invalid evidence must contain payload_base64",
                )
            })?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| TypedRawError::Json {
                    field: "source_native_payload_json.payload_base64".to_owned(),
                    message: error.to_string(),
                })?;
            let decoded_length = u64::try_from(decoded.len()).map_err(|_| {
                invalid(
                    "source_native_payload_json.payload_base64",
                    "decoded length does not fit u64",
                )
            })?;
            if decoded_length > BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES
                || evidence.payload_length_bytes != Some(decoded_length)
                || evidence.observed_bytes_at_least != decoded_length
                || evidence
                    .declared_content_length
                    .is_some_and(|declared| declared > BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES)
            {
                return Err(invalid(
                    "source_native_payload_json",
                    "complete invalid evidence length facts are inconsistent",
                ));
            }
            let expected_sha256 = evidence.payload_sha256.as_deref().ok_or_else(|| {
                invalid(
                    "source_native_payload_json.payload_sha256",
                    "complete invalid evidence must contain a digest",
                )
            })?;
            require_sha256(expected_sha256, "source_native_payload_json.payload_sha256")?;
            let actual_sha256 = format!("{:x}", Sha256::digest(&decoded));
            if actual_sha256 != expected_sha256 {
                return Err(invalid(
                    "source_native_payload_json.payload_sha256",
                    "invalid evidence payload digest mismatch",
                ));
            }
        }
        BinanceAggTradeCaptureStatus::OversizeNotCaptured => {
            if evidence.payload_encoding.is_some()
                || evidence.payload_base64.is_some()
                || evidence.payload_sha256.is_some()
                || evidence.payload_length_bytes != evidence.declared_content_length
                || (evidence.observed_bytes_at_least <= BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES
                    && evidence.declared_content_length.is_none_or(|declared| {
                        declared <= BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES
                    }))
            {
                return Err(invalid(
                    "source_native_payload_json",
                    "oversize invalid evidence facts are inconsistent",
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypedRawRow {
    PolymarketOrderbook(PolymarketOrderbookRow),
    PolymarketMarketMetadata(Box<PolymarketMarketMetadataRow>),
    PolymarketRtdsChainlinkBtcUsd(PolymarketRtdsChainlinkBtcUsdRow),
    BinanceDepthSnapshot(BinanceDepthSnapshotRow),
    BinanceDepthUpdate(BinanceDepthUpdateRow),
    BinanceAggTrade(BinanceAggTradeRow),
}

impl TypedRawRow {
    pub const fn source_kind(&self) -> TypedRawSourceKind {
        match self {
            Self::PolymarketOrderbook(_) => TypedRawSourceKind::PolymarketOrderbook,
            Self::PolymarketMarketMetadata(_) => TypedRawSourceKind::PolymarketMarketMetadata,
            Self::PolymarketRtdsChainlinkBtcUsd(_) => {
                TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd
            }
            Self::BinanceDepthSnapshot(_) => TypedRawSourceKind::BinanceDepthSnapshot,
            Self::BinanceDepthUpdate(_) => TypedRawSourceKind::BinanceDepthUpdate,
            Self::BinanceAggTrade(_) => TypedRawSourceKind::BinanceAggTrade,
        }
    }

    pub fn validate(&self) -> Result<(), TypedRawError> {
        match self {
            Self::PolymarketOrderbook(row) => row.validate(),
            Self::PolymarketMarketMetadata(row) => row.validate(),
            Self::PolymarketRtdsChainlinkBtcUsd(row) => row.validate(),
            Self::BinanceDepthSnapshot(row) => row.validate(),
            Self::BinanceDepthUpdate(row) => row.validate(),
            Self::BinanceAggTrade(row) => row.validate(),
        }
    }
}

impl PolymarketOrderbookRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(&self.schema_version, POLYMARKET_ORDERBOOK_SCHEMA)?;
        require_positive(self.timestamp_received, "timestamp_received")?;
        if let Some(timestamp) = self.timestamp {
            require_positive(timestamp, "timestamp")?;
        }
        require_non_empty(&self.market, "market")?;
        require_non_empty(&self.collector_host_id, "collector_host_id")?;
        require_non_empty(&self.collector_region, "collector_region")?;
        require_non_empty(&self.source_endpoint, "source_endpoint")?;
        require_non_empty(&self.source_sequence_epoch_id, "source_sequence_epoch_id")?;
        require_positive_u64(
            self.collector_admission_sequence,
            "collector_admission_sequence",
        )?;
        require_positive(
            self.collector_admission_time_utc_ms,
            "collector_admission_time_utc_ms",
        )?;
        if self.timestamp_received > self.collector_admission_time_utc_ms {
            return Err(invalid(
                "timestamp_received",
                "must not exceed collector_admission_time_utc_ms",
            ));
        }
        validate_polymarket_event(&self.event)
    }
}

fn validate_polymarket_event(event: &PolymarketOrderbookEvent) -> Result<(), TypedRawError> {
    match event {
        PolymarketOrderbookEvent::Book {
            asset_id,
            bids,
            asks,
            last_trade_price,
        } => {
            require_decimal_id(asset_id, "book.asset_id")?;
            for (index, level) in bids.iter().chain(asks).enumerate() {
                require_positive_decimal(&level.price, &format!("book.levels[{index}].price"))?;
                require_positive_decimal(&level.size, &format!("book.levels[{index}].size"))?;
            }
            if let Some(price) = last_trade_price {
                require_positive_decimal(price, "book.last_trade_price")?;
            }
        }
        PolymarketOrderbookEvent::PriceChange { price_changes } => {
            if price_changes.is_empty() {
                return Err(invalid("price_changes", "must contain at least one entry"));
            }
            for (index, change) in price_changes.iter().enumerate() {
                require_decimal_id(
                    &change.asset_id,
                    &format!("price_changes[{index}].asset_id"),
                )?;
                require_positive_decimal(&change.price, &format!("price_changes[{index}].price"))?;
                if let Some(size) = &change.size {
                    require_nonnegative_decimal(size, &format!("price_changes[{index}].size"))?;
                }
                for (name, value) in [
                    ("best_bid", change.best_bid.as_deref()),
                    ("best_ask", change.best_ask.as_deref()),
                ] {
                    if let Some(value) = value {
                        require_nonnegative_decimal(
                            value,
                            &format!("price_changes[{index}].{name}"),
                        )?;
                    }
                }
            }
        }
        PolymarketOrderbookEvent::TickSizeChange {
            asset_id,
            old_tick_size,
            new_tick_size,
        } => {
            require_decimal_id(asset_id, "tick_size_change.asset_id")?;
            if let Some(old) = old_tick_size {
                require_positive_decimal(old, "tick_size_change.old_tick_size")?;
            }
            require_positive_decimal(new_tick_size, "tick_size_change.new_tick_size")?;
        }
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id,
            price,
            size,
            fee_rate_bps,
            ..
        } => {
            require_decimal_id(asset_id, "last_trade_price.asset_id")?;
            require_positive_decimal(price, "last_trade_price.price")?;
            if let Some(size) = size {
                require_positive_decimal(size, "last_trade_price.size")?;
            }
            if let Some(fee) = fee_rate_bps {
                require_nonnegative_decimal(fee, "last_trade_price.fee_rate_bps")?;
            }
        }
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id,
            best_bid,
            best_ask,
            spread,
        } => {
            require_decimal_id(asset_id, "best_bid_ask.asset_id")?;
            for (field, value) in [
                ("best_bid_ask.best_bid", best_bid.as_deref()),
                ("best_bid_ask.best_ask", best_ask.as_deref()),
                ("best_bid_ask.spread", spread.as_deref()),
            ] {
                if let Some(value) = value {
                    require_nonnegative_decimal(value, field)?;
                }
            }
        }
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id,
            asset_ids,
            outcomes,
            event_message,
            ..
        } => validate_polymarket_market_identity(
            clob_market_id,
            asset_ids,
            outcomes,
            event_message.as_ref(),
            true,
        )?,
        PolymarketOrderbookEvent::MarketResolved {
            clob_market_id,
            asset_ids,
            outcomes,
            event_message,
            winning_asset_id,
            winning_outcome,
            ..
        } => {
            validate_polymarket_market_identity(
                clob_market_id,
                asset_ids,
                outcomes,
                event_message.as_ref(),
                false,
            )?;
            require_decimal_id(winning_asset_id, "market_resolved.winning_asset_id")?;
            require_non_empty(winning_outcome, "market_resolved.winning_outcome")?;
            if !asset_ids.contains(winning_asset_id) {
                return Err(invalid(
                    "market_resolved.winning_asset_id",
                    "must belong to asset_ids",
                ));
            }
        }
    }
    Ok(())
}

fn validate_polymarket_market_identity(
    clob_market_id: &str,
    asset_ids: &[String],
    outcomes: &[String],
    event_message: Option<&PolymarketEventMessage>,
    outcomes_required: bool,
) -> Result<(), TypedRawError> {
    require_non_empty(clob_market_id, "clob_market_id")?;
    if asset_ids.is_empty() {
        return Err(invalid("asset_ids", "must not be empty"));
    }
    for (index, asset_id) in asset_ids.iter().enumerate() {
        require_decimal_id(asset_id, &format!("asset_ids[{index}]"))?;
    }
    if (outcomes_required && outcomes.is_empty())
        || (!outcomes.is_empty() && outcomes.len() != asset_ids.len())
        || outcomes.iter().any(|value| value.trim().is_empty())
    {
        return Err(invalid("outcomes", "must map one-to-one to asset_ids"));
    }
    if let Some(message) = event_message {
        require_non_empty(&message.id, "event_message.id")?;
    }
    Ok(())
}

impl PolymarketMarketMetadataRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(&self.schema_version, POLYMARKET_MARKET_METADATA_SCHEMA)?;
        require_positive(self.timestamp_received, "timestamp_received")?;
        require_positive_u64(
            self.collector_admission_sequence,
            "collector_admission_sequence",
        )?;
        require_positive(
            self.collector_admission_time_utc_ms,
            "collector_admission_time_utc_ms",
        )?;
        require_non_empty(&self.collector_host_id, "collector_host_id")?;
        require_non_empty(&self.collector_region, "collector_region")?;
        require_non_empty(&self.slug, "slug")?;
        require_non_empty(&self.condition_id, "condition_id")?;
        require_non_empty(&self.clob_market_id, "clob_market_id")?;
        require_positive(self.market_start_utc, "market_start_utc")?;
        require_positive(self.market_end_utc, "market_end_utc")?;
        require_positive(
            self.fixed_identity_verified_at_utc,
            "fixed_identity_verified_at_utc",
        )?;
        let outcome_evidence = map_metadata_outcomes(
            &self.asset_ids,
            self.outcomes_raw.as_deref(),
            self.outcome_prices_raw.as_deref(),
        )?;
        match (
            self.source_evidence_payload_json.as_deref(),
            self.source_evidence_raw_sha256.as_deref(),
        ) {
            (Some(payload), Some(digest)) => {
                let _ = extract_metadata_initial_tick(payload)?;
                verify_sha256(payload, digest, "source_evidence_raw_sha256")?;
            }
            (Some(_), None) => {
                return Err(invalid(
                    "source_evidence_raw_sha256",
                    "is required when source_evidence_payload_json is present",
                ));
            }
            (None, Some(digest)) => {
                // metadata v5 permits a digest-only local reference to a unique payload authority.
                require_sha256(digest, "source_evidence_raw_sha256")?;
            }
            (None, None) => {}
        }
        match (
            self.ws_claimant_source_sequence_epoch_id.as_deref(),
            self.ws_claimant_collector_admission_sequence,
        ) {
            (Some(epoch), Some(sequence)) => {
                require_non_empty(epoch, "ws_claimant_source_sequence_epoch_id")?;
                require_positive_u64(sequence, "ws_claimant_collector_admission_sequence")?;
                if self.metadata_row_kind != PolymarketMetadataRowKind::ResolutionTerminalWs {
                    return Err(invalid(
                        "ws_claimant_source_sequence_epoch_id",
                        "is only valid for resolution_terminal_ws",
                    ));
                }
            }
            (None, None) => {
                if self.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs {
                    return Err(invalid(
                        "ws_claimant_source_sequence_epoch_id",
                        "is required for resolution_terminal_ws",
                    ));
                }
            }
            _ => {
                return Err(invalid(
                    "ws_claimant",
                    "epoch and admission sequence must be both present or both absent",
                ));
            }
        }
        if let Some(timestamp) = self.ws_platform_timestamp_utc {
            require_positive(timestamp, "ws_platform_timestamp_utc")?;
        }
        let waiting_exhausted = matches!(
            self.resolution_followup_status,
            Some(
                MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal
                    | MetadataResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse
            )
        );
        if self.metadata_row_kind == PolymarketMetadataRowKind::ResolutionFollowupSummary
            && !waiting_exhausted
        {
            return Err(invalid(
                "resolution_followup_status",
                "resolution_followup_summary requires a budget-exhausted status",
            ));
        }
        if self.metadata_row_kind != PolymarketMetadataRowKind::ResolutionFollowupSummary
            && waiting_exhausted
        {
            return Err(invalid(
                "resolution_followup_status",
                "budget-exhausted status is only valid for resolution_followup_summary",
            ));
        }
        validate_metadata_winner(
            &outcome_evidence,
            self.winning_asset_id.as_deref(),
            self.winning_outcome.as_deref(),
            self.resolution_evidence_source,
        )?;
        Ok(())
    }
}

fn validate_metadata_winner(
    outcome_evidence: &[MetadataOutcomeEvidence],
    winning_asset_id: Option<&str>,
    winning_outcome: Option<&str>,
    resolution_evidence_source: Option<MetadataResolutionEvidenceSource>,
) -> Result<(), TypedRawError> {
    match (
        winning_asset_id,
        winning_outcome,
        resolution_evidence_source,
    ) {
        (Some(asset_id), Some(winning_outcome), Some(source)) => {
            require_decimal_id(asset_id, "winning_asset_id")?;
            require_non_empty(winning_outcome, "winning_outcome")?;
            let matching_asset = outcome_evidence
                .iter()
                .find(|evidence| evidence.asset_id == asset_id)
                .ok_or_else(|| invalid("winning_asset_id", "must belong to asset_ids"))?;
            if matching_asset
                .outcome
                .as_deref()
                .is_some_and(|outcome| !outcome.trim().eq_ignore_ascii_case(winning_outcome.trim()))
            {
                return Err(invalid(
                    "winning_outcome",
                    "must match the outcome paired with winning_asset_id",
                ));
            }
            let _ = metadata_winning_asset_id_origin(source);
        }
        (Some(_), Some(_), None) => {
            return Err(invalid(
                "resolution_evidence_source",
                "is required to resolve winning_asset_id origin",
            ));
        }
        (None, None, Some(_)) => {
            return Err(invalid(
                "winning_asset_id",
                "is required when resolution_evidence_source is present",
            ));
        }
        (None, None, None) => {}
        _ => {
            return Err(invalid(
                "winning_asset_id/winning_outcome",
                "must be both present or both absent",
            ));
        }
    }
    Ok(())
}

impl PolymarketRtdsChainlinkBtcUsdRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(
            &self.schema_version,
            POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA,
        )?;
        require_positive(self.timestamp_received, "timestamp_received")?;
        if self.freshness_status != RtdsFreshnessStatus::InvalidTimestamp {
            require_positive(self.message_timestamp, "message_timestamp")?;
            require_positive(self.payload_timestamp, "payload_timestamp")?;
            let expected_age = self
                .timestamp_received
                .checked_sub(self.payload_timestamp)
                .ok_or_else(|| invalid("age_ms", "timestamp subtraction overflow"))?;
            if self.age_ms != Some(expected_age) {
                return Err(invalid(
                    "age_ms",
                    "must equal receive time minus payload time",
                ));
            }
        }
        require_non_empty(&self.symbol, "symbol")?;
        require_positive_decimal(&self.value, "value")?;
        require_non_empty(&self.topic, "topic")?;
        require_non_empty(&self.message_type, "message_type")?;
        require_non_empty(&self.subscription_filter, "subscription_filter")?;
        require_non_empty(
            &self.source_native_payload_json,
            "source_native_payload_json",
        )?;
        require_non_empty(&self.collector_host_id, "collector_host_id")?;
        require_non_empty(&self.collector_region, "collector_region")?;
        require_non_empty(&self.source_endpoint, "source_endpoint")
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_depth_common(
    timestamp_received: i64,
    source_sequence_epoch_id: &str,
    connection_generation: u64,
    depth_ingress_sequence: u64,
    collector_admission_sequence: u64,
    collector_admission_time_utc_ms: i64,
    collector_host_id: &str,
    collector_region: &str,
    source_endpoint: &str,
) -> Result<(), TypedRawError> {
    require_positive(timestamp_received, "timestamp_received")?;
    require_non_empty(source_sequence_epoch_id, "source_sequence_epoch_id")?;
    require_positive_u64(connection_generation, "connection_generation")?;
    require_positive_u64(depth_ingress_sequence, "depth_ingress_sequence")?;
    require_positive_u64(collector_admission_sequence, "collector_admission_sequence")?;
    require_positive(
        collector_admission_time_utc_ms,
        "collector_admission_time_utc_ms",
    )?;
    require_non_empty(collector_host_id, "collector_host_id")?;
    require_non_empty(collector_region, "collector_region")?;
    require_non_empty(source_endpoint, "source_endpoint")
}

fn validate_depth_levels(levels: &[DepthLevel], field: &str) -> Result<(), TypedRawError> {
    let mut prices = BTreeSet::new();
    for (index, level) in levels.iter().enumerate() {
        let price_field = format!("{field}[{index}].price");
        let canonical_price = normalize_decimal_text(&level.price, &price_field)?;
        if canonical_price.0 == "0" || canonical_price.0.starts_with('-') {
            return Err(invalid(&price_field, "must be positive"));
        }
        require_nonnegative_decimal(&level.quantity, &format!("{field}[{index}].quantity"))?;
        if !prices.insert(canonical_price.0) {
            return Err(invalid(field, "contains a duplicate canonical price"));
        }
    }
    Ok(())
}

impl BinanceDepthSnapshotRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(&self.schema_version, BINANCE_DEPTH_SNAPSHOT_SCHEMA)?;
        validate_depth_common(
            self.timestamp_received,
            &self.source_sequence_epoch_id,
            self.connection_generation,
            self.depth_ingress_sequence,
            self.collector_admission_sequence,
            self.collector_admission_time_utc_ms,
            &self.collector_host_id,
            &self.collector_region,
            &self.source_endpoint,
        )?;
        require_positive(self.snapshot_fetch_start_utc, "snapshot_fetch_start_utc")?;
        require_positive_u64(self.last_update_id, "last_update_id")?;
        if self.bids.is_empty() || self.asks.is_empty() {
            return Err(invalid(
                "bids/asks",
                "snapshot sides must both be non-empty",
            ));
        }
        validate_depth_levels(&self.bids, "bids")?;
        validate_depth_levels(&self.asks, "asks")?;
        if self.symbol != "BTCUSDT" {
            return Err(invalid("symbol", "must equal BTCUSDT"));
        }
        if self.snapshot_status != "complete" {
            return Err(invalid("snapshot_status", "must equal complete"));
        }
        Ok(())
    }
}

impl BinanceDepthUpdateRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(&self.schema_version, BINANCE_DEPTH_UPDATE_SCHEMA)?;
        validate_depth_common(
            self.timestamp_received,
            &self.source_sequence_epoch_id,
            self.connection_generation,
            self.depth_ingress_sequence,
            self.collector_admission_sequence,
            self.collector_admission_time_utc_ms,
            &self.collector_host_id,
            &self.collector_region,
            &self.source_endpoint,
        )?;
        require_positive(self.event_time, "event_time")?;
        require_positive_u64(self.first_update_id, "first_update_id")?;
        if self.final_update_id < self.first_update_id {
            return Err(invalid(
                "final_update_id",
                "must be at least first_update_id",
            ));
        }
        if self.bids.is_empty() && self.asks.is_empty() {
            return Err(invalid("bids/asks", "update must change at least one side"));
        }
        validate_depth_levels(&self.bids, "bids")?;
        validate_depth_levels(&self.asks, "asks")?;
        if self.event_type != "depthUpdate" {
            return Err(invalid("event_type", "must equal depthUpdate"));
        }
        if self.symbol != "BTCUSDT" {
            return Err(invalid("symbol", "must equal BTCUSDT"));
        }
        if self.stream != "btcusdt@depth" {
            return Err(invalid("stream", "must equal btcusdt@depth"));
        }
        if self.stitch_verdict != BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable
            && self
                .stitch_evidence
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(invalid(
                "stitch_evidence",
                "is required for every verdict except snapshot unavailable",
            ));
        }
        match self.stitch_verdict {
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable => {
                if self.snapshot_last_update_id.is_some()
                    || self.local_u_before.is_some()
                    || self.stitch_gap_reason.is_some()
                {
                    return Err(invalid(
                        "stitch_verdict",
                        "snapshot-unavailable row must not claim snapshot coordinates or gap",
                    ));
                }
            }
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole => {
                if self.snapshot_last_update_id.is_none_or(|value| value == 0)
                    || self
                        .stitch_gap_reason
                        .as_deref()
                        .is_none_or(|value| value.trim().is_empty())
                {
                    return Err(invalid(
                        "stitch_verdict",
                        "confirmed-hole row requires snapshot and gap evidence",
                    ));
                }
            }
            BinanceDepthStitchVerdict::Applied
            | BinanceDepthStitchVerdict::NotAppliedStaleDuplicate => {
                if self.snapshot_last_update_id.is_none_or(|value| value == 0)
                    || self.stitch_gap_reason.is_some()
                {
                    return Err(invalid(
                        "stitch_verdict",
                        "row requires snapshot evidence and no gap reason",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl BinanceAggTradeRow {
    pub fn validate(&self) -> Result<(), TypedRawError> {
        require_schema(&self.schema_version, BINANCE_AGGTRADE_SCHEMA)?;
        require_positive(self.timestamp_received, "timestamp_received")?;
        if self.rest_backfill_timestamp_received.is_some()
            || self.rest_was_best_price_match.is_some()
            || self.rest_response_ingress_sequence.is_some()
            || self.rest_response_item_index.is_some()
        {
            return Err(invalid(
                "rest_backfill",
                "current v7 rows must use the WebSocket ingress path",
            ));
        }
        if self.ingest_source != "websocket" {
            return Err(invalid("ingest_source", "must equal websocket"));
        }
        require_non_empty(
            &self.source_native_payload_json,
            "source_native_payload_json",
        )?;
        require_non_empty(&self.source_sequence_epoch_id, "source_sequence_epoch_id")?;
        require_positive_u64(self.connection_generation, "connection_generation")?;
        require_positive_u64(
            self.websocket_ingress_sequence,
            "websocket_ingress_sequence",
        )?;
        require_positive_u64(
            self.collector_admission_sequence,
            "collector_admission_sequence",
        )?;
        require_positive(
            self.collector_admission_time_utc_ms,
            "collector_admission_time_utc_ms",
        )?;
        require_non_empty(&self.collector_host_id, "collector_host_id")?;
        require_non_empty(&self.collector_region, "collector_region")?;
        require_non_empty(&self.source_endpoint, "source_endpoint")?;

        match &self.record {
            BinanceAggTradeRecord::Trade {
                event_type,
                event_time,
                trade_time,
                symbol,
                aggregate_trade_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                stream,
                ..
            } => {
                if event_type != "aggTrade" {
                    return Err(invalid("event_type", "must equal aggTrade"));
                }
                require_positive(*event_time, "event_time")?;
                require_positive(*trade_time, "trade_time")?;
                if symbol != "BTCUSDT" {
                    return Err(invalid("symbol", "must equal BTCUSDT"));
                }
                require_positive_u64(*aggregate_trade_id, "aggregate_trade_id")?;
                require_positive_decimal(price, "price")?;
                require_positive_decimal(quantity, "quantity")?;
                require_positive_u64(*first_trade_id, "first_trade_id")?;
                if last_trade_id < first_trade_id {
                    return Err(invalid("last_trade_id", "must be at least first_trade_id"));
                }
                if stream != "btcusdt@aggTrade" {
                    return Err(invalid("stream", "must equal btcusdt@aggTrade"));
                }
            }
            BinanceAggTradeRecord::InvalidInput {
                invalid_reason_code,
                payload_encoding,
                payload_length_bytes,
                payload_sha256,
                payload_capture_status,
                ..
            } => {
                match payload_capture_status {
                    BinanceAggTradeCaptureStatus::Complete => {
                        if *payload_encoding != Some(BinanceAggTradePayloadEncoding::Base64) {
                            return Err(invalid(
                                "payload_encoding",
                                "complete capture must use base64",
                            ));
                        }
                        if payload_length_bytes.is_none() {
                            return Err(invalid("payload_length_bytes", "is required"));
                        }
                        require_sha256(
                            payload_sha256.as_deref().unwrap_or_default(),
                            "payload_sha256",
                        )?;
                    }
                    BinanceAggTradeCaptureStatus::OversizeNotCaptured => {
                        if payload_encoding.is_some() || payload_sha256.is_some() {
                            return Err(invalid(
                                "payload_capture_status",
                                "oversize uncaptured input must not claim encoding or digest",
                            ));
                        }
                    }
                }
                validate_binance_aggtrade_invalid_evidence(
                    self,
                    *invalid_reason_code,
                    *payload_encoding,
                    *payload_length_bytes,
                    payload_sha256.as_deref(),
                    *payload_capture_status,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_normalization_is_exact_and_never_uses_binary_float() {
        assert_eq!(
            normalize_decimal_text("001.2300", "price").unwrap(),
            ExactDecimal("1.23".to_owned())
        );
        assert_eq!(
            normalize_decimal_text("-0.000", "price").unwrap(),
            ExactDecimal("0".to_owned())
        );
        assert!(normalize_decimal_text("1e-3", "price").is_err());
        assert!(
            normalize_decimal_text("1000000000000000000000000000000000000000000", "price").is_err()
        );
    }

    #[test]
    fn depth_json_keeps_text_and_rejects_non_pair_shape() {
        assert_eq!(
            parse_depth_levels_json(r#"[["0.0100","2.500"]]"#, "bids", false).unwrap(),
            vec![DepthLevel {
                price: "0.0100".to_owned(),
                quantity: "2.500".to_owned(),
            }]
        );
        assert!(parse_depth_levels_json(r#"[["0.1","2","extra"]]"#, "bids", false).is_err());
    }

    #[test]
    fn metadata_tick_parser_only_extracts_the_owned_tick_evidence() {
        let evidence = extract_metadata_initial_tick(
            r#"{"orderPriceMinTickSize":"0.0100","orderMinSize":"5","unknown":true}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(evidence.raw_text, "0.0100");
        assert_eq!(evidence.exact, ExactDecimal("0.01".to_owned()));
        assert_eq!(
            extract_metadata_initial_tick(r#"{"orderMinSize":"5"}"#).unwrap(),
            None
        );
    }

    #[test]
    fn metadata_outcomes_preserve_raw_prices_and_index_alignment() {
        let mapped = map_metadata_outcomes(
            r#"["11","22"]"#,
            Some(r#"["UP","DOWN"]"#),
            Some(r#"["0.0100","0.9900"]"#),
        )
        .unwrap();
        assert_eq!(mapped[0].asset_id, "11");
        assert_eq!(mapped[0].outcome.as_deref(), Some("UP"));
        assert_eq!(mapped[0].price.as_ref().unwrap().raw_text, "0.0100");
        assert_eq!(
            mapped[0].price.as_ref().unwrap().exact,
            ExactDecimal("0.01".to_owned())
        );
        assert!(
            map_metadata_outcomes(
                r#"["11","22"]"#,
                Some(r#"["UP"]"#),
                Some(r#"["0.01","0.99"]"#),
            )
            .is_err()
        );
        assert!(
            map_metadata_outcomes(
                r#"["11","11"]"#,
                Some(r#"["UP","DOWN"]"#),
                Some(r#"["0.01","0.99"]"#),
            )
            .is_err()
        );
        assert!(
            validate_metadata_winner(
                &mapped,
                Some("11"),
                Some(" "),
                Some(MetadataResolutionEvidenceSource::MarketResolvedWs),
            )
            .is_err()
        );
    }

    #[test]
    fn current_closed_enums_reject_unknown_values() {
        assert!(PolymarketMetadataOrigin::try_from("future_origin").is_err());
        assert!(MetadataResolutionPollResult::try_from("future_result").is_err());
        assert!(BinanceAggTradeInvalidReason::try_from("future_reason").is_err());
        assert!(BinanceAggTradePayloadEncoding::try_from("hex").is_err());
    }

    #[test]
    fn metadata_payload_digest_rejects_mismatch_but_digest_only_reference_is_valid() {
        let payload = r#"{"orderPriceMinTickSize":"0.0100"}"#;
        let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
        verify_sha256(payload, &digest, "source_evidence_raw_sha256").unwrap();
        assert!(verify_sha256(payload, &"0".repeat(64), "source_evidence_raw_sha256").is_err());
        assert!(require_sha256(&digest, "source_evidence_raw_sha256").is_ok());
    }

    #[test]
    fn source_kind_strings_match_manifest_partition_names() {
        assert_eq!(
            TypedRawSourceKind::PolymarketOrderbook.as_str(),
            "polymarket_market"
        );
        assert_eq!(
            TypedRawSourceKind::PolymarketMarketMetadata.as_str(),
            "polymarket_market_metadata"
        );
        assert_eq!(
            TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd.as_str(),
            "polymarket_rtds_chainlink_btc_usd"
        );
        assert_eq!(
            TypedRawSourceKind::BinanceDepthSnapshot.as_str(),
            "binance_depth_snapshot"
        );
        assert_eq!(
            TypedRawSourceKind::BinanceDepthUpdate.as_str(),
            "binance_depth_update"
        );
        assert_eq!(
            TypedRawSourceKind::BinanceAggTrade.as_str(),
            "binance_aggtrade"
        );
    }

    #[test]
    fn source_semantic_mappings_make_units_and_origins_explicit() {
        assert!(
            TypedRawSourceKind::PolymarketOrderbook
                .identifier_mappings()
                .contains(&identifier!("event.book.asset_id", SourceNative))
        );
        assert!(
            TypedRawSourceKind::PolymarketOrderbook
                .identifier_mappings()
                .contains(&identifier!(
                    "collector_admission_sequence",
                    CollectorDerived
                ))
        );
        assert!(
            TypedRawSourceKind::PolymarketMarketMetadata
                .identifier_mappings()
                .contains(&identifier!("source_evidence_raw_sha256", EvidenceDigest))
        );
        assert!(
            TypedRawSourceKind::PolymarketMarketMetadata
                .identifier_mappings()
                .contains(&identifier!("winning_asset_id", RowEvidenceDependent))
        );
        assert_eq!(
            metadata_winning_asset_id_origin(MetadataResolutionEvidenceSource::MarketResolvedWs),
            TypedRawFieldOrigin::SourceNative
        );
        assert_eq!(
            metadata_winning_asset_id_origin(MetadataResolutionEvidenceSource::GammaBySlug),
            TypedRawFieldOrigin::CollectorDerived
        );
        assert!(
            TypedRawSourceKind::BinanceDepthUpdate
                .identifier_mappings()
                .contains(&identifier!("snapshot_last_update_id", SourceNative))
        );
        assert!(
            TypedRawSourceKind::BinanceDepthUpdate
                .identifier_mappings()
                .contains(&identifier!("local_u_before", CollectorDerived))
        );

        assert!(
            TypedRawSourceKind::PolymarketOrderbook
                .value_mappings()
                .contains(&value!(
                    "event.last_trade_price.price",
                    SourceNative,
                    PusdPerShare
                ))
        );
        assert!(
            TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd
                .value_mappings()
                .contains(&value!("value", SourceNative, UsdPerBtc))
        );
        assert!(
            TypedRawSourceKind::BinanceDepthUpdate
                .value_mappings()
                .contains(&value!("bids[].price", SourceNative, UsdtPerBtc))
        );
        assert!(
            TypedRawSourceKind::BinanceAggTrade
                .value_mappings()
                .contains(&value!("record.trade.quantity", SourceNative, Btc))
        );
        assert!(
            TypedRawSourceKind::BinanceAggTrade
                .identifier_mappings()
                .contains(&identifier!(
                    "record.trade.aggregate_trade_id",
                    SourceNative
                ))
        );
        assert!(
            TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd
                .value_mappings()
                .contains(&value!("age_ms", CollectorDerived, Milliseconds))
        );
        assert!(
            TypedRawSourceKind::BinanceDepthUpdate
                .value_mappings()
                .contains(&value!("stitch_verdict", CollectorDerived))
        );

        for source_kind in [
            TypedRawSourceKind::PolymarketOrderbook,
            TypedRawSourceKind::PolymarketMarketMetadata,
            TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd,
            TypedRawSourceKind::BinanceDepthSnapshot,
            TypedRawSourceKind::BinanceDepthUpdate,
            TypedRawSourceKind::BinanceAggTrade,
        ] {
            assert!(!source_kind.identifier_mappings().is_empty());
            assert!(!source_kind.value_mappings().is_empty());
        }
    }

    #[test]
    fn current_polymarket_unknown_side_remains_explicit() {
        assert_eq!(
            PolymarketSide::try_from("UNKNOWN").unwrap(),
            PolymarketSide::Unknown
        );
    }
}
