use serde_json::Value;

use crate::event::{EventEnvelope, SourceKind};
use crate::source_format::{
    BinanceAggTradeRaw, BinanceDepthLevel, BinanceDepthSnapshotRaw, BinanceDepthUpdateRaw,
};

pub const BINANCE_DEPTH_STREAM: &str = "btcusdt@depth";
pub const BINANCE_AGGTRADE_STREAM: &str = "btcusdt@aggTrade";
pub const BINANCE_AGGTRADE_WS_ENDPOINT: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
pub const BINANCE_DEPTH_SNAPSHOT_ENDPOINT: &str =
    "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepthUpdateVerdict {
    Applied,
    NotAppliedStaleDuplicate,
    NotAppliedConfirmedHole,
    NotAppliedSnapshotUnavailable,
}

impl DepthUpdateVerdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotAppliedStaleDuplicate => "not_applied_stale_duplicate",
            Self::NotAppliedConfirmedHole => "not_applied_confirmed_hole",
            Self::NotAppliedSnapshotUnavailable => "not_applied_snapshot_unavailable",
        }
    }
}

impl TryFrom<&str> for DepthUpdateVerdict {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "applied" => Ok(Self::Applied),
            "not_applied_stale_duplicate" => Ok(Self::NotAppliedStaleDuplicate),
            "not_applied_confirmed_hole" => Ok(Self::NotAppliedConfirmedHole),
            "not_applied_snapshot_unavailable" => Ok(Self::NotAppliedSnapshotUnavailable),
            other => Err(format!("unsupported frozen Binance depth verdict: {other}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepthUpdateAssessment {
    pub snapshot_last_update_id: Option<u64>,
    pub local_u_before: Option<u64>,
    pub verdict: DepthUpdateVerdict,
    pub local_u_after: Option<u64>,
    pub stitch_evidence: Option<String>,
    pub gap_reason: Option<String>,
}

impl DepthUpdateAssessment {
    pub fn validate_contract(&self) -> Result<(), String> {
        if self
            .snapshot_last_update_id
            .is_some_and(|snapshot_last_update_id| snapshot_last_update_id == 0)
        {
            return Err("snapshot_last_update_id_is_zero".to_owned());
        }
        if self
            .local_u_before
            .zip(self.snapshot_last_update_id)
            .is_some_and(|(local_u, snapshot_last_update_id)| local_u <= snapshot_last_update_id)
        {
            return Err("local_u_does_not_follow_snapshot_last_update_id".to_owned());
        }
        match self.verdict {
            DepthUpdateVerdict::Applied => {
                if self.snapshot_last_update_id.is_none() {
                    return Err("applied_depth_update_missing_snapshot".to_owned());
                }
                if self.local_u_after.is_none() {
                    return Err("applied_depth_update_missing_local_u_after".to_owned());
                }
                if self
                    .stitch_evidence
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err("applied_depth_update_missing_stitch_evidence".to_owned());
                }
                if self.gap_reason.is_some() {
                    return Err("applied_depth_update_must_not_have_gap_reason".to_owned());
                }
            }
            DepthUpdateVerdict::NotAppliedConfirmedHole => {
                if self.snapshot_last_update_id.is_none() {
                    return Err("confirmed_hole_missing_snapshot".to_owned());
                }
                if self.local_u_after.is_some() {
                    return Err("confirmed_hole_must_not_advance_local_u".to_owned());
                }
                if self
                    .gap_reason
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err("confirmed_hole_missing_gap_reason".to_owned());
                }
            }
            DepthUpdateVerdict::NotAppliedStaleDuplicate => {
                if self.snapshot_last_update_id.is_none() {
                    return Err("stale_depth_update_missing_snapshot".to_owned());
                }
                if self.local_u_after.is_some() {
                    return Err("not_applied_depth_update_must_not_advance_local_u".to_owned());
                }
                if self.gap_reason.is_some() {
                    return Err("non_gap_depth_update_must_not_have_gap_reason".to_owned());
                }
            }
            DepthUpdateVerdict::NotAppliedSnapshotUnavailable => {
                if self.snapshot_last_update_id.is_some() || self.local_u_before.is_some() {
                    return Err("snapshot_unavailable_must_not_claim_book_state".to_owned());
                }
                if self.local_u_after.is_some() {
                    return Err("not_applied_depth_update_must_not_advance_local_u".to_owned());
                }
                if self.gap_reason.is_some() {
                    return Err("non_gap_depth_update_must_not_have_gap_reason".to_owned());
                }
            }
        }
        Ok(())
    }
}

pub fn assess_depth_update(
    snapshot_last_update_id: Option<u64>,
    local_u: Option<u64>,
    update: &BinanceDepthUpdateRaw,
) -> Result<DepthUpdateAssessment, String> {
    if !update.validate_contract().is_complete() {
        return Err("invalid_depth_update_contract".to_owned());
    }
    if snapshot_last_update_id.is_none() {
        if local_u.is_some() {
            return Err("local_u_without_snapshot".to_owned());
        }
        return Ok(DepthUpdateAssessment {
            snapshot_last_update_id: None,
            local_u_before: None,
            verdict: DepthUpdateVerdict::NotAppliedSnapshotUnavailable,
            local_u_after: None,
            stitch_evidence: None,
            gap_reason: None,
        });
    }

    let snapshot_last_update_id = snapshot_last_update_id.unwrap_or_default();
    if snapshot_last_update_id == 0 {
        return Err("snapshot_last_update_id_is_zero".to_owned());
    }
    if local_u.is_some_and(|value| value <= snapshot_last_update_id) {
        return Err("local_u_does_not_follow_snapshot_last_update_id".to_owned());
    }

    let previous_u = local_u.unwrap_or(snapshot_last_update_id);
    let required = previous_u
        .checked_add(1)
        .ok_or_else(|| "depth_update_sequence_overflow".to_owned())?;

    if update.final_update_id <= previous_u {
        return Ok(DepthUpdateAssessment {
            snapshot_last_update_id: Some(snapshot_last_update_id),
            local_u_before: local_u,
            verdict: DepthUpdateVerdict::NotAppliedStaleDuplicate,
            local_u_after: None,
            stitch_evidence: Some(if local_u.is_some() {
                "u <= local_u".to_owned()
            } else {
                "u <= lastUpdateId".to_owned()
            }),
            gap_reason: None,
        });
    }

    if local_u.is_none() {
        if update.first_update_id <= required && required <= update.final_update_id {
            return Ok(DepthUpdateAssessment {
                snapshot_last_update_id: Some(snapshot_last_update_id),
                local_u_before: local_u,
                verdict: DepthUpdateVerdict::Applied,
                local_u_after: Some(update.final_update_id),
                stitch_evidence: Some("U <= lastUpdateId + 1 <= u".to_owned()),
                gap_reason: None,
            });
        }
        return Ok(DepthUpdateAssessment {
            snapshot_last_update_id: Some(snapshot_last_update_id),
            local_u_before: local_u,
            verdict: DepthUpdateVerdict::NotAppliedConfirmedHole,
            local_u_after: None,
            stitch_evidence: Some("first update does not bridge snapshot".to_owned()),
            gap_reason: Some("first_update_does_not_bridge_snapshot".to_owned()),
        });
    }

    if update.first_update_id > required {
        return Ok(DepthUpdateAssessment {
            snapshot_last_update_id: Some(snapshot_last_update_id),
            local_u_before: local_u,
            verdict: DepthUpdateVerdict::NotAppliedConfirmedHole,
            local_u_after: None,
            stitch_evidence: Some("U > local_u + 1".to_owned()),
            gap_reason: Some("U > local_u + 1".to_owned()),
        });
    }
    Ok(DepthUpdateAssessment {
        snapshot_last_update_id: Some(snapshot_last_update_id),
        local_u_before: local_u,
        verdict: DepthUpdateVerdict::Applied,
        local_u_after: Some(update.final_update_id),
        stitch_evidence: Some("continuous_depth_update".to_owned()),
        gap_reason: None,
    })
}

pub fn parse_depth_update_raw(
    envelope: EventEnvelope,
    raw_json: &str,
) -> Result<BinanceDepthUpdateRaw, String> {
    if envelope.source_kind != SourceKind::BinanceDepthUpdate {
        return Err("envelope_source_kind_mismatch".to_owned());
    }
    let raw_value = serde_json::from_str::<Value>(raw_json)
        .map_err(|err| format!("parse_depth_update_json:{err}"))?;
    let (combined_stream, value) = match raw_value.get("data") {
        Some(data) => (
            raw_value
                .get("stream")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            data,
        ),
        None => (None, &raw_value),
    };

    let event_type = str_field(value, "e")?;
    let event_time_ms = i64_field(value, "E")?;
    let symbol = str_field(value, "s")?;
    if event_type != "depthUpdate" {
        return Err(format!("unexpected_depth_event_type:{event_type}"));
    }
    if symbol != "BTCUSDT" {
        return Err(format!("unexpected_depth_symbol:{symbol}"));
    }
    let first_update_id = u64_field(value, "U")?;
    let final_update_id = u64_field(value, "u")?;
    let bids = parse_levels(value, "b")?;
    let asks = parse_levels(value, "a")?;
    let stream_name = combined_stream
        .clone()
        .unwrap_or_else(|| BINANCE_DEPTH_STREAM.to_owned());
    if stream_name != BINANCE_DEPTH_STREAM {
        return Err(format!("unexpected_depth_stream:{stream_name}"));
    }

    Ok(BinanceDepthUpdateRaw {
        envelope,
        event_type,
        event_time_ms,
        symbol,
        stream_name,
        first_update_id,
        final_update_id,
        bids,
        asks,
        combined_stream,
    })
}

pub fn parse_depth_snapshot_raw(
    envelope: EventEnvelope,
    raw_json: &str,
    snapshot_fetch_start_utc_ms: i64,
    snapshot_recv_time_utc_ms: i64,
    official_rest_endpoint: impl Into<String>,
) -> Result<BinanceDepthSnapshotRaw, String> {
    if envelope.source_kind != SourceKind::BinanceDepthSnapshot {
        return Err("envelope_source_kind_mismatch".to_owned());
    }
    let value = serde_json::from_str::<Value>(raw_json)
        .map_err(|err| format!("parse_depth_snapshot_json:{err}"))?;
    Ok(BinanceDepthSnapshotRaw {
        envelope,
        last_update_id: u64_field(&value, "lastUpdateId")?,
        bids: parse_levels(&value, "bids")?,
        asks: parse_levels(&value, "asks")?,
        snapshot_fetch_start_utc_ms,
        snapshot_recv_time_utc_ms,
        official_rest_endpoint: official_rest_endpoint.into(),
        no_exchange_timestamp_evidence: "official REST depth snapshot has no exchange timestamp"
            .to_owned(),
    })
}

pub fn parse_aggtrade_raw(
    envelope: EventEnvelope,
    raw_json: &str,
) -> Result<BinanceAggTradeRaw, String> {
    if envelope.source_kind != SourceKind::BinanceAggTrade {
        return Err("envelope_source_kind_mismatch".to_owned());
    }
    let raw_value = serde_json::from_str::<Value>(raw_json)
        .map_err(|err| format!("parse_aggtrade_json:{err}"))?;
    let (combined_stream, value) = match raw_value.get("data") {
        Some(data) => (
            raw_value
                .get("stream")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            data,
        ),
        None => (None, &raw_value),
    };
    let event_type = str_field(value, "e")?;
    let symbol = str_field(value, "s")?;
    let stream_name = combined_stream.unwrap_or_else(|| BINANCE_AGGTRADE_STREAM.to_owned());
    if event_type != "aggTrade" {
        return Err(format!("unexpected_aggtrade_event_type:{event_type}"));
    }
    if symbol != "BTCUSDT" {
        return Err(format!("unexpected_aggtrade_symbol:{symbol}"));
    }
    if stream_name != BINANCE_AGGTRADE_STREAM {
        return Err(format!("unexpected_aggtrade_stream:{stream_name}"));
    }

    Ok(BinanceAggTradeRaw {
        envelope,
        event_type,
        event_time_ms: i64_field(value, "E")?,
        symbol,
        aggregate_trade_id: u64_field(value, "a")?,
        price: str_field(value, "p")?,
        quantity: str_field(value, "q")?,
        first_trade_id: u64_field(value, "f")?,
        last_trade_id: u64_field(value, "l")?,
        trade_time_ms: i64_field(value, "T")?,
        buyer_is_maker: value
            .get("m")
            .and_then(Value::as_bool)
            .ok_or_else(|| "missing_bool_field:m".to_owned())?,
        ws_ignore: value
            .get("M")
            .and_then(Value::as_bool)
            .ok_or_else(|| "missing_bool_field:M".to_owned())?,
        stream_name,
    })
}

fn parse_levels(value: &Value, field: &str) -> Result<Vec<BinanceDepthLevel>, String> {
    let levels = value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing_array_field:{field}"))?;
    levels
        .iter()
        .map(|level| {
            let pair = level
                .as_array()
                .ok_or_else(|| format!("invalid_depth_level:{field}"))?;
            let [price, quantity] = pair.as_slice() else {
                return Err(format!("invalid_depth_level_width:{field}"));
            };
            Ok(BinanceDepthLevel {
                price: price
                    .as_str()
                    .ok_or_else(|| format!("invalid_depth_price:{field}"))?
                    .to_owned(),
                quantity: quantity
                    .as_str()
                    .ok_or_else(|| format!("invalid_depth_quantity:{field}"))?
                    .to_owned(),
            })
        })
        .collect()
}

fn str_field(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("missing_string_field:{field}"))
}

fn i64_field(value: &Value, field: &str) -> Result<i64, String> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("missing_i64_field:{field}"))
}

fn u64_field(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("missing_u64_field:{field}"))
}
