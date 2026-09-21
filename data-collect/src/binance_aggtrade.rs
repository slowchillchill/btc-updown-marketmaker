use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

pub const BINANCE_AGGTRADE_SYMBOL: &str = "BTCUSDT";
pub const BINANCE_AGGTRADE_STREAM: &str = "btcusdt@aggTrade";
pub const BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES: usize = 524_288;
const BINANCE_AGGTRADE_INVALID_REASON_DETAIL_MAX_BYTES: usize = 2_048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAggTradeIngressCoordinate {
    Websocket { sequence: u64 },
}

impl BinanceAggTradeIngressCoordinate {
    fn validate(&self) -> Result<(), String> {
        let Self::Websocket { sequence } = self;
        if *sequence == 0 {
            return Err("binance_aggtrade_ingress_sequence_is_zero".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceAggTradeIngressSequenceError;

impl fmt::Display for BinanceAggTradeIngressSequenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("binance_aggtrade_ingress_sequence_overflow")
    }
}

impl Error for BinanceAggTradeIngressSequenceError {}

#[derive(Clone, Debug)]
pub struct BinanceAggTradeIngressSequenceAllocator {
    last_allocated: Arc<AtomicU64>,
}

impl Default for BinanceAggTradeIngressSequenceAllocator {
    fn default() -> Self {
        Self::from_last_allocated(0)
    }
}

impl BinanceAggTradeIngressSequenceAllocator {
    #[must_use]
    pub fn from_last_allocated(last_allocated: u64) -> Self {
        Self {
            last_allocated: Arc::new(AtomicU64::new(last_allocated)),
        }
    }

    pub fn allocate_websocket(
        &self,
    ) -> Result<BinanceAggTradeIngressCoordinate, BinanceAggTradeIngressSequenceError> {
        Ok(BinanceAggTradeIngressCoordinate::Websocket {
            sequence: self.allocate()?,
        })
    }

    #[must_use]
    pub fn last_allocated(&self) -> u64 {
        self.last_allocated.load(Ordering::SeqCst)
    }

    fn allocate(&self) -> Result<u64, BinanceAggTradeIngressSequenceError> {
        let previous = self
            .last_allocated
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
                last.checked_add(1)
            })
            .map_err(|_| BinanceAggTradeIngressSequenceError)?;
        Ok(previous + 1)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAggTradeInvalidTransport {
    Websocket,
}

impl BinanceAggTradeInvalidTransport {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAggTradeInvalidReason {
    BodyTooLarge,
    InvalidJson,
    InvalidPayload,
}

impl BinanceAggTradeInvalidReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "body_too_large",
            Self::InvalidJson => "invalid_json",
            Self::InvalidPayload => "invalid_payload",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAggTradeRawCaptureStatus {
    Complete,
    OversizeNotCaptured,
}

impl BinanceAggTradeRawCaptureStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::OversizeNotCaptured => "oversize_not_captured",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeRawCapture {
    pub status: BinanceAggTradeRawCaptureStatus,
    pub bytes: Vec<u8>,
    /// Exact for complete input; a truthful lower bound for oversize input.
    pub observed_bytes_at_least: u64,
    pub declared_content_length: Option<u64>,
    pub limit_bytes: u64,
    pub payload_sha256: Option<String>,
}

impl BinanceAggTradeRawCapture {
    pub fn complete(bytes: Vec<u8>, declared_content_length: Option<u64>) -> Result<Self, String> {
        if bytes.len() > BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES {
            return Err(format!(
                "binance_aggtrade_complete_capture_exceeds_limit:actual={}:limit={}",
                bytes.len(),
                BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES
            ));
        }
        Ok(Self {
            status: BinanceAggTradeRawCaptureStatus::Complete,
            observed_bytes_at_least: u64::try_from(bytes.len())
                .map_err(|_| "binance_aggtrade_capture_size_overflow".to_owned())?,
            payload_sha256: Some(sha256_hex(&bytes)),
            bytes,
            declared_content_length,
            limit_bytes: BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES as u64,
        })
    }

    pub fn oversize_not_captured(
        observed_bytes_at_least: u64,
        declared_content_length: Option<u64>,
    ) -> Result<Self, String> {
        let limit = BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES as u64;
        if observed_bytes_at_least <= limit
            && declared_content_length.is_none_or(|declared| declared <= limit)
        {
            return Err(format!(
                "binance_aggtrade_oversize_capture_not_over_limit:observed={observed_bytes_at_least}:limit={limit}"
            ));
        }
        Ok(Self {
            status: BinanceAggTradeRawCaptureStatus::OversizeNotCaptured,
            bytes: Vec::new(),
            observed_bytes_at_least,
            declared_content_length,
            limit_bytes: limit,
            payload_sha256: None,
        })
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeInvalidInput {
    pub transport: BinanceAggTradeInvalidTransport,
    pub ingress_coordinate: BinanceAggTradeIngressCoordinate,
    pub timestamp_received: i64,
    pub endpoint: String,
    pub reason: BinanceAggTradeInvalidReason,
    pub reason_detail: String,
    pub raw_capture: BinanceAggTradeRawCapture,
}

impl BinanceAggTradeInvalidInput {
    pub fn new(
        transport: BinanceAggTradeInvalidTransport,
        ingress_coordinate: BinanceAggTradeIngressCoordinate,
        timestamp_received: i64,
        endpoint: String,
        reason: BinanceAggTradeInvalidReason,
        reason_detail: impl Into<String>,
        raw_capture: BinanceAggTradeRawCapture,
    ) -> Result<Self, String> {
        ingress_coordinate.validate()?;
        if timestamp_received <= 0 {
            return Err("binance_aggtrade_invalid_receive_time_not_positive".to_owned());
        }
        if endpoint.is_empty() {
            return Err("binance_aggtrade_invalid_endpoint_empty".to_owned());
        }
        Ok(Self {
            transport,
            ingress_coordinate,
            timestamp_received,
            endpoint,
            reason,
            reason_detail: bounded_reason_detail(reason_detail.into()),
            raw_capture,
        })
    }
}

fn bounded_reason_detail(mut detail: String) -> String {
    if detail.len() <= BINANCE_AGGTRADE_INVALID_REASON_DETAIL_MAX_BYTES {
        return detail;
    }
    let mut end = BINANCE_AGGTRADE_INVALID_REASON_DETAIL_MAX_BYTES;
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail.truncate(end);
    detail
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeSharedFields {
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub trade_time_ms: i64,
    pub buyer_is_maker: bool,
}

impl BinanceAggTradeSharedFields {
    fn from_wire(value: &Value) -> Result<Self, String> {
        let first_trade_id = required_u64(value, "f")?;
        let last_trade_id = required_u64(value, "l")?;
        if last_trade_id < first_trade_id {
            return Err("binance_aggtrade_l_before_f".to_owned());
        }
        let trade_time_ms = required_i64(value, "T")?;
        if trade_time_ms <= 0 {
            return Err("binance_aggtrade_T_not_positive".to_owned());
        }
        Ok(Self {
            aggregate_trade_id: required_u64(value, "a")?,
            price: required_string(value, "p")?,
            quantity: required_string(value, "q")?,
            first_trade_id,
            last_trade_id,
            trade_time_ms,
            buyer_is_maker: required_bool(value, "m")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceWsAggTrade {
    pub ingress_coordinate: BinanceAggTradeIngressCoordinate,
    pub event_type: String,
    pub event_time_ms: i64,
    pub symbol: String,
    pub stream: String,
    pub shared: BinanceAggTradeSharedFields,
    pub ws_ignore: bool,
    pub timestamp_received: i64,
    pub source_native_payload_json: String,
}

impl BinanceWsAggTrade {
    pub fn from_wire(
        value: &Value,
        stream: &str,
        timestamp_received: i64,
        ingress_coordinate: BinanceAggTradeIngressCoordinate,
    ) -> Result<Self, String> {
        ingress_coordinate.validate()?;
        let event_type = required_string(value, "e")?;
        if event_type != "aggTrade" {
            return Err(format!("unexpected_aggtrade_event_type:{event_type}"));
        }
        let symbol = required_string(value, "s")?;
        if symbol != BINANCE_AGGTRADE_SYMBOL {
            return Err(format!("unexpected_aggtrade_symbol:{symbol}"));
        }
        if stream != BINANCE_AGGTRADE_STREAM {
            return Err(format!("unexpected_aggtrade_stream:{stream}"));
        }
        if timestamp_received <= 0 {
            return Err("binance_aggtrade_websocket_receive_time_not_positive".to_owned());
        }
        let event_time_ms = required_i64(value, "E")?;
        if event_time_ms <= 0 {
            return Err("binance_aggtrade_E_not_positive".to_owned());
        }
        Ok(Self {
            ingress_coordinate,
            event_type,
            event_time_ms,
            symbol,
            stream: stream.to_owned(),
            shared: BinanceAggTradeSharedFields::from_wire(value)?,
            ws_ignore: required_bool(value, "M")?,
            timestamp_received,
            source_native_payload_json: serde_json::to_string(&json!({
                "source": "binance_aggtrade_websocket",
                "stream": stream,
                "payload": value,
            }))
            .map_err(|error| format!("serialize_binance_aggtrade_websocket:{error}"))?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggTradeIngestSource {
    Websocket,
}

impl AggTradeIngestSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceAggTradeOutput {
    Websocket(BinanceWsAggTrade),
}

impl BinanceAggTradeOutput {
    #[must_use]
    pub const fn ingest_source(&self) -> AggTradeIngestSource {
        match self {
            Self::Websocket(_) => AggTradeIngestSource::Websocket,
        }
    }

    #[must_use]
    pub fn aggregate_trade_id(&self) -> u64 {
        self.shared().aggregate_trade_id
    }

    #[must_use]
    pub fn shared(&self) -> &BinanceAggTradeSharedFields {
        match self {
            Self::Websocket(websocket) => &websocket.shared,
        }
    }

    #[must_use]
    pub fn timestamp_received(&self) -> i64 {
        match self {
            Self::Websocket(websocket) => websocket.timestamp_received,
        }
    }

    #[must_use]
    pub fn websocket_ingress_sequence(&self) -> Option<u64> {
        match self {
            Self::Websocket(BinanceWsAggTrade {
                ingress_coordinate: BinanceAggTradeIngressCoordinate::Websocket { sequence },
                ..
            }) => Some(*sequence),
        }
    }

    pub fn source_native_payload_json(&self) -> Result<String, String> {
        let websocket = match self {
            Self::Websocket(websocket) => websocket.source_native_payload_json.as_str(),
        };
        serde_json::to_string(&json!({
            "websocket": websocket,
            "rest_backfill": Value::Null,
        }))
        .map_err(|error| format!("serialize_binance_aggtrade_sources:{error}"))
    }
}

fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .filter(|number| *number > 0)
        .ok_or_else(|| format!("binance_aggtrade_missing_u64:{field}"))
}

fn required_i64(value: &Value, field: &str) -> Result<i64, String> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("binance_aggtrade_missing_i64:{field}"))
}

fn required_string(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("binance_aggtrade_missing_string:{field}"))
}

fn required_bool(value: &Value, field: &str) -> Result<bool, String> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("binance_aggtrade_missing_bool:{field}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
        BinanceAggTradeIngressSequenceAllocator, BinanceAggTradeInvalidInput,
        BinanceAggTradeInvalidReason, BinanceAggTradeInvalidTransport, BinanceAggTradeRawCapture,
        BinanceAggTradeRawCaptureStatus, BinanceWsAggTrade,
    };

    fn wire(id: u64) -> serde_json::Value {
        json!({
            "e": "aggTrade",
            "E": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
            "s": "BTCUSDT",
            "a": id,
            "p": id.to_string(),
            "q": "0.001",
            "f": id * 10,
            "l": id * 10,
            "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
            "m": true,
            "M": false
        })
    }

    #[test]
    fn websocket_allocator_is_monotonic_and_does_not_wrap() {
        let allocator = BinanceAggTradeIngressSequenceAllocator::default();
        assert_eq!(
            allocator.allocate_websocket().unwrap(),
            BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 }
        );
        assert_eq!(
            allocator.allocate_websocket().unwrap(),
            BinanceAggTradeIngressCoordinate::Websocket { sequence: 2 }
        );

        let exhausted = BinanceAggTradeIngressSequenceAllocator::from_last_allocated(u64::MAX);
        assert_eq!(
            exhausted.allocate_websocket().unwrap_err().to_string(),
            "binance_aggtrade_ingress_sequence_overflow"
        );
        assert_eq!(exhausted.last_allocated(), u64::MAX);
    }

    #[test]
    fn websocket_parser_does_not_impose_aggregate_trade_id_continuity() {
        for (sequence, id) in [(1, 100), (2, 103), (3, 99)] {
            let row = BinanceWsAggTrade::from_wire(
                &wire(id),
                "btcusdt@aggTrade",
                1_000 + i64::try_from(sequence).unwrap(),
                BinanceAggTradeIngressCoordinate::Websocket { sequence },
            )
            .unwrap();
            assert_eq!(row.shared.aggregate_trade_id, id);
        }
    }

    #[test]
    fn websocket_invalid_evidence_preserves_exact_bounded_input() {
        let bytes = b"{not-json".to_vec();
        let input = BinanceAggTradeInvalidInput::new(
            BinanceAggTradeInvalidTransport::Websocket,
            BinanceAggTradeIngressCoordinate::Websocket { sequence: 7 },
            1_000,
            "wss://stream.binance.com/ws/btcusdt@aggTrade".to_owned(),
            BinanceAggTradeInvalidReason::InvalidJson,
            "expected value",
            BinanceAggTradeRawCapture::complete(bytes.clone(), None).unwrap(),
        )
        .unwrap();

        assert_eq!(
            input.raw_capture.status,
            BinanceAggTradeRawCaptureStatus::Complete
        );
        assert_eq!(input.raw_capture.bytes, bytes);
        assert_eq!(
            input.raw_capture.payload_sha256.as_deref(),
            Some("f1dec6e9ee608550bd1c39ff2b90134059bac5d02e4e78f6410aed2fbd870bd0")
        );
    }

    #[test]
    fn oversize_capture_contains_only_truthful_bounded_evidence() {
        let observed = u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES).unwrap() + 1;
        let capture = BinanceAggTradeRawCapture::oversize_not_captured(observed, None).unwrap();
        assert_eq!(
            capture.status,
            BinanceAggTradeRawCaptureStatus::OversizeNotCaptured
        );
        assert!(capture.bytes.is_empty());
        assert_eq!(capture.observed_bytes_at_least, observed);
        assert!(capture.payload_sha256.is_none());
    }
}
