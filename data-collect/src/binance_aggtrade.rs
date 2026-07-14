use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const BINANCE_AGGTRADE_SYMBOL: &str = "BTCUSDT";
pub const BINANCE_AGGTRADE_STREAM: &str = "btcusdt@aggTrade";
pub const BINANCE_AGGTRADE_PENDING_MAX_ROWS: usize = 25_000;
pub const BINANCE_AGGTRADE_REST_PAGE_LIMIT: u64 = 1_000;
pub const BINANCE_AGGTRADE_BACKFILL_MAX_ROWS: u64 = 25_000;
pub const BINANCE_AGGTRADE_BACKFILL_MAX_ATTEMPTS: u32 = 3;
pub const BINANCE_AGGTRADE_BACKFILL_MAX_ELAPSED: Duration = Duration::from_secs(20);

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
    pub fn from_wire(value: &Value, stream: &str, timestamp_received: i64) -> Result<Self, String> {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceRestAggTrade {
    pub symbol: String,
    pub shared: BinanceAggTradeSharedFields,
    pub rest_was_best_price_match: bool,
    pub timestamp_received: i64,
    pub source_native_payload_json: String,
}

impl BinanceRestAggTrade {
    pub fn from_wire(value: &Value, timestamp_received: i64) -> Result<Self, String> {
        if timestamp_received <= 0 {
            return Err("binance_aggtrade_rest_receive_time_not_positive".to_owned());
        }
        Ok(Self {
            symbol: BINANCE_AGGTRADE_SYMBOL.to_owned(),
            shared: BinanceAggTradeSharedFields::from_wire(value)?,
            rest_was_best_price_match: required_bool(value, "M")?,
            timestamp_received,
            source_native_payload_json: serde_json::to_string(&json!({
                "source": "binance_aggtrade_rest",
                "request_symbol": BINANCE_AGGTRADE_SYMBOL,
                "payload": value,
            }))
            .map_err(|error| format!("serialize_binance_aggtrade_rest:{error}"))?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggTradeIngestSource {
    Websocket,
    RestBackfill,
    WebsocketRestMerged,
}

impl AggTradeIngestSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::RestBackfill => "rest_backfill",
            Self::WebsocketRestMerged => "websocket_rest_merged",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceAggTradeOutput {
    Websocket(BinanceWsAggTrade),
    RestBackfill(BinanceRestAggTrade),
    WebsocketRestMerged {
        websocket: BinanceWsAggTrade,
        rest_backfill: BinanceRestAggTrade,
    },
}

impl BinanceAggTradeOutput {
    #[must_use]
    pub const fn ingest_source(&self) -> AggTradeIngestSource {
        match self {
            Self::Websocket(_) => AggTradeIngestSource::Websocket,
            Self::RestBackfill(_) => AggTradeIngestSource::RestBackfill,
            Self::WebsocketRestMerged { .. } => AggTradeIngestSource::WebsocketRestMerged,
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
            Self::RestBackfill(rest) => &rest.shared,
            Self::WebsocketRestMerged { websocket, .. } => &websocket.shared,
        }
    }

    #[must_use]
    pub fn timestamp_received(&self) -> i64 {
        match self {
            Self::Websocket(websocket) => websocket.timestamp_received,
            Self::RestBackfill(rest) => rest.timestamp_received,
            Self::WebsocketRestMerged {
                websocket,
                rest_backfill,
            } => websocket
                .timestamp_received
                .max(rest_backfill.timestamp_received),
        }
    }

    #[must_use]
    pub fn websocket_timestamp_received(&self) -> Option<i64> {
        match self {
            Self::Websocket(websocket) | Self::WebsocketRestMerged { websocket, .. } => {
                Some(websocket.timestamp_received)
            }
            Self::RestBackfill(_) => None,
        }
    }

    #[must_use]
    pub fn rest_backfill_timestamp_received(&self) -> Option<i64> {
        match self {
            Self::RestBackfill(rest)
            | Self::WebsocketRestMerged {
                rest_backfill: rest,
                ..
            } => Some(rest.timestamp_received),
            Self::Websocket(_) => None,
        }
    }

    pub fn source_native_payload_json(&self) -> Result<String, String> {
        let (websocket, rest_backfill) = match self {
            Self::Websocket(websocket) => {
                (Some(websocket.source_native_payload_json.as_str()), None)
            }
            Self::RestBackfill(rest) => (None, Some(rest.source_native_payload_json.as_str())),
            Self::WebsocketRestMerged {
                websocket,
                rest_backfill,
            } => (
                Some(websocket.source_native_payload_json.as_str()),
                Some(rest_backfill.source_native_payload_json.as_str()),
            ),
        };
        serde_json::to_string(&json!({
            "websocket": websocket,
            "rest_backfill": rest_backfill,
        }))
        .map_err(|error| format!("serialize_binance_aggtrade_sources:{error}"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeContinuityEpoch {
    pub epoch_start_id: u64,
    pub last_observed_id: u64,
    pub last_contiguous_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeUnresolvedRange {
    pub start_id: u64,
    pub end_id: u64,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinanceAggTradeContinuityState {
    pub latest_epoch: Option<BinanceAggTradeContinuityEpoch>,
    pub unresolved_ranges: Vec<BinanceAggTradeUnresolvedRange>,
    pub initial_coverage_boundary_id: Option<u64>,
}

impl BinanceAggTradeContinuityState {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(epoch) = &self.latest_epoch
            && (epoch.epoch_start_id == 0
                || epoch.epoch_start_id > epoch.last_contiguous_id
                || epoch.last_contiguous_id > epoch.last_observed_id)
        {
            return Err(format!(
                "invalid Binance continuity epoch: start={} observed={} contiguous={}",
                epoch.epoch_start_id, epoch.last_observed_id, epoch.last_contiguous_id
            ));
        }
        if self
            .unresolved_ranges
            .iter()
            .any(|range| range.start_id == 0 || range.end_id < range.start_id)
        {
            return Err("invalid Binance unresolved range".to_owned());
        }
        Ok(())
    }

    pub fn recover_persisted_id(&mut self, id: u64) -> Result<(), String> {
        if id == 0 {
            return Err("cannot recover zero Binance aggregate trade ID".to_owned());
        }
        let Some(epoch) = self.latest_epoch.as_ref() else {
            self.start_initial(id);
            return Ok(());
        };
        if id <= epoch.last_contiguous_id {
            return Ok(());
        }
        if id == epoch.last_contiguous_id.saturating_add(1) {
            return self.accept_contiguous(id);
        }
        let _ = self.close_gap_and_start_epoch(id, "finalized_parquet_observed_gap");
        Ok(())
    }

    fn start_initial(&mut self, id: u64) {
        self.initial_coverage_boundary_id.get_or_insert(id);
        self.latest_epoch = Some(BinanceAggTradeContinuityEpoch {
            epoch_start_id: id,
            last_observed_id: id,
            last_contiguous_id: id,
        });
    }

    fn accept_contiguous(&mut self, id: u64) -> Result<(), String> {
        let Some(epoch) = self.latest_epoch.as_mut() else {
            self.start_initial(id);
            return Ok(());
        };
        let expected = epoch.last_contiguous_id.saturating_add(1);
        if id != expected {
            return Err(format!(
                "binance_aggtrade_non_contiguous_accept:expected={expected}:actual={id}"
            ));
        }
        epoch.last_contiguous_id = id;
        epoch.last_observed_id = epoch.last_observed_id.max(id);
        Ok(())
    }

    fn close_gap_and_start_epoch(
        &mut self,
        new_epoch_start_id: u64,
        reason: &str,
    ) -> Option<BinanceAggTradeUnresolvedRange> {
        let Some(previous) = self.latest_epoch.as_ref() else {
            self.start_initial(new_epoch_start_id);
            return None;
        };
        let missing_start = previous.last_contiguous_id.saturating_add(1);
        let missing =
            (missing_start < new_epoch_start_id).then(|| BinanceAggTradeUnresolvedRange {
                start_id: missing_start,
                end_id: new_epoch_start_id - 1,
                reason: reason.to_owned(),
            });
        if let Some(range) = &missing {
            self.unresolved_ranges.push(range.clone());
        }
        self.latest_epoch = Some(BinanceAggTradeContinuityEpoch {
            epoch_start_id: new_epoch_start_id,
            last_observed_id: previous.last_observed_id.max(new_epoch_start_id),
            last_contiguous_id: new_epoch_start_id,
        });
        missing
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggTradeBackfillRange {
    pub start_id: u64,
    pub end_id: u64,
}

impl AggTradeBackfillRange {
    fn row_count(self) -> Result<u64, AggTradeBackfillError> {
        if self.start_id == 0 || self.end_id < self.start_id {
            return Err(AggTradeBackfillError::InvalidRange {
                start_id: self.start_id,
                end_id: self.end_id,
            });
        }
        let rows = self
            .end_id
            .checked_sub(self.start_id)
            .and_then(|difference| difference.checked_add(1))
            .ok_or(AggTradeBackfillError::InvalidRange {
                start_id: self.start_id,
                end_id: self.end_id,
            })?;
        if rows > BINANCE_AGGTRADE_BACKFILL_MAX_ROWS {
            return Err(AggTradeBackfillError::RangeTooLarge {
                rows,
                limit: BINANCE_AGGTRADE_BACKFILL_MAX_ROWS,
            });
        }
        Ok(rows)
    }
}

#[derive(Debug)]
pub enum AggTradeBackfillError {
    InvalidRange {
        start_id: u64,
        end_id: u64,
    },
    RangeTooLarge {
        rows: u64,
        limit: u64,
    },
    Request(String),
    HttpStatus {
        status: u16,
        retry_after_seconds: Option<u64>,
    },
    EmptyPage {
        from_id: u64,
    },
    InvalidJson(String),
    InvalidPayload(String),
    InvalidSequence {
        expected: u64,
        actual: u64,
    },
    TooManyRows {
        requested: u64,
        actual: usize,
    },
    RetryBudgetExhausted {
        attempts: u32,
        last_error: String,
    },
}

impl fmt::Display for AggTradeBackfillError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRange { start_id, end_id } => {
                write!(
                    formatter,
                    "invalid aggTrade backfill range {start_id}..={end_id}"
                )
            }
            Self::RangeTooLarge { rows, limit } => {
                write!(
                    formatter,
                    "aggTrade backfill range has {rows} rows, limit is {limit}"
                )
            }
            Self::Request(error) => write!(formatter, "aggTrade REST request failed: {error}"),
            Self::HttpStatus {
                status,
                retry_after_seconds,
            } => write!(
                formatter,
                "aggTrade REST HTTP status {status}, retry-after={retry_after_seconds:?}"
            ),
            Self::EmptyPage { from_id } => {
                write!(
                    formatter,
                    "aggTrade REST returned an empty page fromId={from_id}"
                )
            }
            Self::InvalidJson(error) => {
                write!(formatter, "aggTrade REST JSON invalid: {error}")
            }
            Self::InvalidPayload(error) => {
                write!(formatter, "aggTrade REST payload invalid: {error}")
            }
            Self::InvalidSequence { expected, actual } => write!(
                formatter,
                "aggTrade REST sequence invalid: expected={expected} actual={actual}"
            ),
            Self::TooManyRows { requested, actual } => write!(
                formatter,
                "aggTrade REST page exceeds requested limit: requested={requested} actual={actual}"
            ),
            Self::RetryBudgetExhausted {
                attempts,
                last_error,
            } => write!(
                formatter,
                "aggTrade REST retry budget exhausted after {attempts} attempts: {last_error}"
            ),
        }
    }
}

impl Error for AggTradeBackfillError {}

pub async fn fetch_binance_aggtrade_range_once(
    client: &reqwest::Client,
    endpoint: &str,
    range: AggTradeBackfillRange,
) -> Result<Vec<BinanceRestAggTrade>, AggTradeBackfillError> {
    let expected_rows = range.row_count()?;
    let capacity =
        usize::try_from(expected_rows).map_err(|_| AggTradeBackfillError::RangeTooLarge {
            rows: expected_rows,
            limit: BINANCE_AGGTRADE_BACKFILL_MAX_ROWS,
        })?;
    let mut rows = Vec::with_capacity(capacity);
    let mut from_id = range.start_id;
    while from_id <= range.end_id {
        let remaining = range.end_id - from_id + 1;
        let limit = remaining.min(BINANCE_AGGTRADE_REST_PAGE_LIMIT);
        let response = client
            .get(endpoint)
            .query(&[
                ("symbol", BINANCE_AGGTRADE_SYMBOL.to_owned()),
                ("fromId", from_id.to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await
            .map_err(|error| AggTradeBackfillError::Request(error.to_string()))?;
        let status = response.status();
        let retry_after_seconds = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if !status.is_success() {
            return Err(AggTradeBackfillError::HttpStatus {
                status: status.as_u16(),
                retry_after_seconds,
            });
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| AggTradeBackfillError::Request(error.to_string()))?;
        let timestamp_received = system_time_ms();
        let page = serde_json::from_slice::<Vec<Value>>(&body)
            .map_err(|error| AggTradeBackfillError::InvalidJson(error.to_string()))?;
        if page.is_empty() {
            return Err(AggTradeBackfillError::EmptyPage { from_id });
        }
        if page.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
            return Err(AggTradeBackfillError::TooManyRows {
                requested: limit,
                actual: page.len(),
            });
        }
        for value in page {
            let row = BinanceRestAggTrade::from_wire(&value, timestamp_received)
                .map_err(AggTradeBackfillError::InvalidPayload)?;
            let actual = row.shared.aggregate_trade_id;
            if actual != from_id {
                return Err(AggTradeBackfillError::InvalidSequence {
                    expected: from_id,
                    actual,
                });
            }
            rows.push(row);
            from_id = from_id.saturating_add(1);
        }
    }
    Ok(rows)
}

pub async fn fetch_binance_aggtrade_range(
    client: &reqwest::Client,
    endpoint: &str,
    range: AggTradeBackfillRange,
) -> Result<Vec<BinanceRestAggTrade>, AggTradeBackfillError> {
    fetch_binance_aggtrade_range_with_budget(
        client,
        endpoint,
        range,
        BINANCE_AGGTRADE_BACKFILL_MAX_ELAPSED,
    )
    .await
}

async fn fetch_binance_aggtrade_range_with_budget(
    client: &reqwest::Client,
    endpoint: &str,
    range: AggTradeBackfillRange,
    max_elapsed: Duration,
) -> Result<Vec<BinanceRestAggTrade>, AggTradeBackfillError> {
    let _ = range.row_count()?;
    let deadline = tokio::time::Instant::now() + max_elapsed;
    let mut attempts = 0_u32;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(AggTradeBackfillError::RetryBudgetExhausted {
                attempts,
                last_error: format!(
                    "elapsed budget reached after {} ms",
                    max_elapsed.as_millis()
                ),
            });
        }
        attempts = attempts.saturating_add(1);
        match tokio::time::timeout_at(
            deadline,
            fetch_binance_aggtrade_range_once(client, endpoint, range),
        )
        .await
        {
            Err(_) => {
                return Err(AggTradeBackfillError::RetryBudgetExhausted {
                    attempts,
                    last_error: format!(
                        "elapsed budget reached after {} ms",
                        max_elapsed.as_millis()
                    ),
                });
            }
            Ok(Ok(rows)) => return Ok(rows),
            Ok(Err(error)) => {
                if attempts >= BINANCE_AGGTRADE_BACKFILL_MAX_ATTEMPTS {
                    return Err(AggTradeBackfillError::RetryBudgetExhausted {
                        attempts,
                        last_error: error.to_string(),
                    });
                }
                let exponential = Duration::from_millis(
                    250_u64.saturating_mul(1_u64 << attempts.saturating_sub(1).min(3)),
                );
                let retry_after = match &error {
                    AggTradeBackfillError::HttpStatus {
                        retry_after_seconds: Some(seconds),
                        ..
                    } => Duration::from_secs(*seconds),
                    _ => Duration::ZERO,
                };
                let wait = exponential.max(retry_after);
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if wait >= remaining {
                    return Err(AggTradeBackfillError::RetryBudgetExhausted {
                        attempts,
                        last_error: error.to_string(),
                    });
                }
                tokio::time::sleep(wait).await;
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AggTradeCoordinatorResult {
    pub outputs: Vec<BinanceAggTradeOutput>,
    pub gaps: Vec<String>,
    pub start_backfill: Option<AggTradeBackfillRange>,
}

impl AggTradeCoordinatorResult {
    fn append(&mut self, mut other: Self) {
        self.outputs.append(&mut other.outputs);
        self.gaps.append(&mut other.gaps);
        if self.start_backfill.is_none() {
            self.start_backfill = other.start_backfill;
        }
    }
}

pub struct AggTradeCoordinator {
    state: BinanceAggTradeContinuityState,
    pending_limit: usize,
    pending_rows: usize,
    pending: BTreeMap<u64, Vec<BinanceWsAggTrade>>,
    active_backfill: Option<AggTradeBackfillRange>,
}

impl AggTradeCoordinator {
    #[must_use]
    pub fn with_pending_limit(state: BinanceAggTradeContinuityState, pending_limit: usize) -> Self {
        Self {
            state,
            pending_limit,
            pending_rows: 0,
            pending: BTreeMap::new(),
            active_backfill: None,
        }
    }

    pub fn accept_websocket(
        &mut self,
        websocket: BinanceWsAggTrade,
    ) -> Result<AggTradeCoordinatorResult, String> {
        let id = websocket.shared.aggregate_trade_id;
        let Some(epoch) = self.state.latest_epoch.as_ref() else {
            self.state.start_initial(id);
            return Ok(AggTradeCoordinatorResult {
                outputs: vec![BinanceAggTradeOutput::Websocket(websocket)],
                ..AggTradeCoordinatorResult::default()
            });
        };
        let last_contiguous_id = epoch.last_contiguous_id;

        if id <= last_contiguous_id {
            return Ok(AggTradeCoordinatorResult {
                outputs: vec![BinanceAggTradeOutput::Websocket(websocket)],
                ..AggTradeCoordinatorResult::default()
            });
        }
        if let Some(epoch) = self.state.latest_epoch.as_mut() {
            epoch.last_observed_id = epoch.last_observed_id.max(id);
        }

        if self.active_backfill.is_some() {
            if self.pending_rows >= self.pending_limit {
                let mut result = self.fail_backfill(&format!(
                    "pending_buffer_overflow:limit={}",
                    self.pending_limit
                ))?;
                result.gaps.insert(
                    0,
                    format!(
                        "binance_aggtrade_pending_buffer_overflow:limit={}",
                        self.pending_limit
                    ),
                );
                result.append(self.accept_websocket(websocket)?);
                return Ok(result);
            }
            self.pending.entry(id).or_default().push(websocket);
            self.pending_rows += 1;
            return Ok(AggTradeCoordinatorResult::default());
        }

        if id == last_contiguous_id.saturating_add(1) {
            self.state.accept_contiguous(id)?;
            return Ok(AggTradeCoordinatorResult {
                outputs: vec![BinanceAggTradeOutput::Websocket(websocket)],
                ..AggTradeCoordinatorResult::default()
            });
        }

        if self.pending_limit == 0 {
            return Err("binance_aggtrade_pending_limit_is_zero".to_owned());
        }
        self.pending.insert(id, vec![websocket]);
        self.pending_rows = 1;
        let range = AggTradeBackfillRange {
            start_id: last_contiguous_id.saturating_add(1),
            end_id: id,
        };
        self.active_backfill = Some(range);
        Ok(AggTradeCoordinatorResult {
            start_backfill: Some(range),
            ..AggTradeCoordinatorResult::default()
        })
    }

    pub fn accept_rest_page(
        &mut self,
        rest_page: Vec<BinanceRestAggTrade>,
    ) -> Result<AggTradeCoordinatorResult, String> {
        let active = self
            .active_backfill
            .ok_or_else(|| "binance_aggtrade_rest_page_without_active_backfill".to_owned())?;
        if rest_page.is_empty() {
            return Err("binance_aggtrade_empty_rest_page".to_owned());
        }
        let mut result = AggTradeCoordinatorResult::default();
        for rest in rest_page {
            let expected = self
                .state
                .latest_epoch
                .as_ref()
                .ok_or_else(|| "binance_aggtrade_rest_without_epoch".to_owned())?
                .last_contiguous_id
                .saturating_add(1);
            let id = rest.shared.aggregate_trade_id;
            if id != expected || id > active.end_id {
                return Err(format!(
                    "binance_aggtrade_rest_sequence_invalid:expected={expected}:actual={id}:end={}",
                    active.end_id
                ));
            }
            let websocket_rows = self.pending.remove(&id).unwrap_or_default();
            self.pending_rows = self.pending_rows.saturating_sub(websocket_rows.len());
            merge_transport_rows(rest, websocket_rows, &mut result);
            self.state.accept_contiguous(id)?;
        }
        Ok(result)
    }

    pub fn complete_backfill(&mut self) -> Result<AggTradeCoordinatorResult, String> {
        let active = self
            .active_backfill
            .ok_or_else(|| "binance_aggtrade_complete_without_active_backfill".to_owned())?;
        let contiguous = self
            .state
            .latest_epoch
            .as_ref()
            .ok_or_else(|| "binance_aggtrade_complete_without_epoch".to_owned())?
            .last_contiguous_id;
        if contiguous != active.end_id {
            return Err(format!(
                "binance_aggtrade_backfill_incomplete:expected_end={}:actual={contiguous}",
                active.end_id
            ));
        }
        self.active_backfill = None;
        self.drain_pending_contiguous()
    }

    pub fn fail_backfill(&mut self, reason: &str) -> Result<AggTradeCoordinatorResult, String> {
        if self.active_backfill.take().is_none() {
            return Err("binance_aggtrade_fail_without_active_backfill".to_owned());
        }
        let mut result = AggTradeCoordinatorResult {
            gaps: vec![format!("binance_aggtrade_backfill_failed:{reason}")],
            ..AggTradeCoordinatorResult::default()
        };
        let first_id = self
            .pending
            .first_key_value()
            .map(|(id, _)| *id)
            .ok_or_else(|| "binance_aggtrade_failed_backfill_without_pending_ws".to_owned())?;
        if let Some(range) = self.state.close_gap_and_start_epoch(first_id, reason) {
            result.gaps.push(format!(
                "binance_aggtrade_unresolved_range:{}-{}",
                range.start_id, range.end_id
            ));
        }
        let first_rows = self.pending.remove(&first_id).unwrap_or_default();
        self.pending_rows = self.pending_rows.saturating_sub(first_rows.len());
        result
            .outputs
            .extend(first_rows.into_iter().map(BinanceAggTradeOutput::Websocket));
        result.append(self.drain_pending_contiguous()?);
        Ok(result)
    }

    pub fn flush_pending_without_backfill(
        &mut self,
        reason: &str,
    ) -> Result<AggTradeCoordinatorResult, String> {
        let mut result = AggTradeCoordinatorResult::default();
        while self.active_backfill.is_some() {
            let mut failed = self.fail_backfill(reason)?;
            failed.start_backfill = None;
            result.append(failed);
        }
        if self.pending_rows != 0 || !self.pending.is_empty() {
            return Err(format!(
                "binance_aggtrade_flush_left_pending_rows:{}",
                self.pending_rows
            ));
        }
        result.start_backfill = None;
        Ok(result)
    }

    fn drain_pending_contiguous(&mut self) -> Result<AggTradeCoordinatorResult, String> {
        let mut result = AggTradeCoordinatorResult::default();
        loop {
            let Some(next_id) = self.pending.first_key_value().map(|(id, _)| *id) else {
                return Ok(result);
            };
            let last_contiguous = self
                .state
                .latest_epoch
                .as_ref()
                .ok_or_else(|| "binance_aggtrade_pending_without_epoch".to_owned())?
                .last_contiguous_id;
            if next_id > last_contiguous.saturating_add(1) {
                let range = AggTradeBackfillRange {
                    start_id: last_contiguous.saturating_add(1),
                    end_id: next_id,
                };
                self.active_backfill = Some(range);
                result.start_backfill = Some(range);
                return Ok(result);
            }
            let rows = self.pending.remove(&next_id).unwrap_or_default();
            self.pending_rows = self.pending_rows.saturating_sub(rows.len());
            if next_id == last_contiguous.saturating_add(1) {
                self.state.accept_contiguous(next_id)?;
            }
            result
                .outputs
                .extend(rows.into_iter().map(BinanceAggTradeOutput::Websocket));
        }
    }

    #[must_use]
    pub const fn state(&self) -> &BinanceAggTradeContinuityState {
        &self.state
    }

    #[must_use]
    pub const fn pending_rows(&self) -> usize {
        self.pending_rows
    }

    #[must_use]
    pub const fn active_backfill(&self) -> Option<AggTradeBackfillRange> {
        self.active_backfill
    }
}

fn merge_transport_rows(
    rest: BinanceRestAggTrade,
    websocket_rows: Vec<BinanceWsAggTrade>,
    result: &mut AggTradeCoordinatorResult,
) {
    let matching = websocket_rows
        .iter()
        .position(|websocket| websocket.shared == rest.shared);
    match matching {
        Some(index) => {
            let mut websocket_rows = websocket_rows;
            let websocket = websocket_rows.remove(index);
            result
                .outputs
                .push(BinanceAggTradeOutput::WebsocketRestMerged {
                    websocket,
                    rest_backfill: rest,
                });
            if !websocket_rows.is_empty() {
                let id = websocket_rows[0].shared.aggregate_trade_id;
                result
                    .gaps
                    .push(format!("binance_aggtrade_ws_rest_conflict:id={id}"));
                result.outputs.extend(
                    websocket_rows
                        .into_iter()
                        .map(BinanceAggTradeOutput::Websocket),
                );
            }
        }
        None if websocket_rows.is_empty() => {
            result
                .outputs
                .push(BinanceAggTradeOutput::RestBackfill(rest));
        }
        None => {
            let id = rest.shared.aggregate_trade_id;
            result
                .outputs
                .push(BinanceAggTradeOutput::RestBackfill(rest));
            result.outputs.extend(
                websocket_rows
                    .into_iter()
                    .map(BinanceAggTradeOutput::Websocket),
            );
            result
                .gaps
                .push(format!("binance_aggtrade_ws_rest_conflict:id={id}"));
        }
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

fn system_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncReadExt as _;
    use tokio::net::TcpListener;

    use super::{
        AggTradeBackfillError, AggTradeBackfillRange, fetch_binance_aggtrade_range_with_budget,
    };

    #[tokio::test]
    async fn aggregate_backfill_budget_cancels_an_inflight_request_at_the_absolute_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1_024];
            let _ = socket.read(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let started = tokio::time::Instant::now();
        let error = fetch_binance_aggtrade_range_with_budget(
            &client,
            &format!("http://{address}/api/v3/aggTrades"),
            AggTradeBackfillRange {
                start_id: 101,
                end_id: 101,
            },
            Duration::from_millis(30),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AggTradeBackfillError::RetryBudgetExhausted { attempts: 1, .. }
        ));
        assert!(started.elapsed() < Duration::from_millis(250));
        server.abort();
    }
}
