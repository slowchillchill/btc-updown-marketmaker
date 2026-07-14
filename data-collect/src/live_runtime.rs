use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array as _, StringArray, TimestampMillisecondArray, UInt64Array};
use futures::{SinkExt as _, StreamExt as _};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use polymarket_client_sdk_v2::clob::ws::{
    BookUpdate, LastTradePrice, MarketResolved, NewMarket, PriceChange, TickSizeChange, WsMessage,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::rtds::{ChainlinkPrice, Subscription};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::backpressure::bounded_event_queue;
use crate::binance_aggtrade::{
    AggTradeBackfillRange, AggTradeCoordinator, AggTradeCoordinatorResult,
    BINANCE_AGGTRADE_PENDING_MAX_ROWS, BinanceAggTradeContinuityState, BinanceAggTradeOutput,
    BinanceRestAggTrade, BinanceWsAggTrade, fetch_binance_aggtrade_range,
};
use crate::daily_manifest::{
    DailyFileRecoveryMetadata, DailyFinalizedFileDelta, DailyManifestIdentity,
    SourceOperationalCounterDelta, add_manifest_operational_counters, close_day_manifest,
    close_reconciled_day_manifest, day_has_finalized_parquet, load_day_manifest,
    manifest_binance_continuity, manifest_state, merge_finalized_file_delta,
    reconcile_open_day_manifest, record_day_barrier_state, record_manifest_gaps,
    remove_unowned_stale_temporary_files, typed_raw_root, update_manifest_continuity,
    utc_day_directories, validate_manifest_day, validate_manifest_recovery_files,
    validate_unmanifested_recovery_files,
};
use crate::dedupe::{
    BinanceIdDecision, BinanceRecentIdWindow, FingerprintDecision, LifecycleFingerprintSets,
    TimedFingerprintWindow,
};
use crate::event::{EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp};
use crate::health::{NetworkFailure, RetryPolicy, plan_retry_trace};
use crate::hourly_partition::{HourlyPartition, HourlyPartitionPolicy};
use crate::inflight::{BUFFER_ACQUIRE_TIMEOUT, InflightBudget, InflightReservation};
pub use crate::inflight::{
    GLOBAL_INFLIGHT_MAX_BYTES, GLOBAL_INFLIGHT_MAX_ROWS, MAX_TYPED_ROW_BYTES,
};
use crate::latency::{LatencyStats, QueueDurationTracker};
use crate::manifest::SourceCoverage;
use crate::market_discovery::{
    BtcUpdownDiscovery, BtcUpdownMarketCandidate, discovery_windows, select_btc_updown_markets,
};
use crate::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataContext, PolymarketMarketMetadataRow,
};
use crate::polymarket_orderbook_layout::{PolymarketOrderbookContext, PolymarketOrderbookRow};
use crate::polymarket_sdk_runtime::{MarketSdkRuntime, RtdsSdkRuntime, SdkStreamError};
use crate::source_format::{RtdsChainlinkBtcUsdRaw, plausible_unix_timestamp_ms};
use crate::source_hourly_layout::{
    BinanceAggTradeHourlyRow, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RtdsChainlinkBtcUsdHourlyRow,
};
use crate::sources::binance::{
    apply_depth_stitch_evidence, parse_depth_snapshot_raw, parse_depth_update_raw,
    stitch_depth_update,
};
use crate::writer::{
    DAILY_FILE_RECOVERY_METADATA_KEY, PublishedSegment, RawEvent, TypedParquetSegmentWriter,
};
#[cfg(test)]
use crate::writer::{PARQUET_ROW_GROUP_MAX_BYTES, PARQUET_ROW_GROUP_MAX_ROWS};

const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
const BINANCE_DEPTH_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";
const BINANCE_AGGTRADE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const BINANCE_AGGTRADE_REST: &str = "https://api.binance.com/api/v3/aggTrades";
const BINANCE_DEPTH_SNAPSHOT: &str =
    "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000";

const DEFAULT_CONTINUOUS_DURATION_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_DISCOVERY_WAIT_SECS: u64 = 90;
const DEFAULT_QUEUE_CAPACITY: usize = 25_000;
const DEFAULT_FLUSH_ROWS: usize = 5_000;
const DEFAULT_MAX_FLUSH_INTERVAL_MS: u64 = 1_000;
const UTC_DAY_MS: i64 = 86_400_000;
const DEFAULT_WRITER_CHANNEL_CAPACITY: usize = 8;
const DATASET_CAPACITY_SCAN_INTERVAL_SECS: u64 = 60;
const WRITER_FINALIZE_BUDGET_MS: u64 = 30_000;
const COORDINATED_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(90);
const MAX_RECORDED_SOURCE_GAPS: usize = 256;
const SOURCE_GAP_OVERFLOW_PREFIX: &str = "source_gap_overflow_count:";
const BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS: u32 = 3;
const BINANCE_HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const BINANCE_HTTP_TOTAL_TIMEOUT_SECS: u64 = 15;
const WS_CONNECT_TIMEOUT_SECS: u64 = 20;
const BINANCE_WS_RECONNECT_BASE_MS: u64 = 500;
const BINANCE_WS_RECONNECT_MAX_MS: u64 = 5_000;
const SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS: u32 = 12;
const SOURCE_RECOVERY_CYCLE_MAX_ELAPSED: Duration = Duration::from_secs(120);
const SOURCE_SUPERVISOR_BACKOFF_BASE_MS: u64 = 1_000;
const SOURCE_SUPERVISOR_BACKOFF_MAX_MS: u64 = 30_000;
const SOURCE_RECOVERY_JITTER_BPS: u32 = 2_000;
const RTDS_RESUBSCRIBE_INTERVAL_SECS: u64 = 3;
const RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW: usize = 2;
const RTDS_FRESHNESS_TIMEOUT_SECS: u64 = 60;
const RTDS_FRESH_MIN_AGE_MS: i64 = -5_000;
const RTDS_FRESH_MAX_AGE_MS: i64 = 30_000;
const MARKET_FIRST_EVENT_TIMEOUT_SECS: u64 = 30;
const MARKET_FRESHNESS_TIMEOUT_SECS: u64 = 60;
const MARKET_RESUBSCRIBE_ATTEMPTS_PER_SILENCE: u32 = 2;
const MARKET_NEXT_DISCOVERY_INTERVAL_SECS: u64 = 10;
const POLYMARKET_FINGERPRINTS_PER_LIFECYCLE: usize = 2_000_000;
const BINANCE_AGGTRADE_RECOVERY_IDS: usize = 25_000;
const RTDS_DEDUPE_WINDOW_MS: i64 = 60 * 60 * 1000;
const RTDS_DEDUPE_MAX_KEYS: usize = 100_000;
const MANIFEST_CHANNEL_CAPACITY: usize = 64;
const AGGTRADE_CONTROL_CHANNEL_CAPACITY: usize = 4;

const LOGICAL_PRODUCERS: &[&str] = &[
    "polymarket",
    "polymarket_rtds",
    "binance_depth",
    "binance_aggtrade",
];

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, Default)]
struct SourceGapStore {
    all: Vec<String>,
    by_utc_day: BTreeMap<String, Vec<String>>,
}

impl SourceGapStore {
    fn all_reasons(&self) -> Vec<String> {
        self.all.clone()
    }

    fn reasons_for_day(&self, utc_day: &str) -> Vec<String> {
        self.by_utc_day.get(utc_day).cloned().unwrap_or_default()
    }
}

impl std::ops::Deref for SourceGapStore {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.all
    }
}

type SharedSourceGaps = Arc<Mutex<SourceGapStore>>;

pub const LIVE_SOURCES: &[&str] = &[
    "polymarket_market",
    "polymarket_market_metadata",
    "binance_depth_snapshot",
    "binance_depth_update",
    "binance_aggtrade",
    "polymarket_rtds_chainlink_btc_usd",
];

pub fn is_live_runtime_source_gap_reason(reason: &str) -> bool {
    !reason.starts_with("bounded_queue_backpressure_wait:")
        && !reason.starts_with("writer_queue_backpressure_wait:")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RtdsFreshnessAction {
    wait: Duration,
    resubscribe_attempt: u64,
    exit_after_wait: bool,
}

#[derive(Clone, Debug)]
struct ClassifiedRtdsMessage {
    payload: ChainlinkPrice,
    age_ms: Option<i64>,
    freshness_status: &'static str,
}

fn classify_rtds_message(
    message: &polymarket_client_sdk_v2::rtds::RtdsMessage,
    recv_time_utc_ms: i64,
) -> Result<ClassifiedRtdsMessage> {
    let payload = serde_json::from_value::<ChainlinkPrice>(message.payload.clone())
        .map_err(|error| invalid_data(format!("RTDS Chainlink payload invalid: {error}")))?;
    let age_ms = recv_time_utc_ms.checked_sub(payload.timestamp);
    let freshness_status = if !plausible_unix_timestamp_ms(message.timestamp)
        || !plausible_unix_timestamp_ms(payload.timestamp)
        || age_ms.is_none()
    {
        "invalid_timestamp"
    } else if message.topic != "crypto_prices_chainlink" {
        "invalid_topic"
    } else if message.msg_type != "update" {
        "invalid_type"
    } else if payload.symbol != "btc/usd" {
        "invalid_symbol"
    } else if age_ms
        .is_some_and(|age| (RTDS_FRESH_MIN_AGE_MS..=RTDS_FRESH_MAX_AGE_MS).contains(&age))
    {
        "fresh"
    } else {
        "stale"
    };
    Ok(ClassifiedRtdsMessage {
        payload,
        age_ms,
        freshness_status,
    })
}

fn rtds_freshness_actions() -> [RtdsFreshnessAction; RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW]
{
    let mut actions = [RtdsFreshnessAction {
        wait: Duration::ZERO,
        resubscribe_attempt: 0,
        exit_after_wait: false,
    }; RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW];
    for (index, action) in actions.iter_mut().enumerate() {
        let attempt = u64::try_from(index + 1).expect("RTDS attempt index fits in u64");
        *action = RtdsFreshnessAction {
            wait: Duration::from_secs(RTDS_RESUBSCRIBE_INTERVAL_SECS),
            resubscribe_attempt: attempt,
            exit_after_wait: index + 1 == RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW,
        };
    }
    actions
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarketSilenceKind {
    FirstEvent,
    Freshness,
}

impl MarketSilenceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FirstEvent => "first_event",
            Self::Freshness => "freshness",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MarketAssetSilence {
    asset_id: String,
    kind: MarketSilenceKind,
}

#[derive(Clone, Debug)]
struct MarketAssetHealth {
    subscribed_at: Instant,
    last_business_event: Option<Instant>,
    resubscribe_attempts: u32,
}

struct MarketAssetFreshnessWatchdog {
    first_event_timeout: Duration,
    freshness_timeout: Duration,
    assets: BTreeMap<String, MarketAssetHealth>,
}

impl MarketAssetFreshnessWatchdog {
    fn new(first_event_timeout: Duration, freshness_timeout: Duration) -> Self {
        Self {
            first_event_timeout,
            freshness_timeout,
            assets: BTreeMap::new(),
        }
    }

    fn replace_assets(&mut self, desired: BTreeSet<String>, now: Instant) {
        self.assets.retain(|asset, _| desired.contains(asset));
        for asset in desired {
            self.assets.entry(asset).or_insert(MarketAssetHealth {
                subscribed_at: now,
                last_business_event: None,
                resubscribe_attempts: 0,
            });
        }
    }

    fn observe(&mut self, observed: &BTreeSet<String>, now: Instant) {
        for asset in observed {
            if let Some(health) = self.assets.get_mut(asset) {
                health.last_business_event = Some(now);
                health.resubscribe_attempts = 0;
            }
        }
    }

    fn expired(&self, now: Instant) -> Vec<MarketAssetSilence> {
        self.assets
            .iter()
            .filter_map(|(asset_id, health)| {
                let (since, timeout, kind) = health.last_business_event.map_or(
                    (
                        health.subscribed_at,
                        self.first_event_timeout,
                        MarketSilenceKind::FirstEvent,
                    ),
                    |last| (last, self.freshness_timeout, MarketSilenceKind::Freshness),
                );
                (now.saturating_duration_since(since) >= timeout).then(|| MarketAssetSilence {
                    asset_id: asset_id.clone(),
                    kind,
                })
            })
            .collect()
    }

    fn record_resubscribe(
        &mut self,
        expired: &[MarketAssetSilence],
        now: Instant,
    ) -> std::result::Result<(), String> {
        let exhausted = expired
            .iter()
            .filter_map(|entry| {
                self.assets
                    .get(&entry.asset_id)
                    .filter(|health| {
                        health.resubscribe_attempts >= MARKET_RESUBSCRIBE_ATTEMPTS_PER_SILENCE
                    })
                    .map(|_| entry.asset_id.clone())
            })
            .collect::<Vec<_>>();
        if !exhausted.is_empty() {
            return Err(format!(
                "polymarket market freshness recovery exhausted for assets {}",
                exhausted.join(",")
            ));
        }
        for entry in expired {
            if let Some(health) = self.assets.get_mut(&entry.asset_id) {
                health.subscribed_at = now;
                health.last_business_event = None;
                health.resubscribe_attempts = health.resubscribe_attempts.saturating_add(1);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct SourceQueueBackpressureStats {
    wait_count: AtomicU64,
    timeout_count: AtomicU64,
    dropped_rows: AtomicU64,
    operational_counters: Mutex<BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>>,
    active_recovery_sources: Mutex<BTreeSet<String>>,
}

impl SourceQueueBackpressureStats {
    fn record_wait(&self) {
        self.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout(&self) {
        self.timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_drop(&self, rows: u64) {
        self.dropped_rows.fetch_add(rows, Ordering::Relaxed);
    }

    fn record_structural_dedupe(&self, source_kind: &str, recv_time_utc_ms: i64, events: u64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.structural_dedupe_dropped_events = counter
                .structural_dedupe_dropped_events
                .saturating_add(events);
        });
    }

    fn record_lag(&self, source_kind: &str, recv_time_utc_ms: i64, events: u64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.lag_events = counter.lag_events.saturating_add(events);
        });
    }

    fn record_reconnect(&self, source_kind: &str, recv_time_utc_ms: i64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.reconnect_events = counter.reconnect_events.saturating_add(1);
        });
    }

    fn record_operational_drop(&self, source_kind: &str, recv_time_utc_ms: i64, rows: u64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.dropped_rows = counter.dropped_rows.saturating_add(rows);
        });
    }

    fn record_operational(
        &self,
        source_kind: &str,
        recv_time_utc_ms: i64,
        update: impl FnOnce(&mut SourceOperationalCounterDelta),
    ) {
        let Some(utc_day) = utc_day_string_from_ms(recv_time_utc_ms) else {
            return;
        };
        let mut counters = self
            .operational_counters
            .lock()
            .expect("operational counter mutex poisoned");
        let counter = counters
            .entry(utc_day)
            .or_default()
            .entry(source_kind.to_owned())
            .or_default();
        update(counter);
    }

    fn drain_operational_day(
        &self,
        utc_day: &str,
    ) -> BTreeMap<String, SourceOperationalCounterDelta> {
        self.operational_counters
            .lock()
            .expect("operational counter mutex poisoned")
            .remove(utc_day)
            .unwrap_or_default()
    }

    fn drain_all_operational(
        &self,
    ) -> BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>> {
        std::mem::take(
            &mut *self
                .operational_counters
                .lock()
                .expect("operational counter mutex poisoned"),
        )
    }

    fn set_recovery_active(&self, source_kind: &str, active: bool) {
        let mut sources = self
            .active_recovery_sources
            .lock()
            .expect("active recovery mutex poisoned");
        if active {
            sources.insert(source_kind.to_owned());
        } else {
            sources.remove(source_kind);
        }
    }

    fn is_recovery_active(&self, source_kind: &str) -> bool {
        self.active_recovery_sources
            .lock()
            .expect("active recovery mutex poisoned")
            .contains(source_kind)
    }

    fn apply_to_tracker(&self, tracker: &mut QueueDurationTracker) {
        let wait_count = self.wait_count.swap(0, Ordering::Relaxed);
        for _ in 0..wait_count {
            tracker.record_backpressure_wait();
        }
        let timeout_count = self.timeout_count.swap(0, Ordering::Relaxed);
        for _ in 0..timeout_count {
            tracker.record_backpressure_timeout();
        }
        let dropped_rows = self.dropped_rows.swap(0, Ordering::Relaxed);
        if dropped_rows > 0 {
            tracker.record_drop(dropped_rows);
        }
    }
}

#[derive(Clone)]
struct SourceEventSink {
    tx: mpsc::Sender<InflightRawEvent>,
    gaps: SharedSourceGaps,
    source_queue_backpressure: Arc<SourceQueueBackpressureStats>,
    inflight_budget: InflightBudget,
    enqueue_lock: Arc<AsyncMutex<()>>,
}

impl SourceEventSink {
    fn fork_logical_producer(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            gaps: Arc::clone(&self.gaps),
            source_queue_backpressure: Arc::clone(&self.source_queue_backpressure),
            inflight_budget: self.inflight_budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        }
    }
}

enum IngressControl {
    DayBarrier {
        producer: String,
        utc_day: String,
    },
    BinanceContinuity {
        utc_day: String,
        continuity: BinanceAggTradeContinuityState,
    },
}

enum AggTradeSourceControl {
    Flush {
        reason: String,
        utc_day: String,
        stop_after_flush: bool,
        resume_after_barrier: Option<oneshot::Receiver<()>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

enum DepthSourceControl {
    Fence {
        stop_after_fence: bool,
        resume_after_barrier: Option<oneshot::Receiver<()>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

struct PendingAggTradeStop {
    reason: String,
    utc_day: String,
    reply: oneshot::Sender<std::result::Result<(), String>>,
}

struct InflightRawEvent {
    event: RawEvent,
    control: Option<IngressControl>,
    _source_recv_monotonic_ms: u64,
    _reservation: Option<InflightReservation>,
}

impl InflightRawEvent {
    #[cfg(test)]
    fn for_test(event: RawEvent) -> Self {
        let budget = InflightBudget::new();
        let bytes = owned_event_bytes(&event);
        Self {
            event,
            control: None,
            _source_recv_monotonic_ms: 0,
            _reservation: Some(budget.try_acquire(1, bytes)),
        }
    }
}

#[derive(Clone)]
struct RuntimeProvenance {
    code_version: String,
    collector_binary_sha256: String,
    source_config_digest: String,
    collector_host_id: String,
    collector_region: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveTrainingRuntimeConfig {
    pub dataset_root: PathBuf,
    pub duration_secs: Option<u64>,
    pub run_forever: bool,
    pub collector_host_id: String,
    pub collector_region: String,
    pub code_version: String,
    pub collector_binary_sha256: String,
    pub source_config_digest: String,
    pub max_dataset_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LiveTrainingRuntimeReceipt {
    pub finalized_segments: Vec<LiveFinalizedSegment>,
    pub source_coverage: Vec<SourceCoverage>,
    pub source_gaps: Vec<String>,
}

fn deadline_reached(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn deadline_remaining(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

async fn run_logical_producer_with_day_barriers<F>(
    producer: &'static str,
    sink: SourceEventSink,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for producer barrier"))?;
            let next_utc_day = utc_day_string_from_ms(next_boundary_ms)
                .ok_or_else(|| invalid_data("next UTC day unavailable for producer gap"))?;
            record_recovery_boundary_gaps(&sink, producer, &utc_day, &next_utc_day);
            if source_started {
                if let Some(source_result) = wait_for_day_barrier_while_source_runs(
                    source.as_mut(),
                    &sink,
                    run_started,
                    producer,
                    utc_day,
                )
                .await?
                {
                    return source_result;
                }
            } else {
                send_day_barrier(&sink, run_started, producer, utc_day).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => return result,
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

async fn wait_for_day_barrier_while_source_runs<F>(
    mut source: Pin<&mut F>,
    sink: &SourceEventSink,
    run_started: Instant,
    producer: &str,
    utc_day: String,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let barrier = send_day_barrier(sink, run_started, producer, utc_day);
    tokio::pin!(barrier);
    tokio::select! {
        result = source.as_mut() => Ok(Some(result)),
        result = &mut barrier => {
            result?;
            Ok(None)
        },
    }
}

async fn run_aggtrade_logical_producer_with_day_barriers<F>(
    sink: SourceEventSink,
    control_tx: mpsc::Sender<AggTradeSourceControl>,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for aggTrade barrier"))?;
            let next_utc_day = utc_day_string_from_ms(next_boundary_ms)
                .ok_or_else(|| invalid_data("next UTC day unavailable for aggTrade gap"))?;
            if source_started {
                if let Some(source_result) = flush_aggtrade_and_send_day_barrier(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &utc_day,
                    &next_utc_day,
                )
                .await?
                {
                    return source_result;
                }
            } else {
                record_recovery_boundary_gaps(&sink, "binance_aggtrade", &utc_day, &next_utc_day);
                send_day_barrier(&sink, run_started, "binance_aggtrade", utc_day).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => return result,
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

async fn run_depth_logical_producer_with_day_barriers<F>(
    sink: SourceEventSink,
    control_tx: mpsc::Sender<DepthSourceControl>,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for depth barrier"))?;
            let next_utc_day = utc_day_string_from_ms(next_boundary_ms)
                .ok_or_else(|| invalid_data("next UTC day unavailable for depth gap"))?;
            if source_started {
                if let Some(source_result) = fence_depth_and_send_day_barrier(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &utc_day,
                    &next_utc_day,
                )
                .await?
                {
                    return source_result;
                }
            } else {
                record_recovery_boundary_gaps(&sink, "binance_depth", &utc_day, &next_utc_day);
                send_day_barrier(&sink, run_started, "binance_depth", utc_day).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => return result,
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

async fn fence_depth_and_send_day_barrier<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<DepthSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    utc_day: &str,
    next_utc_day: &str,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let (reply_tx, mut reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            stop_after_fence: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .map_err(|_| invalid_data("depth source control channel closed before day barrier"))?;
    tokio::select! {
        result = source.as_mut() => Ok(Some(result)),
        reply = &mut reply_rx => {
            match reply {
                Ok(Ok(())) => {
                    record_recovery_boundary_gaps(
                        sink,
                        "binance_depth",
                        utc_day,
                        next_utc_day,
                    );
                    let barrier_result = send_day_barrier(
                        sink,
                        run_started,
                        "binance_depth",
                        utc_day.to_owned(),
                    )
                    .await;
                    let _ = resume_tx.send(());
                    barrier_result?;
                    Ok(None)
                }
                Ok(Err(error)) => Err(invalid_data(format!(
                    "depth source fence failed before day barrier:{error}"
                )).into()),
                Err(_) => Err(invalid_data(
                    "depth source dropped day-barrier fence reply",
                ).into()),
            }
        }
    }
}

async fn flush_aggtrade_and_send_day_barrier<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<AggTradeSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    utc_day: &str,
    next_utc_day: &str,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let (reply_tx, mut reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(AggTradeSourceControl::Flush {
            reason: format!("utc_day_barrier:{utc_day}"),
            utc_day: utc_day.to_owned(),
            stop_after_flush: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .map_err(|_| invalid_data("aggTrade source control channel closed before day barrier"))?;
    tokio::select! {
        result = source.as_mut() => Ok(Some(result)),
        reply = &mut reply_rx => {
            match reply {
                Ok(Ok(())) => {
                    record_recovery_boundary_gaps(
                        sink,
                        "binance_aggtrade",
                        utc_day,
                        next_utc_day,
                    );
                    let barrier_result = send_day_barrier(
                        sink,
                        run_started,
                        "binance_aggtrade",
                        utc_day.to_owned(),
                    )
                    .await;
                    let _ = resume_tx.send(());
                    barrier_result?;
                    Ok(None)
                }
                Ok(Err(error)) => Err(invalid_data(format!(
                    "aggTrade source flush failed before day barrier:{error}"
                )).into()),
                Err(_) => Err(invalid_data(
                    "aggTrade source dropped day-barrier flush reply",
                ).into()),
            }
        }
    }
}

fn record_recovery_boundary_gaps(
    sink: &SourceEventSink,
    producer: &str,
    old_utc_day: &str,
    new_utc_day: &str,
) {
    let Some(source_kind) = recovery_source_for_producer(producer) else {
        return;
    };
    if !sink
        .source_queue_backpressure
        .is_recovery_active(source_kind)
    {
        return;
    }
    let reason = format!("source_recovery_overlaps_utc_day_boundary:{source_kind}");
    record_gap_for_day(&sink.gaps, old_utc_day, reason.clone());
    record_gap_for_day(&sink.gaps, new_utc_day, reason);
}

fn recovery_source_for_producer(producer: &str) -> Option<&'static str> {
    match producer {
        "polymarket" => Some("polymarket_market"),
        "polymarket_rtds" => Some("polymarket_rtds_chainlink_btc_usd"),
        "binance_depth" => Some("binance_depth_update"),
        "binance_aggtrade" => Some("binance_aggtrade"),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceRecoveryWaitLevel {
    InnerAttempt,
    OuterCycle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SourceRecoveryWait {
    level: SourceRecoveryWaitLevel,
    delay: Duration,
    attempt: u32,
}

struct SourceRecoverySupervisor {
    source_kind: &'static str,
    inner_attempts: u32,
    outer_cycles: u32,
    cycle_started: Instant,
    jitter_seed: u64,
}

impl SourceRecoverySupervisor {
    fn new(source_kind: &'static str, jitter_seed: u64) -> Self {
        Self {
            source_kind,
            inner_attempts: 0,
            outer_cycles: 0,
            cycle_started: Instant::now(),
            jitter_seed,
        }
    }

    fn record_business_recovery(&mut self) {
        self.inner_attempts = 0;
        self.outer_cycles = 0;
        self.cycle_started = Instant::now();
    }

    fn next_wait(&mut self) -> SourceRecoveryWait {
        self.inner_attempts = self.inner_attempts.saturating_add(1);
        let cycle_exhausted = self.inner_attempts > SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS
            || self.cycle_started.elapsed() >= SOURCE_RECOVERY_CYCLE_MAX_ELAPSED;
        if cycle_exhausted {
            self.outer_cycles = self.outer_cycles.saturating_add(1);
            self.inner_attempts = 0;
            self.cycle_started = Instant::now();
            let attempt = self.outer_cycles.min(32);
            return SourceRecoveryWait {
                level: SourceRecoveryWaitLevel::OuterCycle,
                delay: retry_delay(
                    self.source_kind,
                    attempt,
                    SOURCE_SUPERVISOR_BACKOFF_BASE_MS,
                    SOURCE_SUPERVISOR_BACKOFF_MAX_MS,
                    self.jitter_seed,
                ),
                attempt,
            };
        }
        SourceRecoveryWait {
            level: SourceRecoveryWaitLevel::InnerAttempt,
            delay: retry_delay(
                self.source_kind,
                self.inner_attempts,
                BINANCE_WS_RECONNECT_BASE_MS,
                BINANCE_WS_RECONNECT_MAX_MS,
                self.jitter_seed,
            ),
            attempt: self.inner_attempts,
        }
    }
}

fn retry_delay(
    source_kind: &str,
    attempt: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    jitter_seed: u64,
) -> Duration {
    let trace = plan_retry_trace(
        source_kind,
        NetworkFailure::Disconnected,
        RetryPolicy {
            max_attempts: attempt.max(1),
            initial_backoff_ms,
            max_backoff_ms,
            max_elapsed_ms: u64::MAX / 4,
            multiplier: 2,
            jitter_bps: SOURCE_RECOVERY_JITTER_BPS,
            jitter_seed,
        },
    );
    Duration::from_millis(
        trace
            .last()
            .map(|entry| entry.sleep_ms)
            .unwrap_or(initial_backoff_ms),
    )
}

async fn wait_for_source_recovery(
    recovery: &mut SourceRecoverySupervisor,
    sink: &SourceEventSink,
    deadline: Option<Instant>,
) -> bool {
    let wait = recovery.next_wait();
    if wait.level == SourceRecoveryWaitLevel::OuterCycle {
        record_gap(
            &sink.gaps,
            format!(
                "source_recovery_cycle_exhausted:{}:outer_cycle={}",
                recovery.source_kind, wait.attempt
            ),
        );
    }
    let delay = deadline_remaining(deadline)
        .map(|remaining| remaining.min(wait.delay))
        .unwrap_or(wait.delay);
    if delay.is_zero() {
        return false;
    }
    sink.source_queue_backpressure
        .record_reconnect(recovery.source_kind, now_ms());
    sink.source_queue_backpressure
        .set_recovery_active(recovery.source_kind, true);
    sleep(delay).await;
    !deadline_reached(deadline)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LiveFinalizedSegment {
    pub source_kind: String,
    pub parquet_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
}

enum ManifestCommand {
    Finalized(DailyFinalizedFileDelta),
    BinanceContinuity {
        utc_day: String,
        continuity: BinanceAggTradeContinuityState,
    },
    ProducerBarrier {
        utc_day: String,
        producer: String,
        gaps: Vec<String>,
        operational_counters: BTreeMap<String, SourceOperationalCounterDelta>,
    },
    WriterDayFinalized {
        utc_day: String,
        source_kind: String,
    },
    Shutdown {
        utc_day: String,
        gaps: Vec<String>,
        operational_counters: BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

struct ManifestOwnerState {
    dataset_root: PathBuf,
    identity: DailyManifestIdentity,
    continuity: BinanceAggTradeContinuityState,
    manifests: BTreeMap<String, Value>,
    producer_barriers: BTreeMap<String, BTreeSet<String>>,
    writer_finalize_acks: BTreeMap<String, BTreeSet<String>>,
}

impl ManifestOwnerState {
    fn new(
        dataset_root: PathBuf,
        identity: DailyManifestIdentity,
        continuity: BinanceAggTradeContinuityState,
    ) -> Self {
        Self {
            dataset_root,
            identity,
            continuity,
            manifests: BTreeMap::new(),
            producer_barriers: BTreeMap::new(),
            writer_finalize_acks: BTreeMap::new(),
        }
    }

    fn insert_reconciled_manifest(&mut self, utc_day: String, mut manifest: Value) -> Result<()> {
        add_manifest_operational_counters(
            &self.day_dir(&utc_day),
            &mut manifest,
            &required_source_counter_defaults(),
        )?;
        self.manifests.insert(utc_day, manifest);
        Ok(())
    }

    fn day_dir(&self, utc_day: &str) -> PathBuf {
        typed_raw_root(&self.dataset_root).join(format!("utc_day={utc_day}"))
    }

    fn ensure_open(&mut self, utc_day: &str) -> Result<()> {
        if self.manifests.contains_key(utc_day) {
            return Ok(());
        }
        let mut manifest = reconcile_open_day_manifest(
            &self.day_dir(utc_day),
            utc_day,
            &self.identity,
            &self.continuity,
        )?;
        add_manifest_operational_counters(
            &self.day_dir(utc_day),
            &mut manifest,
            &required_source_counter_defaults(),
        )?;
        self.manifests.insert(utc_day.to_owned(), manifest);
        Ok(())
    }

    fn apply_finalized(&mut self, delta: DailyFinalizedFileDelta) -> Result<()> {
        self.ensure_open(&delta.utc_day)?;
        let day_dir = self.day_dir(&delta.utc_day);
        let manifest = self
            .manifests
            .get_mut(&delta.utc_day)
            .expect("manifest was ensured");
        merge_finalized_file_delta(&day_dir, manifest, &delta)?;
        Ok(())
    }

    fn update_continuity(
        &mut self,
        utc_day: String,
        continuity: BinanceAggTradeContinuityState,
    ) -> Result<()> {
        self.continuity = continuity;
        self.ensure_open(&utc_day)?;
        let day_dir = self.day_dir(&utc_day);
        let manifest = self
            .manifests
            .get_mut(&utc_day)
            .expect("manifest was ensured");
        update_manifest_continuity(&day_dir, manifest, &self.continuity)?;
        Ok(())
    }

    fn record_gaps(&mut self, utc_day: &str, gaps: &[String]) -> Result<()> {
        self.ensure_open(utc_day)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        record_manifest_gaps(&day_dir, manifest, gaps)?;
        Ok(())
    }

    fn record_operational_counters(
        &mut self,
        utc_day: &str,
        counters: &BTreeMap<String, SourceOperationalCounterDelta>,
    ) -> Result<()> {
        if counters.is_empty() {
            return Ok(());
        }
        self.ensure_open(utc_day)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        add_manifest_operational_counters(&day_dir, manifest, counters)?;
        Ok(())
    }

    fn record_producer_barrier(
        &mut self,
        utc_day: String,
        producer: String,
        gaps: &[String],
        operational_counters: &BTreeMap<String, SourceOperationalCounterDelta>,
    ) -> Result<()> {
        if !LOGICAL_PRODUCERS.contains(&producer.as_str()) {
            return Err(
                invalid_data(format!("unknown logical producer barrier: {producer}")).into(),
            );
        }
        self.record_operational_counters(&utc_day, operational_counters)?;
        self.record_gaps(&utc_day, gaps)?;
        self.producer_barriers
            .entry(utc_day.clone())
            .or_default()
            .insert(producer);
        self.persist_barriers_and_maybe_close(&utc_day)
    }

    fn record_writer_ack(&mut self, utc_day: String, source_kind: String) -> Result<()> {
        if !LIVE_SOURCES.contains(&source_kind.as_str()) {
            return Err(invalid_data(format!("unknown writer finalize ack: {source_kind}")).into());
        }
        self.ensure_open(&utc_day)?;
        self.writer_finalize_acks
            .entry(utc_day.clone())
            .or_default()
            .insert(source_kind);
        self.persist_barriers_and_maybe_close(&utc_day)
    }

    fn persist_barriers_and_maybe_close(&mut self, utc_day: &str) -> Result<()> {
        let producers = self
            .producer_barriers
            .get(utc_day)
            .cloned()
            .unwrap_or_default();
        let writers = self
            .writer_finalize_acks
            .get(utc_day)
            .cloned()
            .unwrap_or_default();
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        record_day_barrier_state(&day_dir, manifest, &producers, &writers)?;
        if producers.len() == LOGICAL_PRODUCERS.len() && writers.len() == LIVE_SOURCES.len() {
            close_day_manifest(&day_dir, manifest, "utc_day_barrier")?;
        }
        Ok(())
    }

    fn shutdown(
        &mut self,
        utc_day: &str,
        gaps: &[String],
        operational_counters: &BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>,
    ) -> Result<()> {
        for (counter_day, counters) in operational_counters {
            self.record_operational_counters(counter_day, counters)?;
        }
        if !self.manifests.contains_key(utc_day) {
            return Ok(());
        }
        self.record_gaps(utc_day, gaps)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self.manifests.get_mut(utc_day).expect("manifest exists");
        if manifest_state(manifest)? == "open" {
            update_manifest_continuity(&day_dir, manifest, &self.continuity)?;
        }
        Ok(())
    }
}

fn required_source_counter_defaults() -> BTreeMap<String, SourceOperationalCounterDelta> {
    LIVE_SOURCES
        .iter()
        .map(|source_kind| {
            (
                (*source_kind).to_owned(),
                SourceOperationalCounterDelta::default(),
            )
        })
        .collect()
}

async fn run_manifest_owner(
    mut owner: ManifestOwnerState,
    mut commands: mpsc::Receiver<ManifestCommand>,
) -> Result<()> {
    while let Some(command) = commands.recv().await {
        match command {
            ManifestCommand::Finalized(delta) => owner.apply_finalized(delta)?,
            ManifestCommand::BinanceContinuity {
                utc_day,
                continuity,
            } => owner.update_continuity(utc_day, continuity)?,
            ManifestCommand::ProducerBarrier {
                utc_day,
                producer,
                gaps,
                operational_counters,
            } => owner.record_producer_barrier(utc_day, producer, &gaps, &operational_counters)?,
            ManifestCommand::WriterDayFinalized {
                utc_day,
                source_kind,
            } => owner.record_writer_ack(utc_day, source_kind)?,
            ManifestCommand::Shutdown {
                utc_day,
                gaps,
                operational_counters,
                reply,
            } => {
                let result = owner
                    .shutdown(&utc_day, &gaps, &operational_counters)
                    .map_err(|error| error.to_string());
                let should_stop = result.is_ok();
                let _ = reply.send(result);
                if should_stop {
                    return Ok(());
                }
                return Err(io::Error::other("daily manifest owner shutdown failed").into());
            }
        }
    }
    Err(io::Error::other("daily manifest command channel closed without shutdown").into())
}

pub async fn run_live_training_runtime(
    config: LiveTrainingRuntimeConfig,
) -> Result<LiveTrainingRuntimeReceipt> {
    fs::create_dir_all(&config.dataset_root)?;
    let dataset_root = config.dataset_root.canonicalize()?;
    let _dataset_root_lock = lock_training_dataset_root(&dataset_root)?;
    if config.max_dataset_bytes == 0 {
        return Err(invalid_data(
            "operator_intervention_required:max_dataset_bytes must be greater than zero",
        )
        .into());
    }
    let startup_dataset_bytes = training_dataset_size_bytes(&dataset_root).map_err(|error| {
        invalid_data(format!(
            "operator_intervention_required:dataset_size_scan_failed:{error}"
        ))
    })?;
    if startup_dataset_bytes >= config.max_dataset_bytes {
        return Err(io::Error::other(format!(
            "dataset_capacity_reached:bytes={startup_dataset_bytes}:limit={}",
            config.max_dataset_bytes
        ))
        .into());
    }
    let duration_secs = config
        .duration_secs
        .unwrap_or(DEFAULT_CONTINUOUS_DURATION_SECS);
    let bounded_run = !config.run_forever;

    let run_started = Instant::now();
    let deadline = bounded_run.then(|| run_started + Duration::from_secs(duration_secs));
    let deadline_ms = bounded_run.then(|| now_ms().saturating_add((duration_secs * 1000) as i64));
    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let source_queue_backpressure = Arc::new(SourceQueueBackpressureStats::default());
    let collector_identity = CollectorIdentity {
        host_id: config.collector_host_id.clone(),
        region: config.collector_region.clone(),
    };
    let cleanup_root = dataset_root.clone();
    let removed_stale_files =
        tokio::task::spawn_blocking(move || remove_unowned_stale_temporary_files(&cleanup_root))
            .await
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:stale_temporary_cleanup_join_failed:{error}"
                ))
            })?
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:stale_temporary_cleanup_failed:{error}"
                ))
            })?;
    for relative_path in removed_stale_files {
        record_gap(
            &source_gaps,
            format!("stale_temporary_removed:{relative_path}"),
        );
    }
    let inflight_budget = InflightBudget::new();
    let (event_tx, mut event_rx) = bounded_event_queue::<InflightRawEvent>(DEFAULT_QUEUE_CAPACITY)?;
    let event_sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&source_gaps),
        source_queue_backpressure: Arc::clone(&source_queue_backpressure),
        inflight_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let provenance = RuntimeProvenance {
        code_version: config.code_version.clone(),
        collector_binary_sha256: config.collector_binary_sha256.clone(),
        source_config_digest: config.source_config_digest.clone(),
        collector_host_id: config.collector_host_id.clone(),
        collector_region: config.collector_region.clone(),
    };
    let daily_manifest_identity = DailyManifestIdentity {
        build_commit: config.code_version.clone(),
        collector_binary_sha256: config.collector_binary_sha256.clone(),
        config_digest: config.source_config_digest.clone(),
        collector_host_id: config.collector_host_id.clone(),
        collector_region: config.collector_region.clone(),
    };
    let recovery_root = dataset_root.clone();
    let recovery_identity = daily_manifest_identity.clone();
    let startup_recovery = tokio::task::spawn_blocking(move || {
        reconcile_stable_startup_recovery(&recovery_root, &recovery_identity, now_ms)
    })
    .await
    .map_err(|error| {
        invalid_data(format!(
            "operator_intervention_required:startup recovery join failed:{error}"
        ))
    })?
    .map_err(|error| {
        invalid_data(format!(
            "operator_intervention_required:startup recovery failed:{error}"
        ))
    })?;
    let StableStartupRecovery {
        utc_day: recovery_day,
        initial_next_boundary_ms,
        binance_aggtrade: binance_aggtrade_recovery,
        current_manifest,
    } = startup_recovery;
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let mut manifest_owner = ManifestOwnerState::new(
        dataset_root.clone(),
        daily_manifest_identity,
        binance_aggtrade_recovery.continuity.clone(),
    );
    manifest_owner.insert_reconciled_manifest(recovery_day.clone(), current_manifest)?;
    let mut manifest_owner_handle = tokio::spawn(run_manifest_owner(manifest_owner, manifest_rx));
    let mut writer_handles = start_writer_workers(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &source_queue_backpressure,
    )?;

    let polymarket_sink = event_sink.fork_logical_producer();
    let polymarket_source_sink = polymarket_sink.clone();
    let polymarket_source = run_polymarket_source(
        dataset_root.clone(),
        polymarket_source_sink,
        collector_identity.clone(),
        deadline_ms,
        DEFAULT_DISCOVERY_WAIT_SECS,
        run_started,
    );
    let rtds_sink = event_sink.fork_logical_producer();
    let rtds_source = run_rtds_source(
        dataset_root.clone(),
        rtds_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
    );
    let depth_sink = event_sink.fork_logical_producer();
    let (depth_control_tx, depth_control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let depth_source = run_binance_depth_source(
        depth_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
        depth_control_rx,
    );
    let aggtrade_sink = event_sink.fork_logical_producer();
    let (aggtrade_control_tx, aggtrade_control_rx) =
        mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let aggtrade_source = run_binance_ws_source(
        binance_aggtrade_recovery,
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url: BINANCE_AGGTRADE_WS.to_owned(),
            rest_endpoint: BINANCE_AGGTRADE_REST.to_owned(),
            freshness_timeout_secs: 60,
        },
        aggtrade_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
        aggtrade_control_rx,
    );
    let mut handles: Vec<SourceTaskHandle> = vec![
        (
            "polymarket_market",
            tokio::spawn(run_logical_producer_with_day_barriers(
                "polymarket",
                polymarket_sink,
                run_started,
                initial_next_boundary_ms,
                polymarket_source,
            )),
        ),
        (
            "polymarket_rtds_chainlink_btc_usd",
            tokio::spawn(run_logical_producer_with_day_barriers(
                "polymarket_rtds",
                rtds_sink,
                run_started,
                initial_next_boundary_ms,
                rtds_source,
            )),
        ),
        (
            "binance_depth_update",
            tokio::spawn(run_depth_logical_producer_with_day_barriers(
                depth_sink,
                depth_control_tx.clone(),
                run_started,
                initial_next_boundary_ms,
                depth_source,
            )),
        ),
        (
            "binance_aggtrade",
            tokio::spawn(run_aggtrade_logical_producer_with_day_barriers(
                aggtrade_sink,
                aggtrade_control_tx.clone(),
                run_started,
                initial_next_boundary_ms,
                aggtrade_source,
            )),
        ),
    ];
    drop(event_sink);

    let mut coverage = BTreeMap::<String, CoverageStats>::new();
    let mut batches = LIVE_SOURCES
        .iter()
        .map(|source| {
            (
                (*source).to_owned(),
                Vec::<InflightRawEvent>::with_capacity(DEFAULT_FLUSH_ROWS),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source_queue_tracker = QueueDurationTracker::new("source_event_queue");
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut next_flush = run_started + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
    let mut next_queue_sample = run_started;
    let mut next_capacity_scan =
        run_started + Duration::from_secs(DATASET_CAPACITY_SCAN_INTERVAL_SECS);
    let mut average_event_bytes = 512_u64;
    let mut received_rows = 0_u64;
    let mut controlled_shutdown = false;
    let mut operator_shutdown = false;
    let mut capacity_shutdown = false;
    let mut runtime_failure = None::<String>;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    'runtime: while !deadline_reached(deadline) {
        tokio::select! {
            () = &mut shutdown => {
                controlled_shutdown = true;
                operator_shutdown = true;
                break;
            }
            received = timeout(Duration::from_millis(250), event_rx.recv()) => {
                match received {
                    Ok(Some(inflight_event)) => {
                        if let Err(error) = process_ingress_event(
                            inflight_event,
                            &mut writer_handles,
                            &mut batches,
                            &mut coverage,
                            &mut received_rows,
                            &mut average_event_bytes,
                            &source_gaps,
                            &source_queue_backpressure,
                            &mut writer_queue_tracker,
                            run_started,
                            &manifest_tx,
                        )
                        .await
                        {
                            let message = format!("ingress_processing_failed:{error}");
                            record_gap(&source_gaps, message.clone());
                            runtime_failure = Some(message);
                            break 'runtime;
                        }
                    }
                    Ok(None) if deadline_reached(deadline) => break,
                    Ok(None) => {
                        let message = "source_event_queue_closed_unexpectedly".to_owned();
                        record_gap(&source_gaps, message.clone());
                        runtime_failure = Some(message);
                        break;
                    }
                    Err(_) => {}
                }
            }
        }

        if let Some(source_kind) = finished_source_task(&handles)
            && !deadline_reached(deadline)
        {
            let message = format!("source_task_ended_unexpectedly:{source_kind}");
            record_gap(&source_gaps, message.clone());
            runtime_failure = Some(message);
            break;
        }

        if let Some(message) = poll_writer_completion(&mut writer_handles) {
            record_gap(&source_gaps, message.clone());
            runtime_failure = Some(message);
            break;
        }
        if manifest_owner_handle.is_finished() {
            let message = "daily_manifest_owner_ended_unexpectedly".to_owned();
            record_gap(&source_gaps, message.clone());
            runtime_failure = Some(message);
            break;
        }

        if Instant::now() >= next_flush {
            if let Err(error) = flush_all_batches(
                &mut writer_handles,
                &mut batches,
                &source_gaps,
                &source_queue_backpressure,
                &mut writer_queue_tracker,
                run_started,
            )
            .await
            {
                let message = format!("writer_periodic_flush_failed:{error}");
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
                break;
            }
            next_flush += Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
        }
        if Instant::now() >= next_queue_sample {
            source_queue_backpressure.apply_to_tracker(&mut source_queue_tracker);
            let writer_queue_rows = writer_handles
                .values()
                .map(|handle| handle.queue_rows.load(Ordering::Relaxed))
                .sum::<u64>();
            let source_queue_rows = event_rx.len() as u64;
            let source_queue_bytes = source_queue_rows.saturating_mul(average_event_bytes);
            let writer_queue_bytes = writer_queue_rows.saturating_mul(average_event_bytes);
            source_queue_tracker.observe(
                monotonic_ms(run_started),
                source_queue_rows,
                source_queue_bytes,
            );
            writer_queue_tracker.observe(
                monotonic_ms(run_started),
                writer_queue_rows,
                writer_queue_bytes,
            );
            next_queue_sample += Duration::from_secs(1);
        }
        if Instant::now() >= next_capacity_scan {
            let scan_root = dataset_root.clone();
            let capacity_scan =
                tokio::task::spawn_blocking(move || training_dataset_size_bytes(&scan_root));
            tokio::pin!(capacity_scan);
            tokio::select! {
                () = &mut shutdown => {
                    controlled_shutdown = true;
                    operator_shutdown = true;
                    break;
                }
                result = &mut capacity_scan => {
                    match result {
                        Ok(Ok(bytes)) if bytes >= config.max_dataset_bytes => {
                            record_gap(&source_gaps, format!(
                                "dataset_capacity_reached:bytes={bytes}:limit={}",
                                config.max_dataset_bytes
                            ));
                            controlled_shutdown = true;
                            capacity_shutdown = true;
                            break;
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            let message = format!(
                                "operator_intervention_required:dataset_size_scan_failed:{error}"
                            );
                            record_gap(&source_gaps, message.clone());
                            runtime_failure = Some(message);
                            controlled_shutdown = true;
                            break;
                        }
                        Err(error) => {
                            let message = format!(
                                "operator_intervention_required:dataset_size_scan_join_failed:{error}"
                            );
                            record_gap(&source_gaps, message.clone());
                            runtime_failure = Some(message);
                            controlled_shutdown = true;
                            break;
                        }
                    }
                }
            }
            next_capacity_scan += Duration::from_secs(DATASET_CAPACITY_SCAN_INTERVAL_SECS);
        }
    }

    let coordinated_shutdown_deadline = Instant::now() + COORDINATED_SHUTDOWN_DEADLINE;
    let aborting_source_tasks = controlled_shutdown || runtime_failure.is_some();
    let mut depth_stop_reply = None;
    let mut aggtrade_stop_reply = None;
    if aborting_source_tasks {
        if handles.iter().any(|(source_kind, handle)| {
            *source_kind == "binance_depth_update" && !handle.is_finished()
        }) {
            let (reply_tx, reply_rx) = oneshot::channel();
            match timeout(
                Duration::from_secs(5),
                depth_control_tx.send(DepthSourceControl::Fence {
                    stop_after_fence: true,
                    resume_after_barrier: None,
                    reply: reply_tx,
                }),
            )
            .await
            {
                Ok(Ok(())) => depth_stop_reply = Some(reply_rx),
                Ok(Err(_)) => {
                    let message = "depth_shutdown_control_channel_closed".to_owned();
                    record_gap(&source_gaps, message.clone());
                    runtime_failure = Some(message);
                }
                Err(_) => {
                    let message = "depth_shutdown_control_timeout".to_owned();
                    record_gap(&source_gaps, message.clone());
                    runtime_failure = Some(message);
                }
            }
        }
        if handles.iter().any(|(source_kind, handle)| {
            *source_kind == "binance_aggtrade" && !handle.is_finished()
        }) {
            let stop_reason = if operator_shutdown {
                "operator_shutdown"
            } else if capacity_shutdown {
                "dataset_capacity_reached"
            } else {
                "runtime_failure"
            };
            let stop_utc_day = utc_day_string_from_ms(now_ms())
                .ok_or_else(|| invalid_data("UTC day unavailable for aggTrade shutdown"))?;
            let (reply_tx, reply_rx) = oneshot::channel();
            match timeout(
                Duration::from_secs(5),
                aggtrade_control_tx.send(AggTradeSourceControl::Flush {
                    reason: stop_reason.to_owned(),
                    utc_day: stop_utc_day,
                    stop_after_flush: true,
                    resume_after_barrier: None,
                    reply: reply_tx,
                }),
            )
            .await
            {
                Ok(Ok(())) => aggtrade_stop_reply = Some(reply_rx),
                Ok(Err(_)) => {
                    let message = "aggTrade_shutdown_control_channel_closed".to_owned();
                    record_gap(&source_gaps, message.clone());
                    runtime_failure = Some(message);
                }
                Err(_) => {
                    let message = "aggTrade_shutdown_control_timeout".to_owned();
                    record_gap(&source_gaps, message.clone());
                    runtime_failure = Some(message);
                }
            }
        }
        for (source_kind, handle) in &handles {
            if *source_kind != "binance_depth_update" && *source_kind != "binance_aggtrade" {
                handle.abort();
            }
        }
    }

    while !handles.is_empty() {
        let mut index = 0;
        while index < handles.len() {
            if !handles[index].1.is_finished() {
                index += 1;
                continue;
            }
            let (source_kind, handle) = handles.swap_remove(index);
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    let message = format!("source_task_error:{source_kind}:{err}");
                    runtime_failure = Some(message.clone());
                    record_gap(&source_gaps, message);
                }
                Err(err) if err.is_cancelled() && aborting_source_tasks => {}
                Err(err) => {
                    let message = format!("source_task_join_error:{source_kind}:{err}");
                    runtime_failure = Some(message.clone());
                    record_gap(&source_gaps, message);
                }
            }
        }
        if handles.is_empty() {
            break;
        }
        let remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            for (source_kind, handle) in handles.drain(..) {
                let message = format!("source_task_join_timeout:{source_kind}");
                runtime_failure = Some(message.clone());
                record_gap(&source_gaps, message);
                handle.abort();
                let _ = handle.await;
            }
            break;
        }
        if let Ok(Some(inflight_event)) =
            timeout(remaining.min(Duration::from_millis(100)), event_rx.recv()).await
            && let Err(error) = process_ingress_event(
                inflight_event,
                &mut writer_handles,
                &mut batches,
                &mut coverage,
                &mut received_rows,
                &mut average_event_bytes,
                &source_gaps,
                &source_queue_backpressure,
                &mut writer_queue_tracker,
                run_started,
                &manifest_tx,
            )
            .await
        {
            let message = format!("shutdown_ingress_processing_failed:{error}");
            record_gap(&source_gaps, message.clone());
            runtime_failure = Some(message);
        }
        if Instant::now() >= next_flush {
            if let Err(error) = flush_all_batches(
                &mut writer_handles,
                &mut batches,
                &source_gaps,
                &source_queue_backpressure,
                &mut writer_queue_tracker,
                run_started,
            )
            .await
            {
                let message = format!("shutdown_periodic_flush_failed:{error}");
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
            next_flush = Instant::now() + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
        }
    }

    if let Some(reply_rx) = depth_stop_reply {
        let remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, reply_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                let message = format!("depth_shutdown_fence_failed:{error}");
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
            Ok(Err(_)) => {
                let message = "depth_shutdown_fence_reply_dropped".to_owned();
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
            Err(_) => {
                let message = "depth_shutdown_fence_reply_timeout".to_owned();
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
        }
    }

    if let Some(reply_rx) = aggtrade_stop_reply {
        let remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, reply_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                let message = format!("aggTrade_shutdown_flush_failed:{error}");
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
            Ok(Err(_)) => {
                let message = "aggTrade_shutdown_flush_reply_dropped".to_owned();
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
            Err(_) => {
                let message = "aggTrade_shutdown_flush_reply_timeout".to_owned();
                record_gap(&source_gaps, message.clone());
                runtime_failure = Some(message);
            }
        }
    }

    while let Ok(inflight_event) = event_rx.try_recv() {
        if let Err(error) = process_ingress_event(
            inflight_event,
            &mut writer_handles,
            &mut batches,
            &mut coverage,
            &mut received_rows,
            &mut average_event_bytes,
            &source_gaps,
            &source_queue_backpressure,
            &mut writer_queue_tracker,
            run_started,
            &manifest_tx,
        )
        .await
        {
            let message = format!("shutdown_ingress_processing_failed:{error}");
            record_gap(&source_gaps, message.clone());
            runtime_failure = Some(message);
        }
    }
    let flush_remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
    let flush_result = if flush_remaining.is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "coordinated shutdown deadline reached before batch flush",
        )
        .into())
    } else {
        match timeout(
            flush_remaining,
            flush_all_batches(
                &mut writer_handles,
                &mut batches,
                &source_gaps,
                &source_queue_backpressure,
                &mut writer_queue_tracker,
                run_started,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "coordinated shutdown batch flush timeout",
            )
            .into()),
        }
    };
    if let Err(error) = flush_result {
        let message = format!("flush_batches_failed:{error}");
        record_gap(&source_gaps, message.clone());
        runtime_failure = Some(message);
    }

    let writer_remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
    let writer_outputs_result = stop_writer_workers_with_budget(
        writer_handles,
        writer_remaining.min(Duration::from_millis(WRITER_FINALIZE_BUDGET_MS)),
    )
    .await;
    let (writer_outputs, all_writers_terminal) =
        writer_outputs_or_record_failure(writer_outputs_result, &source_gaps, &mut runtime_failure);
    if !all_writers_terminal {
        manifest_owner_handle.abort();
        let _ = manifest_owner_handle.await;
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            runtime_failure.unwrap_or_else(|| "writer finalize deadline exceeded".to_owned()),
        )
        .into());
    }

    let finalized_segments = writer_outputs
        .iter()
        .flat_map(|output| output.finalized_segments.clone())
        .collect::<Vec<_>>();
    let manifest_shutdown_day = utc_day_string_from_ms(now_ms())
        .ok_or_else(|| invalid_data("current UTC day unavailable during manifest shutdown"))?;
    let gaps = source_gaps
        .lock()
        .expect("source gap mutex poisoned")
        .reasons_for_day(&manifest_shutdown_day);
    let (manifest_shutdown_tx, manifest_shutdown_rx) = oneshot::channel();
    let manifest_shutdown_command = ManifestCommand::Shutdown {
        utc_day: manifest_shutdown_day,
        gaps: gaps.clone(),
        operational_counters: source_queue_backpressure.drain_all_operational(),
        reply: manifest_shutdown_tx,
    };
    let manifest_shutdown_result = match timeout(
        coordinated_shutdown_deadline.saturating_duration_since(Instant::now()),
        manifest_tx.send(manifest_shutdown_command),
    )
    .await
    {
        Ok(Ok(())) => match timeout(
            coordinated_shutdown_deadline.saturating_duration_since(Instant::now()),
            manifest_shutdown_rx,
        )
        .await
        {
            Ok(Ok(Ok(()))) => match timeout(
                coordinated_shutdown_deadline.saturating_duration_since(Instant::now()),
                &mut manifest_owner_handle,
            )
            .await
            {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(io::Error::other(format!(
                    "daily manifest owner failed: {error}"
                ))),
                Ok(Err(error)) => Err(io::Error::other(format!(
                    "daily manifest owner join failed: {error}"
                ))),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "daily manifest owner join timeout",
                )),
            },
            Ok(Ok(Err(error))) => Err(io::Error::other(error)),
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "daily manifest owner dropped shutdown reply",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "daily manifest owner shutdown timeout",
            )),
        },
        Ok(Err(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "daily manifest owner command channel closed",
        )),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "daily manifest owner command timeout",
        )),
    };
    if let Err(error) = manifest_shutdown_result {
        let message = format!("daily_manifest_shutdown_failed:{error}");
        record_gap(&source_gaps, message.clone());
        runtime_failure = Some(message);
    }

    let source_coverage = coverage
        .iter()
        .filter_map(|(source_kind, stats)| stats.to_source_coverage(source_kind))
        .collect::<Vec<_>>();
    source_queue_backpressure.apply_to_tracker(&mut source_queue_tracker);
    let _ = source_queue_tracker.finish(monotonic_ms(run_started));
    let _ = writer_queue_tracker.finish(monotonic_ms(run_started));
    if Instant::now() > coordinated_shutdown_deadline {
        let message = "coordinated_shutdown_timeout".to_owned();
        record_gap(&source_gaps, message.clone());
        runtime_failure = Some(message);
    }
    if capacity_shutdown && runtime_failure.is_none() {
        eprintln!("dataset_capacity_reached");
        return Err(io::Error::other("dataset_capacity_reached").into());
    }
    if operator_shutdown && runtime_failure.is_none() {
        eprintln!("coordinated_shutdown_complete");
    }
    match runtime_failure {
        Some(error) => Err(io::Error::other(error).into()),
        None => Ok(LiveTrainingRuntimeReceipt {
            finalized_segments,
            source_coverage,
            source_gaps: source_gaps
                .lock()
                .expect("source gap mutex poisoned")
                .all_reasons(),
        }),
    }
}

fn start_writer_workers(
    dataset_root: &Path,
    provenance: &RuntimeProvenance,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &Arc<SourceQueueBackpressureStats>,
) -> Result<BTreeMap<String, SourceWriterHandle>> {
    let mut handles = BTreeMap::new();
    for source_kind in LIVE_SOURCES {
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let accepting_commands = Arc::new(Mutex::new(true));
        let accepting_commands_for_worker = Arc::clone(&accepting_commands);
        let runtime_stats = Arc::clone(runtime_stats);
        let dataset_root = dataset_root.to_path_buf();
        let provenance = provenance.clone();
        let manifest_tx = manifest_tx.clone();
        let source_kind_owned = (*source_kind).to_owned();
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(format!("data-collect-writer-{source_kind_owned}"))
            .spawn(move || {
                let result = source_writer_worker(
                    dataset_root,
                    provenance,
                    source_kind_owned,
                    rx,
                    queue_rows_for_worker,
                    accepting_commands_for_worker,
                    runtime_stats,
                    manifest_tx,
                );
                let _ = result_tx.send(result);
            })
            .map_err(|err| io::Error::other(format!("spawn writer thread failed: {err}")))?;
        handles.insert(
            (*source_kind).to_owned(),
            SourceWriterHandle {
                tx,
                queue_rows,
                accepting_commands,
                result_rx,
                completed_result: None,
            },
        );
    }
    Ok(handles)
}

fn poll_writer_completion(writers: &mut BTreeMap<String, SourceWriterHandle>) -> Option<String> {
    for (source_kind, writer) in writers {
        if writer.completed_result.is_none() {
            match writer.result_rx.try_recv() {
                Ok(result) => writer.completed_result = Some(result),
                Err(std::sync::mpsc::TryRecvError::Empty) => continue,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    let message = format!("writer_result_channel_disconnected:{source_kind}");
                    writer.completed_result = Some(Err(io::Error::other(message.clone()).into()));
                    return Some(message);
                }
            }
        }
        if let Some(result) = writer.completed_result.as_ref() {
            return Some(match result {
                Ok(_) => format!("writer_ended_unexpectedly:{source_kind}"),
                Err(error) => format!("writer_failed:{source_kind}:{error}"),
            });
        }
    }
    None
}

#[cfg(test)]
fn start_writer_workers_with_hung_result(
    dataset_root: &Path,
    provenance: &RuntimeProvenance,
) -> Result<BTreeMap<String, SourceWriterHandle>> {
    let _ = (dataset_root, provenance);
    let mut handles = BTreeMap::new();
    let (tx, _command_rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("data-collect-writer-hung".to_owned())
        .spawn(move || {
            let _result_tx = result_tx;
            std::thread::sleep(Duration::from_secs(60));
        })
        .map_err(|err| io::Error::other(format!("spawn hung writer thread failed: {err}")))?;
    handles.insert(
        "hung_writer".to_owned(),
        SourceWriterHandle {
            tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
        },
    );
    Ok(handles)
}

async fn flush_all_batches(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut BTreeMap<String, Vec<InflightRawEvent>>,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    let sources = batches.keys().cloned().collect::<Vec<_>>();
    for source_kind in sources {
        let Some(batch) = batches.get_mut(&source_kind) else {
            continue;
        };
        flush_source_batch(
            writers,
            &source_kind,
            batch,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_ingress_control(
    control: IngressControl,
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut BTreeMap<String, Vec<InflightRawEvent>>,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
) -> Result<()> {
    match control {
        IngressControl::DayBarrier { producer, utc_day } => {
            flush_all_batches(
                writers,
                batches,
                gaps,
                runtime_stats,
                writer_queue_tracker,
                run_started,
            )
            .await?;
            send_writer_day_finalize(writers, &producer, &utc_day).await?;
            let gap_snapshot = gaps
                .lock()
                .expect("source gap mutex poisoned")
                .reasons_for_day(&utc_day);
            let operational_counters = runtime_stats.drain_operational_day(&utc_day);
            manifest_tx
                .send(ManifestCommand::ProducerBarrier {
                    utc_day,
                    producer,
                    gaps: gap_snapshot,
                    operational_counters,
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "daily manifest owner channel closed during producer barrier",
                    )
                })?;
        }
        IngressControl::BinanceContinuity {
            utc_day,
            continuity,
        } => {
            manifest_tx
                .send(ManifestCommand::BinanceContinuity {
                    utc_day,
                    continuity,
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "daily manifest owner channel closed during continuity update",
                    )
                })?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_ingress_event(
    mut inflight_event: InflightRawEvent,
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut BTreeMap<String, Vec<InflightRawEvent>>,
    coverage: &mut BTreeMap<String, CoverageStats>,
    received_rows: &mut u64,
    average_event_bytes: &mut u64,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
) -> Result<()> {
    if let Some(control) = inflight_event.control.take() {
        drop(inflight_event);
        return handle_ingress_control(
            control,
            writers,
            batches,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
            manifest_tx,
        )
        .await;
    }
    *received_rows = received_rows.saturating_add(1);
    let event = &inflight_event.event;
    update_coverage(coverage, event);
    *average_event_bytes = rolling_average_bytes(
        *average_event_bytes,
        estimated_event_bytes(event),
        *received_rows,
    );
    let source_kind = event.source_kind.clone();
    let batch = batches.entry(source_kind.clone()).or_default();
    batch.push(inflight_event);
    if batch.len() >= DEFAULT_FLUSH_ROWS {
        flush_source_batch(
            writers,
            &source_kind,
            batch,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
        )
        .await?;
    }
    Ok(())
}

async fn flush_source_batch(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    source_kind: &str,
    batch: &mut Vec<InflightRawEvent>,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = batch.len() as u64;
    let events = std::mem::take(batch);
    let drop_facts = events
        .iter()
        .map(|event| {
            (
                event.event.source_kind.clone(),
                event.event.recv_time_utc_ms,
            )
        })
        .collect::<Vec<_>>();
    let Some(writer) = writers.get(source_kind) else {
        writer_queue_tracker.record_drop(rows);
        record_writer_drop_facts(runtime_stats, &drop_facts);
        let gap = format!("writer_missing_for_source:{source_kind}");
        record_gap(gaps, gap.clone());
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
    };
    let mut command = WriterCommand::Events(events);
    writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
    match try_send_writer_command(writer, command) {
        Ok(()) => {
            writer_queue_tracker.observe(
                monotonic_ms(run_started),
                writers
                    .values()
                    .map(|handle| handle.queue_rows.load(Ordering::Relaxed))
                    .sum::<u64>(),
                0,
            );
            return Ok(());
        }
        Err(std::sync::mpsc::TrySendError::Full(returned)) => {
            writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
            writer_queue_tracker.record_backpressure_wait();
            command = returned;
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
            writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, &drop_facts);
            let gap = format!("writer_queue_disconnected:{source_kind}");
            record_gap(gaps, gap.clone());
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
        }
    }
    match timeout(BUFFER_ACQUIRE_TIMEOUT, async {
        loop {
            writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
            match try_send_writer_command(writer, command) {
                Ok(()) => return None,
                Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                    writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
                    command = returned;
                    sleep(Duration::from_millis(50)).await;
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
                    return Some(format!("writer_queue_disconnected:{source_kind}"));
                }
            }
        }
    })
    .await
    {
        Ok(Some(gap)) => {
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, &drop_facts);
            record_gap(gaps, gap.clone());
            Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into())
        }
        Ok(None) => Ok(()),
        Err(_) => {
            writer_queue_tracker.record_backpressure_timeout();
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, &drop_facts);
            let gap = format!("writer_queue_backpressure_timeout:{source_kind}");
            record_gap(gaps, gap.clone());
            Err(io::Error::new(io::ErrorKind::TimedOut, gap).into())
        }
    }
}

fn try_send_writer_command(
    writer: &SourceWriterHandle,
    command: WriterCommand,
) -> std::result::Result<(), std::sync::mpsc::TrySendError<WriterCommand>> {
    let accepting = writer
        .accepting_commands
        .lock()
        .expect("writer accepting-command mutex poisoned");
    if !*accepting {
        return Err(std::sync::mpsc::TrySendError::Disconnected(command));
    }
    writer.tx.try_send(command)
}

fn record_writer_drop_facts(
    runtime_stats: &SourceQueueBackpressureStats,
    drop_facts: &[(String, i64)],
) {
    runtime_stats.record_drop(drop_facts.len() as u64);
    for (source_kind, recv_time_utc_ms) in drop_facts {
        runtime_stats.record_operational_drop(source_kind, *recv_time_utc_ms, 1);
    }
}

fn producer_writer_sources(producer: &str) -> Result<&'static [&'static str]> {
    match producer {
        "polymarket" => Ok(&["polymarket_market", "polymarket_market_metadata"]),
        "polymarket_rtds" => Ok(&["polymarket_rtds_chainlink_btc_usd"]),
        "binance_depth" => Ok(&["binance_depth_snapshot", "binance_depth_update"]),
        "binance_aggtrade" => Ok(&["binance_aggtrade"]),
        other => Err(invalid_data(format!("unknown logical producer: {other}")).into()),
    }
}

async fn send_writer_day_finalize(
    writers: &BTreeMap<String, SourceWriterHandle>,
    producer: &str,
    utc_day: &str,
) -> Result<()> {
    for source_kind in producer_writer_sources(producer)? {
        let writer = writers.get(*source_kind).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("writer missing for day barrier: {source_kind}"),
            )
        })?;
        let mut command = WriterCommand::FinalizeDay {
            utc_day: utc_day.to_owned(),
        };
        timeout(BUFFER_ACQUIRE_TIMEOUT, async {
            loop {
                match try_send_writer_command(writer, command) {
                    Ok(()) => return Ok(()),
                    Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                        command = returned;
                        sleep(Duration::from_millis(50)).await;
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            format!("writer queue disconnected during day barrier: {source_kind}"),
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("writer day barrier timeout: {source_kind}"),
            )
        })??;
    }
    Ok(())
}

async fn stop_writer_workers_with_budget(
    writers: BTreeMap<String, SourceWriterHandle>,
    budget: Duration,
) -> WriterStopOutcome {
    let mut receivers = Vec::new();
    let mut outputs = Vec::new();
    let mut errors = Vec::new();
    for (source_kind, writer) in writers {
        let SourceWriterHandle {
            tx,
            queue_rows: _,
            accepting_commands: _,
            result_rx,
            completed_result,
        } = writer;
        drop(tx);
        match completed_result {
            Some(Ok(output)) => outputs.push(output),
            Some(Err(error)) => {
                errors.push(format!("{source_kind} writer finalize failed: {error}"))
            }
            None => receivers.push((source_kind, result_rx)),
        }
    }
    let deadline = Instant::now() + budget;
    let mut all_terminal = true;
    for (source_kind, result_rx) in receivers {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            match result_rx.try_recv() {
                Ok(Ok(output)) => outputs.push(output),
                Ok(Err(error)) => {
                    errors.push(format!("{source_kind} writer finalize failed: {error}"))
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    all_terminal = false;
                    errors.push(format!(
                        "writer finalize timeout before {source_kind} completed"
                    ));
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    errors.push(format!("{source_kind} writer result channel disconnected"))
                }
            }
            continue;
        }
        match result_rx.recv_timeout(remaining) {
            Ok(Ok(output)) => outputs.push(output),
            Ok(Err(error)) => errors.push(format!("{source_kind} writer finalize failed: {error}")),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                all_terminal = false;
                errors.push(format!("writer finalize timeout for {source_kind}"));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                errors.push(format!("{source_kind} writer result channel disconnected"))
            }
        }
    }
    WriterStopOutcome {
        outputs,
        errors,
        all_terminal,
    }
}

fn writer_outputs_or_record_failure(
    outcome: WriterStopOutcome,
    source_gaps: &SharedSourceGaps,
    runtime_failure: &mut Option<String>,
) -> (Vec<SourceWriterOutput>, bool) {
    if !outcome.errors.is_empty() {
        let message = format!("writer_finalize_failed:{}", outcome.errors.join(" | "));
        record_gap(source_gaps, message.clone());
        *runtime_failure = Some(message);
    }
    (outcome.outputs, outcome.all_terminal)
}

#[allow(clippy::too_many_arguments)]
fn source_writer_worker(
    dataset_root: PathBuf,
    provenance: RuntimeProvenance,
    source_kind: String,
    rx: std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    accepting_commands: Arc<Mutex<bool>>,
    runtime_stats: Arc<SourceQueueBackpressureStats>,
    manifest_tx: mpsc::Sender<ManifestCommand>,
) -> Result<SourceWriterOutput> {
    let mut active = None::<ActiveSegment>;
    let mut finalized_segments = Vec::new();
    let mut stats = SourceWriterStatsBuilder::new(source_kind.clone());
    let partition_policy = HourlyPartitionPolicy::new(&dataset_root)?;
    while let Ok(command) = rx.recv() {
        match command {
            WriterCommand::Events(events) => {
                queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                let mut events = events.into_iter();
                while let Some(inflight_event) = events.next() {
                    let event = &inflight_event.event;
                    let process_result = (|| -> Result<()> {
                        let current_partition = partition_policy.partition(
                            &source_kind,
                            event.recv_time_utc_ms,
                            None,
                        )?;
                        if let Some(reason) = active.as_ref().and_then(|segment| {
                            live_segment_rollover_reason(segment, &current_partition.utc_hour)
                        }) {
                            let segment = active.take().expect("active segment exists");
                            finalized_segments.push(finalize_segment_with_drop_accounting(
                                segment,
                                reason,
                                &provenance,
                                &manifest_tx,
                                &runtime_stats,
                            )?);
                        }
                        if active.is_none() {
                            let (naming, selected_hour_part) = next_available_hourly_partition(
                                &partition_policy,
                                &source_kind,
                                event.recv_time_utc_ms,
                                None,
                            )?;
                            active = Some(ActiveSegment::new(
                                naming,
                                &source_kind,
                                selected_hour_part,
                            )?);
                        }
                        let segment = active.as_mut().expect("active segment exists");
                        let write_started = Instant::now();
                        let written_rows = segment.writer.write_event(event)?;
                        let write_events_us = write_started.elapsed().as_micros() as u64;
                        segment.observe(event, written_rows);
                        stats.record_written_event(
                            event.recv_time_utc_ms,
                            written_rows,
                            write_events_us,
                        );
                        Ok(())
                    })();
                    if let Err(error) = process_result {
                        if let Some(segment) = active.as_ref() {
                            record_unfinalized_segment_drop(&runtime_stats, segment);
                        }
                        record_writer_drop_facts(
                            &runtime_stats,
                            &std::iter::once(inflight_event)
                                .chain(events)
                                .map(|event| {
                                    (event.event.source_kind, event.event.recv_time_utc_ms)
                                })
                                .collect::<Vec<_>>(),
                        );
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error);
                    }
                }
            }
            WriterCommand::FinalizeDay { utc_day } => {
                if active
                    .as_ref()
                    .is_some_and(|segment| segment.utc_day.as_str() <= utc_day.as_str())
                {
                    if active
                        .as_ref()
                        .is_some_and(|segment| segment.utc_day != utc_day)
                    {
                        let error = invalid_data(format!(
                            "writer day barrier skipped UTC day: source={source_kind} active={} barrier={utc_day}",
                            active.as_ref().expect("active segment exists").utc_day
                        ));
                        record_unfinalized_segment_drop(
                            &runtime_stats,
                            active.as_ref().expect("active segment exists"),
                        );
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    let segment = active.take().expect("active segment exists");
                    let finalized = finalize_segment_with_drop_accounting(
                        segment,
                        "utc_day_barrier",
                        &provenance,
                        &manifest_tx,
                        &runtime_stats,
                    );
                    match finalized {
                        Ok(finalized) => finalized_segments.push(finalized),
                        Err(error) => {
                            stop_accepting_and_record_queued_drops(
                                &accepting_commands,
                                &rx,
                                &queue_rows,
                                &runtime_stats,
                            );
                            return Err(error);
                        }
                    }
                }
                if let Err(error) = manifest_tx
                    .blocking_send(ManifestCommand::WriterDayFinalized {
                        utc_day,
                        source_kind: source_kind.clone(),
                    })
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "daily manifest owner channel closed during writer day finalize",
                        )
                    })
                {
                    stop_accepting_and_record_queued_drops(
                        &accepting_commands,
                        &rx,
                        &queue_rows,
                        &runtime_stats,
                    );
                    return Err(error.into());
                }
            }
        }
    }
    if let Some(segment) = active.take() {
        finalized_segments.push(finalize_segment_with_drop_accounting(
            segment,
            "shutdown_finalize",
            &provenance,
            &manifest_tx,
            &runtime_stats,
        )?);
    }
    let stats = stats.finish(&finalized_segments);
    Ok(SourceWriterOutput {
        source_kind,
        finalized_segments,
        stats,
    })
}

fn finalize_segment_with_drop_accounting(
    segment: ActiveSegment,
    rollover_reason: &str,
    provenance: &RuntimeProvenance,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &SourceQueueBackpressureStats,
) -> Result<LiveFinalizedSegment> {
    let final_path = segment.writer.final_path().to_path_buf();
    let source_kind = segment.source_kind.clone();
    let recv_time_utc_ms = segment.segment_start_recv_time_utc_ms;
    let rows = segment.rows;
    match finalize_segment(segment, rollover_reason, provenance, manifest_tx) {
        Ok(finalized) => Ok(finalized),
        Err(error) => {
            if rows > 0 && !final_path.exists() {
                runtime_stats.record_drop(rows);
                runtime_stats.record_operational_drop(&source_kind, recv_time_utc_ms, rows);
            }
            Err(error)
        }
    }
}

fn record_unfinalized_segment_drop(
    runtime_stats: &SourceQueueBackpressureStats,
    segment: &ActiveSegment,
) {
    if segment.rows == 0 || segment.writer.final_path().exists() {
        return;
    }
    runtime_stats.record_drop(segment.rows);
    runtime_stats.record_operational_drop(
        &segment.source_kind,
        segment.segment_start_recv_time_utc_ms,
        segment.rows,
    );
}

fn stop_accepting_and_record_queued_drops(
    accepting_commands: &Mutex<bool>,
    rx: &std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: &AtomicU64,
    runtime_stats: &SourceQueueBackpressureStats,
) {
    let mut accepting = accepting_commands
        .lock()
        .expect("writer accepting-command mutex poisoned");
    *accepting = false;
    let mut drop_facts = Vec::new();
    while let Ok(command) = rx.try_recv() {
        if let WriterCommand::Events(events) = command {
            queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
            drop_facts.extend(
                events
                    .into_iter()
                    .map(|event| (event.event.source_kind, event.event.recv_time_utc_ms)),
            );
        }
    }
    drop(accepting);
    record_writer_drop_facts(runtime_stats, &drop_facts);
}

fn finalize_segment(
    mut segment: ActiveSegment,
    rollover_reason: &str,
    provenance: &RuntimeProvenance,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
) -> Result<LiveFinalizedSegment> {
    let source_kind = segment.source_kind.clone();
    let manifest_context = SegmentManifestContext::from_segment(&segment);
    let facts = segment_daily_facts(&manifest_context, rollover_reason, provenance);
    let recovery_metadata = DailyFileRecoveryMetadata {
        source_kind: source_kind.clone(),
        utc_hour: manifest_context.utc_hour.clone(),
        part: manifest_context.hour_part,
        rows: manifest_context.rows,
        schema_version: typed_schema_version(&source_kind).to_owned(),
        first_recv_time_utc_ms: manifest_context.segment_start_recv_time_utc_ms,
        last_recv_time_utc_ms: manifest_context.last_recv_time_utc_ms,
        source_timestamp_start: manifest_context.source_ts_start.clone(),
        source_timestamp_end: manifest_context.source_ts_end.clone(),
        event_type_counts: manifest_context.event_type_counts.clone(),
        facts: facts.clone(),
    };
    segment.writer.append_key_value_metadata(
        DAILY_FILE_RECOVERY_METADATA_KEY,
        serde_json::to_string(&recovery_metadata)?,
    )?;
    let published = segment.writer.finalize()?;
    if published.row_count != recovery_metadata.rows
        || published.schema_version != recovery_metadata.schema_version
    {
        return Err(invalid_data(format!(
            "finalized Parquet recovery metadata mismatch: source={source_kind} rows={} metadata_rows={} schema={} metadata_schema={}",
            published.row_count,
            recovery_metadata.rows,
            published.schema_version,
            recovery_metadata.schema_version,
        ))
        .into());
    }
    let bytes = fs::metadata(&published.final_path)?.len();
    let delta = DailyFinalizedFileDelta {
        utc_day: manifest_context.utc_day.clone(),
        source_kind: source_kind.clone(),
        utc_hour: manifest_context.utc_hour.clone(),
        part: manifest_context.hour_part,
        final_path: published.final_path.clone(),
        rows: published.row_count,
        bytes,
        sha256: published.sha256.clone(),
        schema_version: published.schema_version.clone(),
        first_recv_time_utc_ms: manifest_context.segment_start_recv_time_utc_ms,
        last_recv_time_utc_ms: manifest_context.last_recv_time_utc_ms,
        source_timestamp_start: manifest_context.source_ts_start.clone(),
        source_timestamp_end: manifest_context.source_ts_end.clone(),
        event_type_counts: manifest_context.event_type_counts.clone(),
        facts,
    };
    manifest_tx
        .blocking_send(ManifestCommand::Finalized(delta))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "daily manifest owner channel closed during finalize",
            )
        })?;
    Ok(LiveFinalizedSegment {
        source_kind,
        parquet_path: published.final_path,
        row_count: published.row_count,
        sha256: published.sha256,
    })
}

fn next_available_hourly_partition(
    partition_policy: &HourlyPartitionPolicy,
    source_kind: &str,
    recv_time_utc_ms: i64,
    preferred_part: Option<u32>,
) -> Result<(HourlyPartition, u32)> {
    let mut part = preferred_part.unwrap_or_default();
    for _ in 0..10_000 {
        let partition_part = if part == 0 { None } else { Some(part) };
        let partition =
            partition_policy.partition(source_kind, recv_time_utc_ms, partition_part)?;
        if !partition.running_path.exists() && !partition.final_path.exists() {
            return Ok((partition, part));
        }
        part = part.saturating_add(1);
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("no available hourly part for source {source_kind}"),
    )
    .into())
}

#[derive(Clone, Debug)]
struct SegmentManifestContext {
    source_kind: String,
    utc_day: String,
    utc_hour: String,
    hour_start_utc_ms: i64,
    hour_end_utc_ms: i64,
    hour_part: u32,
    segment_start_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    event_type_counts: BTreeMap<String, u64>,
    backfill_rows: u64,
    book_rows_with_bids_asks: u64,
    covered_market_slugs: BTreeSet<String>,
    covered_condition_ids: BTreeSet<String>,
    covered_asset_ids: BTreeSet<String>,
    depth_snapshot_last_update_id: Option<u64>,
    depth_stitch_applied_rows: u64,
    depth_stitch_gap_rows: u64,
    depth_resnapshot_count: u64,
    depth_local_u_min: Option<u64>,
    depth_local_u_max: Option<u64>,
    depth_first_update_bridged_snapshot: bool,
    depth_source_gaps: BTreeSet<String>,
    metadata_complete_rows: u64,
    metadata_unavailable_rows: u64,
}

impl SegmentManifestContext {
    fn from_segment(segment: &ActiveSegment) -> Self {
        Self {
            source_kind: segment.source_kind.clone(),
            utc_day: segment.utc_day.clone(),
            utc_hour: segment.utc_hour.clone(),
            hour_start_utc_ms: segment.hour_start_utc_ms,
            hour_end_utc_ms: segment.hour_end_utc_ms,
            hour_part: segment.hour_part,
            segment_start_recv_time_utc_ms: segment.segment_start_recv_time_utc_ms,
            last_recv_time_utc_ms: segment.last_recv_time_utc_ms,
            source_ts_start: segment.source_ts_start.clone(),
            source_ts_end: segment.source_ts_end.clone(),
            rows: segment.rows,
            event_type_counts: segment.event_type_counts.clone(),
            backfill_rows: segment.backfill_rows,
            book_rows_with_bids_asks: segment.book_rows_with_bids_asks,
            covered_market_slugs: segment.covered_market_slugs.clone(),
            covered_condition_ids: segment.covered_condition_ids.clone(),
            covered_asset_ids: segment.covered_asset_ids.clone(),
            depth_snapshot_last_update_id: segment.depth_snapshot_last_update_id,
            depth_stitch_applied_rows: segment.depth_stitch_applied_rows,
            depth_stitch_gap_rows: segment.depth_stitch_gap_rows,
            depth_resnapshot_count: segment.depth_resnapshot_count,
            depth_local_u_min: segment.depth_local_u_min,
            depth_local_u_max: segment.depth_local_u_max,
            depth_first_update_bridged_snapshot: segment.depth_first_update_bridged_snapshot,
            depth_source_gaps: segment.depth_source_gaps.clone(),
            metadata_complete_rows: segment.metadata_complete_rows,
            metadata_unavailable_rows: segment.metadata_unavailable_rows,
        }
    }
}

fn segment_daily_facts(
    segment: &SegmentManifestContext,
    rollover_reason: &str,
    provenance: &RuntimeProvenance,
) -> Value {
    json!({
        "rollover_reason": rollover_reason,
        "hour_window_utc_ms": {
            "start": segment.hour_start_utc_ms,
            "end": segment.hour_end_utc_ms,
        },
        "layout_family": layout_family_for_source(&segment.source_kind),
        "sort_order": "receive_order",
        "compression": "zstd_level_3",
        "backfill_rows": segment.backfill_rows,
        "book_rows_with_bids_asks": segment.book_rows_with_bids_asks,
        "covered_market_slugs": segment.covered_market_slugs,
        "covered_condition_ids": segment.covered_condition_ids,
        "covered_asset_ids": segment.covered_asset_ids,
        "binance_depth_stitch": {
            "snapshot_last_update_id": segment.depth_snapshot_last_update_id,
            "stitch_applied_rows": segment.depth_stitch_applied_rows,
            "gap_rows": segment.depth_stitch_gap_rows,
            "resnapshot_count": segment.depth_resnapshot_count,
            "local_u_min": segment.depth_local_u_min,
            "local_u_max": segment.depth_local_u_max,
            "first_update_bridged_snapshot": segment.depth_first_update_bridged_snapshot,
        },
        "source_gaps": segment.depth_source_gaps,
        "metadata_complete_rows": segment.metadata_complete_rows,
        "metadata_unavailable_rows": segment.metadata_unavailable_rows,
        "code_version": provenance.code_version,
        "collector_binary_sha256": provenance.collector_binary_sha256,
        "source_config_digest": provenance.source_config_digest,
        "collector_host_id": provenance.collector_host_id,
        "collector_region": provenance.collector_region,
    })
}

fn layout_family_for_source(source_kind: &str) -> &'static str {
    match source_kind {
        "polymarket_market" => "pmxt_inspired_polymarket_orderbook",
        "polymarket_market_metadata" => "pmxt_inspired_polymarket_market_metadata",
        _ => "source_specific_hourly_typed_raw",
    }
}

async fn run_polymarket_source(
    dataset_root: PathBuf,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline_ms: Option<i64>,
    discovery_wait_secs: u64,
    run_started: Instant,
) -> Result<()> {
    let gamma = GammaClient::default();
    let deadline = deadline_ms.map(|deadline_ms| {
        Instant::now()
            + Duration::from_millis(
                u64::try_from(deadline_ms.saturating_sub(now_ms())).unwrap_or(0),
            )
    });
    let mut recovery = SourceRecoverySupervisor::new(
        "polymarket_market",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    let mut market_runtime = None::<MarketSdkRuntime>;
    let mut dedupe = LifecycleFingerprintSets::with_limit(POLYMARKET_FINGERPRINTS_PER_LIFECYCLE);
    let mut dedupe_restored = false;
    while deadline_ms.is_none_or(|deadline_ms| now_ms() < deadline_ms) {
        let discovery = match discover_markets_with_wait(&gamma, discovery_wait_secs).await {
            Ok(discovery) => discovery,
            Err(err) => {
                record_gap(&sink.gaps, format!("polymarket_discovery_source_gap:{err}"));
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
        };
        if !dedupe_restored {
            let dataset_root = dataset_root.clone();
            let lifecycles = expected_lifecycle_slugs(&discovery);
            dedupe = tokio::task::spawn_blocking(move || -> Result<LifecycleFingerprintSets> {
                let mut dedupe =
                    LifecycleFingerprintSets::with_limit(POLYMARKET_FINGERPRINTS_PER_LIFECYCLE);
                dedupe
                    .replace_lifecycles(lifecycles)
                    .map_err(operator_dedupe_error)?;
                restore_polymarket_dedupe(&dataset_root, &mut dedupe)?;
                Ok(dedupe)
            })
            .await
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:Polymarket restore join failed:{error}"
                ))
            })?
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:Polymarket restore failed:{error}"
                ))
            })?;
            dedupe_restored = true;
        } else {
            dedupe
                .replace_lifecycles(expected_lifecycle_slugs(&discovery))
                .map_err(operator_dedupe_error)?;
        }
        if market_runtime.is_none() {
            market_runtime = Some(MarketSdkRuntime::new(
                format!("{MARKET_WS_ENDPOINT}/ws/market"),
                bounded_ws_config(),
            )?);
        }
        let current_end_ms = discovery.current.end_ts_s * 1000 + 5_000;
        let segment_end_ms = deadline_ms
            .map(|deadline_ms| current_end_ms.min(deadline_ms))
            .unwrap_or(current_end_ms);
        let collected = collect_polymarket_single_receiver(
            discovery,
            &sink,
            &collector_identity,
            segment_end_ms,
            run_started,
            market_runtime
                .as_mut()
                .expect("market runtime is initialized before collection"),
            &gamma,
            &mut dedupe,
            &mut recovery,
        )
        .await;
        match collected {
            Ok(_) => {
                recovery.record_business_recovery();
                sink.source_queue_backpressure
                    .set_recovery_active("polymarket_market", false);
            }
            Err(error) if market_source_error_is_retryable(error.as_ref()) => {
                record_gap(
                    &sink.gaps,
                    format!("polymarket_market_recovery_cycle_restart:{error}"),
                );
                market_runtime = None;
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn market_source_error_is_retryable(error: &(dyn Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<SdkStreamError>(),
        Some(
            SdkStreamError::Connection(_)
                | SdkStreamError::Subscription(_)
                | SdkStreamError::TerminalDisconnected
                | SdkStreamError::Timeout(_)
        )
    )
}

#[allow(clippy::too_many_arguments)]
async fn collect_polymarket_single_receiver(
    mut discovery: BtcUpdownDiscovery,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    segment_end_ms: i64,
    run_started: Instant,
    market_runtime: &mut MarketSdkRuntime,
    gamma: &GammaClient,
    dedupe: &mut LifecycleFingerprintSets,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<BtcUpdownDiscovery> {
    let mut target_asset_ids = discovery.asset_ids();
    let mut target_assets = target_asset_ids
        .iter()
        .map(|asset| U256::from_str(asset))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    let mut asset_to_slug = discovery_asset_to_slug(&discovery);
    market_runtime
        .replace_and_wait(target_assets.clone(), Duration::from_secs(20))
        .await?;
    let mut freshness = MarketAssetFreshnessWatchdog::new(
        Duration::from_secs(MARKET_FIRST_EVENT_TIMEOUT_SECS),
        Duration::from_secs(MARKET_FRESHNESS_TIMEOUT_SECS),
    );
    freshness.replace_assets(target_asset_ids.clone(), Instant::now());
    send_polymarket_metadata_event(
        sink,
        run_started,
        &discovery.current,
        "current",
        collector_identity,
    )
    .await?;
    if let Some(next) = &discovery.next {
        send_polymarket_metadata_event(sink, run_started, next, "next", collector_identity).await?;
    }

    let mut current_market_resolved = false;
    let mut next_discovery_at = if discovery.next.is_none() {
        Instant::now()
    } else {
        Instant::now() + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS)
    };
    let mut next_discovery_tasks = JoinSet::new();
    while now_ms() < segment_end_ms {
        match market_runtime.recv(Duration::from_millis(250)).await {
            Ok(Some(message)) => {
                if let Some(source_native_payload_json) = prepare_polymarket_message(
                    dedupe,
                    &discovery,
                    &asset_to_slug,
                    &message,
                    Some(&sink.source_queue_backpressure),
                )? {
                    let outcome = dispatch_polymarket_message(
                        message,
                        &discovery,
                        &asset_to_slug,
                        sink,
                        collector_identity,
                        run_started,
                        &source_native_payload_json,
                    )
                    .await?;
                    current_market_resolved |= outcome.current_market_resolved;
                    if !outcome.matched_assets.is_empty() {
                        recovery.record_business_recovery();
                        sink.source_queue_backpressure
                            .set_recovery_active("polymarket_market", false);
                    }
                    freshness.observe(&outcome.matched_assets, Instant::now());
                }
            }
            Ok(None) => {}
            Err(err) => {
                if let SdkStreamError::Lagged(count) = &err {
                    sink.source_queue_backpressure.record_lag(
                        "polymarket_market",
                        now_ms(),
                        *count,
                    );
                }
                record_gap(&sink.gaps, format!("polymarket_sdk_stream_fatal:{err}"));
                return Err(err.into());
            }
        }

        if discovery.next.is_none()
            && next_discovery_tasks.is_empty()
            && Instant::now() >= next_discovery_at
            && now_ms() < discovery.current.end_ts_s * 1000
        {
            next_discovery_at =
                Instant::now() + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS);
            let next_slug = discovery_windows(discovery.current.start_ts_s * 1000)
                .next
                .slug;
            let gamma = gamma.clone();
            next_discovery_tasks
                .spawn(async move { fetch_exact_slug_candidates(&gamma, &next_slug).await });
        }

        if let Some(result) = next_discovery_tasks.try_join_next() {
            match result {
                Ok(Ok(candidates)) => match discovery.attach_next(&candidates) {
                    Ok(true) => {
                        let next = discovery
                            .next
                            .as_ref()
                            .expect("attach_next true installs next market");
                        send_polymarket_metadata_event(
                            sink,
                            run_started,
                            next,
                            "next",
                            collector_identity,
                        )
                        .await?;
                        target_asset_ids = discovery.asset_ids();
                        target_assets = target_asset_ids
                            .iter()
                            .map(|asset| U256::from_str(asset))
                            .collect::<std::result::Result<BTreeSet<_>, _>>()?;
                        asset_to_slug = discovery_asset_to_slug(&discovery);
                        dedupe
                            .replace_lifecycles(expected_lifecycle_slugs(&discovery))
                            .map_err(operator_dedupe_error)?;
                        market_runtime
                            .replace_and_wait(target_assets.clone(), Duration::from_secs(20))
                            .await?;
                        freshness.replace_assets(target_asset_ids.clone(), Instant::now());
                    }
                    Ok(false) => {}
                    Err(err) => record_gap(
                        &sink.gaps,
                        format!("polymarket_next_discovery_invalid:{err:?}"),
                    ),
                },
                Ok(Err(err)) => record_gap(
                    &sink.gaps,
                    format!("polymarket_next_discovery_fetch_failed:{err}"),
                ),
                Err(err) => record_gap(
                    &sink.gaps,
                    format!("polymarket_next_discovery_task_failed:{err}"),
                ),
            }
        }

        let expired = freshness.expired(Instant::now());
        if !expired.is_empty() {
            sink.source_queue_backpressure.record_lag(
                "polymarket_market",
                now_ms(),
                expired.len() as u64,
            );
            for entry in &expired {
                record_gap(
                    &sink.gaps,
                    format!(
                        "polymarket_asset_silence:{}:{}",
                        entry.asset_id,
                        entry.kind.as_str()
                    ),
                );
            }
            freshness
                .record_resubscribe(&expired, Instant::now())
                .map_err(io::Error::other)?;
            sink.source_queue_backpressure
                .record_reconnect("polymarket_market", now_ms());
            sink.source_queue_backpressure
                .set_recovery_active("polymarket_market", true);
            let resubscribe = market_runtime.resubscribe(Duration::from_secs(20)).await;
            sink.source_queue_backpressure
                .set_recovery_active("polymarket_market", false);
            resubscribe?;
        }
    }
    if !current_market_resolved {
        send_event(
            sink,
            run_started,
            raw_event(
                "polymarket_market_metadata",
                "active_scan",
                &discovery.current.slug,
                Some(now_ms().to_string()),
                "https://gamma-api.polymarket.com/markets",
                collector_identity,
                polymarket_window_end_observation_raw_json_from_discovery(&discovery.current),
            ),
        )
        .await?;
    }
    Ok(discovery)
}

#[derive(Default)]
struct PolymarketDispatchOutcome {
    current_market_resolved: bool,
    matched_assets: BTreeSet<String>,
}

fn polymarket_event_message_json(
    message: &polymarket_client_sdk_v2::clob::ws::EventMessage,
) -> Value {
    json!({
        "id": message.id,
        "ticker": message.ticker,
        "slug": message.slug,
        "title": message.title,
        "description": message.description,
    })
}

fn polymarket_source_native_payload_json(message: &WsMessage) -> Result<Option<String>> {
    let value = match message {
        WsMessage::Book(event) => json!({
            "source": "polymarket_market",
            "event_type": "book",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "timestamp": event.timestamp,
            "bids": event.bids.iter().map(|level| {
                json!([level.price.to_string(), level.size.to_string()])
            }).collect::<Vec<_>>(),
            "asks": event.asks.iter().map(|level| {
                json!([level.price.to_string(), level.size.to_string()])
            }).collect::<Vec<_>>(),
            "hash": event.hash,
        }),
        WsMessage::PriceChange(event) => json!({
            "source": "polymarket_market",
            "event_type": "price_change",
            "market": event.market.to_string(),
            "timestamp": event.timestamp,
            "price_changes": event.price_changes.iter().map(|change| json!({
                "asset_id": change.asset_id.to_string(),
                "price": change.price.to_string(),
                "size": change.size.as_ref().map(ToString::to_string),
                "side": change.side.to_string(),
                "hash": change.hash,
                "best_bid": change.best_bid.as_ref().map(ToString::to_string),
                "best_ask": change.best_ask.as_ref().map(ToString::to_string),
            })).collect::<Vec<_>>(),
        }),
        WsMessage::TickSizeChange(event) => json!({
            "source": "polymarket_market",
            "event_type": "tick_size_change",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "old_tick_size": event.old_tick_size.to_string(),
            "new_tick_size": event.new_tick_size.to_string(),
            "timestamp": event.timestamp,
        }),
        WsMessage::LastTradePrice(event) => json!({
            "source": "polymarket_market",
            "event_type": "last_trade_price",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "price": event.price.to_string(),
            "side": event.side.as_ref().map(ToString::to_string),
            "size": event.size.as_ref().map(ToString::to_string),
            "fee_rate_bps": event.fee_rate_bps.as_ref().map(ToString::to_string),
            "timestamp": event.timestamp,
        }),
        WsMessage::BestBidAsk(event) => json!({
            "source": "polymarket_market",
            "event_type": "best_bid_ask",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "best_bid": event.best_bid.to_string(),
            "best_ask": event.best_ask.to_string(),
            "spread": event.spread.to_string(),
            "timestamp": event.timestamp,
        }),
        WsMessage::NewMarket(event) => json!({
            "source": "polymarket_market",
            "event_type": "new_market",
            "id": event.id,
            "question": event.question,
            "market": event.market.to_string(),
            "slug": event.slug,
            "description": event.description,
            "asset_ids": event.asset_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "outcomes": event.outcomes,
            "event_message": event.event_message.as_ref().map(polymarket_event_message_json),
            "timestamp": event.timestamp,
        }),
        WsMessage::MarketResolved(event) => json!({
            "source": "polymarket_market",
            "event_type": "market_resolved",
            "id": event.id,
            "question": event.question,
            "market": event.market.to_string(),
            "slug": event.slug,
            "description": event.description,
            "asset_ids": event.asset_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "outcomes": event.outcomes,
            "winning_asset_id": event.winning_asset_id.to_string(),
            "winning_outcome": event.winning_outcome,
            "event_message": event.event_message.as_ref().map(polymarket_event_message_json),
            "timestamp": event.timestamp,
        }),
        WsMessage::Trade(_) | WsMessage::Order(_) => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(serde_json::to_string(&value)?))
}

fn expected_lifecycle_slugs(discovery: &BtcUpdownDiscovery) -> BTreeSet<String> {
    let next_slug = discovery.next.as_ref().map_or_else(
        || {
            discovery_windows(discovery.current.start_ts_s * 1000)
                .next
                .slug
        },
        |next| next.slug.clone(),
    );
    BTreeSet::from([discovery.current.slug.clone(), next_slug])
}

fn polymarket_message_lifecycle_slug(
    message: &WsMessage,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
) -> Option<String> {
    let (market, asset_ids) = match message {
        WsMessage::Book(event) => (event.market.to_string(), vec![event.asset_id.to_string()]),
        WsMessage::PriceChange(event) => (
            event.market.to_string(),
            event
                .price_changes
                .iter()
                .map(|change| change.asset_id.to_string())
                .collect(),
        ),
        WsMessage::TickSizeChange(event) => {
            (event.market.to_string(), vec![event.asset_id.to_string()])
        }
        WsMessage::LastTradePrice(event) => {
            (event.market.to_string(), vec![event.asset_id.to_string()])
        }
        WsMessage::BestBidAsk(event) => {
            (event.market.to_string(), vec![event.asset_id.to_string()])
        }
        WsMessage::NewMarket(event) => (
            event.market.to_string(),
            event.asset_ids.iter().map(ToString::to_string).collect(),
        ),
        WsMessage::MarketResolved(event) => (
            event.market.to_string(),
            event.asset_ids.iter().map(ToString::to_string).collect(),
        ),
        _ => return None,
    };
    if market == discovery.current.condition_id {
        return Some(discovery.current.slug.clone());
    }
    if let Some(next) = &discovery.next
        && market == next.condition_id
    {
        return Some(next.slug.clone());
    }
    asset_ids
        .iter()
        .find_map(|asset_id| asset_to_slug.get(asset_id).cloned())
}

fn prepare_polymarket_message(
    dedupe: &mut LifecycleFingerprintSets,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
    message: &WsMessage,
    operational_stats: Option<&SourceQueueBackpressureStats>,
) -> Result<Option<String>> {
    let Some(source_native_payload_json) = polymarket_source_native_payload_json(message)? else {
        return Ok(None);
    };
    if source_native_payload_json.len() > MAX_TYPED_ROW_BYTES {
        return Err(io::Error::other(format!(
            "operator_intervention_required:Polymarket source-native payload exceeds {MAX_TYPED_ROW_BYTES} bytes"
        ))
        .into());
    }
    let Some(lifecycle) = polymarket_message_lifecycle_slug(message, discovery, asset_to_slug)
    else {
        return Ok(Some(source_native_payload_json));
    };
    match dedupe
        .observe(&lifecycle, source_native_payload_json.as_bytes())
        .map_err(operator_dedupe_error)?
    {
        FingerprintDecision::Unique => Ok(Some(source_native_payload_json)),
        FingerprintDecision::Duplicate => {
            if let Some(stats) = operational_stats {
                stats.record_structural_dedupe("polymarket_market", now_ms(), 1);
            }
            Ok(None)
        }
    }
}

fn restore_polymarket_dedupe(
    dataset_root: &Path,
    dedupe: &mut LifecycleFingerprintSets,
) -> Result<()> {
    let mut parquet_paths = Vec::new();
    let now = now_ms();
    let days = BTreeSet::from([
        utc_day_string_from_ms(now.saturating_sub(86_400_000))
            .ok_or_else(|| io::Error::other("previous UTC day is out of range"))?,
        utc_day_string_from_ms(now)
            .ok_or_else(|| io::Error::other("current UTC day is out of range"))?,
    ]);
    for day in days {
        let source_dir = dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join("source_kind=polymarket_market");
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
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.ends_with(".parquet")
                && !file_name.ends_with("-running.parquet")
                && !file_name.ends_with("-digesting.parquet")
            {
                parquet_paths.push(entry.path());
            }
        }
    }
    parquet_paths.sort_by_key(|path| finalized_parquet_order_key(path));

    for path in parquet_paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("slug").is_err()
            || builder
                .schema()
                .index_of("source_native_payload_json")
                .is_err()
        {
            continue;
        }
        if builder.metadata().file_metadata().num_rows() == 0 {
            continue;
        }
        let projection = ProjectionMask::columns(
            builder.parquet_schema(),
            ["slug", "source_native_payload_json"],
        );
        let mut reader = builder
            .with_projection(projection)
            .with_batch_size(16)
            .build()?;
        for batch in &mut reader {
            let batch = batch?;
            let slug_index = batch.schema().index_of("slug")?;
            let payload_index = batch.schema().index_of("source_native_payload_json")?;
            let slugs = batch
                .column(slug_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| io::Error::other("Polymarket restore slug is not UTF8"))?;
            let payloads = batch
                .column(payload_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    io::Error::other("Polymarket restore source-native payload is not UTF8")
                })?;
            for row in 0..batch.num_rows() {
                if slugs.is_null(row) {
                    continue;
                }
                let slug = slugs.value(row);
                if !dedupe.contains_lifecycle(slug) {
                    continue;
                }
                if payloads.is_null(row) {
                    return Err(io::Error::other(format!(
                        "Polymarket restore payload missing in {}",
                        path.display()
                    ))
                    .into());
                }
                let payload = payloads.value(row);
                if payload.len() > MAX_TYPED_ROW_BYTES {
                    return Err(io::Error::other(format!(
                        "operator_intervention_required:Polymarket restore payload exceeds {MAX_TYPED_ROW_BYTES} bytes in {}",
                        path.display()
                    ))
                    .into());
                }
                let _ = dedupe
                    .observe(slug, payload.as_bytes())
                    .map_err(operator_dedupe_error)?;
            }
        }
    }
    Ok(())
}

async fn dispatch_polymarket_message(
    message: WsMessage,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    source_native_payload_json: &str,
) -> Result<PolymarketDispatchOutcome> {
    let mut outcome = PolymarketDispatchOutcome::default();
    match message {
        WsMessage::BestBidAsk(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = asset_to_slug.get(&asset_id) else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "best_bid_ask",
                    &format!("{slug}:{}", event.asset_id),
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    with_source_native_payload_json(
                        json!({
                            "event_type": "best_bid_ask",
                            "market": event.market.to_string(),
                            "asset_id": asset_id,
                            "slug": slug,
                            "best_bid": event.best_bid.to_string(),
                            "best_ask": event.best_ask.to_string(),
                            "spread": event.spread.to_string(),
                            "timestamp": event.timestamp
                        }),
                        source_native_payload_json,
                    ),
                ),
            )
            .await?;
        }
        WsMessage::Book(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = asset_to_slug.get(&asset_id) else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "book",
                    &format!("{slug}:{}", event.asset_id),
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    with_source_native_payload_json(
                        polymarket_book_raw_json(&event, slug),
                        source_native_payload_json,
                    ),
                ),
            )
            .await?;
        }
        WsMessage::PriceChange(mut event) => {
            outcome.matched_assets = retain_target_price_changes(&mut event, asset_to_slug);
            if event.price_changes.is_empty() {
                return Ok(outcome);
            }
            for change in event.price_changes.clone() {
                let asset_id = change.asset_id.to_string();
                let slug = asset_to_slug
                    .get(&asset_id)
                    .expect("retained price change has target slug");
                let mut row_event = event.clone();
                row_event.price_changes = vec![change];
                send_event(
                    sink,
                    run_started,
                    raw_event(
                        "polymarket_market",
                        "price_change",
                        &format!("{slug}:{asset_id}"),
                        Some(row_event.timestamp.to_string()),
                        MARKET_WS_ENDPOINT,
                        collector_identity,
                        with_source_native_payload_json(
                            polymarket_price_change_raw_json(&row_event, asset_to_slug),
                            source_native_payload_json,
                        ),
                    ),
                )
                .await?;
            }
        }
        WsMessage::LastTradePrice(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = asset_to_slug.get(&asset_id) else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "last_trade_price",
                    &format!("{slug}:{asset_id}"),
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    with_source_native_payload_json(
                        polymarket_last_trade_raw_json(&event, slug),
                        source_native_payload_json,
                    ),
                ),
            )
            .await?;
        }
        WsMessage::TickSizeChange(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = asset_to_slug.get(&asset_id) else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "tick_size_change",
                    &format!("{slug}:{asset_id}"),
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    with_source_native_payload_json(
                        polymarket_tick_size_change_raw_json(&event, slug),
                        source_native_payload_json,
                    ),
                ),
            )
            .await?;
        }
        WsMessage::NewMarket(event) => {
            outcome.matched_assets = matching_target_asset_ids(&event.asset_ids, asset_to_slug);
            if outcome.matched_assets.is_empty() {
                return Ok(outcome);
            }
            let metadata_raw_json = polymarket_new_market_raw_json(&event);
            let market_raw_json = with_source_native_payload_json(
                metadata_raw_json.clone(),
                source_native_payload_json,
            );
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "new_market",
                    &event.slug,
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    market_raw_json,
                ),
            )
            .await?;
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market_metadata",
                    "new_market",
                    &event.slug,
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    metadata_raw_json,
                ),
            )
            .await?;
        }
        WsMessage::MarketResolved(event) => {
            outcome.matched_assets = matching_target_asset_ids(&event.asset_ids, asset_to_slug);
            if outcome.matched_assets.is_empty() {
                return Ok(outcome);
            }
            let source_identifier = event
                .slug
                .clone()
                .unwrap_or_else(|| event.market.to_string());
            let lifecycle_slug = event.slug.clone().or_else(|| {
                outcome
                    .matched_assets
                    .iter()
                    .find_map(|asset_id| asset_to_slug.get(asset_id).cloned())
            });
            let market_raw_json = with_polymarket_row_context(
                polymarket_market_resolved_raw_json(&event),
                lifecycle_slug.as_deref(),
                source_native_payload_json,
            );
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market",
                    "market_resolved",
                    &source_identifier,
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    market_raw_json,
                ),
            )
            .await?;
            send_event(
                sink,
                run_started,
                raw_event(
                    "polymarket_market_metadata",
                    "fallback_close",
                    &source_identifier,
                    Some(event.timestamp.to_string()),
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    polymarket_fallback_close_raw_json_from_resolved(&event),
                ),
            )
            .await?;
            outcome.current_market_resolved =
                event.market.to_string() == discovery.current.condition_id;
        }
        _ => return Ok(outcome),
    }
    Ok(outcome)
}

fn retain_target_price_changes(
    event: &mut PriceChange,
    asset_to_slug: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    event
        .price_changes
        .retain(|change| asset_to_slug.contains_key(&change.asset_id.to_string()));
    event
        .price_changes
        .iter()
        .map(|change| change.asset_id.to_string())
        .collect()
}

fn matching_target_asset_ids(
    asset_ids: &[U256],
    asset_to_slug: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    asset_ids
        .iter()
        .map(ToString::to_string)
        .filter(|asset| asset_to_slug.contains_key(asset))
        .collect()
}

async fn send_polymarket_metadata_event(
    sink: &SourceEventSink,
    run_started: Instant,
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    discovery_slot: &str,
    collector_identity: &CollectorIdentity,
) -> Result<()> {
    let mut event = raw_event(
        "polymarket_market_metadata",
        "active_scan",
        &market.slug,
        Some(now_ms().to_string()),
        "https://gamma-api.polymarket.com/markets",
        collector_identity,
        polymarket_metadata_snapshot_raw_json(market, discovery_slot),
    );
    if let Some(timestamp_received) = market.metadata_received_utc_ms {
        event.recv_time_utc_ms = timestamp_received;
    }
    send_event(sink, run_started, event).await?;
    Ok(())
}

fn polymarket_metadata_snapshot_raw_json(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    discovery_slot: &str,
) -> Value {
    json!({
        "event_type": "active_scan",
        "metadata_source": "active_scan",
        "discovery_slot": discovery_slot,
        "slug": market.slug,
        "condition_id": market.condition_id,
        "clob_market_id": market.id,
        "asset_ids": [market.up_asset_id, market.down_asset_id],
        "metadata_fetch_start_utc": market.metadata_fetch_start_utc_ms,
        "market_start_utc": market.start_ts_s * 1000,
        "market_end_utc": market.end_ts_s * 1000,
        "active": market.active,
        "accepting_orders": market.accepting_orders,
        "market_lifecycle_state": if market.active { "active" } else { "candidate" },
        "tick_size": market.tick_size,
        "min_size": market.min_size,
        "fee_schedule_raw": market.fee_schedule_raw,
        "reward_eligibility_raw": market.reward_eligibility_raw,
        "metadata_unavailable_reason": missing_metadata_reason(market),
        "same_condition_market_verified": true,
        "raw_json_sha256": market.raw_json_sha256,
        "gamma_market_raw_json": market.raw_json,
    })
}

fn polymarket_window_end_observation_raw_json_from_discovery(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
) -> Value {
    let mut value = polymarket_metadata_snapshot_raw_json(market, "current_window_end_unresolved");
    let reason = "market_resolved_not_observed_before_window_end";
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "market_lifecycle_state".to_owned(),
            json!("unresolved_window_end_observation"),
        );
        object.insert("window_end_observation_reason".to_owned(), json!(reason));
        let mut reasons = missing_metadata_reason(market)
            .into_iter()
            .collect::<Vec<_>>();
        reasons.push(reason.to_owned());
        object.insert("metadata_unavailable_reason".to_owned(), json!(reasons));
    }
    value
}

fn missing_metadata_reason(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
) -> Option<String> {
    let missing = [
        ("tick_size", market.tick_size.is_none()),
        ("min_size", market.min_size.is_none()),
        ("fee_schedule_raw", market.fee_schedule_raw.is_none()),
        (
            "reward_eligibility_raw",
            market.reward_eligibility_raw.is_none(),
        ),
    ]
    .into_iter()
    .filter_map(|(field, missing)| missing.then_some(field))
    .collect::<Vec<_>>();
    (!missing.is_empty()).then(|| format!("gamma_market_fields_unavailable:{}", missing.join(",")))
}

fn rtds_source_native_payload_json(
    message: &polymarket_client_sdk_v2::rtds::RtdsMessage,
) -> Result<String> {
    let canonical = serde_json::to_string(&json!({
        "source": "polymarket_rtds_chainlink_btc_usd",
        "topic": message.topic,
        "type": message.msg_type,
        "timestamp": message.timestamp,
        "payload": message.payload,
    }))?;
    if canonical.len() > MAX_TYPED_ROW_BYTES {
        return Err(io::Error::other(format!(
            "operator_intervention_required:RTDS source-native payload exceeds {MAX_TYPED_ROW_BYTES} bytes"
        ))
        .into());
    }
    Ok(canonical)
}

fn restore_rtds_dedupe(
    dataset_root: &Path,
    restore_time_ms: i64,
) -> Result<TimedFingerprintWindow> {
    let mut window =
        TimedFingerprintWindow::with_limits(RTDS_DEDUPE_WINDOW_MS, RTDS_DEDUPE_MAX_KEYS);
    let mut paths = Vec::new();
    let days = BTreeSet::from([
        utc_day_string_from_ms(restore_time_ms.saturating_sub(RTDS_DEDUPE_WINDOW_MS))
            .ok_or_else(|| io::Error::other("RTDS previous UTC day is out of range"))?,
        utc_day_string_from_ms(restore_time_ms)
            .ok_or_else(|| io::Error::other("RTDS current UTC day is out of range"))?,
    ]);
    for day in days {
        paths.extend(finalized_parquet_files(
            &dataset_root
                .join("training")
                .join("typed_raw")
                .join(format!("utc_day={day}"))
                .join("source_kind=polymarket_rtds_chainlink_btc_usd"),
        )?);
    }
    paths.sort_by_key(|path| finalized_parquet_order_key(path));
    let window_start_ms = restore_time_ms.saturating_sub(RTDS_DEDUPE_WINDOW_MS);
    for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("timestamp_received").is_err()
            || builder
                .schema()
                .index_of("source_native_payload_json")
                .is_err()
        {
            continue;
        }
        if builder.metadata().file_metadata().num_rows() == 0 {
            continue;
        }
        let projection = ProjectionMask::columns(
            builder.parquet_schema(),
            ["timestamp_received", "source_native_payload_json"],
        );
        let mut reader = builder
            .with_projection(projection)
            .with_batch_size(16)
            .build()?;
        for batch in &mut reader {
            let batch = batch?;
            let received_index = batch.schema().index_of("timestamp_received")?;
            let payload_index = batch.schema().index_of("source_native_payload_json")?;
            let received = batch
                .column(received_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| io::Error::other("RTDS restore receive time is not timestamp-ms"))?;
            let payloads = batch
                .column(payload_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| io::Error::other("RTDS restore payload is not UTF8"))?;
            for row in 0..batch.num_rows() {
                if received.is_null(row) || payloads.is_null(row) {
                    return Err(io::Error::other(format!(
                        "RTDS restore key missing in {}",
                        path.display()
                    ))
                    .into());
                }
                let received_ms = received.value(row);
                if received_ms < window_start_ms {
                    continue;
                }
                let payload = payloads.value(row);
                if payload.len() > MAX_TYPED_ROW_BYTES {
                    return Err(io::Error::other(format!(
                        "operator_intervention_required:RTDS restore payload exceeds {MAX_TYPED_ROW_BYTES} bytes in {}",
                        path.display()
                    ))
                    .into());
                }
                let _ = window
                    .observe(payload.as_bytes(), received_ms)
                    .map_err(operator_dedupe_error)?;
            }
        }
    }
    Ok(window)
}

async fn run_rtds_source(
    dataset_root: PathBuf,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
) -> Result<()> {
    let restore_time_ms = now_ms();
    let mut dedupe =
        tokio::task::spawn_blocking(move || restore_rtds_dedupe(&dataset_root, restore_time_ms))
            .await
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:RTDS dedupe restore join failed:{error}"
                ))
            })?
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:RTDS dedupe restore failed:{error}"
                ))
            })?;
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let mut runtime = RtdsSdkRuntime::new(RTDS_WS_ENDPOINT.to_owned(), bounded_ws_config())?;
    runtime.wait_connected(Duration::from_secs(20)).await?;
    runtime
        .subscribe(subscription.clone(), Duration::from_secs(3))
        .await?;

    'source: loop {
        let freshness_deadline = Instant::now() + Duration::from_secs(RTDS_FRESHNESS_TIMEOUT_SECS);
        match wait_for_fresh_rtds_message(
            &mut runtime,
            &sink,
            &collector_identity,
            run_started,
            &mut dedupe,
            freshness_deadline,
            deadline,
        )
        .await?
        {
            RtdsWaitOutcome::Fresh => continue,
            RtdsWaitOutcome::RunDeadline => break,
            RtdsWaitOutcome::WaitExpired => {
                sink.source_queue_backpressure.record_lag(
                    "polymarket_rtds_chainlink_btc_usd",
                    now_ms(),
                    1,
                );
            }
        }

        for action in rtds_freshness_actions() {
            sink.source_queue_backpressure
                .record_reconnect("polymarket_rtds_chainlink_btc_usd", now_ms());
            sink.source_queue_backpressure
                .set_recovery_active("polymarket_rtds_chainlink_btc_usd", true);
            if let Err(err) = runtime
                .resubscribe(subscription.clone(), Duration::from_secs(3))
                .await
            {
                record_gap(
                    &sink.gaps,
                    format!(
                        "rtds_resubscribe_failed:attempt={}:{}",
                        action.resubscribe_attempt, err
                    ),
                );
                return Err(err.into());
            }
            match wait_for_fresh_rtds_message(
                &mut runtime,
                &sink,
                &collector_identity,
                run_started,
                &mut dedupe,
                Instant::now() + action.wait,
                deadline,
            )
            .await?
            {
                RtdsWaitOutcome::Fresh => {
                    sink.source_queue_backpressure
                        .set_recovery_active("polymarket_rtds_chainlink_btc_usd", false);
                    continue 'source;
                }
                RtdsWaitOutcome::RunDeadline => {
                    sink.source_queue_backpressure
                        .set_recovery_active("polymarket_rtds_chainlink_btc_usd", false);
                    break 'source;
                }
                RtdsWaitOutcome::WaitExpired if action.exit_after_wait => {
                    record_gap(
                        &sink.gaps,
                        "rtds_business_message_timeout_after_two_resubscriptions".to_owned(),
                    );
                    return Err("RTDS business stream did not recover".into());
                }
                RtdsWaitOutcome::WaitExpired => {}
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RtdsWaitOutcome {
    Fresh,
    WaitExpired,
    RunDeadline,
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_fresh_rtds_message(
    runtime: &mut RtdsSdkRuntime,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    dedupe: &mut TimedFingerprintWindow,
    wait_deadline: Instant,
    run_deadline: Option<Instant>,
) -> Result<RtdsWaitOutcome> {
    loop {
        if deadline_reached(run_deadline) {
            return Ok(RtdsWaitOutcome::RunDeadline);
        }
        let wait_remaining = wait_deadline.saturating_duration_since(Instant::now());
        if wait_remaining.is_zero() {
            return Ok(RtdsWaitOutcome::WaitExpired);
        }
        let wait_duration = deadline_remaining(run_deadline)
            .map(|remaining| remaining.min(wait_remaining))
            .unwrap_or(wait_remaining);
        match runtime.recv(wait_duration).await {
            Ok(Some(message)) => {
                if emit_rtds_message(sink, collector_identity, run_started, dedupe, message).await?
                {
                    return Ok(RtdsWaitOutcome::Fresh);
                }
            }
            Ok(None) => {}
            Err(err) => {
                if let SdkStreamError::Lagged(count) = &err {
                    sink.source_queue_backpressure.record_lag(
                        "polymarket_rtds_chainlink_btc_usd",
                        now_ms(),
                        *count,
                    );
                }
                record_gap(&sink.gaps, format!("rtds_sdk_stream_fatal:{err}"));
                return Err(err.into());
            }
        }
    }
}

async fn emit_rtds_message(
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    dedupe: &mut TimedFingerprintWindow,
    message: polymarket_client_sdk_v2::rtds::RtdsMessage,
) -> Result<bool> {
    let recv_time_utc_ms = now_ms();
    let classified = match classify_rtds_message(&message, recv_time_utc_ms) {
        Ok(classified) => classified,
        Err(error) => {
            record_gap(
                &sink.gaps,
                format!("rtds_unparseable_chainlink_payload:{error}"),
            );
            return Err(error);
        }
    };
    let source_native_payload_json = rtds_source_native_payload_json(&message)?;
    if dedupe
        .observe(source_native_payload_json.as_bytes(), recv_time_utc_ms)
        .map_err(operator_dedupe_error)?
        == FingerprintDecision::Duplicate
    {
        sink.source_queue_backpressure.record_structural_dedupe(
            "polymarket_rtds_chainlink_btc_usd",
            recv_time_utc_ms,
            1,
        );
        return Ok(false);
    }
    let source_ts = classified.payload.timestamp;
    let source_identifier = classified.payload.symbol.clone();
    let value = classified.payload.value.to_string();
    let is_fresh = classified.freshness_status == "fresh";
    if classified.freshness_status == "stale" {
        sink.source_queue_backpressure.record_lag(
            "polymarket_rtds_chainlink_btc_usd",
            recv_time_utc_ms,
            1,
        );
    }
    send_event(
        sink,
        run_started,
        RawEvent {
            source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
            source_event_type: message.msg_type.clone(),
            source_identifier,
            source_ts: Some(source_ts.to_string()),
            recv_time_utc_ms,
            collector_host_id: collector_identity.host_id.clone(),
            collector_region: collector_identity.region.clone(),
            source_endpoint: RTDS_WS_ENDPOINT.to_owned(),
            raw_json: json!({
                "topic": message.topic,
                "type": message.msg_type,
                "message_timestamp": message.timestamp,
                "payload_timestamp": classified.payload.timestamp,
                "symbol": classified.payload.symbol,
                "value": value,
                "age_ms": classified.age_ms,
                "freshness_status": classified.freshness_status,
                "subscription_raw_json": rtds_subscription_evidence_json(),
                "payload": message.payload,
                "source_native_payload_json": source_native_payload_json,
            })
            .to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
        },
    )
    .await?;
    Ok(is_fresh)
}

async fn run_binance_depth_source(
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    control_rx: mpsc::Receiver<DepthSourceControl>,
) -> Result<()> {
    run_binance_depth_source_with_spec_and_control(
        BinanceDepthSpec::production(),
        sink,
        collector_identity,
        deadline,
        run_started,
        Some(control_rx),
    )
    .await
}

#[cfg(test)]
async fn run_binance_depth_source_with_spec(
    spec: BinanceDepthSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
) -> Result<()> {
    run_binance_depth_source_with_spec_and_control(
        spec,
        sink,
        collector_identity,
        deadline,
        run_started,
        None,
    )
    .await
}

async fn run_binance_depth_source_with_spec_and_control(
    spec: BinanceDepthSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    mut control_rx: Option<mpsc::Receiver<DepthSourceControl>>,
) -> Result<()> {
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(BINANCE_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BINANCE_HTTP_TOTAL_TIMEOUT_SECS))
        .build()?;
    let mut snapshot_trigger_reason = "bootstrap".to_owned();
    let mut recovery = SourceRecoverySupervisor::new(
        "binance_depth_update",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    'connections: while !deadline_reached(deadline) {
        if let Some(receiver) = control_rx.as_mut() {
            while let Ok(control) = receiver.try_recv() {
                if handle_depth_source_control(control).await? {
                    break 'connections;
                }
            }
        }
        let (depth_stream, _) = match timeout(
            Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
            connect_async(&spec.websocket_endpoint),
        )
        .await
        {
            Ok(Ok(connected)) => connected,
            Ok(Err(err)) => {
                let reason = if snapshot_trigger_reason == "bootstrap" {
                    format!("connect_error:{err}")
                } else {
                    format!("reconnect_error:{err}")
                };
                let gap = if snapshot_trigger_reason == "bootstrap" {
                    format!("binance_depth_update_ws_connect_error:{err}")
                } else {
                    format!("binance_depth_update_ws_reconnect_error:{err}")
                };
                if !handle_binance_depth_ws_connect_failure(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    gap,
                    deadline,
                )
                .await
                {
                    break;
                }
                continue;
            }
            Err(_) => {
                let reason = if snapshot_trigger_reason == "bootstrap" {
                    "connect_timeout".to_owned()
                } else {
                    "reconnect_timeout".to_owned()
                };
                let gap = if snapshot_trigger_reason == "bootstrap" {
                    "binance_depth_update_ws_connect_timeout".to_owned()
                } else {
                    "binance_depth_update_ws_reconnect_timeout".to_owned()
                };
                if !handle_binance_depth_ws_connect_failure(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    gap,
                    deadline,
                )
                .await
                {
                    break;
                }
                continue;
            }
        };
        let snapshot_last_update_id = match fetch_binance_depth_snapshot(
            &http_client,
            &sink,
            &collector_identity,
            run_started,
            &spec.snapshot_endpoint,
            &snapshot_trigger_reason,
        )
        .await
        {
            Ok(snapshot_last_update_id) => snapshot_last_update_id,
            Err(error)
                if error
                    .to_string()
                    .contains("binance_depth_snapshot_recovery_exhausted") =>
            {
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        let snapshot_utc_day =
            utc_day_string_from_ms(now_ms()).unwrap_or_else(|| "unknown".to_owned());
        let update_exit = match run_binance_depth_update_ws_source_with_control(
            depth_stream,
            BinanceDepthUpdateContext {
                http_client: http_client.clone(),
                sink: sink.clone(),
                collector_identity: collector_identity.clone(),
                deadline,
                run_started,
                spec: spec.clone(),
                snapshot_last_update_id,
                snapshot_utc_day,
                pending_snapshot_trigger_reason: (snapshot_trigger_reason != "bootstrap")
                    .then(|| snapshot_trigger_reason.clone()),
            },
            control_rx.as_mut(),
        )
        .await
        {
            Ok(exit) => exit,
            Err(error)
                if error
                    .to_string()
                    .contains("binance_depth_snapshot_recovery_exhausted") =>
            {
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        match update_exit {
            BinanceDepthUpdateExit::DeadlineReached => break,
            BinanceDepthUpdateExit::Stopped => break,
            BinanceDepthUpdateExit::StreamEnded {
                reason,
                saw_business,
            } => {
                if saw_business {
                    recovery.record_business_recovery();
                    sink.source_queue_backpressure
                        .set_recovery_active("binance_depth_update", false);
                }
                if !prepare_binance_depth_reconnect(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    deadline,
                )
                .await
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn handle_binance_depth_ws_connect_failure(
    sink: &SourceEventSink,
    recovery: &mut SourceRecoverySupervisor,
    snapshot_trigger_reason: &mut String,
    reason: String,
    gap: String,
    deadline: Option<Instant>,
) -> bool {
    record_gap(&sink.gaps, gap);
    prepare_binance_depth_reconnect(sink, recovery, snapshot_trigger_reason, reason, deadline).await
}

async fn prepare_binance_depth_reconnect(
    sink: &SourceEventSink,
    recovery: &mut SourceRecoverySupervisor,
    snapshot_trigger_reason: &mut String,
    reason: String,
    deadline: Option<Instant>,
) -> bool {
    if reason == "no_business_messages" {
        sink.source_queue_backpressure
            .record_lag("binance_depth_update", now_ms(), 1);
    }
    record_gap(
        &sink.gaps,
        format!("binance_depth_reconnect_required:{reason}"),
    );
    *snapshot_trigger_reason = format!("reconnect:{reason}");
    wait_for_source_recovery(recovery, sink, deadline).await
}

async fn fetch_binance_depth_snapshot(
    http_client: &reqwest::Client,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    snapshot_endpoint: &str,
    trigger_reason: &str,
) -> Result<u64> {
    for attempt in 1..=BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
        let snapshot_fetch_start_utc = now_ms();
        let fetched = async {
            let response = http_client
                .get(snapshot_endpoint)
                .send()
                .await
                .map_err(|error| format!("request:{error}"))?;
            let status = response.status();
            let body = response
                .bytes()
                .await
                .map_err(|error| format!("body:{error}"))?;
            let timestamp_received = now_ms();
            if !status.is_success() {
                return Err(format!("http_status:{status}"));
            }
            if body.is_empty() {
                return Err("empty_body".to_owned());
            }
            let mut snapshot_value =
                serde_json::from_slice::<Value>(&body).map_err(|error| format!("json:{error}"))?;
            let snapshot_last_update_id = snapshot_value
                .get("lastUpdateId")
                .and_then(Value::as_u64)
                .filter(|id| *id > 0)
                .ok_or_else(|| "last_update_id_missing".to_owned())?;
            let object = snapshot_value
                .as_object_mut()
                .ok_or_else(|| "snapshot_not_object".to_owned())?;
            object.insert("snapshot_trigger_reason".to_owned(), json!(trigger_reason));
            object.insert(
                "snapshot_fetch_start_utc".to_owned(),
                json!(snapshot_fetch_start_utc),
            );
            Ok::<_, String>((snapshot_last_update_id, snapshot_value, timestamp_received))
        }
        .await;
        match fetched {
            Ok((snapshot_last_update_id, snapshot_value, timestamp_received)) => {
                let mut event = raw_event(
                    "binance_depth_snapshot",
                    "depth_snapshot",
                    "BTCUSDT",
                    None,
                    snapshot_endpoint,
                    collector_identity,
                    snapshot_value,
                );
                event.recv_time_utc_ms = timestamp_received;
                send_event(sink, run_started, event).await?;
                return Ok(snapshot_last_update_id);
            }
            Err(error) => {
                record_gap(
                    &sink.gaps,
                    format!(
                        "binance_depth_snapshot_attempt_failed:attempt={attempt}:{error}:{trigger_reason}"
                    ),
                );
                if attempt == BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
                    let prefix = if error.starts_with("json:")
                        || error == "last_update_id_missing"
                        || error == "snapshot_not_object"
                    {
                        "binance_depth_snapshot_schema_fatal"
                    } else {
                        "binance_depth_snapshot_recovery_exhausted"
                    };
                    return Err(io::Error::other(format!("{prefix}:{error}")).into());
                }
                sleep(Duration::from_millis(250 * u64::from(attempt))).await;
            }
        }
    }
    Err(io::Error::other("Binance depth snapshot attempt loop exhausted").into())
}

fn utc_day_string_from_ms(utc_ms: i64) -> Option<String> {
    let dt = OffsetDateTime::from_unix_timestamp(utc_ms.div_euclid(1000)).ok()?;
    Some(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    ))
}

async fn run_binance_depth_update_ws_source_with_control(
    mut stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    mut context: BinanceDepthUpdateContext,
    mut control_rx: Option<&mut mpsc::Receiver<DepthSourceControl>>,
) -> Result<BinanceDepthUpdateExit> {
    let mut local_u = None::<u64>;
    let mut saw_business = false;
    'messages: while !deadline_reached(context.deadline) {
        let read_timeout = deadline_remaining(context.deadline)
            .map(|remaining| remaining.min(Duration::from_secs(30)))
            .unwrap_or_else(|| Duration::from_secs(30));
        let message = match control_rx.as_deref_mut() {
            Some(receiver) => {
                tokio::select! {
                    biased;
                    control = receiver.recv() => {
                        match control {
                            Some(control) => {
                                if handle_depth_source_control(control).await? {
                                    return Ok(BinanceDepthUpdateExit::Stopped);
                                }
                                continue 'messages;
                            }
                            None => {
                                return Err(invalid_data("depth source control channel closed").into());
                            }
                        }
                    }
                    message = timeout(read_timeout, stream.next()) => message,
                }
            }
            None => timeout(read_timeout, stream.next()).await,
        };
        match message {
            Ok(Some(Ok(Message::Text(text)))) => {
                let raw_json = text.to_string();
                let recv_time_utc_ms = now_ms();
                let recv_utc_day = utc_day_string_from_ms(recv_time_utc_ms)
                    .unwrap_or(context.snapshot_utc_day.clone());
                let value = serde_json::from_str::<Value>(&raw_json).map_err(|error| {
                    invalid_data(format!(
                        "binance_depth_update_parse_error:parse_depth_update_json:{error}"
                    ))
                })?;
                let payload = binance_ws_payload(&value);
                let event_type = payload.get("e").and_then(Value::as_str).ok_or_else(|| {
                    invalid_data("binance_depth_update_parse_error:missing_string_field:e")
                })?;
                if event_type == "serverShutdown" {
                    return Ok(BinanceDepthUpdateExit::StreamEnded {
                        reason: "server_shutdown".to_owned(),
                        saw_business,
                    });
                }
                if event_type != "depthUpdate" {
                    return Err(invalid_data(format!(
                        "binance_depth_update_parse_error:unexpected_depth_event_type:{event_type}"
                    ))
                    .into());
                }
                let mut base_event = RawEvent {
                    source_kind: "binance_depth_update".to_owned(),
                    source_event_type: "depth_update".to_owned(),
                    source_identifier: "BTCUSDT".to_owned(),
                    source_ts: payload
                        .get("E")
                        .and_then(Value::as_i64)
                        .map(|ts| ts.to_string()),
                    recv_time_utc_ms,
                    collector_host_id: context.collector_identity.host_id.clone(),
                    collector_region: context.collector_identity.region.clone(),
                    source_endpoint: context.spec.websocket_endpoint.clone(),
                    raw_json: raw_json.clone(),
                    parquet_raw_layout_version: 1,
                    parquet_raw_source_kind: "binance_depth_update".to_owned(),
                };
                let parsed = parse_depth_update_raw(
                    event_envelope(&base_event, SourceKind::BinanceDepthUpdate),
                    &raw_json,
                )
                .map_err(|error| {
                    invalid_data(format!("binance_depth_update_parse_error:{error}"))
                })?;
                if recv_utc_day != context.snapshot_utc_day {
                    let next_snapshot_last_update_id = match fetch_binance_depth_snapshot(
                        &context.http_client,
                        &context.sink,
                        &context.collector_identity,
                        context.run_started,
                        &context.spec.snapshot_endpoint,
                        "utc_day_boundary",
                    )
                    .await
                    {
                        Ok(snapshot_last_update_id) => snapshot_last_update_id,
                        Err(error) => {
                            record_gap(
                                &context.sink.gaps,
                                "binance_depth_resync_failed:utc_day_boundary".to_owned(),
                            );
                            return Err(error);
                        }
                    };
                    context.snapshot_last_update_id = next_snapshot_last_update_id;
                    context.snapshot_utc_day = recv_utc_day.clone();
                    context.pending_snapshot_trigger_reason = Some("utc_day_boundary".to_owned());
                    local_u = None;
                }
                let mut local_u_before = local_u;
                let mut resnapshot_attempted = false;
                let stitched = loop {
                    match stitch_depth_update(context.snapshot_last_update_id, local_u, &parsed) {
                        crate::sources::binance::DepthUpdateDecision::Apply {
                            local_u: next_local_u,
                            stitch_evidence,
                        } => {
                            local_u = Some(next_local_u);
                            let stitched = apply_depth_stitch_evidence(
                                parsed,
                                crate::sources::binance::DepthUpdateDecision::Apply {
                                    local_u: next_local_u,
                                    stitch_evidence,
                                },
                            )
                            .map_err(invalid_data)?;
                            break stitched;
                        }
                        crate::sources::binance::DepthUpdateDecision::DropStaleOrDuplicate {
                            ..
                        } => {
                            context
                                .sink
                                .source_queue_backpressure
                                .record_structural_dedupe(
                                    "binance_depth_update",
                                    base_event.recv_time_utc_ms,
                                    1,
                                );
                            continue 'messages;
                        }
                        crate::sources::binance::DepthUpdateDecision::SourceGapResnapshot {
                            reason,
                        } => {
                            if resnapshot_attempted {
                                record_gap(
                                    &context.sink.gaps,
                                    format!(
                                        "binance_depth_update_stitch_gap_after_resnapshot:{reason}"
                                    ),
                                );
                                return Ok(BinanceDepthUpdateExit::StreamEnded {
                                    reason: format!("stitch_gap_after_resnapshot:{reason}"),
                                    saw_business,
                                });
                            }
                            record_gap(
                                &context.sink.gaps,
                                format!("binance_depth_update_stitch_gap:{reason}"),
                            );
                            let trigger = format!("stitch_gap:{reason}");
                            let next_snapshot_last_update_id = match fetch_binance_depth_snapshot(
                                &context.http_client,
                                &context.sink,
                                &context.collector_identity,
                                context.run_started,
                                &context.spec.snapshot_endpoint,
                                &trigger,
                            )
                            .await
                            {
                                Ok(snapshot_last_update_id) => snapshot_last_update_id,
                                Err(error) => {
                                    record_gap(
                                        &context.sink.gaps,
                                        format!("binance_depth_resync_failed:{reason}"),
                                    );
                                    return Err(error);
                                }
                            };
                            context.snapshot_last_update_id = next_snapshot_last_update_id;
                            context.snapshot_utc_day = recv_utc_day.clone();
                            context.pending_snapshot_trigger_reason = Some(trigger);
                            local_u = None;
                            local_u_before = None;
                            resnapshot_attempted = true;
                        }
                    }
                };
                let mut enriched = serde_json::from_str::<Value>(&raw_json).unwrap_or(Value::Null);
                if let Some(object) = enriched.as_object_mut() {
                    object.insert(
                        "snapshot_last_update_id".to_owned(),
                        json!(context.snapshot_last_update_id),
                    );
                    object.insert("local_u_before".to_owned(), json!(local_u_before));
                    object.insert(
                        "stitch_evidence".to_owned(),
                        json!(stitched.stitch_evidence),
                    );
                    if let Some(trigger_reason) = context.pending_snapshot_trigger_reason.take() {
                        object.insert("snapshot_trigger_reason".to_owned(), json!(trigger_reason));
                    }
                }
                base_event.raw_json = enriched.to_string();
                send_event(&context.sink, context.run_started, base_event).await?;
                context
                    .sink
                    .source_queue_backpressure
                    .set_recovery_active("binance_depth_update", false);
                saw_business = true;
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = stream.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: format!("ws_closed:{frame:?}"),
                    saw_business,
                });
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: format!("ws_error:{err}"),
                    saw_business,
                });
            }
            Ok(None) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: "ws_ended".to_owned(),
                    saw_business,
                });
            }
            Err(_) => {
                if deadline_reached(context.deadline) {
                    return Ok(BinanceDepthUpdateExit::DeadlineReached);
                }
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: "no_business_messages".to_owned(),
                    saw_business,
                });
            }
        }
    }
    Ok(BinanceDepthUpdateExit::DeadlineReached)
}

async fn handle_depth_source_control(control: DepthSourceControl) -> Result<bool> {
    let DepthSourceControl::Fence {
        stop_after_fence,
        resume_after_barrier,
        reply,
    } = control;
    let _ = reply.send(Ok(()));
    if let Some(resume) = resume_after_barrier {
        resume
            .await
            .map_err(|_| invalid_data("depth day-barrier owner dropped source resume signal"))?;
    }
    Ok(stop_after_fence)
}

fn finalized_parquet_files(source_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(source_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.ends_with(".parquet")
            && !file_name.ends_with("-running.parquet")
            && !file_name.ends_with("-digesting.parquet")
        {
            paths.push(entry.path());
        }
    }
    paths.sort_by_key(|path| finalized_parquet_order_key(path));
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

fn all_finalized_source_parquet_paths(
    dataset_root: &Path,
    source_kind: &str,
) -> Result<Vec<PathBuf>> {
    let typed_raw = dataset_root.join("training").join("typed_raw");
    let entries = match fs::read_dir(&typed_raw) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || !entry.file_name().to_string_lossy().starts_with("utc_day=")
        {
            continue;
        }
        paths.extend(finalized_parquet_files(
            &entry.path().join(format!("source_kind={source_kind}")),
        )?);
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

struct BinanceAggTradeRecovery {
    websocket_dedupe: BinanceRecentIdWindow,
    rest_dedupe: BinanceRecentIdWindow,
    continuity: BinanceAggTradeContinuityState,
}

struct StableStartupRecovery {
    utc_day: String,
    initial_next_boundary_ms: i64,
    binance_aggtrade: BinanceAggTradeRecovery,
    current_manifest: Value,
}

fn reconcile_stable_startup_recovery<F>(
    dataset_root: &Path,
    identity: &DailyManifestIdentity,
    mut clock_ms: F,
) -> Result<StableStartupRecovery>
where
    F: FnMut() -> i64,
{
    loop {
        let recovery_started_ms = clock_ms();
        let recovery_day = utc_day_string_from_ms(recovery_started_ms)
            .ok_or_else(|| invalid_data("current UTC day unavailable for Binance recovery"))?;
        let binance_aggtrade =
            restore_binance_aggtrade_recovery(dataset_root, &recovery_day, identity)?;
        let current_manifest = reconcile_open_day_manifest(
            &typed_raw_root(dataset_root).join(format!("utc_day={recovery_day}")),
            &recovery_day,
            identity,
            &binance_aggtrade.continuity,
        )?;
        let confirmed_ms = clock_ms();
        let confirmed_day = utc_day_string_from_ms(confirmed_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable after startup recovery"))?;
        if confirmed_day < recovery_day {
            return Err(invalid_data(format!(
                "UTC day moved backward during startup recovery: started={recovery_day} confirmed={confirmed_day}"
            ))
            .into());
        }
        if confirmed_day == recovery_day {
            return Ok(StableStartupRecovery {
                utc_day: recovery_day,
                initial_next_boundary_ms: confirmed_ms
                    .div_euclid(UTC_DAY_MS)
                    .saturating_add(1)
                    .saturating_mul(UTC_DAY_MS),
                binance_aggtrade,
                current_manifest,
            });
        }
    }
}

fn restore_binance_aggtrade_recovery(
    dataset_root: &Path,
    current_day: &str,
    current_identity: &DailyManifestIdentity,
) -> Result<BinanceAggTradeRecovery> {
    let days = utc_day_directories(dataset_root)?;
    if let Some((future_day, _)) = days.iter().find(|(day, _)| day.as_str() > current_day) {
        return Err(invalid_data(format!(
            "future UTC day cannot participate in recovery: {future_day}"
        ))
        .into());
    }
    let mut continuity = BinanceAggTradeContinuityState::default();
    for (day, day_dir) in days.iter().filter(|(day, _)| day.as_str() < current_day) {
        let Some(mut manifest) = load_day_manifest(day_dir)? else {
            if day_has_finalized_parquet(day_dir)? {
                return Err(io::Error::other(format!(
                    "prior UTC day has Parquet but no daily manifest: {day}"
                ))
                .into());
            }
            continue;
        };
        validate_manifest_day(&manifest, day)?;
        let state = manifest_state(&manifest)?;
        validate_manifest_recovery_files(day_dir, &manifest, state == "open")?;
        if let Some(manifest_continuity) = manifest_binance_continuity(&manifest)? {
            manifest_continuity.validate().map_err(invalid_data)?;
            continuity = manifest_continuity;
        }
        if state == "open" {
            overlay_binance_continuity_from_day(day_dir, &mut continuity)?;
            add_manifest_operational_counters(
                day_dir,
                &mut manifest,
                &required_source_counter_defaults(),
            )?;
            close_reconciled_day_manifest(day_dir, &mut manifest, &continuity)?;
        }
    }

    if let Some((day, day_dir)) = days.iter().find(|(day, _)| day == current_day) {
        if let Some(manifest) = load_day_manifest(day_dir)? {
            validate_manifest_day(&manifest, day)?;
            if manifest_state(&manifest)? == "closed" {
                return Err(io::Error::other(format!(
                    "current UTC day manifest is already closed: {day}"
                ))
                .into());
            }
            validate_manifest_recovery_files(day_dir, &manifest, true)?;
            if let Some(manifest_continuity) = manifest_binance_continuity(&manifest)? {
                manifest_continuity.validate().map_err(invalid_data)?;
                continuity = manifest_continuity;
            }
        } else {
            validate_unmanifested_recovery_files(day_dir, current_identity)?;
        }
        overlay_binance_continuity_from_day(day_dir, &mut continuity)?;
    }
    continuity.validate().map_err(invalid_data)?;

    let (websocket_dedupe, rest_dedupe) = restore_binance_transport_dedupe(dataset_root)?;
    Ok(BinanceAggTradeRecovery {
        websocket_dedupe,
        rest_dedupe,
        continuity,
    })
}

fn overlay_binance_continuity_from_day(
    day_dir: &Path,
    continuity: &mut BinanceAggTradeContinuityState,
) -> Result<()> {
    for path in finalized_parquet_files(&day_dir.join("source_kind=binance_aggtrade"))? {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("aggregate_trade_id").is_err() {
            continue;
        }
        let projection = ProjectionMask::columns(builder.parquet_schema(), ["aggregate_trade_id"]);
        let mut reader = builder
            .with_projection(projection)
            .with_batch_size(16)
            .build()?;
        for batch in &mut reader {
            let batch = batch?;
            let ids = batch
                .column(batch.schema().index_of("aggregate_trade_id")?)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| io::Error::other("Binance continuity ID is not UInt64"))?;
            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    return Err(io::Error::other(format!(
                        "Binance continuity ID missing in {}",
                        path.display()
                    ))
                    .into());
                }
                continuity
                    .recover_persisted_id(ids.value(row))
                    .map_err(invalid_data)?;
            }
        }
    }
    Ok(())
}

fn restore_binance_transport_dedupe(
    dataset_root: &Path,
) -> Result<(BinanceRecentIdWindow, BinanceRecentIdWindow)> {
    let mut websocket = BinanceRecentIdWindow::with_limit(BINANCE_AGGTRADE_RECOVERY_IDS);
    let mut rest = BinanceRecentIdWindow::with_limit(BINANCE_AGGTRADE_RECOVERY_IDS);
    let mut recovery_ids = BTreeSet::new();
    let mut paths = all_finalized_source_parquet_paths(dataset_root, "binance_aggtrade")?;
    paths.reverse();
    'files: for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("aggregate_trade_id").is_err()
            || builder
                .schema()
                .index_of("source_native_payload_json")
                .is_err()
        {
            continue;
        }
        if builder.metadata().file_metadata().num_rows() == 0 {
            continue;
        }
        let projection = ProjectionMask::columns(
            builder.parquet_schema(),
            ["aggregate_trade_id", "source_native_payload_json"],
        );
        for row_group in (0..builder.metadata().num_row_groups()).rev() {
            let row_group_builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
            let mut reader = row_group_builder
                .with_projection(projection.clone())
                .with_row_groups(vec![row_group])
                .with_batch_size(16)
                .build()?;
            for batch in &mut reader {
                let batch = batch?;
                let id_index = batch.schema().index_of("aggregate_trade_id")?;
                let payload_index = batch.schema().index_of("source_native_payload_json")?;
                let ids = batch
                    .column(id_index)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| io::Error::other("Binance restore ID is not UInt64"))?;
                let payloads = batch
                    .column(payload_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| io::Error::other("Binance restore payload is not UTF8"))?;
                for row in 0..batch.num_rows() {
                    if ids.is_null(row) || payloads.is_null(row) {
                        return Err(io::Error::other(format!(
                            "Binance restore key missing in {}",
                            path.display()
                        ))
                        .into());
                    }
                    let native_id = ids.value(row);
                    recovery_ids.insert(native_id);
                    while recovery_ids.len() > BINANCE_AGGTRADE_RECOVERY_IDS {
                        let Some(evicted) = recovery_ids.pop_first() else {
                            break;
                        };
                        websocket.remove(evicted);
                        rest.remove(evicted);
                    }
                    if !recovery_ids.contains(&native_id) {
                        continue;
                    }
                    let persisted_sources = payloads.value(row);
                    if persisted_sources.len() > MAX_TYPED_ROW_BYTES {
                        return Err(io::Error::other(format!(
                            "operator_intervention_required:Binance restore payload exceeds {MAX_TYPED_ROW_BYTES} bytes in {}",
                            path.display()
                        ))
                        .into());
                    }
                    let parsed = serde_json::from_str::<Value>(persisted_sources)?;
                    if parsed.get("websocket").is_some() {
                        if let Some(websocket_payload) =
                            parsed.get("websocket").and_then(Value::as_str)
                        {
                            let _ = websocket.observe(native_id, websocket_payload.as_bytes());
                        }
                        if let Some(rest_payload) =
                            parsed.get("rest_backfill").and_then(Value::as_str)
                        {
                            let _ = rest.observe(native_id, rest_payload.as_bytes());
                        }
                    } else {
                        let _ = websocket.observe(native_id, persisted_sources.as_bytes());
                    }
                }
            }
            if recovery_ids.len() == BINANCE_AGGTRADE_RECOVERY_IDS {
                break 'files;
            }
        }
    }
    Ok((websocket, rest))
}

async fn run_binance_ws_source(
    recovery: BinanceAggTradeRecovery,
    spec: BinanceWsSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    mut control_rx: mpsc::Receiver<AggTradeSourceControl>,
) -> Result<()> {
    let mut websocket_dedupe = recovery.websocket_dedupe;
    let mut rest_dedupe = recovery.rest_dedupe;
    let mut coordinator = AggTradeCoordinator::with_pending_limit(
        recovery.continuity,
        BINANCE_AGGTRADE_PENDING_MAX_ROWS,
    );
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(BINANCE_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BINANCE_HTTP_TOTAL_TIMEOUT_SECS))
        .build()?;
    let mut backfill_task = None::<AggTradeBackfillTask>;
    let mut pending_websocket_reservations = Vec::<InflightReservation>::new();
    let mut recovery = SourceRecoverySupervisor::new(
        "binance_aggtrade",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    let mut pending_stop = None::<PendingAggTradeStop>;
    let run_result: Result<()> = async {
        'connections: while !deadline_reached(deadline) {
            while let Ok(control) = control_rx.try_recv() {
                if let Some(stop) = handle_aggtrade_source_control(
                    control,
                    &mut coordinator,
                    &mut backfill_task,
                    &mut pending_websocket_reservations,
                    &sink,
                    &collector_identity,
                    &spec,
                    run_started,
                )
                .await?
                {
                    pending_stop = Some(stop);
                    break 'connections;
                }
            }
        let connect_timeout = deadline_remaining(deadline)
            .map(|remaining| remaining.min(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS)))
            .unwrap_or_else(|| Duration::from_secs(WS_CONNECT_TIMEOUT_SECS));
        if connect_timeout.is_zero() {
            break;
        }
        let (mut stream, _) = match timeout(connect_timeout, connect_async(&spec.url)).await {
            Ok(Ok(connected)) => connected,
            Ok(Err(err)) => {
                record_gap(
                    &sink.gaps,
                    format!("{}_ws_connect_error:{err}", spec.source_kind),
                );
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
            Err(_) => {
                if deadline_reached(deadline) {
                    break;
                }
                record_gap(
                    &sink.gaps,
                    format!("{}_ws_connect_timeout", spec.source_kind),
                );
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
        };

        let freshness_window = Duration::from_secs(spec.freshness_timeout_secs);
        let mut business_message_deadline = Instant::now() + freshness_window;
        let reconnect_reason = loop {
            if deadline_reached(deadline) {
                break 'connections;
            }
            let freshness_remaining =
                business_message_deadline.saturating_duration_since(Instant::now());
            if freshness_remaining.is_zero() {
                break "business_message_timeout".to_owned();
            }
            let read_timeout = deadline_remaining(deadline)
                .map(|remaining| remaining.min(freshness_remaining))
                .unwrap_or(freshness_remaining);
            let input = match backfill_task.as_mut() {
                Some(task) => tokio::select! {
                    biased;
                    control = control_rx.recv() => AggTradeLoopInput::Control(control),
                    backfill = task => AggTradeLoopInput::Backfill(backfill),
                    message = timeout(read_timeout, stream.next()) => {
                        AggTradeLoopInput::Websocket(message)
                    }
                },
                None => tokio::select! {
                    biased;
                    control = control_rx.recv() => AggTradeLoopInput::Control(control),
                    message = timeout(read_timeout, stream.next()) => {
                        AggTradeLoopInput::Websocket(message)
                    }
                },
            };
            match input {
                AggTradeLoopInput::Control(Some(control)) => {
                    if let Some(stop) = handle_aggtrade_source_control(
                        control,
                        &mut coordinator,
                        &mut backfill_task,
                        &mut pending_websocket_reservations,
                        &sink,
                        &collector_identity,
                        &spec,
                        run_started,
                    )
                    .await?
                    {
                        pending_stop = Some(stop);
                        break 'connections;
                    }
                    continue;
                }
                AggTradeLoopInput::Control(None) => {
                    return Err(invalid_data("aggTrade source control channel closed").into());
                }
                AggTradeLoopInput::Backfill(joined) => {
                    let _ = backfill_task.take();
                    let next_range = match joined {
                        Ok(Ok(budgeted_backfill)) => {
                            let BudgetedAggTradeBackfill {
                                rows: rest_rows,
                                reservations: rest_reservations,
                            } = budgeted_backfill;
                            for rest in &rest_rows {
                                let id = rest.shared.aggregate_trade_id;
                                match rest_dedupe
                                    .observe(id, rest.source_native_payload_json.as_bytes())
                                {
                                    BinanceIdDecision::Conflict => record_gap(
                                        &sink.gaps,
                                        format!("binance_aggtrade_rest_payload_conflict:id={id}"),
                                    ),
                                    BinanceIdDecision::OutsideWindow => record_gap(
                                        &sink.gaps,
                                        format!(
                                            "binance_aggtrade_rest_replay_outside_recovery_window:id={id}"
                                        ),
                                    ),
                                    BinanceIdDecision::Unique | BinanceIdDecision::Duplicate => {}
                                }
                            }
                            let page = coordinator
                                .accept_rest_page(rest_rows)
                                .map_err(invalid_data)?;
                            let mut next = emit_aggtrade_result(
                                page,
                                &sink,
                                &collector_identity,
                                &spec,
                                run_started,
                            )
                            .await?;
                            let complete = coordinator.complete_backfill().map_err(invalid_data)?;
                            let after_complete = emit_aggtrade_result(
                                complete,
                                &sink,
                                &collector_identity,
                                &spec,
                                run_started,
                            )
                            .await?;
                            if next.is_none() {
                                next = after_complete;
                            }
                            if coordinator.pending_rows() == 0 {
                                pending_websocket_reservations.clear();
                            }
                            drop(rest_reservations);
                            next
                        }
                        Ok(Err(error)) => {
                            let failed = coordinator
                                .fail_backfill(&error.to_string())
                                .map_err(invalid_data)?;
                            emit_aggtrade_result(
                                failed,
                                &sink,
                                &collector_identity,
                                &spec,
                                run_started,
                            )
                            .await?
                        }
                        Err(error) => {
                            let failed = coordinator
                                .fail_backfill(&format!("task_join_error:{error}"))
                                .map_err(invalid_data)?;
                            emit_aggtrade_result(
                                failed,
                                &sink,
                                &collector_identity,
                                &spec,
                                run_started,
                            )
                            .await?
                        }
                    };
                    if let Some(range) = next_range {
                        backfill_task = Some(spawn_aggtrade_backfill(
                            http_client.clone(),
                            spec.rest_endpoint.clone(),
                            range,
                            sink.inflight_budget.clone(),
                        ));
                    }
                    send_binance_continuity(&sink, run_started, coordinator.state().clone())
                        .await?;
                    continue;
                }
                AggTradeLoopInput::Websocket(message) => match message {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let raw_json = text.to_string();
                        let value = match serde_json::from_str::<Value>(&raw_json) {
                            Ok(value) => value,
                            Err(err) => {
                                return Err(invalid_data(format!(
                                    "{}_ws_invalid_json:{err}",
                                    spec.source_kind
                                ))
                                .into());
                            }
                        };
                        let payload = binance_ws_payload(&value);
                        let Some(event_type) = payload.get("e").and_then(Value::as_str) else {
                            return Err(invalid_data(format!(
                                "{}_ws_missing_event_type",
                                spec.source_kind
                            ))
                            .into());
                        };
                        let event_type = event_type.to_owned();
                        if event_type == "serverShutdown" {
                            break "server_shutdown".to_owned();
                        }
                        if event_type != spec.default_event_type {
                            return Err(invalid_data(format!(
                                "{}_ws_unexpected_event_type:{event_type}",
                                spec.source_kind
                            ))
                            .into());
                        }
                        let stream_name = value
                            .get("stream")
                            .and_then(Value::as_str)
                            .unwrap_or("btcusdt@aggTrade");
                        let websocket =
                            BinanceWsAggTrade::from_wire(payload, stream_name, now_ms())
                                .map_err(invalid_data)?;
                        let websocket_bytes = owned_binance_websocket_aggtrade_bytes(&websocket);
                        if websocket_bytes > MAX_TYPED_ROW_BYTES {
                            sink.source_queue_backpressure.record_operational_drop(
                                &spec.source_kind,
                                websocket.timestamp_received,
                                1,
                            );
                            return Err(invalid_data(format!(
                                "operator_intervention_required:binance_aggtrade_pending_row_too_large:bytes={websocket_bytes}:limit={MAX_TYPED_ROW_BYTES}"
                            ))
                            .into());
                        }
                        let websocket_reservation = sink
                            .inflight_budget
                            .acquire(1, websocket_bytes, BUFFER_ACQUIRE_TIMEOUT)
                            .await?;
                        let native_id = websocket.shared.aggregate_trade_id;
                        match websocket_dedupe
                            .observe(native_id, websocket.source_native_payload_json.as_bytes())
                        {
                            BinanceIdDecision::Duplicate => {
                                sink.source_queue_backpressure.record_structural_dedupe(
                                    &spec.source_kind,
                                    websocket.timestamp_received,
                                    1,
                                );
                                continue;
                            }
                            BinanceIdDecision::Conflict => record_gap(
                                &sink.gaps,
                                format!("binance_aggtrade_payload_conflict:id={native_id}"),
                            ),
                            BinanceIdDecision::OutsideWindow => record_gap(
                                &sink.gaps,
                                format!(
                                    "binance_aggtrade_replay_outside_recovery_window:id={native_id}"
                                ),
                            ),
                            BinanceIdDecision::Unique => {}
                        }
                        let result = coordinator
                            .accept_websocket(websocket)
                            .map_err(invalid_data)?;
                        let websocket_is_pending = !result
                            .outputs
                            .iter()
                            .any(|output| output.aggregate_trade_id() == native_id);
                        let next_range = emit_aggtrade_result(
                            result,
                            &sink,
                            &collector_identity,
                            &spec,
                            run_started,
                        )
                        .await?;
                        if websocket_is_pending {
                            pending_websocket_reservations.push(websocket_reservation);
                        }
                        if coordinator.pending_rows() == 0 {
                            pending_websocket_reservations.clear();
                        }
                        if let Some(range) = next_range {
                            if backfill_task.is_some() {
                                return Err(invalid_data(
                                    "Binance aggTrade started a second concurrent backfill",
                                )
                                .into());
                            }
                            backfill_task = Some(spawn_aggtrade_backfill(
                                http_client.clone(),
                                spec.rest_endpoint.clone(),
                                range,
                                sink.inflight_budget.clone(),
                            ));
                        }
                        send_binance_continuity(&sink, run_started, coordinator.state().clone())
                            .await?;
                        recovery.record_business_recovery();
                        sink.source_queue_backpressure
                            .set_recovery_active(&spec.source_kind, false);
                        business_message_deadline = Instant::now() + freshness_window;
                    }
                    Ok(Some(Ok(Message::Ping(payload)))) => {
                        if let Err(err) = stream.send(Message::Pong(payload)).await {
                            break format!("pong_send_error:{err}");
                        }
                    }
                    Ok(Some(Ok(Message::Close(frame)))) => {
                        break format!("closed:{frame:?}");
                    }
                    Ok(Some(Ok(_))) => {}
                    Ok(Some(Err(err))) => break format!("error:{err}"),
                    Ok(None) => break "stream_ended".to_owned(),
                    Err(_) if deadline_reached(deadline) => break 'connections,
                    Err(_) => break "business_message_timeout".to_owned(),
                },
            }
        };

        if reconnect_reason == "business_message_timeout" {
            sink.source_queue_backpressure
                .record_lag(&spec.source_kind, now_ms(), 1);
        }
        if reconnect_reason == "server_shutdown" {
            eprintln!("{}_ws_reconnect:server_shutdown", spec.source_kind);
            if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                break;
            }
        } else {
            record_gap(
                &sink.gaps,
                format!("{}_ws_reconnect:{reconnect_reason}", spec.source_kind),
            );
            if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                break;
            }
        }
        }
        Ok(())
    }
    .await;

    let current_utc_day = utc_day_string_from_ms(now_ms()).unwrap_or_else(|| "unknown".to_owned());
    let (flush_reason, flush_utc_day) = pending_stop.as_ref().map_or_else(
        || {
            (
                run_result.as_ref().err().map_or_else(
                    || "source_deadline".to_owned(),
                    |error| format!("source_exit:{error}"),
                ),
                current_utc_day,
            )
        },
        |stop| (stop.reason.clone(), stop.utc_day.clone()),
    );
    let flush_result = flush_aggtrade_pending_state(
        &mut coordinator,
        &mut backfill_task,
        &mut pending_websocket_reservations,
        &sink,
        &collector_identity,
        &spec,
        run_started,
        &flush_reason,
        &flush_utc_day,
    )
    .await;
    let failure = match (run_result.err(), flush_result.err()) {
        (None, None) => None,
        (Some(error), None) => Some(error.to_string()),
        (None, Some(error)) => Some(error.to_string()),
        (Some(run_error), Some(flush_error)) => Some(format!(
            "{run_error}; aggTrade coordinated flush failed:{flush_error}"
        )),
    };
    if let Some(stop) = pending_stop {
        let reply = failure.clone().map_or(Ok(()), Err);
        let _ = stop.reply.send(reply);
    }
    match failure {
        Some(error) => Err(io::Error::other(error).into()),
        None => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_aggtrade_source_control(
    control: AggTradeSourceControl,
    coordinator: &mut AggTradeCoordinator,
    backfill_task: &mut Option<AggTradeBackfillTask>,
    pending_websocket_reservations: &mut Vec<InflightReservation>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    run_started: Instant,
) -> Result<Option<PendingAggTradeStop>> {
    let AggTradeSourceControl::Flush {
        reason,
        utc_day,
        stop_after_flush,
        resume_after_barrier,
        reply,
    } = control;
    if stop_after_flush {
        return Ok(Some(PendingAggTradeStop {
            reason,
            utc_day,
            reply,
        }));
    }
    let flush_result = flush_aggtrade_pending_state(
        coordinator,
        backfill_task,
        pending_websocket_reservations,
        sink,
        collector_identity,
        spec,
        run_started,
        &reason,
        &utc_day,
    )
    .await;
    match flush_result {
        Ok(()) => {
            let _ = reply.send(Ok(()));
            if let Some(resume) = resume_after_barrier {
                resume.await.map_err(|_| {
                    invalid_data("aggTrade day-barrier owner dropped source resume signal")
                })?;
            }
            Ok(None)
        }
        Err(error) => {
            let error = error.to_string();
            let _ = reply.send(Err(error.clone()));
            Err(io::Error::other(error).into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_aggtrade_pending_state(
    coordinator: &mut AggTradeCoordinator,
    backfill_task: &mut Option<AggTradeBackfillTask>,
    pending_websocket_reservations: &mut Vec<InflightReservation>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    run_started: Instant,
    reason: &str,
    utc_day: &str,
) -> Result<()> {
    if let Some(task) = backfill_task.take() {
        task.abort();
        let _ = task.await;
    }
    let result = coordinator
        .flush_pending_without_backfill(reason)
        .map_err(invalid_data)?;
    let next_range = emit_aggtrade_result_for_day(
        result,
        sink,
        collector_identity,
        spec,
        run_started,
        Some(utc_day),
    )
    .await?;
    if next_range.is_some()
        || coordinator.active_backfill().is_some()
        || coordinator.pending_rows() != 0
    {
        return Err(invalid_data("aggTrade coordinated flush left pending recovery state").into());
    }
    pending_websocket_reservations.clear();
    send_binance_continuity_for_day(
        sink,
        run_started,
        coordinator.state().clone(),
        utc_day.to_owned(),
    )
    .await
}

struct BudgetedAggTradeBackfill {
    rows: Vec<BinanceRestAggTrade>,
    reservations: Vec<InflightReservation>,
}

type AggTradeBackfillTask = tokio::task::JoinHandle<Result<BudgetedAggTradeBackfill>>;

enum AggTradeLoopInput {
    Control(Option<AggTradeSourceControl>),
    Backfill(std::result::Result<Result<BudgetedAggTradeBackfill>, tokio::task::JoinError>),
    Websocket(
        std::result::Result<
            Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>,
            tokio::time::error::Elapsed,
        >,
    ),
}

fn spawn_aggtrade_backfill(
    client: reqwest::Client,
    endpoint: String,
    range: AggTradeBackfillRange,
    inflight_budget: InflightBudget,
) -> AggTradeBackfillTask {
    tokio::spawn(async move {
        let rows = fetch_binance_aggtrade_range(&client, &endpoint, range).await?;
        let mut reservations = Vec::with_capacity(rows.len());
        for row in &rows {
            let bytes = owned_binance_rest_aggtrade_bytes(row);
            if bytes > MAX_TYPED_ROW_BYTES {
                return Err(invalid_data(format!(
                    "operator_intervention_required:binance_aggtrade_backfill_row_too_large:bytes={bytes}:limit={MAX_TYPED_ROW_BYTES}"
                ))
                .into());
            }
            reservations.push(
                inflight_budget
                    .acquire(1, bytes, BUFFER_ACQUIRE_TIMEOUT)
                    .await?,
            );
        }
        Ok(BudgetedAggTradeBackfill { rows, reservations })
    })
}

async fn emit_aggtrade_result(
    result: AggTradeCoordinatorResult,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    run_started: Instant,
) -> Result<Option<AggTradeBackfillRange>> {
    emit_aggtrade_result_for_day(result, sink, collector_identity, spec, run_started, None).await
}

async fn emit_aggtrade_result_for_day(
    result: AggTradeCoordinatorResult,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    run_started: Instant,
    gap_utc_day: Option<&str>,
) -> Result<Option<AggTradeBackfillRange>> {
    for gap in result.gaps {
        match gap_utc_day {
            Some(utc_day) => record_gap_for_day(&sink.gaps, utc_day, gap),
            None => record_gap(&sink.gaps, gap),
        }
    }
    for output in result.outputs {
        send_event(
            sink,
            run_started,
            binance_aggtrade_raw_event(&output, collector_identity, spec)?,
        )
        .await?;
    }
    Ok(result.start_backfill)
}

fn binance_aggtrade_raw_event(
    output: &BinanceAggTradeOutput,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
) -> Result<RawEvent> {
    let row = BinanceAggTradeHourlyRow::from_output(
        output,
        &collector_identity.host_id,
        &collector_identity.region,
        &spec.url,
        &spec.rest_endpoint,
    )
    .map_err(invalid_data)?;
    Ok(RawEvent {
        source_kind: spec.source_kind.clone(),
        source_event_type: spec.default_event_type.clone(),
        source_identifier: row.symbol.clone(),
        source_ts: Some(row.trade_time.to_string()),
        recv_time_utc_ms: row.timestamp_received,
        collector_host_id: row.collector_host_id.clone(),
        collector_region: row.collector_region.clone(),
        source_endpoint: row.source_endpoint.clone(),
        raw_json: json!({
            "timestamp_received": row.timestamp_received,
            "websocket_timestamp_received": row.websocket_timestamp_received,
            "rest_backfill_timestamp_received": row.rest_backfill_timestamp_received,
            "ingest_source": row.ingest_source,
            "event_type": row.event_type,
            "event_time": row.event_time,
            "trade_time": row.trade_time,
            "symbol": row.symbol,
            "aggregate_trade_id": row.aggregate_trade_id,
            "price": row.price,
            "quantity": row.quantity,
            "first_trade_id": row.first_trade_id,
            "last_trade_id": row.last_trade_id,
            "buyer_is_maker": row.buyer_is_maker,
            "stream": row.stream,
            "ws_ignore": row.ws_ignore,
            "rest_was_best_price_match": row.rest_was_best_price_match,
            "source_native_payload_json": row.source_native_payload_json,
        })
        .to_string(),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: spec.source_kind.clone(),
    })
}

fn binance_ws_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

async fn discover_markets_with_wait(
    gamma: &GammaClient,
    wait_secs: u64,
) -> Result<BtcUpdownDiscovery> {
    let started_ms = now_ms();
    let final_error = loop {
        let query_ms = now_ms();
        let windows = discovery_windows(query_ms);
        let current = fetch_exact_slug_candidates(gamma, &windows.current.slug).await?;
        match select_btc_updown_markets(query_ms, &current, &[]) {
            Ok(discovery) => return Ok(discovery),
            Err(err) => {
                let last_error = format!("{err:?}");
                if now_ms().saturating_sub(started_ms) >= (wait_secs * 1000) as i64 {
                    break last_error;
                }
                sleep(Duration::from_secs(10)).await;
            }
        }
    };
    Err(format!(
        "btc-updown-5m current discovery failed after wait_secs={wait_secs}: {final_error}"
    )
    .into())
}

async fn fetch_exact_slug_candidates(
    gamma: &GammaClient,
    slug: &str,
) -> Result<Vec<BtcUpdownMarketCandidate>> {
    let mut request = MarketsRequest::default();
    request.slug = vec![slug.to_owned()];
    request.closed = Some(false);
    request.limit = Some(10);
    let metadata_fetch_start_utc_ms = now_ms();
    let markets = gamma.markets(&request).await?;
    let metadata_received_utc_ms = now_ms();
    Ok(markets
        .iter()
        .map(|market| {
            let mut candidate = BtcUpdownMarketCandidate::from_sdk_market(market);
            candidate.metadata_fetch_start_utc_ms = Some(metadata_fetch_start_utc_ms);
            candidate.metadata_received_utc_ms = Some(metadata_received_utc_ms);
            candidate
        })
        .collect())
}

fn bounded_ws_config() -> WsConfig {
    let mut config = WsConfig::default();
    config.reconnect.max_attempts = Some(3);
    config.reconnect.max_backoff = Duration::from_secs(5);
    config.heartbeat_interval = Duration::from_secs(10);
    config.heartbeat_timeout = Duration::from_secs(20);
    config
}

async fn send_event(sink: &SourceEventSink, run_started: Instant, event: RawEvent) -> Result<()> {
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let source_kind = event.source_kind.clone();
    let recv_time_utc_ms = event.recv_time_utc_ms;
    let owned_bytes = owned_event_bytes(&event);
    if owned_bytes > MAX_TYPED_ROW_BYTES {
        let gap = format!(
            "operator_intervention_required:max_typed_row_bytes_exceeded:{source_kind}:bytes={owned_bytes}:limit={MAX_TYPED_ROW_BYTES}"
        );
        sink.source_queue_backpressure
            .record_operational_drop(&source_kind, recv_time_utc_ms, 1);
        record_gap(&sink.gaps, gap.clone());
        return Err(invalid_data(gap).into());
    }
    let reservation = match sink
        .inflight_budget
        .acquire(1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            sink.source_queue_backpressure.record_timeout();
            sink.source_queue_backpressure.record_drop(1);
            sink.source_queue_backpressure.record_operational_drop(
                &source_kind,
                recv_time_utc_ms,
                1,
            );
            let gap = format!("global_inflight_budget_failed:{source_kind}:{error}");
            record_gap(&sink.gaps, gap);
            return Err(error.into());
        }
    };
    let inflight_event = InflightRawEvent {
        event,
        control: None,
        _source_recv_monotonic_ms: monotonic_ms(run_started),
        _reservation: Some(reservation),
    };
    match sink.tx.try_send(inflight_event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(inflight_event)) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.send(inflight_event)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => {
                    sink.source_queue_backpressure.record_drop(1);
                    sink.source_queue_backpressure.record_operational_drop(
                        &source_kind,
                        recv_time_utc_ms,
                        1,
                    );
                    let gap = format!("bounded_queue_send_failed:{source_kind}:{err}");
                    record_gap(&sink.gaps, gap.clone());
                    Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into())
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    sink.source_queue_backpressure.record_drop(1);
                    sink.source_queue_backpressure.record_operational_drop(
                        &source_kind,
                        recv_time_utc_ms,
                        1,
                    );
                    let gap = format!("bounded_queue_backpressure_timeout:{source_kind}");
                    record_gap(&sink.gaps, gap.clone());
                    Err(io::Error::new(io::ErrorKind::TimedOut, gap).into())
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            sink.source_queue_backpressure.record_drop(1);
            sink.source_queue_backpressure.record_operational_drop(
                &source_kind,
                recv_time_utc_ms,
                1,
            );
            let gap = format!("bounded_queue_send_closed:{source_kind}");
            record_gap(&sink.gaps, gap.clone());
            Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into())
        }
    }
}

async fn send_ingress_control(
    sink: &SourceEventSink,
    run_started: Instant,
    control: IngressControl,
    label: &str,
) -> Result<()> {
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let message = InflightRawEvent {
        event: RawEvent {
            source_kind: "__ingress_control".to_owned(),
            source_event_type: label.to_owned(),
            source_identifier: label.to_owned(),
            source_ts: None,
            recv_time_utc_ms: now_ms(),
            collector_host_id: String::new(),
            collector_region: String::new(),
            source_endpoint: String::new(),
            raw_json: "{}".to_owned(),
            parquet_raw_layout_version: 0,
            parquet_raw_source_kind: "__ingress_control".to_owned(),
        },
        control: Some(control),
        _source_recv_monotonic_ms: monotonic_ms(run_started),
        _reservation: None,
    };
    match sink.tx.try_send(message) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(message)) => {
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.send(message)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("ingress control queue closed: {label}"),
                )
                .into()),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("ingress control queue timeout: {label}"),
                )
                .into()),
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("ingress control queue closed: {label}"),
        )
        .into()),
    }
}

async fn send_day_barrier(
    sink: &SourceEventSink,
    run_started: Instant,
    producer: &str,
    utc_day: String,
) -> Result<()> {
    send_ingress_control(
        sink,
        run_started,
        IngressControl::DayBarrier {
            producer: producer.to_owned(),
            utc_day,
        },
        "utc_day_barrier",
    )
    .await
}

async fn send_binance_continuity(
    sink: &SourceEventSink,
    run_started: Instant,
    continuity: BinanceAggTradeContinuityState,
) -> Result<()> {
    let utc_day = utc_day_string_from_ms(now_ms())
        .ok_or_else(|| invalid_data("current UTC day unavailable for Binance continuity"))?;
    send_binance_continuity_for_day(sink, run_started, continuity, utc_day).await
}

async fn send_binance_continuity_for_day(
    sink: &SourceEventSink,
    run_started: Instant,
    continuity: BinanceAggTradeContinuityState,
    utc_day: String,
) -> Result<()> {
    send_ingress_control(
        sink,
        run_started,
        IngressControl::BinanceContinuity {
            utc_day,
            continuity,
        },
        "binance_aggtrade_continuity",
    )
    .await
}

fn raw_event(
    source_kind: &str,
    source_event_type: &str,
    source_identifier: &str,
    source_ts: Option<String>,
    source_endpoint: &str,
    collector_identity: &CollectorIdentity,
    raw: Value,
) -> RawEvent {
    RawEvent {
        source_kind: source_kind.to_owned(),
        source_event_type: source_event_type.to_owned(),
        source_identifier: source_identifier.to_owned(),
        source_ts,
        recv_time_utc_ms: now_ms(),
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        source_endpoint: source_endpoint.to_owned(),
        raw_json: raw.to_string(),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind.to_owned(),
    }
}

fn with_source_native_payload_json(mut value: Value, source_native_payload_json: &str) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "source_native_payload_json".to_owned(),
            Value::String(source_native_payload_json.to_owned()),
        );
    }
    value
}

fn with_polymarket_row_context(
    mut value: Value,
    slug: Option<&str>,
    source_native_payload_json: &str,
) -> Value {
    if let Some(object) = value.as_object_mut()
        && let Some(slug) = slug
    {
        object.insert("slug".to_owned(), Value::String(slug.to_owned()));
    }
    with_source_native_payload_json(value, source_native_payload_json)
}

fn polymarket_book_raw_json(event: &BookUpdate, slug: &str) -> Value {
    json!({
        "event_type": "book",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "timestamp": event.timestamp,
        "bids": event
            .bids
            .iter()
            .map(|level| json!([level.price.to_string(), level.size.to_string()]))
            .collect::<Vec<_>>(),
        "asks": event
            .asks
            .iter()
            .map(|level| json!([level.price.to_string(), level.size.to_string()]))
            .collect::<Vec<_>>(),
        "hash": event.hash,
    })
}

fn polymarket_price_change_raw_json(
    event: &PriceChange,
    asset_to_slug: &BTreeMap<String, String>,
) -> Value {
    json!({
        "event_type": "price_change",
        "market": event.market.to_string(),
        "timestamp": event.timestamp,
        "price_changes": event
            .price_changes
            .iter()
            .map(|change| {
                let asset_id = change.asset_id.to_string();
                json!({
                    "asset_id": asset_id,
                    "slug": asset_to_slug.get(&asset_id),
                    "price": change.price.to_string(),
                    "size": change.size.as_ref().map(ToString::to_string),
                    "side": change.side.to_string(),
                    "hash": change.hash.clone(),
                    "best_bid": change.best_bid.as_ref().map(ToString::to_string),
                    "best_ask": change.best_ask.as_ref().map(ToString::to_string),
                })
            })
            .collect::<Vec<_>>()
    })
}

fn polymarket_last_trade_raw_json(event: &LastTradePrice, slug: &str) -> Value {
    json!({
        "event_type": "last_trade_price",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "price": event.price.to_string(),
        "side": event.side.as_ref().map(ToString::to_string),
        "size": event.size.as_ref().map(ToString::to_string),
        "fee_rate_bps": event.fee_rate_bps.as_ref().map(ToString::to_string),
        "timestamp": event.timestamp,
    })
}

fn polymarket_tick_size_change_raw_json(event: &TickSizeChange, slug: &str) -> Value {
    json!({
        "event_type": "tick_size_change",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "old_tick_size": event.old_tick_size.to_string(),
        "new_tick_size": event.new_tick_size.to_string(),
        "timestamp": event.timestamp,
    })
}

fn polymarket_new_market_raw_json(event: &NewMarket) -> Value {
    json!({
        "event_type": "new_market",
        "id": event.id,
        "question": event.question,
        "market": event.market.to_string(),
        "slug": event.slug,
        "description": event.description,
        "asset_ids": event
            .asset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "outcomes": event.outcomes,
        "metadata_unavailable_reason": "market_ws_new_market_payload_lacks_market_start_end_active_accepting_orders_tick_size_min_size_fee_schedule_reward_eligibility",
        "event_message": event.event_message.as_ref().map(|message| {
            json!({
                "id": message.id,
                "ticker": message.ticker,
                "slug": message.slug,
                "title": message.title,
                "description": message.description,
            })
        }),
        "timestamp": event.timestamp,
    })
}

fn polymarket_fallback_close_raw_json_from_resolved(event: &MarketResolved) -> Value {
    let asset_ids = event
        .asset_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    json!({
        "event_type": "fallback_close",
        "metadata_source": "fallback_close",
        "id": event.id,
        "question": event.question,
        "condition_id": event.market.to_string(),
        "clob_market_id": event.id,
        "market": event.market.to_string(),
        "slug": event.slug.clone().unwrap_or_else(|| event.market.to_string()),
        "description": event.description,
        "asset_ids": asset_ids.clone(),
        "active": false,
        "accepting_orders": false,
        "market_lifecycle_state": "retired",
        "tick_size": Value::Null,
        "min_size": Value::Null,
        "fee_schedule_raw": Value::Null,
        "reward_eligibility_raw": Value::Null,
        "metadata_unavailable_reason": "market_resolved_ws_payload_lacks_market_start_end_tick_size_min_size_fee_schedule_reward_eligibility",
        "same_condition_market_verified": true,
        "fallback_close_reason": "market_resolved",
        "timestamp": event.timestamp,
    })
}

fn polymarket_market_resolved_raw_json(event: &MarketResolved) -> Value {
    let asset_ids = event
        .asset_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    json!({
        "event_type": "market_resolved",
        "id": event.id,
        "question": event.question,
        "market": event.market.to_string(),
        "slug": event.slug,
        "description": event.description,
        "asset_ids": asset_ids,
        "resolved_event_asset_ids": asset_ids,
        "outcomes": event.outcomes,
        "winning_asset_id": event.winning_asset_id.to_string(),
        "winning_outcome": event.winning_outcome,
        "close_reason_if_present": Value::Null,
        "event_message": event.event_message.as_ref().map(|message| {
            json!({
                "id": message.id,
                "ticker": message.ticker,
                "slug": message.slug,
                "title": message.title,
                "description": message.description,
            })
        }),
        "timestamp": event.timestamp,
    })
}

fn update_coverage(coverage: &mut BTreeMap<String, CoverageStats>, event: &RawEvent) {
    let stats = coverage.entry(event.source_kind.clone()).or_default();
    stats.rows = stats.rows.saturating_add(1);
    *stats
        .event_type_counts
        .entry(event.source_event_type.clone())
        .or_default() += 1;
    stats.first_recv_time_utc_ms = stats
        .first_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.min(event.recv_time_utc_ms))
        });
    stats.last_recv_time_utc_ms = stats
        .last_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.max(event.recv_time_utc_ms))
        });
    if event.source_kind == "polymarket_market"
        && let Some((slug, _)) = event.source_identifier.split_once(':')
        && slug.starts_with("btc-updown-5m-")
    {
        stats.market_windows.insert(slug.to_owned());
        stats
            .market_window_first_recv_time_utc_ms
            .entry(slug.to_owned())
            .and_modify(|value| *value = (*value).min(event.recv_time_utc_ms))
            .or_insert(event.recv_time_utc_ms);
    }
}

fn discovery_asset_to_slug(discovery: &BtcUpdownDiscovery) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(
        discovery.current.up_asset_id.clone(),
        discovery.current.slug.clone(),
    );
    map.insert(
        discovery.current.down_asset_id.clone(),
        discovery.current.slug.clone(),
    );
    if let Some(next) = &discovery.next {
        map.insert(next.up_asset_id.clone(), next.slug.clone());
        map.insert(next.down_asset_id.clone(), next.slug.clone());
    }
    map
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn record_gap(gaps: &SharedSourceGaps, gap: String) {
    let utc_day = utc_day_string_from_ms(now_ms()).unwrap_or_else(|| "unknown".to_owned());
    record_gap_for_day(gaps, &utc_day, gap);
}

fn record_gap_for_day(gaps: &SharedSourceGaps, utc_day: &str, gap: String) {
    if !is_live_runtime_source_gap_reason(&gap) {
        return;
    }
    let mut store = gaps.lock().expect("source gap mutex poisoned");
    push_bounded_gap(&mut store.all, gap.clone());
    push_bounded_gap(store.by_utc_day.entry(utc_day.to_owned()).or_default(), gap);
}

fn push_bounded_gap(gaps: &mut Vec<String>, gap: String) {
    if gaps.iter().any(|existing| existing == &gap) {
        return;
    }
    if gaps.len() < MAX_RECORDED_SOURCE_GAPS {
        gaps.push(gap);
        return;
    }
    if let Some(existing) = gaps
        .iter_mut()
        .find(|item| item.starts_with(SOURCE_GAP_OVERFLOW_PREFIX))
    {
        let next_count = existing
            .trim_start_matches(SOURCE_GAP_OVERFLOW_PREFIX)
            .parse::<u64>()
            .unwrap_or(0)
            .saturating_add(1);
        *existing = format!("{SOURCE_GAP_OVERFLOW_PREFIX}{next_count}");
    } else {
        gaps.pop();
        gaps.push(format!("{SOURCE_GAP_OVERFLOW_PREFIX}2"));
    }
}

fn rolling_average_bytes(previous_average: u64, current_value: u64, observed_rows: u64) -> u64 {
    if observed_rows <= 1 {
        return current_value.max(1);
    }
    let prior_weight = observed_rows.saturating_sub(1);
    previous_average
        .saturating_mul(prior_weight)
        .saturating_add(current_value)
        / observed_rows
}

fn estimated_event_bytes(event: &RawEvent) -> u64 {
    event.source_kind.len() as u64
        + event.source_event_type.len() as u64
        + event.source_identifier.len() as u64
        + event.source_ts.as_deref().unwrap_or_default().len() as u64
        + event.collector_host_id.len() as u64
        + event.collector_region.len() as u64
        + event.source_endpoint.len() as u64
        + event.raw_json.len() as u64
        + event.parquet_raw_source_kind.len() as u64
}

fn owned_event_bytes(event: &RawEvent) -> usize {
    std::mem::size_of::<RawEvent>()
        .saturating_add(event.source_kind.capacity())
        .saturating_add(event.source_event_type.capacity())
        .saturating_add(event.source_identifier.capacity())
        .saturating_add(event.source_ts.as_ref().map_or(0, String::capacity))
        .saturating_add(event.collector_host_id.capacity())
        .saturating_add(event.collector_region.capacity())
        .saturating_add(event.source_endpoint.capacity())
        .saturating_add(event.raw_json.capacity())
        .saturating_add(event.parquet_raw_source_kind.capacity())
}

fn owned_binance_websocket_aggtrade_bytes(row: &BinanceWsAggTrade) -> usize {
    std::mem::size_of::<BinanceWsAggTrade>()
        .saturating_add(row.event_type.capacity())
        .saturating_add(row.symbol.capacity())
        .saturating_add(row.stream.capacity())
        .saturating_add(row.shared.price.capacity())
        .saturating_add(row.shared.quantity.capacity())
        .saturating_add(row.source_native_payload_json.capacity())
        .saturating_add(128)
}

fn owned_binance_rest_aggtrade_bytes(row: &BinanceRestAggTrade) -> usize {
    std::mem::size_of::<BinanceRestAggTrade>()
        .saturating_add(row.symbol.capacity())
        .saturating_add(row.shared.price.capacity())
        .saturating_add(row.shared.quantity.capacity())
        .saturating_add(row.source_native_payload_json.capacity())
}

fn training_dataset_size_bytes(dataset_root: &Path) -> io::Result<u64> {
    let training_root = dataset_root.join("training");
    let metadata = match fs::symlink_metadata(&training_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "training dataset root must be a real directory: {}",
                training_root.display()
            ),
        ));
    }
    directory_tree_size_bytes(&training_root)
}

fn lock_training_dataset_root(dataset_root: &Path) -> Result<File> {
    let directory = File::open(dataset_root)?;
    directory.try_lock().map_err(|error| {
        invalid_data(format!(
            "operator_intervention_required:training_dataset_root_locked:{}:{error}",
            dataset_root.display()
        ))
    })?;
    Ok(directory)
}

fn directory_tree_size_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "symlink is forbidden in training dataset: {}",
                    entry.path().display()
                ),
            ));
        }
        if file_type.is_dir() {
            total = total.saturating_add(directory_tree_size_bytes(&entry.path())?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "special file is forbidden in training dataset: {}",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok(total)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn monotonic_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

#[derive(Clone)]
struct CollectorIdentity {
    host_id: String,
    region: String,
}

struct BinanceWsSpec {
    source_kind: String,
    default_event_type: String,
    url: String,
    rest_endpoint: String,
    freshness_timeout_secs: u64,
}

#[derive(Clone)]
struct BinanceDepthSpec {
    websocket_endpoint: String,
    snapshot_endpoint: String,
}

impl BinanceDepthSpec {
    fn production() -> Self {
        Self {
            websocket_endpoint: BINANCE_DEPTH_WS.to_owned(),
            snapshot_endpoint: BINANCE_DEPTH_SNAPSHOT.to_owned(),
        }
    }
}

#[derive(Debug)]
enum BinanceDepthUpdateExit {
    DeadlineReached,
    Stopped,
    StreamEnded { reason: String, saw_business: bool },
}

struct BinanceDepthUpdateContext {
    http_client: reqwest::Client,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    spec: BinanceDepthSpec,
    snapshot_last_update_id: u64,
    snapshot_utc_day: String,
    pending_snapshot_trigger_reason: Option<String>,
}

type SourceTaskHandle = (&'static str, tokio::task::JoinHandle<Result<()>>);

fn finished_source_task(handles: &[SourceTaskHandle]) -> Option<&'static str> {
    handles
        .iter()
        .find_map(|(source_kind, handle)| handle.is_finished().then_some(*source_kind))
}

struct SourceWriterHandle {
    tx: std::sync::mpsc::SyncSender<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    accepting_commands: Arc<Mutex<bool>>,
    result_rx: std::sync::mpsc::Receiver<Result<SourceWriterOutput>>,
    completed_result: Option<Result<SourceWriterOutput>>,
}

enum WriterCommand {
    Events(Vec<InflightRawEvent>),
    FinalizeDay { utc_day: String },
}

#[derive(Clone, Debug, Serialize)]
struct SourceWriterOutput {
    source_kind: String,
    finalized_segments: Vec<LiveFinalizedSegment>,
    stats: SourceWriterStats,
}

struct WriterStopOutcome {
    outputs: Vec<SourceWriterOutput>,
    errors: Vec<String>,
    all_terminal: bool,
}

#[derive(Clone, Debug, Serialize)]
struct SourceWriterStats {
    source_kind: String,
    events_received: u64,
    events_written: u64,
    events_dropped: u64,
    typed_rows_written: u64,
    typed_rows_dropped: u64,
    finalized_segments: u64,
    finalized_segment_rows: u64,
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    write_events_us: LatencyStats,
}

impl SourceWriterStats {
    fn empty(source_kind: String) -> Self {
        Self {
            source_kind,
            events_received: 0,
            events_written: 0,
            events_dropped: 0,
            typed_rows_written: 0,
            typed_rows_dropped: 0,
            finalized_segments: 0,
            finalized_segment_rows: 0,
            first_recv_time_utc_ms: None,
            last_recv_time_utc_ms: None,
            write_events_us: LatencyStats::from_values(&[]),
        }
    }
}

struct SourceWriterStatsBuilder {
    stats: SourceWriterStats,
    write_events_us: LatencyAccumulator,
}

impl SourceWriterStatsBuilder {
    fn new(source_kind: String) -> Self {
        Self {
            stats: SourceWriterStats::empty(source_kind),
            write_events_us: LatencyAccumulator::default(),
        }
    }

    fn record_written_event(&mut self, recv_time_utc_ms: i64, typed_rows: u64, write_us: u64) {
        self.stats.events_received = self.stats.events_received.saturating_add(1);
        self.stats.events_written = self.stats.events_written.saturating_add(1);
        self.stats.typed_rows_written = self.stats.typed_rows_written.saturating_add(typed_rows);
        self.stats.first_recv_time_utc_ms = Some(
            self.stats
                .first_recv_time_utc_ms
                .map_or(recv_time_utc_ms, |current| current.min(recv_time_utc_ms)),
        );
        self.stats.last_recv_time_utc_ms = Some(
            self.stats
                .last_recv_time_utc_ms
                .map_or(recv_time_utc_ms, |current| current.max(recv_time_utc_ms)),
        );
        self.write_events_us.observe(write_us);
    }

    fn finish(mut self, finalized_segments: &[LiveFinalizedSegment]) -> SourceWriterStats {
        self.stats.finalized_segments = finalized_segments.len() as u64;
        self.stats.finalized_segment_rows = finalized_segments
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        self.stats.write_events_us = self.write_events_us.finish();
        self.stats
    }
}

struct LatencyAccumulator {
    count: u64,
    min: Option<u64>,
    max: u64,
    buckets: [u64; 65],
}

impl Default for LatencyAccumulator {
    fn default() -> Self {
        Self {
            count: 0,
            min: None,
            max: 0,
            buckets: [0; 65],
        }
    }
}

impl LatencyAccumulator {
    fn observe(&mut self, value: u64) {
        self.count = self.count.saturating_add(1);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = self.max.max(value);
        self.buckets[latency_bucket(value)] = self.buckets[latency_bucket(value)].saturating_add(1);
    }

    fn finish(&self) -> LatencyStats {
        if self.count == 0 {
            return LatencyStats::from_values(&[]);
        }
        LatencyStats {
            count: self.count,
            min: self.min.unwrap_or(0),
            p50: self.percentile(50),
            p95: self.percentile(95),
            p99: self.percentile(99),
            max: self.max,
        }
    }

    fn percentile(&self, percentile: u64) -> u64 {
        let target_rank = self.count.saturating_mul(percentile).saturating_add(99) / 100;
        let target_rank = target_rank.max(1);
        let mut cumulative = 0_u64;
        for (bucket, count) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            if cumulative >= target_rank {
                return latency_bucket_upper_bound(bucket);
            }
        }
        self.max
    }
}

fn latency_bucket(value: u64) -> usize {
    if value == 0 {
        0
    } else {
        (64 - value.leading_zeros()) as usize
    }
}

fn latency_bucket_upper_bound(bucket: usize) -> u64 {
    match bucket {
        0 => 0,
        64.. => u64::MAX,
        _ => (1_u64 << bucket) - 1,
    }
}

struct ActiveSegment {
    source_kind: String,
    writer: LiveSegmentWriter,
    utc_day: String,
    utc_hour: String,
    hour_start_utc_ms: i64,
    hour_end_utc_ms: i64,
    hour_part: u32,
    segment_start_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    bytes: u64,
    event_type_counts: BTreeMap<String, u64>,
    backfill_rows: u64,
    book_rows_with_bids_asks: u64,
    covered_market_slugs: BTreeSet<String>,
    covered_condition_ids: BTreeSet<String>,
    covered_asset_ids: BTreeSet<String>,
    depth_snapshot_last_update_id: Option<u64>,
    depth_stitch_applied_rows: u64,
    depth_stitch_gap_rows: u64,
    depth_resnapshot_count: u64,
    depth_local_u_min: Option<u64>,
    depth_local_u_max: Option<u64>,
    depth_first_update_bridged_snapshot: bool,
    depth_source_gaps: BTreeSet<String>,
    metadata_complete_rows: u64,
    metadata_unavailable_rows: u64,
}

impl ActiveSegment {
    fn new(naming: HourlyPartition, source_kind: &str, hour_part: u32) -> Result<Self> {
        let writer = LiveSegmentWriter::create(&naming, source_kind)?;
        Ok(Self {
            source_kind: source_kind.to_owned(),
            utc_day: naming.utc_day.clone(),
            utc_hour: naming.utc_hour.clone(),
            hour_start_utc_ms: naming.hour_start_utc_ms,
            hour_end_utc_ms: naming.hour_end_utc_ms,
            writer,
            hour_part,
            segment_start_recv_time_utc_ms: 0,
            last_recv_time_utc_ms: 0,
            source_ts_start: None,
            source_ts_end: None,
            rows: 0,
            bytes: 0,
            event_type_counts: BTreeMap::new(),
            backfill_rows: 0,
            book_rows_with_bids_asks: 0,
            covered_market_slugs: BTreeSet::new(),
            covered_condition_ids: BTreeSet::new(),
            covered_asset_ids: BTreeSet::new(),
            depth_snapshot_last_update_id: None,
            depth_stitch_applied_rows: 0,
            depth_stitch_gap_rows: 0,
            depth_resnapshot_count: 0,
            depth_local_u_min: None,
            depth_local_u_max: None,
            depth_first_update_bridged_snapshot: false,
            depth_source_gaps: BTreeSet::new(),
            metadata_complete_rows: 0,
            metadata_unavailable_rows: 0,
        })
    }

    fn observe(&mut self, event: &RawEvent, typed_rows: u64) {
        if self.rows == 0 {
            self.segment_start_recv_time_utc_ms = event.recv_time_utc_ms;
        }
        self.last_recv_time_utc_ms = event.recv_time_utc_ms;
        if let Some(source_ts) = event.source_ts.clone() {
            if self.source_ts_start.is_none() {
                self.source_ts_start = Some(source_ts.clone());
            }
            self.source_ts_end = Some(source_ts);
        }
        self.rows = self.rows.saturating_add(typed_rows);
        self.bytes = self.bytes.saturating_add(estimated_event_bytes(event));
        *self
            .event_type_counts
            .entry(event.source_event_type.clone())
            .or_default() += typed_rows;
        self.observe_raw_json_evidence(event, typed_rows);
    }

    fn observe_raw_json_evidence(&mut self, event: &RawEvent, typed_rows: u64) {
        let Ok(value) = serde_json::from_str::<Value>(&event.raw_json) else {
            return;
        };
        if event.source_kind == "binance_aggtrade"
            && value
                .get("ingest_source")
                .and_then(Value::as_str)
                .is_some_and(|source| matches!(source, "rest_backfill" | "websocket_rest_merged"))
        {
            self.backfill_rows = self.backfill_rows.saturating_add(typed_rows);
        }
        if event.source_event_type == "depth_snapshot" {
            self.depth_snapshot_last_update_id = value
                .get("lastUpdateId")
                .and_then(Value::as_u64)
                .or(self.depth_snapshot_last_update_id);
        }
        if event.source_event_type == "depth_update" {
            self.depth_snapshot_last_update_id = value
                .get("snapshot_last_update_id")
                .and_then(Value::as_u64)
                .or(self.depth_snapshot_last_update_id);
            if let Some(applied_u) = value.get("u").and_then(Value::as_u64) {
                self.depth_stitch_applied_rows =
                    self.depth_stitch_applied_rows.saturating_add(typed_rows);
                self.depth_local_u_min = Some(
                    self.depth_local_u_min
                        .map_or(applied_u, |current| current.min(applied_u)),
                );
                self.depth_local_u_max = Some(
                    self.depth_local_u_max
                        .map_or(applied_u, |current| current.max(applied_u)),
                );
            }
            if value
                .get("stitch_evidence")
                .and_then(Value::as_str)
                .is_some_and(|evidence| evidence == "U <= lastUpdateId + 1 <= u")
            {
                self.depth_first_update_bridged_snapshot = true;
            }
            if let Some(trigger_reason) =
                value.get("snapshot_trigger_reason").and_then(Value::as_str)
            {
                self.depth_resnapshot_count = self.depth_resnapshot_count.saturating_add(1);
                if trigger_reason.starts_with("stitch_gap:") {
                    self.depth_stitch_gap_rows = self.depth_stitch_gap_rows.saturating_add(1);
                    self.depth_source_gaps.insert(format!(
                        "binance_depth_update_stitch_gap:{}",
                        trigger_reason.trim_start_matches("stitch_gap:")
                    ));
                } else if let Some(reason) = trigger_reason.strip_prefix("reconnect:") {
                    self.depth_source_gaps
                        .insert(format!("binance_depth_reconnect_required:{reason}"));
                }
            }
        }
        if event.source_event_type == "book"
            && value.get("bids").and_then(Value::as_array).is_some()
            && value.get("asks").and_then(Value::as_array).is_some()
        {
            self.book_rows_with_bids_asks =
                self.book_rows_with_bids_asks.saturating_add(typed_rows);
        }
        if event.source_kind == "polymarket_market_metadata" {
            if metadata_unavailable_reason(&value).is_some() {
                self.metadata_unavailable_rows =
                    self.metadata_unavailable_rows.saturating_add(typed_rows);
            } else {
                self.metadata_complete_rows =
                    self.metadata_complete_rows.saturating_add(typed_rows);
            }
        }
        if let Some(slug) = value
            .get("slug")
            .and_then(Value::as_str)
            .filter(|slug| !slug.trim().is_empty())
        {
            self.covered_market_slugs.insert(slug.to_owned());
        }
        if let Some(market) = value
            .get("market")
            .and_then(Value::as_str)
            .filter(|market| !market.trim().is_empty())
        {
            self.covered_condition_ids.insert(market.to_owned());
        }
        if let Some(asset_id) = value
            .get("asset_id")
            .and_then(Value::as_str)
            .filter(|asset_id| !asset_id.trim().is_empty())
        {
            self.covered_asset_ids.insert(asset_id.to_owned());
        }
        for field in ["asset_ids", "resolved_event_asset_ids"] {
            if let Some(asset_ids) = value.get(field).and_then(Value::as_array) {
                for asset_id in asset_ids.iter().filter_map(Value::as_str) {
                    if !asset_id.trim().is_empty() {
                        self.covered_asset_ids.insert(asset_id.to_owned());
                    }
                }
            }
        }
        if let Some(changes) = value.get("price_changes").and_then(Value::as_array) {
            for change in changes {
                if let Some(asset_id) = change
                    .get("asset_id")
                    .and_then(Value::as_str)
                    .filter(|asset_id| !asset_id.trim().is_empty())
                {
                    self.covered_asset_ids.insert(asset_id.to_owned());
                }
            }
        }
    }
}

enum LiveSegmentWriter {
    Typed(TypedParquetSegmentWriter),
}

impl LiveSegmentWriter {
    fn create(naming: &HourlyPartition, source_kind: &str) -> Result<Self> {
        Ok(Self::Typed(
            TypedParquetSegmentWriter::create_for_typed_source(
                &naming.partition_path,
                &naming.stem,
                typed_schema_version(source_kind),
                source_kind,
            )?,
        ))
    }

    fn write_event(&mut self, event: &RawEvent) -> Result<u64> {
        match self {
            Self::Typed(writer) => write_typed_live_event(writer, event),
        }
    }

    fn append_key_value_metadata(&mut self, key: &str, value: String) -> Result<()> {
        match self {
            Self::Typed(writer) => writer.append_key_value_metadata(key, value),
        }
    }

    fn final_path(&self) -> &Path {
        match self {
            Self::Typed(writer) => writer.final_path(),
        }
    }

    fn finalize(self) -> Result<PublishedSegment> {
        match self {
            Self::Typed(writer) => writer.finalize(),
        }
    }
}

fn typed_schema_version(source_kind: &str) -> &'static str {
    match source_kind {
        "polymarket_market" => "polymarket_orderbook_v2",
        "polymarket_market_metadata" => "polymarket_market_metadata_v2",
        "binance_depth_snapshot" => "binance_depth_snapshot_v2",
        "binance_depth_update" => "binance_depth_update_v3",
        "binance_aggtrade" => "binance_aggtrade_v5",
        "polymarket_rtds_chainlink_btc_usd" => "polymarket_rtds_chainlink_btc_usd_v4",
        _ => "unknown_typed_layout",
    }
}

fn write_typed_live_event(writer: &mut TypedParquetSegmentWriter, event: &RawEvent) -> Result<u64> {
    let logical_bytes = u64::try_from(owned_event_bytes(event))
        .map_err(|_| invalid_data("typed event logical size does not fit in u64"))?;
    match event.source_kind.as_str() {
        "polymarket_market" => {
            let rows = polymarket_orderbook_rows_from_event(event)?;
            let written = rows.len() as u64;
            require_at_most_one_typed_row(event, written)?;
            writer.write_polymarket_orderbook_rows_with_logical_bytes(&rows, logical_bytes)?;
            Ok(written)
        }
        "polymarket_market_metadata" => {
            let rows = polymarket_metadata_rows_from_event(event)?;
            let written = rows.len() as u64;
            require_at_most_one_typed_row(event, written)?;
            writer
                .write_polymarket_market_metadata_rows_with_logical_bytes(&rows, logical_bytes)?;
            Ok(written)
        }
        "binance_depth_snapshot" => {
            let row = binance_depth_snapshot_row_from_event(event)?;
            writer.write_binance_depth_snapshot_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "binance_depth_update" => {
            let row = binance_depth_update_row_from_event(event)?;
            writer.write_binance_depth_update_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "binance_aggtrade" => {
            let row = binance_aggtrade_row_from_event(event)?;
            writer.write_binance_aggtrade_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "polymarket_rtds_chainlink_btc_usd" => {
            let row = rtds_row_from_event(event)?;
            writer.write_rtds_chainlink_btc_usd_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported live typed source kind: {other}"),
        )
        .into()),
    }
}

fn require_at_most_one_typed_row(event: &RawEvent, rows: u64) -> Result<()> {
    if rows <= 1 {
        return Ok(());
    }
    Err(invalid_data(format!(
        "typed event fan-out must be split before ingress: source={} event_type={} rows={rows}",
        event.source_kind, event.source_event_type
    ))
    .into())
}

fn polymarket_metadata_rows_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketMarketMetadataRow>> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or(event.source_event_type.as_str());
    let row = match event_type {
        "active_scan" => metadata_row_from_value(event, &value, "active_scan")?,
        "new_market" => metadata_row_from_value(event, &value, "new_market")?,
        "fallback_close" => metadata_row_from_value(event, &value, "fallback_close")?,
        other => {
            return Err(invalid_data(format!(
                "unsupported polymarket metadata event type: {other}"
            ))
            .into());
        }
    };
    Ok(vec![row])
}

fn metadata_row_from_value(
    event: &RawEvent,
    value: &Value,
    metadata_source: &str,
) -> Result<PolymarketMarketMetadataRow> {
    let context = PolymarketMarketMetadataContext {
        timestamp_received_ms: event.recv_time_utc_ms,
        metadata_fetch_start_utc_ms: value
            .get("metadata_fetch_start_utc")
            .and_then(Value::as_i64),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "polymarket_market_metadata_v2".to_owned(),
    };
    let slug = required_value_string(value, "slug")?;
    let condition_id = value
        .get("condition_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("market").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid_data("condition_id missing"))?;
    let clob_market_id = value
        .get("clob_market_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("id").and_then(Value::as_str));
    let asset_ids = json_string_vec(value, "asset_ids")?;
    let market_start_utc = optional_i64(value, "market_start_utc");
    let market_end_utc = optional_i64(value, "market_end_utc");
    let active = optional_bool(value, "active");
    let accepting_orders = optional_bool(value, "accepting_orders");
    let market_lifecycle_state = value
        .get("market_lifecycle_state")
        .and_then(Value::as_str)
        .unwrap_or(match metadata_source {
            "new_market" => "prefetched",
            "fallback_close" => "retired",
            _ => "active",
        });
    let mut row = match metadata_source {
        "new_market" => PolymarketMarketMetadataRow::new_market(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified").or(Some(true)),
            context,
        ),
        "fallback_close" => PolymarketMarketMetadataRow::fallback_close(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified").or(Some(true)),
            context,
        ),
        _ => PolymarketMarketMetadataRow::active_scan(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified").or(Some(true)),
            context,
        ),
    };
    row.metadata_unavailable_reason = metadata_unavailable_reason(value);
    let validation = row.validate();
    if !validation.is_complete() && row.metadata_unavailable_reason.is_none() {
        return Err(invalid_data(format!(
            "polymarket metadata row missing fields without unavailable reason: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn polymarket_orderbook_rows_from_event(event: &RawEvent) -> Result<Vec<PolymarketOrderbookRow>> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or(event.source_event_type.as_str());
    let context = polymarket_context(event, &value);
    let rows = match event_type {
        "book" => vec![
            PolymarketOrderbookRow::book_from_raw_json(&event.raw_json, context)
                .map_err(invalid_data)?,
        ],
        "price_change" => {
            let market = required_value_string(&value, "market")?;
            let timestamp = optional_i64(&value, "timestamp");
            value
                .get("price_changes")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("price_changes missing"))?
                .iter()
                .map(|change| {
                    let mut row = base_polymarket_row(
                        event,
                        &value,
                        "price_change",
                        market.clone(),
                        Some(required_value_string(change, "asset_id")?),
                        timestamp,
                    );
                    row.slug = change
                        .get("slug")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            value
                                .get("slug")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        });
                    row.price = optional_string(change, "price");
                    row.size = optional_string(change, "size");
                    row.side = optional_string(change, "side");
                    row.best_bid = optional_string(change, "best_bid");
                    row.best_ask = optional_string(change, "best_ask");
                    row.hash = optional_string(change, "hash");
                    Ok(row)
                })
                .collect::<Result<Vec<_>>>()?
        }
        "last_trade_price" => {
            let row = PolymarketOrderbookRow::last_trade_price(
                &required_value_string(&value, "market")?,
                &required_value_string(&value, "asset_id")?,
                required_i64(&value, "timestamp")?,
                &required_value_string(&value, "price")?,
                value.get("fee_rate_bps").and_then(Value::as_str),
                value.get("side").and_then(Value::as_str),
                value.get("size").and_then(Value::as_str),
                value.get("transaction_hash").and_then(Value::as_str),
                context,
            );
            vec![row]
        }
        "tick_size_change" => vec![PolymarketOrderbookRow::tick_size_change(
            &required_value_string(&value, "market")?,
            &required_value_string(&value, "asset_id")?,
            required_i64(&value, "timestamp")?,
            &required_value_string(&value, "old_tick_size")?,
            &required_value_string(&value, "new_tick_size")?,
            context,
        )],
        "market_resolved" => {
            let resolved_asset_ids = value
                .get("resolved_event_asset_ids")
                .or_else(|| value.get("asset_ids"))
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("resolved_event_asset_ids missing"))?
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()?;
            vec![PolymarketOrderbookRow::market_resolved(
                &required_value_string(&value, "market")?,
                optional_i64(&value, "timestamp").unwrap_or(event.recv_time_utc_ms),
                &required_value_string(&value, "winning_asset_id")?,
                &required_value_string(&value, "winning_outcome")?,
                value.get("close_reason_if_present").and_then(Value::as_str),
                &resolved_asset_ids,
                context,
            )]
        }
        "new_market" => {
            let mut row = base_polymarket_row(
                event,
                &value,
                "new_market",
                required_value_string(&value, "market")?,
                None,
                optional_i64(&value, "timestamp"),
            );
            let asset_ids = value
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("asset_ids missing"))?
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()?;
            row.subscribed_asset_ids_for_market = Some(serde_json::to_string(&asset_ids)?);
            vec![row]
        }
        "market_discovery_switch" => Vec::new(),
        "best_bid_ask" => {
            let mut row = base_polymarket_row(
                event,
                &value,
                "best_bid_ask",
                required_value_string(&value, "market")?,
                Some(required_value_string(&value, "asset_id")?),
                optional_i64(&value, "timestamp"),
            );
            row.best_bid = optional_string(&value, "best_bid");
            row.best_ask = optional_string(&value, "best_ask");
            vec![row]
        }
        other => {
            return Err(
                invalid_data(format!("unsupported polymarket market event type: {other}")).into(),
            );
        }
    };
    Ok(rows)
}

fn polymarket_context(event: &RawEvent, value: &Value) -> PolymarketOrderbookContext {
    PolymarketOrderbookContext {
        timestamp_received_ms: event.recv_time_utc_ms,
        slug: value
            .get("slug")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        source_native_payload_json: value
            .get("source_native_payload_json")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "polymarket_orderbook_v2".to_owned(),
    }
}

fn base_polymarket_row(
    event: &RawEvent,
    value: &Value,
    event_type: &str,
    market: String,
    asset_id: Option<String>,
    timestamp: Option<i64>,
) -> PolymarketOrderbookRow {
    let context = polymarket_context(event, value);
    PolymarketOrderbookRow {
        timestamp_received: context.timestamp_received_ms,
        timestamp,
        market,
        event_type: event_type.to_owned(),
        source_native_payload_json: context.source_native_payload_json,
        asset_id,
        bids: None,
        asks: None,
        price: None,
        size: None,
        side: None,
        best_bid: None,
        best_ask: None,
        hash: None,
        fee_rate_bps: None,
        transaction_hash: None,
        old_tick_size: None,
        new_tick_size: None,
        winning_asset_id: None,
        winning_outcome: None,
        close_reason_if_present: None,
        subscribed_asset_ids_for_market: None,
        resolved_event_asset_ids: None,
        slug: context.slug,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}

fn binance_depth_snapshot_row_from_event(
    event: &RawEvent,
) -> Result<BinanceDepthSnapshotHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let snapshot_fetch_start_utc = required_i64(&value, "snapshot_fetch_start_utc")?;
    let raw = parse_depth_snapshot_raw(
        event_envelope(event, SourceKind::BinanceDepthSnapshot),
        &event.raw_json,
        snapshot_fetch_start_utc,
        event.recv_time_utc_ms,
        event.source_endpoint.clone(),
    )
    .map_err(invalid_data)?;
    Ok(BinanceDepthSnapshotHourlyRow::from_raw(&raw))
}

fn binance_depth_update_row_from_event(event: &RawEvent) -> Result<BinanceDepthUpdateHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let mut raw = parse_depth_update_raw(
        event_envelope(event, SourceKind::BinanceDepthUpdate),
        &event.raw_json,
    )
    .map_err(invalid_data)?;
    raw.stitch_evidence = value
        .get("stitch_evidence")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let snapshot_last_update_id = value
        .get("snapshot_last_update_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_data("snapshot_last_update_id missing"))?;
    let local_u_before = value.get("local_u_before").and_then(Value::as_u64);
    Ok(BinanceDepthUpdateHourlyRow::from_raw(
        &raw,
        snapshot_last_update_id,
        local_u_before,
    ))
}

fn binance_aggtrade_row_from_event(event: &RawEvent) -> Result<BinanceAggTradeHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let row = BinanceAggTradeHourlyRow {
        timestamp_received: required_i64(&value, "timestamp_received")?,
        websocket_timestamp_received: optional_i64(&value, "websocket_timestamp_received"),
        rest_backfill_timestamp_received: optional_i64(&value, "rest_backfill_timestamp_received"),
        ingest_source: required_value_string(&value, "ingest_source")?,
        event_type: optional_string(&value, "event_type"),
        event_time: optional_i64(&value, "event_time"),
        trade_time: required_i64(&value, "trade_time")?,
        symbol: required_value_string(&value, "symbol")?,
        aggregate_trade_id: required_u64(&value, "aggregate_trade_id")?,
        price: required_value_string(&value, "price")?,
        quantity: required_value_string(&value, "quantity")?,
        first_trade_id: required_u64(&value, "first_trade_id")?,
        last_trade_id: required_u64(&value, "last_trade_id")?,
        buyer_is_maker: value
            .get("buyer_is_maker")
            .and_then(Value::as_bool)
            .ok_or_else(|| invalid_data("buyer_is_maker missing"))?,
        stream: optional_string(&value, "stream"),
        ws_ignore: optional_bool(&value, "ws_ignore"),
        rest_was_best_price_match: optional_bool(&value, "rest_was_best_price_match"),
        source_native_payload_json: required_value_string(&value, "source_native_payload_json")?,
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "binance_aggtrade_v5".to_owned(),
    };
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "Binance aggTrade row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn rtds_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let raw = RtdsChainlinkBtcUsdRaw {
        envelope: event_envelope(event, SourceKind::PolymarketRtdsChainlinkBtcUsd),
        message_timestamp_ms: required_i64(&value, "message_timestamp")?,
        payload_timestamp_ms: required_i64(&value, "payload_timestamp")?,
        symbol: required_value_string(&value, "symbol")?,
        value: required_value_string(&value, "value")?,
        source_topic: required_value_string(&value, "topic")?,
        message_type: required_value_string(&value, "type")?,
        age_ms: optional_i64(&value, "age_ms"),
        freshness_status: required_value_string(&value, "freshness_status")?,
        subscription_raw_json: value
            .get("subscription_raw_json")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(rtds_subscription_evidence_json),
        ack_or_first_message_evidence: Some("first_chainlink_price_message".to_owned()),
        close_or_error_evidence: None,
    };
    let mut row = RtdsChainlinkBtcUsdHourlyRow::from_raw(&raw);
    row.source_native_payload_json = required_value_string(&value, "source_native_payload_json")?;
    row.schema_version = "polymarket_rtds_chainlink_btc_usd_v4".to_owned();
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "RTDS row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn event_envelope(event: &RawEvent, source_kind: SourceKind) -> EventEnvelope {
    EventEnvelope {
        source_kind,
        source_event_type: event.source_event_type.clone(),
        source_identifier: event.source_identifier.clone(),
        source_timestamp: event
            .source_ts
            .clone()
            .map(|value| SourceTimestamp::Timestamp { value })
            .unwrap_or_else(|| SourceTimestamp::NoSourceTimestamp {
                reason: "source_event_has_no_timestamp".to_owned(),
                snapshot_boundary: event.recv_time_utc_ms.to_string(),
            }),
        recv_time_utc_ms: Some(event.recv_time_utc_ms),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        host_clock_sync_status: HostClockSyncStatus::Unavailable {
            reason: "not_checked_in_live_runtime".to_owned(),
        },
        raw_evidence: RawEvidence::Json(event.raw_json.clone()),
        parquet_raw_layout_version: event.parquet_raw_layout_version,
        parquet_raw_source_kind: source_kind,
    }
}

fn rtds_subscription_evidence_json() -> String {
    json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }]
    })
    .to_string()
}

fn required_value_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn optional_json_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|value| {
        if value.is_null() {
            None
        } else if let Some(value) = value.as_str() {
            (!value.trim().is_empty()).then(|| value.to_owned())
        } else {
            Some(value.to_string())
        }
    })
}

fn optional_bool(value: &Value, field: &str) -> Option<bool> {
    value.get(field).and_then(Value::as_bool)
}

fn json_string_vec(value: &Value, field: &str) -> Result<Vec<String>> {
    let Some(values) = value.get(field).and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    values.iter().map(json_scalar_to_string).collect()
}

fn metadata_unavailable_reason(value: &Value) -> Option<String> {
    match value.get("metadata_unavailable_reason") {
        Some(Value::String(reason)) if !reason.trim().is_empty() => Some(reason.clone()),
        Some(Value::Array(reasons)) if !reasons.is_empty() => {
            let reasons = reasons
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()
                .ok()?;
            Some(reasons.join(";"))
        }
        Some(other) if !other.is_null() => Some(other.to_string()),
        _ => None,
    }
}

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    optional_i64(value, field).ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

fn required_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

fn optional_i64(value: &Value, field: &str) -> Option<i64> {
    value.get(field).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn json_scalar_to_string(value: &Value) -> Result<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(invalid_data("expected string or number JSON scalar").into()),
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn operator_dedupe_error(message: String) -> io::Error {
    invalid_data(format!(
        "operator_intervention_required:dedupe_invariant:{message}"
    ))
}

fn live_segment_rollover_reason(
    segment: &ActiveSegment,
    current_utc_hour: &str,
) -> Option<&'static str> {
    if segment.utc_hour != current_utc_hour {
        return Some("utc_hour_boundary");
    }
    None
}

#[derive(Default)]
struct CoverageStats {
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    rows: u64,
    market_windows: BTreeSet<String>,
    market_window_first_recv_time_utc_ms: BTreeMap<String, i64>,
    event_type_counts: BTreeMap<String, u64>,
}

impl CoverageStats {
    fn to_source_coverage(&self, source_kind: &str) -> Option<SourceCoverage> {
        Some(SourceCoverage {
            source_kind: source_kind.to_owned(),
            first_recv_time_utc_ms: self.first_recv_time_utc_ms?,
            last_recv_time_utc_ms: self.last_recv_time_utc_ms?,
            rows: self.rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use polymarket_client_sdk_v2::clob::ws::WsMessage;
    use std::fs::File;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn source_queue_recoverable_wait_is_recorded_without_source_gap() {
        let stats = SourceQueueBackpressureStats::default();
        stats.record_wait();

        let mut tracker = QueueDurationTracker::new("source_event_queue");
        stats.apply_to_tracker(&mut tracker);
        let summary = tracker.finish(0);

        assert_eq!(summary.backpressure_wait_count, 1);
        assert_eq!(summary.backpressure_timeout_count, 0);
        assert_eq!(summary.dropped_rows, 0);
        assert!(!is_live_runtime_source_gap_reason(
            "bounded_queue_backpressure_wait:binance_depth_update"
        ));
    }

    #[tokio::test]
    async fn all_producers_enqueue_crossed_startup_barrier_before_first_source_event() {
        let initial_boundary = now_ms().saturating_sub(1);
        for producer in ["polymarket", "polymarket_rtds"] {
            let (tx, mut rx) = mpsc::channel(4);
            let marker_tx = tx.clone();
            let sink = SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            };
            let source = async move {
                marker_tx
                    .send(InflightRawEvent::for_test(test_raw_event(
                        "binance_aggtrade",
                        now_ms(),
                    )))
                    .await
                    .unwrap();
                Ok(())
            };

            run_logical_producer_with_day_barriers(
                producer,
                sink,
                Instant::now(),
                initial_boundary,
                source,
            )
            .await
            .unwrap();

            assert!(matches!(
                rx.recv().await.unwrap().control,
                Some(IngressControl::DayBarrier { producer: actual, .. }) if actual == producer
            ));
            assert!(rx.recv().await.unwrap().control.is_none());
        }

        let (tx, mut rx) = mpsc::channel(4);
        let marker_tx = tx.clone();
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (control_tx, _control_rx) = mpsc::channel(1);
        let source = async move {
            marker_tx
                .send(InflightRawEvent::for_test(test_raw_event(
                    "binance_aggtrade",
                    now_ms(),
                )))
                .await
                .unwrap();
            Ok(())
        };
        run_aggtrade_logical_producer_with_day_barriers(
            sink,
            control_tx,
            Instant::now(),
            initial_boundary,
            source,
        )
        .await
        .unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().control,
            Some(IngressControl::DayBarrier { producer, .. }) if producer == "binance_aggtrade"
        ));
        assert!(rx.recv().await.unwrap().control.is_none());

        let (tx, mut rx) = mpsc::channel(4);
        let marker_tx = tx.clone();
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (control_tx, _control_rx) = mpsc::channel(1);
        let source = async move {
            marker_tx
                .send(InflightRawEvent::for_test(test_raw_event(
                    "binance_aggtrade",
                    now_ms(),
                )))
                .await
                .unwrap();
            Ok(())
        };
        run_depth_logical_producer_with_day_barriers(
            sink,
            control_tx,
            Instant::now(),
            initial_boundary,
            source,
        )
        .await
        .unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().control,
            Some(IngressControl::DayBarrier { producer, .. }) if producer == "binance_depth"
        ));
        assert!(rx.recv().await.unwrap().control.is_none());
    }

    #[tokio::test]
    async fn producer_barrier_cannot_overtake_an_old_day_event_waiting_for_queue_space() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let run_started = Instant::now();
        send_event(
            &sink,
            run_started,
            test_raw_event("binance_aggtrade", 1_783_468_799_998),
        )
        .await
        .unwrap();

        let old_sink = sink.clone();
        let old_event = tokio::spawn(async move {
            send_event(
                &old_sink,
                run_started,
                test_raw_event("binance_aggtrade", 1_783_468_799_999),
            )
            .await
        });
        while sink.enqueue_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        let barrier_sink = sink.clone();
        let barrier = tokio::spawn(async move {
            send_day_barrier(
                &barrier_sink,
                run_started,
                "binance_aggtrade",
                "2026-07-07".to_owned(),
            )
            .await
        });

        let first = rx.recv().await.unwrap();
        assert_eq!(first.event.recv_time_utc_ms, 1_783_468_799_998);
        old_event.await.unwrap().unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(second.event.recv_time_utc_ms, 1_783_468_799_999);
        assert!(second.control.is_none());
        barrier.await.unwrap().unwrap();
        let third = rx.recv().await.unwrap();
        assert!(matches!(
            third.control,
            Some(IngressControl::DayBarrier {
                producer,
                utc_day
            }) if producer == "binance_aggtrade" && utc_day == "2026-07-07"
        ));
    }

    #[tokio::test]
    async fn barrier_wait_keeps_polling_a_source_that_holds_the_enqueue_lock() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let run_started = Instant::now();
        send_event(
            &sink,
            run_started,
            test_raw_event("binance_aggtrade", 1_783_468_799_998),
        )
        .await
        .unwrap();
        let source_sink = sink.clone();
        let source = async move {
            send_event(
                &source_sink,
                run_started,
                test_raw_event("binance_aggtrade", 1_783_468_799_999),
            )
            .await?;
            std::future::pending::<Result<()>>().await
        };
        tokio::pin!(source);
        assert!(matches!(
            futures::poll!(source.as_mut()),
            std::task::Poll::Pending
        ));
        let consumer = tokio::spawn(async move {
            let first = rx.recv().await.unwrap();
            let second = rx.recv().await.unwrap();
            let barrier = rx.recv().await.unwrap();
            (first, second, barrier)
        });

        assert!(
            timeout(
                Duration::from_secs(1),
                wait_for_day_barrier_while_source_runs(
                    source.as_mut(),
                    &sink,
                    run_started,
                    "polymarket_rtds",
                    "2026-07-07".to_owned(),
                ),
            )
            .await
            .expect("barrier must not deadlock behind the source enqueue lock")
            .unwrap()
            .is_none()
        );
        let (first, second, barrier) = timeout(Duration::from_secs(1), consumer)
            .await
            .expect("source event and barrier must both reach ingress")
            .unwrap();
        assert_eq!(first.event.recv_time_utc_ms, 1_783_468_799_998);
        assert_eq!(second.event.recv_time_utc_ms, 1_783_468_799_999);
        assert!(matches!(
            barrier.control,
            Some(IngressControl::DayBarrier {
                ref producer,
                ref utc_day,
            }) if producer == "polymarket_rtds" && utc_day == "2026-07-07"
        ));
    }

    #[tokio::test]
    async fn aggtrade_day_flush_fences_source_local_pending_before_barrier() {
        let (tx, mut rx) = mpsc::channel(8);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (control_tx, mut control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source_sink = sink.clone();
        let run_started = Instant::now();
        let source = async move {
            let collector_identity = CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            };
            let spec = BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS.to_owned(),
                rest_endpoint: BINANCE_AGGTRADE_REST.to_owned(),
                freshness_timeout_secs: 60,
            };
            let mut coordinator = AggTradeCoordinator::with_pending_limit(
                BinanceAggTradeContinuityState::default(),
                BINANCE_AGGTRADE_PENDING_MAX_ROWS,
            );
            let first = coordinator
                .accept_websocket(
                    BinanceWsAggTrade::from_wire(
                        &test_ws_aggtrade_value(100),
                        "btcusdt@aggTrade",
                        1_783_468_799_998,
                    )
                    .unwrap(),
                )
                .unwrap();
            emit_aggtrade_result(first, &source_sink, &collector_identity, &spec, run_started)
                .await?;
            let _ = coordinator
                .accept_websocket(
                    BinanceWsAggTrade::from_wire(
                        &test_ws_aggtrade_value(103),
                        "btcusdt@aggTrade",
                        1_783_468_799_999,
                    )
                    .unwrap(),
                )
                .unwrap();
            let Some(control) = control_rx.recv().await else {
                return Err(invalid_data("missing test aggTrade flush control").into());
            };
            let mut backfill_task = None;
            let mut pending_reservations = Vec::new();
            assert!(
                handle_aggtrade_source_control(
                    control,
                    &mut coordinator,
                    &mut backfill_task,
                    &mut pending_reservations,
                    &source_sink,
                    &collector_identity,
                    &spec,
                    run_started,
                )
                .await?
                .is_none()
            );
            send_event(
                &source_sink,
                run_started,
                test_raw_event("binance_aggtrade", 1_783_468_800_001),
            )
            .await?;
            std::future::pending::<Result<()>>().await
        };
        tokio::pin!(source);

        assert!(
            flush_aggtrade_and_send_day_barrier(
                source.as_mut(),
                &control_tx,
                &sink,
                run_started,
                "2026-07-07",
                "2026-07-08",
            )
            .await
            .unwrap()
            .is_none()
        );

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        let continuity = rx.recv().await.unwrap();
        let barrier = rx.recv().await.unwrap();
        assert_eq!(first.event.recv_time_utc_ms, 1_783_468_799_998);
        assert_eq!(second.event.recv_time_utc_ms, 1_783_468_799_999);
        assert!(matches!(
            continuity.control,
            Some(IngressControl::BinanceContinuity { ref utc_day, .. })
                if utc_day == "2026-07-07"
        ));
        assert!(matches!(
            barrier.control,
            Some(IngressControl::DayBarrier {
                ref producer,
                ref utc_day,
            }) if producer == "binance_aggtrade" && utc_day == "2026-07-07"
        ));
        let after_barrier = tokio::select! {
            message = rx.recv() => message.unwrap(),
            result = &mut source => panic!("aggTrade source ended before post-barrier event: {result:?}"),
        };
        assert!(after_barrier.control.is_none());
        assert_eq!(after_barrier.event.recv_time_utc_ms, 1_783_468_800_001);
    }

    #[test]
    fn recovery_crossing_midnight_is_recorded_on_both_utc_days() {
        let (tx, _rx) = mpsc::channel(1);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        stats.set_recovery_active("binance_aggtrade", true);

        record_recovery_boundary_gaps(&sink, "binance_aggtrade", "2026-07-07", "2026-07-08");

        let gaps = gaps.lock().unwrap();
        let expected = "source_recovery_overlaps_utc_day_boundary:binance_aggtrade";
        assert_eq!(gaps.reasons_for_day("2026-07-07"), [expected]);
        assert_eq!(gaps.reasons_for_day("2026-07-08"), [expected]);
        assert_eq!(gaps.all_reasons(), [expected]);
    }

    #[test]
    fn manifest_owner_closes_only_after_all_producer_barriers_and_writer_acks() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let day = "2026-07-07";
        let mut owner = ManifestOwnerState::new(
            dataset_root.clone(),
            test_daily_manifest_identity(),
            BinanceAggTradeContinuityState::default(),
        );

        let operational = BTreeMap::from([(
            "binance_aggtrade".to_owned(),
            SourceOperationalCounterDelta {
                structural_dedupe_dropped_events: 2,
                lag_events: 1,
                reconnect_events: 3,
                dropped_rows: 0,
            },
        )]);
        let no_operational_counters = BTreeMap::new();
        for (index, producer) in LOGICAL_PRODUCERS.iter().enumerate() {
            owner
                .record_producer_barrier(
                    day.to_owned(),
                    (*producer).to_owned(),
                    &[],
                    if index == 0 {
                        &operational
                    } else {
                        &no_operational_counters
                    },
                )
                .unwrap();
        }
        for source in &LIVE_SOURCES[..LIVE_SOURCES.len() - 1] {
            owner
                .record_writer_ack(day.to_owned(), (*source).to_owned())
                .unwrap();
        }
        let day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={day}"));
        let open = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_eq!(manifest_state(&open).unwrap(), "open");

        owner
            .record_writer_ack(day.to_owned(), LIVE_SOURCES.last().unwrap().to_string())
            .unwrap();
        let closed = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_eq!(manifest_state(&closed).unwrap(), "closed");
        assert_eq!(closed["closure_reason"], "utc_day_barrier");
        assert_eq!(closed["producer_barriers"].as_array().unwrap().len(), 4);
        assert_eq!(closed["writer_finalize_acks"].as_array().unwrap().len(), 6);
        assert_eq!(closed["source_counters"].as_object().unwrap().len(), 6);
        assert_eq!(
            closed["source_counters"]["binance_aggtrade"]["structural_dedupe_dropped_events"],
            2
        );
        assert_eq!(
            closed["source_counters"]["binance_aggtrade"]["lag_events"],
            1
        );
        assert_eq!(
            closed["source_counters"]["binance_aggtrade"]["reconnect_events"],
            3
        );
        assert_eq!(
            fs::read_dir(&day_dir)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_type().unwrap().is_file())
                .count(),
            1,
            "the UTC day root may contain only its one manifest file"
        );
        assert!(
            owner
                .record_gaps(day, &["late_old_day_gap".to_owned()])
                .is_err()
        );
    }

    #[test]
    fn manifest_owner_shutdown_keeps_current_day_open() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let day = "2026-07-07";
        let mut owner = ManifestOwnerState::new(
            dataset_root.clone(),
            test_daily_manifest_identity(),
            BinanceAggTradeContinuityState::default(),
        );
        owner.ensure_open(day).unwrap();
        let operational = BTreeMap::from([(
            day.to_owned(),
            BTreeMap::from([(
                "polymarket_market".to_owned(),
                SourceOperationalCounterDelta {
                    dropped_rows: 1,
                    ..SourceOperationalCounterDelta::default()
                },
            )]),
        )]);
        owner
            .shutdown(day, &["operator_shutdown".to_owned()], &operational)
            .unwrap();

        let manifest =
            load_day_manifest(&typed_raw_root(&dataset_root).join(format!("utc_day={day}")))
                .unwrap()
                .unwrap();
        assert_eq!(manifest_state(&manifest).unwrap(), "open");
        assert_eq!(manifest["gaps"][0], "operator_shutdown");
        assert_eq!(
            manifest["source_counters"]["polymarket_market"]["dropped_rows"],
            1
        );
        assert!(manifest.get("closure_reason").is_none());
    }

    #[test]
    fn dataset_capacity_scan_counts_regular_files_and_rejects_symlinks() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().join("datasets");
        let training_root = dataset_root.join("training");
        fs::create_dir_all(training_root.join("typed_raw")).unwrap();
        fs::write(training_root.join("typed_raw/a"), [0_u8; 7]).unwrap();
        assert_eq!(training_dataset_size_bytes(&dataset_root).unwrap(), 7);

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                training_root.join("typed_raw/a"),
                training_root.join("typed_raw/link"),
            )
            .unwrap();
            assert!(training_dataset_size_bytes(&dataset_root).is_err());
        }
    }

    #[test]
    fn dataset_root_lock_allows_only_one_manifest_owner_process() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let first = lock_training_dataset_root(&dataset_root).unwrap();
        let error = lock_training_dataset_root(&dataset_root).unwrap_err();
        assert!(error.to_string().contains("training_dataset_root_locked"));
        drop(first);
        lock_training_dataset_root(&dataset_root).unwrap();
    }

    #[test]
    fn typed_writer_closes_row_groups_at_exact_row_and_logical_byte_limits() {
        let tmp = tempdir().unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            "binance_aggtrade_00",
            typed_schema_version("binance_aggtrade"),
            "binance_aggtrade",
        )
        .unwrap();
        let row = test_aggtrade_hourly_row(1, 1_783_392_400_001);
        writer
            .write_binance_aggtrade_rows_with_logical_bytes(
                &vec![row.clone(); PARQUET_ROW_GROUP_MAX_ROWS as usize],
                PARQUET_ROW_GROUP_MAX_ROWS,
            )
            .unwrap();
        for _ in 0..17 {
            writer
                .write_binance_aggtrade_rows_with_logical_bytes(
                    std::slice::from_ref(&row),
                    PARQUET_ROW_GROUP_MAX_BYTES / 16,
                )
                .unwrap();
        }

        let published = writer.finalize().unwrap();
        let reader = SerializedFileReader::new(File::open(published.final_path).unwrap()).unwrap();
        let row_groups = reader.metadata().row_groups();
        assert_eq!(row_groups.len(), 3);
        assert_eq!(row_groups[0].num_rows(), 50_000);
        assert_eq!(row_groups[1].num_rows(), 16);
        assert_eq!(row_groups[2].num_rows(), 1);
        assert!(
            !tmp.path()
                .join("binance_aggtrade_00.parquet.running")
                .exists()
        );
    }

    #[test]
    fn source_recovery_cycles_are_finite_jittered_capped_and_not_tight() {
        let mut recovery = SourceRecoverySupervisor::new("test_source", 7);
        for attempt in 1..=SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS {
            let wait = recovery.next_wait();
            assert_eq!(wait.level, SourceRecoveryWaitLevel::InnerAttempt);
            assert_eq!(wait.attempt, attempt);
            assert!(!wait.delay.is_zero());
            assert!(wait.delay <= Duration::from_millis(BINANCE_WS_RECONNECT_MAX_MS));
        }
        let outer = recovery.next_wait();
        assert_eq!(outer.level, SourceRecoveryWaitLevel::OuterCycle);
        assert_eq!(outer.attempt, 1);
        assert!(!outer.delay.is_zero());
        assert!(outer.delay <= Duration::from_millis(SOURCE_SUPERVISOR_BACKOFF_MAX_MS));
        assert_eq!(recovery.inner_attempts, 0);

        recovery.record_business_recovery();
        assert_eq!(recovery.inner_attempts, 0);
        assert_eq!(recovery.outer_cycles, 0);
        assert_eq!(
            recovery.next_wait().level,
            SourceRecoveryWaitLevel::InnerAttempt
        );

        recovery.cycle_started = Instant::now() - SOURCE_RECOVERY_CYCLE_MAX_ELAPSED;
        assert_eq!(
            recovery.next_wait().level,
            SourceRecoveryWaitLevel::OuterCycle,
            "max elapsed must end a cycle even before max attempts"
        );

        assert_ne!(
            retry_delay("test_source", 3, 500, 5_000, 7),
            retry_delay("test_source", 3, 500, 5_000, 8),
            "different stable jitter seeds should produce different retry delays"
        );
    }

    #[test]
    fn market_supervisor_retries_only_transport_exhaustion() {
        assert!(market_source_error_is_retryable(
            &SdkStreamError::TerminalDisconnected
        ));
        assert!(market_source_error_is_retryable(
            &SdkStreamError::Connection("network".to_owned())
        ));
        assert!(!market_source_error_is_retryable(&SdkStreamError::Lagged(
            1
        )));
        assert!(!market_source_error_is_retryable(&SdkStreamError::Parser(
            "schema".to_owned()
        )));
        assert!(!market_source_error_is_retryable(
            &SdkStreamError::SubscriptionActor("internal".to_owned())
        ));
    }

    #[tokio::test]
    async fn global_inflight_budget_enforces_exact_production_boundaries_and_releases() {
        let budget = InflightBudget::new();
        let reservation = budget
            .acquire(
                GLOBAL_INFLIGHT_MAX_ROWS,
                GLOBAL_INFLIGHT_MAX_BYTES,
                Duration::from_millis(10),
            )
            .await
            .unwrap();
        assert_eq!(budget.available(), (0, 0));
        let error = budget
            .acquire(1, 1, Duration::from_millis(10))
            .await
            .expect_err("the 100,001st row must not pass the global budget");
        assert!(error.to_string().contains("row_acquire_timeout"));

        drop(reservation);
        assert_eq!(
            budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );
    }

    #[tokio::test]
    async fn send_event_accepts_exact_one_mib_owned_row_and_rejects_one_byte_more() {
        let budget = InflightBudget::new();
        let (tx, mut rx) = mpsc::channel(2);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut exact = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        exact.raw_json.clear();
        exact.raw_json.shrink_to_fit();
        let fixed_bytes = owned_event_bytes(&exact);
        exact.raw_json = "x".repeat(MAX_TYPED_ROW_BYTES - fixed_bytes);
        exact.raw_json.shrink_to_fit();
        assert_eq!(owned_event_bytes(&exact), MAX_TYPED_ROW_BYTES);
        send_event(&sink, Instant::now(), exact).await.unwrap();

        let accepted = rx.recv().await.unwrap();
        drop(accepted);
        assert_eq!(
            budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );

        let mut oversized = test_raw_event("binance_aggtrade", 1_783_392_404_600);
        oversized.raw_json.clear();
        oversized.raw_json.shrink_to_fit();
        let fixed_bytes = owned_event_bytes(&oversized);
        oversized.raw_json = "x".repeat(MAX_TYPED_ROW_BYTES + 1 - fixed_bytes);
        oversized.raw_json.shrink_to_fit();
        let error = send_event(&sink, Instant::now(), oversized)
            .await
            .expect_err("a row over 1 MiB must fail before queueing");
        assert!(error.to_string().contains("max_typed_row_bytes_exceeded"));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn copied_owned_payloads_are_counted_independently_until_each_is_released() {
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        let copied_event = event.clone();
        let copied_bytes = owned_event_bytes(&copied_event);
        let original_bytes = owned_event_bytes(&event);
        let budget = InflightBudget::new();
        let (tx, mut rx) = mpsc::channel(2);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };

        send_event(&sink, Instant::now(), copied_event)
            .await
            .unwrap();
        send_event(&sink, Instant::now(), event).await.unwrap();
        assert_eq!(
            budget.available(),
            (
                GLOBAL_INFLIGHT_MAX_ROWS - 2,
                GLOBAL_INFLIGHT_MAX_BYTES - copied_bytes - original_bytes
            )
        );

        drop(rx.recv().await.unwrap());
        assert_eq!(budget.available().0, GLOBAL_INFLIGHT_MAX_ROWS - 1);
        drop(rx.recv().await.unwrap());
        assert_eq!(
            budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );
    }

    #[tokio::test]
    async fn short_source_queue_backpressure_waits_without_dropping() {
        let (tx, mut rx) = mpsc::channel(1);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        send_event(
            &sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", 1_783_392_404_599),
        )
        .await
        .unwrap();
        let second_sink = sink.clone();
        let second = tokio::spawn(async move {
            send_event(
                &second_sink,
                Instant::now(),
                test_raw_event("binance_aggtrade", 1_783_392_404_600),
            )
            .await
        });
        sleep(Duration::from_millis(25)).await;
        drop(rx.recv().await.unwrap());
        second.await.unwrap().unwrap();
        drop(rx.recv().await.unwrap());

        assert_eq!(stats.wait_count.load(Ordering::Relaxed), 1);
        assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 0);
        assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn closed_source_queue_returns_error_and_releases_budget() {
        let budget = InflightBudget::new();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let error = send_event(
            &sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", 1_783_392_404_599),
        )
        .await
        .expect_err("closed ingress must stop the source");
        assert!(error.to_string().contains("bounded_queue_send_closed"));
        assert_eq!(
            budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );
    }

    #[test]
    fn source_gaps_are_deduped_and_bounded() {
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        record_gap_for_day(&gaps, "2026-07-07", "same_gap".to_owned());
        record_gap_for_day(&gaps, "2026-07-07", "same_gap".to_owned());
        record_gap_for_day(&gaps, "2026-07-08", "same_gap".to_owned());
        for index in 0..(MAX_RECORDED_SOURCE_GAPS + 5) {
            record_gap(&gaps, format!("unique_gap:{index}"));
        }

        let recorded = gaps.lock().unwrap();
        assert_eq!(recorded.iter().filter(|gap| *gap == "same_gap").count(), 1);
        assert_eq!(recorded.reasons_for_day("2026-07-07"), ["same_gap"]);
        assert_eq!(recorded.reasons_for_day("2026-07-08"), ["same_gap"]);
        assert_eq!(recorded.len(), MAX_RECORDED_SOURCE_GAPS);
        assert!(
            recorded
                .last()
                .is_some_and(|gap| gap.starts_with(SOURCE_GAP_OVERFLOW_PREFIX))
        );
    }

    #[tokio::test]
    async fn completed_source_task_is_detected_during_runtime() {
        let handle = tokio::spawn(async { Ok::<(), Box<dyn Error + Send + Sync>>(()) });
        timeout(Duration::from_secs(1), async {
            while !handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let handles = vec![("binance_aggtrade", handle)];

        assert_eq!(finished_source_task(&handles), Some("binance_aggtrade"));
    }

    #[test]
    fn rtds_freshness_waits_60s_then_uses_two_3s_resubscription_attempts() {
        let observed = rtds_freshness_actions()
            .iter()
            .map(|action| {
                (
                    action.wait.as_secs(),
                    action.resubscribe_attempt,
                    action.exit_after_wait,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(RTDS_FRESHNESS_TIMEOUT_SECS, 60);
        assert_eq!(observed, vec![(3, 1, false), (3, 2, true)]);
    }

    #[test]
    fn rtds_resubscription_attempts_are_finite() {
        assert_eq!(RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW, 2);
        assert!(rtds_freshness_actions().last().unwrap().exit_after_wait);
    }

    #[test]
    fn rtds_freshness_classification_keeps_exact_boundaries_and_invalid_reasons() {
        const BASE_MS: i64 = 1_783_392_400_000;
        let message = |topic: &str, message_type: &str, symbol: &str, payload_timestamp: i64| {
            serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                "topic": topic,
                "type": message_type,
                "timestamp": BASE_MS,
                "payload": {
                    "symbol": symbol,
                    "timestamp": payload_timestamp,
                    "value": "65000.10"
                }
            }))
            .unwrap()
        };
        let status = |message| {
            classify_rtds_message(&message, BASE_MS)
                .unwrap()
                .freshness_status
        };

        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                BASE_MS + 5_000
            )),
            "fresh"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                BASE_MS - 30_000
            )),
            "fresh"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                BASE_MS + 5_001
            )),
            "stale"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                BASE_MS - 30_001
            )),
            "stale"
        );
        assert_eq!(
            status(message("wrong_topic", "update", "btc/usd", BASE_MS)),
            "invalid_topic"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "snapshot",
                "btc/usd",
                BASE_MS
            )),
            "invalid_type"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "eth/usd",
                BASE_MS
            )),
            "invalid_symbol"
        );
        assert_eq!(
            status(message("crypto_prices_chainlink", "update", "btc/usd", 0)),
            "invalid_timestamp"
        );
        assert_eq!(
            status(message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                BASE_MS / 1_000
            )),
            "invalid_timestamp"
        );
        let seconds_message =
            serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "timestamp": BASE_MS / 1_000,
                "payload": {
                    "symbol": "btc/usd",
                    "timestamp": BASE_MS,
                    "value": "65000.10"
                }
            }))
            .unwrap();
        assert_eq!(status(seconds_message), "invalid_timestamp");
    }

    #[tokio::test]
    async fn rtds_only_new_fresh_matching_payload_returns_healthy() {
        let (tx, mut rx) = mpsc::channel(16);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let mut dedupe = TimedFingerprintWindow::with_limits(60 * 60 * 1000, 100);
        let base = now_ms();
        let message =
            |topic: &str, message_type: &str, symbol: &str, payload_timestamp: i64, value: &str| {
                serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                    "topic": topic,
                    "type": message_type,
                    "timestamp": base,
                    "payload": {
                        "symbol": symbol,
                        "timestamp": payload_timestamp,
                        "value": value
                    }
                }))
                .unwrap()
            };
        let run_started = Instant::now();
        for (input, expected_healthy) in [
            (
                message("crypto_prices_chainlink", "snapshot", "btc/usd", base, "1"),
                false,
            ),
            (message("wrong", "update", "btc/usd", base, "2"), false),
            (
                message("crypto_prices_chainlink", "update", "eth/usd", base, "3"),
                false,
            ),
            (
                message(
                    "crypto_prices_chainlink",
                    "update",
                    "btc/usd",
                    base - 31_000,
                    "4",
                ),
                false,
            ),
            (
                message(
                    "crypto_prices_chainlink",
                    "update",
                    "btc/usd",
                    base + 6_000,
                    "5",
                ),
                false,
            ),
            (
                message("crypto_prices_chainlink", "update", "btc/usd", 0, "6"),
                false,
            ),
            (
                message("crypto_prices_chainlink", "update", "btc/usd", base, "7"),
                true,
            ),
        ] {
            assert_eq!(
                emit_rtds_message(&sink, &identity, run_started, &mut dedupe, input,)
                    .await
                    .unwrap(),
                expected_healthy
            );
        }
        let duplicate = message("crypto_prices_chainlink", "update", "btc/usd", base, "7");
        assert!(
            !emit_rtds_message(&sink, &identity, run_started, &mut dedupe, duplicate,)
                .await
                .unwrap()
        );

        let mut statuses = Vec::new();
        while let Ok(timed) = rx.try_recv() {
            let row = rtds_row_from_event(&timed.event).unwrap();
            statuses.push(row.freshness_status);
        }
        assert_eq!(
            statuses,
            vec![
                "invalid_type",
                "invalid_topic",
                "invalid_symbol",
                "stale",
                "stale",
                "invalid_timestamp",
                "fresh"
            ]
        );
    }

    #[tokio::test]
    async fn rtds_expected_topic_payload_schema_error_is_process_fatal() {
        let (tx, mut rx) = mpsc::channel(4);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let mut dedupe = TimedFingerprintWindow::with_limits(60 * 60 * 1000, 100);
        let base = now_ms();
        let valid = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": base,
            "payload": {
                "symbol": "btc/usd",
                "timestamp": base,
                "value": "65000.10"
            }
        }))
        .unwrap();
        assert!(
            emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, valid)
                .await
                .unwrap()
        );
        assert!(rx.recv().await.is_some());

        let malformed =
            serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "timestamp": base,
                "payload": {
                    "symbol": "btc/usd",
                    "timestamp": base
                }
            }))
            .unwrap();
        let error = emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, malformed)
            .await
            .expect_err("subscribed RTDS payload schema errors must terminate the source");
        assert!(error.to_string().contains("RTDS Chainlink payload invalid"));
        assert!(
            gaps.lock()
                .unwrap()
                .iter()
                .any(|gap| gap.starts_with("rtds_unparseable_chainlink_payload:"))
        );
    }

    #[test]
    fn market_freshness_is_independent_per_subscribed_asset() {
        let base = Instant::now();
        let mut watchdog =
            MarketAssetFreshnessWatchdog::new(Duration::from_secs(30), Duration::from_secs(60));
        watchdog.replace_assets(
            BTreeSet::from(["current-up".to_owned(), "current-down".to_owned()]),
            base,
        );
        watchdog.observe(
            &BTreeSet::from(["current-up".to_owned()]),
            base + Duration::from_secs(29),
        );

        let expired = watchdog.expired(base + Duration::from_secs(30));
        assert_eq!(
            expired
                .iter()
                .map(|entry| entry.asset_id.as_str())
                .collect::<Vec<_>>(),
            vec!["current-down"]
        );
        assert_eq!(expired[0].kind, MarketSilenceKind::FirstEvent);

        watchdog.replace_assets(
            BTreeSet::from([
                "current-up".to_owned(),
                "current-down".to_owned(),
                "next-up".to_owned(),
                "next-down".to_owned(),
            ]),
            base + Duration::from_secs(40),
        );
        watchdog.observe(
            &BTreeSet::from(["current-up".to_owned(), "current-down".to_owned()]),
            base + Duration::from_secs(69),
        );
        let expired = watchdog.expired(base + Duration::from_secs(70));
        assert_eq!(
            expired
                .iter()
                .map(|entry| entry.asset_id.as_str())
                .collect::<Vec<_>>(),
            vec!["next-down", "next-up"]
        );
    }

    #[test]
    fn market_silence_allows_only_two_resubscriptions_before_exit() {
        let base = Instant::now();
        let mut watchdog =
            MarketAssetFreshnessWatchdog::new(Duration::from_secs(30), Duration::from_secs(60));
        watchdog.replace_assets(BTreeSet::from(["asset".to_owned()]), base);

        let first = watchdog.expired(base + Duration::from_secs(30));
        watchdog
            .record_resubscribe(&first, base + Duration::from_secs(30))
            .expect("first recovery is allowed");
        let second = watchdog.expired(base + Duration::from_secs(60));
        watchdog
            .record_resubscribe(&second, base + Duration::from_secs(60))
            .expect("second recovery is allowed");
        let exhausted = watchdog.expired(base + Duration::from_secs(90));
        let error = watchdog
            .record_resubscribe(&exhausted, base + Duration::from_secs(90))
            .expect_err("a third recovery must force process restart");

        assert!(error.contains("asset"));
        assert_eq!(MARKET_RESUBSCRIBE_ATTEMPTS_PER_SILENCE, 2);
    }

    #[test]
    fn live_segment_finalize_emits_daily_delta_without_segment_manifest() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut owner = ManifestOwnerState::new(
            dataset_root.clone(),
            test_daily_manifest_identity(),
            BinanceAggTradeContinuityState::default(),
        );
        owner.ensure_open("2026-07-07").unwrap();
        let mut segment = ActiveSegment::new(naming.clone(), "binance_aggtrade", 0).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);

        assert_eq!(segment.writer.write_event(&event).unwrap(), 1);
        segment.observe(&event, 1);
        let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let finalized = finalize_segment(
            segment,
            "shutdown_finalize",
            &test_runtime_provenance("run"),
            &manifest_tx,
        )
        .unwrap();
        let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
            panic!("expected finalized-file delta");
        };
        assert_eq!(delta.final_path, finalized.parquet_path);
        assert_eq!(delta.utc_day, "2026-07-07");
        assert_eq!(delta.utc_hour, "2026-07-07T02");
        assert_eq!(
            delta.facts["hour_window_utc_ms"]["start"],
            1_783_389_600_000i64
        );
        assert_eq!(
            delta.facts["hour_window_utc_ms"]["end"],
            1_783_393_200_000i64
        );
        assert_eq!(delta.part, 0);
        assert_eq!(
            delta.facts["layout_family"],
            "source_specific_hourly_typed_raw"
        );
        assert_eq!(delta.facts["sort_order"], "receive_order");
        assert_eq!(delta.facts["compression"], "zstd_level_3");
        assert!(delta.facts.get("required_typed_fields_present").is_none());
        assert_eq!(delta.facts["backfill_rows"], 0);
        assert_eq!(delta.event_type_counts["aggTrade"], 1);
        assert!(fs::read_dir(&naming.partition_path).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".manifest.json")
        }));

        owner
            .record_operational_counters(
                "2026-07-07",
                &BTreeMap::from([(
                    "binance_aggtrade".to_owned(),
                    SourceOperationalCounterDelta {
                        structural_dedupe_dropped_events: 1,
                        ..SourceOperationalCounterDelta::default()
                    },
                )]),
            )
            .unwrap();
        owner.apply_finalized(delta).unwrap();
        let manifest = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-07"))
            .unwrap()
            .unwrap();
        assert_eq!(
            manifest["source_counters"]["binance_aggtrade"]["written_rows"],
            1
        );
        assert_eq!(
            manifest["source_counters"]["binance_aggtrade"]["backfill_rows"],
            0
        );
        assert_eq!(
            manifest["source_counters"]["binance_aggtrade"]["structural_dedupe_dropped_events"],
            1
        );
        let reconciled = reconcile_open_day_manifest(
            &typed_raw_root(&dataset_root).join("utc_day=2026-07-07"),
            "2026-07-07",
            &test_daily_manifest_identity(),
            &BinanceAggTradeContinuityState::default(),
        )
        .unwrap();
        assert_eq!(reconciled["files"].as_array().unwrap().len(), 1);

        let reader =
            SerializedFileReader::new(File::open(&finalized.parquet_path).unwrap()).unwrap();
        let fields = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"aggregate_trade_id".to_owned()));
        assert!(!fields.contains(&"raw_json".to_owned()));
    }

    #[test]
    fn daily_file_facts_count_rest_and_merged_backfill_rows() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
        for ingest_source in ["rest_backfill", "websocket_rest_merged", "websocket"] {
            let mut event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
            let mut raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
            raw.as_object_mut()
                .unwrap()
                .insert("ingest_source".to_owned(), json!(ingest_source));
            event.raw_json = raw.to_string();
            segment.observe(&event, 1);
        }

        let context = SegmentManifestContext::from_segment(&segment);
        let facts = segment_daily_facts(
            &context,
            "shutdown_finalize",
            &test_runtime_provenance("run"),
        );
        assert_eq!(facts["backfill_rows"], 2);
    }

    #[test]
    fn live_runtime_keeps_same_hour_file_at_former_max_bytes() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        segment.observe(&event, 1);
        segment.bytes = 128 * 1024 * 1024;

        assert_eq!(
            live_segment_rollover_reason(&segment, "2026-07-07T02"),
            None
        );
    }

    #[test]
    fn live_runtime_rolls_segment_on_utc_hour_boundary() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        segment.observe(&event, 1);

        assert_eq!(
            live_segment_rollover_reason(&segment, "2026-07-07T03"),
            Some("utc_hour_boundary")
        );
    }

    #[test]
    fn live_runtime_uses_next_part_when_same_hour_file_already_exists() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let policy = HourlyPartitionPolicy::new(&dataset_root).unwrap();
        let base = policy
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        fs::create_dir_all(&base.partition_path).unwrap();
        fs::write(&base.final_path, b"existing finalized parquet").unwrap();

        let (next, hour_part) =
            next_available_hourly_partition(&policy, "binance_aggtrade", 1_783_392_404_599, None)
                .unwrap();

        assert_eq!(hour_part, 1);
        assert!(next.final_path.ends_with(
            "training/typed_raw/utc_day=2026-07-07/source_kind=binance_aggtrade/binance_aggtrade_02-part-1.parquet"
        ));
    }

    #[test]
    fn source_writer_output_uses_aggregated_stats_without_per_event_receipts() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let provenance = test_runtime_provenance("stats-run");
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let dataset_root_for_worker = dataset_root.clone();
        let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_aggtrade".to_owned(),
                rx,
                queue_rows_for_worker,
                Arc::new(Mutex::new(true)),
                Arc::new(SourceQueueBackpressureStats::default()),
                manifest_tx,
            )
        });

        let events = vec![
            InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_599)),
            InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_600)),
        ];
        queue_rows.store(events.len() as u64, Ordering::Relaxed);
        tx.send(WriterCommand::Events(events)).unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        assert_eq!(output.source_kind, "binance_aggtrade");
        assert_eq!(output.stats.source_kind, "binance_aggtrade");
        assert_eq!(output.stats.events_received, 2);
        assert_eq!(
            output.stats.events_received,
            output
                .stats
                .events_written
                .saturating_add(output.stats.events_dropped)
        );
        let finalized_rows = output
            .finalized_segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>();
        assert_eq!(output.stats.typed_rows_written, finalized_rows);
        assert_eq!(
            output.stats.write_events_us.count,
            output.stats.events_received
        );

        let artifact_dir = dataset_root.join("training/runtime_artifacts/stats-run");
        if artifact_dir.exists() {
            assert!(
                std::fs::read_dir(&artifact_dir).unwrap().all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("retention_after_finalize_")),
                "writer finalization must not emit source-specific retention receipts"
            );
        }
    }

    #[tokio::test]
    async fn writer_finalize_timeout_returns_error_without_waiting_for_worker() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let provenance = test_runtime_provenance("hung-writer-run");
        let writers = start_writer_workers_with_hung_result(&dataset_root, &provenance).unwrap();

        let outcome = stop_writer_workers_with_budget(writers, Duration::from_millis(10)).await;

        assert!(!outcome.all_terminal);
        assert!(
            outcome
                .errors
                .iter()
                .any(|error| error.contains("writer finalize timeout")),
            "expected timeout error, got {:?}",
            outcome.errors
        );
    }

    #[tokio::test]
    async fn writer_stop_collects_late_terminal_state_after_another_writer_failed() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (failed_tx, _failed_rx) = std::sync::mpsc::sync_channel(1);
        let (failed_result_tx, failed_result_rx) = std::sync::mpsc::sync_channel(1);
        drop(failed_result_tx);
        let (late_tx, _late_rx) = std::sync::mpsc::sync_channel(1);
        let (late_result_tx, late_result_rx) = std::sync::mpsc::sync_channel(1);
        let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
        let late_stats = Arc::clone(&runtime_stats);
        let late_worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            late_stats.record_operational_drop("binance_depth_update", 1_783_392_404_599, 2);
            late_result_tx
                .send(Ok(SourceWriterOutput {
                    source_kind: "binance_depth_update".to_owned(),
                    finalized_segments: Vec::new(),
                    stats: SourceWriterStats::empty("binance_depth_update".to_owned()),
                }))
                .unwrap();
        });
        let writers = BTreeMap::from([
            (
                "binance_aggtrade".to_owned(),
                SourceWriterHandle {
                    tx: failed_tx,
                    queue_rows: Arc::new(AtomicU64::new(0)),
                    accepting_commands: Arc::new(Mutex::new(true)),
                    result_rx: failed_result_rx,
                    completed_result: Some(Err(io::Error::other("first writer failed").into())),
                },
            ),
            (
                "binance_depth_update".to_owned(),
                SourceWriterHandle {
                    tx: late_tx,
                    queue_rows: Arc::new(AtomicU64::new(0)),
                    accepting_commands: Arc::new(Mutex::new(true)),
                    result_rx: late_result_rx,
                    completed_result: None,
                },
            ),
        ]);

        let outcome = stop_writer_workers_with_budget(writers, Duration::from_secs(1)).await;
        late_worker.join().unwrap();

        assert!(outcome.all_terminal);
        assert_eq!(outcome.outputs.len(), 1);
        assert_eq!(outcome.outputs[0].source_kind, "binance_depth_update");
        assert_eq!(outcome.errors.len(), 1);
        assert!(outcome.errors[0].contains("binance_aggtrade writer finalize failed"));
        let counters = runtime_stats.drain_all_operational();
        assert_eq!(
            counters["2026-07-07"]["binance_depth_update"].dropped_rows,
            2
        );
        let mut owner = ManifestOwnerState::new(
            dataset_root.clone(),
            test_daily_manifest_identity(),
            BinanceAggTradeContinuityState::default(),
        );
        owner.ensure_open("2026-07-07").unwrap();
        owner
            .record_operational_counters("2026-07-07", &counters["2026-07-07"])
            .unwrap();
        let manifest = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-07"))
            .unwrap()
            .unwrap();
        assert_eq!(
            manifest["source_counters"]["binance_depth_update"]["dropped_rows"],
            2
        );
    }

    #[tokio::test]
    async fn binance_depth_reconnect_records_source_gap_and_snapshot_trigger() {
        let (tx, _rx) = mpsc::channel(1);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut recovery = SourceRecoverySupervisor::new("binance_depth_update", 1);
        let mut snapshot_trigger_reason = "bootstrap".to_owned();

        assert!(
            prepare_binance_depth_reconnect(
                &sink,
                &mut recovery,
                &mut snapshot_trigger_reason,
                "stream_closed".to_owned(),
                None,
            )
            .await
        );

        assert_eq!(recovery.inner_attempts, 1);
        assert_eq!(snapshot_trigger_reason, "reconnect:stream_closed");
        assert!(
            gaps.lock()
                .unwrap()
                .contains(&"binance_depth_reconnect_required:stream_closed".to_owned())
        );
    }

    #[tokio::test]
    async fn binance_depth_bootstrap_connect_failure_uses_retry_budget() {
        let (tx, _rx) = mpsc::channel(1);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut recovery = SourceRecoverySupervisor::new("binance_depth_update", 1);
        let mut snapshot_trigger_reason = "bootstrap".to_owned();

        assert!(
            handle_binance_depth_ws_connect_failure(
                &sink,
                &mut recovery,
                &mut snapshot_trigger_reason,
                "connect_timeout".to_owned(),
                "binance_depth_update_ws_connect_timeout".to_owned(),
                None,
            )
            .await
        );

        assert_eq!(recovery.inner_attempts, 1);
        assert_eq!(snapshot_trigger_reason, "reconnect:connect_timeout");
        let recorded = gaps.lock().unwrap();
        assert!(recorded.contains(&"binance_depth_update_ws_connect_timeout".to_owned()));
        assert!(recorded.contains(&"binance_depth_reconnect_required:connect_timeout".to_owned()));
    }

    #[tokio::test]
    async fn binance_depth_snapshot_keeps_real_fetch_start_and_full_response_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            sleep(Duration::from_millis(30)).await;
            let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let (tx, mut rx) = mpsc::channel(2);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let id = fetch_binance_depth_snapshot(
            &reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
            &sink,
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
            &endpoint,
            "bootstrap",
        )
        .await
        .unwrap();
        assert_eq!(id, 100);
        let event = rx.recv().await.unwrap().event;
        let raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
        let fetch_start = raw["snapshot_fetch_start_utc"].as_i64().unwrap();
        assert!(fetch_start < event.recv_time_utc_ms);
        let row = binance_depth_snapshot_row_from_event(&event).unwrap();
        assert_eq!(row.snapshot_fetch_start_utc, fetch_start);
        assert_eq!(row.timestamp_received, event.recv_time_utc_ms);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gamma_candidate_keeps_real_fetch_start_and_full_response_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            sleep(Duration::from_millis(30)).await;
            let body = r#"[{"id":"market-test"}]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let candidates = fetch_exact_slug_candidates(
            &GammaClient::new(&endpoint).unwrap(),
            "btc-updown-5m-1783392300",
        )
        .await
        .unwrap();
        assert_eq!(candidates.len(), 1);
        let fetch_start = candidates[0].metadata_fetch_start_utc_ms.unwrap();
        let timestamp_received = candidates[0].metadata_received_utc_ms.unwrap();
        assert!(fetch_start < timestamp_received);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_depth_runtime_bootstraps_bridges_resnapshots_and_preserves_order() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
        let snapshot_endpoint = format!(
            "http://{}/api/v3/depth",
            http_listener.local_addr().unwrap()
        );
        let (resnapshot_ready_tx, resnapshot_ready_rx) = tokio::sync::oneshot::channel();

        let http_server = tokio::spawn(async move {
            for (index, last_update_id) in [100_u64, 104_u64].into_iter().enumerate() {
                let (mut socket, _) = http_listener.accept().await.unwrap();
                let mut request = [0_u8; 2048];
                let _ = socket.read(&mut request).await.unwrap();
                let body = json!({
                    "lastUpdateId": last_update_id,
                    "bids": [["1", "2"]],
                    "asks": [["3", "4"]]
                })
                .to_string();
                if index == 1 {
                    sleep(Duration::from_millis(30)).await;
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                if index == 1 {
                    resnapshot_ready_tx.send(()).unwrap();
                    break;
                }
            }
        });
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            for (first, last) in [(101_u64, 102_u64), (103, 103), (100, 100), (105, 105)] {
                websocket
                    .send(Message::Text(
                        json!({
                            "e": "depthUpdate",
                            "E": now_ms(),
                            "s": "BTCUSDT",
                            "U": first,
                            "u": last,
                            "b": [["1", "2"]],
                            "a": [["3", "4"]]
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
            }
            resnapshot_ready_rx.await.unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "e": "depthUpdate",
                        "E": now_ms(),
                        "s": "BTCUSDT",
                        "U": 105_u64,
                        "u": 106_u64,
                        "b": [["1", "2"]],
                        "a": [["3", "4"]]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            sleep(Duration::from_secs(1)).await;
        });

        let (tx, mut rx) = mpsc::channel(16);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        run_binance_depth_source_with_spec(
            BinanceDepthSpec {
                websocket_endpoint,
                snapshot_endpoint,
            },
            SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::clone(&stats),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_millis(600)),
            Instant::now(),
        )
        .await
        .unwrap();

        let mut events = Vec::new();
        while let Ok(timed) = rx.try_recv() {
            events.push(timed.event);
        }
        assert_eq!(
            events
                .iter()
                .map(|event| event.source_event_type.as_str())
                .collect::<Vec<_>>(),
            vec![
                "depth_snapshot",
                "depth_update",
                "depth_update",
                "depth_snapshot",
                "depth_update",
                "depth_update"
            ]
        );
        let rows = events
            .iter()
            .filter(|event| event.source_event_type == "depth_update")
            .map(|event| serde_json::from_str::<Value>(&event.raw_json).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows[0]["u"], 102);
        assert_eq!(rows[1]["u"], 103);
        assert_eq!(rows[2]["u"], 105);
        assert_eq!(rows[2]["snapshot_last_update_id"], 104);
        assert_eq!(
            rows[2]["snapshot_trigger_reason"],
            "stitch_gap:U > local_u + 1"
        );
        assert_eq!(rows[3]["u"], 106);
        let resnapshot_recv_time = events[3].recv_time_utc_ms;
        let triggering_update_recv_time = events[4].recv_time_utc_ms;
        assert!(
            triggering_update_recv_time < resnapshot_recv_time,
            "the triggering frame must retain its pre-resnapshot receive time"
        );
        assert!(rows.iter().all(|row| row.get("local_u_after").is_none()));
        assert!(
            gaps.lock()
                .unwrap()
                .iter()
                .any(|gap| gap == "binance_depth_update_stitch_gap:U > local_u + 1")
        );
        let counters = stats.drain_all_operational();
        assert_eq!(
            counters
                .values()
                .next()
                .unwrap()
                .get("binance_depth_update")
                .unwrap()
                .structural_dedupe_dropped_events,
            1
        );
        http_server.await.unwrap();
        ws_server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_depth_utc_day_resnapshot_reprocesses_the_triggering_frame() {
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
        let snapshot_endpoint = format!(
            "http://{}/api/v3/depth",
            http_listener.local_addr().unwrap()
        );
        let (request_started_tx, mut request_started_rx) = oneshot::channel();
        let http_server = tokio::spawn(async move {
            let (mut socket, _) = http_listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let _ = request_started_tx.send(());
            sleep(Duration::from_millis(30)).await;
            let body = json!({
                "lastUpdateId": 100_u64,
                "bids": [["1", "2"]],
                "asks": [["3", "4"]]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "e": "depthUpdate",
                        "E": now_ms(),
                        "s": "BTCUSDT",
                        "U": 101_u64,
                        "u": 101_u64,
                        "b": [["1", "2"]],
                        "a": [["3", "4"]]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            websocket
                .send(Message::Text(
                    json!({
                        "e": "depthUpdate",
                        "E": now_ms(),
                        "s": "BTCUSDT",
                        "U": 102_u64,
                        "u": 102_u64,
                        "b": [["1", "2"]],
                        "a": [["3", "4"]]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            sleep(Duration::from_millis(200)).await;
        });
        let (tx, mut rx) = mpsc::channel(8);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
        let (control_tx, mut control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let depth_source = run_binance_depth_update_ws_source_with_control(
            stream,
            BinanceDepthUpdateContext {
                http_client: reqwest::Client::new(),
                sink: sink.clone(),
                collector_identity: CollectorIdentity {
                    host_id: "collector-test".to_owned(),
                    region: "local-test".to_owned(),
                },
                deadline: Some(Instant::now() + Duration::from_millis(100)),
                run_started: Instant::now(),
                spec: BinanceDepthSpec {
                    websocket_endpoint,
                    snapshot_endpoint,
                },
                snapshot_last_update_id: 90,
                snapshot_utc_day: "1970-01-01".to_owned(),
                pending_snapshot_trigger_reason: None,
            },
            Some(&mut control_rx),
        );
        let source = async { depth_source.await.map(|_| ()) };
        tokio::pin!(source);
        tokio::select! {
            result = &mut source => panic!("depth source ended before resnapshot started: {result:?}"),
            result = &mut request_started_rx => result.unwrap(),
        }
        assert!(
            fence_depth_and_send_day_barrier(
                source.as_mut(),
                &control_tx,
                &sink,
                Instant::now(),
                "2026-07-07",
                "2026-07-08",
            )
            .await
            .unwrap()
            .is_none()
        );

        let snapshot = rx.recv().await.unwrap().event;
        let update = rx.recv().await.unwrap().event;
        let barrier = rx.recv().await.unwrap();
        assert_eq!(snapshot.source_event_type, "depth_snapshot");
        assert_eq!(update.source_event_type, "depth_update");
        assert!(update.recv_time_utc_ms < snapshot.recv_time_utc_ms);
        let update = serde_json::from_str::<Value>(&update.raw_json).unwrap();
        assert_eq!(update["u"], 101);
        assert_eq!(update["snapshot_last_update_id"], 100);
        assert_eq!(update["snapshot_trigger_reason"], "utc_day_boundary");
        assert!(matches!(
            barrier.control,
            Some(IngressControl::DayBarrier {
                ref producer,
                ref utc_day,
            }) if producer == "binance_depth" && utc_day == "2026-07-07"
        ));
        let after_barrier = tokio::select! {
            message = rx.recv() => message.unwrap(),
            result = &mut source => panic!("depth source ended before buffered next-day frame: {result:?}"),
        };
        assert!(after_barrier.control.is_none());
        assert_eq!(
            serde_json::from_str::<Value>(&after_barrier.event.raw_json).unwrap()["u"],
            102
        );
        http_server.await.unwrap();
        ws_server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_depth_schema_error_is_process_fatal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text("{not-json".into()))
                .await
                .unwrap();
            sleep(Duration::from_millis(100)).await;
        });
        let (tx, _rx) = mpsc::channel(8);
        let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
        let error = run_binance_depth_update_ws_source_with_control(
            stream,
            BinanceDepthUpdateContext {
                http_client: reqwest::Client::new(),
                sink: SourceEventSink {
                    tx,
                    gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                    source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                    inflight_budget: InflightBudget::new(),
                    enqueue_lock: Arc::new(AsyncMutex::new(())),
                },
                collector_identity: CollectorIdentity {
                    host_id: "collector-test".to_owned(),
                    region: "local-test".to_owned(),
                },
                deadline: Some(Instant::now() + Duration::from_secs(1)),
                run_started: Instant::now(),
                spec: BinanceDepthSpec {
                    websocket_endpoint,
                    snapshot_endpoint: "http://127.0.0.1:9/api/v3/depth".to_owned(),
                },
                snapshot_last_update_id: 100,
                snapshot_utc_day: utc_day_string_from_ms(now_ms()).unwrap(),
                pending_snapshot_trigger_reason: None,
            },
            None,
        )
        .await
        .expect_err("depth parser/schema errors must terminate the source");
        assert!(
            error
                .to_string()
                .contains("binance_depth_update_parse_error:parse_depth_update_json"),
            "{error}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_depth_server_shutdown_is_a_transport_event_for_raw_and_combined_frames() {
        for frame in [
            json!({"e": "serverShutdown", "E": now_ms()}),
            json!({
                "stream": "!serverShutdown",
                "data": {"e": "serverShutdown", "E": now_ms()}
            }),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut websocket = accept_async(socket).await.unwrap();
                websocket
                    .send(Message::Text(frame.to_string().into()))
                    .await
                    .unwrap();
                sleep(Duration::from_millis(50)).await;
            });
            let (tx, _rx) = mpsc::channel(2);
            let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
            let outcome =
                run_binance_depth_update_ws_source_with_control(
                    stream,
                    BinanceDepthUpdateContext {
                        http_client: reqwest::Client::new(),
                        sink: SourceEventSink {
                            tx,
                            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                            source_queue_backpressure: Arc::new(
                                SourceQueueBackpressureStats::default(),
                            ),
                            inflight_budget: InflightBudget::new(),
                            enqueue_lock: Arc::new(AsyncMutex::new(())),
                        },
                        collector_identity: CollectorIdentity {
                            host_id: "collector-test".to_owned(),
                            region: "local-test".to_owned(),
                        },
                        deadline: Some(Instant::now() + Duration::from_secs(1)),
                        run_started: Instant::now(),
                        spec: BinanceDepthSpec {
                            websocket_endpoint,
                            snapshot_endpoint: "http://127.0.0.1:9/api/v3/depth".to_owned(),
                        },
                        snapshot_last_update_id: 100,
                        snapshot_utc_day: utc_day_string_from_ms(now_ms()).unwrap(),
                        pending_snapshot_trigger_reason: None,
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                BinanceDepthUpdateExit::StreamEnded {
                    ref reason,
                    saw_business: false,
                } if reason == "server_shutdown"
            ));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn binance_depth_snapshot_retries_429_then_recovers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for status in ["429 Too Many Requests", "200 OK"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = socket.read(&mut request).await.unwrap();
                let body = if status == "200 OK" {
                    r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#
                } else {
                    r#"{}"#
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let (tx, mut rx) = mpsc::channel(2);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let result = fetch_binance_depth_snapshot(
            &reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
            &SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
            &endpoint,
            "stitch_gap:test",
        )
        .await
        .unwrap();
        assert_eq!(result, 100);
        assert!(rx.recv().await.is_some());
        assert!(
            gaps.lock()
                .unwrap()
                .iter()
                .any(|gap| gap.contains("attempt=1:http_status:429"))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_depth_snapshot_timeout_exhaustion_is_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = socket.read(&mut request).await.unwrap();
                sleep(Duration::from_millis(50)).await;
            }
        });
        let (tx, _rx) = mpsc::channel(1);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let error = fetch_binance_depth_snapshot(
            &reqwest::Client::builder()
                .timeout(Duration::from_millis(10))
                .build()
                .unwrap(),
            &SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
            &endpoint,
            "bootstrap",
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("binance_depth_snapshot_recovery_exhausted")
        );
        assert_eq!(
            gaps.lock()
                .unwrap()
                .iter()
                .filter(|gap| gap.starts_with("binance_depth_snapshot_attempt_failed:"))
                .count(),
            usize::try_from(BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS).unwrap()
        );
        server.await.unwrap();
    }

    #[test]
    fn polymarket_price_change_stats_count_typed_rows_not_raw_events() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let provenance = test_runtime_provenance("polymarket-stats-run");
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let dataset_root_for_worker = dataset_root.clone();
        let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "polymarket_market".to_owned(),
                rx,
                queue_rows_for_worker,
                Arc::new(Mutex::new(true)),
                Arc::new(SourceQueueBackpressureStats::default()),
                manifest_tx,
            )
        });

        let event = test_polymarket_price_change_event(1_783_392_404_600);
        let value = serde_json::from_str::<Value>(&event.raw_json).unwrap();
        let events = value["price_changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|change| {
                let mut row_event = event.clone();
                let mut row_value = value.clone();
                row_value["price_changes"] = Value::Array(vec![change.clone()]);
                row_event.raw_json = row_value.to_string();
                InflightRawEvent::for_test(row_event)
            })
            .collect::<Vec<_>>();
        queue_rows.store(events.len() as u64, Ordering::Relaxed);
        tx.send(WriterCommand::Events(events)).unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        assert_eq!(output.stats.events_received, 2);
        assert_eq!(output.stats.events_written, 2);
        assert_eq!(output.stats.typed_rows_written, 2);
        assert_eq!(output.stats.finalized_segment_rows, 2);

        let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
            panic!("expected finalized-file delta");
        };
        assert_eq!(delta.rows, 2);
        assert_eq!(delta.event_type_counts["price_change"], 2);
    }

    #[test]
    fn binance_depth_manifest_carries_compact_stitch_evidence() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let provenance = test_runtime_provenance("depth-stitch-run");
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let dataset_root_for_worker = dataset_root.clone();
        let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_depth_update".to_owned(),
                rx,
                queue_rows_for_worker,
                Arc::new(Mutex::new(true)),
                Arc::new(SourceQueueBackpressureStats::default()),
                manifest_tx,
            )
        });

        let event = test_binance_depth_update_event(1_783_392_404_600);
        queue_rows.store(1, Ordering::Relaxed);
        tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
            event,
        )]))
        .unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        assert_eq!(output.stats.typed_rows_written, 1);
        let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
            panic!("expected finalized-file delta");
        };
        assert_eq!(delta.event_type_counts["depth_update"], 1);
        assert_eq!(
            delta.facts["binance_depth_stitch"]["snapshot_last_update_id"],
            100
        );
        assert_eq!(
            delta.facts["binance_depth_stitch"]["stitch_applied_rows"],
            1
        );
        assert_eq!(delta.facts["binance_depth_stitch"]["gap_rows"], 0);
        assert_eq!(delta.facts["binance_depth_stitch"]["resnapshot_count"], 0);
        assert_eq!(delta.facts["binance_depth_stitch"]["local_u_min"], 103);
        assert_eq!(delta.facts["binance_depth_stitch"]["local_u_max"], 103);
        assert_eq!(
            delta.facts["binance_depth_stitch"]["first_update_bridged_snapshot"],
            true
        );
        assert_eq!(
            utc_day_string_from_ms(1_783_392_404_600).as_deref(),
            Some("2026-07-07")
        );
    }

    #[test]
    fn binance_depth_manifest_counts_resnapshot_trigger_evidence() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let provenance = test_runtime_provenance("depth-resnapshot-run");
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let dataset_root_for_worker = dataset_root.clone();
        let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_depth_update".to_owned(),
                rx,
                queue_rows_for_worker,
                Arc::new(Mutex::new(true)),
                Arc::new(SourceQueueBackpressureStats::default()),
                manifest_tx,
            )
        });

        let event = test_binance_depth_update_event_with_trigger(
            1_783_392_404_600,
            "stitch_gap:U > local_u + 1",
        );
        queue_rows.store(1, Ordering::Relaxed);
        tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
            event,
        )]))
        .unwrap();
        drop(tx);

        let _output = handle.join().unwrap().unwrap();
        let ManifestCommand::Finalized(delta) = manifest_rx.blocking_recv().unwrap() else {
            panic!("expected finalized-file delta");
        };
        assert_eq!(
            delta.facts["binance_depth_stitch"]["stitch_applied_rows"],
            1
        );
        assert_eq!(delta.facts["binance_depth_stitch"]["gap_rows"], 1);
        assert_eq!(delta.facts["binance_depth_stitch"]["resnapshot_count"], 1);
        assert!(
            delta.facts["source_gaps"]
                .as_array()
                .is_some_and(|gaps| !gaps.is_empty())
        );
    }

    #[test]
    fn polymarket_book_raw_event_keeps_full_depth_not_lengths() {
        let WsMessage::Book(book) = serde_json::from_str(
            r#"{
                "event_type": "book",
                "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
                "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                "timestamp": "1783392404599",
                "bids": [{"price": "0.49", "size": "120.5"}],
                "asks": [{"price": "0.51", "size": "88"}],
                "hash": "book-hash"
            }"#,
        )
        .unwrap()
        else {
            panic!("expected book message");
        };

        let raw = polymarket_book_raw_json(&book, "btc-updown-5m-1783392300");

        assert_eq!(raw["event_type"], "book");
        assert_eq!(raw["bids"][0][0], "0.49");
        assert_eq!(raw["bids"][0][1], "120.5");
        assert_eq!(raw["asks"][0][0], "0.51");
        assert_eq!(raw["asks"][0][1], "88");
        assert!(raw.get("bids_len").is_none());
        assert!(raw.get("asks_len").is_none());
    }

    #[test]
    fn polymarket_mixed_price_change_keeps_only_target_entries() {
        let target = "101";
        let external = "999";
        let WsMessage::PriceChange(mut event) = serde_json::from_str(&format!(
            r#"{{
                "event_type": "price_change",
                "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp": "1783392404600",
                "price_changes": [
                    {{"asset_id":"{target}","price":"0.52","size":"10","side":"BUY"}},
                    {{"asset_id":"{external}","price":"0.48","size":"8","side":"SELL"}}
                ]
            }}"#
        ))
        .unwrap() else {
            panic!("expected price_change message");
        };
        let asset_to_slug =
            BTreeMap::from([(target.to_owned(), "btc-updown-5m-1783392300".to_owned())]);

        let matched = retain_target_price_changes(&mut event, &asset_to_slug);
        let raw = polymarket_price_change_raw_json(&event, &asset_to_slug);

        assert_eq!(matched, BTreeSet::from([target.to_owned()]));
        assert_eq!(event.price_changes.len(), 1);
        assert_eq!(event.price_changes[0].asset_id.to_string(), target);
        assert_eq!(raw["price_changes"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn unmatched_lifecycle_events_and_synthetic_discovery_produce_no_market_rows() {
        let WsMessage::NewMarket(new_market) = serde_json::from_str(
            r#"{
                "id":"market-1",
                "question":"Question",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":"unmatched",
                "description":"Description",
                "assets_ids":["999","1000"],
                "outcomes":["Up","Down"],
                "timestamp":"1783392404600",
                "event_type":"new_market"
            }"#,
        )
        .unwrap() else {
            panic!("expected new_market message");
        };
        let asset_to_slug =
            BTreeMap::from([("101".to_owned(), "btc-updown-5m-1783392300".to_owned())]);
        assert!(matching_target_asset_ids(&new_market.asset_ids, &asset_to_slug).is_empty());

        let synthetic = RawEvent {
            source_kind: "polymarket_market".to_owned(),
            source_event_type: "market_discovery_switch".to_owned(),
            source_identifier: "current:next".to_owned(),
            source_ts: Some("1783392404600".to_owned()),
            recv_time_utc_ms: 1_783_392_404_600,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
            raw_json: json!({
                "event_type": "market_discovery_switch",
                "current": {"condition_id":"0x01","up_asset_id":"101","down_asset_id":"102"},
                "next": {"condition_id":"0x02","up_asset_id":"201","down_asset_id":"202"}
            })
            .to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_market".to_owned(),
        };
        assert!(
            polymarket_orderbook_rows_from_event(&synthetic)
                .unwrap()
                .is_empty()
        );

        let native = RawEvent {
            source_event_type: "new_market".to_owned(),
            source_identifier: "native".to_owned(),
            raw_json: json!({
                "event_type":"new_market",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_ids":["101","102"],
                "timestamp":1_783_392_404_600_i64
            })
            .to_string(),
            ..synthetic
        };
        let rows = polymarket_orderbook_rows_from_event(&native).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, "new_market");
    }

    #[tokio::test]
    async fn polymarket_dispatch_writes_only_matching_native_business_events() {
        let (tx, mut rx) = mpsc::channel(8);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let discovery = test_btc_discovery();
        let asset_to_slug = discovery_asset_to_slug(&discovery);
        let run_started = Instant::now();

        let WsMessage::PriceChange(mixed) = serde_json::from_str(
            r#"{
                "event_type":"price_change",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp":"1783392404600",
                "price_changes":[
                    {"asset_id":"101","price":"0.52","size":"10","side":"BUY"},
                    {"asset_id":"999","price":"0.48","size":"8","side":"SELL"}
                ]
            }"#,
        )
        .unwrap() else {
            panic!("expected price_change message");
        };
        let outcome = dispatch_polymarket_message(
            WsMessage::PriceChange(mixed),
            &discovery,
            &asset_to_slug,
            &sink,
            &identity,
            run_started,
            r#"{"source":"test"}"#,
        )
        .await
        .unwrap();
        assert_eq!(outcome.matched_assets, BTreeSet::from(["101".to_owned()]));
        let written = rx.try_recv().unwrap().event;
        let written_json: Value = serde_json::from_str(&written.raw_json).unwrap();
        assert_eq!(written_json["price_changes"].as_array().unwrap().len(), 1);
        assert_eq!(written_json["price_changes"][0]["asset_id"], "101");

        let WsMessage::NewMarket(unmatched_new) = serde_json::from_str(
            r#"{
                "id":"market-unmatched",
                "question":"Question",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
                "slug":"unmatched",
                "description":"Description",
                "assets_ids":["999","1000"],
                "outcomes":["Up","Down"],
                "timestamp":"1783392404601",
                "event_type":"new_market"
            }"#,
        )
        .unwrap() else {
            panic!("expected new_market message");
        };
        let outcome = dispatch_polymarket_message(
            WsMessage::NewMarket(unmatched_new),
            &discovery,
            &asset_to_slug,
            &sink,
            &identity,
            run_started,
            r#"{"source":"test"}"#,
        )
        .await
        .unwrap();
        assert!(outcome.matched_assets.is_empty());
        assert!(rx.try_recv().is_err());

        let WsMessage::MarketResolved(unmatched_resolved) = serde_json::from_str(
            r#"{
                "id":"market-unmatched",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
                "assets_ids":["999","1000"],
                "winning_asset_id":"999",
                "winning_outcome":"Up",
                "timestamp":"1783392404602",
                "event_type":"market_resolved"
            }"#,
        )
        .unwrap() else {
            panic!("expected market_resolved message");
        };
        let outcome = dispatch_polymarket_message(
            WsMessage::MarketResolved(unmatched_resolved),
            &discovery,
            &asset_to_slug,
            &sink,
            &identity,
            run_started,
            r#"{"source":"test"}"#,
        )
        .await
        .unwrap();
        assert!(outcome.matched_assets.is_empty());
        assert!(rx.try_recv().is_err());

        let WsMessage::NewMarket(matching_new) = serde_json::from_str(
            r#"{
                "id":"market-native",
                "question":"Bitcoin Up or Down",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":"btc-updown-5m-1783392300",
                "description":"Description",
                "assets_ids":["101","102"],
                "outcomes":["Up","Down"],
                "timestamp":"1783392404603",
                "event_type":"new_market"
            }"#,
        )
        .unwrap() else {
            panic!("expected new_market message");
        };
        let outcome = dispatch_polymarket_message(
            WsMessage::NewMarket(matching_new),
            &discovery,
            &asset_to_slug,
            &sink,
            &identity,
            run_started,
            r#"{"source":"test"}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome.matched_assets,
            BTreeSet::from(["101".to_owned(), "102".to_owned()])
        );
        let written = [rx.try_recv().unwrap().event, rx.try_recv().unwrap().event];
        assert_eq!(
            written
                .iter()
                .filter(|event| {
                    event.source_kind == "polymarket_market"
                        && event.source_event_type == "new_market"
                })
                .count(),
            1
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn polymarket_source_native_canonical_covers_fields_lost_by_the_old_layout() {
        let message = serde_json::from_str::<WsMessage>(
            r#"{
                "event_type":"best_bid_ask",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "best_bid":"0.49",
                "best_ask":"0.51",
                "spread":"0.02",
                "timestamp":"1783392404600"
            }"#,
        )
        .unwrap();
        let changed_spread = serde_json::from_str::<WsMessage>(
            r#"{
                "event_type":"best_bid_ask",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "best_bid":"0.49",
                "best_ask":"0.51",
                "spread":"0.020",
                "timestamp":"1783392404600"
            }"#,
        )
        .unwrap();
        let first = polymarket_source_native_payload_json(&message)
            .unwrap()
            .unwrap();
        let second = polymarket_source_native_payload_json(&changed_spread)
            .unwrap()
            .unwrap();
        assert_ne!(first, second);
        assert!(!first.contains("btc-updown-5m"));

        let new_market_a = serde_json::from_str::<WsMessage>(
            r#"{
                "id":"market-native",
                "question":"Bitcoin Up or Down",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":"btc-updown-5m-1783392300",
                "description":"description-a",
                "assets_ids":["101","102"],
                "outcomes":["Up","Down"],
                "event_message":{"id":"1","ticker":"btc","slug":"btc","title":"title","description":"event-a"},
                "timestamp":"1783392404600",
                "event_type":"new_market"
            }"#,
        )
        .unwrap();
        let new_market_b = serde_json::from_str::<WsMessage>(
            r#"{
                "id":"market-native",
                "question":"Bitcoin Up or Down",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug":"btc-updown-5m-1783392300",
                "description":"description-b",
                "assets_ids":["101","102"],
                "outcomes":["Up","Down"],
                "event_message":{"id":"1","ticker":"btc","slug":"btc","title":"title","description":"event-b"},
                "timestamp":"1783392404600",
                "event_type":"new_market"
            }"#,
        )
        .unwrap();
        assert_ne!(
            polymarket_source_native_payload_json(&new_market_a)
                .unwrap()
                .unwrap(),
            polymarket_source_native_payload_json(&new_market_b)
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn polymarket_source_native_canonical_covers_every_market_variant() {
        let cases = [
            (
                "book",
                r#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","timestamp":"1783392404600","bids":[{"price":"0.49","size":"10"}],"asks":[{"price":"0.51","size":"8"}],"hash":"h"}"#,
            ),
            (
                "price_change",
                r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","timestamp":"1783392404600","price_changes":[{"asset_id":"101","price":"0.50","size":"10","side":"BUY","hash":"h","best_bid":"0.49","best_ask":"0.51"}]}"#,
            ),
            (
                "tick_size_change",
                r#"{"event_type":"tick_size_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1783392404600"}"#,
            ),
            (
                "last_trade_price",
                r#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","price":"0.50","side":"BUY","size":"10","fee_rate_bps":"0","timestamp":"1783392404600"}"#,
            ),
            (
                "best_bid_ask",
                r#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","best_bid":"0.49","best_ask":"0.51","spread":"0.02","timestamp":"1783392404600"}"#,
            ),
            (
                "new_market",
                r#"{"event_type":"new_market","id":"m","question":"q","market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":"native-slug","description":"d","assets_ids":["101","102"],"outcomes":["Up","Down"],"timestamp":"1783392404600"}"#,
            ),
            (
                "market_resolved",
                r#"{"event_type":"market_resolved","id":"m","question":"q","market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":"native-slug","description":"d","assets_ids":["101","102"],"outcomes":["Up","Down"],"winning_asset_id":"101","winning_outcome":"Up","timestamp":"1783392404600"}"#,
            ),
        ];

        for (event_type, raw) in cases {
            let message = serde_json::from_str::<WsMessage>(raw).unwrap();
            let canonical = polymarket_source_native_payload_json(&message)
                .unwrap()
                .unwrap();
            let value = serde_json::from_str::<Value>(&canonical).unwrap();
            assert_eq!(value["source"], "polymarket_market");
            assert_eq!(value["event_type"], event_type);
        }
    }

    #[test]
    fn polymarket_message_dedupe_happens_once_before_price_change_fanout() {
        let discovery = test_btc_discovery();
        let asset_to_slug = discovery_asset_to_slug(&discovery);
        let mut dedupe = LifecycleFingerprintSets::with_limit(8);
        dedupe
            .replace_lifecycles(expected_lifecycle_slugs(&discovery))
            .unwrap();
        let message = serde_json::from_str::<WsMessage>(
            r#"{
                "event_type":"price_change",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp":"1783392404600",
                "price_changes":[
                    {"asset_id":"101","price":"0.52","size":"10","side":"BUY"},
                    {"asset_id":"102","price":"0.48","size":"8","side":"SELL"}
                ]
            }"#,
        )
        .unwrap();
        let stats = SourceQueueBackpressureStats::default();

        assert!(
            prepare_polymarket_message(
                &mut dedupe,
                &discovery,
                &asset_to_slug,
                &message,
                Some(&stats),
            )
            .unwrap()
            .is_some()
        );
        assert!(
            prepare_polymarket_message(
                &mut dedupe,
                &discovery,
                &asset_to_slug,
                &message,
                Some(&stats),
            )
            .unwrap()
            .is_none(),
            "the replay is dropped once at message level, not independently per output row"
        );
        let counters = stats.drain_all_operational();
        let counter = counters
            .values()
            .next()
            .unwrap()
            .get("polymarket_market")
            .unwrap();
        assert_eq!(counter.structural_dedupe_dropped_events, 1);
    }

    #[test]
    fn polymarket_dedupe_restores_from_finalized_parquet_before_process_b_replay() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let discovery = test_btc_discovery();
        let asset_to_slug = discovery_asset_to_slug(&discovery);
        let message = serde_json::from_str::<WsMessage>(
            r#"{
                "event_type":"price_change",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp":"1783392404600",
                "price_changes":[
                    {"asset_id":"101","price":"0.52","size":"10","side":"BUY"},
                    {"asset_id":"102","price":"0.48","size":"8","side":"SELL"}
                ]
            }"#,
        )
        .unwrap();
        let canonical = polymarket_source_native_payload_json(&message)
            .unwrap()
            .unwrap();
        let WsMessage::PriceChange(event) = &message else {
            panic!("expected price_change message");
        };
        let raw_json = with_source_native_payload_json(
            polymarket_price_change_raw_json(event, &asset_to_slug),
            &canonical,
        )
        .to_string();
        let raw_event = RawEvent {
            source_kind: "polymarket_market".to_owned(),
            source_event_type: "price_change".to_owned(),
            source_identifier: format!("{}:101", discovery.current.slug),
            source_ts: Some(event.timestamp.to_string()),
            recv_time_utc_ms: now_ms(),
            collector_host_id: "collector-a".to_owned(),
            collector_region: "test".to_owned(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            raw_json,
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_market".to_owned(),
        };
        let rows = polymarket_orderbook_rows_from_event(&raw_event).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row.source_native_payload_json == canonical)
        );

        let day = utc_day_string_from_ms(raw_event.recv_time_utc_ms).unwrap();
        let source_dir = dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join("source_kind=polymarket_market");
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &source_dir,
            "process-a",
            "polymarket_orderbook_v2",
            "polymarket_market",
        )
        .unwrap();
        writer.write_polymarket_orderbook_rows(&rows).unwrap();
        writer.finalize().unwrap();

        let mut process_b = LifecycleFingerprintSets::with_limit(8);
        process_b
            .replace_lifecycles(expected_lifecycle_slugs(&discovery))
            .unwrap();
        restore_polymarket_dedupe(&dataset_root, &mut process_b).unwrap();
        assert!(
            prepare_polymarket_message(&mut process_b, &discovery, &asset_to_slug, &message, None,)
                .unwrap()
                .is_none(),
            "process B must drop a message already finalized by process A"
        );
    }

    #[test]
    fn binance_aggtrade_dedupe_restores_id_and_complete_payload() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let recv_time_utc_ms = 1_783_392_404_599;
        let payload = json!({
            "e": "aggTrade",
            "E": 1_783_392_404_599_i64,
            "s": "BTCUSDT",
            "a": 123_456_u64,
            "p": "65000.10",
            "q": "0.001",
            "f": 123_450_u64,
            "l": 123_456_u64,
            "T": 1_783_392_404_599_i64,
            "m": true,
            "M": false
        });
        let websocket =
            BinanceWsAggTrade::from_wire(&payload, "btcusdt@aggTrade", recv_time_utc_ms).unwrap();
        let native_id = websocket.shared.aggregate_trade_id;
        let canonical = websocket.source_native_payload_json.clone();
        let spec = BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url: BINANCE_AGGTRADE_WS.to_owned(),
            rest_endpoint: BINANCE_AGGTRADE_REST.to_owned(),
            freshness_timeout_secs: 60,
        };
        let event = binance_aggtrade_raw_event(
            &BinanceAggTradeOutput::Websocket(websocket),
            &CollectorIdentity {
                host_id: "collector-a".to_owned(),
                region: "test".to_owned(),
            },
            &spec,
        )
        .unwrap();
        let row = binance_aggtrade_row_from_event(&event).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&row.source_native_payload_json).unwrap()["websocket"],
            canonical
        );

        let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
        let source_dir = dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join("source_kind=binance_aggtrade");
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &source_dir,
            "binance_aggtrade_02",
            "binance_aggtrade_v5",
            "binance_aggtrade",
        )
        .unwrap();
        writer.write_binance_aggtrade_rows(&[row]).unwrap();
        append_test_recovery_metadata(
            &mut writer,
            "binance_aggtrade",
            "2026-07-07T02",
            0,
            1,
            recv_time_utc_ms,
            recv_time_utc_ms,
            "aggTrade",
            "binance_aggtrade_v5",
        );
        writer.finalize().unwrap();

        let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
        let mut process_b =
            restore_binance_aggtrade_recovery(&dataset_root, &day, &test_daily_manifest_identity())
                .unwrap()
                .websocket_dedupe;
        assert_eq!(
            process_b.observe(native_id, canonical.as_bytes()),
            BinanceIdDecision::Duplicate
        );
        let mut changed = payload;
        changed["M"] = Value::Bool(true);
        let changed_canonical =
            BinanceWsAggTrade::from_wire(&changed, "btcusdt@aggTrade", recv_time_utc_ms + 1)
                .unwrap()
                .source_native_payload_json;
        assert_eq!(
            process_b.observe(native_id, changed_canonical.as_bytes()),
            BinanceIdDecision::Conflict,
            "a field outside the WS/REST shared subset is still part of same-source payload identity"
        );
    }

    #[test]
    fn binance_recovery_closes_prior_open_day_and_overlays_current_finalized_tail() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let prior_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
        let prior_source_dir = prior_day_dir.join("source_kind=binance_aggtrade");
        fs::create_dir_all(&prior_source_dir).unwrap();
        let mut prior_writer = TypedParquetSegmentWriter::create_for_typed_source(
            &prior_source_dir,
            "binance_aggtrade_23",
            "binance_aggtrade_v5",
            "binance_aggtrade",
        )
        .unwrap();
        prior_writer
            .write_binance_aggtrade_rows(&[
                test_aggtrade_hourly_row(100, 1_783_987_100_000),
                test_aggtrade_hourly_row(101, 1_783_987_100_001),
            ])
            .unwrap();
        append_test_recovery_metadata(
            &mut prior_writer,
            "binance_aggtrade",
            "2026-07-13T23",
            0,
            2,
            1_783_987_100_000,
            1_783_987_100_001,
            "aggTrade",
            "binance_aggtrade_v5",
        );
        prior_writer.finalize().unwrap();
        crate::daily_manifest::write_json_atomic(
            &prior_day_dir.join("manifest.json"),
            &json!({
                "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
                "utc_day": "2026-07-13",
                "state": "open",
                "build_commit": "test-code-version",
                "collector_binary_sha256": "test-binary-sha256",
                "config_digest": "test-source-config-digest",
                "collector_host_id": "collector-test",
                "collector_region": "local-test",
                "files": [],
                "binance_aggtrade_continuity": BinanceAggTradeContinuityState::default(),
            }),
        )
        .unwrap();

        let recovered = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .unwrap();
        let prior_manifest = load_day_manifest(&prior_day_dir).unwrap().unwrap();
        assert_eq!(prior_manifest["state"], "closed");
        assert_eq!(prior_manifest["closure_reason"], "unclean_exit_recovery");
        assert_eq!(prior_manifest["files"].as_array().unwrap().len(), 1);
        assert_eq!(
            prior_manifest["files"][0]["facts"]["collector_binary_sha256"],
            "test-binary-sha256"
        );
        assert_eq!(
            prior_manifest["files"][0]["event_type_counts"]["aggTrade"],
            2
        );
        assert_eq!(
            prior_manifest["binance_aggtrade_continuity"]["latest_epoch"]["last_contiguous_id"],
            101
        );
        assert_eq!(
            recovered
                .continuity
                .latest_epoch
                .as_ref()
                .unwrap()
                .last_contiguous_id,
            101
        );

        let current_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
        let current_source_dir = current_day_dir.join("source_kind=binance_aggtrade");
        fs::create_dir_all(&current_source_dir).unwrap();
        let mut current_writer = TypedParquetSegmentWriter::create_for_typed_source(
            &current_source_dir,
            "binance_aggtrade_00",
            "binance_aggtrade_v5",
            "binance_aggtrade",
        )
        .unwrap();
        current_writer
            .write_binance_aggtrade_rows(&[test_aggtrade_hourly_row(102, 1_783_987_200_000)])
            .unwrap();
        append_test_recovery_metadata(
            &mut current_writer,
            "binance_aggtrade",
            "2026-07-14T00",
            0,
            1,
            1_783_987_200_000,
            1_783_987_200_000,
            "aggTrade",
            "binance_aggtrade_v5",
        );
        current_writer.finalize().unwrap();

        let recovered = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .unwrap();
        assert_eq!(
            recovered
                .continuity
                .latest_epoch
                .as_ref()
                .unwrap()
                .last_contiguous_id,
            102,
            "a finalized rename not yet reflected in today's manifest must advance the tail"
        );
        let canonical = BinanceWsAggTrade::from_wire(
            &test_ws_aggtrade_value(102),
            "btcusdt@aggTrade",
            1_783_987_200_000,
        )
        .unwrap()
        .source_native_payload_json;
        let mut websocket_dedupe = recovered.websocket_dedupe;
        assert_eq!(
            websocket_dedupe.observe(102, canonical.as_bytes()),
            BinanceIdDecision::Duplicate
        );
        assert!(!prior_day_dir.join("manifest.json.tmp").exists());
    }

    #[test]
    fn binance_recovery_uses_closed_day_when_current_manifest_has_no_parquet() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let prior_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
        let current_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
        fs::create_dir_all(&prior_day_dir).unwrap();
        fs::create_dir_all(&current_day_dir).unwrap();
        let continuity = BinanceAggTradeContinuityState {
            latest_epoch: Some(crate::binance_aggtrade::BinanceAggTradeContinuityEpoch {
                epoch_start_id: 90,
                last_observed_id: 99,
                last_contiguous_id: 99,
            }),
            unresolved_ranges: Vec::new(),
            initial_coverage_boundary_id: Some(90),
        };
        crate::daily_manifest::write_json_atomic(
            &prior_day_dir.join("manifest.json"),
            &json!({
                "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
                "utc_day": "2026-07-13",
                "state": "closed",
                "build_commit": "test-code-version",
                "collector_binary_sha256": "test-binary-sha256",
                "config_digest": "test-source-config-digest",
                "collector_host_id": "collector-test",
                "collector_region": "local-test",
                "files": [],
                "binance_aggtrade_continuity": continuity,
            }),
        )
        .unwrap();
        crate::daily_manifest::write_json_atomic(
            &current_day_dir.join("manifest.json"),
            &json!({
                "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
                "utc_day": "2026-07-14",
                "state": "open",
                "build_commit": "test-code-version",
                "collector_binary_sha256": "test-binary-sha256",
                "config_digest": "test-source-config-digest",
                "collector_host_id": "collector-test",
                "collector_region": "local-test",
                "files": [],
                "binance_aggtrade_continuity": continuity,
            }),
        )
        .unwrap();

        let recovered = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .unwrap();
        assert_eq!(recovered.continuity, continuity);
        assert!(recovered.websocket_dedupe.is_empty());
        assert!(recovered.rest_dedupe.is_empty());
    }

    #[test]
    fn binance_recovery_rejects_closed_day_file_set_changes_before_source_start() {
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let (_day_dir, final_path) = create_closed_test_aggtrade_day(&dataset_root);
            fs::remove_file(final_path).unwrap();

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &test_daily_manifest_identity(),
            )
            .err()
            .expect("a closed-day missing file must stop recovery before sources start");
            assert!(error.to_string().contains("missing finalized Parquet"));
        }
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let (day_dir, final_path) = create_closed_test_aggtrade_day(&dataset_root);
            fs::remove_file(final_path).unwrap();
            write_test_aggtrade_recovery_file(
                &day_dir,
                "2026-07-13T23",
                0,
                100,
                2,
                1_783_987_100_000,
            );

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &test_daily_manifest_identity(),
            )
            .err()
            .expect("a closed-day mutated file must stop recovery before sources start");
            assert!(error.to_string().contains("does not match disk"));
        }
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
            write_test_aggtrade_recovery_file(
                &day_dir,
                "2026-07-13T23",
                1,
                101,
                1,
                1_783_987_100_001,
            );

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &test_daily_manifest_identity(),
            )
            .err()
            .expect("a closed-day extra file must stop recovery before sources start");
            assert!(error.to_string().contains("unrecorded finalized Parquet"));
        }
    }

    #[test]
    fn binance_recovery_rejects_manifest_schema_and_identity_corruption_before_source_start() {
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
            let mut manifest = load_day_manifest(&day_dir).unwrap().unwrap();
            manifest["schema_version"] = json!("data_collect_daily_manifest_v1");
            crate::daily_manifest::write_json_atomic(&day_dir.join("manifest.json"), &manifest)
                .unwrap();

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &test_daily_manifest_identity(),
            )
            .err()
            .expect("a wrong manifest schema must stop recovery before sources start");
            assert!(error.to_string().contains("manifest schema mismatch"));
        }
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
            let mut manifest = load_day_manifest(&day_dir).unwrap().unwrap();
            manifest.as_object_mut().unwrap().remove("config_digest");
            crate::daily_manifest::write_json_atomic(&day_dir.join("manifest.json"), &manifest)
                .unwrap();

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &test_daily_manifest_identity(),
            )
            .err()
            .expect("a missing manifest identity must stop recovery before sources start");
            assert!(
                error
                    .to_string()
                    .contains("identity field missing: config_digest")
            );
        }
        {
            let tmp = tempdir().unwrap();
            let dataset_root = tmp.path().canonicalize().unwrap();
            let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
            write_test_aggtrade_recovery_file(
                &day_dir,
                "2026-07-14T00",
                0,
                100,
                1,
                1_783_987_200_000,
            );
            let mut wrong_identity = test_daily_manifest_identity();
            wrong_identity.config_digest = "different-config".to_owned();

            let error = restore_binance_aggtrade_recovery(
                &dataset_root,
                "2026-07-14",
                &wrong_identity,
            )
            .err()
            .expect(
                "an orphan footer with another identity must stop recovery before sources start",
            );
            assert!(
                error
                    .to_string()
                    .contains("recovery collection identity mismatch")
            );
        }
    }

    #[test]
    fn startup_recovery_crossing_midnight_closes_prior_day_before_source_construction() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let mut clock = [
            1_783_987_199_000,
            1_783_987_201_000,
            1_783_987_201_000,
            1_783_987_202_000,
        ]
        .into_iter();

        let startup = reconcile_stable_startup_recovery(
            &dataset_root,
            &test_daily_manifest_identity(),
            || clock.next().expect("startup recovery clock sample"),
        )
        .unwrap();

        assert_eq!(startup.utc_day, "2026-07-14");
        assert_eq!(startup.initial_next_boundary_ms, 1_784_073_600_000);
        assert!(clock.next().is_none());
        let prior = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-13"))
            .unwrap()
            .unwrap();
        assert_eq!(prior["state"], "closed");
        assert_eq!(prior["closure_reason"], "unclean_exit_recovery");
        let current = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-14"))
            .unwrap()
            .unwrap();
        assert_eq!(current["state"], "open");
    }

    #[test]
    fn binance_recovery_keeps_only_the_latest_fixed_id_window() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let source_dir = dataset_root
            .join("training/typed_raw/utc_day=2026-07-13")
            .join("source_kind=binance_aggtrade");
        fs::create_dir_all(&source_dir).unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &source_dir,
            "binance_aggtrade_00",
            "binance_aggtrade_v5",
            "binance_aggtrade",
        )
        .unwrap();
        let rows = (1..=25_002)
            .map(|id| test_aggtrade_hourly_row(id, 1_783_900_800_000 + i64::try_from(id).unwrap()))
            .collect::<Vec<_>>();
        writer.write_binance_aggtrade_rows(&rows).unwrap();
        append_test_recovery_metadata(
            &mut writer,
            "binance_aggtrade",
            "2026-07-13T00",
            0,
            25_002,
            1_783_900_800_001,
            1_783_900_825_002,
            "aggTrade",
            "binance_aggtrade_v5",
        );
        writer.finalize().unwrap();

        let mut recovered = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-13",
            &test_daily_manifest_identity(),
        )
        .unwrap()
        .websocket_dedupe;
        assert_eq!(recovered.len(), BINANCE_AGGTRADE_RECOVERY_IDS);
        let canonical = |id| {
            BinanceWsAggTrade::from_wire(
                &test_ws_aggtrade_value(id),
                "btcusdt@aggTrade",
                1_783_900_800_000,
            )
            .unwrap()
            .source_native_payload_json
        };
        assert_eq!(
            recovered.observe(2, canonical(2).as_bytes()),
            BinanceIdDecision::OutsideWindow
        );
        assert_eq!(
            recovered.observe(3, canonical(3).as_bytes()),
            BinanceIdDecision::Duplicate
        );
        assert_eq!(
            recovered.observe(25_002, canonical(25_002).as_bytes()),
            BinanceIdDecision::Duplicate
        );
    }

    #[test]
    fn rtds_dedupe_restores_first_receive_anchored_payload() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let recv_time_utc_ms = now_ms();
        let message =
            serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "timestamp": 1_783_392_404_599_i64,
                "payload": {
                    "symbol": "btc/usd",
                    "timestamp": 1_783_392_404_500_i64,
                    "value": "65000.10"
                }
            }))
            .unwrap();
        let canonical = rtds_source_native_payload_json(&message).unwrap();
        let event = RawEvent {
            source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
            source_event_type: "update".to_owned(),
            source_identifier: "btc/usd".to_owned(),
            source_ts: Some("1783392404500".to_owned()),
            recv_time_utc_ms,
            collector_host_id: "collector-a".to_owned(),
            collector_region: "test".to_owned(),
            source_endpoint: RTDS_WS_ENDPOINT.to_owned(),
            raw_json: json!({
                "topic": message.topic,
                "type": message.msg_type,
                "message_timestamp": message.timestamp,
                "payload_timestamp": 1_783_392_404_500_i64,
                "symbol": "btc/usd",
                "value": "65000.10",
                "age_ms": recv_time_utc_ms - 1_783_392_404_500_i64,
                "freshness_status": "stale",
                "subscription_raw_json": rtds_subscription_evidence_json(),
                "payload": message.payload,
                "source_native_payload_json": canonical,
            })
            .to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
        };
        let row = rtds_row_from_event(&event).unwrap();
        let canonical = row.source_native_payload_json.clone();
        let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
        let source_dir = dataset_root
            .join("training")
            .join("typed_raw")
            .join(format!("utc_day={day}"))
            .join("source_kind=polymarket_rtds_chainlink_btc_usd");
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &source_dir,
            "process-a",
            "polymarket_rtds_chainlink_btc_usd_v4",
            "polymarket_rtds_chainlink_btc_usd",
        )
        .unwrap();
        writer.write_rtds_chainlink_btc_usd_rows(&[row]).unwrap();
        writer.finalize().unwrap();

        let mut process_b =
            restore_rtds_dedupe(&dataset_root, recv_time_utc_ms.saturating_add(1)).unwrap();
        assert_eq!(
            process_b
                .observe(
                    canonical.as_bytes(),
                    recv_time_utc_ms.saturating_add(59_999)
                )
                .unwrap(),
            FingerprintDecision::Duplicate
        );
        assert_eq!(
            process_b
                .observe(
                    canonical.as_bytes(),
                    recv_time_utc_ms.saturating_add(60 * 60 * 1000)
                )
                .unwrap(),
            FingerprintDecision::Unique,
            "the restored first receive time, not payload time, controls expiry"
        );
    }

    #[test]
    fn finalized_parquet_recovery_orders_numeric_parts() {
        let mut paths = [
            "source_00-part-10.parquet",
            "source_00-part-2.parquet",
            "source_00.parquet",
            "source_00-part-1.parquet",
        ]
        .map(PathBuf::from);
        paths.sort_by_key(|path| finalized_parquet_order_key(path));
        assert_eq!(
            paths.map(|path| path.file_name().unwrap().to_owned()),
            [
                "source_00.parquet",
                "source_00-part-1.parquet",
                "source_00-part-2.parquet",
                "source_00-part-10.parquet",
            ]
            .map(std::ffi::OsString::from)
        );
    }

    #[test]
    fn polymarket_target_raw_events_keep_event_specific_fields() {
        let asset_id =
            "106585164761922456203746651621390029417453862034640469075081961934906147433548";
        let mut asset_to_slug = BTreeMap::new();
        asset_to_slug.insert(asset_id.to_owned(), "btc-updown-5m-1783392300".to_owned());

        let WsMessage::PriceChange(price_change) = serde_json::from_str(&format!(
            r#"{{
                "event_type": "price_change",
                "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                "timestamp": "1783392404600",
                "price_changes": [{{
                    "asset_id": "{asset_id}",
                    "price": "0.52",
                    "size": "10",
                    "side": "BUY",
                    "hash": "price-hash",
                    "best_bid": "0.51",
                    "best_ask": "0.53"
                }}]
            }}"#
        ))
        .unwrap() else {
            panic!("expected price_change message");
        };
        let raw = polymarket_price_change_raw_json(&price_change, &asset_to_slug);
        assert_eq!(raw["event_type"], "price_change");
        assert_eq!(raw["price_changes"][0]["price"], "0.52");
        assert_eq!(raw["price_changes"][0]["best_bid"], "0.51");
        assert_eq!(raw["price_changes"][0]["slug"], "btc-updown-5m-1783392300");

        let WsMessage::LastTradePrice(last_trade) = serde_json::from_str(&format!(
            r#"{{
                "asset_id": "{asset_id}",
                "event_type": "last_trade_price",
                "fee_rate_bps": "0",
                "market": "0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347",
                "price": "0.456",
                "side": "BUY",
                "size": "219.217767",
                "timestamp": "1750428146322"
            }}"#
        ))
        .unwrap() else {
            panic!("expected last_trade_price message");
        };
        let raw = polymarket_last_trade_raw_json(&last_trade, "btc-updown-5m-1783392300");
        assert_eq!(raw["event_type"], "last_trade_price");
        assert_eq!(raw["price"], "0.456");
        assert_eq!(raw["size"], "219.217767");
        assert_eq!(raw["fee_rate_bps"], "0");

        let WsMessage::TickSizeChange(tick_size) = serde_json::from_str(&format!(
            r#"{{
                "event_type": "tick_size_change",
                "asset_id": "{asset_id}",
                "market": "0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347",
                "old_tick_size": "0.01",
                "new_tick_size": "0.001",
                "timestamp": "1750428146323"
            }}"#
        ))
        .unwrap() else {
            panic!("expected tick_size_change message");
        };
        let raw = polymarket_tick_size_change_raw_json(&tick_size, "btc-updown-5m-1783392300");
        assert_eq!(raw["event_type"], "tick_size_change");
        assert_eq!(raw["old_tick_size"], "0.01");
        assert_eq!(raw["new_tick_size"], "0.001");

        let WsMessage::NewMarket(new_market) = serde_json::from_str(
            r#"{
                "id": "1031769",
                "question": "Will NVIDIA (NVDA) close above $240 end of January?",
                "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
                "slug": "nvda-above-240-on-january-30-2026",
                "description": "This market will resolve to Yes or No.",
                "assets_ids": [
                    "76043073756653678226373981964075571318267289248134717369284518995922789326425",
                    "31690934263385727664202099278545688007799199447969475608906331829650099442770"
                ],
                "outcomes": ["Yes", "No"],
                "event_message": {
                    "id": "125819",
                    "ticker": "nvda-above-in-january-2026",
                    "slug": "nvda-above-in-january-2026",
                    "title": "Will NVIDIA (NVDA) close above ___ end of January?",
                    "description": "Market description"
                },
                "timestamp": "1766790415550",
                "event_type": "new_market"
            }"#,
        )
        .unwrap() else {
            panic!("expected new_market message");
        };
        let raw = polymarket_new_market_raw_json(&new_market);
        assert_eq!(raw["event_type"], "new_market");
        assert_eq!(raw["slug"], "nvda-above-240-on-january-30-2026");
        assert_eq!(raw["asset_ids"].as_array().unwrap().len(), 2);
        assert_eq!(raw["event_message"]["ticker"], "nvda-above-in-january-2026");

        let WsMessage::MarketResolved(resolved) = serde_json::from_str(
            r#"{
                "id": "1031769",
                "question": "Will NVIDIA (NVDA) close above $240 end of January?",
                "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
                "slug": "nvda-above-240-on-january-30-2026",
                "description": "This market will resolve to Yes or No.",
                "assets_ids": [
                    "76043073756653678226373981964075571318267289248134717369284518995922789326425",
                    "31690934263385727664202099278545688007799199447969475608906331829650099442770"
                ],
                "outcomes": ["Yes", "No"],
                "winning_asset_id": "76043073756653678226373981964075571318267289248134717369284518995922789326425",
                "winning_outcome": "Yes",
                "timestamp": "1766790415550",
                "event_type": "market_resolved"
            }"#,
        )
        .unwrap()
        else {
            panic!("expected market_resolved message");
        };
        let raw = polymarket_market_resolved_raw_json(&resolved);
        assert_eq!(raw["event_type"], "market_resolved");
        assert_eq!(raw["winning_outcome"], "Yes");
        assert_eq!(raw["resolved_event_asset_ids"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn polymarket_metadata_runtime_conversion_marks_unavailable_rows() {
        let active = test_metadata_raw_event(
            "active_scan",
            json!({
                "event_type": "active_scan",
                "slug": "btc-updown-5m-1783392300",
                "condition_id": "0x0000000000000000000000000000000000000000000000000000000000000002",
                "clob_market_id": "123",
                "asset_ids": ["1", "2"],
                "metadata_fetch_start_utc": 1_783_392_299_900_i64,
                "market_start_utc": 1_783_392_300_000_i64,
                "market_end_utc": 1_783_392_600_000_i64,
                "active": true,
                "accepting_orders": true,
                "market_lifecycle_state": "active",
                "tick_size": "0.01",
                "min_size": "1",
                "fee_schedule_raw": r#"{"fee":"0"}"#,
                "reward_eligibility_raw": r#"{"rewards_min_size":"1"}"#,
                "same_condition_market_verified": true
            }),
        );
        let rows = polymarket_metadata_rows_from_event(&active).unwrap();
        assert_eq!(rows[0].metadata_source, "active_scan");
        assert_eq!(rows[0].metadata_fetch_start_utc, Some(1_783_392_299_900));
        assert_eq!(rows[0].metadata_unavailable_reason, None);
        assert!(rows[0].validate().is_complete());

        let new_market = test_metadata_raw_event(
            "new_market",
            json!({
                "event_type": "new_market",
                "slug": "btc-updown-5m-1783392600",
                "condition_id": "0x0000000000000000000000000000000000000000000000000000000000000003",
                "asset_ids": ["3", "4"],
                "metadata_unavailable_reason": "market_ws_new_market_payload_lacks_market_start_end_active_accepting_orders_tick_size_min_size_fee_schedule_reward_eligibility",
                "same_condition_market_verified": true
            }),
        );
        let rows = polymarket_metadata_rows_from_event(&new_market).unwrap();
        assert_eq!(rows[0].metadata_source, "new_market");
        assert_eq!(rows[0].metadata_fetch_start_utc, None);
        assert!(
            rows[0]
                .metadata_unavailable_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("market_ws_new_market_payload_lacks"))
        );

        let fallback = test_metadata_raw_event(
            "fallback_close",
            json!({
                "event_type": "fallback_close",
                "slug": "btc-updown-5m-1783392300",
                "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_ids": ["1", "2"],
                "metadata_unavailable_reason": "market_resolved_ws_payload_lacks_market_start_end_tick_size_min_size_fee_schedule_reward_eligibility",
                "same_condition_market_verified": true,
                "fallback_close_reason": "market_resolved"
            }),
        );
        let rows = polymarket_metadata_rows_from_event(&fallback).unwrap();
        assert_eq!(rows[0].metadata_source, "fallback_close");
        assert_eq!(rows[0].metadata_fetch_start_utc, None);
        assert_eq!(rows[0].market_lifecycle_state, "retired");
        assert!(
            rows[0]
                .metadata_unavailable_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("market_resolved_ws_payload_lacks"))
        );
    }

    #[test]
    fn window_end_without_resolved_is_observation_not_retired_close() {
        let market = crate::market_discovery::DiscoveredBtcUpdownMarket {
            id: "123".to_owned(),
            slug: "btc-updown-5m-1783392300".to_owned(),
            condition_id: "0x0000000000000000000000000000000000000000000000000000000000000002"
                .to_owned(),
            up_asset_id: "1".to_owned(),
            down_asset_id: "2".to_owned(),
            start_ts_s: 1_783_392_300,
            end_ts_s: 1_783_392_600,
            candidate_start_ts_s: 1_783_392_300,
            candidate_end_ts_s: 1_783_392_600,
            active: true,
            accepting_orders: true,
            tick_size: Some("0.01".to_owned()),
            min_size: Some("1".to_owned()),
            fee_schedule_raw: Some(r#"{"fee":"0"}"#.to_owned()),
            reward_eligibility_raw: Some(r#"{"rewards_min_size":"1"}"#.to_owned()),
            raw_json_sha256: Some("raw-sha256".to_owned()),
            raw_json: Some(r#"{"id":"123"}"#.to_owned()),
            metadata_fetch_start_utc_ms: Some(1_783_392_299_900),
            metadata_received_utc_ms: Some(1_783_392_300_000),
        };
        let raw = polymarket_window_end_observation_raw_json_from_discovery(&market);

        assert_eq!(raw["event_type"], "active_scan");
        assert_eq!(raw["metadata_source"], "active_scan");
        assert_eq!(
            raw["market_lifecycle_state"],
            "unresolved_window_end_observation"
        );
        assert_ne!(raw["market_lifecycle_state"], "retired");
        assert!(
            raw["metadata_unavailable_reason"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "market_resolved_not_observed_before_window_end")
        );

        let event = test_metadata_raw_event("active_scan", raw);
        let rows = polymarket_metadata_rows_from_event(&event).unwrap();
        assert_eq!(rows[0].metadata_source, "active_scan");
        assert_eq!(
            rows[0].market_lifecycle_state,
            "unresolved_window_end_observation"
        );
        assert!(
            rows[0]
                .metadata_unavailable_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("market_resolved_not_observed"))
        );
    }

    #[test]
    fn metadata_manifest_distinguishes_complete_and_unavailable_rows() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("polymarket_market_metadata", 1_783_392_300_000, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming, "polymarket_market_metadata", 0).unwrap();

        let unavailable = test_metadata_raw_event(
            "active_scan",
            json!({
                "event_type": "active_scan",
                "slug": "btc-updown-5m-1783392300",
                "condition_id": "condition-1",
                "asset_ids": ["1", "2"],
                "metadata_unavailable_reason": "market_resolved_not_observed_before_window_end",
                "same_condition_market_verified": true
            }),
        );
        segment.observe(&unavailable, 1);
        let context = SegmentManifestContext::from_segment(&segment);
        assert_eq!(context.metadata_unavailable_rows, 1);
        assert_eq!(context.metadata_complete_rows, 0);

        let complete = test_metadata_raw_event(
            "active_scan",
            json!({
                "event_type": "active_scan",
                "slug": "btc-updown-5m-1783392300",
                "condition_id": "condition-1",
                "asset_ids": ["1", "2"],
                "market_start_utc": 1_783_392_300_000_i64,
                "market_end_utc": 1_783_392_600_000_i64,
                "active": true,
                "accepting_orders": true,
                "market_lifecycle_state": "active",
                "tick_size": "0.01",
                "min_size": "1",
                "fee_schedule_raw": r#"{"fee":"0"}"#,
                "reward_eligibility_raw": r#"{"rewards_min_size":"1"}"#,
                "same_condition_market_verified": true
            }),
        );
        segment.observe(&complete, 1);
        let context = SegmentManifestContext::from_segment(&segment);
        assert_eq!(context.metadata_unavailable_rows, 1);
        assert_eq!(context.metadata_complete_rows, 1);
    }

    #[tokio::test]
    async fn missing_writer_is_a_runtime_error() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let mut writers = BTreeMap::new();
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let runtime_stats = SourceQueueBackpressureStats::default();
        let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
        let mut batch = vec![InflightRawEvent::for_test(test_raw_event(
            "binance_aggtrade",
            1_783_392_404_599,
        ))];

        flush_source_batch(
            &mut writers,
            "binance_aggtrade",
            &mut batch,
            &gaps,
            &runtime_stats,
            &mut tracker,
            Instant::now(),
        )
        .await
        .expect_err("a missing writer must stop the runtime");

        let summary = tracker.finish(0);
        assert_eq!(summary.dropped_rows, 1);
        assert!(
            gaps.lock()
                .unwrap()
                .iter()
                .any(|gap| gap == "writer_missing_for_source:binance_aggtrade")
        );
        let mut owner = ManifestOwnerState::new(
            dataset_root.clone(),
            test_daily_manifest_identity(),
            BinanceAggTradeContinuityState::default(),
        );
        owner.ensure_open("2026-07-07").unwrap();
        let counters = runtime_stats.drain_all_operational();
        owner
            .record_operational_counters("2026-07-07", &counters["2026-07-07"])
            .unwrap();
        let manifest = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-07"))
            .unwrap()
            .unwrap();
        assert_eq!(
            manifest["source_counters"]["binance_aggtrade"]["dropped_rows"],
            1
        );
    }

    #[tokio::test]
    async fn disconnected_writer_channel_is_a_runtime_error() {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        drop(rx);
        let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let mut writers = BTreeMap::from([(
            "binance_aggtrade".to_owned(),
            SourceWriterHandle {
                tx,
                queue_rows: Arc::new(AtomicU64::new(0)),
                accepting_commands: Arc::new(Mutex::new(true)),
                result_rx,
                completed_result: None,
            },
        )]);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let runtime_stats = SourceQueueBackpressureStats::default();
        let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
        let mut batch = vec![InflightRawEvent::for_test(test_raw_event(
            "binance_aggtrade",
            1_783_392_404_599,
        ))];

        let error = flush_source_batch(
            &mut writers,
            "binance_aggtrade",
            &mut batch,
            &gaps,
            &runtime_stats,
            &mut tracker,
            Instant::now(),
        )
        .await
        .expect_err("a disconnected writer must stop the runtime");

        assert!(error.to_string().contains("writer_queue_disconnected"));
        assert_eq!(tracker.finish(0).dropped_rows, 1);
        assert_eq!(
            runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
            1
        );
    }

    #[tokio::test]
    async fn writer_parser_failure_is_reported_when_workers_stop() {
        let tmp = tempdir().unwrap();
        let provenance = test_runtime_provenance("writer-parser-failure");
        let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
        let mut writers =
            start_writer_workers(tmp.path(), &provenance, &manifest_tx, &runtime_stats).unwrap();
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
        let mut event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        event.raw_json = json!({"e": "serverShutdown", "E": 1_783_392_404_599_i64}).to_string();
        let mut batch = vec![InflightRawEvent::for_test(event)];

        flush_source_batch(
            &mut writers,
            "binance_aggtrade",
            &mut batch,
            &gaps,
            &runtime_stats,
            &mut tracker,
            Instant::now(),
        )
        .await
        .unwrap();
        let mut runtime_failure = None;
        let (outputs, all_terminal) = writer_outputs_or_record_failure(
            stop_writer_workers_with_budget(
                writers,
                Duration::from_millis(WRITER_FINALIZE_BUDGET_MS),
            )
            .await,
            &gaps,
            &mut runtime_failure,
        );

        assert!(all_terminal);
        assert_eq!(outputs.len(), LIVE_SOURCES.len() - 1);
        assert!(
            outputs
                .iter()
                .all(|output| output.source_kind != "binance_aggtrade")
        );
        assert!(
            runtime_failure
                .is_some_and(|error| { error.contains("binance_aggtrade writer finalize failed") })
        );
        assert_eq!(
            runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
            1
        );
    }

    #[test]
    fn writer_failure_counts_active_current_remaining_and_queued_rows_exactly() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(5));
        let accepting_commands = Arc::new(Mutex::new(true));
        let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
        let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);

        tx.send(WriterCommand::Events(vec![InflightRawEvent::for_test(
            test_raw_event("binance_aggtrade", 1_783_392_404_599),
        )]))
        .unwrap();
        let mut invalid = test_raw_event("binance_aggtrade", 1_783_392_404_600);
        invalid.raw_json = json!({"e": "serverShutdown"}).to_string();
        tx.send(WriterCommand::Events(vec![
            InflightRawEvent::for_test(invalid),
            InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_601)),
        ]))
        .unwrap();
        tx.send(WriterCommand::Events(vec![
            InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_602)),
            InflightRawEvent::for_test(test_raw_event("binance_aggtrade", 1_783_392_404_603)),
        ]))
        .unwrap();

        let result = source_writer_worker(
            dataset_root,
            test_runtime_provenance("writer-exact-drop"),
            "binance_aggtrade".to_owned(),
            rx,
            Arc::clone(&queue_rows),
            Arc::clone(&accepting_commands),
            Arc::clone(&runtime_stats),
            manifest_tx,
        );

        assert!(!result.unwrap_err().to_string().is_empty());
        assert_eq!(queue_rows.load(Ordering::Relaxed), 0);
        assert!(!*accepting_commands.lock().unwrap());
        assert_eq!(
            runtime_stats.drain_all_operational()["2026-07-07"]["binance_aggtrade"].dropped_rows,
            5
        );
    }

    #[tokio::test]
    async fn binance_server_shutdown_reconnects_before_forwarding_business_data() {
        assert_binance_reconnects_after(Some(Message::Text(
            json!({"e": "serverShutdown", "E": 1_783_392_404_500_i64})
                .to_string()
                .into(),
        )))
        .await;
    }

    #[tokio::test]
    async fn binance_combined_server_shutdown_reconnects_before_business_data() {
        assert_binance_reconnects_after(Some(Message::Text(
            json!({
                "stream": "!serverShutdown",
                "data": {"e": "serverShutdown", "E": 1_783_392_404_500_i64}
            })
            .to_string()
            .into(),
        )))
        .await;
    }

    #[tokio::test]
    async fn binance_server_close_reconnects_for_24_hour_connection_rotation() {
        assert_binance_reconnects_after(Some(Message::Close(None))).await;
    }

    #[tokio::test]
    async fn binance_eof_reconnects_before_forwarding_business_data() {
        assert_binance_reconnects_after(None).await;
    }

    async fn assert_binance_reconnects_after(first_message: Option<Message>) {
        let dataset_root = tempdir().unwrap();
        let dataset_root_path = dataset_root.path().canonicalize().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = accept_async(first).await.unwrap();
            match first_message {
                Some(first_message) => first.send(first_message).await.unwrap(),
                None => drop(first),
            }

            let (second, _) = listener.accept().await.unwrap();
            let mut second = accept_async(second).await.unwrap();
            second.send(test_aggtrade_message()).await.unwrap();
        });

        let (tx, mut rx) = bounded_event_queue::<InflightRawEvent>(8).unwrap();
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source = tokio::spawn(run_binance_ws_source(
            restore_binance_aggtrade_recovery(
                &dataset_root_path,
                &utc_day_string_from_ms(now_ms()).unwrap(),
                &test_daily_manifest_identity(),
            )
            .unwrap(),
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url,
                rest_endpoint: "http://127.0.0.1:9/api/v3/aggTrades".to_owned(),
                freshness_timeout_secs: 10,
            },
            sink,
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_secs(3)),
            Instant::now(),
            control_rx,
        ));

        let event = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("aggTrade should arrive after reconnect")
            .expect("source queue should stay open")
            .event;

        assert_eq!(event.source_event_type, "aggTrade");
        assert_eq!(event.source_ts.as_deref(), Some("1783392404599"));
        assert_eq!(
            serde_json::from_str::<Value>(&event.raw_json).unwrap()["event_type"],
            "aggTrade"
        );
        source.abort();
        let _ = source.await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_ping_does_not_reset_business_message_freshness() {
        let dataset_root = tempdir().unwrap();
        let dataset_root_path = dataset_root.path().canonicalize().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = accept_async(first).await.unwrap();
            let (second, _) = loop {
                tokio::select! {
                    accepted = listener.accept() => break accepted.unwrap(),
                    () = sleep(Duration::from_millis(50)) => {
                        if first
                            .send(Message::Ping(Vec::<u8>::new().into()))
                            .await
                            .is_err()
                        {
                            break listener.accept().await.unwrap();
                        }
                    }
                }
            };

            let mut second = accept_async(second).await.unwrap();
            second.send(test_aggtrade_message()).await.unwrap();
        });

        let (tx, mut rx) = bounded_event_queue::<InflightRawEvent>(8).unwrap();
        let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source = tokio::spawn(run_binance_ws_source(
            restore_binance_aggtrade_recovery(
                &dataset_root_path,
                &utc_day_string_from_ms(now_ms()).unwrap(),
                &test_daily_manifest_identity(),
            )
            .unwrap(),
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url,
                rest_endpoint: "http://127.0.0.1:9/api/v3/aggTrades".to_owned(),
                freshness_timeout_secs: 1,
            },
            SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_secs(4)),
            Instant::now(),
            control_rx,
        ));

        let event = timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("freshness timeout should reconnect despite Ping frames")
            .expect("source queue should stay open")
            .event;
        assert_eq!(event.source_event_type, "aggTrade");

        source.abort();
        let _ = source.await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_aggtrade_schema_errors_are_process_fatal() {
        for (frame, expected) in [
            ("{not-json".to_owned(), "ws_invalid_json"),
            (
                json!({"s": "BTCUSDT", "a": 1_u64}).to_string(),
                "ws_missing_event_type",
            ),
            (
                json!({"e": "depthUpdate", "s": "BTCUSDT"}).to_string(),
                "ws_unexpected_event_type:depthUpdate",
            ),
        ] {
            assert_binance_aggtrade_schema_error_is_fatal(frame, expected).await;
        }
    }

    async fn assert_binance_aggtrade_schema_error_is_fatal(frame: String, expected: &str) {
        let dataset_root = tempdir().unwrap();
        let dataset_root_path = dataset_root.path().canonicalize().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket.send(Message::Text(frame.into())).await.unwrap();
            sleep(Duration::from_millis(100)).await;
        });
        let (tx, _rx) = bounded_event_queue::<InflightRawEvent>(8).unwrap();
        let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source = tokio::spawn(run_binance_ws_source(
            restore_binance_aggtrade_recovery(
                &dataset_root_path,
                &utc_day_string_from_ms(now_ms()).unwrap(),
                &test_daily_manifest_identity(),
            )
            .unwrap(),
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url,
                rest_endpoint: "http://127.0.0.1:9/api/v3/aggTrades".to_owned(),
                freshness_timeout_secs: 5,
            },
            SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_secs(2)),
            Instant::now(),
            control_rx,
        ));

        let error = timeout(Duration::from_secs(1), source)
            .await
            .expect("schema error must terminate the source promptly")
            .unwrap()
            .expect_err("schema error must propagate as a process-level source failure");
        assert!(error.to_string().contains(expected), "{error}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binance_aggtrade_stop_flushes_pending_before_source_exit() {
        let dataset_root = tempdir().unwrap();
        let dataset_root_path = dataset_root.path().canonicalize().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(test_aggtrade_message_for_id(100))
                .await
                .unwrap();
            websocket
                .send(test_aggtrade_message_for_id(103))
                .await
                .unwrap();
            sleep(Duration::from_secs(5)).await;
        });
        let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_endpoint = format!(
            "http://{}/api/v3/aggTrades",
            rest_listener.local_addr().unwrap()
        );
        let (rest_started_tx, rest_started_rx) = oneshot::channel();
        let rest_server = tokio::spawn(async move {
            let (mut socket, _) = rest_listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let _ = rest_started_tx.send(());
            sleep(Duration::from_secs(5)).await;
        });
        let (tx, mut rx) = bounded_event_queue::<InflightRawEvent>(32).unwrap();
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let inflight_budget = InflightBudget::new();
        let (control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source = tokio::spawn(run_binance_ws_source(
            restore_binance_aggtrade_recovery(
                &dataset_root_path,
                &utc_day_string_from_ms(now_ms()).unwrap(),
                &test_daily_manifest_identity(),
            )
            .unwrap(),
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: ws_url,
                rest_endpoint,
                freshness_timeout_secs: 5,
            },
            SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: inflight_budget.clone(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_secs(10)),
            Instant::now(),
            control_rx,
        ));

        rest_started_rx.await.unwrap();
        let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
        let (reply_tx, reply_rx) = oneshot::channel();
        control_tx
            .send(AggTradeSourceControl::Flush {
                reason: "operator_shutdown".to_owned(),
                utc_day,
                stop_after_flush: true,
                resume_after_barrier: None,
                reply: reply_tx,
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("coordinated flush reply must be bounded")
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), source)
            .await
            .expect("source must exit after stop flush")
            .unwrap()
            .unwrap();

        let mut data_ids = Vec::new();
        let mut last_data_index = None;
        let mut continuity_indexes = Vec::new();
        let mut index = 0;
        while let Ok(message) = rx.try_recv() {
            match message.control {
                Some(IngressControl::BinanceContinuity { .. }) => {
                    continuity_indexes.push(index);
                }
                Some(IngressControl::DayBarrier { .. }) => {}
                None => {
                    data_ids.push(
                        serde_json::from_str::<Value>(&message.event.raw_json).unwrap()
                            ["aggregate_trade_id"]
                            .as_u64()
                            .unwrap(),
                    );
                    last_data_index = Some(index);
                }
            }
            index += 1;
        }
        assert_eq!(data_ids, vec![100, 103]);
        assert!(
            continuity_indexes
                .iter()
                .any(|continuity| Some(*continuity) > last_data_index),
            "final continuity must follow the flushed pending event"
        );
        let recorded_gaps = gaps.lock().unwrap().all_reasons();
        assert!(
            recorded_gaps
                .iter()
                .any(|gap| { gap == "binance_aggtrade_backfill_failed:operator_shutdown" })
        );
        assert!(
            recorded_gaps
                .iter()
                .any(|gap| gap == "binance_aggtrade_unresolved_range:101-102")
        );
        assert_eq!(
            inflight_budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
        );
        rest_server.abort();
        let _ = rest_server.await;
        ws_server.abort();
        let _ = ws_server.await;
    }

    #[tokio::test]
    async fn binance_gap_backfills_while_later_websocket_rows_stay_pending() {
        let dataset_root = tempdir().unwrap();
        let dataset_root_path = dataset_root.path().canonicalize().unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            socket
                .send(test_aggtrade_message_for_id(100))
                .await
                .unwrap();
            socket
                .send(test_aggtrade_message_for_id(103))
                .await
                .unwrap();
            sleep(Duration::from_millis(25)).await;
            socket
                .send(test_aggtrade_message_for_id(104))
                .await
                .unwrap();
            sleep(Duration::from_secs(2)).await;
        });

        let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_endpoint = format!(
            "http://{}/api/v3/aggTrades",
            rest_listener.local_addr().unwrap()
        );
        let rest_server = tokio::spawn(async move {
            let (mut socket, _) = rest_listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4_096];
            let read = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8(request[..read].to_vec()).unwrap();
            let request_line = request.lines().next().unwrap();
            assert!(request_line.contains("symbol=BTCUSDT"));
            assert!(request_line.contains("fromId=101"));
            assert!(request_line.contains("limit=3"));
            sleep(Duration::from_millis(100)).await;
            let body = serde_json::to_string(
                &(101_u64..=103)
                    .map(test_rest_aggtrade_value)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let (tx, mut rx) = bounded_event_queue::<InflightRawEvent>(16).unwrap();
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let inflight_budget = InflightBudget::new();
        let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
        let source = tokio::spawn(run_binance_ws_source(
            restore_binance_aggtrade_recovery(
                &dataset_root_path,
                &utc_day_string_from_ms(now_ms()).unwrap(),
                &test_daily_manifest_identity(),
            )
            .unwrap(),
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: ws_url,
                rest_endpoint,
                freshness_timeout_secs: 5,
            },
            SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: inflight_budget.clone(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(Instant::now() + Duration::from_secs(3)),
            Instant::now(),
            control_rx,
        ));

        let mut rows = Vec::new();
        while rows.len() < 5 {
            let message = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("expected gap/backfill output")
                .expect("source queue remains open");
            if message.control.is_none() {
                rows.push(serde_json::from_str::<Value>(&message.event.raw_json).unwrap());
            }
        }
        assert_eq!(
            rows.iter()
                .map(|row| row["aggregate_trade_id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![100, 101, 102, 103, 104]
        );
        assert_eq!(rows[1]["ingest_source"], "rest_backfill");
        assert_eq!(rows[2]["ingest_source"], "rest_backfill");
        assert_eq!(rows[3]["ingest_source"], "websocket_rest_merged");
        assert!(rows[3]["websocket_timestamp_received"].as_i64().unwrap() > 0);
        assert!(
            rows[3]["rest_backfill_timestamp_received"]
                .as_i64()
                .unwrap()
                >= rows[3]["websocket_timestamp_received"].as_i64().unwrap()
        );
        assert_eq!(rows[4]["ingest_source"], "websocket");
        assert!(gaps.lock().unwrap().is_empty());
        assert_eq!(
            inflight_budget.available(),
            (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES),
            "pending WebSocket, REST backfill, ingress and output reservations must all release"
        );

        source.abort();
        let _ = source.await;
        rest_server.await.unwrap();
        ws_server.abort();
        let _ = ws_server.await;
    }

    fn test_aggtrade_message() -> Message {
        Message::Text(
            test_ws_aggtrade_value_at(123_456, 1_783_392_404_599)
                .to_string()
                .into(),
        )
    }

    fn test_aggtrade_message_for_id(id: u64) -> Message {
        Message::Text(test_ws_aggtrade_value(id).to_string().into())
    }

    fn test_ws_aggtrade_value(id: u64) -> Value {
        test_ws_aggtrade_value_at(id, 1_783_392_400_000_i64 + i64::try_from(id).unwrap())
    }

    fn test_ws_aggtrade_value_at(id: u64, timestamp: i64) -> Value {
        json!({
            "e": "aggTrade",
            "E": timestamp,
            "s": "BTCUSDT",
            "a": id,
            "p": id.to_string(),
            "q": "0.001",
            "f": id * 10,
            "l": id * 10,
            "T": timestamp,
            "m": true,
            "M": false,
        })
    }

    fn test_aggtrade_hourly_row(id: u64, timestamp_received: i64) -> BinanceAggTradeHourlyRow {
        let websocket = BinanceWsAggTrade::from_wire(
            &test_ws_aggtrade_value(id),
            "btcusdt@aggTrade",
            timestamp_received,
        )
        .unwrap();
        BinanceAggTradeHourlyRow::from_output(
            &BinanceAggTradeOutput::Websocket(websocket),
            "collector-test",
            "local-test",
            BINANCE_AGGTRADE_WS,
            BINANCE_AGGTRADE_REST,
        )
        .unwrap()
    }

    fn test_rest_aggtrade_value(id: u64) -> Value {
        json!({
            "a": id,
            "p": id.to_string(),
            "q": "0.001",
            "f": id * 10,
            "l": id * 10,
            "T": 1_783_392_400_000_i64 + i64::try_from(id).unwrap(),
            "m": true,
            "M": true,
        })
    }

    fn test_raw_event(source_kind: &str, recv_time_utc_ms: i64) -> RawEvent {
        assert_eq!(source_kind, "binance_aggtrade");
        let websocket = BinanceWsAggTrade::from_wire(
            &test_ws_aggtrade_value_at(123_456, recv_time_utc_ms),
            "btcusdt@aggTrade",
            recv_time_utc_ms,
        )
        .unwrap();
        binance_aggtrade_raw_event(
            &BinanceAggTradeOutput::Websocket(websocket),
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            &BinanceWsSpec {
                source_kind: source_kind.to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS.to_owned(),
                rest_endpoint: BINANCE_AGGTRADE_REST.to_owned(),
                freshness_timeout_secs: 60,
            },
        )
        .unwrap()
    }

    fn test_metadata_raw_event(source_event_type: &str, raw_json: Value) -> RawEvent {
        RawEvent {
            source_kind: "polymarket_market_metadata".to_owned(),
            source_event_type: source_event_type.to_owned(),
            source_identifier: raw_json
                .get("slug")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            source_ts: Some("1783392300000".to_owned()),
            recv_time_utc_ms: 1_783_392_300_000,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
            raw_json: raw_json.to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_market_metadata".to_owned(),
        }
    }

    fn test_btc_discovery() -> BtcUpdownDiscovery {
        BtcUpdownDiscovery {
            current: crate::market_discovery::DiscoveredBtcUpdownMarket {
                id: "market-current".to_owned(),
                slug: "btc-updown-5m-1783392300".to_owned(),
                condition_id: "0x0000000000000000000000000000000000000000000000000000000000000002"
                    .to_owned(),
                up_asset_id: "101".to_owned(),
                down_asset_id: "102".to_owned(),
                start_ts_s: 1_783_392_300,
                end_ts_s: 1_783_392_600,
                candidate_start_ts_s: 1_783_392_300,
                candidate_end_ts_s: 1_783_392_600,
                active: true,
                accepting_orders: true,
                tick_size: Some("0.01".to_owned()),
                min_size: Some("1".to_owned()),
                fee_schedule_raw: Some(r#"{"fee":"0"}"#.to_owned()),
                reward_eligibility_raw: Some(r#"{"rewards_min_size":"1"}"#.to_owned()),
                raw_json_sha256: Some("current-sha256".to_owned()),
                raw_json: Some(r#"{"id":"market-current"}"#.to_owned()),
                metadata_fetch_start_utc_ms: None,
                metadata_received_utc_ms: None,
            },
            next: None,
            relation: None,
        }
    }

    fn test_polymarket_price_change_event(recv_time_utc_ms: i64) -> RawEvent {
        RawEvent {
            source_kind: "polymarket_market".to_owned(),
            source_event_type: "price_change".to_owned(),
            source_identifier: "btc-updown-5m-1783392300".to_owned(),
            source_ts: Some(recv_time_utc_ms.to_string()),
            recv_time_utc_ms,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            raw_json: json!({
                "event_type": "price_change",
                "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                "slug": "btc-updown-5m-1783392300",
                "timestamp": recv_time_utc_ms.to_string(),
                "price_changes": [
                    {
                        "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
                        "price": "0.52",
                        "size": "10",
                        "side": "BUY",
                        "hash": "price-hash-yes",
                        "best_bid": "0.51",
                        "best_ask": "0.53",
                        "slug": "btc-updown-5m-1783392300"
                    },
                    {
                        "asset_id": "71333938092403785983382165334934931210342159432628258568410798125372712594273",
                        "price": "0.48",
                        "size": "8",
                        "side": "SELL",
                        "hash": "price-hash-no",
                        "best_bid": "0.47",
                        "best_ask": "0.49",
                        "slug": "btc-updown-5m-1783392300"
                    }
                ]
            })
            .to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_market".to_owned(),
        }
    }

    fn test_binance_depth_update_event(recv_time_utc_ms: i64) -> RawEvent {
        test_binance_depth_update_event_inner(recv_time_utc_ms, None)
    }

    fn test_binance_depth_update_event_with_trigger(
        recv_time_utc_ms: i64,
        trigger_reason: &str,
    ) -> RawEvent {
        test_binance_depth_update_event_inner(recv_time_utc_ms, Some(trigger_reason))
    }

    fn test_binance_depth_update_event_inner(
        recv_time_utc_ms: i64,
        trigger_reason: Option<&str>,
    ) -> RawEvent {
        let mut raw_json = json!({
            "e": "depthUpdate",
            "E": recv_time_utc_ms,
            "s": "BTCUSDT",
            "U": 101_u64,
            "u": 103_u64,
            "b": [["65000.10", "0.25"]],
            "a": [["65000.20", "0.30"]],
            "snapshot_last_update_id": 100_u64,
            "local_u_before": Value::Null,
            "stitch_evidence": "U <= lastUpdateId + 1 <= u"
        });
        if let Some(trigger_reason) = trigger_reason {
            raw_json["snapshot_trigger_reason"] = json!(trigger_reason);
        }
        RawEvent {
            source_kind: "binance_depth_update".to_owned(),
            source_event_type: "depth_update".to_owned(),
            source_identifier: "BTCUSDT".to_owned(),
            source_ts: Some(recv_time_utc_ms.to_string()),
            recv_time_utc_ms,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: BINANCE_DEPTH_WS.to_owned(),
            raw_json: raw_json.to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "binance_depth_update".to_owned(),
        }
    }

    fn test_runtime_provenance(_run_id: &str) -> RuntimeProvenance {
        RuntimeProvenance {
            code_version: "test-code-version".to_owned(),
            collector_binary_sha256: "test-binary-sha256".to_owned(),
            source_config_digest: "test-source-config-digest".to_owned(),
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_test_recovery_metadata(
        writer: &mut TypedParquetSegmentWriter,
        source_kind: &str,
        utc_hour: &str,
        part: u32,
        rows: u64,
        first_recv_time_utc_ms: i64,
        last_recv_time_utc_ms: i64,
        event_type: &str,
        schema_version: &str,
    ) {
        let hour_start = first_recv_time_utc_ms.div_euclid(3_600_000) * 3_600_000;
        let metadata = DailyFileRecoveryMetadata {
            source_kind: source_kind.to_owned(),
            utc_hour: utc_hour.to_owned(),
            part,
            rows,
            schema_version: schema_version.to_owned(),
            first_recv_time_utc_ms,
            last_recv_time_utc_ms,
            source_timestamp_start: Some(first_recv_time_utc_ms.to_string()),
            source_timestamp_end: Some(last_recv_time_utc_ms.to_string()),
            event_type_counts: BTreeMap::from([(event_type.to_owned(), rows)]),
            facts: json!({
                "rollover_reason": "test_recovery",
                "hour_window_utc_ms": {
                    "start": hour_start,
                    "end": hour_start + 3_600_000,
                },
                "layout_family": "source_specific_hourly_typed_raw",
                "sort_order": "receive_order",
                "compression": "zstd_level_3",
                "backfill_rows": 0,
                "book_rows_with_bids_asks": 0,
                "covered_market_slugs": [],
                "covered_condition_ids": [],
                "covered_asset_ids": [],
                "binance_depth_stitch": {},
                "source_gaps": [],
                "metadata_complete_rows": 0,
                "metadata_unavailable_rows": 0,
                "code_version": "test-code-version",
                "collector_binary_sha256": "test-binary-sha256",
                "source_config_digest": "test-source-config-digest",
                "collector_host_id": "collector-test",
                "collector_region": "local-test",
            }),
        };
        writer
            .append_key_value_metadata(
                DAILY_FILE_RECOVERY_METADATA_KEY,
                serde_json::to_string(&metadata).unwrap(),
            )
            .unwrap();
    }

    fn write_test_aggtrade_recovery_file(
        day_dir: &Path,
        utc_hour: &str,
        part: u32,
        first_id: u64,
        rows: u64,
        first_recv_time_utc_ms: i64,
    ) -> PathBuf {
        let source_dir = day_dir.join("source_kind=binance_aggtrade");
        fs::create_dir_all(&source_dir).unwrap();
        let hour = &utc_hour[11..13];
        let stem = if part == 0 {
            format!("binance_aggtrade_{hour}")
        } else {
            format!("binance_aggtrade_{hour}-part-{part}")
        };
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            &source_dir,
            &stem,
            "binance_aggtrade_v5",
            "binance_aggtrade",
        )
        .unwrap();
        let events = (0..rows)
            .map(|offset| {
                test_aggtrade_hourly_row(
                    first_id + offset,
                    first_recv_time_utc_ms + i64::try_from(offset).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        writer.write_binance_aggtrade_rows(&events).unwrap();
        append_test_recovery_metadata(
            &mut writer,
            "binance_aggtrade",
            utc_hour,
            part,
            rows,
            first_recv_time_utc_ms,
            first_recv_time_utc_ms + i64::try_from(rows - 1).unwrap(),
            "aggTrade",
            "binance_aggtrade_v5",
        );
        let final_path = writer.final_path().to_path_buf();
        writer.finalize().unwrap();
        final_path
    }

    fn create_closed_test_aggtrade_day(dataset_root: &Path) -> (PathBuf, PathBuf) {
        let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
        let final_path = write_test_aggtrade_recovery_file(
            &day_dir,
            "2026-07-13T23",
            0,
            100,
            1,
            1_783_987_100_000,
        );
        let mut manifest = reconcile_open_day_manifest(
            &day_dir,
            "2026-07-13",
            &test_daily_manifest_identity(),
            &BinanceAggTradeContinuityState::default(),
        )
        .unwrap();
        close_day_manifest(&day_dir, &mut manifest, "test_closed_day").unwrap();
        (day_dir, final_path)
    }

    fn test_daily_manifest_identity() -> DailyManifestIdentity {
        DailyManifestIdentity {
            build_commit: "test-code-version".to_owned(),
            collector_binary_sha256: "test-binary-sha256".to_owned(),
            config_digest: "test-source-config-digest".to_owned(),
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
        }
    }
}
