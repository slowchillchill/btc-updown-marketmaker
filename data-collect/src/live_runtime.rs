use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::{
    BookUpdate, ChannelType, Client as MarketWsClient, LastTradePrice, MarketResolved, NewMarket,
    PriceChange, TickSizeChange,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::rtds::{Client as RtdsClient, Subscription};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::ValidationVerdict;
use crate::backpressure::{BackpressureReceipt, bounded_event_queue};
use crate::event::{EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp};
use crate::hourly_partition::{HourlyPartition, HourlyPartitionPolicy};
use crate::latency::{LatencyStats, QueueDurationSummary, QueueDurationTracker, TimedRawEvent};
use crate::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use crate::market_discovery::{
    BtcUpdownDiscoveryPair, BtcUpdownMarketCandidate, discovery_windows, select_btc_updown_pair,
};
use crate::polymarket_orderbook_layout::{PolymarketOrderbookContext, PolymarketOrderbookRow};
use crate::resource::{
    ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt, ResourceThresholds,
    ResourceWatermark,
};
use crate::retention::TrainingRetentionPolicy;
use crate::source_format::RtdsChainlinkBtcUsdRaw;
use crate::source_hourly_layout::{
    BinanceAggTradeHourlyRow, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RtdsChainlinkBtcUsdHourlyRow,
};
use crate::sources::binance::{
    apply_depth_stitch_evidence, parse_aggtrade_raw, parse_depth_snapshot_raw,
    parse_depth_update_raw, stitch_depth_update,
};
use crate::writer::{PublishedSegment, RawEvent, TypedParquetSegmentWriter};

const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";
const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
const BINANCE_DEPTH_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";
const BINANCE_AGGTRADE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const BINANCE_DEPTH_SNAPSHOT: &str =
    "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000";

const DEFAULT_CONTINUOUS_DURATION_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_DISCOVERY_WAIT_SECS: u64 = 90;
const DEFAULT_QUEUE_CAPACITY: usize = 25_000;
const DEFAULT_FLUSH_ROWS: usize = 5_000;
const DEFAULT_MAX_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_WRITER_CHANNEL_CAPACITY: usize = 8;
const DEFAULT_MAX_SEGMENT_ROWS: u64 = 300_000;
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_DURATION_MS: i64 = 15 * 60 * 1000;
const DEFAULT_MIN_LONG_RUN_WINDOWS: usize = 20;
const RETENTION_SCAN_INTERVAL_SECS: u64 = 60;
const RETENTION_SCAN_BUDGET_MS: u64 = 5_000;
const SHUTDOWN_RETENTION_SCAN_BUDGET_MS: u64 = RETENTION_SCAN_BUDGET_MS;
const WRITER_FINALIZE_BUDGET_MS: u64 = 30_000;
const MAX_RECORDED_SOURCE_GAPS: usize = 256;
const SOURCE_GAP_OVERFLOW_PREFIX: &str = "source_gap_overflow_count:";
const BINANCE_DEPTH_MAX_RECONNECTS: u32 = 12;
const SOURCE_SEND_TIMEOUT_MS: u64 = 2_000;
const BINANCE_HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const BINANCE_HTTP_TOTAL_TIMEOUT_SECS: u64 = 15;
const WS_CONNECT_TIMEOUT_SECS: u64 = 20;
const RTDS_RESUBSCRIBE_INTERVAL_SECS: u64 = 10;
const RTDS_RESUBSCRIBES_PER_RECONNECT: u64 = 2;
const RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW: usize = 6;
const RTDS_FRESHNESS_TIMEOUT_SECS: u64 = 60;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub const LIVE_SOURCES: &[&str] = &[
    "polymarket_market",
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
    elapsed: Duration,
    resubscribe_attempt: u64,
    reconnect_after_resubscribe: bool,
    source_gap_after_resubscribe: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RtdsTimeoutRecoveryStep {
    unsubscribe_current_client: bool,
    resubscribe_current_client: bool,
    reconnect_with_new_client: bool,
}

fn rtds_freshness_actions() -> [RtdsFreshnessAction; RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW]
{
    let mut actions = [RtdsFreshnessAction {
        wait: Duration::ZERO,
        elapsed: Duration::ZERO,
        resubscribe_attempt: 0,
        reconnect_after_resubscribe: false,
        source_gap_after_resubscribe: false,
    }; RTDS_RESUBSCRIBE_ATTEMPTS_PER_FRESHNESS_WINDOW];
    for (index, action) in actions.iter_mut().enumerate() {
        let attempt = u64::try_from(index + 1).expect("RTDS attempt index fits in u64");
        let elapsed_secs = attempt * RTDS_RESUBSCRIBE_INTERVAL_SECS;
        *action = RtdsFreshnessAction {
            wait: Duration::from_secs(RTDS_RESUBSCRIBE_INTERVAL_SECS),
            elapsed: Duration::from_secs(elapsed_secs),
            resubscribe_attempt: attempt,
            reconnect_after_resubscribe: attempt % RTDS_RESUBSCRIBES_PER_RECONNECT == 0,
            source_gap_after_resubscribe: elapsed_secs >= RTDS_FRESHNESS_TIMEOUT_SECS,
        };
    }
    actions
}

fn rtds_timeout_recovery_step(action: RtdsFreshnessAction) -> RtdsTimeoutRecoveryStep {
    RtdsTimeoutRecoveryStep {
        unsubscribe_current_client: true,
        resubscribe_current_client: !action.reconnect_after_resubscribe,
        reconnect_with_new_client: action.reconnect_after_resubscribe,
    }
}

#[derive(Default)]
struct SourceQueueBackpressureStats {
    wait_count: AtomicU64,
    timeout_count: AtomicU64,
    dropped_rows: AtomicU64,
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
    tx: mpsc::Sender<TimedRawEvent>,
    gaps: Arc<Mutex<Vec<String>>>,
    source_queue_backpressure: Arc<SourceQueueBackpressureStats>,
}

#[derive(Clone)]
struct RuntimeProvenance {
    run_id: String,
    code_version: String,
    collector_binary_sha256: String,
    source_config_digest: String,
    collector_host_id: String,
    collector_region: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveTrainingRuntimeConfig {
    pub dataset_root: PathBuf,
    pub run_id: String,
    pub duration_secs: Option<u64>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub code_version: String,
    pub collector_binary_sha256: String,
    pub source_config_digest: String,
    pub stop_after_complete_polymarket_market: Option<u32>,
    pub retention_policy_receipt_path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LiveTrainingRuntimeReceipt {
    pub run_id: String,
    pub run_manifest_path: PathBuf,
    pub finalized_segments: Vec<LiveFinalizedSegment>,
    pub source_coverage: Vec<SourceCoverage>,
    pub source_gaps: Vec<String>,
    pub resource_watermark_recorded: bool,
    pub writer_used_blocking_pool: bool,
    pub verdict: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LiveFinalizedSegment {
    pub source_kind: String,
    pub parquet_path: PathBuf,
    pub manifest_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
}

pub async fn run_live_training_runtime(
    config: LiveTrainingRuntimeConfig,
) -> Result<LiveTrainingRuntimeReceipt> {
    fs::create_dir_all(&config.dataset_root)?;
    let dataset_root = config.dataset_root.canonicalize()?;
    let artifact_dir = dataset_root
        .join("training")
        .join("runtime_artifacts")
        .join(&config.run_id);
    fs::create_dir_all(&artifact_dir)?;
    let duration_secs = config
        .duration_secs
        .unwrap_or(DEFAULT_CONTINUOUS_DURATION_SECS);
    let min_market_windows = if duration_secs >= 7_200 {
        DEFAULT_MIN_LONG_RUN_WINDOWS
    } else {
        1
    };
    let min_rollover_edges = usize::from(duration_secs >= 1_800);

    if duration_secs == 0 {
        return write_control_manifest(
            &dataset_root,
            &artifact_dir,
            &config,
            "SOURCE_GAP",
            LIVE_SOURCES
                .iter()
                .map(|source| format!("missing_source_coverage:{source}"))
                .collect(),
            Vec::new(),
            Vec::new(),
            empty_queue_summary("source_event_queue"),
            empty_queue_summary("writer_chunk_queue"),
            Vec::new(),
            LIVE_SOURCES
                .iter()
                .map(|source| SourceWriterStats::empty((*source).to_owned()))
                .collect(),
            default_shutdown_stage_timings("zero_duration_control_manifest"),
            SHUTDOWN_RETENTION_SCAN_BUDGET_MS,
        );
    }

    let run_started = Instant::now();
    let deadline = run_started + Duration::from_secs(duration_secs);
    let deadline_ms = now_ms().saturating_add((duration_secs * 1000) as i64);
    let source_gaps = Arc::new(Mutex::new(Vec::<String>::new()));
    let source_queue_backpressure = Arc::new(SourceQueueBackpressureStats::default());
    let rollover_edges = Arc::new(Mutex::new(Vec::<RolloverEdge>::new()));
    let collector_identity = CollectorIdentity {
        host_id: config.collector_host_id.clone(),
        region: config.collector_region.clone(),
    };
    let (event_tx, mut event_rx) = bounded_event_queue::<TimedRawEvent>(DEFAULT_QUEUE_CAPACITY)?;
    let event_sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&source_gaps),
        source_queue_backpressure: Arc::clone(&source_queue_backpressure),
    };
    let provenance = RuntimeProvenance {
        run_id: config.run_id.clone(),
        code_version: config.code_version.clone(),
        collector_binary_sha256: config.collector_binary_sha256.clone(),
        source_config_digest: config.source_config_digest.clone(),
        collector_host_id: config.collector_host_id.clone(),
        collector_region: config.collector_region.clone(),
    };
    let mut writer_handles = start_writer_workers(&dataset_root, &provenance)?;

    let handles = vec![
        tokio::spawn(run_polymarket_source(
            event_sink.clone(),
            Arc::clone(&rollover_edges),
            collector_identity.clone(),
            deadline_ms,
            DEFAULT_DISCOVERY_WAIT_SECS,
            run_started,
        )),
        tokio::spawn(run_rtds_source(
            event_sink.clone(),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
        tokio::spawn(run_binance_depth_source(
            event_sink.clone(),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
        tokio::spawn(run_binance_ws_source(
            BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS.to_owned(),
                freshness_timeout_secs: 60,
            },
            event_sink.clone(),
            collector_identity.clone(),
            deadline,
            run_started,
        )),
    ];
    drop(event_sink);

    let mut coverage = BTreeMap::<String, CoverageStats>::new();
    let mut batches = LIVE_SOURCES
        .iter()
        .map(|source| {
            (
                (*source).to_owned(),
                Vec::<RawEvent>::with_capacity(DEFAULT_FLUSH_ROWS),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source_queue_tracker = QueueDurationTracker::new("source_event_queue");
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut next_flush = run_started + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
    let mut next_queue_sample = run_started;
    let mut next_retention_scan = run_started + Duration::from_secs(RETENTION_SCAN_INTERVAL_SECS);
    let retention_policy_receipt_path = config
        .retention_policy_receipt_path
        .as_ref()
        .map(|path| dataset_root.join(path))
        .unwrap_or_else(|| {
            dataset_root
                .join("training")
                .join("runtime_artifacts")
                .join(&config.run_id)
                .join("retention_policy_receipt.json")
        });
    let mut average_event_bytes = 512_u64;
    let mut received_rows = 0_u64;
    let mut controlled_shutdown = false;
    let mut shutdown_stage_timings = BTreeMap::<String, ShutdownStageTiming>::new();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    while Instant::now() < deadline {
        tokio::select! {
            () = &mut shutdown => {
                controlled_shutdown = true;
                break;
            }
            received = timeout(Duration::from_millis(250), event_rx.recv()) => {
                match received {
                    Ok(Some(timed_event)) => {
                        received_rows = received_rows.saturating_add(1);
                        let event = timed_event.event;
                        update_coverage(&mut coverage, &event);
                        average_event_bytes = rolling_average_bytes(
                            average_event_bytes,
                            estimated_event_bytes(&event),
                            received_rows,
                        );
                        let source_kind = event.source_kind.clone();
                        let batch = batches.entry(source_kind.clone()).or_default();
                        batch.push(event);
                        if batch.len() >= DEFAULT_FLUSH_ROWS {
                            flush_source_batch(
                                &mut writer_handles,
                                &source_kind,
                                batch,
                                &source_gaps,
                                &mut writer_queue_tracker,
                                run_started,
                            )
                            .await?;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        }

        if Instant::now() >= next_flush {
            flush_all_batches(
                &mut writer_handles,
                &mut batches,
                &source_gaps,
                &mut writer_queue_tracker,
                run_started,
            )
            .await?;
            next_flush += Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
        }
        if Instant::now() >= next_queue_sample {
            source_queue_backpressure.apply_to_tracker(&mut source_queue_tracker);
            let writer_queue_rows = writer_handles
                .values()
                .map(|handle| handle.queue_rows.load(Ordering::Relaxed))
                .sum::<u64>();
            source_queue_tracker.observe(
                monotonic_ms(run_started),
                event_rx.len() as u64,
                (event_rx.len() as u64).saturating_mul(average_event_bytes),
            );
            writer_queue_tracker.observe(
                monotonic_ms(run_started),
                writer_queue_rows,
                writer_queue_rows.saturating_mul(average_event_bytes),
            );
            next_queue_sample += Duration::from_secs(1);
        }
        if Instant::now() >= next_retention_scan {
            let retention_scan = run_retention_scan_on_blocking_pool(
                dataset_root.clone(),
                retention_policy_receipt_path.clone(),
                RETENTION_SCAN_BUDGET_MS,
            );
            tokio::pin!(retention_scan);
            tokio::select! {
                () = &mut shutdown => {
                    controlled_shutdown = true;
                    break;
                }
                result = &mut retention_scan => {
                    if let Err(error) = result {
                        record_gap(
                            &source_gaps,
                            format!("retention_periodic_scan_error:{error}"),
                        );
                    }
                }
            }
            next_retention_scan += Duration::from_secs(RETENTION_SCAN_INTERVAL_SECS);
        }
    }

    let source_task_stage_started_utc_ms = now_ms();
    let source_task_stage_started = Instant::now();
    let mut source_task_status = "success";
    let mut source_task_errors = Vec::new();
    if controlled_shutdown {
        for handle in &handles {
            handle.abort();
        }
    }
    for handle in handles {
        match timeout(Duration::from_secs(15), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => {
                source_task_status = "failed";
                let message = format!("source_task_error:{err}");
                source_task_errors.push(message.clone());
                record_gap(&source_gaps, message);
            }
            Ok(Err(err)) if err.is_cancelled() && controlled_shutdown => {}
            Ok(Err(err)) => {
                source_task_status = "failed";
                let message = format!("source_task_join_error:{err}");
                source_task_errors.push(message.clone());
                record_gap(&source_gaps, message);
            }
            Err(_) => {
                source_task_status = "timeout";
                let message = "source_task_join_timeout".to_owned();
                source_task_errors.push(message.clone());
                record_gap(&source_gaps, message);
            }
        }
    }
    record_shutdown_stage(
        &mut shutdown_stage_timings,
        "source_task_stop",
        source_task_stage_started_utc_ms,
        source_task_stage_started,
        source_task_status,
        (!source_task_errors.is_empty()).then(|| source_task_errors.join(";")),
    );

    let drain_stage_started_utc_ms = now_ms();
    let drain_stage_started = Instant::now();
    while let Ok(timed_event) = event_rx.try_recv() {
        let event = timed_event.event;
        update_coverage(&mut coverage, &event);
        let source_kind = event.source_kind.clone();
        batches.entry(source_kind).or_default().push(event);
    }
    record_shutdown_stage(
        &mut shutdown_stage_timings,
        "event_queue_drain",
        drain_stage_started_utc_ms,
        drain_stage_started,
        "success",
        None,
    );

    let flush_stage_started_utc_ms = now_ms();
    let flush_stage_started = Instant::now();
    let flush_result = flush_all_batches(
        &mut writer_handles,
        &mut batches,
        &source_gaps,
        &mut writer_queue_tracker,
        run_started,
    )
    .await;
    record_shutdown_stage(
        &mut shutdown_stage_timings,
        "flush_batches",
        flush_stage_started_utc_ms,
        flush_stage_started,
        if flush_result.is_ok() {
            "success"
        } else {
            "failed"
        },
        flush_result.as_ref().err().map(|error| error.to_string()),
    );
    flush_result?;

    let writer_stage_started_utc_ms = now_ms();
    let writer_stage_started = Instant::now();
    let writer_outputs_result = stop_writer_workers(writer_handles).await;
    record_shutdown_stage(
        &mut shutdown_stage_timings,
        "writer_finalize",
        writer_stage_started_utc_ms,
        writer_stage_started,
        if writer_outputs_result.is_ok() {
            "success"
        } else {
            "failed"
        },
        writer_outputs_result
            .as_ref()
            .err()
            .map(|error| error.to_string()),
    );
    let writer_outputs = match writer_outputs_result {
        Ok(outputs) => outputs,
        Err(error) => {
            record_gap(&source_gaps, format!("writer_finalize_failed:{error}"));
            Vec::new()
        }
    };

    let retention_stage_started_utc_ms = now_ms();
    let retention_stage_started = Instant::now();
    record_shutdown_stage(
        &mut shutdown_stage_timings,
        "retention_shutdown_scan",
        retention_stage_started_utc_ms,
        retention_stage_started,
        "skipped",
        Some("shutdown skips retention scan; startup and periodic run-level receipt remain authoritative".to_owned()),
    );
    let finalized_segments = writer_outputs
        .iter()
        .flat_map(|output| output.finalized_segments.clone())
        .collect::<Vec<_>>();
    let source_writer_stats = writer_outputs
        .iter()
        .map(|output| output.stats.clone())
        .collect::<Vec<_>>();
    let mut gaps = source_gaps
        .lock()
        .expect("source gap mutex poisoned")
        .clone();
    for source in LIVE_SOURCES {
        if coverage.get(*source).is_none_or(|stats| stats.rows == 0) {
            gaps.push(format!("missing_source_coverage:{source}"));
        }
    }
    let polymarket_windows = coverage
        .get("polymarket_market")
        .map(|stats| stats.market_windows.len())
        .unwrap_or_default();
    if polymarket_windows < min_market_windows {
        gaps.push(format!(
            "insufficient_polymarket_market_windows: observed={polymarket_windows} required={min_market_windows}"
        ));
    }
    let rollover_evidence = build_rollover_receipts(
        &rollover_edges
            .lock()
            .expect("rollover edge mutex poisoned")
            .clone(),
        &coverage,
    );
    let valid_rollover_edges = rollover_evidence
        .iter()
        .filter(|edge| {
            edge.prefetched_promoted_to_current
                && edge.previous_current_retired_by_time
                && edge.windows_contiguous
                && edge.new_current_first_recv_time_utc_ms.is_some()
        })
        .count();
    if valid_rollover_edges < min_rollover_edges {
        gaps.push(format!(
            "insufficient_rollover_edges: observed={valid_rollover_edges} required={min_rollover_edges}"
        ));
    }

    let source_coverage = coverage
        .iter()
        .filter_map(|(source_kind, stats)| stats.to_source_coverage(source_kind))
        .collect::<Vec<_>>();
    source_queue_backpressure.apply_to_tracker(&mut source_queue_tracker);
    let source_queue_summary = source_queue_tracker.finish(monotonic_ms(run_started));
    let writer_queue_summary = writer_queue_tracker.finish(monotonic_ms(run_started));
    let verdict = if gaps.is_empty() {
        "PASS"
    } else {
        "SOURCE_GAP"
    };
    record_instant_shutdown_stage(
        &mut shutdown_stage_timings,
        "post_manifest_cleanup",
        "skipped",
        Some("default runtime sidecars are not written".to_owned()),
    );
    write_control_manifest(
        &dataset_root,
        &artifact_dir,
        &config,
        verdict,
        gaps,
        source_coverage,
        finalized_segments,
        source_queue_summary,
        writer_queue_summary,
        rollover_evidence,
        source_writer_stats,
        shutdown_stage_timings,
        SHUTDOWN_RETENTION_SCAN_BUDGET_MS,
    )
}

fn start_writer_workers(
    dataset_root: &Path,
    provenance: &RuntimeProvenance,
) -> Result<BTreeMap<String, SourceWriterHandle>> {
    let mut handles = BTreeMap::new();
    for source_kind in LIVE_SOURCES {
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let dataset_root = dataset_root.to_path_buf();
        let provenance = provenance.clone();
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
                );
                let _ = result_tx.send(result);
            })
            .map_err(|err| io::Error::other(format!("spawn writer thread failed: {err}")))?;
        handles.insert(
            (*source_kind).to_owned(),
            SourceWriterHandle {
                tx,
                queue_rows,
                result_rx,
            },
        );
    }
    Ok(handles)
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
            result_rx,
        },
    );
    Ok(handles)
}

async fn flush_all_batches(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut BTreeMap<String, Vec<RawEvent>>,
    gaps: &Arc<Mutex<Vec<String>>>,
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
    batch: &mut Vec<RawEvent>,
    gaps: &Arc<Mutex<Vec<String>>>,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = batch.len() as u64;
    let events = std::mem::take(batch);
    let Some(writer) = writers.get(source_kind) else {
        writer_queue_tracker.record_drop(rows);
        record_gap(gaps, format!("writer_missing_for_source:{source_kind}"));
        return Ok(());
    };
    let mut command = WriterCommand::Events(events);
    loop {
        writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
        match writer.tx.try_send(command) {
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
                sleep(Duration::from_millis(50)).await;
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
                writer_queue_tracker.record_drop(rows);
                record_gap(gaps, format!("writer_queue_disconnected:{source_kind}"));
                return Ok(());
            }
        }
        if monotonic_ms(run_started) > SOURCE_SEND_TIMEOUT_MS {
            match timeout(Duration::from_millis(SOURCE_SEND_TIMEOUT_MS), async {
                loop {
                    writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
                    match writer.tx.try_send(command) {
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
                    record_gap(gaps, gap);
                }
                Ok(None) => return Ok(()),
                Err(_) => {
                    writer_queue_tracker.record_backpressure_timeout();
                    writer_queue_tracker.record_drop(rows);
                    record_gap(
                        gaps,
                        format!("writer_queue_backpressure_timeout:{source_kind}"),
                    );
                }
            }
            return Ok(());
        }
    }
}

async fn stop_writer_workers(
    writers: BTreeMap<String, SourceWriterHandle>,
) -> Result<Vec<SourceWriterOutput>> {
    stop_writer_workers_with_budget(writers, Duration::from_millis(WRITER_FINALIZE_BUDGET_MS)).await
}

async fn stop_writer_workers_with_budget(
    writers: BTreeMap<String, SourceWriterHandle>,
    budget: Duration,
) -> Result<Vec<SourceWriterOutput>> {
    let mut receivers = Vec::new();
    for (source_kind, writer) in writers {
        let SourceWriterHandle {
            tx,
            queue_rows: _,
            result_rx,
        } = writer;
        drop(tx);
        receivers.push((source_kind, result_rx));
    }

    let mut outputs = Vec::new();
    let deadline = Instant::now() + budget;
    for (source_kind, result_rx) in receivers {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("writer finalize timeout before {source_kind} completed"),
            )
            .into());
        }
        match result_rx.recv_timeout(remaining) {
            Ok(Ok(output)) => outputs.push(output),
            Ok(Err(error)) => {
                return Err(io::Error::other(format!(
                    "{source_kind} writer finalize failed: {error}"
                ))
                .into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("writer finalize timeout for {source_kind}"),
                )
                .into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(format!(
                    "{source_kind} writer result channel disconnected"
                ))
                .into());
            }
        }
    }
    Ok(outputs)
}

async fn run_retention_scan_on_blocking_pool(
    dataset_root: PathBuf,
    receipt_path: PathBuf,
    budget_ms: u64,
) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(budget_ms);
        TrainingRetentionPolicy::new(&dataset_root)?
            .enforce_with_deadline(&receipt_path, deadline)
            .map(|_| ())
    })
    .await
    .map_err(|err| io::Error::other(format!("retention scan join failed: {err}")))??;
    Ok(())
}

fn source_writer_worker(
    dataset_root: PathBuf,
    provenance: RuntimeProvenance,
    source_kind: String,
    rx: std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
) -> Result<SourceWriterOutput> {
    let mut active = None::<ActiveSegment>;
    let mut finalized_segments = Vec::new();
    let mut stats = SourceWriterStatsBuilder::new(source_kind.clone());
    let partition_policy = HourlyPartitionPolicy::new(&dataset_root)?;
    let mut current_hour_part = 0_u32;
    while let Ok(command) = rx.recv() {
        match command {
            WriterCommand::Events(events) => {
                queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                for event in events {
                    let current_partition =
                        partition_policy.partition(&source_kind, event.recv_time_utc_ms, None)?;
                    let mut next_part = None;
                    if let Some(reason) = active.as_ref().and_then(|segment| {
                        live_segment_rollover_reason(segment, &current_partition.utc_hour)
                    }) {
                        let same_hour = active
                            .as_ref()
                            .is_some_and(|segment| segment.utc_hour == current_partition.utc_hour);
                        let segment = active.take().expect("active segment exists");
                        finalized_segments.push(finalize_segment(segment, reason, &provenance)?);
                        if same_hour && is_protective_part_rollover(reason) {
                            current_hour_part = current_hour_part.saturating_add(1);
                            next_part = Some(current_hour_part);
                        } else {
                            current_hour_part = 0;
                        }
                    }
                    if active.is_none() {
                        let (naming, selected_hour_part) = next_available_hourly_partition(
                            &partition_policy,
                            &source_kind,
                            event.recv_time_utc_ms,
                            next_part,
                        )?;
                        current_hour_part = selected_hour_part;
                        active = Some(ActiveSegment::new(
                            naming,
                            &source_kind,
                            selected_hour_part,
                        )?);
                    }
                    let segment = active.as_mut().expect("active segment exists");
                    let write_started = Instant::now();
                    let written_rows = segment.writer.write_event(&event)?;
                    let write_events_us = write_started.elapsed().as_micros() as u64;
                    segment.observe(&event, written_rows);
                    stats.record_written_event(
                        event.recv_time_utc_ms,
                        written_rows,
                        write_events_us,
                    );
                }
            }
        }
    }
    if let Some(segment) = active.take() {
        finalized_segments.push(finalize_segment(segment, "shutdown_finalize", &provenance)?);
    }
    let stats = stats.finish(&finalized_segments);
    Ok(SourceWriterOutput {
        source_kind,
        finalized_segments,
        stats,
    })
}

fn finalize_segment(
    segment: ActiveSegment,
    rollover_reason: &str,
    provenance: &RuntimeProvenance,
) -> Result<LiveFinalizedSegment> {
    let manifest_path = segment.naming.manifest_path.clone();
    let source_kind = segment.source_kind.clone();
    let manifest_context = SegmentManifestContext::from_segment(&segment);
    let published = segment.writer.finalize()?;
    let manifest = build_segment_manifest(&manifest_context, &published, provenance);
    let payload =
        segment_manifest_payload(&manifest, &manifest_context, rollover_reason, provenance)?;
    write_manifest_atomic(&manifest_path, &payload)?;
    Ok(LiveFinalizedSegment {
        source_kind,
        parquet_path: published.final_path,
        manifest_path,
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
        if !partition.running_path.exists()
            && !partition.digesting_path.exists()
            && !partition.final_path.exists()
            && !partition.manifest_path.exists()
        {
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
    bytes: u64,
    event_type_counts: BTreeMap<String, u64>,
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
            bytes: segment.bytes,
            event_type_counts: segment.event_type_counts.clone(),
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
        }
    }
}

fn build_segment_manifest(
    segment: &SegmentManifestContext,
    published: &PublishedSegment,
    provenance: &RuntimeProvenance,
) -> ManifestDraft {
    let recv_start = segment.segment_start_recv_time_utc_ms;
    let recv_end = segment.last_recv_time_utc_ms.max(recv_start);
    let source_coverage = SourceCoverage {
        source_kind: segment.source_kind.clone(),
        first_recv_time_utc_ms: recv_start,
        last_recv_time_utc_ms: recv_end,
        rows: published.row_count,
    };
    let resource_watermark = segment_manifest_resource_watermark(segment, published);

    ManifestDraft {
        artifact_path: Some(published.final_path.clone()),
        row_count: Some(published.row_count),
        sha256: Some(published.sha256.clone()),
        schema_version: Some(published.schema_version.clone()),
        code_version: Some(provenance.code_version.clone()),
        collector_binary_sha256: Some(provenance.collector_binary_sha256.clone()),
        source_config_digest: Some(provenance.source_config_digest.clone()),
        validation_verdict: ValidationVerdict::Complete,
        consumer_boundary_declarations: required_consumer_boundary_declarations(),
        source_coverage: vec![source_coverage],
        source_gaps: segment.depth_source_gaps.iter().cloned().collect(),
        manifest_source_gaps: Vec::new(),
        coverage_incomplete_reason: None,
        health_artifact_path: None,
        health_artifact_digest: None,
        gap_artifact_path: None,
        gap_artifact_digest: None,
        reconnect_artifact_path: None,
        reconnect_artifact_digest: None,
        start_time_utc_ms: Some(recv_start),
        end_time_utc_ms: Some(recv_end),
        coverage_window: Some(TimeWindow {
            start_utc_ms: recv_start,
            end_utc_ms: recv_end.max(recv_start.saturating_add(1)),
        }),
        resource_watermark: Some(resource_watermark),
        resource_threshold_policy_receipt: Some(segment_resource_threshold_policy_receipt()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: segment.rows,
            bytes: segment.bytes,
            oldest_event_age_ms: recv_end.saturating_sub(recv_start).max(0) as u64,
            writer_lag_ms: 0,
        }),
        backpressure_receipt: Some(BackpressureReceipt::empty(segment.source_kind.clone())),
        flush_latency_ms: Some(0),
        collector_host_id: Some(provenance.collector_host_id.clone()),
        collector_region: Some(provenance.collector_region.clone()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: published.final_path.clone(),
            finalized: true,
            row_count: Some(published.row_count),
            sha256: Some(published.sha256.clone()),
            schema_version: Some(published.schema_version.clone()),
            missing_reason: None,
        }],
    }
}

fn segment_manifest_payload(
    manifest: &ManifestDraft,
    segment: &SegmentManifestContext,
    rollover_reason: &str,
    provenance: &RuntimeProvenance,
) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(manifest)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "recv_time_window_ms".to_owned(),
            json!({
                "start": segment.segment_start_recv_time_utc_ms,
                "end": segment.last_recv_time_utc_ms,
            }),
        );
        object.insert(
            "source_timestamp_range".to_owned(),
            json!({
                "start": segment.source_ts_start.clone(),
                "end": segment.source_ts_end.clone(),
            }),
        );
        object.insert("market_slug_window".to_owned(), Value::Null);
        object.insert("utc_day".to_owned(), json!(segment.utc_day));
        object.insert("utc_hour".to_owned(), json!(segment.utc_hour));
        object.insert(
            "hour_window_utc_ms".to_owned(),
            json!({
                "start": segment.hour_start_utc_ms,
                "end": segment.hour_end_utc_ms,
            }),
        );
        object.insert("hour_part".to_owned(), json!(segment.hour_part));
        object.insert(
            "hour_part_count".to_owned(),
            json!(u64::from(segment.hour_part) + 1),
        );
        object.insert("segment_seq".to_owned(), json!(segment.hour_part));
        object.insert(
            "layout_family".to_owned(),
            json!(layout_family_for_source(&segment.source_kind)),
        );
        object.insert(
            "pmxt_reference_url".to_owned(),
            if segment.source_kind == "polymarket_market" {
                json!("https://archive.pmxt.dev/docs/v2-data-overview#overview")
            } else {
                Value::Null
            },
        );
        object.insert("sort_order".to_owned(), json!("receive_order"));
        object.insert("compression".to_owned(), json!("zstd_level_3"));
        object.insert(
            "required_typed_fields_present".to_owned(),
            json!(required_typed_fields_present(segment)),
        );
        object.insert("book_rows".to_owned(), json!(event_count(segment, "book")));
        object.insert(
            "book_rows_with_bids_asks".to_owned(),
            json!(segment.book_rows_with_bids_asks),
        );
        object.insert(
            "price_change_rows".to_owned(),
            json!(event_count(segment, "price_change")),
        );
        object.insert(
            "metadata_rows".to_owned(),
            json!(
                event_count(segment, "new_market")
                    + event_count(segment, "market_discovery_switch")
            ),
        );
        object.insert(
            "new_market_rows".to_owned(),
            json!(event_count(segment, "new_market")),
        );
        object.insert(
            "market_resolved_rows".to_owned(),
            json!(event_count(segment, "market_resolved")),
        );
        object.insert(
            "tick_size_change_rows".to_owned(),
            json!(event_count(segment, "tick_size_change")),
        );
        object.insert(
            "depth_update_rows".to_owned(),
            json!(event_count(segment, "depth_update")),
        );
        object.insert(
            "binance_depth_stitch".to_owned(),
            json!({
                "snapshot_last_update_id": segment.depth_snapshot_last_update_id,
                "stitch_applied_rows": segment.depth_stitch_applied_rows,
                "gap_rows": segment.depth_stitch_gap_rows,
                "resnapshot_count": segment.depth_resnapshot_count,
                "local_u_min": segment.depth_local_u_min,
                "local_u_max": segment.depth_local_u_max,
                "first_update_bridged_snapshot": segment.depth_first_update_bridged_snapshot,
            }),
        );
        object.insert(
            "snapshot_rows".to_owned(),
            json!(event_count(segment, "depth_snapshot")),
        );
        object.insert(
            "aggtrade_rows".to_owned(),
            json!(event_count(segment, "aggTrade")),
        );
        object.insert(
            "rtds_rows".to_owned(),
            json!(event_count(segment, "update")),
        );
        object.insert(
            "covered_market_slugs".to_owned(),
            json!(segment.covered_market_slugs),
        );
        object.insert(
            "covered_condition_ids".to_owned(),
            json!(segment.covered_condition_ids),
        );
        object.insert(
            "covered_asset_ids".to_owned(),
            json!(segment.covered_asset_ids),
        );
        object.insert("retention_policy_digest".to_owned(), Value::Null);
        object.insert(
            "retention_policy_receipt_path".to_owned(),
            json!(run_artifact_path(
                &provenance.run_id,
                "retention_policy_receipt.json"
            )),
        );
        object.insert("deletion_receipt_path".to_owned(), Value::Null);
        object.insert("rollover_reason".to_owned(), json!(rollover_reason));
        object.insert(
            "event_available_time_semantics".to_owned(),
            json!("collector_recv_time"),
        );
        object.insert("soft_rollover_not_process_fatal".to_owned(), json!(true));
        object.insert("shutdown_finalize_result".to_owned(), json!("finalized"));
    }
    Ok(serde_json::to_vec_pretty(&value)?)
}

fn event_count(segment: &SegmentManifestContext, event_type: &str) -> u64 {
    segment
        .event_type_counts
        .get(event_type)
        .copied()
        .unwrap_or_default()
}

fn layout_family_for_source(source_kind: &str) -> &'static str {
    if source_kind == "polymarket_market" {
        "pmxt_inspired_polymarket_orderbook"
    } else {
        "source_specific_hourly_typed_raw"
    }
}

fn required_typed_fields_present(segment: &SegmentManifestContext) -> bool {
    match segment.source_kind.as_str() {
        "polymarket_market" => {
            let book_rows = event_count(segment, "book");
            book_rows == 0 || segment.book_rows_with_bids_asks == book_rows
        }
        "binance_depth_snapshot" => event_count(segment, "depth_snapshot") > 0,
        "binance_depth_update" => event_count(segment, "depth_update") > 0,
        "binance_aggtrade" => event_count(segment, "aggTrade") > 0,
        "polymarket_rtds_chainlink_btc_usd" => event_count(segment, "update") > 0,
        _ => segment.rows > 0,
    }
}

fn write_manifest_atomic(manifest_path: &Path, payload: &[u8]) -> Result<()> {
    let file_name = manifest_path
        .file_name()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "manifest path has no file name",
            )
        })?
        .to_string_lossy();
    let tmp_path = manifest_path.with_file_name(format!("{file_name}.tmp"));
    fs::write(&tmp_path, payload)?;
    fs::rename(&tmp_path, manifest_path)?;
    Ok(())
}

fn predeclared_manifest_atomic_write_stage(manifest_path: &Path) -> ShutdownStageTiming {
    let started_utc_ms = now_ms();
    ShutdownStageTiming {
        started_utc_ms,
        ended_utc_ms: started_utc_ms,
        duration_ms: 0,
        status: "success".to_owned(),
        error: None,
        manifest_path: Some(manifest_path.to_path_buf()),
    }
}

fn run_artifact_path(run_id: &str, file_name: &str) -> PathBuf {
    PathBuf::from("training")
        .join("runtime_artifacts")
        .join(run_id)
        .join(file_name)
}

fn segment_manifest_resource_watermark(
    segment: &SegmentManifestContext,
    published: &PublishedSegment,
) -> ResourceWatermark {
    let recv_age_ms = segment
        .last_recv_time_utc_ms
        .saturating_sub(segment.segment_start_recv_time_utc_ms)
        .max(0) as u64;
    let parquet_bytes = published
        .final_path
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(segment.bytes);
    ResourceWatermark {
        rss_bytes: read_rss_bytes(),
        cpu_percent: None,
        disk_free_bytes: None,
        artifact_dir_bytes: Some(parquet_bytes),
        journal_log_bytes: None,
        journal_limit_status: None,
        open_files: None,
        queue_bytes: Some(segment.bytes),
        queue_age_ms: Some(recv_age_ms),
        parquet_writer_memory_estimate_bytes: Some(segment.bytes),
        cgroup_memory_current_bytes: None,
        cgroup_memory_stat: None,
        cgroup_memory_events: None,
        psi_cpu: None,
        psi_memory: None,
        psi_io: None,
        stage_latency_ms: Some(0),
        ttl_ms: Some(DEFAULT_MAX_SEGMENT_DURATION_MS as u64),
        unavailable: Vec::new(),
    }
}

fn segment_resource_threshold_policy_receipt() -> ResourceThresholdPolicyReceipt {
    let thresholds = ResourceThresholds {
        min_disk_free_bytes: 1,
        max_rss_bytes: 2 * 1024 * 1024 * 1024,
        max_cpu_percent: 95.0,
        max_artifact_dir_bytes: 1024 * 1024 * 1024,
        max_journal_log_bytes: 1024 * 1024 * 1024,
        max_open_files: 4096,
        max_queue_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        max_queue_age_ms: DEFAULT_MAX_SEGMENT_DURATION_MS as u64,
        max_parquet_writer_memory_estimate_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        max_cgroup_memory_current_bytes: 2 * 1024 * 1024 * 1024,
        max_cgroup_memory_anon_bytes: 2 * 1024 * 1024 * 1024,
        max_cgroup_memory_file_bytes: 2 * 1024 * 1024 * 1024,
        max_cgroup_memory_slab_bytes: 512 * 1024 * 1024,
        max_psi_some_avg10: 90.0,
        max_psi_full_avg10: 50.0,
        max_stage_latency_ms: 1_000,
        max_ttl_ms: (DEFAULT_MAX_SEGMENT_DURATION_MS as u64).saturating_mul(2),
    };
    ResourceThresholdPolicyReceipt {
        soft_thresholds: thresholds,
        hard_thresholds: thresholds,
        threshold_unit: "bytes/percent/count/milliseconds/psi_avg10".to_owned(),
        hard_hysteresis_samples: 2,
        recovery_hysteresis_samples: 2,
        soft_actions: vec![
            ResourceAction::RecordBackpressure,
            ResourceAction::ObserveRecovery,
        ],
        hard_actions: vec![
            ResourceAction::StopNewIntake,
            ResourceAction::FinalizeOpenSegments,
            ResourceAction::WriteIncompleteManifest,
        ],
        recovery_actions: vec![
            ResourceAction::ObserveRecovery,
            ResourceAction::ResumeIntake,
        ],
        recovery_condition: "below soft thresholds for recovery_hysteresis_samples".to_owned(),
        last_result: ResourceSeverity::Healthy,
    }
}

async fn run_polymarket_source(
    sink: SourceEventSink,
    rollover_edges: Arc<Mutex<Vec<RolloverEdge>>>,
    collector_identity: CollectorIdentity,
    deadline_ms: i64,
    discovery_wait_secs: u64,
    run_started: Instant,
) -> Result<()> {
    let gamma = GammaClient::default();
    let mut previous_pair = None::<BtcUpdownDiscoveryPair>;
    while now_ms() < deadline_ms {
        let pair = match discover_pair_with_wait(&gamma, discovery_wait_secs).await {
            Ok(pair) => pair,
            Err(err) => {
                record_gap(&sink.gaps, format!("polymarket_discovery_source_gap:{err}"));
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };
        if let Some(previous) = &previous_pair {
            rollover_edges
                .lock()
                .expect("rollover edge mutex poisoned")
                .push(RolloverEdge::from_pairs(previous, &pair));
        }
        previous_pair = Some(pair.clone());
        let segment_end_ms = (pair.current.end_ts_s * 1000 + 5_000).min(deadline_ms);
        collect_polymarket_pair(
            &pair,
            &sink,
            &collector_identity,
            segment_end_ms,
            run_started,
        )
        .await?;
    }
    Ok(())
}

async fn collect_polymarket_pair(
    pair: &BtcUpdownDiscoveryPair,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    segment_end_ms: i64,
    run_started: Instant,
) -> Result<()> {
    let assets = pair_asset_ids(pair)
        .iter()
        .map(|asset| U256::from_str(asset))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let asset_to_slug = pair_asset_to_slug(pair);
    let client = MarketWsClient::new(MARKET_WS_ENDPOINT, bounded_ws_config())?;
    let mut bba_stream = Box::pin(client.subscribe_best_bid_ask(assets.clone())?);
    let mut book_stream = Box::pin(client.subscribe_orderbook(assets.clone())?);
    let mut price_stream = Box::pin(client.subscribe_prices(assets.clone())?);
    let mut last_trade_stream = Box::pin(client.subscribe_last_trade_price(assets.clone())?);
    let mut tick_size_stream = Box::pin(client.subscribe_tick_size_change(assets.clone())?);
    let mut new_market_stream = Box::pin(client.subscribe_new_markets(assets.clone())?);
    let mut market_resolved_stream = Box::pin(client.subscribe_market_resolutions(assets)?);
    wait_market_connected(&client, Duration::from_secs(20)).await?;
    send_event(
        sink,
        run_started,
        RawEvent {
            source_kind: "polymarket_market".to_owned(),
            source_event_type: "market_discovery_switch".to_owned(),
            source_identifier: format!("{}:{}", pair.current.slug, pair.next.slug),
            source_ts: Some(now_ms().to_string()),
            recv_time_utc_ms: now_ms(),
            collector_host_id: collector_identity.host_id.clone(),
            collector_region: collector_identity.region.clone(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            raw_json: serde_json::to_string(&json!({
                "event_type": "market_discovery_switch",
                "current": pair.current,
                "next": pair.next,
                "connection_state": format!("{:?}", client.connection_state(ChannelType::Market)),
                "subscription_count": client.subscription_count()
            }))?,
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_market".to_owned(),
        },
    )
    .await;

    while now_ms() < segment_end_ms {
        tokio::select! {
            maybe_bba = bba_stream.next() => {
                match maybe_bba {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "best_bid_ask",
                            &format!("{}:{}", slug, event.asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
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
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_bba_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_bba_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_book = book_stream.next() => {
                match maybe_book {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "book",
                            &format!("{}:{}", slug, event.asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_book_raw_json(&event, &slug),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_book_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_book_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_price = price_stream.next() => {
                match maybe_price {
                    Some(Ok(event)) => {
                        let source_identifier = event.price_changes.first().map(|change| {
                            let asset_id = change.asset_id.to_string();
                            let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                            format!("{slug}:{asset_id}")
                        }).unwrap_or_else(|| event.market.to_string());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "price_change",
                            &source_identifier,
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_price_change_raw_json(&event, &asset_to_slug),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_price_change_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_price_change_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_last_trade = last_trade_stream.next() => {
                match maybe_last_trade {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "last_trade_price",
                            &format!("{}:{}", slug, asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_last_trade_raw_json(&event, &slug),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_last_trade_price_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_last_trade_price_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_tick_size = tick_size_stream.next() => {
                match maybe_tick_size {
                    Some(Ok(event)) => {
                        let asset_id = event.asset_id.to_string();
                        let slug = asset_to_slug.get(&asset_id).cloned().unwrap_or_else(|| "unknown".to_owned());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "tick_size_change",
                            &format!("{}:{}", slug, asset_id),
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_tick_size_change_raw_json(&event, &slug),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_tick_size_change_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_tick_size_change_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_new_market = new_market_stream.next() => {
                match maybe_new_market {
                    Some(Ok(event)) => {
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "new_market",
                            &event.slug,
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_new_market_raw_json(&event),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_new_market_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_new_market_stream_closed".to_owned());
                        break;
                    }
                }
            }
            maybe_market_resolved = market_resolved_stream.next() => {
                match maybe_market_resolved {
                    Some(Ok(event)) => {
                        let source_identifier = event.slug.clone().unwrap_or_else(|| event.market.to_string());
                        send_event(sink, run_started, raw_event(
                            "polymarket_market",
                            "market_resolved",
                            &source_identifier,
                            Some(event.timestamp.to_string()),
                            MARKET_WS_ENDPOINT,
                            collector_identity,
                            polymarket_market_resolved_raw_json(&event),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_market_resolved_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_market_resolved_stream_closed".to_owned());
                        break;
                    }
                }
            }
            () = sleep(Duration::from_millis(250)) => {}
        }
    }
    Ok(())
}

async fn run_rtds_source(
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<()> {
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let actions = rtds_freshness_actions();
    let mut startup_connected = false;
    let mut action_index = 0_usize;

    'source: loop {
        if Instant::now() >= deadline {
            break;
        }

        let client = RtdsClient::new(RTDS_WS_ENDPOINT, bounded_ws_config())?;
        let mut stream = Box::pin(client.subscribe_raw(subscription.clone())?);
        if !startup_connected {
            wait_rtds_connected(&client, Duration::from_secs(20)).await?;
            startup_connected = true;
        }

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break 'source;
            }
            let action = actions[action_index];
            let limited_by_deadline = remaining < action.wait;
            let wait_duration = action.wait.min(remaining);

            match timeout(wait_duration, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    let price = message.as_chainlink_price();
                    let source_ts = price
                        .as_ref()
                        .map(|payload| payload.timestamp)
                        .unwrap_or(message.timestamp);
                    send_event(
                        &sink,
                        run_started,
                        raw_event(
                            "polymarket_rtds_chainlink_btc_usd",
                            "update",
                            "btc/usd",
                            Some(source_ts.to_string()),
                            RTDS_WS_ENDPOINT,
                            &collector_identity,
                            json!({
                                "topic": message.topic,
                                "type": message.msg_type,
                                "message_timestamp": message.timestamp,
                                "payload_timestamp": price.as_ref().map(|payload| payload.timestamp),
                                "symbol": price.as_ref().map(|payload| payload.symbol.clone()),
                                "value": price.as_ref().map(|payload| payload.value.to_string()),
                                "subscription_raw_json": rtds_subscription_evidence_json(),
                                "payload": message.payload
                            }),
                        ),
                    )
                    .await;
                    action_index = 0;
                }
                Ok(Some(Err(err))) => {
                    record_gap(&sink.gaps, format!("rtds_stream_error:{err}"));
                    action_index = 0;
                    continue 'source;
                }
                Ok(None) => {
                    record_gap(&sink.gaps, "rtds_stream_closed".to_owned());
                    break 'source;
                }
                Err(_) if limited_by_deadline => break 'source,
                Err(_) => {
                    let recovery = rtds_timeout_recovery_step(action);
                    if recovery.unsubscribe_current_client
                        && let Err(err) = client.unsubscribe_chainlink_prices()
                    {
                        record_gap(&sink.gaps, format!("rtds_resubscribe_failed:{err}"));
                    }

                    if recovery.resubscribe_current_client {
                        match client.subscribe_raw(subscription.clone()) {
                            Ok(next_stream) => stream = Box::pin(next_stream),
                            Err(err) => {
                                record_gap(&sink.gaps, format!("rtds_resubscribe_failed:{err}"));
                            }
                        }
                    }

                    action_index += 1;

                    if action.source_gap_after_resubscribe {
                        record_gap(&sink.gaps, "rtds_business_message_timeout".to_owned());
                        action_index = 0;
                    }

                    if recovery.reconnect_with_new_client {
                        continue 'source;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_binance_depth_source(
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<()> {
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(BINANCE_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BINANCE_HTTP_TOTAL_TIMEOUT_SECS))
        .build()?;
    let mut snapshot_trigger_reason = "bootstrap".to_owned();
    let mut reconnect_attempts = 0_u32;
    while Instant::now() < deadline {
        let (depth_stream, _) = match timeout(
            Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
            connect_async(BINANCE_DEPTH_WS),
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
                    &mut reconnect_attempts,
                    &mut snapshot_trigger_reason,
                    reason,
                    gap,
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
                    &mut reconnect_attempts,
                    &mut snapshot_trigger_reason,
                    reason,
                    gap,
                )
                .await
                {
                    break;
                }
                continue;
            }
        };
        let Some(snapshot_last_update_id) = fetch_binance_depth_snapshot(
            &http_client,
            &sink,
            &collector_identity,
            run_started,
            &snapshot_trigger_reason,
        )
        .await
        else {
            record_gap(
                &sink.gaps,
                if snapshot_trigger_reason == "bootstrap" {
                    "binance_depth_snapshot_last_update_id_missing".to_owned()
                } else {
                    format!("binance_depth_resync_failed:{snapshot_trigger_reason}")
                },
            );
            return Ok(());
        };
        let snapshot_utc_day =
            utc_day_string_from_ms(now_ms()).unwrap_or_else(|| "unknown".to_owned());
        match run_binance_depth_update_ws_source(
            depth_stream,
            BinanceDepthUpdateContext {
                http_client: http_client.clone(),
                sink: sink.clone(),
                collector_identity: collector_identity.clone(),
                deadline,
                run_started,
                snapshot_last_update_id,
                snapshot_utc_day,
                pending_snapshot_trigger_reason: (snapshot_trigger_reason != "bootstrap")
                    .then(|| snapshot_trigger_reason.clone()),
            },
        )
        .await?
        {
            BinanceDepthUpdateExit::DeadlineReached | BinanceDepthUpdateExit::ResyncFailed => {
                break;
            }
            BinanceDepthUpdateExit::StreamEnded(reason) => {
                if !prepare_binance_depth_reconnect(
                    &sink,
                    &mut reconnect_attempts,
                    &mut snapshot_trigger_reason,
                    reason,
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
    reconnect_attempts: &mut u32,
    snapshot_trigger_reason: &mut String,
    reason: String,
    gap: String,
) -> bool {
    record_gap(&sink.gaps, gap);
    prepare_binance_depth_reconnect(sink, reconnect_attempts, snapshot_trigger_reason, reason).await
}

async fn prepare_binance_depth_reconnect(
    sink: &SourceEventSink,
    reconnect_attempts: &mut u32,
    snapshot_trigger_reason: &mut String,
    reason: String,
) -> bool {
    *reconnect_attempts = reconnect_attempts.saturating_add(1);
    if *reconnect_attempts > BINANCE_DEPTH_MAX_RECONNECTS {
        record_gap(
            &sink.gaps,
            format!("binance_depth_reconnect_budget_exhausted:{reason}"),
        );
        return false;
    }
    record_gap(
        &sink.gaps,
        format!("binance_depth_reconnect_required:{reason}"),
    );
    *snapshot_trigger_reason = format!("reconnect:{reason}");
    let backoff_ms = u64::from(*reconnect_attempts)
        .saturating_mul(500)
        .min(5_000);
    sleep(Duration::from_millis(backoff_ms)).await;
    true
}

async fn fetch_binance_depth_snapshot(
    http_client: &reqwest::Client,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    trigger_reason: &str,
) -> Option<u64> {
    match http_client.get(BINANCE_DEPTH_SNAPSHOT).send().await {
        Ok(response) => {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if !status.is_success() || text.trim().is_empty() {
                record_gap(
                    &sink.gaps,
                    format!("binance_depth_snapshot_bad_status:{status}:{trigger_reason}"),
                );
                return None;
            }
            let mut snapshot_value = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
            let snapshot_last_update_id =
                snapshot_value.get("lastUpdateId").and_then(Value::as_u64);
            if let Some(object) = snapshot_value.as_object_mut() {
                object.insert("snapshot_trigger_reason".to_owned(), json!(trigger_reason));
            }
            send_event(
                sink,
                run_started,
                raw_event(
                    "binance_depth_snapshot",
                    "depth_snapshot",
                    "BTCUSDT",
                    None,
                    BINANCE_DEPTH_SNAPSHOT,
                    collector_identity,
                    snapshot_value,
                ),
            )
            .await;
            snapshot_last_update_id
        }
        Err(err) => {
            record_gap(
                &sink.gaps,
                format!("binance_depth_snapshot_error:{err}:{trigger_reason}"),
            );
            None
        }
    }
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

async fn run_binance_depth_update_ws_source(
    mut stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    mut context: BinanceDepthUpdateContext,
) -> Result<BinanceDepthUpdateExit> {
    let mut local_u = None::<u64>;
    while Instant::now() < context.deadline {
        match timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let raw_json = text.to_string();
                let recv_time_utc_ms = now_ms();
                let recv_utc_day = utc_day_string_from_ms(recv_time_utc_ms)
                    .unwrap_or(context.snapshot_utc_day.clone());
                if recv_utc_day != context.snapshot_utc_day {
                    match fetch_binance_depth_snapshot(
                        &context.http_client,
                        &context.sink,
                        &context.collector_identity,
                        context.run_started,
                        "utc_day_boundary",
                    )
                    .await
                    {
                        Some(next_snapshot_last_update_id) => {
                            context.snapshot_last_update_id = next_snapshot_last_update_id;
                            context.snapshot_utc_day = recv_utc_day;
                            context.pending_snapshot_trigger_reason =
                                Some("utc_day_boundary".to_owned());
                            local_u = None;
                            continue;
                        }
                        None => {
                            record_gap(
                                &context.sink.gaps,
                                "binance_depth_resync_failed:utc_day_boundary".to_owned(),
                            );
                            return Ok(BinanceDepthUpdateExit::ResyncFailed);
                        }
                    }
                }
                let base_event = RawEvent {
                    source_kind: "binance_depth_update".to_owned(),
                    source_event_type: "depth_update".to_owned(),
                    source_identifier: "BTCUSDT".to_owned(),
                    source_ts: serde_json::from_str::<Value>(&raw_json)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("E")
                                .and_then(Value::as_i64)
                                .map(|ts| ts.to_string())
                        }),
                    recv_time_utc_ms,
                    collector_host_id: context.collector_identity.host_id.clone(),
                    collector_region: context.collector_identity.region.clone(),
                    source_endpoint: BINANCE_DEPTH_WS.to_owned(),
                    raw_json: raw_json.clone(),
                    parquet_raw_layout_version: 1,
                    parquet_raw_source_kind: "binance_depth_update".to_owned(),
                };
                let parsed = match parse_depth_update_raw(
                    event_envelope(&base_event, SourceKind::BinanceDepthUpdate),
                    &raw_json,
                ) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        record_gap(
                            &context.sink.gaps,
                            format!("binance_depth_update_parse_error:{error}"),
                        );
                        continue;
                    }
                };
                let local_u_before = local_u;
                let stitched =
                    match stitch_depth_update(context.snapshot_last_update_id, local_u, &parsed) {
                        crate::sources::binance::DepthUpdateDecision::Apply {
                            local_u: next_local_u,
                            stitch_evidence,
                        } => {
                            local_u = Some(next_local_u);
                            apply_depth_stitch_evidence(
                                parsed,
                                crate::sources::binance::DepthUpdateDecision::Apply {
                                    local_u: next_local_u,
                                    stitch_evidence,
                                },
                            )
                            .map_err(invalid_data)?
                        }
                        crate::sources::binance::DepthUpdateDecision::DropStaleOrDuplicate {
                            ..
                        } => {
                            continue;
                        }
                        crate::sources::binance::DepthUpdateDecision::SourceGapResnapshot {
                            reason,
                        } => {
                            record_gap(
                                &context.sink.gaps,
                                format!("binance_depth_update_stitch_gap:{reason}"),
                            );
                            let trigger = format!("stitch_gap:{reason}");
                            match fetch_binance_depth_snapshot(
                                &context.http_client,
                                &context.sink,
                                &context.collector_identity,
                                context.run_started,
                                &trigger,
                            )
                            .await
                            {
                                Some(next_snapshot_last_update_id) => {
                                    context.snapshot_last_update_id = next_snapshot_last_update_id;
                                    context.snapshot_utc_day = utc_day_string_from_ms(now_ms())
                                        .unwrap_or(context.snapshot_utc_day);
                                    context.pending_snapshot_trigger_reason = Some(trigger);
                                    local_u = None;
                                    continue;
                                }
                                None => {
                                    record_gap(
                                        &context.sink.gaps,
                                        format!("binance_depth_resync_failed:{reason}"),
                                    );
                                    return Ok(BinanceDepthUpdateExit::ResyncFailed);
                                }
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
                    object.insert("local_u_after".to_owned(), json!(stitched.local_u));
                    object.insert(
                        "stitch_evidence".to_owned(),
                        json!(stitched.stitch_evidence),
                    );
                    if let Some(trigger_reason) = context.pending_snapshot_trigger_reason.take() {
                        object.insert("snapshot_trigger_reason".to_owned(), json!(trigger_reason));
                    }
                }
                send_event(
                    &context.sink,
                    context.run_started,
                    raw_event(
                        "binance_depth_update",
                        "depth_update",
                        "BTCUSDT",
                        base_event.source_ts,
                        BINANCE_DEPTH_WS,
                        &context.collector_identity,
                        enriched,
                    ),
                )
                .await;
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = stream.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded(format!(
                    "ws_closed:{frame:?}"
                )));
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded(format!(
                    "ws_error:{err}"
                )));
            }
            Ok(None) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded("ws_ended".to_owned()));
            }
            Err(_) => {
                return Ok(BinanceDepthUpdateExit::StreamEnded(
                    "no_business_messages".to_owned(),
                ));
            }
        }
    }
    Ok(BinanceDepthUpdateExit::DeadlineReached)
}

async fn run_binance_ws_source(
    spec: BinanceWsSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
) -> Result<()> {
    let (mut stream, _) = match timeout(
        Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
        connect_async(&spec.url),
    )
    .await
    {
        Ok(Ok(connected)) => connected,
        Ok(Err(err)) => {
            record_gap(
                &sink.gaps,
                format!("{}_ws_connect_error:{err}", spec.source_kind),
            );
            return Ok(());
        }
        Err(_) => {
            record_gap(
                &sink.gaps,
                format!("{}_ws_connect_timeout", spec.source_kind),
            );
            return Ok(());
        }
    };
    while Instant::now() < deadline {
        match timeout(
            Duration::from_secs(spec.freshness_timeout_secs),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(Message::Text(text)))) => {
                let raw_json = text.to_string();
                let value = serde_json::from_str::<Value>(&raw_json).unwrap_or(Value::Null);
                let event_type = value
                    .get("e")
                    .and_then(Value::as_str)
                    .unwrap_or(&spec.default_event_type);
                let source_ts = value
                    .get("E")
                    .or_else(|| value.get("T"))
                    .and_then(Value::as_i64)
                    .map(|ts| ts.to_string());
                send_event(
                    &sink,
                    run_started,
                    RawEvent {
                        source_kind: spec.source_kind.clone(),
                        source_event_type: event_type.to_owned(),
                        source_identifier: "BTCUSDT".to_owned(),
                        source_ts,
                        recv_time_utc_ms: now_ms(),
                        collector_host_id: collector_identity.host_id.clone(),
                        collector_region: collector_identity.region.clone(),
                        source_endpoint: spec.url.clone(),
                        raw_json,
                        parquet_raw_layout_version: 1,
                        parquet_raw_source_kind: spec.source_kind.clone(),
                    },
                )
                .await;
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = stream.send(Message::Pong(payload)).await;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                record_gap(
                    &sink.gaps,
                    format!("{}_ws_closed:{frame:?}", spec.source_kind),
                );
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                record_gap(&sink.gaps, format!("{}_ws_error:{err}", spec.source_kind));
            }
            Ok(None) => {
                record_gap(&sink.gaps, format!("{}_ws_stream_ended", spec.source_kind));
                break;
            }
            Err(_) => record_gap(
                &sink.gaps,
                format!("{}_business_message_timeout", spec.source_kind),
            ),
        }
    }
    Ok(())
}

async fn discover_pair_with_wait(
    gamma: &GammaClient,
    wait_secs: u64,
) -> Result<BtcUpdownDiscoveryPair> {
    let started_ms = now_ms();
    let final_error = loop {
        let query_ms = now_ms();
        let windows = discovery_windows(query_ms);
        let current = fetch_exact_slug_candidates(gamma, &windows.current.slug).await?;
        let next = fetch_exact_slug_candidates(gamma, &windows.next.slug).await?;
        match select_btc_updown_pair(query_ms, &current, &next) {
            Ok(pair) => return Ok(pair),
            Err(err) => {
                let last_error = format!("{err:?}");
                if now_ms().saturating_sub(started_ms) >= (wait_secs * 1000) as i64 {
                    break last_error;
                }
                sleep(Duration::from_secs(10)).await;
            }
        }
    };
    Err(format!("btc-updown-5m discovery failed after wait_secs={wait_secs}: {final_error}").into())
}

async fn fetch_exact_slug_candidates(
    gamma: &GammaClient,
    slug: &str,
) -> Result<Vec<BtcUpdownMarketCandidate>> {
    let mut request = MarketsRequest::default();
    request.slug = vec![slug.to_owned()];
    request.closed = Some(false);
    request.limit = Some(10);
    let markets = gamma.markets(&request).await?;
    Ok(markets
        .iter()
        .map(BtcUpdownMarketCandidate::from_sdk_market)
        .collect())
}

async fn wait_market_connected(client: &MarketWsClient, timeout_duration: Duration) -> Result<()> {
    timeout(timeout_duration, async {
        loop {
            if matches!(
                client.connection_state(ChannelType::Market),
                ConnectionState::Connected { .. }
            ) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "market ws did not connect in time")?;
    Ok(())
}

async fn wait_rtds_connected(client: &RtdsClient, timeout_duration: Duration) -> Result<()> {
    timeout(timeout_duration, async {
        loop {
            if matches!(client.connection_state(), ConnectionState::Connected { .. }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "rtds ws did not connect in time")?;
    Ok(())
}

fn bounded_ws_config() -> WsConfig {
    let mut config = WsConfig::default();
    config.reconnect.max_attempts = Some(3);
    config.reconnect.max_backoff = Duration::from_secs(5);
    config.heartbeat_interval = Duration::from_secs(10);
    config.heartbeat_timeout = Duration::from_secs(20);
    config
}

async fn send_event(sink: &SourceEventSink, run_started: Instant, event: RawEvent) {
    let source_kind = event.source_kind.clone();
    let timed_event = TimedRawEvent::new(event, monotonic_ms(run_started));
    match sink.tx.try_send(timed_event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(timed_event)) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(
                Duration::from_millis(SOURCE_SEND_TIMEOUT_MS),
                sink.tx.send(timed_event),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    sink.source_queue_backpressure.record_drop(1);
                    record_gap(&sink.gaps, format!("bounded_queue_send_failed:{err}"));
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    sink.source_queue_backpressure.record_drop(1);
                    record_gap(
                        &sink.gaps,
                        format!("bounded_queue_backpressure_timeout:{source_kind}"),
                    );
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            sink.source_queue_backpressure.record_drop(1);
            record_gap(
                &sink.gaps,
                format!("bounded_queue_send_closed:{source_kind}"),
            );
        }
    }
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

#[allow(clippy::too_many_arguments)]
fn write_control_manifest(
    dataset_root: &Path,
    artifact_dir: &Path,
    config: &LiveTrainingRuntimeConfig,
    verdict: &str,
    source_gaps: Vec<String>,
    source_coverage: Vec<SourceCoverage>,
    finalized_segments: Vec<LiveFinalizedSegment>,
    source_queue_summary: QueueDurationSummary,
    writer_queue_summary: QueueDurationSummary,
    rollover_evidence: Vec<RolloverEvidenceReceipt>,
    source_writer_stats: Vec<SourceWriterStats>,
    shutdown_stage_timings: BTreeMap<String, ShutdownStageTiming>,
    shutdown_retention_scan_budget_ms: u64,
) -> Result<LiveTrainingRuntimeReceipt> {
    let run_manifest_dir = dataset_root.join("training").join("run_manifests");
    fs::create_dir_all(&run_manifest_dir)?;
    let run_manifest_path = run_manifest_dir.join(format!("{}.manifest.json", config.run_id));
    let resource_watermark =
        runtime_resource_watermark(artifact_dir, &source_queue_summary, &writer_queue_summary);
    let manual_review = config
        .stop_after_complete_polymarket_market
        .map(|target_market_count| {
            json!({
                "target_market_count": target_market_count,
                "selected_market_slug": null,
                "selected_condition_id": null,
                "subscribed_asset_ids_for_market": [],
                "planned_start_utc": null,
                "planned_end_utc": null,
                "preload_start_utc": null,
                "stop_reason": "manual_review_complete_market_shutdown"
            })
        });
    let mode = if manual_review.is_some() {
        "manual_review_complete_market"
    } else {
        "live"
    };
    let retention_policy_receipt_path = config
        .retention_policy_receipt_path
        .clone()
        .unwrap_or_else(|| run_artifact_path(&config.run_id, "retention_policy_receipt.json"));
    let mut shutdown_stage_timings =
        complete_shutdown_stage_timings(shutdown_stage_timings, "not reached");
    shutdown_stage_timings.insert(
        "run_manifest_atomic_write".to_owned(),
        predeclared_manifest_atomic_write_stage(&run_manifest_path),
    );
    let final_manifest = json!({
        "run_id": config.run_id,
        "mode": mode,
        "manual_review_complete_market": manual_review,
        "retention_policy_receipt_path": retention_policy_receipt_path,
        "verdict": verdict,
        "dataset_root": dataset_root,
        "artifact_dir": artifact_dir,
        "collector_host_id": config.collector_host_id,
        "collector_region": config.collector_region,
        "source_coverage": source_coverage,
        "source_gaps": source_gaps,
        "rollover_edges": rollover_evidence,
        "finalized_segments": finalized_segments,
        "source_writer_stats": source_writer_stats,
        "queue_summary": {
            "source_event_queue": source_queue_summary,
            "writer_chunk_queue": writer_queue_summary
        },
        "shutdown_retention_scan_budget_ms": shutdown_retention_scan_budget_ms,
        "shutdown_stage_timings": shutdown_stage_timings,
        "resource_watermark": resource_watermark,
        "writer_used_blocking_pool": true,
        "recv_time_utc_ms": now_ms()
    });
    write_manifest_atomic(
        &run_manifest_path,
        &serde_json::to_vec_pretty(&final_manifest)?,
    )?;
    Ok(LiveTrainingRuntimeReceipt {
        run_id: config.run_id.clone(),
        run_manifest_path,
        finalized_segments,
        source_coverage,
        source_gaps,
        resource_watermark_recorded: true,
        writer_used_blocking_pool: true,
        verdict: verdict.to_owned(),
    })
}

fn runtime_resource_watermark(
    artifact_dir: &Path,
    source_queue_summary: &QueueDurationSummary,
    writer_queue_summary: &QueueDurationSummary,
) -> Value {
    let mut resource = serde_json::Map::new();
    if let Some(rss_bytes) = read_rss_bytes() {
        resource.insert("rss_bytes".to_owned(), json!(rss_bytes));
    }
    if let Ok(artifact_dir_bytes) = dir_size_bytes(artifact_dir) {
        resource.insert("artifact_dir_bytes".to_owned(), json!(artifact_dir_bytes));
    }
    resource.insert(
        "queue_rows_high".to_owned(),
        json!(
            source_queue_summary
                .rows_high
                .max(writer_queue_summary.rows_high)
        ),
    );
    resource.insert(
        "queue_bytes_high".to_owned(),
        json!(
            source_queue_summary
                .bytes_high
                .max(writer_queue_summary.bytes_high)
        ),
    );
    Value::Object(resource)
}

fn build_rollover_receipts(
    edges: &[RolloverEdge],
    coverage: &BTreeMap<String, CoverageStats>,
) -> Vec<RolloverEvidenceReceipt> {
    let market_coverage = coverage.get("polymarket_market");
    edges
        .iter()
        .map(|edge| {
            let new_current_first_recv_time_utc_ms = market_coverage
                .and_then(|stats| {
                    stats
                        .market_window_first_recv_time_utc_ms
                        .get(&edge.new_current_slug)
                })
                .copied();
            let previous_next_first_recv_time_utc_ms = market_coverage
                .and_then(|stats| {
                    stats
                        .market_window_first_recv_time_utc_ms
                        .get(&edge.previous_next_slug)
                })
                .copied();
            RolloverEvidenceReceipt {
                edge_recv_time_utc_ms: edge.edge_recv_time_utc_ms,
                previous_current_slug: edge.previous_current_slug.clone(),
                previous_next_slug: edge.previous_next_slug.clone(),
                new_current_slug: edge.new_current_slug.clone(),
                new_next_slug: edge.new_next_slug.clone(),
                prefetched_promoted_to_current: edge.previous_next_slug == edge.new_current_slug,
                previous_current_retired_by_time: edge.edge_recv_time_utc_ms
                    >= edge.previous_current_end_ts_s * 1000,
                windows_contiguous: edge.previous_current_end_ts_s == edge.previous_next_start_ts_s
                    && edge.previous_next_start_ts_s == edge.new_current_start_ts_s,
                new_current_first_recv_time_utc_ms,
                previous_next_first_recv_time_utc_ms,
            }
        })
        .collect()
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

fn pair_asset_ids(pair: &BtcUpdownDiscoveryPair) -> Vec<String> {
    vec![
        pair.current.up_asset_id.clone(),
        pair.current.down_asset_id.clone(),
        pair.next.up_asset_id.clone(),
        pair.next.down_asset_id.clone(),
    ]
}

fn pair_asset_to_slug(pair: &BtcUpdownDiscoveryPair) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(pair.current.up_asset_id.clone(), pair.current.slug.clone());
    map.insert(
        pair.current.down_asset_id.clone(),
        pair.current.slug.clone(),
    );
    map.insert(pair.next.up_asset_id.clone(), pair.next.slug.clone());
    map.insert(pair.next.down_asset_id.clone(), pair.next.slug.clone());
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

fn record_gap(gaps: &Arc<Mutex<Vec<String>>>, gap: String) {
    if !is_live_runtime_source_gap_reason(&gap) {
        return;
    }
    let mut gaps = gaps.lock().expect("source gap mutex poisoned");
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
        gaps.push(format!("{SOURCE_GAP_OVERFLOW_PREFIX}1"));
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

fn empty_queue_summary(queue_name: impl Into<String>) -> QueueDurationSummary {
    QueueDurationSummary {
        queue_name: queue_name.into(),
        rows_high: 0,
        bytes_high: 0,
        non_empty_ms: 0,
        over_10_rows_ms: 0,
        over_100_rows_ms: 0,
        over_1000_rows_ms: 0,
        longest_contiguous_over_10_rows_ms: 0,
        longest_contiguous_over_100_rows_ms: 0,
        longest_contiguous_over_1000_rows_ms: 0,
        backpressure_wait_count: 0,
        backpressure_timeout_count: 0,
        dropped_rows: 0,
    }
}

fn read_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(rss_pages * 4096)
}

fn dir_size_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(dir_size_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
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
    freshness_timeout_secs: u64,
}

enum BinanceDepthUpdateExit {
    DeadlineReached,
    StreamEnded(String),
    ResyncFailed,
}

struct BinanceDepthUpdateContext {
    http_client: reqwest::Client,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Instant,
    run_started: Instant,
    snapshot_last_update_id: u64,
    snapshot_utc_day: String,
    pending_snapshot_trigger_reason: Option<String>,
}

struct SourceWriterHandle {
    tx: std::sync::mpsc::SyncSender<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    result_rx: std::sync::mpsc::Receiver<Result<SourceWriterOutput>>,
}

enum WriterCommand {
    Events(Vec<RawEvent>),
}

#[derive(Clone, Debug, Serialize)]
struct SourceWriterOutput {
    source_kind: String,
    finalized_segments: Vec<LiveFinalizedSegment>,
    stats: SourceWriterStats,
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
    naming: HourlyPartition,
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
            naming,
            writer,
            hour_part,
            segment_start_recv_time_utc_ms: 0,
            last_recv_time_utc_ms: 0,
            source_ts_start: None,
            source_ts_end: None,
            rows: 0,
            bytes: 0,
            event_type_counts: BTreeMap::new(),
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
            if let Some(local_u_after) = value.get("local_u_after").and_then(Value::as_u64) {
                self.depth_stitch_applied_rows =
                    self.depth_stitch_applied_rows.saturating_add(typed_rows);
                self.depth_local_u_min = Some(
                    self.depth_local_u_min
                        .map_or(local_u_after, |current| current.min(local_u_after)),
                );
                self.depth_local_u_max = Some(
                    self.depth_local_u_max
                        .map_or(local_u_after, |current| current.max(local_u_after)),
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

    fn finalize(self) -> Result<PublishedSegment> {
        match self {
            Self::Typed(writer) => writer.finalize(),
        }
    }
}

fn typed_schema_version(source_kind: &str) -> &'static str {
    match source_kind {
        "polymarket_market" => "polymarket_orderbook_v1",
        "binance_depth_snapshot" => "binance_depth_snapshot_v1",
        "binance_depth_update" => "binance_depth_update_v1",
        "binance_aggtrade" => "binance_aggtrade_v1",
        "polymarket_rtds_chainlink_btc_usd" => "polymarket_rtds_chainlink_btc_usd_v1",
        _ => "unknown_typed_layout",
    }
}

fn write_typed_live_event(writer: &mut TypedParquetSegmentWriter, event: &RawEvent) -> Result<u64> {
    match event.source_kind.as_str() {
        "polymarket_market" => {
            let rows = polymarket_orderbook_rows_from_event(event)?;
            let written = rows.len() as u64;
            writer.write_polymarket_orderbook_rows(&rows)?;
            Ok(written)
        }
        "binance_depth_snapshot" => {
            let row = binance_depth_snapshot_row_from_event(event)?;
            writer.write_binance_depth_snapshot_rows(&[row])?;
            Ok(1)
        }
        "binance_depth_update" => {
            let row = binance_depth_update_row_from_event(event)?;
            writer.write_binance_depth_update_rows(&[row])?;
            Ok(1)
        }
        "binance_aggtrade" => {
            let row = binance_aggtrade_row_from_event(event)?;
            writer.write_binance_aggtrade_rows(&[row])?;
            Ok(1)
        }
        "polymarket_rtds_chainlink_btc_usd" => {
            let row = rtds_row_from_event(event)?;
            writer.write_rtds_chainlink_btc_usd_rows(&[row])?;
            Ok(1)
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported live typed source kind: {other}"),
        )
        .into()),
    }
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
        "market_discovery_switch" => ["current", "next"]
            .iter()
            .filter_map(|field| value.get(field).map(|candidate| (*field, candidate)))
            .map(|(_, candidate)| {
                let mut row = base_polymarket_row(
                    event,
                    candidate,
                    "new_market",
                    required_value_string(candidate, "condition_id")?,
                    None,
                    event
                        .source_ts
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                );
                row.slug = optional_string(candidate, "slug");
                let asset_ids = [
                    required_value_string(candidate, "up_asset_id")?,
                    required_value_string(candidate, "down_asset_id")?,
                ];
                row.subscribed_asset_ids_for_market = Some(serde_json::to_string(&asset_ids)?);
                Ok(row)
            })
            .collect::<Result<Vec<_>>>()?,
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
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "polymarket_orderbook_v1".to_owned(),
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
    let raw = parse_depth_snapshot_raw(
        event_envelope(event, SourceKind::BinanceDepthSnapshot),
        &event.raw_json,
        event.recv_time_utc_ms,
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
    raw.local_u = value.get("local_u_after").and_then(Value::as_u64);
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
    let raw = parse_aggtrade_raw(
        event_envelope(event, SourceKind::BinanceAggTrade),
        &event.raw_json,
    )
    .map_err(invalid_data)?;
    Ok(BinanceAggTradeHourlyRow::from_raw(&raw))
}

fn rtds_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let raw = RtdsChainlinkBtcUsdRaw {
        envelope: event_envelope(event, SourceKind::PolymarketRtdsChainlinkBtcUsd),
        message_timestamp_ms: required_i64(&value, "message_timestamp")?,
        payload_timestamp_ms: value
            .get("payload_timestamp")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| {
                value
                    .get("message_timestamp")
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
            }),
        symbol: required_value_string(&value, "symbol")?,
        value: required_value_string(&value, "value")?,
        source_topic: required_value_string(&value, "topic")?,
        subscription_raw_json: value
            .get("subscription_raw_json")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(rtds_subscription_evidence_json),
        ack_or_first_message_evidence: Some("first_chainlink_price_message".to_owned()),
        close_or_error_evidence: None,
    };
    Ok(RtdsChainlinkBtcUsdHourlyRow::from_raw(&raw))
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

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    optional_i64(value, field).ok_or_else(|| invalid_data(format!("{field} missing")).into())
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

fn live_segment_rollover_reason(
    segment: &ActiveSegment,
    current_utc_hour: &str,
) -> Option<&'static str> {
    if segment.utc_hour != current_utc_hour {
        return Some("utc_hour_boundary");
    }
    if DEFAULT_MAX_SEGMENT_ROWS > 0 && segment.rows >= DEFAULT_MAX_SEGMENT_ROWS {
        return Some("max_rows");
    }
    if DEFAULT_MAX_SEGMENT_BYTES > 0 && segment.bytes >= DEFAULT_MAX_SEGMENT_BYTES {
        return Some("max_bytes");
    }
    None
}

fn is_protective_part_rollover(reason: &str) -> bool {
    matches!(reason, "max_rows" | "max_bytes")
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

const SHUTDOWN_STAGE_NAMES: &[&str] = &[
    "source_task_stop",
    "event_queue_drain",
    "flush_batches",
    "writer_finalize",
    "retention_shutdown_scan",
    "run_manifest_atomic_write",
    "post_manifest_cleanup",
];

#[derive(Clone, Debug, Serialize)]
struct ShutdownStageTiming {
    started_utc_ms: i64,
    ended_utc_ms: i64,
    duration_ms: u64,
    status: String,
    error: Option<String>,
    manifest_path: Option<PathBuf>,
}

fn record_shutdown_stage(
    stages: &mut BTreeMap<String, ShutdownStageTiming>,
    name: &str,
    started_utc_ms: i64,
    started: Instant,
    status: &str,
    error: Option<String>,
) {
    stages.insert(
        name.to_owned(),
        ShutdownStageTiming {
            started_utc_ms,
            ended_utc_ms: now_ms(),
            duration_ms: started.elapsed().as_millis() as u64,
            status: status.to_owned(),
            error,
            manifest_path: None,
        },
    );
}

fn record_instant_shutdown_stage(
    stages: &mut BTreeMap<String, ShutdownStageTiming>,
    name: &str,
    status: &str,
    error: Option<String>,
) {
    let now = now_ms();
    stages.insert(
        name.to_owned(),
        ShutdownStageTiming {
            started_utc_ms: now,
            ended_utc_ms: now,
            duration_ms: 0,
            status: status.to_owned(),
            error,
            manifest_path: None,
        },
    );
}

fn default_shutdown_stage_timings(reason: &str) -> BTreeMap<String, ShutdownStageTiming> {
    let mut stages = BTreeMap::new();
    for name in SHUTDOWN_STAGE_NAMES {
        record_instant_shutdown_stage(&mut stages, name, "skipped", Some(reason.to_owned()));
    }
    stages
}

fn complete_shutdown_stage_timings(
    mut stages: BTreeMap<String, ShutdownStageTiming>,
    missing_reason: &str,
) -> BTreeMap<String, ShutdownStageTiming> {
    for name in SHUTDOWN_STAGE_NAMES {
        if !stages.contains_key(*name) {
            record_instant_shutdown_stage(
                &mut stages,
                name,
                "skipped",
                Some(missing_reason.to_owned()),
            );
        }
    }
    stages
}

#[derive(Clone, Serialize)]
struct RolloverEdge {
    edge_recv_time_utc_ms: i64,
    previous_current_slug: String,
    previous_next_slug: String,
    new_current_slug: String,
    new_next_slug: String,
    previous_current_end_ts_s: i64,
    previous_next_start_ts_s: i64,
    new_current_start_ts_s: i64,
}

impl RolloverEdge {
    fn from_pairs(previous: &BtcUpdownDiscoveryPair, next: &BtcUpdownDiscoveryPair) -> Self {
        Self {
            edge_recv_time_utc_ms: now_ms(),
            previous_current_slug: previous.current.slug.clone(),
            previous_next_slug: previous.next.slug.clone(),
            new_current_slug: next.current.slug.clone(),
            new_next_slug: next.next.slug.clone(),
            previous_current_end_ts_s: previous.current.end_ts_s,
            previous_next_start_ts_s: previous.next.start_ts_s,
            new_current_start_ts_s: next.current.start_ts_s,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct RolloverEvidenceReceipt {
    edge_recv_time_utc_ms: i64,
    previous_current_slug: String,
    previous_next_slug: String,
    new_current_slug: String,
    new_next_slug: String,
    prefetched_promoted_to_current: bool,
    previous_current_retired_by_time: bool,
    windows_contiguous: bool,
    new_current_first_recv_time_utc_ms: Option<i64>,
    previous_next_first_recv_time_utc_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publish::discover_finalized_artifacts;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use polymarket_client_sdk_v2::clob::ws::WsMessage;
    use std::fs::File;
    use tempfile::tempdir;

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

    #[test]
    fn source_gaps_are_deduped_and_bounded() {
        let gaps = Arc::new(Mutex::new(Vec::new()));
        record_gap(&gaps, "same_gap".to_owned());
        record_gap(&gaps, "same_gap".to_owned());
        for index in 0..(MAX_RECORDED_SOURCE_GAPS + 5) {
            record_gap(&gaps, format!("unique_gap:{index}"));
        }

        let recorded = gaps.lock().unwrap();
        assert_eq!(recorded.iter().filter(|gap| *gap == "same_gap").count(), 1);
        assert_eq!(recorded.len(), MAX_RECORDED_SOURCE_GAPS + 1);
        assert!(
            recorded
                .last()
                .is_some_and(|gap| gap.starts_with(SOURCE_GAP_OVERFLOW_PREFIX))
        );
    }

    #[test]
    fn rtds_freshness_schedule_resubscribes_every_10s_reconnects_every_2_and_gaps_at_60() {
        let observed = rtds_freshness_actions()
            .iter()
            .map(|action| {
                (
                    action.elapsed.as_secs(),
                    action.resubscribe_attempt,
                    action.reconnect_after_resubscribe,
                    action.source_gap_after_resubscribe,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                (10, 1, false, false),
                (20, 2, true, false),
                (30, 3, false, false),
                (40, 4, true, false),
                (50, 5, false, false),
                (60, 6, true, true)
            ]
        );
    }

    #[test]
    fn rtds_reconnect_action_does_not_resubscribe_old_client_before_new_client() {
        let observed = rtds_freshness_actions()
            .iter()
            .map(|action| {
                let step = rtds_timeout_recovery_step(*action);
                (
                    action.elapsed.as_secs(),
                    step.unsubscribe_current_client,
                    step.resubscribe_current_client,
                    step.reconnect_with_new_client,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                (10, true, true, false),
                (20, true, false, true),
                (30, true, true, false),
                (40, true, false, true),
                (50, true, true, false),
                (60, true, false, true)
            ]
        );
    }

    #[test]
    fn live_segment_manifest_is_discoverable_as_complete() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming.clone(), "binance_aggtrade", 0).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);

        assert_eq!(segment.writer.write_event(&event).unwrap(), 1);
        segment.observe(&event, 1);
        let finalized = finalize_segment(
            segment,
            "shutdown_finalize",
            &test_runtime_provenance("run"),
        )
        .unwrap();

        let discovered = discover_finalized_artifacts(&naming.partition_path).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].parquet_path, finalized.parquet_path);
        assert_eq!(discovered[0].manifest_path, finalized.manifest_path);
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(finalized.manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["utc_day"], "2026-07-07");
        assert_eq!(manifest["utc_hour"], "2026-07-07T02");
        assert_eq!(
            manifest["hour_window_utc_ms"]["start"],
            1_783_389_600_000i64
        );
        assert_eq!(manifest["hour_window_utc_ms"]["end"], 1_783_393_200_000i64);
        assert_eq!(manifest["hour_part"], 0);
        assert_eq!(manifest["hour_part_count"], 1);
        assert_eq!(
            manifest["layout_family"],
            "source_specific_hourly_typed_raw"
        );
        assert_eq!(manifest["sort_order"], "receive_order");
        assert_eq!(manifest["compression"], "zstd_level_3");
        assert_eq!(manifest["required_typed_fields_present"], true);
        assert_eq!(manifest["aggtrade_rows"], 1);
        assert_eq!(
            manifest["retention_policy_receipt_path"],
            "training/runtime_artifacts/run/retention_policy_receipt.json"
        );
        assert!(manifest["deletion_receipt_path"].is_null());
        assert!(manifest["health_artifact_path"].is_null());
        assert!(manifest["gap_artifact_path"].is_null());
        assert!(manifest["reconnect_artifact_path"].is_null());
        assert!(
            !manifest.to_string().contains("source_coverage.json")
                && !manifest.to_string().contains("source_gaps.json")
                && !manifest.to_string().contains("rollover_edges.json"),
            "segment manifest must not reference deleted runtime sidecar JSON"
        );
        assert!(
            manifest["resource_watermark"]["unavailable"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(
            !manifest.to_string().contains("not measured"),
            "segment manifest must not contain noisy not-measured placeholders"
        );
        let draft: ManifestDraft = serde_json::from_value(manifest).unwrap();
        assert_eq!(draft.effective_verdict(), ValidationVerdict::Complete);

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
    fn live_runtime_rolls_segment_on_max_bytes() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = HourlyPartitionPolicy::new(&dataset_root)
            .unwrap()
            .partition("binance_aggtrade", 1_783_392_404_599, None)
            .unwrap();
        let mut segment = ActiveSegment::new(naming, "binance_aggtrade", 0).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        segment.observe(&event, 1);
        segment.bytes = DEFAULT_MAX_SEGMENT_BYTES;

        assert_eq!(
            live_segment_rollover_reason(&segment, "2026-07-07T02"),
            Some("max_bytes")
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
        fs::write(&base.manifest_path, b"existing manifest").unwrap();

        let (next, hour_part) =
            next_available_hourly_partition(&policy, "binance_aggtrade", 1_783_392_404_599, None)
                .unwrap();

        assert_eq!(hour_part, 1);
        assert!(next.final_path.ends_with(
            "training/typed_raw/utc_day=2026-07-07/source_kind=binance_aggtrade/binance_aggtrade_2026-07-07T02--part=1.parquet"
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
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_aggtrade".to_owned(),
                rx,
                queue_rows_for_worker,
            )
        });

        let events = vec![
            test_raw_event("binance_aggtrade", 1_783_392_404_599),
            test_raw_event("binance_aggtrade", 1_783_392_404_600),
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

        let error = stop_writer_workers_with_budget(writers, Duration::from_millis(10))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("writer finalize timeout"),
            "expected timeout error, got {error}"
        );
    }

    #[tokio::test]
    async fn binance_depth_reconnect_records_source_gap_and_snapshot_trigger() {
        let (tx, _rx) = mpsc::channel(1);
        let gaps = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
        };
        let mut reconnect_attempts = 0_u32;
        let mut snapshot_trigger_reason = "bootstrap".to_owned();

        assert!(
            prepare_binance_depth_reconnect(
                &sink,
                &mut reconnect_attempts,
                &mut snapshot_trigger_reason,
                "stream_closed".to_owned(),
            )
            .await
        );

        assert_eq!(reconnect_attempts, 1);
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
        let gaps = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
        };
        let mut reconnect_attempts = 0_u32;
        let mut snapshot_trigger_reason = "bootstrap".to_owned();

        assert!(
            handle_binance_depth_ws_connect_failure(
                &sink,
                &mut reconnect_attempts,
                &mut snapshot_trigger_reason,
                "connect_timeout".to_owned(),
                "binance_depth_update_ws_connect_timeout".to_owned(),
            )
            .await
        );

        assert_eq!(reconnect_attempts, 1);
        assert_eq!(snapshot_trigger_reason, "reconnect:connect_timeout");
        let recorded = gaps.lock().unwrap();
        assert!(recorded.contains(&"binance_depth_update_ws_connect_timeout".to_owned()));
        assert!(recorded.contains(&"binance_depth_reconnect_required:connect_timeout".to_owned()));
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
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "polymarket_market".to_owned(),
                rx,
                queue_rows_for_worker,
            )
        });

        let events = vec![test_polymarket_price_change_event(1_783_392_404_600)];
        queue_rows.store(events.len() as u64, Ordering::Relaxed);
        tx.send(WriterCommand::Events(events)).unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        assert_eq!(output.stats.events_received, 1);
        assert_eq!(output.stats.events_written, 1);
        assert_eq!(output.stats.typed_rows_written, 2);
        assert_eq!(output.stats.finalized_segment_rows, 2);

        let manifest_path = &output.finalized_segments[0].manifest_path;
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["row_count"], 2);
        assert_eq!(manifest["source_coverage"][0]["rows"], 2);
        assert_eq!(manifest["price_change_rows"], 2);
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
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_depth_update".to_owned(),
                rx,
                queue_rows_for_worker,
            )
        });

        let event = test_binance_depth_update_event(1_783_392_404_600);
        queue_rows.store(1, Ordering::Relaxed);
        tx.send(WriterCommand::Events(vec![event])).unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        assert_eq!(output.stats.typed_rows_written, 1);
        let manifest_path = &output.finalized_segments[0].manifest_path;
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["depth_update_rows"], 1);
        assert_eq!(
            manifest["binance_depth_stitch"]["snapshot_last_update_id"],
            100
        );
        assert_eq!(manifest["binance_depth_stitch"]["stitch_applied_rows"], 1);
        assert_eq!(manifest["binance_depth_stitch"]["gap_rows"], 0);
        assert_eq!(manifest["binance_depth_stitch"]["resnapshot_count"], 0);
        assert_eq!(manifest["binance_depth_stitch"]["local_u_min"], 103);
        assert_eq!(manifest["binance_depth_stitch"]["local_u_max"], 103);
        assert_eq!(
            manifest["binance_depth_stitch"]["first_update_bridged_snapshot"],
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
        let handle = std::thread::spawn(move || {
            source_writer_worker(
                dataset_root_for_worker,
                provenance,
                "binance_depth_update".to_owned(),
                rx,
                queue_rows_for_worker,
            )
        });

        let event = test_binance_depth_update_event_with_trigger(
            1_783_392_404_600,
            "stitch_gap:U > local_u + 1",
        );
        queue_rows.store(1, Ordering::Relaxed);
        tx.send(WriterCommand::Events(vec![event])).unwrap();
        drop(tx);

        let output = handle.join().unwrap().unwrap();
        let manifest_path = &output.finalized_segments[0].manifest_path;
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["binance_depth_stitch"]["stitch_applied_rows"], 1);
        assert_eq!(manifest["binance_depth_stitch"]["gap_rows"], 1);
        assert_eq!(manifest["binance_depth_stitch"]["resnapshot_count"], 1);
        let draft: ManifestDraft = serde_json::from_value(manifest).unwrap();
        assert_eq!(draft.effective_verdict(), ValidationVerdict::Incomplete);
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

    #[tokio::test]
    async fn writer_side_batch_loss_records_dropped_rows() {
        let mut writers = BTreeMap::new();
        let gaps = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
        let mut batch = vec![test_raw_event("binance_aggtrade", 1_783_392_404_599)];

        flush_source_batch(
            &mut writers,
            "binance_aggtrade",
            &mut batch,
            &gaps,
            &mut tracker,
            Instant::now(),
        )
        .await
        .unwrap();

        let summary = tracker.finish(0);
        assert_eq!(summary.dropped_rows, 1);
        assert!(
            gaps.lock()
                .unwrap()
                .iter()
                .any(|gap| gap == "writer_missing_for_source:binance_aggtrade")
        );
    }

    fn test_raw_event(source_kind: &str, recv_time_utc_ms: i64) -> RawEvent {
        RawEvent {
            source_kind: source_kind.to_owned(),
            source_event_type: "aggTrade".to_owned(),
            source_identifier: "BTCUSDT".to_owned(),
            source_ts: Some(recv_time_utc_ms.to_string()),
            recv_time_utc_ms,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: BINANCE_AGGTRADE_WS.to_owned(),
            raw_json: json!({
                "e": "aggTrade",
                "E": recv_time_utc_ms,
                "s": "BTCUSDT",
                "a": 123456_u64,
                "p": "65000.10",
                "q": "0.001",
                "f": 123450_u64,
                "l": 123456_u64,
                "T": recv_time_utc_ms,
                "m": true
            })
            .to_string(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: source_kind.to_owned(),
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
            "local_u_after": 103_u64,
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

    fn test_runtime_provenance(run_id: &str) -> RuntimeProvenance {
        RuntimeProvenance {
            run_id: run_id.to_owned(),
            code_version: "test-code-version".to_owned(),
            collector_binary_sha256: "test-binary-sha256".to_owned(),
            source_config_digest: "test-source-config-digest".to_owned(),
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
        }
    }
}
