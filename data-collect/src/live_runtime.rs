use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::{ChannelType, Client as MarketWsClient};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::rtds::{Client as RtdsClient, Subscription};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::ValidationVerdict;
use crate::backpressure::{BackpressureReceipt, bounded_event_queue};
use crate::latency::{LatencyStats, QueueDurationSummary, QueueDurationTracker, TimedRawEvent};
use crate::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use crate::market_discovery::{
    BtcUpdownDiscoveryPair, BtcUpdownMarketCandidate, discovery_windows, select_btc_updown_pair,
};
use crate::resource::{
    MetricUnavailable, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds, ResourceWatermark,
};
use crate::segment::{
    RolloverReason, SegmentNaming, SegmentPolicy, SegmentState, TrainingAvailabilityPolicy,
};
use crate::writer::{ParquetSegmentWriter, PublishedSegment, RawEvent};

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
const SOURCE_SEND_TIMEOUT_MS: u64 = 2_000;
const BINANCE_HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const BINANCE_HTTP_TOTAL_TIMEOUT_SECS: u64 = 15;
const WS_CONNECT_TIMEOUT_SECS: u64 = 20;
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
    let mut queue_samples = Vec::<QueueSampleReceipt>::new();
    let mut next_flush = run_started + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
    let mut next_queue_sample = run_started;
    let mut average_event_bytes = 512_u64;
    let mut received_rows = 0_u64;
    let mut controlled_shutdown = false;
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
            queue_samples.push(QueueSampleReceipt {
                sample_time_utc_ms: now_ms(),
                sample_monotonic_ms: monotonic_ms(run_started),
                source_event_queue: source_queue_tracker.snapshot(monotonic_ms(run_started)),
                writer_chunk_queue: writer_queue_tracker.snapshot(monotonic_ms(run_started)),
            });
            next_queue_sample += Duration::from_secs(1);
        }
    }

    if controlled_shutdown {
        for handle in &handles {
            handle.abort();
        }
    }
    for handle in handles {
        match timeout(Duration::from_secs(15), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => record_gap(&source_gaps, format!("source_task_error:{err}")),
            Ok(Err(err)) if err.is_cancelled() && controlled_shutdown => {}
            Ok(Err(err)) => record_gap(&source_gaps, format!("source_task_join_error:{err}")),
            Err(_) => record_gap(&source_gaps, "source_task_join_timeout".to_owned()),
        }
    }

    while let Ok(timed_event) = event_rx.try_recv() {
        let event = timed_event.event;
        update_coverage(&mut coverage, &event);
        let source_kind = event.source_kind.clone();
        batches.entry(source_kind).or_default().push(event);
    }
    flush_all_batches(
        &mut writer_handles,
        &mut batches,
        &source_gaps,
        &mut writer_queue_tracker,
        run_started,
    )
    .await?;

    let writer_outputs = stop_writer_workers(writer_handles).await?;
    let finalized_segments = writer_outputs
        .iter()
        .flat_map(|output| output.finalized_segments.clone())
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
    write_runtime_artifacts(
        &artifact_dir,
        &coverage,
        &gaps,
        &rollover_evidence,
        &queue_samples,
        &writer_outputs,
        &source_queue_summary,
        &writer_queue_summary,
    )?;
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
        let handle = tokio::task::spawn_blocking(move || {
            source_writer_worker(
                dataset_root,
                provenance,
                source_kind_owned,
                rx,
                queue_rows_for_worker,
            )
        });
        handles.insert(
            (*source_kind).to_owned(),
            SourceWriterHandle {
                tx,
                queue_rows,
                handle,
            },
        );
    }
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
    let mut outputs = Vec::new();
    for (source_kind, writer) in writers {
        drop(writer.tx);
        let output = writer.handle.await.map_err(|err| {
            io::Error::other(format!("{source_kind} writer join failed: {err}"))
        })??;
        outputs.push(output);
    }
    Ok(outputs)
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
    let mut write_receipts = Vec::new();
    let mut segment_seq = 0_u64;
    while let Ok(command) = rx.recv() {
        match command {
            WriterCommand::Events(events) => {
                queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                for event in events {
                    let current_day = TrainingAvailabilityPolicy::event_availability(
                        &dataset_root,
                        &source_kind,
                        event.recv_time_utc_ms,
                        parse_source_ts_ms(&event),
                    )?
                    .utc_day;
                    if let Some(reason) = active
                        .as_ref()
                        .and_then(|segment| live_segment_rollover_reason(segment, &current_day))
                    {
                        let segment = active.take().expect("active segment exists");
                        finalized_segments.push(finalize_segment(segment, reason, &provenance)?);
                    }
                    if active.is_none() {
                        let naming = SegmentNaming::new(
                            &dataset_root,
                            &source_kind,
                            event.recv_time_utc_ms,
                            segment_seq,
                            &provenance.run_id,
                        )?;
                        segment_seq = segment_seq.saturating_add(1);
                        active = Some(ActiveSegment::new(naming, &source_kind, current_day)?);
                    }
                    let segment = active.as_mut().expect("active segment exists");
                    let write_started = Instant::now();
                    segment.writer.write_events(std::slice::from_ref(&event))?;
                    let write_events_us = write_started.elapsed().as_micros() as u64;
                    segment.observe(&event);
                    write_receipts.push(LiveWriteReceipt {
                        source_kind: source_kind.clone(),
                        rows: 1,
                        recv_time_utc_ms: event.recv_time_utc_ms,
                        write_done_time_utc_ms: now_ms(),
                        write_events_us,
                    });
                }
            }
        }
    }
    if let Some(segment) = active.take() {
        finalized_segments.push(finalize_segment(segment, "shutdown_finalize", &provenance)?);
    }
    Ok(SourceWriterOutput {
        source_kind,
        finalized_segments,
        write_receipts,
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
    let payload = segment_manifest_payload(&manifest, &manifest_context, rollover_reason)?;
    write_manifest_atomic(&manifest_path, &payload)?;
    Ok(LiveFinalizedSegment {
        source_kind,
        parquet_path: published.final_path,
        manifest_path,
        row_count: published.row_count,
        sha256: published.sha256,
    })
}

#[derive(Clone, Debug)]
struct SegmentManifestContext {
    source_kind: String,
    segment_seq: u64,
    segment_start_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    bytes: u64,
}

impl SegmentManifestContext {
    fn from_segment(segment: &ActiveSegment) -> Self {
        Self {
            source_kind: segment.source_kind.clone(),
            segment_seq: segment.segment_seq,
            segment_start_recv_time_utc_ms: segment.segment_start_recv_time_utc_ms,
            last_recv_time_utc_ms: segment.last_recv_time_utc_ms,
            source_ts_start: segment.source_ts_start.clone(),
            source_ts_end: segment.source_ts_end.clone(),
            rows: segment.rows,
            bytes: segment.bytes,
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
        source_gaps: Vec::new(),
        manifest_source_gaps: Vec::new(),
        coverage_incomplete_reason: None,
        health_artifact_path: Some(run_artifact_path(
            &provenance.run_id,
            "source_coverage.json",
        )),
        health_artifact_digest: None,
        gap_artifact_path: Some(run_artifact_path(&provenance.run_id, "source_gaps.json")),
        gap_artifact_digest: None,
        reconnect_artifact_path: Some(run_artifact_path(&provenance.run_id, "source_gaps.json")),
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
        object.insert("segment_seq".to_owned(), json!(segment.segment_seq));
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
        unavailable: segment_manifest_unavailable_metrics(read_rss_bytes().is_none()),
    }
}

fn segment_manifest_unavailable_metrics(rss_unavailable: bool) -> Vec<MetricUnavailable> {
    let mut metrics = Vec::new();
    if rss_unavailable {
        metrics.push(metric_unavailable("rss_bytes"));
    }
    metrics.extend([
        metric_unavailable("cpu_percent"),
        metric_unavailable("disk_free_bytes"),
        metric_unavailable("journal_log_bytes"),
        metric_unavailable("journal_limit_status"),
        metric_unavailable("open_files"),
        metric_unavailable("cgroup_memory_current_bytes"),
        metric_unavailable("cgroup_memory_stat"),
        metric_unavailable("cgroup_memory_events"),
        metric_unavailable("psi_cpu"),
        metric_unavailable("psi_memory"),
        metric_unavailable("psi_io"),
    ]);
    metrics
}

fn metric_unavailable(metric: &str) -> MetricUnavailable {
    MetricUnavailable {
        metric: metric.to_owned(),
        reason: "not measured by phase7 segment manifest runtime".to_owned(),
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
    let mut book_stream = Box::pin(client.subscribe_orderbook(assets)?);
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
                            json!({
                                "event_type": "book",
                                "market": event.market.to_string(),
                                "asset_id": asset_id,
                                "slug": slug,
                                "timestamp": event.timestamp,
                                "bids_len": event.bids.len(),
                                "asks_len": event.asks.len(),
                                "hash": event.hash
                            }),
                        )).await;
                    }
                    Some(Err(err)) => record_gap(&sink.gaps, format!("polymarket_book_stream_error:{err}")),
                    None => {
                        record_gap(&sink.gaps, "polymarket_book_stream_closed".to_owned());
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
    let client = RtdsClient::new(RTDS_WS_ENDPOINT, bounded_ws_config())?;
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let mut stream = Box::pin(client.subscribe_raw(subscription)?);
    wait_rtds_connected(&client, Duration::from_secs(20)).await?;
    while Instant::now() < deadline {
        match timeout(
            Duration::from_secs(RTDS_FRESHNESS_TIMEOUT_SECS),
            stream.next(),
        )
        .await
        {
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
                            "payload": message.payload
                        }),
                    ),
                )
                .await;
            }
            Ok(Some(Err(err))) => record_gap(&sink.gaps, format!("rtds_stream_error:{err}")),
            Ok(None) => {
                record_gap(&sink.gaps, "rtds_stream_closed".to_owned());
                break;
            }
            Err(_) => record_gap(&sink.gaps, "rtds_business_message_timeout".to_owned()),
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
    match http_client.get(BINANCE_DEPTH_SNAPSHOT).send().await {
        Ok(response) => {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if status.is_success() && !text.trim().is_empty() {
                send_event(
                    &sink,
                    run_started,
                    raw_event(
                        "binance_depth_snapshot",
                        "snapshot",
                        "BTCUSDT",
                        None,
                        BINANCE_DEPTH_SNAPSHOT,
                        &collector_identity,
                        json!({"endpoint": BINANCE_DEPTH_SNAPSHOT, "raw": text}),
                    ),
                )
                .await;
            } else {
                record_gap(
                    &sink.gaps,
                    format!("binance_depth_snapshot_bad_status:{status}"),
                );
            }
        }
        Err(err) => record_gap(&sink.gaps, format!("binance_depth_snapshot_error:{err}")),
    }
    run_binance_ws_source(
        BinanceWsSpec {
            source_kind: "binance_depth_update".to_owned(),
            default_event_type: "depthUpdate".to_owned(),
            url: BINANCE_DEPTH_WS.to_owned(),
            freshness_timeout_secs: 30,
        },
        sink,
        collector_identity,
        deadline,
        run_started,
    )
    .await
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
) -> Result<LiveTrainingRuntimeReceipt> {
    let run_manifest_dir = dataset_root.join("training").join("run_manifests");
    fs::create_dir_all(&run_manifest_dir)?;
    let run_manifest_path = run_manifest_dir.join(format!("{}.manifest.json", config.run_id));
    let resource_watermark = json!({
        "rss_bytes": read_rss_bytes(),
        "artifact_dir_bytes": dir_size_bytes(artifact_dir).ok(),
        "queue_rows_high": source_queue_summary.rows_high.max(writer_queue_summary.rows_high),
        "queue_bytes_high": source_queue_summary.bytes_high.max(writer_queue_summary.bytes_high),
        "source_event_queue": source_queue_summary,
        "writer_chunk_queue": writer_queue_summary,
        "unavailable": [
            {"metric":"cpu_percent","reason":"not measured by phase7 deployment runtime"},
            {"metric":"disk_free_bytes","reason":"not measured by phase7 deployment runtime"},
            {"metric":"journal_log_bytes","reason":"not measured by phase7 deployment runtime"},
            {"metric":"journal_limit_status","reason":"not measured by phase7 deployment runtime"},
            {"metric":"open_files","reason":"not measured by phase7 deployment runtime"},
            {"metric":"cgroup_memory_current_bytes","reason":"not measured by phase7 deployment runtime"},
            {"metric":"cgroup_memory_stat","reason":"not measured by phase7 deployment runtime"},
            {"metric":"cgroup_memory_events","reason":"not measured by phase7 deployment runtime"},
            {"metric":"psi_cpu","reason":"not measured by phase7 deployment runtime"},
            {"metric":"psi_memory","reason":"not measured by phase7 deployment runtime"},
            {"metric":"psi_io","reason":"not measured by phase7 deployment runtime"}
        ]
    });
    let manifest = json!({
        "run_id": config.run_id,
        "mode": "live",
        "verdict": verdict,
        "dataset_root": dataset_root,
        "artifact_dir": artifact_dir,
        "collector_host_id": config.collector_host_id,
        "collector_region": config.collector_region,
        "source_coverage": source_coverage,
        "source_gaps": source_gaps,
        "rollover_edges": rollover_evidence,
        "finalized_segments": finalized_segments,
        "resource_watermark": resource_watermark,
        "writer_used_blocking_pool": true,
        "recv_time_utc_ms": now_ms()
    });
    fs::write(&run_manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
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

#[allow(clippy::too_many_arguments)]
fn write_runtime_artifacts(
    artifact_dir: &Path,
    coverage: &BTreeMap<String, CoverageStats>,
    gaps: &[String],
    rollover_evidence: &[RolloverEvidenceReceipt],
    queue_samples: &[QueueSampleReceipt],
    writer_outputs: &[SourceWriterOutput],
    source_queue_summary: &QueueDurationSummary,
    writer_queue_summary: &QueueDurationSummary,
) -> Result<()> {
    fs::write(
        artifact_dir.join("source_coverage.json"),
        serde_json::to_vec_pretty(
            &coverage
                .iter()
                .map(|(source_kind, stats)| stats.to_receipt(source_kind))
                .collect::<Vec<_>>(),
        )?,
    )?;
    fs::write(
        artifact_dir.join("source_gaps.json"),
        serde_json::to_vec_pretty(gaps)?,
    )?;
    fs::write(
        artifact_dir.join("rollover_edges.json"),
        serde_json::to_vec_pretty(rollover_evidence)?,
    )?;
    write_jsonl(&artifact_dir.join("queue_samples.jsonl"), queue_samples)?;
    let write_receipts = writer_outputs
        .iter()
        .flat_map(|output| output.write_receipts.clone())
        .collect::<Vec<_>>();
    write_jsonl(&artifact_dir.join("write_receipts.jsonl"), &write_receipts)?;
    fs::write(
        artifact_dir.join("latency_summary.json"),
        serde_json::to_vec_pretty(&json!({
            "write_events_us": LatencyStats::from_values(
                &write_receipts.iter().map(|receipt| receipt.write_events_us).collect::<Vec<_>>()
            ),
            "source_event_queue": source_queue_summary,
            "writer_chunk_queue": writer_queue_summary,
            "source_writer_rows": writer_outputs
                .iter()
                .map(|output| (output.source_kind.clone(), output.finalized_segments.iter().map(|segment| segment.row_count).sum::<u64>()))
                .collect::<BTreeMap<_, _>>()
        }))?,
    )?;
    Ok(())
}

fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> Result<()> {
    let mut file = File::create(path)?;
    for item in items {
        serde_json::to_writer(&mut file, item)?;
        file.write_all(b"\n")?;
    }
    Ok(())
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

fn parse_source_ts_ms(event: &RawEvent) -> Option<i64> {
    event.source_ts.as_deref()?.parse().ok()
}

fn record_gap(gaps: &Arc<Mutex<Vec<String>>>, gap: String) {
    if !is_live_runtime_source_gap_reason(&gap) {
        return;
    }
    gaps.lock().expect("source gap mutex poisoned").push(gap);
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

struct SourceWriterHandle {
    tx: std::sync::mpsc::SyncSender<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    handle: tokio::task::JoinHandle<Result<SourceWriterOutput>>,
}

enum WriterCommand {
    Events(Vec<RawEvent>),
}

#[derive(Clone, Debug, Serialize)]
struct SourceWriterOutput {
    source_kind: String,
    finalized_segments: Vec<LiveFinalizedSegment>,
    write_receipts: Vec<LiveWriteReceipt>,
}

#[derive(Clone, Debug, Serialize)]
struct LiveWriteReceipt {
    source_kind: String,
    rows: u64,
    recv_time_utc_ms: i64,
    write_done_time_utc_ms: i64,
    write_events_us: u64,
}

struct ActiveSegment {
    source_kind: String,
    naming: SegmentNaming,
    writer: ParquetSegmentWriter,
    utc_day: String,
    segment_seq: u64,
    segment_start_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    bytes: u64,
}

impl ActiveSegment {
    fn new(naming: SegmentNaming, source_kind: &str, utc_day: String) -> Result<Self> {
        let segment_seq = naming
            .stem
            .split("--segment_seq=")
            .nth(1)
            .and_then(|value| value.split("--").next())
            .and_then(|value| value.parse().ok())
            .unwrap_or_default();
        let writer = ParquetSegmentWriter::create_for_source(
            &naming.partition_path,
            &naming.stem,
            "parquet_raw_layout_v1",
            source_kind,
        )?;
        Ok(Self {
            source_kind: source_kind.to_owned(),
            naming,
            writer,
            utc_day,
            segment_seq,
            segment_start_recv_time_utc_ms: 0,
            last_recv_time_utc_ms: 0,
            source_ts_start: None,
            source_ts_end: None,
            rows: 0,
            bytes: 0,
        })
    }

    fn observe(&mut self, event: &RawEvent) {
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
        self.rows = self.rows.saturating_add(1);
        self.bytes = self.bytes.saturating_add(estimated_event_bytes(event));
    }
}

fn live_segment_rollover_reason(
    segment: &ActiveSegment,
    current_utc_day: &str,
) -> Option<&'static str> {
    let policy = SegmentPolicy {
        max_rows: DEFAULT_MAX_SEGMENT_ROWS,
        max_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        max_segment_duration_ms: DEFAULT_MAX_SEGMENT_DURATION_MS,
    };
    let decision = policy.evaluate(
        &SegmentState {
            rows: segment.rows,
            bytes: segment.bytes,
            segment_start_recv_time_utc_ms: segment.segment_start_recv_time_utc_ms,
            latest_recv_time_utc_ms: segment.last_recv_time_utc_ms,
            utc_day: segment.utc_day.clone(),
        },
        current_utc_day,
    );
    match decision.reason {
        Some(RolloverReason::MaxRows) => Some("max_rows"),
        Some(RolloverReason::MaxBytes) => Some("max_bytes"),
        Some(RolloverReason::MaxSegmentDuration) => Some("max_segment_duration"),
        Some(RolloverReason::UtcDayBoundary) => Some("utc_day_boundary"),
        _ => None,
    }
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

    fn to_receipt(&self, source_kind: &str) -> CoverageReceipt {
        CoverageReceipt {
            source_kind: source_kind.to_owned(),
            first_recv_time_utc_ms: self.first_recv_time_utc_ms,
            last_recv_time_utc_ms: self.last_recv_time_utc_ms,
            rows: self.rows,
            market_windows: self.market_windows.iter().cloned().collect(),
            market_window_first_recv_time_utc_ms: self.market_window_first_recv_time_utc_ms.clone(),
            event_type_counts: self.event_type_counts.clone(),
        }
    }
}

#[derive(Serialize)]
struct CoverageReceipt {
    source_kind: String,
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    rows: u64,
    market_windows: Vec<String>,
    market_window_first_recv_time_utc_ms: BTreeMap<String, i64>,
    event_type_counts: BTreeMap<String, u64>,
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

#[derive(Clone, Debug, Serialize)]
struct QueueSampleReceipt {
    sample_time_utc_ms: i64,
    sample_monotonic_ms: u64,
    source_event_queue: QueueDurationSummary,
    writer_chunk_queue: QueueDurationSummary,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publish::discover_finalized_artifacts;
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
    fn live_segment_manifest_is_discoverable_as_complete() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = SegmentNaming::new(
            &dataset_root,
            "binance_aggtrade",
            1_783_392_404_599,
            0,
            "run",
        )
        .unwrap();
        let mut segment =
            ActiveSegment::new(naming.clone(), "binance_aggtrade", "2026-07-07".to_owned())
                .unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);

        segment
            .writer
            .write_events(std::slice::from_ref(&event))
            .unwrap();
        segment.observe(&event);
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
    }

    #[test]
    fn live_runtime_rolls_segment_on_max_bytes() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let naming = SegmentNaming::new(
            &dataset_root,
            "binance_aggtrade",
            1_783_392_404_599,
            0,
            "run",
        )
        .unwrap();
        let mut segment =
            ActiveSegment::new(naming, "binance_aggtrade", "2026-07-07".to_owned()).unwrap();
        let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
        segment.observe(&event);
        segment.bytes = DEFAULT_MAX_SEGMENT_BYTES;

        assert_eq!(
            live_segment_rollover_reason(&segment, "2026-07-07"),
            Some("max_bytes")
        );
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
            source_event_type: "test".to_owned(),
            source_identifier: "BTCUSDT".to_owned(),
            source_ts: Some(recv_time_utc_ms.to_string()),
            recv_time_utc_ms,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: "test://source".to_owned(),
            raw_json: "{\"event\":\"test\"}".to_owned(),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: source_kind.to_owned(),
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
