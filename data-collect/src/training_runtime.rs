mod binance_aggtrade_source;
mod binance_depth_source;
mod dataset_capacity;
mod dataset_root_lock;
mod ingress_control_sender;
mod ingress_deferred;
mod ingress_owned_bytes;
mod manifest_owner;
mod polymarket_event_adapter;
mod polymarket_source;
mod producer_day_barrier;
mod raw_day;
mod raw_ingress_admission;
mod rollover_coordinator;
mod rtds_source;
mod runtime_coverage;
mod runtime_gaps;
mod runtime_ingress;
mod runtime_rollover_driver;
mod runtime_shutdown;
mod runtime_time;
mod source_ingress_state;
mod source_recovery;
mod source_task_supervision;
mod source_topology;
mod startup_coordinator;
mod startup_recovery;
mod writer_supervisor;

use binance_aggtrade_source::*;
use binance_depth_source::*;
use dataset_capacity::*;
use dataset_root_lock::*;
use ingress_control_sender::*;
use ingress_deferred::*;
use ingress_owned_bytes::*;
use manifest_owner::*;
use polymarket_event_adapter::*;
use polymarket_source::*;
use producer_day_barrier::*;
use raw_day::*;
use raw_ingress_admission::*;
use rollover_coordinator::*;
use rtds_source::*;
use runtime_coverage::*;
pub use runtime_gaps::is_training_runtime_source_gap_reason;
#[cfg(test)]
use runtime_gaps::{MAX_RECORDED_SOURCE_GAPS, SOURCE_GAP_OVERFLOW_PREFIX};
use runtime_gaps::{
    SharedSourceGaps, SourceGapStore, record_gap, record_gap_at, record_gap_for_day,
};
use runtime_ingress::*;
use runtime_rollover_driver::*;
use runtime_shutdown::*;
use runtime_time::*;
use source_ingress_state::*;
use source_recovery::*;
use source_task_supervision::*;
use source_topology::*;
use startup_coordinator::*;
use startup_recovery::*;
use writer_supervisor::*;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fs::{self, File};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr as _;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array as _, StringArray, TimestampMillisecondArray, UInt64Array};
use base64::Engine as _;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use polymarket_client_sdk_v2::clob::ws::types::response::OrderBookLevel;
use polymarket_client_sdk_v2::clob::ws::{
    EventMessage, MarketResolved, PriceChangeBatchEntry, WsMessage,
};
use polymarket_client_sdk_v2::error::Kind as PolymarketSdkErrorKind;
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::{MarketBySlugRequest, MarketsRequest};
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use polymarket_client_sdk_v2::rtds::{ChainlinkPrice, Subscription};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio::net::TcpStream;
use tokio::sync::{
    Mutex as AsyncMutex, OwnedRwLockReadGuard, RwLock as AsyncRwLock, mpsc, oneshot, watch,
};
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, sleep, timeout};
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, connect_async_with_config,
};

use crate::backpressure::bounded_event_queue;
use crate::binance_aggtrade::{
    BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
    BinanceAggTradeIngressSequenceAllocator, BinanceAggTradeInvalidInput,
    BinanceAggTradeInvalidReason, BinanceAggTradeInvalidTransport, BinanceAggTradeOutput,
    BinanceAggTradeRawCapture, BinanceWsAggTrade,
};
use crate::daily_manifest::{
    CollectionDayCoverageState, DailyFileRecoveryMetadata, DailyFinalizedFileDelta,
    DailyManifestIdentity, SourceOperationalCounterDelta, activate_collection_day_coverage,
    add_manifest_operational_counters, close_day_manifest_with_collection_end,
    close_day_manifest_with_unclean_end, close_reconciled_day_manifest, day_has_finalized_parquet,
    load_day_manifest, manifest_collection_day_coverage, manifest_state,
    mark_collection_day_coverage_ended_known, mark_collection_day_coverage_unclean,
    merge_finalized_file_delta, reconcile_open_day_manifest, record_day_barrier_state,
    record_manifest_gaps, remove_unowned_stale_temporary_files, typed_raw_root,
    utc_day_directories, validate_manifest_day, validate_manifest_recovery_files,
    validate_unmanifested_recovery_files,
};
use crate::dedupe::{
    BinanceIdDecision, BinanceRecentIdWindow, FingerprintDecision, TimedFingerprintWindow,
};
use crate::event::{EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp};
use crate::hourly_partition::{HourlyPartition, HourlyPartitionPolicy};
use crate::inflight::{BUFFER_ACQUIRE_TIMEOUT, InflightBudget, InflightReservation};
pub use crate::inflight::{
    GLOBAL_INFLIGHT_MAX_BYTES, GLOBAL_INFLIGHT_MAX_ROWS, MAX_TYPED_ROW_BYTES,
};
use crate::latency::{LatencyStats, QueueDurationTracker};
use crate::manifest::SourceCoverage;
use crate::market_discovery::{
    BtcUpdownDiscovery, BtcUpdownMarketCandidate, CandidateRejectReason, DiscoveredBtcUpdownMarket,
    DiscoverySelectionError, discovery_windows, select_btc_updown_markets,
};
#[cfg(test)]
use crate::polymarket_market_metadata_layout::POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION;
use crate::polymarket_market_metadata_layout::{
    ActiveUnavailableReason, ClosedTimeCheckFailureReason, MarketLifecycleState,
    MarketLifecycleTransition, MarketRuntimeRole, MarketRuntimeRoleTransition,
    POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION, PolymarketMarketMetadataRow,
    PolymarketMarketMetadataV5Row, PolymarketMetadataOrigin, PolymarketMetadataReferenceRecord,
    PolymarketMetadataRowKind, ResolutionEvidenceSource, ResolutionFollowupStatus,
    ResolutionIntegrityErrorClass, ResolutionPollResult, ResolutionTransportErrorClass,
    ResponseFieldsUnavailableReason, SourceEndpointUnavailableReason, SourceEvidenceChannel,
    SourceEvidenceEventType, SourceEvidenceOrigin, TerminalClaimOwner,
    TerminalPredicateFailureReason, validate_polymarket_metadata_local_references,
};
#[cfg(test)]
use crate::polymarket_orderbook_v3::PolymarketOrderbookV3Admission;
use crate::polymarket_orderbook_v3::{
    POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PendingPolymarketOrderbookV3Admission,
    PolymarketOrderbookV3Event, PolymarketOrderbookV3Message, PolymarketOrderbookV3Projection,
    same_outcome_label,
};
use crate::polymarket_resolution_recovery::{
    PolymarketRestartRecovery, RecoveredPolymarketFixedIdentities,
    RecoveredPolymarketFixedIdentity, RecoveredPolymarketPending,
    restore_polymarket_resolution_recovery,
};
use crate::polymarket_sdk_runtime::{
    MarketSdkMessageReceiver, MarketSdkRuntime, RtdsRuntimeEvent, RtdsSdkRuntime, SdkStreamError,
};
use crate::source_format::{
    BinanceDepthOrdering, RtdsChainlinkBtcUsdRaw, plausible_unix_timestamp_ms,
};
#[cfg(test)]
use crate::source_hourly_layout::BinanceAggTradeOrdering;
use crate::source_hourly_layout::{
    BINANCE_AGGTRADE_SCHEMA_VERSION, BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
    BINANCE_DEPTH_UPDATE_SCHEMA_VERSION, BINANCE_DEPTH_VERDICT_APPLIED,
    BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE, BinanceAggTradeHourlyRow,
    BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow, RtdsChainlinkBtcUsdHourlyRow,
};
#[cfg(test)]
use crate::source_hourly_layout::{
    BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE,
    BINANCE_DEPTH_VERDICT_NOT_APPLIED_STALE_DUPLICATE,
};
use crate::sources::binance::{
    BINANCE_AGGTRADE_WS_ENDPOINT, BINANCE_DEPTH_SNAPSHOT_ENDPOINT, DepthUpdateAssessment,
    DepthUpdateVerdict, assess_depth_update, parse_depth_snapshot_raw, parse_depth_update_raw,
};
use crate::sources::polymarket::{
    GammaTerminalAttemptContext, GammaTerminalFixedIdentity, GammaTerminalResponse,
    classify_gamma_market_by_slug_transport_error, evaluate_gamma_terminal_response,
};
use crate::writer::{
    DAILY_FILE_RECOVERY_METADATA_KEY, PolymarketOrderbookV3WriteSequenceState, PublishedSegment,
    RawEvent, RawEventAdmission, TypedParquetSegmentWriter, typed_sort_order,
};
#[cfg(test)]
use crate::writer::{PARQUET_ROW_GROUP_MAX_BYTES, PARQUET_ROW_GROUP_MAX_ROWS};

const GAMMA_MARKETS_ENDPOINT: &str = "https://gamma-api.polymarket.com/markets";
const GAMMA_MARKET_BY_SLUG_ENDPOINT: &str = "https://gamma-api.polymarket.com/markets/slug/{slug}";
const DEFAULT_CONTINUOUS_DURATION_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_QUEUE_CAPACITY: usize = 25_000;
const DEFAULT_FLUSH_ROWS: usize = 5_000;
const DEFAULT_MAX_FLUSH_INTERVAL_MS: u64 = 1_000;
const WRITER_FINALIZE_BUDGET_MS: u64 = 30_000;
const COORDINATED_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(90);
const WS_CONNECT_TIMEOUT_SECS: u64 = 20;
const WS_WRITE_TIMEOUT_SECS: u64 = 5;
const BINANCE_WS_RECONNECT_BASE_MS: u64 = 500;
const BINANCE_WS_RECONNECT_MAX_MS: u64 = 5_000;
const SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS: u32 = 12;
const SOURCE_RECOVERY_CYCLE_MAX_ELAPSED: Duration = Duration::from_secs(120);
const SOURCE_SUPERVISOR_BACKOFF_BASE_MS: u64 = 1_000;
const SOURCE_SUPERVISOR_BACKOFF_MAX_MS: u64 = 30_000;
const SOURCE_RECOVERY_JITTER_BPS: u32 = 2_000;
const AGGTRADE_CONTROL_CHANNEL_CAPACITY: usize = 4;
const STARTUP_GATE_TIMEOUT: Duration = Duration::from_secs(30);

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn append_runtime_failure(runtime_failure: &mut Option<String>, message: String) {
    match runtime_failure {
        None => *runtime_failure = Some(message),
        Some(existing)
            if !existing
                .split("; shutdown_secondary:")
                .any(|recorded| recorded == message) =>
        {
            existing.push_str("; shutdown_secondary:");
            existing.push_str(&message);
        }
        Some(_) => {}
    }
}

fn promote_runtime_primary(runtime_failure: &mut Option<String>, primary: String) {
    match runtime_failure.take() {
        None => *runtime_failure = Some(primary),
        Some(existing) if existing == primary || existing.starts_with(&format!("{primary};")) => {
            *runtime_failure = Some(existing);
        }
        Some(existing) => {
            *runtime_failure = Some(format!("{primary}; shutdown_secondary:{existing}"));
        }
    }
}

fn runtime_error_with_secondary(
    runtime_failure: &mut Option<String>,
    error: Box<dyn Error + Send + Sync>,
) -> Box<dyn Error + Send + Sync> {
    append_runtime_failure(runtime_failure, error.to_string());
    io::Error::other(
        runtime_failure
            .take()
            .expect("runtime failure contains the supplied error"),
    )
    .into()
}

pub const TRAINING_SOURCE_KINDS: &[&str] = &[
    "polymarket_market",
    "polymarket_market_metadata",
    "binance_depth_snapshot",
    "binance_depth_update",
    "binance_aggtrade",
    "polymarket_rtds_chainlink_btc_usd",
];

enum IngressControl {
    FinalizeSourceHour {
        source_kind: String,
        utc_hour: String,
    },
    DayBarrier {
        producer: String,
        close: RawDayClose,
        reply: oneshot::Sender<std::result::Result<DayBarrierRelease, String>>,
    },
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
pub struct TrainingRuntimeConfig {
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
pub struct TrainingRunSummary {
    pub finalized_segments: Vec<TrainingFinalizedSegment>,
    pub source_coverage: Vec<SourceCoverage>,
    pub source_gaps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TrainingFinalizedSegment {
    pub source_kind: String,
    pub parquet_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
}

pub async fn run_training_runtime(config: TrainingRuntimeConfig) -> Result<TrainingRunSummary> {
    run_training_runtime_with_sources_and_stop(
        config,
        TrainingSourceSpecs::production(),
        shutdown_signal(),
    )
    .await
}

async fn run_training_runtime_with_sources_and_stop<S>(
    config: TrainingRuntimeConfig,
    source_specs: TrainingSourceSpecs,
    operator_stop: S,
) -> Result<TrainingRunSummary>
where
    S: Future<Output = ()> + Send + 'static,
{
    validate_mandatory_producer_source_mapping()?;
    fs::create_dir_all(&config.dataset_root)?;
    let dataset_root = config.dataset_root.canonicalize()?;
    let dataset_root_lock = Arc::new(lock_training_dataset_root(&dataset_root)?);
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
    let runtime_deadline = deadline
        .zip(deadline_ms)
        .map(|(deadline, observed_utc_ms)| RuntimeDeadlineCandidate {
            deadline,
            observed_utc_ms,
        });
    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let source_queue_backpressure = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate.install_runtime_deadline(runtime_deadline)?;
    source_queue_backpressure.install_raw_day_gate(Arc::clone(&raw_day_gate))?;
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
    let (event_tx, mut event_rx) =
        bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY)?;
    let event_sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&source_gaps),
        source_queue_backpressure: Arc::clone(&source_queue_backpressure),
        inflight_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut coverage = BTreeMap::<String, CoverageStats>::new();
    let mut batches = PendingIngressBatches::for_training_source_kinds();
    let mut source_queue_tracker = QueueDurationTracker::new("source_event_queue");
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut next_flush = run_started + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
    let mut next_queue_sample = run_started;
    let mut average_event_bytes = 512_u64;
    let mut received_rows = 0_u64;
    let mut operator_shutdown = false;
    let mut runtime_failure = None::<String>;
    let (runtime_exit_tx, mut runtime_exit_rx) = mpsc::channel(1);
    let runtime_exit_deadline = deadline;
    let runtime_exit_deadline_ms = deadline_ms;
    let runtime_exit_gate = Arc::clone(&raw_day_gate);
    let mut runtime_exit_watcher = RuntimeExitWatcher::new(tokio::spawn(async move {
        let request = if let (Some(deadline), Some(deadline_ms)) =
            (runtime_exit_deadline, runtime_exit_deadline_ms)
        {
            tokio::select! {
                biased;
                () = operator_stop => RuntimeExitRequest {
                    observed_utc_ms: now_ms(),
                    cause: RuntimeCloseCause::OperatorStop,
                },
                () = tokio::time::sleep_until(deadline.into()) => RuntimeExitRequest {
                    observed_utc_ms: deadline_ms,
                    cause: RuntimeCloseCause::RunDeadline,
                },
            }
        } else {
            operator_stop.await;
            RuntimeExitRequest {
                observed_utc_ms: now_ms(),
                cause: RuntimeCloseCause::OperatorStop,
            }
        };
        let _ = runtime_exit_gate
            .prepare_runtime_exit_with_cause(request.observed_utc_ms, request.cause.clone());
        let _ = runtime_exit_tx.send(request).await;
    }));
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
        polymarket: polymarket_recovery,
        current_manifest,
    } = startup_recovery;
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    let mut rollover_coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let (startup_ready_tx, startup_ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);
    let mut manifest_owner = ManifestOwnerState::new(dataset_root.clone(), daily_manifest_identity);
    manifest_owner.insert_reconciled_manifest(recovery_day.clone(), current_manifest)?;
    let manifest_owner_dataset_lease = Arc::clone(&dataset_root_lock);
    let mut manifest_owner_handle = tokio::spawn(async move {
        let _dataset_lease = manifest_owner_dataset_lease;
        run_manifest_owner(manifest_owner, manifest_rx, rollover_ready_tx).await
    });
    let (mut writer_handles, writer_startup_releases) = match start_writer_workers_gated(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &source_queue_backpressure,
        &startup_ready_tx,
        Arc::clone(&raw_day_gate),
        Some(Arc::clone(&dataset_root_lock)),
    ) {
        Ok(started) => started,
        Err(error) => {
            manifest_owner_handle.abort();
            let _ = manifest_owner_handle.await;
            return Err(error);
        }
    };

    let polymarket_sink = event_sink.fork_logical_producer();
    let polymarket_source_sink = polymarket_sink.clone();
    let (polymarket_control_tx, polymarket_control_rx) =
        mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let polymarket_source = run_polymarket_source_with_spec(
        source_specs.polymarket,
        dataset_root.clone(),
        polymarket_source_sink,
        collector_identity.clone(),
        deadline_ms,
        run_started,
        polymarket_control_rx,
        polymarket_recovery,
    );
    let rtds_sink = event_sink.fork_logical_producer();
    let rtds_source = run_rtds_source_with_spec(
        source_specs.rtds,
        dataset_root.clone(),
        rtds_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
    );
    let depth_sink = event_sink.fork_logical_producer();
    let (depth_control_tx, depth_control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let depth_source = run_binance_depth_source_with_spec_and_control(
        source_specs.depth,
        depth_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
        Some(depth_control_rx),
    );
    let aggtrade_sink = event_sink.fork_logical_producer();
    let (aggtrade_control_tx, aggtrade_control_rx) =
        mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let aggtrade_source = run_binance_ws_source(
        binance_aggtrade_recovery,
        source_specs.aggtrade,
        aggtrade_sink.clone(),
        collector_identity.clone(),
        deadline,
        run_started,
        aggtrade_control_rx,
    );
    let polymarket_participant = StartupParticipant::producer("polymarket");
    let (polymarket_release_tx, polymarket_release_rx) = oneshot::channel();
    let rtds_participant = StartupParticipant::producer("polymarket_rtds");
    let (rtds_release_tx, rtds_release_rx) = oneshot::channel();
    let depth_participant = StartupParticipant::producer("binance_depth");
    let (depth_release_tx, depth_release_rx) = oneshot::channel();
    let aggtrade_participant = StartupParticipant::producer("binance_aggtrade");
    let (aggtrade_release_tx, aggtrade_release_rx) = oneshot::channel();
    let polymarket_observation = SourceTaskObservationCell::default();
    let rtds_observation = SourceTaskObservationCell::default();
    let depth_observation = SourceTaskObservationCell::default();
    let aggtrade_observation = SourceTaskObservationCell::default();
    let rollover_fallback_sinks = BTreeMap::from([
        ("polymarket", polymarket_sink.clone()),
        ("polymarket_rtds", rtds_sink.clone()),
        ("binance_depth", depth_sink.clone()),
        ("binance_aggtrade", aggtrade_sink.clone()),
    ]);
    let mut handles: Vec<SourceTaskHandle> = vec![
        SourceTaskHandle::spawn_with_dataset_lease(
            "polymarket_market",
            run_producer_after_startup_gate(
                polymarket_participant,
                startup_ready_tx.clone(),
                polymarket_release_rx,
                run_polymarket_logical_producer_with_day_barriers(
                    polymarket_sink,
                    polymarket_control_tx.clone(),
                    run_started,
                    initial_next_boundary_ms,
                    observe_inner_source_return(
                        "polymarket_market",
                        polymarket_source,
                        polymarket_observation.clone(),
                        Arc::clone(&raw_day_gate),
                    ),
                ),
            ),
            polymarket_observation,
            Arc::clone(&raw_day_gate),
            Arc::clone(&dataset_root_lock),
        ),
        SourceTaskHandle::spawn_with_dataset_lease(
            "polymarket_rtds_chainlink_btc_usd",
            run_producer_after_startup_gate(
                rtds_participant,
                startup_ready_tx.clone(),
                rtds_release_rx,
                run_logical_producer_with_day_barriers(
                    "polymarket_rtds",
                    rtds_sink,
                    run_started,
                    initial_next_boundary_ms,
                    observe_inner_source_return(
                        "polymarket_rtds_chainlink_btc_usd",
                        rtds_source,
                        rtds_observation.clone(),
                        Arc::clone(&raw_day_gate),
                    ),
                ),
            ),
            rtds_observation,
            Arc::clone(&raw_day_gate),
            Arc::clone(&dataset_root_lock),
        ),
        SourceTaskHandle::spawn_with_dataset_lease(
            "binance_depth_update",
            run_producer_after_startup_gate(
                depth_participant,
                startup_ready_tx.clone(),
                depth_release_rx,
                run_depth_logical_producer_with_time_barriers(
                    depth_sink,
                    depth_control_tx.clone(),
                    run_started,
                    initial_next_boundary_ms,
                    next_utc_hour_boundary_ms(now_ms()),
                    observe_inner_source_return(
                        "binance_depth_update",
                        depth_source,
                        depth_observation.clone(),
                        Arc::clone(&raw_day_gate),
                    ),
                ),
            ),
            depth_observation,
            Arc::clone(&raw_day_gate),
            Arc::clone(&dataset_root_lock),
        ),
        SourceTaskHandle::spawn_with_dataset_lease(
            "binance_aggtrade",
            run_producer_after_startup_gate(
                aggtrade_participant,
                startup_ready_tx.clone(),
                aggtrade_release_rx,
                run_aggtrade_logical_producer_with_day_barriers(
                    aggtrade_sink,
                    aggtrade_control_tx.clone(),
                    run_started,
                    initial_next_boundary_ms,
                    observe_inner_source_return(
                        "binance_aggtrade",
                        aggtrade_source,
                        aggtrade_observation.clone(),
                        Arc::clone(&raw_day_gate),
                    ),
                ),
            ),
            aggtrade_observation,
            Arc::clone(&raw_day_gate),
            Arc::clone(&dataset_root_lock),
        ),
    ];
    drop(event_sink);
    drop(startup_ready_tx);
    let startup_releases = StartupReleaseHandles {
        writers: writer_startup_releases,
        producers: vec![
            StartupReleaseSender {
                participant: polymarket_participant,
                sender: polymarket_release_tx,
            },
            StartupReleaseSender {
                participant: rtds_participant,
                sender: rtds_release_tx,
            },
            StartupReleaseSender {
                participant: depth_participant,
                sender: depth_release_tx,
            },
            StartupReleaseSender {
                participant: aggtrade_participant,
                sender: aggtrade_release_tx,
            },
        ],
    };
    let startup_outcome = match activate_and_release_startup(
        &manifest_tx,
        Arc::clone(&raw_day_gate),
        startup_ready_rx,
        startup_releases,
        recovery_day,
        ManifestActivationAckMode::Send,
        runtime_deadline,
        now_ms,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            converge_spawned_runtime_resources(
                &mut handles,
                &mut writer_handles,
                &mut manifest_owner_handle,
                &mut runtime_exit_watcher,
                None,
                None,
                Instant::now() + STARTUP_GATE_TIMEOUT,
            )
            .await;
            return Err(error);
        }
    };
    let startup_stopped = matches!(
        startup_outcome,
        StartupActivationOutcome::CommittedStopped(_)
    );

    let mut completed_rollover_exit = None::<PendingExit>;
    let mut completed_source_terminations = Vec::<SourceTaskTermination>::new();
    let mut completed_rollover_writer_outcome = None::<WriterStopOutcome>;
    let mut rollover_release_stop_issued = startup_stopped;

    if !startup_stopped {
        'runtime: loop {
            tokio::select! {
                biased;
                request = runtime_exit_rx.recv() => {
                    let Some(request) = request else {
                        let message = freeze_coordinator_failure_message(
                            &raw_day_gate,
                            "runtime_exit_request_channel_closed",
                            "runtime_exit_request_channel_closed".to_owned(),
                        );
                        record_gap(&source_gaps, message.clone());
                        append_runtime_failure(&mut runtime_failure, message);
                        break 'runtime;
                    };
                    match request.cause {
                        RuntimeCloseCause::OperatorStop => operator_shutdown = true,
                        RuntimeCloseCause::RunDeadline => {}
                        _ => {
                            let message = format!("invalid_runtime_exit_request:{request:?}");
                            record_gap(&source_gaps, message.clone());
                            append_runtime_failure(&mut runtime_failure, message);
                        }
                    }
                    if let Err(error) = apply_runtime_exit_request(&raw_day_gate, &request) {
                        let message = format!("runtime_exit_prepare_failed:{error}");
                        record_gap(&source_gaps, message.clone());
                        append_runtime_failure(&mut runtime_failure, message);
                    }
                    break 'runtime;
                }
                ready = rollover_ready_rx.recv() => {
                    match ready {
                        Some(ready) => {
                            match drive_normal_rollover(
                                ready,
                                &manifest_tx,
                                &raw_day_gate,
                                &mut runtime_exit_rx,
                                &mut event_rx,
                                &mut writer_handles,
                                &mut batches,
                                &mut coverage,
                                &mut received_rows,
                                &mut average_event_bytes,
                                &source_gaps,
                                &source_queue_backpressure,
                                &mut writer_queue_tracker,
                                run_started,
                                &mut rollover_coordinator,
                                &mut rollover_release_stop_issued,
                                runtime_deadline,
                            ).await {
                                Ok(RolloverCommitOutcome::Activated) => {}
                                Ok(RolloverCommitOutcome::PendingExit) => {
                                    break 'runtime;
                                }
                                Ok(RolloverCommitOutcome::ActivatedStopped) => {
                                    rollover_release_stop_issued = true;
                                    break 'runtime;
                                }
                                Err(error) => {
                                    let message = format!("normal_rollover_failed:{error}");
                                    record_gap(&source_gaps, message.clone());
                                    append_runtime_failure(&mut runtime_failure, message);
                                    break 'runtime;
                                }
                            }
                        }
                        None => {
                            let message = freeze_coordinator_failure_message(
                                &raw_day_gate,
                                "manifest_rollover_ready_channel_closed",
                                "manifest_rollover_ready_channel_closed".to_owned(),
                            );
                            record_gap(&source_gaps, message.clone());
                            append_runtime_failure(&mut runtime_failure, message);
                            break 'runtime;
                        }
                    }
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
                                &mut rollover_coordinator,
                            )
                            .await
                            {
                                let message = freeze_coordinator_failure_message(
                                    &raw_day_gate,
                                    "ingress_processing",
                                    format!("ingress_processing_failed:{error}"),
                                );
                                record_gap(&source_gaps, message.clone());
                                append_runtime_failure(&mut runtime_failure, message);
                                break 'runtime;
                            }
                        }
                        Ok(None) => {
                            let message = freeze_coordinator_failure_message(
                                &raw_day_gate,
                                "source_event_queue_closed",
                                "source_event_queue_closed_unexpectedly".to_owned(),
                            );
                            record_gap(&source_gaps, message.clone());
                            append_runtime_failure(&mut runtime_failure, message);
                            break;
                        }
                        Err(_) => {}
                    }
                }
            }

            if let Some(index) = finished_source_task_index(&handles)
                && !deadline_reached(deadline)
            {
                match raw_day_gate.prepared_runtime_exit() {
                    Some(PreparedRuntimeExit::SameDay(close)) => {
                        let handle = handles.swap_remove(index);
                        let termination = handle.await_termination(&close.identity).await;
                        let message = assess_source_shutdown(&termination, None, &close.identity)
                            .error
                            .unwrap_or_else(|| {
                                source_task_ended_unexpectedly(termination.source_kind)
                            });
                        record_gap(&source_gaps, message.clone());
                        if close.cause.source_task_kind() == Some(termination.source_kind) {
                            promote_runtime_primary(&mut runtime_failure, message);
                        } else {
                            append_runtime_failure(&mut runtime_failure, message);
                        }
                        completed_source_terminations.push(termination);
                    }
                    Some(PreparedRuntimeExit::Rollover(_)) => {
                        // The pending-exit driver must retain and join this handle so it can
                        // synthesize the producer fence for the old UTC day.
                    }
                    None => {
                        let source_kind = handles[index].source_kind;
                        let message = freeze_coordinator_failure_message(
                            &raw_day_gate,
                            "source_task_ended_before_join",
                            source_task_ended_unexpectedly(source_kind),
                        );
                        record_gap(&source_gaps, message.clone());
                        append_runtime_failure(&mut runtime_failure, message);
                    }
                }
                break;
            }

            if let Some(completion) =
                poll_writer_completion(&mut writer_handles, &raw_day_gate, None)
            {
                record_gap(&source_gaps, completion.message.clone());
                append_runtime_failure(&mut runtime_failure, completion.message);
                break;
            }
            if manifest_owner_handle.is_finished() {
                let message = freeze_coordinator_failure_message(
                    &raw_day_gate,
                    "daily_manifest_owner_ended",
                    "daily_manifest_owner_ended_unexpectedly".to_owned(),
                );
                record_gap(&source_gaps, message.clone());
                append_runtime_failure(&mut runtime_failure, message);
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
                    let message = freeze_coordinator_failure_message(
                        &raw_day_gate,
                        "writer_periodic_flush",
                        format!("writer_periodic_flush_failed:{error}"),
                    );
                    record_gap(&source_gaps, message.clone());
                    append_runtime_failure(&mut runtime_failure, message);
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
        }
    }

    runtime_exit_watcher.abort_and_join().await;
    let coordinated_shutdown_deadline = Instant::now() + COORDINATED_SHUTDOWN_DEADLINE;
    let rollover_outcome_uncertain =
        rollover_coordinator.cutpoint() == Some(RolloverCutpoint::OutcomeUncertain);
    if rollover_outcome_uncertain {
        let close = match rollover_coordinator.ready() {
            Ok(ready) => ready.close.clone(),
            Err(error) => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    None,
                    None,
                    coordinated_shutdown_deadline,
                )
                .await;
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        };
        let observed_utc_ms = now_ms().max(close.boundary_utc_ms);
        if raw_day_gate.prepared_runtime_exit().is_none()
            && let Err(error) = raw_day_gate.prepare_runtime_exit_with_cause(
                observed_utc_ms,
                RuntimeCloseCause::CoordinatorFailure {
                    stage: "durable_rollover_outcome_uncertain",
                },
            )
        {
            converge_spawned_runtime_resources(
                &mut handles,
                &mut writer_handles,
                &mut manifest_owner_handle,
                &mut runtime_exit_watcher,
                None,
                None,
                coordinated_shutdown_deadline,
            )
            .await;
            return Err(runtime_error_with_secondary(&mut runtime_failure, error));
        }
        completed_rollover_exit = Some(PendingExit {
            close,
            observed_utc_ms,
            cause: RuntimeCloseCause::CoordinatorFailure {
                stage: "durable_rollover_outcome_uncertain",
            },
            unclean: true,
            unclean_cause: Some(RuntimeCloseCause::CoordinatorFailure {
                stage: "durable_rollover_outcome_uncertain",
            }),
        });
    }
    if completed_rollover_exit.is_none() {
        let prepared_exit = match raw_day_gate.prepare_runtime_exit(now_ms()) {
            Ok(prepared) => prepared,
            Err(error) => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    None,
                    None,
                    coordinated_shutdown_deadline,
                )
                .await;
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        };
        match prepared_exit {
            PreparedRuntimeExit::SameDay(_) => {}
            PreparedRuntimeExit::Rollover(exit) => {
                let close = exit.close.clone();
                let completed = match complete_pending_exit_rollover(
                    exit,
                    &mut handles,
                    &mut completed_source_terminations,
                    &rollover_fallback_sinks,
                    &mut event_rx,
                    &mut rollover_ready_rx,
                    &mut writer_handles,
                    &mut completed_rollover_writer_outcome,
                    &mut batches,
                    &mut coverage,
                    &mut received_rows,
                    &mut average_event_bytes,
                    &source_gaps,
                    &source_queue_backpressure,
                    &mut writer_queue_tracker,
                    run_started,
                    &manifest_tx,
                    &mut rollover_coordinator,
                    &raw_day_gate,
                    &mut runtime_failure,
                    coordinated_shutdown_deadline,
                )
                .await
                {
                    Ok(completed) => completed,
                    Err(error) => {
                        let source_close = RuntimeCloseIdentity::from_rollover_old_day(&close);
                        let writer_close = RuntimeCloseIdentity::from_rollover_next_day(&close);
                        converge_spawned_runtime_resources(
                            &mut handles,
                            &mut writer_handles,
                            &mut manifest_owner_handle,
                            &mut runtime_exit_watcher,
                            Some(&source_close),
                            Some(writer_close),
                            coordinated_shutdown_deadline,
                        )
                        .await;
                        return Err(runtime_error_with_secondary(&mut runtime_failure, error));
                    }
                };
                completed_rollover_exit = Some(completed);
            }
        }
    }
    let mut prepared_runtime_close = match completed_rollover_exit.as_ref() {
        Some(_) => None,
        None => match raw_day_gate.prepared_runtime_exit() {
            Some(PreparedRuntimeExit::SameDay(close)) => Some(close),
            other => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    None,
                    None,
                    coordinated_shutdown_deadline,
                )
                .await;
                let error = invalid_data(format!(
                    "runtime shutdown lost same-day close state: {other:?}"
                ))
                .into();
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        },
    };
    let runtime_close_identity = prepared_runtime_close
        .as_ref()
        .map(|close| close.identity.clone())
        .or_else(|| {
            completed_rollover_exit
                .as_ref()
                .map(|exit| RuntimeCloseIdentity::from_rollover_old_day(&exit.close))
        })
        .expect("runtime shutdown has a close identity");
    let writer_close_identity = prepared_runtime_close
        .as_ref()
        .map(|close| close.identity.clone())
        .or_else(|| {
            completed_rollover_exit
                .as_ref()
                .map(|exit| RuntimeCloseIdentity::from_rollover_next_day(&exit.close))
        })
        .expect("runtime shutdown has a writer close identity");
    let runtime_close_cause = prepared_runtime_close
        .as_ref()
        .map(|close| close.cause.clone())
        .or_else(|| {
            completed_rollover_exit
                .as_ref()
                .map(|exit| exit.cause.clone())
        })
        .expect("runtime shutdown has a close cause");
    let same_day_shutdown = prepared_runtime_close.is_some();
    let aborting_source_tasks = same_day_shutdown && !rollover_release_stop_issued;
    let mut termination_unclean = false;
    let mut polymarket_stop_reply = None;
    let mut depth_stop_reply = None;
    let mut aggtrade_stop_reply = None;
    let mut source_control_stop_facts = BTreeMap::<&'static str, SourceControlStopFact>::new();
    if aborting_source_tasks {
        if handles
            .iter()
            .any(|handle| handle.source_kind == "polymarket_market" && !handle.is_finished())
        {
            let (reply_tx, reply_rx) = oneshot::channel();
            match timeout(
                Duration::from_secs(5),
                polymarket_control_tx.send(PolymarketSourceControl::Fence {
                    stop_after_fence: true,
                    day_boundary_utc_ms: None,
                    resume_after_barrier: None,
                    reply: reply_tx,
                }),
            )
            .await
            {
                Ok(Ok(())) => polymarket_stop_reply = Some(reply_rx),
                Ok(Err(_)) => {
                    source_control_stop_facts.insert(
                        "polymarket_market",
                        SourceControlStopFact::Failed(
                            "polymarket_shutdown_control_channel_closed".to_owned(),
                        ),
                    );
                }
                Err(_) => {
                    source_control_stop_facts.insert(
                        "polymarket_market",
                        SourceControlStopFact::Failed(
                            "polymarket_shutdown_control_timeout".to_owned(),
                        ),
                    );
                }
            }
        }
        if handles
            .iter()
            .any(|handle| handle.source_kind == "binance_depth_update" && !handle.is_finished())
        {
            let (reply_tx, reply_rx) = oneshot::channel();
            match timeout(
                Duration::from_secs(5),
                depth_control_tx.send(DepthSourceControl::Fence {
                    kind: DepthFenceKind::ResetContinuity,
                    stop_after_fence: true,
                    resume_after_barrier: None,
                    reply: reply_tx,
                }),
            )
            .await
            {
                Ok(Ok(())) => depth_stop_reply = Some(reply_rx),
                Ok(Err(_)) => {
                    source_control_stop_facts.insert(
                        "binance_depth_update",
                        SourceControlStopFact::Failed(
                            "depth_shutdown_control_channel_closed".to_owned(),
                        ),
                    );
                }
                Err(_) => {
                    source_control_stop_facts.insert(
                        "binance_depth_update",
                        SourceControlStopFact::Failed("depth_shutdown_control_timeout".to_owned()),
                    );
                }
            }
        }
        if handles
            .iter()
            .any(|handle| handle.source_kind == "binance_aggtrade" && !handle.is_finished())
        {
            let (reply_tx, reply_rx) = oneshot::channel();
            match timeout(
                Duration::from_secs(5),
                aggtrade_control_tx.send(AggTradeSourceControl::Fence {
                    stop_after_fence: true,
                    resume_after_barrier: None,
                    reply: reply_tx,
                }),
            )
            .await
            {
                Ok(Ok(())) => aggtrade_stop_reply = Some(reply_rx),
                Ok(Err(_)) => {
                    source_control_stop_facts.insert(
                        "binance_aggtrade",
                        SourceControlStopFact::Failed(
                            "aggTrade_shutdown_control_channel_closed".to_owned(),
                        ),
                    );
                }
                Err(_) => {
                    source_control_stop_facts.insert(
                        "binance_aggtrade",
                        SourceControlStopFact::Failed(
                            "aggTrade_shutdown_control_timeout".to_owned(),
                        ),
                    );
                }
            }
        }
        for handle in &mut handles {
            if handle.source_kind != "polymarket_market"
                && handle.source_kind != "binance_depth_update"
                && handle.source_kind != "binance_aggtrade"
                && !handle.is_finished()
                && let Err(error) = handle.abort_with_permit(&runtime_close_identity)
            {
                let message = format!(
                    "source_task_abort_permit_failed:{}:{error}",
                    handle.source_kind
                );
                source_control_stop_facts
                    .insert(handle.source_kind, SourceControlStopFact::Failed(message));
            }
        }
    }

    while !handles.is_empty() {
        let mut index = 0;
        while index < handles.len() {
            if !handles[index].is_finished() {
                index += 1;
                continue;
            }
            let handle = handles.swap_remove(index);
            let termination = handle.await_termination(&runtime_close_identity).await;
            completed_source_terminations.push(termination);
        }
        if handles.is_empty() {
            break;
        }
        let remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            for handle in handles.drain(..) {
                let source_kind = handle.source_kind;
                handle.abort_without_permit();
                let mut termination = handle.await_termination(&runtime_close_identity).await;
                termination.error = Some(format!("source_task_join_timeout:{source_kind}"));
                completed_source_terminations.push(termination);
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
                &mut rollover_coordinator,
            )
            .await
        {
            let message = format!("shutdown_ingress_processing_failed:{error}");
            record_gap(&source_gaps, message.clone());
            append_runtime_failure(&mut runtime_failure, message);
            termination_unclean = true;
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
                append_runtime_failure(&mut runtime_failure, message);
                termination_unclean = true;
            }
            next_flush = Instant::now() + Duration::from_millis(DEFAULT_MAX_FLUSH_INTERVAL_MS);
        }
    }

    if let Some(reply_rx) = polymarket_stop_reply {
        source_control_stop_facts.insert(
            "polymarket_market",
            await_source_control_stop_fact(
                reply_rx,
                coordinated_shutdown_deadline,
                "polymarket_shutdown_fence",
            )
            .await,
        );
    }

    if let Some(reply_rx) = depth_stop_reply {
        source_control_stop_facts.insert(
            "binance_depth_update",
            await_source_control_stop_fact(
                reply_rx,
                coordinated_shutdown_deadline,
                "depth_shutdown_fence",
            )
            .await,
        );
    }

    if let Some(reply_rx) = aggtrade_stop_reply {
        source_control_stop_facts.insert(
            "binance_aggtrade",
            await_source_control_stop_fact(
                reply_rx,
                coordinated_shutdown_deadline,
                "aggTrade_shutdown_flush",
            )
            .await,
        );
    }

    if same_day_shutdown {
        let mut assessed_sources = BTreeSet::new();
        for termination in &completed_source_terminations {
            if !assessed_sources.insert(termination.source_kind) {
                let message = format!(
                    "duplicate_source_task_termination:{}",
                    termination.source_kind
                );
                record_gap(&source_gaps, message.clone());
                append_runtime_failure(&mut runtime_failure, message);
                termination_unclean = true;
                continue;
            }
            let assessment = assess_source_shutdown(
                termination,
                source_control_stop_facts.get(termination.source_kind),
                &runtime_close_identity,
            );
            termination_unclean |= assessment.unclean;
            if let Some(message) = assessment.error {
                record_gap(&source_gaps, message.clone());
                if runtime_close_cause.source_task_kind() == Some(termination.source_kind) {
                    promote_runtime_primary(&mut runtime_failure, message);
                } else {
                    append_runtime_failure(&mut runtime_failure, message);
                }
            }
        }
        if assessed_sources.len() != LOGICAL_PRODUCERS.len() {
            let message = format!(
                "source_task_termination_set_incomplete:expected={} actual={}",
                LOGICAL_PRODUCERS.len(),
                assessed_sources.len()
            );
            record_gap(&source_gaps, message.clone());
            append_runtime_failure(&mut runtime_failure, message);
            termination_unclean = true;
        }
    }

    if termination_unclean && same_day_shutdown {
        let marked = match raw_day_gate.mark_runtime_exit_unclean(&runtime_close_identity) {
            Ok(marked) => marked,
            Err(error) => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    Some(&runtime_close_identity),
                    Some(writer_close_identity.clone()),
                    coordinated_shutdown_deadline,
                )
                .await;
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        };
        let PreparedRuntimeExit::SameDay(close) = marked else {
            converge_spawned_runtime_resources(
                &mut handles,
                &mut writer_handles,
                &mut manifest_owner_handle,
                &mut runtime_exit_watcher,
                Some(&runtime_close_identity),
                Some(writer_close_identity.clone()),
                coordinated_shutdown_deadline,
            )
            .await;
            let error =
                invalid_data("same-day shutdown changed into rollover while marking unclean")
                    .into();
            return Err(runtime_error_with_secondary(&mut runtime_failure, error));
        };
        prepared_runtime_close = Some(close);
    }
    if same_day_shutdown {
        let identity = prepared_runtime_close
            .as_ref()
            .expect("same-day close exists")
            .identity
            .clone();
        prepared_runtime_close = Some(match raw_day_gate.seal_same_day_close(&identity).await {
            Ok(close) => close,
            Err(error) => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    Some(&runtime_close_identity),
                    Some(writer_close_identity.clone()),
                    coordinated_shutdown_deadline,
                )
                .await;
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        });
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
            &mut rollover_coordinator,
        )
        .await
        {
            let message = format!("shutdown_ingress_processing_failed:{error}");
            record_gap(&source_gaps, message.clone());
            append_runtime_failure(&mut runtime_failure, message);
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
        append_runtime_failure(&mut runtime_failure, message);
        termination_unclean = true;
    }

    let writer_remaining = coordinated_shutdown_deadline.saturating_duration_since(Instant::now());
    let (writer_outputs, all_writers_terminal, all_writer_closes_proven) =
        if let Some(outcome) = completed_rollover_writer_outcome.take() {
            debug_assert!(writer_handles.is_empty());
            take_writer_stop_outcome(outcome)
        } else {
            let outcome = shutdown_writer_workers_with_budget(
                std::mem::take(&mut writer_handles),
                writer_close_identity.clone(),
                writer_remaining.min(Duration::from_millis(WRITER_FINALIZE_BUDGET_MS)),
            )
            .await;
            writer_outputs_or_record_failure(outcome, &source_gaps, &mut runtime_failure)
        };
    if !all_writer_closes_proven {
        termination_unclean = true;
        if same_day_shutdown {
            let marked = match raw_day_gate.mark_runtime_exit_unclean(&runtime_close_identity) {
                Ok(marked) => marked,
                Err(error) => {
                    converge_spawned_runtime_resources(
                        &mut handles,
                        &mut writer_handles,
                        &mut manifest_owner_handle,
                        &mut runtime_exit_watcher,
                        Some(&runtime_close_identity),
                        None,
                        coordinated_shutdown_deadline,
                    )
                    .await;
                    return Err(runtime_error_with_secondary(&mut runtime_failure, error));
                }
            };
            let PreparedRuntimeExit::SameDay(close) = marked else {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    Some(&runtime_close_identity),
                    None,
                    coordinated_shutdown_deadline,
                )
                .await;
                let error =
                    invalid_data("same-day writer failure changed shutdown into rollover").into();
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            };
            prepared_runtime_close = Some(close);
        }
    }
    if termination_unclean && same_day_shutdown {
        let marked = match raw_day_gate.mark_runtime_exit_unclean(&runtime_close_identity) {
            Ok(marked) => marked,
            Err(error) => {
                converge_spawned_runtime_resources(
                    &mut handles,
                    &mut writer_handles,
                    &mut manifest_owner_handle,
                    &mut runtime_exit_watcher,
                    Some(&runtime_close_identity),
                    None,
                    coordinated_shutdown_deadline,
                )
                .await;
                return Err(runtime_error_with_secondary(&mut runtime_failure, error));
            }
        };
        let PreparedRuntimeExit::SameDay(close) = marked else {
            converge_spawned_runtime_resources(
                &mut handles,
                &mut writer_handles,
                &mut manifest_owner_handle,
                &mut runtime_exit_watcher,
                Some(&runtime_close_identity),
                None,
                coordinated_shutdown_deadline,
            )
            .await;
            let error =
                invalid_data("same-day shutdown changed into rollover before manifest commit")
                    .into();
            return Err(runtime_error_with_secondary(&mut runtime_failure, error));
        };
        prepared_runtime_close = Some(close);
    }
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
    let manifest_shutdown_result =
        if let Some(prepared_runtime_close) = prepared_runtime_close.as_ref() {
            let manifest_shutdown_day = prepared_runtime_close.identity.utc_day.clone();
            let gaps = source_gaps
                .lock()
                .expect("source gap mutex poisoned")
                .reasons_for_day(&manifest_shutdown_day);
            let (manifest_shutdown_tx, manifest_shutdown_rx) = oneshot::channel();
            let manifest_shutdown_command = ManifestCommand::Shutdown {
                close: prepared_runtime_close.identity.clone(),
                disposition: if prepared_runtime_close.unclean {
                    ManifestShutdownDisposition::UncleanEndUnknown
                } else {
                    ManifestShutdownDisposition::EndedKnown {
                        ended_utc_ms: prepared_runtime_close.ended_utc_ms,
                    }
                },
                gaps,
                operational_counters: source_queue_backpressure.drain_all_operational(),
                reply: manifest_shutdown_tx,
            };
            match timeout(
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
            }
        } else if rollover_outcome_uncertain {
            manifest_owner_handle.abort();
            match (&mut manifest_owner_handle).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(io::Error::other(format!(
                    "daily manifest owner failed while durable outcome was uncertain: {error}"
                ))),
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(io::Error::other(format!(
                    "daily manifest owner join failed while durable outcome was uncertain: {error}"
                ))),
            }
        } else {
            match timeout(
                coordinated_shutdown_deadline.saturating_duration_since(Instant::now()),
                &mut manifest_owner_handle,
            )
            .await
            {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(io::Error::other(format!(
                    "daily manifest owner failed after rollover exit: {error}"
                ))),
                Ok(Err(error)) => Err(io::Error::other(format!(
                    "daily manifest owner join failed after rollover exit: {error}"
                ))),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "daily manifest owner rollover-exit join timeout",
                )),
            }
        };
    if let Err(error) = manifest_shutdown_result {
        abort_manifest_owner_if_running(&mut manifest_owner_handle, coordinated_shutdown_deadline)
            .await;
        let message = format!("daily_manifest_shutdown_failed:{error}");
        record_gap(&source_gaps, message.clone());
        append_runtime_failure(&mut runtime_failure, message);
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
        append_runtime_failure(&mut runtime_failure, message);
    }
    if operator_shutdown && runtime_failure.is_none() {
        eprintln!("coordinated_shutdown_complete");
    }
    match runtime_failure {
        Some(error) => Err(io::Error::other(error).into()),
        None => Ok(TrainingRunSummary {
            finalized_segments,
            source_coverage,
            source_gaps: source_gaps
                .lock()
                .expect("source gap mutex poisoned")
                .all_reasons(),
        }),
    }
}

#[cfg(test)]
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

#[derive(Clone)]
struct CollectorIdentity {
    host_id: String,
    region: String,
}

#[derive(Clone)]
struct TrainingSourceSpecs {
    polymarket: PolymarketSourceSpec,
    rtds: RtdsSourceSpec,
    depth: BinanceDepthSpec,
    aggtrade: BinanceWsSpec,
}

impl TrainingSourceSpecs {
    fn production() -> Self {
        Self {
            polymarket: PolymarketSourceSpec::production(),
            rtds: RtdsSourceSpec::production(),
            depth: BinanceDepthSpec::production(),
            aggtrade: BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS_ENDPOINT.to_owned(),
                frame_inactivity_timeout_secs: 30,
                #[cfg(test)]
                recovery_wait_override: None,
            },
        }
    }
}

pub(crate) struct TrainingSourceEndpoints {
    pub(crate) polymarket_market: String,
    pub(crate) polymarket_rtds: String,
    pub(crate) binance_depth: String,
    pub(crate) binance_aggtrade: String,
}

pub(crate) fn production_training_source_endpoints() -> TrainingSourceEndpoints {
    let specs = TrainingSourceSpecs::production();
    TrainingSourceEndpoints {
        polymarket_market: specs.polymarket.market_ws_endpoint,
        polymarket_rtds: specs.rtds.endpoint,
        binance_depth: specs.depth.websocket_endpoint,
        binance_aggtrade: specs.aggtrade.url,
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daily_manifest::DAILY_MANIFEST_FILE_NAME;
    use crate::writer::sha256_file;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use polymarket_client_sdk_v2::clob::ws::WsMessage;
    use std::fs::File;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    mod binance_aggtrade_source;
    mod binance_depth_source;
    mod dataset_capacity;
    mod dataset_root_lock;
    mod ingress_control_sender;
    mod ingress_deferred;
    mod manifest_owner;
    mod polymarket_event_adapter;
    mod polymarket_source;
    mod producer_day_barrier;
    mod raw_day;
    mod raw_ingress_admission;
    mod rollover_coordinator;
    mod rtds_source;
    mod runtime_gaps;
    mod runtime_ingress;
    mod runtime_rollover_driver;
    mod runtime_shutdown;
    mod source_ingress_state;
    mod source_task_supervision;
    mod source_topology;
    mod startup_coordinator;
    mod startup_recovery;
    mod writer_supervisor;

    use manifest_owner::*;

    fn delegate_resource_test_to_isolated_process(test_name: &str) -> bool {
        const ISOLATED_TEST_ENV: &str = "DATA_COLLECT_ISOLATED_RESOURCE_TEST";
        if std::env::var(ISOLATED_TEST_ENV).as_deref() == Ok(test_name) {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([test_name, "--exact", "--test-threads=1"])
            .env(ISOLATED_TEST_ENV, test_name)
            .status()
            .expect("isolated resource test process must start");
        assert!(
            status.success(),
            "isolated resource test failed: {test_name}"
        );
        true
    }

    struct StartupPollProbe {
        polls: Arc<AtomicU64>,
    }

    impl Future for StartupPollProbe {
        type Output = Result<()>;

        fn poll(
            self: Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        }
    }

    type StartupProducerProbe = (Arc<AtomicU64>, tokio::task::JoinHandle<Result<()>>);

    fn startup_gate_fixture() -> (
        mpsc::Receiver<StartupParticipant>,
        StartupReleaseHandles,
        Vec<StartupProducerProbe>,
        Vec<oneshot::Receiver<StartupReleasePhase>>,
    ) {
        let (ready_tx, ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);
        let mut producer_releases = Vec::new();
        let mut producer_probes = Vec::new();
        for producer in LOGICAL_PRODUCERS {
            let participant = StartupParticipant::producer(producer);
            let (release_tx, release_rx) = oneshot::channel();
            let polls = Arc::new(AtomicU64::new(0));
            let handle = tokio::spawn(run_producer_after_startup_gate(
                participant,
                ready_tx.clone(),
                release_rx,
                StartupPollProbe {
                    polls: Arc::clone(&polls),
                },
            ));
            producer_releases.push(StartupReleaseSender {
                participant,
                sender: release_tx,
            });
            producer_probes.push((polls, handle));
        }

        let mut writer_releases = Vec::new();
        let mut writer_receivers = Vec::new();
        for source_kind in TRAINING_SOURCE_KINDS {
            let participant = StartupParticipant::writer(source_kind);
            ready_tx
                .try_send(participant)
                .expect("ten-slot startup ready channel must hold all mandatory readiness");
            let (release_tx, release_rx) = oneshot::channel();
            writer_releases.push(StartupReleaseSender {
                participant,
                sender: release_tx,
            });
            writer_receivers.push(release_rx);
        }
        drop(ready_tx);
        (
            ready_rx,
            StartupReleaseHandles {
                writers: writer_releases,
                producers: producer_releases,
            },
            producer_probes,
            writer_receivers,
        )
    }

    fn test_rollover_close(boundary_utc_ms: i64) -> RawDayClose {
        RawDayClose {
            utc_day: utc_day_string_from_ms(boundary_utc_ms - 1).unwrap(),
            next_utc_day: utc_day_string_from_ms(boundary_utc_ms).unwrap(),
            generation: INITIAL_RAW_DAY_GENERATION,
            next_generation: INITIAL_RAW_DAY_GENERATION + 1,
            close_id: 1,
            boundary_utc_ms,
        }
    }

    fn commit_test_manifest_activation(gate: &RawDayGate, close: &RawDayClose) {
        assert!(
            gate.commit_manifest_activation(close, None, || Ok(()))
                .unwrap()
        );
    }

    fn prepare_rollover_for_producer(
        gate: &RawDayGate,
        producer: &str,
        utc_day: &str,
        boundary_utc_ms: i64,
    ) -> RawDayClose {
        let close = gate.prepare_rollover(utc_day, boundary_utc_ms).unwrap();
        for other in LOGICAL_PRODUCERS {
            if *other != producer {
                gate.register_source_fence(other, &close).unwrap();
            }
        }
        close
    }

    async fn install_active_raw_day_gate(
        stats: &Arc<SourceQueueBackpressureStats>,
        recv_time_utc_ms: i64,
    ) -> Arc<RawDayGate> {
        let gate = Arc::new(RawDayGate::new());
        gate.activate_startup(
            utc_day_string_from_ms(recv_time_utc_ms).unwrap(),
            INITIAL_RAW_DAY_GENERATION,
        )
        .await
        .unwrap();
        stats.install_raw_day_gate(Arc::clone(&gate)).unwrap();
        gate
    }

    async fn active_raw_day_gate_for_test(recv_time_utc_ms: i64) -> Arc<RawDayGate> {
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, recv_time_utc_ms).await
    }

    async fn accepted_raw_day_ticket_from_gate_for_test(
        gate: &Arc<RawDayGate>,
        recv_time_utc_ms: i64,
    ) -> RawDayAcceptedTicket {
        gate.accept_unadmitted_event_ticket("polymarket", recv_time_utc_ms)
            .await
            .unwrap()
            .expect("test ticket must precede controlled cutoff")
    }

    async fn accepted_raw_day_ticket_for_test(recv_time_utc_ms: i64) -> RawDayAcceptedTicket {
        let gate = active_raw_day_gate_for_test(recv_time_utc_ms).await;
        accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await
    }

    fn accepted_raw_day_ticket_for_sync_test(recv_time_utc_ms: i64) -> RawDayAcceptedTicket {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(accepted_raw_day_ticket_for_test(recv_time_utc_ms))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_pre_rename_manifest_failure_keeps_disk_unmodified_and_converges() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let started_utc_ms = 1_783_987_201_000;
        let utc_day = utc_day_string_from_ms(started_utc_ms).unwrap();
        let (owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
        let manifest_path = day_dir.join(DAILY_MANIFEST_FILE_NAME);
        let manifest_before = fs::read(&manifest_path).unwrap();
        let digest_before = manifest_file_digest(&day_dir);
        let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());

        let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
        let owner_lease = Arc::clone(&dataset_lease);
        let (owner_result_tx, owner_result_rx) = oneshot::channel();
        let mut manifest_owner_handle = tokio::spawn(async move {
            let _dataset_lease = owner_lease;
            let result = run_manifest_owner(owner, manifest_rx, rollover_ready_tx).await;
            let _ = owner_result_tx.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
            result
        });

        let raw_day_gate = Arc::new(RawDayGate::new());
        let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
        runtime_stats
            .install_raw_day_gate(Arc::clone(&raw_day_gate))
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel(STARTUP_READY_CHANNEL_CAPACITY);
        let (mut writer_handles, writer_releases) = start_writer_workers_gated(
            &dataset_root,
            &test_runtime_provenance("startup-pre-rename-manifest-failure"),
            &manifest_tx,
            &runtime_stats,
            &ready_tx,
            Arc::clone(&raw_day_gate),
            Some(Arc::clone(&dataset_lease)),
        )
        .unwrap();

        let mut source_handles = Vec::new();
        let mut producer_releases = Vec::new();
        let mut producer_polls = Vec::new();
        for (producer, source_kind) in [
            ("polymarket", "polymarket_market"),
            ("polymarket_rtds", "polymarket_rtds_chainlink_btc_usd"),
            ("binance_depth", "binance_depth_update"),
            ("binance_aggtrade", "binance_aggtrade"),
        ] {
            let participant = StartupParticipant::producer(producer);
            let (release_tx, release_rx) = oneshot::channel();
            let polls = Arc::new(AtomicU64::new(0));
            source_handles.push(SourceTaskHandle::spawn_with_dataset_lease(
                source_kind,
                run_producer_after_startup_gate(
                    participant,
                    ready_tx.clone(),
                    release_rx,
                    StartupPollProbe {
                        polls: Arc::clone(&polls),
                    },
                ),
                SourceTaskObservationCell::default(),
                Arc::clone(&raw_day_gate),
                Arc::clone(&dataset_lease),
            ));
            producer_releases.push(StartupReleaseSender {
                participant,
                sender: release_tx,
            });
            producer_polls.push(polls);
        }
        drop(ready_tx);

        let releases = StartupReleaseHandles {
            writers: writer_releases,
            producers: producer_releases,
        };
        let temporary_path = day_dir.join(format!("{DAILY_MANIFEST_FILE_NAME}.tmp"));
        let error = timeout(
            Duration::from_secs(2),
            activate_and_release_startup(
                &manifest_tx,
                Arc::clone(&raw_day_gate),
                ready_rx,
                releases,
                utc_day,
                ManifestActivationAckMode::Send,
                None,
                || {
                    fs::create_dir(&temporary_path).unwrap();
                    started_utc_ms
                },
            ),
        )
        .await
        .expect("real startup participants must reach the activation attempt")
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("daily manifest activation failed")
        );
        assert!(matches!(
            raw_day_gate.current_state(),
            RawDayGateState::Inactive
        ));
        assert!(
            raw_day_gate
                .startup_activation_committed
                .lock()
                .unwrap()
                .is_none()
        );
        assert!(raw_day_gate.activation_committed.lock().unwrap().is_none());
        for polls in &producer_polls {
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
        assert!(
            timeout(Duration::from_secs(1), owner_result_rx)
                .await
                .expect("manifest owner must return after the failed pre-rename publish")
                .unwrap()
                .is_err()
        );
        timeout(Duration::from_secs(1), async {
            while source_handles.iter().any(|handle| !handle.is_finished()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all producers must stop without polling after activation failure");
        let writer_outcome = stop_writer_workers_with_budget(
            std::mem::take(&mut writer_handles),
            Duration::from_secs(1),
        )
        .await;
        assert!(writer_outcome.all_terminal, "{:?}", writer_outcome.errors);
        assert!(writer_outcome.outputs.is_empty());
        for source_kind in TRAINING_SOURCE_KINDS {
            assert!(writer_outcome.errors.iter().any(|error| {
                error.contains(source_kind)
                    && error.contains("startup activation gate closed before writer release")
            }));
        }

        let (watcher_dropped_tx, watcher_dropped_rx) = oneshot::channel::<()>();
        let mut runtime_exit_watcher = RuntimeExitWatcher::new(tokio::spawn(async move {
            let _watcher_dropped_tx = watcher_dropped_tx;
            std::future::pending::<()>().await;
        }));
        converge_spawned_runtime_resources(
            &mut source_handles,
            &mut writer_handles,
            &mut manifest_owner_handle,
            &mut runtime_exit_watcher,
            None,
            None,
            Instant::now() + Duration::from_secs(2),
        )
        .await;
        drop(dataset_lease);

        assert!(source_handles.is_empty());
        assert!(writer_handles.is_empty());
        assert!(manifest_owner_handle.is_finished());
        assert!(runtime_exit_watcher.join.is_none());
        assert!(watcher_dropped_rx.await.is_err());
        for polls in producer_polls {
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
        assert!(temporary_path.is_dir());
        assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(manifest_file_digest(&day_dir), digest_before);
        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_eq!(manifest["state"], "open");
        assert_eq!(manifest["collection_day_coverage"]["state"], "not_started");
        assert!(manifest["collection_day_coverage"]["first_started_utc_ms"].is_null());
        assert!(manifest["collection_day_coverage"]["last_started_utc_ms"].is_null());
        assert!(manifest["collection_day_coverage"]["ended_utc_ms"].is_null());
        assert_eq!(
            manifest["collection_day_coverage"]["reactivated_within_day"],
            false
        );
        assert!(lock_training_dataset_root(&dataset_root).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_after_startup_commit_stops_unreleased_participants_and_closes_known() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let started_utc_ms = 1_783_987_201_000;
        let ended_utc_ms = started_utc_ms + 1_000;
        let utc_day = utc_day_string_from_ms(started_utc_ms).unwrap();
        let (mut owner, day_dir) = test_manifest_owner(&dataset_root, &utc_day);
        let raw_day_gate = Arc::new(RawDayGate::new());
        let deadline = RuntimeDeadlineCandidate {
            deadline: Instant::now() + Duration::from_millis(10),
            observed_utc_ms: ended_utc_ms,
        };
        raw_day_gate
            .commit_startup_manifest_activation(
                &utc_day,
                INITIAL_RAW_DAY_GENERATION,
                Some(deadline),
                || owner.activate_initial(&utc_day, started_utc_ms, INITIAL_RAW_DAY_GENERATION),
            )
            .unwrap();
        sleep(Duration::from_millis(15)).await;

        let (mut ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();
        collect_startup_ready(&mut ready_rx, &expected_startup_participants())
            .await
            .unwrap();
        let outcome = raw_day_gate
            .activate_or_stop_startup_after_manifest(
                utc_day.clone(),
                INITIAL_RAW_DAY_GENERATION,
                Some(deadline),
                releases,
            )
            .await
            .unwrap();
        let StartupActivationOutcome::CommittedStopped(PreparedRuntimeExit::SameDay(close)) =
            outcome
        else {
            panic!("a post-commit in-day deadline must stop the committed generation");
        };
        assert_eq!(close.ended_utc_ms, ended_utc_ms);
        assert_eq!(close.cause, RuntimeCloseCause::RunDeadline);

        for receiver in writer_receivers {
            assert_eq!(receiver.await.unwrap(), StartupReleasePhase::Run);
        }
        for (polls, handle) in producer_probes {
            let error = handle.await.unwrap().unwrap_err();
            let stop = error
                .downcast_ref::<CoordinatedSourceStop>()
                .expect("startup stop must use the typed coordinated-stop proof");
            assert_eq!(stop.close, close.identity);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }

        owner
            .shutdown(
                &close.identity,
                &ManifestShutdownDisposition::EndedKnown { ended_utc_ms },
                &[],
                &BTreeMap::new(),
            )
            .unwrap();
        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_eq!(manifest["collection_day_coverage"]["state"], "ended_known");
        assert_eq!(
            manifest["collection_day_coverage"]["ended_utc_ms"],
            ended_utc_ms
        );
        assert_eq!(
            manifest["collection_day_coverage"]["reactivated_within_day"],
            false
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn boundary_deadline_after_startup_commit_runs_only_writers_for_pending_exit() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let boundary_utc_ms = 1_783_468_800_000;
        let close = test_rollover_close(boundary_utc_ms);
        let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
        let raw_day_gate = Arc::new(RawDayGate::new());
        let deadline = RuntimeDeadlineCandidate {
            deadline: Instant::now() + Duration::from_millis(10),
            observed_utc_ms: boundary_utc_ms,
        };
        raw_day_gate
            .commit_startup_manifest_activation(
                &close.utc_day,
                close.generation,
                Some(deadline),
                || {
                    owner.activate_initial(
                        &close.utc_day,
                        boundary_utc_ms - 1_000,
                        close.generation,
                    )
                },
            )
            .unwrap();
        sleep(Duration::from_millis(15)).await;

        let (mut ready_rx, releases, producer_probes, writer_receivers) = startup_gate_fixture();
        collect_startup_ready(&mut ready_rx, &expected_startup_participants())
            .await
            .unwrap();
        let outcome = raw_day_gate
            .activate_or_stop_startup_after_manifest(
                close.utc_day.clone(),
                close.generation,
                Some(deadline),
                releases,
            )
            .await
            .unwrap();
        let StartupActivationOutcome::CommittedStopped(PreparedRuntimeExit::Rollover(exit)) =
            outcome
        else {
            panic!("a boundary deadline must prepare a startup PendingExit");
        };
        assert_eq!(exit.close, close);

        for receiver in writer_receivers {
            assert_eq!(receiver.await.unwrap(), StartupReleasePhase::Run);
        }
        for (polls, handle) in producer_probes {
            let error = handle.await.unwrap().unwrap_err();
            let stop = error
                .downcast_ref::<CoordinatedSourceStop>()
                .expect("startup boundary stop must be typed");
            assert_eq!(
                stop.close,
                RuntimeCloseIdentity::from_rollover_old_day(&close)
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }

        for producer in LOGICAL_PRODUCERS {
            raw_day_gate
                .register_source_fence(producer, &close)
                .unwrap();
            owner
                .record_producer_barrier(
                    close.clone(),
                    (*producer).to_owned(),
                    &[],
                    &BTreeMap::new(),
                )
                .unwrap();
        }
        raw_day_gate.seal_prepared_rollover(&close).await.unwrap();
        for source in TRAINING_SOURCE_KINDS {
            owner
                .record_writer_ack(close.clone(), (*source).to_owned())
                .unwrap();
        }
        owner
            .commit_rollover_old_day(
                &close,
                ManifestRolloverCommitMode::PendingExitKnown {
                    ended_utc_ms: boundary_utc_ms,
                },
            )
            .unwrap();
        owner.prepare_rollover_exit_day(&close).unwrap();

        let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
        assert_eq!(
            old_manifest["collection_day_coverage"]["state"],
            "ended_known"
        );
        let next_day_dir =
            typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
        let next_manifest = load_day_manifest(&next_day_dir).unwrap().unwrap();
        assert_eq!(
            next_manifest["collection_day_coverage"]["state"],
            "not_started"
        );
        assert_eq!(
            next_manifest["collection_day_coverage"]["reactivated_within_day"],
            false
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_terminal_and_activation_have_one_generation_owner_in_both_orders() {
        let boundary_utc_ms = now_ms().div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
        let old_day = utc_day_string_from_ms(boundary_utc_ms - 1).unwrap();

        let terminal_first_gate = Arc::new(RawDayGate::new());
        terminal_first_gate
            .activate_startup(old_day.clone(), INITIAL_RAW_DAY_GENERATION)
            .await
            .unwrap();
        let terminal_first_close = terminal_first_gate
            .close_for_rollover(&old_day, boundary_utc_ms)
            .await
            .unwrap();
        let terminal_first_handle = SourceTaskHandle::spawn(
            "binance_aggtrade",
            async { Ok(()) },
            SourceTaskObservationCell::default(),
            Arc::clone(&terminal_first_gate),
        );
        let terminal_first_report = terminal_first_handle
            .await_termination(&RuntimeCloseIdentity::from_rollover_old_day(
                &terminal_first_close,
            ))
            .await;
        assert_eq!(
            terminal_first_report.kind,
            SourceTaskTerminationKind::ReliableReturn
        );
        assert_eq!(
            terminal_first_report.epoch,
            Some(RuntimeEpoch {
                utc_day: terminal_first_close.utc_day.clone(),
                generation: terminal_first_close.generation,
            })
        );
        let terminal_first_mutations = AtomicU64::new(0);
        assert!(
            !terminal_first_gate
                .commit_manifest_activation(&terminal_first_close, None, || {
                    terminal_first_mutations.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap()
        );
        assert_eq!(terminal_first_mutations.load(Ordering::SeqCst), 0);
        assert!(matches!(
            terminal_first_gate.prepared_runtime_exit(),
            Some(PreparedRuntimeExit::Rollover(_))
        ));

        let activation_first_gate = Arc::new(RawDayGate::new());
        activation_first_gate
            .activate_startup(old_day.clone(), INITIAL_RAW_DAY_GENERATION)
            .await
            .unwrap();
        let activation_first_close = activation_first_gate
            .close_for_rollover(&old_day, boundary_utc_ms)
            .await
            .unwrap();
        let activation_first_mutations = AtomicU64::new(0);
        assert!(
            activation_first_gate
                .commit_manifest_activation(&activation_first_close, None, || {
                    activation_first_mutations.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap()
        );
        activation_first_gate
            .activate_after_rollover(&activation_first_close)
            .await
            .unwrap();
        let activation_first_handle = SourceTaskHandle::spawn(
            "binance_aggtrade",
            async { Ok(()) },
            SourceTaskObservationCell::default(),
            Arc::clone(&activation_first_gate),
        );
        while !activation_first_handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let PreparedRuntimeExit::SameDay(new_day_close) = activation_first_gate
            .prepared_runtime_exit()
            .expect("activation-first terminal must close the new generation")
        else {
            panic!("activation-first terminal must belong to the new day");
        };
        let activation_first_report = activation_first_handle
            .await_termination(&new_day_close.identity)
            .await;
        assert_eq!(
            activation_first_report.kind,
            SourceTaskTerminationKind::ReliableReturn
        );
        assert_eq!(
            activation_first_report.epoch,
            Some(RuntimeEpoch {
                utc_day: activation_first_close.next_utc_day.clone(),
                generation: activation_first_close.next_generation,
            })
        );
        assert_eq!(activation_first_mutations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn websocket_heartbeat_configs_are_channel_specific() {
        let clob = bounded_ws_config();
        assert_eq!(
            clob.connect_timeout,
            Some(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS))
        );
        assert_eq!(clob.reconnect.max_attempts, Some(3));
        assert_eq!(clob.heartbeat_interval, Duration::from_secs(10));
        assert!(clob.require_text_pong);
        assert_eq!(clob.heartbeat_timeout, Duration::from_secs(20));

        let rtds = bounded_rtds_ws_config();
        assert_eq!(
            rtds.connect_timeout,
            Some(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS))
        );
        assert_eq!(rtds.reconnect.max_attempts, None);
        assert_eq!(rtds.reconnect.max_backoff, Duration::from_secs(5));
        assert_eq!(rtds.heartbeat_interval, Duration::from_secs(5));
        assert!(!rtds.require_text_pong);
        assert_eq!(rtds.heartbeat_timeout, Duration::from_secs(15));
    }

    #[test]
    fn source_specs_keep_production_endpoints_and_timings_private() {
        let polymarket = PolymarketSourceSpec::production();
        assert_eq!(
            polymarket.gamma_endpoint,
            "https://gamma-api.polymarket.com"
        );
        assert_eq!(polymarket.market_ws_endpoint, MARKET_WS_ENDPOINT);
        assert_eq!(
            polymarket.discovery_wait,
            Duration::from_secs(DEFAULT_DISCOVERY_WAIT_SECS)
        );
        assert_eq!(polymarket.discovery_retry_delay, Duration::from_secs(10));
        assert_eq!(polymarket.recovery_wait_override, None);

        let rtds = RtdsSourceSpec::production();
        assert_eq!(rtds.endpoint, RTDS_WS_ENDPOINT);
        assert_eq!(rtds.ws_config.reconnect.max_attempts, None);
        assert_eq!(
            rtds.ws_config.connect_timeout,
            Some(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS))
        );

        let runtime = TrainingSourceSpecs::production();
        assert_eq!(runtime.polymarket.gamma_endpoint, polymarket.gamma_endpoint);
        assert_eq!(runtime.rtds.endpoint, rtds.endpoint);
        assert_eq!(runtime.depth.websocket_endpoint, BINANCE_DEPTH_WS);
        assert_eq!(
            runtime.depth.snapshot_endpoint,
            BINANCE_DEPTH_SNAPSHOT_ENDPOINT
        );
        assert_eq!(runtime.aggtrade.url, BINANCE_AGGTRADE_WS_ENDPOINT);
    }

    #[derive(Default)]
    struct GammaRecoveryLoopbackCounters {
        gamma_attempts: AtomicU64,
        gamma_successes: AtomicU64,
        clob_subscriptions: AtomicU64,
        gamma_fd_samples: Mutex<Vec<usize>>,
    }

    async fn gamma_recovery_loopback(
        failures: u64,
        counters: Arc<GammaRecoveryLoopbackCounters>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 4_096];
                let Ok(read) = socket.read(&mut request).await else {
                    continue;
                };
                if read == 0 {
                    continue;
                }
                let attempt = counters.gamma_attempts.fetch_add(1, Ordering::Relaxed) + 1;
                counters
                    .gamma_fd_samples
                    .lock()
                    .unwrap()
                    .push(fs::read_dir("/proc/self/fd").unwrap().count());
                let (status, body) = if attempt <= failures {
                    ("503 Service Unavailable", "[]".to_owned())
                } else {
                    counters.gamma_successes.fetch_add(1, Ordering::Relaxed);
                    let windows = discovery_windows(now_ms());
                    ("200 OK", gamma_market_response_for_window(&windows.current))
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (endpoint, server)
    }

    async fn silent_clob_loopback(
        counters: Arc<GammaRecoveryLoopbackCounters>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(mut socket) = accept_async(stream).await else {
                    continue;
                };
                while let Some(frame) = socket.next().await {
                    match frame {
                        Ok(Message::Text(text)) if text == "PING" => {
                            match socket.send(Message::Text("PONG".into())).await {
                                Ok(()) => {}
                                Err(_) => break,
                            }
                        }
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<Value>(&text).ok() {
                                Some(request) if request["operation"] == "subscribe" => {
                                    counters.clob_subscriptions.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
                while let Some(frame) = socket.next().await {
                    match frame {
                        Ok(Message::Text(text)) if text == "PING" => {
                            match socket.send(Message::Text("PONG".into())).await {
                                Ok(()) => {}
                                Err(_) => break,
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
        });
        (endpoint, server)
    }

    fn tune_composed_specs(specs: &mut TrainingSourceSpecs) {
        specs.polymarket.discovery_wait = Duration::ZERO;
        specs.polymarket.discovery_retry_delay = Duration::from_millis(1);
        specs.polymarket.recovery_wait_override = Some(Duration::from_millis(1));
        specs.polymarket.ws_config.connect_timeout = Some(Duration::from_millis(500));
        specs.polymarket.ws_config.reconnect.initial_backoff = Duration::from_millis(1);
        specs.polymarket.ws_config.reconnect.max_backoff = Duration::from_millis(1);
        specs.polymarket.ws_config.heartbeat_interval = Duration::from_millis(20);
        specs.polymarket.ws_config.heartbeat_timeout = Duration::from_millis(100);
        specs.rtds.ws_config.connect_timeout = Some(Duration::from_millis(500));
        specs.rtds.ws_config.reconnect.initial_backoff = Duration::from_millis(1);
        specs.rtds.ws_config.reconnect.max_backoff = Duration::from_millis(1);
        specs.rtds.ws_config.reconnect.backoff_multiplier = 1.0;
        specs.rtds.ws_config.heartbeat_interval = Duration::from_millis(20);
        specs.rtds.ws_config.heartbeat_timeout = Duration::from_millis(100);
        specs.depth.frame_inactivity_timeout = Duration::from_millis(100);
        specs.depth.recovery_wait_override = Some(Duration::from_millis(1));
        specs.aggtrade.frame_inactivity_timeout_secs = 1;
        specs.aggtrade.recovery_wait_override = Some(Duration::from_millis(1));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ComposedIoBehavior {
        Healthy,
        Unavailable,
        Silent,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ComposedIoPath {
        Gamma,
        Clob,
        Rtds,
        DepthSnapshot,
        DepthWebsocket,
        AggTrade,
    }

    #[derive(Clone, Copy, Debug)]
    struct ComposedLoopbackMode {
        gamma: ComposedIoBehavior,
        clob: ComposedIoBehavior,
        rtds: ComposedIoBehavior,
        depth_snapshot: ComposedIoBehavior,
        depth_websocket: ComposedIoBehavior,
        aggtrade: ComposedIoBehavior,
    }

    impl ComposedLoopbackMode {
        fn healthy() -> Self {
            Self {
                gamma: ComposedIoBehavior::Healthy,
                clob: ComposedIoBehavior::Healthy,
                rtds: ComposedIoBehavior::Healthy,
                depth_snapshot: ComposedIoBehavior::Healthy,
                depth_websocket: ComposedIoBehavior::Healthy,
                aggtrade: ComposedIoBehavior::Healthy,
            }
        }

        fn with_unavailable(paths: &[ComposedIoPath]) -> Self {
            let mut mode = Self::healthy();
            for path in paths {
                match path {
                    ComposedIoPath::Gamma => mode.gamma = ComposedIoBehavior::Unavailable,
                    ComposedIoPath::Clob => mode.clob = ComposedIoBehavior::Unavailable,
                    ComposedIoPath::Rtds => mode.rtds = ComposedIoBehavior::Unavailable,
                    ComposedIoPath::DepthSnapshot => {
                        mode.depth_snapshot = ComposedIoBehavior::Unavailable;
                    }
                    ComposedIoPath::DepthWebsocket => {
                        mode.depth_websocket = ComposedIoBehavior::Unavailable;
                    }
                    ComposedIoPath::AggTrade => {
                        mode.aggtrade = ComposedIoBehavior::Unavailable;
                    }
                }
            }
            mode
        }

        fn all_unavailable() -> Self {
            Self::with_unavailable(&[
                ComposedIoPath::Gamma,
                ComposedIoPath::Clob,
                ComposedIoPath::Rtds,
                ComposedIoPath::DepthSnapshot,
                ComposedIoPath::DepthWebsocket,
                ComposedIoPath::AggTrade,
            ])
        }

        fn stream_silent() -> Self {
            Self {
                gamma: ComposedIoBehavior::Healthy,
                clob: ComposedIoBehavior::Silent,
                rtds: ComposedIoBehavior::Silent,
                depth_snapshot: ComposedIoBehavior::Healthy,
                depth_websocket: ComposedIoBehavior::Silent,
                aggtrade: ComposedIoBehavior::Silent,
            }
        }

        fn behavior(self, path: ComposedIoPath) -> ComposedIoBehavior {
            match path {
                ComposedIoPath::Gamma => self.gamma,
                ComposedIoPath::Clob => self.clob,
                ComposedIoPath::Rtds => self.rtds,
                ComposedIoPath::DepthSnapshot => self.depth_snapshot,
                ComposedIoPath::DepthWebsocket => self.depth_websocket,
                ComposedIoPath::AggTrade => self.aggtrade,
            }
        }
    }

    #[derive(Default)]
    struct ComposedLoopbackCounters {
        gamma_requests: AtomicU64,
        clob_connections: AtomicU64,
        clob_subscriptions: AtomicU64,
        clob_business_sent: AtomicU64,
        rtds_connections: AtomicU64,
        rtds_subscriptions: AtomicU64,
        rtds_business_sent: AtomicU64,
        depth_snapshot_requests: AtomicU64,
        depth_snapshot_sent: AtomicU64,
        depth_connections: AtomicU64,
        depth_business_sent: AtomicU64,
        aggtrade_connections: AtomicU64,
        aggtrade_business_sent: AtomicU64,
    }

    struct ComposedLoopbackFixture {
        specs: TrainingSourceSpecs,
        mode: ComposedLoopbackMode,
        counters: Arc<ComposedLoopbackCounters>,
        servers: Vec<tokio::task::JoinHandle<()>>,
    }

    async fn read_loopback_http_request(socket: &mut tokio::net::TcpStream) -> Option<String> {
        let mut request = [0_u8; 4_096];
        let read = socket.read(&mut request).await.ok()?;
        (read > 0).then(|| String::from_utf8_lossy(&request[..read]).into_owned())
    }

    async fn write_loopback_http_response(
        socket: &mut tokio::net::TcpStream,
        status: &str,
        body: &str,
    ) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
    }

    async fn keep_binance_loopback_alive(
        socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) {
        loop {
            sleep(Duration::from_millis(20)).await;
            if socket
                .send(Message::Ping(Vec::<u8>::new().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    }

    async fn keep_text_heartbeat_loopback_alive(
        socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) {
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(Message::Text(text)) if text == "PING" => {
                    match socket.send(Message::Text("PONG".into())).await {
                        Ok(()) => {}
                        Err(_) => return,
                    }
                }
                Ok(Message::Close(_)) | Err(_) => return,
                _ => {}
            }
        }
    }

    impl ComposedLoopbackFixture {
        async fn spawn(mode: ComposedLoopbackMode) -> Self {
            let counters = Arc::new(ComposedLoopbackCounters::default());
            let mut servers = Vec::new();

            let gamma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let gamma_endpoint = format!("http://{}", gamma_listener.local_addr().unwrap());
            let gamma_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = gamma_listener.accept().await else {
                        return;
                    };
                    let Some(request) = read_loopback_http_request(&mut socket).await else {
                        continue;
                    };
                    gamma_counters
                        .gamma_requests
                        .fetch_add(1, Ordering::Relaxed);
                    if mode.gamma == ComposedIoBehavior::Unavailable {
                        write_loopback_http_response(&mut socket, "503 Service Unavailable", "[]")
                            .await;
                    } else {
                        let windows = discovery_windows(now_ms());
                        let is_next = request.contains(&windows.next.slug);
                        let body = if is_next {
                            "[]".to_owned()
                        } else {
                            gamma_market_response_for_window(&windows.current)
                        };
                        write_loopback_http_response(&mut socket, "200 OK", &body).await;
                    }
                }
            }));

            let clob_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let clob_endpoint = format!("ws://{}", clob_listener.local_addr().unwrap());
            let clob_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = clob_listener.accept().await else {
                        return;
                    };
                    clob_counters
                        .clob_connections
                        .fetch_add(1, Ordering::Relaxed);
                    if mode.clob == ComposedIoBehavior::Unavailable {
                        drop(stream);
                        continue;
                    }
                    let Ok(mut socket) = accept_async(stream).await else {
                        continue;
                    };
                    let mut subscribed = false;
                    while let Some(frame) = socket.next().await {
                        match frame {
                            Ok(Message::Text(text)) if text == "PING" => {
                                match socket.send(Message::Text("PONG".into())).await {
                                    Ok(()) => {}
                                    Err(_) => break,
                                }
                            }
                            Ok(Message::Text(text)) => {
                                match serde_json::from_str::<Value>(&text).ok() {
                                    Some(request) if request["operation"] == "subscribe" => {
                                        clob_counters
                                            .clob_subscriptions
                                            .fetch_add(1, Ordering::Relaxed);
                                        subscribed = true;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    if !subscribed {
                        continue;
                    }
                    if mode.clob == ComposedIoBehavior::Healthy
                        && socket
                            .send(Message::Text(
                                json!({
                                    "event_type": "book",
                                    "asset_id": "101",
                                    "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                                    "timestamp": now_ms().to_string(),
                                    "bids": [],
                                    "asks": []
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_ok()
                    {
                        clob_counters
                            .clob_business_sent
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    keep_text_heartbeat_loopback_alive(&mut socket).await;
                }
            }));

            let rtds_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let rtds_endpoint = format!("ws://{}", rtds_listener.local_addr().unwrap());
            let rtds_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = rtds_listener.accept().await else {
                        return;
                    };
                    rtds_counters
                        .rtds_connections
                        .fetch_add(1, Ordering::Relaxed);
                    if mode.rtds == ComposedIoBehavior::Unavailable {
                        drop(stream);
                        continue;
                    }
                    let Ok(mut socket) = accept_async(stream).await else {
                        continue;
                    };
                    let mut subscribed = false;
                    while let Some(frame) = socket.next().await {
                        match frame {
                            Ok(Message::Text(text)) if text == "PING" => {
                                match socket.send(Message::Text("PONG".into())).await {
                                    Ok(()) => {}
                                    Err(_) => break,
                                }
                            }
                            Ok(Message::Text(text))
                                if serde_json::from_str::<Value>(&text).is_ok() =>
                            {
                                rtds_counters
                                    .rtds_subscriptions
                                    .fetch_add(1, Ordering::Relaxed);
                                subscribed = true;
                                break;
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    if !subscribed {
                        continue;
                    }
                    if mode.rtds == ComposedIoBehavior::Healthy {
                        let timestamp = now_ms();
                        if socket
                            .send(Message::Text(
                                json!({
                                    "topic": "crypto_prices_chainlink",
                                    "type": "update",
                                    "timestamp": timestamp,
                                    "payload": {
                                        "symbol": "btc/usd",
                                        "timestamp": timestamp,
                                        "value": "65000.10"
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_ok()
                        {
                            rtds_counters
                                .rtds_business_sent
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    keep_text_heartbeat_loopback_alive(&mut socket).await;
                }
            }));

            let snapshot_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let snapshot_endpoint = format!(
                "http://{}/api/v3/depth",
                snapshot_listener.local_addr().unwrap()
            );
            let snapshot_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = snapshot_listener.accept().await else {
                        return;
                    };
                    if read_loopback_http_request(&mut socket).await.is_none() {
                        continue;
                    }
                    snapshot_counters
                        .depth_snapshot_requests
                        .fetch_add(1, Ordering::Relaxed);
                    if mode.depth_snapshot == ComposedIoBehavior::Unavailable {
                        write_loopback_http_response(&mut socket, "503 Service Unavailable", "[]")
                            .await;
                    } else {
                        let body = json!({
                            "lastUpdateId": 100_u64,
                            "bids": [["1", "2"]],
                            "asks": [["3", "4"]]
                        })
                        .to_string();
                        write_loopback_http_response(&mut socket, "200 OK", &body).await;
                        snapshot_counters
                            .depth_snapshot_sent
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));

            let depth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let depth_endpoint = format!("ws://{}", depth_listener.local_addr().unwrap());
            let depth_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = depth_listener.accept().await else {
                        return;
                    };
                    depth_counters
                        .depth_connections
                        .fetch_add(1, Ordering::Relaxed);
                    if mode.depth_websocket == ComposedIoBehavior::Unavailable {
                        drop(stream);
                        continue;
                    }
                    let Ok(mut socket) = accept_async(stream).await else {
                        continue;
                    };
                    if mode.depth_websocket == ComposedIoBehavior::Healthy
                        && mode.depth_snapshot != ComposedIoBehavior::Unavailable
                        && socket
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
                            .is_ok()
                    {
                        depth_counters
                            .depth_business_sent
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    keep_binance_loopback_alive(&mut socket).await;
                }
            }));

            let aggtrade_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let aggtrade_endpoint = format!("ws://{}", aggtrade_listener.local_addr().unwrap());
            let aggtrade_counters = Arc::clone(&counters);
            servers.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = aggtrade_listener.accept().await else {
                        return;
                    };
                    let connection = aggtrade_counters
                        .aggtrade_connections
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    if mode.aggtrade == ComposedIoBehavior::Unavailable {
                        drop(stream);
                        continue;
                    }
                    let Ok(mut socket) = accept_async(stream).await else {
                        continue;
                    };
                    if mode.aggtrade == ComposedIoBehavior::Healthy
                        && socket
                            .send(test_aggtrade_message_for_id(100 + connection))
                            .await
                            .is_ok()
                    {
                        aggtrade_counters
                            .aggtrade_business_sent
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    keep_binance_loopback_alive(&mut socket).await;
                }
            }));

            let mut specs = TrainingSourceSpecs::production();
            specs.polymarket.gamma_endpoint = gamma_endpoint;
            specs.polymarket.market_ws_endpoint = clob_endpoint;
            specs.rtds.endpoint = rtds_endpoint;
            specs.depth.snapshot_endpoint = snapshot_endpoint;
            specs.depth.websocket_endpoint = depth_endpoint;
            specs.aggtrade.url = aggtrade_endpoint;
            tune_composed_specs(&mut specs);
            Self {
                specs,
                mode,
                counters,
                servers,
            }
        }

        fn path_ready(&self, path: ComposedIoPath) -> bool {
            let unavailable_attempts = 2;
            match (path, self.mode.behavior(path)) {
                (ComposedIoPath::Gamma, ComposedIoBehavior::Unavailable) => {
                    self.counters.gamma_requests.load(Ordering::Relaxed) >= unavailable_attempts
                }
                (ComposedIoPath::Gamma, _) => {
                    self.counters.gamma_requests.load(Ordering::Relaxed) >= 1
                }
                (ComposedIoPath::Clob, ComposedIoBehavior::Unavailable) => {
                    self.mode.gamma == ComposedIoBehavior::Unavailable
                        || self.counters.clob_connections.load(Ordering::Relaxed)
                            >= unavailable_attempts
                }
                (ComposedIoPath::Clob, _) => {
                    self.mode.gamma == ComposedIoBehavior::Unavailable
                        || (self.counters.clob_subscriptions.load(Ordering::Relaxed) >= 1
                            && (self.mode.clob == ComposedIoBehavior::Silent
                                || self.counters.clob_business_sent.load(Ordering::Relaxed) >= 1))
                }
                (ComposedIoPath::Rtds, ComposedIoBehavior::Unavailable) => {
                    self.counters.rtds_connections.load(Ordering::Relaxed) >= unavailable_attempts
                }
                (ComposedIoPath::Rtds, _) => {
                    self.counters.rtds_subscriptions.load(Ordering::Relaxed) >= 1
                        && (self.mode.rtds == ComposedIoBehavior::Silent
                            || self.counters.rtds_business_sent.load(Ordering::Relaxed) >= 1)
                }
                (ComposedIoPath::DepthSnapshot, ComposedIoBehavior::Unavailable) => {
                    self.mode.depth_websocket == ComposedIoBehavior::Unavailable
                        || self
                            .counters
                            .depth_snapshot_requests
                            .load(Ordering::Relaxed)
                            >= 1
                }
                (ComposedIoPath::DepthSnapshot, _) => {
                    self.mode.depth_websocket == ComposedIoBehavior::Unavailable
                        || self.counters.depth_snapshot_sent.load(Ordering::Relaxed) >= 1
                }
                (ComposedIoPath::DepthWebsocket, ComposedIoBehavior::Unavailable) => {
                    self.counters.depth_connections.load(Ordering::Relaxed) >= unavailable_attempts
                }
                (ComposedIoPath::DepthWebsocket, _) => {
                    self.counters.depth_connections.load(Ordering::Relaxed) >= 1
                        && (self.mode.depth_websocket == ComposedIoBehavior::Silent
                            || self.mode.depth_snapshot == ComposedIoBehavior::Unavailable
                            || self.counters.depth_business_sent.load(Ordering::Relaxed) >= 1)
                }
                (ComposedIoPath::AggTrade, ComposedIoBehavior::Unavailable) => {
                    self.counters.aggtrade_connections.load(Ordering::Relaxed)
                        >= unavailable_attempts
                }
                (ComposedIoPath::AggTrade, _) => {
                    self.counters.aggtrade_connections.load(Ordering::Relaxed) >= 1
                        && (self.mode.aggtrade == ComposedIoBehavior::Silent
                            || self.counters.aggtrade_business_sent.load(Ordering::Relaxed) >= 1)
                }
            }
        }

        async fn wait_ready(&self, runtime: &std::thread::JoinHandle<Result<TrainingRunSummary>>) {
            timeout(Duration::from_secs(5), async {
                loop {
                    assert!(
                        !runtime.is_finished(),
                        "composed runtime exited before loopback fixture became ready: {:?}",
                        self.mode
                    );
                    if [
                        ComposedIoPath::Gamma,
                        ComposedIoPath::Clob,
                        ComposedIoPath::Rtds,
                        ComposedIoPath::DepthSnapshot,
                        ComposedIoPath::DepthWebsocket,
                        ComposedIoPath::AggTrade,
                    ]
                    .into_iter()
                    .all(|path| self.path_ready(path))
                    {
                        break;
                    }
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("all required composed loopback paths must become observable");
        }

        async fn shutdown(self) {
            for server in self.servers {
                server.abort();
                let _ = server.await;
            }
        }
    }

    fn composed_runtime_config(dataset_root: PathBuf, label: &str) -> TrainingRuntimeConfig {
        TrainingRuntimeConfig {
            dataset_root,
            duration_secs: None,
            run_forever: true,
            collector_host_id: format!("collector-{label}"),
            collector_region: "local-test".to_owned(),
            code_version: format!("stage7-{label}"),
            collector_binary_sha256: "test-binary-sha256".to_owned(),
            source_config_digest: format!("stage7-source-{label}"),
            max_dataset_bytes: u64::MAX,
        }
    }

    fn spawn_composed_runtime_thread(
        config: TrainingRuntimeConfig,
        specs: TrainingSourceSpecs,
        stop_rx: oneshot::Receiver<()>,
    ) -> std::thread::JoinHandle<Result<TrainingRunSummary>> {
        // Polling the complete coordinator overflows the test harness's 2 MiB stack;
        // the smallest verified bounded test thread is 4 MiB.
        std::thread::Builder::new()
            .name("stage7-composed-runtime".to_owned())
            .stack_size(4 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(run_training_runtime_with_sources_and_stop(
                        config,
                        specs,
                        async move {
                            let _ = stop_rx.await;
                        },
                    ))
            })
            .unwrap()
    }

    async fn join_composed_runtime(
        runtime: std::thread::JoinHandle<Result<TrainingRunSummary>>,
    ) -> Result<TrainingRunSummary> {
        timeout(Duration::from_secs(5), async {
            while !runtime.is_finished() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("composed runtime thread must converge");
        runtime.join().expect("composed runtime thread panicked")
    }

    fn composed_expected_minimum_rows(mode: ComposedLoopbackMode) -> BTreeMap<&'static str, u64> {
        let polymarket_connected = mode.gamma != ComposedIoBehavior::Unavailable
            && mode.clob != ComposedIoBehavior::Unavailable;
        BTreeMap::from([
            (
                "polymarket_market",
                u64::from(polymarket_connected && mode.clob == ComposedIoBehavior::Healthy),
            ),
            (
                "polymarket_market_metadata",
                u64::from(polymarket_connected),
            ),
            (
                "polymarket_rtds_chainlink_btc_usd",
                u64::from(mode.rtds == ComposedIoBehavior::Healthy),
            ),
            (
                "binance_depth_snapshot",
                u64::from(
                    mode.depth_websocket != ComposedIoBehavior::Unavailable
                        && mode.depth_snapshot == ComposedIoBehavior::Healthy,
                ),
            ),
            (
                "binance_depth_update",
                u64::from(
                    mode.depth_websocket == ComposedIoBehavior::Healthy
                        && mode.depth_snapshot != ComposedIoBehavior::Unavailable,
                ),
            ),
            (
                "binance_aggtrade",
                if mode.aggtrade == ComposedIoBehavior::Healthy {
                    1
                } else {
                    0
                },
            ),
        ])
    }

    fn assert_composed_artifacts(
        label: &str,
        mode: ComposedLoopbackMode,
        receipt: &TrainingRunSummary,
        manifest: &Value,
    ) {
        assert!(
            receipt.source_gaps.is_empty(),
            "{label} availability created confirmed gaps: {:?}",
            receipt.source_gaps
        );
        assert_eq!(manifest["collection_day_coverage"]["state"], "ended_known");
        let expected = composed_expected_minimum_rows(mode);
        let counters = manifest["source_counters"].as_object().unwrap();
        assert_eq!(counters.len(), TRAINING_SOURCE_KINDS.len());
        for (source, minimum_rows) in expected {
            let counter = &counters[source];
            let rows = counter["rows"].as_u64().unwrap();
            let written_rows = counter["written_rows"].as_u64().unwrap();
            if minimum_rows == 0 {
                assert_eq!(rows, 0, "{label}:{source} fabricated rows");
                assert_eq!(written_rows, 0, "{label}:{source} fabricated writes");
                assert_eq!(counter["files"], 0, "{label}:{source} fabricated a file");
                continue;
            }
            assert!(
                rows >= minimum_rows,
                "{label}:{source} rows={rows} expected_at_least={minimum_rows}"
            );
            assert_eq!(written_rows, rows, "{label}:{source} write accounting");
            assert!(counter["files"].as_u64().is_some_and(|files| files > 0));
            let segments = receipt
                .finalized_segments
                .iter()
                .filter(|segment| segment.source_kind == source)
                .collect::<Vec<_>>();
            assert!(!segments.is_empty(), "{label}:{source} missing Parquet");
            let segment_rows = segments
                .iter()
                .map(|segment| segment.row_count)
                .sum::<u64>();
            assert_eq!(segment_rows, rows, "{label}:{source} receipt rows");
            for segment in segments {
                assert!(segment.parquet_path.is_file());
                let reader =
                    SerializedFileReader::new(File::open(&segment.parquet_path).unwrap()).unwrap();
                assert_eq!(
                    u64::try_from(reader.metadata().file_metadata().num_rows()).unwrap(),
                    segment.row_count,
                    "{label}:{source} physical Parquet rows"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn full_runtime_all_io_unavailable_stays_live_until_injected_operator_stop() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let mode = ComposedLoopbackMode::all_unavailable();
        let fixture = ComposedLoopbackFixture::spawn(mode).await;
        let (stop_tx, stop_rx) = oneshot::channel();
        let runtime_root = dataset_root.clone();
        let runtime = spawn_composed_runtime_thread(
            composed_runtime_config(runtime_root, "all-unavailable"),
            fixture.specs.clone(),
            stop_rx,
        );
        fixture.wait_ready(&runtime).await;
        assert_eq!(fixture.counters.clob_connections.load(Ordering::Relaxed), 0);
        assert_eq!(
            fixture
                .counters
                .depth_snapshot_requests
                .load(Ordering::Relaxed),
            0
        );
        sleep(Duration::from_millis(50)).await;
        assert!(!runtime.is_finished());
        stop_tx.send(()).unwrap();
        let receipt = join_composed_runtime(runtime).await.unwrap();
        let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
        let day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={utc_day}"));
        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_composed_artifacts("all-unavailable", mode, &receipt, &manifest);
        fixture.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn full_runtime_cross_platform_degradation_converges_at_run_deadline_and_finalizes_rows()
    {
        let mode = ComposedLoopbackMode::with_unavailable(&[
            ComposedIoPath::Clob,
            ComposedIoPath::DepthSnapshot,
        ]);
        let fixture = ComposedLoopbackFixture::spawn(mode).await;
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let mut config =
            composed_runtime_config(dataset_root.clone(), "deadline-cross-platform-degraded");
        config.duration_secs = Some(1);
        config.run_forever = false;
        let runtime = spawn_composed_runtime_thread(config, fixture.specs.clone(), stop_rx);
        fixture.wait_ready(&runtime).await;
        let receipt = join_composed_runtime(runtime).await.unwrap();
        drop(stop_tx);
        let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
        let day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={utc_day}"));
        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_composed_artifacts(
            "deadline-cross-platform-degraded",
            mode,
            &receipt,
            &manifest,
        );
        fixture.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn full_runtime_protocol_alive_business_silence_does_not_fabricate_rows_or_reconnect() {
        let mode = ComposedLoopbackMode::stream_silent();
        let fixture = ComposedLoopbackFixture::spawn(mode).await;
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let runtime = spawn_composed_runtime_thread(
            composed_runtime_config(dataset_root.clone(), "all-streams-silent"),
            fixture.specs.clone(),
            stop_rx,
        );
        fixture.wait_ready(&runtime).await;
        sleep(Duration::from_millis(100)).await;
        let stable_transport = (
            fixture.counters.clob_connections.load(Ordering::Relaxed),
            fixture.counters.clob_subscriptions.load(Ordering::Relaxed),
            fixture.counters.rtds_connections.load(Ordering::Relaxed),
            fixture.counters.rtds_subscriptions.load(Ordering::Relaxed),
            fixture.counters.depth_connections.load(Ordering::Relaxed),
            fixture
                .counters
                .depth_snapshot_requests
                .load(Ordering::Relaxed),
            fixture
                .counters
                .aggtrade_connections
                .load(Ordering::Relaxed),
        );
        sleep(Duration::from_millis(250)).await;
        assert_eq!(
            stable_transport,
            (
                fixture.counters.clob_connections.load(Ordering::Relaxed),
                fixture.counters.clob_subscriptions.load(Ordering::Relaxed),
                fixture.counters.rtds_connections.load(Ordering::Relaxed),
                fixture.counters.rtds_subscriptions.load(Ordering::Relaxed),
                fixture.counters.depth_connections.load(Ordering::Relaxed),
                fixture
                    .counters
                    .depth_snapshot_requests
                    .load(Ordering::Relaxed),
                fixture
                    .counters
                    .aggtrade_connections
                    .load(Ordering::Relaxed),
            ),
            "protocol-alive silence must stay on the original sessions"
        );
        assert_eq!(
            fixture.counters.clob_business_sent.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            fixture.counters.rtds_business_sent.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            fixture.counters.depth_business_sent.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            fixture
                .counters
                .aggtrade_business_sent
                .load(Ordering::Relaxed),
            0
        );
        assert!(!runtime.is_finished());
        stop_tx.send(()).unwrap();
        let receipt = join_composed_runtime(runtime).await.unwrap();
        let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
        let day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={utc_day}"));
        let manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        assert_composed_artifacts("all-streams-silent", mode, &receipt, &manifest);
        assert_eq!(
            manifest["source_counters"]["polymarket_market_metadata"]["rows"],
            1
        );
        assert_eq!(
            manifest["source_counters"]["binance_depth_snapshot"]["rows"],
            1
        );
        fixture.shutdown().await;
    }

    async fn spawn_polymarket_book_burst_server(
        count: i64,
        asset_id: &'static str,
        market: &'static str,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            loop {
                let frame = socket.next().await.unwrap().unwrap();
                let Message::Text(text) = frame else {
                    continue;
                };
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                    continue;
                }
                if serde_json::from_str::<Value>(&text)
                    .ok()
                    .is_some_and(|value| value["operation"] == "subscribe")
                {
                    break;
                }
            }
            for timestamp in 0..count {
                socket
                    .send(Message::Text(
                        json!({
                            "event_type": "book",
                            "asset_id": asset_id,
                            "market": market,
                            "timestamp": timestamp.to_string(),
                            "bids": [],
                            "asks": []
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
            }
            sleep(Duration::from_secs(10)).await;
        });
        (address, server)
    }

    async fn spawn_polymarket_price_change_load_server(
        initial_burst: usize,
        sustained_ticks: usize,
        messages_per_tick: usize,
    ) -> (
        std::net::SocketAddr,
        oneshot::Sender<Instant>,
        oneshot::Receiver<(Instant, Vec<Instant>)>,
        Arc<AtomicU64>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (start_tx, start_rx) = oneshot::channel();
        let (complete_tx, complete_rx) = oneshot::channel();
        let offered_messages = Arc::new(AtomicU64::new(0));
        let offered_messages_for_server = Arc::clone(&offered_messages);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            loop {
                let frame = socket.next().await.unwrap().unwrap();
                let Message::Text(text) = frame else {
                    continue;
                };
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                    continue;
                }
                if serde_json::from_str::<Value>(&text)
                    .ok()
                    .is_some_and(|value| value["operation"] == "subscribe")
                {
                    break;
                }
            }

            let sustained_start: Instant = start_rx.await.unwrap();
            let offered_started = Instant::now();
            let mut sequence = 0_u64;
            for _ in 0..initial_burst {
                socket
                    .send(Message::Text(
                        json!({
                            "event_type": "price_change",
                            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                            "timestamp": (1_783_392_400_000_u64 + sequence).to_string(),
                            "price_changes": [
                                {
                                    "asset_id": "101",
                                    "price": "0.51",
                                    "size": "10",
                                    "side": "BUY",
                                    "hash": format!("up-{sequence}"),
                                    "best_bid": "0.50",
                                    "best_ask": "0.52"
                                },
                                {
                                    "asset_id": "102",
                                    "price": "0.49",
                                    "size": "8",
                                    "side": "SELL",
                                    "hash": format!("down-{sequence}"),
                                    "best_bid": "0.48",
                                    "best_ask": "0.50"
                                }
                            ]
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                offered_messages_for_server.fetch_add(1, Ordering::Relaxed);
                sequence += 1;
            }

            let mut tick_finished = Vec::with_capacity(sustained_ticks);
            for tick in 0..sustained_ticks {
                let deadline =
                    sustained_start + Duration::from_millis(u64::try_from(tick).unwrap() * 100);
                sleep(deadline.saturating_duration_since(Instant::now())).await;
                for _ in 0..messages_per_tick {
                    socket
                        .send(Message::Text(
                            json!({
                                "event_type": "price_change",
                                "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
                                "timestamp": (1_783_392_400_000_u64 + sequence).to_string(),
                                "price_changes": [
                                    {
                                        "asset_id": "101",
                                        "price": "0.51",
                                        "size": "10",
                                        "side": "BUY",
                                        "hash": format!("up-{sequence}"),
                                        "best_bid": "0.50",
                                        "best_ask": "0.52"
                                    },
                                    {
                                        "asset_id": "102",
                                        "price": "0.49",
                                        "size": "8",
                                        "side": "SELL",
                                        "hash": format!("down-{sequence}"),
                                        "best_bid": "0.48",
                                        "best_ask": "0.50"
                                    }
                                ]
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    offered_messages_for_server.fetch_add(1, Ordering::Relaxed);
                    sequence += 1;
                }
                tick_finished.push(Instant::now());
            }
            complete_tx.send((offered_started, tick_finished)).unwrap();
            std::future::pending::<()>().await;
        });
        (address, start_tx, complete_rx, offered_messages, server)
    }

    async fn budgeted_polymarket_message_for_test(
        message: WsMessage,
        global_budget: &InflightBudget,
        decoded_budget: &InflightBudget,
    ) -> BudgetedPolymarketMessage {
        budgeted_polymarket_message_at_for_test(message, now_ms(), global_budget, decoded_budget)
            .await
    }

    async fn budgeted_polymarket_message_at_for_test(
        message: WsMessage,
        recv_time_utc_ms: i64,
        global_budget: &InflightBudget,
        decoded_budget: &InflightBudget,
    ) -> BudgetedPolymarketMessage {
        let owned_bytes = owned_polymarket_message_bytes(&message);
        let raw_day_ticket = accepted_raw_day_ticket_for_test(recv_time_utc_ms).await;
        BudgetedPolymarketMessage {
            message,
            recv_time_utc_ms,
            raw_day_ticket,
            _global_reservation: global_budget
                .acquire(1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
                .await
                .unwrap(),
            _decoded_reservation: decoded_budget
                .acquire(1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
                .await
                .unwrap(),
        }
    }

    async fn polymarket_runtime_with_fence_messages_for_test(
        messages: Vec<BudgetedPolymarketMessage>,
    ) -> PolymarketDrainRuntime {
        let mut runtime = PolymarketDrainRuntime::new(
            "ws://127.0.0.1:9".to_owned(),
            bounded_ws_config(),
            InflightBudget::new(),
            active_raw_day_gate_for_test(now_ms()).await,
        )
        .unwrap();
        runtime.abort_pump_and_wait().await;
        let (event_tx, event_rx) =
            mpsc::channel::<PolymarketPumpItem>(messages.len().saturating_add(1).max(1));
        let (control_tx, mut control_rx) =
            mpsc::channel::<PolymarketPumpControl>(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
        runtime.events = event_rx;
        runtime.controls = control_tx;
        runtime.pump = Some(tokio::spawn(async move {
            let Some(PolymarketPumpControl::Fence {
                id,
                stop_after_fence,
                ..
            }) = control_rx.recv().await
            else {
                return;
            };
            for message in messages {
                if event_tx
                    .send(PolymarketPumpItem::Message(message))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = event_tx
                .send(PolymarketPumpItem::Fence {
                    id,
                    stop_after_fence,
                })
                .await;
        }));
        runtime
    }

    #[derive(Debug)]
    struct PipelineBacklogSample {
        polymarket_pipeline_messages: u64,
        polymarket_decoded_messages: u64,
        shared_ingress_rows: u64,
        writer_rows: u64,
        global_inflight_rows: u64,
    }

    fn assert_no_sustained_monotonic_backlog(
        name: &str,
        samples: &[PipelineBacklogSample],
        value: impl Fn(&PipelineBacklogSample) -> u64,
        tail_tolerance: u64,
    ) {
        const TWO_SECONDS_OF_SAMPLES: usize = 21;
        const COMPARISON_SAMPLES: usize = 10;
        assert!(
            samples.len() >= 40,
            "{name} requires at least forty sustained-window samples, got {}",
            samples.len()
        );
        for window in samples.windows(TWO_SECONDS_OF_SAMPLES) {
            let first = value(&window[0]);
            let last = value(window.last().unwrap());
            let non_decreasing = window
                .windows(2)
                .all(|pair| value(&pair[1]) >= value(&pair[0]));
            assert!(
                !non_decreasing || last <= first,
                "{name} grew monotonically for two seconds: first={first}, last={last}, window={window:?}"
            );
        }
        let head_average = samples
            .iter()
            .take(COMPARISON_SAMPLES)
            .map(&value)
            .sum::<u64>()
            / u64::try_from(COMPARISON_SAMPLES).unwrap();
        let tail_average = samples
            .iter()
            .rev()
            .take(COMPARISON_SAMPLES)
            .map(value)
            .sum::<u64>()
            / u64::try_from(COMPARISON_SAMPLES).unwrap();
        assert!(
            tail_average <= head_average.saturating_add(tail_tolerance),
            "{name} tail backlog exceeded the sustained-window head: head_average={head_average}, tail_average={tail_average}, tolerance={tail_tolerance}, samples={samples:?}"
        );
    }

    #[tokio::test]
    async fn training_manifest_owner_task_retains_the_dataset_lease_until_it_returns() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let utc_day = utc_day_string_from_ms(now_ms()).unwrap();
        let (owner, _) = test_manifest_owner(&dataset_root, &utc_day);
        let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());
        let owner_lease = Arc::clone(&dataset_lease);
        let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let (rollover_ready_tx, _rollover_ready_rx) = mpsc::channel(1);
        let owner_handle = tokio::spawn(async move {
            let _dataset_lease = owner_lease;
            run_manifest_owner(owner, manifest_rx, rollover_ready_tx).await
        });
        drop(dataset_lease);

        let error = lock_training_dataset_root(&dataset_root).unwrap_err();
        assert!(error.to_string().contains("training_dataset_root_locked"));

        drop(manifest_tx);
        assert!(owner_handle.await.unwrap().is_err());
        lock_training_dataset_root(&dataset_root).unwrap();
    }

    #[tokio::test]
    async fn training_source_task_retains_the_dataset_lease_until_it_returns() {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let dataset_lease = Arc::new(lock_training_dataset_root(&dataset_root).unwrap());
        let handle = SourceTaskHandle::spawn_with_dataset_lease(
            "binance_aggtrade",
            std::future::pending::<Result<()>>(),
            SourceTaskObservationCell::default(),
            Arc::new(RawDayGate::new()),
            Arc::clone(&dataset_lease),
        );
        drop(dataset_lease);

        let error = lock_training_dataset_root(&dataset_root).unwrap_err();
        assert!(error.to_string().contains("training_dataset_root_locked"));

        handle.join.abort();
        assert!(matches!(handle.join.await, Err(error) if error.is_cancelled()));
        lock_training_dataset_root(&dataset_root).unwrap();
    }

    #[tokio::test]
    async fn enqueue_paths_reject_a_missing_raw_day_gate_in_test_builds() {
        let recv_time_utc_ms = 1_783_392_404_599;
        let (tx, mut rx) = mpsc::channel(2);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };

        let rollover_error =
            raw_day_gate_for_rollover(&sink, &utc_day_string_from_ms(recv_time_utc_ms).unwrap())
                .await
                .err()
                .expect("a producer wrapper must not synthesize a missing raw-day gate");
        assert!(
            rollover_error
                .to_string()
                .contains("raw-day gate missing from producer rollover wrapper")
        );

        let send_error = send_event(
            &sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", recv_time_utc_ms),
        )
        .await
        .expect_err("generic enqueue must reject a missing raw-day gate");
        assert!(
            send_error
                .to_string()
                .contains("raw-day gate missing from production enqueue path")
        );

        let committed = Arc::new(AtomicBool::new(false));
        let committed_for_call = Arc::clone(&committed);
        let fanout_error = send_polymarket_events_atomically(
            &sink,
            recv_time_utc_ms,
            vec![InflightRawEvent::for_test(
                test_polymarket_price_change_event(recv_time_utc_ms),
            )],
            move || {
                committed_for_call.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect_err("Polymarket fan-out must reject a missing raw-day gate");
        assert!(
            fanout_error
                .to_string()
                .contains("raw-day gate missing from production Polymarket enqueue path")
        );
        assert!(!committed.load(Ordering::SeqCst));
        assert!(rx.try_recv().is_err());
    }

    fn recovered_pending_for_test(
        market: &DiscoveredBtcUpdownMarket,
        finalized_attempt_ordinals: Vec<u8>,
        missed_attempt_ordinals: Vec<u8>,
        next_attempt_ordinal: u8,
    ) -> RecoveredPolymarketPending {
        RecoveredPolymarketPending {
            slug: market.slug.clone(),
            clob_market_id: market.id.clone(),
            condition_id: market.condition_id.clone(),
            up_asset_id: market.up_asset_id.clone(),
            down_asset_id: market.down_asset_id.clone(),
            market_start_utc_ms: market.start_ts_s.saturating_mul(1_000),
            market_end_utc_ms: market.end_ts_s.saturating_mul(1_000),
            fixed_identity_verified_at_utc_ms: market.fixed_identity_verified_at_utc_ms,
            discovery_source_received_at_utc_ms: market.metadata_received_utc_ms.unwrap(),
            discovery_source_raw_sha256: market.raw_json_sha256.clone().unwrap(),
            attempts_recorded: u32::try_from(finalized_attempt_ordinals.len()).unwrap(),
            valid_response_count: u32::try_from(finalized_attempt_ordinals.len()).unwrap(),
            transport_unavailable_count: 0,
            missed_slot_count: 0,
            last_valid_metadata_received_at_utc_ms: finalized_attempt_ordinals.last().map(
                |ordinal| {
                    market.end_ts_s.saturating_mul(1_000).saturating_add(
                        POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS
                            + i64::from(ordinal.saturating_sub(1))
                                * POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS,
                    )
                },
            ),
            finalized_attempt_ordinals,
            missed_attempt_ordinals,
            next_attempt_ordinal,
        }
    }

    fn market_resolved_message_for_test(
        market: &DiscoveredBtcUpdownMarket,
        platform_timestamp_utc_ms: i64,
    ) -> WsMessage {
        serde_json::from_value(json!({
            "id": market.id,
            "market": market.condition_id,
            "slug": market.slug,
            "assets_ids": [market.up_asset_id, market.down_asset_id],
            "outcomes": ["Up", "Down"],
            "winning_asset_id": market.up_asset_id,
            "winning_outcome": "Up",
            "timestamp": platform_timestamp_utc_ms.to_string(),
            "event_type": "market_resolved"
        }))
        .unwrap()
    }

    fn next_market_for_test(current: &DiscoveredBtcUpdownMarket) -> DiscoveredBtcUpdownMarket {
        let mut next = current.clone();
        next.id = "market-next".to_owned();
        next.condition_id =
            "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
        next.up_asset_id = "201".to_owned();
        next.down_asset_id = "202".to_owned();
        next.start_ts_s = current.end_ts_s;
        next.end_ts_s = next.start_ts_s.saturating_add(300);
        next.candidate_start_ts_s = next.start_ts_s;
        next.candidate_end_ts_s = next.end_ts_s;
        next.slug = format!("btc-updown-5m-{}", next.start_ts_s);
        let received_at_utc_ms = next.start_ts_s.saturating_mul(1_000).saturating_sub(1);
        refresh_test_discovery_evidence(&mut next, received_at_utc_ms);
        next
    }

    #[tokio::test]
    async fn four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second()
    {
        const SUSTAINED_TICKS: usize = 60;
        const POLYMARKET_INITIAL_BURST: usize = 1_025;
        const POLYMARKET_MESSAGES_PER_TICK: usize = 210;
        const AUXILIARY_MESSAGES_PER_TICK: usize = 84;

        let polymarket_native_messages =
            POLYMARKET_INITIAL_BURST + SUSTAINED_TICKS * POLYMARKET_MESSAGES_PER_TICK;
        let auxiliary_native_messages = SUSTAINED_TICKS * AUXILIARY_MESSAGES_PER_TICK;
        let sustained_native_messages =
            SUSTAINED_TICKS * (POLYMARKET_MESSAGES_PER_TICK + 3 * AUXILIARY_MESSAGES_PER_TICK);
        let total_native_messages = polymarket_native_messages + 3 * auxiliary_native_messages;
        let polymarket_rows = polymarket_native_messages;
        let total_rows = polymarket_rows + 3 * auxiliary_native_messages;
        let one_tick_rows = POLYMARKET_MESSAGES_PER_TICK + 3 * AUXILIARY_MESSAGES_PER_TICK;
        assert_eq!(sustained_native_messages, 27_720);
        assert_eq!(total_native_messages, 28_745);
        assert_eq!(total_rows, 28_745);

        let (address, polymarket_start_tx, polymarket_complete_rx, polymarket_offered, server) =
            spawn_polymarket_price_change_load_server(
                POLYMARKET_INITIAL_BURST,
                SUSTAINED_TICKS,
                POLYMARKET_MESSAGES_PER_TICK,
            )
            .await;
        let load_recv_base_ms = now_ms()
            .div_euclid(UTC_DAY_MS)
            .saturating_mul(UTC_DAY_MS)
            .saturating_add(UTC_DAY_MS / 2);
        let global_budget = InflightBudget::new();
        let global_baseline = global_budget.available();
        let mut polymarket_runtime = PolymarketDrainRuntime::new_with_recv_clock(
            format!("ws://{address}"),
            bounded_ws_config(),
            global_budget.clone(),
            active_raw_day_gate_for_test(load_recv_base_ms).await,
            Arc::new(move || load_recv_base_ms),
        )
        .unwrap();
        let decoded_baseline = polymarket_runtime.decoded_budget.available();
        polymarket_runtime
            .sdk
            .replace_and_wait(
                BTreeSet::from([U256::from(101), U256::from(102)]),
                Duration::from_secs(2),
            )
            .await
            .unwrap();

        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
        let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&runtime_stats, load_recv_base_ms).await;
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let mut writers = start_writer_workers(
            &dataset_root,
            &test_runtime_provenance("four-source-production-load"),
            &manifest_tx,
            &runtime_stats,
        )
        .unwrap();
        let (event_tx, mut event_rx) =
            bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY).unwrap();
        let root_sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&runtime_stats),
            inflight_budget: global_budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let source_queue_tx_for_samples = root_sink.tx.clone();
        let writer_queue_rows_for_samples = writers
            .values()
            .map(|writer| Arc::clone(&writer.queue_rows))
            .collect::<Vec<_>>();
        let polymarket_sink = root_sink.fork_logical_producer();
        let rtds_sink = root_sink.fork_logical_producer();
        let depth_sink = root_sink.fork_logical_producer();
        let aggtrade_sink = root_sink.fork_logical_producer();
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let run_started = Instant::now();
        let sustained_start = Instant::now() + Duration::from_millis(500);
        let polymarket_processed = Arc::new(AtomicU64::new(0));
        let offered_for_samples = Arc::clone(&polymarket_offered);
        let processed_for_samples = Arc::clone(&polymarket_processed);
        let decoded_budget_for_samples = polymarket_runtime.decoded_budget.clone();
        let global_budget_for_samples = global_budget.clone();
        let backlog_sampler = tokio::spawn(async move {
            let mut samples = Vec::with_capacity(SUSTAINED_TICKS);
            let mut deadline = sustained_start + Duration::from_secs(1);
            let final_deadline = sustained_start
                + Duration::from_millis(
                    u64::try_from(SUSTAINED_TICKS.saturating_sub(1)).unwrap() * 100,
                );
            while deadline <= final_deadline {
                sleep(deadline.saturating_duration_since(Instant::now())).await;
                let offered = offered_for_samples.load(Ordering::Relaxed);
                let processed = processed_for_samples.load(Ordering::Relaxed);
                let decoded_available = decoded_budget_for_samples.available().0;
                let global_available = global_budget_for_samples.available().0;
                let shared_ingress_rows = source_queue_tx_for_samples
                    .max_capacity()
                    .saturating_sub(source_queue_tx_for_samples.capacity());
                let writer_rows = writer_queue_rows_for_samples
                    .iter()
                    .map(|rows| rows.load(Ordering::Relaxed))
                    .sum();
                samples.push(PipelineBacklogSample {
                    polymarket_pipeline_messages: offered.saturating_sub(processed),
                    polymarket_decoded_messages: u64::try_from(
                        decoded_baseline.0.saturating_sub(decoded_available),
                    )
                    .unwrap(),
                    shared_ingress_rows: u64::try_from(shared_ingress_rows).unwrap(),
                    writer_rows,
                    global_inflight_rows: u64::try_from(
                        global_baseline.0.saturating_sub(global_available),
                    )
                    .unwrap(),
                });
                deadline += Duration::from_millis(100);
            }
            samples
        });

        let polymarket_identity = collector_identity.clone();
        let processed_for_processor = Arc::clone(&polymarket_processed);
        let polymarket_processor = tokio::spawn(async move {
            let mut current = test_btc_discovery().current;
            let current_start_ts_s = load_recv_base_ms.div_euclid(300_000) * 300;
            current.slug = format!("btc-updown-5m-{current_start_ts_s}");
            current.start_ts_s = current_start_ts_s;
            current.end_ts_s = current_start_ts_s + 300;
            current.candidate_start_ts_s = current.start_ts_s;
            current.candidate_end_ts_s = current.end_ts_s;
            refresh_test_discovery_evidence(&mut current, load_recv_base_ms - 1);
            let discovery = BtcUpdownDiscovery {
                current,
                next: None,
            };
            let mut context = PolymarketProcessingContext::new(discovery);
            let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
            for _ in 0..polymarket_native_messages {
                let message = match polymarket_runtime.recv(Duration::from_secs(5)).await {
                    Ok(Some(PolymarketDrainEvent::Message(message))) => message,
                    Ok(Some(PolymarketDrainEvent::Fence { .. })) => {
                        panic!("unexpected Polymarket fence before all load messages")
                    }
                    Ok(None) => panic!("Polymarket load message timed out"),
                    Err(error) => panic!("Polymarket load terminal: {error:?}"),
                };
                process_polymarket_budgeted_message(
                    message,
                    &mut context,
                    &polymarket_sink,
                    &polymarket_identity,
                    run_started,
                    &mut recovery,
                )
                .await
                .unwrap();
                processed_for_processor.fetch_add(1, Ordering::Relaxed);
            }
            fence_polymarket_processor(
                &mut polymarket_runtime,
                true,
                &mut context,
                &polymarket_sink,
                &polymarket_identity,
                run_started,
                &mut recovery,
            )
            .await
            .unwrap();
            polymarket_runtime
        });

        let rtds_identity = collector_identity.clone();
        let rtds_producer = tokio::spawn(async move {
            let mut dedupe = TimedFingerprintWindow::with_limits(
                RTDS_DEDUPE_WINDOW_MS,
                auxiliary_native_messages + 1,
            );
            let mut tick_finished = Vec::with_capacity(SUSTAINED_TICKS);
            let mut sequence = 0_usize;
            for tick in 0..SUSTAINED_TICKS {
                let deadline =
                    sustained_start + Duration::from_millis(u64::try_from(tick).unwrap() * 100);
                sleep(deadline.saturating_duration_since(Instant::now())).await;
                for _ in 0..AUXILIARY_MESSAGES_PER_TICK {
                    let timestamp = now_ms();
                    let message = serde_json::from_value::<
                        polymarket_client_sdk_v2::rtds::RtdsMessage,
                    >(json!({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": timestamp,
                        "payload": {
                            "symbol": "btc/usd",
                            "timestamp": timestamp,
                            "value": (65_000_u64 + u64::try_from(sequence).unwrap()).to_string()
                        }
                    }))
                    .unwrap();
                    assert!(
                        emit_rtds_message(
                            &rtds_sink,
                            &rtds_identity,
                            run_started,
                            &mut dedupe,
                            message,
                        )
                        .await
                        .unwrap()
                    );
                    sequence += 1;
                }
                tick_finished.push(Instant::now());
            }
            (sequence, tick_finished)
        });

        let depth_producer = tokio::spawn(async move {
            let mut tick_finished = Vec::with_capacity(SUSTAINED_TICKS);
            let mut sequence = 0_usize;
            for tick in 0..SUSTAINED_TICKS {
                let deadline =
                    sustained_start + Duration::from_millis(u64::try_from(tick).unwrap() * 100);
                sleep(deadline.saturating_duration_since(Instant::now())).await;
                for _ in 0..AUXILIARY_MESSAGES_PER_TICK {
                    send_event(
                        &depth_sink,
                        run_started,
                        test_binance_depth_update_event(
                            load_recv_base_ms + i64::try_from(sequence).unwrap(),
                        ),
                    )
                    .await
                    .unwrap();
                    sequence += 1;
                }
                tick_finished.push(Instant::now());
            }
            (sequence, tick_finished)
        });

        let aggtrade_identity = collector_identity.clone();
        let aggtrade_producer = tokio::spawn(async move {
            let spec = BinanceWsSpec {
                source_kind: "binance_aggtrade".to_owned(),
                default_event_type: "aggTrade".to_owned(),
                url: BINANCE_AGGTRADE_WS_ENDPOINT.to_owned(),
                frame_inactivity_timeout_secs: 60,
                recovery_wait_override: None,
            };
            let mut tick_finished = Vec::with_capacity(SUSTAINED_TICKS);
            let mut sequence = 0_usize;
            for tick in 0..SUSTAINED_TICKS {
                let deadline =
                    sustained_start + Duration::from_millis(u64::try_from(tick).unwrap() * 100);
                sleep(deadline.saturating_duration_since(Instant::now())).await;
                for _ in 0..AUXILIARY_MESSAGES_PER_TICK {
                    let id = u64::try_from(sequence + 1).unwrap();
                    let timestamp = load_recv_base_ms + i64::try_from(sequence).unwrap();
                    let websocket = BinanceWsAggTrade::from_wire(
                        &test_ws_aggtrade_value_at(id, timestamp),
                        "btcusdt@aggTrade",
                        timestamp,
                        BinanceAggTradeIngressCoordinate::Websocket { sequence: id },
                    )
                    .unwrap();
                    let event = binance_aggtrade_raw_event(
                        &BinanceAggTradeOutput::Websocket(websocket),
                        &aggtrade_identity,
                        &spec,
                        "test-source-epoch",
                        1,
                    )
                    .unwrap();
                    send_event(&aggtrade_sink, run_started, event)
                        .await
                        .unwrap();
                    sequence += 1;
                }
                tick_finished.push(Instant::now());
            }
            (sequence, tick_finished)
        });

        polymarket_start_tx.send(sustained_start).unwrap();

        let mut batches = PendingIngressBatches::for_training_source_kinds();
        let mut coverage = BTreeMap::<String, CoverageStats>::new();
        let mut received_rows = 0_u64;
        let mut average_event_bytes = 0_u64;
        let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
        let mut rollover_coordinator =
            RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
        timeout(Duration::from_secs(30), async {
            while received_rows < u64::try_from(total_rows).unwrap() {
                let inflight_event = event_rx
                    .recv()
                    .await
                    .expect("all four-source load rows must reach shared ingress");
                process_ingress_event(
                    inflight_event,
                    &mut writers,
                    &mut batches,
                    &mut coverage,
                    &mut received_rows,
                    &mut average_event_bytes,
                    &gaps,
                    &runtime_stats,
                    &mut writer_queue_tracker,
                    run_started,
                    &manifest_tx,
                    &mut rollover_coordinator,
                )
                .await
                .unwrap();
            }
        })
        .await
        .expect("four-source production load must finish within 30 seconds");

        let (polymarket_offered_started, polymarket_ticks) =
            timeout(Duration::from_secs(5), polymarket_complete_rx)
                .await
                .unwrap()
                .unwrap();
        let (rtds_messages, rtds_ticks) = timeout(Duration::from_secs(5), rtds_producer)
            .await
            .unwrap()
            .unwrap();
        let (depth_messages, depth_ticks) = timeout(Duration::from_secs(5), depth_producer)
            .await
            .unwrap()
            .unwrap();
        let (aggtrade_messages, aggtrade_ticks) =
            timeout(Duration::from_secs(5), aggtrade_producer)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(rtds_messages, auxiliary_native_messages);
        assert_eq!(depth_messages, auxiliary_native_messages);
        assert_eq!(aggtrade_messages, auxiliary_native_messages);

        let mut latest_tick_finished = Vec::with_capacity(SUSTAINED_TICKS);
        for tick in 0..SUSTAINED_TICKS {
            let latest = [
                polymarket_ticks[tick],
                rtds_ticks[tick],
                depth_ticks[tick],
                aggtrade_ticks[tick],
            ]
            .into_iter()
            .max()
            .unwrap();
            let scheduled =
                sustained_start + Duration::from_millis(u64::try_from(tick).unwrap() * 100);
            assert!(
                latest <= scheduled + Duration::from_secs(1),
                "load producer fell more than one second behind at tick {tick}"
            );
            latest_tick_finished.push(latest);
        }
        let offered_finished = *latest_tick_finished.last().unwrap();
        let sustained_rate = sustained_native_messages as f64
            / offered_finished
                .duration_since(sustained_start)
                .as_secs_f64();
        let actual_rate = total_native_messages as f64
            / offered_finished
                .duration_since(polymarket_offered_started)
                .as_secs_f64();
        assert!((2_000.0..=5_000.0).contains(&sustained_rate));
        assert!((2_000.0..=5_000.0).contains(&actual_rate));
        let backlog_samples = timeout(Duration::from_secs(5), backlog_sampler)
            .await
            .unwrap()
            .unwrap();
        assert_no_sustained_monotonic_backlog(
            "Polymarket socket-to-processor messages",
            &backlog_samples,
            |sample| sample.polymarket_pipeline_messages,
            u64::try_from(POLYMARKET_MESSAGES_PER_TICK).unwrap(),
        );
        assert_no_sustained_monotonic_backlog(
            "Polymarket decoded queue messages",
            &backlog_samples,
            |sample| sample.polymarket_decoded_messages,
            u64::try_from(POLYMARKET_MESSAGES_PER_TICK).unwrap(),
        );
        assert_no_sustained_monotonic_backlog(
            "shared ingress rows",
            &backlog_samples,
            |sample| sample.shared_ingress_rows,
            u64::try_from(one_tick_rows).unwrap(),
        );
        assert_no_sustained_monotonic_backlog(
            "writer rows",
            &backlog_samples,
            |sample| sample.writer_rows,
            u64::try_from(DEFAULT_FLUSH_ROWS).unwrap(),
        );
        // Each auxiliary source may legitimately fill one bounded ingress batch during the window.
        assert_no_sustained_monotonic_backlog(
            "global inflight rows",
            &backlog_samples,
            |sample| sample.global_inflight_rows,
            u64::try_from(DEFAULT_FLUSH_ROWS * 3).unwrap(),
        );

        let polymarket_runtime = timeout(Duration::from_secs(5), polymarket_processor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            polymarket_offered.load(Ordering::Relaxed),
            u64::try_from(polymarket_native_messages).unwrap()
        );
        assert_eq!(
            polymarket_processed.load(Ordering::Relaxed),
            u64::try_from(polymarket_native_messages).unwrap()
        );
        assert!(polymarket_runtime.pump.is_none());
        assert!(polymarket_runtime.events.is_empty());
        assert_eq!(
            polymarket_runtime.decoded_budget.available(),
            decoded_baseline
        );
        assert!(event_rx.try_recv().is_err());
        assert_eq!(received_rows, u64::try_from(total_rows).unwrap());

        flush_all_batches(
            &mut writers,
            &mut batches,
            &gaps,
            &runtime_stats,
            &mut writer_queue_tracker,
            run_started,
        )
        .await
        .unwrap();
        assert!(batches.raw_events.values().all(Vec::is_empty));
        assert!(batches.polymarket_orderbook_v3.is_empty());
        let writer_outcome =
            stop_writer_workers_with_budget(writers, Duration::from_secs(30)).await;
        assert!(writer_outcome.all_terminal);
        assert!(
            writer_outcome.errors.is_empty(),
            "{:?}",
            writer_outcome.errors
        );
        assert_eq!(writer_outcome.outputs.len(), TRAINING_SOURCE_KINDS.len());

        let expected_rows = BTreeMap::from([
            ("polymarket_market", u64::try_from(polymarket_rows).unwrap()),
            ("polymarket_market_metadata", 0),
            ("binance_depth_snapshot", 0),
            (
                "binance_depth_update",
                u64::try_from(auxiliary_native_messages).unwrap(),
            ),
            (
                "binance_aggtrade",
                u64::try_from(auxiliary_native_messages).unwrap(),
            ),
            (
                "polymarket_rtds_chainlink_btc_usd",
                u64::try_from(auxiliary_native_messages).unwrap(),
            ),
        ]);
        for output in &writer_outcome.outputs {
            let expected = expected_rows[output.source_kind.as_str()];
            assert_eq!(output.stats.events_received, expected);
            assert_eq!(output.stats.events_written, expected);
            assert_eq!(output.stats.typed_rows_written, expected);
            assert_eq!(output.stats.events_dropped, 0);
            assert_eq!(output.stats.typed_rows_dropped, 0);
            assert_eq!(output.stats.finalized_segment_rows, expected);
            let mut parquet_rows = 0_u64;
            for segment in &output.finalized_segments {
                let reader = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(&segment.parquet_path).unwrap(),
                )
                .unwrap();
                parquet_rows +=
                    u64::try_from(reader.metadata().file_metadata().num_rows()).unwrap();
            }
            assert_eq!(parquet_rows, expected);
        }
        for source in [
            "polymarket_market",
            "binance_depth_update",
            "binance_aggtrade",
            "polymarket_rtds_chainlink_btc_usd",
        ] {
            assert!(expected_rows[source] >= u64::try_from(DEFAULT_FLUSH_ROWS).unwrap());
            assert_eq!(coverage[source].rows, expected_rows[source]);
        }
        assert_eq!(runtime_stats.wait_count.load(Ordering::Relaxed), 0);
        assert_eq!(runtime_stats.timeout_count.load(Ordering::Relaxed), 0);
        assert_eq!(runtime_stats.dropped_rows.load(Ordering::Relaxed), 0);
        assert!(runtime_stats.drain_all_operational().is_empty());
        assert!(gaps.lock().unwrap().is_empty());
        assert_eq!(global_budget.available(), global_baseline);
        eprintln!(
            "four-source production load: native_messages={total_native_messages} rows={total_rows} sustained_rate={sustained_rate:.1} msg/s actual_rate={actual_rate:.1} msg/s"
        );
        server.abort();
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
            &test_ws_aggtrade_value_at(id, timestamp_received),
            "btcusdt@aggTrade",
            timestamp_received,
            BinanceAggTradeIngressCoordinate::Websocket { sequence: id },
        )
        .unwrap();
        BinanceAggTradeHourlyRow::from_output(
            &BinanceAggTradeOutput::Websocket(websocket),
            "collector-test",
            "local-test",
            BINANCE_AGGTRADE_WS_ENDPOINT,
            BinanceAggTradeOrdering {
                source_sequence_epoch_id: "test-source-epoch".to_owned(),
                connection_generation: 1,
                websocket_ingress_sequence: id,
                collector_admission_sequence: id,
                collector_admission_time_utc_ms: timestamp_received,
            },
        )
        .unwrap()
    }

    fn test_raw_event(source_kind: &str, recv_time_utc_ms: i64) -> RawEvent {
        assert_eq!(source_kind, "binance_aggtrade");
        let websocket = BinanceWsAggTrade::from_wire(
            &test_ws_aggtrade_value_at(123_456, recv_time_utc_ms),
            "btcusdt@aggTrade",
            recv_time_utc_ms,
            BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
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
                url: BINANCE_AGGTRADE_WS_ENDPOINT.to_owned(),
                frame_inactivity_timeout_secs: 30,
                recovery_wait_override: None,
            },
            "test-source-epoch",
            1,
        )
        .unwrap()
    }

    fn test_btc_discovery() -> BtcUpdownDiscovery {
        let mut current = crate::market_discovery::DiscoveredBtcUpdownMarket {
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
            raw_json_sha256: None,
            raw_json: None,
            metadata_fetch_start_utc_ms: Some(1_783_392_180_000),
            metadata_received_utc_ms: Some(1_783_392_180_010),
            fixed_identity_verified_at_utc_ms: 1_783_392_299_999,
            metadata_validation_completed_at_utc_ms: 1_783_392_299_999,
        };
        refresh_test_discovery_evidence(&mut current, 1_783_392_180_010);
        BtcUpdownDiscovery {
            current,
            next: None,
        }
    }

    fn refresh_test_discovery_evidence(
        market: &mut DiscoveredBtcUpdownMarket,
        received_at_utc_ms: i64,
    ) {
        let sdk_market =
            serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(market))
                .unwrap();
        let payload = serde_json::to_string(&sdk_market).unwrap();
        market.raw_json_sha256 = Some(
            Sha256::digest(payload.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        );
        market.raw_json = Some(payload);
        market.metadata_fetch_start_utc_ms = Some(received_at_utc_ms.saturating_sub(10));
        market.metadata_received_utc_ms = Some(received_at_utc_ms);
        market.fixed_identity_verified_at_utc_ms = received_at_utc_ms.saturating_add(1);
        market.metadata_validation_completed_at_utc_ms = market.fixed_identity_verified_at_utc_ms;
    }

    fn gamma_market_response_for_window(window: &crate::market_discovery::MarketWindow) -> String {
        let rfc3339 = |timestamp_s: i64| {
            let value = OffsetDateTime::from_unix_timestamp(timestamp_s).unwrap();
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                value.year(),
                value.month() as u8,
                value.day(),
                value.hour(),
                value.minute(),
                value.second()
            )
        };
        json!([{
            "id": "gamma-current",
            "question": "Will BTC go Up or Down?",
            "conditionId": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "slug": window.slug,
            "startDate": rfc3339(window.target_ts_s),
            "endDate": rfc3339(window.end_ts_s),
            "outcomes": r#"["Up","Down"]"#,
            "active": true,
            "closed": false,
            "archived": false,
            "enableOrderBook": true,
            "clobTokenIds": r#"["101","102"]"#,
            "acceptingOrders": true
        }])
        .to_string()
    }

    fn gamma_market_value_for_discovered(
        market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    ) -> Value {
        let rfc3339 = |timestamp_s: i64| {
            let value = OffsetDateTime::from_unix_timestamp(timestamp_s).unwrap();
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                value.year(),
                value.month() as u8,
                value.day(),
                value.hour(),
                value.minute(),
                value.second()
            )
        };
        json!({
            "id": market.id,
            "question": "Will BTC go Up or Down?",
            "conditionId": market.condition_id,
            "slug": market.slug,
            "startDate": rfc3339(market.start_ts_s),
            "endDate": rfc3339(market.end_ts_s),
            "outcomes": r#"["Up","Down"]"#,
            "outcomePrices": r#"["0.5","0.5"]"#,
            "active": true,
            "closed": false,
            "archived": false,
            "enableOrderBook": true,
            "clobTokenIds": json!([market.up_asset_id, market.down_asset_id]).to_string(),
            "acceptingOrders": true
        })
    }

    fn gamma_terminal_market_for_discovered(
        market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    ) -> SdkGammaMarket {
        let closed_time = OffsetDateTime::from_unix_timestamp(market.end_ts_s).unwrap();
        let mut value = gamma_market_value_for_discovered(market);
        value["closed"] = json!(true);
        value["acceptingOrders"] = json!(false);
        value["closedTime"] = json!(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            closed_time.year(),
            closed_time.month() as u8,
            closed_time.day(),
            closed_time.hour(),
            closed_time.minute(),
            closed_time.second(),
        ));
        value["umaResolutionStatus"] = json!("resolved");
        value["outcomePrices"] = json!(r#"["1","0"]"#);
        serde_json::from_value(value).unwrap()
    }

    fn paused_timeline_utc_ms(anchor: TokioInstant, anchor_utc_ms: i64) -> i64 {
        anchor_utc_ms.saturating_add(
            i64::try_from(
                TokioInstant::now()
                    .saturating_duration_since(anchor)
                    .as_millis(),
            )
            .unwrap(),
        )
    }

    async fn advance_paused_timeline_to(
        anchor: TokioInstant,
        anchor_utc_ms: i64,
        target_utc_ms: i64,
    ) {
        let current_utc_ms = paused_timeline_utc_ms(anchor, anchor_utc_ms);
        assert!(
            current_utc_ms <= target_utc_ms,
            "paused timeline already passed target:current={current_utc_ms}:target={target_utc_ms}"
        );
        tokio::time::advance(Duration::from_millis(
            u64::try_from(target_utc_ms - current_utc_ms).unwrap(),
        ))
        .await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_cross_thread_clob_trace_quiet(trace: &Arc<Mutex<Vec<(i64, Value)>>>) {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut observed_len = trace.lock().unwrap().len();
        let mut unchanged_since = Instant::now();
        loop {
            tokio::time::sleep(Duration::ZERO).await;
            let current_len = trace.lock().unwrap().len();
            if current_len != observed_len {
                observed_len = current_len;
                unchanged_since = Instant::now();
            }
            if unchanged_since.elapsed() >= Duration::from_millis(10) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "cross-thread CLOB trace did not quiesce within the wall-clock test harness bound"
            );
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
            collector_admission: None,
        }
    }

    fn test_binance_depth_update_event(recv_time_utc_ms: i64) -> RawEvent {
        test_binance_depth_update_event_inner(recv_time_utc_ms, None)
    }

    fn test_binance_depth_snapshot_event(recv_time_utc_ms: i64, trigger_reason: &str) -> RawEvent {
        RawEvent {
            source_kind: "binance_depth_snapshot".to_owned(),
            source_event_type: "depth_snapshot".to_owned(),
            source_identifier: "BTCUSDT".to_owned(),
            source_ts: None,
            recv_time_utc_ms,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: BINANCE_DEPTH_SNAPSHOT_ENDPOINT.to_owned(),
            raw_json: json!({
                "lastUpdateId": 100_u64,
                "bids": [["65000.10", "0.25"]],
                "asks": [["65000.20", "0.30"]],
                "snapshot_trigger_reason": trigger_reason,
                "snapshot_fetch_start_utc": recv_time_utc_ms - 10,
                "source_sequence_epoch_id": "depth-source-epoch-test",
                "connection_generation": 1_u64,
                "depth_ingress_sequence": 1_u64,
            })
            .to_string(),
            collector_admission: None,
        }
    }

    fn test_binance_depth_update_event_inner(
        recv_time_utc_ms: i64,
        gap_reason: Option<&str>,
    ) -> RawEvent {
        let is_gap = gap_reason.is_some();
        let raw_json = json!({
            "e": "depthUpdate",
            "E": recv_time_utc_ms,
            "s": "BTCUSDT",
            "U": if is_gap { 105_u64 } else { 101_u64 },
            "u": if is_gap { 105_u64 } else { 103_u64 },
            "b": [["65000.10", "0.25"]],
            "a": [["65000.20", "0.30"]],
            "snapshot_last_update_id": 100_u64,
            "local_u_before": if is_gap { json!(103_u64) } else { Value::Null },
            "local_u_after": if is_gap { Value::Null } else { json!(103_u64) },
            "stitch_verdict": if is_gap {
                BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE
            } else {
                BINANCE_DEPTH_VERDICT_APPLIED
            },
            "stitch_evidence": if is_gap {
                "U > local_u + 1"
            } else {
                "U <= lastUpdateId + 1 <= u"
            },
            "stitch_gap_reason": gap_reason.map(|reason| reason.trim_start_matches("stitch_gap:")),
            "source_sequence_epoch_id": "depth-source-epoch-test",
            "connection_generation": 1_u64,
            "depth_ingress_sequence": 1_u64,
        });
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
            collector_admission: None,
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
