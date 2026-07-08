use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest as _, Sha256};

use crate::collector_identity::resolve_collector_region;
use crate::deployment::{
    CollectorBinaryRole, DeploymentRuntimeConfig, Result, RuntimeReceipt, validate_runtime_config,
};
use crate::handoff::{HandoffRuntimeConfig, run_heartbeat_handoff_server};
use crate::live_runtime::{LiveTrainingRuntimeConfig, run_live_training_runtime};
use crate::manifest::SourceCoverage;
use crate::publish::PaperLiveEnvironment;
use crate::retention::TrainingRetentionPolicy;
use crate::segment::SegmentNaming;
use crate::writer::{RawEvent, sha256_file, write_segment_on_blocking_pool_for_source};

const SYNTHETIC_SOURCES: &[&str] = &[
    "polymarket_market",
    "binance_depth_update",
    "binance_aggtrade",
    "polymarket_rtds_chainlink_btc_usd",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliArgs {
    pub dataset_root: PathBuf,
    pub run_id: String,
    pub max_runtime_secs: Option<u64>,
    pub collector_region: Option<String>,
    pub config_path: Option<PathBuf>,
    pub stop_after_complete_polymarket_market: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrainingRuntimeMode {
    Synthetic,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrainingRuntimeConfig {
    pub dataset_root: PathBuf,
    pub run_id: String,
    pub mode: TrainingRuntimeMode,
    pub max_runtime_secs: Option<u64>,
    pub collector_region: Option<String>,
    pub code_version: Option<String>,
    pub collector_binary_sha256: Option<String>,
    pub source_config_digest: Option<String>,
    pub stop_after_complete_polymarket_market: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceTaskPlan {
    sources: Vec<String>,
}

impl SourceTaskPlan {
    pub fn four_sources() -> Self {
        Self {
            sources: SYNTHETIC_SOURCES
                .iter()
                .map(|source| (*source).to_owned())
                .collect(),
        }
    }

    pub fn startup_decisions(&self, polymarket_discovery_ready: bool) -> BTreeMap<String, String> {
        self.sources
            .iter()
            .map(|source| {
                let action = if source == "polymarket_market" && !polymarket_discovery_ready {
                    "record_source_gap"
                } else {
                    "start"
                };
                (source.clone(), action.to_owned())
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedRuntimeSegment {
    pub source_kind: String,
    pub parquet_path: PathBuf,
    pub manifest_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrainingRuntimeReceipt {
    pub run_id: String,
    pub run_manifest_path: PathBuf,
    pub finalized_segments: Vec<FinalizedRuntimeSegment>,
    pub source_coverage: Vec<SourceCoverage>,
    pub source_gaps: Vec<String>,
    pub resource_watermark_recorded: bool,
    pub writer_used_blocking_pool: bool,
}

pub async fn run_collector_from_env(role: CollectorBinaryRole) -> Result<()> {
    run_collector(role, env::args_os().skip(1)).await
}

pub async fn run_collector<I, S>(role: CollectorBinaryRole, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString>,
{
    let cli = parse_args(args)?;
    fs::create_dir_all(&cli.dataset_root)?;
    let dataset_root = cli.dataset_root.canonicalize()?;
    let mut config = DeploymentRuntimeConfig::for_role(role, dataset_root, cli.run_id)?;
    config.max_runtime_secs = cli.max_runtime_secs;
    config.collector_region_override = cli.collector_region;
    config.config_path = canonical_config_path(cli.config_path.as_deref())?;
    validate_runtime_config(&config)?;

    fs::create_dir_all(&config.output_root)?;
    let receipt = startup_receipt(&config).await?;
    write_startup_receipt(&config.output_root, &receipt)?;
    if role == CollectorBinaryRole::TrainingCold {
        let training_mode = match env::var("DATA_COLLECT_RUNTIME_MODE").ok().as_deref() {
            Some("live") => TrainingRuntimeMode::Live,
            _ => TrainingRuntimeMode::Synthetic,
        };
        let training_config = TrainingRuntimeConfig {
            dataset_root: config.dataset_root.clone(),
            run_id: config.run_id.clone(),
            mode: training_mode,
            max_runtime_secs: config.max_runtime_secs,
            collector_region: Some(receipt.collector_region),
            code_version: Some(receipt.git_commit),
            collector_binary_sha256: Some(receipt.binary_sha256),
            source_config_digest: Some(receipt.config_digest),
            stop_after_complete_polymarket_market: cli.stop_after_complete_polymarket_market,
        };
        run_training_runtime(training_config).await?;
    } else {
        let environment = match role {
            CollectorBinaryRole::PaperHot => PaperLiveEnvironment::Paper,
            CollectorBinaryRole::LiveHot => PaperLiveEnvironment::Live,
            CollectorBinaryRole::TrainingCold => unreachable!("handled above"),
        };
        let handoff_config =
            HandoffRuntimeConfig::for_environment(&config.dataset_root, environment)?;
        run_heartbeat_handoff_server(
            &handoff_config,
            &receipt.collector_host_id,
            &receipt.collector_region,
            config.max_runtime_secs,
        )
        .await?;
    }
    Ok(())
}

pub async fn run_training_runtime(config: TrainingRuntimeConfig) -> Result<TrainingRuntimeReceipt> {
    fs::create_dir_all(&config.dataset_root)?;
    let dataset_root = config.dataset_root.canonicalize()?;
    let collector_region = config
        .collector_region
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());
    let retention_policy_receipt_path =
        enforce_startup_retention_policy(&dataset_root, &config.run_id)?;
    match config.mode {
        TrainingRuntimeMode::Synthetic => {
            run_synthetic_training_runtime(
                &dataset_root,
                &config.run_id,
                &collector_region,
                config.stop_after_complete_polymarket_market,
                &retention_policy_receipt_path,
            )
            .await
        }
        TrainingRuntimeMode::Live => {
            let receipt = run_live_training_runtime(LiveTrainingRuntimeConfig {
                dataset_root: dataset_root.clone(),
                run_id: config.run_id,
                duration_secs: config.max_runtime_secs,
                collector_host_id: hostname(),
                collector_region,
                code_version: config
                    .code_version
                    .unwrap_or_else(|| "test-code-version".to_owned()),
                collector_binary_sha256: config
                    .collector_binary_sha256
                    .unwrap_or_else(|| "test-binary-sha256".to_owned()),
                source_config_digest: config
                    .source_config_digest
                    .unwrap_or_else(|| "test-source-config-digest".to_owned()),
                stop_after_complete_polymarket_market: config.stop_after_complete_polymarket_market,
                retention_policy_receipt_path: Some(retention_policy_receipt_path),
            })
            .await?;
            Ok(TrainingRuntimeReceipt {
                run_id: receipt.run_id,
                run_manifest_path: receipt.run_manifest_path,
                finalized_segments: receipt
                    .finalized_segments
                    .into_iter()
                    .map(|segment| FinalizedRuntimeSegment {
                        source_kind: segment.source_kind,
                        parquet_path: segment.parquet_path,
                        manifest_path: segment.manifest_path,
                        row_count: segment.row_count,
                        sha256: segment.sha256,
                    })
                    .collect(),
                source_coverage: receipt.source_coverage,
                source_gaps: receipt.source_gaps,
                resource_watermark_recorded: receipt.resource_watermark_recorded,
                writer_used_blocking_pool: receipt.writer_used_blocking_pool,
            })
        }
    }
}

pub fn parse_args<I, S>(args: I) -> Result<CliArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<std::ffi::OsString>,
{
    let mut values = BTreeMap::new();
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        let arg = arg
            .into_string()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "argument is not utf-8"))?;
        match arg.as_str() {
            "--dataset-root"
            | "--run-id"
            | "--max-runtime-secs"
            | "--collector-region"
            | "--config"
            | "--stop-after-complete-polymarket-market" => {
                let value = iter.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{arg} requires a value"),
                    )
                })?;
                let value = value.into_string().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{arg} value is not utf-8"),
                    )
                })?;
                values.insert(arg, value);
            }
            "--help" | "-h" => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, usage()).into());
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}\n{}", usage()),
                )
                .into());
            }
        }
    }

    let dataset_root = values
        .remove("--dataset-root")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--dataset-root is required"))?;
    let run_id = values
        .remove("--run-id")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--run-id is required"))?;
    let max_runtime_secs = values
        .remove("--max-runtime-secs")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let stop_after_complete_polymarket_market = values
        .remove("--stop-after-complete-polymarket-market")
        .map(|value| parse_positive_u32("--stop-after-complete-polymarket-market", &value))
        .transpose()?;
    Ok(CliArgs {
        dataset_root: PathBuf::from(dataset_root),
        run_id,
        max_runtime_secs,
        collector_region: values.remove("--collector-region"),
        config_path: values.remove("--config").map(PathBuf::from),
        stop_after_complete_polymarket_market,
    })
}

async fn startup_receipt(config: &DeploymentRuntimeConfig) -> Result<RuntimeReceipt> {
    let current_exe = env::current_exe()?;
    let binary_sha256 = sha256_file(&current_exe)?;
    let git_commit = option_env!("DATA_COLLECT_GIT_COMMIT")
        .or(option_env!("GIT_COMMIT"))
        .unwrap_or("unknown-build-git-commit");
    let config_digest = config_digest(config)?;
    let collector_region = match config.collector_region_override.as_deref() {
        Some(region) if !region.trim().is_empty() => region.trim().to_owned(),
        _ => resolve_collector_region().await,
    };
    RuntimeReceipt::new(
        config,
        git_commit,
        binary_sha256,
        config_digest,
        hostname(),
        collector_region,
        host_clock_sync_status(),
    )
}

fn write_startup_receipt(output_root: &Path, receipt: &RuntimeReceipt) -> Result<()> {
    let path = output_root.join(format!("{}.startup_receipt.json", receipt.run_id));
    let payload = serde_json::to_vec_pretty(receipt)?;
    fs::write(path, payload)?;
    Ok(())
}

fn canonical_config_path(path: Option<&Path>) -> Result<Option<PathBuf>> {
    match path {
        Some(path) => Ok(Some(path.canonicalize()?)),
        None => Ok(None),
    }
}

fn config_digest(config: &DeploymentRuntimeConfig) -> Result<String> {
    let payload = serde_json::to_vec(config)?;
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    Ok(format!("{:x}", hasher.finalize()))
}

fn hostname() -> String {
    fs::read_to_string("/etc/hostname")
        .ok()
        .and_then(|value| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_owned())
        })
        .unwrap_or_else(|| "unknown-host".to_owned())
}

fn host_clock_sync_status() -> String {
    if Path::new("/run/systemd/timesync/synchronized").exists() {
        "systemd-timesyncd synchronized marker present".to_owned()
    } else {
        "unavailable: host clock sync status not probed by minimal runtime".to_owned()
    }
}

fn usage() -> &'static str {
    "usage: --dataset-root <absolute-path> --run-id <id> [--max-runtime-secs <seconds>] [--collector-region <region>] [--config <path>] [--stop-after-complete-polymarket-market <count>]"
}

async fn run_synthetic_training_runtime(
    dataset_root: &Path,
    run_id: &str,
    collector_region: &str,
    stop_after_complete_polymarket_market: Option<u32>,
    retention_policy_receipt_path: &Path,
) -> Result<TrainingRuntimeReceipt> {
    let run_started_ms = now_ms();
    let mut finalized_segments = Vec::new();
    let mut source_coverage = Vec::new();
    let manual_review = stop_after_complete_polymarket_market.map(manual_review_receipt);
    let mode = if manual_review.is_some() {
        "manual_review_complete_market"
    } else {
        "synthetic"
    };
    let rollover_reason = if manual_review.is_some() {
        "manual_review_complete_market_shutdown"
    } else {
        "shutdown_finalize"
    };

    for (index, source_kind) in SYNTHETIC_SOURCES.iter().enumerate() {
        let recv_time_utc_ms = run_started_ms + index as i64;
        let naming = SegmentNaming::new(dataset_root, source_kind, recv_time_utc_ms, 0, run_id)?;
        let event = synthetic_raw_event(source_kind, recv_time_utc_ms, collector_region, index);
        let published = write_segment_on_blocking_pool_for_source(
            naming.partition_path.clone(),
            naming.stem.clone(),
            "parquet_raw_layout_v1".to_owned(),
            (*source_kind).to_owned(),
            vec![vec![event]],
        )
        .await?;
        let manifest = serde_json::json!({
            "run_id": run_id,
            "source_kind": source_kind,
            "artifact_path": published.final_path,
            "row_count": published.row_count,
            "sha256": published.sha256,
            "schema_version": published.schema_version,
            "recv_time_window_ms": {
                "start": recv_time_utc_ms,
                "end": recv_time_utc_ms
            },
            "source_timestamp_range": {
                "start": recv_time_utc_ms.to_string(),
                "end": recv_time_utc_ms.to_string()
            },
            "event_available_time_semantics": "collector_recv_time",
            "market_slug_window": if *source_kind == "polymarket_market" {
                Some("btc-updown-5m-synthetic")
            } else {
                None
            },
            "segment_seq": 0,
            "rollover_reason": rollover_reason,
            "soft_rollover_not_process_fatal": true,
            "shutdown_finalize_result": "finalized"
        });
        fs::write(&naming.manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        source_coverage.push(SourceCoverage {
            source_kind: (*source_kind).to_owned(),
            first_recv_time_utc_ms: recv_time_utc_ms,
            last_recv_time_utc_ms: recv_time_utc_ms,
            rows: published.row_count,
        });
        finalized_segments.push(FinalizedRuntimeSegment {
            source_kind: (*source_kind).to_owned(),
            parquet_path: published.final_path,
            manifest_path: naming.manifest_path,
            row_count: published.row_count,
            sha256: published.sha256,
        });
    }

    let run_manifest_dir = dataset_root.join("training").join("run_manifests");
    fs::create_dir_all(&run_manifest_dir)?;
    let run_manifest_path = run_manifest_dir.join(format!("{run_id}.manifest.json"));
    let run_manifest = serde_json::json!({
        "run_id": run_id,
        "mode": mode,
        "manual_review_complete_market": manual_review,
        "retention_policy_receipt_path": retention_policy_receipt_path,
        "source_coverage": source_coverage,
        "source_gaps": [],
        "resource_watermark": {
            "rss_bytes": null,
            "unavailable": [{"metric": "rss_bytes", "reason": "synthetic runtime contract"}]
        },
        "writer_used_blocking_pool": true,
        "recv_time_utc_ms": now_ms()
    });
    fs::write(
        &run_manifest_path,
        serde_json::to_vec_pretty(&run_manifest)?,
    )?;

    Ok(TrainingRuntimeReceipt {
        run_id: run_id.to_owned(),
        run_manifest_path,
        finalized_segments,
        source_coverage,
        source_gaps: Vec::new(),
        resource_watermark_recorded: true,
        writer_used_blocking_pool: true,
    })
}

fn parse_positive_u32(flag: &str, value: &str) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    if parsed == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} must be greater than zero"),
        )
        .into());
    }
    Ok(parsed)
}

fn manual_review_receipt(target_market_count: u32) -> serde_json::Value {
    serde_json::json!({
        "target_market_count": target_market_count,
        "selected_market_slug": null,
        "selected_condition_id": null,
        "subscribed_asset_ids_for_market": [],
        "planned_start_utc": null,
        "planned_end_utc": null,
        "preload_start_utc": null,
        "stop_reason": "manual_review_complete_market_shutdown"
    })
}

fn synthetic_raw_event(
    source_kind: &str,
    recv_time_utc_ms: i64,
    collector_region: &str,
    index: usize,
) -> RawEvent {
    RawEvent {
        source_kind: source_kind.to_owned(),
        source_event_type: "synthetic".to_owned(),
        source_identifier: format!("{source_kind}:{index}"),
        source_ts: Some(recv_time_utc_ms.to_string()),
        recv_time_utc_ms,
        collector_host_id: hostname(),
        collector_region: collector_region.to_owned(),
        source_endpoint: format!("synthetic://{source_kind}"),
        raw_json: serde_json::json!({
            "event_type": "synthetic",
            "source_kind": source_kind,
            "seq": index
        })
        .to_string(),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind.to_owned(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn enforce_startup_retention_policy(dataset_root: &Path, run_id: &str) -> Result<PathBuf> {
    let relative_path = PathBuf::from("training")
        .join("runtime_artifacts")
        .join(run_id)
        .join("retention_policy_receipt.json");
    let absolute_path = dataset_root.join(&relative_path);
    TrainingRetentionPolicy::new(dataset_root)?.enforce(&absolute_path)?;
    Ok(relative_path)
}
