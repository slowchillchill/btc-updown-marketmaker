use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::collector_identity::resolve_collector_region;
use crate::deployment::{
    CollectorBinaryRole, DeploymentRuntimeConfig, Result, RuntimeReceipt, validate_runtime_config,
};
use crate::handoff::{HandoffRuntimeConfig, run_heartbeat_handoff_server};
use crate::live_runtime::{LiveTrainingRuntimeConfig, run_live_training_runtime};
use crate::publish::PaperLiveEnvironment;
use crate::writer::sha256_file;

pub const RESOURCE_HARD_THRESHOLD_EXIT_CODE: i32 = 75;
pub const OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE: i32 = 76;
pub const DEFAULT_TRAINING_DATASET_MAX_BYTES: u64 = 50_000_000_000;
pub const TRAINING_COLLECTOR_USAGE: &str = "usage: data-collect-training-collector --dataset-root <absolute-path> [--collector-region <region>] [--run-forever | --max-runtime-secs <seconds>] [--max-dataset-bytes <bytes>]";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CliArgs {
    pub dataset_root: PathBuf,
    pub max_runtime_secs: Option<u64>,
    pub run_forever: bool,
    pub collector_region: Option<String>,
    pub max_dataset_bytes: u64,
}

#[derive(Serialize)]
struct TrainingConfigDigest<'a> {
    contract_version: &'static str,
    args: &'a CliArgs,
    polymarket_market_endpoint: &'static str,
    polymarket_rtds_endpoint: &'static str,
    binance_depth_endpoint: &'static str,
    binance_aggtrade_endpoint: &'static str,
}

#[derive(Debug)]
struct HotCliArgs {
    dataset_root: PathBuf,
    run_id: String,
    max_runtime_secs: Option<u64>,
    collector_region: Option<String>,
    config_path: Option<PathBuf>,
}

pub async fn run_collector_from_env(role: CollectorBinaryRole) -> Result<()> {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    match role {
        CollectorBinaryRole::TrainingCold => run_training_collector(args).await,
        CollectorBinaryRole::PaperHot | CollectorBinaryRole::LiveHot => {
            run_hot_collector(role, args).await
        }
    }
}

pub async fn run_training_collector<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let cli = parse_args(args).map_err(operator_intervention_error)?;
    validate_training_cli(&cli).map_err(operator_intervention_error)?;

    let current_exe = env::current_exe().map_err(operator_intervention_error)?;
    let collector_binary_sha256 = sha256_file(&current_exe).map_err(operator_intervention_error)?;
    let code_version = option_env!("DATA_COLLECT_GIT_COMMIT")
        .or(option_env!("GIT_COMMIT"))
        .unwrap_or("unknown-build-git-commit")
        .to_owned();
    let source_config_digest = training_config_digest(&cli)?;
    let collector_region = match cli.collector_region.as_deref() {
        Some(region) => region.trim().to_owned(),
        None => resolve_collector_region().await,
    };

    run_live_training_runtime(LiveTrainingRuntimeConfig {
        dataset_root: cli.dataset_root,
        duration_secs: cli.max_runtime_secs,
        run_forever: cli.run_forever,
        collector_host_id: hostname(),
        collector_region,
        code_version,
        collector_binary_sha256,
        source_config_digest,
        max_dataset_bytes: cli.max_dataset_bytes,
    })
    .await?;
    Ok(())
}

pub fn parse_args<I, S>(args: I) -> Result<CliArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut values = BTreeMap::new();
    let mut run_forever = false;
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        let arg = os_string(arg, "argument")?;
        match arg.as_str() {
            "--run-forever" => run_forever = true,
            "--dataset-root"
            | "--max-runtime-secs"
            | "--collector-region"
            | "--max-dataset-bytes" => {
                let value = iter.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{arg} requires a value"),
                    )
                })?;
                values.insert(arg, os_string(value, "argument value")?);
            }
            "--help" | "-h" | "--version" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--help/--version must use the binary fast path",
                )
                .into());
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}\n{TRAINING_COLLECTOR_USAGE}"),
                )
                .into());
            }
        }
    }

    let dataset_root = values
        .remove("--dataset-root")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--dataset-root is required"))?;
    let max_runtime_secs = values
        .remove("--max-runtime-secs")
        .map(|value| parse_positive_u64("--max-runtime-secs", &value))
        .transpose()?;
    if run_forever && max_runtime_secs.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--run-forever cannot be combined with --max-runtime-secs",
        )
        .into());
    }
    let max_dataset_bytes = values
        .remove("--max-dataset-bytes")
        .map(|value| parse_positive_u64("--max-dataset-bytes", &value))
        .transpose()?
        .unwrap_or(DEFAULT_TRAINING_DATASET_MAX_BYTES);

    Ok(CliArgs {
        dataset_root: PathBuf::from(dataset_root),
        max_runtime_secs,
        run_forever,
        collector_region: values.remove("--collector-region"),
        max_dataset_bytes,
    })
}

pub fn collector_exit_code(error: &(dyn std::error::Error + 'static)) -> i32 {
    let message = error.to_string();
    if message.contains("operator_intervention_required:") {
        OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE
    } else if message.contains("dataset_capacity_reached") {
        RESOURCE_HARD_THRESHOLD_EXIT_CODE
    } else {
        1
    }
}

pub fn training_collector_version() -> String {
    format!(
        "data-collect-training-collector {}",
        env!("CARGO_PKG_VERSION")
    )
}

fn validate_training_cli(cli: &CliArgs) -> io::Result<()> {
    if !cli.dataset_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--dataset-root must be absolute",
        ));
    }
    if cli
        .collector_region
        .as_deref()
        .is_some_and(|region| region.trim().is_empty())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--collector-region must not be empty",
        ));
    }
    Ok(())
}

fn training_config_digest(cli: &CliArgs) -> Result<String> {
    let digest_input = TrainingConfigDigest {
        contract_version: "data_collect_training_config_v2",
        args: cli,
        polymarket_market_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        polymarket_rtds_endpoint: "wss://ws-live-data.polymarket.com",
        binance_depth_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
        binance_aggtrade_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    };
    sha256_serialized(&digest_input)
}

async fn run_hot_collector(
    role: CollectorBinaryRole,
    args: impl IntoIterator<Item = OsString>,
) -> Result<()> {
    let cli = parse_hot_args(args).map_err(operator_intervention_error)?;
    fs::create_dir_all(&cli.dataset_root).map_err(operator_intervention_error)?;
    let dataset_root = cli
        .dataset_root
        .canonicalize()
        .map_err(operator_intervention_error)?;
    let mut config = DeploymentRuntimeConfig::for_role(role, dataset_root, cli.run_id)
        .map_err(operator_intervention_error)?;
    config.max_runtime_secs = cli.max_runtime_secs;
    config.collector_region_override = cli.collector_region;
    config.config_path =
        canonical_config_path(cli.config_path.as_deref()).map_err(operator_intervention_error)?;
    validate_runtime_config(&config).map_err(operator_intervention_error)?;

    fs::create_dir_all(&config.output_root)?;
    let receipt = startup_receipt(&config).await?;
    write_startup_receipt(&config.output_root, &receipt)?;
    let environment = match role {
        CollectorBinaryRole::PaperHot => PaperLiveEnvironment::Paper,
        CollectorBinaryRole::LiveHot => PaperLiveEnvironment::Live,
        CollectorBinaryRole::TrainingCold => unreachable!("training uses its own entry point"),
    };
    let handoff_config = HandoffRuntimeConfig::for_environment(&config.dataset_root, environment)?;
    run_heartbeat_handoff_server(
        &handoff_config,
        &receipt.collector_host_id,
        &receipt.collector_region,
        config.max_runtime_secs,
    )
    .await
}

fn parse_hot_args(args: impl IntoIterator<Item = OsString>) -> Result<HotCliArgs> {
    let mut values = BTreeMap::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let arg = os_string(arg, "argument")?;
        match arg.as_str() {
            "--dataset-root" | "--run-id" | "--max-runtime-secs" | "--collector-region"
            | "--config" => {
                let value = iter.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{arg} requires a value"),
                    )
                })?;
                values.insert(arg, os_string(value, "argument value")?);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown hot-collector argument: {arg}"),
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
        .map(|value| parse_positive_u64("--max-runtime-secs", &value))
        .transpose()?;
    Ok(HotCliArgs {
        dataset_root: PathBuf::from(dataset_root),
        run_id,
        max_runtime_secs,
        collector_region: values.remove("--collector-region"),
        config_path: values.remove("--config").map(PathBuf::from),
    })
}

async fn startup_receipt(config: &DeploymentRuntimeConfig) -> Result<RuntimeReceipt> {
    let current_exe = env::current_exe()?;
    let binary_sha256 = sha256_file(&current_exe)?;
    let git_commit = option_env!("DATA_COLLECT_GIT_COMMIT")
        .or(option_env!("GIT_COMMIT"))
        .unwrap_or("unknown-build-git-commit");
    let config_digest = sha256_serialized(config)?;
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
    fs::write(path, serde_json::to_vec_pretty(receipt)?)?;
    Ok(())
}

fn canonical_config_path(path: Option<&Path>) -> Result<Option<PathBuf>> {
    path.map(Path::canonicalize).transpose().map_err(Into::into)
}

fn sha256_serialized(value: &impl Serialize) -> Result<String> {
    let payload = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(payload);
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

fn parse_positive_u64(flag: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
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

fn os_string(value: OsString, label: &str) -> Result<String> {
    value.into_string().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{label} is not UTF-8")).into()
    })
}

fn operator_intervention_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("operator_intervention_required:{error}"))
}
