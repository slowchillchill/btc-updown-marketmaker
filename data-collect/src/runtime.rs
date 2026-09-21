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
    CollectorBinaryRole, CollectorStartupReceipt, DeploymentRuntimeConfig, Result,
};
use crate::handoff::{HandoffRuntimeConfig, run_source_handoff_server};
use crate::publish::PaperLiveEnvironment;
use crate::training_runtime::{
    TrainingRuntimeConfig, production_training_source_endpoints, run_hot_sources,
    run_training_runtime_for_cli,
};
use crate::writer::sha256_file;

pub const RESOURCE_HARD_THRESHOLD_EXIT_CODE: i32 = 75;
pub const OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE: i32 = 76;
pub const TRAINING_COLLECTOR_USAGE: &str = "usage: data-collect-training-collector --dataset-root <absolute-path> [--collector-region <region>] [--run-forever | --max-runtime-secs <seconds>]";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CliArgs {
    pub dataset_root: PathBuf,
    pub max_runtime_secs: Option<u64>,
    pub run_forever: bool,
    pub collector_region: Option<String>,
}

#[derive(Serialize)]
struct TrainingConfigDigest<'a> {
    contract_version: &'static str,
    args: &'a CliArgs,
    polymarket_market_endpoint: &'a str,
    polymarket_rtds_endpoint: &'a str,
    binance_depth_endpoint: &'a str,
    binance_aggtrade_endpoint: &'a str,
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

    run_training_runtime_for_cli(TrainingRuntimeConfig {
        dataset_root: cli.dataset_root,
        duration_secs: cli.max_runtime_secs,
        run_forever: cli.run_forever,
        collector_host_id: hostname(),
        collector_region,
        code_version,
        collector_binary_sha256,
        source_config_digest,
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
            "--dataset-root" | "--max-runtime-secs" | "--collector-region" => {
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
    Ok(CliArgs {
        dataset_root: PathBuf::from(dataset_root),
        max_runtime_secs,
        run_forever,
        collector_region: values.remove("--collector-region"),
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
    let endpoints = production_training_source_endpoints();
    let digest_input = TrainingConfigDigest {
        contract_version: "data_collect_training_config_v2",
        args: cli,
        polymarket_market_endpoint: &endpoints.polymarket_market,
        polymarket_rtds_endpoint: &endpoints.polymarket_rtds,
        binance_depth_endpoint: &endpoints.binance_depth,
        binance_aggtrade_endpoint: &endpoints.binance_aggtrade,
    };
    sha256_serialized(&digest_input)
}

async fn run_hot_collector(
    role: CollectorBinaryRole,
    args: impl IntoIterator<Item = OsString>,
) -> Result<()> {
    run_hot_collector_with_runner(role, args, run_hot_sources).await
}

pub(crate) async fn run_hot_collector_with_runner<F, Fut>(
    role: CollectorBinaryRole,
    args: impl IntoIterator<Item = OsString>,
    run_sources: F,
) -> Result<()>
where
    F: FnOnce(
        tokio::net::UnixStream,
        CollectorStartupReceipt,
        std::time::Instant,
        Option<std::time::Instant>,
        Option<i64>,
    ) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
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

    fs::create_dir_all(&config.output_root)?;
    let receipt = startup_receipt(&config).await?;
    write_startup_receipt(&config.output_root, &receipt)?;
    let environment = match role {
        CollectorBinaryRole::PaperHot => PaperLiveEnvironment::Paper,
        CollectorBinaryRole::LiveHot => PaperLiveEnvironment::Live,
        CollectorBinaryRole::TrainingCold => unreachable!("training uses its own entry point"),
    };
    let handoff_config = HandoffRuntimeConfig::for_environment(&config.dataset_root, environment)?;
    run_source_handoff_server(
        &handoff_config,
        receipt,
        config.max_runtime_secs,
        run_sources,
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

async fn startup_receipt(config: &DeploymentRuntimeConfig) -> Result<CollectorStartupReceipt> {
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
    Ok(CollectorStartupReceipt::new(
        config,
        git_commit,
        binary_sha256,
        config_digest,
        hostname(),
        collector_region,
        host_clock_sync_status(),
    ))
}

fn write_startup_receipt(output_root: &Path, receipt: &CollectorStartupReceipt) -> Result<()> {
    let path = output_root.join(format!("{}.startup_receipt.json", receipt.run_id));
    fs::write(path, serde_json::to_vec_pretty(receipt)?)?;
    Ok(())
}

fn canonical_config_path(path: Option<&Path>) -> Result<Option<PathBuf>> {
    path.map(|path| {
        let path = path.canonicalize()?;
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("config file does not exist: {}", path.display()),
            )
            .into());
        }
        Ok(path)
    })
    .transpose()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hot_role_starts_with_a_length_framed_session_before_sources() {
        use tokio::io::AsyncReadExt as _;
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("paper/data_collect_handoff/hot.sock");
        let args = vec![
            OsString::from("--dataset-root"),
            root.path().as_os_str().to_owned(),
            OsString::from("--run-id"),
            OsString::from("session-contract"),
            OsString::from("--collector-region"),
            OsString::from("fixture"),
            OsString::from("--max-runtime-secs"),
            OsString::from("1"),
        ];
        let server = tokio::spawn(run_hot_collector(CollectorBinaryRole::PaperHot, args));
        let mut stream = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Ok(stream) = tokio::net::UnixStream::connect(&socket).await {
                    break stream;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut prefix = [0_u8; 8];
        stream.read_exact(&mut prefix).await.unwrap();
        assert_eq!(
            &prefix[..4],
            &[0; 4],
            "the fixed Session must use the u64 frame header"
        );
        let mut body = vec![0; u64::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut body).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value["Session"]["frame_schema_version"],
            "data_collect_handoff_v2"
        );
        assert_eq!(value["Session"]["run_id"], "session-contract");
        drop(stream);
        assert!(
            server.await.unwrap().is_err(),
            "unconfirmed Session is incomplete"
        );
        assert!(!root.path().join("training").exists());
    }

    #[tokio::test]
    async fn hot_deadline_without_consumer_never_starts_sources() {
        let root = tempfile::tempdir().unwrap();
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = std::sync::Arc::clone(&started);
        let args = vec![
            OsString::from("--dataset-root"),
            root.path().as_os_str().to_owned(),
            OsString::from("--run-id"),
            OsString::from("no-consumer"),
            OsString::from("--collector-region"),
            OsString::from("fixture"),
            OsString::from("--max-runtime-secs"),
            OsString::from("1"),
        ];
        run_hot_collector_with_runner(CollectorBinaryRole::PaperHot, args, move |_, _, _, _, _| {
            observed.store(true, std::sync::atomic::Ordering::Relaxed);
            async { Ok(()) }
        })
        .await
        .unwrap();
        assert!(!started.load(std::sync::atomic::Ordering::Relaxed));
        assert!(
            !root
                .path()
                .join("paper/data_collect_handoff/hot.sock")
                .exists()
        );
        assert!(!root.path().join("training").exists());
    }

    #[tokio::test]
    async fn hot_explicit_deadline_includes_consumer_and_session_waits() {
        use crate::handoff::{read_handoff_v2, write_handoff_v2};
        use data_pipeline_core::public_input::DataCollectHandoffV2;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
        for confirm in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("paper/data_collect_handoff/hot.sock");
            let started_sources = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&started_sources);
            let args = vec![
                "--dataset-root".into(),
                root.path().as_os_str().to_owned(),
                "--run-id".into(),
                "wait-deadline".into(),
                "--collector-region".into(),
                "fixture".into(),
                "--max-runtime-secs".into(),
                "1".into(),
            ];
            let server = tokio::spawn(run_hot_collector_with_runner(
                CollectorBinaryRole::PaperHot,
                args,
                move |_, _, started, deadline, deadline_ms| async move {
                    observed.store(true, Ordering::Relaxed);
                    assert!(started.elapsed() >= Duration::from_millis(400));
                    assert_eq!(deadline, Some(started + Duration::from_secs(1)));
                    let wall_now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;
                    let wall_remaining = deadline_ms.unwrap() - wall_now;
                    let remaining = deadline
                        .unwrap()
                        .saturating_duration_since(Instant::now())
                        .as_millis() as i64;
                    assert!(
                        (wall_remaining - remaining).abs() < 100,
                        "UTC 与单调截止必须对应同一期限"
                    );
                    Ok(())
                },
            ));
            tokio::time::timeout(Duration::from_secs(30), async {
                while !socket.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
            assert!(matches!(
                read_handoff_v2(&mut stream).await.unwrap(),
                DataCollectHandoffV2::Session(_)
            ));
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(!started_sources.load(Ordering::Relaxed));
            if confirm {
                write_handoff_v2(
                    &mut stream,
                    &DataCollectHandoffV2::Received {
                        transport_sequence: 0,
                    },
                )
                .await
                .unwrap();
                server.await.unwrap().unwrap();
                assert!(started_sources.load(Ordering::Relaxed));
            } else {
                let error = tokio::time::timeout(Duration::from_millis(750), server)
                    .await
                    .expect("原一秒期限必须包含消费者和 Session 等待")
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("hot session was not confirmed before runtime cutoff")
                );
                assert!(!started_sources.load(Ordering::Relaxed));
            }
            assert!(!socket.exists());
        }
    }
}
