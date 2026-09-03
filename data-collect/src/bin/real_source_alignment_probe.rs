use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use data_collect::daily_manifest::DAILY_MANIFEST_FILE_NAME;
use data_collect::runtime::run_training_collector;
use serde::Serialize;
use serde_json::{Value, json};

const DEFAULT_DURATION_SECS: u64 = 45 * 60;
static RUN_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("real_source_alignment_probe_error={error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config = ProbeConfig::from_env_and_args()?;
    prepare_artifact_dir(
        &config.artifact_dir,
        config.dataset_root.as_deref(),
        &config.run_id,
    )?;

    let artifact_dir = fs::canonicalize(&config.artifact_dir)?;
    let production_dataset_root = artifact_dir.join("production-data");
    let mut collector_args = vec![
        OsString::from("--dataset-root"),
        production_dataset_root.as_os_str().to_owned(),
        OsString::from("--max-runtime-secs"),
        OsString::from(config.duration_secs.to_string()),
    ];
    if let Some(region) = config.collector_region {
        collector_args.push(OsString::from("--collector-region"));
        collector_args.push(OsString::from(region));
    }

    run_training_collector(collector_args).await?;

    let summary =
        summarize_production_artifacts(&config.run_id, &artifact_dir, &production_dataset_root)?;
    write_json_create_new(
        &artifact_dir.join("probe_summary.json"),
        &serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[derive(Debug)]
struct ProbeConfig {
    run_id: String,
    artifact_dir: PathBuf,
    dataset_root: Option<PathBuf>,
    duration_secs: u64,
    collector_region: Option<String>,
}

impl ProbeConfig {
    fn from_env_and_args() -> Result<Self> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        let dataset_root = std::env::var("DATASET_ROOT").ok().map(PathBuf::from);
        let explicit_artifact_dir = artifact_dir_from_args(&args)?.or_else(|| {
            std::env::var("REAL_SOURCE_ARTIFACT_DIR")
                .ok()
                .filter(|path| !path.trim().is_empty())
                .map(PathBuf::from)
        });
        let explicit_run_id = std::env::var("REAL_SOURCE_RUN_ID").ok();
        let current_dir = std::env::current_dir()?;
        let (run_id, artifact_dir) = resolve_run_identity(
            explicit_artifact_dir,
            dataset_root.as_deref(),
            explicit_run_id.as_deref(),
            &current_dir,
        )?;

        Ok(Self {
            run_id,
            artifact_dir,
            dataset_root,
            duration_secs: env_u64("REAL_SOURCE_RUN_SECS", DEFAULT_DURATION_SECS),
            collector_region: std::env::var("COLLECTOR_REGION")
                .ok()
                .filter(|region| !region.trim().is_empty()),
        })
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct ProbeSummary {
    run_id: String,
    artifact_dir: PathBuf,
    production_dataset_root: PathBuf,
    manifest_count: usize,
    manifests: Vec<ProductionManifestSummary>,
    completed_at_utc_ms: i64,
}

#[derive(Debug, PartialEq, Serialize)]
struct ProductionManifestSummary {
    path: PathBuf,
    schema_version: Option<String>,
    utc_day: Option<String>,
    config_digest: Option<String>,
    finalized_file_count: usize,
    source_counter_count: usize,
    gap_count: usize,
    collection_day_coverage: Option<Value>,
}

fn summarize_production_artifacts(
    run_id: &str,
    artifact_dir: &Path,
    production_dataset_root: &Path,
) -> Result<ProbeSummary> {
    let mut manifest_paths = Vec::new();
    if production_dataset_root.exists() {
        collect_named_files(
            production_dataset_root,
            DAILY_MANIFEST_FILE_NAME,
            &mut manifest_paths,
        )?;
    }
    manifest_paths.sort();

    let manifests = manifest_paths
        .into_iter()
        .map(|path| {
            let value = serde_json::from_slice::<Value>(&fs::read(&path)?)?;
            Ok(ProductionManifestSummary {
                path: path
                    .strip_prefix(artifact_dir)
                    .unwrap_or(path.as_path())
                    .to_path_buf(),
                schema_version: json_string(&value, "schema_version"),
                utc_day: json_string(&value, "utc_day"),
                config_digest: json_string(&value, "config_digest"),
                finalized_file_count: json_collection_len(&value, "files"),
                source_counter_count: json_collection_len(&value, "source_counters"),
                gap_count: json_collection_len(&value, "gaps"),
                collection_day_coverage: value.get("collection_day_coverage").cloned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ProbeSummary {
        run_id: run_id.to_owned(),
        artifact_dir: artifact_dir.to_path_buf(),
        production_dataset_root: production_dataset_root.to_path_buf(),
        manifest_count: manifests.len(),
        manifests,
        completed_at_utc_ms: now_ms(),
    })
}

fn collect_named_files(root: &Path, file_name: &str, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_named_files(&entry.path(), file_name, paths)?;
        } else if file_type.is_file() && entry.file_name() == file_name {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn json_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn json_collection_len(value: &Value, field: &str) -> usize {
    value
        .get(field)
        .and_then(|value| match value {
            Value::Array(values) => Some(values.len()),
            Value::Object(values) => Some(values.len()),
            _ => None,
        })
        .unwrap_or_default()
}

fn artifact_dir_from_args(args: &[String]) -> Result<Option<PathBuf>> {
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--artifact-dir" {
            let path = args
                .get(index + 1)
                .ok_or("--artifact-dir requires a following path")?;
            return Ok(Some(PathBuf::from(path)));
        }
        index += 1;
    }
    Ok(None)
}

fn resolve_run_identity(
    explicit_artifact_dir: Option<PathBuf>,
    dataset_root: Option<&Path>,
    explicit_run_id: Option<&str>,
    current_dir: &Path,
) -> Result<(String, PathBuf)> {
    let run_id = match explicit_run_id {
        Some(run_id) => {
            validate_run_id(run_id)?;
            run_id.to_owned()
        }
        None => format!(
            "real-source-alignment-{}-{}-{}",
            now_ms(),
            std::process::id(),
            RUN_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
    };
    let artifact_dir = match explicit_artifact_dir {
        Some(path) => path,
        None => match dataset_root {
            Some(root) => root.join("real-source-alignment-artifacts").join(&run_id),
            None => current_dir
                .join("real-source-alignment-artifacts")
                .join(&run_id),
        },
    };
    Ok((run_id, artifact_dir))
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty()
        || run_id == "."
        || run_id == ".."
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(format!("REAL_SOURCE_RUN_ID is not a safe path component: {run_id:?}").into());
    }
    Ok(())
}

fn prepare_artifact_dir(
    artifact_dir: &Path,
    dataset_root: Option<&Path>,
    run_id: &str,
) -> Result<()> {
    if let Some(dataset_root) = dataset_root {
        fs::create_dir_all(dataset_root)?;
    }
    fs::create_dir_all(artifact_dir)?;

    if let Some(dataset_root) = dataset_root {
        let root = fs::canonicalize(dataset_root)?;
        let artifact = fs::canonicalize(artifact_dir)?;
        if !artifact.starts_with(&root) {
            return Err(format!(
                "artifact dir {} is outside DATASET_ROOT {}",
                artifact.display(),
                root.display()
            )
            .into());
        }
    }

    if fs::read_dir(artifact_dir)?.next().transpose()?.is_some() {
        return Err(format!("artifact dir is not empty: {}", artifact_dir.display()).into());
    }

    write_json_create_new(
        &artifact_dir.join("run_owner.json"),
        &serde_json::to_vec_pretty(&json!({
            "run_id": run_id,
            "pid": std::process::id(),
            "created_at_utc_ms": now_ms(),
            "artifact_dir": artifact_dir,
        }))?,
    )
}

fn write_json_create_new(path: &Path, payload: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(payload)?;
    Ok(())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    #[test]
    fn cargo_metadata_exposes_probe_only_as_bin() {
        let output = Command::new(env!("CARGO"))
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--locked",
                "--offline",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect("cargo metadata must run");
        assert!(
            output.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let metadata: Value =
            serde_json::from_slice(&output.stdout).expect("metadata must be valid JSON");
        let targets = metadata["packages"]
            .as_array()
            .expect("metadata packages must be an array")
            .iter()
            .flat_map(|package| {
                package["targets"]
                    .as_array()
                    .expect("package targets must be an array")
            })
            .filter(|target| target["name"] == "real_source_alignment_probe")
            .collect::<Vec<_>>();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["kind"], json!(["bin"]));
    }

    #[test]
    fn default_run_identity_is_unique_and_consistent() -> Result<()> {
        let root = tempdir()?;
        let (first_run_id, first_dir) = resolve_run_identity(None, None, None, root.path())?;
        let (second_run_id, second_dir) = resolve_run_identity(None, None, None, root.path())?;

        assert_ne!(first_run_id, second_run_id);
        assert_ne!(first_dir, second_dir);
        assert!(first_run_id.starts_with("real-source-alignment-"));
        assert_eq!(
            first_dir
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str()),
            Some("real-source-alignment-artifacts")
        );
        assert_eq!(
            first_dir.file_name().and_then(|name| name.to_str()),
            Some(first_run_id.as_str())
        );
        assert_eq!(
            second_dir.file_name().and_then(|name| name.to_str()),
            Some(second_run_id.as_str())
        );

        prepare_artifact_dir(&first_dir, None, &first_run_id)?;
        prepare_artifact_dir(&second_dir, None, &second_run_id)?;
        let first_owner: Value =
            serde_json::from_slice(&fs::read(first_dir.join("run_owner.json"))?)?;
        let second_owner: Value =
            serde_json::from_slice(&fs::read(second_dir.join("run_owner.json"))?)?;
        assert_eq!(first_owner["run_id"], first_run_id);
        assert_eq!(second_owner["run_id"], second_run_id);
        Ok(())
    }

    #[test]
    fn nonempty_explicit_artifact_dir_is_preserved() -> Result<()> {
        let root = tempdir()?;
        let artifact_dir = root.path().join("explicit-artifacts");
        fs::create_dir_all(&artifact_dir)?;
        let sentinel_path = artifact_dir.join("sentinel.txt");
        let sentinel = b"existing artifact bytes";
        fs::write(&sentinel_path, sentinel)?;

        let error = prepare_artifact_dir(&artifact_dir, None, "explicit-run")
            .expect_err("nonempty explicit artifact directory must be rejected");
        assert!(error.to_string().contains("artifact dir is not empty"));
        assert_eq!(fs::read(&sentinel_path)?, sentinel);
        assert!(!artifact_dir.join("run_owner.json").exists());
        Ok(())
    }

    #[test]
    fn offline_summary_reads_only_production_manifest_facts() -> Result<()> {
        let root = tempdir()?;
        let artifact_dir = root.path().join("probe");
        let production_root = artifact_dir.join("production-data");
        let day_dir = production_root
            .join("training")
            .join("typed_raw")
            .join("utc_day=2026-09-03");
        fs::create_dir_all(&day_dir)?;
        fs::write(
            day_dir.join(DAILY_MANIFEST_FILE_NAME),
            serde_json::to_vec(&json!({
                "schema_version": "data_collect_daily_manifest_v3",
                "utc_day": "2026-09-03",
                "config_digest": "digest",
                "files": [{"path": "one.parquet"}, {"path": "two.parquet"}],
                "source_counters": {"binance_aggtrade": {"rows": 2}},
                "gaps": ["fixture-gap"],
                "collection_day_coverage": {"state": "ended_known"}
            }))?,
        )?;

        let summary =
            summarize_production_artifacts("offline-run", &artifact_dir, &production_root)?;
        assert_eq!(summary.manifest_count, 1);
        assert_eq!(summary.manifests[0].finalized_file_count, 2);
        assert_eq!(summary.manifests[0].source_counter_count, 1);
        assert_eq!(summary.manifests[0].gap_count, 1);
        assert_eq!(
            summary.manifests[0].config_digest.as_deref(),
            Some("digest")
        );
        assert_eq!(
            summary.manifests[0].path,
            PathBuf::from("production-data/training/typed_raw/utc_day=2026-09-03/manifest.json")
        );
        Ok(())
    }
}
