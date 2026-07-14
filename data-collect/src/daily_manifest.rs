use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd as _;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::binance_aggtrade::BinanceAggTradeContinuityState;
use crate::writer::{DAILY_FILE_RECOVERY_METADATA_KEY, sha256_file};

pub const DAILY_MANIFEST_FILE_NAME: &str = "manifest.json";
pub const BINANCE_AGGTRADE_CONTINUITY_FIELD: &str = "binance_aggtrade_continuity";
pub const DAILY_MANIFEST_SCHEMA_VERSION: &str = "data_collect_daily_manifest_v3";
const MAX_MANIFEST_GAPS: usize = 256;
const HOUR_MS: i64 = 3_600_000;
const OPERATIONAL_COUNTER_FIELDS: &[&str] = &[
    "structural_dedupe_dropped_events",
    "lag_events",
    "reconnect_events",
    "dropped_rows",
];

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SourceOperationalCounterDelta {
    pub structural_dedupe_dropped_events: u64,
    pub lag_events: u64,
    pub reconnect_events: u64,
    pub dropped_rows: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DailyFinalizedFileDelta {
    pub utc_day: String,
    pub source_kind: String,
    pub utc_hour: String,
    pub part: u32,
    pub final_path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
    pub schema_version: String,
    pub first_recv_time_utc_ms: i64,
    pub last_recv_time_utc_ms: i64,
    pub source_timestamp_start: Option<String>,
    pub source_timestamp_end: Option<String>,
    pub event_type_counts: BTreeMap<String, u64>,
    pub facts: Value,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DailyFileRecoveryMetadata {
    pub source_kind: String,
    pub utc_hour: String,
    pub part: u32,
    pub rows: u64,
    pub schema_version: String,
    pub first_recv_time_utc_ms: i64,
    pub last_recv_time_utc_ms: i64,
    pub source_timestamp_start: Option<String>,
    pub source_timestamp_end: Option<String>,
    pub event_type_counts: BTreeMap<String, u64>,
    pub facts: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DailyManifestIdentity {
    pub build_commit: String,
    pub collector_binary_sha256: String,
    pub config_digest: String,
    pub collector_host_id: String,
    pub collector_region: String,
}

pub fn typed_raw_root(dataset_root: &Path) -> PathBuf {
    dataset_root.join("training").join("typed_raw")
}

pub fn utc_day_directories(dataset_root: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let root = typed_raw_root(dataset_root);
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut days = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(day) = name.strip_prefix("utc_day=") else {
            continue;
        };
        if !valid_utc_day(day) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid UTC day directory: {}", entry.path().display()),
            ));
        }
        days.push((day.to_owned(), entry.path()));
    }
    days.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(days)
}

pub fn remove_unowned_stale_temporary_files(dataset_root: &Path) -> io::Result<Vec<String>> {
    let expected_uid = current_process_uid()?;
    require_owned_real_directory(dataset_root, expected_uid, "dataset root")?;
    let dataset_root = dataset_root.canonicalize()?;
    let root = typed_raw_root(&dataset_root);
    let root_metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "typed raw root must be a real directory: {}",
                root.display()
            ),
        ));
    }
    require_expected_owner(&root, &root_metadata, expected_uid, "typed raw root")?;
    let canonical_root = root.canonicalize()?;
    if !canonical_root.starts_with(&dataset_root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("typed raw root escaped dataset root: {}", root.display()),
        ));
    }

    let mut removed = Vec::new();
    for day_entry in fs::read_dir(&canonical_root)? {
        let day_entry = day_entry?;
        let day_type = day_entry.file_type()?;
        if day_type.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "symlink is forbidden in typed raw root: {}",
                    day_entry.path().display()
                ),
            ));
        }
        if !day_type.is_dir() {
            continue;
        }
        require_expected_owner(
            &day_entry.path(),
            &day_entry.metadata()?,
            expected_uid,
            "UTC day directory",
        )?;
        remove_stale_files_in_directory(
            &canonical_root,
            &day_entry.path(),
            expected_uid,
            &mut removed,
        )?;
        for source_entry in fs::read_dir(day_entry.path())? {
            let source_entry = source_entry?;
            let source_type = source_entry.file_type()?;
            if source_type.is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "symlink is forbidden in UTC day directory: {}",
                        source_entry.path().display()
                    ),
                ));
            }
            if source_type.is_dir() {
                require_expected_owner(
                    &source_entry.path(),
                    &source_entry.metadata()?,
                    expected_uid,
                    "source directory",
                )?;
                remove_stale_files_in_directory(
                    &canonical_root,
                    &source_entry.path(),
                    expected_uid,
                    &mut removed,
                )?;
            }
        }
    }
    removed.sort();
    Ok(removed)
}

fn remove_stale_files_in_directory(
    canonical_root: &Path,
    directory: &Path,
    expected_uid: u32,
    removed: &mut Vec<String>,
) -> io::Result<()> {
    let canonical_directory = directory.canonicalize()?;
    if !canonical_directory.starts_with(canonical_root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stale cleanup directory escaped typed raw root: {}",
                directory.display()
            ),
        ));
    }
    for entry in fs::read_dir(&canonical_directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "symlink is forbidden during stale cleanup: {}",
                    entry.path().display()
                ),
            ));
        }
        if !file_type.is_file() || !is_stale_temporary_name(&entry.file_name().to_string_lossy()) {
            continue;
        }
        let path = entry.path();
        let metadata = entry.metadata()?;
        require_expected_owner(&path, &metadata, expected_uid, "stale temporary file")?;
        if accessible_process_holds(&path)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("stale temporary file is still open: {}", path.display()),
            ));
        }
        let _lease = acquire_exclusive_cleanup_lease(&path)?;
        fs::remove_file(&path)?;
        removed.push(
            path.strip_prefix(canonical_root)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "removed stale file escaped typed raw root",
                    )
                })?
                .to_string_lossy()
                .into_owned(),
        );
    }
    Ok(())
}

fn is_stale_temporary_name(name: &str) -> bool {
    name.ends_with(".tmp")
        || name.ends_with(".running")
        || name.ends_with("-running.parquet")
        || name.ends_with(".running.parquet")
        || name.ends_with("-digesting.parquet")
}

#[cfg(unix)]
fn current_process_uid() -> io::Result<u32> {
    Ok(fs::metadata("/proc/self")?.uid())
}

#[cfg(not(unix))]
fn current_process_uid() -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stale temporary owner check requires Unix",
    ))
}

#[cfg(unix)]
fn require_owned_real_directory(path: &Path, expected_uid: u32, label: &str) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} must be a real directory: {}", path.display()),
        ));
    }
    require_expected_owner(path, &metadata, expected_uid, label)
}

#[cfg(not(unix))]
fn require_owned_real_directory(_path: &Path, _expected_uid: u32, _label: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stale temporary owner check requires Unix",
    ))
}

#[cfg(unix)]
fn require_expected_owner(
    path: &Path,
    metadata: &fs::Metadata,
    expected_uid: u32,
    label: &str,
) -> io::Result<()> {
    if metadata.uid() != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{label} owner mismatch: path={} expected_uid={expected_uid} actual_uid={}",
                path.display(),
                metadata.uid()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_expected_owner(
    _path: &Path,
    _metadata: &fs::Metadata,
    _expected_uid: u32,
    _label: &str,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stale temporary owner check requires Unix",
    ))
}

#[cfg(unix)]
fn accessible_process_holds(path: &Path) -> io::Result<bool> {
    let target = fs::metadata(path)?;
    for process in fs::read_dir("/proc")? {
        let process = process?;
        let pid = process.file_name();
        let pid = pid.to_string_lossy();
        if !pid.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let descriptors = match fs::read_dir(process.path().join("fd")) {
            Ok(descriptors) => descriptors,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        for descriptor in descriptors {
            let descriptor = descriptor?;
            match fs::metadata(descriptor.path()) {
                Ok(metadata)
                    if metadata.dev() == target.dev() && metadata.ino() == target.ino() =>
                {
                    return Ok(true);
                }
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                    ) => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(false)
}

#[cfg(not(unix))]
fn accessible_process_holds(_path: &Path) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stale temporary open-file check requires Unix /proc",
    ))
}

#[cfg(target_os = "linux")]
fn acquire_exclusive_cleanup_lease(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    // SAFETY: `file` owns a valid descriptor for the duration of this call. F_SETLEASE does not
    // dereference pointers; F_WRLCK asks the kernel to reject the lease if any other descriptor
    // currently has this regular file open.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLEASE, libc::F_WRLCK) };
    if result == -1 {
        let error = io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(libc::EACCES | libc::EAGAIN)) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "stale temporary file holder could not be excluded: {}",
                    path.display()
                ),
            ));
        }
        return Err(io::Error::new(
            error.kind(),
            format!(
                "stale temporary file lease check failed for {}: {error}",
                path.display()
            ),
        ));
    }
    Ok(file)
}

#[cfg(not(target_os = "linux"))]
fn acquire_exclusive_cleanup_lease(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stale temporary holder proof requires Linux file leases",
    ))
}

pub fn load_day_manifest(day_dir: &Path) -> io::Result<Option<Value>> {
    let path = day_dir.join(DAILY_MANIFEST_FILE_NAME);
    match fs::read(&path) {
        Ok(payload) => serde_json::from_slice(&payload).map(Some).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid daily manifest {}: {error}", path.display()),
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn validate_manifest_day(manifest: &Value, expected_day: &str) -> io::Result<()> {
    let schema_version = manifest
        .get("schema_version")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest schema_version missing",
            )
        })?;
    if schema_version != DAILY_MANIFEST_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "daily manifest schema mismatch: expected={DAILY_MANIFEST_SCHEMA_VERSION} actual={schema_version}"
            ),
        ));
    }
    manifest_identity(manifest)?;
    let actual_day = manifest
        .get("utc_day")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "manifest utc_day missing"))?;
    if actual_day != expected_day {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("manifest UTC day mismatch: expected={expected_day} actual={actual_day}"),
        ));
    }
    let state = manifest_state(manifest)?;
    if state != "open" && state != "closed" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid daily manifest state: {state}"),
        ));
    }
    Ok(())
}

pub fn manifest_state(manifest: &Value) -> io::Result<&str> {
    manifest
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "manifest state missing"))
}

pub fn manifest_binance_continuity(
    manifest: &Value,
) -> io::Result<Option<BinanceAggTradeContinuityState>> {
    manifest
        .get(BINANCE_AGGTRADE_CONTINUITY_FIELD)
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Binance continuity in daily manifest: {error}"),
                )
            })
        })
        .transpose()
}

pub fn set_manifest_binance_continuity(
    manifest: &mut Value,
    continuity: &BinanceAggTradeContinuityState,
) -> io::Result<()> {
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    object.insert(
        BINANCE_AGGTRADE_CONTINUITY_FIELD.to_owned(),
        serde_json::to_value(continuity).map_err(io::Error::other)?,
    );
    Ok(())
}

pub fn day_has_finalized_parquet(day_dir: &Path) -> io::Result<bool> {
    for source_entry in fs::read_dir(day_dir)? {
        let source_entry = source_entry?;
        if !source_entry.file_type()?.is_dir() {
            continue;
        }
        for file_entry in fs::read_dir(source_entry.path())? {
            let file_entry = file_entry?;
            if !file_entry.file_type()?.is_file() {
                continue;
            }
            let name = file_entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".parquet")
                && !name.ends_with("-running.parquet")
                && !name.ends_with(".running.parquet")
                && !name.ends_with("-digesting.parquet")
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn close_reconciled_day_manifest(
    day_dir: &Path,
    manifest: &mut Value,
    continuity: &BinanceAggTradeContinuityState,
) -> io::Result<()> {
    let existing_source_counters = manifest.get("source_counters").cloned();
    let files = reconciled_manifest_file_facts(day_dir, manifest, true)?;
    {
        let object = manifest.as_object_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "daily manifest is not an object",
            )
        })?;
        object.insert("files".to_owned(), Value::Array(files));
        object.insert(
            "source_counters".to_owned(),
            source_counters_from_files(
                object
                    .get("files")
                    .and_then(Value::as_array)
                    .expect("files inserted as an array"),
                existing_source_counters.as_ref(),
            ),
        );
    }
    record_manifest_gaps(
        day_dir,
        manifest,
        &["unclean_exit_recovery_gap:previous_process_ended_without_day_barrier".to_owned()],
    )?;
    let object = manifest
        .as_object_mut()
        .expect("manifest object was validated");
    object.insert("state".to_owned(), Value::String("closed".to_owned()));
    object.insert(
        "closure_reason".to_owned(),
        Value::String("unclean_exit_recovery".to_owned()),
    );
    set_manifest_binance_continuity(manifest, continuity)?;
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn reconcile_open_day_manifest(
    day_dir: &Path,
    utc_day: &str,
    identity: &DailyManifestIdentity,
    continuity: &BinanceAggTradeContinuityState,
) -> io::Result<Value> {
    fs::create_dir_all(day_dir)?;
    ensure_no_stale_temporary_files(day_dir)?;
    let path = day_dir.join(DAILY_MANIFEST_FILE_NAME);
    let mut manifest = load_day_manifest(day_dir)?.unwrap_or_else(|| {
        json!({
            "schema_version": DAILY_MANIFEST_SCHEMA_VERSION,
            "utc_day": utc_day,
            "state": "open",
            "build_commit": identity.build_commit,
            "collector_binary_sha256": identity.collector_binary_sha256,
            "config_digest": identity.config_digest,
            "collector_host_id": identity.collector_host_id,
            "collector_region": identity.collector_region,
            "files": [],
            "source_counters": {},
            "gaps": [],
        })
    });
    validate_manifest_day(&manifest, utc_day)?;
    if manifest_state(&manifest)? != "open" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot reopen closed UTC day manifest: {utc_day}"),
        ));
    }
    require_identity(&manifest, identity)?;
    let existing_source_counters = manifest.get("source_counters").cloned();
    let files = reconciled_manifest_file_facts(day_dir, &manifest, true)?;
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    object.insert("files".to_owned(), Value::Array(files));
    object.insert(
        "source_counters".to_owned(),
        source_counters_from_files(
            object
                .get("files")
                .and_then(Value::as_array)
                .expect("files inserted as an array"),
            existing_source_counters.as_ref(),
        ),
    );
    set_manifest_binance_continuity(&mut manifest, continuity)?;
    write_json_atomic(&path, &manifest)?;
    Ok(manifest)
}

pub fn merge_finalized_file_delta(
    day_dir: &Path,
    manifest: &mut Value,
    delta: &DailyFinalizedFileDelta,
) -> io::Result<()> {
    validate_manifest_day(manifest, &delta.utc_day)?;
    if manifest_state(manifest)? != "open" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "cannot add finalized file to closed UTC day: {}",
                delta.utc_day
            ),
        ));
    }
    let canonical_day = day_dir.canonicalize()?;
    let canonical_file = delta.final_path.canonicalize()?;
    if !canonical_file.starts_with(&canonical_day) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "finalized Parquet escaped UTC day directory: {}",
                delta.final_path.display()
            ),
        ));
    }
    let relative_path = canonical_file
        .strip_prefix(&canonical_day)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "finalized path escaped day"))?
        .to_string_lossy()
        .into_owned();
    let expected_source_dir = format!("source_kind={}", delta.source_kind);
    if canonical_file
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        != Some(expected_source_dir.as_str())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("finalized Parquet source directory mismatch: {relative_path}"),
        ));
    }
    let metadata = fs::metadata(&canonical_file)?;
    if !metadata.is_file() || metadata.len() != delta.bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("finalized Parquet metadata mismatch: {relative_path}"),
        ));
    }
    let file_fact = json!({
        "path": relative_path,
        "source_kind": delta.source_kind,
        "utc_hour": delta.utc_hour,
        "part": delta.part,
        "rows": delta.rows,
        "bytes": delta.bytes,
        "sha256": delta.sha256,
        "schema_version": delta.schema_version,
        "first_recv_time_utc_ms": delta.first_recv_time_utc_ms,
        "last_recv_time_utc_ms": delta.last_recv_time_utc_ms,
        "source_timestamp_start": delta.source_timestamp_start,
        "source_timestamp_end": delta.source_timestamp_end,
        "event_type_counts": delta.event_type_counts,
        "facts": delta.facts,
    });
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    let existing_source_counters = object.get("source_counters").cloned();
    let source_counters = {
        let files = object
            .entry("files")
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "manifest files is not array")
            })?;
        if let Some(existing) = files
            .iter_mut()
            .find(|file| file.get("path").and_then(Value::as_str) == Some(relative_path.as_str()))
        {
            if existing != &file_fact {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("conflicting finalized-file delta: {relative_path}"),
                ));
            }
        } else {
            files.push(file_fact);
            files.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
        }
        source_counters_from_files(files, existing_source_counters.as_ref())
    };
    object.insert("source_counters".to_owned(), source_counters);
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn add_manifest_operational_counters(
    day_dir: &Path,
    manifest: &mut Value,
    deltas: &BTreeMap<String, SourceOperationalCounterDelta>,
) -> io::Result<()> {
    if deltas.is_empty() {
        return Ok(());
    }
    if manifest_state(manifest)? != "open" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot update operational counters in closed daily manifest",
        ));
    }
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    let source_counters = object
        .entry("source_counters")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest source_counters is not an object",
            )
        })?;
    for (source_kind, delta) in deltas {
        if source_kind.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "operational counter source_kind is empty",
            ));
        }
        let counter = source_counters
            .entry(source_kind.clone())
            .or_insert_with(empty_source_counter)
            .as_object_mut()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("source counter is not an object: {source_kind}"),
                )
            })?;
        for (field, increment) in [
            (
                "structural_dedupe_dropped_events",
                delta.structural_dedupe_dropped_events,
            ),
            ("lag_events", delta.lag_events),
            ("reconnect_events", delta.reconnect_events),
            ("dropped_rows", delta.dropped_rows),
        ] {
            let current = counter.get(field).and_then(Value::as_u64).unwrap_or(0);
            counter.insert(field.to_owned(), json!(current.saturating_add(increment)));
        }
    }
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn update_manifest_continuity(
    day_dir: &Path,
    manifest: &mut Value,
    continuity: &BinanceAggTradeContinuityState,
) -> io::Result<()> {
    continuity.validate().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid Binance continuity: {error}"),
        )
    })?;
    if manifest_state(manifest)? != "open" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot update continuity in closed daily manifest",
        ));
    }
    set_manifest_binance_continuity(manifest, continuity)?;
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn record_manifest_gaps(
    day_dir: &Path,
    manifest: &mut Value,
    new_gaps: &[String],
) -> io::Result<()> {
    if manifest_state(manifest)? != "open" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cannot update gaps in closed daily manifest",
        ));
    }
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    let existing_gaps = object
        .get("gaps")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let prior_overflow = existing_gaps
        .iter()
        .filter_map(|gap| gap.strip_prefix("manifest_gap_overflow_count:"))
        .filter_map(|count| count.parse::<usize>().ok())
        .sum::<usize>();
    let mut gaps = existing_gaps
        .into_iter()
        .filter(|gap| !gap.starts_with("manifest_gap_overflow_count:"))
        .collect::<BTreeSet<_>>();
    gaps.extend(new_gaps.iter().cloned());
    let real_gap_limit = if prior_overflow > 0 || gaps.len() > MAX_MANIFEST_GAPS {
        MAX_MANIFEST_GAPS.saturating_sub(1)
    } else {
        MAX_MANIFEST_GAPS
    };
    let overflow = prior_overflow.saturating_add(gaps.len().saturating_sub(real_gap_limit));
    let mut bounded = gaps.into_iter().take(real_gap_limit).collect::<Vec<_>>();
    if overflow > 0 {
        bounded.push(format!("manifest_gap_overflow_count:{overflow}"));
    }
    object.insert("gaps".to_owned(), json!(bounded));
    if !new_gaps.is_empty() {
        write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)?;
    }
    Ok(())
}

pub fn record_day_barrier_state(
    day_dir: &Path,
    manifest: &mut Value,
    producer_barriers: &BTreeSet<String>,
    writer_finalize_acks: &BTreeSet<String>,
) -> io::Result<()> {
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    object.insert("producer_barriers".to_owned(), json!(producer_barriers));
    object.insert(
        "writer_finalize_acks".to_owned(),
        json!(writer_finalize_acks),
    );
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn close_day_manifest(
    day_dir: &Path,
    manifest: &mut Value,
    closure_reason: &str,
) -> io::Result<()> {
    ensure_no_stale_temporary_files(day_dir)?;
    let object = manifest.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "daily manifest is not an object",
        )
    })?;
    object.insert("state".to_owned(), Value::String("closed".to_owned()));
    object.insert(
        "closure_reason".to_owned(),
        Value::String(closure_reason.to_owned()),
    );
    write_json_atomic(&day_dir.join(DAILY_MANIFEST_FILE_NAME), manifest)
}

pub fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "manifest path has no parent")
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "manifest file name is not UTF-8",
            )
        })?;
    let temporary = parent.join(format!("{file_name}.tmp"));
    let payload = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    if let Err(error) = (|| -> io::Result<()> {
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

pub fn validate_manifest_recovery_files(
    day_dir: &Path,
    manifest: &Value,
    allow_unrecorded_files: bool,
) -> io::Result<()> {
    reconciled_manifest_file_facts(day_dir, manifest, allow_unrecorded_files).map(|_| ())
}

pub fn validate_unmanifested_recovery_files(
    day_dir: &Path,
    identity: &DailyManifestIdentity,
) -> io::Result<()> {
    ensure_no_stale_temporary_files(day_dir)?;
    finalized_file_facts(day_dir, identity).map(|_| ())
}

fn reconciled_manifest_file_facts(
    day_dir: &Path,
    manifest: &Value,
    allow_unrecorded_files: bool,
) -> io::Result<Vec<Value>> {
    ensure_no_stale_temporary_files(day_dir)?;
    let existing_files = manifest
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "manifest files is not an array")
        })?;
    let identity = manifest_identity(manifest)?;
    let files = merge_existing_and_disk_file_facts(
        existing_files,
        finalized_file_facts(day_dir, &identity)?,
    )?;
    if !allow_unrecorded_files && files.len() != existing_files.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "closed daily manifest has unrecorded finalized Parquet",
        ));
    }
    Ok(files)
}

fn finalized_file_facts(
    day_dir: &Path,
    identity: &DailyManifestIdentity,
) -> io::Result<Vec<Value>> {
    let mut facts = Vec::new();
    for source_entry in fs::read_dir(day_dir)? {
        let source_entry = source_entry?;
        if !source_entry.file_type()?.is_dir() {
            continue;
        }
        let source_name = source_entry.file_name();
        let source_name = source_name.to_string_lossy();
        let Some(source_kind) = source_name.strip_prefix("source_kind=") else {
            continue;
        };
        for file_entry in fs::read_dir(source_entry.path())? {
            let file_entry = file_entry?;
            if !file_entry.file_type()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "non-regular object in source directory: {}",
                        file_entry.path().display()
                    ),
                ));
            }
            let file_name = file_entry.file_name();
            let file_name = file_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "non-UTF-8 Parquet file name: {}",
                        file_entry.path().display()
                    ),
                )
            })?;
            if !file_name.ends_with(".parquet")
                || file_name.ends_with("-running.parquet")
                || file_name.ends_with(".running.parquet")
                || file_name.ends_with("-digesting.parquet")
            {
                continue;
            }
            let path = file_entry.path();
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)
                .map_err(io::Error::other)?;
            let file_metadata = builder.metadata().file_metadata();
            let rows = u64::try_from(file_metadata.num_rows()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "negative Parquet row count")
            })?;
            let parquet_source_kind = parquet_metadata_value(
                file_metadata.key_value_metadata(),
                "parquet_raw_source_kind",
                file_name,
            )?;
            let parquet_schema_version = parquet_metadata_value(
                file_metadata.key_value_metadata(),
                "parquet_raw_layout_version",
                file_name,
            )?;
            let artifact_kind = parquet_metadata_value(
                file_metadata.key_value_metadata(),
                "data_collect_artifact_kind",
                file_name,
            )?;
            let recovery_payload = parquet_metadata_value(
                file_metadata.key_value_metadata(),
                DAILY_FILE_RECOVERY_METADATA_KEY,
                file_name,
            )?;
            let recovery = serde_json::from_str::<DailyFileRecoveryMetadata>(recovery_payload)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid recovery metadata in {file_name}: {error}"),
                    )
                })?;
            let relative_path = path.strip_prefix(day_dir).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Parquet escaped UTC day directory",
                )
            })?;
            let (hour, part) = hourly_file_identity(file_name, source_kind)?;
            let utc_day = day_dir
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("utc_day="))
                .filter(|day| valid_utc_day(day))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid UTC day directory: {}", day_dir.display()),
                    )
                })?;
            validate_recovery_metadata(
                &recovery,
                utc_day,
                source_kind,
                &hour,
                part,
                rows,
                parquet_source_kind,
                parquet_schema_version,
                artifact_kind,
                file_name,
            )?;
            validate_recovery_collection_identity(&recovery.facts, identity, file_name)?;
            let bytes = file_entry.metadata()?.len();
            let sha256 = sha256_file(&path).map_err(io::Error::other)?;
            facts.push(json!({
                "path": relative_path,
                "source_kind": recovery.source_kind,
                "utc_hour": recovery.utc_hour,
                "part": recovery.part,
                "rows": recovery.rows,
                "bytes": bytes,
                "sha256": sha256,
                "schema_version": recovery.schema_version,
                "first_recv_time_utc_ms": recovery.first_recv_time_utc_ms,
                "last_recv_time_utc_ms": recovery.last_recv_time_utc_ms,
                "source_timestamp_start": recovery.source_timestamp_start,
                "source_timestamp_end": recovery.source_timestamp_end,
                "event_type_counts": recovery.event_type_counts,
                "facts": recovery.facts,
            }));
        }
    }
    facts.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
    Ok(facts)
}

fn merge_existing_and_disk_file_facts(
    existing: &[Value],
    disk: Vec<Value>,
) -> io::Result<Vec<Value>> {
    let mut disk_by_path = BTreeMap::new();
    for fact in disk {
        let path = file_fact_path(&fact)?;
        if disk_by_path.insert(path.clone(), fact).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duplicate finalized Parquet path on disk: {path}"),
            ));
        }
    }
    let mut existing_paths = BTreeSet::new();
    for fact in existing {
        let path = file_fact_path(fact)?;
        if !existing_paths.insert(path.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duplicate finalized Parquet path in manifest: {path}"),
            ));
        }
        let disk_fact = disk_by_path.get(&path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("manifest references missing finalized Parquet: {path}"),
            )
        })?;
        if fact != disk_fact {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("manifest finalized-file fact does not match disk: {path}"),
            ));
        }
    }
    Ok(disk_by_path.into_values().collect())
}

fn file_fact_path(fact: &Value) -> io::Result<String> {
    let object = fact.as_object().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest file fact is not an object",
        )
    })?;
    let path = object.get("path").and_then(Value::as_str).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest file fact path missing",
        )
    })?;
    if path.is_empty() || Path::new(path).is_absolute() || path.contains("..") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid manifest file fact path: {path}"),
        ));
    }
    Ok(path.to_owned())
}

fn parquet_metadata_value<'a>(
    metadata: Option<&'a Vec<parquet::file::metadata::KeyValue>>,
    key: &str,
    file_name: &str,
) -> io::Result<&'a str> {
    let mut matches = metadata
        .into_iter()
        .flatten()
        .filter(|entry| entry.key == key);
    let entry = matches.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("required Parquet metadata {key} missing in {file_name}"),
        )
    })?;
    if matches.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("duplicate Parquet metadata {key} in {file_name}"),
        ));
    }
    entry.value.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Parquet metadata {key} has no value in {file_name}"),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_recovery_metadata(
    recovery: &DailyFileRecoveryMetadata,
    utc_day: &str,
    source_kind: &str,
    hour: &str,
    part: u32,
    rows: u64,
    parquet_source_kind: &str,
    parquet_schema_version: &str,
    artifact_kind: &str,
    file_name: &str,
) -> io::Result<()> {
    let expected_utc_hour = format!("{utc_day}T{hour}");
    if recovery.source_kind != source_kind
        || parquet_source_kind != source_kind
        || recovery.utc_hour != expected_utc_hour
        || recovery.part != part
        || recovery.rows != rows
        || recovery.schema_version != parquet_schema_version
        || artifact_kind != "typed_raw_parquet"
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Parquet recovery identity mismatch in {file_name}"),
        ));
    }
    if rows == 0
        || recovery.first_recv_time_utc_ms > recovery.last_recv_time_utc_ms
        || utc_hour_from_ms(recovery.first_recv_time_utc_ms)? != expected_utc_hour
        || utc_hour_from_ms(recovery.last_recv_time_utc_ms)? != expected_utc_hour
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Parquet recovery receive-time range mismatch in {file_name}"),
        ));
    }
    let counted_rows =
        recovery
            .event_type_counts
            .iter()
            .try_fold(0_u64, |total, (event_type, count)| {
                if event_type.trim().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("empty event type in recovery metadata for {file_name}"),
                    ));
                }
                total.checked_add(*count).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("event count overflow in recovery metadata for {file_name}"),
                    )
                })
            })?;
    if counted_rows != rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Parquet recovery event counts do not equal rows in {file_name}"),
        ));
    }
    validate_recovery_collection_facts(
        &recovery.facts,
        recovery.first_recv_time_utc_ms,
        rows,
        file_name,
    )
}

fn validate_recovery_collection_facts(
    facts: &Value,
    first_recv_time_utc_ms: i64,
    rows: u64,
    file_name: &str,
) -> io::Result<()> {
    let facts = facts.as_object().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Parquet recovery collection facts are not an object in {file_name}"),
        )
    })?;
    if facts.contains_key("required_typed_fields_present") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("validation verdict is forbidden in recovery facts for {file_name}"),
        ));
    }
    for field in [
        "rollover_reason",
        "layout_family",
        "sort_order",
        "compression",
        "code_version",
        "collector_binary_sha256",
        "source_config_digest",
        "collector_host_id",
        "collector_region",
    ] {
        if !facts.get(field).is_some_and(Value::is_string) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recovery collection fact {field} missing in {file_name}"),
            ));
        }
    }
    for field in [
        "covered_market_slugs",
        "covered_condition_ids",
        "covered_asset_ids",
        "source_gaps",
    ] {
        if !facts
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().all(Value::is_string))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recovery collection fact {field} is invalid in {file_name}"),
            ));
        }
    }
    for field in [
        "backfill_rows",
        "book_rows_with_bids_asks",
        "metadata_complete_rows",
        "metadata_unavailable_rows",
    ] {
        if facts
            .get(field)
            .and_then(Value::as_u64)
            .is_none_or(|count| count > rows)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recovery collection fact {field} is invalid in {file_name}"),
            ));
        }
    }
    if !facts
        .get("binance_depth_stitch")
        .is_some_and(Value::is_object)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("recovery collection fact binance_depth_stitch missing in {file_name}"),
        ));
    }
    let expected_hour_start = first_recv_time_utc_ms.div_euclid(HOUR_MS) * HOUR_MS;
    let hour_window = facts
        .get("hour_window_utc_ms")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recovery hour window missing in {file_name}"),
            )
        })?;
    if hour_window.get("start").and_then(Value::as_i64) != Some(expected_hour_start)
        || hour_window.get("end").and_then(Value::as_i64)
            != Some(expected_hour_start.saturating_add(HOUR_MS))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("recovery hour window mismatch in {file_name}"),
        ));
    }
    Ok(())
}

fn validate_recovery_collection_identity(
    facts: &Value,
    identity: &DailyManifestIdentity,
    file_name: &str,
) -> io::Result<()> {
    for (field, expected) in [
        ("code_version", identity.build_commit.as_str()),
        (
            "collector_binary_sha256",
            identity.collector_binary_sha256.as_str(),
        ),
        ("source_config_digest", identity.config_digest.as_str()),
        ("collector_host_id", identity.collector_host_id.as_str()),
        ("collector_region", identity.collector_region.as_str()),
    ] {
        let actual = facts.get(field).and_then(Value::as_str).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recovery collection identity {field} missing in {file_name}"),
            )
        })?;
        if actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recovery collection identity mismatch in {file_name}: field={field} expected={expected} actual={actual}"
                ),
            ));
        }
    }
    Ok(())
}

fn utc_hour_from_ms(timestamp_ms: i64) -> io::Result<String> {
    let timestamp = OffsetDateTime::from_unix_timestamp(timestamp_ms.div_euclid(1000))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour()
    ))
}

fn source_counters_from_files(files: &[Value], existing: Option<&Value>) -> Value {
    let mut counters = BTreeMap::<String, Value>::new();
    for file in files {
        let Some(source_kind) = file.get("source_kind").and_then(Value::as_str) else {
            continue;
        };
        let entry = counters
            .entry(source_kind.to_owned())
            .or_insert_with(empty_source_counter);
        let Some(object) = entry.as_object_mut() else {
            continue;
        };
        let file_count = object.get("files").and_then(Value::as_u64).unwrap_or(0);
        object.insert("files".to_owned(), json!(file_count.saturating_add(1)));
        let rows = object.get("rows").and_then(Value::as_u64).unwrap_or(0);
        object.insert(
            "rows".to_owned(),
            json!(rows.saturating_add(file["rows"].as_u64().unwrap_or(0))),
        );
        object.insert(
            "written_rows".to_owned(),
            json!(rows.saturating_add(file["rows"].as_u64().unwrap_or(0))),
        );
        let backfill_rows = object
            .get("backfill_rows")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        object.insert(
            "backfill_rows".to_owned(),
            json!(
                backfill_rows.saturating_add(
                    file.get("facts")
                        .and_then(|facts| facts.get("backfill_rows"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                )
            ),
        );
        if let Some(first) = file.get("first_recv_time_utc_ms").and_then(Value::as_i64) {
            let current = object.get("first_recv_time_utc_ms").and_then(Value::as_i64);
            object.insert(
                "first_recv_time_utc_ms".to_owned(),
                json!(current.map_or(first, |value| value.min(first))),
            );
        }
        if let Some(last) = file.get("last_recv_time_utc_ms").and_then(Value::as_i64) {
            let current = object.get("last_recv_time_utc_ms").and_then(Value::as_i64);
            object.insert(
                "last_recv_time_utc_ms".to_owned(),
                json!(current.map_or(last, |value| value.max(last))),
            );
        }
        let event_counts = object
            .get_mut("event_type_counts")
            .and_then(Value::as_object_mut)
            .expect("source counter event_type_counts is an object");
        if let Some(file_counts) = file.get("event_type_counts").and_then(Value::as_object) {
            for (event_type, count) in file_counts {
                let current = event_counts
                    .get(event_type)
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                event_counts.insert(
                    event_type.clone(),
                    json!(current.saturating_add(count.as_u64().unwrap_or(0))),
                );
            }
        }
    }
    if let Some(existing) = existing.and_then(Value::as_object) {
        for (source_kind, previous) in existing {
            let counter = counters
                .entry(source_kind.clone())
                .or_insert_with(empty_source_counter);
            let Some(counter) = counter.as_object_mut() else {
                continue;
            };
            for field in OPERATIONAL_COUNTER_FIELDS {
                if let Some(value) = previous.get(*field).and_then(Value::as_u64) {
                    counter.insert((*field).to_owned(), json!(value));
                }
            }
        }
    }
    json!(counters)
}

fn empty_source_counter() -> Value {
    json!({
        "files": 0,
        "rows": 0,
        "written_rows": 0,
        "backfill_rows": 0,
        "structural_dedupe_dropped_events": 0,
        "lag_events": 0,
        "reconnect_events": 0,
        "dropped_rows": 0,
        "first_recv_time_utc_ms": null,
        "last_recv_time_utc_ms": null,
        "event_type_counts": {},
    })
}

fn ensure_no_stale_temporary_files(day_dir: &Path) -> io::Result<()> {
    for source_entry in fs::read_dir(day_dir)? {
        let source_entry = source_entry?;
        if !source_entry.file_type()?.is_dir() {
            continue;
        }
        for file_entry in fs::read_dir(source_entry.path())? {
            let file_entry = file_entry?;
            let name = file_entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".tmp")
                || name.ends_with(".running")
                || name.ends_with("-running.parquet")
                || name.ends_with(".running.parquet")
                || name.ends_with("-digesting.parquet")
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "stale temporary file requires reconciliation: {}",
                        file_entry.path().display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn hourly_file_identity(file_name: &str, source_kind: &str) -> io::Result<(String, u32)> {
    let stem = file_name.strip_suffix(".parquet").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Parquet file has invalid suffix",
        )
    })?;
    let suffix = stem
        .strip_prefix(&format!("{source_kind}_"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Parquet file source name mismatch: {file_name}"),
            )
        })?;
    let (hour, part) = if let Some((hour, part_text)) = suffix.split_once("-part-") {
        let part = part_text
            .parse::<u32>()
            .map_err(|_| invalid_part(file_name))?;
        if part == 0 || part.to_string() != part_text || hour.contains("-part-") {
            return Err(invalid_part(file_name));
        }
        (hour, part)
    } else {
        (suffix, 0)
    };
    if hour.len() != 2
        || !hour.bytes().all(|byte| byte.is_ascii_digit())
        || hour.parse::<u8>().map_or(true, |hour| hour > 23)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cannot parse UTC hour from {file_name}"),
        ));
    }
    Ok((hour.to_owned(), part))
}

fn invalid_part(file_name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid hourly part in {file_name}"),
    )
}

fn require_identity(manifest: &Value, identity: &DailyManifestIdentity) -> io::Result<()> {
    let actual_identity = manifest_identity(manifest)?;
    for (field, expected) in [
        ("build_commit", identity.build_commit.as_str()),
        (
            "collector_binary_sha256",
            identity.collector_binary_sha256.as_str(),
        ),
        ("config_digest", identity.config_digest.as_str()),
        ("collector_host_id", identity.collector_host_id.as_str()),
        ("collector_region", identity.collector_region.as_str()),
    ] {
        let actual = match field {
            "build_commit" => actual_identity.build_commit.as_str(),
            "collector_binary_sha256" => actual_identity.collector_binary_sha256.as_str(),
            "config_digest" => actual_identity.config_digest.as_str(),
            "collector_host_id" => actual_identity.collector_host_id.as_str(),
            "collector_region" => actual_identity.collector_region.as_str(),
            _ => unreachable!("identity field list is fixed"),
        };
        if actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "daily manifest identity mismatch for {field}: expected={expected} actual={actual}"
                ),
            ));
        }
    }
    Ok(())
}

fn manifest_identity(manifest: &Value) -> io::Result<DailyManifestIdentity> {
    let required = |field: &str| {
        manifest
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("daily manifest identity field missing: {field}"),
                )
            })
    };
    Ok(DailyManifestIdentity {
        build_commit: required("build_commit")?,
        collector_binary_sha256: required("collector_binary_sha256")?,
        config_digest: required("config_digest")?,
        collector_host_id: required("collector_host_id")?,
        collector_region: required("collector_region")?,
    })
}

fn valid_utc_day(day: &str) -> bool {
    let bytes = day.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn file_counters_keep_exact_written_backfill_and_operational_totals() {
        let files = vec![json!({
            "source_kind": "binance_aggtrade",
            "rows": 5,
            "first_recv_time_utc_ms": 100,
            "last_recv_time_utc_ms": 200,
            "event_type_counts": {"aggTrade": 5},
            "facts": {"backfill_rows": 2},
        })];
        let existing = json!({
            "binance_aggtrade": {
                "structural_dedupe_dropped_events": 3,
                "lag_events": 4,
                "reconnect_events": 5,
                "dropped_rows": 6,
            }
        });

        let counters = source_counters_from_files(&files, Some(&existing));
        let counter = &counters["binance_aggtrade"];
        assert_eq!(counter["rows"], 5);
        assert_eq!(counter["written_rows"], 5);
        assert_eq!(counter["backfill_rows"], 2);
        assert_eq!(counter["structural_dedupe_dropped_events"], 3);
        assert_eq!(counter["lag_events"], 4);
        assert_eq!(counter["reconnect_events"], 5);
        assert_eq!(counter["dropped_rows"], 6);
    }

    #[test]
    fn manifest_gap_list_stays_within_its_compact_bound() {
        let tmp = tempdir().unwrap();
        let mut manifest = json!({
            "utc_day": "2026-07-07",
            "state": "open",
            "gaps": [],
        });
        let gaps = (0..300)
            .map(|index| format!("gap:{index}"))
            .collect::<Vec<_>>();
        record_manifest_gaps(tmp.path(), &mut manifest, &gaps).unwrap();

        let recorded = manifest["gaps"].as_array().unwrap();
        assert_eq!(recorded.len(), MAX_MANIFEST_GAPS);
        assert!(recorded.iter().any(|gap| {
            gap.as_str()
                .is_some_and(|gap| gap == "manifest_gap_overflow_count:45")
        }));
    }

    #[test]
    fn recovery_rejects_missing_or_mutated_manifest_file_facts() {
        let disk_fact = json!({
            "path": "source_kind=binance_aggtrade/binance_aggtrade_00.parquet",
            "rows": 2,
            "sha256": "disk-sha",
        });
        let missing = merge_existing_and_disk_file_facts(
            &[json!({
                "path": "source_kind=binance_aggtrade/binance_aggtrade_01.parquet",
                "rows": 1,
                "sha256": "missing-sha",
            })],
            vec![disk_fact.clone()],
        )
        .expect_err("a manifest path missing on disk must fail closed");
        assert!(missing.to_string().contains("missing finalized Parquet"));

        let mutated = merge_existing_and_disk_file_facts(
            &[json!({
                "path": "source_kind=binance_aggtrade/binance_aggtrade_00.parquet",
                "rows": 3,
                "sha256": "disk-sha",
            })],
            vec![disk_fact.clone()],
        )
        .expect_err("a changed manifest fact must fail closed");
        assert!(mutated.to_string().contains("does not match disk"));

        assert_eq!(
            merge_existing_and_disk_file_facts(&[], vec![disk_fact.clone()]).unwrap(),
            vec![disk_fact],
            "a finalized orphan with complete disk facts is recovered"
        );
    }

    #[cfg(unix)]
    #[test]
    fn stale_cleanup_owner_check_rejects_every_owner_mismatch() {
        let tmp = tempdir().unwrap();
        let metadata = fs::metadata(tmp.path()).unwrap();
        let wrong_uid = metadata.uid().wrapping_add(1);

        let error = require_expected_owner(tmp.path(), &metadata, wrong_uid, "test path")
            .expect_err("an unexpected owner must fail closed");

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("owner mismatch"));
    }
}
