use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::manifest::{ManifestDraft, ValidationVerdict};
use crate::publish::{Result, manifest_path_for_parquet};

pub const DEFAULT_TRAINING_DATASET_MAX_BYTES: u64 = 50_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionDeleteMode {
    DryRun,
    Enabled,
}

impl RetentionDeleteMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Enabled => "enabled",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TrainingRetentionPolicy {
    dataset_root: PathBuf,
    threshold_bytes: u64,
    delete_mode: RetentionDeleteMode,
    current_utc_day: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingRetentionReceipt {
    pub threshold_bytes: u64,
    pub dataset_bytes_before: u64,
    pub filesystem_free_bytes_before: Option<u64>,
    pub dataset_bytes_after: u64,
    pub filesystem_free_bytes_after: Option<u64>,
    pub action: String,
    pub verdict: String,
    pub delete_mode: RetentionDeleteMode,
    pub candidate_utc_day: Option<String>,
    pub candidate_path: Option<PathBuf>,
    pub would_delete_utc_day: Option<String>,
    pub would_delete_path: Option<PathBuf>,
    pub would_free_bytes: Option<u64>,
    pub deletion_receipt: Option<RetentionDeletionReceipt>,
    pub non_deletable_reasons: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionDeletionReceipt {
    pub threshold_bytes: u64,
    pub dataset_bytes_before: u64,
    pub filesystem_free_bytes_before: Option<u64>,
    pub deleted_utc_day: String,
    pub deleted_path: PathBuf,
    pub deleted_bytes: u64,
    pub dataset_bytes_after: u64,
    pub filesystem_free_bytes_after: Option<u64>,
    pub reason: String,
    pub verdict: String,
    pub delete_mode: RetentionDeleteMode,
    pub candidate_complete_day_verdict: String,
}

#[derive(Clone, Debug)]
struct RetentionCandidate {
    utc_day: String,
    path: PathBuf,
    bytes: u64,
}

impl TrainingRetentionPolicy {
    pub fn new(dataset_root: &Path) -> Result<Self> {
        let dataset_root = validate_dataset_root(dataset_root)?;
        Ok(Self {
            dataset_root,
            threshold_bytes: DEFAULT_TRAINING_DATASET_MAX_BYTES,
            delete_mode: RetentionDeleteMode::DryRun,
            current_utc_day: current_utc_day()?,
        })
    }

    pub fn with_threshold_bytes(mut self, threshold_bytes: u64) -> Self {
        self.threshold_bytes = threshold_bytes;
        self
    }

    pub fn with_delete_mode(mut self, delete_mode: RetentionDeleteMode) -> Self {
        self.delete_mode = delete_mode;
        self
    }

    pub fn with_current_utc_day(mut self, current_utc_day: impl Into<String>) -> Self {
        self.current_utc_day = current_utc_day.into();
        self
    }

    pub fn enforce(&self, receipt_path: &Path) -> Result<TrainingRetentionReceipt> {
        self.enforce_inner(receipt_path, None)
    }

    pub fn enforce_with_deadline(
        &self,
        receipt_path: &Path,
        deadline: Instant,
    ) -> Result<TrainingRetentionReceipt> {
        self.enforce_inner(receipt_path, Some(deadline))
    }

    fn enforce_inner(
        &self,
        receipt_path: &Path,
        deadline: Option<Instant>,
    ) -> Result<TrainingRetentionReceipt> {
        let training_root = self.dataset_root.join("training");
        check_retention_deadline(deadline)?;
        let dataset_bytes_before = dir_size_bytes(&training_root, deadline)?;
        let filesystem_free_bytes_before = None;
        let mut non_deletable_reasons = Vec::new();

        if dataset_bytes_before <= self.threshold_bytes {
            let receipt = TrainingRetentionReceipt {
                threshold_bytes: self.threshold_bytes,
                dataset_bytes_before,
                filesystem_free_bytes_before,
                dataset_bytes_after: dataset_bytes_before,
                filesystem_free_bytes_after: filesystem_free_bytes_before,
                action: "noop".to_owned(),
                verdict: "within_threshold".to_owned(),
                delete_mode: self.delete_mode,
                candidate_utc_day: None,
                candidate_path: None,
                would_delete_utc_day: None,
                would_delete_path: None,
                would_free_bytes: None,
                deletion_receipt: None,
                non_deletable_reasons,
            };
            write_receipt(receipt_path, &receipt)?;
            return Ok(receipt);
        }

        let candidate = oldest_complete_day_candidate(self, &mut non_deletable_reasons, deadline)?;
        let Some(candidate) = candidate else {
            let receipt = TrainingRetentionReceipt {
                threshold_bytes: self.threshold_bytes,
                dataset_bytes_before,
                filesystem_free_bytes_before,
                dataset_bytes_after: dataset_bytes_before,
                filesystem_free_bytes_after: filesystem_free_bytes_before,
                action: "fail_closed".to_owned(),
                verdict: "no_deletable_finalized_complete_utc_day".to_owned(),
                delete_mode: self.delete_mode,
                candidate_utc_day: None,
                candidate_path: None,
                would_delete_utc_day: None,
                would_delete_path: None,
                would_free_bytes: None,
                deletion_receipt: None,
                non_deletable_reasons,
            };
            write_receipt(receipt_path, &receipt)?;
            return Ok(receipt);
        };

        match self.delete_mode {
            RetentionDeleteMode::DryRun => {
                let receipt = TrainingRetentionReceipt {
                    threshold_bytes: self.threshold_bytes,
                    dataset_bytes_before,
                    filesystem_free_bytes_before,
                    dataset_bytes_after: dataset_bytes_before,
                    filesystem_free_bytes_after: filesystem_free_bytes_before,
                    action: "dry_run".to_owned(),
                    verdict: "would_delete_oldest_complete_utc_day".to_owned(),
                    delete_mode: self.delete_mode,
                    candidate_utc_day: Some(candidate.utc_day.clone()),
                    candidate_path: Some(candidate.path.clone()),
                    would_delete_utc_day: Some(candidate.utc_day),
                    would_delete_path: Some(candidate.path),
                    would_free_bytes: Some(candidate.bytes),
                    deletion_receipt: None,
                    non_deletable_reasons,
                };
                write_receipt(receipt_path, &receipt)?;
                Ok(receipt)
            }
            RetentionDeleteMode::Enabled => {
                let deleted_path = candidate.path.clone();
                check_retention_deadline(deadline)?;
                remove_dir_all_with_deadline(&deleted_path, deadline)?;
                let dataset_bytes_after = dir_size_bytes(&training_root, deadline)?;
                let deletion_receipt = RetentionDeletionReceipt {
                    threshold_bytes: self.threshold_bytes,
                    dataset_bytes_before,
                    filesystem_free_bytes_before,
                    deleted_utc_day: candidate.utc_day.clone(),
                    deleted_path: deleted_path.clone(),
                    deleted_bytes: candidate.bytes,
                    dataset_bytes_after,
                    filesystem_free_bytes_after: None,
                    reason: "dataset_bytes_exceeded_threshold".to_owned(),
                    verdict: "deleted_oldest_complete_utc_day".to_owned(),
                    delete_mode: RetentionDeleteMode::Enabled,
                    candidate_complete_day_verdict: "complete".to_owned(),
                };
                let receipt = TrainingRetentionReceipt {
                    threshold_bytes: self.threshold_bytes,
                    dataset_bytes_before,
                    filesystem_free_bytes_before,
                    dataset_bytes_after,
                    filesystem_free_bytes_after: None,
                    action: "delete".to_owned(),
                    verdict: "deleted_oldest_complete_utc_day".to_owned(),
                    delete_mode: self.delete_mode,
                    candidate_utc_day: Some(candidate.utc_day),
                    candidate_path: Some(deleted_path),
                    would_delete_utc_day: None,
                    would_delete_path: None,
                    would_free_bytes: None,
                    deletion_receipt: Some(deletion_receipt),
                    non_deletable_reasons,
                };
                write_receipt(receipt_path, &receipt)?;
                Ok(receipt)
            }
        }
    }
}

fn oldest_complete_day_candidate(
    policy: &TrainingRetentionPolicy,
    non_deletable_reasons: &mut Vec<String>,
    deadline: Option<Instant>,
) -> Result<Option<RetentionCandidate>> {
    let typed_raw_root = policy.dataset_root.join("training").join("typed_raw");
    if !typed_raw_root.is_dir() {
        return Ok(None);
    }
    let mut candidates = Vec::new();
    check_retention_deadline(deadline)?;
    for entry in fs::read_dir(&typed_raw_root)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let day_path = entry.path();
        let file_type = entry.file_type()?;
        let Some(file_name) = day_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_type.is_symlink() {
            non_deletable_reasons.push(format!("symlink_day_entry:{}", day_path.display()));
            continue;
        }
        if !file_type.is_dir() || !file_name.starts_with("utc_day=") {
            non_deletable_reasons.push(format!("non_day_entry:{}", day_path.display()));
            continue;
        }
        let utc_day = file_name.trim_start_matches("utc_day=").to_owned();
        if utc_day == policy.current_utc_day {
            non_deletable_reasons.push(format!("current_utc_day:{utc_day}"));
            continue;
        }
        match complete_day_candidate(&typed_raw_root, &day_path, &utc_day, deadline)? {
            Some(candidate) => candidates.push(candidate),
            None => non_deletable_reasons.push(format!("not_complete:{utc_day}")),
        }
    }
    candidates.sort_by(|left, right| left.utc_day.cmp(&right.utc_day));
    Ok(candidates.into_iter().next())
}

fn complete_day_candidate(
    typed_raw_root: &Path,
    day_path: &Path,
    utc_day: &str,
    deadline: Option<Instant>,
) -> Result<Option<RetentionCandidate>> {
    check_retention_deadline(deadline)?;
    if fs::symlink_metadata(day_path)?.file_type().is_symlink() {
        return Ok(None);
    }
    let canonical_day = day_path.canonicalize()?;
    let canonical_root = typed_raw_root.canonicalize()?;
    if !canonical_day.starts_with(&canonical_root)
        || day_path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Ok(None);
    }

    let mut source_dirs = 0_u64;
    for entry in fs::read_dir(day_path)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Ok(None);
        };
        if file_type.is_symlink() || !file_type.is_dir() || !file_name.starts_with("source_kind=") {
            return Ok(None);
        }
        source_dirs = source_dirs.saturating_add(1);
        if !source_dir_is_complete(&path, deadline)? {
            return Ok(None);
        }
    }
    if source_dirs == 0 {
        return Ok(None);
    }
    Ok(Some(RetentionCandidate {
        utc_day: utc_day.to_owned(),
        path: day_path.to_path_buf(),
        bytes: dir_size_bytes(day_path, deadline)?,
    }))
}

fn source_dir_is_complete(source_dir: &Path, deadline: Option<Instant>) -> Result<bool> {
    check_retention_deadline(deadline)?;
    if contains_partial_artifact(source_dir, deadline)? {
        return Ok(false);
    }
    let mut finalized_parquets = Vec::<PathBuf>::new();
    let mut manifest_paths = Vec::<PathBuf>::new();
    for entry in fs::read_dir(source_dir)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Ok(false);
        }
        if path.is_dir() {
            return Ok(false);
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Ok(false);
        };
        if path.extension().and_then(|extension| extension.to_str()) == Some("parquet") {
            finalized_parquets.push(path);
        } else if file_name.ends_with(".manifest.json") {
            manifest_paths.push(path);
        } else {
            return Ok(false);
        }
    }
    if finalized_parquets.is_empty() {
        return Ok(false);
    }
    finalized_parquets.sort();
    manifest_paths.sort();
    for parquet_path in &finalized_parquets {
        check_retention_deadline(deadline)?;
        let manifest_path = manifest_path_for_parquet(parquet_path);
        if !manifest_paths.iter().any(|path| path == &manifest_path)
            || !manifest_is_complete_for_retention(&manifest_path, deadline)?
        {
            return Ok(false);
        }
    }
    Ok(manifest_paths.len() == finalized_parquets.len())
}

fn manifest_is_complete_for_retention(
    manifest_path: &Path,
    deadline: Option<Instant>,
) -> Result<bool> {
    check_retention_deadline(deadline)?;
    let bytes = fs::read(manifest_path)?;
    check_retention_deadline(deadline)?;
    let manifest: ManifestDraft = serde_json::from_slice(&bytes)?;
    check_retention_deadline(deadline)?;
    Ok(manifest.effective_verdict() == ValidationVerdict::Complete)
}

fn contains_partial_artifact(root: &Path, deadline: Option<Instant>) -> Result<bool> {
    check_retention_deadline(deadline)?;
    for entry in fs::read_dir(root)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Ok(true);
        }
        if file_name == ".rsync-partial"
            || file_name.contains(".rsync-partial")
            || file_name.ends_with(".tmp")
            || file_name.ends_with(".partial")
            || file_name.ends_with(".incomplete")
            || file_name.ends_with("-running.parquet")
            || file_name.ends_with("-digesting.parquet")
        {
            return Ok(true);
        }
        if file_type.is_dir() && contains_partial_artifact(&path, deadline)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dir_size_bytes(root: &Path, deadline: Option<Instant>) -> Result<u64> {
    check_retention_deadline(deadline)?;
    if !root.exists() {
        return Ok(0);
    }
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bytes = bytes.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            bytes = bytes.saturating_add(dir_size_bytes(&path, deadline)?);
        } else {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

fn remove_dir_all_with_deadline(root: &Path, deadline: Option<Instant>) -> Result<()> {
    check_retention_deadline(deadline)?;
    if !root.exists() {
        return Ok(());
    }
    if fs::symlink_metadata(root)?.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("retention delete refuses symlink path: {}", root.display()),
        )
        .into());
    }
    for entry in fs::read_dir(root)? {
        check_retention_deadline(deadline)?;
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("retention delete refuses symlink path: {}", path.display()),
            )
            .into());
        }
        if metadata.is_dir() {
            remove_dir_all_with_deadline(&path, deadline)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    check_retention_deadline(deadline)?;
    fs::remove_dir(root)?;
    Ok(())
}

fn check_retention_deadline(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(
            io::Error::new(io::ErrorKind::TimedOut, "retention scan exceeded deadline").into(),
        );
    }
    Ok(())
}

fn validate_dataset_root(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("dataset root must be absolute: {}", root.display()),
        )
        .into());
    }
    if root
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dataset root must not contain parent components",
        )
        .into());
    }
    if root.exists() {
        Ok(root.canonicalize()?)
    } else {
        Ok(root.to_path_buf())
    }
}

fn write_receipt(path: &Path, receipt: &TrainingRetentionReceipt) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(receipt)?)?;
    Ok(())
}

fn current_utc_day() -> Result<String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let date = OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .date();
    Ok(format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    ))
}
