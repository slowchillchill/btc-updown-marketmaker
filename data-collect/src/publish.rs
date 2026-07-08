use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use time::OffsetDateTime;

use crate::manifest::{
    ManifestDraft, ManifestFailureClass, PAPER_LIVE_FORBIDDEN_DATA_COLLECT_ROLES, RequiredFile,
    ValidationVerdict,
};
use crate::writer::PublishedSegment;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const DATA_COLLECT_ALLOWED_ARTIFACT_ROLES: &[&str] = &[
    "finalized_typed_raw_parquet",
    "manifest",
    "source_coverage",
    "source_health",
    "source_gap",
    "reconnect_trace",
    "backpressure_state",
    "external_market_event",
    "timestamp_namespace",
    "realtime_handoff_boundary_evidence",
];

pub const PAPER_LIVE_FORBIDDEN_RUNTIME_ROLES: &[&str] = PAPER_LIVE_FORBIDDEN_DATA_COLLECT_ROLES;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UtcDayPartition {
    pub dataset_root: PathBuf,
    pub source_kind: String,
    pub utc_day: String,
    pub partition_path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaperLiveEnvironment {
    Paper,
    Live,
}

impl PaperLiveEnvironment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

impl fmt::Display for PaperLiveEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperLivePublishBoundary {
    pub environment: PaperLiveEnvironment,
    pub artifact_root: PathBuf,
    pub allowed_roles: Vec<&'static str>,
    pub forbidden_runtime_roles: Vec<&'static str>,
}

impl PaperLivePublishBoundary {
    pub fn allows_role(&self, role: &str) -> bool {
        self.allowed_roles.contains(&role)
    }

    pub fn forbids_runtime_role(&self, role: &str) -> bool {
        self.forbidden_runtime_roles.contains(&role)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedArtifact {
    pub parquet_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPublishDecision {
    pub required_file: RequiredFile,
    pub verdict: ValidationVerdict,
    pub failure_class: ManifestFailureClass,
    pub discoverable_as_complete: bool,
}

pub fn training_utc_day_partition(
    dataset_root: &Path,
    source_kind: &str,
    recv_time_utc_ms: i64,
) -> Result<UtcDayPartition> {
    let dataset_root = validate_absolute_root(dataset_root)?;
    let source_kind = validate_path_component("source_kind", source_kind)?;
    let seconds = recv_time_utc_ms.div_euclid(1000);
    let date = OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .date();
    let utc_day = format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    );
    let partition_path = dataset_root
        .join("training")
        .join("typed_raw")
        .join(format!("utc_day={utc_day}"))
        .join(format!("source_kind={source_kind}"));
    ensure_under_root(&dataset_root, &partition_path)?;

    Ok(UtcDayPartition {
        dataset_root,
        source_kind: source_kind.to_owned(),
        utc_day,
        partition_path,
    })
}

pub fn paper_live_handoff_boundary(
    dataset_root: &Path,
    environment: PaperLiveEnvironment,
) -> Result<PaperLivePublishBoundary> {
    let dataset_root = validate_absolute_root(dataset_root)?;
    let artifact_root = dataset_root
        .join(environment.as_str())
        .join("data_collect_handoff");
    ensure_under_root(&dataset_root, &artifact_root)?;

    Ok(PaperLivePublishBoundary {
        environment,
        artifact_root,
        allowed_roles: DATA_COLLECT_ALLOWED_ARTIFACT_ROLES.to_vec(),
        forbidden_runtime_roles: PAPER_LIVE_FORBIDDEN_RUNTIME_ROLES.to_vec(),
    })
}

pub fn discover_finalized_artifacts(root: &Path) -> Result<Vec<FinalizedArtifact>> {
    let mut artifacts = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if is_partial_artifact_name(file_name)
            || path.extension().and_then(|extension| extension.to_str()) != Some("parquet")
        {
            continue;
        }
        let manifest_path = manifest_path_for_parquet(&path);
        let Some(manifest_file_name) = manifest_path.file_name().and_then(|name| name.to_str())
        else {
            continue;
        };
        if manifest_path.is_file()
            && !is_partial_artifact_name(manifest_file_name)
            && manifest_sidecar_matches_complete_artifact(&manifest_path, &path)
        {
            artifacts.push(FinalizedArtifact {
                parquet_path: path,
                manifest_path,
            });
        }
    }
    artifacts.sort_by(|left, right| left.parquet_path.cmp(&right.parquet_path));
    Ok(artifacts)
}

pub fn manifest_path_for_parquet(parquet_path: &Path) -> PathBuf {
    parquet_path.with_extension("manifest.json")
}

pub fn complete_publish_decision(
    file_role: impl Into<String>,
    segment: &PublishedSegment,
) -> ArtifactPublishDecision {
    ArtifactPublishDecision {
        required_file: RequiredFile {
            file_role: file_role.into(),
            path: segment.final_path.clone(),
            finalized: true,
            row_count: Some(segment.row_count),
            sha256: Some(segment.sha256.clone()),
            schema_version: Some(segment.schema_version.clone()),
            missing_reason: None,
        },
        verdict: ValidationVerdict::Complete,
        failure_class: ManifestFailureClass::Complete,
        discoverable_as_complete: true,
    }
}

pub fn failed_atomic_publish_decision(
    file_role: impl Into<String>,
    path: impl Into<PathBuf>,
    reason: impl AsRef<str>,
) -> ArtifactPublishDecision {
    ArtifactPublishDecision {
        required_file: RequiredFile {
            file_role: file_role.into(),
            path: path.into(),
            finalized: false,
            row_count: None,
            sha256: None,
            schema_version: None,
            missing_reason: Some(format!("atomic_publish_failed:{}", reason.as_ref().trim())),
        },
        verdict: ValidationVerdict::Incomplete,
        failure_class: ManifestFailureClass::ArtifactIncomplete,
        discoverable_as_complete: false,
    }
}

pub fn manifest_discoverable_as_complete(manifest: &ManifestDraft) -> bool {
    manifest.effective_verdict() == ValidationVerdict::Complete
}

fn manifest_sidecar_matches_complete_artifact(manifest_path: &Path, parquet_path: &Path) -> bool {
    let Ok(raw_manifest) = fs::read(manifest_path) else {
        return false;
    };
    let Ok(manifest) = serde_json::from_slice::<ManifestDraft>(&raw_manifest) else {
        return false;
    };
    manifest_discoverable_as_complete(&manifest)
        && manifest_references_parquet(&manifest, parquet_path)
}

fn manifest_references_parquet(manifest: &ManifestDraft, parquet_path: &Path) -> bool {
    let Some(parquet_file_name) = parquet_path.file_name() else {
        return false;
    };
    let artifact_path_matches = manifest
        .artifact_path
        .as_ref()
        .and_then(|path| path.file_name())
        == Some(parquet_file_name);
    let required_file_matches = manifest.required_files.iter().any(|file| {
        file.finalized
            && file.missing_reason.is_none()
            && file.path.file_name() == Some(parquet_file_name)
            && file.row_count == manifest.row_count
            && file.sha256 == manifest.sha256
            && file.schema_version == manifest.schema_version
    });

    artifact_path_matches && required_file_matches
}

fn is_partial_artifact_name(file_name: &str) -> bool {
    file_name == ".rsync-partial"
        || file_name.contains(".rsync-partial")
        || file_name.ends_with(".tmp")
        || file_name.ends_with(".partial")
        || file_name.ends_with(".incomplete")
        || file_name.ends_with("-running.parquet")
        || file_name.ends_with("-digesting.parquet")
        || file_name.ends_with("-running.manifest.json")
        || file_name.ends_with("-digesting.manifest.json")
}

fn validate_absolute_root(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("dataset root must be absolute: {}", root.display()),
        )
        .into());
    }
    reject_parent_components("dataset_root", root)?;
    Ok(root.to_path_buf())
}

fn validate_path_component<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    if value.trim().is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} is not a safe path component"),
        )
        .into());
    }
    Ok(value)
}

fn ensure_under_root(root: &Path, path: &Path) -> Result<()> {
    reject_parent_components("artifact_path", path)?;
    if !path.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "artifact path {} escapes dataset root {}",
                path.display(),
                root.display()
            ),
        )
        .into());
    }
    Ok(())
}

fn reject_parent_components(field: &str, path: &Path) -> Result<()> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} must not contain parent components"),
        )
        .into());
    }
    Ok(())
}
