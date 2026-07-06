use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::resource::ResourceWatermark;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationVerdict {
    Complete,
    Incomplete,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceCoverage {
    pub source_kind: String,
    pub first_recv_time_utc_ms: i64,
    pub last_recv_time_utc_ms: i64,
    pub rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueWatermark {
    pub rows: u64,
    pub bytes: u64,
    pub oldest_event_age_ms: u64,
    pub writer_lag_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredFile {
    pub file_role: String,
    pub path: PathBuf,
    pub finalized: bool,
    pub row_count: Option<u64>,
    pub sha256: Option<String>,
    pub schema_version: Option<String>,
    pub missing_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start_utc_ms: i64,
    pub end_utc_ms: i64,
}

impl TimeWindow {
    fn is_valid(self) -> bool {
        self.start_utc_ms < self.end_utc_ms
    }
}

impl SourceCoverage {
    fn is_valid(&self) -> bool {
        !self.source_kind.trim().is_empty()
            && self.rows > 0
            && self.first_recv_time_utc_ms <= self.last_recv_time_utc_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceGapSeverity {
    SourceDegraded,
    BackpressureDegraded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSourceGap {
    pub source_kind: String,
    pub reason: String,
    pub severity: SourceGapSeverity,
    pub first_seen_utc_ms: i64,
    pub recovered_at_utc_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestFailureClass {
    Complete,
    ArtifactIncomplete,
    ProcessFatal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestDraft {
    pub artifact_path: Option<PathBuf>,
    pub row_count: Option<u64>,
    pub sha256: Option<String>,
    pub schema_version: Option<String>,
    pub code_version: Option<String>,
    pub collector_binary_sha256: Option<String>,
    pub source_config_digest: Option<String>,
    pub validation_verdict: ValidationVerdict,
    pub source_coverage: Vec<SourceCoverage>,
    pub source_gaps: Vec<String>,
    pub manifest_source_gaps: Vec<ManifestSourceGap>,
    pub coverage_incomplete_reason: Option<String>,
    pub health_artifact_path: Option<PathBuf>,
    pub health_artifact_digest: Option<String>,
    pub gap_artifact_path: Option<PathBuf>,
    pub gap_artifact_digest: Option<String>,
    pub reconnect_artifact_path: Option<PathBuf>,
    pub reconnect_artifact_digest: Option<String>,
    pub start_time_utc_ms: Option<i64>,
    pub end_time_utc_ms: Option<i64>,
    pub coverage_window: Option<TimeWindow>,
    pub resource_watermark: Option<ResourceWatermark>,
    pub writer_queue_high_watermark: Option<QueueWatermark>,
    pub flush_latency_ms: Option<u64>,
    pub collector_host_id: Option<String>,
    pub collector_region: Option<String>,
    pub required_files: Vec<RequiredFile>,
}

impl ManifestDraft {
    pub fn effective_verdict(&self) -> ValidationVerdict {
        if self.validation_verdict != ValidationVerdict::Complete {
            return ValidationVerdict::Incomplete;
        }

        if self.artifact_path.is_none()
            || self.row_count.is_none()
            || self.sha256.as_deref().is_none_or(str::is_empty)
            || self.schema_version.as_deref().is_none_or(str::is_empty)
            || self.code_version.as_deref().is_none_or(str::is_empty)
            || self
                .collector_binary_sha256
                .as_deref()
                .is_none_or(str::is_empty)
            || self
                .source_config_digest
                .as_deref()
                .is_none_or(str::is_empty)
            || self.source_coverage.is_empty()
            || self
                .source_coverage
                .iter()
                .any(|coverage| !coverage.is_valid())
            || self.coverage_incomplete_reason.is_some()
            || !has_artifact_reference(&self.health_artifact_path, &self.health_artifact_digest)
            || !has_artifact_reference(&self.gap_artifact_path, &self.gap_artifact_digest)
            || !has_artifact_reference(
                &self.reconnect_artifact_path,
                &self.reconnect_artifact_digest,
            )
            || self.start_time_utc_ms.is_none()
            || self.end_time_utc_ms.is_none()
            || !self.coverage_window.is_some_and(TimeWindow::is_valid)
            || !self
                .resource_watermark
                .as_ref()
                .is_some_and(ResourceWatermark::has_required_contract_metrics)
            || self.writer_queue_high_watermark.is_none()
            || self.flush_latency_ms.is_none()
            || self.collector_host_id.as_deref().is_none_or(str::is_empty)
            || self.collector_region.as_deref().is_none_or(str::is_empty)
            || self.required_files.is_empty()
        {
            return ValidationVerdict::Incomplete;
        }

        if self.required_files.iter().any(|file| {
            file.file_role.is_empty()
                || file.path.as_os_str().is_empty()
                || !file.finalized
                || file.row_count.is_none()
                || file.sha256.as_deref().is_none_or(str::is_empty)
                || file.schema_version.as_deref().is_none_or(str::is_empty)
                || file.missing_reason.is_some()
        }) {
            return ValidationVerdict::Incomplete;
        }

        ValidationVerdict::Complete
    }

    pub fn failure_class(&self) -> ManifestFailureClass {
        if self.effective_verdict() == ValidationVerdict::Complete {
            ManifestFailureClass::Complete
        } else {
            ManifestFailureClass::ArtifactIncomplete
        }
    }

    pub fn implies_process_fatal(&self) -> bool {
        self.failure_class() == ManifestFailureClass::ProcessFatal
    }
}

fn has_artifact_reference(path: &Option<PathBuf>, digest: &Option<String>) -> bool {
    path.as_ref()
        .is_some_and(|path| !path.as_os_str().is_empty())
        || digest
            .as_deref()
            .is_some_and(|digest| !digest.trim().is_empty())
}
