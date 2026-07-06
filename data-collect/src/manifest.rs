use std::path::PathBuf;

use crate::resource::ResourceWatermark;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationVerdict {
    Complete,
    Incomplete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceCoverage {
    pub source_kind: String,
    pub first_recv_time_utc_ms: i64,
    pub last_recv_time_utc_ms: i64,
    pub rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueWatermark {
    pub rows: u64,
    pub bytes: u64,
    pub oldest_event_age_ms: u64,
    pub writer_lag_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredFile {
    pub file_role: String,
    pub path: PathBuf,
    pub finalized: bool,
    pub row_count: Option<u64>,
    pub sha256: Option<String>,
    pub schema_version: Option<String>,
    pub missing_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
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
}
