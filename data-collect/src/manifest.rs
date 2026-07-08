use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::backpressure::BackpressureReceipt;
use crate::resource::{ResourceThresholdPolicyReceipt, ResourceWatermark};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationVerdict {
    Complete,
    Incomplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerBoundaryKind {
    Training,
    PaperLive,
}

pub const TRAINING_ALLOWED_DATA_COLLECT_ROLES: &[&str] = &[
    "finalized_typed_raw_parquet",
    "manifest",
    "source_coverage",
    "source_health",
    "source_gap",
    "reconnect_trace",
    "backpressure_state",
];

pub const TRAINING_REQUIRED_EXTERNAL_ROLES: &[&str] = &[
    "data_pipeline_labels",
    "data_pipeline_features",
    "data_pipeline_tick_stream",
    "data_pipeline_training_split",
];

pub const TRAINING_FORBIDDEN_DATA_COLLECT_ROLES: &[&str] = &[
    "labels",
    "features",
    "tick_stream",
    "canonical_split",
    "purged_embargo_split",
    "t30_training_sample",
    "training_approval",
];

pub const PAPER_LIVE_ALLOWED_DATA_COLLECT_ROLES: &[&str] = &[
    "external_market_event",
    "timestamp_namespace",
    "source_coverage",
    "source_health",
    "source_gap",
    "reconnect_trace",
    "backpressure_state",
    "manifest",
    "realtime_handoff_boundary_evidence",
];

pub const PAPER_LIVE_REQUIRED_EXTERNAL_ROLES: &[&str] = &[
    "paper_live_runtime_receive_timestamp",
    "paper_live_append_only_event_log",
    "paper_live_market_context_builder",
    "paper_live_noe_ledger",
];

pub const PAPER_LIVE_FORBIDDEN_DATA_COLLECT_ROLES: &[&str] = &[
    "paper_live_append_only_event_log",
    "rotation_artifact",
    "live_artifact",
    "order_evidence",
    "NormalizedOrderEvent",
    "ledger",
    "noe_ledger",
    "execution_feedback_ingestion",
    "trading_decision",
    "risk_control",
    "submit_cancel_path",
    "model_input_assembly",
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerBoundaryDeclaration {
    pub consumer: ConsumerBoundaryKind,
    pub allowed_data_collect_roles: Vec<String>,
    pub required_external_roles: Vec<String>,
    pub forbidden_data_collect_roles: Vec<String>,
    pub requires_realtime_handoff: bool,
    pub requires_consumer_host_runtime_receive_timestamp: bool,
    pub finalized_parquet_is_hot_path_source_of_truth: bool,
    pub data_collect_output_is_training_approved: bool,
    pub may_emit_trading_decision: bool,
}

impl ConsumerBoundaryDeclaration {
    pub fn allows_data_collect_role(&self, role: &str) -> bool {
        contains_role(&self.allowed_data_collect_roles, role)
    }

    pub fn requires_external_role(&self, role: &str) -> bool {
        contains_role(&self.required_external_roles, role)
    }

    pub fn forbids_data_collect_role(&self, role: &str) -> bool {
        contains_role(&self.forbidden_data_collect_roles, role)
    }

    pub fn is_valid_for_manifest(&self) -> bool {
        if self.allowed_data_collect_roles.is_empty()
            || self.forbidden_data_collect_roles.is_empty()
            || self
                .allowed_data_collect_roles
                .iter()
                .any(|role| role.trim().is_empty())
            || self
                .forbidden_data_collect_roles
                .iter()
                .any(|role| role.trim().is_empty())
            || self
                .allowed_data_collect_roles
                .iter()
                .any(|role| self.forbids_data_collect_role(role))
            || self.may_emit_trading_decision
        {
            return false;
        }

        match self.consumer {
            ConsumerBoundaryKind::Training => {
                !self.requires_realtime_handoff
                    && !self.requires_consumer_host_runtime_receive_timestamp
                    && !self.finalized_parquet_is_hot_path_source_of_truth
                    && !self.data_collect_output_is_training_approved
                    && has_exact_roles(
                        &self.allowed_data_collect_roles,
                        TRAINING_ALLOWED_DATA_COLLECT_ROLES,
                    )
                    && has_exact_roles(
                        &self.required_external_roles,
                        TRAINING_REQUIRED_EXTERNAL_ROLES,
                    )
                    && has_exact_roles(
                        &self.forbidden_data_collect_roles,
                        TRAINING_FORBIDDEN_DATA_COLLECT_ROLES,
                    )
            }
            ConsumerBoundaryKind::PaperLive => {
                self.requires_realtime_handoff
                    && self.requires_consumer_host_runtime_receive_timestamp
                    && !self.finalized_parquet_is_hot_path_source_of_truth
                    && !self.data_collect_output_is_training_approved
                    && has_exact_roles(
                        &self.allowed_data_collect_roles,
                        PAPER_LIVE_ALLOWED_DATA_COLLECT_ROLES,
                    )
                    && has_exact_roles(
                        &self.required_external_roles,
                        PAPER_LIVE_REQUIRED_EXTERNAL_ROLES,
                    )
                    && has_exact_roles(
                        &self.forbidden_data_collect_roles,
                        PAPER_LIVE_FORBIDDEN_DATA_COLLECT_ROLES,
                    )
            }
        }
    }
}

pub fn training_consumer_boundary_declaration() -> ConsumerBoundaryDeclaration {
    ConsumerBoundaryDeclaration {
        consumer: ConsumerBoundaryKind::Training,
        allowed_data_collect_roles: roles(TRAINING_ALLOWED_DATA_COLLECT_ROLES),
        required_external_roles: roles(TRAINING_REQUIRED_EXTERNAL_ROLES),
        forbidden_data_collect_roles: roles(TRAINING_FORBIDDEN_DATA_COLLECT_ROLES),
        requires_realtime_handoff: false,
        requires_consumer_host_runtime_receive_timestamp: false,
        finalized_parquet_is_hot_path_source_of_truth: false,
        data_collect_output_is_training_approved: false,
        may_emit_trading_decision: false,
    }
}

pub fn paper_live_consumer_boundary_declaration() -> ConsumerBoundaryDeclaration {
    ConsumerBoundaryDeclaration {
        consumer: ConsumerBoundaryKind::PaperLive,
        allowed_data_collect_roles: roles(PAPER_LIVE_ALLOWED_DATA_COLLECT_ROLES),
        required_external_roles: roles(PAPER_LIVE_REQUIRED_EXTERNAL_ROLES),
        forbidden_data_collect_roles: roles(PAPER_LIVE_FORBIDDEN_DATA_COLLECT_ROLES),
        requires_realtime_handoff: true,
        requires_consumer_host_runtime_receive_timestamp: true,
        finalized_parquet_is_hot_path_source_of_truth: false,
        data_collect_output_is_training_approved: false,
        may_emit_trading_decision: false,
    }
}

pub fn required_consumer_boundary_declarations() -> Vec<ConsumerBoundaryDeclaration> {
    vec![
        training_consumer_boundary_declaration(),
        paper_live_consumer_boundary_declaration(),
    ]
}

fn has_required_consumer_boundary_declarations(
    declarations: &[ConsumerBoundaryDeclaration],
) -> bool {
    if declarations.len() != 2
        || declarations
            .iter()
            .any(|declaration| !declaration.is_valid_for_manifest())
    {
        return false;
    }

    declarations
        .iter()
        .filter(|declaration| declaration.consumer == ConsumerBoundaryKind::Training)
        .count()
        == 1
        && declarations
            .iter()
            .filter(|declaration| declaration.consumer == ConsumerBoundaryKind::PaperLive)
            .count()
            == 1
}

fn roles(values: &[&str]) -> Vec<String> {
    values.iter().map(|role| (*role).to_owned()).collect()
}

fn contains_role(roles: &[String], role: &str) -> bool {
    roles.iter().any(|candidate| candidate == role)
}

fn has_exact_roles(actual: &[String], expected: &[&str]) -> bool {
    actual.len() == expected.len() && expected.iter().all(|role| contains_role(actual, role))
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
    #[serde(default)]
    pub consumer_boundary_declarations: Vec<ConsumerBoundaryDeclaration>,
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
    pub resource_threshold_policy_receipt: Option<ResourceThresholdPolicyReceipt>,
    pub writer_queue_high_watermark: Option<QueueWatermark>,
    pub backpressure_receipt: Option<BackpressureReceipt>,
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
            || !has_required_consumer_boundary_declarations(&self.consumer_boundary_declarations)
            || self.source_coverage.is_empty()
            || self
                .source_coverage
                .iter()
                .any(|coverage| !coverage.is_valid())
            || !self.source_gaps.is_empty()
            || !self.manifest_source_gaps.is_empty()
            || self.coverage_incomplete_reason.is_some()
            || references_deleted_runtime_sidecar(&self.health_artifact_path)
            || references_deleted_runtime_sidecar(&self.gap_artifact_path)
            || references_deleted_runtime_sidecar(&self.reconnect_artifact_path)
            || self.start_time_utc_ms.is_none()
            || self.end_time_utc_ms.is_none()
            || !self.coverage_window.is_some_and(TimeWindow::is_valid)
            || !self.resource_watermark.as_ref().is_some_and(|watermark| {
                watermark.has_required_contract_metrics()
                    || (self.allows_compact_runtime_resource_watermark()
                        && watermark.has_compact_runtime_metrics())
            })
            || !self
                .resource_threshold_policy_receipt
                .as_ref()
                .is_some_and(ResourceThresholdPolicyReceipt::is_valid_for_complete_manifest)
            || self.writer_queue_high_watermark.is_none()
            || !self.backpressure_receipt.as_ref().is_some_and(|receipt| {
                receipt.is_valid_for_complete_manifest()
                    && self
                        .source_coverage
                        .iter()
                        .all(|coverage| receipt.covers_source_kind(&coverage.source_kind))
            })
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

    fn allows_compact_runtime_resource_watermark(&self) -> bool {
        self.health_artifact_path.is_none()
            && self.gap_artifact_path.is_none()
            && self.reconnect_artifact_path.is_none()
            && self.source_coverage.len() == 1
            && self.required_files.len() == 1
            && self.required_files[0].file_role == "typed_raw_parquet"
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

fn references_deleted_runtime_sidecar(path: &Option<PathBuf>) -> bool {
    let Some(path) = path else {
        return false;
    };
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    matches!(
        file_name,
        "source_coverage.json"
            | "source_gaps.json"
            | "rollover_edges.json"
            | "write_receipts.jsonl"
            | "queue_samples.jsonl"
    )
}
