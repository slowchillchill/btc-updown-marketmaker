//! 门 A 质量证据、能力判定与原子发布控制面。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use contracts::{
    CANONICAL_EVENT_RECORD_SCHEMA_V1, CANONICAL_REFERENCE_EVENT_SCHEMA_V1, CanonicalEventId,
    CanonicalEventRecordV1, CanonicalReferenceEventPayloadV1, CapabilityStatus, DATASET_DATE_FLOOR,
    DATASET_LINEAGE_ROOT, EVENT_TIME_INDEX_SCHEMA_V2, EXECUTION_MARKET_EVENT_SCHEMA_V3,
    ExecutionMarketEventPayloadV3, GATE_A_CAPABILITY_MATRIX_SCHEMA_V2, GATE_A_RECEIPT_SCHEMA_V3,
    GateACapabilityEntryV2, GateACapabilityId, GateACapabilityMatrixV2, GateAReceiptV3,
    LevelExecutionBasis, MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3, MarketDataQuality,
    ObservationComponentId, PublicTradeIdSourceV1, ResyncQuality, Sha256Hex,
    VISIBILITY_INDEX_SCHEMA_V2,
};
use data_pipeline_core::reduction::{
    REDUCTION_REPORT_SCHEMA_V1, ReductionDispositionV1, ReductionOperationKindV1,
};
use data_pipeline_core::{
    REQUIRED_OBSERVATION_COMPONENTS, decode_gate_a_capability_matrix, decode_gate_a_receipt_v3,
    derive_gate_a_receipt_v3_id, encode_gate_a_capability_matrix, encode_gate_a_receipt_v3,
    gate_a_capability_matrix_digest, validate_gate_a_capability_matrix,
};
use rustix::fs::{RenameFlags, renameat_with};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Date, macros::format_description};

use crate::canonical_run::{
    CanonicalRunConfig, CanonicalRunDigests, CanonicalRunInputFile, CanonicalRunMetrics,
    CanonicalRunScope, SourceRowCounts,
};
use crate::gate_a::{
    CanonicalEventStoreCursor, GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1, GateAArtifactDescriptor,
    GateABuildResourceMetrics, GateACandidate, GateAError, GateAOutputConfig,
    GateAProjectionWitnessKind, GateAProjectionWitnesses, MarketObservationCursor,
    ReductionReportCursor, ValidatedGateACandidate, pipeline_core_identity_sha256,
    supported_schema_set_identity_sha256, validate_gate_a_candidate,
};
use crate::{SortConfig, SortMetrics};

pub const GATE_A_QUALITY_ISOLATION_REPORT_SCHEMA_V1: &str = "gate_a_quality_isolation_report_v1";
pub const GATE_A_RESOURCE_HIGH_WATER_REPORT_SCHEMA_V1: &str =
    "gate_a_resource_high_water_report_v1";
pub const GATE_A_TEST_EVIDENCE_REPORT_SCHEMA_V1: &str = "gate_a_test_evidence_report_v1";
pub const GATE_A_RUN_MANIFEST_SCHEMA_V2: &str = "gate_a_run_manifest_v2";
pub const GATE_A_MEASURED_BUILD_STATE_SCHEMA_V1: &str = "gate_a_measured_build_state_v1";

pub const GATE_A_MAX_RSS_KIB: u64 = 1024 * 1024;
pub const GATE_A_CGROUP_MEMORY_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub(crate) const QUALITY_SCAN_CANONICAL_PAGE_ROWS: usize = 1;

pub fn gate_a_formal_sort_config() -> SortConfig {
    SortConfig {
        max_run_rows: 8_192,
        max_run_bytes: 64 * 1024 * 1024,
        max_temp_run_files: 512,
        merge_fan_in: 4,
        max_open_files: 5,
        batch_rows: 1_024,
        max_temp_bytes: 1024 * 1024 * 1024,
    }
}

pub fn gate_a_formal_canonical_config() -> CanonicalRunConfig {
    CanonicalRunConfig {
        decode_batch_rows: 8_192,
        max_decode_batch_bytes: 64 * 1024 * 1024,
        max_temporary_row_bytes: 128 * 1024,
        max_epoch_count_per_source: 1,
        max_pending_ws_terminals: 1,
        max_retained_reducer_entries: 160 * 1024,
        canonical_rule_version: "canonical-production-run-v1".to_owned(),
        sort: gate_a_formal_sort_config(),
    }
}

pub fn gate_a_formal_output_config() -> GateAOutputConfig {
    GateAOutputConfig {
        max_artifact_payload_bytes: 3 * 1024 * 1024,
        max_capture_output_bytes: 1024 * 1024 * 1024,
        max_candidate_directory_bytes: 4 * 1024 * 1024 * 1024,
        max_observation_l2_levels: 256,
        sort: gate_a_formal_sort_config(),
    }
}

pub fn gate_a_perturbation_configs() -> (CanonicalRunConfig, GateAOutputConfig) {
    let mut canonical = gate_a_formal_canonical_config();
    canonical.decode_batch_rows = 4_096;
    canonical.sort.max_run_rows = 4_096;
    canonical.sort.batch_rows = 2_048;
    let mut output = gate_a_formal_output_config();
    output.sort.max_run_rows = 4_096;
    output.sort.batch_rows = 2_048;
    (canonical, output)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactManifestBindingsV1 {
    pub canonical_event_store: Sha256Hex,
    pub event_time_index: Sha256Hex,
    pub visibility_index: Sha256Hex,
    pub market_observations: Sha256Hex,
    pub reduction_reports: Sha256Hex,
}

impl GateAArtifactManifestBindingsV1 {
    pub fn from_candidate(candidate: &GateACandidate) -> Self {
        Self {
            canonical_event_store: candidate.canonical_event_store.manifest_sha256.clone(),
            event_time_index: candidate.event_time_index.manifest_sha256.clone(),
            visibility_index: candidate.visibility_index.manifest_sha256.clone(),
            market_observations: candidate.market_observations.manifest_sha256.clone(),
            reduction_reports: candidate.reduction_reports.manifest_sha256.clone(),
        }
    }

    fn all(&self) -> Vec<Sha256Hex> {
        vec![
            self.canonical_event_store.clone(),
            self.event_time_index.clone(),
            self.visibility_index.clone(),
            self.market_observations.clone(),
            self.reduction_reports.clone(),
        ]
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAQualityCountsV1 {
    pub fresh: u64,
    pub stale: u64,
    pub missing: u64,
    pub unknown_not_observed: u64,
    pub confirmed_gap: u64,
    pub invalid: u64,
}

impl GateAQualityCountsV1 {
    fn increment(&mut self, quality: MarketDataQuality) {
        let value = match quality {
            MarketDataQuality::Fresh => &mut self.fresh,
            MarketDataQuality::Stale => &mut self.stale,
            MarketDataQuality::Missing => &mut self.missing,
            MarketDataQuality::UnknownNotObserved => &mut self.unknown_not_observed,
            MarketDataQuality::ConfirmedGap => &mut self.confirmed_gap,
            MarketDataQuality::Invalid => &mut self.invalid,
        };
        *value = value.saturating_add(1);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAResyncCountsV1 {
    pub not_required: u64,
    pub required: u64,
    pub resync_boundary: u64,
}

impl GateAResyncCountsV1 {
    fn increment(&mut self, quality: ResyncQuality) {
        let value = match quality {
            ResyncQuality::NotRequired => &mut self.not_required,
            ResyncQuality::Required => &mut self.required,
            ResyncQuality::ResyncBoundary => &mut self.resync_boundary,
        };
        *value = value.saturating_add(1);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAMarketEventCountsV1 {
    pub market_lifecycle_transition: u64,
    pub market_trading_state: u64,
    pub initial_l2_snapshot: u64,
    pub best_bid_ask: u64,
    pub tick_size_change: u64,
    pub public_trade: u64,
    pub level_execution_transition: u64,
    pub quality_boundary: u64,
    pub market_resolution_terminal: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateATradingStateFieldCountsV1 {
    pub active: u64,
    pub closed: u64,
    pub accepting_orders: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAReferenceEventCountsV1 {
    pub chainlink_btc_usd: u64,
    pub binance_depth_state: u64,
    pub binance_aggregate_trade: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAReductionOperationCountsV1 {
    pub merge: u64,
    pub overwrite: u64,
    pub delete: u64,
    pub confirm: u64,
    pub reject: u64,
    pub suppress: u64,
}

impl GateAReductionOperationCountsV1 {
    fn increment(&mut self, operation: ReductionOperationKindV1) {
        let value = match operation {
            ReductionOperationKindV1::Merge => &mut self.merge,
            ReductionOperationKindV1::Overwrite => &mut self.overwrite,
            ReductionOperationKindV1::Delete => &mut self.delete,
            ReductionOperationKindV1::Confirm => &mut self.confirm,
            ReductionOperationKindV1::Reject => &mut self.reject,
            ReductionOperationKindV1::Suppress => &mut self.suppress,
        };
        *value = value.saturating_add(1);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAReductionDispositionCountsV1 {
    pub produced: u64,
    pub no_output: u64,
    pub suppressed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAObservationComponentFactsV1 {
    pub component: ObservationComponentId,
    pub rows: u64,
    pub missing_rows: u64,
    pub anchored_rows: u64,
    pub quality_counts: GateAQualityCountsV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAEvidenceRecordV1 {
    pub evidence_id: String,
    pub passed: bool,
    pub artifact_manifest_sha256s: Vec<Sha256Hex>,
    pub event_ids: Vec<CanonicalEventId>,
    pub observation_ids: Vec<Sha256Hex>,
    pub blocking_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAQualityIsolationReportV1 {
    pub schema_version: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub scope: CanonicalRunScope,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub artifact_rows: GateAArtifactRowCountsV1,
    pub selected_files: Vec<CanonicalRunInputFile>,
    pub selected_source_rows: SourceRowCounts,
    pub market_event_counts: GateAMarketEventCountsV1,
    pub trading_state_field_counts: GateATradingStateFieldCountsV1,
    pub reference_event_counts: GateAReferenceEventCountsV1,
    pub event_quality_counts: GateAQualityCountsV1,
    pub event_resync_counts: GateAResyncCountsV1,
    pub fresh_resync_boundary: u64,
    pub event_without_source_evidence: u64,
    pub event_without_source_time: u64,
    pub distinct_market_windows: u64,
    pub initial_snapshot_token_counts: BTreeMap<String, u64>,
    pub best_bid_ask_tokens: Vec<String>,
    pub tick_change_tokens: Vec<String>,
    pub known_old_tick_change_tokens: Vec<String>,
    pub public_trade_platform_id: u64,
    pub public_trade_derived_id: u64,
    pub level_execution_basis_counts: BTreeMap<String, u64>,
    pub terminal_with_source_time: u64,
    pub terminal_without_source_time: u64,
    pub observation_rows: u64,
    pub observation_component_facts: Vec<GateAObservationComponentFactsV1>,
    pub reduction_operation_counts: GateAReductionOperationCountsV1,
    pub reduction_disposition_counts: GateAReductionDispositionCountsV1,
    pub reduction_reason_counts: BTreeMap<String, u64>,
    pub quantity_conservation_reports: u64,
    pub blocking_terminal_reports: u64,
    pub evidence: Vec<GateAEvidenceRecordV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactWriteMetricsV1 {
    pub canonical_event_store: SortMetrics,
    pub event_time_index: SortMetrics,
    pub visibility_index: SortMetrics,
    pub market_observations: SortMetrics,
    pub reduction_reports: SortMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateACgroupMemoryEventsV1 {
    pub low: u64,
    pub high: u64,
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
    pub oom_group_kill: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAExternalResourceFactsV1 {
    pub build_exit_code: i32,
    pub maximum_resident_set_size_kib: u64,
    pub publication_filesystem_type: String,
    pub publication_root_device_id: u64,
    pub candidate_device_id: u64,
    pub cgroup_v2_relative_path: String,
    pub cgroup_process_ids_before: Vec<u32>,
    pub cgroup_process_ids_after: Vec<u32>,
    pub memory_peak_bytes: u64,
    pub memory_max_bytes: u64,
    pub memory_swap_peak_bytes: u64,
    pub memory_swap_max_bytes: u64,
    pub memory_events_before: GateACgroupMemoryEventsV1,
    pub memory_events_after: GateACgroupMemoryEventsV1,
    pub network_namespace: String,
    pub network_interfaces: Vec<String>,
    pub network_isolation_supervisor_pid: u32,
    pub network_isolation_supervisor_name: String,
    pub network_isolation_supervisor_argv: Vec<String>,
    pub time_v_output: String,
    pub time_v_output_sha256: Sha256Hex,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAHostFactsV1 {
    pub operating_system: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cpu_count: u32,
    pub physical_memory_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAResourceHighWaterReportV1 {
    pub schema_version: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub scope: CanonicalRunScope,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub quality_isolation_report_sha256: Sha256Hex,
    pub canonical_config: CanonicalRunConfig,
    pub output_config: GateAOutputConfig,
    pub canonical_metrics: CanonicalRunMetrics,
    pub build_metrics: GateABuildResourceMetrics,
    pub artifact_write_metrics: GateAArtifactWriteMetricsV1,
    pub final_artifact_directory_bytes: u64,
    pub host: GateAHostFactsV1,
    pub external: GateAExternalResourceFactsV1,
    pub evidence: GateAEvidenceRecordV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateATestCommandEvidenceV1 {
    pub argv: Vec<String>,
    pub working_directory: String,
    pub exit_code: i32,
    pub combined_output: String,
    pub combined_output_sha256: Sha256Hex,
    pub covered_evidence_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateATestEvidenceReportV1 {
    pub schema_version: String,
    pub git_commit: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub commands: Vec<GateATestCommandEvidenceV1>,
    pub evidence: Vec<GateAEvidenceRecordV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAInputManifestBindingV1 {
    pub utc_day: String,
    pub path: String,
    pub sha256: Sha256Hex,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactRowCountsV1 {
    pub canonical_event_store: u64,
    pub event_time_index: u64,
    pub visibility_index: u64,
    pub market_observations: u64,
    pub reduction_reports: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAControlFileBindingV1 {
    pub relative_path: String,
    pub sha256: Sha256Hex,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAControlBindingsV1 {
    pub quality_isolation_report: GateAControlFileBindingV1,
    pub resource_high_water_report: GateAControlFileBindingV1,
    pub test_evidence_report: GateAControlFileBindingV1,
    pub capability_matrix: GateAControlFileBindingV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAContractVersionsV1 {
    pub canonical_event_record: String,
    pub execution_market_event: String,
    pub canonical_reference_event: String,
    pub market_observation: String,
    pub event_time_index: String,
    pub visibility_index: String,
    pub artifact_manifest: String,
    pub reduction_report: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateATypedRawScanPolicyV1 {
    pub source_kind: String,
    pub schema_version: String,
    pub received_time_column: String,
    pub received_start_inclusive_utc_ms: i64,
    pub received_end_exclusive_utc_ms: i64,
    pub condition_id_column: Option<String>,
    pub condition_id_equals: Option<String>,
    pub projected_columns: Vec<String>,
    pub skipped_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateARunManifestV2 {
    pub schema_version: String,
    pub requirement_id: String,
    pub git_commit: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub contract_versions: GateAContractVersionsV1,
    pub canonical_rule_version: String,
    pub dataset_lineage_root: String,
    pub dataset_date_floor: String,
    pub covered_utc_dates: Vec<String>,
    pub condition_id: String,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub reference_received_start_utc_ms: i64,
    pub reference_received_end_utc_ms: i64,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub supported_schema_set_identity_sha256: Sha256Hex,
    pub supported_schemas: Vec<String>,
    pub typed_raw_scan_policies: Vec<GateATypedRawScanPolicyV1>,
    pub input_manifests: Vec<GateAInputManifestBindingV1>,
    pub selected_files: Vec<CanonicalRunInputFile>,
    pub canonical_config: CanonicalRunConfig,
    pub output_config: GateAOutputConfig,
    pub host: GateAHostFactsV1,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub control_files: GateAControlBindingsV1,
    pub artifact_rows: GateAArtifactRowCountsV1,
    pub canonical_digests: CanonicalRunDigests,
    pub rejected_reports: u64,
    pub suppressed_reports: u64,
    pub validation_result: String,
}

/// 只在计时构建与正式发布之间短暂存在；原子发布前必须删除。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAMeasuredBuildStateV1 {
    pub schema_version: String,
    pub git_commit: String,
    pub candidate_path: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub scope: CanonicalRunScope,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub artifact_rows: GateAArtifactRowCountsV1,
    pub quality_isolation_report_sha256: Sha256Hex,
    pub canonical_config: CanonicalRunConfig,
    pub output_config: GateAOutputConfig,
    pub canonical_metrics: CanonicalRunMetrics,
    pub build_metrics: GateABuildResourceMetrics,
    pub artifact_write_metrics: GateAArtifactWriteMetricsV1,
    pub artifact_directory_bytes: u64,
}

#[derive(Debug)]
pub enum GateAPublicationError {
    Artifact(GateAError),
    Invalid(&'static str),
    Io(io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for GateAPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Artifact(error) => write!(formatter, "门 A 产物错误：{error}"),
            Self::Invalid(message) => write!(formatter, "门 A 发布证据无效：{message}"),
            Self::Io(error) => write!(formatter, "门 A 发布 I/O 错误：{error}"),
            Self::Json(error) => write!(formatter, "门 A 发布 JSON 错误：{error}"),
        }
    }
}

impl std::error::Error for GateAPublicationError {}

impl From<GateAError> for GateAPublicationError {
    fn from(error: GateAError) -> Self {
        Self::Artifact(error)
    }
}

impl From<io::Error> for GateAPublicationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for GateAPublicationError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Default)]
struct QualityScan {
    market_event_counts: GateAMarketEventCountsV1,
    trading_state_field_counts: GateATradingStateFieldCountsV1,
    reference_event_counts: GateAReferenceEventCountsV1,
    event_quality_counts: GateAQualityCountsV1,
    event_resync_counts: GateAResyncCountsV1,
    fresh_resync_boundary: u64,
    event_without_source_evidence: u64,
    event_without_source_time: u64,
    market_windows: BTreeSet<Vec<u8>>,
    initial_snapshot_token_counts: BTreeMap<String, u64>,
    best_bid_ask_tokens: BTreeSet<String>,
    tick_change_tokens: BTreeSet<String>,
    known_old_tick_change_tokens: BTreeSet<String>,
    public_trade_platform_id: u64,
    public_trade_derived_id: u64,
    level_execution_basis_counts: BTreeMap<String, u64>,
    terminal_with_source_time: u64,
    terminal_without_source_time: u64,
    observation_rows: u64,
    observation_component_facts: Vec<GateAObservationComponentFactsV1>,
    reduction_operation_counts: GateAReductionOperationCountsV1,
    reduction_disposition_counts: GateAReductionDispositionCountsV1,
    reduction_reason_counts: BTreeMap<String, u64>,
    quantity_conservation_reports: u64,
    blocking_terminal_reports: u64,
    observation_witnesses: BTreeMap<ObservationComponentId, Sha256Hex>,
}

impl QualityScan {
    fn new() -> Self {
        Self {
            observation_component_facts: REQUIRED_OBSERVATION_COMPONENTS
                .into_iter()
                .map(|component| GateAObservationComponentFactsV1 {
                    component,
                    rows: 0,
                    missing_rows: 0,
                    anchored_rows: 0,
                    quality_counts: GateAQualityCountsV1::default(),
                })
                .collect(),
            ..Self::default()
        }
    }

    fn scan_record(&mut self, record: CanonicalEventRecordV1) -> Result<(), serde_json::Error> {
        match record {
            CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
                self.event_quality_counts
                    .increment(event.quality.availability);
                self.event_resync_counts.increment(event.quality.resync);
                if event.lineage.source_evidence.is_empty() {
                    self.event_without_source_evidence =
                        self.event_without_source_evidence.saturating_add(1);
                }
                if event.time.source_event_at_utc_ms.is_none() {
                    self.event_without_source_time =
                        self.event_without_source_time.saturating_add(1);
                }
                self.market_windows
                    .insert(serde_json::to_vec(&event.market)?);
                match event.payload {
                    ExecutionMarketEventPayloadV3::MarketLifecycleTransition(_) => {
                        self.market_event_counts.market_lifecycle_transition += 1;
                    }
                    ExecutionMarketEventPayloadV3::MarketTradingState(state) => {
                        self.market_event_counts.market_trading_state += 1;
                        self.trading_state_field_counts.active += u64::from(state.active.is_some());
                        self.trading_state_field_counts.closed += u64::from(state.closed.is_some());
                        self.trading_state_field_counts.accepting_orders +=
                            u64::from(state.accepting_orders.is_some());
                    }
                    ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                        self.market_event_counts.initial_l2_snapshot += 1;
                        *self
                            .initial_snapshot_token_counts
                            .entry(snapshot.asset.token_id.0)
                            .or_default() += 1;
                    }
                    ExecutionMarketEventPayloadV3::BestBidAsk(bba) => {
                        self.market_event_counts.best_bid_ask += 1;
                        self.best_bid_ask_tokens.insert(bba.asset.token_id.0);
                    }
                    ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
                        self.market_event_counts.tick_size_change += 1;
                        let token_id = change.asset.token_id.0;
                        if change.old_tick_size_pusd_per_share.is_some() {
                            self.known_old_tick_change_tokens.insert(token_id.clone());
                        }
                        self.tick_change_tokens.insert(token_id);
                    }
                    ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                        self.market_event_counts.public_trade += 1;
                        match trade.trade_id_source {
                            PublicTradeIdSourceV1::PlatformTransactionHash => {
                                self.public_trade_platform_id += 1;
                            }
                            PublicTradeIdSourceV1::DerivedSourceEvidence => {
                                self.public_trade_derived_id += 1;
                            }
                        }
                    }
                    ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                        self.market_event_counts.level_execution_transition += 1;
                        let basis = match transition.basis {
                            LevelExecutionBasis::TradeEvidence => "trade_evidence",
                            LevelExecutionBasis::LevelDeltaInference => "level_delta_inference",
                            LevelExecutionBasis::BookResyncDeltaInference => {
                                "book_resync_delta_inference"
                            }
                        };
                        *self
                            .level_execution_basis_counts
                            .entry(basis.to_owned())
                            .or_default() += 1;
                    }
                    ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => {
                        self.market_event_counts.quality_boundary += 1;
                        if boundary.previous_quality == MarketDataQuality::Missing
                            && boundary.current_quality == MarketDataQuality::Fresh
                            && event.quality.availability == MarketDataQuality::Fresh
                            && event.quality.resync == ResyncQuality::ResyncBoundary
                        {
                            self.fresh_resync_boundary =
                                self.fresh_resync_boundary.saturating_add(1);
                        }
                    }
                    ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {
                        self.market_event_counts.market_resolution_terminal += 1;
                        if event.time.source_event_at_utc_ms.is_some() {
                            self.terminal_with_source_time += 1;
                        } else {
                            self.terminal_without_source_time += 1;
                        }
                    }
                }
            }
            CanonicalEventRecordV1::ReferenceEvent(event) => {
                self.event_quality_counts
                    .increment(event.quality.availability);
                self.event_resync_counts.increment(event.quality.resync);
                if event.lineage.source_evidence.is_empty() {
                    self.event_without_source_evidence =
                        self.event_without_source_evidence.saturating_add(1);
                }
                if event.time.source_event_at_utc_ms.is_none() {
                    self.event_without_source_time =
                        self.event_without_source_time.saturating_add(1);
                }
                match event.payload {
                    CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(_) => {
                        self.reference_event_counts.chainlink_btc_usd += 1;
                    }
                    CanonicalReferenceEventPayloadV1::BinanceDepthState(_) => {
                        self.reference_event_counts.binance_depth_state += 1;
                        if event.quality.availability == MarketDataQuality::Fresh
                            && event.quality.resync == ResyncQuality::ResyncBoundary
                        {
                            self.fresh_resync_boundary =
                                self.fresh_resync_boundary.saturating_add(1);
                        }
                    }
                    CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(_) => {
                        self.reference_event_counts.binance_aggregate_trade += 1;
                    }
                }
            }
        }
        Ok(())
    }
}

fn increment_reason(reasons: &mut BTreeMap<String, u64>, reason: &str) {
    *reasons.entry(reason.to_owned()).or_default() += 1;
}

fn is_blocking_terminal_reason(reason: &str) -> bool {
    matches!(
        reason,
        "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_AFTER_VALID_NONTERMINAL"
            | "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_WITHOUT_VALID_RESPONSE"
            | "POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT"
            | "POLYMARKET_TERMINAL_CLAIMANT_CONFLICT"
            | "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT"
            | "POLYMARKET_TERMINAL_GAMMA_VISIBLE_ONLY_NO_SOURCE_TIME"
            | "POLYMARKET_TERMINAL_MARKET_ALREADY_QUARANTINED"
            | "POLYMARKET_TERMINAL_MARKET_IDENTITY_CONFLICT"
            | "POLYMARKET_TERMINAL_PAYOUT_EVIDENCE_CONFLICT"
            | "POLYMARKET_TERMINAL_RECEIVE_TIME_CONFLICT"
            | "POLYMARKET_TERMINAL_SOURCE_TIME_CONFLICT"
            | "POLYMARKET_TERMINAL_WINNER_CONFLICT"
    )
}

pub fn build_quality_isolation_report(
    candidate: &GateACandidate,
    validated: &ValidatedGateACandidate,
) -> Result<GateAQualityIsolationReportV1, GateAPublicationError> {
    build_quality_isolation_report_with_identity(
        candidate,
        validated,
        &pipeline_core_identity_sha256(),
    )
}

fn build_quality_isolation_report_with_identity(
    candidate: &GateACandidate,
    validated: &ValidatedGateACandidate,
    producer_pipeline_core_identity_sha256: &Sha256Hex,
) -> Result<GateAQualityIsolationReportV1, GateAPublicationError> {
    let artifact_manifests = GateAArtifactManifestBindingsV1::from_candidate(candidate);
    if artifact_manifests.canonical_event_store
        != *validated.canonical_event_store.manifest_sha256()
        || artifact_manifests.event_time_index != *validated.event_time_index.manifest_sha256()
        || artifact_manifests.visibility_index != *validated.visibility_index.manifest_sha256()
        || artifact_manifests.market_observations
            != *validated.market_observations.manifest_sha256()
        || artifact_manifests.reduction_reports != *validated.reduction_reports.manifest_sha256()
        || candidate.canonical_digests != validated.recomputed_stream_digests
    {
        return Err(GateAPublicationError::Invalid(
            "质量报告输入未绑定同一已验证候选",
        ));
    }

    let mut scan = QualityScan::new();
    let mut records = CanonicalEventStoreCursor::open(
        &candidate.canonical_event_store.manifest_path,
        candidate.output_config.sort.batch_rows,
        candidate.output_config.sort.max_run_bytes,
        QUALITY_SCAN_CANONICAL_PAGE_ROWS,
    )?;
    loop {
        let page = records.next_page(QUALITY_SCAN_CANONICAL_PAGE_ROWS)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            scan.scan_record(record)?;
        }
    }

    let mut observations = MarketObservationCursor::open(
        &candidate.market_observations.manifest_path,
        Some(candidate.scope.condition_id().to_owned()),
        Some((
            candidate.scope.received_start_utc_ms(),
            candidate.scope.received_end_utc_ms(),
        )),
        candidate.output_config.sort.batch_rows,
        candidate.output_config.sort.max_run_bytes,
        candidate.output_config.sort.batch_rows,
    )?;
    loop {
        let page = observations.next_page(candidate.output_config.sort.batch_rows)?;
        if page.is_empty() {
            break;
        }
        for observation in page {
            scan.observation_rows = scan.observation_rows.saturating_add(1);
            for (index, state) in observation.component_states.iter().enumerate() {
                let facts = scan.observation_component_facts.get_mut(index).ok_or(
                    GateAPublicationError::Invalid("Observation 组件数量超过冻结集合"),
                )?;
                if facts.component != state.component {
                    return Err(GateAPublicationError::Invalid(
                        "Observation 组件顺序偏离冻结集合",
                    ));
                }
                facts.rows = facts.rows.saturating_add(1);
                facts.quality_counts.increment(state.quality);
                if observation.missing_mask.contains(&state.component) {
                    facts.missing_rows = facts.missing_rows.saturating_add(1);
                }
                if state.last_visible_event_id.is_some() {
                    facts.anchored_rows = facts.anchored_rows.saturating_add(1);
                    scan.observation_witnesses
                        .entry(state.component)
                        .or_insert_with(|| observation.observation_id.clone());
                }
            }
        }
    }

    let mut reports = ReductionReportCursor::open(
        &candidate.reduction_reports.manifest_path,
        candidate.output_config.sort.batch_rows,
        candidate.output_config.sort.max_run_bytes,
        candidate.output_config.sort.batch_rows,
    )?;
    loop {
        let page = reports.next_page(candidate.output_config.sort.batch_rows)?;
        if page.is_empty() {
            break;
        }
        for report in page {
            scan.reduction_operation_counts.increment(report.operation);
            increment_reason(&mut scan.reduction_reason_counts, &report.reason_code);
            if report.quantity_conservation.is_some() {
                scan.quantity_conservation_reports += 1;
            }
            if is_blocking_terminal_reason(&report.reason_code) {
                scan.blocking_terminal_reports += 1;
            }
            match report.disposition {
                ReductionDispositionV1::Produced { .. } => {
                    scan.reduction_disposition_counts.produced += 1;
                }
                ReductionDispositionV1::NoOutput { .. } => {
                    scan.reduction_disposition_counts.no_output += 1;
                }
                ReductionDispositionV1::Suppressed { .. } => {
                    scan.reduction_disposition_counts.suppressed += 1;
                }
            }
        }
    }

    let observation_witnesses = std::mem::take(&mut scan.observation_witnesses);
    let mut report = GateAQualityIsolationReportV1 {
        schema_version: GATE_A_QUALITY_ISOLATION_REPORT_SCHEMA_V1.to_owned(),
        pipeline_core_identity_sha256: producer_pipeline_core_identity_sha256.clone(),
        input_set_identity_sha256: candidate.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: candidate.input_manifest_set_sha256.clone(),
        scope: candidate.scope.clone(),
        artifact_manifests,
        artifact_rows: GateAArtifactRowCountsV1 {
            canonical_event_store: candidate.canonical_event_store.manifest.file.rows,
            event_time_index: candidate.event_time_index.manifest.file.rows,
            visibility_index: candidate.visibility_index.manifest.file.rows,
            market_observations: candidate.market_observations.manifest.file.rows,
            reduction_reports: candidate.reduction_reports.manifest.file.rows,
        },
        selected_files: candidate.selected_files.clone(),
        selected_source_rows: candidate.canonical_metrics.selected_rows.clone(),
        market_event_counts: scan.market_event_counts,
        trading_state_field_counts: scan.trading_state_field_counts,
        reference_event_counts: scan.reference_event_counts,
        event_quality_counts: scan.event_quality_counts,
        event_resync_counts: scan.event_resync_counts,
        fresh_resync_boundary: scan.fresh_resync_boundary,
        event_without_source_evidence: scan.event_without_source_evidence,
        event_without_source_time: scan.event_without_source_time,
        distinct_market_windows: scan.market_windows.len() as u64,
        initial_snapshot_token_counts: scan.initial_snapshot_token_counts,
        best_bid_ask_tokens: scan.best_bid_ask_tokens.into_iter().collect(),
        tick_change_tokens: scan.tick_change_tokens.into_iter().collect(),
        known_old_tick_change_tokens: scan.known_old_tick_change_tokens.into_iter().collect(),
        public_trade_platform_id: scan.public_trade_platform_id,
        public_trade_derived_id: scan.public_trade_derived_id,
        level_execution_basis_counts: scan.level_execution_basis_counts,
        terminal_with_source_time: scan.terminal_with_source_time,
        terminal_without_source_time: scan.terminal_without_source_time,
        observation_rows: scan.observation_rows,
        observation_component_facts: scan.observation_component_facts,
        reduction_operation_counts: scan.reduction_operation_counts,
        reduction_disposition_counts: scan.reduction_disposition_counts,
        reduction_reason_counts: scan.reduction_reason_counts,
        quantity_conservation_reports: scan.quantity_conservation_reports,
        blocking_terminal_reports: scan.blocking_terminal_reports,
        evidence: Vec::new(),
    };
    report.evidence = quality_evidence(
        &report,
        &validated.projection_witnesses,
        &observation_witnesses,
    );
    validate_quality_isolation_report(&report)?;
    Ok(report)
}

fn quality_evidence(
    report: &GateAQualityIsolationReportV1,
    projection_witnesses: &GateAProjectionWitnesses,
    observation_witnesses: &BTreeMap<ObservationComponentId, Sha256Hex>,
) -> Vec<GateAEvidenceRecordV1> {
    let manifests = report.artifact_manifests.all();
    let predicates = quality_predicates(report);
    let checks = [
        ("quality/input-six-schemas", predicates[0], None, &[][..]),
        ("quality/dual-time-projection", predicates[1], None, &[][..]),
        (
            "quality/market-lifecycle",
            predicates[2],
            Some(GateAProjectionWitnessKind::MarketLifecycle),
            &[ObservationComponentId::MarketLifecycle][..],
        ),
        (
            "quality/initial-l2",
            predicates[3],
            Some(GateAProjectionWitnessKind::InitialL2),
            &[
                ObservationComponentId::PolymarketL2,
                ObservationComponentId::CurrentTick,
            ][..],
        ),
        (
            "quality/price-level-transition",
            predicates[4],
            Some(GateAProjectionWitnessKind::LevelExecution),
            &[ObservationComponentId::PolymarketL2][..],
        ),
        (
            "quality/best-bid-ask",
            predicates[5],
            Some(GateAProjectionWitnessKind::BestBidAsk),
            &[ObservationComponentId::BestBidAsk][..],
        ),
        (
            "quality/dynamic-tick",
            predicates[6],
            Some(GateAProjectionWitnessKind::DynamicTick),
            &[ObservationComponentId::CurrentTick][..],
        ),
        (
            "quality/public-trade",
            predicates[7],
            Some(GateAProjectionWitnessKind::PublicTrade),
            &[ObservationComponentId::PublicTrade][..],
        ),
        (
            "quality/level-execution",
            predicates[8],
            Some(GateAProjectionWitnessKind::LevelExecution),
            &[ObservationComponentId::PolymarketL2][..],
        ),
        (
            "quality/gap-resync-watermark",
            predicates[9],
            Some(GateAProjectionWitnessKind::GapResync),
            &[
                ObservationComponentId::PolymarketL2,
                ObservationComponentId::BinanceBtcUsdtDepth,
            ][..],
        ),
        (
            "quality/market-terminal",
            predicates[10],
            Some(GateAProjectionWitnessKind::MarketTerminal),
            &[ObservationComponentId::MarketResolution][..],
        ),
        (
            "quality/observation-lineage",
            predicates[11],
            None,
            &REQUIRED_OBSERVATION_COMPONENTS[..],
        ),
        ("quality/artifact-closure", predicates[12], None, &[][..]),
        ("quality/source-traceability", predicates[13], None, &[][..]),
    ];
    checks
        .into_iter()
        .map(|(id, passed, witness_kind, components)| {
            let linked_witness =
                witness_kind.and_then(|kind| projection_witnesses.linked.get(&kind));
            let event_ids = linked_witness
                .map(|witness| vec![witness.event_id.clone()])
                .unwrap_or_default();
            let mut observation_ids = if let Some(witness) = linked_witness {
                vec![witness.observation_id.clone()]
            } else if witness_kind.is_none() {
                components
                    .iter()
                    .filter_map(|component| observation_witnesses.get(component).cloned())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            observation_ids.sort_by(|left, right| left.0.cmp(&right.0));
            observation_ids.dedup();
            GateAEvidenceRecordV1 {
                evidence_id: id.to_owned(),
                passed,
                artifact_manifest_sha256s: manifests.clone(),
                event_ids,
                observation_ids,
                blocking_reason: (!passed).then(|| format!("{id}/predicate_failed")),
            }
        })
        .collect()
}

fn report_reason_count(report: &GateAQualityIsolationReportV1, reason: &str) -> u64 {
    report
        .reduction_reason_counts
        .get(reason)
        .copied()
        .unwrap_or(0)
}

fn quality_gap_resync_predicate(
    quality: &GateAQualityCountsV1,
    resync: &GateAResyncCountsV1,
    fresh_resync_boundary: u64,
) -> bool {
    let gap_count = u128::from(quality.confirmed_gap) + u128::from(quality.missing);
    let clean = gap_count == 0 && resync.required == 0 && fresh_resync_boundary == 0;
    (clean || quality_gap_resync_is_complete(quality, resync, fresh_resync_boundary))
        && quality.invalid == 0
}

fn quality_gap_resync_is_complete(
    quality: &GateAQualityCountsV1,
    resync: &GateAResyncCountsV1,
    fresh_resync_boundary: u64,
) -> bool {
    let gap_count = u128::from(quality.confirmed_gap) + u128::from(quality.missing);
    gap_count > 0
        && gap_count == u128::from(resync.required)
        && gap_count == u128::from(fresh_resync_boundary)
}

fn quality_predicates(report: &GateAQualityIsolationReportV1) -> [bool; 14] {
    let all_sources = &report.selected_source_rows;
    let all_nine_components_anchored = report
        .observation_component_facts
        .iter()
        .all(|facts| facts.anchored_rows > 0);
    let price_transition_reports = report_reason_count(report, "POLYMARKET_ABSOLUTE_LEVEL_UPDATE")
        .saturating_add(report_reason_count(report, "POLYMARKET_BOOK_RESYNC_DELTA"));
    let post_initial_book_reports =
        report_reason_count(report, "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION")
            .saturating_add(report_reason_count(report, "POLYMARKET_BOOK_RESYNC_DELTA"));
    [
        all_sources.polymarket_orderbook > 0
            && all_sources.polymarket_market_metadata > 0
            && all_sources.polymarket_rtds_chainlink_btc_usd > 0
            && all_sources.binance_depth_snapshot > 0
            && all_sources.binance_depth_update > 0
            && all_sources.binance_aggtrade > 0,
        report.artifact_rows.canonical_event_store > 0
            && report.artifact_rows.visibility_index == report.artifact_rows.canonical_event_store
            && report.artifact_rows.event_time_index > 0,
        report.market_event_counts.market_lifecycle_transition > 0
            && report.market_event_counts.market_trading_state > 0
            && report.trading_state_field_counts.active > 0
            && report.trading_state_field_counts.closed > 0
            && report.trading_state_field_counts.accepting_orders > 0
            && report.distinct_market_windows == 1,
        report.initial_snapshot_token_counts.len() == 2
            && report
                .initial_snapshot_token_counts
                .values()
                .all(|count| *count == 1)
            && post_initial_book_reports > 0,
        price_transition_reports > 0
            && report.quantity_conservation_reports > 0
            && report.market_event_counts.level_execution_transition > 0,
        report.market_event_counts.best_bid_ask > 0 && report.best_bid_ask_tokens.len() == 2,
        report.market_event_counts.tick_size_change > 0
            && report.tick_change_tokens.len() == 2
            && report.known_old_tick_change_tokens.len() == 2,
        report.market_event_counts.public_trade > 0
            && report
                .public_trade_platform_id
                .checked_add(report.public_trade_derived_id)
                == Some(report.market_event_counts.public_trade),
        report.market_event_counts.level_execution_transition > 0,
        quality_gap_resync_predicate(
            &report.event_quality_counts,
            &report.event_resync_counts,
            report.fresh_resync_boundary,
        ),
        report.market_event_counts.market_resolution_terminal == 1
            && report.terminal_with_source_time == 1
            && report.terminal_without_source_time == 0
            && report.blocking_terminal_reports == 0,
        report.observation_rows > 0 && all_nine_components_anchored,
        true,
        report.event_without_source_evidence == 0,
    ]
}

const QUALITY_EVIDENCE_IDS: [&str; 14] = [
    "quality/input-six-schemas",
    "quality/dual-time-projection",
    "quality/market-lifecycle",
    "quality/initial-l2",
    "quality/price-level-transition",
    "quality/best-bid-ask",
    "quality/dynamic-tick",
    "quality/public-trade",
    "quality/level-execution",
    "quality/gap-resync-watermark",
    "quality/market-terminal",
    "quality/observation-lineage",
    "quality/artifact-closure",
    "quality/source-traceability",
];

pub fn validate_quality_isolation_report(
    report: &GateAQualityIsolationReportV1,
) -> Result<(), GateAPublicationError> {
    if report.schema_version != GATE_A_QUALITY_ISOLATION_REPORT_SCHEMA_V1
        || CanonicalRunScope::new(
            report.scope.condition_id(),
            report.scope.received_start_utc_ms(),
            report.scope.received_end_utc_ms(),
        )
        .is_err()
        || report.selected_files.is_empty()
    {
        return Err(GateAPublicationError::Invalid("质量报告 schema 版本不匹配"));
    }
    for digest in std::iter::once(&report.pipeline_core_identity_sha256)
        .chain(std::iter::once(&report.input_set_identity_sha256))
        .chain(std::iter::once(&report.input_manifest_set_sha256))
        .chain(report.artifact_manifests.all().iter())
    {
        require_sha256(digest)?;
    }
    validate_selected_files(&report.selected_files)?;
    let market_events = sum_u64([
        report.market_event_counts.market_lifecycle_transition,
        report.market_event_counts.market_trading_state,
        report.market_event_counts.initial_l2_snapshot,
        report.market_event_counts.best_bid_ask,
        report.market_event_counts.tick_size_change,
        report.market_event_counts.public_trade,
        report.market_event_counts.level_execution_transition,
        report.market_event_counts.quality_boundary,
        report.market_event_counts.market_resolution_terminal,
    ]);
    let reference_events = sum_u64([
        report.reference_event_counts.chainlink_btc_usd,
        report.reference_event_counts.binance_depth_state,
        report.reference_event_counts.binance_aggregate_trade,
    ]);
    let event_qualities = sum_u64([
        report.event_quality_counts.fresh,
        report.event_quality_counts.stale,
        report.event_quality_counts.missing,
        report.event_quality_counts.unknown_not_observed,
        report.event_quality_counts.confirmed_gap,
        report.event_quality_counts.invalid,
    ]);
    let event_resync = sum_u64([
        report.event_resync_counts.not_required,
        report.event_resync_counts.required,
        report.event_resync_counts.resync_boundary,
    ]);
    let reduction_operations = sum_u64([
        report.reduction_operation_counts.merge,
        report.reduction_operation_counts.overwrite,
        report.reduction_operation_counts.delete,
        report.reduction_operation_counts.confirm,
        report.reduction_operation_counts.reject,
        report.reduction_operation_counts.suppress,
    ]);
    let reduction_dispositions = sum_u64([
        report.reduction_disposition_counts.produced,
        report.reduction_disposition_counts.no_output,
        report.reduction_disposition_counts.suppressed,
    ]);
    let initial_snapshots = report
        .initial_snapshot_token_counts
        .values()
        .copied()
        .map(u128::from)
        .sum::<u128>();
    let level_execution_bases = report
        .level_execution_basis_counts
        .values()
        .copied()
        .map(u128::from)
        .sum::<u128>();
    let reduction_reasons = report
        .reduction_reason_counts
        .values()
        .copied()
        .map(u128::from)
        .sum::<u128>();
    if market_events + reference_events != u128::from(report.artifact_rows.canonical_event_store)
        || event_qualities != u128::from(report.artifact_rows.canonical_event_store)
        || event_resync != u128::from(report.artifact_rows.canonical_event_store)
        || reduction_operations != u128::from(report.artifact_rows.reduction_reports)
        || reduction_dispositions != u128::from(report.artifact_rows.reduction_reports)
        || report.artifact_rows.visibility_index != report.artifact_rows.canonical_event_store
        || report.artifact_rows.event_time_index > report.artifact_rows.canonical_event_store
        || report.artifact_rows.market_observations != report.observation_rows
        || u128::from(report.terminal_with_source_time)
            + u128::from(report.terminal_without_source_time)
            != u128::from(report.market_event_counts.market_resolution_terminal)
        || u128::from(report.public_trade_platform_id) + u128::from(report.public_trade_derived_id)
            != u128::from(report.market_event_counts.public_trade)
        || initial_snapshots != u128::from(report.market_event_counts.initial_l2_snapshot)
        || level_execution_bases
            != u128::from(report.market_event_counts.level_execution_transition)
        || reduction_reasons != u128::from(report.artifact_rows.reduction_reports)
        || report.trading_state_field_counts.active
            > report.market_event_counts.market_trading_state
        || report.trading_state_field_counts.closed
            > report.market_event_counts.market_trading_state
        || report.trading_state_field_counts.accepting_orders
            > report.market_event_counts.market_trading_state
        || report.fresh_resync_boundary > report.event_quality_counts.fresh
        || report.fresh_resync_boundary > report.event_resync_counts.resync_boundary
    {
        return Err(GateAPublicationError::Invalid(
            "质量报告事件、索引、Observation 或归约计数不守恒",
        ));
    }
    if report.observation_component_facts.len() != REQUIRED_OBSERVATION_COMPONENTS.len()
        || report
            .observation_component_facts
            .iter()
            .zip(REQUIRED_OBSERVATION_COMPONENTS)
            .any(|(facts, expected)| facts.component != expected)
    {
        return Err(GateAPublicationError::Invalid(
            "质量报告 Observation 组件集合不完整或顺序错误",
        ));
    }
    for facts in &report.observation_component_facts {
        let qualities = sum_u64([
            facts.quality_counts.fresh,
            facts.quality_counts.stale,
            facts.quality_counts.missing,
            facts.quality_counts.unknown_not_observed,
            facts.quality_counts.confirmed_gap,
            facts.quality_counts.invalid,
        ]);
        if facts.rows != report.observation_rows
            || facts.missing_rows > facts.rows
            || facts.anchored_rows > facts.rows
            || qualities != u128::from(facts.rows)
        {
            return Err(GateAPublicationError::Invalid(
                "质量报告 Observation 组件计数不守恒",
            ));
        }
    }
    if !is_sorted_unique_strings(&report.best_bid_ask_tokens)
        || !is_sorted_unique_strings(&report.tick_change_tokens)
        || !is_sorted_unique_strings(&report.known_old_tick_change_tokens)
        || report
            .known_old_tick_change_tokens
            .iter()
            .any(|token| report.tick_change_tokens.binary_search(token).is_err())
    {
        return Err(GateAPublicationError::Invalid(
            "质量报告资产集合未规范排序去重",
        ));
    }
    if report.evidence.len() != QUALITY_EVIDENCE_IDS.len()
        || report
            .evidence
            .iter()
            .zip(QUALITY_EVIDENCE_IDS)
            .any(|(evidence, expected)| evidence.evidence_id != expected)
    {
        return Err(GateAPublicationError::Invalid(
            "质量报告证据集合不完整或顺序错误",
        ));
    }
    let expected_manifests = report.artifact_manifests.all();
    let predicates = quality_predicates(report);
    for (index, evidence) in report.evidence.iter().enumerate() {
        let expected_pass = predicates[index];
        let requires_gap_resync_witness = index == 9
            && quality_gap_resync_is_complete(
                &report.event_quality_counts,
                &report.event_resync_counts,
                report.fresh_resync_boundary,
            );
        let requires_event_witness = matches!(index, 2..=8 | 10) || requires_gap_resync_witness;
        let requires_observation_witness =
            matches!(index, 2..=8 | 10..=11) || requires_gap_resync_witness;
        if evidence.artifact_manifest_sha256s != expected_manifests {
            return Err(GateAPublicationError::Invalid(
                "质量证据未绑定当前五份产物 manifest",
            ));
        }
        if evidence.passed != expected_pass
            || evidence.passed != evidence.blocking_reason.is_none()
            || evidence.blocking_reason
                != (!expected_pass).then(|| format!("{}/predicate_failed", evidence.evidence_id))
            || (expected_pass && requires_event_witness && evidence.event_ids.is_empty())
            || (expected_pass
                && requires_observation_witness
                && evidence.observation_ids.is_empty())
            || (expected_pass
                && requires_event_witness
                && (evidence.event_ids.len() != 1 || evidence.observation_ids.len() != 1))
        {
            return Err(GateAPublicationError::Invalid(
                "质量证据状态与报告事实或阻断原因不一致",
            ));
        }
        if !is_sorted_unique_event_ids(&evidence.event_ids)
            || !is_sorted_unique_sha256(&evidence.observation_ids)
            || evidence
                .event_ids
                .iter()
                .any(|event_id| require_sha256(&Sha256Hex(event_id.0.clone())).is_err())
            || evidence
                .observation_ids
                .iter()
                .any(|observation_id| require_sha256(observation_id).is_err())
        {
            return Err(GateAPublicationError::Invalid(
                "质量证据事件或 Observation 身份未规范排序去重",
            ));
        }
    }
    Ok(())
}

fn sum_u64<const N: usize>(values: [u64; N]) -> u128 {
    values.into_iter().map(u128::from).sum()
}

fn is_sorted_unique_strings(values: &[String]) -> bool {
    values
        .windows(2)
        .all(|pair| pair[0].as_str() < pair[1].as_str())
}

fn validate_selected_files(files: &[CanonicalRunInputFile]) -> Result<(), GateAPublicationError> {
    let mut keys = BTreeSet::new();
    for file in files {
        let relative_path = Path::new(&file.manifest_relative_path);
        let mut path_parts = file.manifest_relative_path.split('/');
        let source_directory = path_parts.next();
        let file_name = path_parts.next();
        if file.utc_day.as_str() < DATASET_DATE_FLOOR
            || Date::parse(&file.utc_day, &format_description!("[year]-[month]-[day]")).is_err()
            || file.source_kind.trim().is_empty()
            || crate::supported_schema_for_source_kind(&file.source_kind)
                != Some(file.schema_version.as_str())
            || !is_valid_utc_hour(&file.utc_day, &file.utc_hour)
            || file.bytes == 0
            || file.rows == 0
            || source_directory != Some(format!("source_kind={}", file.source_kind).as_str())
            || file_name.is_none_or(|name| name.is_empty())
            || path_parts.next().is_some()
            || relative_path.is_absolute()
            || relative_path.components().count() != 2
            || relative_path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            || !keys.insert((file.utc_day.clone(), file.manifest_relative_path.clone()))
        {
            return Err(GateAPublicationError::Invalid(
                "报告选中文件身份、schema、路径、大小或唯一性无效",
            ));
        }
        require_sha256(&Sha256Hex(file.manifest_sha256.clone()))?;
        require_sha256(&Sha256Hex(file.file_sha256.clone()))?;
    }
    Ok(())
}

fn is_valid_utc_hour(utc_day: &str, utc_hour: &str) -> bool {
    let prefix = format!("{utc_day}T");
    utc_hour
        .strip_prefix(&prefix)
        .filter(|hour| hour.len() == 2 && hour.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|hour| hour.parse::<u8>().ok())
        .is_some_and(|hour| hour < 24)
}

pub fn encode_quality_isolation_report(
    report: &GateAQualityIsolationReportV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_quality_isolation_report(report)?;
    encode_bounded_json(report, 4 * 1024 * 1024, "质量报告超过 4 MiB 上限")
}

pub fn decode_quality_isolation_report(
    bytes: &[u8],
) -> Result<GateAQualityIsolationReportV1, GateAPublicationError> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(GateAPublicationError::Invalid("质量报告超过 4 MiB 上限"));
    }
    let report = serde_json::from_slice(bytes)?;
    validate_quality_isolation_report(&report)?;
    Ok(report)
}

fn require_sha256(value: &Sha256Hex) -> Result<(), GateAPublicationError> {
    if value.0.len() != 64
        || !value
            .0
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(GateAPublicationError::Invalid(
            "证据摘要不是 64 位小写十六进制 SHA-256",
        ));
    }
    Ok(())
}

fn encode_bounded_json<T: Serialize>(
    value: &T,
    max_bytes: usize,
    overflow_message: &'static str,
) -> Result<Vec<u8>, GateAPublicationError> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > max_bytes {
        return Err(GateAPublicationError::Invalid(overflow_message));
    }
    Ok(encoded)
}

fn is_sorted_unique_event_ids(values: &[CanonicalEventId]) -> bool {
    values
        .windows(2)
        .all(|pair| pair[0].0.as_str() < pair[1].0.as_str())
}

fn is_sorted_unique_sha256(values: &[Sha256Hex]) -> bool {
    values
        .windows(2)
        .all(|pair| pair[0].0.as_str() < pair[1].0.as_str())
}

pub fn build_measured_build_state(
    candidate: &GateACandidate,
    quality_isolation_report_sha256: Sha256Hex,
    artifact_directory_bytes: u64,
    git_commit: String,
) -> Result<GateAMeasuredBuildStateV1, GateAPublicationError> {
    let state = GateAMeasuredBuildStateV1 {
        schema_version: GATE_A_MEASURED_BUILD_STATE_SCHEMA_V1.to_owned(),
        git_commit,
        candidate_path: candidate.candidate_path.to_string_lossy().into_owned(),
        pipeline_core_identity_sha256: pipeline_core_identity_sha256(),
        input_set_identity_sha256: candidate.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: candidate.input_manifest_set_sha256.clone(),
        scope: candidate.scope.clone(),
        artifact_manifests: GateAArtifactManifestBindingsV1::from_candidate(candidate),
        artifact_rows: GateAArtifactRowCountsV1 {
            canonical_event_store: candidate.canonical_event_store.manifest.file.rows,
            event_time_index: candidate.event_time_index.manifest.file.rows,
            visibility_index: candidate.visibility_index.manifest.file.rows,
            market_observations: candidate.market_observations.manifest.file.rows,
            reduction_reports: candidate.reduction_reports.manifest.file.rows,
        },
        quality_isolation_report_sha256,
        canonical_config: candidate.config.clone(),
        output_config: candidate.output_config.clone(),
        canonical_metrics: candidate.canonical_metrics.clone(),
        build_metrics: candidate.resource_metrics.clone(),
        artifact_write_metrics: GateAArtifactWriteMetricsV1 {
            canonical_event_store: candidate.canonical_event_store.sort_metrics.clone(),
            event_time_index: candidate.event_time_index.sort_metrics.clone(),
            visibility_index: candidate.visibility_index.sort_metrics.clone(),
            market_observations: candidate.market_observations.sort_metrics.clone(),
            reduction_reports: candidate.reduction_reports.sort_metrics.clone(),
        },
        artifact_directory_bytes,
    };
    validate_measured_build_state(&state)?;
    Ok(state)
}

pub fn validate_measured_build_state(
    state: &GateAMeasuredBuildStateV1,
) -> Result<(), GateAPublicationError> {
    if state.schema_version != GATE_A_MEASURED_BUILD_STATE_SCHEMA_V1
        || state.pipeline_core_identity_sha256 != pipeline_core_identity_sha256()
        || state.git_commit.len() != 40
        || !state
            .git_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !Path::new(&state.candidate_path).is_absolute()
        || state.canonical_config != gate_a_formal_canonical_config()
        || state.output_config != gate_a_formal_output_config()
        || state.artifact_directory_bytes == 0
        || state.artifact_rows.canonical_event_store == 0
        || state.artifact_rows.event_time_index == 0
        || state.artifact_rows.visibility_index != state.artifact_rows.canonical_event_store
        || state.artifact_rows.market_observations == 0
        || state.artifact_rows.reduction_reports == 0
        || state.canonical_metrics.canonical_records != state.artifact_rows.canonical_event_store
        || state.canonical_metrics.event_time_projections != state.artifact_rows.event_time_index
        || state.canonical_metrics.visibility_projections != state.artifact_rows.visibility_index
        || state.canonical_metrics.reduction_reports != state.artifact_rows.reduction_reports
        || state.build_metrics.peak_observation_l2_levels == 0
    {
        return Err(GateAPublicationError::Invalid(
            "计时构建状态的身份、配置、路径、行数或峰值无效",
        ));
    }
    for digest in std::iter::once(&state.pipeline_core_identity_sha256)
        .chain(std::iter::once(&state.input_set_identity_sha256))
        .chain(std::iter::once(&state.input_manifest_set_sha256))
        .chain(std::iter::once(&state.quality_isolation_report_sha256))
        .chain(state.artifact_manifests.all().iter())
    {
        require_sha256(digest)?;
    }
    Ok(())
}

pub fn encode_measured_build_state(
    state: &GateAMeasuredBuildStateV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_measured_build_state(state)?;
    encode_bounded_json(state, 4 * 1024 * 1024, "计时构建状态超过 4 MiB 上限")
}

pub fn decode_measured_build_state(
    bytes: &[u8],
) -> Result<GateAMeasuredBuildStateV1, GateAPublicationError> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(GateAPublicationError::Invalid(
            "计时构建状态超过 4 MiB 上限",
        ));
    }
    let state = serde_json::from_slice(bytes)?;
    validate_measured_build_state(&state)?;
    Ok(state)
}

pub fn build_resource_high_water_report_from_state(
    state: &GateAMeasuredBuildStateV1,
    host: GateAHostFactsV1,
    external: GateAExternalResourceFactsV1,
) -> Result<GateAResourceHighWaterReportV1, GateAPublicationError> {
    validate_measured_build_state(state)?;
    let passed = resource_facts_fit(
        &state.canonical_config,
        &state.output_config,
        &state.canonical_metrics,
        &state.build_metrics,
        &state.artifact_write_metrics,
        state.artifact_directory_bytes,
        &external,
    );
    let report = GateAResourceHighWaterReportV1 {
        schema_version: GATE_A_RESOURCE_HIGH_WATER_REPORT_SCHEMA_V1.to_owned(),
        pipeline_core_identity_sha256: pipeline_core_identity_sha256(),
        input_set_identity_sha256: state.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: state.input_manifest_set_sha256.clone(),
        scope: state.scope.clone(),
        artifact_manifests: state.artifact_manifests.clone(),
        quality_isolation_report_sha256: state.quality_isolation_report_sha256.clone(),
        canonical_config: state.canonical_config.clone(),
        output_config: state.output_config.clone(),
        canonical_metrics: state.canonical_metrics.clone(),
        build_metrics: state.build_metrics.clone(),
        artifact_write_metrics: state.artifact_write_metrics.clone(),
        final_artifact_directory_bytes: state.artifact_directory_bytes,
        host,
        external,
        evidence: GateAEvidenceRecordV1 {
            evidence_id: "resource/formal-run".to_owned(),
            passed,
            artifact_manifest_sha256s: state.artifact_manifests.all(),
            event_ids: Vec::new(),
            observation_ids: Vec::new(),
            blocking_reason: (!passed).then(|| "resource/formal-run/limit_failed".to_owned()),
        },
    };
    validate_resource_high_water_report(&report)?;
    Ok(report)
}

fn resource_report_limits_pass(report: &GateAResourceHighWaterReportV1) -> bool {
    resource_facts_fit(
        &report.canonical_config,
        &report.output_config,
        &report.canonical_metrics,
        &report.build_metrics,
        &report.artifact_write_metrics,
        report.final_artifact_directory_bytes,
        &report.external,
    )
}

fn resource_facts_fit(
    config: &CanonicalRunConfig,
    output: &GateAOutputConfig,
    metrics: &CanonicalRunMetrics,
    build_metrics: &GateABuildResourceMetrics,
    artifact_write_metrics: &GateAArtifactWriteMetricsV1,
    final_artifact_directory_bytes: u64,
    external: &GateAExternalResourceFactsV1,
) -> bool {
    let capture_bytes_fit = build_metrics
        .canonical_record_capture_bytes
        .checked_add(build_metrics.reduction_report_capture_bytes)
        .is_some_and(|total| total <= output.max_capture_output_bytes);
    config == &gate_a_formal_canonical_config()
        && output == &gate_a_formal_output_config()
        && external.build_exit_code == 0
        && formal_memory_gate_fits(
            external.maximum_resident_set_size_kib,
            external.memory_max_bytes,
            external.memory_peak_bytes,
        )
        && external.publication_filesystem_type == "ext4"
        && external.publication_root_device_id != 0
        && external.publication_root_device_id == external.candidate_device_id
        && external.cgroup_process_ids_before.len() == 2
        && external
            .cgroup_process_ids_before
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && external.cgroup_process_ids_before == external.cgroup_process_ids_after
        && external.network_isolation_supervisor_pid > 0
        && external
            .cgroup_process_ids_before
            .contains(&external.network_isolation_supervisor_pid)
        && external.memory_swap_max_bytes == 0
        && external.memory_swap_peak_bytes == 0
        && external.memory_events_after.max == external.memory_events_before.max
        && external.memory_events_after.oom == external.memory_events_before.oom
        && external.memory_events_after.oom_kill == external.memory_events_before.oom_kill
        && external.memory_events_after.oom_group_kill
            == external.memory_events_before.oom_group_kill
        && external.memory_events_before
            == (GateACgroupMemoryEventsV1 {
                low: 0,
                high: 0,
                max: 0,
                oom: 0,
                oom_kill: 0,
                oom_group_kill: 0,
            })
        && !external.network_namespace.is_empty()
        && external.network_interfaces == ["lo"]
        && metrics.peak_decode_batch_rows <= config.decode_batch_rows
        && metrics.peak_decode_batch_bytes <= config.max_decode_batch_bytes
        && metrics.peak_temporary_row_bytes <= config.max_temporary_row_bytes
        && metrics.polymarket_epoch_count <= config.max_epoch_count_per_source
        && metrics.binance_depth_epoch_count <= config.max_epoch_count_per_source
        && metrics.peak_pending_ws_terminals <= config.max_pending_ws_terminals
        && metrics.peak_retained_reducer_entries <= config.max_retained_reducer_entries
        && metrics.peak_open_input_files <= 1
        && metrics.peak_business_queue_rows <= 1
        && metrics.peak_hot_markets <= 1
        && metrics.parallelism == 1
        && metrics.peak_canonical_record_bytes <= output.max_artifact_payload_bytes
        && metrics.peak_projection_bytes <= output.max_artifact_payload_bytes
        && metrics.peak_reduction_report_bytes <= output.max_artifact_payload_bytes
        && build_metrics.peak_observation_l2_levels <= output.max_observation_l2_levels
        && capture_bytes_fit
        && build_metrics.peak_candidate_directory_bytes_bound
            <= output.max_candidate_directory_bytes
        && build_metrics.peak_candidate_directory_bytes_bound >= final_artifact_directory_bytes
        && formal_work_directory_bound_covers(
            build_metrics.peak_candidate_directory_bytes_bound,
            build_metrics.peak_formal_work_directory_bytes_bound,
            None,
        )
        && build_metrics.peak_formal_work_directory_bytes_bound
            <= output.max_candidate_directory_bytes
        && final_artifact_directory_bytes <= output.max_candidate_directory_bytes
        && sort_metrics_fit(&metrics.sort, &config.sort)
        && sort_metrics_fit(&build_metrics.visibility_material_sort, &output.sort)
        && sort_metrics_fit(&artifact_write_metrics.canonical_event_store, &output.sort)
        && sort_metrics_fit(&artifact_write_metrics.event_time_index, &output.sort)
        && sort_metrics_fit(&artifact_write_metrics.visibility_index, &output.sort)
        && sort_metrics_fit(&artifact_write_metrics.market_observations, &output.sort)
        && sort_metrics_fit(&artifact_write_metrics.reduction_reports, &output.sort)
}

fn formal_memory_gate_fits(
    maximum_resident_set_size_kib: u64,
    memory_max_bytes: u64,
    memory_peak_bytes: u64,
) -> bool {
    maximum_resident_set_size_kib <= GATE_A_MAX_RSS_KIB
        && memory_max_bytes == GATE_A_CGROUP_MEMORY_MAX_BYTES
        && memory_peak_bytes <= GATE_A_CGROUP_MEMORY_MAX_BYTES
}

fn formal_work_directory_bound_covers(
    candidate_peak_bytes: u64,
    reported_formal_peak_bytes: u64,
    revalidated_formal_peak_bytes: Option<u64>,
) -> bool {
    candidate_peak_bytes
        .checked_add(crate::gate_a::GATE_A_CONTROL_FILE_BYTES_BOUND)
        .is_some_and(|minimum| reported_formal_peak_bytes >= minimum)
        && revalidated_formal_peak_bytes.is_none_or(|minimum| reported_formal_peak_bytes >= minimum)
}

fn sort_metrics_fit(metrics: &SortMetrics, config: &crate::SortConfig) -> bool {
    metrics.input_rows == metrics.output_rows
        && metrics.peak_run_rows <= config.max_run_rows
        && metrics.peak_run_bytes <= config.max_run_bytes
        && metrics.peak_write_buffer_rows <= config.batch_rows
        && metrics.peak_write_buffer_bytes <= config.max_run_bytes
        && metrics.peak_temp_run_files <= config.max_temp_run_files
        && metrics.peak_open_files <= config.max_open_files
        && metrics.peak_heap_entries <= config.merge_fan_in
        && metrics.peak_temp_bytes <= config.max_temp_bytes
}

pub fn validate_resource_high_water_report(
    report: &GateAResourceHighWaterReportV1,
) -> Result<(), GateAPublicationError> {
    let expected_pass = resource_report_limits_pass(report);
    if report.schema_version != GATE_A_RESOURCE_HIGH_WATER_REPORT_SCHEMA_V1
        || report.evidence.evidence_id != "resource/formal-run"
        || report.evidence.passed != expected_pass
        || report.evidence.passed != report.evidence.blocking_reason.is_none()
        || report.evidence.blocking_reason
            != (!expected_pass).then(|| "resource/formal-run/limit_failed".to_owned())
        || report.evidence.artifact_manifest_sha256s != report.artifact_manifests.all()
        || !report.evidence.event_ids.is_empty()
        || !report.evidence.observation_ids.is_empty()
    {
        return Err(GateAPublicationError::Invalid(
            "资源报告 schema、证据状态或产物绑定无效",
        ));
    }
    if report.host.operating_system.trim().is_empty()
        || report.host.architecture.trim().is_empty()
        || report.host.cpu_model.trim().is_empty()
        || report.host.logical_cpu_count == 0
        || report.host.physical_memory_bytes == 0
    {
        return Err(GateAPublicationError::Invalid("资源报告主机事实无效"));
    }
    for digest in [
        &report.pipeline_core_identity_sha256,
        &report.input_set_identity_sha256,
        &report.input_manifest_set_sha256,
        &report.quality_isolation_report_sha256,
        &report.external.time_v_output_sha256,
    ] {
        require_sha256(digest)?;
    }
    for digest in report.artifact_manifests.all() {
        require_sha256(&digest)?;
    }
    let time_digest = Sha256Hex(format!(
        "{:x}",
        Sha256::digest(report.external.time_v_output.as_bytes())
    ));
    if report.external.time_v_output.is_empty()
        || !report.external.cgroup_v2_relative_path.starts_with('/')
        || report.external.cgroup_v2_relative_path == "/"
        || Path::new(&report.external.cgroup_v2_relative_path)
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
        || Path::new(&report.external.cgroup_v2_relative_path)
            .file_name()
            .and_then(|name| name.to_str())
            .is_none_or(|name| !name.starts_with("dpt-gate-a-") || !name.ends_with(".service"))
        || report.external.publication_filesystem_type != "ext4"
        || report.external.publication_root_device_id == 0
        || report.external.publication_root_device_id != report.external.candidate_device_id
        || report.external.cgroup_process_ids_before.len() != 2
        || !report
            .external
            .cgroup_process_ids_before
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        || report.external.cgroup_process_ids_before != report.external.cgroup_process_ids_after
        || report.external.network_namespace.is_empty()
        || report.external.network_interfaces != ["lo"]
        || report.external.network_isolation_supervisor_pid == 0
        || !report
            .external
            .cgroup_process_ids_before
            .contains(&report.external.network_isolation_supervisor_pid)
        || report.external.network_isolation_supervisor_name != "bwrap"
        || report.external.network_isolation_supervisor_argv.len() <= 7
        || report.external.network_isolation_supervisor_argv[0] != "/usr/bin/bwrap"
        || report.external.network_isolation_supervisor_argv[1..7]
            != [
                "--unshare-user",
                "--uid",
                "0",
                "--gid",
                "0",
                "--unshare-net",
            ]
        || report
            .external
            .network_isolation_supervisor_argv
            .iter()
            .any(|argument| argument == "--share-net")
        || !report
            .external
            .network_isolation_supervisor_argv
            .windows(2)
            .any(|pair| pair == ["--proc", "/proc"])
        || time_digest != report.external.time_v_output_sha256
        || parse_time_v_maximum_rss_kib(&report.external.time_v_output)
            != Some(report.external.maximum_resident_set_size_kib)
        || parse_time_v_exit_status(&report.external.time_v_output)
            != Some(report.external.build_exit_code)
        || report.external.memory_peak_bytes == 0
        || !memory_events_are_monotonic(
            &report.external.memory_events_before,
            &report.external.memory_events_after,
        )
        || report.final_artifact_directory_bytes == 0
    {
        return Err(GateAPublicationError::Invalid(
            "资源报告的 time 或 cgroup 原始证据无效",
        ));
    }
    Ok(())
}

fn parse_time_v_maximum_rss_kib(output: &str) -> Option<u64> {
    output.lines().find_map(|line| {
        line.trim()
            .strip_prefix("Maximum resident set size (kbytes):")
            .and_then(|value| value.trim().parse().ok())
    })
}

fn parse_time_v_exit_status(output: &str) -> Option<i32> {
    output.lines().find_map(|line| {
        line.trim()
            .strip_prefix("Exit status:")
            .and_then(|value| value.trim().parse().ok())
    })
}

fn memory_events_are_monotonic(
    before: &GateACgroupMemoryEventsV1,
    after: &GateACgroupMemoryEventsV1,
) -> bool {
    after.low >= before.low
        && after.high >= before.high
        && after.max >= before.max
        && after.oom >= before.oom
        && after.oom_kill >= before.oom_kill
        && after.oom_group_kill >= before.oom_group_kill
}

pub fn encode_resource_high_water_report(
    report: &GateAResourceHighWaterReportV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_resource_high_water_report(report)?;
    encode_bounded_json(report, 4 * 1024 * 1024, "资源报告超过 4 MiB 上限")
}

pub fn decode_resource_high_water_report(
    bytes: &[u8],
) -> Result<GateAResourceHighWaterReportV1, GateAPublicationError> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(GateAPublicationError::Invalid("资源报告超过 4 MiB 上限"));
    }
    let report = serde_json::from_slice(bytes)?;
    validate_resource_high_water_report(&report)?;
    Ok(report)
}

pub const REQUIRED_GATE_A_TEST_EVIDENCE_IDS: [&str; 9] = [
    "test/atomic-publication",
    "test/candidate-corruption",
    "test/epoch-bba",
    "test/forbidden-low-frequency-fields",
    "test/offline-boundary",
    "test/physical-perturbation",
    "test/quality-boundaries",
    "test/training-entry-rejects-gate-a",
    "test/workspace-regression",
];

pub const GATE_A_WORKSPACE_TEST_ARGV: [&str; 12] = [
    "cargo",
    "test",
    "--workspace",
    "--lib",
    "--bins",
    "--tests",
    "--examples",
    "--locked",
    "--offline",
    "--",
    "--skip",
    "four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second",
];

pub const GATE_A_WORKSPACE_TEST_EVIDENCE_IDS: [&str; 7] = [
    "test/atomic-publication",
    "test/candidate-corruption",
    "test/epoch-bba",
    "test/forbidden-low-frequency-fields",
    "test/quality-boundaries",
    "test/training-entry-rejects-gate-a",
    "test/workspace-regression",
];

pub fn build_test_evidence_report(
    git_commit: String,
    artifact_manifests: GateAArtifactManifestBindingsV1,
    commands: Vec<GateATestCommandEvidenceV1>,
) -> Result<GateATestEvidenceReportV1, GateAPublicationError> {
    let manifests = artifact_manifests.all();
    let evidence = REQUIRED_GATE_A_TEST_EVIDENCE_IDS
        .into_iter()
        .map(|evidence_id| {
            let matching = commands
                .iter()
                .filter(|command| {
                    command
                        .covered_evidence_ids
                        .binary_search_by(|value| value.as_str().cmp(evidence_id))
                        .is_ok()
                })
                .collect::<Vec<_>>();
            let passed =
                !matching.is_empty() && matching.iter().all(|command| command.exit_code == 0);
            GateAEvidenceRecordV1 {
                evidence_id: evidence_id.to_owned(),
                passed,
                artifact_manifest_sha256s: manifests.clone(),
                event_ids: Vec::new(),
                observation_ids: Vec::new(),
                blocking_reason: (!passed)
                    .then(|| format!("{evidence_id}/command_failed_or_missing")),
            }
        })
        .collect();
    let report = GateATestEvidenceReportV1 {
        schema_version: GATE_A_TEST_EVIDENCE_REPORT_SCHEMA_V1.to_owned(),
        git_commit,
        pipeline_core_identity_sha256: pipeline_core_identity_sha256(),
        artifact_manifests,
        commands,
        evidence,
    };
    validate_test_evidence_report(&report)?;
    Ok(report)
}

pub fn validate_test_evidence_report(
    report: &GateATestEvidenceReportV1,
) -> Result<(), GateAPublicationError> {
    if report.schema_version != GATE_A_TEST_EVIDENCE_REPORT_SCHEMA_V1
        || report.git_commit.len() != 40
        || !report
            .git_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || report.commands.len() != 2
        || report.evidence.len() != REQUIRED_GATE_A_TEST_EVIDENCE_IDS.len()
        || report
            .evidence
            .iter()
            .zip(REQUIRED_GATE_A_TEST_EVIDENCE_IDS)
            .any(|(evidence, expected)| evidence.evidence_id != expected)
    {
        return Err(GateAPublicationError::Invalid(
            "测试证据的 schema、提交或固定证据集合无效",
        ));
    }
    require_sha256(&report.pipeline_core_identity_sha256)?;
    let manifests = report.artifact_manifests.all();
    for command in &report.commands {
        if command.argv.is_empty()
            || command.argv.iter().any(|argument| argument.is_empty())
            || command.working_directory != "/home/chillchill/sim-system"
            || command.combined_output.is_empty()
            || command.covered_evidence_ids.is_empty()
            || command
                .covered_evidence_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || command.covered_evidence_ids.iter().any(|evidence_id| {
                REQUIRED_GATE_A_TEST_EVIDENCE_IDS
                    .binary_search(&evidence_id.as_str())
                    .is_err()
            })
        {
            return Err(GateAPublicationError::Invalid(
                "测试命令或其固定覆盖集合无效",
            ));
        }
        require_sha256(&command.combined_output_sha256)?;
        if command.combined_output_sha256
            != Sha256Hex(format!(
                "{:x}",
                Sha256::digest(command.combined_output.as_bytes())
            ))
        {
            return Err(GateAPublicationError::Invalid("测试命令输出摘要不匹配"));
        }
    }
    let physical = &report.commands[0];
    let workspace = &report.commands[1];
    if physical.covered_evidence_ids != ["test/offline-boundary", "test/physical-perturbation"]
        || physical.argv.first().map(String::as_str) != Some("/usr/bin/time")
        || !physical.argv.iter().any(|argument| argument == "-v")
        || !physical
            .argv
            .iter()
            .any(|argument| argument == "__build-child")
        || !physical
            .combined_output
            .contains("physical_perturbation=PASS")
        || !physical
            .combined_output
            .contains("runtime_network_isolation=PASS")
        || workspace.argv
            != GATE_A_WORKSPACE_TEST_ARGV
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        || workspace.covered_evidence_ids
            != GATE_A_WORKSPACE_TEST_EVIDENCE_IDS
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
    {
        return Err(GateAPublicationError::Invalid(
            "测试报告必须精确绑定真实物理扰动与全工作区离线回归",
        ));
    }
    for evidence in &report.evidence {
        let matching = report.commands.iter().filter(|command| {
            command
                .covered_evidence_ids
                .binary_search(&evidence.evidence_id)
                .is_ok()
        });
        let matching = matching.collect::<Vec<_>>();
        let expected_pass =
            !matching.is_empty() && matching.iter().all(|command| command.exit_code == 0);
        if evidence.passed != expected_pass
            || evidence.passed != evidence.blocking_reason.is_none()
            || evidence.artifact_manifest_sha256s != manifests
            || !evidence.event_ids.is_empty()
            || !evidence.observation_ids.is_empty()
        {
            return Err(GateAPublicationError::Invalid(
                "测试证据状态与实际命令结果不一致",
            ));
        }
    }
    Ok(())
}

pub fn encode_test_evidence_report(
    report: &GateATestEvidenceReportV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_test_evidence_report(report)?;
    encode_bounded_json(report, 16 * 1024 * 1024, "测试证据报告超过 16 MiB 上限")
}

pub fn decode_test_evidence_report(
    bytes: &[u8],
) -> Result<GateATestEvidenceReportV1, GateAPublicationError> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(GateAPublicationError::Invalid(
            "测试证据报告超过 16 MiB 上限",
        ));
    }
    let report = serde_json::from_slice(bytes)?;
    validate_test_evidence_report(&report)?;
    Ok(report)
}

pub fn build_gate_a_capability_matrix(
    quality: &GateAQualityIsolationReportV1,
    resource: &GateAResourceHighWaterReportV1,
    tests: &GateATestEvidenceReportV1,
) -> Result<GateACapabilityMatrixV2, GateAPublicationError> {
    validate_quality_isolation_report(quality)?;
    validate_resource_high_water_report(resource)?;
    validate_test_evidence_report(tests)?;
    if quality.pipeline_core_identity_sha256 != resource.pipeline_core_identity_sha256
        || quality.pipeline_core_identity_sha256 != tests.pipeline_core_identity_sha256
        || quality.input_set_identity_sha256 != resource.input_set_identity_sha256
        || quality.input_manifest_set_sha256 != resource.input_manifest_set_sha256
        || quality.artifact_manifests != resource.artifact_manifests
        || quality.artifact_manifests != tests.artifact_manifests
        || quality.scope != resource.scope
    {
        return Err(GateAPublicationError::Invalid(
            "能力矩阵输入没有绑定同一核心、输入、范围与产物",
        ));
    }
    let mut available = BTreeMap::new();
    for evidence in quality
        .evidence
        .iter()
        .chain(std::iter::once(&resource.evidence))
        .chain(tests.evidence.iter())
    {
        if available
            .insert(evidence.evidence_id.as_str(), evidence)
            .is_some()
        {
            return Err(GateAPublicationError::Invalid("证据 ID 重复"));
        }
    }

    let entries = data_pipeline_core::REQUIRED_GATE_A_CAPABILITIES
        .into_iter()
        .map(|capability| {
            let (field_path, required_evidence) = capability_spec(capability);
            let passed = required_evidence
                .iter()
                .all(|evidence_id| available.get(evidence_id).is_some_and(|item| item.passed));
            GateACapabilityEntryV2 {
                capability,
                status: if passed {
                    CapabilityStatus::Pass
                } else {
                    CapabilityStatus::Fail
                },
                contract_field_paths: vec![field_path.to_owned()],
                evidence_ids: required_evidence
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
                blocking_reason: (!passed)
                    .then(|| format!("{field_path}/required_evidence_failed")),
            }
        })
        .collect::<Vec<_>>();
    let matrix = GateACapabilityMatrixV2 {
        schema_version: GATE_A_CAPABILITY_MATRIX_SCHEMA_V2.to_owned(),
        entries,
    };
    validate_gate_a_capability_matrix(&matrix)
        .map_err(|_| GateAPublicationError::Invalid("能力矩阵不满足公共合同"))?;

    let consumed = matrix
        .entries
        .iter()
        .flat_map(|entry| entry.evidence_ids.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let available = available.keys().copied().collect::<BTreeSet<_>>();
    if consumed != available {
        return Err(GateAPublicationError::Invalid(
            "能力矩阵存在未解析引用或未消费证据",
        ));
    }
    Ok(matrix)
}

fn capability_spec(capability: GateACapabilityId) -> (&'static str, &'static [&'static str]) {
    match capability {
        GateACapabilityId::InputAdmissionAndSixSchemaMapping => (
            "GateAReceiptV3.input_manifest_set_sha256",
            &["quality/input-six-schemas", "test/offline-boundary"],
        ),
        GateACapabilityId::CanonicalIdentityAndDualTimeProjection => (
            "CanonicalEventRecordV1.record",
            &["quality/dual-time-projection", "test/physical-perturbation"],
        ),
        GateACapabilityId::MarketIdentityWindowAndLifecycle => (
            "ExecutionMarketEventV3.market",
            &["quality/market-lifecycle"],
        ),
        GateACapabilityId::InitialL2SnapshotEpochCursorAndDigest => (
            "InitialL2SnapshotV2.source_sequence_epoch_id",
            &["quality/initial-l2", "test/epoch-bba"],
        ),
        GateACapabilityId::PriceLevelQuantityTransition => (
            "LevelExecutionTransitionV3.quantity_after_shares",
            &["quality/price-level-transition"],
        ),
        GateACapabilityId::BestBidAsk => (
            "BestBidAskV2.best_bid_pusd_per_share",
            &["quality/best-bid-ask", "test/epoch-bba"],
        ),
        GateACapabilityId::DynamicTickSize => (
            "TickSizeChangeV2.new_tick_size_pusd_per_share",
            &["quality/dynamic-tick"],
        ),
        GateACapabilityId::PublicTradeEvidence => {
            ("PublicTradeV2.trade_id_source", &["quality/public-trade"])
        }
        GateACapabilityId::LevelExecutionTransition => (
            "LevelExecutionTransitionV3.platform_effective_at_utc_ms",
            &["quality/level-execution"],
        ),
        GateACapabilityId::QualityGapResyncAndWatermark => (
            "EventQualityV2.resync",
            &["quality/gap-resync-watermark", "test/quality-boundaries"],
        ),
        GateACapabilityId::MarketResolutionTerminal => (
            "MarketResolutionTerminalV3.status",
            &["quality/market-terminal", "test/quality-boundaries"],
        ),
        GateACapabilityId::ObservationSharedLineageAndVisibilityCutoff => (
            "MarketObservationCausalShellV3.visibility_cutoff",
            &["quality/observation-lineage", "test/epoch-bba"],
        ),
        GateACapabilityId::ImmutableEventStoreAndDualIndexes => (
            "GateAReceiptV3.canonical_event_store_manifest_sha256",
            &[
                "quality/artifact-closure",
                "test/candidate-corruption",
                "test/workspace-regression",
            ],
        ),
        GateACapabilityId::DeterminismAndSourceTraceability => (
            "ExecutionMarketEventV3.lineage",
            &["quality/source-traceability", "test/physical-perturbation"],
        ),
        GateACapabilityId::BoundedResourcesAndAtomicPublication => (
            "GateAReceiptV3.training_dataset_ready",
            &[
                "resource/formal-run",
                "test/atomic-publication",
                "test/training-entry-rejects-gate-a",
            ],
        ),
        GateACapabilityId::ForbiddenLowFrequencyFields => (
            "ExecutionMarketEventV3.payload",
            &[
                "test/forbidden-low-frequency-fields",
                "test/offline-boundary",
            ],
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_gate_a_run_manifest(
    quality: &GateAQualityIsolationReportV1,
    quality_sha256: Sha256Hex,
    resource: &GateAResourceHighWaterReportV1,
    resource_sha256: Sha256Hex,
    tests: &GateATestEvidenceReportV1,
    tests_sha256: Sha256Hex,
    matrix: &GateACapabilityMatrixV2,
    matrix_file_sha256: Sha256Hex,
    validated: &ValidatedGateACandidate,
) -> Result<GateARunManifestV2, GateAPublicationError> {
    validate_quality_isolation_report(quality)?;
    validate_resource_high_water_report(resource)?;
    validate_test_evidence_report(tests)?;
    let expected_matrix = build_gate_a_capability_matrix(quality, resource, tests)?;
    if matrix != &expected_matrix {
        return Err(GateAPublicationError::Invalid(
            "运行清单能力矩阵不是当前证据的机械判定结果",
        ));
    }
    if matrix
        .entries
        .iter()
        .any(|entry| entry.status != CapabilityStatus::Pass)
    {
        return Err(GateAPublicationError::Invalid(
            "存在未通过能力时不得生成成功运行清单",
        ));
    }
    for digest in [
        &quality_sha256,
        &resource_sha256,
        &tests_sha256,
        &matrix_file_sha256,
    ] {
        require_sha256(digest)?;
    }
    let encoded_quality = encode_quality_isolation_report(quality)?;
    let encoded_resource = encode_resource_high_water_report(resource)?;
    let encoded_tests = encode_test_evidence_report(tests)?;
    let encoded_matrix = encode_gate_a_capability_matrix(matrix)
        .map_err(|_| GateAPublicationError::Invalid("能力矩阵无法编码"))?;
    if Sha256Hex(format!("{:x}", Sha256::digest(&encoded_quality))) != quality_sha256
        || Sha256Hex(format!("{:x}", Sha256::digest(&encoded_resource))) != resource_sha256
        || Sha256Hex(format!("{:x}", Sha256::digest(&encoded_tests))) != tests_sha256
        || Sha256Hex(format!("{:x}", Sha256::digest(&encoded_matrix))) != matrix_file_sha256
        || resource.quality_isolation_report_sha256 != quality_sha256
        || quality.pipeline_core_identity_sha256 != resource.pipeline_core_identity_sha256
        || quality.pipeline_core_identity_sha256 != tests.pipeline_core_identity_sha256
        || quality.artifact_manifests != resource.artifact_manifests
        || quality.artifact_manifests != tests.artifact_manifests
        || !quality_matches_validated_candidate(quality, validated)
        || !resource_matches_validated_candidate(resource, validated)
    {
        return Err(GateAPublicationError::Invalid(
            "运行清单输入报告或能力矩阵绑定不一致",
        ));
    }

    let mut covered_utc_dates = quality
        .selected_files
        .iter()
        .map(|file| file.utc_day.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    covered_utc_dates.sort();
    let mut manifest_by_day = BTreeMap::new();
    for file in &quality.selected_files {
        if manifest_by_day
            .insert(file.utc_day.clone(), file.manifest_sha256.clone())
            .is_some_and(|existing| existing != file.manifest_sha256)
        {
            return Err(GateAPublicationError::Invalid(
                "同一 UTC 日绑定了不同输入 manifest",
            ));
        }
    }
    let input_manifests = manifest_by_day
        .into_iter()
        .map(|(utc_day, sha256)| GateAInputManifestBindingV1 {
            path: format!(
                "{DATASET_LINEAGE_ROOT}/{utc_day}/typed_raw/utc_day={utc_day}/manifest.json"
            ),
            utc_day,
            sha256: Sha256Hex(sha256),
        })
        .collect::<Vec<_>>();

    let typed_raw_scan_policies = expected_typed_raw_scan_policies(
        quality.scope.condition_id(),
        quality.scope.received_start_utc_ms(),
        quality.scope.received_end_utc_ms(),
    )?;
    let manifest = GateARunManifestV2 {
        schema_version: GATE_A_RUN_MANIFEST_SCHEMA_V2.to_owned(),
        requirement_id: "DATA-PIPELINE-TRAINING-20260803".to_owned(),
        git_commit: tests.git_commit.clone(),
        pipeline_core_identity_sha256: quality.pipeline_core_identity_sha256.clone(),
        contract_versions: GateAContractVersionsV1 {
            canonical_event_record: CANONICAL_EVENT_RECORD_SCHEMA_V1.to_owned(),
            execution_market_event: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            canonical_reference_event: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
            market_observation: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3.to_owned(),
            event_time_index: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
            visibility_index: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
            artifact_manifest: GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1.to_owned(),
            reduction_report: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
        },
        canonical_rule_version: validated
            .canonical_event_store
            .manifest()
            .canonical_rule_version
            .clone(),
        dataset_lineage_root: DATASET_LINEAGE_ROOT.to_owned(),
        dataset_date_floor: DATASET_DATE_FLOOR.to_owned(),
        covered_utc_dates,
        condition_id: quality.scope.condition_id().to_owned(),
        received_start_utc_ms: quality.scope.received_start_utc_ms(),
        received_end_utc_ms: quality.scope.received_end_utc_ms(),
        reference_received_start_utc_ms: quality.scope.received_start_utc_ms(),
        reference_received_end_utc_ms: quality.scope.received_end_utc_ms(),
        input_set_identity_sha256: quality.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: quality.input_manifest_set_sha256.clone(),
        supported_schema_set_identity_sha256: supported_schema_set_identity_sha256(),
        supported_schemas: supported_schema_bindings(),
        typed_raw_scan_policies,
        input_manifests,
        selected_files: quality.selected_files.clone(),
        canonical_config: resource.canonical_config.clone(),
        output_config: resource.output_config.clone(),
        host: resource.host.clone(),
        artifact_manifests: quality.artifact_manifests.clone(),
        control_files: GateAControlBindingsV1 {
            quality_isolation_report: control_binding(
                "reports/quality-isolation-report.json",
                quality_sha256,
            ),
            resource_high_water_report: control_binding(
                "reports/resource-evidence.json",
                resource_sha256,
            ),
            test_evidence_report: control_binding("reports/test-evidence.json", tests_sha256),
            capability_matrix: control_binding("gate-a-capability-matrix.json", matrix_file_sha256),
        },
        artifact_rows: GateAArtifactRowCountsV1 {
            canonical_event_store: validated.canonical_event_store.manifest().file.rows,
            event_time_index: validated.event_time_index.manifest().file.rows,
            visibility_index: validated.visibility_index.manifest().file.rows,
            market_observations: validated.market_observations.manifest().file.rows,
            reduction_reports: validated.reduction_reports.manifest().file.rows,
        },
        canonical_digests: validated.recomputed_stream_digests.clone(),
        rejected_reports: quality.reduction_operation_counts.reject,
        suppressed_reports: quality.reduction_disposition_counts.suppressed,
        validation_result: "PASS".to_owned(),
    };
    validate_gate_a_run_manifest(&manifest)?;
    Ok(manifest)
}

fn control_binding(relative_path: &str, sha256: Sha256Hex) -> GateAControlFileBindingV1 {
    GateAControlFileBindingV1 {
        relative_path: relative_path.to_owned(),
        sha256,
    }
}

fn quality_matches_validated_candidate(
    quality: &GateAQualityIsolationReportV1,
    validated: &ValidatedGateACandidate,
) -> bool {
    let store = validated.canonical_event_store.manifest();
    quality.artifact_manifests
        == GateAArtifactManifestBindingsV1 {
            canonical_event_store: validated.canonical_event_store.manifest_sha256().clone(),
            event_time_index: validated.event_time_index.manifest_sha256().clone(),
            visibility_index: validated.visibility_index.manifest_sha256().clone(),
            market_observations: validated.market_observations.manifest_sha256().clone(),
            reduction_reports: validated.reduction_reports.manifest_sha256().clone(),
        }
        && quality.artifact_rows
            == GateAArtifactRowCountsV1 {
                canonical_event_store: store.file.rows,
                event_time_index: validated.event_time_index.manifest().file.rows,
                visibility_index: validated.visibility_index.manifest().file.rows,
                market_observations: validated.market_observations.manifest().file.rows,
                reduction_reports: validated.reduction_reports.manifest().file.rows,
            }
        && quality.input_set_identity_sha256 == store.input_set_identity_sha256
        && quality.input_manifest_set_sha256 == store.input_manifest_set_sha256
        && store.covered_condition_ids == [quality.scope.condition_id()]
        && store.received_start_utc_ms == quality.scope.received_start_utc_ms()
        && store.received_end_utc_ms == quality.scope.received_end_utc_ms()
}

fn resource_matches_validated_candidate(
    resource: &GateAResourceHighWaterReportV1,
    validated: &ValidatedGateACandidate,
) -> bool {
    resource.artifact_manifests
        == GateAArtifactManifestBindingsV1 {
            canonical_event_store: validated.canonical_event_store.manifest_sha256().clone(),
            event_time_index: validated.event_time_index.manifest_sha256().clone(),
            visibility_index: validated.visibility_index.manifest_sha256().clone(),
            market_observations: validated.market_observations.manifest_sha256().clone(),
            reduction_reports: validated.reduction_reports.manifest_sha256().clone(),
        }
        && resource.canonical_metrics.canonical_records
            == validated.canonical_event_store.manifest().file.rows
        && resource.canonical_metrics.event_time_projections
            == validated.event_time_index.manifest().file.rows
        && resource.canonical_metrics.visibility_projections
            == validated.visibility_index.manifest().file.rows
        && resource.canonical_metrics.reduction_reports
            == validated.reduction_reports.manifest().file.rows
        && resource.build_metrics.peak_observation_l2_levels == validated.peak_observation_l2_levels
        && formal_work_directory_bound_covers(
            resource.build_metrics.peak_candidate_directory_bytes_bound,
            resource
                .build_metrics
                .peak_formal_work_directory_bytes_bound,
            Some(validated.peak_formal_work_directory_bytes_bound),
        )
}

fn supported_schema_bindings() -> Vec<String> {
    [
        "binance_aggtrade",
        "binance_depth_snapshot",
        "binance_depth_update",
        "polymarket_market",
        "polymarket_market_metadata",
        "polymarket_rtds_chainlink_btc_usd",
    ]
    .into_iter()
    .map(|source| {
        format!(
            "{source}={}",
            crate::supported_schema_for_source_kind(source)
                .expect("固定六类来源必须都有受支持 schema")
        )
    })
    .collect()
}

fn expected_typed_raw_scan_policies(
    condition_id: &str,
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
) -> Result<Vec<GateATypedRawScanPolicyV1>, GateAPublicationError> {
    supported_schema_bindings()
        .into_iter()
        .map(|binding| {
            let (source_kind, schema_version) =
                binding
                    .split_once('=')
                    .ok_or(GateAPublicationError::Invalid(
                        "固定 typed_raw schema 绑定无效",
                    ))?;
            let condition_id_column = match source_kind {
                "polymarket_market" => Some("market"),
                "polymarket_market_metadata" => Some("condition_id"),
                _ => None,
            };
            let projected_columns =
                crate::typed_raw::expected_projection_columns(source_kind, schema_version)
                    .map_err(|_| {
                        GateAPublicationError::Invalid("固定 typed_raw schema 无法生成完整投影列")
                    })?;
            Ok(GateATypedRawScanPolicyV1 {
                source_kind: source_kind.to_owned(),
                schema_version: schema_version.to_owned(),
                received_time_column: "timestamp_received".to_owned(),
                received_start_inclusive_utc_ms: received_start_utc_ms,
                received_end_exclusive_utc_ms: received_end_utc_ms,
                condition_id_column: condition_id_column.map(str::to_owned),
                condition_id_equals: condition_id_column.map(|_| condition_id.to_owned()),
                projected_columns,
                skipped_columns: Vec::new(),
            })
        })
        .collect()
}

fn validate_typed_raw_scan_policies(
    policies: &[GateATypedRawScanPolicyV1],
    condition_id: &str,
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
) -> Result<(), GateAPublicationError> {
    let expected =
        expected_typed_raw_scan_policies(condition_id, received_start_utc_ms, received_end_utc_ms);
    match expected {
        Ok(expected) if expected == policies => Ok(()),
        _ => Err(GateAPublicationError::Invalid(
            "运行清单 typed_raw 扫描策略无效",
        )),
    }
}

pub fn validate_gate_a_run_manifest(
    manifest: &GateARunManifestV2,
) -> Result<(), GateAPublicationError> {
    validate_typed_raw_scan_policies(
        &manifest.typed_raw_scan_policies,
        &manifest.condition_id,
        manifest.received_start_utc_ms,
        manifest.received_end_utc_ms,
    )?;
    let expected_contracts = GateAContractVersionsV1 {
        canonical_event_record: CANONICAL_EVENT_RECORD_SCHEMA_V1.to_owned(),
        execution_market_event: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        canonical_reference_event: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        market_observation: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3.to_owned(),
        event_time_index: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
        visibility_index: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        artifact_manifest: GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1.to_owned(),
        reduction_report: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
    };
    if manifest.schema_version != GATE_A_RUN_MANIFEST_SCHEMA_V2
        || manifest.requirement_id != "DATA-PIPELINE-TRAINING-20260803"
        || manifest.git_commit.len() != 40
        || !manifest
            .git_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || manifest.contract_versions != expected_contracts
        || manifest.canonical_rule_version.trim().is_empty()
        || manifest.dataset_lineage_root != DATASET_LINEAGE_ROOT
        || manifest.dataset_date_floor != DATASET_DATE_FLOOR
        || manifest.covered_utc_dates.is_empty()
        || manifest
            .covered_utc_dates
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || manifest
            .covered_utc_dates
            .iter()
            .any(|date| date.as_str() < DATASET_DATE_FLOOR)
        || manifest.condition_id.trim().is_empty()
        || manifest.received_start_utc_ms <= 0
        || manifest.received_end_utc_ms <= manifest.received_start_utc_ms
        || manifest.reference_received_start_utc_ms != manifest.received_start_utc_ms
        || manifest.reference_received_end_utc_ms != manifest.received_end_utc_ms
        || manifest.supported_schemas != supported_schema_bindings()
        || manifest.canonical_config.canonical_rule_version != manifest.canonical_rule_version
        || manifest.validation_result != "PASS"
    {
        return Err(GateAPublicationError::Invalid(
            "运行清单固定身份、范围、合同或结论无效",
        ));
    }
    for digest in [
        &manifest.pipeline_core_identity_sha256,
        &manifest.input_set_identity_sha256,
        &manifest.input_manifest_set_sha256,
        &manifest.supported_schema_set_identity_sha256,
        &manifest.artifact_manifests.canonical_event_store,
        &manifest.artifact_manifests.event_time_index,
        &manifest.artifact_manifests.visibility_index,
        &manifest.artifact_manifests.market_observations,
        &manifest.artifact_manifests.reduction_reports,
        &manifest.control_files.quality_isolation_report.sha256,
        &manifest.control_files.resource_high_water_report.sha256,
        &manifest.control_files.test_evidence_report.sha256,
        &manifest.control_files.capability_matrix.sha256,
    ] {
        require_sha256(digest)?;
    }
    for digest in [
        &manifest.canonical_digests.canonical_records_sha256,
        &manifest.canonical_digests.event_time_projections_sha256,
        &manifest.canonical_digests.visibility_projections_sha256,
        &manifest.canonical_digests.reduction_reports_sha256,
    ] {
        require_sha256(&Sha256Hex(digest.clone()))?;
    }
    if manifest.supported_schema_set_identity_sha256 != supported_schema_set_identity_sha256()
        || manifest
            .control_files
            .quality_isolation_report
            .relative_path
            != "reports/quality-isolation-report.json"
        || manifest
            .control_files
            .resource_high_water_report
            .relative_path
            != "reports/resource-evidence.json"
        || manifest.control_files.test_evidence_report.relative_path != "reports/test-evidence.json"
        || manifest.control_files.capability_matrix.relative_path != "gate-a-capability-matrix.json"
        || manifest.artifact_rows.canonical_event_store == 0
        || manifest.artifact_rows.event_time_index == 0
        || manifest.artifact_rows.visibility_index != manifest.artifact_rows.canonical_event_store
        || manifest.artifact_rows.market_observations == 0
        || manifest.artifact_rows.reduction_reports == 0
        || manifest.selected_files.is_empty()
    {
        return Err(GateAPublicationError::Invalid(
            "运行清单 schema 集合、控制文件或产物行数无效",
        ));
    }

    let selected_dates = manifest
        .selected_files
        .iter()
        .map(|file| file.utc_day.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    validate_selected_files(&manifest.selected_files)?;
    if selected_dates != manifest.covered_utc_dates
        || manifest.input_manifests.len() != manifest.covered_utc_dates.len()
    {
        return Err(GateAPublicationError::Invalid(
            "运行清单覆盖日期与选中文件不一致",
        ));
    }
    for (binding, date) in manifest
        .input_manifests
        .iter()
        .zip(&manifest.covered_utc_dates)
    {
        let expected_path =
            format!("{DATASET_LINEAGE_ROOT}/{date}/typed_raw/utc_day={date}/manifest.json");
        let matching_files = manifest
            .selected_files
            .iter()
            .filter(|file| file.utc_day == *date)
            .collect::<Vec<_>>();
        if binding.utc_day != *date
            || binding.path != expected_path
            || matching_files.is_empty()
            || matching_files
                .iter()
                .any(|file| file.manifest_sha256 != binding.sha256.0)
        {
            return Err(GateAPublicationError::Invalid(
                "运行清单输入 manifest 与选中文件不一致",
            ));
        }
        require_sha256(&binding.sha256)?;
    }
    Ok(())
}

pub fn encode_gate_a_run_manifest(
    manifest: &GateARunManifestV2,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_gate_a_run_manifest(manifest)?;
    encode_bounded_json(manifest, 16 * 1024 * 1024, "运行清单超过 16 MiB 上限")
}

pub fn decode_gate_a_run_manifest(
    bytes: &[u8],
) -> Result<GateARunManifestV2, GateAPublicationError> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(GateAPublicationError::Invalid("运行清单超过 16 MiB 上限"));
    }
    let manifest = serde_json::from_slice(bytes)?;
    validate_gate_a_run_manifest(&manifest)?;
    Ok(manifest)
}

pub fn build_gate_a_receipt(
    run_manifest: &GateARunManifestV2,
    run_manifest_sha256: Sha256Hex,
    matrix: &GateACapabilityMatrixV2,
    matrix_file_sha256: Sha256Hex,
) -> Result<GateAReceiptV3, GateAPublicationError> {
    validate_gate_a_run_manifest(run_manifest)?;
    validate_gate_a_capability_matrix(matrix)
        .map_err(|_| GateAPublicationError::Invalid("回执能力矩阵无效"))?;
    require_sha256(&run_manifest_sha256)?;
    require_sha256(&matrix_file_sha256)?;
    let encoded_run_manifest = encode_gate_a_run_manifest(run_manifest)?;
    let encoded_matrix = encode_gate_a_capability_matrix(matrix)
        .map_err(|_| GateAPublicationError::Invalid("能力矩阵无法编码"))?;
    let matrix_contract_digest = gate_a_capability_matrix_digest(matrix)
        .map_err(|_| GateAPublicationError::Invalid("能力矩阵合同摘要无法计算"))?;
    if matrix
        .entries
        .iter()
        .any(|entry| entry.status != CapabilityStatus::Pass)
        || run_manifest.control_files.capability_matrix.sha256 != matrix_file_sha256
        || Sha256Hex(format!("{:x}", Sha256::digest(&encoded_matrix))) != matrix_file_sha256
        || Sha256Hex(format!("{:x}", Sha256::digest(&encoded_run_manifest))) != run_manifest_sha256
    {
        return Err(GateAPublicationError::Invalid(
            "未全通过或摘要不一致的能力矩阵不得签发回执",
        ));
    }
    let mut receipt = GateAReceiptV3 {
        schema_version: GATE_A_RECEIPT_SCHEMA_V3.to_owned(),
        receipt_id: Sha256Hex("0".repeat(64)),
        requirement_id: run_manifest.requirement_id.clone(),
        pipeline_core_identity_sha256: run_manifest.pipeline_core_identity_sha256.clone(),
        canonical_event_record_contract_version: CANONICAL_EVENT_RECORD_SCHEMA_V1.to_owned(),
        execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        reference_event_contract_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
        observation_contract_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3.to_owned(),
        event_time_index_contract_version: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
        visibility_index_contract_version: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        dataset_lineage_root: DATASET_LINEAGE_ROOT.to_owned(),
        dataset_date_floor: DATASET_DATE_FLOOR.to_owned(),
        covered_utc_dates: run_manifest.covered_utc_dates.clone(),
        input_set_identity_sha256: run_manifest.input_set_identity_sha256.clone(),
        supported_schema_set_identity_sha256: run_manifest
            .supported_schema_set_identity_sha256
            .clone(),
        input_manifest_set_sha256: run_manifest.input_manifest_set_sha256.clone(),
        canonical_event_store_manifest_sha256: run_manifest
            .artifact_manifests
            .canonical_event_store
            .clone(),
        event_time_index_manifest_sha256: run_manifest.artifact_manifests.event_time_index.clone(),
        visibility_index_manifest_sha256: run_manifest.artifact_manifests.visibility_index.clone(),
        observation_manifest_sha256: run_manifest.artifact_manifests.market_observations.clone(),
        capability_matrix_sha256: matrix_contract_digest,
        run_manifest_sha256,
        training_dataset_ready: false,
        training_split_manifest_id: "N/A".to_owned(),
        training_split_manifest_sha256: "N/A".to_owned(),
    };
    receipt.receipt_id = derive_gate_a_receipt_v3_id(&receipt)
        .map_err(|_| GateAPublicationError::Invalid("回执身份无法派生"))?;
    encode_gate_a_receipt_v3(&receipt, matrix)
        .map_err(|_| GateAPublicationError::Invalid("回执不满足公共合同"))?;
    Ok(receipt)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedGateA {
    pub root: PathBuf,
    pub receipt: GateAReceiptV3,
}

/// 已对不可变 Gate A final 执行完整机械闭包复核的只读句柄。
#[derive(Debug)]
pub struct ValidatedPublishedGateAFinal {
    root: PathBuf,
    receipt: GateAReceiptV3,
    run_manifest: GateARunManifestV2,
    producer_pipeline_core_identity_sha256: Sha256Hex,
    reader_pipeline_core_identity_sha256: Sha256Hex,
    candidate: ValidatedGateACandidate,
}

impl ValidatedPublishedGateAFinal {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn receipt(&self) -> &GateAReceiptV3 {
        &self.receipt
    }

    pub fn run_manifest(&self) -> &GateARunManifestV2 {
        &self.run_manifest
    }

    pub fn producer_pipeline_core_identity_sha256(&self) -> &Sha256Hex {
        &self.producer_pipeline_core_identity_sha256
    }

    pub fn reader_pipeline_core_identity_sha256(&self) -> &Sha256Hex {
        &self.reader_pipeline_core_identity_sha256
    }

    pub fn candidate(&self) -> &ValidatedGateACandidate {
        &self.candidate
    }
}

/// 只读打开并完整复核一个已经发布的 Gate A final。
pub fn open_published_gate_a_final(
    root: &Path,
    validation_work_path: &Path,
) -> Result<ValidatedPublishedGateAFinal, GateAPublicationError> {
    require_final_layout(root)?;
    let canonical_root = fs::canonicalize(root)?;
    let validation_work_path =
        require_external_validation_work_path(&canonical_root, validation_work_path)?;
    let quality_bytes = read_regular_file(
        &canonical_root.join("reports/quality-isolation-report.json"),
        4 * 1024 * 1024,
    )?;
    let resource_bytes = read_regular_file(
        &canonical_root.join("reports/resource-evidence.json"),
        4 * 1024 * 1024,
    )?;
    let tests_bytes = read_regular_file(
        &canonical_root.join("reports/test-evidence.json"),
        16 * 1024 * 1024,
    )?;
    let matrix_bytes = read_regular_file(
        &canonical_root.join("gate-a-capability-matrix.json"),
        1024 * 1024,
    )?;
    let run_manifest_bytes =
        read_regular_file(&canonical_root.join("run-manifest.json"), 16 * 1024 * 1024)?;
    let receipt_bytes =
        read_regular_file(&canonical_root.join("gate-a-receipt.json"), 1024 * 1024)?;

    let matrix = decode_gate_a_capability_matrix(&matrix_bytes)
        .map_err(|_| GateAPublicationError::Invalid("落盘能力矩阵无法解码"))?;
    let run_manifest = decode_gate_a_run_manifest(&run_manifest_bytes)?;
    let receipt = decode_gate_a_receipt_v3(&receipt_bytes, &matrix)
        .map_err(|_| GateAPublicationError::Invalid("落盘门 A 回执无法解码"))?;
    if canonical_root.file_name().and_then(|value| value.to_str())
        != Some(receipt.receipt_id.0.as_str())
    {
        return Err(GateAPublicationError::Invalid(
            "发布 final 目录名与回执 ID 不一致",
        ));
    }
    let producer_pipeline_core_identity_sha256 = receipt.pipeline_core_identity_sha256.clone();
    let candidate = verify_complete_publication(
        &canonical_root,
        &validation_work_path,
        &quality_bytes,
        &resource_bytes,
        &tests_bytes,
        &matrix_bytes,
        &run_manifest_bytes,
        &receipt_bytes,
        &producer_pipeline_core_identity_sha256,
    )?;

    Ok(ValidatedPublishedGateAFinal {
        root: canonical_root,
        receipt,
        run_manifest,
        producer_pipeline_core_identity_sha256,
        reader_pipeline_core_identity_sha256: pipeline_core_identity_sha256(),
        candidate,
    })
}

fn require_external_validation_work_path(
    canonical_final_root: &Path,
    validation_work_path: &Path,
) -> Result<PathBuf, GateAPublicationError> {
    if !validation_work_path.is_absolute() {
        return Err(GateAPublicationError::Invalid(
            "发布 final 的验证工作目录必须是绝对路径",
        ));
    }
    let parent = validation_work_path
        .parent()
        .ok_or(GateAPublicationError::Invalid(
            "发布 final 的验证工作目录必须具有父目录",
        ))?;
    let name = validation_work_path
        .file_name()
        .ok_or(GateAPublicationError::Invalid(
            "发布 final 的验证工作目录必须具有目录名",
        ))?;
    let canonical_path = fs::canonicalize(parent)?.join(name);
    if canonical_path.starts_with(canonical_final_root) {
        return Err(GateAPublicationError::Invalid(
            "发布 final 的验证工作目录必须位于 final 外部",
        ));
    }
    Ok(canonical_path)
}

struct PublicationCandidateGuard {
    path: PathBuf,
    retain: bool,
}

impl PublicationCandidateGuard {
    fn acquire(path: &Path) -> Result<Self, GateAPublicationError> {
        if !path.is_absolute() || path.parent().is_none() {
            return Err(GateAPublicationError::Invalid(
                "发布候选必须是具有明确父目录的绝对路径",
            ));
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(GateAPublicationError::Invalid("发布候选目录名必须是 UTF-8"))?;
        if !name.starts_with('.') || !name.ends_with(".candidate") {
            return Err(GateAPublicationError::Invalid(
                "发布候选目录名必须使用隐藏 candidate 后缀",
            ));
        }
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(GateAPublicationError::Invalid(
                "发布候选必须是普通目录且不得为符号链接",
            ));
        }
        let parent = path.parent().expect("前面已检查父目录");
        let parent_metadata = fs::symlink_metadata(parent)?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
            return Err(GateAPublicationError::Invalid(
                "发布候选父路径必须是普通目录且不得为符号链接",
            ));
        }
        let canonical_parent = fs::canonicalize(parent)?;
        let canonical_path = fs::canonicalize(path)?;
        if canonical_path.parent() != Some(canonical_parent.as_path()) {
            return Err(GateAPublicationError::Invalid(
                "发布候选必须是其父目录的直接子目录",
            ));
        }
        Ok(Self {
            path: canonical_path,
            retain: false,
        })
    }
}

impl Drop for PublicationCandidateGuard {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn reconstructed_candidate(
    quality: &GateAQualityIsolationReportV1,
    resource: &GateAResourceHighWaterReportV1,
    validated: &ValidatedGateACandidate,
) -> GateACandidate {
    let descriptor = |artifact: &crate::gate_a::ValidatedGateAArtifact,
                      sort_metrics: &SortMetrics| GateAArtifactDescriptor {
        manifest_path: artifact.manifest_path().to_path_buf(),
        manifest_sha256: artifact.manifest_sha256().clone(),
        manifest: artifact.manifest().clone(),
        sort_metrics: sort_metrics.clone(),
    };
    GateACandidate {
        candidate_path: validated.root.clone(),
        input_set_identity_sha256: quality.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: quality.input_manifest_set_sha256.clone(),
        scope: quality.scope.clone(),
        config: resource.canonical_config.clone(),
        output_config: resource.output_config.clone(),
        selected_files: quality.selected_files.clone(),
        canonical_digests: validated.recomputed_stream_digests.clone(),
        canonical_metrics: resource.canonical_metrics.clone(),
        canonical_event_store: descriptor(
            &validated.canonical_event_store,
            &resource.artifact_write_metrics.canonical_event_store,
        ),
        event_time_index: descriptor(
            &validated.event_time_index,
            &resource.artifact_write_metrics.event_time_index,
        ),
        visibility_index: descriptor(
            &validated.visibility_index,
            &resource.artifact_write_metrics.visibility_index,
        ),
        market_observations: descriptor(
            &validated.market_observations,
            &resource.artifact_write_metrics.market_observations,
        ),
        reduction_reports: descriptor(
            &validated.reduction_reports,
            &resource.artifact_write_metrics.reduction_reports,
        ),
        resource_metrics: resource.build_metrics.clone(),
    }
}

fn read_regular_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, GateAPublicationError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(GateAPublicationError::Invalid(
            "发布控制文件必须是普通文件且不得为符号链接",
        ));
    }
    if metadata.len() > max_bytes {
        return Err(GateAPublicationError::Invalid("发布控制文件超过字节上限"));
    }
    Ok(fs::read(path)?)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), GateAPublicationError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sha256_bytes_value(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

fn directory_entry_names(path: &Path) -> Result<BTreeSet<String>, GateAPublicationError> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| GateAPublicationError::Invalid("发布目录项名称必须是 UTF-8"))?;
        if !names.insert(name) {
            return Err(GateAPublicationError::Invalid("发布目录项名称重复"));
        }
    }
    Ok(names)
}

fn require_direct_directory(
    parent: &Path,
    name: &'static str,
) -> Result<PathBuf, GateAPublicationError> {
    let path = parent.join(name);
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(GateAPublicationError::Invalid(
            "发布候选的直接子目录必须是普通目录且不得为符号链接",
        ));
    }
    let canonical_parent = fs::canonicalize(parent)?;
    let canonical_path = fs::canonicalize(&path)?;
    if canonical_path.parent() != Some(canonical_parent.as_path()) {
        return Err(GateAPublicationError::Invalid(
            "发布候选目录必须保持为候选根的直接子目录",
        ));
    }
    Ok(canonical_path)
}

fn require_prepublication_layout(stage: &Path) -> Result<(), GateAPublicationError> {
    let expected_root = ["artifacts", "reports"]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let expected_reports = ["quality-isolation-report.json", "resource-evidence.json"]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    require_direct_directory(stage, "artifacts")?;
    let reports = require_direct_directory(stage, "reports")?;
    if directory_entry_names(stage)? != expected_root
        || directory_entry_names(&reports)? != expected_reports
    {
        return Err(GateAPublicationError::Invalid(
            "发布候选初始布局不是冻结的 artifacts 加两份报告",
        ));
    }
    Ok(())
}

fn require_final_layout(stage: &Path) -> Result<(), GateAPublicationError> {
    let expected_root = [
        "artifacts",
        "reports",
        "gate-a-capability-matrix.json",
        "run-manifest.json",
        "gate-a-receipt.json",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let expected_reports = [
        "quality-isolation-report.json",
        "resource-evidence.json",
        "test-evidence.json",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    require_direct_directory(stage, "artifacts")?;
    let reports = require_direct_directory(stage, "reports")?;
    if directory_entry_names(stage)? != expected_root
        || directory_entry_names(&reports)? != expected_reports
    {
        return Err(GateAPublicationError::Invalid(
            "发布候选最终布局偏离冻结集合",
        ));
    }
    Ok(())
}

pub fn regular_directory_bytes(path: &Path) -> Result<u64, GateAPublicationError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(GateAPublicationError::Invalid(
            "字节统计根必须是普通目录且不得为符号链接",
        ));
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(GateAPublicationError::Invalid("发布目录不得包含符号链接"));
        }
        if metadata.is_dir() {
            total = total
                .checked_add(regular_directory_bytes(&entry.path())?)
                .ok_or(GateAPublicationError::Invalid("发布目录字节计数溢出"))?;
        } else if metadata.is_file() {
            total = total
                .checked_add(metadata.len())
                .ok_or(GateAPublicationError::Invalid("发布目录字节计数溢出"))?;
        } else {
            return Err(GateAPublicationError::Invalid("发布目录不得包含特殊文件"));
        }
    }
    Ok(total)
}

fn sync_tree(path: &Path) -> Result<(), GateAPublicationError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(GateAPublicationError::Invalid("同步发布目录时发现符号链接"));
        }
        if metadata.is_dir() {
            sync_tree(&entry.path())?;
        } else if metadata.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            return Err(GateAPublicationError::Invalid("同步发布目录时发现特殊文件"));
        }
    }
    File::open(path)?.sync_all()?;
    Ok(())
}

fn commit_candidate_no_replace(
    mut guard: PublicationCandidateGuard,
    target_name: &str,
) -> Result<PathBuf, GateAPublicationError> {
    let stage = guard.path.clone();
    let parent = stage
        .parent()
        .ok_or(GateAPublicationError::Invalid("发布候选缺少父目录"))?;
    let stage_name = stage
        .file_name()
        .ok_or(GateAPublicationError::Invalid("发布候选缺少目录名"))?;
    let target = parent.join(target_name);
    let parent_file = File::open(parent)?;
    renameat_with(
        &parent_file,
        stage_name,
        &parent_file,
        target_name,
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))?;
    guard.path = target.clone();
    parent_file.sync_all()?;
    guard.retain = true;
    Ok(target)
}

/// 完整重验候选、最后写回执，并以一次禁止覆盖的同父目录重命名提交成功运行。
pub fn finalize_and_publish_gate_a_candidate(
    stage_path: &Path,
    tests: &GateATestEvidenceReportV1,
) -> Result<PublishedGateA, GateAPublicationError> {
    finalize_and_publish_gate_a_candidate_with_identity(
        stage_path,
        tests,
        &pipeline_core_identity_sha256(),
    )
}

fn finalize_and_publish_gate_a_candidate_with_identity(
    stage_path: &Path,
    tests: &GateATestEvidenceReportV1,
    producer_pipeline_core_identity_sha256: &Sha256Hex,
) -> Result<PublishedGateA, GateAPublicationError> {
    let guard = PublicationCandidateGuard::acquire(stage_path)?;
    let stage = guard.path.clone();
    require_prepublication_layout(&stage)?;

    let quality_path = stage.join("reports/quality-isolation-report.json");
    let resource_path = stage.join("reports/resource-evidence.json");
    let quality_bytes = read_regular_file(&quality_path, 4 * 1024 * 1024)?;
    let resource_bytes = read_regular_file(&resource_path, 4 * 1024 * 1024)?;
    let quality = decode_quality_isolation_report(&quality_bytes)?;
    let resource = decode_resource_high_water_report(&resource_bytes)?;
    let quality_sha256 = sha256_bytes_value(&quality_bytes);
    let resource_sha256 = sha256_bytes_value(&resource_bytes);
    if resource.quality_isolation_report_sha256 != quality_sha256
        || quality.pipeline_core_identity_sha256 != resource.pipeline_core_identity_sha256
        || quality.input_set_identity_sha256 != resource.input_set_identity_sha256
        || quality.input_manifest_set_sha256 != resource.input_manifest_set_sha256
        || quality.scope != resource.scope
        || quality.artifact_manifests != resource.artifact_manifests
        || tests.pipeline_core_identity_sha256 != quality.pipeline_core_identity_sha256
        || tests.artifact_manifests != quality.artifact_manifests
    {
        return Err(GateAPublicationError::Invalid(
            "候选报告、测试证据、输入、范围或产物绑定不一致",
        ));
    }

    let artifacts_path = stage.join("artifacts");
    let validated = validate_gate_a_candidate(
        &artifacts_path,
        &stage.join(".validation"),
        &resource.output_config,
    )?;
    if !quality_matches_validated_candidate(&quality, &validated)
        || !resource_matches_validated_candidate(&resource, &validated)
        || regular_directory_bytes(&artifacts_path)? != resource.final_artifact_directory_bytes
    {
        return Err(GateAPublicationError::Invalid(
            "报告事实与发布时重新验证的物理候选不一致",
        ));
    }
    let candidate = reconstructed_candidate(&quality, &resource, &validated);
    let regenerated_quality = build_quality_isolation_report_with_identity(
        &candidate,
        &validated,
        producer_pipeline_core_identity_sha256,
    )?;
    if encode_quality_isolation_report(&regenerated_quality)? != quality_bytes {
        return Err(GateAPublicationError::Invalid(
            "质量报告无法从已验证产物机械重建",
        ));
    }

    let tests_bytes = encode_test_evidence_report(tests)?;
    let tests_sha256 = sha256_bytes_value(&tests_bytes);
    write_new_synced(&stage.join("reports/test-evidence.json"), &tests_bytes)?;

    let matrix = build_gate_a_capability_matrix(&quality, &resource, tests)?;
    if matrix
        .entries
        .iter()
        .any(|entry| entry.status != CapabilityStatus::Pass)
    {
        return Err(GateAPublicationError::Invalid(
            "存在未通过能力时不得发布门 A",
        ));
    }
    let matrix_bytes = encode_gate_a_capability_matrix(&matrix)
        .map_err(|_| GateAPublicationError::Invalid("能力矩阵无法编码"))?;
    let matrix_file_sha256 = sha256_bytes_value(&matrix_bytes);
    write_new_synced(&stage.join("gate-a-capability-matrix.json"), &matrix_bytes)?;

    let run_manifest = build_gate_a_run_manifest(
        &quality,
        quality_sha256,
        &resource,
        resource_sha256,
        tests,
        tests_sha256,
        &matrix,
        matrix_file_sha256.clone(),
        &validated,
    )?;
    let run_manifest_bytes = encode_gate_a_run_manifest(&run_manifest)?;
    let run_manifest_sha256 = sha256_bytes_value(&run_manifest_bytes);
    write_new_synced(&stage.join("run-manifest.json"), &run_manifest_bytes)?;

    let receipt = build_gate_a_receipt(
        &run_manifest,
        run_manifest_sha256,
        &matrix,
        matrix_file_sha256,
    )?;
    let receipt_bytes = encode_gate_a_receipt_v3(&receipt, &matrix)
        .map_err(|_| GateAPublicationError::Invalid("门 A 回执无法编码"))?;
    write_new_synced(&stage.join("gate-a-receipt.json"), &receipt_bytes)?;

    require_final_layout(&stage)?;
    verify_complete_publication(
        &stage,
        &stage.join(".final-validation"),
        &quality_bytes,
        &resource_bytes,
        &tests_bytes,
        &matrix_bytes,
        &run_manifest_bytes,
        &receipt_bytes,
        producer_pipeline_core_identity_sha256,
    )?;
    sync_tree(&stage)?;

    let target_name = receipt.receipt_id.0.as_str();
    let target = commit_candidate_no_replace(guard, target_name)?;
    Ok(PublishedGateA {
        root: target,
        receipt,
    })
}

#[allow(clippy::too_many_arguments)]
fn verify_complete_publication(
    stage: &Path,
    validation_work_path: &Path,
    expected_quality_bytes: &[u8],
    expected_resource_bytes: &[u8],
    expected_tests_bytes: &[u8],
    expected_matrix_bytes: &[u8],
    expected_run_manifest_bytes: &[u8],
    expected_receipt_bytes: &[u8],
    producer_pipeline_core_identity_sha256: &Sha256Hex,
) -> Result<ValidatedGateACandidate, GateAPublicationError> {
    let quality_bytes = read_regular_file(
        &stage.join("reports/quality-isolation-report.json"),
        4 * 1024 * 1024,
    )?;
    let resource_bytes = read_regular_file(
        &stage.join("reports/resource-evidence.json"),
        4 * 1024 * 1024,
    )?;
    let tests_bytes =
        read_regular_file(&stage.join("reports/test-evidence.json"), 16 * 1024 * 1024)?;
    let matrix_bytes =
        read_regular_file(&stage.join("gate-a-capability-matrix.json"), 1024 * 1024)?;
    let run_manifest_bytes = read_regular_file(&stage.join("run-manifest.json"), 16 * 1024 * 1024)?;
    let receipt_bytes = read_regular_file(&stage.join("gate-a-receipt.json"), 1024 * 1024)?;
    if quality_bytes != expected_quality_bytes
        || resource_bytes != expected_resource_bytes
        || tests_bytes != expected_tests_bytes
        || matrix_bytes != expected_matrix_bytes
        || run_manifest_bytes != expected_run_manifest_bytes
        || receipt_bytes != expected_receipt_bytes
    {
        return Err(GateAPublicationError::Invalid(
            "发布控制文件在写入后发生变化",
        ));
    }

    let quality = decode_quality_isolation_report(&quality_bytes)?;
    let resource = decode_resource_high_water_report(&resource_bytes)?;
    let tests = decode_test_evidence_report(&tests_bytes)?;
    let matrix = decode_gate_a_capability_matrix(&matrix_bytes)
        .map_err(|_| GateAPublicationError::Invalid("落盘能力矩阵无法解码"))?;
    let run_manifest = decode_gate_a_run_manifest(&run_manifest_bytes)?;
    let receipt = decode_gate_a_receipt_v3(&receipt_bytes, &matrix)
        .map_err(|_| GateAPublicationError::Invalid("落盘门 A 回执无法解码"))?;

    let artifacts_path = stage.join("artifacts");
    let validated = validate_gate_a_candidate(
        &artifacts_path,
        validation_work_path,
        &resource.output_config,
    )?;
    let candidate = reconstructed_candidate(&quality, &resource, &validated);
    if encode_quality_isolation_report(&build_quality_isolation_report_with_identity(
        &candidate,
        &validated,
        producer_pipeline_core_identity_sha256,
    )?)? != quality_bytes
        || regular_directory_bytes(&artifacts_path)? != resource.final_artifact_directory_bytes
    {
        return Err(GateAPublicationError::Invalid(
            "最终复核无法从五份物理产物重建同一质量或资源事实",
        ));
    }
    let rebuilt_matrix = build_gate_a_capability_matrix(&quality, &resource, &tests)?;
    if rebuilt_matrix != matrix {
        return Err(GateAPublicationError::Invalid(
            "最终能力矩阵不是报告证据的机械判定结果",
        ));
    }
    let rebuilt_run_manifest = build_gate_a_run_manifest(
        &quality,
        sha256_bytes_value(&quality_bytes),
        &resource,
        sha256_bytes_value(&resource_bytes),
        &tests,
        sha256_bytes_value(&tests_bytes),
        &matrix,
        sha256_bytes_value(&matrix_bytes),
        &validated,
    )?;
    if rebuilt_run_manifest != run_manifest {
        return Err(GateAPublicationError::Invalid(
            "最终运行清单无法从同一产物与报告机械重建",
        ));
    }
    let rebuilt_receipt = build_gate_a_receipt(
        &run_manifest,
        sha256_bytes_value(&run_manifest_bytes),
        &matrix,
        sha256_bytes_value(&matrix_bytes),
    )?;
    if rebuilt_receipt != receipt {
        return Err(GateAPublicationError::Invalid(
            "最终门 A 回执无法从同一运行清单机械重建",
        ));
    }
    require_final_layout(stage)?;
    Ok(validated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    struct PublishedFinalFixture {
        _candidate: crate::gate_a::tests::MicroCandidate,
        root: PathBuf,
        producer_pipeline_core_identity_sha256: Sha256Hex,
    }

    fn immutable_file_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, Sha256Hex)> {
        fn visit(root: &Path, path: &Path, snapshot: &mut BTreeMap<PathBuf, (u64, Sha256Hex)>) {
            let mut entries = fs::read_dir(path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path).unwrap();
                assert!(!metadata.file_type().is_symlink());
                if metadata.is_dir() {
                    visit(root, &path, snapshot);
                } else {
                    assert!(metadata.is_file());
                    let bytes = fs::read(&path).unwrap();
                    snapshot.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        (metadata.len(), sha256_bytes_value(&bytes)),
                    );
                }
            }
        }

        let mut snapshot = BTreeMap::new();
        visit(root, root, &mut snapshot);
        snapshot
    }

    fn different_pipeline_identity() -> Sha256Hex {
        let mut identity = pipeline_core_identity_sha256().0.into_bytes();
        identity[0] = if identity[0] == b'0' { b'1' } else { b'0' };
        Sha256Hex(String::from_utf8(identity).unwrap())
    }

    fn passing_external_resource_facts() -> GateAExternalResourceFactsV1 {
        let time_v_output = "Maximum resident set size (kbytes): 1024\nExit status: 0\n".to_owned();
        let memory_events = GateACgroupMemoryEventsV1 {
            low: 0,
            high: 0,
            max: 0,
            oom: 0,
            oom_kill: 0,
            oom_group_kill: 0,
        };
        GateAExternalResourceFactsV1 {
            build_exit_code: 0,
            maximum_resident_set_size_kib: 1_024,
            publication_filesystem_type: "ext4".to_owned(),
            publication_root_device_id: 1,
            candidate_device_id: 1,
            cgroup_v2_relative_path: "/fixture/dpt-gate-a-fixture.service".to_owned(),
            cgroup_process_ids_before: vec![1, 2],
            cgroup_process_ids_after: vec![1, 2],
            memory_peak_bytes: 1,
            memory_max_bytes: GATE_A_CGROUP_MEMORY_MAX_BYTES,
            memory_swap_peak_bytes: 0,
            memory_swap_max_bytes: 0,
            memory_events_before: memory_events.clone(),
            memory_events_after: memory_events,
            network_namespace: "fixture".to_owned(),
            network_interfaces: vec!["lo".to_owned()],
            network_isolation_supervisor_pid: 1,
            network_isolation_supervisor_name: "bwrap".to_owned(),
            network_isolation_supervisor_argv: [
                "/usr/bin/bwrap",
                "--unshare-user",
                "--uid",
                "0",
                "--gid",
                "0",
                "--unshare-net",
                "--proc",
                "/proc",
                "fixture",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            time_v_output_sha256: sha256_bytes_value(time_v_output.as_bytes()),
            time_v_output,
        }
    }

    fn passing_test_evidence(
        artifact_manifests: GateAArtifactManifestBindingsV1,
    ) -> GateATestEvidenceReportV1 {
        let command = |argv: Vec<String>, output: &str, covered_evidence_ids: Vec<String>| {
            GateATestCommandEvidenceV1 {
                argv,
                working_directory: "/home/chillchill/sim-system".to_owned(),
                exit_code: 0,
                combined_output: output.to_owned(),
                combined_output_sha256: sha256_bytes_value(output.as_bytes()),
                covered_evidence_ids,
            }
        };
        build_test_evidence_report(
            "1".repeat(40),
            artifact_manifests,
            vec![
                command(
                    ["/usr/bin/time", "-v", "dpt-gate-a", "__build-child"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    "physical_perturbation=PASS\nruntime_network_isolation=PASS\n",
                    vec![
                        "test/offline-boundary".to_owned(),
                        "test/physical-perturbation".to_owned(),
                    ],
                ),
                command(
                    GATE_A_WORKSPACE_TEST_ARGV
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    "workspace_regression=PASS\n",
                    GATE_A_WORKSPACE_TEST_EVIDENCE_IDS
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                ),
            ],
        )
        .unwrap()
    }

    fn build_published_final_fixture() -> PublishedFinalFixture {
        let fixture = crate::gate_a::tests::build_published_final_fixture_candidate();
        let stage = fixture.candidate_path.parent().unwrap();
        let producer_pipeline_core_identity_sha256 = different_pipeline_identity();
        let quality = build_quality_isolation_report_with_identity(
            &fixture.candidate,
            &fixture.validated,
            &producer_pipeline_core_identity_sha256,
        )
        .unwrap();
        assert!(quality.evidence.iter().all(|evidence| evidence.passed));
        let quality_bytes = encode_quality_isolation_report(&quality).unwrap();
        let quality_sha256 = sha256_bytes_value(&quality_bytes);
        let state = build_measured_build_state(
            &fixture.candidate,
            quality_sha256,
            regular_directory_bytes(&fixture.candidate_path).unwrap(),
            "1".repeat(40),
        )
        .unwrap();
        let mut resource = build_resource_high_water_report_from_state(
            &state,
            GateAHostFactsV1 {
                operating_system: "fixture".to_owned(),
                architecture: "x86_64".to_owned(),
                cpu_model: "fixture".to_owned(),
                logical_cpu_count: 1,
                physical_memory_bytes: 1,
            },
            passing_external_resource_facts(),
        )
        .unwrap();
        assert!(resource.evidence.passed);
        resource.pipeline_core_identity_sha256 = producer_pipeline_core_identity_sha256.clone();
        let mut tests = passing_test_evidence(GateAArtifactManifestBindingsV1::from_candidate(
            &fixture.candidate,
        ));
        tests.pipeline_core_identity_sha256 = producer_pipeline_core_identity_sha256.clone();
        fs::write(
            stage.join("reports/quality-isolation-report.json"),
            quality_bytes,
        )
        .unwrap();
        fs::write(
            stage.join("reports/resource-evidence.json"),
            encode_resource_high_water_report(&resource).unwrap(),
        )
        .unwrap();
        let published = finalize_and_publish_gate_a_candidate_with_identity(
            stage,
            &tests,
            &producer_pipeline_core_identity_sha256,
        )
        .unwrap();
        PublishedFinalFixture {
            _candidate: fixture,
            root: published.root,
            producer_pipeline_core_identity_sha256,
        }
    }

    #[test]
    fn gate_a_run_manifest_scan_policies_bind_exact_filters_and_full_schema_projection() {
        let condition_id = "condition-1";
        let policies = expected_typed_raw_scan_policies(condition_id, 100, 200)
            .expect("固定扫描策略必须可生成");
        let expected_bindings = supported_schema_bindings();

        assert_eq!(policies.len(), 6);
        assert_eq!(policies.len(), expected_bindings.len());
        for (policy, binding) in policies.iter().zip(expected_bindings) {
            let (source_kind, schema_version) =
                binding.split_once('=').expect("固定 schema 绑定必须合法");
            assert_eq!(policy.source_kind, source_kind);
            assert_eq!(policy.schema_version, schema_version);
            assert_eq!(policy.received_time_column, "timestamp_received");
            assert_eq!(policy.received_start_inclusive_utc_ms, 100);
            assert_eq!(policy.received_end_exclusive_utc_ms, 200);
            assert_eq!(
                policy.projected_columns,
                crate::typed_raw::expected_projection_columns(source_kind, schema_version)
                    .expect("固定 schema 必须可生成完整投影列")
            );
            assert!(policy.skipped_columns.is_empty());

            match source_kind {
                "polymarket_market" => {
                    assert_eq!(policy.condition_id_column.as_deref(), Some("market"));
                    assert_eq!(policy.condition_id_equals.as_deref(), Some(condition_id));
                }
                "polymarket_market_metadata" => {
                    assert_eq!(policy.condition_id_column.as_deref(), Some("condition_id"));
                    assert_eq!(policy.condition_id_equals.as_deref(), Some(condition_id));
                }
                _ => {
                    assert_eq!(policy.condition_id_column, None);
                    assert_eq!(policy.condition_id_equals, None);
                }
            }
        }

        let encoded = serde_json::to_value(&policies).expect("扫描策略必须可编码");
        for policy in encoded.as_array().expect("扫描策略必须编码为数组") {
            assert!(policy.get("projected_columns").is_some());
            assert!(policy.get("skipped_columns").is_some());
        }

        let mut missing_projection = policies;
        assert!(missing_projection[0].projected_columns.pop().is_some());
        assert!(matches!(
            validate_typed_raw_scan_policies(&missing_projection, condition_id, 100, 200,),
            Err(GateAPublicationError::Invalid(
                "运行清单 typed_raw 扫描策略无效"
            ))
        ));
    }

    #[test]
    fn formal_memory_gates_use_one_gib_rss_and_two_gib_cgroup() {
        assert_eq!(GATE_A_MAX_RSS_KIB, 1024 * 1024);
        assert_eq!(GATE_A_CGROUP_MEMORY_MAX_BYTES, 2 * 1024 * 1024 * 1024);
        assert!(formal_memory_gate_fits(
            GATE_A_MAX_RSS_KIB,
            GATE_A_CGROUP_MEMORY_MAX_BYTES,
            GATE_A_CGROUP_MEMORY_MAX_BYTES,
        ));
        assert!(!formal_memory_gate_fits(
            GATE_A_MAX_RSS_KIB + 1,
            GATE_A_CGROUP_MEMORY_MAX_BYTES,
            GATE_A_CGROUP_MEMORY_MAX_BYTES,
        ));
        assert!(!formal_memory_gate_fits(
            GATE_A_MAX_RSS_KIB,
            GATE_A_CGROUP_MEMORY_MAX_BYTES / 2,
            GATE_A_CGROUP_MEMORY_MAX_BYTES / 2,
        ));
        assert!(!formal_memory_gate_fits(
            GATE_A_MAX_RSS_KIB,
            GATE_A_CGROUP_MEMORY_MAX_BYTES,
            GATE_A_CGROUP_MEMORY_MAX_BYTES + 1,
        ));
    }

    #[test]
    fn clean_quality_scope_passes_without_fabricated_gap_witness() {
        let quality = GateAQualityCountsV1 {
            fresh: 213_458,
            ..GateAQualityCountsV1::default()
        };
        let resync = GateAResyncCountsV1 {
            not_required: 213_096,
            required: 0,
            resync_boundary: 362,
        };

        assert!(quality_gap_resync_predicate(&quality, &resync, 0));
        assert!(!quality_gap_resync_is_complete(&quality, &resync, 0));

        let observed_gap = GateAQualityCountsV1 {
            fresh: 213_457,
            missing: 1,
            ..GateAQualityCountsV1::default()
        };
        let required_resync = GateAResyncCountsV1 {
            not_required: 213_096,
            required: 1,
            resync_boundary: 363,
        };
        assert!(!quality_gap_resync_predicate(
            &observed_gap,
            &required_resync,
            0,
        ));
        assert!(!quality_gap_resync_is_complete(
            &observed_gap,
            &required_resync,
            0,
        ));
        assert!(quality_gap_resync_predicate(
            &observed_gap,
            &required_resync,
            1,
        ));
        assert!(quality_gap_resync_is_complete(
            &observed_gap,
            &required_resync,
            1,
        ));

        let two_gaps = GateAQualityCountsV1 {
            fresh: 213_456,
            missing: 2,
            ..GateAQualityCountsV1::default()
        };
        let two_required = GateAResyncCountsV1 {
            not_required: 213_096,
            required: 2,
            resync_boundary: 363,
        };
        assert!(!quality_gap_resync_predicate(&two_gaps, &two_required, 1,));
        assert!(!quality_gap_resync_is_complete(&two_gaps, &two_required, 1,));
    }

    #[test]
    fn formal_work_directory_bound_rejects_missing_control_file_budget() {
        let candidate_peak = 100;
        assert!(!formal_work_directory_bound_covers(
            candidate_peak,
            candidate_peak,
            None,
        ));
        assert!(formal_work_directory_bound_covers(
            candidate_peak,
            candidate_peak + crate::gate_a::GATE_A_CONTROL_FILE_BYTES_BOUND,
            None,
        ));
    }

    #[test]
    fn formal_work_directory_bound_rejects_value_below_revalidation_peak() {
        let candidate_peak = 100;
        let revalidated_peak = candidate_peak + crate::gate_a::GATE_A_CONTROL_FILE_BYTES_BOUND + 1;
        assert!(!formal_work_directory_bound_covers(
            candidate_peak,
            revalidated_peak - 1,
            Some(revalidated_peak),
        ));
        assert!(formal_work_directory_bound_covers(
            candidate_peak,
            revalidated_peak,
            Some(revalidated_peak),
        ));
    }

    #[test]
    fn published_final_validation_work_path_must_be_outside_final() {
        let temporary = TempDir::new().unwrap();
        let final_root = temporary.path().join("final");
        fs::create_dir(&final_root).unwrap();

        assert!(matches!(
            require_external_validation_work_path(
                &fs::canonicalize(&final_root).unwrap(),
                &final_root.join("validation"),
            ),
            Err(GateAPublicationError::Invalid(
                "发布 final 的验证工作目录必须位于 final 外部"
            ))
        ));
        assert!(
            require_external_validation_work_path(
                &fs::canonicalize(&final_root).unwrap(),
                &temporary.path().join("validation"),
            )
            .is_ok()
        );
    }

    #[test]
    fn published_final_public_opener_validates_complete_read_only_fixture() {
        let fixture = build_published_final_fixture();
        let before = immutable_file_snapshot(&fixture.root);
        let validation_path = fixture
            ._candidate
            ._temporary
            .path()
            .join("successful-open-validation");

        let opened = open_published_gate_a_final(&fixture.root, &validation_path).unwrap();

        assert_eq!(opened.root(), fs::canonicalize(&fixture.root).unwrap());
        assert_eq!(
            opened.producer_pipeline_core_identity_sha256(),
            &fixture.producer_pipeline_core_identity_sha256
        );
        assert_eq!(
            opened.reader_pipeline_core_identity_sha256(),
            &pipeline_core_identity_sha256()
        );
        assert_ne!(
            opened.producer_pipeline_core_identity_sha256(),
            opened.reader_pipeline_core_identity_sha256()
        );
        assert_eq!(
            &opened.run_manifest().pipeline_core_identity_sha256,
            opened.producer_pipeline_core_identity_sha256()
        );
        assert_eq!(
            opened
                .candidate()
                .canonical_event_store
                .manifest()
                .file
                .rows,
            14
        );
        assert!(!validation_path.exists());
        assert!(!fixture.root.join(".final-validation").exists());
        assert_eq!(immutable_file_snapshot(&fixture.root), before);
    }

    #[test]
    fn published_final_public_opener_rejects_run_manifest_binding_corruption() {
        let fixture = build_published_final_fixture();
        let before = immutable_file_snapshot(&fixture.root);
        let run_manifest_path = fixture.root.join("run-manifest.json");
        let mut run_manifest: GateARunManifestV2 =
            serde_json::from_slice(&fs::read(&run_manifest_path).unwrap()).unwrap();
        run_manifest.control_files.quality_isolation_report.sha256 = Sha256Hex("e".repeat(64));
        fs::write(
            &run_manifest_path,
            serde_json::to_vec(&run_manifest).unwrap(),
        )
        .unwrap();
        let validation_path = fixture
            ._candidate
            ._temporary
            .path()
            .join("failed-open-validation");

        assert!(matches!(
            open_published_gate_a_final(&fixture.root, &validation_path),
            Err(GateAPublicationError::Invalid(
                "最终运行清单无法从同一产物与报告机械重建"
            ))
        ));

        assert!(!validation_path.exists());
        assert!(!fixture.root.join(".final-validation").exists());
        let mut before_without_perturbation = before;
        let before_run_manifest = before_without_perturbation
            .remove(Path::new("run-manifest.json"))
            .unwrap();
        let mut after_without_perturbation = immutable_file_snapshot(&fixture.root);
        let after_run_manifest = after_without_perturbation
            .remove(Path::new("run-manifest.json"))
            .unwrap();
        assert_ne!(after_run_manifest, before_run_manifest);
        assert_eq!(after_without_perturbation, before_without_perturbation);
    }

    #[test]
    fn prepublication_layout_rejects_reports_symlink() {
        let temporary = TempDir::new().unwrap();
        let stage = temporary.path().join(".symlink.candidate");
        let external_reports = temporary.path().join("external-reports");
        fs::create_dir_all(stage.join("artifacts")).unwrap();
        fs::create_dir(&external_reports).unwrap();
        fs::write(
            external_reports.join("quality-isolation-report.json"),
            b"quality",
        )
        .unwrap();
        fs::write(external_reports.join("resource-evidence.json"), b"resource").unwrap();
        symlink(&external_reports, stage.join("reports")).unwrap();

        assert!(matches!(
            require_prepublication_layout(&stage),
            Err(GateAPublicationError::Invalid(
                "发布候选的直接子目录必须是普通目录且不得为符号链接"
            ))
        ));
        assert_eq!(
            directory_entry_names(&external_reports).unwrap(),
            [
                "quality-isolation-report.json".to_owned(),
                "resource-evidence.json".to_owned(),
            ]
            .into()
        );
    }

    #[test]
    fn existing_publication_target_is_not_overwritten_and_candidate_is_cleaned() {
        let temporary = TempDir::new().unwrap();
        let stage = temporary.path().join(".collision.candidate");
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("gate-a-receipt.json"), b"new receipt").unwrap();

        let target_name = "a".repeat(64);
        let target = temporary.path().join(&target_name);
        fs::create_dir(&target).unwrap();
        fs::write(target.join("existing-marker"), b"keep").unwrap();
        fs::write(target.join("gate-a-receipt.json"), b"old receipt").unwrap();

        let guard = PublicationCandidateGuard::acquire(&stage).unwrap();
        assert!(matches!(
            commit_candidate_no_replace(guard, &target_name),
            Err(GateAPublicationError::Io(error))
                if error.kind() == io::ErrorKind::AlreadyExists
        ));

        assert!(!stage.exists(), "失败后必须清理本次精确候选目录");
        assert_eq!(fs::read(target.join("existing-marker")).unwrap(), b"keep");
        assert_eq!(
            fs::read(target.join("gate-a-receipt.json")).unwrap(),
            b"old receipt",
            "已有成功目标的回执不得被候选覆盖"
        );
        assert_eq!(
            directory_entry_names(temporary.path()).unwrap(),
            [target_name].into()
        );
    }
}
