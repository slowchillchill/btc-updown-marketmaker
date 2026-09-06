//! 规范事实与观测用途的显式发布；复用既有候选、事实统计和原子提交。

use super::*;
use crate::FrozenDirectSourceInputSetV1;
use contracts::{DPT_OBSERVATION_RECEIPT_SCHEMA_V1, DptObservationReceiptV1};
use data_pipeline_core::{
    decode_dpt_observation_receipt_v1, derive_dpt_observation_receipt_v1_id,
    encode_dpt_observation_receipt_v1,
};

pub const DPT_OBSERVATION_QUALITY_REPORT_SCHEMA_V1: &str = "dpt_observation_quality_report_v1";
pub const DPT_OBSERVATION_TEST_EVIDENCE_REPORT_SCHEMA_V1: &str =
    "dpt_observation_test_evidence_report_v1";
pub const DPT_OBSERVATION_RUN_MANIFEST_SCHEMA_V1: &str = "dpt_observation_run_manifest_v1";
pub const DPT_OBSERVATION_MEASURED_BUILD_STATE_SCHEMA_V1: &str =
    "dpt_observation_measured_build_state_v1";
pub const REQUIRED_DPT_OBSERVATION_TEST_EVIDENCE_IDS: [&str; 8] = [
    "MS-T01", "MS-T02", "MS-T03", "MS-T04", "MS-T05", "MS-T06", "MS-T07", "MS-T08",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DptObservationQualityReportV1 {
    pub schema_version: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub input_manifests: Vec<GateAInputManifestBindingV1>,
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

impl DptObservationQualityReportV1 {
    fn from_facts(
        facts: GateAQualityIsolationReportV1,
        input_manifests: Vec<GateAInputManifestBindingV1>,
    ) -> Self {
        Self {
            schema_version: facts.schema_version,
            pipeline_core_identity_sha256: facts.pipeline_core_identity_sha256,
            input_set_identity_sha256: facts.input_set_identity_sha256,
            input_manifest_set_sha256: facts.input_manifest_set_sha256,
            scope: facts.scope,
            artifact_manifests: facts.artifact_manifests,
            artifact_rows: facts.artifact_rows,
            selected_files: facts.selected_files,
            selected_source_rows: facts.selected_source_rows,
            market_event_counts: facts.market_event_counts,
            trading_state_field_counts: facts.trading_state_field_counts,
            reference_event_counts: facts.reference_event_counts,
            event_quality_counts: facts.event_quality_counts,
            event_resync_counts: facts.event_resync_counts,
            fresh_resync_boundary: facts.fresh_resync_boundary,
            event_without_source_evidence: facts.event_without_source_evidence,
            event_without_source_time: facts.event_without_source_time,
            distinct_market_windows: facts.distinct_market_windows,
            initial_snapshot_token_counts: facts.initial_snapshot_token_counts,
            best_bid_ask_tokens: facts.best_bid_ask_tokens,
            tick_change_tokens: facts.tick_change_tokens,
            known_old_tick_change_tokens: facts.known_old_tick_change_tokens,
            public_trade_platform_id: facts.public_trade_platform_id,
            public_trade_derived_id: facts.public_trade_derived_id,
            level_execution_basis_counts: facts.level_execution_basis_counts,
            terminal_with_source_time: facts.terminal_with_source_time,
            terminal_without_source_time: facts.terminal_without_source_time,
            observation_rows: facts.observation_rows,
            observation_component_facts: facts.observation_component_facts,
            reduction_operation_counts: facts.reduction_operation_counts,
            reduction_disposition_counts: facts.reduction_disposition_counts,
            reduction_reason_counts: facts.reduction_reason_counts,
            quantity_conservation_reports: facts.quantity_conservation_reports,
            blocking_terminal_reports: facts.blocking_terminal_reports,
            evidence: facts.evidence,
            input_manifests,
        }
    }
    fn shared_facts(&self) -> GateAQualityIsolationReportV1 {
        GateAQualityIsolationReportV1 {
            schema_version: self.schema_version.clone(),
            pipeline_core_identity_sha256: self.pipeline_core_identity_sha256.clone(),
            input_set_identity_sha256: self.input_set_identity_sha256.clone(),
            input_manifest_set_sha256: self.input_manifest_set_sha256.clone(),
            scope: self.scope.clone(),
            artifact_manifests: self.artifact_manifests.clone(),
            artifact_rows: self.artifact_rows.clone(),
            selected_files: self.selected_files.clone(),
            selected_source_rows: self.selected_source_rows.clone(),
            market_event_counts: self.market_event_counts.clone(),
            trading_state_field_counts: self.trading_state_field_counts.clone(),
            reference_event_counts: self.reference_event_counts.clone(),
            event_quality_counts: self.event_quality_counts.clone(),
            event_resync_counts: self.event_resync_counts.clone(),
            fresh_resync_boundary: self.fresh_resync_boundary,
            event_without_source_evidence: self.event_without_source_evidence,
            event_without_source_time: self.event_without_source_time,
            distinct_market_windows: self.distinct_market_windows,
            initial_snapshot_token_counts: self.initial_snapshot_token_counts.clone(),
            best_bid_ask_tokens: self.best_bid_ask_tokens.clone(),
            tick_change_tokens: self.tick_change_tokens.clone(),
            known_old_tick_change_tokens: self.known_old_tick_change_tokens.clone(),
            public_trade_platform_id: self.public_trade_platform_id,
            public_trade_derived_id: self.public_trade_derived_id,
            level_execution_basis_counts: self.level_execution_basis_counts.clone(),
            terminal_with_source_time: self.terminal_with_source_time,
            terminal_without_source_time: self.terminal_without_source_time,
            observation_rows: self.observation_rows,
            observation_component_facts: self.observation_component_facts.clone(),
            reduction_operation_counts: self.reduction_operation_counts.clone(),
            reduction_disposition_counts: self.reduction_disposition_counts.clone(),
            reduction_reason_counts: self.reduction_reason_counts.clone(),
            quantity_conservation_reports: self.quantity_conservation_reports,
            blocking_terminal_reports: self.blocking_terminal_reports,
            evidence: self.evidence.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DptObservationTestEvidenceReportV1 {
    pub schema_version: String,
    pub git_commit: String,
    pub pipeline_core_identity_sha256: Sha256Hex,
    pub artifact_manifests: GateAArtifactManifestBindingsV1,
    pub commands: Vec<GateATestCommandEvidenceV1>,
    pub evidence: Vec<GateAEvidenceRecordV1>,
}

impl DptObservationTestEvidenceReportV1 {
    fn from_facts(facts: GateATestEvidenceReportV1) -> Self {
        Self {
            schema_version: facts.schema_version,
            git_commit: facts.git_commit,
            pipeline_core_identity_sha256: facts.pipeline_core_identity_sha256,
            artifact_manifests: facts.artifact_manifests,
            commands: facts.commands,
            evidence: facts.evidence,
        }
    }
    fn shared_facts(&self) -> GateATestEvidenceReportV1 {
        GateATestEvidenceReportV1 {
            schema_version: self.schema_version.clone(),
            git_commit: self.git_commit.clone(),
            pipeline_core_identity_sha256: self.pipeline_core_identity_sha256.clone(),
            artifact_manifests: self.artifact_manifests.clone(),
            commands: self.commands.clone(),
            evidence: self.evidence.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DptObservationMeasuredBuildStateV1 {
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

impl DptObservationMeasuredBuildStateV1 {
    fn from_facts(facts: GateAMeasuredBuildStateV1) -> Self {
        Self {
            schema_version: facts.schema_version,
            git_commit: facts.git_commit,
            candidate_path: facts.candidate_path,
            pipeline_core_identity_sha256: facts.pipeline_core_identity_sha256,
            input_set_identity_sha256: facts.input_set_identity_sha256,
            input_manifest_set_sha256: facts.input_manifest_set_sha256,
            scope: facts.scope,
            artifact_manifests: facts.artifact_manifests,
            artifact_rows: facts.artifact_rows,
            quality_isolation_report_sha256: facts.quality_isolation_report_sha256,
            canonical_config: facts.canonical_config,
            output_config: facts.output_config,
            canonical_metrics: facts.canonical_metrics,
            build_metrics: facts.build_metrics,
            artifact_write_metrics: facts.artifact_write_metrics,
            artifact_directory_bytes: facts.artifact_directory_bytes,
        }
    }
    fn shared_facts(&self) -> GateAMeasuredBuildStateV1 {
        GateAMeasuredBuildStateV1 {
            schema_version: self.schema_version.clone(),
            git_commit: self.git_commit.clone(),
            candidate_path: self.candidate_path.clone(),
            pipeline_core_identity_sha256: self.pipeline_core_identity_sha256.clone(),
            input_set_identity_sha256: self.input_set_identity_sha256.clone(),
            input_manifest_set_sha256: self.input_manifest_set_sha256.clone(),
            scope: self.scope.clone(),
            artifact_manifests: self.artifact_manifests.clone(),
            artifact_rows: self.artifact_rows.clone(),
            quality_isolation_report_sha256: self.quality_isolation_report_sha256.clone(),
            canonical_config: self.canonical_config.clone(),
            output_config: self.output_config.clone(),
            canonical_metrics: self.canonical_metrics.clone(),
            build_metrics: self.build_metrics.clone(),
            artifact_write_metrics: self.artifact_write_metrics.clone(),
            artifact_directory_bytes: self.artifact_directory_bytes,
        }
    }
}

/// 共享清单事实布局，但通过独立控制绑定类型与旧用途隔离；不携带能力矩阵。
pub type DptObservationRunManifestV1 = GateARunManifestV2<DptObservationControlBindingsV1>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DptObservationControlBindingsV1 {
    pub quality_isolation_report: GateAControlFileBindingV1,
    pub resource_high_water_report: GateAControlFileBindingV1,
    pub test_evidence_report: GateAControlFileBindingV1,
}

/// 从同一冻结输入及已验证候选统计事实，保留全部提供的日清单（包括空日）。
pub fn build_dpt_observation_quality_report(
    input: &FrozenDirectSourceInputSetV1,
    candidate: &GateACandidate,
    validated: &ValidatedGateACandidate,
) -> Result<DptObservationQualityReportV1, GateAPublicationError> {
    if input.input_set_identity_sha256() != candidate.input_set_identity_sha256.0
        || input.input_manifest_set_sha256() != candidate.input_manifest_set_sha256.0
    {
        return Err(GateAPublicationError::Invalid(
            "观测质量报告没有绑定产生候选的冻结输入",
        ));
    }
    let bindings = input
        .admitted_daily_inputs()
        .iter()
        .map(|daily| GateAInputManifestBindingV1 {
            utc_day: daily.utc_day().to_owned(),
            path: daily.manifest_path().to_string_lossy().into_owned(),
            sha256: Sha256Hex(daily.manifest_sha256().to_owned()),
        })
        .collect();
    rebuild_quality_report(
        candidate,
        validated,
        bindings,
        &pipeline_core_identity_sha256(),
    )
}

fn rebuild_quality_report(
    candidate: &GateACandidate,
    validated: &ValidatedGateACandidate,
    bindings: Vec<GateAInputManifestBindingV1>,
    producer: &Sha256Hex,
) -> Result<DptObservationQualityReportV1, GateAPublicationError> {
    let facts = build_quality_report_facts(
        candidate,
        validated,
        producer,
        PublicationPurpose::Observation,
    )?;
    let report = DptObservationQualityReportV1::from_facts(facts, bindings);
    validate_dpt_observation_quality_report(&report)?;
    Ok(report)
}

fn validate_input_manifest_bindings(
    bindings: &[GateAInputManifestBindingV1],
    expected_digest: &Sha256Hex,
    selected_files: &[CanonicalRunInputFile],
) -> Result<(), GateAPublicationError> {
    let mut previous = None;
    for binding in bindings {
        let key = (&binding.utc_day, &binding.sha256.0, &binding.path);
        if previous.is_some_and(|previous| previous >= key)
            || binding.utc_day.as_str() < DATASET_DATE_FLOOR
            || Date::parse(
                &binding.utc_day,
                &format_description!("[year]-[month]-[day]"),
            )
            .is_err()
            || !Path::new(&binding.path).is_absolute()
            || !binding.path.ends_with(&format!(
                "/{}/typed_raw/utc_day={}/manifest.json",
                binding.utc_day, binding.utc_day
            ))
        {
            return Err(GateAPublicationError::Invalid(
                "输入日清单身份、路径或稳定顺序无效",
            ));
        }
        require_sha256(&binding.sha256)?;
        previous = Some(key);
    }
    let digest = crate::input_manifest_bindings_sha256(
        bindings
            .iter()
            .map(|binding| (binding.utc_day.as_str(), binding.sha256.0.as_str()))
            .collect(),
    );
    if digest != expected_digest.0
        || selected_files.iter().any(|file| {
            !bindings.iter().any(|binding| {
                binding.utc_day == file.utc_day && binding.sha256.0 == file.manifest_sha256
            })
        })
    {
        return Err(GateAPublicationError::Invalid(
            "完整输入日清单集合摘要或所选文件绑定不一致",
        ));
    }
    Ok(())
}

pub fn validate_dpt_observation_quality_report(
    report: &DptObservationQualityReportV1,
) -> Result<(), GateAPublicationError> {
    validate_quality_report_facts(&report.shared_facts(), PublicationPurpose::Observation)?;
    validate_input_manifest_bindings(
        &report.input_manifests,
        &report.input_manifest_set_sha256,
        &report.selected_files,
    )
}

pub fn encode_dpt_observation_quality_report(
    report: &DptObservationQualityReportV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_dpt_observation_quality_report(report)?;
    encode_bounded_json(report, 4 * 1024 * 1024, "观测质量报告超过 4 MiB 上限")
}

pub fn decode_dpt_observation_quality_report(
    bytes: &[u8],
) -> Result<DptObservationQualityReportV1, GateAPublicationError> {
    let report = decode_bounded(bytes, 4 * 1024 * 1024)?;
    validate_dpt_observation_quality_report(&report)?;
    Ok(report)
}

fn decode_bounded<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
) -> Result<T, GateAPublicationError> {
    if bytes.len() > limit {
        return Err(GateAPublicationError::Invalid(
            "观测控制文件超过既有字节上限",
        ));
    }
    Ok(serde_json::from_slice(bytes)?)
}

pub fn build_dpt_observation_measured_build_state(
    candidate: &GateACandidate,
    quality_sha256: Sha256Hex,
    artifact_directory_bytes: u64,
    git_commit: String,
) -> Result<DptObservationMeasuredBuildStateV1, GateAPublicationError> {
    Ok(DptObservationMeasuredBuildStateV1::from_facts(
        build_measured_facts(
            candidate,
            quality_sha256,
            artifact_directory_bytes,
            git_commit,
            PublicationPurpose::Observation,
        )?,
    ))
}

pub fn validate_dpt_observation_measured_build_state(
    state: &DptObservationMeasuredBuildStateV1,
) -> Result<(), GateAPublicationError> {
    validate_measured_facts(&state.shared_facts(), PublicationPurpose::Observation)
}

pub fn encode_dpt_observation_measured_build_state(
    state: &DptObservationMeasuredBuildStateV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_dpt_observation_measured_build_state(state)?;
    encode_bounded_json(state, 4 * 1024 * 1024, "观测计时构建状态超过 4 MiB 上限")
}

pub fn decode_dpt_observation_measured_build_state(
    bytes: &[u8],
) -> Result<DptObservationMeasuredBuildStateV1, GateAPublicationError> {
    let state = decode_bounded(bytes, 4 * 1024 * 1024)?;
    validate_dpt_observation_measured_build_state(&state)?;
    Ok(state)
}

/// 使用正式配置及调用方提供的真实外部资源测量；本函数不自行执行测量。
pub fn build_dpt_observation_resource_report_from_state(
    state: &DptObservationMeasuredBuildStateV1,
    host: GateAHostFactsV1,
    external: GateAExternalResourceFactsV1,
) -> Result<GateAResourceHighWaterReportV1, GateAPublicationError> {
    validate_dpt_observation_measured_build_state(state)?;
    build_resource_report_from_facts(&state.shared_facts(), host, external)
}

/// 按 MS-T01 至 MS-T08 汇总调用方的真实命令记录；不执行命令或补写成功证据。
pub fn build_dpt_observation_test_evidence_report(
    git_commit: String,
    artifacts: GateAArtifactManifestBindingsV1,
    commands: Vec<GateATestCommandEvidenceV1>,
) -> Result<DptObservationTestEvidenceReportV1, GateAPublicationError> {
    Ok(DptObservationTestEvidenceReportV1::from_facts(
        build_test_report_facts(
            git_commit,
            artifacts,
            commands,
            PublicationPurpose::Observation,
        )?,
    ))
}

pub fn validate_dpt_observation_test_evidence_report(
    report: &DptObservationTestEvidenceReportV1,
) -> Result<(), GateAPublicationError> {
    validate_test_report_facts(&report.shared_facts(), PublicationPurpose::Observation)
}

pub fn encode_dpt_observation_test_evidence_report(
    report: &DptObservationTestEvidenceReportV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_dpt_observation_test_evidence_report(report)?;
    encode_bounded_json(report, 16 * 1024 * 1024, "观测测试报告超过 16 MiB 上限")
}

pub fn decode_dpt_observation_test_evidence_report(
    bytes: &[u8],
) -> Result<DptObservationTestEvidenceReportV1, GateAPublicationError> {
    let report = decode_bounded(bytes, 16 * 1024 * 1024)?;
    validate_dpt_observation_test_evidence_report(&report)?;
    Ok(report)
}

fn covered_dates(start: i64, end: i64) -> Result<Vec<String>, GateAPublicationError> {
    let date = |millis: i64| {
        time::OffsetDateTime::from_unix_timestamp(millis.div_euclid(1000))
            .map(|time| time.date())
            .map_err(|_| GateAPublicationError::Invalid("观测范围不能转换为 UTC 日期"))
    };
    let mut current = date(start)?;
    let last = date(end - 1)?;
    let mut dates = Vec::new();
    while current <= last {
        dates.push(current.to_string());
        if current == last {
            break;
        }
        current = current
            .next_day()
            .ok_or(GateAPublicationError::Invalid("观测覆盖日期溢出"))?;
    }
    Ok(dates)
}

pub fn validate_dpt_observation_run_manifest(
    manifest: &DptObservationRunManifestV1,
) -> Result<(), GateAPublicationError> {
    validate_run_manifest_facts(manifest, PublicationPurpose::Observation)?;
    validate_control_file_bindings(&[
        (
            &manifest.control_files.quality_isolation_report,
            "reports/quality-isolation-report.json",
        ),
        (
            &manifest.control_files.resource_high_water_report,
            "reports/resource-evidence.json",
        ),
        (
            &manifest.control_files.test_evidence_report,
            "reports/test-evidence.json",
        ),
    ])?;
    if manifest.covered_utc_dates
        != covered_dates(manifest.received_start_utc_ms, manifest.received_end_utc_ms)?
    {
        return Err(GateAPublicationError::Invalid(
            "观测覆盖日期未保留显式接收范围",
        ));
    }
    validate_input_manifest_bindings(
        &manifest.input_manifests,
        &manifest.input_manifest_set_sha256,
        &manifest.selected_files,
    )
}

pub fn encode_dpt_observation_run_manifest(
    manifest: &DptObservationRunManifestV1,
) -> Result<Vec<u8>, GateAPublicationError> {
    validate_dpt_observation_run_manifest(manifest)?;
    encode_bounded_json(manifest, 16 * 1024 * 1024, "观测运行清单超过 16 MiB 上限")
}

pub fn decode_dpt_observation_run_manifest(
    bytes: &[u8],
) -> Result<DptObservationRunManifestV1, GateAPublicationError> {
    let manifest = decode_bounded(bytes, 16 * 1024 * 1024)?;
    validate_dpt_observation_run_manifest(&manifest)?;
    Ok(manifest)
}

pub fn build_dpt_observation_run_manifest(
    quality: &DptObservationQualityReportV1,
    resource: &GateAResourceHighWaterReportV1,
    tests: &DptObservationTestEvidenceReportV1,
    validated: &ValidatedGateACandidate,
) -> Result<DptObservationRunManifestV1, GateAPublicationError> {
    let quality_bytes = encode_dpt_observation_quality_report(quality)?;
    let resource_bytes = encode_resource_high_water_report(resource)?;
    let tests_bytes = encode_dpt_observation_test_evidence_report(tests)?;
    let facts = quality.shared_facts();
    if resource.quality_isolation_report_sha256 != sha256_bytes_value(&quality_bytes)
        || quality.pipeline_core_identity_sha256 != resource.pipeline_core_identity_sha256
        || quality.pipeline_core_identity_sha256 != tests.pipeline_core_identity_sha256
        || quality.input_set_identity_sha256 != resource.input_set_identity_sha256
        || quality.input_manifest_set_sha256 != resource.input_manifest_set_sha256
        || quality.scope != resource.scope
        || quality.artifact_manifests != resource.artifact_manifests
        || quality.artifact_manifests != tests.artifact_manifests
        || quality.selected_source_rows != resource.canonical_metrics.selected_rows
        || !quality_matches_validated_candidate(&facts, validated)
        || !resource_matches_validated_candidate(resource, validated)
        || !resource.evidence.passed
        || tests.evidence.iter().any(|evidence| !evidence.passed)
    {
        return Err(GateAPublicationError::Invalid(
            "观测发布报告绑定、测试结果或资源事实不满足合同",
        ));
    }
    let manifest = assemble_run_manifest(
        (&facts, resource, &tests.shared_facts()),
        validated,
        (
            covered_dates(
                quality.scope.received_start_utc_ms(),
                quality.scope.received_end_utc_ms(),
            )?,
            quality.input_manifests.clone(),
        ),
        DptObservationControlBindingsV1 {
            quality_isolation_report: control_binding(
                "reports/quality-isolation-report.json",
                sha256_bytes_value(&quality_bytes),
            ),
            resource_high_water_report: control_binding(
                "reports/resource-evidence.json",
                sha256_bytes_value(&resource_bytes),
            ),
            test_evidence_report: control_binding(
                "reports/test-evidence.json",
                sha256_bytes_value(&tests_bytes),
            ),
        },
        PublicationPurpose::Observation,
    )?;
    validate_dpt_observation_run_manifest(&manifest)?;
    Ok(manifest)
}

pub fn build_dpt_observation_receipt(
    manifest: &DptObservationRunManifestV1,
) -> Result<DptObservationReceiptV1, GateAPublicationError> {
    let bytes = encode_dpt_observation_run_manifest(manifest)?;
    let mut receipt = DptObservationReceiptV1 {
        schema_version: DPT_OBSERVATION_RECEIPT_SCHEMA_V1.to_owned(),
        receipt_id: Sha256Hex("0".repeat(64)),
        pipeline_core_identity_sha256: manifest.pipeline_core_identity_sha256.clone(),
        input_set_identity_sha256: manifest.input_set_identity_sha256.clone(),
        input_manifest_set_sha256: manifest.input_manifest_set_sha256.clone(),
        observation_run_manifest_sha256: sha256_bytes_value(&bytes),
        training_dataset_ready: false,
    };
    receipt.receipt_id = derive_dpt_observation_receipt_v1_id(&receipt)
        .map_err(|_| GateAPublicationError::Invalid("观测凭据身份无法计算"))?;
    Ok(receipt)
}

#[derive(Debug)]
pub struct PublishedDptObservationV1 {
    pub root: PathBuf,
    pub receipt: DptObservationReceiptV1,
}

#[derive(Debug)]
pub struct ValidatedPublishedDptObservationV1 {
    root: PathBuf,
    receipt: DptObservationReceiptV1,
    run_manifest: DptObservationRunManifestV1,
    reader_pipeline_core_identity_sha256: Sha256Hex,
    candidate: ValidatedGateACandidate,
}

impl ValidatedPublishedDptObservationV1 {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn receipt(&self) -> &DptObservationReceiptV1 {
        &self.receipt
    }
    pub fn run_manifest(&self) -> &DptObservationRunManifestV1 {
        &self.run_manifest
    }
    pub fn producer_pipeline_core_identity_sha256(&self) -> &Sha256Hex {
        &self.receipt.pipeline_core_identity_sha256
    }
    pub fn reader_pipeline_core_identity_sha256(&self) -> &Sha256Hex {
        &self.reader_pipeline_core_identity_sha256
    }
    pub fn candidate(&self) -> &ValidatedGateACandidate {
        &self.candidate
    }
}

fn require_observation_layout(root: &Path) -> Result<(), GateAPublicationError> {
    require_publication_layout(
        root,
        &[
            "artifacts",
            "reports",
            "observation-run-manifest.json",
            "observation-receipt.json",
        ],
    )
}

fn validate_observation_candidate(
    stage: &Path,
    validation_path: &Path,
    quality: &DptObservationQualityReportV1,
    resource: &GateAResourceHighWaterReportV1,
    producer: &Sha256Hex,
) -> Result<ValidatedGateACandidate, GateAPublicationError> {
    let artifacts = stage.join("artifacts");
    let validated =
        validate_gate_a_candidate(&artifacts, validation_path, &resource.output_config)?;
    let facts = quality.shared_facts();
    if !quality_matches_validated_candidate(&facts, &validated)
        || !resource_matches_validated_candidate(resource, &validated)
        || regular_directory_bytes(&artifacts)? != resource.final_artifact_directory_bytes
    {
        return Err(GateAPublicationError::Invalid(
            "观测报告与重新验证的物理候选不一致",
        ));
    }
    let candidate = reconstructed_candidate(&facts, resource, &validated);
    let rebuilt = rebuild_quality_report(
        &candidate,
        &validated,
        quality.input_manifests.clone(),
        producer,
    )?;
    if &rebuilt != quality {
        return Err(GateAPublicationError::Invalid(
            "观测质量事实不能从产物及冻结日清单绑定重建",
        ));
    }
    Ok(validated)
}

fn verify_observation_publication(
    stage: &Path,
    validation_path: &Path,
) -> Result<ValidatedPublishedDptObservationV1, GateAPublicationError> {
    require_observation_layout(stage)?;
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
    let manifest_bytes = read_regular_file(
        &stage.join("observation-run-manifest.json"),
        16 * 1024 * 1024,
    )?;
    let receipt_bytes = read_regular_file(&stage.join("observation-receipt.json"), 1024 * 1024)?;
    let quality = decode_dpt_observation_quality_report(&quality_bytes)?;
    let resource = decode_resource_high_water_report(&resource_bytes)?;
    let tests = decode_dpt_observation_test_evidence_report(&tests_bytes)?;
    let manifest = decode_dpt_observation_run_manifest(&manifest_bytes)?;
    let receipt = decode_dpt_observation_receipt_v1(&receipt_bytes)
        .map_err(|_| GateAPublicationError::Invalid("观测凭据解码失败"))?;
    let validated = validate_observation_candidate(
        stage,
        validation_path,
        &quality,
        &resource,
        &receipt.pipeline_core_identity_sha256,
    )?;
    let rebuilt_manifest =
        build_dpt_observation_run_manifest(&quality, &resource, &tests, &validated)?;
    if rebuilt_manifest != manifest
        || encode_dpt_observation_quality_report(&quality)? != quality_bytes
        || encode_resource_high_water_report(&resource)? != resource_bytes
        || encode_dpt_observation_test_evidence_report(&tests)? != tests_bytes
        || encode_dpt_observation_run_manifest(&manifest)? != manifest_bytes
        || build_dpt_observation_receipt(&manifest)? != receipt
    {
        return Err(GateAPublicationError::Invalid(
            "观测凭据、运行清单与五份产物及报告摘要闭包不一致",
        ));
    }
    require_observation_layout(stage)?;
    Ok(ValidatedPublishedDptObservationV1 {
        root: fs::canonicalize(stage)?,
        receipt,
        run_manifest: manifest,
        reader_pipeline_core_identity_sha256: pipeline_core_identity_sha256(),
        candidate: validated,
    })
}

/// 只读重建完整发布闭包；验证临时目录必须位于发布目录之外，不读取原始数据。
pub fn open_published_dpt_observation(
    root: &Path,
    validation_work_path: &Path,
) -> Result<ValidatedPublishedDptObservationV1, GateAPublicationError> {
    require_observation_layout(root)?;
    let canonical_root = fs::canonicalize(root)?;
    let validation = require_external_validation_work_path(&canonical_root, validation_work_path)?;
    let opened = verify_observation_publication(&canonical_root, &validation)?;
    if canonical_root.file_name().and_then(|name| name.to_str())
        != Some(opened.receipt.receipt_id.0.as_str())
    {
        return Err(GateAPublicationError::Invalid(
            "观测发布目录名与凭据 ID 不一致",
        ));
    }
    Ok(opened)
}

/// 发布同级隐藏 `.candidate` 中的 artifacts 与质量/资源报告，回执最后写入并原子提交。
/// 测试或资源证据失败时不签发；失败清理本候选，已有同名回执目录绝不覆盖。
pub fn finalize_and_publish_dpt_observation_candidate(
    stage_path: &Path,
    tests: &DptObservationTestEvidenceReportV1,
) -> Result<PublishedDptObservationV1, GateAPublicationError> {
    let guard = PublicationCandidateGuard::acquire(stage_path)?;
    let stage = guard.path.clone();
    require_prepublication_layout(&stage)?;
    let quality = decode_dpt_observation_quality_report(&read_regular_file(
        &stage.join("reports/quality-isolation-report.json"),
        4 * 1024 * 1024,
    )?)?;
    let resource = decode_resource_high_water_report(&read_regular_file(
        &stage.join("reports/resource-evidence.json"),
        4 * 1024 * 1024,
    )?)?;
    let producer = pipeline_core_identity_sha256();
    let validated = validate_observation_candidate(
        &stage,
        &stage.join(".validation"),
        &quality,
        &resource,
        &producer,
    )?;
    let manifest = build_dpt_observation_run_manifest(&quality, &resource, tests, &validated)?;
    write_new_synced(
        &stage.join("reports/test-evidence.json"),
        &encode_dpt_observation_test_evidence_report(tests)?,
    )?;
    write_new_synced(
        &stage.join("observation-run-manifest.json"),
        &encode_dpt_observation_run_manifest(&manifest)?,
    )?;
    let receipt = build_dpt_observation_receipt(&manifest)?;
    let receipt_bytes = encode_dpt_observation_receipt_v1(&receipt)
        .map_err(|_| GateAPublicationError::Invalid("观测凭据无法编码"))?;
    write_new_synced(&stage.join("observation-receipt.json"), &receipt_bytes)?;
    let checked = verify_observation_publication(&stage, &stage.join(".final-validation"))?;
    if checked.receipt != receipt || checked.run_manifest != manifest {
        return Err(GateAPublicationError::Invalid(
            "观测发布控制文件在写入后发生变化",
        ));
    }
    sync_tree(&stage)?;
    let root = commit_candidate_no_replace(guard, &receipt.receipt_id.0)?;
    Ok(PublishedDptObservationV1 { root, receipt })
}
