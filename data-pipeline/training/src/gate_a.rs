//! 门 A 物理产物、类型化读取与原子发布。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_schema::DataType;
use contracts::{
    CANONICAL_EVENT_RECORD_SCHEMA_V1, CanonicalEventId, CanonicalEventRecordV1,
    CanonicalReferenceEventPayloadV1, CanonicalReferenceEventV1, EVENT_TIME_INDEX_SCHEMA_V2,
    EventTimeIndexEntryV2, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3, MarketDataQuality, MarketObservationCausalShellV3,
    ObservationComponentId, QualityScopeV1, ResyncQuality, Sha256Hex, VISIBILITY_INDEX_SCHEMA_V2,
    VisibilityIndexEntryV2,
};
use data_pipeline_core::reduction::{
    REDUCTION_REPORT_SCHEMA_V1, ReductionDispositionV1, ReductionReportEntryV1,
    decode_reduction_report_entry_v1,
};
use data_pipeline_core::{
    ContractError, decode_canonical_event_record_v1, decode_event_time_index_entry,
    decode_market_observation_causal_shell_v3, decode_visibility_index_entry,
    encode_event_time_index_entry, encode_market_observation_causal_shell_v3,
    encode_visibility_index_entry, project_record_event_time_index_entry,
    project_record_visibility_index_entry,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rustix::fs::{Advice, fadvise};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical_run::{
    CanonicalRunCapture, CanonicalRunConfig, CanonicalRunDigests, CanonicalRunError,
    CanonicalRunInputFile, CanonicalRunMetrics, CanonicalRunScope,
    EVENT_TIME_PROJECTION_DIGEST_DOMAIN, RECORD_DIGEST_DOMAIN, REDUCTION_REPORT_DIGEST_DOMAIN,
    StreamDigest, VISIBILITY_PROJECTION_DIGEST_DOMAIN, reduce_frozen_input_with_capture,
};
use crate::observation::{ObservationBuildError, ObservationProjector};
use crate::{
    ExternalSorter, FrozenDirectSourceInputSetV1, RunCursor, SortError, SortMetrics, SortOutcome,
    SortRow, batch_from_rows, scan_logical_output, sort_row_buffer_bytes, sort_row_inner_bytes,
};

pub const GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1: &str = "gate_a_artifact_manifest_v1";
pub const GATE_A_CONTROL_FILE_BYTES_BOUND: u64 = 51 * 1024 * 1024;
const KEY_ENCODING_VERSION_V1: &str = "gate_a_binary_sort_key_v1";
const CAPTURE_RECORD_DIRECTORY: &str = "capture-canonical-records";
const CAPTURE_REPORT_DIRECTORY: &str = "capture-reduction-reports";
const CAPTURE_FILE: &str = "captured.parquet";
const WORK_DIRECTORY: &str = ".work";
const PIPELINE_CORE_SOURCES: [(&str, &[u8]); 18] = [
    ("Cargo.lock", include_bytes!("../../../Cargo.lock")),
    ("Cargo.toml", include_bytes!("../../../Cargo.toml")),
    (
        "contracts/Cargo.toml",
        include_bytes!("../../../contracts/Cargo.toml"),
    ),
    (
        "contracts/src/lib.rs",
        include_bytes!("../../../contracts/src/lib.rs"),
    ),
    (
        "data-pipeline/core/Cargo.toml",
        include_bytes!("../../core/Cargo.toml"),
    ),
    (
        "data-pipeline/core/src/lib.rs",
        include_bytes!("../../core/src/lib.rs"),
    ),
    (
        "data-pipeline/core/src/reduction.rs",
        include_bytes!("../../core/src/reduction.rs"),
    ),
    (
        "data-pipeline/core/src/typed_raw.rs",
        include_bytes!("../../core/src/typed_raw.rs"),
    ),
    (
        "data-pipeline/training/Cargo.toml",
        include_bytes!("../Cargo.toml"),
    ),
    (
        "data-pipeline/training/src/lib.rs",
        include_bytes!("lib.rs"),
    ),
    (
        "data-pipeline/training/src/canonical_run.rs",
        include_bytes!("canonical_run.rs"),
    ),
    (
        "data-pipeline/training/src/typed_raw.rs",
        include_bytes!("typed_raw.rs"),
    ),
    (
        "data-pipeline/training/src/observation.rs",
        include_bytes!("observation.rs"),
    ),
    (
        "data-pipeline/training/src/gate_a.rs",
        include_bytes!("gate_a.rs"),
    ),
    (
        "data-pipeline/training/src/gate_a_publication.rs",
        include_bytes!("gate_a_publication.rs"),
    ),
    (
        "data-pipeline/training/src/dpt_observation_publication.rs",
        include_bytes!("dpt_observation_publication.rs"),
    ),
    (
        "data-pipeline/training/src/dpt_gate_a_cli.rs",
        include_bytes!("dpt_gate_a_cli.rs"),
    ),
    (
        "data-pipeline/training/src/bin/dpt-gate-a.rs",
        include_bytes!("bin/dpt-gate-a.rs"),
    ),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateAArtifactKind {
    CanonicalEventStore,
    EventTimeIndex,
    VisibilityIndex,
    MarketObservations,
    ReductionReports,
}

impl GateAArtifactKind {
    const fn all() -> [Self; 5] {
        [
            Self::CanonicalEventStore,
            Self::EventTimeIndex,
            Self::VisibilityIndex,
            Self::MarketObservations,
            Self::ReductionReports,
        ]
    }

    fn file_stem(self) -> &'static str {
        match self {
            Self::CanonicalEventStore => "canonical-event-store",
            Self::EventTimeIndex => "event-time-index",
            Self::VisibilityIndex => "visibility-index",
            Self::MarketObservations => "market-observations",
            Self::ReductionReports => "reduction-reports",
        }
    }

    fn contract_version(self) -> &'static str {
        match self {
            Self::CanonicalEventStore => CANONICAL_EVENT_RECORD_SCHEMA_V1,
            Self::EventTimeIndex => EVENT_TIME_INDEX_SCHEMA_V2,
            Self::VisibilityIndex => VISIBILITY_INDEX_SCHEMA_V2,
            Self::MarketObservations => MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3,
            Self::ReductionReports => REDUCTION_REPORT_SCHEMA_V1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactFileV1 {
    pub relative_path: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: Sha256Hex,
    pub logical_sha256: Sha256Hex,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactManifestV1 {
    pub schema_version: String,
    pub artifact_kind: GateAArtifactKind,
    pub contract_version: String,
    pub physical_schema_version: String,
    pub key_encoding_version: String,
    pub canonical_rule_version: String,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub canonical_record_stream_sha256: Sha256Hex,
    pub covered_condition_ids: Vec<String>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub dependencies: Vec<GateAArtifactDependencyV1>,
    pub file: GateAArtifactFileV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAArtifactDependencyV1 {
    pub artifact_kind: GateAArtifactKind,
    pub manifest_sha256: Sha256Hex,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateAOutputConfig {
    pub max_artifact_payload_bytes: usize,
    pub max_capture_output_bytes: u64,
    pub max_candidate_directory_bytes: u64,
    pub max_observation_l2_levels: usize,
    pub sort: crate::SortConfig,
}

impl GateAOutputConfig {
    fn validate(&self) -> Result<(), GateAError> {
        if self.max_artifact_payload_bytes == 0 {
            return Err(GateAError::InvalidConfig(
                "门 A 产物单载荷字节上限必须为正数",
            ));
        }
        if self.max_artifact_payload_bytes > self.sort.max_run_bytes {
            return Err(GateAError::InvalidConfig(
                "门 A 产物单载荷上限不得超过排序 run 内存上限",
            ));
        }
        if self.max_capture_output_bytes == 0 || self.max_candidate_directory_bytes == 0 {
            return Err(GateAError::InvalidConfig(
                "门 A 捕获流与候选目录字节上限必须为正数",
            ));
        }
        if self.max_capture_output_bytes > self.max_candidate_directory_bytes {
            return Err(GateAError::InvalidConfig(
                "门 A 捕获流字节上限不得超过候选目录字节上限",
            ));
        }
        if self.max_observation_l2_levels == 0 {
            return Err(GateAError::InvalidConfig(
                "Observation L2 保留条目上限必须为正数",
            ));
        }
        if GATE_A_CONTROL_FILE_BYTES_BOUND > self.max_candidate_directory_bytes {
            return Err(GateAError::InvalidConfig(
                "发布控制文件预算不得超过候选目录字节上限",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateAArtifactDescriptor {
    pub manifest_path: PathBuf,
    pub manifest_sha256: Sha256Hex,
    pub manifest: GateAArtifactManifestV1,
    pub sort_metrics: SortMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateACandidate {
    pub candidate_path: PathBuf,
    pub input_set_identity_sha256: Sha256Hex,
    pub input_manifest_set_sha256: Sha256Hex,
    pub scope: CanonicalRunScope,
    pub config: CanonicalRunConfig,
    pub output_config: GateAOutputConfig,
    pub selected_files: Vec<CanonicalRunInputFile>,
    pub canonical_digests: CanonicalRunDigests,
    pub canonical_metrics: CanonicalRunMetrics,
    pub canonical_event_store: GateAArtifactDescriptor,
    pub event_time_index: GateAArtifactDescriptor,
    pub visibility_index: GateAArtifactDescriptor,
    pub market_observations: GateAArtifactDescriptor,
    pub reduction_reports: GateAArtifactDescriptor,
    pub resource_metrics: GateABuildResourceMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateABuildResourceMetrics {
    pub canonical_record_capture_bytes: u64,
    pub reduction_report_capture_bytes: u64,
    pub visibility_material_sort: SortMetrics,
    pub peak_observation_l2_levels: usize,
    /// 各阶段持久文件与排序临时文件重叠时的保守字节上界，不冒充采样得到的瞬时峰值。
    pub peak_candidate_directory_bytes_bound: u64,
    /// 主/扰动候选、验证临时区和固定发布控制文件预算的正式工作目录保守峰值。
    pub peak_formal_work_directory_bytes_bound: u64,
}

#[derive(Debug)]
pub enum GateAError {
    InvalidConfig(&'static str),
    WorkspaceExists(PathBuf),
    InvalidCapture(&'static str),
    InvalidManifest(String),
    ArtifactMismatch(String),
    Io(io::Error),
    Json(serde_json::Error),
    Sort(SortError),
    Contract(ContractError),
    CanonicalRun(CanonicalRunError),
    Observation(ObservationBuildError),
}

impl fmt::Display for GateAError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "门 A 配置无效：{message}"),
            Self::WorkspaceExists(path) => {
                write!(formatter, "门 A 候选目录已存在：{}", path.display())
            }
            Self::InvalidCapture(message) => write!(formatter, "门 A 捕获产物无效：{message}"),
            Self::InvalidManifest(message) => write!(formatter, "门 A manifest 无效：{message}"),
            Self::ArtifactMismatch(message) => write!(formatter, "门 A 产物不一致：{message}"),
            Self::Io(error) => write!(formatter, "门 A I/O 错误：{error}"),
            Self::Json(error) => write!(formatter, "门 A JSON 错误：{error}"),
            Self::Sort(error) => write!(formatter, "门 A 排序错误：{error}"),
            Self::Contract(error) => write!(formatter, "门 A 合同错误：{error}"),
            Self::CanonicalRun(error) => write!(formatter, "门 A 规范归约错误：{error}"),
            Self::Observation(error) => write!(formatter, "门 A Observation 错误：{error}"),
        }
    }
}

impl std::error::Error for GateAError {}

impl From<io::Error> for GateAError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for GateAError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<SortError> for GateAError {
    fn from(error: SortError) -> Self {
        Self::Sort(error)
    }
}

impl From<ContractError> for GateAError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<CanonicalRunError> for GateAError {
    fn from(error: CanonicalRunError) -> Self {
        Self::CanonicalRun(error)
    }
}

impl From<ObservationBuildError> for GateAError {
    fn from(error: ObservationBuildError) -> Self {
        Self::Observation(error)
    }
}

struct CandidateGuard {
    path: PathBuf,
    retain: bool,
}

impl CandidateGuard {
    fn create(path: &Path) -> Result<Self, GateAError> {
        match fs::create_dir(path) {
            Ok(()) => Ok(Self {
                path: path.to_path_buf(),
                retain: false,
            }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(GateAError::WorkspaceExists(path.to_path_buf()))
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for CandidateGuard {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

struct SharedQuotaWriter {
    file: File,
    total_bytes: Arc<Mutex<u64>>,
    max_total_bytes: u64,
}

impl SharedQuotaWriter {
    fn sync_data_and_drop_cache(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        fadvise(&self.file, 0, None, Advice::DontNeed)
            .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))
    }
}

impl Write for SharedQuotaWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut total = self
            .total_bytes
            .lock()
            .map_err(|_| io::Error::other("捕获产物字节计数锁已损坏"))?;
        let requested =
            u64::try_from(bytes.len()).map_err(|_| io::Error::other("捕获产物写入长度无法表示"))?;
        if total
            .checked_add(requested)
            .is_none_or(|next| next > self.max_total_bytes)
        {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "捕获产物超过冻结的总字节上限",
            ));
        }
        let written = self.file.write(bytes)?;
        *total = total
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("捕获产物字节计数溢出"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub(crate) struct SequentialCaptureWriter {
    stream_name: &'static str,
    sync_batches: bool,
    path: PathBuf,
    writer: ArrowWriter<SharedQuotaWriter>,
    rows: Vec<SortRow>,
    row_inner_bytes: usize,
    max_payload_bytes: usize,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    expected_sequence: u64,
    metrics: SortMetrics,
}

impl SequentialCaptureWriter {
    pub(crate) fn create(
        stream_name: &'static str,
        directory: &Path,
        total_bytes: Arc<Mutex<u64>>,
        output_config: &GateAOutputConfig,
    ) -> Result<Self, GateAError> {
        fs::create_dir(directory)?;
        let path = directory.join(CAPTURE_FILE);
        let file = File::create(&path)?;
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("sort_key", DataType::Binary, false),
            arrow_schema::Field::new("payload", DataType::Binary, false),
        ]));
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(output_config.sort.batch_rows))
            .build();
        let writer = ArrowWriter::try_new(
            SharedQuotaWriter {
                file,
                total_bytes,
                max_total_bytes: output_config.max_capture_output_bytes,
            },
            schema,
            Some(properties),
        )
        .map_err(|error| GateAError::Io(io::Error::other(error)))?;
        Ok(Self {
            stream_name,
            sync_batches: true,
            path,
            writer,
            rows: Vec::with_capacity(output_config.sort.batch_rows),
            row_inner_bytes: 0,
            max_payload_bytes: output_config.max_artifact_payload_bytes,
            max_batch_rows: output_config.sort.batch_rows,
            max_batch_bytes: output_config.sort.max_run_bytes,
            expected_sequence: 0,
            metrics: SortMetrics::default(),
        })
    }

    pub(crate) fn push(&mut self, sequence: u64, key: Vec<u8>, payload: &[u8]) -> io::Result<()> {
        if sequence != self.expected_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "捕获产物发射位置必须从零开始严格连续",
            ));
        }
        if payload.len() > self.max_payload_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "捕获产物流 {} 的载荷超过冻结的单行上限：observed={} limit={}",
                    self.stream_name,
                    payload.len(),
                    self.max_payload_bytes
                ),
            ));
        }
        let row = SortRow::from_sort_key(key, payload.to_vec());
        let mut prospective_bytes = sort_row_buffer_bytes(&self.rows, self.row_inner_bytes)
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?));
        if !self.rows.is_empty()
            && (self.rows.len() >= self.max_batch_rows
                || prospective_bytes.is_none_or(|bytes| bytes > self.max_batch_bytes))
        {
            self.flush_batch()?;
            prospective_bytes = sort_row_buffer_bytes(&self.rows, self.row_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?));
        }
        let observed = prospective_bytes.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "捕获产物行分配字节数溢出")
        })?;
        if observed > self.max_batch_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "捕获产物流 {} 的行分配超过冻结的写入批次字节上限：observed={} limit={}",
                    self.stream_name, observed, self.max_batch_bytes
                ),
            ));
        }
        self.row_inner_bytes += sort_row_inner_bytes(&row).expect("validated row capacity");
        self.rows.push(row);
        self.expected_sequence = self
            .expected_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("捕获产物发射位置溢出"))?;
        self.metrics.input_rows = self.expected_sequence;
        self.metrics.output_rows = self.expected_sequence;
        self.metrics.peak_write_buffer_rows =
            self.metrics.peak_write_buffer_rows.max(self.rows.len());
        self.metrics.peak_write_buffer_bytes = self.metrics.peak_write_buffer_bytes.max(observed);
        self.metrics.peak_run_rows = self.metrics.peak_run_rows.max(self.rows.len());
        self.metrics.peak_run_bytes = self.metrics.peak_run_bytes.max(observed);
        Ok(())
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        self.write_batch(self.sync_batches)
    }

    fn write_batch(&mut self, sync: bool) -> io::Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = batch_from_rows(&self.rows).map_err(io::Error::other)?;
        self.writer.write(&batch).map_err(io::Error::other)?;
        self.writer.flush().map_err(io::Error::other)?;
        if sync {
            self.writer.sync()?;
            self.writer.inner_mut().sync_data_and_drop_cache()?;
        }
        self.metrics.initial_runs = self
            .metrics
            .initial_runs
            .checked_add(1)
            .ok_or_else(|| io::Error::other("捕获产物行组计数溢出"))?;
        self.rows.clear();
        self.row_inner_bytes = 0;
        Ok(())
    }

    /// 新后台批次在显式稳定边界关闭；不执行旧离线逻辑摘要扫描。
    pub(crate) fn defer_batch_sync(&mut self) {
        self.sync_batches = false;
    }
    pub(crate) fn close_segment(mut self) -> Result<PathBuf, GateAError> {
        self.write_batch(false)?;
        self.writer
            .close()
            .map_err(|e| GateAError::Io(io::Error::other(e)))?;
        Ok(self.path)
    }

    fn finish(mut self, output_config: &GateAOutputConfig) -> Result<SortOutcome, GateAError> {
        self.flush_batch()?;
        self.writer
            .close()
            .map_err(|error| GateAError::Io(io::Error::other(error)))?;
        File::open(&self.path)?.sync_all()?;
        let (logical_sha256, rows) = scan_logical_output(
            &self.path,
            output_config.sort.batch_rows,
            output_config.sort.max_run_bytes,
        )?;
        if rows != self.metrics.output_rows {
            return Err(GateAError::ArtifactMismatch(
                "捕获产物关闭后的行数不守恒".to_owned(),
            ));
        }
        Ok(SortOutcome {
            output_path: self.path,
            logical_sha256,
            metrics: self.metrics,
        })
    }
}

struct CaptureArtifacts {
    records: SequentialCaptureWriter,
    reports: SequentialCaptureWriter,
    total_bytes: Arc<Mutex<u64>>,
}

impl CaptureArtifacts {
    fn create(work_path: &Path, output_config: &GateAOutputConfig) -> Result<Self, GateAError> {
        fs::create_dir_all(work_path)?;
        let total_bytes = Arc::new(Mutex::new(0));
        Ok(Self {
            records: SequentialCaptureWriter::create(
                "canonical_records",
                &work_path.join(CAPTURE_RECORD_DIRECTORY),
                Arc::clone(&total_bytes),
                output_config,
            )?,
            reports: SequentialCaptureWriter::create(
                "reduction_reports",
                &work_path.join(CAPTURE_REPORT_DIRECTORY),
                Arc::clone(&total_bytes),
                output_config,
            )?,
            total_bytes,
        })
    }

    fn finish(
        self,
        output_config: &GateAOutputConfig,
    ) -> Result<(SortOutcome, SortOutcome, u64), GateAError> {
        let total_bytes = Arc::clone(&self.total_bytes);
        let records = self.records.finish(output_config)?;
        let reports = self.reports.finish(output_config)?;
        let total = *total_bytes
            .lock()
            .map_err(|_| GateAError::InvalidCapture("捕获产物字节计数锁已损坏"))?;
        Ok((records, reports, total))
    }
}

impl CanonicalRunCapture for CaptureArtifacts {
    fn capture_record(&mut self, emission_sequence: u64, encoded: &[u8]) -> io::Result<()> {
        self.records.push(
            emission_sequence,
            emission_sequence.to_be_bytes().to_vec(),
            encoded,
        )
    }

    fn capture_report(&mut self, emission_sequence: u64, encoded: &[u8]) -> io::Result<()> {
        self.reports.push(
            emission_sequence,
            emission_sequence.to_be_bytes().to_vec(),
            encoded,
        )
    }
}

/// 只运行一次规范归约，并从同一捕获流构建门 A 候选四项主数据产物及归约报告。
pub fn build_gate_a_candidate(
    input: &FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    candidate_path: &Path,
    config: &CanonicalRunConfig,
    output_config: &GateAOutputConfig,
) -> Result<GateACandidate, GateAError> {
    if candidate_path.parent().is_none() {
        return Err(GateAError::InvalidConfig("候选目录必须有明确父目录"));
    }
    output_config.validate()?;
    let initial_workspace_limit = config
        .sort
        .max_temp_bytes
        .checked_add(output_config.max_capture_output_bytes)
        .and_then(|value| value.checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND))
        .ok_or(GateAError::InvalidConfig("门 A 初始工作区字节上限溢出"))?;
    if initial_workspace_limit > output_config.max_candidate_directory_bytes {
        return Err(GateAError::InvalidConfig(
            "规范输入排序与捕获流上限之和超过候选目录上限",
        ));
    }
    let mut guard = CandidateGuard::create(candidate_path)?;
    let work_path = candidate_path.join(WORK_DIRECTORY);
    let mut capture = CaptureArtifacts::create(&work_path, output_config)?;
    let staging = reduce_frozen_input_with_capture(input, scope, &work_path, config, &mut capture)?;
    let (record_capture, report_capture, total_capture_bytes) = capture.finish(output_config)?;
    let canonical_record_capture_bytes = fs::metadata(&record_capture.output_path)?.len();
    let reduction_report_capture_bytes = fs::metadata(&report_capture.output_path)?.len();
    if canonical_record_capture_bytes.checked_add(reduction_report_capture_bytes)
        != Some(total_capture_bytes)
    {
        return Err(GateAError::ArtifactMismatch(
            "捕获产物共享字节计数与文件大小不一致".to_owned(),
        ));
    }
    let initial_observed = directory_bytes(candidate_path)?;
    ensure_candidate_directory_limit(candidate_path, output_config, 0)?;
    let initial_sort_and_capture_bound = total_capture_bytes
        .checked_add(staging.metrics.sort.peak_temp_bytes)
        .ok_or_else(|| GateAError::ArtifactMismatch("候选目录峰值上界溢出".to_owned()))?;
    let mut peak_candidate_directory_bytes_bound =
        initial_observed.max(initial_sort_and_capture_bound);
    fs::remove_dir_all(&staging.workspace_path)?;
    let base_bytes = directory_bytes(candidate_path)?;
    let store = finish_artifact(
        record_capture,
        &work_path.join(CAPTURE_RECORD_DIRECTORY),
        candidate_path,
        GateAArtifactKind::CanonicalEventStore,
        &staging,
        Vec::new(),
        output_config,
    )?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &store.sort_metrics,
        candidate_path,
    )?;
    let store_dependency = GateAArtifactDependencyV1 {
        artifact_kind: GateAArtifactKind::CanonicalEventStore,
        manifest_sha256: store.manifest_sha256.clone(),
    };
    let base_bytes = directory_bytes(candidate_path)?;
    ensure_candidate_directory_limit(
        candidate_path,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let event_time = build_event_time_index(
        &candidate_path.join(&store.manifest.file.relative_path),
        candidate_path,
        &staging,
        output_config,
        vec![store_dependency.clone()],
    )?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &event_time.sort_metrics,
        candidate_path,
    )?;
    let store_path = candidate_path.join(&store.manifest.file.relative_path);
    let base_bytes = directory_bytes(candidate_path)?;
    ensure_candidate_directory_limit(
        candidate_path,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let (visibility_material, visibility_material_sort) =
        build_visibility_material(&store_path, &work_path, output_config, &staging)?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &visibility_material_sort,
        candidate_path,
    )?;
    let base_bytes = directory_bytes(candidate_path)?;
    ensure_candidate_directory_limit(
        candidate_path,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let visibility = build_visibility_index(
        &visibility_material,
        candidate_path,
        &staging,
        output_config,
        vec![store_dependency.clone()],
    )?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &visibility.sort_metrics,
        candidate_path,
    )?;
    let base_bytes = directory_bytes(candidate_path)?;
    ensure_candidate_directory_limit(
        candidate_path,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let (observations, peak_observation_l2_levels) = build_observations(
        &visibility_material,
        candidate_path,
        &staging,
        output_config,
        vec![
            store_dependency,
            GateAArtifactDependencyV1 {
                artifact_kind: GateAArtifactKind::VisibilityIndex,
                manifest_sha256: visibility.manifest_sha256.clone(),
            },
        ],
        &visibility.manifest_sha256,
    )?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &observations.sort_metrics,
        candidate_path,
    )?;
    let base_bytes = directory_bytes(candidate_path)?;
    let reports = finish_artifact(
        report_capture,
        &work_path.join(CAPTURE_REPORT_DIRECTORY),
        candidate_path,
        GateAArtifactKind::ReductionReports,
        &staging,
        vec![GateAArtifactDependencyV1 {
            artifact_kind: GateAArtifactKind::CanonicalEventStore,
            manifest_sha256: store.manifest_sha256.clone(),
        }],
        output_config,
    )?;
    observe_sort_stage_peak(
        &mut peak_candidate_directory_bytes_bound,
        base_bytes,
        &reports.sort_metrics,
        candidate_path,
    )?;

    if store.manifest.file.rows != staging.metrics.canonical_records
        || event_time.manifest.file.rows != staging.metrics.event_time_projections
        || visibility.manifest.file.rows != staging.metrics.visibility_projections
        || reports.manifest.file.rows != staging.metrics.reduction_reports
    {
        return Err(GateAError::ArtifactMismatch(
            "物化行数与唯一规范归约计数不一致".to_owned(),
        ));
    }
    fs::remove_dir_all(&work_path)?;

    let candidate = GateACandidate {
        candidate_path: candidate_path.to_path_buf(),
        input_set_identity_sha256: Sha256Hex(staging.input_set_identity_sha256),
        input_manifest_set_sha256: Sha256Hex(staging.input_manifest_set_sha256),
        scope: staging.scope,
        config: staging.config,
        output_config: output_config.clone(),
        selected_files: staging.selected_files,
        canonical_digests: staging.digests,
        canonical_metrics: staging.metrics,
        canonical_event_store: store,
        event_time_index: event_time,
        visibility_index: visibility,
        market_observations: observations,
        reduction_reports: reports,
        resource_metrics: GateABuildResourceMetrics {
            canonical_record_capture_bytes,
            reduction_report_capture_bytes,
            visibility_material_sort,
            peak_observation_l2_levels,
            peak_candidate_directory_bytes_bound,
            peak_formal_work_directory_bytes_bound: peak_candidate_directory_bytes_bound
                .checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND)
                .ok_or_else(|| {
                    GateAError::ArtifactMismatch("正式工作目录峰值上界溢出".to_owned())
                })?,
        },
    };
    guard.retain = true;
    Ok(candidate)
}

fn build_event_time_index(
    store_path: &Path,
    candidate_path: &Path,
    staging: &crate::canonical_run::CanonicalRunStaging,
    output_config: &GateAOutputConfig,
    dependencies: Vec<GateAArtifactDependencyV1>,
) -> Result<GateAArtifactDescriptor, GateAError> {
    let stage_root = candidate_path.join(WORK_DIRECTORY).join("sort-event-time");
    fs::create_dir(&stage_root)?;
    let mut sorter = ExternalSorter::new(&stage_root, &output_config.sort)?;
    let mut store = RunCursor::open(
        store_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    while let Some(row) = store.next_row()? {
        let sequence = sequence_from_emission_key(row.sort_key_bytes())?;
        let record = decode_canonical_event_record_v1(row.payload())?;
        if let Some(entry) = project_record_event_time_index_entry(&record)? {
            sorter.push(bounded_sort_row(
                time_sequence_key(entry.source_event_at_utc_ms, sequence),
                encode_event_time_index_entry(&entry)?,
                output_config,
            )?)?;
        }
    }
    finish_artifact(
        sorter.finish()?,
        &stage_root,
        candidate_path,
        GateAArtifactKind::EventTimeIndex,
        staging,
        dependencies,
        output_config,
    )
}

fn build_visibility_material(
    store_path: &Path,
    work_path: &Path,
    output_config: &GateAOutputConfig,
    staging: &crate::canonical_run::CanonicalRunStaging,
) -> Result<(PathBuf, SortMetrics), GateAError> {
    let stage_root = work_path.join("sort-visibility-material");
    fs::create_dir(&stage_root)?;
    let mut sorter = ExternalSorter::new(&stage_root, &output_config.sort)?;
    let mut store = RunCursor::open(
        store_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut rows = 0u64;
    while let Some(row) = store.next_row()? {
        if row.sort_key_bytes() != rows.to_be_bytes() {
            return Err(GateAError::ArtifactMismatch(
                "唯一事件存储发射位置不连续".to_owned(),
            ));
        }
        let record = decode_canonical_event_record_v1(row.payload())?;
        sorter.push(bounded_sort_row(
            time_sequence_key(record_received_at(&record), rows),
            row.payload().to_vec(),
            output_config,
        )?)?;
        rows = rows
            .checked_add(1)
            .ok_or_else(|| GateAError::ArtifactMismatch("模型可见物化行数溢出".to_owned()))?;
    }
    let outcome = sorter.finish()?;
    if rows != staging.metrics.visibility_projections || outcome.metrics.output_rows != rows {
        return Err(GateAError::ArtifactMismatch(
            "模型可见中间流与规范投影计数不一致".to_owned(),
        ));
    }
    let destination = work_path.join("visibility-material.parquet");
    fs::rename(&outcome.output_path, &destination)?;
    fs::remove_dir_all(&stage_root)?;
    Ok((destination, outcome.metrics))
}

fn build_visibility_index(
    material_path: &Path,
    candidate_path: &Path,
    staging: &crate::canonical_run::CanonicalRunStaging,
    output_config: &GateAOutputConfig,
    dependencies: Vec<GateAArtifactDependencyV1>,
) -> Result<GateAArtifactDescriptor, GateAError> {
    let stage_root = candidate_path
        .join(WORK_DIRECTORY)
        .join("sort-visibility-index");
    fs::create_dir(&stage_root)?;
    let mut sorter = ExternalSorter::new(&stage_root, &output_config.sort)?;
    let mut material = RunCursor::open(
        material_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    while let Some(row) = material.next_row()? {
        let record = decode_canonical_event_record_v1(row.payload())?;
        let entry = project_record_visibility_index_entry(&record)?;
        sorter.push(bounded_sort_row(
            row.sort_key_bytes().to_vec(),
            encode_visibility_index_entry(&entry)?,
            output_config,
        )?)?;
    }
    finish_artifact(
        sorter.finish()?,
        &stage_root,
        candidate_path,
        GateAArtifactKind::VisibilityIndex,
        staging,
        dependencies,
        output_config,
    )
}

fn build_observations(
    material_path: &Path,
    candidate_path: &Path,
    staging: &crate::canonical_run::CanonicalRunStaging,
    output_config: &GateAOutputConfig,
    dependencies: Vec<GateAArtifactDependencyV1>,
    visibility_manifest_sha256: &Sha256Hex,
) -> Result<(GateAArtifactDescriptor, usize), GateAError> {
    let stage_root = candidate_path
        .join(WORK_DIRECTORY)
        .join("sort-observations");
    fs::create_dir(&stage_root)?;
    let mut sorter = ExternalSorter::new(&stage_root, &output_config.sort)?;
    let mut material = RunCursor::open(
        material_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut projector = ObservationProjector::new(
        staging.scope.condition_id(),
        Sha256Hex(staging.input_set_identity_sha256.clone()),
        visibility_manifest_sha256.clone(),
        output_config.max_observation_l2_levels,
    );
    while let Some(row) = material.next_row()? {
        let record = decode_canonical_event_record_v1(row.payload())?;
        if let Some(projected) = projector.push(&record)? {
            push_observation(&mut sorter, projected.observation, output_config)?;
        }
    }
    if let Some(projected) = projector.finish()? {
        push_observation(&mut sorter, projected.observation, output_config)?;
    }
    let peak_retained_l2_levels = projector.peak_retained_l2_levels();
    let descriptor = finish_artifact(
        sorter.finish()?,
        &stage_root,
        candidate_path,
        GateAArtifactKind::MarketObservations,
        staging,
        dependencies,
        output_config,
    )?;
    Ok((descriptor, peak_retained_l2_levels))
}

fn push_observation(
    sorter: &mut ExternalSorter,
    observation: MarketObservationCausalShellV3,
    output_config: &GateAOutputConfig,
) -> Result<(), GateAError> {
    let key = observation_key(
        observation.visibility_cutoff.received_at_utc_ms,
        &observation.market.condition_id.0,
    )?;
    sorter.push(bounded_sort_row(
        key,
        encode_market_observation_causal_shell_v3(&observation)?,
        output_config,
    )?)?;
    Ok(())
}

fn finish_artifact(
    outcome: SortOutcome,
    stage_root: &Path,
    candidate_path: &Path,
    kind: GateAArtifactKind,
    staging: &crate::canonical_run::CanonicalRunStaging,
    dependencies: Vec<GateAArtifactDependencyV1>,
    output_config: &GateAOutputConfig,
) -> Result<GateAArtifactDescriptor, GateAError> {
    let data_directory = candidate_path.join("data");
    let manifest_directory = candidate_path.join("manifests");
    fs::create_dir_all(&data_directory)?;
    fs::create_dir_all(&manifest_directory)?;
    let relative_path = PathBuf::from("data").join(format!("{}.parquet", kind.file_stem()));
    let artifact_path = candidate_path.join(&relative_path);
    fs::rename(&outcome.output_path, &artifact_path)?;
    fs::remove_dir_all(stage_root)?;
    File::open(&artifact_path)?.sync_all()?;
    sync_directory(&data_directory)?;

    let metadata = fs::metadata(&artifact_path)?;
    let manifest = GateAArtifactManifestV1 {
        schema_version: GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1.to_owned(),
        artifact_kind: kind,
        contract_version: kind.contract_version().to_owned(),
        physical_schema_version: "binary_sort_key_payload_parquet_v1".to_owned(),
        key_encoding_version: KEY_ENCODING_VERSION_V1.to_owned(),
        canonical_rule_version: staging.config.canonical_rule_version.clone(),
        input_set_identity_sha256: Sha256Hex(staging.input_set_identity_sha256.clone()),
        input_manifest_set_sha256: Sha256Hex(staging.input_manifest_set_sha256.clone()),
        canonical_record_stream_sha256: Sha256Hex(staging.digests.canonical_records_sha256.clone()),
        covered_condition_ids: vec![staging.scope.condition_id().to_owned()],
        received_start_utc_ms: staging.scope.received_start_utc_ms(),
        received_end_utc_ms: staging.scope.received_end_utc_ms(),
        dependencies,
        file: GateAArtifactFileV1 {
            relative_path: relative_path
                .to_str()
                .ok_or(GateAError::InvalidManifest(
                    "产物相对路径不是 UTF-8".to_owned(),
                ))?
                .to_owned(),
            rows: outcome.metrics.output_rows,
            bytes: metadata.len(),
            sha256: Sha256Hex(sha256_file(&artifact_path)?),
            logical_sha256: Sha256Hex(outcome.logical_sha256),
        },
    };
    let manifest_path = manifest_directory.join(format!("{}.json", kind.file_stem()));
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    ensure_candidate_directory_limit(
        candidate_path,
        output_config,
        u64::try_from(manifest_bytes.len())
            .map_err(|_| GateAError::InvalidManifest("manifest 字节数无法表示".to_owned()))?,
    )?;
    write_new_file(&manifest_path, &manifest_bytes)?;
    sync_directory(&manifest_directory)?;
    let manifest_sha256 = Sha256Hex(sha256_bytes(&manifest_bytes));
    Ok(GateAArtifactDescriptor {
        manifest_path,
        manifest_sha256,
        manifest,
        sort_metrics: outcome.metrics,
    })
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), GateAError> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), GateAError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn directory_bytes(path: &Path) -> Result<u64, GateAError> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            return Err(GateAError::ArtifactMismatch(
                "候选目录不得包含符号链接".to_owned(),
            ));
        }
        let bytes = if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else if metadata.is_file() {
            entry.metadata()?.len()
        } else {
            return Err(GateAError::ArtifactMismatch(
                "候选目录只允许普通文件和目录".to_owned(),
            ));
        };
        total = total
            .checked_add(bytes)
            .ok_or_else(|| GateAError::ArtifactMismatch("候选目录字节数溢出".to_owned()))?;
    }
    Ok(total)
}

fn ensure_candidate_directory_limit(
    candidate_path: &Path,
    output_config: &GateAOutputConfig,
    reserved_bytes: u64,
) -> Result<(), GateAError> {
    let reserved_bytes = reserved_bytes
        .checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND)
        .ok_or_else(|| GateAError::ArtifactMismatch("候选目录预算字节数溢出".to_owned()))?;
    ensure_directory_limit(
        candidate_path,
        output_config.max_candidate_directory_bytes,
        reserved_bytes,
        "候选目录",
    )
}

fn ensure_directory_limit(
    path: &Path,
    max_bytes: u64,
    reserved_bytes: u64,
    description: &str,
) -> Result<(), GateAError> {
    let observed = directory_bytes(path)?;
    let required = observed
        .checked_add(reserved_bytes)
        .ok_or_else(|| GateAError::ArtifactMismatch(format!("{description}预算字节数溢出")))?;
    if required > max_bytes {
        return Err(GateAError::ArtifactMismatch(format!(
            "{description}需要 {required} 字节，超过冻结上限 {max_bytes}"
        )));
    }
    Ok(())
}

fn require_validation_capacity(
    validation_work_path: &Path,
    candidate_directory_bytes: u64,
    output_config: &GateAOutputConfig,
    reserved_bytes: u64,
) -> Result<(), GateAError> {
    let validation_bytes = directory_bytes(validation_work_path)?;
    let required = candidate_directory_bytes
        .checked_add(validation_bytes)
        .and_then(|value| value.checked_add(reserved_bytes))
        .and_then(|value| value.checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND))
        .ok_or_else(|| GateAError::ArtifactMismatch("正式工作目录预算字节数溢出".to_owned()))?;
    if required > output_config.max_candidate_directory_bytes {
        return Err(GateAError::ArtifactMismatch(format!(
            "候选、验证临时区与发布控制文件需要 {required} 字节，超过冻结上限 {}",
            output_config.max_candidate_directory_bytes
        )));
    }
    Ok(())
}

fn observe_validation_stage_peak(
    peak_bytes: &mut u64,
    candidate_directory_bytes: u64,
    validation_base_bytes: u64,
    metrics: &SortMetrics,
    validation_work_path: &Path,
    output_config: &GateAOutputConfig,
) -> Result<(), GateAError> {
    let stage_bound = candidate_directory_bytes
        .checked_add(validation_base_bytes)
        .and_then(|value| value.checked_add(metrics.peak_temp_bytes))
        .and_then(|value| value.checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND))
        .ok_or_else(|| GateAError::ArtifactMismatch("正式工作目录峰值上界溢出".to_owned()))?;
    let observed_bound = candidate_directory_bytes
        .checked_add(directory_bytes(validation_work_path)?)
        .and_then(|value| value.checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND))
        .ok_or_else(|| GateAError::ArtifactMismatch("正式工作目录峰值上界溢出".to_owned()))?;
    *peak_bytes = (*peak_bytes).max(stage_bound).max(observed_bound);
    if *peak_bytes > output_config.max_candidate_directory_bytes {
        return Err(GateAError::ArtifactMismatch(format!(
            "正式工作目录峰值上界 {} 字节，超过冻结上限 {}",
            *peak_bytes, output_config.max_candidate_directory_bytes
        )));
    }
    Ok(())
}

fn bounded_sort_row(
    sort_key: Vec<u8>,
    payload: Vec<u8>,
    output_config: &GateAOutputConfig,
) -> Result<SortRow, GateAError> {
    if payload.len() > output_config.max_artifact_payload_bytes {
        return Err(GateAError::ArtifactMismatch(format!(
            "产物载荷 {} 字节，超过冻结上限 {}",
            payload.len(),
            output_config.max_artifact_payload_bytes
        )));
    }
    Ok(SortRow::from_sort_key(sort_key, payload))
}

fn observe_sort_stage_peak(
    peak: &mut u64,
    base_bytes: u64,
    metrics: &SortMetrics,
    candidate_path: &Path,
) -> Result<(), GateAError> {
    let estimated = base_bytes
        .checked_add(metrics.peak_temp_bytes)
        .ok_or_else(|| GateAError::ArtifactMismatch("候选目录峰值字节数溢出".to_owned()))?;
    *peak = (*peak).max(estimated).max(directory_bytes(candidate_path)?);
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, GateAError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = format!("{:x}", hasher.finalize());
    fadvise(&file, 0, None, Advice::DontNeed)
        .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))?;
    Ok(digest)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn pipeline_core_identity_sha256() -> Sha256Hex {
    pipeline_core_identity_from_sources(PIPELINE_CORE_SOURCES.iter().copied())
}

pub fn workspace_pipeline_core_identity_sha256(workspace: &Path) -> Result<Sha256Hex, GateAError> {
    let mut sources = Vec::with_capacity(PIPELINE_CORE_SOURCES.len());
    for (relative_path, _) in PIPELINE_CORE_SOURCES {
        let path = workspace.join(relative_path);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(GateAError::InvalidManifest(format!(
                "管道身份来源不是普通文件：{relative_path}"
            )));
        }
        sources.push((relative_path, fs::read(path)?));
    }
    Ok(pipeline_core_identity_from_sources(sources.iter().map(
        |(relative_path, bytes)| (*relative_path, bytes.as_slice()),
    )))
}

fn pipeline_core_identity_from_sources<'a>(
    sources: impl ExactSizeIterator<Item = (&'a str, &'a [u8])>,
) -> Sha256Hex {
    let mut hasher = Sha256::new();
    hasher.update(b"data-pipeline/training/pipeline-core-identity/v1\0");
    hasher.update((sources.len() as u64).to_be_bytes());
    for (path, bytes) in sources {
        update_length_prefixed_digest(&mut hasher, path.as_bytes());
        update_length_prefixed_digest(&mut hasher, bytes);
    }
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

pub fn supported_schema_set_identity_sha256() -> Sha256Hex {
    const SOURCE_KINDS: [&str; 6] = [
        "binance_aggtrade",
        "binance_depth_snapshot",
        "binance_depth_update",
        "polymarket_market",
        "polymarket_market_metadata",
        "polymarket_rtds_chainlink_btc_usd",
    ];
    let mut hasher = Sha256::new();
    hasher.update(b"data-pipeline/training/supported-schema-set/v1\0");
    hasher.update((SOURCE_KINDS.len() as u64).to_be_bytes());
    for source_kind in SOURCE_KINDS {
        let schema = crate::supported_schema_for_source_kind(source_kind)
            .expect("固定六类来源必须都有唯一受支持 schema");
        update_length_prefixed_digest(&mut hasher, source_kind.as_bytes());
        update_length_prefixed_digest(&mut hasher, schema.as_bytes());
    }
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

fn update_length_prefixed_digest(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn record_event_id(record: &CanonicalEventRecordV1) -> &CanonicalEventId {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => &event.event_id,
        CanonicalEventRecordV1::ReferenceEvent(event) => &event.event_id,
    }
}

fn record_received_at(record: &CanonicalEventRecordV1) -> i64 {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => event.time.received_at_utc_ms,
        CanonicalEventRecordV1::ReferenceEvent(event) => event.time.received_at_utc_ms,
    }
}

fn ordered_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1u64 << 63)).to_be_bytes()
}

fn time_sequence_key(time: i64, sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(1);
    key.extend_from_slice(&ordered_i64(time));
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn sequence_from_emission_key(key: &[u8]) -> Result<u64, GateAError> {
    if key.len() != 8 {
        return Err(GateAError::ArtifactMismatch(
            "唯一事件存储私有发射位置键无效".to_owned(),
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(key);
    Ok(u64::from_be_bytes(bytes))
}

fn event_id_sort_key(event_id: &CanonicalEventId) -> Vec<u8> {
    let mut key = Vec::with_capacity(65);
    key.push(1);
    key.extend_from_slice(event_id.0.as_bytes());
    key
}

fn event_reference_key(event_id: &CanonicalEventId, sequence: u64) -> Vec<u8> {
    let mut key = event_id_sort_key(event_id);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn observation_key(time: i64, condition_id: &str) -> Result<Vec<u8>, GateAError> {
    let condition_length = u32::try_from(condition_id.len())
        .map_err(|_| GateAError::InvalidConfig("condition_id 长度无法编码"))?;
    let mut key = Vec::with_capacity(1 + 8 + 4 + condition_id.len());
    key.push(1);
    key.extend_from_slice(&ordered_i64(time));
    key.extend_from_slice(&condition_length.to_be_bytes());
    key.extend_from_slice(condition_id.as_bytes());
    Ok(key)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedGateAArtifact {
    root: PathBuf,
    manifest_path: PathBuf,
    manifest_sha256: Sha256Hex,
    manifest: GateAArtifactManifestV1,
    artifact_path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum GateAProjectionWitnessKind {
    MarketLifecycle,
    InitialL2,
    LevelExecution,
    BestBidAsk,
    DynamicTick,
    PublicTrade,
    GapResync,
    MarketTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GateAProjectionWitness {
    pub(crate) event_id: CanonicalEventId,
    pub(crate) observation_id: Sha256Hex,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GateAProjectionWitnesses {
    pub(crate) linked: BTreeMap<GateAProjectionWitnessKind, GateAProjectionWitness>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectionWitnessCandidate {
    event_id: CanonicalEventId,
    primary_kind: Option<GateAProjectionWitnessKind>,
    gap_resync: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedGateACandidate {
    pub root: PathBuf,
    pub canonical_event_store: ValidatedGateAArtifact,
    pub event_time_index: ValidatedGateAArtifact,
    pub visibility_index: ValidatedGateAArtifact,
    pub market_observations: ValidatedGateAArtifact,
    pub reduction_reports: ValidatedGateAArtifact,
    pub recomputed_stream_digests: CanonicalRunDigests,
    pub peak_observation_l2_levels: usize,
    pub peak_formal_work_directory_bytes_bound: u64,
    pub(crate) projection_witnesses: GateAProjectionWitnesses,
}

impl ValidatedGateAArtifact {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        &self.manifest_sha256
    }

    pub fn manifest(&self) -> &GateAArtifactManifestV1 {
        &self.manifest
    }

    pub fn artifact_path(&self) -> &Path {
        &self.artifact_path
    }
}

/// 完整复核一个产物 manifest、物理文件、逻辑摘要、键与合同载荷。
pub fn validate_gate_a_artifact(
    manifest_path: &Path,
    expected_kind: GateAArtifactKind,
    batch_rows: usize,
    max_batch_bytes: usize,
) -> Result<ValidatedGateAArtifact, GateAError> {
    if batch_rows == 0 || max_batch_bytes == 0 {
        return Err(GateAError::InvalidConfig(
            "产物读取批次行数和字节上限必须为正数",
        ));
    }
    let manifest_metadata = fs::symlink_metadata(manifest_path)?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(GateAError::InvalidManifest(
            "artifact manifest 必须是普通文件且不得为符号链接".to_owned(),
        ));
    }
    if manifest_metadata.len() > 1024 * 1024 {
        return Err(GateAError::InvalidManifest(
            "artifact manifest 超过 1 MiB".to_owned(),
        ));
    }
    let manifest_bytes = fs::read(manifest_path)?;
    let manifest: GateAArtifactManifestV1 = serde_json::from_slice(&manifest_bytes)?;
    validate_manifest_fields(&manifest, expected_kind)?;
    let manifest_directory = manifest_path
        .parent()
        .ok_or_else(|| GateAError::InvalidManifest("manifest 路径缺少父目录".to_owned()))?;
    let root = manifest_directory.parent().ok_or_else(|| {
        GateAError::InvalidManifest("manifest 必须位于候选根的 manifests 目录".to_owned())
    })?;
    if manifest_directory
        .file_name()
        .and_then(|value| value.to_str())
        != Some("manifests")
    {
        return Err(GateAError::InvalidManifest(
            "manifest 必须位于 manifests 目录".to_owned(),
        ));
    }
    let expected_manifest_name = format!("{}.json", expected_kind.file_stem());
    if manifest_path.file_name().and_then(|value| value.to_str())
        != Some(expected_manifest_name.as_str())
    {
        return Err(GateAError::InvalidManifest(
            "manifest 文件名不匹配".to_owned(),
        ));
    }
    let relative = Path::new(&manifest.file.relative_path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(GateAError::InvalidManifest(
            "产物路径必须是无跳转的相对路径".to_owned(),
        ));
    }
    let canonical_root = fs::canonicalize(root)?;
    let artifact_path = root.join(relative);
    let artifact_metadata = fs::symlink_metadata(&artifact_path)?;
    if artifact_metadata.file_type().is_symlink() || !artifact_metadata.is_file() {
        return Err(GateAError::InvalidManifest(
            "产物必须是普通文件且不得为符号链接".to_owned(),
        ));
    }
    let canonical_artifact = fs::canonicalize(&artifact_path)?;
    if !canonical_artifact.starts_with(&canonical_root) {
        return Err(GateAError::InvalidManifest(
            "产物解析路径逃逸候选根".to_owned(),
        ));
    }
    let metadata = fs::metadata(&canonical_artifact)?;
    if metadata.len() != manifest.file.bytes {
        return Err(GateAError::ArtifactMismatch(
            "产物文件字节数与 manifest 不一致".to_owned(),
        ));
    }
    if sha256_file(&canonical_artifact)? != manifest.file.sha256.0 {
        return Err(GateAError::ArtifactMismatch(
            "产物文件 SHA-256 与 manifest 不一致".to_owned(),
        ));
    }
    validate_artifact_physical_format(&canonical_artifact, manifest.file.rows)?;
    let (logical_sha256, rows) =
        scan_logical_output(&canonical_artifact, batch_rows, max_batch_bytes)?;
    if logical_sha256 != manifest.file.logical_sha256.0 || rows != manifest.file.rows {
        return Err(GateAError::ArtifactMismatch(
            "产物逻辑摘要或行数与 manifest 不一致".to_owned(),
        ));
    }
    validate_artifact_rows(&canonical_artifact, &manifest, batch_rows, max_batch_bytes)?;
    Ok(ValidatedGateAArtifact {
        root: canonical_root,
        manifest_path: fs::canonicalize(manifest_path)?,
        manifest_sha256: Sha256Hex(sha256_bytes(&manifest_bytes)),
        manifest,
        artifact_path: canonical_artifact,
    })
}

/// 对候选目录执行五份物理产物及其跨产物引用的完整闭包复核。
pub fn validate_gate_a_candidate(
    candidate_path: &Path,
    validation_work_path: &Path,
    output_config: &GateAOutputConfig,
) -> Result<ValidatedGateACandidate, GateAError> {
    output_config.validate()?;
    validate_candidate_data_layout(candidate_path)?;
    ensure_candidate_directory_limit(candidate_path, output_config, 0)?;

    let artifact = |kind: GateAArtifactKind| {
        validate_gate_a_artifact(
            &candidate_path
                .join("manifests")
                .join(format!("{}.json", kind.file_stem())),
            kind,
            output_config.sort.batch_rows,
            output_config.sort.max_run_bytes,
        )
    };
    let canonical_event_store = artifact(GateAArtifactKind::CanonicalEventStore)?;
    let event_time_index = artifact(GateAArtifactKind::EventTimeIndex)?;
    let visibility_index = artifact(GateAArtifactKind::VisibilityIndex)?;
    let market_observations = artifact(GateAArtifactKind::MarketObservations)?;
    let reduction_reports = artifact(GateAArtifactKind::ReductionReports)?;
    let artifacts = [
        &canonical_event_store,
        &event_time_index,
        &visibility_index,
        &market_observations,
        &reduction_reports,
    ];
    validate_candidate_manifest_closure(&artifacts)?;

    let candidate_directory_bytes = directory_bytes(candidate_path)?;
    let mut validation_guard = CandidateGuard::create(validation_work_path)?;
    let mut peak_formal_work_directory_bytes_bound = candidate_directory_bytes
        .checked_add(GATE_A_CONTROL_FILE_BYTES_BOUND)
        .ok_or_else(|| GateAError::ArtifactMismatch("正式工作目录峰值上界溢出".to_owned()))?;
    let validation_base_bytes = directory_bytes(validation_work_path)?;
    require_validation_capacity(
        validation_work_path,
        candidate_directory_bytes,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let event_id_stage = validation_work_path.join("event-id-uniqueness");
    fs::create_dir(&event_id_stage)?;
    let mut event_id_sorter = ExternalSorter::new(&event_id_stage, &output_config.sort)?;
    let mut store = artifact_cursor(&canonical_event_store, output_config)?;
    let mut record_digest = StreamDigest::new(RECORD_DIGEST_DOMAIN);
    let mut emission_sequence = 0u64;
    while let Some(store_row) = store.next_row()? {
        let record = decode_canonical_event_record_v1(store_row.payload())?;
        if store_row.sort_key_bytes() != emission_sequence.to_be_bytes() {
            return Err(GateAError::ArtifactMismatch(
                "唯一事件存储发射位置不连续".to_owned(),
            ));
        }
        record_digest.push(store_row.payload(), "candidate_canonical_records")?;
        let expected_visibility = project_record_visibility_index_entry(&record)?;
        let expected_visibility_bytes = encode_visibility_index_entry(&expected_visibility)?;
        event_id_sorter.push(bounded_sort_row(
            event_id_sort_key(record_event_id(&record)),
            expected_visibility_bytes,
            output_config,
        )?)?;
        emission_sequence = emission_sequence
            .checked_add(1)
            .ok_or_else(|| GateAError::ArtifactMismatch("规范事件发射位置溢出".to_owned()))?;
    }
    let event_ids = event_id_sorter.finish()?;
    observe_validation_stage_peak(
        &mut peak_formal_work_directory_bytes_bound,
        candidate_directory_bytes,
        validation_base_bytes,
        &event_ids.metrics,
        validation_work_path,
        output_config,
    )?;
    let canonical_records_sha256 = record_digest.finish();
    if canonical_event_store
        .manifest()
        .canonical_record_stream_sha256
        .0
        != canonical_records_sha256
    {
        return Err(GateAError::ArtifactMismatch(
            "唯一事件存储无法重算 manifest 的规范事件流摘要".to_owned(),
        ));
    }

    let validation_base_bytes = directory_bytes(validation_work_path)?;
    require_validation_capacity(
        validation_work_path,
        candidate_directory_bytes,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let visibility_material_stage = validation_work_path.join("expected-visibility-material");
    fs::create_dir(&visibility_material_stage)?;
    let mut visibility_material =
        ExternalSorter::new(&visibility_material_stage, &output_config.sort)?;
    let mut store = artifact_cursor(&canonical_event_store, output_config)?;
    let mut visibility_digest = StreamDigest::new(VISIBILITY_PROJECTION_DIGEST_DOMAIN);
    let mut emission_sequence = 0u64;
    while let Some(row) = store.next_row()? {
        if row.sort_key_bytes() != emission_sequence.to_be_bytes() {
            return Err(GateAError::ArtifactMismatch(
                "唯一事件存储发射位置不连续".to_owned(),
            ));
        }
        let record = decode_canonical_event_record_v1(row.payload())?;
        let entry = project_record_visibility_index_entry(&record)?;
        visibility_digest.push(
            &encode_visibility_index_entry(&entry)?,
            "candidate_visibility_projections",
        )?;
        visibility_material.push(bounded_sort_row(
            time_sequence_key(entry.received_at_utc_ms, emission_sequence),
            row.payload().to_vec(),
            output_config,
        )?)?;
        emission_sequence = emission_sequence
            .checked_add(1)
            .ok_or_else(|| GateAError::ArtifactMismatch("规范事件发射位置溢出".to_owned()))?;
    }
    let visibility_material = visibility_material.finish()?;
    observe_validation_stage_peak(
        &mut peak_formal_work_directory_bytes_bound,
        candidate_directory_bytes,
        validation_base_bytes,
        &visibility_material.metrics,
        validation_work_path,
        output_config,
    )?;
    let mut expected_visibility = RunCursor::open(
        &visibility_material.output_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut actual_visibility = artifact_cursor(&visibility_index, output_config)?;
    while let Some(expected_row) = expected_visibility.next_row()? {
        let actual_row = actual_visibility.next_row()?.ok_or_else(|| {
            GateAError::ArtifactMismatch("模型可见索引少于唯一事件存储".to_owned())
        })?;
        let record = decode_canonical_event_record_v1(expected_row.payload())?;
        let entry = project_record_visibility_index_entry(&record)?;
        if actual_row.sort_key_bytes() != expected_row.sort_key_bytes()
            || actual_row.payload() != encode_visibility_index_entry(&entry)?
        {
            return Err(GateAError::ArtifactMismatch(
                "模型可见索引不是唯一事件存储的精确接收时间投影".to_owned(),
            ));
        }
    }
    if actual_visibility.next_row()?.is_some() {
        return Err(GateAError::ArtifactMismatch(
            "模型可见索引多于唯一事件存储".to_owned(),
        ));
    }
    let visibility_projections_sha256 = visibility_digest.finish();

    let validation_base_bytes = directory_bytes(validation_work_path)?;
    require_validation_capacity(
        validation_work_path,
        candidate_directory_bytes,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let expected_event_time_stage = validation_work_path.join("expected-event-time");
    fs::create_dir(&expected_event_time_stage)?;
    let mut expected_event_time =
        ExternalSorter::new(&expected_event_time_stage, &output_config.sort)?;
    let mut store = artifact_cursor(&canonical_event_store, output_config)?;
    let mut event_time_digest = StreamDigest::new(EVENT_TIME_PROJECTION_DIGEST_DOMAIN);
    let mut emission_sequence = 0u64;
    while let Some(row) = store.next_row()? {
        let record = decode_canonical_event_record_v1(row.payload())?;
        if row.sort_key_bytes() != emission_sequence.to_be_bytes() {
            return Err(GateAError::ArtifactMismatch(
                "唯一事件存储发射位置不连续".to_owned(),
            ));
        }
        if let Some(entry) = project_record_event_time_index_entry(&record)? {
            let encoded = encode_event_time_index_entry(&entry)?;
            event_time_digest.push(&encoded, "candidate_event_time_projections")?;
            expected_event_time.push(bounded_sort_row(
                time_sequence_key(entry.source_event_at_utc_ms, emission_sequence),
                encoded,
                output_config,
            )?)?;
        }
        emission_sequence = emission_sequence
            .checked_add(1)
            .ok_or_else(|| GateAError::ArtifactMismatch("规范事件发射位置溢出".to_owned()))?;
    }
    let expected_event_time = expected_event_time.finish()?;
    observe_validation_stage_peak(
        &mut peak_formal_work_directory_bytes_bound,
        candidate_directory_bytes,
        validation_base_bytes,
        &expected_event_time.metrics,
        validation_work_path,
        output_config,
    )?;
    compare_sorted_artifacts(
        &expected_event_time.output_path,
        event_time_index.artifact_path(),
        output_config,
        "事件时间索引不是唯一事件存储的精确投影",
    )?;
    let event_time_projections_sha256 = event_time_digest.finish();
    fs::remove_dir_all(&expected_event_time_stage)?;

    let (peak_observation_l2_levels, projection_witnesses) = validate_observation_projection(
        &visibility_material.output_path,
        canonical_event_store.manifest(),
        &market_observations,
        visibility_index.manifest_sha256(),
        output_config,
        output_config.max_observation_l2_levels,
    )?;
    fs::remove_dir_all(&visibility_material_stage)?;

    let validation_base_bytes = directory_bytes(validation_work_path)?;
    require_validation_capacity(
        validation_work_path,
        candidate_directory_bytes,
        output_config,
        output_config.sort.max_temp_bytes,
    )?;
    let report_reference_stage = validation_work_path.join("report-event-references");
    fs::create_dir(&report_reference_stage)?;
    let mut report_reference_sorter =
        ExternalSorter::new(&report_reference_stage, &output_config.sort)?;
    let mut reports = artifact_cursor(&reduction_reports, output_config)?;
    let mut report_digest = StreamDigest::new(REDUCTION_REPORT_DIGEST_DOMAIN);
    let mut report_sequence = 0u64;
    while let Some(row) = reports.next_row()? {
        if row.sort_key_bytes() != report_sequence.to_be_bytes() {
            return Err(GateAError::ArtifactMismatch(
                "归约报告发射位置不连续".to_owned(),
            ));
        }
        let report = decode_reduction_report_entry_v1(row.payload())?;
        report_digest.push(row.payload(), "candidate_reduction_reports")?;
        if let ReductionDispositionV1::Produced { result_event_id } = report.disposition {
            report_reference_sorter.push(bounded_sort_row(
                event_reference_key(&result_event_id, report_sequence),
                Vec::new(),
                output_config,
            )?)?;
        }
        report_sequence = report_sequence
            .checked_add(1)
            .ok_or_else(|| GateAError::ArtifactMismatch("归约报告发射位置溢出".to_owned()))?;
    }
    let report_references = report_reference_sorter.finish()?;
    observe_validation_stage_peak(
        &mut peak_formal_work_directory_bytes_bound,
        candidate_directory_bytes,
        validation_base_bytes,
        &report_references.metrics,
        validation_work_path,
        output_config,
    )?;
    validate_event_report_closure(
        &event_ids.output_path,
        &report_references.output_path,
        output_config,
    )?;
    let reduction_reports_sha256 = report_digest.finish();

    fs::remove_dir_all(&event_id_stage)?;
    fs::remove_dir_all(&report_reference_stage)?;
    validation_guard.retain = false;
    drop(validation_guard);

    Ok(ValidatedGateACandidate {
        root: fs::canonicalize(candidate_path)?,
        canonical_event_store,
        event_time_index,
        visibility_index,
        market_observations,
        reduction_reports,
        recomputed_stream_digests: CanonicalRunDigests {
            canonical_records_sha256,
            event_time_projections_sha256,
            visibility_projections_sha256,
            reduction_reports_sha256,
        },
        peak_observation_l2_levels,
        peak_formal_work_directory_bytes_bound,
        projection_witnesses,
    })
}

fn validate_candidate_data_layout(candidate_path: &Path) -> Result<(), GateAError> {
    let root_metadata = fs::symlink_metadata(candidate_path)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(GateAError::ArtifactMismatch(
            "门 A 候选根必须是普通目录且不得为符号链接".to_owned(),
        ));
    }
    validate_exact_directory_entries(candidate_path, &["data", "manifests"])?;
    let data_names = GateAArtifactKind::all()
        .into_iter()
        .map(|kind| format!("{}.parquet", kind.file_stem()))
        .collect::<Vec<_>>();
    let manifest_names = GateAArtifactKind::all()
        .into_iter()
        .map(|kind| format!("{}.json", kind.file_stem()))
        .collect::<Vec<_>>();
    validate_exact_directory_entries(
        &candidate_path.join("data"),
        &data_names.iter().map(String::as_str).collect::<Vec<_>>(),
    )?;
    validate_exact_directory_entries(
        &candidate_path.join("manifests"),
        &manifest_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    )
}

fn validate_exact_directory_entries(path: &Path, expected: &[&str]) -> Result<(), GateAError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(GateAError::ArtifactMismatch(format!(
            "候选路径不是普通目录：{}",
            path.display()
        )));
    }
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| GateAError::ArtifactMismatch("候选文件名不是 UTF-8".to_owned()))?;
        if entry.file_type()?.is_symlink() {
            return Err(GateAError::ArtifactMismatch(format!(
                "候选目录不得包含符号链接：{}",
                entry.path().display()
            )));
        }
        actual.insert(name);
    }
    if actual.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected {
        return Err(GateAError::ArtifactMismatch(format!(
            "候选目录内容不匹配冻结闭包：{}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_candidate_manifest_closure(
    artifacts: &[&ValidatedGateAArtifact; 5],
) -> Result<(), GateAError> {
    let store = artifacts[0];
    for artifact in artifacts.iter().skip(1) {
        let left = store.manifest();
        let right = artifact.manifest();
        if left.canonical_rule_version != right.canonical_rule_version
            || left.input_set_identity_sha256 != right.input_set_identity_sha256
            || left.input_manifest_set_sha256 != right.input_manifest_set_sha256
            || left.canonical_record_stream_sha256 != right.canonical_record_stream_sha256
            || left.covered_condition_ids != right.covered_condition_ids
            || left.received_start_utc_ms != right.received_start_utc_ms
            || left.received_end_utc_ms != right.received_end_utc_ms
        {
            return Err(GateAError::ArtifactMismatch(
                "门 A 产物 manifest 未绑定同一输入、规则、事件流与范围".to_owned(),
            ));
        }
    }
    for artifact in [artifacts[1], artifacts[2], artifacts[4]] {
        if dependency_digest(artifact.manifest(), GateAArtifactKind::CanonicalEventStore)
            != Some(store.manifest_sha256())
        {
            return Err(GateAError::ArtifactMismatch(
                "门 A 产物未绑定当前唯一事件存储 manifest".to_owned(),
            ));
        }
    }
    if dependency_digest(
        artifacts[3].manifest(),
        GateAArtifactKind::CanonicalEventStore,
    ) != Some(store.manifest_sha256())
        || dependency_digest(artifacts[3].manifest(), GateAArtifactKind::VisibilityIndex)
            != Some(artifacts[2].manifest_sha256())
    {
        return Err(GateAError::ArtifactMismatch(
            "Observation 未绑定当前事件存储与模型可见索引 manifest".to_owned(),
        ));
    }
    if store.manifest().file.rows != artifacts[2].manifest().file.rows {
        return Err(GateAError::ArtifactMismatch(
            "唯一事件存储与模型可见索引行数不一致".to_owned(),
        ));
    }
    Ok(())
}

fn dependency_digest(
    manifest: &GateAArtifactManifestV1,
    kind: GateAArtifactKind,
) -> Option<&Sha256Hex> {
    manifest
        .dependencies
        .iter()
        .find(|dependency| dependency.artifact_kind == kind)
        .map(|dependency| &dependency.manifest_sha256)
}

fn artifact_cursor(
    artifact: &ValidatedGateAArtifact,
    output_config: &GateAOutputConfig,
) -> Result<RunCursor, GateAError> {
    Ok(RunCursor::open(
        artifact.artifact_path(),
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?)
}

fn compare_sorted_artifacts(
    expected_path: &Path,
    actual_path: &Path,
    output_config: &GateAOutputConfig,
    mismatch: &'static str,
) -> Result<(), GateAError> {
    let mut expected = RunCursor::open(
        expected_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut actual = RunCursor::open(
        actual_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    loop {
        match (expected.next_row()?, actual.next_row()?) {
            (Some(left), Some(right))
                if left.sort_key_bytes() == right.sort_key_bytes()
                    && left.payload() == right.payload() => {}
            (None, None) => return Ok(()),
            _ => return Err(GateAError::ArtifactMismatch(mismatch.to_owned())),
        }
    }
}

fn replace_projection_witness_candidates(
    candidates: &mut BTreeMap<ObservationComponentId, ProjectionWitnessCandidate>,
    components: &[ObservationComponentId],
    candidate: &ProjectionWitnessCandidate,
) {
    for component in components {
        candidates.insert(*component, candidate.clone());
    }
}

fn update_projection_witness_candidates(
    candidates: &mut BTreeMap<ObservationComponentId, ProjectionWitnessCandidate>,
    record: &CanonicalEventRecordV1,
) {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
            let gap_resync = matches!(
                &event.payload,
                ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                    if matches!(&boundary.scope, QualityScopeV1::Asset(_))
                        && boundary.previous_quality == MarketDataQuality::Missing
                        && boundary.current_quality == MarketDataQuality::Fresh
            ) && event.quality.availability == MarketDataQuality::Fresh
                && event.quality.resync == ResyncQuality::ResyncBoundary;
            let (primary_kind, components): (
                Option<GateAProjectionWitnessKind>,
                &[ObservationComponentId],
            ) = match &event.payload {
                ExecutionMarketEventPayloadV3::MarketLifecycleTransition(_)
                | ExecutionMarketEventPayloadV3::MarketTradingState(_) => (
                    Some(GateAProjectionWitnessKind::MarketLifecycle),
                    &[ObservationComponentId::MarketLifecycle],
                ),
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(_) => (
                    Some(GateAProjectionWitnessKind::InitialL2),
                    &[
                        ObservationComponentId::PolymarketL2,
                        ObservationComponentId::CurrentTick,
                    ],
                ),
                ExecutionMarketEventPayloadV3::BestBidAsk(_) => (
                    Some(GateAProjectionWitnessKind::BestBidAsk),
                    &[ObservationComponentId::BestBidAsk],
                ),
                ExecutionMarketEventPayloadV3::TickSizeChange(_) => (
                    Some(GateAProjectionWitnessKind::DynamicTick),
                    &[ObservationComponentId::CurrentTick],
                ),
                ExecutionMarketEventPayloadV3::PublicTrade(_) => (
                    Some(GateAProjectionWitnessKind::PublicTrade),
                    &[ObservationComponentId::PublicTrade],
                ),
                ExecutionMarketEventPayloadV3::LevelExecutionTransition(_) => (
                    Some(GateAProjectionWitnessKind::LevelExecution),
                    &[ObservationComponentId::PolymarketL2],
                ),
                ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => {
                    let components = match &boundary.scope {
                        QualityScopeV1::Asset(_) => &[ObservationComponentId::PolymarketL2][..],
                        QualityScopeV1::Market => &[ObservationComponentId::MarketResolution][..],
                    };
                    (None, components)
                }
                ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => (
                    Some(GateAProjectionWitnessKind::MarketTerminal),
                    &[ObservationComponentId::MarketResolution],
                ),
            };
            let candidate = ProjectionWitnessCandidate {
                event_id: event.event_id.clone(),
                primary_kind,
                gap_resync,
            };
            replace_projection_witness_candidates(candidates, components, &candidate);
        }
        CanonicalEventRecordV1::ReferenceEvent(event) => {
            let gap_resync = matches!(
                &event.payload,
                CanonicalReferenceEventPayloadV1::BinanceDepthState(_)
            ) && event.quality.availability == MarketDataQuality::Fresh
                && event.quality.resync == ResyncQuality::ResyncBoundary;
            let component = match &event.payload {
                CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(_) => {
                    ObservationComponentId::ChainlinkBtcUsd
                }
                CanonicalReferenceEventPayloadV1::BinanceDepthState(_) => {
                    ObservationComponentId::BinanceBtcUsdtDepth
                }
                CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(_) => {
                    ObservationComponentId::BinanceBtcUsdtTrade
                }
            };
            replace_projection_witness_candidates(
                candidates,
                &[component],
                &ProjectionWitnessCandidate {
                    event_id: event.event_id.clone(),
                    primary_kind: None,
                    gap_resync,
                },
            );
        }
    }
}

fn remember_projection_witness(
    component: ObservationComponentId,
    last_visible_event_id: Option<&CanonicalEventId>,
    observation_id: &Sha256Hex,
    candidates: &BTreeMap<ObservationComponentId, ProjectionWitnessCandidate>,
    witnesses: &mut GateAProjectionWitnesses,
) {
    let Some(event_id) = last_visible_event_id else {
        return;
    };
    let Some(candidate) = candidates
        .get(&component)
        .filter(|candidate| candidate.event_id == *event_id)
    else {
        return;
    };
    let mut remember = |kind| {
        witnesses
            .linked
            .entry(kind)
            .or_insert_with(|| GateAProjectionWitness {
                event_id: event_id.clone(),
                observation_id: observation_id.clone(),
            });
    };
    if let Some(kind) = candidate.primary_kind {
        remember(kind);
    }
    if candidate.gap_resync {
        remember(GateAProjectionWitnessKind::GapResync);
    }
}

fn observe_projection_witnesses(
    observation: &MarketObservationCausalShellV3,
    candidates: &BTreeMap<ObservationComponentId, ProjectionWitnessCandidate>,
    witnesses: &mut GateAProjectionWitnesses,
) {
    for state in &observation.component_states {
        remember_projection_witness(
            state.component,
            state.last_visible_event_id.as_ref(),
            &observation.observation_id,
            candidates,
            witnesses,
        );
    }
}

fn validate_observation_projection(
    visibility_material_path: &Path,
    store_manifest: &GateAArtifactManifestV1,
    observation_artifact: &ValidatedGateAArtifact,
    visibility_manifest_sha256: &Sha256Hex,
    output_config: &GateAOutputConfig,
    max_retained_l2_levels: usize,
) -> Result<(usize, GateAProjectionWitnesses), GateAError> {
    let mut projector = ObservationProjector::new(
        store_manifest.covered_condition_ids[0].clone(),
        store_manifest.input_set_identity_sha256.clone(),
        visibility_manifest_sha256.clone(),
        max_retained_l2_levels,
    );
    let mut visibility_material = RunCursor::open(
        visibility_material_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut observations = artifact_cursor(observation_artifact, output_config)?;
    let mut candidates = BTreeMap::new();
    let mut witnesses = GateAProjectionWitnesses::default();
    while let Some(row) = visibility_material.next_row()? {
        let record = decode_canonical_event_record_v1(row.payload())?;
        if let Some(projected) = projector.push(&record)? {
            compare_projected_observation(&projected.observation, &mut observations)?;
            observe_projection_witnesses(&projected.observation, &candidates, &mut witnesses);
        }
        update_projection_witness_candidates(&mut candidates, &record);
    }
    if let Some(projected) = projector.finish()? {
        compare_projected_observation(&projected.observation, &mut observations)?;
        observe_projection_witnesses(&projected.observation, &candidates, &mut witnesses);
    }
    if observations.next_row()?.is_some() {
        return Err(GateAError::ArtifactMismatch(
            "Observation 多于唯一事件存储的确定投影".to_owned(),
        ));
    }
    Ok((projector.peak_retained_l2_levels(), witnesses))
}

fn compare_projected_observation(
    expected: &MarketObservationCausalShellV3,
    actual: &mut RunCursor,
) -> Result<(), GateAError> {
    let row = actual.next_row()?.ok_or_else(|| {
        GateAError::ArtifactMismatch("Observation 少于唯一事件存储的确定投影".to_owned())
    })?;
    let expected_key = observation_key(
        expected.visibility_cutoff.received_at_utc_ms,
        &expected.market.condition_id.0,
    )?;
    let expected_payload = encode_market_observation_causal_shell_v3(expected)?;
    if row.sort_key_bytes() != expected_key || row.payload() != expected_payload {
        return Err(GateAError::ArtifactMismatch(
            "Observation 不是唯一事件存储与可见索引的精确投影".to_owned(),
        ));
    }
    Ok(())
}

fn validate_event_report_closure(
    event_ids_path: &Path,
    references_path: &Path,
    output_config: &GateAOutputConfig,
) -> Result<(), GateAError> {
    let mut event_ids = RunCursor::open(
        event_ids_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut references = RunCursor::open(
        references_path,
        output_config.sort.batch_rows,
        output_config.sort.max_run_bytes,
    )?;
    let mut current_reference = references.next_row()?;
    while let Some(event) = event_ids.next_row()? {
        let event_key = event.sort_key_bytes();
        let reference_key = current_reference
            .as_ref()
            .map(|reference| {
                reference.sort_key_bytes().get(..65).ok_or_else(|| {
                    GateAError::ArtifactMismatch("归约报告事件引用键无效".to_owned())
                })
            })
            .transpose()?;
        if reference_key.is_some_and(|reference_key| reference_key < event_key) {
            return Err(GateAError::ArtifactMismatch(
                "归约报告 produced 结果未解析到唯一事件存储".to_owned(),
            ));
        }
        if reference_key != Some(event_key) {
            return Err(GateAError::ArtifactMismatch(
                "唯一事件存储中的事件缺少 produced 归约报告".to_owned(),
            ));
        }
        loop {
            current_reference = references.next_row()?;
            let Some(reference) = &current_reference else {
                break;
            };
            let reference_key = reference
                .sort_key_bytes()
                .get(..65)
                .ok_or_else(|| GateAError::ArtifactMismatch("归约报告事件引用键无效".to_owned()))?;
            if reference_key != event_key {
                break;
            }
        }
    }
    if current_reference.is_some() {
        return Err(GateAError::ArtifactMismatch(
            "归约报告 produced 结果未解析到唯一事件存储".to_owned(),
        ));
    }
    Ok(())
}

fn validate_artifact_physical_format(path: &Path, expected_rows: u64) -> Result<(), GateAError> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(path)?).map_err(SortError::from)?;
    let schema = builder.schema();
    if schema.fields().len() != 2
        || schema.field(0).name() != "sort_key"
        || schema.field(0).data_type() != &DataType::Binary
        || schema.field(0).is_nullable()
        || schema.field(1).name() != "payload"
        || schema.field(1).data_type() != &DataType::Binary
        || schema.field(1).is_nullable()
    {
        return Err(GateAError::ArtifactMismatch(
            "产物 Arrow schema 不匹配冻结的二列非空 Binary schema".to_owned(),
        ));
    }
    let metadata = builder.metadata();
    let parquet_rows = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| GateAError::ArtifactMismatch("Parquet footer 行数无法表示".to_owned()))?;
    if parquet_rows != expected_rows {
        return Err(GateAError::ArtifactMismatch(
            "Parquet footer 行数与 manifest 不一致".to_owned(),
        ));
    }
    if metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .any(|column| column.compression() != Compression::ZSTD(Default::default()))
    {
        return Err(GateAError::ArtifactMismatch(
            "产物列块未全部使用 Zstd 压缩".to_owned(),
        ));
    }
    Ok(())
}

fn validate_manifest_fields(
    manifest: &GateAArtifactManifestV1,
    expected_kind: GateAArtifactKind,
) -> Result<(), GateAError> {
    if manifest.schema_version != GATE_A_ARTIFACT_MANIFEST_SCHEMA_V1
        || manifest.artifact_kind != expected_kind
        || manifest.contract_version != expected_kind.contract_version()
        || manifest.physical_schema_version != "binary_sort_key_payload_parquet_v1"
        || manifest.key_encoding_version != KEY_ENCODING_VERSION_V1
    {
        return Err(GateAError::InvalidManifest(
            "schema、产物类型、合同或键版本不匹配".to_owned(),
        ));
    }
    if manifest.canonical_rule_version.trim().is_empty()
        || manifest.covered_condition_ids.len() != 1
        || manifest.covered_condition_ids[0].trim().is_empty()
        || manifest.received_start_utc_ms <= 0
        || manifest.received_end_utc_ms <= manifest.received_start_utc_ms
    {
        return Err(GateAError::InvalidManifest(
            "规范规则、覆盖市场或接收时间范围无效".to_owned(),
        ));
    }
    let expected_relative_path = format!("data/{}.parquet", expected_kind.file_stem());
    if manifest.file.relative_path != expected_relative_path {
        return Err(GateAError::InvalidManifest(
            "产物相对路径不匹配冻结文件名".to_owned(),
        ));
    }
    for (field, value) in [
        (
            "input_set_identity_sha256",
            &manifest.input_set_identity_sha256,
        ),
        (
            "input_manifest_set_sha256",
            &manifest.input_manifest_set_sha256,
        ),
        (
            "canonical_record_stream_sha256",
            &manifest.canonical_record_stream_sha256,
        ),
        ("file.sha256", &manifest.file.sha256),
        ("file.logical_sha256", &manifest.file.logical_sha256),
    ] {
        require_sha256(value, field)?;
    }
    let mut previous_dependency = None;
    for dependency in &manifest.dependencies {
        let rank = artifact_kind_rank(dependency.artifact_kind);
        if previous_dependency.is_some_and(|previous| previous >= rank) {
            return Err(GateAError::InvalidManifest(
                "产物依赖必须规范排序且唯一".to_owned(),
            ));
        }
        previous_dependency = Some(rank);
        require_sha256(&dependency.manifest_sha256, "dependencies.manifest_sha256")?;
    }
    let actual_dependencies = manifest
        .dependencies
        .iter()
        .map(|dependency| dependency.artifact_kind)
        .collect::<Vec<_>>();
    let expected_dependencies: &[GateAArtifactKind] = match expected_kind {
        GateAArtifactKind::CanonicalEventStore => &[],
        GateAArtifactKind::EventTimeIndex | GateAArtifactKind::VisibilityIndex => {
            &[GateAArtifactKind::CanonicalEventStore]
        }
        GateAArtifactKind::MarketObservations => &[
            GateAArtifactKind::CanonicalEventStore,
            GateAArtifactKind::VisibilityIndex,
        ],
        GateAArtifactKind::ReductionReports => &[GateAArtifactKind::CanonicalEventStore],
    };
    if actual_dependencies != expected_dependencies {
        return Err(GateAError::InvalidManifest(
            "产物依赖图不匹配冻结闭包".to_owned(),
        ));
    }
    Ok(())
}

fn validate_artifact_rows(
    path: &Path,
    manifest: &GateAArtifactManifestV1,
    batch_rows: usize,
    max_batch_bytes: usize,
) -> Result<(), GateAError> {
    let mut cursor = RunCursor::open(path, batch_rows, max_batch_bytes)?;
    let mut logical_position = 0u64;
    let expected_visibility_manifest = manifest
        .dependencies
        .iter()
        .find(|dependency| dependency.artifact_kind == GateAArtifactKind::VisibilityIndex)
        .map(|dependency| &dependency.manifest_sha256);
    while let Some(row) = cursor.next_row()? {
        match manifest.artifact_kind {
            GateAArtifactKind::CanonicalEventStore => {
                let record = decode_canonical_event_record_v1(row.payload())?;
                if row.sort_key_bytes() != logical_position.to_be_bytes() {
                    return Err(GateAError::ArtifactMismatch(
                        "唯一事件存储发射位置不连续".to_owned(),
                    ));
                }
                let (condition_id, received_at_utc_ms, canonical_rule_version) = match &record {
                    CanonicalEventRecordV1::ExecutionMarketEvent(event) => (
                        Some(event.market.condition_id.0.as_str()),
                        event.time.received_at_utc_ms,
                        event.lineage.canonical_rule_version.as_str(),
                    ),
                    CanonicalEventRecordV1::ReferenceEvent(event) => (
                        None,
                        event.time.received_at_utc_ms,
                        event.lineage.canonical_rule_version.as_str(),
                    ),
                };
                if canonical_rule_version != manifest.canonical_rule_version
                    || condition_id.is_some_and(|condition_id| {
                        condition_id != manifest.covered_condition_ids[0]
                    })
                    || received_at_utc_ms < manifest.received_start_utc_ms
                    || received_at_utc_ms >= manifest.received_end_utc_ms
                {
                    return Err(GateAError::ArtifactMismatch(
                        "事件存储载荷的规则版本或覆盖范围与 manifest 不一致".to_owned(),
                    ));
                }
            }
            GateAArtifactKind::EventTimeIndex => {
                let entry = decode_event_time_index_entry(row.payload())?;
                validate_time_sequence_key(row.sort_key_bytes(), entry.source_event_at_utc_ms)?;
            }
            GateAArtifactKind::VisibilityIndex => {
                let entry = decode_visibility_index_entry(row.payload())?;
                validate_time_sequence_key(row.sort_key_bytes(), entry.received_at_utc_ms)?;
                if entry.received_at_utc_ms < manifest.received_start_utc_ms
                    || entry.received_at_utc_ms >= manifest.received_end_utc_ms
                {
                    return Err(GateAError::ArtifactMismatch(
                        "模型可见索引超出 manifest 接收时间范围".to_owned(),
                    ));
                }
            }
            GateAArtifactKind::MarketObservations => {
                let observation = decode_market_observation_causal_shell_v3(row.payload())?;
                if row.sort_key_bytes()
                    != observation_key(
                        observation.visibility_cutoff.received_at_utc_ms,
                        &observation.market.condition_id.0,
                    )?
                {
                    return Err(GateAError::ArtifactMismatch(
                        "Observation 键与可见截止点、市场或身份不一致".to_owned(),
                    ));
                }
                if expected_visibility_manifest
                    != Some(&observation.lineage.visibility_index_manifest_sha256)
                {
                    return Err(GateAError::ArtifactMismatch(
                        "Observation 未绑定当前模型可见索引 manifest".to_owned(),
                    ));
                }
                if observation.market.condition_id.0 != manifest.covered_condition_ids[0]
                    || observation.visibility_cutoff.received_at_utc_ms
                        < manifest.received_start_utc_ms
                    || observation.visibility_cutoff.received_at_utc_ms
                        >= manifest.received_end_utc_ms
                {
                    return Err(GateAError::ArtifactMismatch(
                        "Observation 超出 manifest 覆盖范围".to_owned(),
                    ));
                }
            }
            GateAArtifactKind::ReductionReports => {
                let report = decode_reduction_report_entry_v1(row.payload())?;
                if row.sort_key_bytes() != logical_position.to_be_bytes() {
                    return Err(GateAError::ArtifactMismatch(
                        "归约报告发射位置不连续".to_owned(),
                    ));
                }
                if report.canonical_rule_version != manifest.canonical_rule_version {
                    return Err(GateAError::ArtifactMismatch(
                        "归约报告规则版本与 manifest 不一致".to_owned(),
                    ));
                }
            }
        }
        logical_position = logical_position
            .checked_add(1)
            .ok_or(GateAError::ArtifactMismatch("产物逻辑位置溢出".to_owned()))?;
    }
    Ok(())
}

fn validate_time_sequence_key(key: &[u8], expected_time: i64) -> Result<(), GateAError> {
    if key.len() != 17 || key[0] != 1 || key[1..9] != ordered_i64(expected_time) {
        return Err(GateAError::ArtifactMismatch(
            "时间索引键与合同时间不一致".to_owned(),
        ));
    }
    Ok(())
}

fn sequence_from_time_key(key: &[u8], expected_time: i64) -> Result<u64, GateAError> {
    validate_time_sequence_key(key, expected_time)?;
    let mut sequence = [0u8; 8];
    sequence.copy_from_slice(&key[9..17]);
    Ok(u64::from_be_bytes(sequence))
}

fn require_sha256(value: &Sha256Hex, field: &str) -> Result<(), GateAError> {
    if value.0.len() != 64
        || !value
            .0
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(GateAError::InvalidManifest(format!(
            "{field} 不是 64 位小写十六进制 SHA-256"
        )));
    }
    Ok(())
}

fn artifact_kind_rank(kind: GateAArtifactKind) -> u8 {
    match kind {
        GateAArtifactKind::CanonicalEventStore => 0,
        GateAArtifactKind::EventTimeIndex => 1,
        GateAArtifactKind::VisibilityIndex => 2,
        GateAArtifactKind::MarketObservations => 3,
        GateAArtifactKind::ReductionReports => 4,
    }
}

struct TypedArtifactCursor<T> {
    validated: ValidatedGateAArtifact,
    cursor: RunCursor,
    decoder: fn(&[u8]) -> Result<T, ContractError>,
    next_logical_position: u64,
    max_page_rows: usize,
}

impl<T> TypedArtifactCursor<T> {
    fn open(
        manifest_path: &Path,
        kind: GateAArtifactKind,
        decoder: fn(&[u8]) -> Result<T, ContractError>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        if max_page_rows == 0 {
            return Err(GateAError::InvalidConfig(
                "类型化游标最大分页行数必须为正数",
            ));
        }
        let validated = validate_gate_a_artifact(manifest_path, kind, batch_rows, max_batch_bytes)?;
        let cursor = RunCursor::open(validated.artifact_path(), batch_rows, max_batch_bytes)?;
        Ok(Self {
            validated,
            cursor,
            decoder,
            next_logical_position: 0,
            max_page_rows,
        })
    }

    fn next_value(&mut self) -> Result<Option<T>, GateAError> {
        Ok(self.next_keyed_value()?.map(|(_sort_key, value)| value))
    }

    fn next_keyed_value(&mut self) -> Result<Option<(Vec<u8>, T)>, GateAError> {
        let Some(row) = self.cursor.next_row()? else {
            return Ok(None);
        };
        let value = (self.decoder)(row.payload())?;
        let sort_key = row.sort_key_bytes().to_vec();
        self.next_logical_position =
            self.next_logical_position
                .checked_add(1)
                .ok_or(GateAError::ArtifactMismatch(
                    "类型化游标逻辑位置溢出".to_owned(),
                ))?;
        Ok(Some((sort_key, value)))
    }

    fn checked_limit(&self, requested: usize) -> Result<usize, GateAError> {
        if requested == 0 || requested > self.max_page_rows {
            return Err(GateAError::InvalidConfig(
                "请求分页行数必须为正且不超过游标冻结上限",
            ));
        }
        Ok(requested)
    }
}

pub struct CanonicalEventStoreCursor {
    inner: TypedArtifactCursor<CanonicalEventRecordV1>,
}

impl CanonicalEventStoreCursor {
    pub fn open(
        manifest_path: &Path,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::CanonicalEventStore,
                decode_canonical_event_record_v1,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit {
            let Some(record) = self.inner.next_value()? else {
                break;
            };
            page.push(record);
        }
        Ok(page)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionMarketEventReadFilter {
    pub condition_id: Option<String>,
    pub source_event_time_range: Option<(i64, i64)>,
    pub received_time_range: Option<(i64, i64)>,
}

impl ExecutionMarketEventReadFilter {
    fn validate(&self) -> Result<(), GateAError> {
        if self
            .condition_id
            .as_deref()
            .is_some_and(|condition_id| condition_id.trim().is_empty())
            || [self.source_event_time_range, self.received_time_range]
                .into_iter()
                .flatten()
                .any(|(start, end)| start <= 0 || end <= start)
        {
            return Err(GateAError::InvalidConfig("市场事件读取过滤条件无效"));
        }
        Ok(())
    }

    fn accepts(&self, event: &ExecutionMarketEventV3) -> bool {
        self.condition_id
            .as_deref()
            .is_none_or(|condition_id| event.market.condition_id.0 == condition_id)
            && self.source_event_time_range.is_none_or(|(start, end)| {
                event
                    .time
                    .source_event_at_utc_ms
                    .is_some_and(|value| start <= value && value < end)
            })
            && self.received_time_range.is_none_or(|(start, end)| {
                start <= event.time.received_at_utc_ms && event.time.received_at_utc_ms < end
            })
    }
}

pub struct ExecutionMarketEventCursor {
    inner: TypedArtifactCursor<CanonicalEventRecordV1>,
    filter: ExecutionMarketEventReadFilter,
}

impl ExecutionMarketEventCursor {
    pub fn open(
        manifest_path: &Path,
        filter: ExecutionMarketEventReadFilter,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        filter.validate()?;
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::CanonicalEventStore,
                decode_canonical_event_record_v1,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
            filter,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<ExecutionMarketEventV3>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit {
            let Some(record) = self.inner.next_value()? else {
                break;
            };
            if let CanonicalEventRecordV1::ExecutionMarketEvent(event) = record
                && self.filter.accepts(&event)
            {
                page.push(*event);
            }
        }
        Ok(page)
    }
}

pub struct CanonicalReferenceEventCursor {
    inner: TypedArtifactCursor<CanonicalEventRecordV1>,
    source_event_time_range: Option<(i64, i64)>,
    received_time_range: Option<(i64, i64)>,
}

impl CanonicalReferenceEventCursor {
    pub fn open(
        manifest_path: &Path,
        source_event_time_range: Option<(i64, i64)>,
        received_time_range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        if [source_event_time_range, received_time_range]
            .into_iter()
            .flatten()
            .any(|(start, end)| start <= 0 || end <= start)
        {
            return Err(GateAError::InvalidConfig("参考事件读取时间范围无效"));
        }
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::CanonicalEventStore,
                decode_canonical_event_record_v1,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
            source_event_time_range,
            received_time_range,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(
        &mut self,
        limit: usize,
    ) -> Result<Vec<CanonicalReferenceEventV1>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit {
            let Some(record) = self.inner.next_value()? else {
                break;
            };
            let CanonicalEventRecordV1::ReferenceEvent(event) = record else {
                continue;
            };
            let source_matches = self.source_event_time_range.is_none_or(|(start, end)| {
                event
                    .time
                    .source_event_at_utc_ms
                    .is_some_and(|value| start <= value && value < end)
            });
            let received_matches = self.received_time_range.is_none_or(|(start, end)| {
                start <= event.time.received_at_utc_ms && event.time.received_at_utc_ms < end
            });
            if source_matches && received_matches {
                page.push(*event);
            }
        }
        Ok(page)
    }
}

pub struct EventTimeIndexCursor {
    inner: TypedArtifactCursor<EventTimeIndexEntryV2>,
    range: Option<(i64, i64)>,
    finished: bool,
}

impl EventTimeIndexCursor {
    pub fn open(
        manifest_path: &Path,
        range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        validate_optional_range(range, "事件时间索引范围无效")?;
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::EventTimeIndex,
                decode_event_time_index_entry,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
            range,
            finished: false,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<EventTimeIndexEntryV2>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit && !self.finished {
            let Some(entry) = self.inner.next_value()? else {
                break;
            };
            if let Some((start, end)) = self.range {
                if entry.source_event_at_utc_ms < start {
                    continue;
                }
                if entry.source_event_at_utc_ms >= end {
                    self.finished = true;
                    break;
                }
            }
            page.push(entry);
        }
        Ok(page)
    }
}

#[derive(Clone)]
struct PlatformReplayRequest {
    sequence: u64,
    entry: EventTimeIndexEntryV2,
    page_position: usize,
}

#[derive(Clone)]
struct VisibilityReplayRequest {
    sequence: u64,
    entry: VisibilityIndexEntryV2,
    page_position: usize,
}

/// 绑定已完整复核候选的唯一事件存储与事件时间索引，按数据源事件时间返回规范记录。
///
/// 每页只保留至多 `max_page_rows` 个索引请求与对应载荷；不会要求下游重建事件映射或自行排序。
pub struct PlatformReplayCursor {
    store_path: PathBuf,
    event_time: TypedArtifactCursor<EventTimeIndexEntryV2>,
    source_event_time_range: Option<(i64, i64)>,
    finished: bool,
    batch_rows: usize,
    max_batch_bytes: usize,
}

impl PlatformReplayCursor {
    pub fn open(
        candidate: &ValidatedGateACandidate,
        source_event_time_range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        validate_optional_range(source_event_time_range, "平台回放事件时间范围无效")?;
        let store = validate_gate_a_artifact(
            candidate.canonical_event_store.manifest_path(),
            GateAArtifactKind::CanonicalEventStore,
            batch_rows,
            max_batch_bytes,
        )?;
        let event_time = TypedArtifactCursor::open(
            candidate.event_time_index.manifest_path(),
            GateAArtifactKind::EventTimeIndex,
            decode_event_time_index_entry,
            batch_rows,
            max_batch_bytes,
            max_page_rows,
        )?;
        if store.manifest_sha256() != candidate.canonical_event_store.manifest_sha256()
            || event_time.validated.manifest_sha256()
                != candidate.event_time_index.manifest_sha256()
            || store.root() != event_time.validated.root()
            || dependency_digest(
                event_time.validated.manifest(),
                GateAArtifactKind::CanonicalEventStore,
            ) != Some(store.manifest_sha256())
        {
            return Err(GateAError::ArtifactMismatch(
                "平台回放读取器未绑定已完整复核的事件存储与事件时间索引".to_owned(),
            ));
        }
        Ok(Self {
            store_path: store.artifact_path().to_path_buf(),
            event_time,
            source_event_time_range,
            finished: false,
            batch_rows,
            max_batch_bytes,
        })
    }

    pub fn canonical_event_store_manifest_sha256(&self) -> &Sha256Hex {
        dependency_digest(
            self.event_time.validated.manifest(),
            GateAArtifactKind::CanonicalEventStore,
        )
        .expect("已验证事件时间索引必须绑定唯一事件存储")
    }

    pub fn event_time_index_manifest_sha256(&self) -> &Sha256Hex {
        self.event_time.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.event_time.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        let limit = self.event_time.checked_limit(limit)?;
        let mut requests = Vec::with_capacity(limit);
        while requests.len() < limit && !self.finished {
            let Some((sort_key, entry)) = self.event_time.next_keyed_value()? else {
                self.finished = true;
                break;
            };
            if let Some((start, end)) = self.source_event_time_range {
                if entry.source_event_at_utc_ms < start {
                    continue;
                }
                if entry.source_event_at_utc_ms >= end {
                    self.finished = true;
                    break;
                }
            }
            let sequence = sequence_from_time_key(&sort_key, entry.source_event_at_utc_ms)?;
            requests.push(PlatformReplayRequest {
                sequence,
                entry,
                page_position: requests.len(),
            });
        }
        self.resolve_page(requests)
    }

    fn resolve_page(
        &self,
        requests: Vec<PlatformReplayRequest>,
    ) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut scan_order = requests.clone();
        scan_order.sort_by_key(|request| request.sequence);
        if scan_order
            .windows(2)
            .any(|pair| pair[0].sequence == pair[1].sequence)
        {
            return Err(GateAError::ArtifactMismatch(
                "事件时间索引重复引用同一规范发射位置".to_owned(),
            ));
        }

        let mut resolved = vec![None; requests.len()];
        let mut store = RunCursor::open(&self.store_path, self.batch_rows, self.max_batch_bytes)?;
        let mut store_position = 0u64;
        let mut request_position = 0usize;
        while request_position < scan_order.len() {
            let row = store.next_row()?.ok_or_else(|| {
                GateAError::ArtifactMismatch(
                    "事件时间索引引用了唯一事件存储范围外的位置".to_owned(),
                )
            })?;
            if row.sort_key_bytes() != store_position.to_be_bytes() {
                return Err(GateAError::ArtifactMismatch(
                    "唯一事件存储发射位置不连续".to_owned(),
                ));
            }
            let request = &scan_order[request_position];
            if store_position == request.sequence {
                let record = decode_canonical_event_record_v1(row.payload())?;
                if project_record_event_time_index_entry(&record)?.as_ref() != Some(&request.entry)
                {
                    return Err(GateAError::ArtifactMismatch(
                        "事件时间索引无法解析为唯一事件存储中的同一规范记录".to_owned(),
                    ));
                }
                resolved[request.page_position] = Some(record);
                request_position += 1;
            }
            store_position = store_position.checked_add(1).ok_or_else(|| {
                GateAError::ArtifactMismatch("唯一事件存储发射位置溢出".to_owned())
            })?;
        }
        resolved
            .into_iter()
            .map(|record| {
                record.ok_or_else(|| {
                    GateAError::ArtifactMismatch("平台回放页存在未解析索引".to_owned())
                })
            })
            .collect()
    }
}

/// 绑定已完整复核候选的唯一事件存储与模型可见索引，按接收时间返回规范记录。
///
/// 每页只保留至多 `max_page_rows` 个索引请求与对应载荷；reference record 保持原样返回，
/// 由消费方在完成全局逻辑位置闭合后过滤。
pub struct VisibilityReplayCursor {
    store_path: PathBuf,
    visibility: TypedArtifactCursor<VisibilityIndexEntryV2>,
    received_time_range: Option<(i64, i64)>,
    finished: bool,
    batch_rows: usize,
    max_batch_bytes: usize,
}

impl VisibilityReplayCursor {
    pub fn open(
        candidate: &ValidatedGateACandidate,
        received_time_range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        validate_optional_range(received_time_range, "模型可见回放接收时间范围无效")?;
        let store = validate_gate_a_artifact(
            candidate.canonical_event_store.manifest_path(),
            GateAArtifactKind::CanonicalEventStore,
            batch_rows,
            max_batch_bytes,
        )?;
        let visibility = TypedArtifactCursor::open(
            candidate.visibility_index.manifest_path(),
            GateAArtifactKind::VisibilityIndex,
            decode_visibility_index_entry,
            batch_rows,
            max_batch_bytes,
            max_page_rows,
        )?;
        if store.manifest_sha256() != candidate.canonical_event_store.manifest_sha256()
            || visibility.validated.manifest_sha256()
                != candidate.visibility_index.manifest_sha256()
            || store.root() != visibility.validated.root()
            || dependency_digest(
                visibility.validated.manifest(),
                GateAArtifactKind::CanonicalEventStore,
            ) != Some(store.manifest_sha256())
        {
            return Err(GateAError::ArtifactMismatch(
                "模型可见回放读取器未绑定已完整复核的事件存储与模型可见索引".to_owned(),
            ));
        }
        Ok(Self {
            store_path: store.artifact_path().to_path_buf(),
            visibility,
            received_time_range,
            finished: false,
            batch_rows,
            max_batch_bytes,
        })
    }

    pub fn canonical_event_store_manifest_sha256(&self) -> &Sha256Hex {
        dependency_digest(
            self.visibility.validated.manifest(),
            GateAArtifactKind::CanonicalEventStore,
        )
        .expect("已验证模型可见索引必须绑定唯一事件存储")
    }

    pub fn visibility_index_manifest_sha256(&self) -> &Sha256Hex {
        self.visibility.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.visibility.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        let limit = self.visibility.checked_limit(limit)?;
        let mut requests = Vec::with_capacity(limit);
        while requests.len() < limit && !self.finished {
            let Some((sort_key, entry)) = self.visibility.next_keyed_value()? else {
                self.finished = true;
                break;
            };
            if let Some((start, end)) = self.received_time_range {
                if entry.received_at_utc_ms < start {
                    continue;
                }
                if entry.received_at_utc_ms >= end {
                    self.finished = true;
                    break;
                }
            }
            let sequence = sequence_from_time_key(&sort_key, entry.received_at_utc_ms)?;
            requests.push(VisibilityReplayRequest {
                sequence,
                entry,
                page_position: requests.len(),
            });
        }
        self.resolve_page(requests)
    }

    fn resolve_page(
        &self,
        requests: Vec<VisibilityReplayRequest>,
    ) -> Result<Vec<CanonicalEventRecordV1>, GateAError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut scan_order = requests.clone();
        scan_order.sort_by_key(|request| request.sequence);
        if scan_order
            .windows(2)
            .any(|pair| pair[0].sequence == pair[1].sequence)
        {
            return Err(GateAError::ArtifactMismatch(
                "模型可见索引重复引用同一规范发射位置".to_owned(),
            ));
        }

        let mut resolved = vec![None; requests.len()];
        let mut store = RunCursor::open(&self.store_path, self.batch_rows, self.max_batch_bytes)?;
        let mut store_position = 0u64;
        let mut request_position = 0usize;
        while request_position < scan_order.len() {
            let row = store.next_row()?.ok_or_else(|| {
                GateAError::ArtifactMismatch(
                    "模型可见索引引用了唯一事件存储范围外的位置".to_owned(),
                )
            })?;
            if row.sort_key_bytes() != store_position.to_be_bytes() {
                return Err(GateAError::ArtifactMismatch(
                    "唯一事件存储发射位置不连续".to_owned(),
                ));
            }
            let request = &scan_order[request_position];
            if store_position == request.sequence {
                let record = decode_canonical_event_record_v1(row.payload())?;
                if project_record_visibility_index_entry(&record)? != request.entry {
                    return Err(GateAError::ArtifactMismatch(
                        "模型可见索引无法解析为唯一事件存储中的同一规范记录".to_owned(),
                    ));
                }
                resolved[request.page_position] = Some(record);
                request_position += 1;
            }
            store_position = store_position.checked_add(1).ok_or_else(|| {
                GateAError::ArtifactMismatch("唯一事件存储发射位置溢出".to_owned())
            })?;
        }
        resolved
            .into_iter()
            .map(|record| {
                record.ok_or_else(|| {
                    GateAError::ArtifactMismatch("模型可见回放页存在未解析索引".to_owned())
                })
            })
            .collect()
    }
}

pub struct VisibilityIndexCursor {
    inner: TypedArtifactCursor<VisibilityIndexEntryV2>,
    range: Option<(i64, i64)>,
    finished: bool,
}

impl VisibilityIndexCursor {
    pub fn open(
        manifest_path: &Path,
        range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        validate_optional_range(range, "模型可见索引范围无效")?;
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::VisibilityIndex,
                decode_visibility_index_entry,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
            range,
            finished: false,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<VisibilityIndexEntryV2>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit && !self.finished {
            let Some(entry) = self.inner.next_value()? else {
                break;
            };
            if let Some((start, end)) = self.range {
                if entry.received_at_utc_ms < start {
                    continue;
                }
                if entry.received_at_utc_ms >= end {
                    self.finished = true;
                    break;
                }
            }
            page.push(entry);
        }
        Ok(page)
    }
}

pub struct MarketObservationCursor {
    inner: TypedArtifactCursor<MarketObservationCausalShellV3>,
    condition_id: Option<String>,
    received_range: Option<(i64, i64)>,
    finished: bool,
}

impl MarketObservationCursor {
    pub fn open(
        manifest_path: &Path,
        condition_id: Option<String>,
        received_range: Option<(i64, i64)>,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        if condition_id
            .as_deref()
            .is_some_and(|condition_id| condition_id.trim().is_empty())
        {
            return Err(GateAError::InvalidConfig(
                "Observation 市场过滤条件不得为空",
            ));
        }
        validate_optional_range(received_range, "Observation 接收时间范围无效")?;
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::MarketObservations,
                decode_market_observation_causal_shell_v3,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
            condition_id,
            received_range,
            finished: false,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(
        &mut self,
        limit: usize,
    ) -> Result<Vec<MarketObservationCausalShellV3>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit && !self.finished {
            let Some(observation) = self.inner.next_value()? else {
                break;
            };
            let cutoff = observation.visibility_cutoff.received_at_utc_ms;
            if let Some((start, end)) = self.received_range {
                if cutoff < start {
                    continue;
                }
                if cutoff >= end {
                    self.finished = true;
                    break;
                }
            }
            if self
                .condition_id
                .as_deref()
                .is_none_or(|condition_id| observation.market.condition_id.0 == condition_id)
            {
                page.push(observation);
            }
        }
        Ok(page)
    }
}

pub struct ReductionReportCursor {
    inner: TypedArtifactCursor<ReductionReportEntryV1>,
}

impl ReductionReportCursor {
    pub fn open(
        manifest_path: &Path,
        batch_rows: usize,
        max_batch_bytes: usize,
        max_page_rows: usize,
    ) -> Result<Self, GateAError> {
        Ok(Self {
            inner: TypedArtifactCursor::open(
                manifest_path,
                GateAArtifactKind::ReductionReports,
                decode_reduction_report_entry_v1,
                batch_rows,
                max_batch_bytes,
                max_page_rows,
            )?,
        })
    }

    pub fn manifest_sha256(&self) -> &Sha256Hex {
        self.inner.validated.manifest_sha256()
    }

    pub const fn next_logical_position(&self) -> u64 {
        self.inner.next_logical_position
    }

    pub fn next_page(&mut self, limit: usize) -> Result<Vec<ReductionReportEntryV1>, GateAError> {
        let limit = self.inner.checked_limit(limit)?;
        let mut page = Vec::with_capacity(limit);
        while page.len() < limit {
            let Some(report) = self.inner.next_value()? else {
                break;
            };
            page.push(report);
        }
        Ok(page)
    }
}

fn validate_optional_range(
    range: Option<(i64, i64)>,
    message: &'static str,
) -> Result<(), GateAError> {
    if range.is_some_and(|(start, end)| start <= 0 || end <= start) {
        return Err(GateAError::InvalidConfig(message));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use contracts::{
        AssetIdentityV1, BestBidAskV2, BinanceAggregateTradeReferenceV1,
        BinanceDepthStateReferenceV1, BookSide, CANONICAL_REFERENCE_EVENT_SCHEMA_V1,
        CanonicalSourceKind, ChainlinkBtcUsdReferenceV1, ClobMarketId, ConditionId,
        EXECUTION_MARKET_EVENT_SCHEMA_V3, EventLineageV2, EventQualityV2, EventTimeV2,
        ExactDecimal, InitialL2SnapshotV2, KnownMarketLifecycleState, LevelExecutionBasis,
        LevelExecutionTransitionV3, MappedMarketLifecycleStateV1, MarketIdentityV2,
        MarketLifecycleTransitionV2, MarketResolutionTerminalV3, MarketTradingStateV2,
        MarketWindowV2, OutcomeIdentityV1, PayoutEntryV1, PriceLevelV1, PublicTradeIdSourceV1,
        PublicTradeV2, QueueResultQuality, ReferencePriceLevelV1, SourceAttributionQuality,
        SourceEvidenceRefV2, SourceNativeIdentityV1, TickSizeChangeV2, TokenId, TradeSide,
    };
    use data_pipeline_core::reduction::{
        QuantityConservationV1, REDUCTION_REPORT_SCHEMA_V1, ReductionOperationKindV1,
        build_chainlink_reference_outcome_v1, encode_reduction_report_entry_v1,
    };
    use data_pipeline_core::typed_raw::{
        POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA, PolymarketRtdsChainlinkBtcUsdRow,
        RtdsFreshnessStatus,
    };
    use data_pipeline_core::{
        derive_execution_market_event_v3_id, derive_reference_event_v1_id,
        encode_canonical_event_record_v1, validate_execution_market_event_v3,
        validate_reference_event_v1,
    };
    use rustix::fs::{CWD, Mode, mkfifoat};
    use tempfile::TempDir;

    pub(crate) struct MicroCandidate {
        pub(crate) _temporary: TempDir,
        pub(crate) candidate: GateACandidate,
        pub(crate) candidate_path: PathBuf,
        pub(crate) output_config: GateAOutputConfig,
        pub(crate) validated: ValidatedGateACandidate,
        pub(crate) event_ids: Vec<CanonicalEventId>,
    }

    fn micro_sort_config() -> crate::SortConfig {
        crate::SortConfig {
            max_run_rows: 16,
            max_run_bytes: 1024 * 1024,
            max_temp_run_files: 8,
            merge_fan_in: 2,
            max_open_files: 3,
            batch_rows: 4,
            max_temp_bytes: 16 * 1024 * 1024,
        }
    }

    fn micro_configs() -> (CanonicalRunConfig, GateAOutputConfig) {
        let sort = micro_sort_config();
        (
            CanonicalRunConfig {
                decode_batch_rows: 4,
                max_decode_batch_bytes: 1024 * 1024,
                max_temporary_row_bytes: 64 * 1024,
                max_epoch_count_per_source: 1,
                max_pending_ws_terminals: 1,
                max_retained_reducer_entries: 16,
                canonical_rule_version: "canonical-test-v1".to_owned(),
                sort: sort.clone(),
            },
            GateAOutputConfig {
                max_artifact_payload_bytes: 64 * 1024,
                max_capture_output_bytes: 16 * 1024 * 1024,
                max_candidate_directory_bytes: 128 * 1024 * 1024,
                max_observation_l2_levels: 16,
                sort,
            },
        )
    }

    #[test]
    fn quality_witness_selection_is_bounded_and_requires_real_link() {
        let component = ObservationComponentId::CurrentTick;
        let mut candidates = BTreeMap::new();
        for sequence in 0u64..10_000 {
            replace_projection_witness_candidates(
                &mut candidates,
                &[component],
                &ProjectionWitnessCandidate {
                    event_id: CanonicalEventId(format!("{sequence:064x}")),
                    primary_kind: Some(GateAProjectionWitnessKind::DynamicTick),
                    gap_resync: false,
                },
            );
        }
        assert_eq!(candidates.len(), 1, "每个组件只能保留最后一个待匹配事件");

        let latest_event_id = CanonicalEventId(format!("{:064x}", 9_999u64));
        let unrelated_event_id = CanonicalEventId("f".repeat(64));
        let observation_id = Sha256Hex("a".repeat(64));
        let mut witnesses = GateAProjectionWitnesses::default();
        remember_projection_witness(
            component,
            Some(&unrelated_event_id),
            &observation_id,
            &candidates,
            &mut witnesses,
        );
        remember_projection_witness(
            ObservationComponentId::BestBidAsk,
            Some(&latest_event_id),
            &observation_id,
            &candidates,
            &mut witnesses,
        );
        assert!(
            witnesses.linked.is_empty(),
            "不得拼接无关事件与 Observation"
        );

        remember_projection_witness(
            component,
            Some(&latest_event_id),
            &observation_id,
            &candidates,
            &mut witnesses,
        );
        assert_eq!(witnesses.linked.len(), 1);
        assert_eq!(
            witnesses
                .linked
                .get(&GateAProjectionWitnessKind::DynamicTick),
            Some(&GateAProjectionWitness {
                event_id: latest_event_id,
                observation_id,
            })
        );
    }

    #[test]
    fn capture_row_groups_follow_the_bounded_write_batches() {
        let temporary = TempDir::new().unwrap();
        let capture_path = temporary.path().join("capture");
        let (_, mut output_config) = micro_configs();
        output_config.sort.batch_rows = 2;
        output_config.sort.max_run_bytes = 64 * 1024;
        output_config.max_artifact_payload_bytes = 40 * 1024;
        let mut writer = SequentialCaptureWriter::create(
            "test_records",
            &capture_path,
            Arc::new(Mutex::new(0)),
            &output_config,
        )
        .unwrap();
        for sequence in 0u64..9 {
            let mut key = sequence.to_be_bytes().to_vec();
            key.reserve(128);
            assert!(key.capacity() > key.len());
            writer.push(sequence, key, &vec![b'x'; 32_720]).unwrap();
            let scanned = writer.rows.capacity() * std::mem::size_of::<SortRow>()
                + writer
                    .rows
                    .iter()
                    .map(|row| row.sort_key.capacity() + row.payload.capacity())
                    .sum::<usize>();
            assert_eq!(
                sort_row_buffer_bytes(&writer.rows, writer.row_inner_bytes),
                Some(scanned)
            );
            assert_eq!(writer.metrics.peak_write_buffer_bytes, scanned);
        }
        writer.flush_batch().unwrap();
        assert!(writer.rows.is_empty());
        assert_eq!(writer.row_inner_bytes, 0);
        let outcome = writer.finish(&output_config).unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(outcome.output_path).unwrap())
                .unwrap();
        let row_groups = builder.metadata().row_groups();

        assert!(row_groups.len() > 1);
        assert!(
            row_groups.iter().all(|row_group| {
                usize::try_from(row_group.num_rows())
                    .is_ok_and(|rows| rows <= outcome.metrics.peak_write_buffer_rows)
            }),
            "捕获 Parquet writer 不得跨越已经按字节上限刷新的内存批次重新累计大行组"
        );
    }

    #[test]
    fn capture_failed_write_keeps_owned_rows_and_capacity() {
        let temporary = TempDir::new().unwrap();
        let (_, mut config) = micro_configs();
        config.max_capture_output_bytes = 4;
        let mut writer = SequentialCaptureWriter::create(
            "test_records",
            &temporary.path().join("capture"),
            Arc::new(Mutex::new(0)),
            &config,
        )
        .unwrap();
        let mut key = 0u64.to_be_bytes().to_vec();
        key.reserve(128);
        writer.push(0, key, b"payload").unwrap();
        let scanned = writer.rows.capacity() * std::mem::size_of::<SortRow>()
            + writer
                .rows
                .iter()
                .map(|row| row.sort_key.capacity() + row.payload.capacity())
                .sum::<usize>();
        let error = writer.flush_batch().unwrap_err();
        assert!(
            error.to_string().contains("捕获产物超过冻结的总字节上限"),
            "{error}"
        );
        assert_eq!(writer.rows.len(), 1);
        assert_eq!(writer.rows[0].payload(), b"payload");
        assert_eq!(writer.expected_sequence, 1);
        assert_eq!(
            sort_row_buffer_bytes(&writer.rows, writer.row_inner_bytes),
            Some(scanned)
        );
    }

    fn chainlink_outcome(
        sequence: u64,
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
    ) -> (CanonicalEventRecordV1, ReductionReportEntryV1) {
        let value = format!("{}", 64_000 + sequence);
        let row = PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: received_at_utc_ms,
            message_timestamp: source_event_at_utc_ms,
            payload_timestamp: source_event_at_utc_ms,
            age_ms: Some(received_at_utc_ms - source_event_at_utc_ms),
            symbol: "btc/usd".to_owned(),
            value: value.clone(),
            topic: "crypto_prices_chainlink".to_owned(),
            message_type: "update".to_owned(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "btc/usd".to_owned(),
            source_native_payload_json: format!(
                r#"{{"sequence":{sequence},"timestamp":{source_event_at_utc_ms},"value":"{value}"}}"#
            ),
            collector_host_id: "collector-1".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://example.invalid".to_owned(),
            schema_version: POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.to_owned(),
        };
        let outcome = build_chainlink_reference_outcome_v1(
            &row,
            &Sha256Hex("a".repeat(64)),
            &Sha256Hex("b".repeat(64)),
            "canonical-test-v1",
        )
        .unwrap();
        (
            CanonicalEventRecordV1::ReferenceEvent(Box::new(outcome.event.unwrap())),
            outcome.report,
        )
    }

    fn market_lifecycle_outcome(
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
    ) -> (CanonicalEventRecordV1, ReductionReportEntryV1) {
        let mut event = ExecutionMarketEventV3 {
            schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            event_id: CanonicalEventId("0".repeat(64)),
            market: MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                clob_market_id: ClobMarketId("3195576".to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 900,
                    end_at_utc_ms: 10_000,
                },
            },
            time: EventTimeV2 {
                source_event_at_utc_ms: Some(source_event_at_utc_ms),
                received_at_utc_ms,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "canonical-test-v1".to_owned(),
                source_evidence: vec![SourceEvidenceRefV2 {
                    source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
                    source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                        sha256: Sha256Hex("8".repeat(64)),
                    }),
                    source_event_at_utc_ms: Some(source_event_at_utc_ms),
                    received_at_utc_ms,
                    collector_admission: None,
                    input_manifest_sha256: Sha256Hex("a".repeat(64)),
                    input_file_sha256: Sha256Hex("b".repeat(64)),
                }],
            },
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            payload: ExecutionMarketEventPayloadV3::MarketLifecycleTransition(
                MarketLifecycleTransitionV2 {
                    previous_state: None,
                    current_state: MappedMarketLifecycleStateV1::Known {
                        value: KnownMarketLifecycleState::Active,
                    },
                    assets: vec![
                        AssetIdentityV1 {
                            token_id: TokenId("1001".to_owned()),
                            outcome: OutcomeIdentityV1 {
                                index: 0,
                                label: "Up".to_owned(),
                            },
                        },
                        AssetIdentityV1 {
                            token_id: TokenId("1002".to_owned()),
                            outcome: OutcomeIdentityV1 {
                                index: 1,
                                label: "Down".to_owned(),
                            },
                        },
                    ],
                },
            ),
        };
        event.event_id = derive_execution_market_event_v3_id(&event).unwrap();
        validate_execution_market_event_v3(&event).unwrap();
        let event_id = event.event_id.clone();
        (
            CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(event)),
            ReductionReportEntryV1 {
                schema_version: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
                canonical_rule_version: "canonical-test-v1".to_owned(),
                scope: "market_lifecycle:test".to_owned(),
                operation: ReductionOperationKindV1::Merge,
                reason_code: "POLYMARKET_MARKET_LIFECYCLE_TRANSITION".to_owned(),
                quantity_conservation: None,
                disposition: ReductionDispositionV1::Produced {
                    result_event_id: event_id,
                },
            },
        )
    }

    fn build_micro_candidate() -> MicroCandidate {
        let temporary = TempDir::new().unwrap();
        let candidate_path = temporary.path().join("candidate");
        let (config, output_config) = micro_configs();
        let scope = CanonicalRunScope::new(format!("0x{}", "1".repeat(64)), 3_000, 7_000).unwrap();
        let outcomes = vec![
            chainlink_outcome(0, 3_000, 6_000),
            chainlink_outcome(1, 1_000, 4_000),
            market_lifecycle_outcome(2_000, 5_000),
        ];
        materialize_micro_candidate(
            temporary,
            candidate_path,
            config,
            output_config,
            scope,
            outcomes,
            Vec::new(),
            crate::canonical_run::SourceRowCounts::default(),
        )
    }

    pub(crate) fn build_sim_adapter_candidate() -> MicroCandidate {
        let temporary = TempDir::new().unwrap();
        let candidate_path = temporary.path().join("sim-adapter-candidate");
        let (config, output_config) = micro_configs();
        let scope = CanonicalRunScope::new(format!("0x{}", "1".repeat(64)), 4_000, 8_000).unwrap();
        let outcomes = vec![
            market_lifecycle_outcome(1_000, 5_500),
            chainlink_outcome(0, 1_000, 5_000),
            market_lifecycle_outcome(1_000, 6_000),
            chainlink_outcome(1, 4_000, 5_000),
            market_lifecycle_outcome(3_000, 7_000),
        ];
        materialize_micro_candidate(
            temporary,
            candidate_path,
            config,
            output_config,
            scope,
            outcomes,
            Vec::new(),
            crate::canonical_run::SourceRowCounts::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_micro_candidate(
        temporary: TempDir,
        candidate_path: PathBuf,
        config: CanonicalRunConfig,
        output_config: GateAOutputConfig,
        scope: CanonicalRunScope,
        outcomes: Vec<(CanonicalEventRecordV1, ReductionReportEntryV1)>,
        selected_files: Vec<CanonicalRunInputFile>,
        selected_rows: crate::canonical_run::SourceRowCounts,
    ) -> MicroCandidate {
        let work_path = candidate_path.join(WORK_DIRECTORY);
        let validation_path = temporary.path().join("validation");
        let records = outcomes
            .iter()
            .map(|(record, _)| record.clone())
            .collect::<Vec<_>>();
        let event_ids = records.iter().map(record_event_id).cloned().collect();

        let mut record_digest = StreamDigest::new(RECORD_DIGEST_DOMAIN);
        let mut event_time_digest = StreamDigest::new(EVENT_TIME_PROJECTION_DIGEST_DOMAIN);
        let mut visibility_digest = StreamDigest::new(VISIBILITY_PROJECTION_DIGEST_DOMAIN);
        let encoded_records = records
            .iter()
            .map(|record| {
                let encoded = encode_canonical_event_record_v1(record).unwrap();
                record_digest.push(&encoded, "micro_records").unwrap();
                if let Some(event_time) = project_record_event_time_index_entry(record).unwrap() {
                    event_time_digest
                        .push(
                            &encode_event_time_index_entry(&event_time).unwrap(),
                            "micro_event_time",
                        )
                        .unwrap();
                }
                visibility_digest
                    .push(
                        &encode_visibility_index_entry(
                            &project_record_visibility_index_entry(record).unwrap(),
                        )
                        .unwrap(),
                        "micro_visibility",
                    )
                    .unwrap();
                encoded
            })
            .collect::<Vec<_>>();
        let mut report_digest = StreamDigest::new(REDUCTION_REPORT_DIGEST_DOMAIN);
        let encoded_reports = outcomes
            .iter()
            .map(|(_, report)| {
                let encoded = encode_reduction_report_entry_v1(report).unwrap();
                report_digest.push(&encoded, "micro_reports").unwrap();
                encoded
            })
            .collect::<Vec<_>>();
        let staging = crate::canonical_run::CanonicalRunStaging {
            workspace_path: work_path.join("unused-workspace"),
            temporary_sorted_input_path: work_path.join("unused-input.parquet"),
            input_set_identity_sha256: "c".repeat(64),
            input_manifest_set_sha256: "d".repeat(64),
            scope,
            config: config.clone(),
            selected_files: selected_files.clone(),
            temporary_sort_logical_sha256: "e".repeat(64),
            digests: CanonicalRunDigests {
                canonical_records_sha256: record_digest.finish(),
                event_time_projections_sha256: event_time_digest.finish(),
                visibility_projections_sha256: visibility_digest.finish(),
                reduction_reports_sha256: report_digest.finish(),
            },
            metrics: CanonicalRunMetrics {
                selected_rows,
                parallelism: 1,
                canonical_records: u64::try_from(records.len()).unwrap(),
                execution_market_events: u64::try_from(
                    records
                        .iter()
                        .filter(|record| record.as_execution_market_event().is_some())
                        .count(),
                )
                .unwrap(),
                reference_events: u64::try_from(
                    records
                        .iter()
                        .filter(|record| record.as_execution_market_event().is_none())
                        .count(),
                )
                .unwrap(),
                event_time_projections: u64::try_from(
                    records
                        .iter()
                        .filter(|record| match record {
                            CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
                                event.time.source_event_at_utc_ms.is_some()
                            }
                            CanonicalEventRecordV1::ReferenceEvent(event) => {
                                event.time.source_event_at_utc_ms.is_some()
                            }
                        })
                        .count(),
                )
                .unwrap(),
                visibility_projections: u64::try_from(records.len()).unwrap(),
                reduction_reports: u64::try_from(outcomes.len()).unwrap(),
                ..CanonicalRunMetrics::default()
            },
        };

        let mut capture = CaptureArtifacts::create(&work_path, &output_config).unwrap();
        for (sequence, encoded) in encoded_records.iter().enumerate() {
            capture
                .capture_record(u64::try_from(sequence).unwrap(), encoded)
                .unwrap();
        }
        for (sequence, encoded) in encoded_reports.iter().enumerate() {
            capture
                .capture_report(u64::try_from(sequence).unwrap(), encoded)
                .unwrap();
        }
        let (record_capture, report_capture, _) = capture.finish(&output_config).unwrap();
        let canonical_record_capture_bytes =
            fs::metadata(&record_capture.output_path).unwrap().len();
        let reduction_report_capture_bytes =
            fs::metadata(&report_capture.output_path).unwrap().len();
        let store = finish_artifact(
            record_capture,
            &work_path.join(CAPTURE_RECORD_DIRECTORY),
            &candidate_path,
            GateAArtifactKind::CanonicalEventStore,
            &staging,
            Vec::new(),
            &output_config,
        )
        .unwrap();
        let store_dependency = GateAArtifactDependencyV1 {
            artifact_kind: GateAArtifactKind::CanonicalEventStore,
            manifest_sha256: store.manifest_sha256.clone(),
        };
        let event_time = build_event_time_index(
            &candidate_path.join(&store.manifest.file.relative_path),
            &candidate_path,
            &staging,
            &output_config,
            vec![store_dependency.clone()],
        )
        .unwrap();
        let (visibility_material, visibility_material_sort) = build_visibility_material(
            &candidate_path.join(&store.manifest.file.relative_path),
            &work_path,
            &output_config,
            &staging,
        )
        .unwrap();
        let visibility = build_visibility_index(
            &visibility_material,
            &candidate_path,
            &staging,
            &output_config,
            vec![store_dependency.clone()],
        )
        .unwrap();
        let (observations, peak_observation_l2_levels) = build_observations(
            &visibility_material,
            &candidate_path,
            &staging,
            &output_config,
            vec![
                store_dependency.clone(),
                GateAArtifactDependencyV1 {
                    artifact_kind: GateAArtifactKind::VisibilityIndex,
                    manifest_sha256: visibility.manifest_sha256.clone(),
                },
            ],
            &visibility.manifest_sha256,
        )
        .unwrap();
        let reduction_reports = finish_artifact(
            report_capture,
            &work_path.join(CAPTURE_REPORT_DIRECTORY),
            &candidate_path,
            GateAArtifactKind::ReductionReports,
            &staging,
            vec![store_dependency],
            &output_config,
        )
        .unwrap();
        fs::remove_dir_all(&work_path).unwrap();
        let peak_candidate_directory_bytes_bound = directory_bytes(&candidate_path).unwrap();
        let mut candidate = GateACandidate {
            candidate_path: candidate_path.clone(),
            input_set_identity_sha256: Sha256Hex(staging.input_set_identity_sha256),
            input_manifest_set_sha256: Sha256Hex(staging.input_manifest_set_sha256),
            scope: staging.scope,
            config: staging.config,
            output_config: output_config.clone(),
            selected_files,
            canonical_digests: staging.digests,
            canonical_metrics: staging.metrics,
            canonical_event_store: store,
            event_time_index: event_time,
            visibility_index: visibility,
            market_observations: observations,
            reduction_reports,
            resource_metrics: GateABuildResourceMetrics {
                canonical_record_capture_bytes,
                reduction_report_capture_bytes,
                visibility_material_sort,
                peak_observation_l2_levels,
                peak_candidate_directory_bytes_bound,
                peak_formal_work_directory_bytes_bound: peak_candidate_directory_bytes_bound
                    + GATE_A_CONTROL_FILE_BYTES_BOUND,
            },
        };
        let validated =
            validate_gate_a_candidate(&candidate_path, &validation_path, &output_config).unwrap();
        candidate
            .resource_metrics
            .peak_formal_work_directory_bytes_bound = candidate
            .resource_metrics
            .peak_formal_work_directory_bytes_bound
            .max(validated.peak_formal_work_directory_bytes_bound);
        MicroCandidate {
            _temporary: temporary,
            candidate,
            candidate_path,
            output_config,
            validated,
            event_ids,
        }
    }

    fn fixture_asset(token_id: &str, index: u32, label: &str) -> AssetIdentityV1 {
        AssetIdentityV1 {
            token_id: TokenId(token_id.to_owned()),
            outcome: OutcomeIdentityV1 {
                index,
                label: label.to_owned(),
            },
        }
    }

    fn fixture_market() -> MarketIdentityV2 {
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            clob_market_id: ClobMarketId("3195576".to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 1_000,
                end_at_utc_ms: 10_000,
            },
        }
    }

    fn fixture_evidence(
        source_kind: CanonicalSourceKind,
        source_event_at_utc_ms: Option<i64>,
        received_at_utc_ms: i64,
    ) -> SourceEvidenceRefV2 {
        let source_native_identity = match source_kind {
            CanonicalSourceKind::PolymarketMarketMetadata
            | CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd => {
                Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: Sha256Hex("8".repeat(64)),
                })
            }
            CanonicalSourceKind::BinanceDepthSnapshot => {
                Some(SourceNativeIdentityV1::BinanceDepthSnapshot {
                    last_update_id: 4_199,
                })
            }
            CanonicalSourceKind::BinanceAggtrade => {
                Some(SourceNativeIdentityV1::BinanceAggregateTrade {
                    aggregate_trade_id: 4_299,
                })
            }
            CanonicalSourceKind::PolymarketOrderbook => None,
            CanonicalSourceKind::BinanceDepthUpdate => unreachable!(),
        };
        let collector_admission = matches!(
            source_kind,
            CanonicalSourceKind::PolymarketOrderbook
                | CanonicalSourceKind::BinanceDepthSnapshot
                | CanonicalSourceKind::BinanceAggtrade
        )
        .then(|| contracts::CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "fixture-epoch".to_owned(),
            collector_admission_sequence: u64::try_from(received_at_utc_ms).unwrap(),
            admitted_at_utc_ms: received_at_utc_ms + 1,
        });
        SourceEvidenceRefV2 {
            source_kind,
            source_native_identity,
            source_event_at_utc_ms,
            received_at_utc_ms,
            collector_admission,
            input_manifest_sha256: Sha256Hex("a".repeat(64)),
            input_file_sha256: Sha256Hex("b".repeat(64)),
        }
    }

    fn fixture_market_record(
        received_at_utc_ms: i64,
        source_kind: CanonicalSourceKind,
        payload: ExecutionMarketEventPayloadV3,
    ) -> CanonicalEventRecordV1 {
        let source_event_at_utc_ms = received_at_utc_ms - 1;
        let mut event = ExecutionMarketEventV3 {
            schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            event_id: CanonicalEventId("0".repeat(64)),
            market: fixture_market(),
            time: EventTimeV2 {
                source_event_at_utc_ms: Some(source_event_at_utc_ms),
                received_at_utc_ms,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "canonical-production-run-v1".to_owned(),
                source_evidence: vec![fixture_evidence(
                    source_kind,
                    Some(source_event_at_utc_ms),
                    received_at_utc_ms,
                )],
            },
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            payload,
        };
        event.event_id = derive_execution_market_event_v3_id(&event).unwrap();
        validate_execution_market_event_v3(&event).unwrap();
        CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(event))
    }

    fn fixture_reference_record(
        received_at_utc_ms: i64,
        source_kind: CanonicalSourceKind,
        source_event_at_utc_ms: Option<i64>,
        payload: CanonicalReferenceEventPayloadV1,
    ) -> CanonicalEventRecordV1 {
        let evidence = fixture_evidence(source_kind, source_event_at_utc_ms, received_at_utc_ms);
        let mut event = CanonicalReferenceEventV1 {
            schema_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
            event_id: CanonicalEventId("0".repeat(64)),
            time: EventTimeV2 {
                source_event_at_utc_ms,
                received_at_utc_ms,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "canonical-production-run-v1".to_owned(),
                source_evidence: vec![evidence],
            },
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            payload,
        };
        event.event_id = derive_reference_event_v1_id(&event).unwrap();
        validate_reference_event_v1(&event).unwrap();
        CanonicalEventRecordV1::ReferenceEvent(Box::new(event))
    }

    fn fixture_outcome(
        record: CanonicalEventRecordV1,
        reason_code: &str,
        quantity_conservation: Option<QuantityConservationV1>,
    ) -> (CanonicalEventRecordV1, ReductionReportEntryV1) {
        let report = ReductionReportEntryV1 {
            schema_version: REDUCTION_REPORT_SCHEMA_V1.to_owned(),
            canonical_rule_version: "canonical-production-run-v1".to_owned(),
            scope: "published-final-fixture".to_owned(),
            operation: ReductionOperationKindV1::Merge,
            reason_code: reason_code.to_owned(),
            quantity_conservation,
            disposition: ReductionDispositionV1::Produced {
                result_event_id: record_event_id(&record).clone(),
            },
        };
        (record, report)
    }

    fn published_final_fixture_outcomes() -> Vec<(CanonicalEventRecordV1, ReductionReportEntryV1)> {
        let up = fixture_asset("1001", 0, "Up");
        let down = fixture_asset("1002", 1, "Down");
        let lifecycle = |received, payload| {
            fixture_market_record(
                received,
                CanonicalSourceKind::PolymarketMarketMetadata,
                payload,
            )
        };
        let orderbook = |received, payload| {
            fixture_market_record(received, CanonicalSourceKind::PolymarketOrderbook, payload)
        };
        let snapshot = |asset: AssetIdentityV1, cursor: u64, digest: &str| {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
                asset,
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                source_sequence_epoch_id: "fixture-epoch".to_owned(),
                source_cursor: cursor,
                book_digest_sha256: Sha256Hex(digest.repeat(64)),
                bids: vec![PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                    quantity_shares: ExactDecimal("10".to_owned()),
                }],
                asks: vec![PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                    quantity_shares: ExactDecimal("10".to_owned()),
                }],
            })
        };
        let bba = |asset| {
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset,
                best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
                best_ask_pusd_per_share: Some(ExactDecimal("0.6".to_owned())),
            })
        };
        let tick = |asset| {
            ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
                asset,
                old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
                new_tick_size_pusd_per_share: ExactDecimal("0.02".to_owned()),
            })
        };
        let ordinary = |record| fixture_outcome(record, "PUBLISHED_FINAL_FIXTURE", None);
        vec![
            ordinary(lifecycle(
                3_100,
                ExecutionMarketEventPayloadV3::MarketLifecycleTransition(
                    MarketLifecycleTransitionV2 {
                        previous_state: None,
                        current_state: MappedMarketLifecycleStateV1::Known {
                            value: KnownMarketLifecycleState::Active,
                        },
                        assets: vec![up.clone(), down.clone()],
                    },
                ),
            )),
            ordinary(lifecycle(
                3_200,
                ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
                    active: Some(true),
                    closed: Some(false),
                    accepting_orders: Some(true),
                }),
            )),
            ordinary(orderbook(3_300, snapshot(up.clone(), 1, "1"))),
            fixture_outcome(
                orderbook(3_400, snapshot(down.clone(), 2, "2")),
                "POLYMARKET_IDENTICAL_BOOK_CONFIRMATION",
                None,
            ),
            ordinary(orderbook(3_500, bba(up.clone()))),
            ordinary(orderbook(3_600, bba(down.clone()))),
            ordinary(orderbook(3_700, tick(up.clone()))),
            ordinary(orderbook(3_800, tick(down.clone()))),
            ordinary(orderbook(
                3_900,
                ExecutionMarketEventPayloadV3::PublicTrade(PublicTradeV2 {
                    asset: up.clone(),
                    trade_id: "fixture-trade".to_owned(),
                    trade_id_source: PublicTradeIdSourceV1::PlatformTransactionHash,
                    price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                    quantity_shares: ExactDecimal("3".to_owned()),
                    aggressor_side: TradeSide::Sell,
                    fee_rate_bps: None,
                }),
            )),
            fixture_outcome(
                orderbook(
                    4_000,
                    ExecutionMarketEventPayloadV3::LevelExecutionTransition(
                        LevelExecutionTransitionV3 {
                            causal_group_id: Sha256Hex("3".repeat(64)),
                            market: fixture_market(),
                            asset: up.clone(),
                            side: BookSide::Bid,
                            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                            quantity_before_shares: ExactDecimal("10".to_owned()),
                            quantity_after_shares: ExactDecimal("7".to_owned()),
                            attributed_trade_quantity_shares: ExactDecimal("3".to_owned()),
                            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
                            net_added_quantity_shares: ExactDecimal("0".to_owned()),
                            net_reduced_quantity_shares: ExactDecimal("3".to_owned()),
                            queue_consumption_quantity_shares: ExactDecimal("3".to_owned()),
                            basis: LevelExecutionBasis::TradeEvidence,
                            platform_effective_at_utc_ms: 3_999,
                            visible_at_utc_ms: 4_000,
                            source_attribution_quality: SourceAttributionQuality::Exact,
                            queue_result_quality: QueueResultQuality::Exact,
                        },
                    ),
                ),
                "POLYMARKET_ABSOLUTE_LEVEL_UPDATE",
                Some(QuantityConservationV1 {
                    quantity_before: ExactDecimal("10".to_owned()),
                    quantity_after: ExactDecimal("7".to_owned()),
                    quantity_added: ExactDecimal("0".to_owned()),
                    quantity_reduced: ExactDecimal("3".to_owned()),
                }),
            ),
            ordinary(fixture_reference_record(
                4_100,
                CanonicalSourceKind::PolymarketRtdsChainlinkBtcUsd,
                Some(4_099),
                CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1 {
                    symbol: "btc/usd".to_owned(),
                    price_usd_per_btc: ExactDecimal("64000".to_owned()),
                    message_timestamp_utc_ms: 4_099,
                }),
            )),
            ordinary(fixture_reference_record(
                4_200,
                CanonicalSourceKind::BinanceDepthSnapshot,
                None,
                CanonicalReferenceEventPayloadV1::BinanceDepthState(BinanceDepthStateReferenceV1 {
                    symbol: "BTCUSDT".to_owned(),
                    last_update_id: 4_199,
                    bids: vec![ReferencePriceLevelV1 {
                        price_usdt_per_btc: ExactDecimal("63999".to_owned()),
                        quantity_btc: ExactDecimal("1".to_owned()),
                    }],
                    asks: vec![ReferencePriceLevelV1 {
                        price_usdt_per_btc: ExactDecimal("64001".to_owned()),
                        quantity_btc: ExactDecimal("1".to_owned()),
                    }],
                }),
            )),
            ordinary(fixture_reference_record(
                4_300,
                CanonicalSourceKind::BinanceAggtrade,
                Some(4_299),
                CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(
                    BinanceAggregateTradeReferenceV1 {
                        symbol: "BTCUSDT".to_owned(),
                        aggregate_trade_id: 4_299,
                        first_trade_id: 4_299,
                        last_trade_id: 4_299,
                        price_usdt_per_btc: ExactDecimal("64000".to_owned()),
                        quantity_btc: ExactDecimal("1".to_owned()),
                        buyer_is_maker: false,
                        event_time_utc_ms: 4_299,
                    },
                ),
            )),
            ordinary(lifecycle(
                4_400,
                ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
                    MarketResolutionTerminalV3::Resolved {
                        winning_asset_id: up.token_id.0.clone(),
                        winning_outcome: up.outcome.label.clone(),
                        payout_vector: vec![
                            PayoutEntryV1 {
                                asset: up,
                                payout_pusd_per_share: ExactDecimal("1".to_owned()),
                            },
                            PayoutEntryV1 {
                                asset: down,
                                payout_pusd_per_share: ExactDecimal("0".to_owned()),
                            },
                        ],
                        mapping_rule_version: "fixture-binary-payout-v1".to_owned(),
                    },
                ),
            )),
        ]
    }

    fn published_final_fixture_selected_files() -> Vec<CanonicalRunInputFile> {
        [
            "binance_aggtrade",
            "binance_depth_snapshot",
            "binance_depth_update",
            "polymarket_market",
            "polymarket_market_metadata",
            "polymarket_rtds_chainlink_btc_usd",
        ]
        .into_iter()
        .enumerate()
        .map(|(part, source_kind)| CanonicalRunInputFile {
            utc_day: "2026-08-03".to_owned(),
            manifest_relative_path: format!("source_kind={source_kind}/part-{part}.parquet"),
            source_kind: source_kind.to_owned(),
            schema_version: crate::supported_schema_for_source_kind(source_kind)
                .unwrap()
                .to_owned(),
            utc_hour: "2026-08-03T00".to_owned(),
            part: u32::try_from(part).unwrap(),
            manifest_sha256: "a".repeat(64),
            file_sha256: format!("{:064x}", part + 1),
            bytes: 1,
            rows: 1,
        })
        .collect()
    }

    pub(crate) fn build_published_final_fixture_candidate() -> MicroCandidate {
        let temporary = TempDir::new().unwrap();
        let stage = temporary.path().join(".published-final-fixture.candidate");
        fs::create_dir(&stage).unwrap();
        fs::create_dir(stage.join("reports")).unwrap();
        materialize_micro_candidate(
            temporary,
            stage.join("artifacts"),
            crate::gate_a_publication::gate_a_formal_canonical_config(),
            crate::gate_a_publication::gate_a_formal_output_config(),
            CanonicalRunScope::new(format!("0x{}", "1".repeat(64)), 3_000, 7_000).unwrap(),
            published_final_fixture_outcomes(),
            published_final_fixture_selected_files(),
            crate::canonical_run::SourceRowCounts {
                polymarket_orderbook: 1,
                polymarket_market_metadata: 1,
                polymarket_rtds_chainlink_btc_usd: 1,
                binance_depth_snapshot: 1,
                binance_depth_update: 1,
                binance_aggtrade: 1,
            },
        )
    }

    #[test]
    fn quality_scan_canonical_page_is_single_record_and_counts_all_rows() {
        let fixture = build_micro_candidate();
        let manifest_path = &fixture.validated.canonical_event_store.manifest_path;
        let batch_rows = fixture.output_config.sort.batch_rows;
        let max_batch_bytes = fixture.output_config.sort.max_run_bytes;

        let mut rejected = CanonicalEventStoreCursor::open(
            manifest_path,
            batch_rows,
            max_batch_bytes,
            crate::gate_a_publication::QUALITY_SCAN_CANONICAL_PAGE_ROWS,
        )
        .unwrap();
        assert!(matches!(
            rejected.next_page(2),
            Err(GateAError::InvalidConfig(_))
        ));
        assert_eq!(rejected.next_logical_position(), 0);

        let mut records = CanonicalEventStoreCursor::open(
            manifest_path,
            batch_rows,
            max_batch_bytes,
            crate::gate_a_publication::QUALITY_SCAN_CANONICAL_PAGE_ROWS,
        )
        .unwrap();
        let mut actual_event_ids = Vec::new();
        loop {
            let page = records
                .next_page(crate::gate_a_publication::QUALITY_SCAN_CANONICAL_PAGE_ROWS)
                .unwrap();
            assert!(page.len() <= 1);
            if page.is_empty() {
                break;
            }
            actual_event_ids.push(record_event_id(&page[0]).clone());
        }

        assert_eq!(actual_event_ids, fixture.event_ids);
        assert_eq!(records.next_logical_position(), 3);
    }

    #[test]
    fn platform_replay_uses_source_time_then_emission_sequence() {
        let fixture = build_micro_candidate();
        let mut replay = PlatformReplayCursor::open(
            &fixture.validated,
            None,
            fixture.output_config.sort.batch_rows,
            fixture.output_config.sort.max_run_bytes,
            1,
        )
        .unwrap();
        let mut actual = Vec::new();
        loop {
            let page = replay.next_page(1).unwrap();
            if page.is_empty() {
                break;
            }
            let source_time = match &page[0] {
                CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
                    event.time.source_event_at_utc_ms
                }
                CanonicalEventRecordV1::ReferenceEvent(event) => event.time.source_event_at_utc_ms,
            }
            .unwrap();
            actual.push((source_time, record_event_id(&page[0]).clone()));
        }

        assert_eq!(
            actual,
            vec![
                (1_000, fixture.event_ids[1].clone()),
                (2_000, fixture.event_ids[2].clone()),
                (3_000, fixture.event_ids[0].clone()),
            ]
        );
    }

    #[test]
    fn visibility_replay_preserves_index_order_and_page_closure() {
        let fixture = build_micro_candidate();
        let mut replay = VisibilityReplayCursor::open(
            &fixture.validated,
            None,
            fixture.output_config.sort.batch_rows,
            fixture.output_config.sort.max_run_bytes,
            2,
        )
        .unwrap();

        let first = replay.next_page(2).unwrap();
        assert_eq!(replay.next_logical_position(), 2);
        assert_eq!(
            first
                .iter()
                .map(record_event_id)
                .cloned()
                .collect::<Vec<_>>(),
            vec![fixture.event_ids[1].clone(), fixture.event_ids[2].clone()]
        );
        assert!(matches!(
            first[0],
            CanonicalEventRecordV1::ReferenceEvent(_)
        ));
        assert!(matches!(
            first[1],
            CanonicalEventRecordV1::ExecutionMarketEvent(_)
        ));

        let second = replay.next_page(2).unwrap();
        assert_eq!(replay.next_logical_position(), 3);
        assert_eq!(
            second
                .iter()
                .map(record_event_id)
                .cloned()
                .collect::<Vec<_>>(),
            vec![fixture.event_ids[0].clone()]
        );
        assert!(replay.next_page(2).unwrap().is_empty());

        let market_ids = first
            .into_iter()
            .chain(second)
            .filter_map(|record| {
                record
                    .as_execution_market_event()
                    .map(|event| event.event_id.clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(market_ids, vec![fixture.event_ids[2].clone()]);
    }

    #[test]
    fn visibility_replay_rejects_entry_that_does_not_project_from_store_record() {
        let fixture = build_micro_candidate();
        let replay = VisibilityReplayCursor::open(
            &fixture.validated,
            None,
            fixture.output_config.sort.batch_rows,
            fixture.output_config.sort.max_run_bytes,
            1,
        )
        .unwrap();
        let mut store = CanonicalEventStoreCursor::open(
            fixture.validated.canonical_event_store.manifest_path(),
            fixture.output_config.sort.batch_rows,
            fixture.output_config.sort.max_run_bytes,
            2,
        )
        .unwrap();
        let records = store.next_page(2).unwrap();
        let wrong_entry = project_record_visibility_index_entry(&records[1]).unwrap();

        assert!(matches!(
            replay.resolve_page(vec![VisibilityReplayRequest {
                sequence: 0,
                entry: wrong_entry,
                page_position: 0,
            }]),
            Err(GateAError::ArtifactMismatch(message))
                if message == "模型可见索引无法解析为唯一事件存储中的同一规范记录"
        ));
    }

    #[test]
    fn candidate_closure_rejects_an_event_without_a_produced_report() {
        let temporary = TempDir::new().unwrap();
        let (_, output_config) = micro_configs();
        fs::create_dir(temporary.path().join("event-ids")).unwrap();
        fs::create_dir(temporary.path().join("report-references")).unwrap();
        let mut event_ids =
            ExternalSorter::new(&temporary.path().join("event-ids"), &output_config.sort).unwrap();
        event_ids
            .push(SortRow::from_sort_key(
                event_id_sort_key(&CanonicalEventId("1".repeat(64))),
                Vec::new(),
            ))
            .unwrap();
        let event_ids = event_ids.finish().unwrap();
        let mut references = ExternalSorter::new(
            &temporary.path().join("report-references"),
            &output_config.sort,
        )
        .unwrap();
        references
            .push(SortRow::from_sort_key(
                event_reference_key(&CanonicalEventId("2".repeat(64)), 0),
                Vec::new(),
            ))
            .unwrap();
        let references = references.finish().unwrap();

        let error = validate_event_report_closure(
            &event_ids.output_path,
            &references.output_path,
            &output_config,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("唯一事件存储中的事件缺少 produced 归约报告")
        );
    }

    #[test]
    fn workspace_pipeline_identity_detects_source_drift() {
        let workspace = TempDir::new().unwrap();
        for (relative_path, bytes) in PIPELINE_CORE_SOURCES {
            let path = workspace.path().join(relative_path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
        assert_eq!(
            workspace_pipeline_core_identity_sha256(workspace.path()).unwrap(),
            pipeline_core_identity_sha256()
        );

        fs::write(workspace.path().join("Cargo.lock"), b"changed").unwrap();
        assert_ne!(
            workspace_pipeline_core_identity_sha256(workspace.path()).unwrap(),
            pipeline_core_identity_sha256()
        );
    }

    #[test]
    fn same_size_index_corruption_is_rejected_by_file_digest() {
        let fixture = build_micro_candidate();
        let path = fixture.candidate_path.join("data/visibility-index.parquet");
        let mut bytes = fs::read(&path).unwrap();
        let offset = bytes.len() / 2;
        bytes[offset] ^= 1;
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            validate_gate_a_candidate(
                &fixture.candidate_path,
                &fixture._temporary.path().join("corrupt-validation"),
                &fixture.output_config,
            ),
            Err(GateAError::ArtifactMismatch(message))
                if message == "产物文件 SHA-256 与 manifest 不一致"
        ));
    }

    #[test]
    fn fifo_manifest_is_rejected_before_reading() {
        let temporary = TempDir::new().unwrap();
        let manifests = temporary.path().join("manifests");
        fs::create_dir(&manifests).unwrap();
        let manifest_path = manifests.join("visibility-index.json");
        mkfifoat(CWD, &manifest_path, Mode::RUSR | Mode::WUSR).unwrap();

        assert!(matches!(
            validate_gate_a_artifact(
                &manifest_path,
                GateAArtifactKind::VisibilityIndex,
                4,
                1024 * 1024,
            ),
            Err(GateAError::InvalidManifest(message))
                if message == "artifact manifest 必须是普通文件且不得为符号链接"
        ));
    }
}
