//! 训练数据管道的只读准入与确定性、有界外部排序能力。
//!
//! 准入边界只接受固定直接数据根；排序行结构只包含调用方提供的不透明字节排序键
//! 和不透明载荷。本 crate 不是通用存储层或查询层，也不解释排序键的业务语义。

pub mod canonical_run;
mod dpt_gate_a_cli;
pub mod gate_a;
mod gate_a_publication;
pub mod observation;
pub mod typed_raw;

pub use gate_a_publication::{
    GateAPublicationError, GateARunManifestV2, ValidatedPublishedGateAFinal,
    gate_a_formal_canonical_config, gate_a_formal_output_config, open_published_gate_a_final,
};

/// 运行唯一正式门 A 命令入口；参数解析、测量和签发检查均在库内私有实现中完成。
pub fn run_dpt_gate_a_cli() {
    dpt_gate_a_cli::main();
}

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use contracts::{DATASET_LINEAGE_ROOT, DatasetDateEligibility, GateAReceiptV3};
use data_pipeline_core::classify_dataset_date;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use rustix::fs::{Advice, fadvise};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::Date;
use time::macros::format_description;

const WORK_DIRECTORY: &str = "dpt-external-sort";
const KEY_COLUMN: &str = "sort_key";
const PAYLOAD_COLUMN: &str = "payload";
const DIGEST_DOMAIN: &[u8] = b"data-pipeline-training/external-sort/logical-v1\0";
const INPUT_SET_IDENTITY_DIGEST_DOMAIN_V1: &[u8] =
    b"data-pipeline-training/input-set-identity/v1\0";
const INPUT_MANIFEST_SET_DIGEST_DOMAIN_V1: &[u8] =
    b"data-pipeline-training/input-manifest-set/v1\0";
const DAILY_MANIFEST_SCHEMA_VERSION_V3: &str = "data_collect_daily_manifest_v3";
const DAILY_MANIFEST_MAX_BYTES: usize = 10 * 1024 * 1024;
const COLLECTION_DAY_COVERAGE_SCHEMA_VERSION_V1: &str = "data_collect_collection_day_coverage_v1";
const REQUIRED_PRODUCER_BARRIERS: [&str; 4] = [
    "binance_aggtrade",
    "binance_depth",
    "polymarket",
    "polymarket_rtds",
];
const REQUIRED_WRITER_FINALIZE_ACKS: [&str; 6] = [
    "binance_aggtrade",
    "binance_depth_snapshot",
    "binance_depth_update",
    "polymarket_market",
    "polymarket_market_metadata",
    "polymarket_rtds_chainlink_btc_usd",
];

/// 仅有门 A 成功回执仍不构成训练数据集准入。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrainingAdmissionError {
    GateAIsNotTrainingDatasetReady,
}

impl fmt::Display for TrainingAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GateAIsNotTrainingDatasetReady => {
                write!(formatter, "门 A 回执不能替代门 B 训练数据集成功回执")
            }
        }
    }
}

impl std::error::Error for TrainingAdmissionError {}

pub fn admit_gate_a_receipt_for_training(
    _receipt: &GateAReceiptV3,
) -> Result<(), TrainingAdmissionError> {
    Err(TrainingAdmissionError::GateAIsNotTrainingDatasetReady)
}

/// 已通过固定数据根和日期下限检查的清单路径。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualifiedSourceManifestPath {
    manifest_path: PathBuf,
    utc_day: String,
}

impl QualifiedSourceManifestPath {
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn utc_day(&self) -> &str {
        &self.utc_day
    }
}

#[derive(Debug)]
pub enum ManifestPathQualificationError {
    OutsideDatasetLineageRoot { path: PathBuf },
    InvalidDatasetDate { path: PathBuf },
    IneligibleBeforeDatasetFloor { utc_day: String },
    Io(io::Error),
}

impl fmt::Display for ManifestPathQualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutsideDatasetLineageRoot { path } => write!(
                formatter,
                "清单路径不在固定数据谱系根内：{}",
                path.display()
            ),
            Self::InvalidDatasetDate { path } => {
                write!(formatter, "清单路径包含非法数据日期：{}", path.display())
            }
            Self::IneligibleBeforeDatasetFloor { utc_day } => {
                write!(formatter, "清单日期早于数据日期下限：{utc_day}")
            }
            Self::Io(error) => write!(formatter, "解析清单路径失败：{error}"),
        }
    }
}

impl std::error::Error for ManifestPathQualificationError {}

/// 校验直接源根中的每日清单路径，不读取或解析清单内容。
pub fn qualify_direct_source_manifest_path(
    manifest_path: &Path,
) -> Result<QualifiedSourceManifestPath, ManifestPathQualificationError> {
    qualify_manifest_path(Path::new(DATASET_LINEAGE_ROOT), manifest_path)
}

/// 已读取且通过每日清单头部校验的窄视图；不代表完整输入准入。
#[derive(Debug)]
pub struct ValidatedDailyManifestHeaderV3 {
    schema_version: String,
    state: String,
    utc_day: String,
    closure_reason: String,
    coverage_schema_version: String,
    coverage_state: String,
    first_started_utc_ms: Option<i64>,
    last_started_utc_ms: Option<i64>,
    ended_utc_ms: Option<i64>,
    reactivated_within_day: bool,
}

impl ValidatedDailyManifestHeaderV3 {
    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn utc_day(&self) -> &str {
        &self.utc_day
    }

    pub fn closure_reason(&self) -> &str {
        &self.closure_reason
    }

    pub fn coverage_schema_version(&self) -> &str {
        &self.coverage_schema_version
    }

    pub fn coverage_state(&self) -> &str {
        &self.coverage_state
    }

    pub fn first_started_utc_ms(&self) -> Option<i64> {
        self.first_started_utc_ms
    }

    pub fn last_started_utc_ms(&self) -> Option<i64> {
        self.last_started_utc_ms
    }

    pub fn ended_utc_ms(&self) -> Option<i64> {
        self.ended_utc_ms
    }

    pub fn reactivated_within_day(&self) -> bool {
        self.reactivated_within_day
    }
}

/// 只经清单成员资格和路径检查的 Parquet 路径；不代表内容完整。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualifiedManifestParquetPath {
    manifest_relative_path: PathBuf,
    canonical_path: PathBuf,
}

impl QualifiedManifestParquetPath {
    pub fn manifest_relative_path(&self) -> &Path {
        &self.manifest_relative_path
    }

    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }
}

/// 已通过每日清单头部和文件集路径资格检查的窄结果。
///
/// 该结果不校验 Parquet 内容、行数、字节数、SHA-256 或围栏，也不是完整准入或门 A 回执。
#[derive(Debug)]
pub struct QualifiedDailyManifestFileSetV3 {
    manifest_path: PathBuf,
    header: ValidatedDailyManifestHeaderV3,
    files: Vec<QualifiedManifestParquetPath>,
    manifest_bytes: Vec<u8>,
}

impl QualifiedDailyManifestFileSetV3 {
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn header(&self) -> &ValidatedDailyManifestHeaderV3 {
        &self.header
    }

    pub fn files(&self) -> &[QualifiedManifestParquetPath] {
        &self.files
    }
}

/// 由清单路径布局派生的不可变文件身份元组。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedManifestFileIdentityV3 {
    pub source_kind: String,
    pub schema_version: String,
    pub utc_day: String,
    pub utc_hour: String,
    pub part: u32,
}

/// 已通过首版完整准入的固定直接源每日输入；不代表门 A 回执。
#[derive(Debug)]
pub struct ValidatedDirectSourceDailyInputV3 {
    manifest_path: PathBuf,
    manifest_sha256: String,
    utc_day: String,
    files: Vec<ValidatedDirectSourceParquetFileV3>,
}

impl ValidatedDirectSourceDailyInputV3 {
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub fn utc_day(&self) -> &str {
        &self.utc_day
    }

    pub fn files(&self) -> &[ValidatedDirectSourceParquetFileV3] {
        &self.files
    }
}

/// 已通过首版完整准入的单个 Parquet 文件事实；字段只读且由 DPT 生成。
#[derive(Debug)]
pub struct ValidatedDirectSourceParquetFileV3 {
    manifest_relative_path: PathBuf,
    canonical_path: PathBuf,
    source_kind: String,
    schema_version: String,
    utc_hour: String,
    part: u32,
    bytes: u64,
    rows: u64,
    sha256: String,
}

impl ValidatedDirectSourceParquetFileV3 {
    pub fn manifest_relative_path(&self) -> &Path {
        &self.manifest_relative_path
    }

    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn source_kind(&self) -> &str {
        &self.source_kind
    }

    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub fn utc_hour(&self) -> &str {
        &self.utc_hour
    }

    pub fn part(&self) -> u32 {
        self.part
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// 运行开始时冻结的固定直接源输入集合；不复制输入文件，也不代表任何门回执。
#[derive(Debug)]
pub struct FrozenDirectSourceInputSetV1 {
    admitted_daily_inputs: Vec<ValidatedDirectSourceDailyInputV3>,
    input_set_identity_sha256: String,
    input_manifest_set_sha256: String,
}

impl FrozenDirectSourceInputSetV1 {
    pub fn admitted_daily_inputs(&self) -> &[ValidatedDirectSourceDailyInputV3] {
        &self.admitted_daily_inputs
    }

    pub fn input_set_identity_sha256(&self) -> &str {
        &self.input_set_identity_sha256
    }

    pub fn input_manifest_set_sha256(&self) -> &str {
        &self.input_manifest_set_sha256
    }

    /// 重新完整准入冻结路径，并拒绝任一摘要发生变化的输入集合。
    pub fn verify_unchanged(&self) -> Result<(), InputSetSnapshotError> {
        self.verify_unchanged_with_admission(validate_direct_source_daily_manifest_admission)
    }

    fn verify_unchanged_with_admission<F>(&self, admission: F) -> Result<(), InputSetSnapshotError>
    where
        F: FnMut(
            &Path,
        )
            -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError>,
    {
        let manifest_paths = self
            .admitted_daily_inputs
            .iter()
            .map(|input| input.manifest_path().to_path_buf())
            .collect::<Vec<_>>();
        let current = freeze_direct_source_input_set_with_admission(&manifest_paths, admission)?;
        if current.input_set_identity_sha256 != self.input_set_identity_sha256
            || current.input_manifest_set_sha256 != self.input_manifest_set_sha256
        {
            return Err(InputSetSnapshotError::DigestMismatch {
                expected_input_set_identity_sha256: self.input_set_identity_sha256.clone(),
                current_input_set_identity_sha256: current.input_set_identity_sha256,
                expected_input_manifest_set_sha256: self.input_manifest_set_sha256.clone(),
                current_input_manifest_set_sha256: current.input_manifest_set_sha256,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum InputSetSnapshotError {
    Admission {
        manifest_path: PathBuf,
        source: ManifestAdmissionValidationError,
    },
    DigestMismatch {
        expected_input_set_identity_sha256: String,
        current_input_set_identity_sha256: String,
        expected_input_manifest_set_sha256: String,
        current_input_manifest_set_sha256: String,
    },
}

impl fmt::Display for InputSetSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission {
                manifest_path,
                source,
            } => write!(
                formatter,
                "冻结输入集合的每日清单准入失败：path={}, error={source}",
                manifest_path.display()
            ),
            Self::DigestMismatch {
                expected_input_set_identity_sha256,
                current_input_set_identity_sha256,
                expected_input_manifest_set_sha256,
                current_input_manifest_set_sha256,
            } => write!(
                formatter,
                "冻结输入集合发生变化：input_set_identity expected={expected_input_set_identity_sha256}, current={current_input_set_identity_sha256}; input_manifest_set expected={expected_input_manifest_set_sha256}, current={current_input_manifest_set_sha256}"
            ),
        }
    }
}

impl std::error::Error for InputSetSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission { source, .. } => Some(source),
            Self::DigestMismatch { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum ManifestAdmissionValidationError {
    FileSet(ManifestFileSetQualificationError),
    Json(serde_json::Error),
    Io(io::Error),
    Parquet(ParquetError),
    AdmissionTerminalMismatch {
        closure_reason: String,
        coverage_state: String,
    },
    FileIdentityMismatch {
        path: PathBuf,
        expected: Box<DerivedManifestFileIdentityV3>,
        found: Box<DerivedManifestFileIdentityV3>,
    },
    FooterRowCountMismatch {
        path: PathBuf,
        manifest_rows: u64,
        footer_rows: u64,
    },
    FileByteCountMismatch {
        path: PathBuf,
        manifest_bytes: u64,
        file_bytes: u64,
    },
    FileSha256Mismatch {
        path: PathBuf,
        manifest_sha256: String,
        file_sha256: String,
    },
    OpenedParquetPathMismatch {
        expected_path: PathBuf,
        opened_path: PathBuf,
    },
    FooterMetadataIdentityMismatch {
        path: PathBuf,
    },
    InvalidFileLayout {
        path: PathBuf,
    },
    BarrierEvidenceMismatch {
        evidence_kind: &'static str,
    },
    InvalidManifestSha256 {
        path: PathBuf,
        found: String,
    },
}

impl fmt::Display for ManifestAdmissionValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileSet(error) => write!(formatter, "每日清单文件集不合格：{error}"),
            Self::Json(error) => write!(formatter, "解析每日清单完整性字段失败：{error}"),
            Self::Io(error) => write!(formatter, "读取每日清单 Parquet 失败：{error}"),
            Self::Parquet(error) => write!(formatter, "解析每日清单 Parquet footer 失败：{error}"),
            Self::AdmissionTerminalMismatch {
                closure_reason,
                coverage_state,
            } => write!(
                formatter,
                "每日清单不满足首版成功准入终态：closure_reason={closure_reason}, coverage_state={coverage_state}"
            ),
            Self::FileIdentityMismatch {
                path,
                expected,
                found,
            } => write!(
                formatter,
                "清单文件身份元组与路径布局不一致：path={}, expected={expected:?}, found={found:?}",
                path.display()
            ),
            Self::FooterRowCountMismatch {
                path,
                manifest_rows,
                footer_rows,
            } => write!(
                formatter,
                "Parquet footer 行数与清单不一致：path={}, manifest={}, footer={}",
                path.display(),
                manifest_rows,
                footer_rows
            ),
            Self::FileByteCountMismatch {
                path,
                manifest_bytes,
                file_bytes,
            } => write!(
                formatter,
                "Parquet 字节数与清单不一致：path={}, manifest={}, file={}",
                path.display(),
                manifest_bytes,
                file_bytes
            ),
            Self::FileSha256Mismatch {
                path,
                manifest_sha256,
                file_sha256,
            } => write!(
                formatter,
                "Parquet SHA-256 与清单不一致：path={}, manifest={}, file={}",
                path.display(),
                manifest_sha256,
                file_sha256
            ),
            Self::OpenedParquetPathMismatch {
                expected_path,
                opened_path,
            } => write!(
                formatter,
                "已打开 Parquet 与已验证路径不一致：expected={}, opened={}",
                expected_path.display(),
                opened_path.display()
            ),
            Self::FooterMetadataIdentityMismatch { path } => write!(
                formatter,
                "Parquet footer 必需元数据与清单身份不一致：path={}",
                path.display()
            ),
            Self::InvalidFileLayout { path } => {
                write!(formatter, "清单文件布局无效：path={}", path.display())
            }
            Self::BarrierEvidenceMismatch { evidence_kind } => write!(
                formatter,
                "每日清单围栏证据不满足精确无重复合同：{evidence_kind}"
            ),
            Self::InvalidManifestSha256 { path, found } => write!(
                formatter,
                "每日清单 SHA-256 格式无效：path={}, sha256={found}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ManifestAdmissionValidationError {}

#[derive(Debug)]
pub enum ManifestFileSetQualificationError {
    Header(ManifestHeaderValidationError),
    Json(serde_json::Error),
    UnsafeManifestFilePath {
        path: PathBuf,
    },
    ManifestFileEscapesDay {
        relative_path: PathBuf,
        canonical_path: PathBuf,
    },
    DuplicateManifestRelativePath {
        path: PathBuf,
    },
    TemporaryManifestObject {
        path: PathBuf,
    },
    NotFinalizedParquet {
        path: PathBuf,
    },
    ManifestFileOutsideBoundedLayout {
        path: PathBuf,
    },
    UnlistedFinalizedParquet {
        path: PathBuf,
    },
    Io(io::Error),
}

impl fmt::Display for ManifestFileSetQualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header(error) => write!(formatter, "每日清单头部不合格：{error}"),
            Self::Json(error) => write!(formatter, "解析每日清单文件集失败：{error}"),
            Self::UnsafeManifestFilePath { path } => {
                write!(
                    formatter,
                    "每日清单包含不安全的文件相对路径：{}",
                    path.display()
                )
            }
            Self::ManifestFileEscapesDay {
                relative_path,
                canonical_path,
            } => write!(
                formatter,
                "每日清单文件逃逸 UTC 日目录：relative={}, canonical={}",
                relative_path.display(),
                canonical_path.display()
            ),
            Self::DuplicateManifestRelativePath { path } => {
                write!(formatter, "每日清单重复列出相对路径：{}", path.display())
            }
            Self::TemporaryManifestObject { path } => {
                write!(formatter, "UTC 日目录包含临时对象：{}", path.display())
            }
            Self::NotFinalizedParquet { path } => {
                write!(
                    formatter,
                    "每日清单成员不是最终化 Parquet：{}",
                    path.display()
                )
            }
            Self::ManifestFileOutsideBoundedLayout { path } => write!(
                formatter,
                "每日清单文件位于有界日目录布局之外：{}",
                path.display()
            ),
            Self::UnlistedFinalizedParquet { path } => {
                write!(
                    formatter,
                    "UTC 日目录包含清单外最终 Parquet：{}",
                    path.display()
                )
            }
            Self::Io(error) => write!(formatter, "检查每日清单文件集失败：{error}"),
        }
    }
}

impl std::error::Error for ManifestFileSetQualificationError {}

#[derive(Debug)]
pub enum ManifestHeaderValidationError {
    Path(ManifestPathQualificationError),
    Io(io::Error),
    ManifestBytesLimitExceeded {
        limit_bytes: usize,
        observed_bytes: usize,
    },
    Json(serde_json::Error),
    UnsupportedManifestSchema {
        found: String,
    },
    ManifestNotClosed {
        found: String,
    },
    InvalidClosureReason,
    UtcDayMismatch {
        path_utc_day: String,
        manifest_utc_day: String,
    },
    OpenedManifestPathMismatch {
        expected_path: PathBuf,
        opened_path: PathBuf,
    },
    InvalidCollectionDayCoverage,
}

impl fmt::Display for ManifestHeaderValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(error) => write!(formatter, "清单路径不合格：{error}"),
            Self::Io(error) => write!(formatter, "读取每日清单失败：{error}"),
            Self::ManifestBytesLimitExceeded {
                limit_bytes,
                observed_bytes,
            } => write!(
                formatter,
                "每日清单原始字节超过准入上限：limit={}, observed={}",
                limit_bytes, observed_bytes
            ),
            Self::Json(error) => write!(formatter, "解析每日清单头部失败：{error}"),
            Self::UnsupportedManifestSchema { found } => {
                write!(formatter, "不支持的每日清单 schema：{found}")
            }
            Self::ManifestNotClosed { found } => {
                write!(formatter, "每日清单未关闭：state={found}")
            }
            Self::InvalidClosureReason => write!(formatter, "每日清单关闭原因不能为空"),
            Self::UtcDayMismatch {
                path_utc_day,
                manifest_utc_day,
            } => write!(
                formatter,
                "每日清单 UTC 日与安全路径不一致：path={path_utc_day}, manifest={manifest_utc_day}"
            ),
            Self::OpenedManifestPathMismatch {
                expected_path,
                opened_path,
            } => write!(
                formatter,
                "每日清单打开路径与已验证路径不一致：expected={}, opened={}",
                expected_path.display(),
                opened_path.display()
            ),
            Self::InvalidCollectionDayCoverage => write!(formatter, "每日清单覆盖对象无效"),
        }
    }
}

impl std::error::Error for ManifestHeaderValidationError {}

#[derive(Deserialize)]
struct DailyManifestHeaderDto {
    schema_version: String,
    state: String,
    utc_day: String,
    closure_reason: String,
    collection_day_coverage: CollectionDayCoverageDto,
}

#[derive(Deserialize)]
struct DailyManifestFilesDto {
    files: Vec<DailyManifestFilePathDto>,
}

#[derive(Deserialize)]
struct DailyManifestFilePathDto {
    path: String,
}

#[derive(Deserialize)]
struct DailyManifestAdmissionDto {
    files: Vec<DailyManifestAdmissionFileDto>,
    producer_barriers: Vec<String>,
    writer_finalize_acks: Vec<String>,
}

#[derive(Deserialize)]
struct DailyManifestAdmissionFileDto {
    source_kind: String,
    schema_version: String,
    utc_hour: String,
    part: u32,
    rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
struct DailyFileFactFooterDto {
    source_kind: String,
    schema_version: String,
    utc_hour: String,
    part: u32,
    rows: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionDayCoverageDto {
    schema_version: String,
    state: String,
    first_started_utc_ms: RequiredNullableUtcMs,
    last_started_utc_ms: RequiredNullableUtcMs,
    ended_utc_ms: RequiredNullableUtcMs,
    reactivated_within_day: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(transparent)]
struct RequiredNullableUtcMs(Option<i64>);

fn validate_collection_day_coverage(
    utc_day: &str,
    coverage: &CollectionDayCoverageDto,
) -> Result<(), ManifestHeaderValidationError> {
    if coverage.schema_version != COLLECTION_DAY_COVERAGE_SCHEMA_VERSION_V1 {
        return Err(ManifestHeaderValidationError::InvalidCollectionDayCoverage);
    }

    let date = Date::parse(utc_day, format_description!("[year]-[month]-[day]"))
        .map_err(|_| ManifestHeaderValidationError::InvalidCollectionDayCoverage)?;
    let day_start_utc_ms = date.midnight().assume_utc().unix_timestamp() * 1_000;
    let day_end_utc_ms = date
        .next_day()
        .ok_or(ManifestHeaderValidationError::InvalidCollectionDayCoverage)?
        .midnight()
        .assume_utc()
        .unix_timestamp()
        * 1_000;

    let valid = match coverage.state.as_str() {
        "active" => false,
        "not_started" => {
            coverage.first_started_utc_ms.0.is_none()
                && coverage.last_started_utc_ms.0.is_none()
                && coverage.ended_utc_ms.0.is_none()
                && !coverage.reactivated_within_day
        }
        "unclean_end_unknown" => match (
            coverage.first_started_utc_ms.0,
            coverage.last_started_utc_ms.0,
            coverage.ended_utc_ms.0,
        ) {
            (Some(first), Some(last), None) => {
                day_start_utc_ms <= first && first <= last && last < day_end_utc_ms
            }
            _ => false,
        },
        "ended_known" => match (
            coverage.first_started_utc_ms.0,
            coverage.last_started_utc_ms.0,
            coverage.ended_utc_ms.0,
        ) {
            (Some(first), Some(last), Some(ended)) => {
                day_start_utc_ms <= first
                    && first < day_end_utc_ms
                    && first <= last
                    && last < day_end_utc_ms
                    && last <= ended
                    && ended <= day_end_utc_ms
            }
            _ => false,
        },
        _ => false,
    };

    if !valid {
        return Err(ManifestHeaderValidationError::InvalidCollectionDayCoverage);
    }
    Ok(())
}

/// 从原始清单路径读取并校验每日清单窄头部；不解析文件清单或后续围栏字段。
pub fn validate_direct_source_daily_manifest_header(
    manifest_path: &Path,
) -> Result<ValidatedDailyManifestHeaderV3, ManifestHeaderValidationError> {
    let qualified = qualify_direct_source_manifest_path(manifest_path)
        .map_err(ManifestHeaderValidationError::Path)?;
    let file = File::open(qualified.manifest_path()).map_err(ManifestHeaderValidationError::Io)?;
    let bytes = read_qualified_manifest_file(Path::new(DATASET_LINEAGE_ROOT), &qualified, file)?;
    validate_daily_manifest_header_bytes(qualified.utc_day(), &bytes)
}

/// 从固定直接源根读取一次每日清单，校验其头部和文件集路径资格。
pub fn qualify_direct_source_daily_manifest_files(
    manifest_path: &Path,
) -> Result<QualifiedDailyManifestFileSetV3, ManifestFileSetQualificationError> {
    qualify_direct_source_daily_manifest_files_at_root(
        Path::new(DATASET_LINEAGE_ROOT),
        manifest_path,
    )
}

fn qualify_direct_source_daily_manifest_files_at_root(
    root: &Path,
    manifest_path: &Path,
) -> Result<QualifiedDailyManifestFileSetV3, ManifestFileSetQualificationError> {
    let qualified = qualify_manifest_path(root, manifest_path).map_err(|error| {
        ManifestFileSetQualificationError::Header(ManifestHeaderValidationError::Path(error))
    })?;
    let file =
        File::open(qualified.manifest_path()).map_err(ManifestFileSetQualificationError::Io)?;
    let bytes = read_qualified_manifest_file(root, &qualified, file)
        .map_err(ManifestFileSetQualificationError::Header)?;
    let header = validate_daily_manifest_header_bytes(qualified.utc_day(), &bytes)
        .map_err(ManifestFileSetQualificationError::Header)?;
    let manifest: DailyManifestFilesDto =
        serde_json::from_slice(&bytes).map_err(ManifestFileSetQualificationError::Json)?;
    let day_dir = qualified.manifest_path().parent().ok_or_else(|| {
        ManifestFileSetQualificationError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "每日清单路径没有父目录",
        ))
    })?;
    let mut seen_relative_paths = BTreeSet::new();
    let mut files = Vec::with_capacity(manifest.files.len());
    for file in manifest.files {
        let unsafe_text_component = file
            .path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..");
        let manifest_relative_path = PathBuf::from(file.path);
        if unsafe_text_component
            || !manifest_relative_path
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(ManifestFileSetQualificationError::UnsafeManifestFilePath {
                path: manifest_relative_path,
            });
        }
        if is_known_temporary_path(&manifest_relative_path) {
            return Err(ManifestFileSetQualificationError::TemporaryManifestObject {
                path: manifest_relative_path,
            });
        }
        if manifest_relative_path
            .extension()
            .and_then(|value| value.to_str())
            != Some("parquet")
        {
            return Err(ManifestFileSetQualificationError::NotFinalizedParquet {
                path: manifest_relative_path,
            });
        }
        if manifest_relative_path.components().count() != 2 {
            return Err(
                ManifestFileSetQualificationError::ManifestFileOutsideBoundedLayout {
                    path: manifest_relative_path,
                },
            );
        }
        if !seen_relative_paths.insert(manifest_relative_path.clone()) {
            return Err(
                ManifestFileSetQualificationError::DuplicateManifestRelativePath {
                    path: manifest_relative_path,
                },
            );
        }
        let candidate_path = day_dir.join(&manifest_relative_path);
        let canonical_path =
            fs::canonicalize(&candidate_path).map_err(ManifestFileSetQualificationError::Io)?;
        let canonical_relative_path = match canonical_path.strip_prefix(day_dir) {
            Ok(path) => path,
            Err(_) => {
                return Err(ManifestFileSetQualificationError::ManifestFileEscapesDay {
                    relative_path: manifest_relative_path,
                    canonical_path,
                });
            }
        };
        if canonical_relative_path.components().count() != 2 {
            return Err(
                ManifestFileSetQualificationError::ManifestFileOutsideBoundedLayout {
                    path: manifest_relative_path,
                },
            );
        }
        if !fs::metadata(&canonical_path)
            .map_err(ManifestFileSetQualificationError::Io)?
            .is_file()
        {
            return Err(ManifestFileSetQualificationError::NotFinalizedParquet {
                path: manifest_relative_path,
            });
        }
        files.push(QualifiedManifestParquetPath {
            manifest_relative_path,
            canonical_path,
        });
    }
    validate_bounded_day_directory(day_dir, &seen_relative_paths)?;

    Ok(QualifiedDailyManifestFileSetV3 {
        manifest_path: qualified.manifest_path().to_path_buf(),
        header,
        files,
        manifest_bytes: bytes,
    })
}

/// 校验固定直接源每日清单及其显式列出的最终化 Parquet，返回 DPT 自有不可变事实。
pub fn validate_direct_source_daily_manifest_admission(
    manifest_path: &Path,
) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError> {
    validate_daily_manifest_admission_at_root(Path::new(DATASET_LINEAGE_ROOT), manifest_path)
}

/// 完整准入显式选择的每日清单，并冻结 DPT 自有的不可变输入集合摘要。
pub fn freeze_direct_source_input_set(
    manifest_paths: &[PathBuf],
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError> {
    freeze_direct_source_input_set_with_admission(
        manifest_paths,
        validate_direct_source_daily_manifest_admission,
    )
}

fn freeze_direct_source_input_set_with_admission<F>(
    manifest_paths: &[PathBuf],
    mut admission: F,
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError>
where
    F: FnMut(&Path) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError>,
{
    let mut admitted_daily_inputs = Vec::with_capacity(manifest_paths.len());
    for manifest_path in manifest_paths {
        admitted_daily_inputs.push(admission(manifest_path).map_err(|source| {
            InputSetSnapshotError::Admission {
                manifest_path: manifest_path.clone(),
                source,
            }
        })?);
    }
    admitted_daily_inputs.sort_by(|left, right| {
        left.utc_day()
            .cmp(right.utc_day())
            .then_with(|| left.manifest_sha256().cmp(right.manifest_sha256()))
            .then_with(|| left.manifest_path().cmp(right.manifest_path()))
    });

    Ok(FrozenDirectSourceInputSetV1 {
        input_set_identity_sha256: input_set_identity_sha256(&admitted_daily_inputs),
        input_manifest_set_sha256: input_manifest_set_sha256(&admitted_daily_inputs),
        admitted_daily_inputs,
    })
}

fn input_set_identity_sha256(inputs: &[ValidatedDirectSourceDailyInputV3]) -> String {
    let mut facts = inputs
        .iter()
        .flat_map(|input| {
            input.files().iter().map(move |file| {
                (
                    input.utc_day(),
                    file.source_kind(),
                    file.schema_version(),
                    file.utc_hour(),
                    file.part(),
                    file.bytes(),
                    file.rows(),
                    file.sha256(),
                )
            })
        })
        .collect::<Vec<_>>();
    facts.sort_unstable();

    let mut hasher = Sha256::new();
    hasher.update(INPUT_SET_IDENTITY_DIGEST_DOMAIN_V1);
    hasher.update((facts.len() as u64).to_be_bytes());
    for (utc_day, source_kind, schema_version, utc_hour, part, bytes, rows, sha256) in facts {
        update_digest_string(&mut hasher, utc_day);
        update_digest_string(&mut hasher, source_kind);
        update_digest_string(&mut hasher, schema_version);
        update_digest_string(&mut hasher, utc_hour);
        hasher.update(part.to_be_bytes());
        hasher.update(bytes.to_be_bytes());
        hasher.update(rows.to_be_bytes());
        update_digest_string(&mut hasher, sha256);
    }
    bytes_to_hex(&hasher.finalize())
}

fn input_manifest_set_sha256(inputs: &[ValidatedDirectSourceDailyInputV3]) -> String {
    let mut facts = inputs
        .iter()
        .map(|input| (input.utc_day(), input.manifest_sha256()))
        .collect::<Vec<_>>();
    facts.sort_unstable();

    let mut hasher = Sha256::new();
    hasher.update(INPUT_MANIFEST_SET_DIGEST_DOMAIN_V1);
    hasher.update((facts.len() as u64).to_be_bytes());
    for (utc_day, manifest_sha256) in facts {
        update_digest_string(&mut hasher, utc_day);
        update_digest_string(&mut hasher, manifest_sha256);
    }
    bytes_to_hex(&hasher.finalize())
}

fn update_digest_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn validate_daily_manifest_admission_at_root(
    root: &Path,
    manifest_path: &Path,
) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError> {
    let file_set = qualify_direct_source_daily_manifest_files_at_root(root, manifest_path)
        .map_err(ManifestAdmissionValidationError::FileSet)?;
    if file_set.header().closure_reason() != "utc_day_barrier"
        || file_set.header().coverage_state() != "ended_known"
    {
        return Err(
            ManifestAdmissionValidationError::AdmissionTerminalMismatch {
                closure_reason: file_set.header().closure_reason().to_owned(),
                coverage_state: file_set.header().coverage_state().to_owned(),
            },
        );
    }
    let manifest: DailyManifestAdmissionDto = serde_json::from_slice(&file_set.manifest_bytes)
        .map_err(ManifestAdmissionValidationError::Json)?;
    if !matches_exact_evidence_set(&manifest.producer_barriers, &REQUIRED_PRODUCER_BARRIERS) {
        return Err(ManifestAdmissionValidationError::BarrierEvidenceMismatch {
            evidence_kind: "producer_barriers",
        });
    }
    if !matches_exact_evidence_set(
        &manifest.writer_finalize_acks,
        &REQUIRED_WRITER_FINALIZE_ACKS,
    ) {
        return Err(ManifestAdmissionValidationError::BarrierEvidenceMismatch {
            evidence_kind: "writer_finalize_acks",
        });
    }

    let mut files = Vec::with_capacity(file_set.files().len());
    for (qualified_file, manifest_file) in file_set.files().iter().zip(&manifest.files) {
        let expected = derived_manifest_file_identity(
            qualified_file.manifest_relative_path(),
            file_set.header().utc_day(),
        )
        .ok_or_else(|| ManifestAdmissionValidationError::InvalidFileLayout {
            path: qualified_file.manifest_relative_path().to_path_buf(),
        })?;
        let found = manifest_file_identity(manifest_file, file_set.header().utc_day());
        if expected != found {
            return Err(ManifestAdmissionValidationError::FileIdentityMismatch {
                path: qualified_file.manifest_relative_path().to_path_buf(),
                expected: Box::new(expected),
                found: Box::new(found),
            });
        }
        if !is_lowercase_sha256(&manifest_file.sha256) {
            return Err(ManifestAdmissionValidationError::InvalidManifestSha256 {
                path: qualified_file.manifest_relative_path().to_path_buf(),
                found: manifest_file.sha256.clone(),
            });
        }
        let inspection = inspect_verified_parquet(qualified_file.canonical_path())?;
        if manifest_file.rows != inspection.footer_rows {
            return Err(ManifestAdmissionValidationError::FooterRowCountMismatch {
                path: qualified_file.manifest_relative_path().to_path_buf(),
                manifest_rows: manifest_file.rows,
                footer_rows: inspection.footer_rows,
            });
        }
        if manifest_file.bytes != inspection.file_bytes {
            return Err(ManifestAdmissionValidationError::FileByteCountMismatch {
                path: qualified_file.manifest_relative_path().to_path_buf(),
                manifest_bytes: manifest_file.bytes,
                file_bytes: inspection.file_bytes,
            });
        }
        if manifest_file.sha256 != inspection.sha256 {
            return Err(ManifestAdmissionValidationError::FileSha256Mismatch {
                path: qualified_file.manifest_relative_path().to_path_buf(),
                manifest_sha256: manifest_file.sha256.clone(),
                file_sha256: inspection.sha256,
            });
        }
        validate_parquet_footer_metadata(
            qualified_file.manifest_relative_path(),
            &inspection.footer_metadata,
            manifest_file,
        )?;
        files.push(ValidatedDirectSourceParquetFileV3 {
            manifest_relative_path: qualified_file.manifest_relative_path().to_path_buf(),
            canonical_path: qualified_file.canonical_path().to_path_buf(),
            source_kind: manifest_file.source_kind.clone(),
            schema_version: manifest_file.schema_version.clone(),
            utc_hour: manifest_file.utc_hour.clone(),
            part: manifest_file.part,
            bytes: manifest_file.bytes,
            rows: manifest_file.rows,
            sha256: manifest_file.sha256.clone(),
        });
    }
    Ok(ValidatedDirectSourceDailyInputV3 {
        manifest_path: file_set.manifest_path().to_path_buf(),
        manifest_sha256: bytes_to_hex(&Sha256::digest(&file_set.manifest_bytes)),
        utc_day: file_set.header().utc_day().to_owned(),
        files,
    })
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn matches_exact_evidence_set(values: &[String], expected: &[&str]) -> bool {
    values.len() == expected.len()
        && values.iter().map(String::as_str).collect::<BTreeSet<_>>()
            == expected.iter().copied().collect()
}

struct ParquetInspection {
    file_bytes: u64,
    sha256: String,
    footer_rows: u64,
    footer_metadata: Vec<parquet::file::metadata::KeyValue>,
}

fn inspect_verified_parquet(
    path: &Path,
) -> Result<ParquetInspection, ManifestAdmissionValidationError> {
    let mut file = File::open(path).map_err(ManifestAdmissionValidationError::Io)?;
    let opened_path =
        fs::canonicalize(Path::new("/proc/self/fd").join(file.as_raw_fd().to_string()))
            .map_err(ManifestAdmissionValidationError::Io)?;
    if opened_path != path {
        return Err(
            ManifestAdmissionValidationError::OpenedParquetPathMismatch {
                expected_path: path.to_path_buf(),
                opened_path,
            },
        );
    }
    let inspection = (|| {
        let file_bytes = file
            .metadata()
            .map_err(ManifestAdmissionValidationError::Io)?
            .len();
        let sha256 = sha256_open_file(&mut file)?;
        file.seek(SeekFrom::Start(0))
            .map_err(ManifestAdmissionValidationError::Io)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            file.try_clone()
                .map_err(ManifestAdmissionValidationError::Io)?,
        )
        .map_err(ManifestAdmissionValidationError::Parquet)?;
        let footer_rows =
            u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                ManifestAdmissionValidationError::Parquet(ParquetError::General(
                    "Parquet footer 行数不能转换为 u64".to_owned(),
                ))
            })?;
        let footer_metadata = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned()
            .unwrap_or_default();
        Ok(ParquetInspection {
            file_bytes,
            sha256,
            footer_rows,
            footer_metadata,
        })
    })();
    let cache_release = fadvise(&file, 0, None, Advice::DontNeed).map_err(|error| {
        ManifestAdmissionValidationError::Io(io::Error::from_raw_os_error(error.raw_os_error()))
    });
    match (inspection, cache_release) {
        (Err(primary_error), _) => Err(primary_error),
        (Ok(_), Err(cache_error)) => Err(cache_error),
        (Ok(inspection), Ok(())) => Ok(inspection),
    }
}

fn validate_parquet_footer_metadata(
    path: &Path,
    metadata: &[parquet::file::metadata::KeyValue],
    manifest_file: &DailyManifestAdmissionFileDto,
) -> Result<(), ManifestAdmissionValidationError> {
    let mismatch = || ManifestAdmissionValidationError::FooterMetadataIdentityMismatch {
        path: path.to_path_buf(),
    };
    if required_footer_metadata_value(metadata, "parquet_raw_source_kind")
        != Some(manifest_file.source_kind.as_str())
        || required_footer_metadata_value(metadata, "parquet_raw_layout_version")
            != Some(manifest_file.schema_version.as_str())
        || required_footer_metadata_value(metadata, "data_collect_artifact_kind")
            != Some("typed_raw_parquet")
    {
        return Err(mismatch());
    }
    let daily_file_fact =
        required_footer_metadata_value(metadata, "data_collect_daily_file_fact_v1")
            .ok_or_else(mismatch)?;
    let footer_fact: DailyFileFactFooterDto =
        serde_json::from_str(daily_file_fact).map_err(|_| mismatch())?;
    if footer_fact.source_kind != manifest_file.source_kind
        || footer_fact.schema_version != manifest_file.schema_version
        || footer_fact.utc_hour != manifest_file.utc_hour
        || footer_fact.part != manifest_file.part
        || footer_fact.rows != manifest_file.rows
    {
        return Err(mismatch());
    }
    Ok(())
}

fn required_footer_metadata_value<'a>(
    metadata: &'a [parquet::file::metadata::KeyValue],
    key: &str,
) -> Option<&'a str> {
    let mut matches = metadata.iter().filter(|entry| entry.key == key);
    let value = matches.next()?.value.as_deref()?;
    if matches.next().is_some() || value.is_empty() {
        return None;
    }
    Some(value)
}

fn sha256_open_file(file: &mut File) -> Result<String, ManifestAdmissionValidationError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(ManifestAdmissionValidationError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(bytes_to_hex(&hasher.finalize()))
}

fn manifest_file_identity(
    file: &DailyManifestAdmissionFileDto,
    utc_day: &str,
) -> DerivedManifestFileIdentityV3 {
    DerivedManifestFileIdentityV3 {
        source_kind: file.source_kind.clone(),
        schema_version: file.schema_version.clone(),
        utc_day: utc_day.to_owned(),
        utc_hour: file.utc_hour.clone(),
        part: file.part,
    }
}

fn derived_manifest_file_identity(
    relative_path: &Path,
    utc_day: &str,
) -> Option<DerivedManifestFileIdentityV3> {
    let source_kind = relative_path
        .parent()?
        .file_name()?
        .to_str()?
        .strip_prefix("source_kind=")?
        .to_owned();
    let schema_version = supported_schema_for_source_kind(&source_kind)?.to_owned();
    let file_name = relative_path.file_name()?.to_str()?;
    let name_without_suffix = file_name.strip_suffix(".parquet")?;
    let hour_and_part = name_without_suffix.strip_prefix(&format!("{source_kind}_"))?;
    let (hour, part) = match hour_and_part.split_once("-part-") {
        Some((hour, part)) => {
            if part.is_empty()
                || part.starts_with('0')
                || !part.bytes().all(|byte| byte.is_ascii_digit())
            {
                return None;
            }
            (hour, part.parse().ok()?)
        }
        None => (hour_and_part, 0),
    };
    if hour.len() != 2 || hour.parse::<u8>().ok()? > 23 {
        return None;
    }
    Some(DerivedManifestFileIdentityV3 {
        source_kind,
        schema_version,
        utc_day: utc_day.to_owned(),
        utc_hour: format!("{utc_day}T{hour}"),
        part,
    })
}

fn supported_schema_for_source_kind(source_kind: &str) -> Option<&'static str> {
    match source_kind {
        "binance_aggtrade" => Some("binance_aggtrade_v7"),
        "binance_depth_snapshot" => Some("binance_depth_snapshot_v3"),
        "binance_depth_update" => Some("binance_depth_update_v4"),
        "polymarket_market" => Some("polymarket_orderbook_v3"),
        "polymarket_market_metadata" => Some("polymarket_market_metadata_v5"),
        "polymarket_rtds_chainlink_btc_usd" => Some("polymarket_rtds_chainlink_btc_usd_v4"),
        _ => None,
    }
}

fn is_known_temporary_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(value) = component else {
            return false;
        };
        let value = value.to_string_lossy();
        value == ".rsync-partial"
            || value.contains(".rsync-partial")
            || value.ends_with(".tmp")
            || value.ends_with(".running")
            || value.ends_with("-running.parquet")
            || value.ends_with(".running.parquet")
            || value.ends_with("-digesting.parquet")
    })
}

fn validate_bounded_day_directory(
    day_dir: &Path,
    listed_relative_paths: &BTreeSet<PathBuf>,
) -> Result<(), ManifestFileSetQualificationError> {
    for entry in fs::read_dir(day_dir).map_err(ManifestFileSetQualificationError::Io)? {
        let entry = entry.map_err(ManifestFileSetQualificationError::Io)?;
        let relative_path = PathBuf::from(entry.file_name());
        reject_temporary_or_unlisted_finalized_object(
            &entry,
            &relative_path,
            listed_relative_paths,
            true,
        )?;

        if entry
            .file_type()
            .map_err(ManifestFileSetQualificationError::Io)?
            .is_dir()
        {
            for child in
                fs::read_dir(entry.path()).map_err(ManifestFileSetQualificationError::Io)?
            {
                let child = child.map_err(ManifestFileSetQualificationError::Io)?;
                let relative_child_path = relative_path.join(child.file_name());
                reject_temporary_or_unlisted_finalized_object(
                    &child,
                    &relative_child_path,
                    listed_relative_paths,
                    false,
                )?;
            }
        }
    }
    Ok(())
}

fn reject_temporary_or_unlisted_finalized_object(
    entry: &fs::DirEntry,
    relative_path: &Path,
    listed_relative_paths: &BTreeSet<PathBuf>,
    allow_real_directory: bool,
) -> Result<(), ManifestFileSetQualificationError> {
    if is_known_temporary_path(relative_path) {
        return Err(ManifestFileSetQualificationError::TemporaryManifestObject {
            path: relative_path.to_path_buf(),
        });
    }

    let file_type = entry
        .file_type()
        .map_err(ManifestFileSetQualificationError::Io)?;
    if !(file_type.is_file() || allow_real_directory && file_type.is_dir()) {
        return Err(
            ManifestFileSetQualificationError::ManifestFileOutsideBoundedLayout {
                path: relative_path.to_path_buf(),
            },
        );
    }
    if file_type.is_file()
        && relative_path.extension().and_then(|value| value.to_str()) == Some("parquet")
        && !listed_relative_paths.contains(relative_path)
    {
        return Err(
            ManifestFileSetQualificationError::UnlistedFinalizedParquet {
                path: relative_path.to_path_buf(),
            },
        );
    }
    Ok(())
}

fn read_qualified_manifest_file(
    root: &Path,
    qualified: &QualifiedSourceManifestPath,
    file: File,
) -> Result<Vec<u8>, ManifestHeaderValidationError> {
    let opened_file_link = Path::new("/proc/self/fd").join(file.as_raw_fd().to_string());
    let opened_path =
        fs::canonicalize(opened_file_link).map_err(ManifestHeaderValidationError::Io)?;
    let opened_qualified =
        qualify_manifest_path(root, &opened_path).map_err(ManifestHeaderValidationError::Path)?;
    if opened_qualified.manifest_path() != qualified.manifest_path()
        || opened_qualified.utc_day() != qualified.utc_day()
    {
        return Err(ManifestHeaderValidationError::OpenedManifestPathMismatch {
            expected_path: qualified.manifest_path().to_path_buf(),
            opened_path,
        });
    }

    let mut bytes = Vec::new();
    file.take((DAILY_MANIFEST_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(ManifestHeaderValidationError::Io)?;
    if bytes.len() > DAILY_MANIFEST_MAX_BYTES {
        return Err(ManifestHeaderValidationError::ManifestBytesLimitExceeded {
            limit_bytes: DAILY_MANIFEST_MAX_BYTES,
            observed_bytes: bytes.len(),
        });
    }
    Ok(bytes)
}

fn validate_daily_manifest_header_bytes(
    qualified_utc_day: &str,
    bytes: &[u8],
) -> Result<ValidatedDailyManifestHeaderV3, ManifestHeaderValidationError> {
    let header: DailyManifestHeaderDto =
        serde_json::from_slice(bytes).map_err(ManifestHeaderValidationError::Json)?;
    if header.schema_version != DAILY_MANIFEST_SCHEMA_VERSION_V3 {
        return Err(ManifestHeaderValidationError::UnsupportedManifestSchema {
            found: header.schema_version,
        });
    }
    if header.state != "closed" {
        return Err(ManifestHeaderValidationError::ManifestNotClosed {
            found: header.state,
        });
    }
    if header.closure_reason.trim().is_empty() {
        return Err(ManifestHeaderValidationError::InvalidClosureReason);
    }
    if header.utc_day != qualified_utc_day {
        return Err(ManifestHeaderValidationError::UtcDayMismatch {
            path_utc_day: qualified_utc_day.to_owned(),
            manifest_utc_day: header.utc_day,
        });
    }
    validate_collection_day_coverage(&header.utc_day, &header.collection_day_coverage)?;

    Ok(ValidatedDailyManifestHeaderV3 {
        schema_version: header.schema_version,
        state: header.state,
        utc_day: header.utc_day,
        closure_reason: header.closure_reason,
        coverage_schema_version: header.collection_day_coverage.schema_version,
        coverage_state: header.collection_day_coverage.state,
        first_started_utc_ms: header.collection_day_coverage.first_started_utc_ms.0,
        last_started_utc_ms: header.collection_day_coverage.last_started_utc_ms.0,
        ended_utc_ms: header.collection_day_coverage.ended_utc_ms.0,
        reactivated_within_day: header.collection_day_coverage.reactivated_within_day,
    })
}

fn lexical_relative_path(
    root: &Path,
    candidate: &Path,
) -> Result<PathBuf, ManifestPathQualificationError> {
    let relative = candidate.strip_prefix(root).map_err(|_| {
        ManifestPathQualificationError::OutsideDatasetLineageRoot {
            path: candidate.to_path_buf(),
        }
    })?;
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ManifestPathQualificationError::OutsideDatasetLineageRoot {
                    path: candidate.to_path_buf(),
                });
            }
        }
    }
    Ok(normalized)
}

fn eligible_dataset_date(
    relative_path: &Path,
    candidate: &Path,
) -> Result<String, ManifestPathQualificationError> {
    let date_component = relative_path.components().next().ok_or_else(|| {
        ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        }
    })?;
    let date_component = match date_component {
        Component::Normal(value) => value,
        _ => {
            return Err(ManifestPathQualificationError::InvalidDatasetDate {
                path: candidate.to_path_buf(),
            });
        }
    };
    let utc_day = date_component.to_str().ok_or_else(|| {
        ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        }
    })?;
    let eligibility = classify_dataset_date(utc_day).map_err(|_| {
        ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        }
    })?;
    if eligibility == DatasetDateEligibility::IneligibleBeforeDatasetFloor {
        return Err(
            ManifestPathQualificationError::IneligibleBeforeDatasetFloor {
                utc_day: utc_day.to_owned(),
            },
        );
    }
    Ok(utc_day.to_owned())
}

fn qualify_manifest_path(
    root: &Path,
    candidate: &Path,
) -> Result<QualifiedSourceManifestPath, ManifestPathQualificationError> {
    let lexical_relative_path = lexical_relative_path(root, candidate)?;
    let lexical_utc_day = eligible_dataset_date(&lexical_relative_path, candidate)?;
    validate_inner_utc_day_partition(&lexical_relative_path, &lexical_utc_day, candidate)?;

    let canonical_root = fs::canonicalize(root).map_err(ManifestPathQualificationError::Io)?;
    let canonical_candidate =
        fs::canonicalize(candidate).map_err(ManifestPathQualificationError::Io)?;
    if !canonical_candidate.starts_with(&canonical_root) {
        return Err(ManifestPathQualificationError::OutsideDatasetLineageRoot {
            path: candidate.to_path_buf(),
        });
    }
    let canonical_relative_path =
        canonical_candidate
            .strip_prefix(&canonical_root)
            .map_err(
                |_| ManifestPathQualificationError::OutsideDatasetLineageRoot {
                    path: candidate.to_path_buf(),
                },
            )?;
    let utc_day = eligible_dataset_date(canonical_relative_path, candidate)?;
    validate_inner_utc_day_partition(canonical_relative_path, &utc_day, candidate)?;

    Ok(QualifiedSourceManifestPath {
        manifest_path: canonical_candidate,
        utc_day,
    })
}

fn validate_inner_utc_day_partition(
    relative_path: &Path,
    outer_utc_day: &str,
    candidate: &Path,
) -> Result<(), ManifestPathQualificationError> {
    let mut components = relative_path.components();
    let _outer_day = components.next();
    let Some(Component::Normal(typed_raw)) = components.next() else {
        return Ok(());
    };
    if typed_raw != "typed_raw" {
        return Ok(());
    }
    let Some(Component::Normal(partition)) = components.next() else {
        return Err(ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        });
    };
    let Some(inner_utc_day) = partition
        .to_str()
        .and_then(|partition| partition.strip_prefix("utc_day="))
    else {
        return Err(ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        });
    };
    if inner_utc_day != outer_utc_day {
        return Err(ManifestPathQualificationError::InvalidDatasetDate {
            path: candidate.to_path_buf(),
        });
    }
    Ok(())
}

/// 能力试验使用的固定行结构。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortRow {
    sort_key: Vec<u8>,
    payload: Vec<u8>,
}

impl SortRow {
    /// 使用调用方已经编码的不透明字节排序键创建一行。
    pub fn from_sort_key(sort_key: impl Into<Vec<u8>>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            sort_key: sort_key.into(),
            payload: payload.into(),
        }
    }

    pub fn sort_key_bytes(&self) -> &[u8] {
        &self.sort_key
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.sort_key.capacity())
            .saturating_add(self.payload.capacity())
    }
}

fn sort_row_inner_bytes(row: &SortRow) -> Option<usize> {
    row.sort_key.capacity().checked_add(row.payload.capacity())
}

fn sort_row_buffer_bytes(rows: &Vec<SortRow>) -> Option<usize> {
    rows.capacity()
        .checked_mul(size_of::<SortRow>())
        .and_then(|bytes| {
            rows.iter().try_fold(bytes, |total, row| {
                total.checked_add(sort_row_inner_bytes(row)?)
            })
        })
}

/// 单次排序调用的显式硬上限。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortConfig {
    /// 形成一个初始有序 run 时最多保留的行数。
    pub max_run_rows: usize,
    /// 初始 run 或合并写入批次允许保留的最大行分配字节数。
    pub max_run_bytes: usize,
    /// 磁盘上同时存活的临时 Parquet run 文件上限，包括正在写入的合并输出。
    pub max_temp_run_files: usize,
    /// 单次合并可参与的读取器上限。
    pub merge_fan_in: usize,
    /// 同时打开的文件句柄上限；一次合并需要 `merge_fan_in + 1` 个句柄。
    pub max_open_files: usize,
    /// 单个 Parquet 读写批次的行数上限。
    pub batch_rows: usize,
    /// 任一时刻全部 run 文件允许占用的最大字节数。
    pub max_temp_bytes: u64,
}

/// 实测资源用量与行守恒计数器。
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortMetrics {
    pub input_rows: u64,
    pub output_rows: u64,
    pub initial_runs: usize,
    pub merge_passes: usize,
    pub peak_run_rows: usize,
    pub peak_run_bytes: usize,
    pub peak_write_buffer_rows: usize,
    pub peak_write_buffer_bytes: usize,
    pub peak_temp_run_files: usize,
    pub peak_open_files: usize,
    pub peak_heap_entries: usize,
    pub peak_temp_bytes: u64,
}

/// 成功的试验输出。摘要覆盖逻辑行，而不是 Parquet 文件字节。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortOutcome {
    pub output_path: PathBuf,
    pub logical_sha256: String,
    pub metrics: SortMetrics,
}

#[derive(Debug)]
pub enum SortError {
    InvalidConfig(&'static str),
    RowMemoryLimit { observed: usize, limit: usize },
    TempRunFileLimit { needed: usize, limit: usize },
    OpenFileLimit { needed: usize, limit: usize },
    TempByteLimit { limit: u64 },
    SortKeyCollision { key_hex: String },
    InvalidRun(&'static str),
    WorkspaceExists(PathBuf),
    Io(io::Error),
    Arrow(ArrowError),
    Parquet(ParquetError),
}

impl fmt::Display for SortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "排序配置无效：{message}"),
            Self::RowMemoryLimit { observed, limit } => write!(
                formatter,
                "行分配字节数 {observed} 超过 run 内存上限 {limit}"
            ),
            Self::TempRunFileLimit { needed, limit } => write!(
                formatter,
                "外部排序需要同时保留 {needed} 个临时 run 文件，上限为 {limit}"
            ),
            Self::OpenFileLimit { needed, limit } => write!(
                formatter,
                "外部排序需要同时打开 {needed} 个文件句柄，上限为 {limit}"
            ),
            Self::TempByteLimit { limit } => {
                write!(formatter, "外部排序超过临时文件字节上限 {limit}")
            }
            Self::SortKeyCollision { key_hex } => {
                write!(formatter, "排序键冲突：{key_hex}")
            }
            Self::InvalidRun(message) => write!(formatter, "Parquet run 无效：{message}"),
            Self::WorkspaceExists(path) => {
                write!(formatter, "排序工作目录已存在：{}", path.display())
            }
            Self::Io(error) => write!(formatter, "I/O 错误：{error}"),
            Self::Arrow(error) => write!(formatter, "Arrow 错误：{error}"),
            Self::Parquet(error) => write!(formatter, "Parquet 错误：{error}"),
        }
    }
}

impl std::error::Error for SortError {}

impl From<io::Error> for SortError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ArrowError> for SortError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow(error)
    }
}

impl From<ParquetError> for SortError {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

/// 使用有界 Parquet run，按不透明字节排序键排序不透明行。
///
/// `temp_root` 必须已经存在。函数会创建专用子目录，成功时保留它，失败时尽力
/// 清理它；已经存在的同名子目录不会被复用或删除。
pub fn external_sort<I>(
    rows: I,
    temp_root: &Path,
    config: &SortConfig,
) -> Result<SortOutcome, SortError>
where
    I: IntoIterator<Item = SortRow>,
{
    let mut sorter = ExternalSorter::new(temp_root, config)?;
    for row in rows {
        sorter.push(row)?;
    }
    sorter.finish()
}

pub(crate) struct ExternalSorter {
    work_directory: PathBuf,
    config: SortConfig,
    metrics: SortMetrics,
    runs: Vec<RunInfo>,
    pending_rows: Vec<SortRow>,
    current_temp_bytes: u64,
    active_temp_run_files: usize,
    next_file_id: usize,
    retain_work_directory: bool,
}

impl ExternalSorter {
    pub(crate) fn new(temp_root: &Path, config: &SortConfig) -> Result<Self, SortError> {
        validate_config(config)?;

        let work_directory = temp_root.join(WORK_DIRECTORY);
        match fs::create_dir(&work_directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(SortError::WorkspaceExists(work_directory));
            }
            Err(error) => return Err(error.into()),
        }

        let mut sorter = Self {
            work_directory,
            config: config.clone(),
            metrics: SortMetrics::default(),
            runs: Vec::new(),
            pending_rows: Vec::with_capacity(config.max_run_rows),
            current_temp_bytes: 0,
            active_temp_run_files: 0,
            next_file_id: 0,
            retain_work_directory: false,
        };
        let pending_base_bytes =
            sort_row_buffer_bytes(&sorter.pending_rows).ok_or(SortError::RowMemoryLimit {
                observed: usize::MAX,
                limit: config.max_run_bytes,
            })?;
        if pending_base_bytes > config.max_run_bytes {
            return Err(SortError::RowMemoryLimit {
                observed: pending_base_bytes,
                limit: config.max_run_bytes,
            });
        }
        sorter.metrics.peak_run_bytes = pending_base_bytes;
        Ok(sorter)
    }

    pub(crate) fn push(&mut self, row: SortRow) -> Result<(), SortError> {
        let exceeds_rows = self.pending_rows.len() >= self.config.max_run_rows;
        let mut prospective_bytes = sort_row_buffer_bytes(&self.pending_rows)
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
            .filter(|bytes| *bytes <= self.config.max_run_bytes);
        let exceeds_bytes = prospective_bytes.is_none();
        if exceeds_rows || exceeds_bytes {
            if self.pending_rows.is_empty() {
                return Err(SortError::RowMemoryLimit {
                    observed: sort_row_buffer_bytes(&self.pending_rows)
                        .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                        .unwrap_or(usize::MAX),
                    limit: self.config.max_run_bytes,
                });
            }
            self.flush_pending()?;
            prospective_bytes = sort_row_buffer_bytes(&self.pending_rows)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= self.config.max_run_bytes);
        }

        let observed = prospective_bytes.ok_or(SortError::RowMemoryLimit {
            observed: sort_row_buffer_bytes(&self.pending_rows)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .unwrap_or(usize::MAX),
            limit: self.config.max_run_bytes,
        })?;
        self.pending_rows.push(row);
        self.metrics.input_rows += 1;
        self.metrics.peak_run_rows = self.metrics.peak_run_rows.max(self.pending_rows.len());
        self.metrics.peak_run_bytes = self.metrics.peak_run_bytes.max(observed);
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<(), SortError> {
        let run = flush_initial_run(
            std::mem::take(&mut self.pending_rows),
            RunDestination {
                work_directory: &self.work_directory,
                pass: 0,
                file_id: self.next_file_id,
                current_temp_bytes: self.current_temp_bytes,
                active_temp_run_files: self.active_temp_run_files,
            },
            &self.config,
            &mut self.metrics,
        )?;
        self.next_file_id += 1;
        self.current_temp_bytes =
            self.current_temp_bytes
                .checked_add(run.bytes)
                .ok_or(SortError::TempByteLimit {
                    limit: self.config.max_temp_bytes,
                })?;
        self.active_temp_run_files += 1;
        self.metrics.peak_temp_run_files = self
            .metrics
            .peak_temp_run_files
            .max(self.active_temp_run_files);
        self.metrics.peak_temp_bytes = self.metrics.peak_temp_bytes.max(self.current_temp_bytes);
        self.runs.push(run);

        self.pending_rows = Vec::with_capacity(self.config.max_run_rows);
        let pending_base_bytes =
            sort_row_buffer_bytes(&self.pending_rows).ok_or(SortError::RowMemoryLimit {
                observed: usize::MAX,
                limit: self.config.max_run_bytes,
            })?;
        if pending_base_bytes > self.config.max_run_bytes {
            return Err(SortError::RowMemoryLimit {
                observed: pending_base_bytes,
                limit: self.config.max_run_bytes,
            });
        }
        self.metrics.peak_run_bytes = self.metrics.peak_run_bytes.max(pending_base_bytes);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<SortOutcome, SortError> {
        if !self.pending_rows.is_empty() {
            self.flush_pending()?;
        }

        self.metrics.initial_runs = self.runs.len();

        if self.runs.is_empty() {
            let final_path = self.work_directory.join("sorted.parquet");
            let empty_run = RunWriter::new(
                final_path.clone(),
                self.current_temp_bytes,
                self.active_temp_run_files,
                &self.config,
                &mut self.metrics,
            )?
            .finish(&mut self.metrics)?;
            self.metrics.peak_temp_bytes = empty_run.bytes;
            self.metrics.peak_temp_run_files = 1;
            self.metrics.peak_open_files = 1;
            let (logical_sha256, output_rows) = scan_logical_output(
                &final_path,
                self.config.batch_rows,
                self.config.max_run_bytes,
            )?;
            self.metrics.output_rows = output_rows;
            self.retain_work_directory = true;
            return Ok(SortOutcome {
                output_path: final_path,
                logical_sha256,
                metrics: std::mem::take(&mut self.metrics),
            });
        }

        let mut runs = std::mem::take(&mut self.runs);
        let mut pass = 0usize;
        while runs.len() > 1 {
            pass += 1;
            self.metrics.merge_passes += 1;
            let mut next_runs = Vec::with_capacity(runs.len().div_ceil(self.config.merge_fan_in));
            let mut remaining = runs.into_iter();

            loop {
                let group: Vec<_> = remaining.by_ref().take(self.config.merge_fan_in).collect();
                if group.is_empty() {
                    break;
                }
                if group.len() == 1 {
                    next_runs.push(group.into_iter().next().expect("恰有一个 run"));
                    continue;
                }

                let merged = merge_group(
                    &group,
                    RunDestination {
                        work_directory: &self.work_directory,
                        pass,
                        file_id: self.next_file_id,
                        current_temp_bytes: self.current_temp_bytes,
                        active_temp_run_files: self.active_temp_run_files,
                    },
                    &self.config,
                    &mut self.metrics,
                )?;
                let expected_rows = group.iter().try_fold(0u64, |total, run| {
                    total
                        .checked_add(run.rows)
                        .ok_or(SortError::InvalidRun("run 行数记账溢出"))
                })?;
                if merged.rows != expected_rows {
                    return Err(SortError::InvalidRun("合并行守恒检查失败"));
                }
                self.next_file_id += 1;

                self.current_temp_bytes = self.current_temp_bytes.checked_add(merged.bytes).ok_or(
                    SortError::TempByteLimit {
                        limit: self.config.max_temp_bytes,
                    },
                )?;
                self.active_temp_run_files += 1;
                self.metrics.peak_temp_run_files = self
                    .metrics
                    .peak_temp_run_files
                    .max(self.active_temp_run_files);
                self.metrics.peak_temp_bytes =
                    self.metrics.peak_temp_bytes.max(self.current_temp_bytes);

                for input in &group {
                    fs::remove_file(&input.path)?;
                    self.current_temp_bytes = self
                        .current_temp_bytes
                        .checked_sub(input.bytes)
                        .ok_or(SortError::InvalidRun("临时文件字节记账下溢"))?;
                    self.active_temp_run_files = self
                        .active_temp_run_files
                        .checked_sub(1)
                        .ok_or(SortError::InvalidRun("run 文件记账下溢"))?;
                }
                next_runs.push(merged);
            }
            runs = next_runs;
        }

        let final_run = runs.pop().expect("run 列表非空");
        if final_run.rows != self.metrics.input_rows {
            return Err(SortError::InvalidRun("run 行守恒检查失败"));
        }
        let final_path = self.work_directory.join("sorted.parquet");
        fs::rename(&final_run.path, &final_path)?;
        self.metrics.peak_open_files = self.metrics.peak_open_files.max(1);
        let (logical_sha256, output_rows) = scan_logical_output(
            &final_path,
            self.config.batch_rows,
            self.config.max_run_bytes,
        )?;
        self.metrics.output_rows = output_rows;
        if self.metrics.output_rows != self.metrics.input_rows {
            return Err(SortError::InvalidRun("行守恒检查失败"));
        }

        self.retain_work_directory = true;
        Ok(SortOutcome {
            output_path: final_path,
            logical_sha256,
            metrics: std::mem::take(&mut self.metrics),
        })
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        if !self.retain_work_directory {
            let _ = fs::remove_dir_all(&self.work_directory);
        }
    }
}

fn validate_config(config: &SortConfig) -> Result<(), SortError> {
    if config.max_run_rows == 0 {
        return Err(SortError::InvalidConfig("max_run_rows 必须为正数"));
    }
    if config.max_run_rows > u32::MAX as usize {
        return Err(SortError::InvalidConfig(
            "max_run_rows 必须能用 Arrow UInt32 索引表示",
        ));
    }
    if config.max_run_bytes < size_of::<SortRow>() {
        return Err(SortError::InvalidConfig(
            "max_run_bytes 无法容纳一个行描述符",
        ));
    }
    if config
        .max_run_rows
        .checked_mul(size_of::<SortRow>())
        .is_none_or(|bytes| bytes > config.max_run_bytes)
    {
        return Err(SortError::InvalidConfig(
            "max_run_rows 的行描述符容量必须受 max_run_bytes 约束",
        ));
    }
    if config.max_run_bytes > i32::MAX as usize {
        return Err(SortError::InvalidConfig(
            "max_run_bytes 必须能用 Arrow Binary 偏移量表示",
        ));
    }
    if config.max_temp_run_files == 0 {
        return Err(SortError::InvalidConfig("max_temp_run_files 必须为正数"));
    }
    if config.merge_fan_in < 2 {
        return Err(SortError::InvalidConfig("merge_fan_in 必须至少为 2"));
    }
    let merge_open_files = config
        .merge_fan_in
        .checked_add(1)
        .ok_or(SortError::InvalidConfig("merge_fan_in + 1 溢出"))?;
    if merge_open_files > config.max_open_files {
        return Err(SortError::OpenFileLimit {
            needed: merge_open_files,
            limit: config.max_open_files,
        });
    }
    if config.batch_rows == 0 {
        return Err(SortError::InvalidConfig("batch_rows 必须为正数"));
    }
    if config.batch_rows > u32::MAX as usize {
        return Err(SortError::InvalidConfig(
            "batch_rows 必须能用 Arrow UInt32 索引表示",
        ));
    }
    if config
        .batch_rows
        .checked_mul(size_of::<SortRow>())
        .is_none_or(|bytes| bytes > config.max_run_bytes)
    {
        return Err(SortError::InvalidConfig(
            "batch_rows 的行描述符容量必须受 max_run_bytes 约束",
        ));
    }
    if config.max_temp_bytes == 0 {
        return Err(SortError::InvalidConfig("max_temp_bytes 必须为正数"));
    }
    Ok(())
}

fn flush_initial_run(
    rows: Vec<SortRow>,
    destination: RunDestination<'_>,
    config: &SortConfig,
    metrics: &mut SortMetrics,
) -> Result<RunInfo, SortError> {
    let sorted = sort_rows_with_arrow(rows)?;
    ensure_unique_keys(&sorted)?;
    let path = run_path(
        destination.work_directory,
        destination.pass,
        destination.file_id,
    );
    let mut writer = RunWriter::new_without_buffer(
        path,
        destination.current_temp_bytes,
        destination.active_temp_run_files,
        config,
        metrics,
    )?;
    for rows in sorted.chunks(config.batch_rows) {
        writer.write_unbuffered(rows)?;
    }
    writer.finish(metrics)
}

fn merge_group(
    group: &[RunInfo],
    destination: RunDestination<'_>,
    config: &SortConfig,
    metrics: &mut SortMetrics,
) -> Result<RunInfo, SortError> {
    let open_files = group
        .len()
        .checked_add(1)
        .ok_or(SortError::InvalidConfig("合并文件句柄数量溢出"))?;
    if open_files > config.max_open_files {
        return Err(SortError::OpenFileLimit {
            needed: open_files,
            limit: config.max_open_files,
        });
    }
    metrics.peak_open_files = metrics.peak_open_files.max(open_files);
    let mut cursors = Vec::with_capacity(group.len());
    for run in group {
        cursors.push(RunCursor::open(
            &run.path,
            config.batch_rows,
            config.max_run_bytes,
        )?);
    }

    let mut heap = BinaryHeap::with_capacity(group.len());
    for (run_slot, cursor) in cursors.iter_mut().enumerate() {
        if let Some(row) = cursor.next_row()? {
            heap.push(HeapEntry { row, run_slot });
        }
    }
    metrics.peak_heap_entries = metrics.peak_heap_entries.max(heap.len());

    let path = run_path(
        destination.work_directory,
        destination.pass,
        destination.file_id,
    );
    let mut writer = RunWriter::new(
        path,
        destination.current_temp_bytes,
        destination.active_temp_run_files,
        config,
        metrics,
    )?;
    let mut previous_key: Option<Vec<u8>> = None;

    while let Some(entry) = heap.pop() {
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous >= entry.row.sort_key.as_slice())
        {
            if previous_key.as_deref() == Some(entry.row.sort_key.as_slice()) {
                return Err(collision_error(&entry.row.sort_key));
            }
            return Err(SortError::InvalidRun("输入 run 未严格递增"));
        }
        previous_key = Some(entry.row.sort_key.clone());
        let run_slot = entry.run_slot;
        writer.push(entry.row, metrics)?;

        if let Some(row) = cursors[run_slot].next_row()? {
            heap.push(HeapEntry { row, run_slot });
            metrics.peak_heap_entries = metrics.peak_heap_entries.max(heap.len());
        }
    }

    writer.finish(metrics)
}

fn sort_rows_with_arrow(mut rows: Vec<SortRow>) -> Result<Vec<SortRow>, SortError> {
    let batch = batch_from_rows(&rows)?;
    let indices = lexsort_to_indices(
        &[SortColumn {
            values: batch.column(0).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        }],
        None,
    )?;
    drop(batch);

    let indices = indices.values();
    if indices.len() != rows.len() {
        return Err(SortError::InvalidRun("Arrow 排序索引长度不匹配"));
    }
    let mut visited = vec![false; rows.len()];
    for &index in indices {
        let source = index as usize;
        if source >= rows.len() || visited[source] {
            return Err(SortError::InvalidRun("Arrow 排序索引无效或重复"));
        }
        visited[source] = true;
    }
    if visited.iter().any(|seen| !seen) {
        return Err(SortError::InvalidRun("Arrow 排序索引未覆盖全部输入行"));
    }

    visited.fill(false);
    for target in 0..rows.len() {
        if visited[target] {
            continue;
        }
        let mut current = target;
        loop {
            visited[current] = true;
            let source = indices[current] as usize;
            if visited[source] {
                break;
            }
            rows.swap(current, source);
            current = source;
        }
    }
    Ok(rows)
}

fn ensure_unique_keys(rows: &[SortRow]) -> Result<(), SortError> {
    for pair in rows.windows(2) {
        if pair[0].sort_key == pair[1].sort_key {
            return Err(collision_error(&pair[0].sort_key));
        }
    }
    Ok(())
}

fn collision_error(key: &[u8]) -> SortError {
    SortError::SortKeyCollision {
        key_hex: bytes_to_hex(key),
    }
}

fn run_path(work_directory: &Path, pass: usize, file_id: usize) -> PathBuf {
    work_directory.join(format!("run-{pass:04}-{file_id:08}.parquet"))
}

fn trial_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(KEY_COLUMN, DataType::Binary, false),
        Field::new(PAYLOAD_COLUMN, DataType::Binary, false),
    ]))
}

fn batch_from_rows(rows: &[SortRow]) -> Result<RecordBatch, SortError> {
    let keys = BinaryArray::from_iter_values(rows.iter().map(|row| row.sort_key.as_slice()));
    let payloads = BinaryArray::from_iter_values(rows.iter().map(|row| row.payload.as_slice()));
    Ok(RecordBatch::try_new(
        trial_schema(),
        vec![Arc::new(keys), Arc::new(payloads)],
    )?)
}

fn rows_from_arrays(keys: &ArrayRef, payloads: &ArrayRef) -> Result<Vec<SortRow>, SortError> {
    let keys = keys
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(SortError::InvalidRun("排序键列不是 Binary"))?;
    let payloads = payloads
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(SortError::InvalidRun("载荷列不是 Binary"))?;
    if keys.len() != payloads.len() || keys.null_count() != 0 || payloads.null_count() != 0 {
        return Err(SortError::InvalidRun("列长度或可空性不一致"));
    }
    Ok((0..keys.len())
        .map(|index| {
            SortRow::from_sort_key(keys.value(index).to_vec(), payloads.value(index).to_vec())
        })
        .collect())
}

fn rows_from_batch(batch: &RecordBatch) -> Result<Vec<SortRow>, SortError> {
    if batch.num_columns() != 2 {
        return Err(SortError::InvalidRun("必须恰有两列"));
    }
    rows_from_arrays(batch.column(0), batch.column(1))
}

#[derive(Debug)]
struct RunInfo {
    path: PathBuf,
    bytes: u64,
    rows: u64,
}

struct RunDestination<'a> {
    work_directory: &'a Path,
    pass: usize,
    file_id: usize,
    current_temp_bytes: u64,
    active_temp_run_files: usize,
}

struct RunWriter {
    final_path: PathBuf,
    partial_path: PathBuf,
    writer: Option<ArrowWriter<QuotaWriter<File>>>,
    quota_exceeded: Arc<AtomicBool>,
    max_temp_bytes: u64,
    buffer: Vec<SortRow>,
    max_buffer_rows: usize,
    max_buffer_bytes: usize,
    rows: u64,
}

impl RunWriter {
    fn new(
        final_path: PathBuf,
        current_temp_bytes: u64,
        active_temp_run_files: usize,
        config: &SortConfig,
        metrics: &mut SortMetrics,
    ) -> Result<Self, SortError> {
        Self::new_with_buffer(
            final_path,
            current_temp_bytes,
            active_temp_run_files,
            config,
            metrics,
            Vec::with_capacity(config.batch_rows),
        )
    }

    fn new_without_buffer(
        final_path: PathBuf,
        current_temp_bytes: u64,
        active_temp_run_files: usize,
        config: &SortConfig,
        metrics: &mut SortMetrics,
    ) -> Result<Self, SortError> {
        Self::new_with_buffer(
            final_path,
            current_temp_bytes,
            active_temp_run_files,
            config,
            metrics,
            Vec::new(),
        )
    }

    fn new_with_buffer(
        final_path: PathBuf,
        current_temp_bytes: u64,
        active_temp_run_files: usize,
        config: &SortConfig,
        metrics: &mut SortMetrics,
        buffer: Vec<SortRow>,
    ) -> Result<Self, SortError> {
        if active_temp_run_files >= config.max_temp_run_files {
            return Err(SortError::TempRunFileLimit {
                needed: active_temp_run_files + 1,
                limit: config.max_temp_run_files,
            });
        }
        let quota = config
            .max_temp_bytes
            .checked_sub(current_temp_bytes)
            .ok_or(SortError::TempByteLimit {
                limit: config.max_temp_bytes,
            })?;
        let buffer_base_bytes =
            sort_row_buffer_bytes(&buffer).ok_or(SortError::RowMemoryLimit {
                observed: usize::MAX,
                limit: config.max_run_bytes,
            })?;
        if buffer_base_bytes > config.max_run_bytes {
            return Err(SortError::RowMemoryLimit {
                observed: buffer_base_bytes,
                limit: config.max_run_bytes,
            });
        }
        metrics.peak_write_buffer_bytes = metrics.peak_write_buffer_bytes.max(buffer_base_bytes);
        let partial_path = final_path.with_extension("parquet.partial");
        let file = File::options()
            .create_new(true)
            .write(true)
            .open(&partial_path)?;
        let quota_exceeded = Arc::new(AtomicBool::new(false));
        let quota_writer = QuotaWriter::new(file, quota, Arc::clone(&quota_exceeded));
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(config.batch_rows))
            .build();
        let writer = match ArrowWriter::try_new(quota_writer, trial_schema(), Some(properties)) {
            Ok(writer) => writer,
            Err(_error) if quota_exceeded.load(AtomicOrdering::Relaxed) => {
                return Err(SortError::TempByteLimit {
                    limit: config.max_temp_bytes,
                });
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            final_path,
            partial_path,
            writer: Some(writer),
            quota_exceeded,
            max_temp_bytes: config.max_temp_bytes,
            buffer,
            max_buffer_rows: config.batch_rows,
            max_buffer_bytes: config.max_run_bytes,
            rows: 0,
        })
    }

    fn push(&mut self, row: SortRow, metrics: &mut SortMetrics) -> Result<(), SortError> {
        let exceeds_rows = self.buffer.len() >= self.max_buffer_rows;
        let mut prospective_bytes = sort_row_buffer_bytes(&self.buffer)
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
            .filter(|bytes| *bytes <= self.max_buffer_bytes);
        let exceeds_bytes = prospective_bytes.is_none();
        if exceeds_rows || exceeds_bytes {
            if self.buffer.is_empty() {
                return Err(SortError::RowMemoryLimit {
                    observed: sort_row_buffer_bytes(&self.buffer)
                        .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                        .unwrap_or(usize::MAX),
                    limit: self.max_buffer_bytes,
                });
            }
            self.flush_buffer(metrics)?;
            prospective_bytes = sort_row_buffer_bytes(&self.buffer)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= self.max_buffer_bytes);
        }
        let observed = prospective_bytes.ok_or(SortError::RowMemoryLimit {
            observed: sort_row_buffer_bytes(&self.buffer)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .unwrap_or(usize::MAX),
            limit: self.max_buffer_bytes,
        })?;
        self.buffer.push(row);
        metrics.peak_write_buffer_rows = metrics.peak_write_buffer_rows.max(self.buffer.len());
        metrics.peak_write_buffer_bytes = metrics.peak_write_buffer_bytes.max(observed);
        Ok(())
    }

    fn write_unbuffered(&mut self, rows: &[SortRow]) -> Result<(), SortError> {
        if self.buffer.capacity() != 0 || rows.len() > self.max_buffer_rows {
            return Err(SortError::InvalidRun("无缓冲写入批次无效"));
        }
        self.write_rows(rows)
    }

    fn flush_buffer(&mut self, metrics: &mut SortMetrics) -> Result<(), SortError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = batch_from_rows(&self.buffer)?;
        self.write_batch(&batch, self.buffer.len())?;
        self.buffer.clear();
        let buffer_base_bytes =
            sort_row_buffer_bytes(&self.buffer).ok_or(SortError::RowMemoryLimit {
                observed: usize::MAX,
                limit: self.max_buffer_bytes,
            })?;
        if buffer_base_bytes > self.max_buffer_bytes {
            return Err(SortError::RowMemoryLimit {
                observed: buffer_base_bytes,
                limit: self.max_buffer_bytes,
            });
        }
        metrics.peak_write_buffer_bytes = metrics.peak_write_buffer_bytes.max(buffer_base_bytes);
        Ok(())
    }

    fn write_rows(&mut self, rows: &[SortRow]) -> Result<(), SortError> {
        let batch = batch_from_rows(rows)?;
        self.write_batch(&batch, rows.len())
    }

    fn write_batch(&mut self, batch: &RecordBatch, rows: usize) -> Result<(), SortError> {
        let writer = self.writer.as_mut().expect("完成前 writer 必须存在");
        let result: Result<(), SortError> = (|| {
            writer.write(batch)?;
            writer.flush()?;
            writer.sync()?;
            writer.inner_mut().sync_data_and_drop_cache()?;
            Ok(())
        })();
        if let Err(error) = result {
            if self.quota_exceeded.load(AtomicOrdering::Relaxed) {
                return Err(SortError::TempByteLimit {
                    limit: self.max_temp_bytes,
                });
            }
            return Err(error);
        }
        self.rows += rows as u64;
        Ok(())
    }

    fn finish(mut self, metrics: &mut SortMetrics) -> Result<RunInfo, SortError> {
        self.flush_buffer(metrics)?;
        let result = self.writer.take().expect("完成前 writer 必须存在").close();
        if let Err(error) = result {
            if self.quota_exceeded.load(AtomicOrdering::Relaxed) {
                return Err(SortError::TempByteLimit {
                    limit: self.max_temp_bytes,
                });
            }
            return Err(error.into());
        }
        fs::rename(&self.partial_path, &self.final_path)?;
        let bytes = fs::metadata(&self.final_path)?.len();
        Ok(RunInfo {
            path: self.final_path,
            bytes,
            rows: self.rows,
        })
    }
}

struct QuotaWriter<W> {
    inner: W,
    remaining: u64,
    exceeded: Arc<AtomicBool>,
}

impl<W> QuotaWriter<W> {
    fn new(inner: W, remaining: u64, exceeded: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            remaining,
            exceeded,
        }
    }
}

impl QuotaWriter<File> {
    fn sync_data_and_drop_cache(&mut self) -> io::Result<()> {
        self.inner.sync_data()?;
        fadvise(&self.inner, 0, None, Advice::DontNeed)
            .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))
    }
}

fn drop_file_cache(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    fadvise(&file, 0, None, Advice::DontNeed)
        .map_err(|error| io::Error::from_raw_os_error(error.raw_os_error()))
}

impl<W: Write> Write for QuotaWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() as u64 > self.remaining {
            self.exceeded.store(true, AtomicOrdering::Relaxed);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "临时文件字节配额已耗尽",
            ));
        }
        let written = self.inner.write(buffer)?;
        self.remaining -= written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct RunCursor {
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    next_row_group: usize,
    reader: Option<ParquetRecordBatchReader>,
    batch: std::vec::IntoIter<SortRow>,
    batch_rows: usize,
    max_batch_bytes: usize,
}

impl RunCursor {
    fn open(path: &Path, batch_rows: usize, max_batch_bytes: usize) -> Result<Self, SortError> {
        let file = File::open(path)?;
        let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
        Ok(Self {
            path: path.to_path_buf(),
            metadata,
            next_row_group: 0,
            reader: None,
            batch: Vec::new().into_iter(),
            batch_rows,
            max_batch_bytes,
        })
    }

    fn open_next_row_group(&mut self) -> Result<bool, SortError> {
        let row_group_count = self.metadata.metadata().num_row_groups();
        if self.next_row_group >= row_group_count {
            return Ok(false);
        }
        let row_group_index = self.next_row_group;
        self.next_row_group += 1;
        self.reader = Some(
            ParquetRecordBatchReaderBuilder::new_with_metadata(
                File::open(&self.path)?,
                self.metadata.clone(),
            )
            .with_row_groups(vec![row_group_index])
            .with_batch_size(self.batch_rows)
            .build()?,
        );
        Ok(true)
    }

    fn next_row(&mut self) -> Result<Option<SortRow>, SortError> {
        loop {
            if let Some(row) = self.batch.next() {
                return Ok(Some(row));
            }
            let Some(reader) = self.reader.as_mut() else {
                if self.open_next_row_group()? {
                    continue;
                }
                return Ok(None);
            };
            match reader.next() {
                Some(batch) => {
                    let rows = rows_from_batch(&batch?)?;
                    let observed = rows
                        .iter()
                        .try_fold(0usize, |total, row| total.checked_add(row.resident_bytes()));
                    if observed.is_none_or(|bytes| bytes > self.max_batch_bytes) {
                        return Err(SortError::RowMemoryLimit {
                            observed: observed.unwrap_or(usize::MAX),
                            limit: self.max_batch_bytes,
                        });
                    }
                    self.batch = rows.into_iter();
                }
                None => {
                    self.reader = None;
                    drop_file_cache(&self.path)?;
                }
            }
        }
    }
}

#[derive(Debug, Eq)]
struct HeapEntry {
    row: SortRow,
    run_slot: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.row.sort_key.cmp(&self.row.sort_key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.sort_key == other.row.sort_key
    }
}

fn scan_logical_output(
    path: &Path,
    batch_rows: usize,
    max_batch_bytes: usize,
) -> Result<(String, u64), SortError> {
    let mut cursor = RunCursor::open(path, batch_rows, max_batch_bytes)?;
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    let mut previous_key: Option<Vec<u8>> = None;
    let mut rows = 0u64;

    while let Some(row) = cursor.next_row()? {
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous >= row.sort_key.as_slice())
        {
            if previous_key.as_deref() == Some(row.sort_key.as_slice()) {
                return Err(collision_error(&row.sort_key));
            }
            return Err(SortError::InvalidRun("最终输出未严格递增"));
        }
        hash_length_prefixed(&mut hasher, &row.sort_key)?;
        hash_length_prefixed(&mut hasher, &row.payload)?;
        previous_key = Some(row.sort_key);
        rows += 1;
    }
    hasher.update(rows.to_be_bytes());
    Ok((bytes_to_hex(&hasher.finalize()), rows))
}

fn hash_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), SortError> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| SortError::InvalidRun("逻辑字段长度无法用 u64 表示"))?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::KeyValue;
    use serde_json::{Value, json};
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn gate_a_only_receipt() -> (GateAReceiptV3, contracts::GateACapabilityMatrixV2) {
        let sha = || contracts::Sha256Hex("a".repeat(64));
        let matrix = contracts::GateACapabilityMatrixV2 {
            schema_version: contracts::GATE_A_CAPABILITY_MATRIX_SCHEMA_V2.to_owned(),
            entries: data_pipeline_core::REQUIRED_GATE_A_CAPABILITIES
                .into_iter()
                .enumerate()
                .map(|(index, capability)| contracts::GateACapabilityEntryV2 {
                    capability,
                    status: contracts::CapabilityStatus::Pass,
                    contract_field_paths: vec!["training.admission".to_owned()],
                    evidence_ids: vec![format!("training-admission-{index}")],
                    blocking_reason: None,
                })
                .collect(),
        };
        let mut receipt = GateAReceiptV3 {
            schema_version: contracts::GATE_A_RECEIPT_SCHEMA_V3.to_owned(),
            receipt_id: contracts::Sha256Hex("0".repeat(64)),
            requirement_id: "DATA-PIPELINE-TRAINING-20260803".to_owned(),
            pipeline_core_identity_sha256: sha(),
            canonical_event_record_contract_version: contracts::CANONICAL_EVENT_RECORD_SCHEMA_V1
                .to_owned(),
            execution_event_contract_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3
                .to_owned(),
            reference_event_contract_version: contracts::CANONICAL_REFERENCE_EVENT_SCHEMA_V1
                .to_owned(),
            observation_contract_version: contracts::MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3
                .to_owned(),
            event_time_index_contract_version: contracts::EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
            visibility_index_contract_version: contracts::VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
            dataset_lineage_root: DATASET_LINEAGE_ROOT.to_owned(),
            dataset_date_floor: contracts::DATASET_DATE_FLOOR.to_owned(),
            covered_utc_dates: vec!["2026-08-03".to_owned()],
            input_set_identity_sha256: sha(),
            supported_schema_set_identity_sha256: sha(),
            input_manifest_set_sha256: sha(),
            canonical_event_store_manifest_sha256: sha(),
            event_time_index_manifest_sha256: sha(),
            visibility_index_manifest_sha256: sha(),
            observation_manifest_sha256: sha(),
            capability_matrix_sha256: data_pipeline_core::gate_a_capability_matrix_digest(&matrix)
                .unwrap(),
            run_manifest_sha256: sha(),
            training_dataset_ready: false,
            training_split_manifest_id: "N/A".to_owned(),
            training_split_manifest_sha256: "N/A".to_owned(),
        };
        receipt.receipt_id = data_pipeline_core::derive_gate_a_receipt_v3_id(&receipt).unwrap();
        (receipt, matrix)
    }

    #[test]
    fn gate_a_receipt_alone_is_rejected_by_training_admission() {
        let (receipt, matrix) = gate_a_only_receipt();
        data_pipeline_core::validate_gate_a_receipt_v3(&receipt, &matrix).unwrap();
        assert_eq!(
            admit_gate_a_receipt_for_training(&receipt),
            Err(TrainingAdmissionError::GateAIsNotTrainingDatasetReady)
        );
    }

    fn valid_daily_manifest_header_value() -> Value {
        json!({
            "schema_version": "data_collect_daily_manifest_v3",
            "state": "closed",
            "utc_day": "2026-07-30",
            "closure_reason": "utc_day_barrier",
            "collection_day_coverage": {
                "schema_version": "data_collect_collection_day_coverage_v1",
                "state": "ended_known",
                "first_started_utc_ms": 1_785_369_600_000_i64,
                "last_started_utc_ms": 1_785_369_600_000_i64,
                "ended_utc_ms": 1_785_456_000_000_i64,
                "reactivated_within_day": false
            }
        })
    }

    fn validate_synthetic_daily_manifest_header(
        value: &Value,
    ) -> Result<ValidatedDailyManifestHeaderV3, ManifestHeaderValidationError> {
        let bytes = serde_json::to_vec(value).expect("编码合成每日清单头部");
        validate_daily_manifest_header_bytes("2026-07-30", &bytes)
    }

    fn daily_manifest_fixture(root: &Path, listed_paths: &[&str]) -> PathBuf {
        let day_dir = root.join("2026-07-30/typed_raw/utc_day=2026-07-30");
        fs::create_dir_all(&day_dir).expect("创建合成 UTC 日目录");
        let mut manifest = valid_daily_manifest_header_value();
        manifest["files"] = Value::Array(
            listed_paths
                .iter()
                .map(|path| {
                    json!({
                        "path": path,
                        "source_kind": "binance_aggtrade",
                        "rows": 1,
                    })
                })
                .collect(),
        );
        let manifest_path = day_dir.join("manifest.json");
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest).expect("编码合成每日清单"),
        )
        .expect("写入合成每日清单");
        manifest_path
    }

    fn complete_daily_manifest_fixture(root: &Path) -> PathBuf {
        let manifest_path = daily_manifest_fixture(
            root,
            &["source_kind=binance_aggtrade/binance_aggtrade_00.parquet"],
        );
        let parquet_path = manifest_path
            .parent()
            .expect("合成清单必须位于 UTC 日目录")
            .join("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        fs::create_dir_all(parquet_path.parent().expect("Parquet 必须有来源目录"))
            .expect("创建合成来源目录");
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "parquet_raw_source_kind".to_owned(),
                    "binance_aggtrade".to_owned(),
                ),
                KeyValue::new(
                    "parquet_raw_layout_version".to_owned(),
                    "binance_aggtrade_v7".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_artifact_kind".to_owned(),
                    "typed_raw_parquet".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_daily_file_fact_v1".to_owned(),
                    r#"{"source_kind":"binance_aggtrade","schema_version":"binance_aggtrade_v7","utc_hour":"2026-07-30T00","part":0,"rows":2}"#.to_owned(),
                ),
            ]))
            .build();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2]))],
        )
        .expect("构造合成 Parquet 批次");
        let mut writer = ArrowWriter::try_new(
            File::create(&parquet_path).expect("创建合成 Parquet"),
            schema,
            Some(properties),
        )
        .expect("创建合成 Parquet writer");
        writer.write(&batch).expect("写入合成 Parquet 批次");
        writer.close().expect("关闭合成 Parquet writer");

        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取初始合成清单"))
                .expect("解析初始合成清单");
        let bytes = fs::metadata(&parquet_path)
            .expect("读取合成 Parquet 字节数")
            .len();
        let sha256 = bytes_to_hex(&Sha256::digest(
            fs::read(&parquet_path).expect("读取合成 Parquet 计算摘要"),
        ));
        manifest["files"][0] = json!({
            "path": "source_kind=binance_aggtrade/binance_aggtrade_00.parquet",
            "source_kind": "binance_aggtrade",
            "schema_version": "binance_aggtrade_v7",
            "utc_hour": "2026-07-30T00",
            "part": 0,
            "bytes": bytes,
            "rows": 2,
            "sha256": sha256,
        });
        manifest["producer_barriers"] = json!([
            "binance_aggtrade",
            "binance_depth",
            "polymarket",
            "polymarket_rtds",
        ]);
        manifest["writer_finalize_acks"] = json!([
            "binance_aggtrade",
            "binance_depth_snapshot",
            "binance_depth_update",
            "polymarket_market",
            "polymarket_market_metadata",
            "polymarket_rtds_chainlink_btc_usd",
        ]);
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest).expect("编码完整合成清单"),
        )
        .expect("写入完整合成清单");
        manifest_path
    }

    fn write_manifest_value(manifest_path: &Path, value: &Value) {
        fs::write(
            manifest_path,
            serde_json::to_vec(value).expect("编码修改后的合成清单"),
        )
        .expect("写入修改后的合成清单");
    }

    fn rewrite_test_parquet_footer_metadata(path: &Path, daily_file_fact: &str) {
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "parquet_raw_source_kind".to_owned(),
                    "binance_aggtrade".to_owned(),
                ),
                KeyValue::new(
                    "parquet_raw_layout_version".to_owned(),
                    "binance_aggtrade_v7".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_artifact_kind".to_owned(),
                    "typed_raw_parquet".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_daily_file_fact_v1".to_owned(),
                    daily_file_fact.to_owned(),
                ),
            ]))
            .build();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1_i64, 2]))],
        )
        .expect("构造改写 Parquet 批次");
        let mut writer = ArrowWriter::try_new(
            File::create(path).expect("重建合成 Parquet"),
            schema,
            Some(properties),
        )
        .expect("创建改写 Parquet writer");
        writer.write(&batch).expect("写入改写 Parquet 批次");
        writer.close().expect("关闭改写 Parquet writer");
    }

    fn rewrite_test_parquet_values(path: &Path, values: Vec<i64>) {
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "parquet_raw_source_kind".to_owned(),
                    "binance_aggtrade".to_owned(),
                ),
                KeyValue::new(
                    "parquet_raw_layout_version".to_owned(),
                    "binance_aggtrade_v7".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_artifact_kind".to_owned(),
                    "typed_raw_parquet".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_daily_file_fact_v1".to_owned(),
                    format!(
                        r#"{{"source_kind":"binance_aggtrade","schema_version":"binance_aggtrade_v7","utc_hour":"2026-07-30T00","part":0,"rows":{}}}"#,
                        values.len()
                    ),
                ),
            ]))
            .build();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int64Array::from(values))],
        )
        .expect("构造内容变化后的合成 Parquet 批次");
        let mut writer = ArrowWriter::try_new(
            File::create(path).expect("重建内容变化后的合成 Parquet"),
            schema,
            Some(properties),
        )
        .expect("创建内容变化后的合成 Parquet writer");
        writer
            .write(&batch)
            .expect("写入内容变化后的合成 Parquet 批次");
        writer.close().expect("关闭内容变化后的合成 Parquet writer");
    }

    #[test]
    fn frozen_input_set_rejects_a_changed_but_independently_admissible_input() {
        let temp = TempDir::new().expect("创建输入集合冻结临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let manifest_paths = vec![manifest_path.clone()];
        let snapshot = freeze_direct_source_input_set_with_admission(&manifest_paths, |path| {
            validate_daily_manifest_admission_at_root(&root, path)
        })
        .expect("初始合成输入应能冻结");
        let repeated = freeze_direct_source_input_set_with_admission(&manifest_paths, |path| {
            validate_daily_manifest_admission_at_root(&root, path)
        })
        .expect("相同合成输入应能重复冻结");

        assert_eq!(snapshot.admitted_daily_inputs().len(), 1);
        assert_eq!(
            snapshot.input_set_identity_sha256(),
            repeated.input_set_identity_sha256()
        );
        assert_eq!(
            snapshot.input_manifest_set_sha256(),
            repeated.input_manifest_set_sha256()
        );
        assert!(is_lowercase_sha256(snapshot.input_set_identity_sha256()));
        assert!(is_lowercase_sha256(snapshot.input_manifest_set_sha256()));

        let parquet_path = manifest_path
            .parent()
            .expect("合成清单必须位于 UTC 日目录")
            .join("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        rewrite_test_parquet_values(&parquet_path, vec![2_i64, 1]);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取初始完整清单"))
                .expect("解析初始完整清单");
        manifest["files"][0]["bytes"] = json!(
            fs::metadata(&parquet_path)
                .expect("读取变化后 Parquet 字节数")
                .len()
        );
        manifest["files"][0]["sha256"] = json!(bytes_to_hex(&Sha256::digest(
            fs::read(&parquet_path).expect("读取变化后 Parquet 计算摘要"),
        )));
        write_manifest_value(&manifest_path, &manifest);
        validate_daily_manifest_admission_at_root(&root, &manifest_path)
            .expect("变化后的 Parquet 与清单仍应独立通过准入");

        assert!(matches!(
            snapshot.verify_unchanged_with_admission(|path| {
                validate_daily_manifest_admission_at_root(&root, path)
            }),
            Err(InputSetSnapshotError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn manifest_inner_utc_day_partition_must_match_outer_dataset_day() {
        let temp = TempDir::new().expect("创建路径资格临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let original_day_dir = manifest_path.parent().expect("合成清单必须位于 UTC 日目录");
        let mismatched_day_dir = original_day_dir
            .parent()
            .expect("合成清单必须位于 typed_raw 目录")
            .join("utc_day=2026-07-31");
        fs::rename(original_day_dir, &mismatched_day_dir).expect("改写内层 UTC 日分区");
        let mismatched_manifest_path = mismatched_day_dir.join("manifest.json");

        let error = qualify_manifest_path(&root, &mismatched_manifest_path)
            .expect_err("内层 UTC 日分区不得与外层数据日期不一致");

        assert!(matches!(
            error,
            ManifestPathQualificationError::InvalidDatasetDate { ref path }
                if path == &mismatched_manifest_path
        ));
    }

    #[test]
    fn manifest_file_identity_tuple_must_match_its_derived_layout_identity() {
        let temp = TempDir::new().expect("创建身份元组校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let day_dir = manifest_path.parent().expect("合成清单必须位于 UTC 日目录");
        let original_path =
            day_dir.join("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        let changed_hour_path =
            day_dir.join("source_kind=binance_aggtrade/binance_aggtrade_01.parquet");
        fs::rename(&original_path, &changed_hour_path).expect("只改写 Parquet 文件名小时");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        manifest["files"][0]["path"] =
            json!("source_kind=binance_aggtrade/binance_aggtrade_01.parquet");
        write_manifest_value(&manifest_path, &manifest);

        let error = validate_daily_manifest_admission_at_root(&root, &manifest_path)
            .expect_err("文件名小时与清单身份元组不得不一致");

        assert!(
            matches!(
                error,
                ManifestAdmissionValidationError::FileIdentityMismatch {
                    ref path,
                    ref expected,
                    ref found,
                } if path == Path::new("source_kind=binance_aggtrade/binance_aggtrade_01.parquet")
                    && expected.as_ref() == &DerivedManifestFileIdentityV3 {
                        source_kind: "binance_aggtrade".to_owned(),
                        schema_version: "binance_aggtrade_v7".to_owned(),
                        utc_day: "2026-07-30".to_owned(),
                        utc_hour: "2026-07-30T01".to_owned(),
                        part: 0,
                    }
                    && found.as_ref() == &DerivedManifestFileIdentityV3 {
                        source_kind: "binance_aggtrade".to_owned(),
                        schema_version: "binance_aggtrade_v7".to_owned(),
                        utc_day: "2026-07-30".to_owned(),
                        utc_hour: "2026-07-30T00".to_owned(),
                        part: 0,
                    }
            ),
            "文件身份元组被错误分类为：{error:?}"
        );
    }

    #[test]
    fn success_admission_requires_the_exact_closed_terminal_tuple() {
        let temp = TempDir::new().expect("创建终态校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = daily_manifest_fixture(&root, &[]);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取恢复形状清单"))
                .expect("解析恢复形状清单");
        manifest["closure_reason"] = json!("reconciled_collection_not_started");
        manifest["collection_day_coverage"]["state"] = json!("not_started");
        manifest["collection_day_coverage"]["first_started_utc_ms"] = Value::Null;
        manifest["collection_day_coverage"]["last_started_utc_ms"] = Value::Null;
        manifest["collection_day_coverage"]["ended_utc_ms"] = Value::Null;
        manifest["collection_day_coverage"]["reactivated_within_day"] = json!(false);
        manifest
            .as_object_mut()
            .expect("恢复形状清单必须是对象")
            .remove("producer_barriers");
        manifest
            .as_object_mut()
            .expect("恢复形状清单必须是对象")
            .remove("writer_finalize_acks");
        write_manifest_value(&manifest_path, &manifest);

        let error = validate_daily_manifest_admission_at_root(&root, &manifest_path)
            .expect_err("恢复关闭终态不得通过成功准入");

        assert!(
            matches!(
                error,
                ManifestAdmissionValidationError::AdmissionTerminalMismatch {
                    ref closure_reason,
                    ref coverage_state,
                } if closure_reason == "reconciled_collection_not_started" && coverage_state == "not_started"
            ),
            "恢复关闭终态被错误分类为：{error:?}"
        );
    }

    #[test]
    fn persistent_file_integrity_mismatches_are_rejected() {
        let temp = TempDir::new().expect("创建内容校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let base: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        let mut footer_rows = base.clone();
        footer_rows["files"][0]["rows"] = json!(3);
        write_manifest_value(&manifest_path, &footer_rows);

        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::FooterRowCountMismatch { .. })
        ));

        let mut bytes = base.clone();
        bytes["files"][0]["bytes"] = json!(1);
        write_manifest_value(&manifest_path, &bytes);
        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::FileByteCountMismatch { .. })
        ));

        let mut sha256 = base;
        sha256["files"][0]["sha256"] = json!("0".repeat(64));
        write_manifest_value(&manifest_path, &sha256);
        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::FileSha256Mismatch { .. })
        ));
    }

    #[test]
    fn parquet_footer_daily_file_fact_must_match_the_manifest_identity_tuple() {
        let temp = TempDir::new().expect("创建 footer 元数据校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let parquet_path = manifest_path
            .parent()
            .expect("合成清单必须位于 UTC 日目录")
            .join("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        rewrite_test_parquet_footer_metadata(
            &parquet_path,
            r#"{"source_kind":"binance_aggtrade","schema_version":"binance_aggtrade_v7","utc_hour":"2026-07-30T00","part":1,"rows":2}"#,
        );
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        manifest["files"][0]["bytes"] = json!(fs::metadata(&parquet_path).unwrap().len());
        manifest["files"][0]["sha256"] = json!(bytes_to_hex(&Sha256::digest(
            fs::read(&parquet_path).expect("读取改写后的合成 Parquet"),
        )));
        write_manifest_value(&manifest_path, &manifest);

        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::FooterMetadataIdentityMismatch { .. })
        ));
    }

    #[test]
    fn noncanonical_part_suffix_is_rejected_as_invalid_file_layout() {
        let temp = TempDir::new().expect("创建分片布局校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let day_dir = manifest_path.parent().expect("合成清单必须位于 UTC 日目录");
        let original_path =
            day_dir.join("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        let invalid_path =
            day_dir.join("source_kind=binance_aggtrade/binance_aggtrade_00-part-01.parquet");
        fs::rename(&original_path, &invalid_path).expect("改写为非规范分片文件名");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        manifest["files"][0]["path"] =
            json!("source_kind=binance_aggtrade/binance_aggtrade_00-part-01.parquet");
        write_manifest_value(&manifest_path, &manifest);

        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::InvalidFileLayout { .. })
        ));
    }

    #[test]
    fn missing_writer_finalize_ack_is_rejected() {
        let temp = TempDir::new().expect("创建围栏校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        manifest["writer_finalize_acks"] = json!([
            "binance_aggtrade",
            "binance_depth_snapshot",
            "binance_depth_update",
            "polymarket_market",
            "polymarket_market_metadata",
        ]);
        write_manifest_value(&manifest_path, &manifest);

        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::BarrierEvidenceMismatch { .. })
        ));
    }

    #[test]
    fn manifest_sha256_must_be_64_lowercase_hex_digits() {
        let temp = TempDir::new().expect("创建摘要格式校验临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = complete_daily_manifest_fixture(&root);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("读取完整合成清单"))
                .expect("解析完整合成清单");
        let sha256 = manifest["files"][0]["sha256"]
            .as_str()
            .expect("完整夹具必须带摘要")
            .to_ascii_uppercase();
        manifest["files"][0]["sha256"] = json!(sha256);
        write_manifest_value(&manifest_path, &manifest);

        assert!(matches!(
            validate_daily_manifest_admission_at_root(&root, &manifest_path),
            Err(ManifestAdmissionValidationError::InvalidManifestSha256 { .. })
        ));
    }

    #[test]
    fn listed_final_parquet_path_is_qualified_without_reading_its_content() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let relative_path = Path::new("source_kind=binance_aggtrade/binance_aggtrade_00.parquet");
        let manifest_path = daily_manifest_fixture(
            &root,
            &["source_kind=binance_aggtrade/binance_aggtrade_00.parquet"],
        );
        let parquet_path = manifest_path.parent().unwrap().join(relative_path);
        fs::create_dir_all(parquet_path.parent().unwrap()).expect("创建合成来源目录");
        fs::write(&parquet_path, b"content validation belongs to task 1.2")
            .expect("写入不解码的路径夹具");

        let file_set = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect("合法清单文件路径必须通过资格检查");

        assert_eq!(file_set.header().utc_day(), "2026-07-30");
        assert_eq!(
            file_set.manifest_path(),
            manifest_path.canonicalize().unwrap()
        );
        assert_eq!(file_set.files().len(), 1);
        assert_eq!(file_set.files()[0].manifest_relative_path(), relative_path);
        assert_eq!(
            file_set.files()[0].canonical_path(),
            parquet_path.canonicalize().unwrap()
        );
    }

    #[test]
    fn manifest_larger_than_10_mib_is_rejected_before_json_parsing() {
        let temp = TempDir::new().expect("创建清单字节上限临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = daily_manifest_fixture(&root, &[]);
        fs::write(&manifest_path, vec![b'x'; 10 * 1024 * 1024 + 1])
            .expect("写入超过上限的非 JSON 清单");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("超过 10 MiB 的清单必须在 JSON 解析前失败");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::Header(
                ManifestHeaderValidationError::ManifestBytesLimitExceeded {
                    limit_bytes,
                    observed_bytes,
                }
            ) if limit_bytes == 10 * 1024 * 1024
                && observed_bytes == 10 * 1024 * 1024 + 1
        ));
    }

    #[test]
    fn malformed_files_structure_is_rejected_as_json() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = daily_manifest_fixture(&root, &[]);
        let mut manifest = valid_daily_manifest_header_value();
        manifest["files"] = json!({ "path": "not-an-array.parquet" });
        fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest).expect("编码损坏 files 结构"),
        )
        .expect("写入损坏 files 结构");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("files 非数组结构必须失败关闭");

        assert!(matches!(error, ManifestFileSetQualificationError::Json(_)));
    }

    #[test]
    fn manifest_file_parent_escape_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = daily_manifest_fixture(&root, &["../outside.parquet"]);
        let outside_path = manifest_path.parent().unwrap().join("../outside.parquet");
        fs::write(&outside_path, b"outside the manifest day directory").expect("写入越界候选文件");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单文件路径不得逃逸 UTC 日目录");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::UnsafeManifestFilePath { ref path }
                if path == Path::new("../outside.parquet")
        ));
    }

    #[test]
    fn manifest_file_curdir_component_is_not_silently_normalized() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let listed_path = "source_kind=binance_aggtrade/./curdir.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[listed_path]);
        let normalized_path = manifest_path
            .parent()
            .unwrap()
            .join("source_kind=binance_aggtrade/curdir.parquet");
        fs::create_dir_all(normalized_path.parent().unwrap()).expect("创建合成来源目录");
        fs::write(&normalized_path, b"curdir must not be normalized").expect("写入规范化目标");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单文件路径不得静默消去当前目录组件");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::UnsafeManifestFilePath { ref path }
                if path == Path::new("source_kind=binance_aggtrade/./curdir.parquet")
        ));
    }

    #[test]
    fn manifest_file_symlink_escape_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let relative_path = Path::new("source_kind=binance_aggtrade/escaped.parquet");
        let manifest_path =
            daily_manifest_fixture(&root, &["source_kind=binance_aggtrade/escaped.parquet"]);
        let outside_path = temp.path().join("outside.parquet");
        fs::write(&outside_path, b"outside the manifest day directory")
            .expect("写入符号链接外部目标");
        let symlink_path = manifest_path.parent().unwrap().join(relative_path);
        fs::create_dir_all(symlink_path.parent().unwrap()).expect("创建合成来源目录");
        symlink(&outside_path, &symlink_path).expect("创建越界符号链接");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单文件符号链接不得逃逸 UTC 日目录");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::ManifestFileEscapesDay {
                ref relative_path,
                ref canonical_path,
            } if relative_path == Path::new("source_kind=binance_aggtrade/escaped.parquet")
                && canonical_path == &outside_path.canonicalize().unwrap()
        ));
    }

    #[test]
    fn duplicate_manifest_relative_path_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let relative = "source_kind=binance_aggtrade/duplicate.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[relative, relative]);
        let parquet_path = manifest_path.parent().unwrap().join(relative);
        fs::create_dir_all(parquet_path.parent().unwrap()).expect("创建合成来源目录");
        fs::write(&parquet_path, b"duplicate manifest entry").expect("写入重复条目目标");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单不得重复列出相同相对路径");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::DuplicateManifestRelativePath { ref path }
                if path == Path::new("source_kind=binance_aggtrade/duplicate.parquet")
        ));
    }

    #[test]
    fn manifest_parquet_symlink_to_non_parquet_file_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let alias_relative = "source_kind=binance_aggtrade/alias.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[alias_relative]);
        let day_dir = manifest_path.parent().unwrap();
        let target_path = day_dir.join("source_kind=binance_aggtrade/ordinary-target.bin");
        fs::create_dir_all(target_path.parent().unwrap()).expect("创建合成来源目录");
        fs::write(&target_path, b"ordinary non-parquet target").expect("写入普通非 Parquet 目标");
        symlink(&target_path, day_dir.join(alias_relative)).expect("创建 Parquet 名称符号链接");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单成员必须是来源目录中的普通文件");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::ManifestFileOutsideBoundedLayout { ref path }
                if path == Path::new("source_kind=binance_aggtrade/alias.parquet")
        ));
    }

    #[test]
    fn unlisted_running_parquet_object_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let relative = "source_kind=binance_aggtrade/binance_aggtrade_00.parquet.running";
        let manifest_path = daily_manifest_fixture(&root, &[]);
        let running_path = manifest_path.parent().unwrap().join(relative);
        fs::create_dir_all(running_path.parent().unwrap()).expect("创建合成来源目录");
        fs::write(&running_path, b"unfinished parquet writer output").expect("写入运行中文件");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("运行中 Parquet 对象不得取得清单成员资格");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::TemporaryManifestObject { ref path }
                if path == Path::new(
                    "source_kind=binance_aggtrade/binance_aggtrade_00.parquet.running"
                )
        ));
    }

    #[test]
    fn unlisted_rsync_partial_directory_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let relative = "source_kind=binance_aggtrade/.rsync-partial/upload.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[]);
        let partial_path = manifest_path.parent().unwrap().join(relative);
        fs::create_dir_all(partial_path.parent().unwrap()).expect("创建 rsync 临时目录");
        fs::write(&partial_path, b"incomplete rsync transfer").expect("写入 rsync 临时对象");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("rsync 临时对象不得取得清单成员资格");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::TemporaryManifestObject { ref path }
                if path == Path::new("source_kind=binance_aggtrade/.rsync-partial")
        ));
    }

    #[test]
    fn unlisted_finalized_parquet_in_one_level_source_directory_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let listed_relative = "source_kind=binance_aggtrade/listed.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[listed_relative]);
        let source_dir = manifest_path
            .parent()
            .unwrap()
            .join("source_kind=binance_aggtrade");
        fs::create_dir_all(&source_dir).expect("创建合成来源目录");
        fs::write(source_dir.join("listed.parquet"), b"listed object").expect("写入清单内对象");
        fs::write(source_dir.join("unlisted.parquet"), b"unlisted object").expect("写入清单外对象");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("清单外最终 Parquet 不得进入合格文件集");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::UnlistedFinalizedParquet { ref path }
                if path == Path::new("source_kind=binance_aggtrade/unlisted.parquet")
        ));
    }

    #[test]
    fn nested_directory_with_unlisted_parquet_is_rejected() {
        let temp = TempDir::new().expect("创建文件集资格临时目录");
        let root = temp.path().join("dataset-root");
        let listed_relative = "source_kind=binance_aggtrade/listed.parquet";
        let manifest_path = daily_manifest_fixture(&root, &[listed_relative]);
        let source_dir = manifest_path
            .parent()
            .unwrap()
            .join("source_kind=binance_aggtrade");
        fs::create_dir_all(&source_dir).expect("创建合法浅层来源目录");
        fs::write(source_dir.join("listed.parquet"), b"listed object").expect("写入清单内浅层对象");
        let nested_dir = source_dir.join("nested");
        fs::create_dir_all(&nested_dir).expect("创建超出有界扫描的目录");
        fs::write(nested_dir.join("unlisted.parquet"), b"unlisted deep object")
            .expect("写入深层清单外对象");

        let error = qualify_direct_source_daily_manifest_files_at_root(&root, &manifest_path)
            .expect_err("来源目录内的嵌套目录不得绕过清单外文件检查");

        assert!(matches!(
            error,
            ManifestFileSetQualificationError::ManifestFileOutsideBoundedLayout { ref path }
                if path == Path::new("source_kind=binance_aggtrade/nested")
        ));
    }

    #[test]
    fn valid_daily_manifest_header_is_accepted() {
        let header = validate_synthetic_daily_manifest_header(&valid_daily_manifest_header_value())
            .expect("合法每日清单头部必须通过校验");

        assert_eq!(header.coverage_state(), "ended_known");
    }

    #[test]
    fn legacy_daily_manifest_schema_is_rejected() {
        let mut value = valid_daily_manifest_header_value();
        value["schema_version"] = json!("data_collect_daily_manifest_v2");

        let error = validate_synthetic_daily_manifest_header(&value)
            .expect_err("旧每日清单 schema 必须失败关闭");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::UnsupportedManifestSchema { ref found }
                if found == "data_collect_daily_manifest_v2"
        ));
    }

    #[test]
    fn non_closed_daily_manifest_is_rejected() {
        let mut value = valid_daily_manifest_header_value();
        value["state"] = json!("open");

        let error = validate_synthetic_daily_manifest_header(&value)
            .expect_err("未关闭的每日清单不得通过头部校验");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::ManifestNotClosed { ref found }
                if found == "open"
        ));
    }

    #[test]
    fn blank_daily_manifest_closure_reason_is_rejected() {
        let mut value = valid_daily_manifest_header_value();
        value["closure_reason"] = json!(" \t\n ");

        let error = validate_synthetic_daily_manifest_header(&value)
            .expect_err("空白关闭原因不得通过头部校验");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::InvalidClosureReason
        ));
    }

    #[test]
    fn collection_last_start_at_next_midnight_is_rejected() {
        let mut value = valid_daily_manifest_header_value();
        value["collection_day_coverage"]["last_started_utc_ms"] = json!(1_785_456_000_000_i64);

        let error = validate_synthetic_daily_manifest_header(&value)
            .expect_err("最后启动时间不得等于次日零点");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::InvalidCollectionDayCoverage
        ));
    }

    #[test]
    fn manifest_utc_day_must_match_qualified_path_day() {
        let mut value = valid_daily_manifest_header_value();
        value["utc_day"] = json!("2026-07-31");
        value["collection_day_coverage"]["first_started_utc_ms"] = json!(1_785_456_000_000_i64);
        value["collection_day_coverage"]["last_started_utc_ms"] = json!(1_785_456_000_000_i64);
        value["collection_day_coverage"]["ended_utc_ms"] = json!(1_785_542_400_000_i64);

        let error = validate_synthetic_daily_manifest_header(&value)
            .expect_err("清单 UTC 日必须与安全路径日期一致");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::UtcDayMismatch {
                ref path_utc_day,
                ref manifest_utc_day,
            } if path_utc_day == "2026-07-30" && manifest_utc_day == "2026-07-31"
        ));
    }

    #[test]
    fn canonical_manifest_path_rejects_symlink_escape() {
        let temp = TempDir::new().expect("创建路径准入临时目录");
        let root = temp.path().join("dataset-root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).expect("创建数据根");
        fs::create_dir_all(&outside).expect("创建根外目录");
        fs::write(
            outside.join("manifest.json"),
            br#"{"utc_day":"2026-07-30"}"#,
        )
        .expect("写入最小真实派生清单");
        symlink(&outside, root.join("2026-07-30")).expect("创建逃逸符号链接");

        let error = qualify_manifest_path(&root, &root.join("2026-07-30/manifest.json"))
            .expect_err("符号链接不得逃逸直接源根");

        assert!(matches!(
            error,
            ManifestPathQualificationError::OutsideDatasetLineageRoot { .. }
        ));
    }

    #[test]
    fn canonical_manifest_path_rejects_symlink_to_older_day_inside_root() {
        let temp = TempDir::new().expect("创建路径准入临时目录");
        let root = temp.path().join("dataset-root");
        let old_day = root.join("2026-07-29");
        fs::create_dir_all(&old_day).expect("创建根内旧日期目录");
        fs::write(old_day.join("manifest.json"), b"{}").expect("写入旧日期候选文件");
        symlink(&old_day, root.join("2026-07-30")).expect("创建根内跨日期符号链接");

        let error = qualify_manifest_path(&root, &root.join("2026-07-30/manifest.json"))
            .expect_err("词法合格日期不得通过符号链接读取根内旧日期文件");

        assert!(matches!(
            error,
            ManifestPathQualificationError::IneligibleBeforeDatasetFloor { ref utc_day }
                if utc_day == "2026-07-29"
        ));
    }

    #[test]
    fn opened_manifest_rejects_parent_swap_after_path_qualification() {
        let temp = TempDir::new().expect("创建路径准入临时目录");
        let root = temp.path().join("dataset-root");
        let day = root.join("2026-07-30");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&day).expect("创建数据日期目录");
        fs::create_dir_all(&outside).expect("创建根外目录");
        let manifest_path = day.join("manifest.json");
        fs::write(&manifest_path, b"{}").expect("写入初始清单");
        fs::write(outside.join("manifest.json"), b"{}").expect("写入根外清单");
        let qualified = qualify_manifest_path(&root, &manifest_path).expect("初始路径应合格");

        fs::remove_file(&manifest_path).expect("移除初始清单");
        fs::remove_dir(&day).expect("移除初始日期目录");
        symlink(&outside, &day).expect("用根外符号链接替换日期目录");
        let opened = File::open(qualified.manifest_path()).expect("打开替换后的清单");

        let error = read_qualified_manifest_file(&root, &qualified, opened)
            .expect_err("读取对象必须重新绑定到已验证的根和日期");

        assert!(matches!(
            error,
            ManifestHeaderValidationError::Path(
                ManifestPathQualificationError::OutsideDatasetLineageRoot { .. }
            )
        ));
    }

    fn sort_key(sequence: u64) -> Vec<u8> {
        sequence.to_be_bytes().to_vec()
    }

    fn row(sequence: u64) -> SortRow {
        SortRow::from_sort_key(
            sort_key(sequence),
            format!("payload-{sequence:020}").into_bytes(),
        )
    }

    fn config(max_run_rows: usize, merge_fan_in: usize) -> SortConfig {
        SortConfig {
            max_run_rows,
            max_run_bytes: 4 * 1024,
            max_temp_run_files: 64,
            merge_fan_in,
            max_open_files: 8,
            batch_rows: 3,
            max_temp_bytes: 4 * 1024 * 1024,
        }
    }

    fn read_rows(path: &Path, batch_rows: usize) -> Vec<SortRow> {
        let mut cursor = RunCursor::open(path, batch_rows, 4 * 1024).expect("打开输出");
        let mut rows = Vec::new();
        while let Some(row) = cursor.next_row().expect("读取输出") {
            rows.push(row);
        }
        rows
    }

    #[test]
    fn forces_multiple_runs_and_multiple_merge_passes() {
        let temp = TempDir::new().expect("创建临时目录");
        let input: Vec<_> = (0u64..17).rev().map(row).collect();

        let outcome = external_sort(input, temp.path(), &config(2, 2)).expect("执行外部排序");

        assert_eq!(outcome.metrics.input_rows, 17);
        assert_eq!(outcome.metrics.output_rows, 17);
        assert_eq!(outcome.metrics.initial_runs, 9);
        assert_eq!(outcome.metrics.merge_passes, 4);
        assert!(outcome.metrics.peak_heap_entries <= 2);
        assert_eq!(outcome.metrics.peak_temp_run_files, 10);
        assert!(outcome.metrics.peak_open_files <= 3);
        assert!(outcome.metrics.peak_run_rows <= 2);
        assert!(outcome.metrics.peak_run_bytes <= 4 * 1024);
        assert!(outcome.metrics.peak_write_buffer_rows > 0);
        assert!(outcome.metrics.peak_write_buffer_bytes > 0);
        assert!(outcome.metrics.peak_write_buffer_rows <= 3);
        assert!(outcome.metrics.peak_write_buffer_bytes <= 4 * 1024);
        assert!(outcome.metrics.peak_temp_bytes <= 4 * 1024 * 1024);

        let keys: Vec<_> = read_rows(&outcome.output_path, 2)
            .into_iter()
            .map(|row| row.sort_key_bytes().to_vec())
            .collect();
        assert_eq!(keys, (0u64..17).map(sort_key).collect::<Vec<_>>());
    }

    #[test]
    fn parquet_row_groups_follow_the_bounded_write_batches() {
        let temp = TempDir::new().expect("创建临时目录");
        let mut sort_config = config(2, 2);
        sort_config.batch_rows = 100;
        sort_config.max_run_bytes = 8 * 1024;
        let input = (0u64..9)
            .rev()
            .map(|sequence| SortRow::from_sort_key(sort_key(sequence), vec![b'x'; 3_000]))
            .collect::<Vec<_>>();

        let outcome = external_sort(input, temp.path(), &sort_config).expect("执行外部排序");
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open(&outcome.output_path).expect("打开外排结果"),
        )
        .expect("读取外排结果元数据");
        let row_groups = builder.metadata().row_groups();

        assert!(row_groups.len() > 1);
        assert!(
            row_groups.iter().all(|row_group| {
                usize::try_from(row_group.num_rows())
                    .is_ok_and(|rows| rows <= outcome.metrics.peak_write_buffer_rows)
            }),
            "Parquet writer 不得跨越已经按字节上限刷新的内存批次重新累计大行组"
        );
    }

    #[test]
    fn run_size_and_fan_in_do_not_change_logical_digest() {
        let input: Vec<_> = [9u64, 1, 8, 2, 7, 3, 6, 4, 5, 0]
            .into_iter()
            .map(row)
            .collect();
        let first_temp = TempDir::new().expect("创建第一个临时目录");
        let second_temp = TempDir::new().expect("创建第二个临时目录");

        let first = external_sort(input.clone(), first_temp.path(), &config(2, 2))
            .expect("执行第一次外部排序");
        let second =
            external_sort(input, second_temp.path(), &config(4, 3)).expect("执行第二次外部排序");

        assert_eq!(first.logical_sha256, second.logical_sha256);
        assert_eq!(
            read_rows(&first.output_path, 1),
            read_rows(&second.output_path, 7)
        );
        assert_ne!(first.metrics.initial_runs, second.metrics.initial_runs);
        assert_ne!(first.metrics.merge_passes, second.metrics.merge_passes);
    }

    #[test]
    fn incremental_sort_pushes_the_same_bounded_runs_as_the_iterator_wrapper() {
        let input = [9u64, 1, 8, 2, 7, 3, 6, 4, 5, 0]
            .into_iter()
            .map(row)
            .collect::<Vec<_>>();
        let wrapper_temp = TempDir::new().expect("创建迭代器包装临时目录");
        let incremental_temp = TempDir::new().expect("创建增量临时目录");
        let sort_config = config(2, 2);

        let wrapper = external_sort(input.clone(), wrapper_temp.path(), &sort_config)
            .expect("执行迭代器包装排序");
        let mut incremental =
            ExternalSorter::new(incremental_temp.path(), &sort_config).expect("初始化增量排序器");
        for row in input.clone() {
            incremental.push(row).expect("增量写入排序行");
        }
        let incremental = incremental.finish().expect("完成增量排序");

        assert_eq!(wrapper.logical_sha256, incremental.logical_sha256);
        assert_eq!(wrapper.metrics, incremental.metrics);
        assert_eq!(
            read_rows(&wrapper.output_path, 1),
            read_rows(&incremental.output_path, 7)
        );
    }

    #[test]
    fn arrow_binary_order_matches_opaque_sort_key_bytes() {
        let input = [9u64, 0, 7, 2, 8, 1, 6].into_iter().map(row).collect();
        let sorted = sort_rows_with_arrow(input).expect("执行 Arrow 字节排序");

        assert_eq!(
            sorted
                .into_iter()
                .map(|row| String::from_utf8(row.payload().to_vec()).expect("载荷为 UTF-8"))
                .collect::<Vec<_>>(),
            [0u64, 1, 2, 6, 7, 8, 9].map(|sequence| format!("payload-{sequence:020}"))
        );
    }

    #[test]
    fn single_initial_run_writes_without_owned_writer_row_buffer() {
        let temp = TempDir::new().expect("创建单初始 run 临时目录");
        let mut single_run_config = config(8, 2);
        single_run_config.batch_rows = 1;
        let outcome = external_sort(
            [4u64, 1, 3, 0, 2].into_iter().map(row),
            temp.path(),
            &single_run_config,
        )
        .expect("执行单初始 run 外部排序");

        assert_eq!(
            read_rows(&outcome.output_path, 1),
            (0u64..5).map(row).collect::<Vec<_>>()
        );
        assert_eq!(outcome.metrics.input_rows, 5);
        assert_eq!(outcome.metrics.output_rows, 5);
        assert_eq!(outcome.metrics.initial_runs, 1);
        assert_eq!(outcome.metrics.merge_passes, 0);
        assert_eq!(
            (
                outcome.metrics.peak_write_buffer_rows,
                outcome.metrics.peak_write_buffer_bytes,
            ),
            (0, 0)
        );
    }

    #[test]
    fn sort_key_collision_fails_across_runs() {
        let temp = TempDir::new().expect("创建临时目录");
        let duplicate_key = sort_key(2);
        let input = vec![
            SortRow::from_sort_key(duplicate_key.clone(), b"first".to_vec()),
            row(1),
            SortRow::from_sort_key(duplicate_key, b"different payload".to_vec()),
        ];

        let error = external_sort(input, temp.path(), &config(1, 2)).expect_err("检测到冲突");

        assert!(matches!(error, SortError::SortKeyCollision { .. }));
        assert!(!temp.path().join(WORK_DIRECTORY).exists());
    }

    #[test]
    fn owned_row_buffer_limit_fails_closed() {
        let temp = TempDir::new().expect("创建行缓冲上限测试临时目录");
        let mut memory_config = config(2, 2);
        memory_config.max_run_rows = 1;
        memory_config.max_run_bytes = size_of::<SortRow>() + 1;
        memory_config.batch_rows = 1;
        let error =
            external_sort(vec![row(1)], temp.path(), &memory_config).expect_err("触发行缓冲上限");
        assert!(matches!(error, SortError::RowMemoryLimit { .. }));
    }

    #[test]
    fn descriptor_capacity_invalid_config_fails_before_work_directory_creation() {
        let temp = TempDir::new().expect("创建初始描述符容量负例临时目录");
        let mut descriptor_config = config(2, 2);
        descriptor_config.batch_rows = 1;
        descriptor_config.max_run_bytes = size_of::<SortRow>() * 2 - 1;
        let error = external_sort(vec![row(1)], temp.path(), &descriptor_config)
            .expect_err("初始描述符容量必须在创建工作目录前被拒绝");

        assert!(matches!(error, SortError::InvalidConfig(_)));
        assert!(!temp.path().join(WORK_DIRECTORY).exists());
    }

    #[test]
    fn temp_run_file_limit_fails_closed() {
        let temp = TempDir::new().expect("创建文件上限测试临时目录");
        let mut file_config = config(1, 2);
        file_config.max_temp_run_files = 2;
        let error = external_sort(vec![row(2), row(1)], temp.path(), &file_config)
            .expect_err("合并输出触发临时 run 文件上限");
        assert!(matches!(error, SortError::TempRunFileLimit { .. }));
    }

    #[test]
    fn open_file_limit_fails_closed() {
        let temp = TempDir::new().expect("创建文件句柄上限测试临时目录");
        let mut open_file_config = config(2, 2);
        open_file_config.max_open_files = 2;
        let error = external_sort(vec![row(2), row(1)], temp.path(), &open_file_config)
            .expect_err("触发文件句柄上限");

        assert!(matches!(error, SortError::OpenFileLimit { .. }));
    }

    #[test]
    fn temp_byte_limit_fails_closed() {
        let temp = TempDir::new().expect("创建字节上限测试临时目录");
        let mut byte_config = config(2, 2);
        byte_config.max_temp_bytes = 3;
        let error = external_sort(vec![row(1)], temp.path(), &byte_config)
            .expect_err("触发临时文件字节上限");

        assert!(matches!(error, SortError::TempByteLimit { .. }));
    }

    #[test]
    fn initial_pending_rows_flushes_before_outer_descriptor_capacity_exceeds_run_budget() {
        let first = row(1);
        let second = row(2);
        let descriptor_capacity = sort_row_inner_bytes(&second)
            .expect("第二行容量可以表示")
            .div_ceil(size_of::<SortRow>())
            .checked_add(2)
            .expect("描述符容量可以表示");
        let mut capacity_probe = Vec::with_capacity(descriptor_capacity);
        capacity_probe.push(first);
        let max_run_bytes = capacity_probe
            .capacity()
            .checked_mul(size_of::<SortRow>())
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].sort_key.capacity()))
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].payload.capacity()))
            .expect("测试边界可以表示");
        let first = capacity_probe.pop().expect("测试探针含第一行");
        let old_counted_is_within = first
            .resident_bytes()
            .checked_add(second.resident_bytes())
            .expect("旧计数可以表示")
            <= max_run_bytes;
        let actual_counted_exceeds = capacity_probe
            .capacity()
            .checked_mul(size_of::<SortRow>())
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&first)?))
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&second)?))
            .expect("实际计数可以表示")
            > max_run_bytes;
        let mut boundary_config = config(capacity_probe.capacity(), 2);
        boundary_config.batch_rows = 1;
        boundary_config.max_run_bytes = max_run_bytes;
        let temp = TempDir::new().expect("创建初始缓冲边界测试临时目录");

        assert!(capacity_probe.capacity() > capacity_probe.len());
        assert!(old_counted_is_within);
        assert!(actual_counted_exceeds);

        let outcome = external_sort(vec![first, second], temp.path(), &boundary_config)
            .expect("第二行前必须切分初始 run");

        assert_eq!(outcome.metrics.initial_runs, 2);
    }

    #[test]
    fn run_writer_buffer_flushes_before_outer_descriptor_capacity_exceeds_run_budget() {
        let first = row(1);
        let second = row(2);
        let descriptor_capacity = sort_row_inner_bytes(&second)
            .expect("第二行容量可以表示")
            .div_ceil(size_of::<SortRow>())
            .checked_add(2)
            .expect("描述符容量可以表示");
        let mut capacity_probe = Vec::with_capacity(descriptor_capacity);
        capacity_probe.push(first);
        let max_run_bytes = capacity_probe
            .capacity()
            .checked_mul(size_of::<SortRow>())
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].sort_key.capacity()))
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].payload.capacity()))
            .expect("测试边界可以表示");
        let first = capacity_probe.pop().expect("测试探针含第一行");
        let old_counted_is_within = first
            .resident_bytes()
            .checked_add(second.resident_bytes())
            .expect("旧计数可以表示")
            <= max_run_bytes;
        let actual_counted_exceeds = capacity_probe
            .capacity()
            .checked_mul(size_of::<SortRow>())
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&first)?))
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&second)?))
            .expect("实际计数可以表示")
            > max_run_bytes;
        let mut boundary_config = config(4, 2);
        boundary_config.batch_rows = capacity_probe.capacity();
        boundary_config.max_run_bytes = max_run_bytes;
        let temp = TempDir::new().expect("创建写缓冲边界测试临时目录");
        let mut metrics = SortMetrics::default();
        let mut writer = RunWriter::new(
            temp.path().join("writer-boundary.parquet"),
            0,
            0,
            &boundary_config,
            &mut metrics,
        )
        .expect("创建写缓冲");

        assert!(capacity_probe.capacity() > capacity_probe.len());
        assert!(old_counted_is_within);
        assert!(actual_counted_exceeds);

        writer.push(first, &mut metrics).expect("写入第一行");
        writer
            .push(second, &mut metrics)
            .expect("第二行前必须刷新写缓冲");

        assert_eq!(metrics.peak_write_buffer_rows, 1);
        let run = writer.finish(&mut metrics).expect("完成写缓冲");
        assert_eq!(read_rows(&run.path, 1), vec![row(1), row(2)]);
    }
}
