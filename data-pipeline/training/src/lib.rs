//! 训练数据管道的只读准入与确定性、有界外部排序能力。
//!
//! 输入边界读取调用者显式选择的清单；排序行结构只包含调用方提供的不透明字节排序键
//! 和不透明载荷。本 crate 不是通用存储层或查询层，也不解释排序键的业务语义。

pub mod canonical_run;
mod dpt_gate_a_cli;
pub mod finite_recording;
pub mod gate_a;
mod gate_a_publication;
mod gate_a_sim_adapter;
pub mod hidden_preparation;
pub mod historical_initialization;
pub mod historical_preparation_progress;
pub mod observation;
pub mod public_input;
pub mod public_online;
pub mod public_recording;
pub mod public_replay;
pub mod typed_raw;

pub use gate_a_publication::{
    GateAPublicationError, PublishedGateAFinal, build_gate_a_publication,
    gate_a_formal_canonical_config, gate_a_formal_output_config, open_published_gate_a_final,
    publish_gate_a_candidate,
};
pub use gate_a_sim_adapter::{
    ClosedExecutionMarketIndexBucketV1, GateASimulationReplayAdapter, ReplayIndexClosureV1,
};

/// 运行显式构建和发布命令入口。
pub fn run_dpt_gate_a_cli() {
    dpt_gate_a_cli::main();
}

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
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

const PUBLIC_WRITER_FINALIZE_ACKS: [&str; 7] = [
    "binance_aggtrade",
    "binance_depth_snapshot",
    "binance_depth_update",
    "polymarket_market",
    "polymarket_market_metadata",
    "polymarket_rtds_chainlink_btc_usd",
    "polymarket_rtds_chainlink_twap",
];

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

/// 已读取显式清单及其列出文件的直接源每日输入。
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
    file_path: PathBuf,
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

    pub fn file_path(&self) -> &Path {
        &self.file_path
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

/// 运行开始时读取的直接源输入集合；保存首次来源事实，不复制或锁定输入文件。
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
}

#[derive(Debug)]
pub enum InputSetSnapshotError {
    Admission {
        manifest_path: PathBuf,
        source: ManifestAdmissionValidationError,
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
        }
    }
}

impl std::error::Error for InputSetSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum ManifestAdmissionValidationError {
    Header(ManifestHeaderValidationError),
    Json(serde_json::Error),
    Io(io::Error),
    Parquet(ParquetError),
    AdmissionTerminalMismatch {
        closure_reason: String,
        coverage_state: String,
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
    FooterMetadataIdentityMismatch {
        path: PathBuf,
    },
    UnsupportedFileSchema {
        source_kind: String,
        schema_version: String,
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
            Self::Header(error) => write!(formatter, "每日清单文件集不合格：{error}"),
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
            Self::FooterMetadataIdentityMismatch { path } => write!(
                formatter,
                "Parquet footer 必需元数据与清单身份不一致：path={}",
                path.display()
            ),
            Self::UnsupportedFileSchema {
                source_kind,
                schema_version,
            } => write!(
                formatter,
                "不支持的输入来源/schema：{source_kind}/{schema_version}"
            ),
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
pub enum ManifestHeaderValidationError {
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
    InvalidCollectionDayCoverage,
}

impl fmt::Display for ManifestHeaderValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
struct DailyManifestDto {
    #[serde(flatten)]
    header: DailyManifestHeaderDto,
    files: Vec<DailyManifestAdmissionFileDto>,
    #[serde(default)]
    producer_barriers: Vec<String>,
    #[serde(default)]
    writer_finalize_acks: Vec<String>,
    #[serde(default)]
    source_counters: serde_json::Value,
}

#[derive(Deserialize)]
struct DailyManifestAdmissionFileDto {
    path: String,
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
    let bytes = read_daily_manifest(manifest_path)?;
    validate_daily_manifest_header_bytes(&bytes)
}

/// 校验显式直接源每日清单及其显式列出的最终化 Parquet，返回 DPT 自有不可变事实。
pub fn validate_direct_source_daily_manifest_admission(
    manifest_path: &Path,
) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError> {
    validate_daily_manifest_admission_with_public_inputs(manifest_path, false, None)
}

/// 新版公共输入显式接受原六来源材料及 V6/TWAP 材料，沿用每日清单 V3 的完整准入。
pub fn validate_public_source_daily_manifest_admission_v1(
    manifest_path: &Path,
) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError> {
    validate_daily_manifest_admission_with_public_inputs(manifest_path, true, None)
}

/// 完整准入显式选择的每日清单，并冻结 DPT 自有的不可变输入集合摘要。
pub fn freeze_direct_source_input_set(
    manifest_paths: &[PathBuf],
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError> {
    freeze_source_input_set(manifest_paths, false, None)
}
/// 新公共来源与旧直接源共用冻结摘要；允许原 V6/TWAP 清单准入。
pub fn freeze_public_source_input_set_v1(
    manifest_paths: &[PathBuf],
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError> {
    freeze_source_input_set(manifest_paths, true, None)
}
pub fn freeze_public_source_input_set_with_progress_v1(
    manifest_paths: &[PathBuf],
    progress: Option<&historical_preparation_progress::HistoricalPreparationNotifierV1<'_>>,
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError> {
    freeze_source_input_set(manifest_paths, true, progress)
}
fn freeze_source_input_set(
    manifest_paths: &[PathBuf],
    public_inputs: bool,
    progress: Option<&historical_preparation_progress::HistoricalPreparationNotifierV1<'_>>,
) -> Result<FrozenDirectSourceInputSetV1, InputSetSnapshotError> {
    let mut admitted_daily_inputs = Vec::with_capacity(manifest_paths.len());
    for manifest_path in manifest_paths {
        admitted_daily_inputs.push(
            validate_daily_manifest_admission_with_public_inputs(
                manifest_path,
                public_inputs,
                progress,
            )
            .map_err(|source| InputSetSnapshotError::Admission {
                manifest_path: manifest_path.clone(),
                source,
            })?,
        );
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
    input_manifest_bindings_sha256(
        inputs
            .iter()
            .map(|input| (input.utc_day(), input.manifest_sha256()))
            .collect(),
    )
}

fn input_manifest_bindings_sha256(mut facts: Vec<(&str, &str)>) -> String {
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

fn validate_daily_manifest_admission_with_public_inputs(
    manifest_path: &Path,
    public_inputs: bool,
    progress: Option<&historical_preparation_progress::HistoricalPreparationNotifierV1<'_>>,
) -> Result<ValidatedDirectSourceDailyInputV3, ManifestAdmissionValidationError> {
    let bytes =
        read_daily_manifest(manifest_path).map_err(ManifestAdmissionValidationError::Header)?;
    let DailyManifestDto {
        header,
        files: manifest_files,
        producer_barriers,
        writer_finalize_acks,
        source_counters,
    }: DailyManifestDto =
        serde_json::from_slice(&bytes).map_err(ManifestAdmissionValidationError::Json)?;
    let header =
        validate_daily_manifest_header(header).map_err(ManifestAdmissionValidationError::Header)?;
    let parent = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    if header.closure_reason() != "utc_day_barrier" || header.coverage_state() != "ended_known" {
        return Err(
            ManifestAdmissionValidationError::AdmissionTerminalMismatch {
                closure_reason: header.closure_reason().to_owned(),
                coverage_state: header.coverage_state().to_owned(),
            },
        );
    }
    if !matches_exact_evidence_set(&producer_barriers, &REQUIRED_PRODUCER_BARRIERS) {
        return Err(ManifestAdmissionValidationError::BarrierEvidenceMismatch {
            evidence_kind: "producer_barriers",
        });
    }
    let requires_twap_writer = if public_inputs {
        // 新采集器在零行日期也为全部来源保存计数条目，不用行情是否到达推断屏障参与者。
        source_counters
            .get("polymarket_rtds_chainlink_twap")
            .is_some()
            || manifest_files.iter().any(|file| {
                file.source_kind == "polymarket_rtds_chainlink_twap"
                    || (file.source_kind == "polymarket_market_metadata"
                        && file.schema_version == "polymarket_market_metadata_v6")
            })
            || writer_finalize_acks
                .iter()
                .any(|source| source == "polymarket_rtds_chainlink_twap")
    } else {
        false
    };
    let required_writer_acks = if requires_twap_writer {
        PUBLIC_WRITER_FINALIZE_ACKS.as_slice()
    } else {
        REQUIRED_WRITER_FINALIZE_ACKS.as_slice()
    };
    if !matches_exact_evidence_set(&writer_finalize_acks, required_writer_acks) {
        return Err(ManifestAdmissionValidationError::BarrierEvidenceMismatch {
            evidence_kind: "writer_finalize_acks",
        });
    }

    let mut files = Vec::with_capacity(manifest_files.len());
    for manifest_file in manifest_files {
        let schema_supported = if public_inputs {
            public_input::expected_public_schema(
                &manifest_file.source_kind,
                &manifest_file.schema_version,
            )
            .is_ok()
        } else {
            supported_schema_for_source_kind(&manifest_file.source_kind)
                == Some(manifest_file.schema_version.as_str())
        };
        if !schema_supported {
            return Err(ManifestAdmissionValidationError::UnsupportedFileSchema {
                source_kind: manifest_file.source_kind,
                schema_version: manifest_file.schema_version,
            });
        }
        let relative_path = PathBuf::from(&manifest_file.path);
        let path = parent.join(&relative_path);
        if !is_lowercase_sha256(&manifest_file.sha256) {
            return Err(ManifestAdmissionValidationError::InvalidManifestSha256 {
                path: relative_path.clone(),
                found: manifest_file.sha256.clone(),
            });
        }
        let inspection = inspect_verified_parquet(&path)?;
        if manifest_file.rows != inspection.footer_rows {
            return Err(ManifestAdmissionValidationError::FooterRowCountMismatch {
                path: relative_path.clone(),
                manifest_rows: manifest_file.rows,
                footer_rows: inspection.footer_rows,
            });
        }
        if manifest_file.bytes != inspection.file_bytes {
            return Err(ManifestAdmissionValidationError::FileByteCountMismatch {
                path: relative_path.clone(),
                manifest_bytes: manifest_file.bytes,
                file_bytes: inspection.file_bytes,
            });
        }
        if manifest_file.sha256 != inspection.sha256 {
            return Err(ManifestAdmissionValidationError::FileSha256Mismatch {
                path: relative_path.clone(),
                manifest_sha256: manifest_file.sha256,
                file_sha256: inspection.sha256,
            });
        }
        validate_parquet_footer_metadata(
            &relative_path,
            &inspection.footer_metadata,
            &manifest_file,
        )?;
        historical_preparation_progress::notify(
            progress,
            historical_preparation_progress::HistoricalPreparationNoticeV1::AdmissionFile {
                bytes: manifest_file.bytes,
                rows: manifest_file.rows,
            },
        );
        files.push(ValidatedDirectSourceParquetFileV3 {
            manifest_relative_path: relative_path.clone(),
            file_path: path,
            source_kind: manifest_file.source_kind,
            schema_version: manifest_file.schema_version,
            utc_hour: manifest_file.utc_hour,
            part: manifest_file.part,
            bytes: manifest_file.bytes,
            rows: manifest_file.rows,
            sha256: manifest_file.sha256,
        });
    }
    Ok(ValidatedDirectSourceDailyInputV3 {
        manifest_path: manifest_path.to_path_buf(),
        manifest_sha256: bytes_to_hex(&Sha256::digest(&bytes)),
        utc_day: header.utc_day().to_owned(),
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

    let _ = fadvise(&file, 0, None, Advice::DontNeed);
    inspection
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

fn read_daily_manifest(manifest_path: &Path) -> Result<Vec<u8>, ManifestHeaderValidationError> {
    let file = File::open(manifest_path).map_err(ManifestHeaderValidationError::Io)?;
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
    bytes: &[u8],
) -> Result<ValidatedDailyManifestHeaderV3, ManifestHeaderValidationError> {
    let header: DailyManifestHeaderDto =
        serde_json::from_slice(bytes).map_err(ManifestHeaderValidationError::Json)?;
    validate_daily_manifest_header(header)
}

fn validate_daily_manifest_header(
    header: DailyManifestHeaderDto,
) -> Result<ValidatedDailyManifestHeaderV3, ManifestHeaderValidationError> {
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

fn sort_row_buffer_bytes(rows: &Vec<SortRow>, inner_bytes: usize) -> Option<usize> {
    rows.capacity()
        .checked_mul(size_of::<SortRow>())
        .and_then(|bytes| bytes.checked_add(inner_bytes))
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

/// 实测资源用量与实际输入、输出行数。
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
    sorter.finish_with_digest()
}

pub(crate) struct ExternalSorter {
    work_directory: PathBuf,
    config: SortConfig,
    metrics: SortMetrics,
    runs: Vec<RunInfo>,
    pending_rows: Vec<SortRow>,
    pending_inner_bytes: usize,
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
            pending_inner_bytes: 0,
            current_temp_bytes: 0,
            active_temp_run_files: 0,
            next_file_id: 0,
            retain_work_directory: false,
        };
        let pending_base_bytes =
            sort_row_buffer_bytes(&sorter.pending_rows, sorter.pending_inner_bytes).ok_or(
                SortError::RowMemoryLimit {
                    observed: usize::MAX,
                    limit: config.max_run_bytes,
                },
            )?;
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
        let mut prospective_bytes =
            sort_row_buffer_bytes(&self.pending_rows, self.pending_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= self.config.max_run_bytes);
        let exceeds_bytes = prospective_bytes.is_none();
        if exceeds_rows || exceeds_bytes {
            if self.pending_rows.is_empty() {
                return Err(SortError::RowMemoryLimit {
                    observed: sort_row_buffer_bytes(&self.pending_rows, self.pending_inner_bytes)
                        .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                        .unwrap_or(usize::MAX),
                    limit: self.config.max_run_bytes,
                });
            }
            self.flush_pending(None)?;
            prospective_bytes = sort_row_buffer_bytes(&self.pending_rows, self.pending_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= self.config.max_run_bytes);
        }

        let observed = prospective_bytes.ok_or(SortError::RowMemoryLimit {
            observed: sort_row_buffer_bytes(&self.pending_rows, self.pending_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .unwrap_or(usize::MAX),
            limit: self.config.max_run_bytes,
        })?;
        // The prospective total above checked both the inner sum and descriptor allocation.
        self.pending_inner_bytes += sort_row_inner_bytes(&row).expect("validated row capacity");
        self.pending_rows.push(row);
        self.metrics.input_rows += 1;
        self.metrics.peak_run_rows = self.metrics.peak_run_rows.max(self.pending_rows.len());
        self.metrics.peak_run_bytes = self.metrics.peak_run_bytes.max(observed);
        Ok(())
    }

    fn flush_pending(&mut self, digest: Option<&mut Sha256>) -> Result<(), SortError> {
        let rows = std::mem::take(&mut self.pending_rows);
        self.pending_inner_bytes = 0;
        let run = flush_initial_run(
            rows,
            RunDestination {
                work_directory: &self.work_directory,
                pass: 0,
                file_id: self.next_file_id,
                current_temp_bytes: self.current_temp_bytes,
                active_temp_run_files: self.active_temp_run_files,
            },
            &self.config,
            &mut self.metrics,
            digest,
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
            sort_row_buffer_bytes(&self.pending_rows, self.pending_inner_bytes).ok_or(
                SortError::RowMemoryLimit {
                    observed: usize::MAX,
                    limit: self.config.max_run_bytes,
                },
            )?;
        if pending_base_bytes > self.config.max_run_bytes {
            return Err(SortError::RowMemoryLimit {
                observed: pending_base_bytes,
                limit: self.config.max_run_bytes,
            });
        }
        self.metrics.peak_run_bytes = self.metrics.peak_run_bytes.max(pending_base_bytes);
        Ok(())
    }

    pub(crate) fn finish_with_digest(self) -> Result<SortOutcome, SortError> {
        let mut digest = logical_hasher();
        let (output_path, metrics) = self.finish(Some(&mut digest))?;
        digest.update(metrics.output_rows.to_be_bytes());
        Ok(SortOutcome {
            output_path,
            logical_sha256: bytes_to_hex(&digest.finalize()),
            metrics,
        })
    }

    pub(crate) fn finish(
        mut self,
        mut digest: Option<&mut Sha256>,
    ) -> Result<(PathBuf, SortMetrics), SortError> {
        if !self.pending_rows.is_empty() {
            let final_initial_run = self.runs.is_empty();
            self.flush_pending(if final_initial_run {
                digest.as_deref_mut()
            } else {
                None
            })?;
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
            self.metrics.output_rows = empty_run.rows;
            self.retain_work_directory = true;
            return Ok((final_path, std::mem::take(&mut self.metrics)));
        }

        let mut runs = std::mem::take(&mut self.runs);
        let mut pass = 0usize;
        while runs.len() > 1 {
            pass += 1;
            self.metrics.merge_passes += 1;
            let mut next_runs = Vec::with_capacity(runs.len().div_ceil(self.config.merge_fan_in));
            let final_merge = runs.len() <= self.config.merge_fan_in;
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
                    if final_merge {
                        digest.as_deref_mut()
                    } else {
                        None
                    },
                )?;
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
        let final_path = self.work_directory.join("sorted.parquet");
        fs::rename(&final_run.path, &final_path)?;
        self.metrics.peak_open_files = self.metrics.peak_open_files.max(1);
        self.metrics.output_rows = final_run.rows;
        self.retain_work_directory = true;
        Ok((final_path, std::mem::take(&mut self.metrics)))
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
    mut digest: Option<&mut Sha256>,
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
        if let Some(digest) = digest.as_deref_mut() {
            for row in rows {
                hash_sort_row(digest, row)?;
            }
        }
    }
    writer.finish(metrics)
}

fn merge_group(
    group: &[RunInfo],
    destination: RunDestination<'_>,
    config: &SortConfig,
    metrics: &mut SortMetrics,
    mut digest: Option<&mut Sha256>,
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
        if let Some(digest) = digest.as_deref_mut() {
            hash_sort_row(digest, &entry.row)?;
        }
        writer.push(entry.row, metrics)?;

        if let Some(row) = cursors[run_slot].next_row()? {
            heap.push(HeapEntry { row, run_slot });
            metrics.peak_heap_entries = metrics.peak_heap_entries.max(heap.len());
        }
    }

    writer.finish(metrics)
}

fn sort_rows_with_arrow(mut rows: Vec<SortRow>) -> Result<Vec<SortRow>, SortError> {
    let keys = BinaryArray::from_iter_values(rows.iter().map(|row| row.sort_key.as_slice()));
    let indices = lexsort_to_indices(
        &[SortColumn {
            values: Arc::new(keys),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        }],
        None,
    )?;
    let indices = indices.values();
    let mut visited = vec![false; rows.len()];
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
    buffer_inner_bytes: usize,
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
            config.batch_rows,
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
            0,
        )
    }

    fn new_with_buffer(
        final_path: PathBuf,
        current_temp_bytes: u64,
        active_temp_run_files: usize,
        config: &SortConfig,
        metrics: &mut SortMetrics,
        buffer_capacity: usize,
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
        let buffer = Vec::with_capacity(buffer_capacity);
        let buffer_base_bytes =
            sort_row_buffer_bytes(&buffer, 0).ok_or(SortError::RowMemoryLimit {
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
            buffer_inner_bytes: 0,
            max_buffer_rows: config.batch_rows,
            max_buffer_bytes: config.max_run_bytes,
            rows: 0,
        })
    }

    fn push(&mut self, row: SortRow, metrics: &mut SortMetrics) -> Result<(), SortError> {
        let exceeds_rows = self.buffer.len() >= self.max_buffer_rows;
        let mut prospective_bytes = sort_row_buffer_bytes(&self.buffer, self.buffer_inner_bytes)
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
            .filter(|bytes| *bytes <= self.max_buffer_bytes);
        let exceeds_bytes = prospective_bytes.is_none();
        if exceeds_rows || exceeds_bytes {
            if self.buffer.is_empty() {
                return Err(SortError::RowMemoryLimit {
                    observed: sort_row_buffer_bytes(&self.buffer, self.buffer_inner_bytes)
                        .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                        .unwrap_or(usize::MAX),
                    limit: self.max_buffer_bytes,
                });
            }
            self.flush_buffer(metrics)?;
            prospective_bytes = sort_row_buffer_bytes(&self.buffer, self.buffer_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= self.max_buffer_bytes);
        }
        let observed = prospective_bytes.ok_or(SortError::RowMemoryLimit {
            observed: sort_row_buffer_bytes(&self.buffer, self.buffer_inner_bytes)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .unwrap_or(usize::MAX),
            limit: self.max_buffer_bytes,
        })?;
        self.buffer_inner_bytes += sort_row_inner_bytes(&row).expect("validated row capacity");
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
        self.buffer_inner_bytes = 0;
        let buffer_base_bytes = sort_row_buffer_bytes(&self.buffer, self.buffer_inner_bytes)
            .ok_or(SortError::RowMemoryLimit {
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
        let _ = fadvise(&self.inner, 0, None, Advice::DontNeed);
        Ok(())
    }
}

fn drop_file_cache(path: &Path) {
    if let Ok(file) = File::open(path) {
        let _ = fadvise(&file, 0, None, Advice::DontNeed);
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunRowPosition {
    pub row_group: usize,
    pub row_in_group: u64,
}

pub(crate) struct RunCursor {
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    next_row_group: usize,
    reader: Option<ParquetRecordBatchReader>,
    batch: std::vec::IntoIter<SortRow>,
    batch_rows: usize,
    max_batch_bytes: usize,
    row_in_group: u64,
}

impl RunCursor {
    pub(crate) fn open(
        path: &Path,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> Result<Self, SortError> {
        let file = File::open(path)?;
        #[cfg(test)]
        let metadata_started = public_replay::cost::decode_clock();
        let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
        #[cfg(test)]
        if public_replay::cost::decode_enabled() {
            public_replay::cost::decoded_metadata(
                metadata.metadata().memory_size(),
                metadata_started.unwrap().elapsed().as_nanos(),
            );
        }
        Ok(Self {
            path: path.to_path_buf(),
            metadata,
            next_row_group: 0,
            reader: None,
            batch: Vec::new().into_iter(),
            batch_rows,
            max_batch_bytes,
            row_in_group: 0,
        })
    }

    fn open_next_row_group(&mut self) -> Result<bool, SortError> {
        let row_group_count = self.metadata.metadata().num_row_groups();
        if self.next_row_group >= row_group_count {
            return Ok(false);
        }
        let row_group_index = self.next_row_group;
        self.next_row_group += 1;
        self.row_in_group = 0;
        #[cfg(test)]
        let group_started = public_replay::cost::decode_clock();
        self.reader = Some(
            ParquetRecordBatchReaderBuilder::new_with_metadata(
                File::open(&self.path)?,
                self.metadata.clone(),
            )
            .with_row_groups(vec![row_group_index])
            .with_batch_size(self.batch_rows)
            .build()?,
        );
        #[cfg(test)]
        public_replay::cost::decoded_row_group(group_started.map_or(0, |s| s.elapsed().as_nanos()));
        Ok(true)
    }

    pub(crate) fn next_row(&mut self) -> Result<Option<SortRow>, SortError> {
        Ok(self.next_row_with_position()?.map(|(row, _)| row))
    }

    // 仅改变本游标的物理起点；位置来自原reader已验证的同一稳定记录。
    pub(crate) fn start_row_group(&mut self, row_group: usize) {
        self.next_row_group = row_group;
        self.reader = None;
        self.batch = Vec::new().into_iter();
        self.row_in_group = 0;
    }

    pub(crate) fn next_row_with_position(
        &mut self,
    ) -> Result<Option<(SortRow, RunRowPosition)>, SortError> {
        loop {
            if let Some(row) = self.batch.next() {
                let position = RunRowPosition {
                    row_group: self.next_row_group - 1,
                    row_in_group: self.row_in_group,
                };
                self.row_in_group += 1;
                return Ok(Some((row, position)));
            }
            let Some(reader) = self.reader.as_mut() else {
                if self.open_next_row_group()? {
                    continue;
                }
                return Ok(None);
            };
            #[cfg(test)]
            let batch_started = public_replay::cost::decode_clock();
            match reader.next() {
                Some(batch) => {
                    let batch = batch?;
                    let rows = rows_from_batch(&batch)?;
                    let observed = rows
                        .iter()
                        .try_fold(0usize, |total, row| total.checked_add(row.resident_bytes()));
                    if observed.is_none_or(|bytes| bytes > self.max_batch_bytes) {
                        return Err(SortError::RowMemoryLimit {
                            observed: observed.unwrap_or(usize::MAX),
                            limit: self.max_batch_bytes,
                        });
                    }
                    #[cfg(test)]
                    if public_replay::cost::decode_enabled() {
                        public_replay::cost::decoded_batch(
                            rows.len(),
                            observed.unwrap(),
                            batch.get_array_memory_size(),
                            batch_started.unwrap().elapsed().as_nanos(),
                        );
                    }
                    self.batch = rows.into_iter();
                }
                None => {
                    self.reader = None;
                    drop_file_cache(&self.path);
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

fn logical_hasher() -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hasher
}

fn hash_sort_row(hasher: &mut Sha256, row: &SortRow) -> Result<(), SortError> {
    hash_length_prefixed(hasher, &row.sort_key)?;
    hash_length_prefixed(hasher, &row.payload)
}

#[cfg(test)]
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
    use tempfile::TempDir;

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
        validate_daily_manifest_header_bytes(&bytes)
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

    #[test]
    fn explicit_manifest_rejects_invalid_date_schema_and_missing_listed_file() {
        let temp = TempDir::new().unwrap();
        let path = complete_daily_manifest_fixture(temp.path());
        let original: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let mut invalid = original.clone();
        invalid["utc_day"] = json!("2026-02-30");
        write_manifest_value(&path, &invalid);
        assert!(matches!(
            validate_direct_source_daily_manifest_admission(&path),
            Err(ManifestAdmissionValidationError::Header(
                ManifestHeaderValidationError::InvalidCollectionDayCoverage
            ))
        ));
        let mut unsupported = original.clone();
        unsupported["files"][0]["schema_version"] = json!("unknown_schema");
        write_manifest_value(&path, &unsupported);
        assert!(matches!(
            validate_direct_source_daily_manifest_admission(&path),
            Err(ManifestAdmissionValidationError::UnsupportedFileSchema { .. })
        ));
        write_manifest_value(&path, &original);
        fs::remove_file(
            path.parent()
                .unwrap()
                .join(original["files"][0]["path"].as_str().unwrap()),
        )
        .unwrap();
        assert!(
            matches!(validate_direct_source_daily_manifest_admission(&path), Err(ManifestAdmissionValidationError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
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

        let error = validate_direct_source_daily_manifest_admission(&manifest_path)
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
            validate_direct_source_daily_manifest_admission(&manifest_path),
            Err(ManifestAdmissionValidationError::FooterRowCountMismatch { .. })
        ));

        let mut bytes = base.clone();
        bytes["files"][0]["bytes"] = json!(1);
        write_manifest_value(&manifest_path, &bytes);
        assert!(matches!(
            validate_direct_source_daily_manifest_admission(&manifest_path),
            Err(ManifestAdmissionValidationError::FileByteCountMismatch { .. })
        ));

        let mut sha256 = base;
        sha256["files"][0]["sha256"] = json!("0".repeat(64));
        write_manifest_value(&manifest_path, &sha256);
        assert!(matches!(
            validate_direct_source_daily_manifest_admission(&manifest_path),
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
            validate_direct_source_daily_manifest_admission(&manifest_path),
            Err(ManifestAdmissionValidationError::FooterMetadataIdentityMismatch { .. })
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
            validate_direct_source_daily_manifest_admission(&manifest_path),
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
            validate_direct_source_daily_manifest_admission(&manifest_path),
            Err(ManifestAdmissionValidationError::InvalidManifestSha256 { .. })
        ));
    }

    #[test]
    fn manifest_larger_than_10_mib_is_rejected_before_json_parsing() {
        let temp = TempDir::new().expect("创建清单字节上限临时目录");
        let root = temp.path().join("dataset-root");
        let manifest_path = daily_manifest_fixture(&root, &[]);
        fs::write(&manifest_path, vec![b'x'; 10 * 1024 * 1024 + 1])
            .expect("写入超过上限的非 JSON 清单");

        let error = validate_direct_source_daily_manifest_admission(&manifest_path)
            .expect_err("超过 10 MiB 的清单必须在 JSON 解析前失败");

        assert!(matches!(
            error,
            ManifestAdmissionValidationError::Header(
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

        let error = validate_direct_source_daily_manifest_admission(&manifest_path)
            .expect_err("files 非数组结构必须失败关闭");

        assert!(matches!(error, ManifestAdmissionValidationError::Json(_)));
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

    fn sort_key(sequence: u64) -> Vec<u8> {
        sequence.to_be_bytes().to_vec()
    }

    fn row(sequence: u64) -> SortRow {
        SortRow::from_sort_key(
            sort_key(sequence),
            format!("payload-{sequence:020}").into_bytes(),
        )
    }

    fn row_with_spare_capacity(sequence: u64) -> SortRow {
        let mut row = row(sequence);
        row.sort_key.reserve(64);
        row.payload.reserve(128);
        assert!(row.sort_key.capacity() > row.sort_key.len());
        assert!(row.payload.capacity() > row.payload.len());
        row
    }

    // Independent test oracle: scan actual owned allocations rather than the maintained sum.
    fn scanned_buffer_bytes(rows: &Vec<SortRow>) -> usize {
        rows.capacity() * size_of::<SortRow>()
            + rows
                .iter()
                .map(|row| row.sort_key.capacity() + row.payload.capacity())
                .sum::<usize>()
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
    fn final_write_digest_matches_the_published_rows_for_empty_single_and_merged_outputs() {
        for count in [0, 1, 17] {
            let temp = TempDir::new().unwrap();
            let outcome =
                external_sort((0..count).rev().map(row), temp.path(), &config(2, 2)).unwrap();
            let published = scan_logical_output(&outcome.output_path, 3, 4 * 1024).unwrap();
            assert_eq!(published, (outcome.logical_sha256, count));
        }
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
        let incremental = incremental.finish_with_digest().expect("完成增量排序");

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
        let first = row_with_spare_capacity(1);
        let second = row_with_spare_capacity(2);
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

        let mut sorter = ExternalSorter::new(temp.path(), &boundary_config).unwrap();
        sorter.push(first).unwrap();
        assert_eq!(
            sort_row_buffer_bytes(&sorter.pending_rows, sorter.pending_inner_bytes),
            Some(scanned_buffer_bytes(&sorter.pending_rows))
        );
        sorter.push(second).expect("第二行前必须切分初始 run");
        assert_eq!(sorter.runs.len(), 1);
        assert_eq!(
            sort_row_buffer_bytes(&sorter.pending_rows, sorter.pending_inner_bytes),
            Some(scanned_buffer_bytes(&sorter.pending_rows))
        );
        assert_eq!(sorter.metrics.peak_run_bytes, max_run_bytes);
        let outcome = sorter.finish_with_digest().unwrap();
        assert_eq!(read_rows(&outcome.output_path, 1), vec![row(1), row(2)]);

        assert_eq!(outcome.metrics.initial_runs, 2);
    }

    #[test]
    fn run_writer_buffer_flushes_before_outer_descriptor_capacity_exceeds_run_budget() {
        let first = row_with_spare_capacity(1);
        let second = row_with_spare_capacity(2);
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
        assert_eq!(
            sort_row_buffer_bytes(&writer.buffer, writer.buffer_inner_bytes),
            Some(scanned_buffer_bytes(&writer.buffer))
        );
        writer
            .push(second, &mut metrics)
            .expect("第二行前必须刷新写缓冲");

        assert_eq!(
            sort_row_buffer_bytes(&writer.buffer, writer.buffer_inner_bytes),
            Some(scanned_buffer_bytes(&writer.buffer))
        );
        assert_eq!(metrics.peak_write_buffer_bytes, max_run_bytes);
        assert_eq!(metrics.peak_write_buffer_rows, 1);
        let run = writer.finish(&mut metrics).expect("完成写缓冲");
        assert_eq!(read_rows(&run.path, 1), vec![row(1), row(2)]);
    }
    #[test]
    fn sorter_transferred_rows_are_not_counted_after_failed_run_write() {
        let temp = TempDir::new().unwrap();
        let mut cfg = config(2, 2);
        cfg.max_temp_bytes = 3;
        let mut sorter = ExternalSorter::new(temp.path(), &cfg).unwrap();
        sorter.push(row_with_spare_capacity(1)).unwrap();
        assert_eq!(
            sort_row_buffer_bytes(&sorter.pending_rows, sorter.pending_inner_bytes),
            Some(scanned_buffer_bytes(&sorter.pending_rows))
        );
        let error = sorter.flush_pending(None).unwrap_err();
        assert!(matches!(error, SortError::TempByteLimit { limit: 3 }));
        assert!(sorter.pending_rows.is_empty());
        assert_eq!(sorter.pending_rows.capacity(), 0);
        assert_eq!(sorter.pending_inner_bytes, 0);
        assert_eq!(
            sort_row_buffer_bytes(&sorter.pending_rows, sorter.pending_inner_bytes),
            Some(scanned_buffer_bytes(&sorter.pending_rows))
        );
    }

    #[test]
    fn run_writer_keeps_row_capacity_after_failed_flush() {
        let temp = TempDir::new().unwrap();
        let mut cfg = config(2, 2);
        cfg.max_temp_bytes = 4;
        let mut metrics = SortMetrics::default();
        let mut writer =
            RunWriter::new(temp.path().join("quota.parquet"), 0, 0, &cfg, &mut metrics).unwrap();
        writer
            .push(row_with_spare_capacity(1), &mut metrics)
            .unwrap();
        let before = scanned_buffer_bytes(&writer.buffer);
        let error = writer.flush_buffer(&mut metrics).unwrap_err();
        assert!(matches!(error, SortError::TempByteLimit { limit: 4 }));
        assert_eq!(writer.buffer.len(), 1);
        assert_eq!(writer.buffer[0], row(1));
        assert_eq!(scanned_buffer_bytes(&writer.buffer), before);
        assert_eq!(
            sort_row_buffer_bytes(&writer.buffer, writer.buffer_inner_bytes),
            Some(before)
        );
    }
}

#[cfg(test)]
mod dpt_observation_real_tests;
