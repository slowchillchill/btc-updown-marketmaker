//! 任务 2.4 使用的唯一生产计算编排路径。
//!
//! 本模块只把已冻结的直接源输入接入既有解码、归约与双时间投影，并返回同次运行的
//! 暂存摘要和资源计数。`TypedRawRow` 的 JSON 只在本次运行的临时工作区内承载外部
//! 排序载荷，不是持久 schema、规范事件身份或门 A 产物。正式事件存储、索引物化、
//! 运行清单和门 A 回执仍由任务 2.5 实现。

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use contracts::{CanonicalEventRecordV1, Sha256Hex};
use data_pipeline_core::reduction::{
    BinanceAggTradeReducer, BinanceDepthReducer, PolymarketMetadataInputV1,
    PolymarketMetadataReducer, PolymarketOrderbookInputV1, PolymarketReducer,
    PolymarketReductionBatch, PolymarketTerminalMerger, ReductionReportEntryV1,
    build_chainlink_reference_outcome_v1, encode_reduction_report_entry_v1,
};
use data_pipeline_core::typed_raw::{
    BinanceAggTradeRecord, MetadataResolutionEvidenceSource, MetadataResolutionPollResult,
    PolymarketMetadataRowKind, PolymarketOrderbookEvent, TypedRawRow, TypedRawSourceKind,
};
use data_pipeline_core::{
    ContractError, encode_canonical_event_record_v1, encode_event_time_index_entry,
    encode_visibility_index_entry, project_record_event_time_index_entry,
    project_record_visibility_index_entry, source_evidence_from_typed_raw,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::Date;
use time::macros::format_description;

use crate::typed_raw::{
    TypedRawParquetStream, TypedRawReadError, TypedRawReadScope,
    open_validated_typed_raw_parquet_in_scope,
};
use crate::{
    ExternalSorter, FrozenDirectSourceInputSetV1, InputSetSnapshotError, RunCursor, SortConfig,
    SortError, SortMetrics, SortRow, ValidatedDirectSourceParquetFileV3,
};

const WORK_DIRECTORY: &str = "dpt-canonical-run";
const SORT_ROOT: &str = "typed-raw-sort";
pub(crate) const RECORD_DIGEST_DOMAIN: &[u8] = b"data-pipeline/task-2.4/record-stream/v1\0";
pub(crate) const EVENT_TIME_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"data-pipeline/task-2.4/event-time-projection-stream/v1\0";
pub(crate) const VISIBILITY_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"data-pipeline/task-2.4/visibility-projection-stream/v1\0";
pub(crate) const REDUCTION_REPORT_DIGEST_DOMAIN: &[u8] =
    b"data-pipeline/task-2.4/reduction-report-stream/v1\0";

/// 单市场最小完整真实运行范围；时间区间使用左闭右开语义。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRunScope {
    condition_id: String,
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
}

impl CanonicalRunScope {
    pub fn new(
        condition_id: impl Into<String>,
        received_start_utc_ms: i64,
        received_end_utc_ms: i64,
    ) -> Result<Self, CanonicalRunError> {
        let scope = Self {
            condition_id: condition_id.into(),
            received_start_utc_ms,
            received_end_utc_ms,
        };
        scope.validate()?;
        Ok(scope)
    }

    pub fn condition_id(&self) -> &str {
        &self.condition_id
    }

    pub const fn received_start_utc_ms(&self) -> i64 {
        self.received_start_utc_ms
    }

    pub const fn received_end_utc_ms(&self) -> i64 {
        self.received_end_utc_ms
    }

    fn validate(&self) -> Result<(), CanonicalRunError> {
        if self.condition_id.trim().is_empty() {
            return Err(CanonicalRunError::InvalidConfig("condition_id 不得为空"));
        }
        if self.received_start_utc_ms <= 0 || self.received_end_utc_ms <= self.received_start_utc_ms
        {
            return Err(CanonicalRunError::InvalidConfig(
                "接收时间范围必须为有效的左闭右开正数区间",
            ));
        }
        Ok(())
    }

    fn contains_received_time(&self, received_at_utc_ms: i64) -> bool {
        self.received_start_utc_ms <= received_at_utc_ms
            && received_at_utc_ms < self.received_end_utc_ms
    }
}

/// 任务 2.4 的显式有界运行配置。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRunConfig {
    pub decode_batch_rows: usize,
    /// `RecordBatch::get_array_memory_size()` 报告的批次物理数组大小上限；不是分配器、RSS 或进程总内存。
    pub max_decode_batch_bytes: usize,
    pub max_temporary_row_bytes: usize,
    pub max_epoch_count_per_source: usize,
    pub max_pending_ws_terminals: usize,
    pub max_retained_reducer_entries: usize,
    pub canonical_rule_version: String,
    pub sort: SortConfig,
}

impl CanonicalRunConfig {
    fn validate(&self) -> Result<(), CanonicalRunError> {
        if self.decode_batch_rows == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "decode_batch_rows 必须为正数",
            ));
        }
        if self.max_decode_batch_bytes == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "max_decode_batch_bytes 必须为正数",
            ));
        }
        if self.max_temporary_row_bytes == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "max_temporary_row_bytes 必须为正数",
            ));
        }
        if self.max_epoch_count_per_source == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "max_epoch_count_per_source 必须为正数",
            ));
        }
        if self.max_pending_ws_terminals == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "max_pending_ws_terminals 必须为正数",
            ));
        }
        if self.max_retained_reducer_entries == 0 {
            return Err(CanonicalRunError::InvalidConfig(
                "max_retained_reducer_entries 必须为正数",
            ));
        }
        if self.canonical_rule_version.trim().is_empty() {
            return Err(CanonicalRunError::InvalidConfig(
                "canonical_rule_version 不得为空",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRowCounts {
    pub polymarket_orderbook: u64,
    pub polymarket_market_metadata: u64,
    pub polymarket_rtds_chainlink_btc_usd: u64,
    pub binance_depth_snapshot: u64,
    pub binance_depth_update: u64,
    pub binance_aggtrade: u64,
}

impl SourceRowCounts {
    fn increment(&mut self, source: TypedRawSourceKind) {
        let counter = match source {
            TypedRawSourceKind::PolymarketOrderbook => &mut self.polymarket_orderbook,
            TypedRawSourceKind::PolymarketMarketMetadata => &mut self.polymarket_market_metadata,
            TypedRawSourceKind::PolymarketRtdsChainlinkBtcUsd => {
                &mut self.polymarket_rtds_chainlink_btc_usd
            }
            TypedRawSourceKind::BinanceDepthSnapshot => &mut self.binance_depth_snapshot,
            TypedRawSourceKind::BinanceDepthUpdate => &mut self.binance_depth_update,
            TypedRawSourceKind::BinanceAggTrade => &mut self.binance_aggtrade,
        };
        *counter = counter.saturating_add(1);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRunInputFile {
    pub utc_day: String,
    pub manifest_relative_path: String,
    pub source_kind: String,
    pub schema_version: String,
    pub utc_hour: String,
    pub part: u32,
    pub manifest_sha256: String,
    pub file_sha256: String,
    pub bytes: u64,
    pub rows: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRunMetrics {
    pub prepass_decode_batches: u64,
    pub prepass_decoded_rows: u64,
    pub production_decode_batches: u64,
    pub production_decoded_rows: u64,
    pub peak_decode_batch_rows: usize,
    /// `RecordBatch::get_array_memory_size()` 报告的批次物理数组大小峰值。
    pub peak_decode_batch_bytes: usize,
    pub peak_selected_decode_batch_rows: usize,
    pub peak_temporary_row_bytes: usize,
    pub selected_rows: SourceRowCounts,
    pub polymarket_epoch_count: usize,
    pub binance_depth_epoch_count: usize,
    pub peak_open_input_files: usize,
    pub peak_business_queue_rows: usize,
    pub peak_hot_markets: usize,
    pub peak_pending_ws_terminals: usize,
    pub peak_retained_reducer_entries: usize,
    pub parallelism: usize,
    pub canonical_records: u64,
    pub execution_market_events: u64,
    pub reference_events: u64,
    pub event_time_projections: u64,
    pub visibility_projections: u64,
    pub reduction_reports: u64,
    pub peak_canonical_record_bytes: usize,
    pub peak_projection_bytes: usize,
    pub peak_reduction_report_bytes: usize,
    pub sort: SortMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRunDigests {
    pub canonical_records_sha256: String,
    pub event_time_projections_sha256: String,
    pub visibility_projections_sha256: String,
    pub reduction_reports_sha256: String,
}

/// 仅属于同次运行的暂存结果；它不是正式门 A 产物或回执。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRunStaging {
    pub workspace_path: PathBuf,
    pub temporary_sorted_input_path: PathBuf,
    pub input_set_identity_sha256: String,
    pub input_manifest_set_sha256: String,
    pub scope: CanonicalRunScope,
    pub config: CanonicalRunConfig,
    pub selected_files: Vec<CanonicalRunInputFile>,
    pub temporary_sort_logical_sha256: String,
    pub digests: CanonicalRunDigests,
    pub metrics: CanonicalRunMetrics,
}

#[derive(Debug)]
pub enum CanonicalRunError {
    InvalidConfig(&'static str),
    WorkspaceExists(PathBuf),
    InvalidPartitionTime {
        utc_day: String,
        utc_hour: String,
    },
    UnsupportedSource(String),
    EpochLimit {
        source_kind: &'static str,
        limit: usize,
    },
    AmbiguousEpochOrder {
        source_kind: &'static str,
        left_epoch: String,
        right_epoch: String,
    },
    MissingEpochRank {
        source_kind: &'static str,
        epoch: String,
    },
    TemporaryRowTooLarge {
        observed_at_least: usize,
        limit: usize,
    },
    MissingIdentitySnapshot,
    MetadataBeforeIdentitySnapshot,
    DuplicateWsTerminalClaim {
        epoch: String,
        collector_admission_sequence: u64,
    },
    PendingWsTerminalLimit {
        limit: usize,
    },
    RetainedReducerStateLimit {
        observed: usize,
        limit: usize,
    },
    MissingWsTerminalMetadata {
        epoch: String,
        collector_admission_sequence: u64,
    },
    UnpairedWsTerminalMetadata {
        count: usize,
    },
    InvalidTemporaryRow(String),
    CounterOverflow(&'static str),
    Io(io::Error),
    InputChanged(InputSetSnapshotError),
    TypedRaw(TypedRawReadError),
    Sort(SortError),
    Contract(ContractError),
    Json(serde_json::Error),
}

impl fmt::Display for CanonicalRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "规范运行配置无效：{message}"),
            Self::WorkspaceExists(path) => {
                write!(formatter, "规范运行工作目录已存在：{}", path.display())
            }
            Self::InvalidPartitionTime { utc_day, utc_hour } => write!(
                formatter,
                "无法解析已准入文件分区时间：utc_day={utc_day}, utc_hour={utc_hour}"
            ),
            Self::UnsupportedSource(source) => write!(formatter, "不支持的直接源：{source}"),
            Self::EpochLimit { source_kind, limit } => {
                write!(formatter, "{source_kind} 的 epoch 数量超过运行上限 {limit}")
            }
            Self::AmbiguousEpochOrder {
                source_kind,
                left_epoch,
                right_epoch,
            } => write!(
                formatter,
                "{source_kind} 的两个 epoch 缺少可区分的首个真实采集坐标：{left_epoch}, {right_epoch}"
            ),
            Self::MissingEpochRank { source_kind, epoch } => {
                write!(formatter, "{source_kind} epoch 未建立顺序：{epoch}")
            }
            Self::TemporaryRowTooLarge {
                observed_at_least,
                limit,
            } => write!(
                formatter,
                "临时排序行至少需要 {observed_at_least} 字节，超过上限 {limit}"
            ),
            Self::MissingIdentitySnapshot => write!(formatter, "运行范围缺少完整身份快照"),
            Self::MetadataBeforeIdentitySnapshot => {
                write!(formatter, "身份快照建立前出现同市场其他 metadata 行")
            }
            Self::DuplicateWsTerminalClaim {
                epoch,
                collector_admission_sequence,
            } => write!(
                formatter,
                "重复的 WS 终态 metadata claimant：epoch={epoch}, sequence={collector_admission_sequence}"
            ),
            Self::PendingWsTerminalLimit { limit } => {
                write!(formatter, "待配对 WS 终态数量超过上限 {limit}")
            }
            Self::RetainedReducerStateLimit { observed, limit } => write!(
                formatter,
                "归约器保留集合条目数 {observed} 超过上限 {limit}"
            ),
            Self::MissingWsTerminalMetadata {
                epoch,
                collector_admission_sequence,
            } => write!(
                formatter,
                "MarketResolved 缺少对应 metadata：epoch={epoch}, sequence={collector_admission_sequence}"
            ),
            Self::UnpairedWsTerminalMetadata { count } => {
                write!(formatter, "运行结束仍有 {count} 条 WS 终态 metadata 未配对")
            }
            Self::InvalidTemporaryRow(message) => {
                write!(formatter, "临时排序载荷无效：{message}")
            }
            Self::CounterOverflow(counter) => write!(formatter, "运行计数器溢出：{counter}"),
            Self::Io(error) => write!(formatter, "I/O 错误：{error}"),
            Self::InputChanged(error) => write!(formatter, "冻结输入在运行期间变化：{error}"),
            Self::TypedRaw(error) => write!(formatter, "typed_raw 读取失败：{error}"),
            Self::Sort(error) => write!(formatter, "外部排序失败：{error}"),
            Self::Contract(error) => write!(formatter, "规范核心拒绝输入：{error}"),
            Self::Json(error) => write!(formatter, "临时 JSON 编解码失败：{error}"),
        }
    }
}

impl std::error::Error for CanonicalRunError {}

impl From<io::Error> for CanonicalRunError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<InputSetSnapshotError> for CanonicalRunError {
    fn from(error: InputSetSnapshotError) -> Self {
        Self::InputChanged(error)
    }
}

impl From<TypedRawReadError> for CanonicalRunError {
    fn from(error: TypedRawReadError) -> Self {
        Self::TypedRaw(error)
    }
}

impl From<SortError> for CanonicalRunError {
    fn from(error: SortError) -> Self {
        Self::Sort(error)
    }
}

impl From<ContractError> for CanonicalRunError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<serde_json::Error> for CanonicalRunError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

struct WorkspaceGuard {
    path: PathBuf,
    retain: bool,
}

impl WorkspaceGuard {
    fn create(work_root: &Path) -> Result<Self, CanonicalRunError> {
        if !work_root.is_dir() {
            return Err(CanonicalRunError::InvalidConfig(
                "work_root 必须是已经存在的目录",
            ));
        }
        let path = work_root.join(WORK_DIRECTORY);
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self {
                path,
                retain: false,
            }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(CanonicalRunError::WorkspaceExists(path))
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CaptureCoordinate {
    admitted_at_utc_ms: i64,
    received_at_utc_ms: i64,
    collector_admission_sequence: u64,
}

#[derive(Default)]
struct EpochRanks {
    polymarket: BTreeMap<String, u32>,
    binance_depth: BTreeMap<String, u32>,
}

struct RunFile<'a> {
    utc_day: &'a str,
    manifest_sha256: &'a str,
    file: &'a ValidatedDirectSourceParquetFileV3,
}

#[derive(Serialize, Deserialize)]
struct TemporaryInputRow {
    input_manifest_sha256: String,
    input_file_sha256: String,
    row: TypedRawRow,
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedVecWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("临时 JSON 长度溢出"));
        };
        if required > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("临时 JSON 超过字节上限"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_bounded_json<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, CanonicalRunError> {
    let mut writer = BoundedVecWriter::new(limit);
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if writer.exceeded {
            return Err(CanonicalRunError::TemporaryRowTooLarge {
                observed_at_least: limit.saturating_add(1),
                limit,
            });
        }
        return Err(error.into());
    }
    Ok(writer.bytes)
}

fn checked_increment(counter: &mut u64, name: &'static str) -> Result<(), CanonicalRunError> {
    *counter = counter
        .checked_add(1)
        .ok_or(CanonicalRunError::CounterOverflow(name))?;
    Ok(())
}

fn observe_decode_batch(
    stream: &TypedRawParquetStream,
    selected_rows: usize,
    metrics: &mut CanonicalRunMetrics,
) {
    let physical_rows = stream.last_physical_batch_rows();
    let physical_bytes = stream.last_physical_batch_bytes();
    metrics.peak_decode_batch_rows = metrics.peak_decode_batch_rows.max(physical_rows);
    metrics.peak_decode_batch_bytes = metrics.peak_decode_batch_bytes.max(physical_bytes);
    metrics.peak_selected_decode_batch_rows =
        metrics.peak_selected_decode_batch_rows.max(selected_rows);
}

fn append_ordered_i64(key: &mut Vec<u8>, value: i64) {
    key.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
}

fn append_u64(key: &mut Vec<u8>, value: u64) {
    key.extend_from_slice(&value.to_be_bytes());
}

fn append_bytes(key: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CanonicalRunError> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| CanonicalRunError::InvalidConfig("排序键字段长度必须能表示为 u32"))?;
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(bytes);
    Ok(())
}

fn source_domain(source: &str) -> Result<u8, CanonicalRunError> {
    match source {
        "polymarket_market_metadata" => Ok(0),
        "polymarket_market" => Ok(1),
        "polymarket_rtds_chainlink_btc_usd" => Ok(2),
        "binance_depth_snapshot" | "binance_depth_update" => Ok(3),
        "binance_aggtrade" => Ok(4),
        other => Err(CanonicalRunError::UnsupportedSource(other.to_owned())),
    }
}

fn file_hour_interval(utc_day: &str, utc_hour: &str) -> Result<(i64, i64), CanonicalRunError> {
    let date = Date::parse(utc_day, format_description!("[year]-[month]-[day]")).map_err(|_| {
        CanonicalRunError::InvalidPartitionTime {
            utc_day: utc_day.to_owned(),
            utc_hour: utc_hour.to_owned(),
        }
    })?;
    let (hour_day, hour) =
        utc_hour
            .rsplit_once('T')
            .ok_or_else(|| CanonicalRunError::InvalidPartitionTime {
                utc_day: utc_day.to_owned(),
                utc_hour: utc_hour.to_owned(),
            })?;
    if hour_day != utc_day {
        return Err(CanonicalRunError::InvalidPartitionTime {
            utc_day: utc_day.to_owned(),
            utc_hour: utc_hour.to_owned(),
        });
    }
    let hour = hour
        .parse::<u8>()
        .ok()
        .filter(|hour| *hour < 24 && utc_hour.ends_with(&format!("T{hour:02}")))
        .ok_or_else(|| CanonicalRunError::InvalidPartitionTime {
            utc_day: utc_day.to_owned(),
            utc_hour: utc_hour.to_owned(),
        })?;
    let start_seconds = date
        .with_hms(hour, 0, 0)
        .map_err(|_| CanonicalRunError::InvalidPartitionTime {
            utc_day: utc_day.to_owned(),
            utc_hour: utc_hour.to_owned(),
        })?
        .assume_utc()
        .unix_timestamp();
    let start = start_seconds
        .checked_mul(1_000)
        .ok_or(CanonicalRunError::InvalidConfig("文件分区时间换算溢出"))?;
    let end = start
        .checked_add(3_600_000)
        .ok_or(CanonicalRunError::InvalidConfig("文件分区时间换算溢出"))?;
    Ok((start, end))
}

fn select_run_files<'a>(
    input: &'a FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
) -> Result<Vec<RunFile<'a>>, CanonicalRunError> {
    let mut files = Vec::new();
    for daily in input.admitted_daily_inputs() {
        for file in daily.files() {
            source_domain(file.source_kind())?;
            let (hour_start, hour_end) = file_hour_interval(daily.utc_day(), file.utc_hour())?;
            if hour_start < scope.received_end_utc_ms && scope.received_start_utc_ms < hour_end {
                files.push(RunFile {
                    utc_day: daily.utc_day(),
                    manifest_sha256: daily.manifest_sha256(),
                    file,
                });
            }
        }
    }
    files.sort_by(|left, right| {
        source_domain(left.file.source_kind())
            .expect("已经验证来源")
            .cmp(&source_domain(right.file.source_kind()).expect("已经验证来源"))
            .then_with(|| left.utc_day.cmp(right.utc_day))
            .then_with(|| left.file.utc_hour().cmp(right.file.utc_hour()))
            .then_with(|| left.file.part().cmp(&right.file.part()))
            .then_with(|| left.file.sha256().cmp(right.file.sha256()))
    });
    Ok(files)
}

fn row_received_at(row: &TypedRawRow) -> i64 {
    match row {
        TypedRawRow::PolymarketOrderbook(row) => row.timestamp_received,
        TypedRawRow::PolymarketMarketMetadata(row) => row.timestamp_received,
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => row.timestamp_received,
        TypedRawRow::BinanceDepthSnapshot(row) => row.timestamp_received,
        TypedRawRow::BinanceDepthUpdate(row) => row.timestamp_received,
        TypedRawRow::BinanceAggTrade(row) => row.timestamp_received,
    }
}

fn row_in_scope(row: &TypedRawRow, scope: &CanonicalRunScope) -> bool {
    if !scope.contains_received_time(row_received_at(row)) {
        return false;
    }
    match row {
        TypedRawRow::PolymarketOrderbook(row) => row.market == scope.condition_id,
        TypedRawRow::PolymarketMarketMetadata(row) => row.condition_id == scope.condition_id,
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_)
        | TypedRawRow::BinanceDepthSnapshot(_)
        | TypedRawRow::BinanceDepthUpdate(_)
        | TypedRawRow::BinanceAggTrade(_) => true,
    }
}

fn typed_raw_read_scope(scope: &CanonicalRunScope) -> TypedRawReadScope {
    TypedRawReadScope::new(
        scope.received_start_utc_ms,
        scope.received_end_utc_ms,
        scope.condition_id.clone(),
    )
}

fn update_epoch_start(
    starts: &mut BTreeMap<String, CaptureCoordinate>,
    epoch: &str,
    coordinate: CaptureCoordinate,
    source_kind: &'static str,
    limit: usize,
) -> Result<(), CanonicalRunError> {
    if !starts.contains_key(epoch) && starts.len() >= limit {
        return Err(CanonicalRunError::EpochLimit { source_kind, limit });
    }
    starts
        .entry(epoch.to_owned())
        .and_modify(|current| *current = (*current).min(coordinate))
        .or_insert(coordinate);
    Ok(())
}

fn rank_epochs(
    starts: BTreeMap<String, CaptureCoordinate>,
    source_kind: &'static str,
) -> Result<BTreeMap<String, u32>, CanonicalRunError> {
    let mut ordered = starts.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|item| item.1);
    for pair in ordered.windows(2) {
        if pair[0].1 == pair[1].1 {
            return Err(CanonicalRunError::AmbiguousEpochOrder {
                source_kind,
                left_epoch: pair[0].0.clone(),
                right_epoch: pair[1].0.clone(),
            });
        }
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(rank, (epoch, _))| {
            let rank = u32::try_from(rank).map_err(|_| CanonicalRunError::EpochLimit {
                source_kind,
                limit: u32::MAX as usize,
            })?;
            Ok((epoch, rank))
        })
        .collect()
}

fn discover_epoch_ranks(
    files: &[RunFile<'_>],
    scope: &CanonicalRunScope,
    config: &CanonicalRunConfig,
    metrics: &mut CanonicalRunMetrics,
) -> Result<EpochRanks, CanonicalRunError> {
    let mut polymarket = BTreeMap::new();
    let mut binance_depth = BTreeMap::new();
    let read_scope = typed_raw_read_scope(scope);

    for selected in files.iter().filter(|selected| {
        matches!(
            selected.file.source_kind(),
            "polymarket_market" | "binance_depth_snapshot" | "binance_depth_update"
        )
    }) {
        metrics.peak_open_input_files = metrics.peak_open_input_files.max(1);
        let mut stream = open_validated_typed_raw_parquet_in_scope(
            selected.file,
            config.decode_batch_rows,
            &read_scope,
            config.max_decode_batch_bytes,
        )?;
        while let Some(batch) = stream.next() {
            let rows = batch?;
            observe_decode_batch(&stream, rows.len(), metrics);
            checked_increment(
                &mut metrics.prepass_decode_batches,
                "prepass_decode_batches",
            )?;
            metrics.prepass_decoded_rows = metrics
                .prepass_decoded_rows
                .checked_add(
                    u64::try_from(rows.len())
                        .map_err(|_| CanonicalRunError::CounterOverflow("prepass_decoded_rows"))?,
                )
                .ok_or(CanonicalRunError::CounterOverflow("prepass_decoded_rows"))?;
            for row in rows {
                if !row_in_scope(&row, scope) {
                    continue;
                }
                match row {
                    TypedRawRow::PolymarketOrderbook(row) => update_epoch_start(
                        &mut polymarket,
                        &row.source_sequence_epoch_id,
                        CaptureCoordinate {
                            admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                            received_at_utc_ms: row.timestamp_received,
                            collector_admission_sequence: row.collector_admission_sequence,
                        },
                        "polymarket_market",
                        config.max_epoch_count_per_source,
                    )?,
                    TypedRawRow::BinanceDepthSnapshot(row) => update_epoch_start(
                        &mut binance_depth,
                        &row.source_sequence_epoch_id,
                        CaptureCoordinate {
                            admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                            received_at_utc_ms: row.timestamp_received,
                            collector_admission_sequence: row.collector_admission_sequence,
                        },
                        "binance_depth",
                        config.max_epoch_count_per_source,
                    )?,
                    TypedRawRow::BinanceDepthUpdate(row) => update_epoch_start(
                        &mut binance_depth,
                        &row.source_sequence_epoch_id,
                        CaptureCoordinate {
                            admitted_at_utc_ms: row.collector_admission_time_utc_ms,
                            received_at_utc_ms: row.timestamp_received,
                            collector_admission_sequence: row.collector_admission_sequence,
                        },
                        "binance_depth",
                        config.max_epoch_count_per_source,
                    )?,
                    _ => {
                        return Err(CanonicalRunError::InvalidTemporaryRow(
                            "epoch 预扫描遇到不属于目标来源的行".to_owned(),
                        ));
                    }
                }
            }
        }
    }

    let polymarket = rank_epochs(polymarket, "polymarket_market")?;
    let binance_depth = rank_epochs(binance_depth, "binance_depth")?;
    metrics.polymarket_epoch_count = polymarket.len();
    metrics.binance_depth_epoch_count = binance_depth.len();
    Ok(EpochRanks {
        polymarket,
        binance_depth,
    })
}

fn epoch_rank(
    ranks: &BTreeMap<String, u32>,
    source_kind: &'static str,
    epoch: &str,
) -> Result<u32, CanonicalRunError> {
    ranks
        .get(epoch)
        .copied()
        .ok_or_else(|| CanonicalRunError::MissingEpochRank {
            source_kind,
            epoch: epoch.to_owned(),
        })
}

fn temporary_sort_key(
    row: &TypedRawRow,
    evidence_source_time: Option<i64>,
    ranks: &EpochRanks,
    row_digest: &[u8],
    manifest_sha256: &str,
    file_sha256: &str,
    file_row_index: u64,
) -> Result<Vec<u8>, CanonicalRunError> {
    let mut key = Vec::with_capacity(256);
    match row {
        TypedRawRow::PolymarketMarketMetadata(row) => {
            key.push(0);
            append_ordered_i64(&mut key, row.timestamp_received);
            append_ordered_i64(&mut key, row.collector_admission_time_utc_ms);
            append_u64(&mut key, row.collector_admission_sequence);
        }
        TypedRawRow::PolymarketOrderbook(row) => {
            key.push(1);
            key.extend_from_slice(
                &epoch_rank(
                    &ranks.polymarket,
                    "polymarket_market",
                    &row.source_sequence_epoch_id,
                )?
                .to_be_bytes(),
            );
            let source_time = evidence_source_time.ok_or_else(|| {
                CanonicalRunError::InvalidTemporaryRow(
                    "Polymarket orderbook 缺少数据源事件时间".to_owned(),
                )
            })?;
            append_ordered_i64(&mut key, source_time);
            append_u64(&mut key, row.collector_admission_sequence);
        }
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => {
            key.push(2);
            append_ordered_i64(&mut key, row.payload_timestamp);
            append_ordered_i64(&mut key, row.timestamp_received);
        }
        TypedRawRow::BinanceDepthSnapshot(row) => {
            key.push(3);
            key.extend_from_slice(
                &epoch_rank(
                    &ranks.binance_depth,
                    "binance_depth",
                    &row.source_sequence_epoch_id,
                )?
                .to_be_bytes(),
            );
            append_u64(&mut key, row.collector_admission_sequence);
            key.push(0);
        }
        TypedRawRow::BinanceDepthUpdate(row) => {
            key.push(3);
            key.extend_from_slice(
                &epoch_rank(
                    &ranks.binance_depth,
                    "binance_depth",
                    &row.source_sequence_epoch_id,
                )?
                .to_be_bytes(),
            );
            append_u64(&mut key, row.collector_admission_sequence);
            key.push(1);
        }
        TypedRawRow::BinanceAggTrade(row) => {
            key.push(4);
            match &row.record {
                BinanceAggTradeRecord::Trade {
                    aggregate_trade_id, ..
                } => {
                    key.push(0);
                    append_u64(&mut key, *aggregate_trade_id);
                }
                BinanceAggTradeRecord::InvalidInput { .. } => {
                    key.push(1);
                    append_ordered_i64(&mut key, row.timestamp_received);
                    append_u64(&mut key, row.collector_admission_sequence);
                }
            }
        }
    }

    // `row_digest` 先稳定排列同一业务坐标上的冲突载荷；最后三个字段只在载荷完全
    // 相同时区分可合法进入 reducer 的重投。它们不得替代前面的业务顺序坐标。
    append_bytes(&mut key, row_digest)?;
    append_bytes(&mut key, manifest_sha256.as_bytes())?;
    append_bytes(&mut key, file_sha256.as_bytes())?;
    append_u64(&mut key, file_row_index);
    Ok(key)
}

fn stage_sorted_input(
    files: &[RunFile<'_>],
    scope: &CanonicalRunScope,
    config: &CanonicalRunConfig,
    ranks: &EpochRanks,
    sort_root: &Path,
    metrics: &mut CanonicalRunMetrics,
) -> Result<crate::SortOutcome, CanonicalRunError> {
    let mut sorter = ExternalSorter::new(sort_root, &config.sort)?;
    let read_scope = typed_raw_read_scope(scope);

    for selected in files {
        metrics.peak_open_input_files = metrics.peak_open_input_files.max(1);
        let mut stream = open_validated_typed_raw_parquet_in_scope(
            selected.file,
            config.decode_batch_rows,
            &read_scope,
            config.max_decode_batch_bytes,
        )?;
        let manifest_sha256 = Sha256Hex(selected.manifest_sha256.to_owned());
        let file_sha256 = Sha256Hex(selected.file.sha256().to_owned());
        let mut file_row_index = 0_u64;

        while let Some(batch) = stream.next() {
            let rows = batch?;
            observe_decode_batch(&stream, rows.len(), metrics);
            checked_increment(
                &mut metrics.production_decode_batches,
                "production_decode_batches",
            )?;
            metrics.production_decoded_rows =
                metrics
                    .production_decoded_rows
                    .checked_add(u64::try_from(rows.len()).map_err(|_| {
                        CanonicalRunError::CounterOverflow("production_decoded_rows")
                    })?)
                    .ok_or(CanonicalRunError::CounterOverflow(
                        "production_decoded_rows",
                    ))?;
            for row in rows {
                let current_row_index = file_row_index;
                file_row_index = file_row_index
                    .checked_add(1)
                    .ok_or(CanonicalRunError::CounterOverflow("file_row_index"))?;
                if !row_in_scope(&row, scope) {
                    continue;
                }

                let evidence =
                    source_evidence_from_typed_raw(&row, &manifest_sha256, &file_sha256)?;
                let row_bytes = encode_bounded_json(&row, config.max_temporary_row_bytes)?;
                let row_digest = Sha256::digest(&row_bytes);
                metrics.peak_temporary_row_bytes =
                    metrics.peak_temporary_row_bytes.max(row_bytes.len());
                drop(row_bytes);

                let temporary = TemporaryInputRow {
                    input_manifest_sha256: selected.manifest_sha256.to_owned(),
                    input_file_sha256: selected.file.sha256().to_owned(),
                    row,
                };
                let payload = encode_bounded_json(&temporary, config.max_temporary_row_bytes)?;
                metrics.peak_temporary_row_bytes =
                    metrics.peak_temporary_row_bytes.max(payload.len());
                metrics.selected_rows.increment(temporary.row.source_kind());
                let sort_key = temporary_sort_key(
                    &temporary.row,
                    evidence.source_event_at_utc_ms,
                    ranks,
                    row_digest.as_slice(),
                    selected.manifest_sha256,
                    selected.file.sha256(),
                    current_row_index,
                )?;
                sorter.push(SortRow::from_sort_key(sort_key, payload))?;
            }
        }
    }

    sorter.finish().map_err(Into::into)
}

pub(crate) struct StreamDigest {
    hasher: Sha256,
    count: u64,
}

impl StreamDigest {
    pub(crate) fn new(domain: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        Self { hasher, count: 0 }
    }

    pub(crate) fn push(
        &mut self,
        bytes: &[u8],
        counter: &'static str,
    ) -> Result<(), CanonicalRunError> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| CanonicalRunError::CounterOverflow(counter))?;
        self.hasher.update(length.to_be_bytes());
        self.hasher.update(bytes);
        checked_increment(&mut self.count, counter)
    }

    pub(crate) fn finish(mut self) -> String {
        self.hasher.update(self.count.to_be_bytes());
        format!("{:x}", self.hasher.finalize())
    }
}

pub(crate) trait CanonicalRunCapture {
    fn capture_record(&mut self, emission_sequence: u64, encoded: &[u8]) -> io::Result<()>;

    fn capture_report(&mut self, emission_sequence: u64, encoded: &[u8]) -> io::Result<()>;
}

struct OutputSink<'a> {
    capture: Option<&'a mut dyn CanonicalRunCapture>,
    records: StreamDigest,
    event_time_projections: StreamDigest,
    visibility_projections: StreamDigest,
    reports: StreamDigest,
    canonical_records: u64,
    execution_market_events: u64,
    reference_events: u64,
    event_time_projection_count: u64,
    visibility_projection_count: u64,
    reduction_report_count: u64,
    peak_canonical_record_bytes: usize,
    peak_projection_bytes: usize,
    peak_reduction_report_bytes: usize,
}

impl<'a> OutputSink<'a> {
    fn new(capture: Option<&'a mut dyn CanonicalRunCapture>) -> Self {
        Self {
            capture,
            records: StreamDigest::new(RECORD_DIGEST_DOMAIN),
            event_time_projections: StreamDigest::new(EVENT_TIME_PROJECTION_DIGEST_DOMAIN),
            visibility_projections: StreamDigest::new(VISIBILITY_PROJECTION_DIGEST_DOMAIN),
            reports: StreamDigest::new(REDUCTION_REPORT_DIGEST_DOMAIN),
            canonical_records: 0,
            execution_market_events: 0,
            reference_events: 0,
            event_time_projection_count: 0,
            visibility_projection_count: 0,
            reduction_report_count: 0,
            peak_canonical_record_bytes: 0,
            peak_projection_bytes: 0,
            peak_reduction_report_bytes: 0,
        }
    }

    fn push_record(&mut self, record: CanonicalEventRecordV1) -> Result<(), CanonicalRunError> {
        let is_execution = matches!(record, CanonicalEventRecordV1::ExecutionMarketEvent(_));
        let record_encoded = encode_canonical_event_record_v1(&record)?;
        self.peak_canonical_record_bytes =
            self.peak_canonical_record_bytes.max(record_encoded.len());
        self.records.push(&record_encoded, "canonical_records")?;

        if let Some(entry) = project_record_event_time_index_entry(&record)? {
            let encoded = encode_event_time_index_entry(&entry)?;
            self.peak_projection_bytes = self.peak_projection_bytes.max(encoded.len());
            self.event_time_projections
                .push(&encoded, "event_time_projections")?;
            checked_increment(
                &mut self.event_time_projection_count,
                "event_time_projections",
            )?;
        }

        let visibility = project_record_visibility_index_entry(&record)?;
        let encoded = encode_visibility_index_entry(&visibility)?;
        self.peak_projection_bytes = self.peak_projection_bytes.max(encoded.len());
        self.visibility_projections
            .push(&encoded, "visibility_projections")?;
        checked_increment(
            &mut self.visibility_projection_count,
            "visibility_projections",
        )?;
        if let Some(capture) = self.capture.as_mut() {
            capture.capture_record(self.canonical_records, &record_encoded)?;
        }
        checked_increment(&mut self.canonical_records, "canonical_records")?;
        if is_execution {
            checked_increment(&mut self.execution_market_events, "execution_market_events")?;
        } else {
            checked_increment(&mut self.reference_events, "reference_events")?;
        }
        Ok(())
    }

    fn push_execution_events<I>(&mut self, events: I) -> Result<(), CanonicalRunError>
    where
        I: IntoIterator<Item = contracts::ExecutionMarketEventV3>,
    {
        for event in events {
            self.push_record(CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(
                event,
            )))?;
        }
        Ok(())
    }

    fn push_reference_event(
        &mut self,
        event: contracts::CanonicalReferenceEventV1,
    ) -> Result<(), CanonicalRunError> {
        self.push_record(CanonicalEventRecordV1::ReferenceEvent(Box::new(event)))
    }

    fn push_report(&mut self, report: ReductionReportEntryV1) -> Result<(), CanonicalRunError> {
        let encoded = encode_reduction_report_entry_v1(&report)?;
        self.peak_reduction_report_bytes = self.peak_reduction_report_bytes.max(encoded.len());
        self.reports.push(&encoded, "reduction_reports")?;
        if let Some(capture) = self.capture.as_mut() {
            capture.capture_report(self.reduction_report_count, &encoded)?;
        }
        checked_increment(&mut self.reduction_report_count, "reduction_reports")
    }

    fn push_reports<I>(&mut self, reports: I) -> Result<(), CanonicalRunError>
    where
        I: IntoIterator<Item = ReductionReportEntryV1>,
    {
        for report in reports {
            self.push_report(report)?;
        }
        Ok(())
    }

    fn push_polymarket_batch(
        &mut self,
        batch: PolymarketReductionBatch,
    ) -> Result<(), CanonicalRunError> {
        self.push_execution_events(batch.events)?;
        self.push_reports(batch.reports)
    }

    fn finish(self, metrics: &mut CanonicalRunMetrics) -> CanonicalRunDigests {
        metrics.canonical_records = self.canonical_records;
        metrics.execution_market_events = self.execution_market_events;
        metrics.reference_events = self.reference_events;
        metrics.event_time_projections = self.event_time_projection_count;
        metrics.visibility_projections = self.visibility_projection_count;
        metrics.reduction_reports = self.reduction_report_count;
        metrics.peak_canonical_record_bytes = self.peak_canonical_record_bytes;
        metrics.peak_projection_bytes = self.peak_projection_bytes;
        metrics.peak_reduction_report_bytes = self.peak_reduction_report_bytes;
        CanonicalRunDigests {
            canonical_records_sha256: self.records.finish(),
            event_time_projections_sha256: self.event_time_projections.finish(),
            visibility_projections_sha256: self.visibility_projections.finish(),
            reduction_reports_sha256: self.reports.finish(),
        }
    }
}

struct PolymarketRunState {
    metadata: Option<PolymarketMetadataReducer>,
    orderbook: Option<PolymarketReducer>,
    terminal: Option<PolymarketTerminalMerger>,
    pending_ws_terminals: BTreeMap<(String, u64), PolymarketMetadataInputV1>,
}

fn validate_pending_ws_terminal_insert(
    duplicate: bool,
    current_len: usize,
    limit: usize,
    epoch: &str,
    collector_admission_sequence: u64,
) -> Result<(), CanonicalRunError> {
    if duplicate {
        return Err(CanonicalRunError::DuplicateWsTerminalClaim {
            epoch: epoch.to_owned(),
            collector_admission_sequence,
        });
    }
    if current_len >= limit {
        return Err(CanonicalRunError::PendingWsTerminalLimit { limit });
    }
    Ok(())
}

impl PolymarketRunState {
    fn new() -> Self {
        Self {
            metadata: None,
            orderbook: None,
            terminal: None,
            pending_ws_terminals: BTreeMap::new(),
        }
    }

    fn retained_state_entries(&self) -> usize {
        self.pending_ws_terminals.len().saturating_add(
            self.orderbook
                .as_ref()
                .map_or(0, PolymarketReducer::retained_state_entries),
        )
    }

    fn initialize(
        &mut self,
        input: PolymarketMetadataInputV1,
        canonical_rule_version: &str,
        sink: &mut OutputSink,
        metrics: &mut CanonicalRunMetrics,
    ) -> Result<(), CanonicalRunError> {
        let (metadata, context, initial) = PolymarketMetadataReducer::from_identity_snapshot(
            input,
            canonical_rule_version.to_owned(),
        )?;
        let orderbook = PolymarketReducer::new(context.clone(), canonical_rule_version.to_owned())?;
        let terminal = PolymarketTerminalMerger::new(context, canonical_rule_version.to_owned())?;
        self.metadata = Some(metadata);
        self.orderbook = Some(orderbook);
        self.terminal = Some(terminal);
        metrics.peak_hot_markets = metrics.peak_hot_markets.max(1);
        sink.push_polymarket_batch(initial)
    }

    fn handle_metadata(
        &mut self,
        input: PolymarketMetadataInputV1,
        canonical_rule_version: &str,
        max_pending_ws_terminals: usize,
        sink: &mut OutputSink,
        metrics: &mut CanonicalRunMetrics,
    ) -> Result<(), CanonicalRunError> {
        if self.metadata.is_none() {
            if input.row.metadata_row_kind != PolymarketMetadataRowKind::MetadataSnapshot {
                return Err(CanonicalRunError::MetadataBeforeIdentitySnapshot);
            }
            return self.initialize(input, canonical_rule_version, sink, metrics);
        }

        if input.row.metadata_row_kind == PolymarketMetadataRowKind::ResolutionTerminalWs {
            let epoch = input
                .row
                .ws_claimant_source_sequence_epoch_id
                .clone()
                .ok_or_else(|| {
                    CanonicalRunError::InvalidTemporaryRow(
                        "ResolutionTerminalWs 缺少 claimant epoch".to_owned(),
                    )
                })?;
            let collector_admission_sequence = input
                .row
                .ws_claimant_collector_admission_sequence
                .ok_or_else(|| {
                CanonicalRunError::InvalidTemporaryRow(
                    "ResolutionTerminalWs 缺少 claimant sequence".to_owned(),
                )
            })?;
            let key = (epoch.clone(), collector_admission_sequence);
            validate_pending_ws_terminal_insert(
                self.pending_ws_terminals.contains_key(&key),
                self.pending_ws_terminals.len(),
                max_pending_ws_terminals,
                &epoch,
                collector_admission_sequence,
            )?;
            let previous = self.pending_ws_terminals.insert(key, input);
            debug_assert!(previous.is_none());
            metrics.peak_pending_ws_terminals = metrics
                .peak_pending_ws_terminals
                .max(self.pending_ws_terminals.len());
            return Ok(());
        }

        let is_gamma_terminal = input.row.metadata_row_kind
            == PolymarketMetadataRowKind::ResolutionPollAttempt
            && input.row.resolution_poll_result == Some(MetadataResolutionPollResult::Terminal)
            && input.row.terminal_predicate_passed == Some(true)
            && input.row.resolution_evidence_source
                == Some(MetadataResolutionEvidenceSource::GammaBySlug);
        if is_gamma_terminal {
            let batch = self
                .terminal
                .as_mut()
                .expect("身份初始化同时建立终态合并器")
                .merge_gamma_terminal(input)?;
            return sink.push_polymarket_batch(batch);
        }

        let batch = self
            .metadata
            .as_mut()
            .expect("身份已经初始化")
            .reduce_metadata(input)?;
        sink.push_polymarket_batch(batch)
    }

    fn handle_orderbook(
        &mut self,
        input: PolymarketOrderbookInputV1,
        sink: &mut OutputSink,
    ) -> Result<(), CanonicalRunError> {
        if self.orderbook.is_none() {
            return Err(CanonicalRunError::MissingIdentitySnapshot);
        }
        if matches!(
            input.row.event,
            PolymarketOrderbookEvent::MarketResolved { .. }
        ) {
            let key = (
                input.row.source_sequence_epoch_id.clone(),
                input.row.collector_admission_sequence,
            );
            let metadata = self.pending_ws_terminals.remove(&key).ok_or_else(|| {
                CanonicalRunError::MissingWsTerminalMetadata {
                    epoch: key.0.clone(),
                    collector_admission_sequence: key.1,
                }
            })?;
            let batch = self
                .terminal
                .as_mut()
                .expect("身份初始化同时建立终态合并器")
                .merge_ws_terminal(metadata, input)?;
            return sink.push_polymarket_batch(batch);
        }

        let batch = self
            .orderbook
            .as_mut()
            .expect("身份已经初始化")
            .reduce_orderbook_group(vec![input])?;
        sink.push_polymarket_batch(batch)
    }
}

fn process_sorted_input(
    sorted_path: &Path,
    config: &CanonicalRunConfig,
    metrics: &mut CanonicalRunMetrics,
    capture: Option<&mut dyn CanonicalRunCapture>,
) -> Result<CanonicalRunDigests, CanonicalRunError> {
    let mut cursor = RunCursor::open(
        sorted_path,
        config.sort.batch_rows,
        config.sort.max_run_bytes,
    )?;
    let mut sink = OutputSink::new(capture);
    let mut polymarket = PolymarketRunState::new();
    let mut binance_depth = BinanceDepthReducer::new(config.canonical_rule_version.clone())?;
    let mut binance_aggtrade = BinanceAggTradeReducer::new(config.canonical_rule_version.clone())?;
    metrics.peak_business_queue_rows = 1;
    metrics.parallelism = 1;

    while let Some(sorted) = cursor.next_row()? {
        let temporary: TemporaryInputRow = serde_json::from_slice(sorted.payload())?;
        temporary
            .row
            .validate()
            .map_err(|error| CanonicalRunError::InvalidTemporaryRow(error.to_string()))?;
        let manifest_sha256 = Sha256Hex(temporary.input_manifest_sha256);
        let file_sha256 = Sha256Hex(temporary.input_file_sha256);

        match temporary.row {
            TypedRawRow::PolymarketMarketMetadata(row) => {
                let typed = TypedRawRow::PolymarketMarketMetadata(row.clone());
                let evidence =
                    source_evidence_from_typed_raw(&typed, &manifest_sha256, &file_sha256)?;
                polymarket.handle_metadata(
                    PolymarketMetadataInputV1 {
                        row: *row,
                        evidence,
                    },
                    &config.canonical_rule_version,
                    config.max_pending_ws_terminals,
                    &mut sink,
                    metrics,
                )?;
            }
            TypedRawRow::PolymarketOrderbook(row) => {
                let typed = TypedRawRow::PolymarketOrderbook(row.clone());
                let evidence =
                    source_evidence_from_typed_raw(&typed, &manifest_sha256, &file_sha256)?;
                polymarket
                    .handle_orderbook(PolymarketOrderbookInputV1 { row, evidence }, &mut sink)?;
            }
            TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row) => {
                // 资格和失败原因继续由唯一核心 builder 给出，不在编排层创建新质量语义。
                let outcome = build_chainlink_reference_outcome_v1(
                    &row,
                    &manifest_sha256,
                    &file_sha256,
                    &config.canonical_rule_version,
                )?;
                if let Some(event) = outcome.event {
                    sink.push_reference_event(event)?;
                }
                sink.push_report(outcome.report)?;
            }
            TypedRawRow::BinanceDepthSnapshot(row) => {
                let outcome = binance_depth.apply_snapshot(&row, &manifest_sha256, &file_sha256)?;
                if let Some(event) = outcome.event {
                    sink.push_reference_event(event)?;
                }
                sink.push_report(outcome.report)?;
            }
            TypedRawRow::BinanceDepthUpdate(row) => {
                let outcome = binance_depth.apply_update(&row, &manifest_sha256, &file_sha256)?;
                if let Some(event) = outcome.event {
                    sink.push_reference_event(event)?;
                }
                sink.push_report(outcome.report)?;
            }
            TypedRawRow::BinanceAggTrade(row) => {
                let outcome = binance_aggtrade.apply(&row, &manifest_sha256, &file_sha256)?;
                if let Some(event) = outcome.event {
                    sink.push_reference_event(event)?;
                }
                sink.push_report(outcome.report)?;
            }
        }

        let retained_state_entries = polymarket
            .retained_state_entries()
            .saturating_add(binance_depth.retained_state_entries());
        metrics.peak_retained_reducer_entries = metrics
            .peak_retained_reducer_entries
            .max(retained_state_entries);
        if retained_state_entries > config.max_retained_reducer_entries {
            return Err(CanonicalRunError::RetainedReducerStateLimit {
                observed: retained_state_entries,
                limit: config.max_retained_reducer_entries,
            });
        }
    }

    if polymarket.metadata.is_none() {
        return Err(CanonicalRunError::MissingIdentitySnapshot);
    }
    if !polymarket.pending_ws_terminals.is_empty() {
        return Err(CanonicalRunError::UnpairedWsTerminalMetadata {
            count: polymarket.pending_ws_terminals.len(),
        });
    }
    Ok(sink.finish(metrics))
}

/// 把已冻结真实输入接入同一解码、排序、归约和双时间投影路径。
///
/// 成功结果只属于任务 2.4 暂存；调用方必须使用独立工作根，并负责在证据记录后清理。
fn reduce_frozen_input(
    input: &FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    work_root: &Path,
    config: &CanonicalRunConfig,
    capture: Option<&mut dyn CanonicalRunCapture>,
) -> Result<CanonicalRunStaging, CanonicalRunError> {
    scope.validate()?;
    config.validate()?;
    let mut workspace = WorkspaceGuard::create(work_root)?;
    let sort_root = workspace.path.join(SORT_ROOT);
    fs::create_dir(&sort_root)?;

    let files = select_run_files(input, scope)?;
    let selected_files = files
        .iter()
        .map(|selected| CanonicalRunInputFile {
            utc_day: selected.utc_day.to_owned(),
            manifest_relative_path: selected
                .file
                .manifest_relative_path()
                .to_str()
                .expect("已验证 manifest 相对路径源自 UTF-8 JSON")
                .to_owned(),
            source_kind: selected.file.source_kind().to_owned(),
            schema_version: selected.file.schema_version().to_owned(),
            utc_hour: selected.file.utc_hour().to_owned(),
            part: selected.file.part(),
            manifest_sha256: selected.manifest_sha256.to_owned(),
            file_sha256: selected.file.sha256().to_owned(),
            bytes: selected.file.bytes(),
            rows: selected.file.rows(),
        })
        .collect::<Vec<_>>();

    let mut metrics = CanonicalRunMetrics::default();
    let ranks = discover_epoch_ranks(&files, scope, config, &mut metrics)?;
    let sorted = stage_sorted_input(&files, scope, config, &ranks, &sort_root, &mut metrics)?;
    metrics.sort = sorted.metrics.clone();
    let digests = process_sorted_input(&sorted.output_path, config, &mut metrics, capture)?;

    // 冻结发生在调用方构造 `input` 时；运行结束重新完整准入，证明处理中输入未改变。
    input.verify_unchanged()?;

    let staging = CanonicalRunStaging {
        workspace_path: workspace.path.clone(),
        temporary_sorted_input_path: sorted.output_path,
        input_set_identity_sha256: input.input_set_identity_sha256().to_owned(),
        input_manifest_set_sha256: input.input_manifest_set_sha256().to_owned(),
        scope: scope.clone(),
        config: config.clone(),
        selected_files,
        temporary_sort_logical_sha256: sorted.logical_sha256,
        digests,
        metrics,
    };
    workspace.retain = true;
    Ok(staging)
}

/// 把已冻结真实输入接入同一解码、排序、归约和双时间投影路径。
///
/// 成功结果只属于任务 2.4 暂存；调用方必须使用独立工作根，并负责在证据记录后清理。
pub fn reduce_frozen_input_to_staging(
    input: &FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    work_root: &Path,
    config: &CanonicalRunConfig,
) -> Result<CanonicalRunStaging, CanonicalRunError> {
    reduce_frozen_input(input, scope, work_root, config, None)
}

pub(crate) fn reduce_frozen_input_with_capture(
    input: &FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    work_root: &Path,
    config: &CanonicalRunConfig,
    capture: &mut dyn CanonicalRunCapture,
) -> Result<CanonicalRunStaging, CanonicalRunError> {
    reduce_frozen_input(input, scope, work_root, config, Some(capture))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_utc_hour_format_maps_to_its_exact_interval() {
        let (start, end) = file_hour_interval("2026-08-04", "2026-08-04T18")
            .expect("已冻结 utc_hour 格式必须可解析");

        assert_eq!(start, 1_785_866_400_000);
        assert_eq!(end, 1_785_870_000_000);
        assert!(file_hour_interval("2026-08-04", "18").is_err());
        assert!(file_hour_interval("2026-08-04", "2026-08-05T18").is_err());
    }

    #[test]
    fn duplicate_ws_terminal_claim_precedes_the_capacity_error() {
        let error = validate_pending_ws_terminal_insert(true, 1, 1, "epoch-a", 42)
            .expect_err("重复 claimant 不会增加待配对集合，必须先报告重复");

        assert!(matches!(
            error,
            CanonicalRunError::DuplicateWsTerminalClaim {
                epoch,
                collector_admission_sequence: 42,
            } if epoch == "epoch-a"
        ));
    }
}
