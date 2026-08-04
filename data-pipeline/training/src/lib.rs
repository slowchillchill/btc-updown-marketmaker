//! 阶段 0 的确定性、有界外部排序能力试验。
//!
//! 本 crate 有意把行结构固定为 `data-pipeline-core` 编码的完整全序键字节和
//! 不透明载荷；它不是通用存储层或查询层。

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use contracts::TotalOrderKeyV1;
use data_pipeline_core::{ContractError, total_order_key_bytes};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

const WORK_DIRECTORY: &str = "dpt-external-sort";
const KEY_COLUMN: &str = "complete_order_key";
const PAYLOAD_COLUMN: &str = "payload";
const DIGEST_DOMAIN: &[u8] = b"data-pipeline-training/external-sort/logical-v1\0";

/// 能力试验使用的固定行结构。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortRow {
    complete_order_key: Vec<u8>,
    payload: Vec<u8>,
}

impl SortRow {
    /// 使用唯一权威的全序编码创建一行。
    pub fn from_total_order_key(
        complete_order_key: &TotalOrderKeyV1,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self, ContractError> {
        Ok(Self::from_encoded(
            total_order_key_bytes(complete_order_key)?,
            payload.into(),
        ))
    }

    pub fn complete_order_key_bytes(&self) -> &[u8] {
        &self.complete_order_key
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn from_encoded(complete_order_key: Vec<u8>, payload: Vec<u8>) -> Self {
        Self {
            complete_order_key,
            payload,
        }
    }

    fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.complete_order_key.capacity())
            .saturating_add(self.payload.capacity())
    }
}

fn sort_row_inner_bytes(row: &SortRow) -> Option<usize> {
    row.complete_order_key
        .capacity()
        .checked_add(row.payload.capacity())
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
    CompleteKeyCollision { key_hex: String },
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
            Self::CompleteKeyCollision { key_hex } => {
                write!(formatter, "完整全序键冲突：{key_hex}")
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

/// 使用有界 Parquet run，按完整字节键排序不透明行。
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
    validate_config(config)?;

    let work_directory = temp_root.join(WORK_DIRECTORY);
    match fs::create_dir(&work_directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(SortError::WorkspaceExists(work_directory));
        }
        Err(error) => return Err(error.into()),
    }

    let result = external_sort_inner(rows, &work_directory, config);
    if result.is_err() {
        let _ = fs::remove_dir_all(&work_directory);
    }
    result
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

fn external_sort_inner<I>(
    rows: I,
    work_directory: &Path,
    config: &SortConfig,
) -> Result<SortOutcome, SortError>
where
    I: IntoIterator<Item = SortRow>,
{
    let mut metrics = SortMetrics::default();
    let mut runs = Vec::new();
    let mut pending_rows = Vec::with_capacity(config.max_run_rows);
    let pending_base_bytes =
        sort_row_buffer_bytes(&pending_rows).ok_or(SortError::RowMemoryLimit {
            observed: usize::MAX,
            limit: config.max_run_bytes,
        })?;
    if pending_base_bytes > config.max_run_bytes {
        return Err(SortError::RowMemoryLimit {
            observed: pending_base_bytes,
            limit: config.max_run_bytes,
        });
    }
    metrics.peak_run_bytes = pending_base_bytes;
    let mut current_temp_bytes = 0u64;
    let mut active_temp_run_files = 0usize;
    let mut next_file_id = 0usize;

    for row in rows {
        let exceeds_rows = pending_rows.len() >= config.max_run_rows;
        let mut prospective_bytes = sort_row_buffer_bytes(&pending_rows)
            .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
            .filter(|bytes| *bytes <= config.max_run_bytes);
        let exceeds_bytes = prospective_bytes.is_none();
        if exceeds_rows || exceeds_bytes {
            if pending_rows.is_empty() {
                return Err(SortError::RowMemoryLimit {
                    observed: sort_row_buffer_bytes(&pending_rows)
                        .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                        .unwrap_or(usize::MAX),
                    limit: config.max_run_bytes,
                });
            }
            let run = flush_initial_run(
                std::mem::take(&mut pending_rows),
                RunDestination {
                    work_directory,
                    pass: 0,
                    file_id: next_file_id,
                    current_temp_bytes,
                    active_temp_run_files,
                },
                config,
                &mut metrics,
            )?;
            next_file_id += 1;
            current_temp_bytes =
                current_temp_bytes
                    .checked_add(run.bytes)
                    .ok_or(SortError::TempByteLimit {
                        limit: config.max_temp_bytes,
                    })?;
            active_temp_run_files += 1;
            metrics.peak_temp_run_files = metrics.peak_temp_run_files.max(active_temp_run_files);
            metrics.peak_temp_bytes = metrics.peak_temp_bytes.max(current_temp_bytes);
            runs.push(run);
            pending_rows = Vec::with_capacity(config.max_run_rows);
            let pending_base_bytes =
                sort_row_buffer_bytes(&pending_rows).ok_or(SortError::RowMemoryLimit {
                    observed: usize::MAX,
                    limit: config.max_run_bytes,
                })?;
            if pending_base_bytes > config.max_run_bytes {
                return Err(SortError::RowMemoryLimit {
                    observed: pending_base_bytes,
                    limit: config.max_run_bytes,
                });
            }
            metrics.peak_run_bytes = metrics.peak_run_bytes.max(pending_base_bytes);
            prospective_bytes = sort_row_buffer_bytes(&pending_rows)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .filter(|bytes| *bytes <= config.max_run_bytes);
        }

        let observed = prospective_bytes.ok_or(SortError::RowMemoryLimit {
            observed: sort_row_buffer_bytes(&pending_rows)
                .and_then(|bytes| bytes.checked_add(sort_row_inner_bytes(&row)?))
                .unwrap_or(usize::MAX),
            limit: config.max_run_bytes,
        })?;
        pending_rows.push(row);
        metrics.input_rows += 1;
        metrics.peak_run_rows = metrics.peak_run_rows.max(pending_rows.len());
        metrics.peak_run_bytes = metrics.peak_run_bytes.max(observed);
    }

    if !pending_rows.is_empty() {
        let run = flush_initial_run(
            pending_rows,
            RunDestination {
                work_directory,
                pass: 0,
                file_id: next_file_id,
                current_temp_bytes,
                active_temp_run_files,
            },
            config,
            &mut metrics,
        )?;
        next_file_id += 1;
        current_temp_bytes =
            current_temp_bytes
                .checked_add(run.bytes)
                .ok_or(SortError::TempByteLimit {
                    limit: config.max_temp_bytes,
                })?;
        active_temp_run_files += 1;
        metrics.peak_temp_run_files = metrics.peak_temp_run_files.max(active_temp_run_files);
        metrics.peak_temp_bytes = metrics.peak_temp_bytes.max(current_temp_bytes);
        runs.push(run);
    }

    metrics.initial_runs = runs.len();

    if runs.is_empty() {
        let final_path = work_directory.join("sorted.parquet");
        let empty_run = RunWriter::new(
            final_path.clone(),
            current_temp_bytes,
            active_temp_run_files,
            config,
            &mut metrics,
        )?
        .finish(&mut metrics)?;
        metrics.peak_temp_bytes = empty_run.bytes;
        metrics.peak_temp_run_files = 1;
        metrics.peak_open_files = 1;
        let (logical_sha256, output_rows) =
            scan_logical_output(&final_path, config.batch_rows, config.max_run_bytes)?;
        metrics.output_rows = output_rows;
        return Ok(SortOutcome {
            output_path: final_path,
            logical_sha256,
            metrics,
        });
    }

    let mut pass = 0usize;
    while runs.len() > 1 {
        pass += 1;
        metrics.merge_passes += 1;
        let mut next_runs = Vec::with_capacity(runs.len().div_ceil(config.merge_fan_in));
        let mut remaining = runs.into_iter();

        loop {
            let group: Vec<_> = remaining.by_ref().take(config.merge_fan_in).collect();
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
                    work_directory,
                    pass,
                    file_id: next_file_id,
                    current_temp_bytes,
                    active_temp_run_files,
                },
                config,
                &mut metrics,
            )?;
            let expected_rows = group.iter().try_fold(0u64, |total, run| {
                total
                    .checked_add(run.rows)
                    .ok_or(SortError::InvalidRun("run 行数记账溢出"))
            })?;
            if merged.rows != expected_rows {
                return Err(SortError::InvalidRun("合并行守恒检查失败"));
            }
            next_file_id += 1;

            current_temp_bytes =
                current_temp_bytes
                    .checked_add(merged.bytes)
                    .ok_or(SortError::TempByteLimit {
                        limit: config.max_temp_bytes,
                    })?;
            active_temp_run_files += 1;
            metrics.peak_temp_run_files = metrics.peak_temp_run_files.max(active_temp_run_files);
            metrics.peak_temp_bytes = metrics.peak_temp_bytes.max(current_temp_bytes);

            for input in &group {
                fs::remove_file(&input.path)?;
                current_temp_bytes = current_temp_bytes
                    .checked_sub(input.bytes)
                    .ok_or(SortError::InvalidRun("临时文件字节记账下溢"))?;
                active_temp_run_files = active_temp_run_files
                    .checked_sub(1)
                    .ok_or(SortError::InvalidRun("run 文件记账下溢"))?;
            }
            next_runs.push(merged);
        }
        runs = next_runs;
    }

    let final_run = runs.pop().expect("run 列表非空");
    if final_run.rows != metrics.input_rows {
        return Err(SortError::InvalidRun("run 行守恒检查失败"));
    }
    let final_path = work_directory.join("sorted.parquet");
    fs::rename(&final_run.path, &final_path)?;
    metrics.peak_open_files = metrics.peak_open_files.max(1);
    let (logical_sha256, output_rows) =
        scan_logical_output(&final_path, config.batch_rows, config.max_run_bytes)?;
    metrics.output_rows = output_rows;
    if metrics.output_rows != metrics.input_rows {
        return Err(SortError::InvalidRun("行守恒检查失败"));
    }

    Ok(SortOutcome {
        output_path: final_path,
        logical_sha256,
        metrics,
    })
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
            .is_some_and(|previous| previous >= entry.row.complete_order_key.as_slice())
        {
            if previous_key.as_deref() == Some(entry.row.complete_order_key.as_slice()) {
                return Err(collision_error(&entry.row.complete_order_key));
            }
            return Err(SortError::InvalidRun("输入 run 未严格递增"));
        }
        previous_key = Some(entry.row.complete_order_key.clone());
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
        if pair[0].complete_order_key == pair[1].complete_order_key {
            return Err(collision_error(&pair[0].complete_order_key));
        }
    }
    Ok(())
}

fn collision_error(key: &[u8]) -> SortError {
    SortError::CompleteKeyCollision {
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
    let keys =
        BinaryArray::from_iter_values(rows.iter().map(|row| row.complete_order_key.as_slice()));
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
        .ok_or(SortError::InvalidRun("完整键列不是 Binary"))?;
    let payloads = payloads
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(SortError::InvalidRun("载荷列不是 Binary"))?;
    if keys.len() != payloads.len() || keys.null_count() != 0 || payloads.null_count() != 0 {
        return Err(SortError::InvalidRun("列长度或可空性不一致"));
    }
    Ok((0..keys.len())
        .map(|index| {
            SortRow::from_encoded(keys.value(index).to_vec(), payloads.value(index).to_vec())
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
        let result = self
            .writer
            .as_mut()
            .expect("完成前 writer 必须存在")
            .write(batch);
        if let Err(error) = result {
            if self.quota_exceeded.load(AtomicOrdering::Relaxed) {
                return Err(SortError::TempByteLimit {
                    limit: self.max_temp_bytes,
                });
            }
            return Err(error.into());
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
    reader: ParquetRecordBatchReader,
    batch: std::vec::IntoIter<SortRow>,
    max_batch_bytes: usize,
}

impl RunCursor {
    fn open(path: &Path, batch_rows: usize, max_batch_bytes: usize) -> Result<Self, SortError> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .with_batch_size(batch_rows)
            .build()?;
        Ok(Self {
            reader,
            batch: Vec::new().into_iter(),
            max_batch_bytes,
        })
    }

    fn next_row(&mut self) -> Result<Option<SortRow>, SortError> {
        loop {
            if let Some(row) = self.batch.next() {
                return Ok(Some(row));
            }
            match self.reader.next() {
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
                None => return Ok(None),
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
        other
            .row
            .complete_order_key
            .cmp(&self.row.complete_order_key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.complete_order_key == other.row.complete_order_key
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
            .is_some_and(|previous| previous >= row.complete_order_key.as_slice())
        {
            if previous_key.as_deref() == Some(row.complete_order_key.as_slice()) {
                return Err(collision_error(&row.complete_order_key));
            }
            return Err(SortError::InvalidRun("最终输出未严格递增"));
        }
        hash_length_prefixed(&mut hasher, &row.complete_order_key)?;
        hash_length_prefixed(&mut hasher, &row.payload)?;
        previous_key = Some(row.complete_order_key);
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
    use contracts::{
        CanonicalSourceKind, OrderingCoordinateV1, Sha256Hex, SourceNativeIdentityV1,
        TOTAL_ORDER_KEY_VERSION_V1,
    };
    use tempfile::TempDir;

    fn contract_key(sequence: u64) -> TotalOrderKeyV1 {
        TotalOrderKeyV1 {
            version: TOTAL_ORDER_KEY_VERSION_V1.to_owned(),
            available_at_utc_ms: 1_725_000_000_000,
            source_kind: CanonicalSourceKind::BinanceDepthUpdate,
            ordering_coordinate: OrderingCoordinateV1::CollectorAdmission {
                source_sequence_epoch_id: "phase-0-trial".to_owned(),
                collector_admission_sequence: sequence + 1,
            },
            source_native_identity: Some(SourceNativeIdentityV1::BinanceDepthUpdate {
                first_update_id: sequence + 1,
                final_update_id: sequence + 1,
            }),
            semantic_tiebreak_sha256: Sha256Hex(format!("{sequence:064x}")),
        }
    }

    fn row(sequence: u64) -> SortRow {
        SortRow::from_total_order_key(
            &contract_key(sequence),
            format!("payload-{sequence:020}").into_bytes(),
        )
        .expect("合同键有效")
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
            .map(|row| row.complete_order_key_bytes().to_vec())
            .collect();
        assert_eq!(
            keys,
            (0u64..17)
                .map(|sequence| total_order_key_bytes(&contract_key(sequence)).expect("键有效"))
                .collect::<Vec<_>>()
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
    fn arrow_binary_order_matches_known_admission_sequence_order() {
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
    fn complete_key_collision_fails_across_runs() {
        let temp = TempDir::new().expect("创建临时目录");
        let duplicate_key = contract_key(2);
        let input = vec![
            SortRow::from_total_order_key(&duplicate_key, b"first".to_vec())
                .expect("重复键本身有效"),
            row(1),
            SortRow::from_total_order_key(&duplicate_key, b"different payload".to_vec())
                .expect("重复键本身有效"),
        ];

        let error = external_sort(input, temp.path(), &config(1, 2)).expect_err("检测到冲突");

        assert!(matches!(error, SortError::CompleteKeyCollision { .. }));
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
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].complete_order_key.capacity()))
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
            .and_then(|bytes| bytes.checked_add(capacity_probe[0].complete_order_key.capacity()))
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
