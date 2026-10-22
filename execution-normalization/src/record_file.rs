//! 不可变物理记录文件。序号和变体由 Arrow 列承载，正文沿 serde_json_v1。
//! 业务边界由调用方的一份文件元数据保存；空记录文件仍保留该边界。
use arrow_array::{Array, BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::{
    ArrowWriter,
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::Arc,
};

pub type Row = (u64, String, Vec<u8>);
/// 原后台 Parquet 行组的排出目标；不限制合法单行大小。
pub const BACKGROUND_ROW_TARGET: usize = 4096;
pub const BACKGROUND_BYTES_TARGET: usize = 4 << 20;
fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn schema(format: &str, boundary: &str) -> Schema {
    Schema::new(vec![
        Field::new("record_sequence", DataType::UInt64, false),
        Field::new("variant", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ])
    .with_metadata(HashMap::from([
        ("physical_format".into(), format.into()),
        ("payload_encoding".into(), "serde_json_v1".into()),
        ("boundary".into(), boundary.into()),
    ]))
}
/// 写出 footer 并同步文件；发布和 manifest 由拥有方顺序完成。
pub fn write(path: &Path, format: &str, boundary: &str, rows: &[Row]) -> io::Result<()> {
    let mut writer = Writer::create(path, format, boundary)?;
    writer.append(rows)?;
    writer.finish()
}
pub fn read(path: &Path, format: &str) -> io::Result<(String, Vec<Row>)> {
    let mut rows = Vec::new();
    let boundary = visit(path, format, |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok((boundary, rows))
}
/// 恢复逐页读取；不把一个市场的 Ledger 正文全部留在内存。
pub fn visit(
    path: &Path,
    format: &str,
    mut consume: impl FnMut(Row) -> io::Result<()>,
) -> io::Result<String> {
    visit_range(path, format, None, &mut consume)
}

/// 按物理行范围读取；共享段内的邻批正文不进入解码器。
pub(crate) fn read_range(
    path: &Path,
    format: &str,
    start: u64,
    count: u64,
) -> io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    visit_range(path, format, Some((start, count)), |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
}
fn visit_range(
    path: &Path,
    format: &str,
    range: Option<(u64, u64)>,
    mut consume: impl FnMut(Row) -> io::Result<()>,
) -> io::Result<String> {
    let (mut builder, boundary) = open_reader(path, format)?;
    if let Some((start, count)) = range {
        let total =
            u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(invalid)?;
        let end = start
            .checked_add(count)
            .filter(|end| *end <= total)
            .ok_or_else(|| invalid("记录范围超出物理段"))?;
        builder = builder.with_row_selection(RowSelection::from(vec![
            RowSelector::skip(usize::try_from(start).map_err(invalid)?),
            RowSelector::select(usize::try_from(count).map_err(invalid)?),
            RowSelector::skip(usize::try_from(total - end).map_err(invalid)?),
        ]));
    }
    for batch in builder.build().map_err(invalid)? {
        let batch = batch.map_err(invalid)?;
        let seq = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("序号列类型"))?;
        let kind = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("变体列类型"))?;
        let body = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("正文列类型"))?;
        if seq.null_count() + kind.null_count() + body.null_count() != 0 {
            return Err(invalid("记录列含空值"));
        }
        for i in 0..batch.num_rows() {
            consume((
                seq.value(i),
                kind.value(i).to_owned(),
                body.value(i).to_vec(),
            ))?;
        }
    }
    Ok(boundary)
}

fn open_reader(
    path: &Path,
    format: &str,
) -> io::Result<(ParquetRecordBatchReaderBuilder<File>, String)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?).map_err(invalid)?;
    let found = builder.schema();
    let boundary = found
        .metadata()
        .get("boundary")
        .ok_or_else(|| invalid("缺少记录边界"))?
        .clone();
    if found.as_ref() != &schema(format, &boundary) {
        return Err(invalid("不支持的记录模式或编码"));
    }
    Ok((builder, boundary))
}

/// 位置恢复只读取 footer 的真实行数与边界，不解码记录正文。
pub(crate) fn read_metadata(path: &Path, format: &str) -> io::Result<(String, u64)> {
    let (builder, boundary) = open_reader(path, format)?;
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(invalid)?;
    Ok((boundary, rows))
}

/// 持续后台写入；由 owner 的有界排出时点封口行组，不保留整市场正文。
pub struct Writer {
    file: File,
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
}
impl Writer {
    pub fn create(path: &Path, format: &str, boundary: &str) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let schema = Arc::new(schema(format, boundary));
        let writer =
            ArrowWriter::try_new(file.try_clone()?, schema.clone(), None).map_err(invalid)?;
        Ok(Self {
            file,
            writer,
            schema,
        })
    }
    pub fn append(&mut self, rows: &[Row]) -> io::Result<()> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.1.as_str()),
                )),
                Arc::new(BinaryArray::from_iter_values(
                    rows.iter().map(|r| r.2.as_slice()),
                )),
            ],
        )
        .map_err(invalid)?;
        self.writer.write(&batch).map_err(invalid)
    }
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().map_err(invalid)
    }
    /// 后台交接不强制每小批产生行组；到有界大小才排出。
    pub fn flush_if_full(&mut self) -> io::Result<()> {
        if self.writer.in_progress_rows() >= BACKGROUND_ROW_TARGET
            || self.writer.in_progress_size() >= BACKGROUND_BYTES_TARGET
        {
            self.flush()?;
        }
        Ok(())
    }
    pub fn finish(self) -> io::Result<()> {
        self.writer.close().map_err(invalid)?;
        self.file.sync_all()
    }
}

/// 顺序历史读取保留一个当前段和一个 Arrow 页；不同逻辑批不反复解码同页。
pub(crate) struct SequentialReader {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    current: Option<(RecordBatch, usize)>,
    pub position: u64,
    pub pages_read: u64,
}
impl SequentialReader {
    pub fn open(path: &Path, format: &str, start: u64) -> io::Result<Self> {
        let (mut builder, _) = open_reader(path, format)?;
        let total =
            u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(invalid)?;
        if start > total {
            return Err(invalid("历史读取范围超出物理段"));
        }
        builder = builder.with_row_selection(RowSelection::from(vec![
            RowSelector::skip(usize::try_from(start).map_err(invalid)?),
            RowSelector::select(usize::try_from(total - start).map_err(invalid)?),
        ]));
        Ok(Self {
            reader: builder.build().map_err(invalid)?,
            current: None,
            position: start,
            pages_read: 0,
        })
    }
    pub fn take(&mut self, count: u64) -> io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        for _ in 0..count {
            if self
                .current
                .as_ref()
                .is_none_or(|(b, i)| *i == b.num_rows())
            {
                self.current = Some((
                    self.reader
                        .next()
                        .ok_or_else(|| invalid("逻辑批范围超过共享段"))?
                        .map_err(invalid)?,
                    0,
                ));
            }
            let (batch, i) = self.current.as_mut().unwrap();
            if *i == 0 {
                self.pages_read += 1;
            }
            let seq = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("序号列类型"))?;
            let kind = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("变体列类型"))?;
            let body = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("正文列类型"))?;
            if seq.is_null(*i) || kind.is_null(*i) || body.is_null(*i) {
                return Err(invalid("记录列含空值"));
            }
            rows.push((
                seq.value(*i),
                kind.value(*i).to_owned(),
                body.value(*i).to_vec(),
            ));
            *i += 1;
            self.position += 1;
        }
        Ok(rows)
    }
}
