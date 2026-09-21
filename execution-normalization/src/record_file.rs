//! 不可变物理记录文件。序号和变体由 Arrow 列承载，正文沿 serde_json_v1。
//! 业务边界由调用方的一份文件元数据保存；空记录文件仍保留该边界。
use arrow_array::{Array, BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::Arc,
};

pub type Row = (u64, String, Vec<u8>);
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

/// 持续后台写入，不保留整市场正文；每个交接批写成独立行组。
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
    pub fn finish(self) -> io::Result<()> {
        self.writer.close().map_err(invalid)?;
        self.file.sync_all()
    }
}
