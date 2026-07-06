use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEvent {
    pub source_kind: String,
    pub source_event_type: String,
    pub source_identifier: String,
    pub source_ts: Option<String>,
    pub recv_time_utc_ms: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub raw_json: String,
    pub parquet_raw_layout_version: u32,
    pub parquet_raw_source_kind: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedSegment {
    pub final_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FinalizeTiming {
    pub finalize_close_us: u64,
    pub running_to_digesting_rename_us: u64,
    pub sha256_us: u64,
    pub digesting_to_final_rename_us: u64,
    pub atomic_rename_us: u64,
    pub finalize_total_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WriterWriteStats {
    pub rows: u64,
    pub estimated_event_bytes: u64,
    pub write_events_us: u64,
    pub row_group_memory_estimate_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentedPublishedSegment {
    pub segment: PublishedSegment,
    pub timing: FinalizeTiming,
}

pub struct ParquetSegmentWriter {
    running_path: PathBuf,
    digesting_path: PathBuf,
    final_path: PathBuf,
    schema_version: String,
    parquet_raw_source_kind: String,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    row_count: u64,
}

impl ParquetSegmentWriter {
    pub fn create(root: &Path, stem: &str, schema_version: &str) -> Result<Self> {
        Self::create_for_source(root, stem, schema_version, "mixed")
    }

    pub fn create_for_source(
        root: &Path,
        stem: &str,
        schema_version: &str,
        parquet_raw_source_kind: &str,
    ) -> Result<Self> {
        validate_segment_stem(stem)?;
        fs::create_dir_all(root)?;

        let running_path = root.join(format!("{stem}-running.parquet"));
        let digesting_path = root.join(format!("{stem}-digesting.parquet"));
        let final_path = root.join(format!("{stem}.parquet"));
        if final_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("final parquet already exists: {}", final_path.display()),
            )
            .into());
        }
        if running_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("running parquet already exists: {}", running_path.display()),
            )
            .into());
        }
        if digesting_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "digesting parquet already exists: {}",
                    digesting_path.display()
                ),
            )
            .into());
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&running_path)?;
        let schema = raw_event_schema();
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "parquet_raw_layout_version".to_owned(),
                    schema_version.to_owned(),
                ),
                KeyValue::new(
                    "parquet_raw_source_kind".to_owned(),
                    parquet_raw_source_kind.to_owned(),
                ),
                KeyValue::new(
                    "data_collect_artifact_kind".to_owned(),
                    "typed_raw_parquet".to_owned(),
                ),
            ]))
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;

        Ok(Self {
            running_path,
            digesting_path,
            final_path,
            schema_version: schema_version.to_owned(),
            parquet_raw_source_kind: parquet_raw_source_kind.to_owned(),
            schema,
            writer: Some(writer),
            row_count: 0,
        })
    }

    pub fn running_path(&self) -> &Path {
        &self.running_path
    }

    pub fn write_events(&mut self, events: &[RawEvent]) -> Result<()> {
        self.write_events_with_stats(events).map(|_| ())
    }

    pub fn write_events_with_stats(&mut self, events: &[RawEvent]) -> Result<WriterWriteStats> {
        if events.is_empty() {
            return Ok(WriterWriteStats {
                rows: 0,
                estimated_event_bytes: 0,
                write_events_us: 0,
                row_group_memory_estimate_bytes: 0,
            });
        }

        for event in events {
            if self.parquet_raw_source_kind != "mixed"
                && event.parquet_raw_source_kind != self.parquet_raw_source_kind
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "event source kind {} does not match segment source kind {}",
                        event.parquet_raw_source_kind, self.parquet_raw_source_kind
                    ),
                )
                .into());
            }
        }

        let write_started = Instant::now();
        let estimated_event_bytes = events.iter().map(estimated_event_bytes).sum();
        let batch = events_to_batch(Arc::clone(&self.schema), events)?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("parquet segment already finalized"))?;
        writer.write(&batch)?;
        self.row_count += events.len() as u64;
        let write_events_us = elapsed_us(write_started);
        Ok(WriterWriteStats {
            rows: events.len() as u64,
            estimated_event_bytes,
            write_events_us,
            row_group_memory_estimate_bytes: estimated_event_bytes,
        })
    }

    pub fn finalize(self) -> Result<PublishedSegment> {
        Ok(self.finalize_with_timing()?.segment)
    }

    pub fn finalize_with_timing(mut self) -> Result<InstrumentedPublishedSegment> {
        let finalize_started = Instant::now();
        let writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("parquet segment already finalized"))?;
        let close_started = Instant::now();
        writer.close()?;
        let finalize_close_us = elapsed_us(close_started);

        let first_rename_started = Instant::now();
        fs::rename(&self.running_path, &self.digesting_path)?;
        let running_to_digesting_rename_us = elapsed_us(first_rename_started);
        let sha256_started = Instant::now();
        let sha256 = sha256_file(&self.digesting_path)?;
        let sha256_us = elapsed_us(sha256_started);
        let final_rename_started = Instant::now();
        fs::rename(&self.digesting_path, &self.final_path)?;
        let digesting_to_final_rename_us = elapsed_us(final_rename_started);

        let timing = FinalizeTiming {
            finalize_close_us,
            running_to_digesting_rename_us,
            sha256_us,
            digesting_to_final_rename_us,
            atomic_rename_us: running_to_digesting_rename_us + digesting_to_final_rename_us,
            finalize_total_us: elapsed_us(finalize_started),
        };

        Ok(InstrumentedPublishedSegment {
            segment: PublishedSegment {
                final_path: self.final_path,
                row_count: self.row_count,
                sha256,
                schema_version: self.schema_version,
            },
            timing,
        })
    }
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros() as u64
}

pub async fn write_segment_on_blocking_pool(
    root: PathBuf,
    stem: String,
    schema_version: String,
    event_chunks: Vec<Vec<RawEvent>>,
) -> Result<PublishedSegment> {
    tokio::task::spawn_blocking(move || {
        let mut writer = ParquetSegmentWriter::create(&root, &stem, &schema_version)?;
        for events in event_chunks {
            writer.write_events(&events)?;
        }
        writer.finalize()
    })
    .await
    .map_err(|error| io::Error::other(format!("writer blocking task failed: {error}")))?
}

pub async fn write_segment_on_blocking_pool_for_source(
    root: PathBuf,
    stem: String,
    schema_version: String,
    parquet_raw_source_kind: String,
    event_chunks: Vec<Vec<RawEvent>>,
) -> Result<PublishedSegment> {
    tokio::task::spawn_blocking(move || {
        let mut writer = ParquetSegmentWriter::create_for_source(
            &root,
            &stem,
            &schema_version,
            &parquet_raw_source_kind,
        )?;
        for events in event_chunks {
            writer.write_events(&events)?;
        }
        writer.finalize()
    })
    .await
    .map_err(|error| io::Error::other(format!("writer blocking task failed: {error}")))?
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn raw_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("source_event_type", DataType::Utf8, false),
        Field::new("source_identifier", DataType::Utf8, false),
        Field::new("source_ts", DataType::Utf8, true),
        Field::new(
            "recv_time_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("raw_json", DataType::Utf8, false),
        Field::new("parquet_raw_layout_version", DataType::UInt32, false),
        Field::new("parquet_raw_source_kind", DataType::Utf8, false),
    ]))
}

fn events_to_batch(schema: Arc<Schema>, events: &[RawEvent]) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.source_kind.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.source_event_type.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.source_identifier.as_str()),
        )),
        Arc::new(StringArray::from_iter(
            events.iter().map(|event| event.source_ts.as_deref()),
        )),
        Arc::new(
            TimestampMillisecondArray::from_iter_values(
                events.iter().map(|event| event.recv_time_utc_ms),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.collector_host_id.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.collector_region.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.source_endpoint.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|event| event.raw_json.as_str()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            events.iter().map(|event| event.parquet_raw_layout_version),
        )),
        Arc::new(StringArray::from_iter_values(
            events
                .iter()
                .map(|event| event.parquet_raw_source_kind.as_str()),
        )),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn validate_segment_stem(stem: &str) -> Result<()> {
    if stem.trim().is_empty()
        || stem == "."
        || stem == ".."
        || stem.contains('/')
        || stem.contains('\\')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "parquet segment stem is not a safe path component",
        )
        .into());
    }
    Ok(())
}

fn estimated_event_bytes(event: &RawEvent) -> u64 {
    [
        event.source_kind.as_str(),
        event.source_event_type.as_str(),
        event.source_identifier.as_str(),
        event.source_ts.as_deref().unwrap_or_default(),
        event.collector_host_id.as_str(),
        event.collector_region.as_str(),
        event.source_endpoint.as_str(),
        event.raw_json.as_str(),
        event.parquet_raw_source_kind.as_str(),
    ]
    .iter()
    .map(|value| value.len() as u64)
    .sum::<u64>()
        + std::mem::size_of::<i64>() as u64
        + std::mem::size_of::<u32>() as u64
}
