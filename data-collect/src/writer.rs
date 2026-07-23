use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION, PolymarketMarketMetadataV3Row,
    polymarket_market_metadata_v3_schema,
};
use crate::polymarket_orderbook_layout::{PolymarketOrderbookRow, polymarket_orderbook_schema};
use crate::source_hourly_layout::{
    BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION, BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
    BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION, BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
    BinanceAggTradeHourlyRow, BinanceDepthOrderingClassification, BinanceDepthSnapshotHourlyRow,
    BinanceDepthUpdateHourlyRow, RtdsChainlinkBtcUsdHourlyRow, binance_aggtrade_schema,
    binance_depth_snapshot_schema, binance_depth_update_schema, rtds_chainlink_btc_usd_schema,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const PARQUET_ROW_GROUP_MAX_ROWS: u64 = 50_000;
pub const PARQUET_ROW_GROUP_MAX_BYTES: u64 = 16_777_216;
pub const DAILY_FILE_RECOVERY_METADATA_KEY: &str = "data_collect_daily_file_fact_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEventAdmission {
    pub sequence: u64,
    pub time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEvent {
    pub source_kind: String,
    pub source_event_type: String,
    pub source_identifier: String,
    pub source_ts: Option<String>,
    pub recv_time_utc_ms: i64,
    pub collector_admission: Option<RawEventAdmission>,
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

pub struct TypedParquetSegmentWriter {
    running_path: PathBuf,
    final_path: PathBuf,
    schema_version: String,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    row_count: u64,
    row_group_rows: u64,
    row_group_logical_bytes: u64,
    last_binance_aggtrade_admission_sequence: Option<u64>,
    last_binance_depth_admission_sequence: Option<u64>,
    last_polymarket_metadata_admission_sequence: Option<u64>,
}

impl TypedParquetSegmentWriter {
    pub fn create_polymarket_orderbook(
        root: &Path,
        stem: &str,
        schema_version: &str,
    ) -> Result<Self> {
        Self::create_polymarket_orderbook_with_zstd_level(root, stem, schema_version, 3)
    }

    pub fn create_polymarket_orderbook_with_zstd_level(
        root: &Path,
        stem: &str,
        schema_version: &str,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_with_schema(
            root,
            stem,
            schema_version,
            "polymarket_market",
            polymarket_orderbook_schema(),
            zstd_level,
        )
    }

    pub fn create_for_typed_source(
        root: &Path,
        stem: &str,
        schema_version: &str,
        source_kind: &str,
    ) -> Result<Self> {
        let schema = match source_kind {
            "polymarket_market" => polymarket_orderbook_schema(),
            "polymarket_market_metadata" => match schema_version {
                POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION => {
                    polymarket_market_metadata_v3_schema()
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "unsupported Polymarket metadata schema version for writer:schema_version={schema_version}"
                        ),
                    )
                    .into());
                }
            },
            "binance_depth_snapshot" => {
                validate_current_depth_schema_version(
                    source_kind,
                    schema_version,
                    BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
                    BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
                )?;
                binance_depth_snapshot_schema()
            }
            "binance_depth_update" => {
                validate_current_depth_schema_version(
                    source_kind,
                    schema_version,
                    BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
                    BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
                )?;
                binance_depth_update_schema()
            }
            "binance_aggtrade" => binance_aggtrade_schema(),
            "polymarket_rtds_chainlink_btc_usd" => rtds_chainlink_btc_usd_schema(),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported typed source kind: {other}"),
                )
                .into());
            }
        };
        Self::create_with_schema(root, stem, schema_version, source_kind, schema, 3)
    }

    fn create_with_schema(
        root: &Path,
        stem: &str,
        schema_version: &str,
        source_kind: &str,
        schema: Arc<Schema>,
        zstd_level: i32,
    ) -> Result<Self> {
        validate_typed_writer_schema(source_kind, schema_version, schema.as_ref())?;
        validate_segment_stem(stem)?;
        fs::create_dir_all(root)?;

        let running_path = root.join(format!("{stem}.parquet.running"));
        let final_path = root.join(format!("{stem}.parquet"));
        if final_path.exists() || running_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("typed parquet segment already exists for stem: {stem}"),
            )
            .into());
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&running_path)?;
        let properties = typed_writer_properties(schema_version, source_kind, zstd_level)?;
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;

        Ok(Self {
            running_path,
            final_path,
            schema_version: schema_version.to_owned(),
            schema,
            writer: Some(writer),
            row_count: 0,
            row_group_rows: 0,
            row_group_logical_bytes: 0,
            last_binance_aggtrade_admission_sequence: None,
            last_binance_depth_admission_sequence: None,
            last_polymarket_metadata_admission_sequence: None,
        })
    }

    pub fn write_polymarket_orderbook_rows(
        &mut self,
        rows: &[PolymarketOrderbookRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = polymarket_orderbook_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
    }

    pub(crate) fn write_polymarket_orderbook_rows_with_logical_bytes(
        &mut self,
        rows: &[PolymarketOrderbookRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = polymarket_orderbook_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)
    }

    pub fn write_polymarket_market_metadata_v3_rows(
        &mut self,
        rows: &[PolymarketMarketMetadataV3Row],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v3_rows(rows)?;
        let batch = polymarket_market_metadata_v3_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_polymarket_market_metadata_v3_rows_with_logical_bytes(
        &mut self,
        rows: &[PolymarketMarketMetadataV3Row],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v3_rows(rows)?;
        let batch = polymarket_market_metadata_v3_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    fn validate_polymarket_metadata_v3_rows(
        &self,
        rows: &[PolymarketMarketMetadataV3Row],
    ) -> Result<u64> {
        if self.schema_version != POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v3 Polymarket metadata rows require a v3 writer",
            )
            .into());
        }
        let mut previous = self.last_polymarket_metadata_admission_sequence;
        for row in rows {
            let validation = row.validate();
            if !validation.is_complete() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid Polymarket metadata v3 row:{}",
                        validation.violations.join(",")
                    ),
                )
                .into());
            }
            if previous.is_some_and(|previous| row.collector_admission_sequence <= previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Polymarket metadata v3 admission sequence must be strictly increasing",
                )
                .into());
            }
            previous = Some(row.collector_admission_sequence);
        }
        Ok(previous.expect("non-empty rows have a last admission sequence"))
    }

    pub fn write_binance_depth_snapshot_rows(
        &mut self,
        rows: &[BinanceDepthSnapshotHourlyRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_depth_snapshot_admission_order(rows)?;
        let batch = binance_depth_snapshot_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)?;
        self.last_binance_depth_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_binance_depth_snapshot_rows_with_logical_bytes(
        &mut self,
        rows: &[BinanceDepthSnapshotHourlyRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_depth_snapshot_admission_order(rows)?;
        let batch = binance_depth_snapshot_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_binance_depth_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub fn write_binance_depth_update_rows(
        &mut self,
        rows: &[BinanceDepthUpdateHourlyRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_depth_update_admission_order(rows)?;
        let batch = binance_depth_update_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)?;
        self.last_binance_depth_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_binance_depth_update_rows_with_logical_bytes(
        &mut self,
        rows: &[BinanceDepthUpdateHourlyRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_depth_update_admission_order(rows)?;
        let batch = binance_depth_update_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_binance_depth_admission_sequence = Some(last_sequence);
        Ok(())
    }

    fn validate_binance_depth_snapshot_admission_order(
        &self,
        rows: &[BinanceDepthSnapshotHourlyRow],
    ) -> Result<u64> {
        let mut previous = self.last_binance_depth_admission_sequence;
        for row in rows {
            let validation = row.validate();
            if !validation.is_complete() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "binance depth snapshot row validation failed:{}",
                        validation.missing_fields.join(",")
                    ),
                )
                .into());
            }
            let classification = row.ordering_classification().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("binance depth snapshot ordering classification failed:{error}"),
                )
            })?;
            if classification != BinanceDepthOrderingClassification::Exact {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "binance depth snapshot writer requires exact ordering",
                )
                .into());
            }
            let sequence = row.collector_admission_sequence.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "binance depth snapshot collector admission sequence missing",
                )
            })?;
            validate_depth_admission_sequence(previous, sequence)?;
            previous = Some(sequence);
        }
        previous.ok_or_else(|| io::Error::other("missing depth snapshot admission sequence").into())
    }

    fn validate_binance_depth_update_admission_order(
        &self,
        rows: &[BinanceDepthUpdateHourlyRow],
    ) -> Result<u64> {
        let mut previous = self.last_binance_depth_admission_sequence;
        for row in rows {
            let validation = row.validate();
            if !validation.is_complete() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "binance depth update row validation failed:{}",
                        validation.missing_fields.join(",")
                    ),
                )
                .into());
            }
            let classification = row.ordering_classification().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("binance depth update ordering classification failed:{error}"),
                )
            })?;
            if classification != BinanceDepthOrderingClassification::Exact {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "binance depth update writer requires exact ordering",
                )
                .into());
            }
            let sequence = row.collector_admission_sequence.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "binance depth update collector admission sequence missing",
                )
            })?;
            validate_depth_admission_sequence(previous, sequence)?;
            previous = Some(sequence);
        }
        previous.ok_or_else(|| io::Error::other("missing depth update admission sequence").into())
    }

    pub fn write_binance_aggtrade_rows(&mut self, rows: &[BinanceAggTradeHourlyRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_aggtrade_admission_order(rows)?;
        let batch = binance_aggtrade_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)?;
        self.last_binance_aggtrade_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_binance_aggtrade_rows_with_logical_bytes(
        &mut self,
        rows: &[BinanceAggTradeHourlyRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_binance_aggtrade_admission_order(rows)?;
        let batch = binance_aggtrade_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_binance_aggtrade_admission_sequence = Some(last_sequence);
        Ok(())
    }

    fn validate_binance_aggtrade_admission_order(
        &self,
        rows: &[BinanceAggTradeHourlyRow],
    ) -> Result<u64> {
        let mut previous = self.last_binance_aggtrade_admission_sequence;
        for row in rows {
            if row.connection_generation == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "binance aggTrade connection generation must be positive",
                )
                .into());
            }
            let validation = row.validate();
            if !validation.is_complete() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "binance aggTrade row validation failed:{}",
                        validation.missing_fields.join(",")
                    ),
                )
                .into());
            }
            if previous.is_some_and(|previous| row.collector_admission_sequence <= previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "binance aggTrade collector admission sequence is not strictly increasing:previous={}:actual={}",
                        previous.unwrap_or_default(),
                        row.collector_admission_sequence
                    ),
                )
                .into());
            }
            previous = Some(row.collector_admission_sequence);
        }
        previous.ok_or_else(|| io::Error::other("missing aggTrade admission sequence").into())
    }

    pub fn write_rtds_chainlink_btc_usd_rows(
        &mut self,
        rows: &[RtdsChainlinkBtcUsdHourlyRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = rtds_chainlink_btc_usd_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
    }

    pub(crate) fn write_rtds_chainlink_btc_usd_rows_with_logical_bytes(
        &mut self,
        rows: &[RtdsChainlinkBtcUsdHourlyRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = rtds_chainlink_btc_usd_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let logical_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "record batch logical size does not fit in u64",
            )
        })?;
        let total_rows = u64::try_from(batch.num_rows()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "record batch row count does not fit in u64",
            )
        })?;
        let rows_per_chunk = if logical_bytes <= PARQUET_ROW_GROUP_MAX_BYTES {
            total_rows.min(PARQUET_ROW_GROUP_MAX_ROWS)
        } else {
            ((u128::from(total_rows) * u128::from(PARQUET_ROW_GROUP_MAX_BYTES))
                / u128::from(logical_bytes))
            .max(1)
            .min(u128::from(PARQUET_ROW_GROUP_MAX_ROWS)) as u64
        };
        let mut offset = 0_u64;
        while offset < total_rows {
            let rows = rows_per_chunk.min(total_rows - offset);
            let chunk = batch.slice(offset as usize, rows as usize);
            let chunk_logical_bytes = ((u128::from(logical_bytes) * u128::from(rows))
                .div_ceil(u128::from(total_rows))) as u64;
            self.write_batch_with_logical_bytes(&chunk, chunk_logical_bytes)?;
            offset += rows;
        }
        Ok(())
    }

    fn write_batch_with_logical_bytes(
        &mut self,
        batch: &RecordBatch,
        logical_bytes: u64,
    ) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record batch schema does not match typed parquet writer schema",
            )
            .into());
        }
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "record batch row count does not fit in u64",
            )
        })?;
        if rows > PARQUET_ROW_GROUP_MAX_ROWS || logical_bytes > PARQUET_ROW_GROUP_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record batch exceeds row-group hard limit: rows={rows} bytes={logical_bytes}"
                ),
            )
            .into());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("typed parquet segment already finalized"))?;
        if self.row_group_rows > 0
            && (self.row_group_rows.saturating_add(rows) > PARQUET_ROW_GROUP_MAX_ROWS
                || self.row_group_logical_bytes.saturating_add(logical_bytes)
                    > PARQUET_ROW_GROUP_MAX_BYTES)
        {
            writer.flush()?;
            self.row_group_rows = 0;
            self.row_group_logical_bytes = 0;
        }
        writer.write(batch)?;
        self.row_count = self.row_count.saturating_add(rows);
        self.row_group_rows = self.row_group_rows.saturating_add(rows);
        self.row_group_logical_bytes = self.row_group_logical_bytes.saturating_add(logical_bytes);
        if self.row_group_rows == PARQUET_ROW_GROUP_MAX_ROWS
            || self.row_group_logical_bytes == PARQUET_ROW_GROUP_MAX_BYTES
        {
            writer.flush()?;
            self.row_group_rows = 0;
            self.row_group_logical_bytes = 0;
        }
        Ok(())
    }

    pub fn append_key_value_metadata(&mut self, key: &str, value: String) -> Result<()> {
        if key.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Parquet metadata key must not be empty",
            )
            .into());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("typed parquet segment already finalized"))?;
        writer.append_key_value_metadata(KeyValue::new(key.to_owned(), value));
        Ok(())
    }

    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    pub fn finalize(mut self) -> Result<PublishedSegment> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("typed parquet segment already finalized"))?;
        writer.close()?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.running_path)?
            .sync_all()?;
        let sha256 = sha256_file(&self.running_path)?;
        fs::rename(&self.running_path, &self.final_path)?;
        File::open(self.final_path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "final parquet has no parent")
        })?)?
        .sync_all()?;
        Ok(PublishedSegment {
            final_path: self.final_path,
            row_count: self.row_count,
            sha256,
            schema_version: self.schema_version,
        })
    }
}

fn typed_writer_properties(
    schema_version: &str,
    source_kind: &str,
    zstd_level: i32,
) -> Result<WriterProperties> {
    let sort_order = typed_sort_order(schema_version, source_kind);
    Ok(WriterProperties::builder()
        .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_MAX_ROWS as usize))
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level)?))
        .set_dictionary_enabled(true)
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "parquet_raw_layout_version".to_owned(),
                schema_version.to_owned(),
            ),
            KeyValue::new("parquet_raw_source_kind".to_owned(), source_kind.to_owned()),
            KeyValue::new(
                "data_collect_artifact_kind".to_owned(),
                "typed_raw_parquet".to_owned(),
            ),
            KeyValue::new("compression".to_owned(), format!("zstd_level_{zstd_level}")),
            KeyValue::new("sort_order".to_owned(), sort_order.to_owned()),
        ]))
        .build())
}

pub(crate) fn typed_sort_order(schema_version: &str, source_kind: &str) -> &'static str {
    if matches!(
        source_kind,
        "binance_aggtrade" | "binance_depth_snapshot" | "binance_depth_update"
    ) || (source_kind == "polymarket_market_metadata"
        && schema_version == POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION)
    {
        "collector_admission_order"
    } else {
        "receive_order"
    }
}

fn validate_current_depth_schema_version(
    source_kind: &str,
    schema_version: &str,
    current_schema_version: &str,
    legacy_schema_version: &str,
) -> Result<()> {
    if schema_version == current_schema_version {
        return Ok(());
    }
    let reason = if schema_version == legacy_schema_version {
        format!(
            "read-only legacy depth schema cannot be opened for writing:source_kind={source_kind}:schema_version={schema_version}"
        )
    } else {
        format!(
            "unsupported depth schema version for writer:source_kind={source_kind}:schema_version={schema_version}:expected={current_schema_version}"
        )
    };
    Err(io::Error::new(io::ErrorKind::InvalidInput, reason).into())
}

fn validate_typed_writer_schema(
    source_kind: &str,
    schema_version: &str,
    schema: &Schema,
) -> Result<()> {
    let expected = match source_kind {
        "binance_depth_snapshot" => {
            validate_current_depth_schema_version(
                source_kind,
                schema_version,
                BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
                BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
            )?;
            Some(binance_depth_snapshot_schema())
        }
        "binance_depth_update" => {
            validate_current_depth_schema_version(
                source_kind,
                schema_version,
                BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
                BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
            )?;
            Some(binance_depth_update_schema())
        }
        _ => None,
    };
    if expected
        .as_deref()
        .is_some_and(|expected| expected != schema)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "typed depth writer schema mismatch:source_kind={source_kind}:schema_version={schema_version}"
            ),
        )
        .into());
    }
    Ok(())
}

fn validate_depth_admission_sequence(previous: Option<u64>, sequence: u64) -> Result<()> {
    if sequence == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "binance depth collector admission sequence must be positive",
        )
        .into());
    }
    if previous.is_some_and(|previous| sequence <= previous) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "binance depth collector admission sequence is not strictly increasing:previous={}:actual={sequence}",
                previous.unwrap_or_default()
            ),
        )
        .into());
    }
    Ok(())
}

fn polymarket_orderbook_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketOrderbookRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|row| row.timestamp_received),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from_iter(rows.iter().map(|row| row.timestamp))
                .with_timezone("UTC"),
        ),
        string_values(rows.iter().map(|row| row.market.as_str())),
        string_values(rows.iter().map(|row| row.event_type.as_str())),
        string_values(
            rows.iter()
                .map(|row| row.source_native_payload_json.as_str()),
        ),
        optional_strings(rows.iter().map(|row| row.asset_id.as_deref())),
        optional_strings(rows.iter().map(|row| row.bids.as_deref())),
        optional_strings(rows.iter().map(|row| row.asks.as_deref())),
        optional_strings(rows.iter().map(|row| row.price.as_deref())),
        optional_strings(rows.iter().map(|row| row.size.as_deref())),
        optional_strings(rows.iter().map(|row| row.side.as_deref())),
        optional_strings(rows.iter().map(|row| row.best_bid.as_deref())),
        optional_strings(rows.iter().map(|row| row.best_ask.as_deref())),
        optional_strings(rows.iter().map(|row| row.hash.as_deref())),
        optional_strings(rows.iter().map(|row| row.fee_rate_bps.as_deref())),
        optional_strings(rows.iter().map(|row| row.transaction_hash.as_deref())),
        optional_strings(rows.iter().map(|row| row.old_tick_size.as_deref())),
        optional_strings(rows.iter().map(|row| row.new_tick_size.as_deref())),
        optional_strings(rows.iter().map(|row| row.winning_asset_id.as_deref())),
        optional_strings(rows.iter().map(|row| row.winning_outcome.as_deref())),
        optional_strings(
            rows.iter()
                .map(|row| row.close_reason_if_present.as_deref()),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.subscribed_asset_ids_for_market.as_deref()),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.resolved_event_asset_ids.as_deref()),
        ),
        optional_strings(rows.iter().map(|row| row.slug.as_deref())),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn polymarket_market_metadata_v3_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataV3Row],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        string_values(rows.iter().map(|row| row.metadata_row_kind.as_str())),
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        uint64_values(rows.iter().map(|row| row.collector_admission_sequence)),
        timestamp_values(rows.iter().map(|row| row.collector_admission_time_utc_ms)),
        string_values(rows.iter().map(|row| row.metadata_origin.as_str())),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
        string_values(rows.iter().map(|row| row.slug.as_str())),
        string_values(rows.iter().map(|row| row.condition_id.as_str())),
        string_values(rows.iter().map(|row| row.clob_market_id.as_str())),
        string_values(rows.iter().map(|row| row.asset_ids.as_str())),
        timestamp_values(rows.iter().map(|row| row.market_start_utc)),
        timestamp_values(rows.iter().map(|row| row.market_end_utc)),
        timestamp_values(rows.iter().map(|row| row.fixed_identity_verified_at_utc)),
        string_values(rows.iter().map(|row| row.market_lifecycle_state.as_str())),
        string_values(rows.iter().map(|row| row.market_runtime_role.as_str())),
        optional_strings(rows.iter().map(|row| row.source_endpoint.as_deref())),
        optional_strings(rows.iter().map(|row| {
            row.source_endpoint_unavailable_reason
                .map(|value| value.as_str())
        })),
        optional_strings(
            rows.iter()
                .map(|row| row.source_evidence_origin.map(|value| value.as_str())),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.source_evidence_event_type.map(|value| value.as_str())),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.source_evidence_channel.map(|value| value.as_str())),
        ),
        optional_timestamp_values(rows.iter().map(|row| row.source_evidence_received_at_utc)),
        optional_timestamp_values(
            rows.iter()
                .map(|row| row.row_evidence_validation_completed_at_utc),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.source_evidence_payload_json.as_deref()),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.source_evidence_raw_sha256.as_deref()),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.market_lifecycle_transition.map(|value| value.as_str())),
        ),
        optional_timestamp_values(rows.iter().map(|row| row.lifecycle_scheduled_at_utc)),
        optional_timestamp_values(rows.iter().map(|row| row.lifecycle_applied_at_utc)),
        optional_strings(rows.iter().map(|row| {
            row.market_runtime_role_transition
                .map(|value| value.as_str())
        })),
        optional_timestamp_values(rows.iter().map(|row| row.runtime_role_scheduled_at_utc)),
        optional_timestamp_values(rows.iter().map(|row| row.runtime_role_applied_at_utc)),
        optional_timestamp_values(rows.iter().map(|row| row.metadata_fetch_start_utc)),
        optional_bool_values(rows.iter().map(|row| row.closed)),
        optional_strings(rows.iter().map(|row| row.closed_time_raw.as_deref())),
        optional_timestamp_values(rows.iter().map(|row| row.closed_time_utc)),
        optional_bool_values(rows.iter().map(|row| row.closed_time_check_passed)),
        optional_strings(rows.iter().map(|row| {
            row.closed_time_check_failure_reason
                .map(|value| value.as_str())
        })),
        optional_bool_values(rows.iter().map(|row| row.automatically_resolved)),
        optional_strings(rows.iter().map(|row| row.uma_resolution_status.as_deref())),
        optional_strings(rows.iter().map(|row| row.outcomes_raw.as_deref())),
        optional_strings(rows.iter().map(|row| row.outcome_prices_raw.as_deref())),
        optional_bool_values(rows.iter().map(|row| row.active)),
        optional_strings(
            rows.iter()
                .map(|row| row.active_unavailable_reason.map(|value| value.as_str())),
        ),
        optional_bool_values(rows.iter().map(|row| row.accepting_orders)),
        optional_strings(
            rows.iter()
                .map(|row| row.ws_platform_timestamp_raw.as_deref()),
        ),
        optional_timestamp_values(rows.iter().map(|row| row.ws_platform_timestamp_utc)),
        optional_strings(rows.iter().map(|row| row.winning_asset_id.as_deref())),
        optional_strings(rows.iter().map(|row| row.winning_outcome.as_deref())),
        optional_strings(
            rows.iter()
                .map(|row| row.resolution_evidence_source.map(|value| value.as_str())),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.resolution_evidence_raw_sha256.as_deref()),
        ),
        optional_strings(
            rows.iter()
                .map(|row| row.terminal_claim_owner.map(|value| value.as_str())),
        ),
        optional_uint8_values(rows.iter().map(|row| row.resolution_poll_attempt_ordinal)),
        optional_timestamp_values(rows.iter().map(|row| row.resolution_poll_scheduled_utc)),
        optional_strings(
            rows.iter()
                .map(|row| row.resolution_poll_result.map(|value| value.as_str())),
        ),
        optional_strings(rows.iter().map(|row| {
            row.resolution_transport_error_class
                .map(|value| value.as_str())
        })),
        optional_uint16_values(rows.iter().map(|row| row.resolution_transport_http_status)),
        optional_strings(rows.iter().map(|row| {
            row.resolution_integrity_error_class
                .map(|value| value.as_str())
        })),
        optional_timestamp_values(rows.iter().map(|row| row.resolution_budget_horizon_utc)),
        optional_strings(
            rows.iter()
                .map(|row| row.resolution_followup_status.map(|value| value.as_str())),
        ),
        optional_uint32_values(rows.iter().map(|row| row.resolution_poll_attempts_recorded)),
        optional_uint32_values(rows.iter().map(|row| row.resolution_valid_response_count)),
        optional_uint32_values(
            rows.iter()
                .map(|row| row.resolution_transport_unavailable_count),
        ),
        optional_uint32_values(rows.iter().map(|row| row.resolution_missed_slot_count)),
        optional_timestamp_values(rows.iter().map(|row| row.last_valid_metadata_received_utc)),
        optional_bool_values(rows.iter().map(|row| row.terminal_predicate_passed)),
        optional_strings(rows.iter().map(|row| {
            row.terminal_predicate_failure_reason
                .map(|value| value.as_str())
        })),
        optional_strings(rows.iter().map(|row| {
            row.response_fields_unavailable_reason
                .map(|value| value.as_str())
        })),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn binance_depth_snapshot_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[BinanceDepthSnapshotHourlyRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        timestamp_values(rows.iter().map(|row| row.snapshot_fetch_start_utc)),
        uint64_values(rows.iter().map(|row| row.last_update_id)),
        string_values(rows.iter().map(|row| row.bids.as_str())),
        string_values(rows.iter().map(|row| row.asks.as_str())),
        string_values(rows.iter().map(|row| row.symbol.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.snapshot_status.as_str())),
        optional_strings(
            rows.iter()
                .map(|row| row.source_sequence_epoch_id.as_deref()),
        ),
        optional_uint64_values(rows.iter().map(|row| row.connection_generation)),
        optional_uint64_values(rows.iter().map(|row| row.depth_ingress_sequence)),
        optional_uint64_values(rows.iter().map(|row| row.collector_admission_sequence)),
        optional_timestamp_values(rows.iter().map(|row| row.collector_admission_time_utc_ms)),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn binance_depth_update_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[BinanceDepthUpdateHourlyRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        string_values(rows.iter().map(|row| row.event_type.as_str())),
        timestamp_values(rows.iter().map(|row| row.event_time)),
        string_values(rows.iter().map(|row| row.symbol.as_str())),
        uint64_values(rows.iter().map(|row| row.first_update_id)),
        uint64_values(rows.iter().map(|row| row.final_update_id)),
        string_values(rows.iter().map(|row| row.bids.as_str())),
        string_values(rows.iter().map(|row| row.asks.as_str())),
        string_values(rows.iter().map(|row| row.stream.as_str())),
        optional_uint64_values(rows.iter().map(|row| row.snapshot_last_update_id)),
        optional_uint64_values(rows.iter().map(|row| row.local_u_before)),
        string_values(rows.iter().map(|row| row.stitch_verdict.as_str())),
        optional_strings(rows.iter().map(|row| row.stitch_evidence.as_deref())),
        optional_strings(rows.iter().map(|row| row.stitch_gap_reason.as_deref())),
        optional_strings(
            rows.iter()
                .map(|row| row.source_sequence_epoch_id.as_deref()),
        ),
        optional_uint64_values(rows.iter().map(|row| row.connection_generation)),
        optional_uint64_values(rows.iter().map(|row| row.depth_ingress_sequence)),
        optional_uint64_values(rows.iter().map(|row| row.collector_admission_sequence)),
        optional_timestamp_values(rows.iter().map(|row| row.collector_admission_time_utc_ms)),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn binance_aggtrade_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[BinanceAggTradeHourlyRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        string_values(rows.iter().map(|row| row.record_kind.as_str())),
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        optional_timestamp_values(rows.iter().map(|row| row.websocket_timestamp_received)),
        optional_timestamp_values(rows.iter().map(|row| row.rest_backfill_timestamp_received)),
        string_values(rows.iter().map(|row| row.ingest_source.as_str())),
        optional_strings(rows.iter().map(|row| row.event_type.as_deref())),
        optional_timestamp_values(rows.iter().map(|row| row.event_time)),
        optional_timestamp_values(rows.iter().map(|row| row.trade_time)),
        optional_strings(rows.iter().map(|row| row.symbol.as_deref())),
        optional_uint64_values(rows.iter().map(|row| row.aggregate_trade_id)),
        optional_strings(rows.iter().map(|row| row.price.as_deref())),
        optional_strings(rows.iter().map(|row| row.quantity.as_deref())),
        optional_uint64_values(rows.iter().map(|row| row.first_trade_id)),
        optional_uint64_values(rows.iter().map(|row| row.last_trade_id)),
        optional_boolean_values(rows.iter().map(|row| row.buyer_is_maker)),
        optional_strings(rows.iter().map(|row| row.stream.as_deref())),
        optional_boolean_values(rows.iter().map(|row| row.ws_ignore)),
        optional_boolean_values(rows.iter().map(|row| row.rest_was_best_price_match)),
        string_values(
            rows.iter()
                .map(|row| row.source_native_payload_json.as_str()),
        ),
        string_values(rows.iter().map(|row| row.source_sequence_epoch_id.as_str())),
        uint64_values(rows.iter().map(|row| row.connection_generation)),
        optional_uint64_values(rows.iter().map(|row| row.websocket_ingress_sequence)),
        optional_uint64_values(rows.iter().map(|row| row.rest_response_ingress_sequence)),
        optional_uint64_values(rows.iter().map(|row| row.rest_response_item_index)),
        uint64_values(rows.iter().map(|row| row.collector_admission_sequence)),
        timestamp_values(rows.iter().map(|row| row.collector_admission_time_utc_ms)),
        optional_strings(rows.iter().map(|row| row.invalid_reason_code.as_deref())),
        optional_strings(rows.iter().map(|row| row.invalid_reason_detail.as_deref())),
        optional_strings(rows.iter().map(|row| row.payload_encoding.as_deref())),
        optional_uint64_values(rows.iter().map(|row| row.payload_length_bytes)),
        optional_strings(rows.iter().map(|row| row.payload_sha256.as_deref())),
        optional_strings(rows.iter().map(|row| row.payload_capture_status.as_deref())),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn rtds_chainlink_btc_usd_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[RtdsChainlinkBtcUsdHourlyRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        timestamp_values(rows.iter().map(|row| row.message_timestamp)),
        timestamp_values(rows.iter().map(|row| row.payload_timestamp)),
        optional_int64_values(rows.iter().map(|row| row.age_ms)),
        string_values(rows.iter().map(|row| row.symbol.as_str())),
        string_values(rows.iter().map(|row| row.value.as_str())),
        string_values(rows.iter().map(|row| row.topic.as_str())),
        string_values(rows.iter().map(|row| row.message_type.as_str())),
        string_values(rows.iter().map(|row| row.freshness_status.as_str())),
        string_values(rows.iter().map(|row| row.subscription_filter.as_str())),
        string_values(
            rows.iter()
                .map(|row| row.source_native_payload_json.as_str()),
        ),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn timestamp_values(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from_iter_values(values).with_timezone("UTC"))
}

fn optional_timestamp_values(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from_iter(values).with_timezone("UTC"))
}

fn uint64_values(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}

fn optional_uint64_values(values: impl Iterator<Item = Option<u64>>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter(values))
}

fn optional_uint8_values(values: impl Iterator<Item = Option<u8>>) -> ArrayRef {
    Arc::new(UInt8Array::from_iter(values))
}

fn optional_uint16_values(values: impl Iterator<Item = Option<u16>>) -> ArrayRef {
    Arc::new(UInt16Array::from_iter(values))
}

fn optional_uint32_values(values: impl Iterator<Item = Option<u32>>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter(values))
}

fn optional_int64_values(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from_iter(values))
}

fn optional_boolean_values(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values))
}

fn string_values<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn optional_bool_values(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_v3_followup_summary(admission_sequence: u64) -> PolymarketMarketMetadataV3Row {
        const MARKET_START: i64 = 1_783_392_300_000;
        const MARKET_END: i64 = MARKET_START + 300_000;
        const HORIZON: i64 = MARKET_END + 480_000;
        let draft = serde_json::json!({
            "metadata_row_kind": "resolution_followup_summary",
            "timestamp_received": HORIZON + 30,
            "metadata_origin": "local_followup_scheduler",
            "collector_host_id": "collector-host",
            "collector_region": "eu-west-1",
            "schema_version": POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            "slug": "btc-updown-5m-1783392300",
            "condition_id": "0x01",
            "clob_market_id": "clob-market-1",
            "asset_ids": "[\"101\",\"102\"]",
            "market_start_utc": MARKET_START,
            "market_end_utc": MARKET_END,
            "fixed_identity_verified_at_utc": MARKET_START - 120_000,
            "market_lifecycle_state": "active",
            "market_runtime_role": "none",
            "source_endpoint_unavailable_reason": "not_applicable_no_source_evidence",
            "market_runtime_role_transition": "resolution_pending_to_none",
            "runtime_role_applied_at_utc": HORIZON + 20,
            "active_unavailable_reason": "not_applicable_for_row_kind",
            "resolution_budget_horizon_utc": HORIZON,
            "resolution_followup_status": "budget_exhausted_after_valid_nonterminal",
            "resolution_poll_attempts_recorded": 6,
            "resolution_valid_response_count": 4,
            "resolution_transport_unavailable_count": 1,
            "resolution_missed_slot_count": 1,
            "last_valid_metadata_received_utc": HORIZON - 60_000,
            "response_fields_unavailable_reason": "not_applicable_for_row_kind"
        });
        PolymarketMarketMetadataV3Row::from_draft_json_with_admission(
            &serde_json::to_string(&draft).unwrap(),
            admission_sequence,
            HORIZON + 40,
        )
        .unwrap()
    }

    #[test]
    fn depth_writer_internal_factory_cannot_bypass_version_or_schema_guards() {
        let legacy = validate_typed_writer_schema(
            "binance_depth_update",
            BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
            binance_depth_update_schema().as_ref(),
        )
        .unwrap_err();
        assert!(legacy.to_string().contains("read-only legacy depth schema"));

        let mismatched = validate_typed_writer_schema(
            "binance_depth_update",
            BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            binance_depth_snapshot_schema().as_ref(),
        )
        .unwrap_err();
        assert!(mismatched.to_string().contains("writer schema mismatch"));

        validate_typed_writer_schema(
            "binance_depth_update",
            BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            binance_depth_update_schema().as_ref(),
        )
        .unwrap();
    }

    #[test]
    fn v3_logical_bytes_writer_keeps_strict_admission_sequence_authority() {
        let root = tempfile::tempdir().unwrap();
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            root.path(),
            "metadata-v3-logical-bytes",
            POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            "polymarket_market_metadata",
        )
        .unwrap();
        let row = valid_v3_followup_summary(2);
        assert!(row.validate().is_complete());
        writer
            .write_polymarket_market_metadata_v3_rows_with_logical_bytes(
                std::slice::from_ref(&row),
                512,
            )
            .unwrap();

        let error = writer
            .write_polymarket_market_metadata_v3_rows_with_logical_bytes(&[row], 512)
            .unwrap_err();
        assert!(error.to_string().contains("strictly increasing"));
    }
}
