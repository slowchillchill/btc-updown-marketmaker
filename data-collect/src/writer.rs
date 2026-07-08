use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataRow, polymarket_market_metadata_schema,
};
use crate::polymarket_orderbook_layout::{PolymarketOrderbookRow, polymarket_orderbook_schema};
use crate::source_hourly_layout::{
    BinanceAggTradeHourlyRow, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RtdsChainlinkBtcUsdHourlyRow, binance_aggtrade_schema, binance_depth_snapshot_schema,
    binance_depth_update_schema, rtds_chainlink_btc_usd_schema,
};

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

pub struct TypedParquetSegmentWriter {
    running_path: PathBuf,
    digesting_path: PathBuf,
    final_path: PathBuf,
    schema_version: String,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    row_count: u64,
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
            "polymarket_market_metadata" => polymarket_market_metadata_schema(),
            "binance_depth_snapshot" => binance_depth_snapshot_schema(),
            "binance_depth_update" => binance_depth_update_schema(),
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

    pub fn create_with_schema(
        root: &Path,
        stem: &str,
        schema_version: &str,
        source_kind: &str,
        schema: Arc<Schema>,
        zstd_level: i32,
    ) -> Result<Self> {
        validate_segment_stem(stem)?;
        fs::create_dir_all(root)?;

        let running_path = root.join(format!("{stem}-running.parquet"));
        let digesting_path = root.join(format!("{stem}-digesting.parquet"));
        let final_path = root.join(format!("{stem}.parquet"));
        if final_path.exists() || running_path.exists() || digesting_path.exists() {
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
            digesting_path,
            final_path,
            schema_version: schema_version.to_owned(),
            schema,
            writer: Some(writer),
            row_count: 0,
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

    pub fn write_polymarket_market_metadata_rows(
        &mut self,
        rows: &[PolymarketMarketMetadataRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = polymarket_market_metadata_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
    }

    pub fn write_binance_depth_snapshot_rows(
        &mut self,
        rows: &[BinanceDepthSnapshotHourlyRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = binance_depth_snapshot_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
    }

    pub fn write_binance_depth_update_rows(
        &mut self,
        rows: &[BinanceDepthUpdateHourlyRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = binance_depth_update_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
    }

    pub fn write_binance_aggtrade_rows(&mut self, rows: &[BinanceAggTradeHourlyRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = binance_aggtrade_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)
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

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record batch schema does not match typed parquet writer schema",
            )
            .into());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("typed parquet segment already finalized"))?;
        writer.write(batch)?;
        self.row_count += batch.num_rows() as u64;
        Ok(())
    }

    pub fn finalize(mut self) -> Result<PublishedSegment> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("typed parquet segment already finalized"))?;
        writer.close()?;
        fs::rename(&self.running_path, &self.digesting_path)?;
        let sha256 = sha256_file(&self.digesting_path)?;
        fs::rename(&self.digesting_path, &self.final_path)?;
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
    Ok(WriterProperties::builder()
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
            KeyValue::new("sort_order".to_owned(), "receive_order".to_owned()),
        ]))
        .build())
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

fn polymarket_market_metadata_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataRow],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        optional_timestamp_values(rows.iter().map(|row| row.metadata_fetch_start_utc)),
        optional_timestamp_values(rows.iter().map(|row| row.metadata_recv_time_utc)),
        string_values(rows.iter().map(|row| row.slug.as_str())),
        string_values(rows.iter().map(|row| row.condition_id.as_str())),
        optional_strings(rows.iter().map(|row| row.clob_market_id.as_deref())),
        optional_strings(rows.iter().map(|row| row.asset_ids.as_deref())),
        optional_strings(rows.iter().map(|row| row.token_ids.as_deref())),
        optional_timestamp_values(rows.iter().map(|row| row.market_start_utc)),
        optional_timestamp_values(rows.iter().map(|row| row.market_end_utc)),
        optional_bool_values(rows.iter().map(|row| row.active)),
        optional_bool_values(rows.iter().map(|row| row.accepting_orders)),
        string_values(rows.iter().map(|row| row.market_lifecycle_state.as_str())),
        optional_strings(rows.iter().map(|row| row.tick_size.as_deref())),
        optional_strings(rows.iter().map(|row| row.min_size.as_deref())),
        optional_strings(rows.iter().map(|row| row.fee_schedule_raw.as_deref())),
        optional_strings(rows.iter().map(|row| row.reward_eligibility_raw.as_deref())),
        optional_strings(
            rows.iter()
                .map(|row| row.metadata_unavailable_reason.as_deref()),
        ),
        string_values(rows.iter().map(|row| row.metadata_source.as_str())),
        optional_bool_values(rows.iter().map(|row| row.same_condition_market_verified)),
        string_values(rows.iter().map(|row| row.collector_host_id.as_str())),
        string_values(rows.iter().map(|row| row.collector_region.as_str())),
        string_values(rows.iter().map(|row| row.source_endpoint.as_str())),
        string_values(rows.iter().map(|row| row.schema_version.as_str())),
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
        uint64_values(rows.iter().map(|row| row.snapshot_last_update_id)),
        optional_uint64_values(rows.iter().map(|row| row.local_u_before)),
        optional_uint64_values(rows.iter().map(|row| row.local_u_after)),
        string_values(rows.iter().map(|row| row.stitch_verdict.as_str())),
        optional_strings(rows.iter().map(|row| row.stitch_gap_reason.as_deref())),
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
        timestamp_values(rows.iter().map(|row| row.timestamp_received)),
        string_values(rows.iter().map(|row| row.event_type.as_str())),
        timestamp_values(rows.iter().map(|row| row.event_time)),
        string_values(rows.iter().map(|row| row.symbol.as_str())),
        uint64_values(rows.iter().map(|row| row.aggregate_trade_id)),
        string_values(rows.iter().map(|row| row.price.as_str())),
        string_values(rows.iter().map(|row| row.quantity.as_str())),
        uint64_values(rows.iter().map(|row| row.first_trade_id)),
        uint64_values(rows.iter().map(|row| row.last_trade_id)),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.buyer_is_maker)),
        )),
        string_values(rows.iter().map(|row| row.stream.as_str())),
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
        string_values(rows.iter().map(|row| row.symbol.as_str())),
        string_values(rows.iter().map(|row| row.value.as_str())),
        string_values(rows.iter().map(|row| row.topic.as_str())),
        string_values(rows.iter().map(|row| row.subscription_filter.as_str())),
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
