use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use sha2::{Digest as _, Sha256};

use crate::event::SourceKind;
#[cfg(test)]
use crate::polymarket_market_metadata_layout::PolymarketMarketMetadataV4Row;
use crate::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
    POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION, PolymarketMarketMetadataRow,
    PolymarketMarketMetadataV5Row, polymarket_market_metadata_v5_schema,
};
use crate::polymarket_market_metadata_v6_layout::{
    POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION, PolymarketMarketMetadataV6Row,
    polymarket_market_metadata_v6_rows_to_batch, polymarket_market_metadata_v6_schema,
};
use crate::polymarket_orderbook_v3::{
    POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketOrderbookV3Message,
    polymarket_orderbook_v3_messages_to_batch, polymarket_orderbook_v3_schema,
};
use crate::source_hourly_layout::{
    BINANCE_AGGTRADE_SCHEMA_VERSION, BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
    BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION, BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
    BINANCE_DEPTH_UPDATE_SCHEMA_VERSION, BinanceAggTradeHourlyRow,
    BinanceDepthOrderingClassification, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RTDS_CHAINLINK_TWAP_SCHEMA_VERSION, RtdsChainlinkBtcUsdHourlyRow, RtdsChainlinkTwapHourlyRow,
    binance_aggtrade_schema, binance_depth_snapshot_schema, binance_depth_update_schema,
    rtds_chainlink_btc_usd_schema, rtds_chainlink_twap_schema,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedSegment {
    pub final_path: PathBuf,
    pub row_count: u64,
    pub sha256: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PolymarketOrderbookV3WriteSequenceState {
    last_by_epoch: HashMap<String, u64>,
}

impl PolymarketOrderbookV3WriteSequenceState {
    fn preview(&self, messages: &[PolymarketOrderbookV3Message]) -> Result<HashMap<String, u64>> {
        let mut touched = HashMap::new();
        for message in messages {
            let epoch = &message.envelope.source_sequence_epoch_id;
            let sequence = message.envelope.collector_admission_sequence;
            if touched
                .get(epoch)
                .or_else(|| self.last_by_epoch.get(epoch))
                .is_some_and(|previous| sequence <= *previous)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Polymarket orderbook v3 admission sequence is not strictly increasing within epoch:epoch={epoch}:sequence={sequence}"
                    ),
                )
                .into());
            }
            touched.insert(epoch.clone(), sequence);
        }
        Ok(touched)
    }

    fn commit(&mut self, touched: HashMap<String, u64>) {
        self.last_by_epoch.extend(touched);
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
    pub fn current_schema_version(source_kind: &str) -> Result<&'static str> {
        let source_kind = source_kind
            .parse::<SourceKind>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok(match source_kind {
            SourceKind::PolymarketMarket => POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            SourceKind::PolymarketMarketMetadata => POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
            SourceKind::BinanceDepthSnapshot => BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
            SourceKind::BinanceDepthUpdate => BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            SourceKind::BinanceAggTrade => BINANCE_AGGTRADE_SCHEMA_VERSION,
            SourceKind::PolymarketRtdsChainlinkBtcUsd => "polymarket_rtds_chainlink_btc_usd_v4",
            SourceKind::PolymarketRtdsChainlinkTwap => RTDS_CHAINLINK_TWAP_SCHEMA_VERSION,
        })
    }

    pub fn create_for_current_source(root: &Path, stem: &str, source_kind: &str) -> Result<Self> {
        let schema_version = Self::current_schema_version(source_kind)?;
        Self::create(root, stem, schema_version, source_kind, 3)
    }

    pub fn create_for_typed_source(
        root: &Path,
        stem: &str,
        schema_version: &str,
        source_kind: &str,
    ) -> Result<Self> {
        Self::create(root, stem, schema_version, source_kind, 3)
    }

    fn create(
        root: &Path,
        stem: &str,
        schema_version: &str,
        source_kind: &str,
        zstd_level: i32,
    ) -> Result<Self> {
        let schema = typed_writer_schema(source_kind, schema_version)?;
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

    pub(crate) fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub fn write_polymarket_orderbook_v3_messages(
        &mut self,
        messages: &[PolymarketOrderbookV3Message],
        sequence_state: &mut PolymarketOrderbookV3WriteSequenceState,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        if self.schema_version != POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Polymarket orderbook v3 messages require a v3 writer",
            )
            .into());
        }
        let batch = polymarket_orderbook_v3_messages_to_batch(messages)?;
        let next = sequence_state.preview(messages)?;
        self.write_batch_chunks(&batch)?;
        sequence_state.commit(next);
        Ok(())
    }

    pub fn write_polymarket_market_metadata_v5_rows(
        &mut self,
        rows: &[PolymarketMarketMetadataV5Row],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v5_rows(rows)?;
        let batch = polymarket_market_metadata_v5_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch(&batch)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_polymarket_market_metadata_v5_rows_with_logical_bytes(
        &mut self,
        rows: &[PolymarketMarketMetadataV5Row],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v5_rows(rows)?;
        let batch = polymarket_market_metadata_v5_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    fn validate_polymarket_metadata_v5_rows(
        &self,
        rows: &[PolymarketMarketMetadataV5Row],
    ) -> Result<u64> {
        if self.schema_version != POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 Polymarket metadata rows require a v5 writer",
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
                        "invalid Polymarket metadata v5 row:{}",
                        validation.violations.join(",")
                    ),
                )
                .into());
            }
            let sequence = row.common().collector_admission_sequence;
            if previous.is_some_and(|previous| sequence <= previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Polymarket metadata v5 admission sequence must be strictly increasing",
                )
                .into());
            }
            previous = Some(sequence);
        }
        Ok(previous.expect("non-empty rows have a last admission sequence"))
    }

    pub fn write_polymarket_market_metadata_v6_rows(
        &mut self,
        rows: &[PolymarketMarketMetadataV6Row],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v6_rows(rows)?;
        let batch = polymarket_market_metadata_v6_rows_to_batch(Arc::clone(&self.schema), rows)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.write_batch(&batch)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn write_polymarket_market_metadata_v6_rows_with_logical_bytes(
        &mut self,
        rows: &[PolymarketMarketMetadataV6Row],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let last_sequence = self.validate_polymarket_metadata_v6_rows(rows)?;
        let batch = polymarket_market_metadata_v6_rows_to_batch(Arc::clone(&self.schema), rows)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)?;
        self.last_polymarket_metadata_admission_sequence = Some(last_sequence);
        Ok(())
    }

    fn validate_polymarket_metadata_v6_rows(
        &self,
        rows: &[PolymarketMarketMetadataV6Row],
    ) -> Result<u64> {
        if self.schema_version != POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v6 Polymarket metadata rows require a v6 writer",
            )
            .into());
        }
        let mut previous = self.last_polymarket_metadata_admission_sequence;
        for row in rows {
            row.validate()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let sequence = row.collector_admission_sequence();
            if previous.is_some_and(|previous| sequence <= previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Polymarket metadata v6 admission sequence must be strictly increasing",
                )
                .into());
            }
            previous = Some(sequence);
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

    pub fn write_rtds_chainlink_twap_rows_with_logical_bytes(
        &mut self,
        rows: &[RtdsChainlinkTwapHourlyRow],
        logical_bytes: u64,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = rtds_chainlink_twap_rows_to_batch(Arc::clone(&self.schema), rows)?;
        self.write_batch_with_logical_bytes(&batch, logical_bytes)
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.schema_version == POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Polymarket orderbook v3 batches must use the canonical typed message writer",
            )
            .into());
        }
        self.write_batch_chunks(batch)
    }

    fn write_batch_chunks(&mut self, batch: &RecordBatch) -> Result<()> {
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
        "binance_aggtrade"
            | "binance_depth_snapshot"
            | "binance_depth_update"
            | "polymarket_rtds_chainlink_twap"
    ) || (source_kind == "polymarket_market_metadata"
        && matches!(
            schema_version,
            POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION
                | POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION
                | POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION
                | POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION
        ))
        || (source_kind == "polymarket_market"
            && schema_version == POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION)
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

fn polymarket_orderbook_schema_for_version(schema_version: &str) -> Result<Arc<Schema>> {
    match schema_version {
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION => Ok(polymarket_orderbook_v3_schema()),
        "polymarket_orderbook_v2" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "read-only legacy Polymarket orderbook schema cannot be opened for writing:schema_version=polymarket_orderbook_v2",
        )
        .into()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unsupported Polymarket orderbook schema version for writer:schema_version={schema_version}"
            ),
        )
        .into()),
    }
}

fn typed_writer_schema(source_kind: &str, schema_version: &str) -> Result<Arc<Schema>> {
    match source_kind {
        "polymarket_market" => polymarket_orderbook_schema_for_version(schema_version),
        "polymarket_market_metadata" => match schema_version {
            POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION => {
                Ok(polymarket_market_metadata_v6_schema())
            }
            POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION => {
                Ok(polymarket_market_metadata_v5_schema())
            }
            _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unsupported Polymarket metadata schema version for writer:schema_version={schema_version}"
                    ),
                )
                .into()),
        },
        "binance_depth_snapshot" => {
            validate_current_depth_schema_version(
                source_kind,
                schema_version,
                BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
                BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
            )?;
            Ok(binance_depth_snapshot_schema())
        }
        "binance_depth_update" => {
            validate_current_depth_schema_version(
                source_kind,
                schema_version,
                BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
                BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
            )?;
            Ok(binance_depth_update_schema())
        }
        "binance_aggtrade" => {
            if schema_version != BINANCE_AGGTRADE_SCHEMA_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unsupported Binance aggTrade schema version for writer:schema_version={schema_version}:expected={BINANCE_AGGTRADE_SCHEMA_VERSION}"
                    ),
                )
                .into());
            }
            Ok(binance_aggtrade_schema())
        }
        "polymarket_rtds_chainlink_btc_usd" => Ok(rtds_chainlink_btc_usd_schema()),
        "polymarket_rtds_chainlink_twap" => Ok(rtds_chainlink_twap_schema()),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported typed source kind: {other}"),
        )
        .into()),
    }
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

fn polymarket_market_metadata_common_columns(
    rows: &[&PolymarketMarketMetadataRow],
    include_legacy_resolution_digest: bool,
) -> Vec<ArrayRef> {
    let mut columns = vec![
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
    ];
    if include_legacy_resolution_digest {
        columns.push(optional_strings(
            rows.iter()
                .map(|row| row.resolution_evidence_raw_sha256.as_deref()),
        ));
    }
    columns.extend([
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
    ]);
    columns
}

#[cfg(test)]
pub(crate) fn polymarket_market_metadata_v3_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataRow],
) -> Result<RecordBatch> {
    let common_rows = rows.iter().collect::<Vec<_>>();
    let columns = polymarket_market_metadata_common_columns(&common_rows, true);
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
pub(crate) fn polymarket_market_metadata_v4_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataV4Row],
) -> Result<RecordBatch> {
    let common_rows = rows.iter().map(|row| &row.common).collect::<Vec<_>>();
    let mut columns = polymarket_market_metadata_common_columns(&common_rows, true);
    columns
        .push(optional_strings(rows.iter().map(|row| {
            row.ws_claimant_source_sequence_epoch_id.as_deref()
        })));
    columns.push(optional_uint64_values(
        rows.iter()
            .map(|row| row.ws_claimant_collector_admission_sequence),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn polymarket_market_metadata_v5_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataV5Row],
) -> Result<RecordBatch> {
    let common_rows = rows
        .iter()
        .map(PolymarketMarketMetadataV5Row::common)
        .collect::<Vec<_>>();
    let mut columns = polymarket_market_metadata_common_columns(&common_rows, false);
    columns
        .push(optional_strings(rows.iter().map(|row| {
            row.ws_claimant_source_sequence_epoch_id.as_deref()
        })));
    columns.push(optional_uint64_values(
        rows.iter()
            .map(|row| row.ws_claimant_collector_admission_sequence),
    ));
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
        optional_bool_values(rows.iter().map(|row| row.buyer_is_maker)),
        optional_strings(rows.iter().map(|row| row.stream.as_deref())),
        optional_bool_values(rows.iter().map(|row| row.ws_ignore)),
        optional_bool_values(rows.iter().map(|row| row.rest_was_best_price_match)),
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
    Ok(RecordBatch::try_new(
        schema,
        rtds_chainlink_columns(rows.iter()),
    )?)
}

fn rtds_chainlink_columns<'a>(
    rows: impl Iterator<Item = &'a RtdsChainlinkBtcUsdHourlyRow> + Clone,
) -> Vec<ArrayRef> {
    vec![
        timestamp_values(rows.clone().map(|row| row.timestamp_received)),
        timestamp_values(rows.clone().map(|row| row.message_timestamp)),
        timestamp_values(rows.clone().map(|row| row.payload_timestamp)),
        optional_int64_values(rows.clone().map(|row| row.age_ms)),
        string_values(rows.clone().map(|row| row.symbol.as_str())),
        string_values(rows.clone().map(|row| row.value.as_str())),
        string_values(rows.clone().map(|row| row.topic.as_str())),
        string_values(rows.clone().map(|row| row.message_type.as_str())),
        string_values(rows.clone().map(|row| row.freshness_status.as_str())),
        string_values(rows.clone().map(|row| row.subscription_filter.as_str())),
        string_values(
            rows.clone()
                .map(|row| row.source_native_payload_json.as_str()),
        ),
        string_values(rows.clone().map(|row| row.collector_host_id.as_str())),
        string_values(rows.clone().map(|row| row.collector_region.as_str())),
        string_values(rows.clone().map(|row| row.source_endpoint.as_str())),
        string_values(rows.map(|row| row.schema_version.as_str())),
    ]
}

fn rtds_chainlink_twap_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[RtdsChainlinkTwapHourlyRow],
) -> Result<RecordBatch> {
    let mut columns = rtds_chainlink_columns(rows.iter().map(|row| &row.common));
    columns.extend([
        string_values(rows.iter().map(|row| row.full_accuracy_value.as_str())),
        uint64_values(rows.iter().map(|row| row.window_s)),
        string_values(rows.iter().map(|row| row.source_sequence_epoch_id.as_str())),
        uint64_values(rows.iter().map(|row| row.collector_admission_sequence)),
        timestamp_values(rows.iter().map(|row| row.collector_admission_time_utc_ms)),
    ]);
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

fn string_values<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn optional_bool_values(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket_orderbook_v3::{
        PolymarketOrderbookV3Admission, PolymarketOrderbookV3Projection,
    };
    use polymarket_client_sdk_v2::clob::ws::WsMessage;
    use tempfile::tempdir;

    #[test]
    fn polymarket_v3_sequence_state_does_not_advance_when_write_fails() {
        let sdk_message = serde_json::from_str::<WsMessage>(
            r#"{
                "event_type":"book",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
                "asset_id":"101",
                "timestamp":"1783392404600",
                "bids":[{"price":"0.49","size":"10"}],
                "asks":[],
                "last_trade_price":"0.50"
            }"#,
        )
        .unwrap();
        let message =
            PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk_message)
                .unwrap()
                .unwrap()
                .admit(PolymarketOrderbookV3Admission {
                    timestamp_received: 1_783_392_404_599,
                    collector_host_id: "collector-host".to_owned(),
                    collector_region: "eu-west-1".to_owned(),
                    source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
                        .to_owned(),
                    source_sequence_epoch_id: "source-epoch-a".to_owned(),
                    collector_admission_sequence: 1,
                    collector_admission_time_utc_ms: 1_783_392_404_600,
                })
                .unwrap();
        let tmp = tempdir().unwrap();
        let mut sequence_state = PolymarketOrderbookV3WriteSequenceState::default();
        let mut failed_writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            "failed-write",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            "polymarket_market",
        )
        .unwrap();
        failed_writer.writer.take();

        let error = failed_writer
            .write_polymarket_orderbook_v3_messages(
                std::slice::from_ref(&message),
                &mut sequence_state,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("typed parquet segment already finalized"),
            "{error}"
        );

        let mut retry_writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            "retry-write",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            "polymarket_market",
        )
        .unwrap();
        retry_writer
            .write_polymarket_orderbook_v3_messages(&[message], &mut sequence_state)
            .unwrap();
        assert_eq!(retry_writer.finalize().unwrap().row_count, 1);
    }
}
