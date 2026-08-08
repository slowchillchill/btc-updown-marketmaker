//! 六类已准入 `typed_raw` Parquet 的严格 Arrow 边界与有界读取入口。
//!
//! 本模块只解释已冻结的物理布局并把行交给唯一规范核心。生产读取只能从
//! [`ValidatedDirectSourceParquetFileV3`] 开始；任意内存批次解码入口保持私有。

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use data_pipeline_core::typed_raw::{
    BinanceAggTradeCaptureStatus, BinanceAggTradeInvalidReason, BinanceAggTradePayloadEncoding,
    BinanceAggTradeRecord, BinanceAggTradeRow, BinanceDepthSnapshotRow, BinanceDepthStitchVerdict,
    BinanceDepthUpdateRow, MetadataActiveUnavailableReason, MetadataClosedTimeCheckFailureReason,
    MetadataMarketLifecycleState, MetadataMarketLifecycleTransition, MetadataMarketRuntimeRole,
    MetadataMarketRuntimeRoleTransition, MetadataResolutionEvidenceSource,
    MetadataResolutionFollowupStatus, MetadataResolutionIntegrityErrorClass,
    MetadataResolutionPollResult, MetadataResolutionTransportErrorClass,
    MetadataResponseFieldsUnavailableReason, MetadataSourceEndpointUnavailableReason,
    MetadataSourceEvidenceChannel, MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin,
    MetadataTerminalClaimOwner, MetadataTerminalPredicateFailureReason, PolymarketEventMessage,
    PolymarketMarketMetadataRow, PolymarketMetadataOrigin, PolymarketMetadataRowKind,
    PolymarketOrderbookEvent, PolymarketOrderbookRow, PolymarketPriceChangeEntry,
    PolymarketPriceLevel, PolymarketRtdsChainlinkBtcUsdRow, PolymarketSide, RtdsFreshnessStatus,
    TypedRawError, TypedRawRow, parse_depth_levels_json,
};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};

use crate::ValidatedDirectSourceParquetFileV3;

const POLYMARKET_ORDERBOOK_SOURCE: &str = "polymarket_market";
const POLYMARKET_ORDERBOOK_SCHEMA: &str = "polymarket_orderbook_v3";
const POLYMARKET_METADATA_SOURCE: &str = "polymarket_market_metadata";
const POLYMARKET_METADATA_SCHEMA: &str = "polymarket_market_metadata_v5";
const POLYMARKET_RTDS_SOURCE: &str = "polymarket_rtds_chainlink_btc_usd";
const POLYMARKET_RTDS_SCHEMA: &str = "polymarket_rtds_chainlink_btc_usd_v4";
const BINANCE_DEPTH_SNAPSHOT_SOURCE: &str = "binance_depth_snapshot";
const BINANCE_DEPTH_SNAPSHOT_SCHEMA: &str = "binance_depth_snapshot_v3";
const BINANCE_DEPTH_UPDATE_SOURCE: &str = "binance_depth_update";
const BINANCE_DEPTH_UPDATE_SCHEMA: &str = "binance_depth_update_v4";
const BINANCE_AGGTRADE_SOURCE: &str = "binance_aggtrade";
const BINANCE_AGGTRADE_SCHEMA: &str = "binance_aggtrade_v7";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TypedRawReadScope {
    received_start_utc_ms: i64,
    received_end_utc_ms: i64,
    polymarket_condition_id: String,
}

impl TypedRawReadScope {
    pub(crate) fn new(
        received_start_utc_ms: i64,
        received_end_utc_ms: i64,
        polymarket_condition_id: impl Into<String>,
    ) -> Self {
        Self {
            received_start_utc_ms,
            received_end_utc_ms,
            polymarket_condition_id: polymarket_condition_id.into(),
        }
    }

    fn contains_received_time(&self, value: i64) -> bool {
        self.received_start_utc_ms <= value && value < self.received_end_utc_ms
    }
}

/// 六类物理模式、Parquet 读取和逐行解码的统一失败类型。
#[derive(Debug)]
pub enum TypedRawReadError {
    InvalidBatchSize,
    UnsupportedSourceSchema {
        source_kind: String,
        schema_version: String,
    },
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Parquet {
        path: PathBuf,
        batch_index: Option<u64>,
        detail: String,
    },
    AdmittedFileChanged {
        path: PathBuf,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    AdmittedFileDigestMismatch {
        path: PathBuf,
        expected_sha256: String,
        actual_sha256: String,
    },
    FooterRowCountMismatch {
        path: PathBuf,
        expected_rows: u64,
        actual_rows: u64,
    },
    DecodeBatchBytesLimit {
        path: PathBuf,
        batch_index: u64,
        observed: usize,
        limit: usize,
    },
    PhysicalSchema {
        source_kind: String,
        schema_version: String,
        field_index: Option<usize>,
        detail: String,
    },
    Decode {
        source_kind: String,
        schema_version: String,
        column: Option<String>,
        row: u64,
        detail: String,
    },
}

impl fmt::Display for TypedRawReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBatchSize => write!(formatter, "typed_raw 批次行数必须为正数"),
            Self::UnsupportedSourceSchema {
                source_kind,
                schema_version,
            } => write!(
                formatter,
                "不支持的 typed_raw 来源/schema：{source_kind}/{schema_version}"
            ),
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "读取 typed_raw 文件 {} 失败：{source}",
                    path.display()
                )
            }
            Self::Parquet {
                path,
                batch_index,
                detail,
            } => {
                if let Some(batch_index) = batch_index {
                    write!(
                        formatter,
                        "读取 typed_raw Parquet {} 的批次 {batch_index} 失败：{detail}",
                        path.display()
                    )
                } else {
                    write!(
                        formatter,
                        "打开 typed_raw Parquet {} 失败：{detail}",
                        path.display()
                    )
                }
            }
            Self::AdmittedFileChanged {
                path,
                expected_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "已准入 typed_raw 文件 {} 的字节数发生变化：期望 {expected_bytes}，实际 {actual_bytes}",
                path.display()
            ),
            Self::AdmittedFileDigestMismatch {
                path,
                expected_sha256,
                actual_sha256,
            } => write!(
                formatter,
                "已准入 typed_raw 文件 {} 的 SHA-256 发生变化：期望 {expected_sha256}，实际 {actual_sha256}",
                path.display()
            ),
            Self::FooterRowCountMismatch {
                path,
                expected_rows,
                actual_rows,
            } => write!(
                formatter,
                "已准入 typed_raw 文件 {} 的 footer 行数不一致：期望 {expected_rows}，实际 {actual_rows}",
                path.display()
            ),
            Self::DecodeBatchBytesLimit {
                path,
                batch_index,
                observed,
                limit,
            } => write!(
                formatter,
                "typed_raw Parquet {} 的 Arrow 批次 {batch_index} 报告物理数组大小 {observed} 字节，超过上限 {limit}",
                path.display()
            ),
            Self::PhysicalSchema {
                source_kind,
                schema_version,
                field_index,
                detail,
            } => {
                if let Some(field_index) = field_index {
                    write!(
                        formatter,
                        "typed_raw {source_kind}/{schema_version} 的物理 schema 字段 #{field_index} 不匹配：{detail}"
                    )
                } else {
                    write!(
                        formatter,
                        "typed_raw {source_kind}/{schema_version} 的物理 schema 不匹配：{detail}"
                    )
                }
            }
            Self::Decode {
                source_kind,
                schema_version,
                column,
                row,
                detail,
            } => {
                if let Some(column) = column {
                    write!(
                        formatter,
                        "typed_raw {source_kind}/{schema_version} 第 {row} 行、列 {column} 解码失败：{detail}"
                    )
                } else {
                    write!(
                        formatter,
                        "typed_raw {source_kind}/{schema_version} 第 {row} 行解码失败：{detail}"
                    )
                }
            }
        }
    }
}

impl std::error::Error for TypedRawReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct DecodeContext<'a> {
    source_kind: &'a str,
    schema_version: &'a str,
    first_row_index: u64,
}

impl DecodeContext<'_> {
    fn error(
        self,
        local_row: usize,
        column: Option<&str>,
        detail: impl Into<String>,
    ) -> TypedRawReadError {
        TypedRawReadError::Decode {
            source_kind: self.source_kind.to_owned(),
            schema_version: self.schema_version.to_owned(),
            column: column.map(str::to_owned),
            row: self
                .first_row_index
                .saturating_add(u64::try_from(local_row).unwrap_or(u64::MAX)),
            detail: detail.into(),
        }
    }
}

fn expected_schema(
    source_kind: &str,
    schema_version: &str,
) -> Result<SchemaRef, TypedRawReadError> {
    let schema = match (source_kind, schema_version) {
        (POLYMARKET_ORDERBOOK_SOURCE, POLYMARKET_ORDERBOOK_SCHEMA) => polymarket_orderbook_schema(),
        (POLYMARKET_METADATA_SOURCE, POLYMARKET_METADATA_SCHEMA) => polymarket_metadata_schema(),
        (POLYMARKET_RTDS_SOURCE, POLYMARKET_RTDS_SCHEMA) => polymarket_rtds_schema(),
        (BINANCE_DEPTH_SNAPSHOT_SOURCE, BINANCE_DEPTH_SNAPSHOT_SCHEMA) => {
            binance_depth_snapshot_schema()
        }
        (BINANCE_DEPTH_UPDATE_SOURCE, BINANCE_DEPTH_UPDATE_SCHEMA) => binance_depth_update_schema(),
        (BINANCE_AGGTRADE_SOURCE, BINANCE_AGGTRADE_SCHEMA) => binance_aggtrade_schema(),
        _ => {
            return Err(TypedRawReadError::UnsupportedSourceSchema {
                source_kind: source_kind.to_owned(),
                schema_version: schema_version.to_owned(),
            });
        }
    };
    Ok(schema)
}

pub(crate) fn expected_projection_columns(
    source_kind: &str,
    schema_version: &str,
) -> Result<Vec<String>, TypedRawReadError> {
    Ok(expected_schema(source_kind, schema_version)?
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect())
}

fn validate_physical_schema(
    source_kind: &str,
    schema_version: &str,
    actual: &Schema,
) -> Result<(), TypedRawReadError> {
    let expected = expected_schema(source_kind, schema_version)?;
    if expected.fields().len() != actual.fields().len() {
        return Err(TypedRawReadError::PhysicalSchema {
            source_kind: source_kind.to_owned(),
            schema_version: schema_version.to_owned(),
            field_index: None,
            detail: format!(
                "字段数期望 {}，实际 {}",
                expected.fields().len(),
                actual.fields().len()
            ),
        });
    }
    for (index, (expected, actual)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        if expected.name() != actual.name()
            || expected.data_type() != actual.data_type()
            || expected.is_nullable() != actual.is_nullable()
        {
            return Err(TypedRawReadError::PhysicalSchema {
                source_kind: source_kind.to_owned(),
                schema_version: schema_version.to_owned(),
                field_index: Some(index),
                detail: format!("期望 {expected:?}，实际 {actual:?}"),
            });
        }
    }
    Ok(())
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}

fn list_of_struct_field(name: &str, fields: Fields) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(fields),
            false,
        ))),
        true,
    )
}

fn list_of_utf8_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, false))),
        true,
    )
}

fn polymarket_orderbook_schema() -> SchemaRef {
    let price_level_fields = Fields::from(vec![
        Field::new("price", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, false),
    ]);
    let price_change_fields = Fields::from(vec![
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("price", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("best_bid", DataType::Utf8, true),
        Field::new("best_ask", DataType::Utf8, true),
    ]);
    let event_message_fields = Fields::from(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("ticker", DataType::Utf8, true),
        Field::new("slug", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("timestamp", true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("asset_id", DataType::Utf8, true),
        list_of_struct_field("bids", price_level_fields.clone()),
        list_of_struct_field("asks", price_level_fields),
        Field::new("last_trade_price", DataType::Utf8, true),
        list_of_struct_field("price_changes", price_change_fields),
        Field::new("old_tick_size", DataType::Utf8, true),
        Field::new("new_tick_size", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("fee_rate_bps", DataType::Utf8, true),
        Field::new("best_bid", DataType::Utf8, true),
        Field::new("best_ask", DataType::Utf8, true),
        Field::new("spread", DataType::Utf8, true),
        Field::new("clob_market_id", DataType::Utf8, true),
        Field::new("question", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        list_of_utf8_field("asset_ids"),
        list_of_utf8_field("outcomes"),
        Field::new(
            "event_message",
            DataType::Struct(event_message_fields),
            true,
        ),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
    ]))
}

fn polymarket_metadata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metadata_row_kind", DataType::Utf8, false),
        timestamp_field("timestamp_received", false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("metadata_origin", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("clob_market_id", DataType::Utf8, false),
        Field::new("asset_ids", DataType::Utf8, false),
        timestamp_field("market_start_utc", false),
        timestamp_field("market_end_utc", false),
        timestamp_field("fixed_identity_verified_at_utc", false),
        Field::new("market_lifecycle_state", DataType::Utf8, false),
        Field::new("market_runtime_role", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, true),
        Field::new("source_endpoint_unavailable_reason", DataType::Utf8, true),
        Field::new("source_evidence_origin", DataType::Utf8, true),
        Field::new("source_evidence_event_type", DataType::Utf8, true),
        Field::new("source_evidence_channel", DataType::Utf8, true),
        timestamp_field("source_evidence_received_at_utc", true),
        timestamp_field("row_evidence_validation_completed_at_utc", true),
        Field::new("source_evidence_payload_json", DataType::Utf8, true),
        Field::new("source_evidence_raw_sha256", DataType::Utf8, true),
        Field::new("market_lifecycle_transition", DataType::Utf8, true),
        timestamp_field("lifecycle_scheduled_at_utc", true),
        timestamp_field("lifecycle_applied_at_utc", true),
        Field::new("market_runtime_role_transition", DataType::Utf8, true),
        timestamp_field("runtime_role_scheduled_at_utc", true),
        timestamp_field("runtime_role_applied_at_utc", true),
        timestamp_field("metadata_fetch_start_utc", true),
        Field::new("closed", DataType::Boolean, true),
        Field::new("closed_time_raw", DataType::Utf8, true),
        timestamp_field("closed_time_utc", true),
        Field::new("closed_time_check_passed", DataType::Boolean, true),
        Field::new("closed_time_check_failure_reason", DataType::Utf8, true),
        Field::new("automatically_resolved", DataType::Boolean, true),
        Field::new("uma_resolution_status", DataType::Utf8, true),
        Field::new("outcomes_raw", DataType::Utf8, true),
        Field::new("outcome_prices_raw", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("active_unavailable_reason", DataType::Utf8, true),
        Field::new("accepting_orders", DataType::Boolean, true),
        Field::new("ws_platform_timestamp_raw", DataType::Utf8, true),
        timestamp_field("ws_platform_timestamp_utc", true),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
        Field::new("resolution_evidence_source", DataType::Utf8, true),
        Field::new("terminal_claim_owner", DataType::Utf8, true),
        Field::new("resolution_poll_attempt_ordinal", DataType::UInt8, true),
        timestamp_field("resolution_poll_scheduled_utc", true),
        Field::new("resolution_poll_result", DataType::Utf8, true),
        Field::new("resolution_transport_error_class", DataType::Utf8, true),
        Field::new("resolution_transport_http_status", DataType::UInt16, true),
        Field::new("resolution_integrity_error_class", DataType::Utf8, true),
        timestamp_field("resolution_budget_horizon_utc", true),
        Field::new("resolution_followup_status", DataType::Utf8, true),
        Field::new("resolution_poll_attempts_recorded", DataType::UInt32, true),
        Field::new("resolution_valid_response_count", DataType::UInt32, true),
        Field::new(
            "resolution_transport_unavailable_count",
            DataType::UInt32,
            true,
        ),
        Field::new("resolution_missed_slot_count", DataType::UInt32, true),
        timestamp_field("last_valid_metadata_received_utc", true),
        Field::new("terminal_predicate_passed", DataType::Boolean, true),
        Field::new("terminal_predicate_failure_reason", DataType::Utf8, true),
        Field::new("response_fields_unavailable_reason", DataType::Utf8, true),
        Field::new("ws_claimant_source_sequence_epoch_id", DataType::Utf8, true),
        Field::new(
            "ws_claimant_collector_admission_sequence",
            DataType::UInt64,
            true,
        ),
    ]))
}

fn polymarket_rtds_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("message_timestamp", false),
        timestamp_field("payload_timestamp", false),
        Field::new("age_ms", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("message_type", DataType::Utf8, false),
        Field::new("freshness_status", DataType::Utf8, false),
        Field::new("subscription_filter", DataType::Utf8, false),
        Field::new("source_native_payload_json", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn binance_depth_snapshot_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("snapshot_fetch_start_utc", false),
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("snapshot_status", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("depth_ingress_sequence", DataType::UInt64, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn binance_depth_update_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        Field::new("event_type", DataType::Utf8, false),
        timestamp_field("event_time", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("snapshot_last_update_id", DataType::UInt64, true),
        Field::new("local_u_before", DataType::UInt64, true),
        Field::new("stitch_verdict", DataType::Utf8, false),
        Field::new("stitch_evidence", DataType::Utf8, true),
        Field::new("stitch_gap_reason", DataType::Utf8, true),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("depth_ingress_sequence", DataType::UInt64, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn binance_aggtrade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("record_kind", DataType::Utf8, false),
        timestamp_field("timestamp_received", false),
        timestamp_field("rest_backfill_timestamp_received", true),
        Field::new("ingest_source", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        timestamp_field("event_time", true),
        timestamp_field("trade_time", true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("aggregate_trade_id", DataType::UInt64, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("quantity", DataType::Utf8, true),
        Field::new("first_trade_id", DataType::UInt64, true),
        Field::new("last_trade_id", DataType::UInt64, true),
        Field::new("buyer_is_maker", DataType::Boolean, true),
        Field::new("stream", DataType::Utf8, true),
        Field::new("ws_ignore", DataType::Boolean, true),
        Field::new("rest_was_best_price_match", DataType::Boolean, true),
        Field::new("source_native_payload_json", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("connection_generation", DataType::UInt64, false),
        Field::new("websocket_ingress_sequence", DataType::UInt64, true),
        Field::new("rest_response_ingress_sequence", DataType::UInt64, true),
        Field::new("rest_response_item_index", DataType::UInt64, true),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("invalid_reason_code", DataType::Utf8, true),
        Field::new("invalid_reason_detail", DataType::Utf8, true),
        Field::new("payload_encoding", DataType::Utf8, true),
        Field::new("payload_length_bytes", DataType::UInt64, true),
        Field::new("payload_sha256", DataType::Utf8, true),
        Field::new("payload_capture_status", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn typed_column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<&'a T, TypedRawReadError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| context.error(0, Some(name), "Arrow 列类型与已校验 schema 不一致"))
}

fn required_string(
    array: &StringArray,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<String, TypedRawReadError> {
    if array.is_null(row) {
        return Err(context.error(row, Some(name), "必需字符串为 null"));
    }
    Ok(array.value(row).to_owned())
}

fn optional_string(array: &StringArray, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}

fn required_timestamp(
    array: &TimestampMillisecondArray,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<i64, TypedRawReadError> {
    if array.is_null(row) {
        return Err(context.error(row, Some(name), "必需毫秒时间戳为 null"));
    }
    Ok(array.value(row))
}

fn optional_timestamp(array: &TimestampMillisecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn row_is_selected(
    scope: Option<&TypedRawReadScope>,
    received: &TimestampMillisecondArray,
    condition: Option<(&StringArray, &str)>,
    row: usize,
    context: DecodeContext<'_>,
) -> Result<bool, TypedRawReadError> {
    let Some(scope) = scope else {
        return Ok(true);
    };
    let received = required_timestamp(received, row, "timestamp_received", context)?;
    let condition_matches = condition
        .map(|(values, name)| {
            required_string(values, row, name, context)
                .map(|value| value == scope.polymarket_condition_id)
        })
        .transpose()?
        .unwrap_or(true);
    Ok(scope.contains_received_time(received) && condition_matches)
}

macro_rules! primitive_accessors {
    ($required:ident, $optional:ident, $array:ty, $value:ty, $label:literal) => {
        fn $required(
            array: &$array,
            row: usize,
            name: &str,
            context: DecodeContext<'_>,
        ) -> Result<$value, TypedRawReadError> {
            if array.is_null(row) {
                return Err(context.error(row, Some(name), concat!("必需 ", $label, " 为 null")));
            }
            Ok(array.value(row))
        }

        fn $optional(array: &$array, row: usize) -> Option<$value> {
            (!array.is_null(row)).then(|| array.value(row))
        }
    };
}

primitive_accessors!(required_u64, optional_u64, UInt64Array, u64, "UInt64");
primitive_accessors!(required_bool, optional_bool, BooleanArray, bool, "Boolean");

fn optional_u32(array: &UInt32Array, row: usize) -> Option<u32> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_u16(array: &UInt16Array, row: usize) -> Option<u16> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_u8(array: &UInt8Array, row: usize) -> Option<u8> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn optional_i64(array: &Int64Array, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn require_schema_version_value(
    actual: String,
    row: usize,
    context: DecodeContext<'_>,
) -> Result<String, TypedRawReadError> {
    if actual != context.schema_version {
        return Err(context.error(
            row,
            Some("schema_version"),
            format!(
                "行内 schema_version 期望 {}，实际 {actual}",
                context.schema_version
            ),
        ));
    }
    Ok(actual)
}

fn core_error(error: TypedRawError, row: usize, context: DecodeContext<'_>) -> TypedRawReadError {
    let column = match &error {
        TypedRawError::InvalidField { field, .. } | TypedRawError::Json { field, .. } => {
            Some(field.as_str())
        }
    };
    context.error(row, column, error.to_string())
}

fn validate_decoded_row(
    row: TypedRawRow,
    local_row: usize,
    context: DecodeContext<'_>,
) -> Result<TypedRawRow, TypedRawReadError> {
    row.validate()
        .map_err(|error| core_error(error, local_row, context))?;
    Ok(row)
}

fn decode_typed_raw_record_batch(
    source_kind: &str,
    schema_version: &str,
    batch: &RecordBatch,
    first_row_index: u64,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    validate_physical_schema(source_kind, schema_version, batch.schema().as_ref())?;
    let context = DecodeContext {
        source_kind,
        schema_version,
        first_row_index,
    };
    match (source_kind, schema_version) {
        (POLYMARKET_ORDERBOOK_SOURCE, POLYMARKET_ORDERBOOK_SCHEMA) => {
            decode_polymarket_orderbook(batch, context, scope)
        }
        (POLYMARKET_METADATA_SOURCE, POLYMARKET_METADATA_SCHEMA) => {
            decode_polymarket_metadata(batch, context, scope)
        }
        (POLYMARKET_RTDS_SOURCE, POLYMARKET_RTDS_SCHEMA) => {
            decode_polymarket_rtds(batch, context, scope)
        }
        (BINANCE_DEPTH_SNAPSHOT_SOURCE, BINANCE_DEPTH_SNAPSHOT_SCHEMA) => {
            decode_binance_depth_snapshot(batch, context, scope)
        }
        (BINANCE_DEPTH_UPDATE_SOURCE, BINANCE_DEPTH_UPDATE_SCHEMA) => {
            decode_binance_depth_update(batch, context, scope)
        }
        (BINANCE_AGGTRADE_SOURCE, BINANCE_AGGTRADE_SCHEMA) => {
            decode_binance_aggtrade(batch, context, scope)
        }
        _ => Err(TypedRawReadError::UnsupportedSourceSchema {
            source_kind: source_kind.to_owned(),
            schema_version: schema_version.to_owned(),
        }),
    }
}

fn decode_polymarket_orderbook(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let timestamp_received =
        typed_column::<TimestampMillisecondArray>(batch, 0, "timestamp_received", context)?;
    let timestamp = typed_column::<TimestampMillisecondArray>(batch, 1, "timestamp", context)?;
    let event_types = typed_column::<StringArray>(batch, 2, "event_type", context)?;
    let markets = typed_column::<StringArray>(batch, 3, "market", context)?;
    let slugs = typed_column::<StringArray>(batch, 4, "slug", context)?;
    let collector_host_ids = typed_column::<StringArray>(batch, 5, "collector_host_id", context)?;
    let collector_regions = typed_column::<StringArray>(batch, 6, "collector_region", context)?;
    let source_endpoints = typed_column::<StringArray>(batch, 7, "source_endpoint", context)?;
    let schema_versions = typed_column::<StringArray>(batch, 8, "schema_version", context)?;
    let source_epochs = typed_column::<StringArray>(batch, 9, "source_sequence_epoch_id", context)?;
    let admission_sequences =
        typed_column::<UInt64Array>(batch, 10, "collector_admission_sequence", context)?;
    let admission_times = typed_column::<TimestampMillisecondArray>(
        batch,
        11,
        "collector_admission_time_utc_ms",
        context,
    )?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        if !row_is_selected(
            scope,
            timestamp_received,
            Some((markets, "market")),
            row_index,
            context,
        )? {
            continue;
        }
        let event_type = required_string(event_types, row_index, "event_type", context)?;
        let event = decode_polymarket_event(batch, row_index, &event_type, context)?;
        let schema_version = require_schema_version_value(
            required_string(schema_versions, row_index, "schema_version", context)?,
            row_index,
            context,
        )?;
        rows.push(validate_decoded_row(
            TypedRawRow::PolymarketOrderbook(PolymarketOrderbookRow {
                timestamp_received: required_timestamp(
                    timestamp_received,
                    row_index,
                    "timestamp_received",
                    context,
                )?,
                timestamp: optional_timestamp(timestamp, row_index),
                market: required_string(markets, row_index, "market", context)?,
                slug: optional_string(slugs, row_index),
                collector_host_id: required_string(
                    collector_host_ids,
                    row_index,
                    "collector_host_id",
                    context,
                )?,
                collector_region: required_string(
                    collector_regions,
                    row_index,
                    "collector_region",
                    context,
                )?,
                source_endpoint: required_string(
                    source_endpoints,
                    row_index,
                    "source_endpoint",
                    context,
                )?,
                schema_version,
                source_sequence_epoch_id: required_string(
                    source_epochs,
                    row_index,
                    "source_sequence_epoch_id",
                    context,
                )?,
                collector_admission_sequence: required_u64(
                    admission_sequences,
                    row_index,
                    "collector_admission_sequence",
                    context,
                )?,
                collector_admission_time_utc_ms: required_timestamp(
                    admission_times,
                    row_index,
                    "collector_admission_time_utc_ms",
                    context,
                )?,
                event,
            }),
            row_index,
            context,
        )?);
    }
    Ok(rows)
}

fn decode_polymarket_event(
    batch: &RecordBatch,
    row: usize,
    event_type: &str,
    context: DecodeContext<'_>,
) -> Result<PolymarketOrderbookEvent, TypedRawReadError> {
    let (event, allowed): (PolymarketOrderbookEvent, &[&str]) = match event_type {
        "book" => (
            PolymarketOrderbookEvent::Book {
                asset_id: required_string_column(batch, 12, row, "asset_id", context)?,
                bids: required_price_levels(batch, 13, row, "bids", context)?,
                asks: required_price_levels(batch, 14, row, "asks", context)?,
                last_trade_price: optional_string_column(
                    batch,
                    15,
                    row,
                    "last_trade_price",
                    context,
                )?,
            },
            &["asset_id", "bids", "asks", "last_trade_price"],
        ),
        "price_change" => (
            PolymarketOrderbookEvent::PriceChange {
                price_changes: required_price_changes(batch, 16, row, context)?,
            },
            &["price_changes"],
        ),
        "tick_size_change" => (
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: required_string_column(batch, 12, row, "asset_id", context)?,
                old_tick_size: optional_string_column(batch, 17, row, "old_tick_size", context)?,
                new_tick_size: required_string_column(batch, 18, row, "new_tick_size", context)?,
            },
            &["asset_id", "old_tick_size", "new_tick_size"],
        ),
        "last_trade_price" => (
            PolymarketOrderbookEvent::LastTradePrice {
                asset_id: required_string_column(batch, 12, row, "asset_id", context)?,
                price: required_string_column(batch, 19, row, "price", context)?,
                size: optional_string_column(batch, 20, row, "size", context)?,
                side: optional_string_column(batch, 21, row, "side", context)?
                    .map(|side| {
                        PolymarketSide::try_from(side.as_str())
                            .map_err(|error| core_error(error, row, context))
                    })
                    .transpose()?,
                fee_rate_bps: optional_string_column(batch, 22, row, "fee_rate_bps", context)?,
            },
            &["asset_id", "price", "size", "side", "fee_rate_bps"],
        ),
        "best_bid_ask" => (
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: required_string_column(batch, 12, row, "asset_id", context)?,
                best_bid: optional_string_column(batch, 23, row, "best_bid", context)?,
                best_ask: optional_string_column(batch, 24, row, "best_ask", context)?,
                spread: optional_string_column(batch, 25, row, "spread", context)?,
            },
            &["asset_id", "best_bid", "best_ask", "spread"],
        ),
        "new_market" => (
            PolymarketOrderbookEvent::NewMarket {
                clob_market_id: required_string_column(batch, 26, row, "clob_market_id", context)?,
                question: optional_string_column(batch, 27, row, "question", context)?,
                description: optional_string_column(batch, 28, row, "description", context)?,
                asset_ids: required_string_list(batch, 29, row, "asset_ids", context)?,
                outcomes: required_string_list(batch, 30, row, "outcomes", context)?,
                event_message: optional_event_message(batch, 31, row, context)?,
            },
            &[
                "clob_market_id",
                "question",
                "description",
                "asset_ids",
                "outcomes",
                "event_message",
            ],
        ),
        "market_resolved" => (
            PolymarketOrderbookEvent::MarketResolved {
                clob_market_id: required_string_column(batch, 26, row, "clob_market_id", context)?,
                question: optional_string_column(batch, 27, row, "question", context)?,
                description: optional_string_column(batch, 28, row, "description", context)?,
                asset_ids: required_string_list(batch, 29, row, "asset_ids", context)?,
                outcomes: required_string_list(batch, 30, row, "outcomes", context)?,
                event_message: optional_event_message(batch, 31, row, context)?,
                winning_asset_id: required_string_column(
                    batch,
                    32,
                    row,
                    "winning_asset_id",
                    context,
                )?,
                winning_outcome: required_string_column(
                    batch,
                    33,
                    row,
                    "winning_outcome",
                    context,
                )?,
            },
            &[
                "clob_market_id",
                "question",
                "description",
                "asset_ids",
                "outcomes",
                "event_message",
                "winning_asset_id",
                "winning_outcome",
            ],
        ),
        _ => {
            return Err(context.error(
                row,
                Some("event_type"),
                format!("不支持的 Polymarket 事件类型 {event_type}"),
            ));
        }
    };
    require_only_polymarket_payload_fields(batch, row, event_type, allowed, context)?;
    Ok(event)
}

fn require_only_polymarket_payload_fields(
    batch: &RecordBatch,
    row: usize,
    event_type: &str,
    allowed: &[&str],
    context: DecodeContext<'_>,
) -> Result<(), TypedRawReadError> {
    let schema = batch.schema();
    for index in 12..batch.num_columns() {
        let field = schema.field(index);
        if !allowed.contains(&field.name().as_str()) && !batch.column(index).is_null(row) {
            return Err(context.error(
                row,
                Some(field.name()),
                format!("{event_type} 行包含相互矛盾的其他事件载荷"),
            ));
        }
    }
    Ok(())
}

fn required_string_column(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<String, TypedRawReadError> {
    required_string(
        typed_column::<StringArray>(batch, index, name, context)?,
        row,
        name,
        context,
    )
}

fn optional_string_column(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<Option<String>, TypedRawReadError> {
    Ok(optional_string(
        typed_column::<StringArray>(batch, index, name, context)?,
        row,
    ))
}

fn required_enum_column<T>(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<T, TypedRawReadError>
where
    for<'a> T: TryFrom<&'a str, Error = TypedRawError>,
{
    let raw = required_string_column(batch, index, row, name, context)?;
    T::try_from(raw.as_str()).map_err(|error| core_error(error, row, context))
}

fn optional_enum_column<T>(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<Option<T>, TypedRawReadError>
where
    for<'a> T: TryFrom<&'a str, Error = TypedRawError>,
{
    optional_string_column(batch, index, row, name, context)?
        .map(|raw| T::try_from(raw.as_str()).map_err(|error| core_error(error, row, context)))
        .transpose()
}

fn required_list_range(
    array: &ListArray,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<std::ops::Range<usize>, TypedRawReadError> {
    if array.is_null(row) {
        return Err(context.error(row, Some(name), "必需列表为 null"));
    }
    let offsets = array.value_offsets();
    let start = usize::try_from(offsets[row])
        .map_err(|_| context.error(row, Some(name), "列表起始偏移为负数"))?;
    let end = usize::try_from(offsets[row + 1])
        .map_err(|_| context.error(row, Some(name), "列表结束偏移为负数"))?;
    Ok(start..end)
}

fn required_price_levels(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<Vec<PolymarketPriceLevel>, TypedRawReadError> {
    let list = typed_column::<ListArray>(batch, index, name, context)?;
    let range = required_list_range(list, row, name, context)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| context.error(row, Some(name), "列表元素不是 Struct"))?;
    let prices = values
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| context.error(row, Some(name), "price 子列不是 Utf8"))?;
    let sizes = values
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| context.error(row, Some(name), "size 子列不是 Utf8"))?;
    range
        .map(|item| {
            if values.is_null(item) {
                return Err(context.error(row, Some(name), format!("第 {item} 个元素为 null")));
            }
            Ok(PolymarketPriceLevel {
                price: required_nested_string(
                    prices,
                    item,
                    row,
                    &format!("{name}.price"),
                    context,
                )?,
                size: required_nested_string(sizes, item, row, &format!("{name}.size"), context)?,
            })
        })
        .collect()
}

fn required_price_changes(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    context: DecodeContext<'_>,
) -> Result<Vec<PolymarketPriceChangeEntry>, TypedRawReadError> {
    let name = "price_changes";
    let list = typed_column::<ListArray>(batch, index, name, context)?;
    let range = required_list_range(list, row, name, context)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| context.error(row, Some(name), "列表元素不是 Struct"))?;
    let child = |child_index: usize, child_name: &str| {
        values
            .column(child_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| context.error(row, Some(name), format!("{child_name} 子列不是 Utf8")))
    };
    let asset_ids = child(0, "asset_id")?;
    let prices = child(1, "price")?;
    let sizes = child(2, "size")?;
    let sides = child(3, "side")?;
    let best_bids = child(4, "best_bid")?;
    let best_asks = child(5, "best_ask")?;
    range
        .map(|item| {
            if values.is_null(item) {
                return Err(context.error(row, Some(name), format!("第 {item} 个元素为 null")));
            }
            let side = required_nested_string(sides, item, row, "price_changes.side", context)?;
            Ok(PolymarketPriceChangeEntry {
                asset_id: required_nested_string(
                    asset_ids,
                    item,
                    row,
                    "price_changes.asset_id",
                    context,
                )?,
                price: required_nested_string(prices, item, row, "price_changes.price", context)?,
                size: optional_string(sizes, item),
                side: PolymarketSide::try_from(side.as_str())
                    .map_err(|error| core_error(error, row, context))?,
                best_bid: optional_string(best_bids, item),
                best_ask: optional_string(best_asks, item),
            })
        })
        .collect()
}

fn required_string_list(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<Vec<String>, TypedRawReadError> {
    let list = typed_column::<ListArray>(batch, index, name, context)?;
    let range = required_list_range(list, row, name, context)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| context.error(row, Some(name), "列表元素不是 Utf8"))?;
    range
        .map(|item| required_nested_string(values, item, row, &format!("{name}.item"), context))
        .collect()
}

fn required_nested_string(
    array: &StringArray,
    item: usize,
    outer_row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<String, TypedRawReadError> {
    if array.is_null(item) {
        return Err(context.error(
            outer_row,
            Some(name),
            format!("列表元素 #{item} 的必需字符串为 null"),
        ));
    }
    Ok(array.value(item).to_owned())
}

fn optional_event_message(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    context: DecodeContext<'_>,
) -> Result<Option<PolymarketEventMessage>, TypedRawReadError> {
    let values = typed_column::<StructArray>(batch, index, "event_message", context)?;
    if values.is_null(row) {
        return Ok(None);
    }
    let child = |child_index: usize, child_name: &str| {
        values
            .column(child_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                context.error(
                    row,
                    Some("event_message"),
                    format!("{child_name} 子列不是 Utf8"),
                )
            })
    };
    let ids = child(0, "id")?;
    let tickers = child(1, "ticker")?;
    let slugs = child(2, "slug")?;
    let titles = child(3, "title")?;
    let descriptions = child(4, "description")?;
    Ok(Some(PolymarketEventMessage {
        id: required_string(ids, row, "event_message.id", context)?,
        ticker: optional_string(tickers, row),
        slug: optional_string(slugs, row),
        title: optional_string(titles, row),
        description: optional_string(descriptions, row),
    }))
}

fn required_timestamp_column(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<i64, TypedRawReadError> {
    required_timestamp(
        typed_column::<TimestampMillisecondArray>(batch, index, name, context)?,
        row,
        name,
        context,
    )
}

fn optional_timestamp_column(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<Option<i64>, TypedRawReadError> {
    Ok(optional_timestamp(
        typed_column::<TimestampMillisecondArray>(batch, index, name, context)?,
        row,
    ))
}

fn required_u64_column(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    name: &str,
    context: DecodeContext<'_>,
) -> Result<u64, TypedRawReadError> {
    required_u64(
        typed_column::<UInt64Array>(batch, index, name, context)?,
        row,
        name,
        context,
    )
}

macro_rules! optional_primitive_column {
    ($function:ident, $array:ty, $accessor:ident, $value:ty) => {
        fn $function(
            batch: &RecordBatch,
            index: usize,
            row: usize,
            name: &str,
            context: DecodeContext<'_>,
        ) -> Result<Option<$value>, TypedRawReadError> {
            Ok($accessor(
                typed_column::<$array>(batch, index, name, context)?,
                row,
            ))
        }
    };
}

optional_primitive_column!(optional_u64_column, UInt64Array, optional_u64, u64);
optional_primitive_column!(optional_u32_column, UInt32Array, optional_u32, u32);
optional_primitive_column!(optional_u16_column, UInt16Array, optional_u16, u16);
optional_primitive_column!(optional_u8_column, UInt8Array, optional_u8, u8);
optional_primitive_column!(optional_bool_column, BooleanArray, optional_bool, bool);

fn decode_polymarket_metadata(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let received =
        typed_column::<TimestampMillisecondArray>(batch, 1, "timestamp_received", context)?;
    let conditions = typed_column::<StringArray>(batch, 9, "condition_id", context)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !row_is_selected(
            scope,
            received,
            Some((conditions, "condition_id")),
            row,
            context,
        )? {
            continue;
        }
        let raw_kind = required_string_column(batch, 0, row, "metadata_row_kind", context)?;
        let schema_version = require_schema_version_value(
            required_string_column(batch, 7, row, "schema_version", context)?,
            row,
            context,
        )?;
        let decoded = TypedRawRow::PolymarketMarketMetadata(Box::new(
            PolymarketMarketMetadataRow {
                metadata_row_kind: PolymarketMetadataRowKind::try_from(raw_kind.as_str())
                    .map_err(|error| core_error(error, row, context))?,
                timestamp_received: required_timestamp_column(
                    batch,
                    1,
                    row,
                    "timestamp_received",
                    context,
                )?,
                collector_admission_sequence: required_u64_column(
                    batch,
                    2,
                    row,
                    "collector_admission_sequence",
                    context,
                )?,
                collector_admission_time_utc_ms: required_timestamp_column(
                    batch,
                    3,
                    row,
                    "collector_admission_time_utc_ms",
                    context,
                )?,
                metadata_origin: required_enum_column::<PolymarketMetadataOrigin>(
                    batch,
                    4,
                    row,
                    "metadata_origin",
                    context,
                )?,
                collector_host_id: required_string_column(
                    batch,
                    5,
                    row,
                    "collector_host_id",
                    context,
                )?,
                collector_region: required_string_column(
                    batch,
                    6,
                    row,
                    "collector_region",
                    context,
                )?,
                schema_version,
                slug: required_string_column(batch, 8, row, "slug", context)?,
                condition_id: required_string_column(batch, 9, row, "condition_id", context)?,
                clob_market_id: required_string_column(batch, 10, row, "clob_market_id", context)?,
                asset_ids: required_string_column(batch, 11, row, "asset_ids", context)?,
                market_start_utc: required_timestamp_column(
                    batch,
                    12,
                    row,
                    "market_start_utc",
                    context,
                )?,
                market_end_utc: required_timestamp_column(
                    batch,
                    13,
                    row,
                    "market_end_utc",
                    context,
                )?,
                fixed_identity_verified_at_utc: required_timestamp_column(
                    batch,
                    14,
                    row,
                    "fixed_identity_verified_at_utc",
                    context,
                )?,
                market_lifecycle_state: required_enum_column::<MetadataMarketLifecycleState>(
                    batch,
                    15,
                    row,
                    "market_lifecycle_state",
                    context,
                )?,
                market_runtime_role: required_enum_column::<MetadataMarketRuntimeRole>(
                    batch,
                    16,
                    row,
                    "market_runtime_role",
                    context,
                )?,
                source_endpoint: optional_string_column(
                    batch,
                    17,
                    row,
                    "source_endpoint",
                    context,
                )?,
                source_endpoint_unavailable_reason: optional_enum_column::<
                    MetadataSourceEndpointUnavailableReason,
                >(
                    batch,
                    18,
                    row,
                    "source_endpoint_unavailable_reason",
                    context,
                )?,
                source_evidence_origin: optional_enum_column::<MetadataSourceEvidenceOrigin>(
                    batch,
                    19,
                    row,
                    "source_evidence_origin",
                    context,
                )?,
                source_evidence_event_type: optional_enum_column::<MetadataSourceEvidenceEventType>(
                    batch,
                    20,
                    row,
                    "source_evidence_event_type",
                    context,
                )?,
                source_evidence_channel: optional_enum_column::<MetadataSourceEvidenceChannel>(
                    batch,
                    21,
                    row,
                    "source_evidence_channel",
                    context,
                )?,
                source_evidence_received_at_utc: optional_timestamp_column(
                    batch,
                    22,
                    row,
                    "source_evidence_received_at_utc",
                    context,
                )?,
                row_evidence_validation_completed_at_utc: optional_timestamp_column(
                    batch,
                    23,
                    row,
                    "row_evidence_validation_completed_at_utc",
                    context,
                )?,
                source_evidence_payload_json: optional_string_column(
                    batch,
                    24,
                    row,
                    "source_evidence_payload_json",
                    context,
                )?,
                source_evidence_raw_sha256: optional_string_column(
                    batch,
                    25,
                    row,
                    "source_evidence_raw_sha256",
                    context,
                )?,
                market_lifecycle_transition: optional_enum_column::<
                    MetadataMarketLifecycleTransition,
                >(
                    batch,
                    26,
                    row,
                    "market_lifecycle_transition",
                    context,
                )?,
                lifecycle_scheduled_at_utc: optional_timestamp_column(
                    batch,
                    27,
                    row,
                    "lifecycle_scheduled_at_utc",
                    context,
                )?,
                lifecycle_applied_at_utc: optional_timestamp_column(
                    batch,
                    28,
                    row,
                    "lifecycle_applied_at_utc",
                    context,
                )?,
                market_runtime_role_transition: optional_enum_column::<
                    MetadataMarketRuntimeRoleTransition,
                >(
                    batch,
                    29,
                    row,
                    "market_runtime_role_transition",
                    context,
                )?,
                runtime_role_scheduled_at_utc: optional_timestamp_column(
                    batch,
                    30,
                    row,
                    "runtime_role_scheduled_at_utc",
                    context,
                )?,
                runtime_role_applied_at_utc: optional_timestamp_column(
                    batch,
                    31,
                    row,
                    "runtime_role_applied_at_utc",
                    context,
                )?,
                metadata_fetch_start_utc: optional_timestamp_column(
                    batch,
                    32,
                    row,
                    "metadata_fetch_start_utc",
                    context,
                )?,
                closed: optional_bool_column(batch, 33, row, "closed", context)?,
                closed_time_raw: optional_string_column(
                    batch,
                    34,
                    row,
                    "closed_time_raw",
                    context,
                )?,
                closed_time_utc: optional_timestamp_column(
                    batch,
                    35,
                    row,
                    "closed_time_utc",
                    context,
                )?,
                closed_time_check_passed: optional_bool_column(
                    batch,
                    36,
                    row,
                    "closed_time_check_passed",
                    context,
                )?,
                closed_time_check_failure_reason: optional_enum_column::<
                    MetadataClosedTimeCheckFailureReason,
                >(
                    batch,
                    37,
                    row,
                    "closed_time_check_failure_reason",
                    context,
                )?,
                automatically_resolved: optional_bool_column(
                    batch,
                    38,
                    row,
                    "automatically_resolved",
                    context,
                )?,
                uma_resolution_status: optional_string_column(
                    batch,
                    39,
                    row,
                    "uma_resolution_status",
                    context,
                )?,
                outcomes_raw: optional_string_column(batch, 40, row, "outcomes_raw", context)?,
                outcome_prices_raw: optional_string_column(
                    batch,
                    41,
                    row,
                    "outcome_prices_raw",
                    context,
                )?,
                active: optional_bool_column(batch, 42, row, "active", context)?,
                active_unavailable_reason: optional_enum_column::<MetadataActiveUnavailableReason>(
                    batch,
                    43,
                    row,
                    "active_unavailable_reason",
                    context,
                )?,
                accepting_orders: optional_bool_column(
                    batch,
                    44,
                    row,
                    "accepting_orders",
                    context,
                )?,
                ws_platform_timestamp_raw: optional_string_column(
                    batch,
                    45,
                    row,
                    "ws_platform_timestamp_raw",
                    context,
                )?,
                ws_platform_timestamp_utc: optional_timestamp_column(
                    batch,
                    46,
                    row,
                    "ws_platform_timestamp_utc",
                    context,
                )?,
                winning_asset_id: optional_string_column(
                    batch,
                    47,
                    row,
                    "winning_asset_id",
                    context,
                )?,
                winning_outcome: optional_string_column(
                    batch,
                    48,
                    row,
                    "winning_outcome",
                    context,
                )?,
                resolution_evidence_source: optional_enum_column::<MetadataResolutionEvidenceSource>(
                    batch,
                    49,
                    row,
                    "resolution_evidence_source",
                    context,
                )?,
                terminal_claim_owner: optional_enum_column::<MetadataTerminalClaimOwner>(
                    batch,
                    50,
                    row,
                    "terminal_claim_owner",
                    context,
                )?,
                resolution_poll_attempt_ordinal: optional_u8_column(
                    batch,
                    51,
                    row,
                    "resolution_poll_attempt_ordinal",
                    context,
                )?,
                resolution_poll_scheduled_utc: optional_timestamp_column(
                    batch,
                    52,
                    row,
                    "resolution_poll_scheduled_utc",
                    context,
                )?,
                resolution_poll_result: optional_enum_column::<MetadataResolutionPollResult>(
                    batch,
                    53,
                    row,
                    "resolution_poll_result",
                    context,
                )?,
                resolution_transport_error_class: optional_enum_column::<
                    MetadataResolutionTransportErrorClass,
                >(
                    batch,
                    54,
                    row,
                    "resolution_transport_error_class",
                    context,
                )?,
                resolution_transport_http_status: optional_u16_column(
                    batch,
                    55,
                    row,
                    "resolution_transport_http_status",
                    context,
                )?,
                resolution_integrity_error_class: optional_enum_column::<
                    MetadataResolutionIntegrityErrorClass,
                >(
                    batch,
                    56,
                    row,
                    "resolution_integrity_error_class",
                    context,
                )?,
                resolution_budget_horizon_utc: optional_timestamp_column(
                    batch,
                    57,
                    row,
                    "resolution_budget_horizon_utc",
                    context,
                )?,
                resolution_followup_status: optional_enum_column::<MetadataResolutionFollowupStatus>(
                    batch,
                    58,
                    row,
                    "resolution_followup_status",
                    context,
                )?,
                resolution_poll_attempts_recorded: optional_u32_column(
                    batch,
                    59,
                    row,
                    "resolution_poll_attempts_recorded",
                    context,
                )?,
                resolution_valid_response_count: optional_u32_column(
                    batch,
                    60,
                    row,
                    "resolution_valid_response_count",
                    context,
                )?,
                resolution_transport_unavailable_count: optional_u32_column(
                    batch,
                    61,
                    row,
                    "resolution_transport_unavailable_count",
                    context,
                )?,
                resolution_missed_slot_count: optional_u32_column(
                    batch,
                    62,
                    row,
                    "resolution_missed_slot_count",
                    context,
                )?,
                last_valid_metadata_received_utc: optional_timestamp_column(
                    batch,
                    63,
                    row,
                    "last_valid_metadata_received_utc",
                    context,
                )?,
                terminal_predicate_passed: optional_bool_column(
                    batch,
                    64,
                    row,
                    "terminal_predicate_passed",
                    context,
                )?,
                terminal_predicate_failure_reason: optional_enum_column::<
                    MetadataTerminalPredicateFailureReason,
                >(
                    batch,
                    65,
                    row,
                    "terminal_predicate_failure_reason",
                    context,
                )?,
                response_fields_unavailable_reason: optional_enum_column::<
                    MetadataResponseFieldsUnavailableReason,
                >(
                    batch,
                    66,
                    row,
                    "response_fields_unavailable_reason",
                    context,
                )?,
                ws_claimant_source_sequence_epoch_id: optional_string_column(
                    batch,
                    67,
                    row,
                    "ws_claimant_source_sequence_epoch_id",
                    context,
                )?,
                ws_claimant_collector_admission_sequence: optional_u64_column(
                    batch,
                    68,
                    row,
                    "ws_claimant_collector_admission_sequence",
                    context,
                )?,
            },
        ));
        rows.push(validate_decoded_row(decoded, row, context)?);
    }
    Ok(rows)
}

fn decode_polymarket_rtds(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let received =
        typed_column::<TimestampMillisecondArray>(batch, 0, "timestamp_received", context)?;
    let message =
        typed_column::<TimestampMillisecondArray>(batch, 1, "message_timestamp", context)?;
    let payload =
        typed_column::<TimestampMillisecondArray>(batch, 2, "payload_timestamp", context)?;
    let ages = typed_column::<Int64Array>(batch, 3, "age_ms", context)?;
    let symbols = typed_column::<StringArray>(batch, 4, "symbol", context)?;
    let values = typed_column::<StringArray>(batch, 5, "value", context)?;
    let topics = typed_column::<StringArray>(batch, 6, "topic", context)?;
    let message_types = typed_column::<StringArray>(batch, 7, "message_type", context)?;
    let freshness = typed_column::<StringArray>(batch, 8, "freshness_status", context)?;
    let filters = typed_column::<StringArray>(batch, 9, "subscription_filter", context)?;
    let payload_json =
        typed_column::<StringArray>(batch, 10, "source_native_payload_json", context)?;
    let hosts = typed_column::<StringArray>(batch, 11, "collector_host_id", context)?;
    let regions = typed_column::<StringArray>(batch, 12, "collector_region", context)?;
    let endpoints = typed_column::<StringArray>(batch, 13, "source_endpoint", context)?;
    let schemas = typed_column::<StringArray>(batch, 14, "schema_version", context)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !row_is_selected(scope, received, None, row, context)? {
            continue;
        }
        let freshness_status = required_string(freshness, row, "freshness_status", context)?;
        let schema_version = require_schema_version_value(
            required_string(schemas, row, "schema_version", context)?,
            row,
            context,
        )?;
        let decoded =
            TypedRawRow::PolymarketRtdsChainlinkBtcUsd(PolymarketRtdsChainlinkBtcUsdRow {
                timestamp_received: required_timestamp(
                    received,
                    row,
                    "timestamp_received",
                    context,
                )?,
                message_timestamp: required_timestamp(message, row, "message_timestamp", context)?,
                payload_timestamp: required_timestamp(payload, row, "payload_timestamp", context)?,
                age_ms: optional_i64(ages, row),
                symbol: required_string(symbols, row, "symbol", context)?,
                value: required_string(values, row, "value", context)?,
                topic: required_string(topics, row, "topic", context)?,
                message_type: required_string(message_types, row, "message_type", context)?,
                freshness_status: RtdsFreshnessStatus::try_from(freshness_status.as_str())
                    .map_err(|error| core_error(error, row, context))?,
                subscription_filter: required_string(filters, row, "subscription_filter", context)?,
                source_native_payload_json: required_string(
                    payload_json,
                    row,
                    "source_native_payload_json",
                    context,
                )?,
                collector_host_id: required_string(hosts, row, "collector_host_id", context)?,
                collector_region: required_string(regions, row, "collector_region", context)?,
                source_endpoint: required_string(endpoints, row, "source_endpoint", context)?,
                schema_version,
            });
        rows.push(validate_decoded_row(decoded, row, context)?);
    }
    Ok(rows)
}

fn decode_binance_depth_snapshot(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let received =
        typed_column::<TimestampMillisecondArray>(batch, 0, "timestamp_received", context)?;
    let fetch_started =
        typed_column::<TimestampMillisecondArray>(batch, 1, "snapshot_fetch_start_utc", context)?;
    let update_ids = typed_column::<UInt64Array>(batch, 2, "last_update_id", context)?;
    let bids = typed_column::<StringArray>(batch, 3, "bids", context)?;
    let asks = typed_column::<StringArray>(batch, 4, "asks", context)?;
    let symbols = typed_column::<StringArray>(batch, 5, "symbol", context)?;
    let endpoints = typed_column::<StringArray>(batch, 6, "source_endpoint", context)?;
    let statuses = typed_column::<StringArray>(batch, 7, "snapshot_status", context)?;
    let epochs = typed_column::<StringArray>(batch, 8, "source_sequence_epoch_id", context)?;
    let generations = typed_column::<UInt64Array>(batch, 9, "connection_generation", context)?;
    let ingress = typed_column::<UInt64Array>(batch, 10, "depth_ingress_sequence", context)?;
    let admissions =
        typed_column::<UInt64Array>(batch, 11, "collector_admission_sequence", context)?;
    let admission_times = typed_column::<TimestampMillisecondArray>(
        batch,
        12,
        "collector_admission_time_utc_ms",
        context,
    )?;
    let hosts = typed_column::<StringArray>(batch, 13, "collector_host_id", context)?;
    let regions = typed_column::<StringArray>(batch, 14, "collector_region", context)?;
    let schemas = typed_column::<StringArray>(batch, 15, "schema_version", context)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !row_is_selected(scope, received, None, row, context)? {
            continue;
        }
        let bids_raw = required_string(bids, row, "bids", context)?;
        let asks_raw = required_string(asks, row, "asks", context)?;
        let schema_version = require_schema_version_value(
            required_string(schemas, row, "schema_version", context)?,
            row,
            context,
        )?;
        let decoded = TypedRawRow::BinanceDepthSnapshot(BinanceDepthSnapshotRow {
            timestamp_received: required_timestamp(received, row, "timestamp_received", context)?,
            snapshot_fetch_start_utc: required_timestamp(
                fetch_started,
                row,
                "snapshot_fetch_start_utc",
                context,
            )?,
            last_update_id: required_u64(update_ids, row, "last_update_id", context)?,
            bids: parse_depth_levels_json(&bids_raw, "bids", false)
                .map_err(|error| core_error(error, row, context))?,
            asks: parse_depth_levels_json(&asks_raw, "asks", false)
                .map_err(|error| core_error(error, row, context))?,
            symbol: required_string(symbols, row, "symbol", context)?,
            source_endpoint: required_string(endpoints, row, "source_endpoint", context)?,
            snapshot_status: required_string(statuses, row, "snapshot_status", context)?,
            source_sequence_epoch_id: required_string(
                epochs,
                row,
                "source_sequence_epoch_id",
                context,
            )?,
            connection_generation: required_u64(
                generations,
                row,
                "connection_generation",
                context,
            )?,
            depth_ingress_sequence: required_u64(ingress, row, "depth_ingress_sequence", context)?,
            collector_admission_sequence: required_u64(
                admissions,
                row,
                "collector_admission_sequence",
                context,
            )?,
            collector_admission_time_utc_ms: required_timestamp(
                admission_times,
                row,
                "collector_admission_time_utc_ms",
                context,
            )?,
            collector_host_id: required_string(hosts, row, "collector_host_id", context)?,
            collector_region: required_string(regions, row, "collector_region", context)?,
            schema_version,
        });
        rows.push(validate_decoded_row(decoded, row, context)?);
    }
    Ok(rows)
}

fn decode_binance_depth_update(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let received =
        typed_column::<TimestampMillisecondArray>(batch, 0, "timestamp_received", context)?;
    let event_types = typed_column::<StringArray>(batch, 1, "event_type", context)?;
    let event_times = typed_column::<TimestampMillisecondArray>(batch, 2, "event_time", context)?;
    let symbols = typed_column::<StringArray>(batch, 3, "symbol", context)?;
    let first_ids = typed_column::<UInt64Array>(batch, 4, "first_update_id", context)?;
    let final_ids = typed_column::<UInt64Array>(batch, 5, "final_update_id", context)?;
    let bids = typed_column::<StringArray>(batch, 6, "bids", context)?;
    let asks = typed_column::<StringArray>(batch, 7, "asks", context)?;
    let streams = typed_column::<StringArray>(batch, 8, "stream", context)?;
    let snapshot_ids = typed_column::<UInt64Array>(batch, 9, "snapshot_last_update_id", context)?;
    let local_u = typed_column::<UInt64Array>(batch, 10, "local_u_before", context)?;
    let verdicts = typed_column::<StringArray>(batch, 11, "stitch_verdict", context)?;
    let evidence = typed_column::<StringArray>(batch, 12, "stitch_evidence", context)?;
    let gaps = typed_column::<StringArray>(batch, 13, "stitch_gap_reason", context)?;
    let epochs = typed_column::<StringArray>(batch, 14, "source_sequence_epoch_id", context)?;
    let generations = typed_column::<UInt64Array>(batch, 15, "connection_generation", context)?;
    let ingress = typed_column::<UInt64Array>(batch, 16, "depth_ingress_sequence", context)?;
    let admissions =
        typed_column::<UInt64Array>(batch, 17, "collector_admission_sequence", context)?;
    let admission_times = typed_column::<TimestampMillisecondArray>(
        batch,
        18,
        "collector_admission_time_utc_ms",
        context,
    )?;
    let hosts = typed_column::<StringArray>(batch, 19, "collector_host_id", context)?;
    let regions = typed_column::<StringArray>(batch, 20, "collector_region", context)?;
    let endpoints = typed_column::<StringArray>(batch, 21, "source_endpoint", context)?;
    let schemas = typed_column::<StringArray>(batch, 22, "schema_version", context)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !row_is_selected(scope, received, None, row, context)? {
            continue;
        }
        let bids_raw = required_string(bids, row, "bids", context)?;
        let asks_raw = required_string(asks, row, "asks", context)?;
        let verdict = required_string(verdicts, row, "stitch_verdict", context)?;
        let schema_version = require_schema_version_value(
            required_string(schemas, row, "schema_version", context)?,
            row,
            context,
        )?;
        let decoded = TypedRawRow::BinanceDepthUpdate(BinanceDepthUpdateRow {
            timestamp_received: required_timestamp(received, row, "timestamp_received", context)?,
            event_type: required_string(event_types, row, "event_type", context)?,
            event_time: required_timestamp(event_times, row, "event_time", context)?,
            symbol: required_string(symbols, row, "symbol", context)?,
            first_update_id: required_u64(first_ids, row, "first_update_id", context)?,
            final_update_id: required_u64(final_ids, row, "final_update_id", context)?,
            bids: parse_depth_levels_json(&bids_raw, "bids", true)
                .map_err(|error| core_error(error, row, context))?,
            asks: parse_depth_levels_json(&asks_raw, "asks", true)
                .map_err(|error| core_error(error, row, context))?,
            stream: required_string(streams, row, "stream", context)?,
            snapshot_last_update_id: optional_u64(snapshot_ids, row),
            local_u_before: optional_u64(local_u, row),
            stitch_verdict: BinanceDepthStitchVerdict::try_from(verdict.as_str())
                .map_err(|error| core_error(error, row, context))?,
            stitch_evidence: optional_string(evidence, row),
            stitch_gap_reason: optional_string(gaps, row),
            source_sequence_epoch_id: required_string(
                epochs,
                row,
                "source_sequence_epoch_id",
                context,
            )?,
            connection_generation: required_u64(
                generations,
                row,
                "connection_generation",
                context,
            )?,
            depth_ingress_sequence: required_u64(ingress, row, "depth_ingress_sequence", context)?,
            collector_admission_sequence: required_u64(
                admissions,
                row,
                "collector_admission_sequence",
                context,
            )?,
            collector_admission_time_utc_ms: required_timestamp(
                admission_times,
                row,
                "collector_admission_time_utc_ms",
                context,
            )?,
            collector_host_id: required_string(hosts, row, "collector_host_id", context)?,
            collector_region: required_string(regions, row, "collector_region", context)?,
            source_endpoint: required_string(endpoints, row, "source_endpoint", context)?,
            schema_version,
        });
        rows.push(validate_decoded_row(decoded, row, context)?);
    }
    Ok(rows)
}

fn decode_binance_aggtrade(
    batch: &RecordBatch,
    context: DecodeContext<'_>,
    scope: Option<&TypedRawReadScope>,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let record_kinds = typed_column::<StringArray>(batch, 0, "record_kind", context)?;
    let received =
        typed_column::<TimestampMillisecondArray>(batch, 1, "timestamp_received", context)?;
    let rest_received = typed_column::<TimestampMillisecondArray>(
        batch,
        2,
        "rest_backfill_timestamp_received",
        context,
    )?;
    let ingest_sources = typed_column::<StringArray>(batch, 3, "ingest_source", context)?;
    let event_types = typed_column::<StringArray>(batch, 4, "event_type", context)?;
    let event_times = typed_column::<TimestampMillisecondArray>(batch, 5, "event_time", context)?;
    let trade_times = typed_column::<TimestampMillisecondArray>(batch, 6, "trade_time", context)?;
    let symbols = typed_column::<StringArray>(batch, 7, "symbol", context)?;
    let aggregate_ids = typed_column::<UInt64Array>(batch, 8, "aggregate_trade_id", context)?;
    let prices = typed_column::<StringArray>(batch, 9, "price", context)?;
    let quantities = typed_column::<StringArray>(batch, 10, "quantity", context)?;
    let first_ids = typed_column::<UInt64Array>(batch, 11, "first_trade_id", context)?;
    let last_ids = typed_column::<UInt64Array>(batch, 12, "last_trade_id", context)?;
    let buyers = typed_column::<BooleanArray>(batch, 13, "buyer_is_maker", context)?;
    let streams = typed_column::<StringArray>(batch, 14, "stream", context)?;
    let ws_ignores = typed_column::<BooleanArray>(batch, 15, "ws_ignore", context)?;
    let rest_matches =
        typed_column::<BooleanArray>(batch, 16, "rest_was_best_price_match", context)?;
    let payload_json =
        typed_column::<StringArray>(batch, 17, "source_native_payload_json", context)?;
    let epochs = typed_column::<StringArray>(batch, 18, "source_sequence_epoch_id", context)?;
    let generations = typed_column::<UInt64Array>(batch, 19, "connection_generation", context)?;
    let ws_ingress = typed_column::<UInt64Array>(batch, 20, "websocket_ingress_sequence", context)?;
    let rest_ingress =
        typed_column::<UInt64Array>(batch, 21, "rest_response_ingress_sequence", context)?;
    let rest_item_indexes =
        typed_column::<UInt64Array>(batch, 22, "rest_response_item_index", context)?;
    let admissions =
        typed_column::<UInt64Array>(batch, 23, "collector_admission_sequence", context)?;
    let admission_times = typed_column::<TimestampMillisecondArray>(
        batch,
        24,
        "collector_admission_time_utc_ms",
        context,
    )?;
    let invalid_codes = typed_column::<StringArray>(batch, 25, "invalid_reason_code", context)?;
    let invalid_details = typed_column::<StringArray>(batch, 26, "invalid_reason_detail", context)?;
    let encodings = typed_column::<StringArray>(batch, 27, "payload_encoding", context)?;
    let payload_lengths = typed_column::<UInt64Array>(batch, 28, "payload_length_bytes", context)?;
    let payload_digests = typed_column::<StringArray>(batch, 29, "payload_sha256", context)?;
    let capture_statuses =
        typed_column::<StringArray>(batch, 30, "payload_capture_status", context)?;
    let hosts = typed_column::<StringArray>(batch, 31, "collector_host_id", context)?;
    let regions = typed_column::<StringArray>(batch, 32, "collector_region", context)?;
    let endpoints = typed_column::<StringArray>(batch, 33, "source_endpoint", context)?;
    let schemas = typed_column::<StringArray>(batch, 34, "schema_version", context)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !row_is_selected(scope, received, None, row, context)? {
            continue;
        }
        let record_kind = required_string(record_kinds, row, "record_kind", context)?;
        let record = match record_kind.as_str() {
            "trade" => {
                for index in 25..=30 {
                    if !batch.column(index).is_null(row) {
                        return Err(context.error(
                            row,
                            Some(batch.schema().field(index).name()),
                            "trade 行不得携带 invalid_input 诊断",
                        ));
                    }
                }
                BinanceAggTradeRecord::Trade {
                    event_type: required_string(event_types, row, "event_type", context)?,
                    event_time: required_timestamp(event_times, row, "event_time", context)?,
                    trade_time: required_timestamp(trade_times, row, "trade_time", context)?,
                    symbol: required_string(symbols, row, "symbol", context)?,
                    aggregate_trade_id: required_u64(
                        aggregate_ids,
                        row,
                        "aggregate_trade_id",
                        context,
                    )?,
                    price: required_string(prices, row, "price", context)?,
                    quantity: required_string(quantities, row, "quantity", context)?,
                    first_trade_id: required_u64(first_ids, row, "first_trade_id", context)?,
                    last_trade_id: required_u64(last_ids, row, "last_trade_id", context)?,
                    buyer_is_maker: required_bool(buyers, row, "buyer_is_maker", context)?,
                    stream: required_string(streams, row, "stream", context)?,
                    ws_ignore: required_bool(ws_ignores, row, "ws_ignore", context)?,
                }
            }
            "invalid_input" => {
                for index in 4..=15 {
                    if !batch.column(index).is_null(row) {
                        return Err(context.error(
                            row,
                            Some(batch.schema().field(index).name()),
                            "invalid_input 行不得声称已解析交易载荷",
                        ));
                    }
                }
                let capture_status =
                    required_string(capture_statuses, row, "payload_capture_status", context)?;
                BinanceAggTradeRecord::InvalidInput {
                    invalid_reason_code: BinanceAggTradeInvalidReason::try_from(
                        required_string(invalid_codes, row, "invalid_reason_code", context)?
                            .as_str(),
                    )
                    .map_err(|error| core_error(error, row, context))?,
                    invalid_reason_detail: optional_string(invalid_details, row),
                    payload_encoding: optional_string(encodings, row)
                        .map(|raw| {
                            BinanceAggTradePayloadEncoding::try_from(raw.as_str())
                                .map_err(|error| core_error(error, row, context))
                        })
                        .transpose()?,
                    payload_length_bytes: optional_u64(payload_lengths, row),
                    payload_sha256: optional_string(payload_digests, row),
                    payload_capture_status: BinanceAggTradeCaptureStatus::try_from(
                        capture_status.as_str(),
                    )
                    .map_err(|error| core_error(error, row, context))?,
                }
            }
            _ => {
                return Err(context.error(
                    row,
                    Some("record_kind"),
                    format!("不支持的 Binance aggTrade record_kind {record_kind}"),
                ));
            }
        };
        let schema_version = require_schema_version_value(
            required_string(schemas, row, "schema_version", context)?,
            row,
            context,
        )?;
        let decoded = TypedRawRow::BinanceAggTrade(BinanceAggTradeRow {
            timestamp_received: required_timestamp(received, row, "timestamp_received", context)?,
            rest_backfill_timestamp_received: optional_timestamp(rest_received, row),
            ingest_source: required_string(ingest_sources, row, "ingest_source", context)?,
            rest_was_best_price_match: optional_bool(rest_matches, row),
            source_native_payload_json: required_string(
                payload_json,
                row,
                "source_native_payload_json",
                context,
            )?,
            source_sequence_epoch_id: required_string(
                epochs,
                row,
                "source_sequence_epoch_id",
                context,
            )?,
            connection_generation: required_u64(
                generations,
                row,
                "connection_generation",
                context,
            )?,
            websocket_ingress_sequence: required_u64(
                ws_ingress,
                row,
                "websocket_ingress_sequence",
                context,
            )?,
            rest_response_ingress_sequence: optional_u64(rest_ingress, row),
            rest_response_item_index: optional_u64(rest_item_indexes, row),
            collector_admission_sequence: required_u64(
                admissions,
                row,
                "collector_admission_sequence",
                context,
            )?,
            collector_admission_time_utc_ms: required_timestamp(
                admission_times,
                row,
                "collector_admission_time_utc_ms",
                context,
            )?,
            collector_host_id: required_string(hosts, row, "collector_host_id", context)?,
            collector_region: required_string(regions, row, "collector_region", context)?,
            source_endpoint: required_string(endpoints, row, "source_endpoint", context)?,
            schema_version,
            record,
        });
        rows.push(validate_decoded_row(decoded, row, context)?);
    }
    Ok(rows)
}

/// 已准入 Parquet 的有界批次解码流；每次迭代最多返回调用方指定的批次行数。
pub struct TypedRawParquetStream {
    reader: ParquetRecordBatchReader,
    path: PathBuf,
    source_kind: String,
    schema_version: String,
    expected_rows: u64,
    next_row_index: u64,
    next_batch_index: u64,
    finished: bool,
    scope: Option<TypedRawReadScope>,
    last_physical_batch_rows: usize,
    last_physical_batch_bytes: usize,
    max_physical_batch_bytes: Option<usize>,
}

impl TypedRawParquetStream {
    pub(crate) const fn last_physical_batch_rows(&self) -> usize {
        self.last_physical_batch_rows
    }

    pub(crate) const fn last_physical_batch_bytes(&self) -> usize {
        self.last_physical_batch_bytes
    }
}

impl Iterator for TypedRawParquetStream {
    type Item = Result<Vec<TypedRawRow>, TypedRawReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.reader.next() {
            Some(Ok(batch)) => {
                self.last_physical_batch_rows = batch.num_rows();
                self.last_physical_batch_bytes = batch.get_array_memory_size();
                let batch_index = self.next_batch_index;
                self.next_batch_index = self.next_batch_index.saturating_add(1);
                if let Some(limit) = self.max_physical_batch_bytes
                    && self.last_physical_batch_bytes > limit
                {
                    self.finished = true;
                    return Some(Err(TypedRawReadError::DecodeBatchBytesLimit {
                        path: self.path.clone(),
                        batch_index,
                        observed: self.last_physical_batch_bytes,
                        limit,
                    }));
                }
                let first_row_index = self.next_row_index;
                let batch_rows = match u64::try_from(batch.num_rows()) {
                    Ok(rows) => rows,
                    Err(_) => {
                        self.finished = true;
                        return Some(Err(TypedRawReadError::Parquet {
                            path: self.path.clone(),
                            batch_index: Some(batch_index),
                            detail: "批次行数无法表示为 u64".to_owned(),
                        }));
                    }
                };
                let decoded = decode_typed_raw_record_batch(
                    &self.source_kind,
                    &self.schema_version,
                    &batch,
                    first_row_index,
                    self.scope.as_ref(),
                );
                match decoded {
                    Ok(rows) => match self.next_row_index.checked_add(batch_rows) {
                        Some(next_row_index) => {
                            self.next_row_index = next_row_index;
                            Some(Ok(rows))
                        }
                        None => {
                            self.finished = true;
                            Some(Err(TypedRawReadError::Parquet {
                                path: self.path.clone(),
                                batch_index: Some(batch_index),
                                detail: "累计行数溢出 u64".to_owned(),
                            }))
                        }
                    },
                    Err(error) => {
                        self.finished = true;
                        Some(Err(error))
                    }
                }
            }
            Some(Err(error)) => {
                self.finished = true;
                Some(Err(TypedRawReadError::Parquet {
                    path: self.path.clone(),
                    batch_index: Some(self.next_batch_index),
                    detail: error.to_string(),
                }))
            }
            None => {
                self.finished = true;
                if self.next_row_index == self.expected_rows {
                    None
                } else {
                    Some(Err(TypedRawReadError::FooterRowCountMismatch {
                        path: self.path.clone(),
                        expected_rows: self.expected_rows,
                        actual_rows: self.next_row_index,
                    }))
                }
            }
        }
    }
}

impl std::iter::FusedIterator for TypedRawParquetStream {}

/// 打开已完成阶段 1 完整准入的直接源文件，并按显式正数批次上限流式解码。
pub fn open_validated_typed_raw_parquet(
    file: &ValidatedDirectSourceParquetFileV3,
    batch_size: usize,
) -> Result<TypedRawParquetStream, TypedRawReadError> {
    open_validated_typed_raw_parquet_inner(file, batch_size, None, None)
}

pub(crate) fn open_validated_typed_raw_parquet_in_scope(
    file: &ValidatedDirectSourceParquetFileV3,
    batch_size: usize,
    scope: &TypedRawReadScope,
    max_physical_batch_bytes: usize,
) -> Result<TypedRawParquetStream, TypedRawReadError> {
    open_validated_typed_raw_parquet_inner(
        file,
        batch_size,
        Some(scope.clone()),
        Some(max_physical_batch_bytes),
    )
}

fn open_validated_typed_raw_parquet_inner(
    file: &ValidatedDirectSourceParquetFileV3,
    batch_size: usize,
    scope: Option<TypedRawReadScope>,
    max_physical_batch_bytes: Option<usize>,
) -> Result<TypedRawParquetStream, TypedRawReadError> {
    if batch_size == 0 {
        return Err(TypedRawReadError::InvalidBatchSize);
    }
    let path = file.canonical_path().to_path_buf();
    let mut opened = File::open(&path).map_err(|source| TypedRawReadError::Io {
        path: path.clone(),
        source,
    })?;
    let actual_bytes = opened
        .metadata()
        .map_err(|source| TypedRawReadError::Io {
            path: path.clone(),
            source,
        })?
        .len();
    if actual_bytes != file.bytes() {
        return Err(TypedRawReadError::AdmittedFileChanged {
            path,
            expected_bytes: file.bytes(),
            actual_bytes,
        });
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = opened
            .read(&mut buffer)
            .map_err(|source| TypedRawReadError::Io {
                path: path.clone(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual_sha256 = format!("{:x}", hasher.finalize());
    if actual_sha256 != file.sha256() {
        return Err(TypedRawReadError::AdmittedFileDigestMismatch {
            path,
            expected_sha256: file.sha256().to_owned(),
            actual_sha256,
        });
    }
    opened
        .seek(SeekFrom::Start(0))
        .map_err(|source| TypedRawReadError::Io {
            path: path.clone(),
            source,
        })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(opened).map_err(|error| {
        TypedRawReadError::Parquet {
            path: path.clone(),
            batch_index: None,
            detail: error.to_string(),
        }
    })?;
    validate_physical_schema(
        file.source_kind(),
        file.schema_version(),
        builder.schema().as_ref(),
    )?;
    let projected_columns = expected_projection_columns(file.source_kind(), file.schema_version())?;
    let projection = ProjectionMask::columns(
        builder.parquet_schema(),
        projected_columns.iter().map(String::as_str),
    );
    let actual_rows =
        u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
            TypedRawReadError::Parquet {
                path: path.clone(),
                batch_index: None,
                detail: "Parquet footer 行数为负数".to_owned(),
            }
        })?;
    if actual_rows != file.rows() {
        return Err(TypedRawReadError::FooterRowCountMismatch {
            path,
            expected_rows: file.rows(),
            actual_rows,
        });
    }
    let reader = builder
        .with_batch_size(batch_size)
        .with_projection(projection)
        .build()
        .map_err(|error| TypedRawReadError::Parquet {
            path: path.clone(),
            batch_index: None,
            detail: error.to_string(),
        })?;
    Ok(TypedRawParquetStream {
        reader,
        path,
        source_kind: file.source_kind().to_owned(),
        schema_version: file.schema_version().to_owned(),
        expected_rows: file.rows(),
        next_row_index: 0,
        next_batch_index: 0,
        finished: false,
        scope,
        last_physical_batch_rows: 0,
        last_physical_batch_bytes: 0,
        max_physical_batch_bytes,
    })
}

/// 只读取已准入 Parquet 的首个有界批次；空文件返回空向量。
pub fn read_first_validated_typed_raw_batch(
    file: &ValidatedDirectSourceParquetFileV3,
    batch_size: usize,
) -> Result<Vec<TypedRawRow>, TypedRawReadError> {
    let mut stream = open_validated_typed_raw_parquet(file, batch_size)?;
    stream.next().transpose().map(Option::unwrap_or_default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn scoped_selection_uses_left_closed_receive_time_and_exact_polymarket_market() {
        let received = TimestampMillisecondArray::from(vec![99, 100, 199, 200]);
        let markets = StringArray::from(vec!["target", "target", "other", "target"]);
        let scope = TypedRawReadScope::new(100, 200, "target");
        let context = DecodeContext {
            source_kind: POLYMARKET_ORDERBOOK_SOURCE,
            schema_version: POLYMARKET_ORDERBOOK_SCHEMA,
            first_row_index: 0,
        };

        assert!(
            !row_is_selected(
                Some(&scope),
                &received,
                Some((&markets, "market")),
                0,
                context,
            )
            .unwrap()
        );
        assert!(
            row_is_selected(
                Some(&scope),
                &received,
                Some((&markets, "market")),
                1,
                context,
            )
            .unwrap()
        );
        assert!(
            !row_is_selected(
                Some(&scope),
                &received,
                Some((&markets, "market")),
                2,
                context,
            )
            .unwrap()
        );
        assert!(
            !row_is_selected(
                Some(&scope),
                &received,
                Some((&markets, "market")),
                3,
                context,
            )
            .unwrap()
        );
    }

    #[test]
    fn wrong_physical_schema_fails_before_row_decode() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let error = decode_typed_raw_record_batch(
            POLYMARKET_ORDERBOOK_SOURCE,
            POLYMARKET_ORDERBOOK_SCHEMA,
            &batch,
            0,
            None,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("schema"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn admitted_file_same_size_replacement_fails_on_content_identity() {
        let temp = TempDir::new().expect("创建准入后替换回归临时目录");
        let day_directory = temp.path().join("2026-07-30/typed_raw/utc_day=2026-07-30");
        let source_directory = day_directory.join("source_kind=binance_aggtrade");
        std::fs::create_dir_all(&source_directory).expect("创建合法 typed_raw 来源目录");
        let parquet_path = source_directory.join("binance_aggtrade_00.parquet");
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new(
                    "parquet_raw_source_kind".to_owned(),
                    BINANCE_AGGTRADE_SOURCE.to_owned(),
                ),
                KeyValue::new(
                    "parquet_raw_layout_version".to_owned(),
                    BINANCE_AGGTRADE_SCHEMA.to_owned(),
                ),
                KeyValue::new(
                    "data_collect_artifact_kind".to_owned(),
                    "typed_raw_parquet".to_owned(),
                ),
                KeyValue::new(
                    "data_collect_daily_file_fact_v1".to_owned(),
                    r#"{"source_kind":"binance_aggtrade","schema_version":"binance_aggtrade_v7","utc_hour":"2026-07-30T00","part":0,"rows":0}"#
                        .to_owned(),
                ),
            ]))
            .build();
        let schema = expected_schema(BINANCE_AGGTRADE_SOURCE, BINANCE_AGGTRADE_SCHEMA)
            .expect("当前 aggTrade schema 必须存在");
        ArrowWriter::try_new(
            File::create(&parquet_path).expect("创建合法微型 Parquet"),
            schema,
            Some(properties),
        )
        .expect("创建合法微型 Parquet writer")
        .close()
        .expect("关闭合法微型 Parquet writer");

        let original_bytes = std::fs::read(&parquet_path).expect("读取合法微型 Parquet");
        let original_sha256 = format!("{:x}", Sha256::digest(&original_bytes));
        let manifest_path = day_directory.join("manifest.json");
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&json!({
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
                },
                "producer_barriers": [
                    "binance_aggtrade",
                    "binance_depth",
                    "polymarket",
                    "polymarket_rtds"
                ],
                "writer_finalize_acks": [
                    "binance_aggtrade",
                    "binance_depth_snapshot",
                    "binance_depth_update",
                    "polymarket_market",
                    "polymarket_market_metadata",
                    "polymarket_rtds_chainlink_btc_usd"
                ],
                "files": [{
                    "path": "source_kind=binance_aggtrade/binance_aggtrade_00.parquet",
                    "source_kind": BINANCE_AGGTRADE_SOURCE,
                    "schema_version": BINANCE_AGGTRADE_SCHEMA,
                    "utc_hour": "2026-07-30T00",
                    "part": 0,
                    "bytes": original_bytes.len(),
                    "rows": 0,
                    "sha256": original_sha256,
                }]
            }))
            .expect("编码合法微型清单"),
        )
        .expect("写入合法微型清单");
        let admitted =
            crate::validate_daily_manifest_admission_at_root(temp.path(), &manifest_path)
                .expect("合法微型 Parquet 必须经阶段 1 入口准入");
        let admitted_file = admitted.files().first().expect("必须有一个已准入文件");

        let mut replacement = original_bytes;
        let replacement_index = replacement.len() / 2;
        replacement[replacement_index] ^= 1;
        std::fs::write(&parquet_path, &replacement).expect("同字节数替换已准入 Parquet");
        assert_eq!(replacement.len() as u64, admitted_file.bytes());

        let error = open_validated_typed_raw_parquet(admitted_file, 1)
            .err()
            .expect("same-size replacement must fail");
        assert!(matches!(
            error,
            TypedRawReadError::AdmittedFileDigestMismatch {
                expected_sha256,
                actual_sha256,
                ..
            } if expected_sha256 == admitted_file.sha256()
                && actual_sha256 == format!("{:x}", Sha256::digest(&replacement))
        ));
    }
}
