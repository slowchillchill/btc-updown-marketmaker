use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array as _, ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine as _;
use data_collect::RawEvent;
use data_collect::binance_aggtrade::{
    BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
    BinanceAggTradeOutput, BinanceWsAggTrade,
};
use data_collect::handoff::{
    DepthHandoffRecordKind, DepthHandoffSourceEvidence, HandoffFrame, HandoffSourceEvidence,
};
use data_collect::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
};
use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow,
};
use data_collect::source_hourly_layout::{
    BINANCE_AGGTRADE_CAPTURE_COMPLETE, BINANCE_AGGTRADE_SCHEMA_VERSION,
    BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION, BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
    BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION, BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
    BINANCE_DEPTH_VERDICT_APPLIED, BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE,
    BinanceAggTradeHourlyRow, BinanceAggTradeInvalidEvidence, BinanceAggTradeOrdering,
    BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
};
use data_collect::writer::{
    ParquetSegmentWriter, TypedParquetSegmentWriter, write_segment_on_blocking_pool_for_source,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_admission: None,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: "synthetic://polymarket-market".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","asset_id":"asset-{seq}"}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

fn depth_snapshot_row(
    depth_ingress_sequence: u64,
    collector_admission_sequence: u64,
) -> BinanceDepthSnapshotHourlyRow {
    BinanceDepthSnapshotHourlyRow {
        timestamp_received: 1_766_789_469_999,
        snapshot_fetch_start_utc: 1_766_789_469_900,
        last_update_id: 100,
        bids: r#"[["99.00","1.5"]]"#.to_owned(),
        asks: r#"[["102.00","2.5"]]"#.to_owned(),
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
            .to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: Some("depth-epoch-1".to_owned()),
        connection_generation: Some(1),
        depth_ingress_sequence: Some(depth_ingress_sequence),
        collector_admission_sequence: Some(collector_admission_sequence),
        collector_admission_time_utc_ms: Some(1_766_789_470_000),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.to_owned(),
    }
}

fn depth_update_row(
    depth_ingress_sequence: u64,
    collector_admission_sequence: u64,
) -> BinanceDepthUpdateHourlyRow {
    BinanceDepthUpdateHourlyRow {
        timestamp_received: 1_766_789_469_998,
        event_type: "depthUpdate".to_owned(),
        event_time: 1_766_789_469_958,
        symbol: "BTCUSDT".to_owned(),
        first_update_id: 101,
        final_update_id: 103,
        bids: r#"[["100.00","1.0"]]"#.to_owned(),
        asks: r#"[["101.00","2.0"]]"#.to_owned(),
        stream: "btcusdt@depth".to_owned(),
        snapshot_last_update_id: Some(100),
        local_u_before: None,
        stitch_verdict: BINANCE_DEPTH_VERDICT_APPLIED.to_owned(),
        stitch_evidence: Some("U <= lastUpdateId + 1 <= u".to_owned()),
        stitch_gap_reason: None,
        source_sequence_epoch_id: Some("depth-epoch-1".to_owned()),
        connection_generation: Some(1),
        depth_ingress_sequence: Some(depth_ingress_sequence),
        collector_admission_sequence: Some(collector_admission_sequence),
        collector_admission_time_utc_ms: Some(1_766_789_470_001),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
        schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.to_owned(),
    }
}

fn read_single_finalized_batch(path: &Path) -> RecordBatch {
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(
        reader.next().is_none(),
        "expected exactly one finalized batch"
    );
    batch
}

fn handoff_json_round_trip(frame: &HandoffFrame) -> HandoffFrame {
    serde_json::from_str(&frame.to_json_line().unwrap()).unwrap()
}

fn parquet_value(batch: &RecordBatch, row_index: usize, column_name: &str) -> serde_json::Value {
    let column = batch.column(batch.schema().index_of(column_name).unwrap());
    if column.is_null(row_index) {
        return serde_json::Value::Null;
    }
    match column.data_type() {
        DataType::Utf8 => serde_json::json!(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_index)
        ),
        DataType::UInt64 => serde_json::json!(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row_index)
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => serde_json::json!(
            column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row_index)
        ),
        data_type => panic!("unsupported projection type for {column_name}: {data_type:?}"),
    }
}

fn parquet_projection(
    batch: &RecordBatch,
    row_index: usize,
    column_names: &[&str],
) -> serde_json::Value {
    serde_json::Value::Object(
        column_names
            .iter()
            .map(|column_name| {
                (
                    (*column_name).to_owned(),
                    parquet_value(batch, row_index, column_name),
                )
            })
            .collect(),
    )
}

#[test]
fn parquet_writer_schema_metadata_contract() {
    let tmp = tempdir().unwrap();
    let first_event = synthetic_event(1);
    assert!(serde_json::from_str::<serde_json::Value>(&first_event.raw_json).is_ok());
    let mut writer = ParquetSegmentWriter::create_for_source(
        tmp.path(),
        "polymarket-market",
        "parquet_raw_layout_v1",
        "polymarket_market",
    )
    .unwrap();

    let stats = writer
        .write_events_with_stats(&[first_event, synthetic_event(2)])
        .unwrap();
    let published = writer.finalize().unwrap();

    assert_eq!(stats.rows, 2);
    assert!(stats.estimated_event_bytes > 0);
    assert!(stats.row_group_memory_estimate_bytes >= stats.estimated_event_bytes);
    assert_eq!(published.row_count, 2);

    let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("parquet key-value metadata");
    let metadata = metadata
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        metadata
            .get("parquet_raw_layout_version")
            .and_then(|value| value.as_deref()),
        Some("parquet_raw_layout_v1")
    );
    assert_eq!(
        metadata
            .get("parquet_raw_source_kind")
            .and_then(|value| value.as_deref()),
        Some("polymarket_market")
    );
    assert_eq!(
        metadata
            .get("data_collect_artifact_kind")
            .and_then(|value| value.as_deref()),
        Some("typed_raw_parquet")
    );
}

#[test]
fn parquet_writer_rejects_path_traversal_segment_stem() {
    let tmp = tempdir().unwrap();

    let result = ParquetSegmentWriter::create_for_source(
        tmp.path(),
        "../escape",
        "parquet_raw_layout_v1",
        "polymarket_market",
    );
    assert!(result.is_err(), "unsafe stem must be rejected");
    let error = result.err().unwrap();

    assert!(
        error
            .to_string()
            .contains("parquet segment stem is not a safe path component")
    );
    assert!(!tmp.path().join("../escape-running.parquet").exists());
}

#[tokio::test]
async fn blocking_work_is_isolated_from_main_runtime() {
    let tmp = tempdir().unwrap();

    let published = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        write_segment_on_blocking_pool_for_source(
            tmp.path().to_path_buf(),
            "polymarket-market".to_owned(),
            "parquet_raw_layout_v1".to_owned(),
            "polymarket_market".to_owned(),
            vec![vec![synthetic_event(1), synthetic_event(2)]],
        ),
    )
    .await
    .expect("blocking writer task should not stall runtime")
    .unwrap();

    assert_eq!(published.row_count, 2);
    assert!(published.final_path.exists());
}

#[test]
fn parquet_writer_writes_pmxt_like_orderbook_schema() {
    let tmp = tempdir().unwrap();
    let row = PolymarketOrderbookRow::book_from_raw_json(
        r#"{
            "event_type": "book",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "timestamp": 1783392404599,
            "bids": [["0.49","120.5"]],
            "asks": [["0.51","88"]],
            "hash": "book-hash"
        }"#,
        PolymarketOrderbookContext {
            timestamp_received_ms: 1_783_392_404_599,
            slug: Some("btc-updown-5m-1783392300".to_owned()),
            source_native_payload_json:
                r#"{"source":"polymarket_market","event_type":"book"}"#.to_owned(),
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            schema_version: "polymarket_orderbook_v2".to_owned(),
        },
    )
    .expect("valid book row");

    let mut writer = TypedParquetSegmentWriter::create_polymarket_orderbook(
        tmp.path(),
        "polymarket-market-typed",
        "polymarket_orderbook_v2",
    )
    .unwrap();
    writer.write_polymarket_orderbook_rows(&[row]).unwrap();
    let published = writer.finalize().unwrap();

    let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
    let fields = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect::<Vec<_>>();
    for field in [
        "timestamp_received",
        "timestamp",
        "market",
        "event_type",
        "asset_id",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
    ] {
        assert!(fields.contains(&field.to_owned()), "missing field {field}");
    }
    assert!(!fields.contains(&"raw_json".to_owned()));

    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("parquet key-value metadata")
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        metadata
            .get("parquet_raw_layout_version")
            .and_then(|value| value.as_deref()),
        Some("polymarket_orderbook_v2")
    );
    assert_eq!(
        metadata
            .get("parquet_raw_source_kind")
            .and_then(|value| value.as_deref()),
        Some("polymarket_market")
    );
    assert_eq!(
        metadata
            .get("data_collect_artifact_kind")
            .and_then(|value| value.as_deref()),
        Some("typed_raw_parquet")
    );

    let file = File::open(&published.final_path).unwrap();
    let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = batch_reader.next().unwrap().unwrap();
    let bids = batch
        .column(batch.schema().index_of("bids").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let asks = batch
        .column(batch.schema().index_of("asks").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(bids.value(0), r#"[["0.49","120.5"]]"#);
    assert_eq!(asks.value(0), r#"[["0.51","88"]]"#);
}

#[test]
fn typed_writer_rejects_new_polymarket_market_metadata_v2_segments() {
    let tmp = tempdir().unwrap();
    let error = match TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata",
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
        "polymarket_market_metadata",
    ) {
        Ok(_) => panic!("historical metadata v2 must remain read-only"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unsupported Polymarket metadata schema")
    );
}

#[test]
fn polymarket_metadata_writer_does_not_mix_v2_and_v3() {
    let tmp = tempdir().unwrap();
    let _v3_writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v3",
        POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();

    let tmp = tempdir().unwrap();
    let error = match TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v2",
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
        "polymarket_market_metadata",
    ) {
        Ok(_) => panic!("historical metadata v2 must remain read-only"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unsupported Polymarket metadata schema")
    );

    let tmp = tempdir().unwrap();
    let error = match TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-unknown",
        "polymarket_market_metadata_v4",
        "polymarket_market_metadata",
    ) {
        Ok(_) => panic!("unknown Polymarket metadata schema must be rejected"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unsupported Polymarket metadata schema")
    );
}

#[test]
fn typed_writer_constructors_publish_source_specific_schema_metadata() {
    let cases = [
        (
            "polymarket_market_metadata",
            POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
            "collector_admission_order",
        ),
        (
            "binance_depth_snapshot",
            BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
            "collector_admission_order",
        ),
        (
            "binance_depth_update",
            BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            "collector_admission_order",
        ),
        (
            "binance_aggtrade",
            BINANCE_AGGTRADE_SCHEMA_VERSION,
            "collector_admission_order",
        ),
        (
            "polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_btc_usd_v4",
            "receive_order",
        ),
    ];

    for (source_kind, schema_version, expected_sort_order) in cases {
        let tmp = tempdir().unwrap();
        let writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            source_kind,
            schema_version,
            source_kind,
        )
        .unwrap();
        let published = writer.finalize().unwrap();
        let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
        let metadata = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("parquet key-value metadata")
            .iter()
            .map(|item| (item.key.clone(), item.value.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            metadata
                .get("parquet_raw_layout_version")
                .and_then(|value| value.as_deref()),
            Some(schema_version)
        );
        assert_eq!(
            metadata
                .get("parquet_raw_source_kind")
                .and_then(|value| value.as_deref()),
            Some(source_kind)
        );
        assert_eq!(
            metadata
                .get("sort_order")
                .and_then(|value| value.as_deref()),
            Some(expected_sort_order)
        );
    }
}

#[test]
fn typed_writer_source_specific_schemas_omit_duplicate_time_columns() {
    let cases = [
        (
            "binance_depth_snapshot",
            BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
            vec!["recv_time_utc", "snapshot_recv_time_utc"],
        ),
        (
            "binance_depth_update",
            BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
            vec!["recv_time_utc"],
        ),
        (
            "binance_aggtrade",
            BINANCE_AGGTRADE_SCHEMA_VERSION,
            vec!["recv_time_utc"],
        ),
        (
            "polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_btc_usd_v4",
            vec!["recv_time_utc"],
        ),
    ];

    for (source_kind, schema_version, removed_fields) in cases {
        let tmp = tempdir().unwrap();
        let writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            source_kind,
            schema_version,
            source_kind,
        )
        .unwrap();
        let published = writer.finalize().unwrap();
        let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
        let fields = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>();

        for removed_field in removed_fields {
            assert!(
                !fields.contains(&removed_field.to_owned()),
                "{source_kind} still writes duplicate field {removed_field}"
            );
        }
        assert!(
            fields.contains(&"timestamp_received".to_owned()),
            "{source_kind} must retain timestamp_received"
        );
    }
}

#[test]
fn typed_writer_depth_v3_v4_round_trip_preserves_ingress_and_admission_coordinates() {
    let snapshot_tmp = tempdir().unwrap();
    let mut snapshot_writer = TypedParquetSegmentWriter::create_for_typed_source(
        snapshot_tmp.path(),
        "depth-snapshot-v3",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        "binance_depth_snapshot",
    )
    .unwrap();
    snapshot_writer
        .write_binance_depth_snapshot_rows(&[depth_snapshot_row(2, 1)])
        .unwrap();
    let snapshot = snapshot_writer.finalize().unwrap();

    let mut snapshot_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(snapshot.final_path).unwrap())
            .unwrap()
            .build()
            .unwrap();
    let snapshot_batch = snapshot_reader.next().unwrap().unwrap();
    let snapshot_ingress = snapshot_batch
        .column(
            snapshot_batch
                .schema()
                .index_of("depth_ingress_sequence")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let snapshot_admission = snapshot_batch
        .column(
            snapshot_batch
                .schema()
                .index_of("collector_admission_sequence")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let snapshot_epoch = snapshot_batch
        .column(
            snapshot_batch
                .schema()
                .index_of("source_sequence_epoch_id")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let snapshot_generation = snapshot_batch
        .column(
            snapshot_batch
                .schema()
                .index_of("connection_generation")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let snapshot_admission_time = snapshot_batch
        .column(
            snapshot_batch
                .schema()
                .index_of("collector_admission_time_utc_ms")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let snapshot_last_update_id = snapshot_batch
        .column(snapshot_batch.schema().index_of("last_update_id").unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let snapshot_bids = snapshot_batch
        .column(snapshot_batch.schema().index_of("bids").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(snapshot_ingress.value(0), 2);
    assert_eq!(snapshot_admission.value(0), 1);
    assert_eq!(snapshot_epoch.value(0), "depth-epoch-1");
    assert_eq!(snapshot_generation.value(0), 1);
    assert_eq!(snapshot_admission_time.value(0), 1_766_789_470_000);
    assert_eq!(snapshot_last_update_id.value(0), 100);
    assert_eq!(snapshot_bids.value(0), r#"[["99.00","1.5"]]"#);

    let update_tmp = tempdir().unwrap();
    let mut update_writer = TypedParquetSegmentWriter::create_for_typed_source(
        update_tmp.path(),
        "depth-update-v4",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
        "binance_depth_update",
    )
    .unwrap();
    let applied = depth_update_row(1, 2);
    let mut unavailable = depth_update_row(2, 3);
    unavailable.snapshot_last_update_id = None;
    unavailable.local_u_before = None;
    unavailable.stitch_verdict = BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE.to_owned();
    unavailable.stitch_evidence = None;
    update_writer
        .write_binance_depth_update_rows(&[applied, unavailable])
        .unwrap();
    let update = update_writer.finalize().unwrap();

    let mut update_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(update.final_path).unwrap())
            .unwrap()
            .build()
            .unwrap();
    let update_batch = update_reader.next().unwrap().unwrap();
    let snapshot_last_update_id = update_batch
        .column(
            update_batch
                .schema()
                .index_of("snapshot_last_update_id")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let verdict = update_batch
        .column(update_batch.schema().index_of("stitch_verdict").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let update_evidence = update_batch
        .column(update_batch.schema().index_of("stitch_evidence").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let update_epoch = update_batch
        .column(
            update_batch
                .schema()
                .index_of("source_sequence_epoch_id")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let update_generation = update_batch
        .column(
            update_batch
                .schema()
                .index_of("connection_generation")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let update_ingress = update_batch
        .column(
            update_batch
                .schema()
                .index_of("depth_ingress_sequence")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let update_admission = update_batch
        .column(
            update_batch
                .schema()
                .index_of("collector_admission_sequence")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let update_admission_time = update_batch
        .column(
            update_batch
                .schema()
                .index_of("collector_admission_time_utc_ms")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let first_update_id = update_batch
        .column(update_batch.schema().index_of("first_update_id").unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(snapshot_last_update_id.value(0), 100);
    assert!(snapshot_last_update_id.is_null(1));
    assert_eq!(
        verdict.value(1),
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert_eq!(update_evidence.value(0), "U <= lastUpdateId + 1 <= u");
    assert!(update_evidence.is_null(1));
    assert_eq!(update_epoch.value(0), "depth-epoch-1");
    assert_eq!(update_generation.value(0), 1);
    assert_eq!(update_ingress.values(), &[1, 2]);
    assert_eq!(update_admission.values(), &[2, 3]);
    assert_eq!(update_admission_time.value(0), 1_766_789_470_001);
    assert_eq!(first_update_id.value(0), 101);
}

#[test]
fn real_legacy_depth_v2_v3_parquet_reads_without_fabricated_ordering_coordinates() {
    let timestamp = |name: &str| {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )
    };
    let snapshot_schema = Arc::new(Schema::new(vec![
        timestamp("timestamp_received"),
        timestamp("snapshot_fetch_start_utc"),
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("snapshot_status", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]));
    let snapshot_columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from_iter_values([1_766_789_469_999]).with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from_iter_values([1_766_789_469_900]).with_timezone("UTC"),
        ),
        Arc::new(UInt64Array::from_iter_values([100])),
        Arc::new(StringArray::from_iter_values([r#"[["99.00","1.5"]]"#])),
        Arc::new(StringArray::from_iter_values([r#"[["102.00","2.5"]]"#])),
        Arc::new(StringArray::from_iter_values(["BTCUSDT"])),
        Arc::new(StringArray::from_iter_values([
            "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000",
        ])),
        Arc::new(StringArray::from_iter_values(["complete"])),
        Arc::new(StringArray::from_iter_values(["collector-host"])),
        Arc::new(StringArray::from_iter_values(["eu-west-1"])),
        Arc::new(StringArray::from_iter_values([
            BINANCE_DEPTH_SNAPSHOT_LEGACY_SCHEMA_VERSION,
        ])),
    ];

    let update_schema = Arc::new(Schema::new(vec![
        timestamp("timestamp_received"),
        Field::new("event_type", DataType::Utf8, false),
        timestamp("event_time"),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
        Field::new("bids", DataType::Utf8, false),
        Field::new("asks", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new("snapshot_last_update_id", DataType::UInt64, false),
        Field::new("local_u_before", DataType::UInt64, true),
        Field::new("stitch_verdict", DataType::Utf8, false),
        Field::new("stitch_gap_reason", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]));
    let update_columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from_iter_values([1_766_789_469_998]).with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(["depthUpdate"])),
        Arc::new(
            TimestampMillisecondArray::from_iter_values([1_766_789_469_958]).with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(["BTCUSDT"])),
        Arc::new(UInt64Array::from_iter_values([101])),
        Arc::new(UInt64Array::from_iter_values([103])),
        Arc::new(StringArray::from_iter_values([r#"[["100.00","1.0"]]"#])),
        Arc::new(StringArray::from_iter_values([r#"[["101.00","2.0"]]"#])),
        Arc::new(StringArray::from_iter_values(["btcusdt@depth"])),
        Arc::new(UInt64Array::from_iter_values([100])),
        Arc::new(UInt64Array::from(vec![None])),
        Arc::new(StringArray::from_iter_values([
            "U <= lastUpdateId + 1 <= u",
        ])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(StringArray::from_iter_values(["collector-host"])),
        Arc::new(StringArray::from_iter_values(["eu-west-1"])),
        Arc::new(StringArray::from_iter_values([
            "wss://stream.binance.com:9443/ws/btcusdt@depth",
        ])),
        Arc::new(StringArray::from_iter_values([
            BINANCE_DEPTH_UPDATE_LEGACY_SCHEMA_VERSION,
        ])),
    ];

    let tmp = tempdir().unwrap();
    let snapshot_path = tmp.path().join("legacy-snapshot-v2.parquet");
    let mut snapshot_writer = ArrowWriter::try_new(
        File::create(&snapshot_path).unwrap(),
        snapshot_schema.clone(),
        None,
    )
    .unwrap();
    snapshot_writer
        .write(&RecordBatch::try_new(snapshot_schema, snapshot_columns).unwrap())
        .unwrap();
    snapshot_writer.close().unwrap();
    let update_path = tmp.path().join("legacy-update-v3.parquet");
    let mut update_writer = ArrowWriter::try_new(
        File::create(&update_path).unwrap(),
        update_schema.clone(),
        None,
    )
    .unwrap();
    update_writer
        .write(&RecordBatch::try_new(update_schema, update_columns).unwrap())
        .unwrap();
    update_writer.close().unwrap();

    let snapshot_batch =
        ParquetRecordBatchReaderBuilder::try_new(File::open(snapshot_path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
    let update_batch = ParquetRecordBatchReaderBuilder::try_new(File::open(update_path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    for batch in [&snapshot_batch, &update_batch] {
        for coordinate in [
            "source_sequence_epoch_id",
            "connection_generation",
            "depth_ingress_sequence",
            "collector_admission_sequence",
            "collector_admission_time_utc_ms",
        ] {
            assert!(batch.schema().index_of(coordinate).is_err(), "{coordinate}");
        }
    }

    let snapshot_version = snapshot_batch
        .column(snapshot_batch.schema().index_of("schema_version").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let mut legacy_snapshot = depth_snapshot_row(1, 1);
    legacy_snapshot.source_sequence_epoch_id = None;
    legacy_snapshot.connection_generation = None;
    legacy_snapshot.depth_ingress_sequence = None;
    legacy_snapshot.collector_admission_sequence = None;
    legacy_snapshot.collector_admission_time_utc_ms = None;
    legacy_snapshot.schema_version = snapshot_version.to_owned();
    assert_eq!(
        legacy_snapshot.ordering_classification().unwrap().as_str(),
        "legacy_ordering_unknown"
    );

    let update_version = update_batch
        .column(update_batch.schema().index_of("schema_version").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let mut legacy_update = depth_update_row(1, 1);
    legacy_update.stitch_verdict = "U <= lastUpdateId + 1 <= u".to_owned();
    legacy_update.stitch_evidence = None;
    legacy_update.source_sequence_epoch_id = None;
    legacy_update.connection_generation = None;
    legacy_update.depth_ingress_sequence = None;
    legacy_update.collector_admission_sequence = None;
    legacy_update.collector_admission_time_utc_ms = None;
    legacy_update.schema_version = update_version.to_owned();
    assert_eq!(
        legacy_update.ordering_classification().unwrap().as_str(),
        "legacy_ordering_unknown"
    );
}

#[test]
fn typed_writer_depth_rejects_legacy_versions_partial_coordinates_and_non_increasing_admission() {
    let tmp = tempdir().unwrap();
    let error = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "legacy-depth-snapshot",
        "binance_depth_snapshot_v2",
        "binance_depth_snapshot",
    )
    .err()
    .expect("legacy depth schema is read-only");
    assert!(error.to_string().contains("read-only legacy depth schema"));

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "depth-update-validation",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
        "binance_depth_update",
    )
    .unwrap();
    let mut partial = depth_update_row(1, 1);
    partial.connection_generation = None;
    let error = writer
        .write_binance_depth_update_rows(&[partial])
        .unwrap_err();
    assert!(error.to_string().contains("row validation failed"));

    let mut wrong_official_constant = depth_update_row(1, 1);
    wrong_official_constant.symbol = "ETHUSDT".to_owned();
    let error = writer
        .write_binance_depth_update_rows(&[wrong_official_constant])
        .unwrap_err();
    assert!(error.to_string().contains("row validation failed"));

    writer
        .write_binance_depth_update_rows(&[depth_update_row(2, 2)])
        .unwrap();
    let error = writer
        .write_binance_depth_update_rows(&[depth_update_row(3, 2)])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("collector admission sequence is not strictly increasing")
    );
}

#[test]
fn typed_writer_binance_aggtrade_schema_keeps_distinct_event_and_trade_times() {
    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    let websocket = BinanceWsAggTrade::from_wire(
        &serde_json::json!({
            "e":"aggTrade","E":1766789469958_i64,"s":"BTCUSDT","a":123_u64,
            "p":"100.50","q":"0.1","f":10_u64,"l":12_u64,"T":1766789469000_i64,
            "m":true,"M":false
        }),
        "btcusdt@aggTrade",
        1_766_789_470_100,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
    )
    .unwrap();
    let websocket_row = BinanceAggTradeHourlyRow::from_output(
        &BinanceAggTradeOutput::Websocket(websocket),
        "collector-host",
        "eu-west-1",
        "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
        BinanceAggTradeOrdering {
            source_sequence_epoch_id: "source-task-epoch-1".to_owned(),
            connection_generation: 4,
            websocket_ingress_sequence: 1,
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: 1_766_789_470_101,
        },
    )
    .unwrap();
    let invalid_timestamp = 1_766_789_470_400;
    let invalid_endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let invalid_payload = b"{bad";
    let invalid_payload_length = u64::try_from(invalid_payload.len()).unwrap();
    let invalid_payload_sha256 = format!("{:x}", Sha256::digest(invalid_payload));
    let invalid_row = BinanceAggTradeHourlyRow::from_invalid_evidence(
        BinanceAggTradeInvalidEvidence {
            timestamp_received: invalid_timestamp,
            ingest_source: "websocket".to_owned(),
            source_native_payload_json: serde_json::json!({
                "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
                "transport": "websocket",
                "endpoint": invalid_endpoint,
                "receive_time_utc_ms": invalid_timestamp,
                "invalid_reason_code": "invalid_json",
                "payload_encoding": "base64",
                "payload_length_bytes": invalid_payload_length,
                "payload_sha256": invalid_payload_sha256,
                "payload_capture_status": BINANCE_AGGTRADE_CAPTURE_COMPLETE,
                "payload_base64": base64::engine::general_purpose::STANDARD
                    .encode(invalid_payload),
                "observed_bytes_at_least": invalid_payload_length,
                "declared_content_length": null,
                "capture_limit_bytes": BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES,
            })
            .to_string(),
            source_endpoint: invalid_endpoint.to_owned(),
            invalid_reason_code: "invalid_json".to_owned(),
            invalid_reason_detail: Some("expected value".to_owned()),
            payload_encoding: Some("base64".to_owned()),
            payload_length_bytes: Some(invalid_payload_length),
            payload_sha256: Some(invalid_payload_sha256),
            payload_capture_status: BINANCE_AGGTRADE_CAPTURE_COMPLETE.to_owned(),
        },
        BinanceAggTradeOrdering {
            source_sequence_epoch_id: "source-task-epoch-1".to_owned(),
            connection_generation: 4,
            websocket_ingress_sequence: 2,
            collector_admission_sequence: 2,
            collector_admission_time_utc_ms: 1_766_789_470_401,
        },
        "collector-host",
        "eu-west-1",
    )
    .unwrap();
    for row in [&websocket_row, &invalid_row] {
        assert!(row.validate().is_complete());
    }
    writer
        .write_binance_aggtrade_rows(&[websocket_row, invalid_row])
        .unwrap();
    let published = writer.finalize().unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&published.final_path).unwrap())
            .unwrap();
    let mut batch_reader = builder.build().unwrap();
    let batch = batch_reader.next().unwrap().unwrap();
    let event_time = batch
        .column(batch.schema().index_of("event_time").unwrap())
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let trade_time = batch
        .column(batch.schema().index_of("trade_time").unwrap())
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let ingest_source = batch
        .column(batch.schema().index_of("ingest_source").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let record_kind = batch
        .column(batch.schema().index_of("record_kind").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let aggregate_trade_id = batch
        .column(batch.schema().index_of("aggregate_trade_id").unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let invalid_reason_code = batch
        .column(batch.schema().index_of("invalid_reason_code").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let admission_sequence = batch
        .column(
            batch
                .schema()
                .index_of("collector_admission_sequence")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let connection_generation = batch
        .column(batch.schema().index_of("connection_generation").unwrap())
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let websocket_received = batch
        .column(
            batch
                .schema()
                .index_of("websocket_timestamp_received")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let rest_received = batch
        .column(
            batch
                .schema()
                .index_of("rest_backfill_timestamp_received")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();

    assert_eq!(event_time.value(0), 1_766_789_469_958);
    assert!(event_time.is_null(1));
    assert_eq!(trade_time.value(0), 1_766_789_469_000);
    assert!(trade_time.is_null(1));
    assert_eq!(ingest_source.value(0), "websocket");
    assert_eq!(ingest_source.value(1), "websocket");
    assert_eq!(record_kind.value(0), "trade");
    assert_eq!(record_kind.value(1), "invalid_input");
    assert_eq!(aggregate_trade_id.value(0), 123);
    assert!(aggregate_trade_id.is_null(1));
    assert!(invalid_reason_code.is_null(0));
    assert_eq!(invalid_reason_code.value(1), "invalid_json");
    assert_eq!(admission_sequence.values(), &[1, 2]);
    assert_eq!(connection_generation.values(), &[4, 4]);
    assert_eq!(websocket_received.value(0), 1_766_789_470_100);
    assert_eq!(websocket_received.value(1), invalid_timestamp);
    assert!(rest_received.is_null(0));
    assert!(rest_received.is_null(1));
}

fn websocket_aggtrade_row(
    aggregate_trade_id: u64,
    timestamp_received: i64,
    websocket_ingress_sequence: u64,
    collector_admission_sequence: u64,
) -> BinanceAggTradeHourlyRow {
    let websocket = BinanceWsAggTrade::from_wire(
        &serde_json::json!({
            "e":"aggTrade",
            "E":timestamp_received - 2,
            "s":"BTCUSDT",
            "a":aggregate_trade_id,
            "p":"100.50",
            "q":"0.1",
            "f":aggregate_trade_id,
            "l":aggregate_trade_id,
            "T":timestamp_received - 1,
            "m":true,
            "M":false
        }),
        "btcusdt@aggTrade",
        timestamp_received,
        BinanceAggTradeIngressCoordinate::Websocket {
            sequence: websocket_ingress_sequence,
        },
    )
    .unwrap();
    BinanceAggTradeHourlyRow::from_output(
        &BinanceAggTradeOutput::Websocket(websocket),
        "collector-host",
        "eu-west-1",
        "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
        BinanceAggTradeOrdering {
            source_sequence_epoch_id: "source-task-epoch-order-test".to_owned(),
            connection_generation: 1,
            websocket_ingress_sequence,
            collector_admission_sequence,
            collector_admission_time_utc_ms: timestamp_received,
        },
    )
    .unwrap()
}

fn invalid_websocket_aggtrade_row(
    payload: &[u8],
    invalid_reason_code: &str,
    timestamp_received: i64,
    websocket_ingress_sequence: u64,
    collector_admission_sequence: u64,
) -> BinanceAggTradeHourlyRow {
    let source_endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let payload_length = u64::try_from(payload.len()).unwrap();
    let payload_sha256 = format!("{:x}", Sha256::digest(payload));
    BinanceAggTradeHourlyRow::from_invalid_evidence(
        BinanceAggTradeInvalidEvidence {
            timestamp_received,
            ingest_source: "websocket".to_owned(),
            source_native_payload_json: serde_json::json!({
                "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
                "transport": "websocket",
                "endpoint": source_endpoint,
                "receive_time_utc_ms": timestamp_received,
                "invalid_reason_code": invalid_reason_code,
                "payload_encoding": "base64",
                "payload_length_bytes": payload_length,
                "payload_sha256": payload_sha256,
                "payload_capture_status": BINANCE_AGGTRADE_CAPTURE_COMPLETE,
                "payload_base64": base64::engine::general_purpose::STANDARD.encode(payload),
                "observed_bytes_at_least": payload_length,
                "declared_content_length": null,
                "capture_limit_bytes": BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES,
            })
            .to_string(),
            source_endpoint: source_endpoint.to_owned(),
            invalid_reason_code: invalid_reason_code.to_owned(),
            invalid_reason_detail: None,
            payload_encoding: Some("base64".to_owned()),
            payload_length_bytes: Some(payload_length),
            payload_sha256: Some(payload_sha256),
            payload_capture_status: BINANCE_AGGTRADE_CAPTURE_COMPLETE.to_owned(),
        },
        BinanceAggTradeOrdering {
            source_sequence_epoch_id: "source-task-epoch-order-test".to_owned(),
            connection_generation: 1,
            websocket_ingress_sequence,
            collector_admission_sequence,
            collector_admission_time_utc_ms: timestamp_received,
        },
        "collector-host",
        "eu-west-1",
    )
    .unwrap()
}

#[test]
fn typed_writer_binance_aggtrade_same_timestamp_preserves_admission_order_across_batches() {
    let tmp = tempdir().unwrap();
    let timestamp_received = 1_766_789_470_100;
    let first_invalid_payload = [0xff, 0x00, b'{'];
    let second_invalid_payload = b"{broken";
    let rows = [
        websocket_aggtrade_row(123, timestamp_received, 10, 1),
        invalid_websocket_aggtrade_row(
            &first_invalid_payload,
            "invalid_utf8",
            timestamp_received,
            11,
            2,
        ),
        invalid_websocket_aggtrade_row(
            second_invalid_payload,
            "invalid_json",
            timestamp_received,
            12,
            3,
        ),
        websocket_aggtrade_row(124, timestamp_received, 13, 4),
    ];
    for row in &rows {
        assert!(row.validate().is_complete(), "{:?}", row.validate());
    }

    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade-order",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    writer.write_binance_aggtrade_rows(&rows[..2]).unwrap();
    writer.write_binance_aggtrade_rows(&rows[2..]).unwrap();
    let published = writer.finalize().unwrap();

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&published.final_path).unwrap())
            .unwrap();
    let mut record_kinds = Vec::new();
    let mut admission_sequences = Vec::new();
    let mut receive_times = Vec::new();
    let mut websocket_ingress_sequences = Vec::new();
    let mut rest_ingress_sequences = Vec::new();
    let mut decoded_invalid_payloads = Vec::new();
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        let record_kind = batch
            .column(batch.schema().index_of("record_kind").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let admission_sequence = batch
            .column(
                batch
                    .schema()
                    .index_of("collector_admission_sequence")
                    .unwrap(),
            )
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamp = batch
            .column(batch.schema().index_of("timestamp_received").unwrap())
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let websocket_ingress = batch
            .column(
                batch
                    .schema()
                    .index_of("websocket_ingress_sequence")
                    .unwrap(),
            )
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let rest_ingress = batch
            .column(
                batch
                    .schema()
                    .index_of("rest_response_ingress_sequence")
                    .unwrap(),
            )
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let native_payload = batch
            .column(
                batch
                    .schema()
                    .index_of("source_native_payload_json")
                    .unwrap(),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row_index in 0..batch.num_rows() {
            let kind = record_kind.value(row_index).to_owned();
            record_kinds.push(kind.clone());
            admission_sequences.push(admission_sequence.value(row_index));
            receive_times.push(timestamp.value(row_index));
            websocket_ingress_sequences.push(if websocket_ingress.is_null(row_index) {
                None
            } else {
                Some(websocket_ingress.value(row_index))
            });
            rest_ingress_sequences.push(if rest_ingress.is_null(row_index) {
                None
            } else {
                Some(rest_ingress.value(row_index))
            });
            if kind == "invalid_input" {
                let envelope =
                    serde_json::from_str::<serde_json::Value>(native_payload.value(row_index))
                        .unwrap();
                decoded_invalid_payloads.push(
                    base64::engine::general_purpose::STANDARD
                        .decode(envelope["payload_base64"].as_str().unwrap())
                        .unwrap(),
                );
            }
        }
    }

    assert_eq!(
        record_kinds,
        ["trade", "invalid_input", "invalid_input", "trade"]
    );
    assert_eq!(admission_sequences, [1, 2, 3, 4]);
    assert_eq!(receive_times, [timestamp_received; 4]);
    assert_eq!(
        websocket_ingress_sequences,
        [Some(10), Some(11), Some(12), Some(13)]
    );
    assert_eq!(rest_ingress_sequences, [None, None, None, None]);
    assert_eq!(
        decoded_invalid_payloads,
        [
            first_invalid_payload.to_vec(),
            second_invalid_payload.to_vec()
        ]
    );
}

#[test]
fn typed_writer_binance_aggtrade_rejects_non_increasing_admission_across_batches() {
    let tmp = tempdir().unwrap();
    let timestamp_received = 1_766_789_470_100;
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade-order-reject",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    writer
        .write_binance_aggtrade_rows(&[websocket_aggtrade_row(123, timestamp_received, 10, 2)])
        .unwrap();

    let error = writer
        .write_binance_aggtrade_rows(&[websocket_aggtrade_row(124, timestamp_received, 11, 2)])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("collector admission sequence is not strictly increasing"),
        "{error}"
    );
    assert_eq!(writer.finalize().unwrap().row_count, 1);
}

#[test]
fn typed_writer_binance_aggtrade_rejects_zero_connection_generation() {
    let tmp = tempdir().unwrap();
    let mut row = websocket_aggtrade_row(123, 1_766_789_470_100, 10, 1);
    row.connection_generation = 0;
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade-generation-reject",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();

    let error = writer.write_binance_aggtrade_rows(&[row]).unwrap_err();
    assert!(error.to_string().contains("connection generation"));
}

#[test]
fn typed_writer_binance_aggtrade_rejects_legacy_rest_compatibility_values() {
    let tmp = tempdir().unwrap();
    let mut row = websocket_aggtrade_row(123, 1_766_789_470_100, 10, 1);
    row.rest_backfill_timestamp_received = Some(1_766_789_470_100);
    row.rest_response_ingress_sequence = Some(1);
    row.rest_response_item_index = Some(0);
    row.rest_was_best_price_match = Some(true);
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade-rest-reject",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();

    let error = writer.write_binance_aggtrade_rows(&[row]).unwrap_err();
    assert!(
        error.to_string().contains("websocket_has_rest_only_fields"),
        "{error}"
    );
}

#[test]
fn parquet_handoff_aggtrade_same_evidence_round_trips() {
    let tmp = tempdir().unwrap();
    let rows = [
        websocket_aggtrade_row(123, 1_766_789_470_100, 10, 1),
        invalid_websocket_aggtrade_row(b"{broken", "invalid_json", 1_766_789_470_101, 11, 2),
    ];
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "binance_aggtrade-parquet-handoff",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    writer.write_binance_aggtrade_rows(&rows).unwrap();
    let published = writer.finalize().unwrap();
    let batch = read_single_finalized_batch(&published.final_path);
    assert_eq!(batch.num_rows(), rows.len());

    let shared_columns = [
        "record_kind",
        "source_sequence_epoch_id",
        "connection_generation",
        "websocket_ingress_sequence",
        "collector_admission_sequence",
        "collector_admission_time_utc_ms",
        "timestamp_received",
        "collector_host_id",
        "collector_region",
        "source_native_payload_json",
        "invalid_reason_code",
    ];
    for (row_index, row) in rows.iter().enumerate() {
        let source_timestamp = row.trade_time.map(|timestamp| timestamp.to_string());
        let frame = HandoffFrame::source_evidence(
            10_000 + u64::try_from(row_index).unwrap(),
            "binance_aggtrade",
            if row.record_kind == "trade" {
                "aggTrade"
            } else {
                "invalid_input"
            },
            "BTCUSDT",
            source_timestamp.as_deref(),
            row.timestamp_received,
            row.collector_admission_time_utc_ms + 1,
            &row.collector_host_id,
            &row.collector_region,
            Some(&row.source_native_payload_json),
            HandoffSourceEvidence::try_from(row).unwrap(),
        )
        .unwrap();
        let frame = handoff_json_round_trip(&frame);
        let evidence = frame.source_evidence.as_ref().unwrap();

        assert_eq!(
            parquet_projection(&batch, row_index, &shared_columns),
            serde_json::json!({
                "record_kind": evidence.record_kind.as_str(),
                "source_sequence_epoch_id": evidence.source_sequence_epoch_id.as_str(),
                "connection_generation": evidence.connection_generation,
                "websocket_ingress_sequence": evidence.websocket_ingress_sequence,
                "collector_admission_sequence": evidence.collector_admission_sequence,
                "collector_admission_time_utc_ms": evidence.collector_admission_time_utc_ms,
                "timestamp_received": frame.collector_recv_time_utc_ms,
                "collector_host_id": frame.collector_host_id.as_str(),
                "collector_region": frame.collector_region.as_str(),
                "source_native_payload_json": frame.raw_json.as_deref(),
                "invalid_reason_code": evidence.invalid_reason_code.as_deref(),
            })
        );
        assert_ne!(frame.event_seq, evidence.collector_admission_sequence);
    }
}

#[test]
fn parquet_handoff_depth_same_evidence_round_trips() {
    let snapshot_tmp = tempdir().unwrap();
    let snapshot_row = depth_snapshot_row(2, 1);
    let mut snapshot_writer = TypedParquetSegmentWriter::create_for_typed_source(
        snapshot_tmp.path(),
        "binance_depth_snapshot-parquet-handoff",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        "binance_depth_snapshot",
    )
    .unwrap();
    snapshot_writer
        .write_binance_depth_snapshot_rows(std::slice::from_ref(&snapshot_row))
        .unwrap();
    let snapshot_published = snapshot_writer.finalize().unwrap();

    let update_tmp = tempdir().unwrap();
    let update_row = depth_update_row(1, 2);
    let mut update_writer = TypedParquetSegmentWriter::create_for_typed_source(
        update_tmp.path(),
        "binance_depth_update-parquet-handoff",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
        "binance_depth_update",
    )
    .unwrap();
    update_writer
        .write_binance_depth_update_rows(std::slice::from_ref(&update_row))
        .unwrap();
    let update_published = update_writer.finalize().unwrap();

    let finalized_source_kind = |path: &Path| {
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|item| item.key == "parquet_raw_source_kind")
            .and_then(|item| item.value.clone())
            .unwrap()
    };

    let snapshot_batch = read_single_finalized_batch(&snapshot_published.final_path);
    let snapshot_raw_json = serde_json::json!({
        "lastUpdateId": snapshot_row.last_update_id,
        "bids": serde_json::from_str::<serde_json::Value>(&snapshot_row.bids).unwrap(),
        "asks": serde_json::from_str::<serde_json::Value>(&snapshot_row.asks).unwrap(),
    })
    .to_string();
    let snapshot_frame = handoff_json_round_trip(
        &HandoffFrame::depth_source_evidence(
            20_000,
            "binance_depth_snapshot",
            "depth_snapshot",
            &snapshot_row.symbol,
            None,
            snapshot_row.timestamp_received,
            snapshot_row.collector_admission_time_utc_ms.unwrap() + 1,
            &snapshot_row.collector_host_id,
            &snapshot_row.collector_region,
            Some(&snapshot_raw_json),
            DepthHandoffSourceEvidence::try_from(&snapshot_row).unwrap(),
        )
        .unwrap(),
    );
    let snapshot_evidence = snapshot_frame.depth_source_evidence.as_ref().unwrap();
    assert_eq!(
        finalized_source_kind(&snapshot_published.final_path),
        snapshot_frame.source_kind
    );
    assert_eq!(
        snapshot_evidence.record_kind,
        DepthHandoffRecordKind::Snapshot
    );
    assert_eq!(
        parquet_projection(
            &snapshot_batch,
            0,
            &[
                "source_sequence_epoch_id",
                "connection_generation",
                "depth_ingress_sequence",
                "collector_admission_sequence",
                "collector_admission_time_utc_ms",
                "timestamp_received",
            ],
        ),
        serde_json::json!({
            "source_sequence_epoch_id": snapshot_evidence.source_sequence_epoch_id.as_str(),
            "connection_generation": snapshot_evidence.connection_generation,
            "depth_ingress_sequence": snapshot_evidence.depth_ingress_sequence,
            "collector_admission_sequence": snapshot_evidence.collector_admission_sequence,
            "collector_admission_time_utc_ms":
                snapshot_evidence.collector_admission_time_utc_ms,
            "timestamp_received": snapshot_frame.collector_recv_time_utc_ms,
        })
    );
    assert!(snapshot_evidence.stitch_verdict.is_none());
    assert_ne!(
        snapshot_frame.event_seq,
        snapshot_evidence.collector_admission_sequence
    );

    let update_batch = read_single_finalized_batch(&update_published.final_path);
    let update_source_timestamp = update_row.event_time.to_string();
    let update_raw_json = serde_json::json!({
        "e": update_row.event_type.as_str(),
        "E": update_row.event_time,
        "s": update_row.symbol.as_str(),
        "U": update_row.first_update_id,
        "u": update_row.final_update_id,
        "b": serde_json::from_str::<serde_json::Value>(&update_row.bids).unwrap(),
        "a": serde_json::from_str::<serde_json::Value>(&update_row.asks).unwrap(),
    })
    .to_string();
    let update_frame = handoff_json_round_trip(
        &HandoffFrame::depth_source_evidence(
            20_001,
            "binance_depth_update",
            "depth_update",
            &update_row.symbol,
            Some(&update_source_timestamp),
            update_row.timestamp_received,
            update_row.collector_admission_time_utc_ms.unwrap() + 1,
            &update_row.collector_host_id,
            &update_row.collector_region,
            Some(&update_raw_json),
            DepthHandoffSourceEvidence::try_from(&update_row).unwrap(),
        )
        .unwrap(),
    );
    let update_evidence = update_frame.depth_source_evidence.as_ref().unwrap();
    assert_eq!(
        finalized_source_kind(&update_published.final_path),
        update_frame.source_kind
    );
    assert_eq!(update_evidence.record_kind, DepthHandoffRecordKind::Update);
    assert_eq!(
        parquet_projection(
            &update_batch,
            0,
            &[
                "source_sequence_epoch_id",
                "connection_generation",
                "depth_ingress_sequence",
                "collector_admission_sequence",
                "collector_admission_time_utc_ms",
                "timestamp_received",
                "stitch_verdict",
            ],
        ),
        serde_json::json!({
            "source_sequence_epoch_id": update_evidence.source_sequence_epoch_id.as_str(),
            "connection_generation": update_evidence.connection_generation,
            "depth_ingress_sequence": update_evidence.depth_ingress_sequence,
            "collector_admission_sequence": update_evidence.collector_admission_sequence,
            "collector_admission_time_utc_ms":
                update_evidence.collector_admission_time_utc_ms,
            "timestamp_received": update_frame.collector_recv_time_utc_ms,
            "stitch_verdict": update_evidence.stitch_verdict.as_deref(),
        })
    );
    assert_ne!(
        update_frame.event_seq,
        update_evidence.collector_admission_sequence
    );
    assert!(update_evidence.depth_ingress_sequence < snapshot_evidence.depth_ingress_sequence);
    assert!(
        snapshot_evidence.collector_admission_sequence
            < update_evidence.collector_admission_sequence
    );
}
