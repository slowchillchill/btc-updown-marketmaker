use std::collections::BTreeMap;
use std::fs::File;

use arrow_array::{Array as _, StringArray, TimestampMillisecondArray, UInt64Array};
use base64::Engine as _;
use data_collect::RawEvent;
use data_collect::binance_aggtrade::{
    BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
    BinanceAggTradeOutput, BinanceWsAggTrade,
};
use data_collect::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataContext, PolymarketMarketMetadataRow,
};
use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow,
};
use data_collect::source_hourly_layout::{
    BINANCE_AGGTRADE_CAPTURE_COMPLETE, BINANCE_AGGTRADE_SCHEMA_VERSION, BinanceAggTradeHourlyRow,
    BinanceAggTradeInvalidEvidence, BinanceAggTradeOrdering,
};
use data_collect::writer::{
    ParquetSegmentWriter, TypedParquetSegmentWriter, write_segment_on_blocking_pool_for_source,
};
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
fn typed_writer_writes_polymarket_market_metadata_rows() {
    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata",
        "polymarket_market_metadata_v2",
        "polymarket_market_metadata",
    )
    .unwrap();
    let row = PolymarketMarketMetadataRow::active_scan(
        "btc-updown-5m-1783392300",
        "condition-1",
        Some("clob-market-1"),
        &["asset-1".to_owned(), "asset-2".to_owned()],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(true),
        Some(true),
        "active",
        None,
        None,
        None,
        None,
        Some(true),
        PolymarketMarketMetadataContext {
            timestamp_received_ms: 1_783_392_404_599,
            metadata_fetch_start_utc_ms: Some(1_783_392_404_100),
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
            schema_version: "polymarket_market_metadata_v2".to_owned(),
        },
    )
    .with_metadata_unavailable_reason(Some(
        "gamma_discovery_snapshot_lacks_tick_size_min_size_fee_schedule_reward_eligibility"
            .to_owned(),
    ));

    writer
        .write_polymarket_market_metadata_rows(&[row])
        .unwrap();
    let published = writer.finalize().unwrap();

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&published.final_path).unwrap())
            .unwrap();
    let mut batch_reader = builder.build().unwrap();
    let batch = batch_reader.next().unwrap().unwrap();
    let reasons = batch
        .column(
            batch
                .schema()
                .index_of("metadata_unavailable_reason")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        reasons.value(0),
        "gamma_discovery_snapshot_lacks_tick_size_min_size_fee_schedule_reward_eligibility"
    );
}

#[test]
fn typed_writer_constructors_publish_source_specific_schema_metadata() {
    let cases = [
        (
            "polymarket_market_metadata",
            "polymarket_market_metadata_v2",
        ),
        ("binance_depth_snapshot", "binance_depth_snapshot_v2"),
        ("binance_depth_update", "binance_depth_update_v3"),
        ("binance_aggtrade", BINANCE_AGGTRADE_SCHEMA_VERSION),
        (
            "polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_btc_usd_v4",
        ),
    ];

    for (source_kind, schema_version) in cases {
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
    }
}

#[test]
fn typed_writer_source_specific_schemas_omit_duplicate_time_columns() {
    let cases = [
        (
            "binance_depth_snapshot",
            vec!["recv_time_utc", "snapshot_recv_time_utc"],
        ),
        ("binance_depth_update", vec!["recv_time_utc"]),
        ("binance_aggtrade", vec!["recv_time_utc"]),
        ("polymarket_rtds_chainlink_btc_usd", vec!["recv_time_utc"]),
    ];

    for (source_kind, removed_fields) in cases {
        let tmp = tempdir().unwrap();
        let writer = TypedParquetSegmentWriter::create_for_typed_source(
            tmp.path(),
            source_kind,
            "schema-under-test",
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
