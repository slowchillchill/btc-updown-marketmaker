use std::collections::BTreeMap;
use std::fs::File;

use arrow_array::{StringArray, TimestampMillisecondArray};
use data_collect::RawEvent;
use data_collect::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataContext, PolymarketMarketMetadataRow,
};
use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow,
};
use data_collect::source_hourly_layout::BinanceAggTradeHourlyRow;
use data_collect::writer::{
    ParquetSegmentWriter, TypedParquetSegmentWriter, write_segment_on_blocking_pool_for_source,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::tempdir;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
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
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            schema_version: "polymarket_orderbook_v1".to_owned(),
        },
    )
    .expect("valid book row");

    let mut writer = TypedParquetSegmentWriter::create_polymarket_orderbook(
        tmp.path(),
        "polymarket-market-typed",
        "polymarket_orderbook_v1",
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
        Some("polymarket_orderbook_v1")
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
        "polymarket_market_metadata_v1",
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
            metadata_recv_time_utc_ms: Some(1_783_392_404_599),
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
            schema_version: "polymarket_market_metadata_v1".to_owned(),
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
            "polymarket_market_metadata_v1",
        ),
        ("binance_depth_snapshot", "binance_depth_snapshot_v2"),
        ("binance_depth_update", "binance_depth_update_v2"),
        ("binance_aggtrade", "binance_aggtrade_v3"),
        (
            "polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_btc_usd_v2",
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
        "binance_aggtrade_v3",
        "binance_aggtrade",
    )
    .unwrap();
    writer
        .write_binance_aggtrade_rows(&[BinanceAggTradeHourlyRow {
            timestamp_received: 1_766_789_470_100,
            event_type: "aggTrade".to_owned(),
            event_time: 1_766_789_469_958,
            trade_time: 1_766_789_469_000,
            symbol: "BTCUSDT".to_owned(),
            aggregate_trade_id: 123,
            price: "100.50".to_owned(),
            quantity: "0.1".to_owned(),
            first_trade_id: 10,
            last_trade_id: 12,
            buyer_is_maker: true,
            stream: "btcusdt@aggTrade".to_owned(),
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".to_owned(),
            schema_version: "binance_aggtrade_v3".to_owned(),
        }])
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

    assert_eq!(event_time.value(0), 1_766_789_469_958);
    assert_eq!(trade_time.value(0), 1_766_789_469_000);
}
