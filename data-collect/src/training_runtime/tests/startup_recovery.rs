use super::*;
use crate::daily_manifest::close_day_manifest;
use crate::source_hourly_layout::binance_aggtrade_schema;

fn write_real_legacy_v5_aggtrade_file(
    path: &Path,
    id: u64,
    timestamp_received: i64,
    source_native_payload_json: &str,
) {
    use arrow_array::{BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;

    let timestamp = |name: &str, nullable| {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            nullable,
        )
    };
    let schema = Arc::new(Schema::new(vec![
        timestamp("timestamp_received", false),
        timestamp("websocket_timestamp_received", true),
        timestamp("rest_backfill_timestamp_received", true),
        Field::new("ingest_source", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        timestamp("event_time", true),
        timestamp("trade_time", false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("aggregate_trade_id", DataType::UInt64, false),
        Field::new("price", DataType::Utf8, false),
        Field::new("quantity", DataType::Utf8, false),
        Field::new("first_trade_id", DataType::UInt64, false),
        Field::new("last_trade_id", DataType::UInt64, false),
        Field::new("buyer_is_maker", DataType::Boolean, false),
        Field::new("stream", DataType::Utf8, true),
        Field::new("ws_ignore", DataType::Boolean, true),
        Field::new("rest_was_best_price_match", DataType::Boolean, true),
        Field::new("source_native_payload_json", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]));
    let event_time = 1_783_392_404_599_i64;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                TimestampMillisecondArray::from(vec![timestamp_received]).with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(timestamp_received)])
                    .with_timezone("UTC"),
            ),
            Arc::new(TimestampMillisecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["websocket"])),
            Arc::new(StringArray::from(vec![Some("aggTrade")])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(event_time)]).with_timezone("UTC")),
            Arc::new(TimestampMillisecondArray::from(vec![event_time]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["BTCUSDT"])),
            Arc::new(UInt64Array::from(vec![id])),
            Arc::new(StringArray::from(vec!["65000.10"])),
            Arc::new(StringArray::from(vec!["0.001"])),
            Arc::new(UInt64Array::from(vec![id * 10])),
            Arc::new(UInt64Array::from(vec![id * 10])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![Some("btcusdt@aggTrade")])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
            Arc::new(BooleanArray::from(vec![None])),
            Arc::new(StringArray::from(vec![source_native_payload_json])),
            Arc::new(StringArray::from(vec!["collector-test"])),
            Arc::new(StringArray::from(vec!["local-test"])),
            Arc::new(StringArray::from(vec![BINANCE_AGGTRADE_WS_ENDPOINT])),
            Arc::new(StringArray::from(vec!["binance_aggtrade_v5"])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

const LEGACY_V6_TRADE_ID: u64 = 100;
const LEGACY_V6_TRADE_TIMESTAMP_RECEIVED: i64 = 1_783_392_404_599;
const LEGACY_V6_WEBSOCKET_PAYLOAD: &str = r#"{"payload":{"E":1783392404599,"M":false,"T":1783392404599,"a":100,"e":"aggTrade","f":1000,"l":1000,"m":true,"p":"100","q":"0.001","s":"BTCUSDT"},"source":"binance_aggtrade_websocket","stream":"btcusdt@aggTrade"}"#;

fn write_real_legacy_v6_aggtrade_file(path: &Path) {
    use arrow_array::{BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;

    let timestamp = |name: &str, nullable| {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            nullable,
        )
    };
    let legacy_schema = Arc::new(Schema::new(vec![
        Field::new("record_kind", DataType::Utf8, false),
        timestamp("timestamp_received", false),
        timestamp("websocket_timestamp_received", true),
        timestamp("rest_backfill_timestamp_received", true),
        Field::new("ingest_source", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        timestamp("event_time", true),
        timestamp("trade_time", true),
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
        timestamp("collector_admission_time_utc_ms", false),
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
    ]));
    let invalid_payload = br#"{"e":"aggTrade""#;
    let invalid_payload_length = u64::try_from(invalid_payload.len()).unwrap();
    let invalid_payload_sha256 = format!("{:x}", Sha256::digest(invalid_payload));
    let trade_sources = json!({
        "websocket": LEGACY_V6_WEBSOCKET_PAYLOAD,
        "rest_backfill": null,
    })
    .to_string();
    let invalid_evidence = json!({
        "capture_limit_bytes": 524_288,
        "declared_content_length": null,
        "endpoint": BINANCE_AGGTRADE_WS_ENDPOINT,
        "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
        "invalid_reason_code": "invalid_json",
        "observed_bytes_at_least": invalid_payload_length,
        "payload_base64": base64::engine::general_purpose::STANDARD.encode(invalid_payload),
        "payload_capture_status": "complete",
        "payload_encoding": "base64",
        "payload_length_bytes": invalid_payload_length,
        "payload_sha256": invalid_payload_sha256,
        "receive_time_utc_ms": LEGACY_V6_TRADE_TIMESTAMP_RECEIVED + 1,
        "transport": "websocket",
    })
    .to_string();
    let legacy_batch = RecordBatch::try_new(
        Arc::clone(&legacy_schema),
        vec![
            Arc::new(StringArray::from(vec!["trade", "invalid_input"])),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    LEGACY_V6_TRADE_TIMESTAMP_RECEIVED,
                    LEGACY_V6_TRADE_TIMESTAMP_RECEIVED + 1,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    Some(LEGACY_V6_TRADE_TIMESTAMP_RECEIVED),
                    Some(LEGACY_V6_TRADE_TIMESTAMP_RECEIVED + 1),
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(TimestampMillisecondArray::from(vec![None::<i64>, None]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["websocket", "websocket"])),
            Arc::new(StringArray::from(vec![Some("aggTrade"), None])),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    Some(LEGACY_V6_TRADE_TIMESTAMP_RECEIVED),
                    None,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    Some(LEGACY_V6_TRADE_TIMESTAMP_RECEIVED),
                    None,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec![Some("BTCUSDT"), None])),
            Arc::new(UInt64Array::from(vec![Some(LEGACY_V6_TRADE_ID), None])),
            Arc::new(StringArray::from(vec![Some("100"), None])),
            Arc::new(StringArray::from(vec![Some("0.001"), None])),
            Arc::new(UInt64Array::from(vec![Some(1_000), None])),
            Arc::new(UInt64Array::from(vec![Some(1_000), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringArray::from(vec![Some("btcusdt@aggTrade"), None])),
            Arc::new(BooleanArray::from(vec![Some(false), None])),
            Arc::new(BooleanArray::from(vec![None, None])),
            Arc::new(StringArray::from(vec![
                trade_sources.as_str(),
                invalid_evidence.as_str(),
            ])),
            Arc::new(StringArray::from(vec![
                "legacy-v6-fixture",
                "legacy-v6-fixture",
            ])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(UInt64Array::from(vec![
                Some(LEGACY_V6_TRADE_ID),
                Some(LEGACY_V6_TRADE_ID + 1),
            ])),
            Arc::new(UInt64Array::from(vec![None, None])),
            Arc::new(UInt64Array::from(vec![None, None])),
            Arc::new(UInt64Array::from(vec![
                LEGACY_V6_TRADE_ID,
                LEGACY_V6_TRADE_ID + 1,
            ])),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    LEGACY_V6_TRADE_TIMESTAMP_RECEIVED,
                    LEGACY_V6_TRADE_TIMESTAMP_RECEIVED + 1,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec![None, Some("invalid_json")])),
            Arc::new(StringArray::from(vec![None, Some("expected comma")])),
            Arc::new(StringArray::from(vec![None, Some("base64")])),
            Arc::new(UInt64Array::from(vec![None, Some(invalid_payload_length)])),
            Arc::new(StringArray::from(vec![
                None,
                Some(invalid_payload_sha256.as_str()),
            ])),
            Arc::new(StringArray::from(vec![None, Some("complete")])),
            Arc::new(StringArray::from(vec!["collector-test", "collector-test"])),
            Arc::new(StringArray::from(vec!["local-test", "local-test"])),
            Arc::new(StringArray::from(vec![
                BINANCE_AGGTRADE_WS_ENDPOINT,
                BINANCE_AGGTRADE_WS_ENDPOINT,
            ])),
            Arc::new(StringArray::from(vec![
                "binance_aggtrade_v6",
                "binance_aggtrade_v6",
            ])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "parquet_raw_layout_version".to_owned(),
                "binance_aggtrade_v6".to_owned(),
            ),
            KeyValue::new(
                "parquet_raw_source_kind".to_owned(),
                "binance_aggtrade".to_owned(),
            ),
        ]))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), legacy_schema, Some(properties)).unwrap();
    writer.write(&legacy_batch).unwrap();
    writer.close().unwrap();
}

#[allow(clippy::too_many_arguments)]
fn append_test_recovery_metadata(
    writer: &mut TypedParquetSegmentWriter,
    source_kind: &str,
    utc_hour: &str,
    part: u32,
    rows: u64,
    first_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    event_type: &str,
    schema_version: &str,
) {
    let hour_start = first_recv_time_utc_ms.div_euclid(3_600_000) * 3_600_000;
    let metadata = DailyFileRecoveryMetadata {
        source_kind: source_kind.to_owned(),
        utc_hour: utc_hour.to_owned(),
        part,
        rows,
        schema_version: schema_version.to_owned(),
        first_recv_time_utc_ms,
        last_recv_time_utc_ms,
        source_timestamp_start: Some(first_recv_time_utc_ms.to_string()),
        source_timestamp_end: Some(last_recv_time_utc_ms.to_string()),
        event_type_counts: BTreeMap::from([(event_type.to_owned(), rows)]),
        facts: json!({
            "rollover_reason": "test_recovery",
            "hour_window_utc_ms": {
                "start": hour_start,
                "end": hour_start + 3_600_000,
            },
            "layout_family": "source_specific_hourly_typed_raw",
            "sort_order": "receive_order",
            "compression": "zstd_level_3",
            "backfill_rows": 0,
            "book_rows_with_bids_asks": 0,
            "covered_market_slugs": [],
            "covered_condition_ids": [],
            "covered_asset_ids": [],
            "binance_depth_stitch": {},
            "source_gaps": [],
            "metadata_complete_rows": 0,
            "metadata_unavailable_rows": 0,
            "code_version": "test-code-version",
            "collector_binary_sha256": "test-binary-sha256",
            "source_config_digest": "test-source-config-digest",
            "collector_host_id": "collector-test",
            "collector_region": "local-test",
        }),
    };
    writer
        .append_key_value_metadata(
            DAILY_FILE_RECOVERY_METADATA_KEY,
            serde_json::to_string(&metadata).unwrap(),
        )
        .unwrap();
}

fn write_test_aggtrade_recovery_file(
    day_dir: &Path,
    utc_hour: &str,
    part: u32,
    first_id: u64,
    rows: u64,
    first_recv_time_utc_ms: i64,
) -> PathBuf {
    let source_dir = day_dir.join("source_kind=binance_aggtrade");
    fs::create_dir_all(&source_dir).unwrap();
    let hour = &utc_hour[11..13];
    let stem = if part == 0 {
        format!("binance_aggtrade_{hour}")
    } else {
        format!("binance_aggtrade_{hour}-part-{part}")
    };
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &source_dir,
        &stem,
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    let events = (0..rows)
        .map(|offset| {
            test_aggtrade_hourly_row(
                first_id + offset,
                first_recv_time_utc_ms + i64::try_from(offset).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    writer.write_binance_aggtrade_rows(&events).unwrap();
    append_test_recovery_metadata(
        &mut writer,
        "binance_aggtrade",
        utc_hour,
        part,
        rows,
        first_recv_time_utc_ms,
        first_recv_time_utc_ms + i64::try_from(rows - 1).unwrap(),
        "aggTrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
    );
    let final_path = writer.final_path().to_path_buf();
    writer.finalize().unwrap();
    final_path
}

fn create_closed_test_aggtrade_day(dataset_root: &Path) -> (PathBuf, PathBuf) {
    let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
    let final_path =
        write_test_aggtrade_recovery_file(&day_dir, "2026-07-13T23", 0, 100, 1, 1_783_987_100_000);
    let mut manifest =
        reconcile_open_day_manifest(&day_dir, "2026-07-13", &test_daily_manifest_identity())
            .unwrap();
    close_day_manifest(&day_dir, &mut manifest, "test_closed_day").unwrap();
    (day_dir, final_path)
}

#[test]
fn binance_aggtrade_dedupe_restores_id_and_complete_payload() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = 1_783_392_404_599;
    let payload = json!({
        "e": "aggTrade",
        "E": 1_783_392_404_599_i64,
        "s": "BTCUSDT",
        "a": 123_456_u64,
        "p": "65000.10",
        "q": "0.001",
        "f": 123_450_u64,
        "l": 123_456_u64,
        "T": 1_783_392_404_599_i64,
        "m": true,
        "M": false
    });
    let websocket = BinanceWsAggTrade::from_wire(
        &payload,
        "btcusdt@aggTrade",
        recv_time_utc_ms,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
    )
    .unwrap();
    let native_id = websocket.shared.aggregate_trade_id;
    let canonical = websocket.source_native_payload_json.clone();
    let spec = BinanceWsSpec {
        source_kind: "binance_aggtrade".to_owned(),
        default_event_type: "aggTrade".to_owned(),
        url: BINANCE_AGGTRADE_WS_ENDPOINT.to_owned(),
        frame_inactivity_timeout_secs: 60,
        recovery_wait_override: None,
    };
    let event = binance_aggtrade_raw_event(
        &BinanceAggTradeOutput::Websocket(websocket),
        &CollectorIdentity {
            host_id: "collector-a".to_owned(),
            region: "test".to_owned(),
        },
        &spec,
        "test-source-epoch",
        1,
    )
    .unwrap();
    let mut event = event;
    let raw_event = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    assert_eq!(raw_event["timestamp_received"], recv_time_utc_ms);
    assert!(
        raw_event.get("websocket_timestamp_received").is_none(),
        "current trade raw event must not carry the removed receive-time alias"
    );
    event.collector_admission = Some(RawEventAdmission {
        sequence: 1,
        time_utc_ms: recv_time_utc_ms,
    });
    let row = binance_aggtrade_row_from_event(&event).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&row.source_native_payload_json).unwrap()["websocket"],
        canonical
    );

    let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
    let source_dir = dataset_root
        .join("training")
        .join("typed_raw")
        .join(format!("utc_day={day}"))
        .join("source_kind=binance_aggtrade");
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &source_dir,
        "binance_aggtrade_02",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    writer.write_binance_aggtrade_rows(&[row]).unwrap();
    append_test_recovery_metadata(
        &mut writer,
        "binance_aggtrade",
        "2026-07-07T02",
        0,
        1,
        recv_time_utc_ms,
        recv_time_utc_ms,
        "aggTrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
    );
    writer.finalize().unwrap();

    let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
    let mut process_b =
        restore_binance_aggtrade_recovery(&dataset_root, &day, &test_daily_manifest_identity())
            .unwrap()
            .websocket_dedupe;
    assert_eq!(
        process_b.observe(native_id, canonical.as_bytes()),
        BinanceIdDecision::Duplicate
    );
    let mut changed = payload;
    changed["M"] = Value::Bool(true);
    let changed_canonical = BinanceWsAggTrade::from_wire(
        &changed,
        "btcusdt@aggTrade",
        recv_time_utc_ms + 1,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 2 },
    )
    .unwrap()
    .source_native_payload_json;
    assert_eq!(
        process_b.observe(native_id, changed_canonical.as_bytes()),
        BinanceIdDecision::Conflict,
        "a field outside the WS/REST shared subset is still part of same-source payload identity"
    );
}

#[test]
fn binance_aggtrade_v5_recovery_reads_real_legacy_schema_without_synthetic_coordinates() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-07");
    let source_dir = day_dir.join("source_kind=binance_aggtrade");
    fs::create_dir_all(&source_dir).unwrap();
    let timestamp_received = 1_783_392_404_599;
    let id = 123_456;
    let websocket = BinanceWsAggTrade::from_wire(
        &test_ws_aggtrade_value_at(id, timestamp_received),
        "btcusdt@aggTrade",
        timestamp_received,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
    )
    .unwrap();
    let canonical = websocket.source_native_payload_json.clone();
    let persisted_sources = json!({
        "websocket": canonical,
        "rest_backfill": null,
    })
    .to_string();
    let path = source_dir.join("binance_aggtrade_02.parquet");
    write_real_legacy_v5_aggtrade_file(&path, id, timestamp_received, &persisted_sources);

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    assert!(builder.schema().index_of("record_kind").is_err());
    assert_eq!(
        builder
            .schema()
            .index_of("websocket_timestamp_received")
            .unwrap(),
        1
    );
    assert!(
        builder
            .schema()
            .index_of("collector_admission_sequence")
            .is_err()
    );
    let digest_before = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));

    let mut restored_websocket = restore_binance_transport_dedupe(&dataset_root).unwrap();
    let digest_after = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));
    assert_eq!(
        digest_after, digest_before,
        "legacy v5 recovery must not rewrite the Parquet fixture"
    );
    assert_eq!(
        restored_websocket.observe(id, canonical.as_bytes()),
        BinanceIdDecision::Duplicate
    );
}

#[test]
fn binance_aggtrade_v5_rest_only_does_not_consume_websocket_recovery_window() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let source_dir = dataset_root
        .join("training/typed_raw/utc_day=2026-07-07")
        .join("source_kind=binance_aggtrade");
    fs::create_dir_all(&source_dir).unwrap();
    let timestamp_received = 1_783_392_404_599;
    let websocket_id = 100;
    let websocket = BinanceWsAggTrade::from_wire(
        &test_ws_aggtrade_value_at(websocket_id, timestamp_received),
        "btcusdt@aggTrade",
        timestamp_received,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
    )
    .unwrap();
    let websocket_payload = websocket.source_native_payload_json;
    let merged_sources = json!({
        "websocket": websocket_payload,
        "rest_backfill": json!({"a": websocket_id}).to_string(),
    })
    .to_string();
    write_real_legacy_v5_aggtrade_file(
        &source_dir.join("binance_aggtrade_01.parquet"),
        websocket_id,
        timestamp_received,
        &merged_sources,
    );
    let rest_only_sources = json!({
        "websocket": null,
        "rest_backfill": json!({"a": 200}).to_string(),
    })
    .to_string();
    write_real_legacy_v5_aggtrade_file(
        &source_dir.join("binance_aggtrade_02.parquet"),
        200,
        timestamp_received + 1,
        &rest_only_sources,
    );

    let mut restored = restore_binance_transport_dedupe_with_limit(&dataset_root, 1).unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(
        restored.observe(websocket_id, websocket_payload.as_bytes()),
        BinanceIdDecision::Duplicate,
        "a newer REST-only legacy row must not hide an older merged WS fact"
    );
}

#[test]
fn binance_aggtrade_v6_recovery_skips_invalid_input_rows() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-07");
    let source_dir = day_dir.join("source_kind=binance_aggtrade");
    fs::create_dir_all(&source_dir).unwrap();
    let path = source_dir.join("binance_aggtrade_02.parquet");
    write_real_legacy_v6_aggtrade_file(&path);
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    assert!(
        builder
            .schema()
            .index_of("websocket_timestamp_received")
            .is_ok()
    );
    let v6_without_removed_field = builder
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != "websocket_timestamp_received")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        binance_aggtrade_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        v6_without_removed_field,
        "v7 must equal the independently frozen v6 physical schema minus the removed field"
    );
    let footer_schema_version = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|metadata| {
            metadata
                .iter()
                .find(|entry| entry.key == "parquet_raw_layout_version")
        })
        .and_then(|entry| entry.value.as_deref());
    assert_eq!(footer_schema_version, Some("binance_aggtrade_v6"));
    let digest_before = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));

    let mut restored_websocket = restore_binance_transport_dedupe(&dataset_root).unwrap();
    let digest_after = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));
    assert_eq!(
        digest_after, digest_before,
        "legacy v6 recovery must not rewrite the Parquet fixture"
    );
    assert_eq!(
        restored_websocket.observe(LEGACY_V6_TRADE_ID, LEGACY_V6_WEBSOCKET_PAYLOAD.as_bytes()),
        BinanceIdDecision::Duplicate
    );
    assert_eq!(restored_websocket.len(), 1);
}

#[test]
fn binance_recovery_closes_prior_open_day_and_overlays_current_finalized_tail() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let prior_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
    let prior_source_dir = prior_day_dir.join("source_kind=binance_aggtrade");
    fs::create_dir_all(&prior_source_dir).unwrap();
    let mut prior_writer = TypedParquetSegmentWriter::create_for_typed_source(
        &prior_source_dir,
        "binance_aggtrade_23",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    prior_writer
        .write_binance_aggtrade_rows(&[
            test_aggtrade_hourly_row(100, 1_783_987_100_000),
            test_aggtrade_hourly_row(101, 1_783_987_100_001),
        ])
        .unwrap();
    append_test_recovery_metadata(
        &mut prior_writer,
        "binance_aggtrade",
        "2026-07-13T23",
        0,
        2,
        1_783_987_100_000,
        1_783_987_100_001,
        "aggTrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
    );
    prior_writer.finalize().unwrap();
    let legacy_continuity = json!({
        "latest_epoch": {
            "epoch_start_id": 90,
            "last_observed_id": 99,
            "last_contiguous_id": 99
        },
        "unresolved_ranges": [],
        "initial_coverage_boundary_id": 90
    });
    crate::daily_manifest::write_json_atomic(
        &prior_day_dir.join("manifest.json"),
        &json!({
            "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
            "utc_day": "2026-07-13",
            "state": "open",
            "build_commit": "test-code-version",
            "collector_binary_sha256": "test-binary-sha256",
            "config_digest": "test-source-config-digest",
            "collector_host_id": "collector-test",
            "collector_region": "local-test",
            "files": [],
            "binance_aggtrade_continuity": legacy_continuity,
        }),
    )
    .unwrap();

    let recovered = restore_binance_aggtrade_recovery(
        &dataset_root,
        "2026-07-14",
        &test_daily_manifest_identity(),
    )
    .unwrap();
    let prior_manifest = load_day_manifest(&prior_day_dir).unwrap().unwrap();
    assert_eq!(prior_manifest["state"], "closed");
    assert_eq!(
        prior_manifest["closure_reason"],
        "reconciled_legacy_coverage_unknown"
    );
    assert!(
        prior_manifest
            .get("gaps")
            .and_then(Value::as_array)
            .is_none_or(|gaps| gaps.is_empty()),
        "legacy unknown coverage must not be relabeled as an unclean gap"
    );
    assert_eq!(prior_manifest["files"].as_array().unwrap().len(), 1);
    assert_eq!(
        prior_manifest["files"][0]["facts"]["collector_binary_sha256"],
        "test-binary-sha256"
    );
    assert_eq!(
        prior_manifest["files"][0]["event_type_counts"]["aggTrade"],
        2
    );
    assert_eq!(
        prior_manifest["binance_aggtrade_continuity"]["latest_epoch"]["last_contiguous_id"], 99,
        "legacy continuity must be preserved rather than recomputed"
    );
    assert_eq!(recovered.websocket_dedupe.len(), 2);

    let current_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
    let current_source_dir = current_day_dir.join("source_kind=binance_aggtrade");
    fs::create_dir_all(&current_source_dir).unwrap();
    let mut current_writer = TypedParquetSegmentWriter::create_for_typed_source(
        &current_source_dir,
        "binance_aggtrade_00",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    current_writer
        .write_binance_aggtrade_rows(&[test_aggtrade_hourly_row(102, 1_783_987_200_000)])
        .unwrap();
    append_test_recovery_metadata(
        &mut current_writer,
        "binance_aggtrade",
        "2026-07-14T00",
        0,
        1,
        1_783_987_200_000,
        1_783_987_200_000,
        "aggTrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
    );
    current_writer.finalize().unwrap();

    let recovered = restore_binance_aggtrade_recovery(
        &dataset_root,
        "2026-07-14",
        &test_daily_manifest_identity(),
    )
    .unwrap();
    assert_eq!(recovered.websocket_dedupe.len(), 3);
    let canonical = BinanceWsAggTrade::from_wire(
        &test_ws_aggtrade_value_at(102, 1_783_987_200_000),
        "btcusdt@aggTrade",
        1_783_987_200_000,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 102 },
    )
    .unwrap()
    .source_native_payload_json;
    let mut websocket_dedupe = recovered.websocket_dedupe;
    assert_eq!(
        websocket_dedupe.observe(102, canonical.as_bytes()),
        BinanceIdDecision::Duplicate
    );
    assert!(!prior_day_dir.join("manifest.json.tmp").exists());
}

#[test]
fn binance_recovery_uses_closed_day_when_current_manifest_has_no_parquet() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let prior_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-13");
    let current_day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
    fs::create_dir_all(&prior_day_dir).unwrap();
    fs::create_dir_all(&current_day_dir).unwrap();
    let continuity = json!({
        "latest_epoch": {
            "epoch_start_id": 90,
            "last_observed_id": 99,
            "last_contiguous_id": 99
        },
        "unresolved_ranges": [],
        "initial_coverage_boundary_id": 90
    });
    crate::daily_manifest::write_json_atomic(
        &prior_day_dir.join("manifest.json"),
        &json!({
            "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
            "utc_day": "2026-07-13",
            "state": "closed",
            "build_commit": "test-code-version",
            "collector_binary_sha256": "test-binary-sha256",
            "config_digest": "test-source-config-digest",
            "collector_host_id": "collector-test",
            "collector_region": "local-test",
            "files": [],
            "binance_aggtrade_continuity": continuity.clone(),
        }),
    )
    .unwrap();
    crate::daily_manifest::write_json_atomic(
        &current_day_dir.join("manifest.json"),
        &json!({
            "schema_version": crate::daily_manifest::DAILY_MANIFEST_SCHEMA_VERSION,
            "utc_day": "2026-07-14",
            "state": "open",
            "build_commit": "test-code-version",
            "collector_binary_sha256": "test-binary-sha256",
            "config_digest": "test-source-config-digest",
            "collector_host_id": "collector-test",
            "collector_region": "local-test",
            "files": [],
            "binance_aggtrade_continuity": continuity.clone(),
        }),
    )
    .unwrap();

    let recovered = restore_binance_aggtrade_recovery(
        &dataset_root,
        "2026-07-14",
        &test_daily_manifest_identity(),
    )
    .unwrap();
    assert!(recovered.websocket_dedupe.is_empty());
    assert_eq!(
        load_day_manifest(&current_day_dir).unwrap().unwrap()["binance_aggtrade_continuity"],
        continuity
    );
}

#[test]
fn binance_recovery_rejects_closed_day_file_set_changes_before_source_start() {
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (_day_dir, final_path) = create_closed_test_aggtrade_day(&dataset_root);
        fs::remove_file(final_path).unwrap();

        let error = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .err()
        .expect("a closed-day missing file must stop recovery before sources start");
        assert!(error.to_string().contains("missing finalized Parquet"));
    }
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (day_dir, final_path) = create_closed_test_aggtrade_day(&dataset_root);
        fs::remove_file(final_path).unwrap();
        write_test_aggtrade_recovery_file(&day_dir, "2026-07-13T23", 0, 100, 2, 1_783_987_100_000);

        let error = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .err()
        .expect("a closed-day mutated file must stop recovery before sources start");
        assert!(error.to_string().contains("does not match disk"));
    }
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
        write_test_aggtrade_recovery_file(&day_dir, "2026-07-13T23", 1, 101, 1, 1_783_987_100_001);

        let error = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .err()
        .expect("a closed-day extra file must stop recovery before sources start");
        assert!(error.to_string().contains("unrecorded finalized Parquet"));
    }
}

#[test]
fn binance_recovery_rejects_manifest_schema_and_identity_corruption_before_source_start() {
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
        let mut manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        manifest["schema_version"] = json!("data_collect_daily_manifest_v1");
        crate::daily_manifest::write_json_atomic(&day_dir.join("manifest.json"), &manifest)
            .unwrap();

        let error = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .err()
        .expect("a wrong manifest schema must stop recovery before sources start");
        assert!(error.to_string().contains("manifest schema mismatch"));
    }
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let (day_dir, _final_path) = create_closed_test_aggtrade_day(&dataset_root);
        let mut manifest = load_day_manifest(&day_dir).unwrap().unwrap();
        manifest.as_object_mut().unwrap().remove("config_digest");
        crate::daily_manifest::write_json_atomic(&day_dir.join("manifest.json"), &manifest)
            .unwrap();

        let error = restore_binance_aggtrade_recovery(
            &dataset_root,
            "2026-07-14",
            &test_daily_manifest_identity(),
        )
        .err()
        .expect("a missing manifest identity must stop recovery before sources start");
        assert!(
            error
                .to_string()
                .contains("identity field missing: config_digest")
        );
    }
    {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let day_dir = dataset_root.join("training/typed_raw/utc_day=2026-07-14");
        write_test_aggtrade_recovery_file(&day_dir, "2026-07-14T00", 0, 100, 1, 1_783_987_200_000);
        let mut wrong_identity = test_daily_manifest_identity();
        wrong_identity.config_digest = "different-config".to_owned();

        let error = restore_binance_aggtrade_recovery(&dataset_root, "2026-07-14", &wrong_identity)
            .err()
            .expect(
                "an orphan footer with another identity must stop recovery before sources start",
            );
        assert!(
            error
                .to_string()
                .contains("recovery collection identity mismatch")
        );
    }
}

#[test]
fn startup_recovery_crossing_midnight_closes_prior_day_before_source_construction() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let mut clock = [
        1_783_987_199_000,
        1_783_987_199_500,
        1_783_987_201_000,
        1_783_987_202_000,
        1_783_987_203_000,
        1_783_987_204_000,
    ]
    .into_iter();

    let startup =
        reconcile_stable_startup_recovery(&dataset_root, &test_daily_manifest_identity(), || {
            clock.next().expect("startup recovery clock sample")
        })
        .unwrap();

    assert_eq!(startup.utc_day, "2026-07-14");
    assert_eq!(startup.initial_next_boundary_ms, 1_784_073_600_000);
    assert!(clock.next().is_none());
    let prior = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-13"))
        .unwrap()
        .unwrap();
    assert_eq!(prior["state"], "closed");
    assert_eq!(prior["closure_reason"], "reconciled_collection_not_started");
    assert_eq!(prior["collection_day_coverage"]["state"], "not_started");
    assert_eq!(
        prior["collection_day_coverage"]["reactivated_within_day"],
        false
    );
    assert!(prior["gaps"].as_array().unwrap().is_empty());
    let current = load_day_manifest(&typed_raw_root(&dataset_root).join("utc_day=2026-07-14"))
        .unwrap()
        .unwrap();
    assert_eq!(current["state"], "open");
}

#[test]
fn startup_recovery_clock_regression_fails_closed_at_every_sample_boundary() {
    let cases: [(&str, &[i64], i64, i64); 3] = [
        (
            "confirmed_same_day_regression",
            &[1_783_987_100_000, 1_783_987_099_999],
            1_783_987_100_000,
            1_783_987_099_999,
        ),
        (
            "scanner_completed_same_day_regression",
            &[1_783_987_100_000, 1_783_987_100_100, 1_783_987_100_099],
            1_783_987_100_100,
            1_783_987_100_099,
        ),
        (
            "next_iteration_start_regression_after_cross_day_continue",
            &[
                1_783_987_199_000,
                1_783_987_199_500,
                1_783_987_201_000,
                1_783_987_200_999,
            ],
            1_783_987_201_000,
            1_783_987_200_999,
        ),
    ];

    for (case, samples, expected_previous_ms, expected_current_ms) in cases {
        let tmp = tempdir().unwrap();
        let dataset_root = tmp.path().canonicalize().unwrap();
        let mut clock = samples.iter().copied();

        let error = reconcile_stable_startup_recovery(
            &dataset_root,
            &test_daily_manifest_identity(),
            || clock.next().expect("startup recovery clock sample"),
        )
        .err()
        .expect("a regressing startup clock must fail closed");
        let message = error.to_string();
        assert!(
            message.contains("UTC clock moved backward during startup recovery"),
            "{case}: {message}"
        );
        assert!(
            message.contains(&format!("previous_ms={expected_previous_ms}")),
            "{case}: {message}"
        );
        assert!(
            message.contains(&format!("current_ms={expected_current_ms}")),
            "{case}: {message}"
        );
        assert!(clock.next().is_none(), "{case}");
    }

    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let mut equal_clock = [1_783_987_100_000; 3].into_iter();
    reconcile_stable_startup_recovery(&dataset_root, &test_daily_manifest_identity(), || {
        equal_clock.next().expect("startup recovery clock sample")
    })
    .expect("equal millisecond samples must remain valid");
    assert!(equal_clock.next().is_none());
}

#[test]
fn binance_recovery_keeps_only_the_latest_fixed_id_window() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let source_dir = dataset_root
        .join("training/typed_raw/utc_day=2026-07-13")
        .join("source_kind=binance_aggtrade");
    fs::create_dir_all(&source_dir).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &source_dir,
        "binance_aggtrade_00",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
        "binance_aggtrade",
    )
    .unwrap();
    let rows = (1..=25_002)
        .map(|id| test_aggtrade_hourly_row(id, 1_783_900_800_000 + i64::try_from(id).unwrap()))
        .collect::<Vec<_>>();
    writer.write_binance_aggtrade_rows(&rows).unwrap();
    append_test_recovery_metadata(
        &mut writer,
        "binance_aggtrade",
        "2026-07-13T00",
        0,
        25_002,
        1_783_900_800_001,
        1_783_900_825_002,
        "aggTrade",
        BINANCE_AGGTRADE_SCHEMA_VERSION,
    );
    writer.finalize().unwrap();

    let mut recovered = restore_binance_aggtrade_recovery(
        &dataset_root,
        "2026-07-13",
        &test_daily_manifest_identity(),
    )
    .unwrap()
    .websocket_dedupe;
    assert_eq!(recovered.len(), BINANCE_AGGTRADE_RECOVERY_IDS);
    let canonical = |id| {
        let timestamp = 1_783_900_800_000 + i64::try_from(id).unwrap();
        BinanceWsAggTrade::from_wire(
            &test_ws_aggtrade_value_at(id, timestamp),
            "btcusdt@aggTrade",
            timestamp,
            BinanceAggTradeIngressCoordinate::Websocket { sequence: id },
        )
        .unwrap()
        .source_native_payload_json
    };
    assert_eq!(
        recovered.observe(2, canonical(2).as_bytes()),
        BinanceIdDecision::OutsideWindow
    );
    assert_eq!(
        recovered.observe(3, canonical(3).as_bytes()),
        BinanceIdDecision::Duplicate
    );
    assert_eq!(
        recovered.observe(25_002, canonical(25_002).as_bytes()),
        BinanceIdDecision::Duplicate
    );
}

#[test]
fn finalized_parquet_recovery_orders_numeric_parts() {
    let mut paths = [
        "source_00-part-10.parquet",
        "source_00-part-2.parquet",
        "source_00.parquet",
        "source_00-part-1.parquet",
    ]
    .map(PathBuf::from);
    paths.sort_by_key(|path| finalized_parquet_order_key(path));
    assert_eq!(
        paths.map(|path| path.file_name().unwrap().to_owned()),
        [
            "source_00.parquet",
            "source_00-part-1.parquet",
            "source_00-part-2.parquet",
            "source_00-part-10.parquet",
        ]
        .map(std::ffi::OsString::from)
    );
}
