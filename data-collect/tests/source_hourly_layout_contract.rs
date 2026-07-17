use arrow_schema::DataType;
use base64::Engine as _;
use data_collect::binance_aggtrade::{
    BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, BinanceAggTradeIngressCoordinate,
    BinanceAggTradeOutput, BinanceWsAggTrade,
};
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::RtdsChainlinkBtcUsdRaw;
use data_collect::source_hourly_layout::{
    BINANCE_AGGTRADE_CAPTURE_COMPLETE, BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED,
    BINANCE_AGGTRADE_RECORD_INVALID_INPUT, BINANCE_AGGTRADE_RECORD_TRADE,
    BINANCE_AGGTRADE_SCHEMA_VERSION, BinanceAggTradeHourlyRow, BinanceAggTradeInvalidEvidence,
    BinanceAggTradeOrdering, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RtdsChainlinkBtcUsdHourlyRow, binance_aggtrade_schema, binance_depth_snapshot_schema,
    binance_depth_update_schema, reject_opaque_raw_json_only, rtds_chainlink_btc_usd_schema,
};
use data_collect::sources::binance::{
    BINANCE_DEPTH_SNAPSHOT_ENDPOINT, DepthUpdateDecision, apply_depth_stitch_evidence,
    parse_aggtrade_raw, parse_depth_snapshot_raw, parse_depth_update_raw, stitch_depth_update,
};
use sha2::{Digest as _, Sha256};

fn envelope(source_kind: SourceKind, event_type: &str, raw_json: &str) -> EventEnvelope {
    let source_timestamp = if source_kind == SourceKind::BinanceDepthSnapshot {
        SourceTimestamp::NoSourceTimestamp {
            reason: "official REST depth snapshot has no exchange timestamp".to_owned(),
            snapshot_boundary: "snapshot_fetch_start_utc_ms=1 snapshot_recv_time_utc_ms=2"
                .to_owned(),
        }
    } else {
        SourceTimestamp::Timestamp {
            value: "1766789469958".to_owned(),
        }
    };
    EventEnvelope {
        source_kind,
        source_event_type: event_type.to_owned(),
        source_identifier: "BTCUSDT".to_owned(),
        source_timestamp,
        recv_time_utc_ms: Some(1_766_789_469_999),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(raw_json.to_owned()),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind,
    }
}

fn aggtrade_ordering(
    connection_generation: u64,
    websocket_ingress_sequence: u64,
    collector_admission_sequence: u64,
    collector_admission_time_utc_ms: i64,
) -> BinanceAggTradeOrdering {
    BinanceAggTradeOrdering {
        source_sequence_epoch_id: "source-task-epoch-1".to_owned(),
        connection_generation,
        websocket_ingress_sequence,
        collector_admission_sequence,
        collector_admission_time_utc_ms,
    }
}

fn complete_invalid_evidence(
    timestamp_received: i64,
    endpoint: &str,
) -> BinanceAggTradeInvalidEvidence {
    let payload = b"{bad";
    let payload_length = u64::try_from(payload.len()).unwrap();
    let payload_sha256 = format!("{:x}", Sha256::digest(payload));
    BinanceAggTradeInvalidEvidence {
        timestamp_received,
        ingest_source: "websocket".to_owned(),
        source_native_payload_json: serde_json::json!({
            "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
            "transport": "websocket",
            "endpoint": endpoint,
            "receive_time_utc_ms": timestamp_received,
            "invalid_reason_code": "invalid_json",
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
        source_endpoint: endpoint.to_owned(),
        invalid_reason_code: "invalid_json".to_owned(),
        invalid_reason_detail: Some("expected value at byte 1".to_owned()),
        payload_encoding: Some("base64".to_owned()),
        payload_length_bytes: Some(payload_length),
        payload_sha256: Some(payload_sha256),
        payload_capture_status: BINANCE_AGGTRADE_CAPTURE_COMPLETE.to_owned(),
    }
}

fn oversize_invalid_evidence(
    timestamp_received: i64,
    endpoint: &str,
) -> BinanceAggTradeInvalidEvidence {
    let observed_bytes = u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES).unwrap() + 1;
    BinanceAggTradeInvalidEvidence {
        timestamp_received,
        ingest_source: "websocket".to_owned(),
        source_native_payload_json: serde_json::json!({
            "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
            "transport": "websocket",
            "endpoint": endpoint,
            "receive_time_utc_ms": timestamp_received,
            "invalid_reason_code": "body_too_large",
            "payload_encoding": null,
            "payload_length_bytes": null,
            "payload_sha256": null,
            "payload_capture_status": BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED,
            "payload_base64": null,
            "observed_bytes_at_least": observed_bytes,
            "declared_content_length": null,
            "capture_limit_bytes": BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES,
        })
        .to_string(),
        source_endpoint: endpoint.to_owned(),
        invalid_reason_code: "body_too_large".to_owned(),
        invalid_reason_detail: Some("capture limit exceeded".to_owned()),
        payload_encoding: None,
        payload_length_bytes: None,
        payload_sha256: None,
        payload_capture_status: BINANCE_AGGTRADE_CAPTURE_OVERSIZE_NOT_CAPTURED.to_owned(),
    }
}

#[test]
fn binance_depth_snapshot_hourly_row_keeps_snapshot_boundary_and_full_depth() {
    let raw_snapshot = r#"{
        "lastUpdateId":100,
        "bids":[["99.00","1.5"]],
        "asks":[["102.00","2.5"]]
    }"#;
    let snapshot = parse_depth_snapshot_raw(
        envelope(
            SourceKind::BinanceDepthSnapshot,
            "depth_snapshot",
            raw_snapshot,
        ),
        raw_snapshot,
        1_766_789_469_100,
        1_766_789_469_200,
        BINANCE_DEPTH_SNAPSHOT_ENDPOINT,
    )
    .expect("parse official snapshot");

    let row = BinanceDepthSnapshotHourlyRow::from_raw(&snapshot);
    assert_eq!(row.last_update_id, 100);
    assert_eq!(row.bids, r#"[["99.00","1.5"]]"#);
    assert_eq!(row.asks, r#"[["102.00","2.5"]]"#);
    assert_eq!(row.snapshot_fetch_start_utc, 1_766_789_469_100);
    assert!(row.validate().is_complete());
}

#[test]
fn binance_depth_update_hourly_row_keeps_official_fields_and_stitch_evidence() {
    let raw_update = r#"{
        "stream":"btcusdt@depth",
        "data":{
            "e":"depthUpdate","E":1766789469958,"s":"BTCUSDT",
            "U":101,"u":103,
            "b":[["100.00","1.0"]],
            "a":[["101.00","2.0"]]
        }
    }"#;
    let update = parse_depth_update_raw(
        envelope(SourceKind::BinanceDepthUpdate, "depthUpdate", raw_update),
        raw_update,
    )
    .expect("parse depth update");
    let decision = stitch_depth_update(100, None, &update);
    assert_eq!(
        decision,
        DepthUpdateDecision::Apply {
            local_u: 103,
            stitch_evidence: "U <= lastUpdateId + 1 <= u".to_owned()
        }
    );
    let stitched = apply_depth_stitch_evidence(update, decision).expect("stitch update");
    let row = BinanceDepthUpdateHourlyRow::from_raw(&stitched, 100, None);

    assert_eq!(row.event_type, "depthUpdate");
    assert_eq!(row.event_time, 1_766_789_469_958);
    assert_eq!(row.symbol, "BTCUSDT");
    assert_eq!(row.first_update_id, 101);
    assert_eq!(row.final_update_id, 103);
    assert_eq!(row.bids, r#"[["100.00","1.0"]]"#);
    assert_eq!(row.asks, r#"[["101.00","2.0"]]"#);
    assert_eq!(row.snapshot_last_update_id, 100);
    assert_eq!(row.local_u_before, None);
    assert_eq!(row.stitch_verdict, "U <= lastUpdateId + 1 <= u");
    assert!(row.validate().is_complete());
}

#[test]
fn binance_aggtrade_hourly_row_keeps_event_and_trade_times_with_identifiers() {
    let raw_trade = r#"{
        "e":"aggTrade","E":1766789469958,"s":"BTCUSDT","a":123,
        "p":"100.50","q":"0.1","f":10,"l":12,"T":1766789469000,"m":true,"M":false
    }"#;
    let trade = parse_aggtrade_raw(
        envelope(SourceKind::BinanceAggTrade, "aggTrade", raw_trade),
        raw_trade,
    )
    .expect("parse aggTrade");
    let row =
        BinanceAggTradeHourlyRow::from_raw(&trade, aggtrade_ordering(1, 1, 1, 1_766_789_470_000))
            .unwrap();

    assert_eq!(row.record_kind, BINANCE_AGGTRADE_RECORD_TRADE);
    assert_eq!(row.event_type.as_deref(), Some("aggTrade"));
    assert_eq!(row.event_time, Some(1_766_789_469_958));
    assert_eq!(row.trade_time, Some(1_766_789_469_000));
    assert_eq!(row.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(row.aggregate_trade_id, Some(123));
    assert_eq!(row.price.as_deref(), Some("100.50"));
    assert_eq!(row.quantity.as_deref(), Some("0.1"));
    assert_eq!(row.first_trade_id, Some(10));
    assert_eq!(row.last_trade_id, Some(12));
    assert_eq!(row.buyer_is_maker, Some(true));
    assert_eq!(row.ingest_source, "websocket");
    assert_eq!(
        row.websocket_timestamp_received,
        Some(row.timestamp_received)
    );
    assert_eq!(row.rest_backfill_timestamp_received, None);
    assert_eq!(row.ws_ignore, Some(false));
    assert_eq!(row.rest_was_best_price_match, None);
    assert_eq!(row.connection_generation, 1);
    assert_eq!(row.websocket_ingress_sequence, Some(1));
    assert_eq!(row.rest_response_ingress_sequence, None);
    assert_eq!(row.rest_response_item_index, None);
    assert!(row.source_native_payload_json.contains("binance_aggtrade"));
    assert!(row.validate().is_complete());
}

#[test]
fn binance_aggtrade_output_only_admits_websocket_evidence() {
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
    let row = BinanceAggTradeHourlyRow::from_output(
        &BinanceAggTradeOutput::Websocket(websocket),
        "collector-host",
        "eu-west-1",
        "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
        aggtrade_ordering(7, 1, 1, 1_766_789_470_301),
    )
    .unwrap();
    assert_eq!(row.connection_generation, 7);
    assert_eq!(row.websocket_ingress_sequence, Some(1));
    assert_eq!(row.rest_backfill_timestamp_received, None);
    assert_eq!(row.rest_response_ingress_sequence, None);
    assert_eq!(row.rest_response_item_index, None);
    assert_eq!(row.rest_was_best_price_match, None);
    assert!(row.validate().is_complete());

    let mut invalid = row.clone();
    invalid.connection_generation = 0;
    assert!(
        invalid
            .validate()
            .missing_fields
            .contains(&"connection_generation".to_owned())
    );

    let mut invalid = row;
    invalid.rest_response_ingress_sequence = Some(2);
    assert!(
        invalid
            .validate()
            .missing_fields
            .contains(&"websocket_has_rest_only_fields".to_owned())
    );
}

#[test]
fn binance_aggtrade_invalid_input_is_mutually_exclusive_and_replayable() {
    let endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let mut rest_evidence = complete_invalid_evidence(1_766_789_470_099, endpoint);
    rest_evidence.ingest_source = "rest_backfill".to_owned();
    assert!(
        BinanceAggTradeHourlyRow::from_invalid_evidence(
            rest_evidence,
            aggtrade_ordering(2, 2, 2, 1_766_789_470_100),
            "collector-host",
            "eu-west-1",
        )
        .is_err()
    );

    let row = BinanceAggTradeHourlyRow::from_invalid_evidence(
        complete_invalid_evidence(1_766_789_470_100, endpoint),
        aggtrade_ordering(2, 3, 3, 1_766_789_470_101),
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    assert_eq!(row.record_kind, BINANCE_AGGTRADE_RECORD_INVALID_INPUT);
    assert_eq!(row.schema_version, BINANCE_AGGTRADE_SCHEMA_VERSION);
    assert_eq!(row.aggregate_trade_id, None);
    assert_eq!(row.trade_time, None);
    assert_eq!(row.symbol, None);
    assert_eq!(row.buyer_is_maker, None);
    assert_eq!(row.websocket_ingress_sequence, Some(3));
    assert_eq!(row.collector_admission_sequence, 3);
    assert!(row.validate().is_complete());

    let mut invalid = row.clone();
    invalid.aggregate_trade_id = Some(123);
    assert!(
        invalid
            .validate()
            .missing_fields
            .contains(&"invalid_input_has_trade_only_fields".to_owned())
    );

    let mut invalid = row;
    invalid.payload_sha256 = None;
    assert!(
        invalid
            .validate()
            .missing_fields
            .contains(&"payload_sha256".to_owned())
    );
}

#[test]
fn binance_aggtrade_invalid_evidence_rejects_envelope_or_payload_mismatch() {
    let endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let row = BinanceAggTradeHourlyRow::from_invalid_evidence(
        complete_invalid_evidence(1_766_789_470_100, endpoint),
        aggtrade_ordering(2, 3, 3, 1_766_789_470_101),
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    let mut wrong_schema = row.clone();
    let mut evidence: serde_json::Value =
        serde_json::from_str(&wrong_schema.source_native_payload_json).unwrap();
    evidence["evidence_schema"] = serde_json::json!("unknown");
    wrong_schema.source_native_payload_json = evidence.to_string();
    assert!(
        wrong_schema
            .validate()
            .missing_fields
            .contains(&"invalid_evidence_schema".to_owned())
    );

    let mut wrong_endpoint = row.clone();
    wrong_endpoint.source_endpoint = "wss://wrong.example/ws".to_owned();
    assert!(
        wrong_endpoint
            .validate()
            .missing_fields
            .contains(&"invalid_evidence_row_consistency".to_owned())
    );

    let mut wrong_payload = row;
    let mut evidence: serde_json::Value =
        serde_json::from_str(&wrong_payload.source_native_payload_json).unwrap();
    evidence["payload_base64"] = serde_json::json!("bm90LXRoZS1wYXlsb2Fk");
    wrong_payload.source_native_payload_json = evidence.to_string();
    assert!(
        wrong_payload
            .validate()
            .missing_fields
            .contains(&"invalid_evidence_complete_payload".to_owned())
    );
}

#[test]
fn binance_aggtrade_oversize_evidence_requires_truthful_bound_without_payload() {
    let endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let row = BinanceAggTradeHourlyRow::from_invalid_evidence(
        oversize_invalid_evidence(1_766_789_470_100, endpoint),
        aggtrade_ordering(2, 3, 3, 1_766_789_470_101),
        "collector-host",
        "eu-west-1",
    )
    .unwrap();
    assert!(row.validate().is_complete());

    let mut fabricated_payload = row.clone();
    let mut evidence: serde_json::Value =
        serde_json::from_str(&fabricated_payload.source_native_payload_json).unwrap();
    evidence["payload_base64"] = serde_json::json!("eA==");
    fabricated_payload.source_native_payload_json = evidence.to_string();
    assert!(
        fabricated_payload
            .validate()
            .missing_fields
            .contains(&"invalid_evidence_oversize_payload".to_owned())
    );

    let mut unproven_oversize = row;
    let mut evidence: serde_json::Value =
        serde_json::from_str(&unproven_oversize.source_native_payload_json).unwrap();
    evidence["observed_bytes_at_least"] =
        serde_json::json!(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES);
    unproven_oversize.source_native_payload_json = evidence.to_string();
    assert!(
        unproven_oversize
            .validate()
            .missing_fields
            .contains(&"invalid_evidence_oversize_payload".to_owned())
    );
}

#[test]
fn rtds_chainlink_hourly_row_keeps_timestamps_topic_filter_and_value() {
    let raw = RtdsChainlinkBtcUsdRaw {
        envelope: envelope(
            SourceKind::PolymarketRtdsChainlinkBtcUsd,
            "crypto_prices_chainlink",
            r#"{"topic":"crypto_prices_chainlink","symbol":"btc/usd","value":"100000"}"#,
        ),
        message_timestamp_ms: 1_766_789_469_900,
        payload_timestamp_ms: 1_766_789_469_800,
        symbol: "btc/usd".to_owned(),
        value: "100000".to_owned(),
        source_topic: "crypto_prices_chainlink".to_owned(),
        message_type: "update".to_owned(),
        age_ms: Some(199),
        freshness_status: "fresh".to_owned(),
        subscription_raw_json:
            r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}"#
                .to_owned(),
        ack_or_first_message_evidence: Some("first crypto_prices_chainlink message".to_owned()),
        close_or_error_evidence: None,
    };
    let row = RtdsChainlinkBtcUsdHourlyRow::from_raw(&raw);

    assert_eq!(row.message_timestamp, 1_766_789_469_900);
    assert_eq!(row.payload_timestamp, 1_766_789_469_800);
    assert_eq!(row.age_ms, Some(199));
    assert_eq!(row.symbol, "btc/usd");
    assert_eq!(row.value, "100000");
    assert_eq!(row.topic, "crypto_prices_chainlink");
    assert_eq!(row.message_type, "update");
    assert_eq!(row.freshness_status, "fresh");
    assert!(row.subscription_filter.contains("btc/usd"));
    assert!(
        row.source_native_payload_json
            .contains("polymarket_rtds_chainlink_btc_usd")
    );
    assert!(row.validate().is_complete());
}

#[test]
fn rtds_seconds_timestamps_remain_valid_invalid_timestamp_facts() {
    const RECEIVE_MS: i64 = 1_766_789_469_999;
    const SECONDS: i64 = 1_766_789_469;
    let raw = |message_timestamp_ms: i64, payload_timestamp_ms: i64| {
        RtdsChainlinkBtcUsdRaw {
            envelope: envelope(
                SourceKind::PolymarketRtdsChainlinkBtcUsd,
                "crypto_prices_chainlink",
                r#"{"topic":"crypto_prices_chainlink","type":"update"}"#,
            ),
            message_timestamp_ms,
            payload_timestamp_ms,
            symbol: "btc/usd".to_owned(),
            value: "100000".to_owned(),
            source_topic: "crypto_prices_chainlink".to_owned(),
            message_type: "update".to_owned(),
            age_ms: RECEIVE_MS.checked_sub(payload_timestamp_ms),
            freshness_status: "invalid_timestamp".to_owned(),
            subscription_raw_json:
                r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}"#
                    .to_owned(),
            ack_or_first_message_evidence: Some(
                "first crypto_prices_chainlink message".to_owned(),
            ),
            close_or_error_evidence: None,
        }
    };

    for invalid in [raw(SECONDS, RECEIVE_MS), raw(RECEIVE_MS, SECONDS)] {
        assert!(
            RtdsChainlinkBtcUsdHourlyRow::from_raw(&invalid)
                .validate()
                .is_complete()
        );
    }
}

#[test]
fn opaque_raw_json_only_is_rejected_for_hourly_typed_layout() {
    let err = reject_opaque_raw_json_only(
        "binance_aggtrade",
        r#"{"source_kind":"binance_aggtrade","raw_json":"{}"}"#,
    )
    .expect_err("opaque raw_json-only row must be rejected");
    assert!(err.contains("required typed fields"));
}

#[test]
fn schemas_contain_source_specific_typed_fields() {
    for (schema, fields) in [
        (
            binance_depth_snapshot_schema(),
            vec![
                "timestamp_received",
                "snapshot_fetch_start_utc",
                "last_update_id",
                "bids",
                "asks",
                "symbol",
                "source_endpoint",
            ],
        ),
        (
            binance_depth_update_schema(),
            vec![
                "timestamp_received",
                "event_time",
                "symbol",
                "first_update_id",
                "final_update_id",
                "bids",
                "asks",
                "snapshot_last_update_id",
                "local_u_before",
                "stitch_verdict",
            ],
        ),
        (
            binance_aggtrade_schema(),
            vec![
                "record_kind",
                "timestamp_received",
                "event_time",
                "trade_time",
                "symbol",
                "aggregate_trade_id",
                "price",
                "quantity",
                "first_trade_id",
                "last_trade_id",
                "buyer_is_maker",
                "source_native_payload_json",
                "source_sequence_epoch_id",
                "connection_generation",
                "websocket_ingress_sequence",
                "rest_response_ingress_sequence",
                "rest_response_item_index",
                "collector_admission_sequence",
                "collector_admission_time_utc_ms",
                "invalid_reason_code",
                "invalid_reason_detail",
                "payload_encoding",
                "payload_length_bytes",
                "payload_sha256",
                "payload_capture_status",
            ],
        ),
        (
            rtds_chainlink_btc_usd_schema(),
            vec![
                "timestamp_received",
                "message_timestamp",
                "payload_timestamp",
                "age_ms",
                "symbol",
                "value",
                "topic",
                "message_type",
                "freshness_status",
                "subscription_filter",
                "source_native_payload_json",
            ],
        ),
    ] {
        for field in fields {
            schema.field_with_name(field).expect(field);
        }
        assert!(matches!(
            schema
                .field_with_name("timestamp_received")
                .expect("timestamp_received")
                .data_type(),
            DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC"
        ));
    }
    assert!(
        binance_depth_update_schema()
            .field_with_name("local_u_after")
            .is_err()
    );
    let aggtrade = binance_aggtrade_schema();
    assert!(
        !aggtrade
            .field_with_name("connection_generation")
            .unwrap()
            .is_nullable()
    );
    for legacy_rest_field in [
        "rest_backfill_timestamp_received",
        "rest_was_best_price_match",
        "rest_response_ingress_sequence",
        "rest_response_item_index",
    ] {
        assert!(
            aggtrade
                .field_with_name(legacy_rest_field)
                .unwrap()
                .is_nullable(),
            "legacy REST compatibility field {legacy_rest_field} must remain nullable"
        );
    }
}

#[test]
fn source_hourly_schemas_do_not_duplicate_receive_time_aliases() {
    for (source_kind, schema) in [
        ("binance_depth_snapshot", binance_depth_snapshot_schema()),
        ("binance_depth_update", binance_depth_update_schema()),
        ("binance_aggtrade", binance_aggtrade_schema()),
        (
            "polymarket_rtds_chainlink_btc_usd",
            rtds_chainlink_btc_usd_schema(),
        ),
    ] {
        assert!(
            schema.field_with_name("recv_time_utc").is_err(),
            "{source_kind} must use timestamp_received as the only collector receive timestamp"
        );
    }

    assert!(
        binance_aggtrade_schema()
            .field_with_name("event_time")
            .is_ok()
    );
    assert!(
        binance_aggtrade_schema()
            .field_with_name("trade_time")
            .is_ok()
    );
    assert!(
        binance_depth_snapshot_schema()
            .field_with_name("snapshot_recv_time_utc")
            .is_err(),
        "binance_depth_snapshot must use timestamp_received as the snapshot receive timestamp"
    );
    for trade_only in [
        "trade_time",
        "symbol",
        "aggregate_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "buyer_is_maker",
    ] {
        assert!(
            binance_aggtrade_schema()
                .field_with_name(trade_only)
                .expect(trade_only)
                .is_nullable(),
            "v6 trade-only field {trade_only} must be nullable for invalid_input rows"
        );
    }
}
