use arrow_schema::DataType;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::RtdsChainlinkBtcUsdRaw;
use data_collect::source_hourly_layout::{
    BinanceAggTradeHourlyRow, BinanceDepthSnapshotHourlyRow, BinanceDepthUpdateHourlyRow,
    RtdsChainlinkBtcUsdHourlyRow, binance_aggtrade_schema, binance_depth_snapshot_schema,
    binance_depth_update_schema, reject_opaque_raw_json_only, rtds_chainlink_btc_usd_schema,
};
use data_collect::sources::binance::{
    BINANCE_DEPTH_SNAPSHOT_ENDPOINT, DepthUpdateDecision, apply_depth_stitch_evidence,
    parse_aggtrade_raw, parse_depth_snapshot_raw, parse_depth_update_raw, stitch_depth_update,
};

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
    assert_eq!(row.snapshot_recv_time_utc, 1_766_789_469_200);
    assert_eq!(row.recv_time_utc, 1_766_789_469_999);
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
    assert_eq!(row.local_u_after, Some(103));
    assert_eq!(row.stitch_verdict, "U <= lastUpdateId + 1 <= u");
    assert!(row.validate().is_complete());
}

#[test]
fn binance_aggtrade_hourly_row_keeps_trade_fields() {
    let raw_trade = r#"{
        "e":"aggTrade","E":1766789469958,"s":"BTCUSDT","a":123,
        "p":"100.50","q":"0.1","f":10,"l":12,"T":1766789469000,"m":true
    }"#;
    let trade = parse_aggtrade_raw(
        envelope(SourceKind::BinanceAggTrade, "aggTrade", raw_trade),
        raw_trade,
    )
    .expect("parse aggTrade");
    let row = BinanceAggTradeHourlyRow::from_raw(&trade);

    assert_eq!(row.event_type, "aggTrade");
    assert_eq!(row.event_time, 1_766_789_469_958);
    assert_eq!(row.symbol, "BTCUSDT");
    assert_eq!(row.aggregate_trade_id, 123);
    assert_eq!(row.price, "100.50");
    assert_eq!(row.quantity, "0.1");
    assert_eq!(row.first_trade_id, 10);
    assert_eq!(row.last_trade_id, 12);
    assert_eq!(row.trade_time, 1_766_789_469_000);
    assert!(row.buyer_is_maker);
    assert!(row.validate().is_complete());
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
        subscription_raw_json:
            r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}"#
                .to_owned(),
        ack_or_first_message_evidence: Some("first crypto_prices_chainlink message".to_owned()),
        close_or_error_evidence: None,
    };
    let row = RtdsChainlinkBtcUsdHourlyRow::from_raw(&raw);

    assert_eq!(row.message_timestamp, 1_766_789_469_900);
    assert_eq!(row.payload_timestamp, 1_766_789_469_800);
    assert_eq!(row.symbol, "btc/usd");
    assert_eq!(row.value, "100000");
    assert_eq!(row.topic, "crypto_prices_chainlink");
    assert!(row.subscription_filter.contains("btc/usd"));
    assert_eq!(row.recv_time_utc, 1_766_789_469_999);
    assert!(row.validate().is_complete());
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
                "snapshot_recv_time_utc",
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
                "local_u_after",
                "stitch_verdict",
            ],
        ),
        (
            binance_aggtrade_schema(),
            vec![
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
            ],
        ),
        (
            rtds_chainlink_btc_usd_schema(),
            vec![
                "timestamp_received",
                "message_timestamp",
                "payload_timestamp",
                "symbol",
                "value",
                "topic",
                "subscription_filter",
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
}
