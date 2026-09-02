use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::{
    BinanceAggTradeRaw, BinanceDepthLevel, BinanceDepthSnapshotRaw, BinanceDepthUpdateRaw,
    RtdsChainlinkBtcUsdRaw,
};

fn envelope(source_kind: SourceKind, event_type: &str, raw_json: &str) -> EventEnvelope {
    EventEnvelope {
        source_kind,
        source_event_type: event_type.to_owned(),
        source_identifier: "source-id".to_owned(),
        source_timestamp: SourceTimestamp::Timestamp {
            value: "1766789469958".to_owned(),
        },
        recv_time_utc_ms: Some(1_766_789_469_999),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "source-endpoint".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(raw_json.to_owned()),
    }
}

#[test]
fn binance_depth_update_and_snapshot_schema_keep_official_fields() {
    let update = BinanceDepthUpdateRaw {
        envelope: envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":10,"u":12,"b":[],"a":[]}"#,
        ),
        event_type: "depthUpdate".to_owned(),
        event_time_ms: 1,
        symbol: "BTCUSDT".to_owned(),
        stream_name: "btcusdt@depth".to_owned(),
        first_update_id: 10,
        final_update_id: 12,
        bids: vec![BinanceDepthLevel {
            price: "100".to_owned(),
            quantity: "1".to_owned(),
        }],
        asks: vec![BinanceDepthLevel {
            price: "101".to_owned(),
            quantity: "2".to_owned(),
        }],
        combined_stream: None,
    };

    assert!(update.validate_contract().is_complete());

    let mut one_sided_update = update.clone();
    one_sided_update.asks.clear();
    assert!(one_sided_update.validate_contract().is_complete());

    let mut empty_update = update.clone();
    empty_update.bids.clear();
    empty_update.asks.clear();
    let report = empty_update.validate_contract();
    assert!(!report.is_complete());
    assert!(report.missing_fields.contains(&"b_or_a".to_owned()));

    let mut missing_stream = update.clone();
    missing_stream.stream_name.clear();
    assert!(!missing_stream.validate_contract().is_complete());

    let mut combined_wrapper = update;
    combined_wrapper.combined_stream = Some("btcusdt@depth".to_owned());
    assert!(combined_wrapper.validate_contract().is_complete());

    let mut blank_combined_wrapper = combined_wrapper;
    blank_combined_wrapper.combined_stream = Some("  ".to_owned());
    assert!(!blank_combined_wrapper.validate_contract().is_complete());

    let snapshot = BinanceDepthSnapshotRaw {
        envelope: EventEnvelope {
            source_timestamp: SourceTimestamp::NoSourceTimestamp {
                reason: "rest_depth_snapshot_has_no_exchange_timestamp".to_owned(),
                snapshot_boundary: "snapshot_fetch_start_utc_ms=1 snapshot_recv_time_utc_ms=2"
                    .to_owned(),
            },
            ..envelope(
                SourceKind::BinanceDepthSnapshot,
                "depth_snapshot",
                r#"{"lastUpdateId":12,"bids":[],"asks":[]}"#,
            )
        },
        last_update_id: 12,
        bids: vec![BinanceDepthLevel {
            price: "100".to_owned(),
            quantity: "1".to_owned(),
        }],
        asks: vec![BinanceDepthLevel {
            price: "101".to_owned(),
            quantity: "2".to_owned(),
        }],
        snapshot_fetch_start_utc_ms: 1,
        snapshot_recv_time_utc_ms: 2,
        official_rest_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT".to_owned(),
        no_exchange_timestamp_evidence: "REST snapshot has no exchange timestamp".to_owned(),
    };

    assert!(snapshot.validate_contract().is_complete());
}

#[test]
fn binance_aggtrade_schema_keeps_full_trade_fields() {
    let trade = BinanceAggTradeRaw {
        envelope: envelope(
            SourceKind::BinanceAggTrade,
            "aggTrade",
            r#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":2,"p":"100","q":"0.1","f":3,"l":4,"T":5,"m":true,"M":false}"#,
        ),
        event_type: "aggTrade".to_owned(),
        event_time_ms: 1,
        symbol: "BTCUSDT".to_owned(),
        aggregate_trade_id: 2,
        price: "100".to_owned(),
        quantity: "0.1".to_owned(),
        first_trade_id: 3,
        last_trade_id: 4,
        trade_time_ms: 5,
        buyer_is_maker: true,
        ws_ignore: false,
        stream_name: "btcusdt@aggTrade".to_owned(),
    };

    assert!(trade.validate_contract().is_complete());
}

#[test]
fn rtds_chainlink_schema_keeps_subscription_and_timestamps() {
    let rtds = RtdsChainlinkBtcUsdRaw {
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

    assert!(rtds.validate_contract().is_complete());

    let mut wrong_filter = rtds;
    wrong_filter.subscription_raw_json =
        r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"eth/usd\"}"}"#
            .to_owned();
    let report = wrong_filter.validate_contract();
    assert!(!report.is_complete());
    assert!(
        report
            .source_gaps
            .contains(&"rtds_subscription_payload_mismatch".to_owned())
    );
}
