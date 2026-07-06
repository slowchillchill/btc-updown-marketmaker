use data_collect::ValidationVerdict;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::health::{CoverageVerdict, FreshnessStatus};
use data_collect::sources::rtds::{
    BTC_USD_SYMBOL, RTDS_CHAINLINK_TOPIC, build_chainlink_btc_usd_subscription_trace,
    build_chainlink_raw_event, validate_reconnect_business_freshness,
};

fn envelope(raw_json: &str) -> EventEnvelope {
    EventEnvelope {
        source_kind: SourceKind::PolymarketRtdsChainlinkBtcUsd,
        source_event_type: RTDS_CHAINLINK_TOPIC.to_owned(),
        source_identifier: BTC_USD_SYMBOL.to_owned(),
        source_timestamp: SourceTimestamp::Timestamp {
            value: "1766789469800".to_owned(),
        },
        recv_time_utc_ms: Some(1_766_789_469_999),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(raw_json.to_owned()),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: SourceKind::PolymarketRtdsChainlinkBtcUsd,
    }
}

#[test]
fn rtds_chainlink_contract() {
    let trace = build_chainlink_btc_usd_subscription_trace().expect("sdk subscription trace");
    assert_eq!(trace.action, "subscribe");
    assert_eq!(trace.topic, RTDS_CHAINLINK_TOPIC);
    assert_eq!(trace.msg_type, "*");
    assert_eq!(trace.filters, r#"{"symbol":"btc/usd"}"#);
    assert!(trace.raw_json.contains("\"subscriptions\""));
    trace.validate_chainlink_btc_usd().expect("valid trace");

    let raw_event = r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":1766789469900,"payload":{"symbol":"btc/usd","timestamp":1766789469800,"value":"100000"}}"#;
    let event = build_chainlink_raw_event(
        envelope(raw_event),
        1_766_789_469_900,
        1_766_789_469_800,
        BTC_USD_SYMBOL,
        "100000",
        &trace,
        Some("first business message after subscribe".to_owned()),
    )
    .expect("build raw event");

    assert_eq!(
        event.validate_contract().verdict,
        ValidationVerdict::Complete
    );
    assert_eq!(event.source_topic, RTDS_CHAINLINK_TOPIC);
    assert_eq!(event.symbol, BTC_USD_SYMBOL);
}

#[test]
fn rtds_reconnect_revalidates_topic_symbol_payload_timestamp() {
    let trace = build_chainlink_btc_usd_subscription_trace().expect("sdk subscription trace");

    let healthy = validate_reconnect_business_freshness(
        &trace,
        Some(1_766_789_469_800),
        Some(1_766_789_469_950),
        1_766_789_470_000,
        1_000,
    );
    assert!(healthy.topic_revalidated);
    assert!(healthy.symbol_revalidated);
    assert!(healthy.payload_timestamp_present);
    assert_eq!(healthy.freshness_status, FreshnessStatus::Healthy);
    assert_eq!(healthy.coverage_verdict, CoverageVerdict::Complete);

    let stale = validate_reconnect_business_freshness(
        &trace,
        Some(1_766_789_469_800),
        Some(1_766_789_468_000),
        1_766_789_470_000,
        1_000,
    );
    assert_eq!(stale.freshness_status, FreshnessStatus::Stale);
    assert_eq!(stale.coverage_verdict, CoverageVerdict::Stale);
    assert_eq!(
        stale.source_gap.as_deref(),
        Some("rtds_business_freshness_not_revalidated")
    );
}
