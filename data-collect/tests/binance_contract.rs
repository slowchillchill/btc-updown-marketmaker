use data_collect::ValidationVerdict;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::health::{
    ArtifactSignal, CircuitState, CoverageVerdict, FreshnessStatus, NetworkFailure,
    SourceHealthEvent,
};
use data_collect::sources::binance::{
    BINANCE_DEPTH_SNAPSHOT_ENDPOINT, BinanceOfficialSourceConfig, DepthUpdateDecision,
    apply_depth_stitch_evidence, parse_aggtrade_raw, parse_depth_snapshot_raw,
    parse_depth_update_raw, stitch_depth_update,
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
fn binance_depth_stitch_contract() {
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
    .expect("parse official depth update");
    assert_eq!(
        update.validate_contract().verdict,
        ValidationVerdict::Incomplete
    );
    assert_eq!(update.combined_stream.as_deref(), Some("btcusdt@depth"));

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
        1,
        2,
        BINANCE_DEPTH_SNAPSHOT_ENDPOINT,
    )
    .expect("parse official snapshot");
    assert_eq!(
        snapshot.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let stitch_decision = stitch_depth_update(snapshot.last_update_id, None, &update);
    assert_eq!(
        stitch_decision,
        DepthUpdateDecision::Apply {
            local_u: 103,
            stitch_evidence: "U <= lastUpdateId + 1 <= u".to_owned()
        }
    );
    let stitched_update =
        apply_depth_stitch_evidence(update.clone(), stitch_decision).expect("apply stitch");
    assert_eq!(
        stitched_update.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let one_sided_update = parse_depth_update_raw(
        envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":104,"u":105,"b":[["100","1"]],"a":[]}"#,
        ),
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":104,"u":105,"b":[["100","1"]],"a":[]}"#,
    )
    .expect("parse one-sided depth update");
    let one_sided_decision =
        stitch_depth_update(snapshot.last_update_id, Some(103), &one_sided_update);
    let one_sided_stitched = apply_depth_stitch_evidence(one_sided_update, one_sided_decision)
        .expect("apply one-sided stitch");
    assert_eq!(
        one_sided_stitched.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let stale = parse_depth_update_raw(
        envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":90,"u":100,"b":[["100","1"]],"a":[["101","1"]]}"#,
        ),
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":90,"u":100,"b":[["100","1"]],"a":[["101","1"]]}"#,
    )
    .expect("parse stale");
    assert_eq!(
        stitch_depth_update(snapshot.last_update_id, None, &stale),
        DepthUpdateDecision::DropStaleOrDuplicate {
            reason: "u <= lastUpdateId".to_owned()
        }
    );

    let gap = parse_depth_update_raw(
        envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":120,"u":121,"b":[["100","1"]],"a":[["101","1"]]}"#,
        ),
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":120,"u":121,"b":[["100","1"]],"a":[["101","1"]]}"#,
    )
    .expect("parse gap");
    assert_eq!(
        stitch_depth_update(snapshot.last_update_id, Some(103), &gap),
        DepthUpdateDecision::SourceGapResnapshot {
            reason: "U > local_u + 1".to_owned()
        }
    );

    let config = BinanceOfficialSourceConfig::default();
    assert!(config.validate_official().is_ok());
    let mut alternate_official_config = config.clone();
    alternate_official_config.depth_ws_endpoint =
        "wss://data-stream.binance.vision/ws/btcusdt@depth".to_owned();
    alternate_official_config.aggtrade_ws_endpoint =
        "wss://data-stream.binance.vision/ws/btcusdt@aggTrade".to_owned();
    alternate_official_config.depth_snapshot_endpoint =
        "https://api-gcp.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000".to_owned();
    assert!(alternate_official_config.validate_official().is_ok());

    let mut proxy_config = config;
    proxy_config.depth_ws_endpoint = "wss://binance.com.evil.invalid/ws/btcusdt@depth".to_owned();
    assert!(proxy_config.validate_official().is_err());

    let wrong_symbol = parse_depth_update_raw(
        envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"ETHUSDT","U":101,"u":103,"b":[["100","1"]],"a":[["101","1"]]}"#,
        ),
        r#"{"e":"depthUpdate","E":1,"s":"ETHUSDT","U":101,"u":103,"b":[["100","1"]],"a":[["101","1"]]}"#,
    );
    assert!(wrong_symbol.is_err());

    let wrong_stream = parse_depth_update_raw(
        envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"stream":"ethusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":101,"u":103,"b":[["100","1"]],"a":[["101","1"]]}}"#,
        ),
        r#"{"stream":"ethusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":101,"u":103,"b":[["100","1"]],"a":[["101","1"]]}}"#,
    );
    assert!(wrong_stream.is_err());

    let empty_snapshot = parse_depth_snapshot_raw(
        envelope(
            SourceKind::BinanceDepthSnapshot,
            "depth_snapshot",
            r#"{"lastUpdateId":100,"bids":[],"asks":[]}"#,
        ),
        r#"{"lastUpdateId":100,"bids":[],"asks":[]}"#,
        1,
        2,
        BINANCE_DEPTH_SNAPSHOT_ENDPOINT,
    )
    .expect("parse empty snapshot");
    assert_eq!(
        empty_snapshot.validate_contract().verdict,
        ValidationVerdict::Incomplete
    );
}

#[test]
fn binance_aggtrade_raw_fields_contract() {
    let raw_trade = r#"{
        "e":"aggTrade","E":1766789469958,"s":"BTCUSDT","a":123,
        "p":"100.50","q":"0.1","f":10,"l":12,"T":1766789469000,"m":true,"M":false
    }"#;
    let trade = parse_aggtrade_raw(
        envelope(SourceKind::BinanceAggTrade, "aggTrade", raw_trade),
        raw_trade,
    )
    .expect("parse aggTrade");

    assert_eq!(
        trade.validate_contract().verdict,
        ValidationVerdict::Complete
    );
    assert_eq!(trade.event_type, "aggTrade");
    assert_eq!(trade.aggregate_trade_id, 123);
    assert_eq!(trade.first_trade_id, 10);
    assert_eq!(trade.last_trade_id, 12);
    assert!(trade.buyer_is_maker);

    let wrong_symbol = parse_aggtrade_raw(
        envelope(
            SourceKind::BinanceAggTrade,
            "aggTrade",
            r#"{"e":"aggTrade","E":1,"s":"ETHUSDT","a":123,"p":"100","q":"0.1","f":10,"l":12,"T":1,"m":true,"M":false}"#,
        ),
        r#"{"e":"aggTrade","E":1,"s":"ETHUSDT","a":123,"p":"100","q":"0.1","f":10,"l":12,"T":1,"m":true,"M":false}"#,
    );
    assert!(wrong_symbol.is_err());
}

#[test]
fn binance_source_gap_and_reconnect_failures_are_classified() {
    for (failure, signal, coverage) in [
        (
            NetworkFailure::SnapshotMissing,
            ArtifactSignal::SourceGap,
            CoverageVerdict::Gap,
        ),
        (
            NetworkFailure::RateLimit,
            ArtifactSignal::ReconnectTrace,
            CoverageVerdict::Incomplete,
        ),
        (
            NetworkFailure::ServerShutdown,
            ArtifactSignal::ReconnectTrace,
            CoverageVerdict::Incomplete,
        ),
        (
            NetworkFailure::NoBusinessMessages,
            ArtifactSignal::SourceHealth,
            CoverageVerdict::Stale,
        ),
    ] {
        let event = SourceHealthEvent::from_failure(
            "binance_depth_update",
            failure,
            Some(1),
            Some(250),
            CircuitState::Degraded,
        );
        assert_eq!(event.signal, signal);
        assert_eq!(event.coverage_verdict, coverage);
        if failure == NetworkFailure::NoBusinessMessages {
            assert_eq!(event.freshness_verdict, FreshnessStatus::Stale);
        }
        assert!(!event.process_fatal);
    }
}
