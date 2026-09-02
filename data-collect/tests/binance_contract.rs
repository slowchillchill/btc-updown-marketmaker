use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::sources::binance::{
    BINANCE_DEPTH_SNAPSHOT_ENDPOINT, DepthUpdateVerdict, assess_depth_update, parse_aggtrade_raw,
    parse_depth_snapshot_raw, parse_depth_update_raw,
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
    }
}

#[test]
fn depth_update_verdict_uses_one_frozen_bidirectional_mapping() {
    for verdict in [
        DepthUpdateVerdict::Applied,
        DepthUpdateVerdict::NotAppliedStaleDuplicate,
        DepthUpdateVerdict::NotAppliedConfirmedHole,
        DepthUpdateVerdict::NotAppliedSnapshotUnavailable,
    ] {
        assert_eq!(DepthUpdateVerdict::try_from(verdict.as_str()), Ok(verdict));
    }
    assert_eq!(
        DepthUpdateVerdict::try_from("unknown").unwrap_err(),
        "unsupported frozen Binance depth verdict: unknown"
    );
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
    assert!(update.validate_contract().is_complete());
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
    assert!(snapshot.validate_contract().is_complete());

    let assessment = assess_depth_update(Some(snapshot.last_update_id), None, &update).unwrap();
    assert_eq!(assessment.verdict, DepthUpdateVerdict::Applied);
    assert_eq!(assessment.local_u_after, Some(103));
    assert_eq!(
        assessment.stitch_evidence.as_deref(),
        Some("U <= lastUpdateId + 1 <= u")
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
    assert_eq!(
        assess_depth_update(Some(snapshot.last_update_id), Some(103), &one_sided_update)
            .unwrap()
            .verdict,
        DepthUpdateVerdict::Applied
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
        assess_depth_update(Some(snapshot.last_update_id), None, &stale)
            .unwrap()
            .verdict,
        DepthUpdateVerdict::NotAppliedStaleDuplicate
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
        assess_depth_update(Some(snapshot.last_update_id), Some(103), &gap)
            .unwrap()
            .verdict,
        DepthUpdateVerdict::NotAppliedConfirmedHole
    );

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
    assert!(!empty_snapshot.validate_contract().is_complete());
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

    assert!(trade.validate_contract().is_complete());
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
