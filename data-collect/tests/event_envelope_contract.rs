use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};

fn complete_envelope() -> EventEnvelope {
    EventEnvelope {
        source_kind: SourceKind::PolymarketMarket,
        source_event_type: "book".to_owned(),
        source_identifier: "condition=0xabc asset=123".to_owned(),
        source_timestamp: SourceTimestamp::Timestamp {
            value: "1766789469958".to_owned(),
        },
        recv_time_utc_ms: Some(1_766_789_469_999),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(
            r#"{"event_type":"book","timestamp":"1766789469958"}"#.to_owned(),
        ),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: SourceKind::PolymarketMarket,
    }
}

#[test]
fn event_envelope_requires_recv_time_and_source_timestamp_boundary() {
    let envelope = complete_envelope();
    assert!(envelope.validate().is_ok());
    assert_eq!(envelope.recv_time_utc_ms, Some(1_766_789_469_999));
    assert_eq!(envelope.source_timestamp.value(), Some("1766789469958"));

    let mut missing_recv = envelope.clone();
    missing_recv.recv_time_utc_ms = None;
    let err = missing_recv.validate().unwrap_err();
    assert!(err.missing_fields.contains(&"recv_time_utc".to_owned()));

    let mut missing_host = envelope.clone();
    missing_host.collector_host_id.clear();
    missing_host.collector_region.clear();
    missing_host.source_endpoint.clear();
    let err = missing_host.validate().unwrap_err();
    assert!(err.missing_fields.contains(&"collector_host_id".to_owned()));
    assert!(err.missing_fields.contains(&"collector_region".to_owned()));
    assert!(err.missing_fields.contains(&"source_endpoint".to_owned()));
}

#[test]
fn event_envelope_uses_numeric_layout_version_consistent_with_writer_schema() {
    let mut envelope = complete_envelope();
    assert_eq!(envelope.parquet_raw_layout_version, 1);
    assert!(envelope.validate().is_ok());

    envelope.parquet_raw_layout_version = 0;
    let err = envelope.validate().unwrap_err();
    assert!(
        err.missing_fields
            .contains(&"parquet_raw_layout_version".to_owned())
    );
}

#[test]
fn snapshot_without_exchange_timestamp_requires_explicit_evidence() {
    let mut snapshot = complete_envelope();
    snapshot.source_kind = SourceKind::BinanceDepthSnapshot;
    snapshot.parquet_raw_source_kind = SourceKind::BinanceDepthSnapshot;
    snapshot.source_event_type = "depth_snapshot".to_owned();
    snapshot.source_timestamp = SourceTimestamp::NoSourceTimestamp {
        reason: "binance_rest_depth_snapshot_has_no_exchange_timestamp".to_owned(),
        snapshot_boundary:
            "snapshot_fetch_start_utc_ms=1766789469000 snapshot_recv_time_utc_ms=1766789469012"
                .to_owned(),
    };

    assert!(snapshot.validate().is_ok());

    let mut invalid_snapshot = snapshot;
    invalid_snapshot.source_timestamp = SourceTimestamp::NoSourceTimestamp {
        reason: String::new(),
        snapshot_boundary: String::new(),
    };
    let err = invalid_snapshot.validate().unwrap_err();
    assert!(
        err.missing_fields
            .contains(&"source_timestamp_evidence".to_owned())
    );
}

#[test]
fn polymarket_platform_timestamp_does_not_replace_recv_time_utc() {
    let envelope = complete_envelope();

    assert_eq!(envelope.source_timestamp.value(), Some("1766789469958"));
    assert_eq!(envelope.recv_time_utc_ms, Some(1_766_789_469_999));
    assert_ne!(
        envelope.source_timestamp.value().unwrap(),
        envelope.recv_time_utc_ms.unwrap().to_string()
    );
}

#[test]
fn raw_json_must_be_parseable_or_have_equivalent_reference() {
    let mut envelope = complete_envelope();
    envelope.raw_evidence = RawEvidence::Json("not-json".to_owned());
    let err = envelope.validate().unwrap_err();
    assert!(
        err.missing_fields
            .contains(&"parseable_raw_json".to_owned())
    );

    envelope.raw_evidence = RawEvidence::ExternalReference {
        uri: "s3://example/raw/segment.jsonl".to_owned(),
        sha256: "0123456789abcdef".to_owned(),
    };
    assert!(envelope.validate().is_ok());
}
