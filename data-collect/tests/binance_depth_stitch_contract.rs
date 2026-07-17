use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::{BinanceDepthLevel, BinanceDepthUpdateRaw};
use data_collect::source_hourly_layout::BinanceDepthStitchReceipt;
use data_collect::sources::binance::{DepthUpdateVerdict, assess_depth_update};

fn update(first_update_id: u64, final_update_id: u64) -> BinanceDepthUpdateRaw {
    BinanceDepthUpdateRaw {
        envelope: EventEnvelope {
            source_kind: SourceKind::BinanceDepthUpdate,
            source_event_type: "depthUpdate".to_owned(),
            source_identifier: "BTCUSDT".to_owned(),
            source_timestamp: SourceTimestamp::Timestamp {
                value: "1766789469958".to_owned(),
            },
            recv_time_utc_ms: Some(1_766_789_469_999),
            collector_host_id: "collector-host".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
            host_clock_sync_status: HostClockSyncStatus::Synchronized {
                source: "chrony".to_owned(),
            },
            raw_evidence: RawEvidence::Json("{}".to_owned()),
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: SourceKind::BinanceDepthUpdate,
        },
        event_type: "depthUpdate".to_owned(),
        event_time_ms: 1_766_789_469_958,
        symbol: "BTCUSDT".to_owned(),
        stream_name: "btcusdt@depth".to_owned(),
        first_update_id,
        final_update_id,
        bids: vec![BinanceDepthLevel {
            price: "100.00".to_owned(),
            quantity: "1.0".to_owned(),
        }],
        asks: Vec::new(),
        combined_stream: None,
    }
}

#[test]
fn binance_depth_stitch_receipt_counts_stale_apply_gap_and_local_u_range() {
    let assessments = [
        assess_depth_update(Some(100), None, &update(99, 100)).unwrap(),
        assess_depth_update(Some(100), None, &update(101, 103)).unwrap(),
        assess_depth_update(Some(100), Some(103), &update(104, 105)).unwrap(),
        assess_depth_update(Some(100), Some(105), &update(107, 108)).unwrap(),
    ];
    let receipt = BinanceDepthStitchReceipt::from_assessments(100, &assessments).unwrap();

    assert_eq!(receipt.snapshot_last_update_id, 100);
    assert_eq!(receipt.stale_update_rows, 1);
    assert_eq!(receipt.stitch_applied_rows, 2);
    assert_eq!(receipt.gap_rows, 1);
    assert_eq!(receipt.resnapshot_count, 1);
    assert_eq!(receipt.local_u_min, Some(103));
    assert_eq!(receipt.local_u_max, Some(105));
    assert!(receipt.first_update_bridged_snapshot);
    assert_eq!(receipt.gap_reasons, vec!["U > local_u + 1"]);
    assert!(!receipt.validate().is_complete());
}

#[test]
fn binance_depth_stitch_receipt_is_complete_without_gap_and_with_initial_bridge() {
    let assessments = [
        assess_depth_update(Some(100), None, &update(99, 100)).unwrap(),
        assess_depth_update(Some(100), None, &update(101, 103)).unwrap(),
        assess_depth_update(Some(100), Some(103), &update(104, 104)).unwrap(),
    ];
    let receipt = BinanceDepthStitchReceipt::from_assessments(100, &assessments).unwrap();

    assert!(receipt.validate().is_complete());
}

#[test]
fn depth_assessment_preserves_all_four_frozen_verdicts_without_reclassifying_holes() {
    let applied = assess_depth_update(Some(100), None, &update(101, 103)).unwrap();
    assert_eq!(applied.verdict, DepthUpdateVerdict::Applied);
    assert_eq!(applied.local_u_after, Some(103));
    assert_eq!(
        applied.stitch_evidence.as_deref(),
        Some("U <= lastUpdateId + 1 <= u")
    );
    assert_eq!(applied.gap_reason, None);

    let stale = assess_depth_update(Some(103), None, &update(101, 103)).unwrap();
    assert_eq!(stale.verdict, DepthUpdateVerdict::NotAppliedStaleDuplicate);
    assert_eq!(stale.local_u_after, None);
    assert_eq!(stale.gap_reason, None);

    let hole = assess_depth_update(Some(100), Some(103), &update(105, 106)).unwrap();
    assert_eq!(hole.verdict, DepthUpdateVerdict::NotAppliedConfirmedHole);
    assert_eq!(hole.local_u_after, None);
    assert_eq!(hole.gap_reason.as_deref(), Some("U > local_u + 1"));

    let unavailable = assess_depth_update(None, None, &update(101, 103)).unwrap();
    assert_eq!(
        unavailable.verdict,
        DepthUpdateVerdict::NotAppliedSnapshotUnavailable
    );
    assert_eq!(unavailable.local_u_after, None);
    assert_eq!(unavailable.gap_reason, None);
}

#[test]
fn depth_assessment_rejects_invalid_state_instead_of_saturating_sequence_math() {
    assert!(assess_depth_update(Some(0), None, &update(1, 1)).is_err());
    assert!(assess_depth_update(Some(100), Some(99), &update(101, 103)).is_err());
    assert!(assess_depth_update(None, Some(100), &update(101, 103)).is_err());
    assert_eq!(
        assess_depth_update(Some(u64::MAX), None, &update(u64::MAX, u64::MAX)).unwrap_err(),
        "depth_update_sequence_overflow"
    );
    assert_eq!(
        assess_depth_update(Some(100), Some(u64::MAX), &update(u64::MAX, u64::MAX)).unwrap_err(),
        "depth_update_sequence_overflow"
    );
}
