use data_collect::source_hourly_layout::BinanceDepthStitchReceipt;
use data_collect::sources::binance::DepthUpdateDecision;

#[test]
fn binance_depth_stitch_receipt_counts_stale_apply_gap_and_local_u_range() {
    let receipt = BinanceDepthStitchReceipt::from_decisions(
        100,
        &[
            DepthUpdateDecision::DropStaleOrDuplicate {
                reason: "u <= lastUpdateId".to_owned(),
            },
            DepthUpdateDecision::Apply {
                local_u: 103,
                stitch_evidence: "U <= lastUpdateId + 1 <= u".to_owned(),
            },
            DepthUpdateDecision::Apply {
                local_u: 105,
                stitch_evidence: "continuous_depth_update".to_owned(),
            },
            DepthUpdateDecision::SourceGapResnapshot {
                reason: "U > local_u + 1".to_owned(),
            },
        ],
    );

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
    let receipt = BinanceDepthStitchReceipt::from_decisions(
        100,
        &[
            DepthUpdateDecision::DropStaleOrDuplicate {
                reason: "u <= lastUpdateId".to_owned(),
            },
            DepthUpdateDecision::Apply {
                local_u: 103,
                stitch_evidence: "U <= lastUpdateId + 1 <= u".to_owned(),
            },
            DepthUpdateDecision::Apply {
                local_u: 104,
                stitch_evidence: "continuous_depth_update".to_owned(),
            },
        ],
    );

    assert!(receipt.validate().is_complete());
}
