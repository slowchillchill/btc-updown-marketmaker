use data_collect::segment::{
    RolloverReason, SegmentNaming, SegmentPolicy, SegmentRolloverDecision, SegmentState,
};
use tempfile::tempdir;

#[test]
fn segment_naming_uses_source_kind_recv_day_and_safe_stem() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();

    let naming = SegmentNaming::new(
        &dataset_root,
        "binance_aggtrade",
        1_767_225_600_001,
        7,
        "run-with/slash",
    )
    .unwrap();

    assert!(
        naming
            .partition_path
            .ends_with("training/typed_raw/source_kind=binance_aggtrade/utc_day=2026-01-01")
    );
    assert!(naming.running_path.ends_with(
        "segment_start_recv_ms=1767225600001--segment_seq=7--run-with-slash-running.parquet"
    ));
    assert!(
        naming.final_path.ends_with(
            "segment_start_recv_ms=1767225600001--segment_seq=7--run-with-slash.parquet"
        )
    );
    assert!(naming.manifest_path.ends_with(
        "segment_start_recv_ms=1767225600001--segment_seq=7--run-with-slash.manifest.json"
    ));
}

#[test]
fn soft_limits_roll_segment_without_process_fatal() {
    let policy = SegmentPolicy {
        max_rows: 2,
        max_bytes: 100,
        max_segment_duration_ms: 60_000,
    };
    let state = SegmentState {
        rows: 2,
        bytes: 50,
        segment_start_recv_time_utc_ms: 1_000,
        latest_recv_time_utc_ms: 2_000,
        utc_day: "1970-01-01".to_owned(),
    };

    let decision = policy.evaluate(&state, "1970-01-01");

    assert_eq!(
        decision,
        SegmentRolloverDecision {
            should_roll: true,
            reason: Some(RolloverReason::MaxRows),
            process_fatal: false,
            open_next_segment: true
        }
    );
}

#[test]
fn utc_day_boundary_rolls_old_day_then_opens_new_segment() {
    let policy = SegmentPolicy {
        max_rows: 10_000,
        max_bytes: 10_000_000,
        max_segment_duration_ms: 3_600_000,
    };
    let state = SegmentState {
        rows: 1,
        bytes: 10,
        segment_start_recv_time_utc_ms: 1_767_225_599_000,
        latest_recv_time_utc_ms: 1_767_225_600_001,
        utc_day: "2025-12-31".to_owned(),
    };

    let decision = policy.evaluate(&state, "2026-01-01");

    assert!(decision.should_roll);
    assert_eq!(decision.reason, Some(RolloverReason::UtcDayBoundary));
    assert!(decision.open_next_segment);
    assert!(!decision.process_fatal);
}

#[test]
fn shutdown_timeout_marks_incomplete_without_publishing_false_complete() {
    let decision = SegmentRolloverDecision::shutdown_finalize_timeout();

    assert!(decision.should_roll);
    assert_eq!(decision.reason, Some(RolloverReason::ShutdownTimeout));
    assert!(!decision.process_fatal);
    assert!(!decision.open_next_segment);
    assert!(decision.write_incomplete_manifest());
}
