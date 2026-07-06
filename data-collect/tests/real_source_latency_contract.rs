use data_collect::{
    RawEvent,
    latency::{
        DurableWriteBatch, FlushPolicy, FlushReason, LatencyFanout, QueueDurationTracker,
        TimedRawEvent,
    },
    writer::ParquetSegmentWriter,
};
use tempfile::tempdir;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_000 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","asset_id":"asset-{seq}"}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

#[test]
fn fanout_assigns_event_seq_and_keeps_hot_path_independent_from_parquet() {
    let timed = TimedRawEvent::new(synthetic_event(1), 10);
    let mut fanout = LatencyFanout::new(41);
    let sequenced = fanout.assign_sequence(timed, 1_011, 20);

    let hot_receipt = sequenced.hot_path_receipt(1_012, 1_013);
    assert_eq!(sequenced.event_seq, 41);
    assert_eq!(hot_receipt.event_seq, 41);
    assert_eq!(hot_receipt.source_recv_to_collector_dequeue_ms, 10);
    assert_eq!(hot_receipt.source_recv_to_hot_delivery_ms, 12);
    assert_eq!(hot_receipt.hot_queue_wait_ms, 1);

    let receipt_json = serde_json::to_value(&hot_receipt).unwrap();
    for forbidden in [
        "final_path",
        "manifest_path",
        "sha256",
        "noe",
        "ledger",
        "order_evidence",
        "execution_feedback",
    ] {
        assert!(
            receipt_json.get(forbidden).is_none(),
            "hot path receipt must not contain paper/live or parquet artifact field {forbidden}"
        );
    }
}

#[test]
fn durable_batch_receipt_maps_each_event_to_write_done_latency() {
    let mut fanout = LatencyFanout::new(7);
    let first = fanout.assign_sequence(TimedRawEvent::new(synthetic_event(1), 10), 1_011, 20);
    let second = fanout.assign_sequence(TimedRawEvent::new(synthetic_event(2), 12), 1_013, 22);

    let batch = DurableWriteBatch::new(
        3,
        vec![first, second],
        FlushReason::Time,
        1_020,
        1_021,
        1_022,
        1_028,
        6,
    );
    let receipt = batch.receipt();

    assert_eq!(receipt.batch_seq, 3);
    assert_eq!(receipt.event_seqs, vec![7, 8]);
    assert_eq!(receipt.first_event_seq, 7);
    assert_eq!(receipt.last_event_seq, 8);
    assert_eq!(receipt.rows, 2);
    assert_eq!(receipt.flush_reason, FlushReason::Time);
    assert_eq!(receipt.writer_channel_wait_ms, 1);
    assert_eq!(receipt.batch_wait_ms, 19);
    assert_eq!(receipt.write_events_us, 6);
    assert_eq!(receipt.source_recv_to_write_done_ms, vec![27, 26]);
}

#[test]
fn max_flush_interval_triggers_time_flush_before_rows_threshold() {
    let policy = FlushPolicy {
        flush_rows: 5_000,
        max_flush_interval_ms: Some(1_000),
    };

    assert_eq!(policy.reason(10, Some(0), 999), None);
    assert_eq!(policy.reason(10, Some(0), 1_000), Some(FlushReason::Time));
    assert_eq!(
        policy.reason(5_000, Some(999), 1_000),
        Some(FlushReason::Rows)
    );
    assert_eq!(policy.reason(10, None, 2_000), None);
}

#[test]
fn queue_duration_summary_distinguishes_burst_from_sustained_backlog() {
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    tracker.observe(0, 0, 0);
    tracker.observe(5, 61, 6_100);
    tracker.observe(15, 61, 6_100);
    tracker.record_backpressure_wait();
    tracker.observe(25, 0, 0);
    tracker.record_backpressure_timeout();
    tracker.record_drop(2);

    let summary = tracker.finish(30);
    assert_eq!(summary.queue_name, "writer_chunk_queue");
    assert_eq!(summary.rows_high, 61);
    assert_eq!(summary.bytes_high, 6_100);
    assert_eq!(summary.non_empty_ms, 20);
    assert_eq!(summary.over_10_rows_ms, 20);
    assert_eq!(summary.longest_contiguous_over_10_rows_ms, 20);
    assert_eq!(summary.backpressure_wait_count, 1);
    assert_eq!(summary.backpressure_timeout_count, 1);
    assert_eq!(summary.dropped_rows, 2);
    assert!(!summary.sustained_congestion_observed(3, 5_000));
}

#[test]
fn writer_finalize_timing_reports_close_sha256_and_atomic_rename() {
    let tmp = tempdir().unwrap();
    let mut writer =
        ParquetSegmentWriter::create(tmp.path(), "latency-probe", "parquet_raw_layout_v1").unwrap();

    writer
        .write_events(&[synthetic_event(1), synthetic_event(2)])
        .unwrap();
    let published = writer.finalize_with_timing().unwrap();

    assert!(published.segment.final_path.exists());
    assert_eq!(published.segment.row_count, 2);
    assert_eq!(
        published.timing.atomic_rename_us,
        published.timing.running_to_digesting_rename_us
            + published.timing.digesting_to_final_rename_us
    );
    assert!(
        published.timing.finalize_total_us
            >= published.timing.finalize_close_us
                + published.timing.sha256_us
                + published.timing.atomic_rename_us
    );
}
