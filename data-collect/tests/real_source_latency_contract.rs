use data_collect::latency::QueueDurationTracker;

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
