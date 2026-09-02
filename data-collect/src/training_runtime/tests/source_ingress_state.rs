use super::*;

#[test]
fn source_queue_recoverable_wait_is_recorded_without_source_gap() {
    let stats = SourceQueueBackpressureStats::default();
    stats.record_wait();

    let mut tracker = QueueDurationTracker::new("source_event_queue");
    stats.apply_to_tracker(&mut tracker);
    let summary = tracker.finish(0);

    assert_eq!(summary.backpressure_wait_count, 1);
    assert_eq!(summary.backpressure_timeout_count, 0);
    assert_eq!(summary.dropped_rows, 0);
    assert!(!is_training_runtime_source_gap_reason(
        "bounded_queue_backpressure_wait:binance_depth_update"
    ));
}
