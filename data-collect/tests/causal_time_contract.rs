use data_collect::segment::{
    EventAvailability, PaperLiveAvailabilityPolicy, TrainingAvailabilityPolicy,
};
use tempfile::tempdir;

#[test]
fn training_partition_and_availability_use_collector_recv_time() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let recv_time_utc_ms = 1_767_225_600_001; // 2026-01-01T00:00:00.001Z
    let source_timestamp_ms = 1_767_225_599_000; // previous UTC day

    let availability = TrainingAvailabilityPolicy::event_availability(
        &dataset_root,
        "polymarket_market",
        recv_time_utc_ms,
        Some(source_timestamp_ms),
    )
    .unwrap();

    assert_eq!(availability.utc_day, "2026-01-01");
    assert!(
        availability
            .partition_path
            .ends_with("training/typed_raw/utc_day=2026-01-01/source_kind=polymarket_market")
    );
    assert_eq!(
        availability.event_available_time_ms,
        EventAvailability::CollectorRecvTime(recv_time_utc_ms)
    );
    assert_eq!(availability.source_timestamp_ms, Some(source_timestamp_ms));
}

#[test]
fn causal_cutoff_uses_availability_time_not_source_time() {
    let late_recv = EventAvailability::CollectorRecvTime(1_000);

    assert!(!TrainingAvailabilityPolicy::visible_at_causal_cutoff(
        late_recv, 999
    ));
    assert!(TrainingAvailabilityPolicy::visible_at_causal_cutoff(
        late_recv, 1_000
    ));
}

#[test]
fn paper_live_available_time_is_consumer_runtime_receive_time() {
    let policy = PaperLiveAvailabilityPolicy::new("paper");

    assert_eq!(
        policy.event_available_time_semantics(),
        "paper_runtime_receive_time"
    );
    assert!(policy.requires_consumer_runtime_receive_timestamp());
    assert_ne!(
        policy.event_available_time_semantics(),
        "training_collector_recv_time"
    );
}
