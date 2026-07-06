use data_collect::RawEvent;
use data_collect::backpressure::{
    BackpressureConfig, BackpressureMonitor, BackpressureSeverity, QueueSample, RuntimeAction,
};
use data_collect::config::{CrossTaskQueueConfig, validate_cross_task_queue_configs};

fn sample(rows: u64, bytes: u64, age_ms: u64, lag_ms: u64) -> QueueSample {
    QueueSample {
        rows,
        bytes,
        oldest_event_age_ms: age_ms,
        writer_lag_ms: lag_ms,
    }
}

fn config() -> BackpressureConfig {
    BackpressureConfig::new(sample(5, 1_000, 50, 20), sample(10, 4_000, 500, 100), 2)
}

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: "condition:asset".to_owned(),
        source_ts: Some(seq.to_string()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: "synthetic://polymarket".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","seq":{seq}}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

#[test]
fn cross_task_queue_config_requires_bounded_rows_bytes_age_and_writer_lag() {
    let configs = vec![
        CrossTaskQueueConfig::new(
            "source_event_queue",
            1_024,
            8_388_608,
            16_777_216,
            1_000,
            250,
        ),
        CrossTaskQueueConfig::new("hot_handoff_queue", 512, 4_194_304, 8_388_608, 250, 10),
        CrossTaskQueueConfig::new(
            "writer_chunk_queue",
            128,
            16_777_216,
            33_554_432,
            2_000,
            1_000,
        ),
    ];
    validate_cross_task_queue_configs(&configs).unwrap();

    let invalid = vec![CrossTaskQueueConfig::new(
        "writer_chunk_queue",
        0,
        1,
        1,
        1,
        1,
    )];
    let err = validate_cross_task_queue_configs(&invalid).unwrap_err();
    assert!(err.to_string().contains("capacity_rows"));

    let invalid = vec![CrossTaskQueueConfig::new(
        "writer_chunk_queue",
        1,
        2,
        1,
        1,
        1,
    )];
    let err = validate_cross_task_queue_configs(&invalid).unwrap_err();
    assert!(err.to_string().contains("max_memory_watermark_bytes"));
}

#[tokio::test]
async fn bounded_queue_is_tokio_mpsc_and_never_unbounded() {
    let (tx, mut rx) = data_collect::backpressure::bounded_event_queue::<RawEvent>(1).unwrap();

    tx.try_send(synthetic_event(1)).unwrap();
    assert!(tx.try_send(synthetic_event(2)).is_err());
    assert!(rx.recv().await.is_some());
}

#[test]
fn recoverable_bounded_wait_records_observation_not_source_gap() {
    let mut monitor = BackpressureMonitor::new("binance_aggtrade", config());

    let verdict = monitor.record_bounded_wait_success(sample(6, 1_500, 75, 30), 4);
    let receipt = monitor.receipt();

    assert_eq!(verdict.severity, BackpressureSeverity::SoftDegraded);
    assert_eq!(verdict.action, RuntimeAction::Continue);
    assert!(!verdict.process_fatal);
    assert_eq!(receipt.bounded_wait_success_count, 1);
    assert_eq!(receipt.send_timeout_count, 0);
    assert_eq!(receipt.drop_count, 0);
    assert_eq!(monitor.source_gaps(), []);
}

#[test]
fn send_timeout_and_drop_record_source_gap_without_process_fatal() {
    let mut monitor = BackpressureMonitor::new("binance_depth_update", config());

    let timeout = monitor.record_send_timeout(sample(11, 4_500, 600, 120));
    let dropped = monitor.record_drop(sample(12, 4_600, 700, 130));

    assert_eq!(timeout.severity, BackpressureSeverity::HardDegraded);
    assert_eq!(dropped.severity, BackpressureSeverity::HardDegraded);
    assert!(!timeout.process_fatal);
    assert_eq!(monitor.source_gaps().len(), 2);
    assert_eq!(monitor.source_gaps()[0].reason, "bounded_send_timeout");
    assert_eq!(monitor.source_gaps()[1].reason, "dropped_event");
}

#[test]
fn backpressure_hysteresis_prevents_flapping_stop_start() {
    let mut monitor = BackpressureMonitor::new("polymarket_market", config());

    let hard_once = monitor.observe(sample(11, 4_500, 600, 120));
    assert_eq!(hard_once.action, RuntimeAction::Continue);

    let recovered_before_pause = monitor.observe(sample(1, 100, 1, 1));
    assert_eq!(
        recovered_before_pause.severity,
        BackpressureSeverity::Healthy
    );

    let hard_again = monitor.observe(sample(11, 4_500, 600, 120));
    let hard_twice = monitor.observe(sample(12, 4_600, 700, 130));
    assert_eq!(hard_again.action, RuntimeAction::Continue);
    assert_eq!(hard_twice.action, RuntimeAction::PauseIntake);

    let receipt_before_recovery = monitor.receipt();
    assert_eq!(receipt_before_recovery.pause_intake_count, 1);
    assert_eq!(receipt_before_recovery.longest_contiguous_hard_samples, 2);

    let recovered = monitor.observe(sample(1, 100, 1, 1));
    assert_eq!(recovered.action, RuntimeAction::ResumeIntake);
    assert_eq!(monitor.receipt().recovery_count, 1);
}
