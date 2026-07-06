use data_collect::RawEvent;
use data_collect::backpressure::{
    BackpressureConfig, BackpressureMonitor, BackpressureSeverity, QueueSample, RuntimeAction,
    bounded_event_queue,
};
use tokio::sync::mpsc::error::TrySendError;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "binance_depth_update".to_owned(),
        source_event_type: "depthUpdate".to_owned(),
        source_identifier: "btcusdt@depth".to_owned(),
        source_ts: Some(seq.to_string()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        raw_json: format!(r#"{{"e":"depthUpdate","u":{seq}}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "binance_depth_update".to_owned(),
    }
}

#[tokio::test]
async fn bounded_queue_records_backpressure_and_source_gap() {
    let (tx, rx) = bounded_event_queue::<RawEvent>(1).unwrap();
    assert_eq!(rx.capacity(), 1);

    tx.try_send(synthetic_event(1)).unwrap();
    let error = tx.try_send(synthetic_event(2)).unwrap_err();
    assert!(matches!(error, TrySendError::Full(_)));

    let mut monitor = BackpressureMonitor::new(
        "binance_depth_update",
        BackpressureConfig::new(
            QueueSample {
                rows: 1,
                bytes: 256,
                oldest_event_age_ms: 10,
                writer_lag_ms: 5,
            },
            QueueSample {
                rows: 10,
                bytes: 4096,
                oldest_event_age_ms: 1000,
                writer_lag_ms: 500,
            },
            2,
        ),
    );

    let verdict = monitor.record_queue_full(QueueSample {
        rows: 1,
        bytes: 512,
        oldest_event_age_ms: 25,
        writer_lag_ms: 9,
    });

    assert_eq!(verdict.severity, BackpressureSeverity::SoftDegraded);
    assert_eq!(verdict.action, RuntimeAction::Continue);
    assert!(!verdict.process_fatal);
    assert_eq!(monitor.source_gaps().len(), 1);
    assert_eq!(monitor.source_gaps()[0].reason, "bounded_queue_full");
}

#[test]
fn bounded_queue_rejects_zero_capacity_without_panic() {
    let result = bounded_event_queue::<RawEvent>(0);

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "bounded event queue capacity must be positive"
    );
}

#[test]
fn queue_watermarks_include_rows_bytes_age_writer_lag() {
    let mut monitor = BackpressureMonitor::new(
        "polymarket_market",
        BackpressureConfig::new(
            QueueSample {
                rows: 5,
                bytes: 1024,
                oldest_event_age_ms: 50,
                writer_lag_ms: 20,
            },
            QueueSample {
                rows: 10,
                bytes: 4096,
                oldest_event_age_ms: 1000,
                writer_lag_ms: 500,
            },
            2,
        ),
    );

    let soft = monitor.observe(QueueSample {
        rows: 6,
        bytes: 2048,
        oldest_event_age_ms: 75,
        writer_lag_ms: 25,
    });

    assert_eq!(soft.severity, BackpressureSeverity::SoftDegraded);
    assert_eq!(soft.action, RuntimeAction::Continue);
    assert_eq!(soft.watermark.rows, 6);
    assert_eq!(soft.watermark.bytes, 2048);
    assert_eq!(soft.watermark.oldest_event_age_ms, 75);
    assert_eq!(soft.watermark.writer_lag_ms, 25);
    assert_eq!(monitor.high_watermark(), soft.watermark);

    let hard_once = monitor.observe(QueueSample {
        rows: 12,
        bytes: 8192,
        oldest_event_age_ms: 1500,
        writer_lag_ms: 700,
    });
    assert_eq!(hard_once.severity, BackpressureSeverity::HardDegraded);
    assert_eq!(hard_once.action, RuntimeAction::Continue);
    assert!(!hard_once.process_fatal);

    let hard_twice = monitor.observe(QueueSample {
        rows: 13,
        bytes: 9000,
        oldest_event_age_ms: 1600,
        writer_lag_ms: 750,
    });
    assert_eq!(hard_twice.severity, BackpressureSeverity::HardDegraded);
    assert_eq!(hard_twice.action, RuntimeAction::PauseIntake);
    assert!(!hard_twice.process_fatal);
}
