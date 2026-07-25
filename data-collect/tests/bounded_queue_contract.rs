use data_collect::RawEvent;
use data_collect::backpressure::bounded_event_queue;
use tokio::sync::mpsc::error::TrySendError;

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
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

#[tokio::test]
async fn bounded_queue_is_tokio_mpsc_and_never_unbounded() {
    let (tx, mut rx) = bounded_event_queue::<RawEvent>(1).unwrap();
    assert_eq!(rx.capacity(), 1);

    tx.try_send(synthetic_event(1)).unwrap();
    let error = tx.try_send(synthetic_event(2)).unwrap_err();
    assert!(matches!(error, TrySendError::Full(_)));
    assert!(rx.recv().await.is_some());
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
