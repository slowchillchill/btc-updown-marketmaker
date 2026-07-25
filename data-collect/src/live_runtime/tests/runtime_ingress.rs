use super::*;

#[tokio::test]
async fn ingress_rejects_an_event_whose_real_enqueue_permit_was_removed() {
    let recv_time_utc_ms = 1_783_392_404_599;
    let (tx, mut rx) = mpsc::channel(1);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    send_event(
        &sink,
        Instant::now(),
        test_raw_event("binance_aggtrade", recv_time_utc_ms),
    )
    .await
    .unwrap();
    let mut inflight_event = rx.recv().await.unwrap();
    assert!(inflight_event.raw_day_permit.take().is_some());

    let mut writers = BTreeMap::new();
    let mut batches = PendingIngressBatches::default();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_queue");
    let (manifest_tx, _manifest_rx) = mpsc::channel(1);
    let mut rollover_coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    let error = process_ingress_event(
        inflight_event,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut writer_queue_tracker,
        Instant::now(),
        &manifest_tx,
        &mut rollover_coordinator,
    )
    .await
    .expect_err("ingress must reject a missing raw-day permit");
    assert!(
        error
            .to_string()
            .contains("production raw event missing day/generation permit")
    );
    assert_eq!(received_rows, 0);
    assert!(coverage.is_empty());
    assert!(batches.legacy.is_empty());
    assert!(batches.polymarket_orderbook_v3.is_empty());
}
