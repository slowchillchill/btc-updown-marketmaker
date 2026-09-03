use super::*;

#[tokio::test]
async fn producer_barrier_cannot_overtake_an_old_day_event_waiting_for_queue_space() {
    let (tx, mut rx) = mpsc::channel(1);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, 1_783_468_799_998).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let run_started = Instant::now();
    send_event(
        &sink,
        run_started,
        test_raw_event("binance_aggtrade", 1_783_468_799_998),
    )
    .await
    .unwrap();

    let old_sink = sink.clone();
    let old_event = tokio::spawn(async move {
        send_event(
            &old_sink,
            run_started,
            test_raw_event("binance_aggtrade", 1_783_468_799_999),
        )
        .await
    });
    while sink.enqueue_lock.try_lock().is_ok() {
        tokio::task::yield_now().await;
    }
    let barrier_sink = sink.clone();
    let close = test_rollover_close(1_783_468_800_000);
    let expected_close = close.clone();
    let barrier = tokio::spawn(async move {
        send_day_barrier(&barrier_sink, run_started, "binance_aggtrade", close).await
    });

    let first = rx.recv().await.unwrap();
    assert_eq!(first.event.recv_time_utc_ms, 1_783_468_799_998);
    old_event.await.unwrap().unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(second.event.recv_time_utc_ms, 1_783_468_799_999);
    assert!(second.control.is_none());
    let third = rx.recv().await.unwrap().into_raw();
    let Some(IngressControl::DayBarrier {
        producer,
        close,
        reply,
    }) = third.control
    else {
        panic!("expected producer day barrier");
    };
    assert_eq!(producer, "binance_aggtrade");
    assert_eq!(close, expected_close);
    reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
    barrier.await.unwrap().unwrap();
}
