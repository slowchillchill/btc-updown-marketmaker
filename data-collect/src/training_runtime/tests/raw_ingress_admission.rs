use super::*;

#[tokio::test]
async fn send_event_accepts_exact_one_mib_owned_row_and_rejects_one_byte_more() {
    let budget = InflightBudget::new();
    let (tx, mut rx) = mpsc::channel(2);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, 1_783_392_404_599).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut exact = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    exact.raw_json.clear();
    exact.raw_json.shrink_to_fit();
    let fixed_bytes = owned_event_bytes(&exact);
    exact.raw_json = "x".repeat(MAX_TYPED_ROW_BYTES - fixed_bytes);
    exact.raw_json.shrink_to_fit();
    assert_eq!(owned_event_bytes(&exact), MAX_TYPED_ROW_BYTES);
    send_event(&sink, Instant::now(), exact).await.unwrap();

    let accepted = rx.recv().await.unwrap();
    drop(accepted);
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );

    let mut oversized = test_raw_event("binance_aggtrade", 1_783_392_404_600);
    oversized.raw_json.clear();
    oversized.raw_json.shrink_to_fit();
    let fixed_bytes = owned_event_bytes(&oversized);
    oversized.raw_json = "x".repeat(MAX_TYPED_ROW_BYTES + 1 - fixed_bytes);
    oversized.raw_json.shrink_to_fit();
    let error = send_event(&sink, Instant::now(), oversized)
        .await
        .expect_err("a row over 1 MiB must fail before queueing");
    assert!(error.to_string().contains("max_typed_row_bytes_exceeded"));
    assert_eq!(crate::runtime::collector_exit_code(error.as_ref()), 76);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn copied_owned_payloads_are_counted_independently_until_each_is_released() {
    let event = test_raw_event("binance_aggtrade", 1_783_392_404_599);
    let copied_event = event.clone();
    let copied_bytes = owned_event_bytes(&copied_event);
    let original_bytes = owned_event_bytes(&event);
    let budget = InflightBudget::new();
    let (tx, mut rx) = mpsc::channel(2);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, event.recv_time_utc_ms).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    send_event(&sink, Instant::now(), copied_event)
        .await
        .unwrap();
    send_event(&sink, Instant::now(), event).await.unwrap();
    assert_eq!(
        budget.available(),
        (
            GLOBAL_INFLIGHT_MAX_ROWS - 2,
            GLOBAL_INFLIGHT_MAX_BYTES - copied_bytes - original_bytes
        )
    );

    drop(rx.recv().await.unwrap());
    assert_eq!(budget.available().0, GLOBAL_INFLIGHT_MAX_ROWS - 1);
    drop(rx.recv().await.unwrap());
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn short_source_queue_backpressure_waits_without_dropping() {
    let (tx, mut rx) = mpsc::channel(1);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, 1_783_392_404_599).await;
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
        test_raw_event("binance_aggtrade", 1_783_392_404_599),
    )
    .await
    .unwrap();
    let second_sink = sink.clone();
    let second = tokio::spawn(async move {
        send_event(
            &second_sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", 1_783_392_404_600),
        )
        .await
    });
    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        stats.aggtrade_admission_sequence.load(Ordering::Relaxed),
        1,
        "a sender waiting for queue capacity must not preallocate admission"
    );
    let first = rx.recv().await.unwrap();
    assert_eq!(
        first.event.collector_admission.as_ref().unwrap().sequence,
        1
    );
    second.await.unwrap().unwrap();
    let second = rx.recv().await.unwrap();
    assert_eq!(
        second.event.collector_admission.as_ref().unwrap().sequence,
        2
    );

    assert_eq!(stats.wait_count.load(Ordering::Relaxed), 1);
    assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 0);
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn closed_source_queue_returns_error_and_releases_budget() {
    let budget = InflightBudget::new();
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, 1_783_392_404_599).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let error = send_event(
        &sink,
        Instant::now(),
        test_raw_event("binance_aggtrade", 1_783_392_404_599),
    )
    .await
    .expect_err("closed ingress must stop the source");
    assert!(error.to_string().contains("bounded_queue_send_closed"));
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn exhausted_global_budgets_stop_admission_with_a_gap_and_ordinary_failure() {
    for exhaust_rows in [true, false] {
        let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
        let held = if exhaust_rows {
            budget.try_acquire(1, 0)
        } else {
            budget.try_acquire(0, MAX_TYPED_ROW_BYTES)
        };
        let (tx, mut rx) = mpsc::channel(1);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let received = now_ms();
        install_active_raw_day_gate(&stats, received).await;
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let started = Instant::now();
        let error = send_event(&sink, started, test_raw_event("binance_aggtrade", received))
            .await
            .expect_err("真实额度耗尽必须停止本轮准入");
        assert!(started.elapsed() >= BUFFER_ACQUIRE_TIMEOUT);
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::TimedOut
        );
        let reason = if exhaust_rows {
            "global_inflight_row_acquire_timeout"
        } else {
            "global_inflight_byte_acquire_timeout"
        };
        assert!(error.to_string().starts_with(reason));
        assert_eq!(crate::runtime::collector_exit_code(error.as_ref()), 1);
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.aggtrade_admission_sequence.load(Ordering::Relaxed), 0);
        assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 1);
        assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 1);
        assert!(gaps.lock().unwrap().iter().any(|gap| gap.contains(reason)));
        drop(held);
        assert_eq!(budget.available(), (1, MAX_TYPED_ROW_BYTES));
        let mixed = io::Error::other(format!(
            "{reason};operator_intervention_required:invalid_path"
        ));
        assert_eq!(crate::runtime::collector_exit_code(&mixed), 76);
    }
}
