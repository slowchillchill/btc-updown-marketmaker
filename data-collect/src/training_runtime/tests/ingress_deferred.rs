use super::*;

#[tokio::test]
async fn raw_day_gate_fences_boundary_events_until_normal_rollover_ack() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(boundary_utc_ms - 1).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_raw_day_gate(Arc::clone(&gate)).unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let run_started = Instant::now();

    assert!(
        send_event_with_reservation_outcome(
            &sink,
            run_started,
            test_raw_event("binance_aggtrade", boundary_utc_ms - 1),
            None,
            vec!["old_day_gap_follows_event_admission_day".to_owned()],
        )
        .await
        .unwrap()
    );
    let before = rx.recv().await.unwrap();
    assert_eq!(
        before.raw_day_permit,
        Some(RawDayPermit {
            utc_day: "2026-07-07".to_owned(),
            generation: INITIAL_RAW_DAY_GENERATION,
        })
    );
    assert!(
        gaps.lock()
            .unwrap()
            .reasons_for_day("2026-07-07")
            .iter()
            .any(|gap| gap == "old_day_gap_follows_event_admission_day")
    );
    assert!(
        gaps.lock()
            .unwrap()
            .reasons_for_day("2026-07-08")
            .is_empty()
    );

    let close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let at_boundary_sink = sink.clone();
    let at_boundary = tokio::spawn(async move {
        send_event_with_reservation_outcome(
            &at_boundary_sink,
            run_started,
            test_raw_event("binance_aggtrade", boundary_utc_ms),
            None,
            vec!["deferred_gap_published_only_after_admission".to_owned()],
        )
        .await
        .map(|_| ())
    });
    let after_boundary_sink = sink.clone();
    let after_boundary = tokio::spawn(async move {
        send_event(
            &after_boundary_sink,
            run_started,
            test_raw_event("binance_aggtrade", boundary_utc_ms + 1),
        )
        .await
    });
    at_boundary.await.unwrap().unwrap();
    after_boundary.await.unwrap().unwrap();
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let deferred_order = {
        let deferred = stats
            .deferred_ingress_batches
            .lock()
            .expect("deferred raw-event mutex poisoned");
        deferred["binance_aggtrade"]
            .iter()
            .flat_map(|batch| match batch {
                DeferredIngressBatch::LegacyRaw(batch) => batch.events.iter(),
                DeferredIngressBatch::PolymarketOrderbookV3(_) => {
                    panic!("Binance producer cannot own a deferred Polymarket v3 event")
                }
            })
            .map(|event| {
                assert!(event.raw_day_permit.is_none());
                assert!(event.event.collector_admission.is_none());
                event.event.recv_time_utc_ms
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(deferred_order.len(), 2);
    assert_eq!(stats.aggtrade_admission_sequence.load(Ordering::Relaxed), 1);
    assert!(
        gaps.lock()
            .unwrap()
            .reasons_for_day("2026-07-08")
            .is_empty()
    );

    send_ingress_control(
        &sink,
        run_started,
        IngressControl::FinalizeSourceHour {
            source_kind: "binance_depth_snapshot".to_owned(),
            utc_hour: "2026-07-08T00".to_owned(),
        },
        "test_control_bypasses_raw_day_fence",
    )
    .await
    .unwrap();
    assert!(matches!(
        rx.recv().await.unwrap().control,
        Some(IngressControl::FinalizeSourceHour { .. })
    ));

    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    flush_deferred_raw_events(&sink, "binance_aggtrade", &close)
        .await
        .unwrap();
    let permitted = [rx.recv().await.unwrap(), rx.recv().await.unwrap()];
    assert_eq!(
        permitted
            .iter()
            .map(|event| event.event.recv_time_utc_ms)
            .collect::<Vec<_>>(),
        deferred_order,
        "normal rollover must preserve deferred staging order"
    );
    assert_eq!(
        permitted
            .iter()
            .map(|event| event.event.collector_admission.as_ref().unwrap().sequence)
            .collect::<Vec<_>>(),
        vec![2, 3],
        "normal rollover must allocate admission only after the new-day permit"
    );
    assert!(permitted.iter().all(|event| {
        event.raw_day_permit.as_ref()
            == Some(&RawDayPermit {
                utc_day: "2026-07-08".to_owned(),
                generation: INITIAL_RAW_DAY_GENERATION + 1,
            })
    }));
    let gaps = gaps.lock().unwrap();
    assert!(
        gaps.reasons_for_day("2026-07-08")
            .iter()
            .any(|gap| gap == "deferred_gap_published_only_after_admission")
    );
    assert!(
        !gaps
            .reasons_for_day("2026-07-07")
            .iter()
            .any(|gap| gap == "deferred_gap_published_only_after_admission")
    );
}

#[tokio::test]
async fn controlled_rollover_cutoff_discards_pre_admission_staging_without_drop_evidence() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(boundary_utc_ms - 1).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    let close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_raw_day_gate(Arc::clone(&gate)).unwrap();
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let budget = InflightBudget::new();
    let available_before = budget.available();
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    assert!(
        send_event_with_reservation_outcome(
            &sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", boundary_utc_ms),
            None,
            vec!["staged_gap_must_not_publish".to_owned()],
        )
        .await
        .unwrap()
    );
    {
        let deferred = stats
            .deferred_ingress_batches
            .lock()
            .expect("deferred raw-event mutex poisoned");
        let DeferredIngressBatch::LegacyRaw(staged) = &deferred["binance_aggtrade"][0] else {
            panic!("Binance producer cannot own a deferred Polymarket v3 event");
        };
        let staged = &staged.events[0];
        assert!(staged.raw_day_permit.is_none());
        assert!(staged.event.collector_admission.is_none());
    }
    assert_eq!(stats.aggtrade_admission_sequence.load(Ordering::Relaxed), 0);
    assert_eq!(budget.available().0 + 1, available_before.0);

    let PreparedRuntimeExit::Rollover(exit) = gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms + 1, RuntimeCloseCause::OperatorStop)
        .unwrap()
    else {
        panic!("post-boundary operator stop must remain a PendingExit");
    };
    let error = apply_day_barrier_release(
        &sink,
        "binance_aggtrade",
        &close,
        DayBarrierRelease::StopWithClose(RuntimeCloseIdentity::from_rollover_old_day(&exit.close)),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("coordinated source stop"));
    assert!(
        stats
            .deferred_ingress_batches
            .lock()
            .expect("deferred raw-event mutex poisoned")
            .is_empty()
    );
    assert_eq!(budget.available(), available_before);
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    assert_eq!(stats.aggtrade_admission_sequence.load(Ordering::Relaxed), 0);
    assert!(stats.drain_all_operational().is_empty());
    assert!(gaps.lock().unwrap().is_empty());
    assert!(rx.try_recv().is_err());

    let post_gate = Arc::new(RawDayGate::new());
    post_gate
        .activate_startup(
            utc_day_string_from_ms(boundary_utc_ms - 1).unwrap(),
            INITIAL_RAW_DAY_GENERATION,
        )
        .await
        .unwrap();
    let post_close = post_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let post_stats = Arc::new(SourceQueueBackpressureStats::default());
    post_stats
        .install_raw_day_gate(Arc::clone(&post_gate))
        .unwrap();
    let post_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let post_budget = InflightBudget::new();
    let post_available_before = post_budget.available();
    let (post_tx, mut post_rx) = mpsc::channel(4);
    let post_sink = SourceEventSink {
        tx: post_tx,
        gaps: Arc::clone(&post_gaps),
        source_queue_backpressure: Arc::clone(&post_stats),
        inflight_budget: post_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    assert!(
        send_event_with_reservation_outcome(
            &post_sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", boundary_utc_ms),
            None,
            vec!["post_activation_staged_gap_must_not_publish".to_owned()],
        )
        .await
        .unwrap()
    );
    commit_test_manifest_activation(&post_gate, &post_close);
    let PreparedRuntimeExit::SameDay(post_exit) = post_gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms + 1, RuntimeCloseCause::OperatorStop)
        .unwrap()
    else {
        panic!("post-activation operator stop must belong to the new day");
    };
    assert_eq!(post_exit.identity.utc_day, "2026-07-08");
    flush_deferred_raw_events(&post_sink, "binance_aggtrade", &post_close)
        .await
        .unwrap();
    assert!(
        post_stats
            .deferred_ingress_batches
            .lock()
            .expect("deferred raw-event mutex poisoned")
            .is_empty()
    );
    assert_eq!(post_budget.available(), post_available_before);
    assert_eq!(post_stats.dropped_rows.load(Ordering::Relaxed), 0);
    assert_eq!(
        post_stats
            .aggtrade_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
    assert!(post_stats.drain_all_operational().is_empty());
    assert!(post_gaps.lock().unwrap().is_empty());
    assert!(post_rx.try_recv().is_err());
}
