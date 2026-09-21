use super::*;

async fn prepared_gate_fixture(
    producer: &str,
    boundary_utc_ms: i64,
) -> (
    Arc<SourceQueueBackpressureStats>,
    Arc<RawDayGate>,
    RawDayClose,
) {
    let gate = Arc::new(RawDayGate::new());
    let utc_day = utc_day_string_from_ms(boundary_utc_ms.saturating_sub(1)).unwrap();
    gate.activate_startup(utc_day.clone(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let close = prepare_rollover_for_producer(&gate, producer, &utc_day, boundary_utc_ms);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_raw_day_gate(Arc::clone(&gate)).unwrap();
    (stats, gate, close)
}

#[tokio::test]
async fn all_producers_enqueue_crossed_startup_barrier_before_first_source_event() {
    let initial_boundary = now_ms().div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
    for producer in ["polymarket_rtds"] {
        let (tx, mut rx) = mpsc::channel(4);
        let marker_tx = tx.clone();
        let (stats, gate, close) = prepared_gate_fixture(producer, initial_boundary).await;
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let source = async move {
            marker_tx
                .send(SourceIngressPayload::RawEvent(InflightRawEvent::for_test(
                    test_raw_event("binance_aggtrade", now_ms()),
                )))
                .await
                .unwrap();
            Ok(())
        };

        let producer_name = producer;
        let wrapper = tokio::spawn(run_logical_producer_with_day_barriers(
            producer,
            sink,
            Instant::now(),
            initial_boundary,
            source,
        ));

        let barrier = rx.recv().await.unwrap().into_raw();
        let Some(IngressControl::DayBarrier {
            producer: actual,
            reply,
            ..
        }) = barrier.control
        else {
            panic!("expected startup-crossing day barrier");
        };
        assert_eq!(actual, producer_name);
        commit_test_manifest_activation(&gate, &close);
        gate.activate_after_rollover(&close).await.unwrap();
        reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
        assert!(rx.recv().await.unwrap().control.is_none());
        wrapper.await.unwrap().unwrap();
    }

    let (tx, mut rx) = mpsc::channel(4);
    let marker_tx = tx.clone();
    let (stats, gate, close) = prepared_gate_fixture("polymarket", initial_boundary).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, _control_rx) = mpsc::channel(1);
    let source = async move {
        marker_tx
            .send(SourceIngressPayload::RawEvent(InflightRawEvent::for_test(
                test_raw_event("binance_aggtrade", now_ms()),
            )))
            .await
            .unwrap();
        Ok(())
    };
    let wrapper = tokio::spawn(run_polymarket_logical_producer_with_day_barriers(
        sink,
        control_tx,
        Instant::now(),
        initial_boundary,
        source,
    ));
    let barrier = rx.recv().await.unwrap().into_raw();
    let Some(IngressControl::DayBarrier {
        producer, reply, ..
    }) = barrier.control
    else {
        panic!("expected Polymarket startup-crossing day barrier");
    };
    assert_eq!(producer, "polymarket");
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
    assert!(rx.recv().await.unwrap().control.is_none());
    wrapper.await.unwrap().unwrap();

    let (tx, mut rx) = mpsc::channel(4);
    let marker_tx = tx.clone();
    let (stats, gate, close) = prepared_gate_fixture("binance_aggtrade", initial_boundary).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, _control_rx) = mpsc::channel(1);
    let source = async move {
        marker_tx
            .send(SourceIngressPayload::RawEvent(InflightRawEvent::for_test(
                test_raw_event("binance_aggtrade", now_ms()),
            )))
            .await
            .unwrap();
        Ok(())
    };
    let wrapper = tokio::spawn(run_aggtrade_logical_producer_with_day_barriers(
        sink,
        control_tx,
        Instant::now(),
        initial_boundary,
        source,
    ));
    let barrier = rx.recv().await.unwrap().into_raw();
    let Some(IngressControl::DayBarrier {
        producer, reply, ..
    }) = barrier.control
    else {
        panic!("expected aggTrade startup-crossing day barrier");
    };
    assert_eq!(producer, "binance_aggtrade");
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
    assert!(rx.recv().await.unwrap().control.is_none());
    wrapper.await.unwrap().unwrap();

    let (tx, mut rx) = mpsc::channel(4);
    let marker_tx = tx.clone();
    let (stats, gate, close) = prepared_gate_fixture("binance_depth", initial_boundary).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, _control_rx) = mpsc::channel(1);
    let source = async move {
        marker_tx
            .send(SourceIngressPayload::RawEvent(InflightRawEvent::for_test(
                test_raw_event("binance_aggtrade", now_ms()),
            )))
            .await
            .unwrap();
        Ok(())
    };
    let wrapper = tokio::spawn(run_depth_logical_producer_with_time_barriers(
        sink,
        control_tx,
        Instant::now(),
        initial_boundary,
        next_utc_hour_boundary_ms(now_ms()),
        source,
    ));
    let barrier = rx.recv().await.unwrap().into_raw();
    let Some(IngressControl::DayBarrier {
        producer, reply, ..
    }) = barrier.control
    else {
        panic!("expected depth startup-crossing day barrier");
    };
    assert_eq!(producer, "binance_depth");
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
    assert!(rx.recv().await.unwrap().control.is_none());
    wrapper.await.unwrap().unwrap();
}

#[tokio::test]
async fn depth_hour_boundary_fences_snapshot_before_finalize_control() {
    let (tx, mut rx) = mpsc::channel(4);
    let recv_time_utc_ms = 1_783_392_404_600;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let source_sink = sink.clone();
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let run_started = Instant::now();
    let next_hour_boundary_ms = now_ms() + 20;
    let source = async move {
        send_event(
            &source_sink,
            run_started,
            test_binance_depth_snapshot_event(recv_time_utc_ms, "bootstrap"),
        )
        .await?;
        let control = control_rx
            .recv()
            .await
            .ok_or_else(|| invalid_data("depth control channel closed before test hour barrier"))?;
        handle_depth_source_control(control).await?;
        Ok(())
    };
    run_depth_logical_producer_with_time_barriers(
        sink,
        control_tx,
        run_started,
        next_hour_boundary_ms + UTC_DAY_MS,
        next_hour_boundary_ms,
        source,
    )
    .await
    .unwrap();

    let snapshot = rx.recv().await.unwrap();
    assert!(snapshot.control.is_none());
    assert_eq!(snapshot.event.source_event_type, "depth_snapshot");
    let expected_hour = utc_hour_string_from_ms(next_hour_boundary_ms - 1).unwrap();
    assert!(matches!(
        rx.recv().await.unwrap().control,
        Some(IngressControl::FinalizeSourceHour {
            ref source_kind,
            ref utc_hour,
        }) if source_kind == "binance_depth_snapshot" && utc_hour == &expected_hour
    ));
}

#[tokio::test]
async fn source_fence_drains_an_accepted_enqueue_before_barrier_freezes_polling() {
    let (tx, mut rx) = mpsc::channel(1);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    runtime_stats
        .install_raw_day_gate(Arc::clone(&gate))
        .unwrap();
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: runtime_stats,
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
    let source_sink = sink.clone();
    let source = async move {
        send_event(
            &source_sink,
            run_started,
            test_raw_event("binance_aggtrade", 1_783_468_799_999),
        )
        .await?;
        std::future::pending::<Result<()>>().await
    };
    tokio::pin!(source);
    assert!(matches!(
        futures::poll!(source.as_mut()),
        std::task::Poll::Pending
    ));
    let consumer = tokio::spawn(async move {
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        let mut barrier = rx.recv().await.unwrap();
        let Some(IngressControl::DayBarrier { reply, .. }) = barrier.control.take() else {
            panic!("expected producer day barrier");
        };
        reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
        (first, second, barrier)
    });

    let close = test_rollover_close(1_783_468_800_000);
    gate.prepare_rollover(&close.utc_day, close.boundary_utc_ms)
        .unwrap();
    for producer in LOGICAL_PRODUCERS {
        gate.register_source_fence(producer, &close).unwrap();
    }

    let sealed = timeout(
        Duration::from_secs(1),
        seal_raw_day_while_source_runs(source.as_mut(), &gate, &close),
    )
    .await
    .expect("source fence must drain the already accepted enqueue")
    .unwrap();
    assert!(matches!(sealed, RawDaySealOutcome::Sealed(_)));

    timeout(Duration::from_secs(1), async {
        let release =
            send_day_barrier(&sink, run_started, "polymarket_rtds", close.clone()).await?;
        apply_day_barrier_release(&sink, "polymarket_rtds", &close, release).await
    })
    .await
    .expect("barrier must proceed after the source fence froze polling")
    .unwrap();
    let (first, second, barrier) = timeout(Duration::from_secs(1), consumer)
        .await
        .expect("source event and barrier must both reach ingress")
        .unwrap();
    assert_eq!(first.event.recv_time_utc_ms, 1_783_468_799_998);
    assert_eq!(second.event.recv_time_utc_ms, 1_783_468_799_999);
    assert!(barrier.control.is_none());
    assert_eq!(close.utc_day, "2026-07-07");
}

#[tokio::test]
async fn transport_recovery_crossing_utc_boundary_is_gap_neutral() {
    let (tx, mut rx) = mpsc::channel(2);
    let boundary_utc_ms = now_ms().div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
    let (stats, gate, close) = prepared_gate_fixture("polymarket_rtds", boundary_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    stats.set_recovery_active("polymarket_rtds_chainlink_btc_usd", true);

    let producer = tokio::spawn(run_logical_producer_with_day_barriers(
        "polymarket_rtds",
        sink,
        Instant::now(),
        boundary_utc_ms,
        async { Ok(()) },
    ));

    let message = rx.recv().await.unwrap().into_raw();
    let Some(IngressControl::DayBarrier { reply, .. }) = message.control else {
        panic!("expected RTDS day barrier");
    };
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
    producer.await.unwrap().unwrap();
    assert!(gaps.lock().unwrap().is_empty());
}

#[tokio::test]
async fn polymarket_day_barrier_is_between_fenced_and_resumed_source_events() {
    let (tx, mut rx) = mpsc::channel(8);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = Arc::new(RawDayGate::new());
    let boundary_utc_ms = 1_783_468_800_000;
    raw_day_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    stats
        .install_raw_day_gate(Arc::clone(&raw_day_gate))
        .unwrap();
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let source_sink = sink.clone();
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let run_started = Instant::now();
    let source = async move {
        send_event(
            &source_sink,
            run_started,
            raw_event_with_recv_time(
                "polymarket_market",
                "book",
                "before-fence",
                None,
                boundary_utc_ms - 1,
                MARKET_WS_ENDPOINT,
                &CollectorIdentity {
                    host_id: "collector-test".to_owned(),
                    region: "local-test".to_owned(),
                },
                json!({"position": "before"}),
            ),
        )
        .await?;
        let PolymarketSourceControl::Fence {
            stop_after_fence,
            day_boundary_utc_ms: _,
            resume_after_barrier,
            reply,
        } = control_rx.recv().await.unwrap();
        assert!(!stop_after_fence);
        reply.send(Ok(())).unwrap();
        resume_after_barrier.unwrap().await.unwrap();
        send_event(
            &source_sink,
            run_started,
            raw_event_with_recv_time(
                "polymarket_market",
                "book",
                "after-resume",
                None,
                boundary_utc_ms,
                MARKET_WS_ENDPOINT,
                &CollectorIdentity {
                    host_id: "collector-test".to_owned(),
                    region: "local-test".to_owned(),
                },
                json!({"position": "after"}),
            ),
        )
        .await?;
        Ok(())
    };
    tokio::pin!(source);
    assert!(matches!(
        futures::poll!(source.as_mut()),
        std::task::Poll::Pending
    ));
    let close =
        prepare_rollover_for_producer(&raw_day_gate, "polymarket", "2026-07-07", boundary_utc_ms);
    let consumer_gate = Arc::clone(&raw_day_gate);
    let consumer_close = close.clone();
    let consumer = tokio::spawn(async move {
        let before = rx.recv().await.unwrap();
        let mut barrier = rx.recv().await.unwrap();
        let Some(IngressControl::DayBarrier { reply, .. }) = barrier.control.take() else {
            panic!("expected Polymarket day barrier");
        };
        commit_test_manifest_activation(&consumer_gate, &consumer_close);
        consumer_gate
            .activate_after_rollover(&consumer_close)
            .await
            .unwrap();
        reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
        let after = rx.recv().await.unwrap();
        (before, barrier, after)
    });

    assert!(
        fence_polymarket_and_send_day_barrier(
            source.as_mut(),
            &control_tx,
            &sink,
            run_started,
            &raw_day_gate,
            &close,
        )
        .await
        .unwrap()
        .is_none()
    );
    source.await.unwrap();
    let (before, barrier, after) = consumer.await.unwrap();
    assert_eq!(before.event.source_identifier, "before-fence");
    assert!(barrier.control.is_none());
    assert_eq!(after.event.source_identifier, "after-resume");
}

#[tokio::test]
async fn polymarket_terminal_stop_matches_post_activation_close_identity() {
    let boundary_utc_ms = now_ms().div_euclid(UTC_DAY_MS).saturating_mul(UTC_DAY_MS);
    let (stats, raw_day_gate, close) = prepared_gate_fixture("polymarket", boundary_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let source = async move {
        let PolymarketSourceControl::Fence {
            stop_after_fence,
            resume_after_barrier,
            reply,
            ..
        } = control_rx.recv().await.unwrap();
        assert!(!stop_after_fence);
        reply.send(Ok(())).unwrap();
        resume_after_barrier.unwrap().await.unwrap();
        Err::<(), Box<dyn Error + Send + Sync>>(SdkStreamError::Lagged(1_364).into())
    };
    tokio::pin!(source);
    let consumer_gate = Arc::clone(&raw_day_gate);
    let consumer_close = close.clone();
    let consumer = tokio::spawn(async move {
        let mut barrier = rx.recv().await.unwrap();
        let Some(IngressControl::DayBarrier { reply, .. }) = barrier.control.take() else {
            panic!("expected Polymarket day barrier");
        };
        let (phase_tx, phase_rx) = watch::channel(ProducerReleasePhase::Held);
        let (ack_tx, mut ack_rx) = mpsc::channel(2);
        reply
            .send(Ok(DayBarrierRelease::PrepareResume(ProducerReleaseToken {
                producer: "polymarket".to_owned(),
                close: consumer_close.clone(),
                phase_rx,
                ack_tx,
            })))
            .unwrap();
        let armed = ack_rx.recv().await.unwrap();
        assert_eq!(armed.producer, "polymarket");
        assert_eq!(armed.kind, ProducerReleaseAckKind::Armed);
        assert!(armed.error.is_none());

        commit_test_manifest_activation(&consumer_gate, &consumer_close);
        phase_tx.send_replace(ProducerReleasePhase::FlushDeferred);
        let flushed = ack_rx.recv().await.unwrap();
        assert_eq!(flushed.producer, "polymarket");
        assert_eq!(flushed.kind, ProducerReleaseAckKind::Flushed);
        assert!(flushed.error.is_none());

        let prepared = consumer_gate
            .prepare_runtime_exit_with_cause(
                boundary_utc_ms + 1,
                RuntimeCloseCause::SourceInnerReturn {
                    source_kind: "polymarket_market",
                },
            )
            .unwrap();
        let stop_identity = RawDayGate::runtime_exit_identity(&prepared);
        phase_tx.send_replace(ProducerReleasePhase::Stop(stop_identity.clone()));
        assert!(rx.try_recv().is_err());
        stop_identity
    });

    let error = timeout(
        Duration::from_secs(1),
        fence_polymarket_and_send_day_barrier(
            source.as_mut(),
            &control_tx,
            &sink,
            Instant::now(),
            &raw_day_gate,
            &close,
        ),
    )
    .await
    .expect("post-activation stop must release the paused source")
    .expect_err("the exact SDK terminal must escape the coordinated stop");
    assert_eq!(error.to_string(), "receiver lagged by 1364 messages");
    let stop_identity = consumer.await.unwrap();
    assert_eq!(
        raw_day_gate
            .prepared_runtime_exit()
            .map(|prepared| RawDayGate::runtime_exit_identity(&prepared)),
        Some(stop_identity)
    );
}
