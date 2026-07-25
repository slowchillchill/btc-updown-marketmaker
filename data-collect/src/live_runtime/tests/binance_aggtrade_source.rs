use super::*;

#[tokio::test]
async fn aggtrade_sequence_overflow_is_terminal_and_records_bounded_evidence() {
    let recv_time_utc_ms = 1_783_392_404_599;
    let (tx, mut rx) = mpsc::channel(2);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    stats
        .aggtrade_admission_sequence
        .store(u64::MAX, Ordering::Relaxed);
    let admission_error = send_event(
        &sink,
        Instant::now(),
        test_raw_event("binance_aggtrade", recv_time_utc_ms),
    )
    .await
    .expect_err("admission sequence overflow must fail closed");
    assert!(
        admission_error
            .to_string()
            .contains("admission sequence overflow")
    );
    assert!(rx.try_recv().is_err());

    let ingress = BinanceAggTradeIngressSequenceAllocator::from_last_allocated(u64::MAX);
    let ingress_error = allocate_aggtrade_websocket_ingress(&ingress, &sink, recv_time_utc_ms)
        .expect_err("ingress sequence overflow must fail closed");
    assert!(
        ingress_error
            .to_string()
            .contains("binance_aggtrade_ingress_sequence_overflow")
    );
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| { gap.starts_with("binance_aggtrade_admission_integrity:") })
    );
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| { gap.starts_with("binance_aggtrade_ingress_sequence_terminal:") })
    );
    let counters = stats.drain_all_operational();
    assert_eq!(
        counters.values().next().unwrap()["binance_aggtrade"].dropped_rows,
        2
    );
}

#[test]
fn aggtrade_max_complete_invalid_capture_fits_the_typed_row_budget() {
    let invalid = binance_aggtrade_websocket_invalid_input(
        vec![b'x'; BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES],
        1_783_392_404_599,
        BINANCE_AGGTRADE_WS,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
        BinanceAggTradeInvalidReason::InvalidJson,
        "max_capture",
    )
    .unwrap();
    let event = binance_aggtrade_invalid_raw_event(
        &invalid,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        &BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url: BINANCE_AGGTRADE_WS.to_owned(),
            frame_inactivity_timeout_secs: 30,
            recovery_wait_override: None,
        },
        "test-source-epoch",
        1,
    )
    .unwrap();
    assert!(
        owned_event_bytes(&event) <= MAX_TYPED_ROW_BYTES,
        "max complete capture owns {} bytes, above the typed-row limit {}",
        owned_event_bytes(&event),
        MAX_TYPED_ROW_BYTES
    );
}

#[tokio::test]
async fn aggtrade_max_websocket_invalid_releases_constrained_budget_after_queue() {
    let recv_time_utc_ms = 1_783_392_404_599;
    let max_rows = 1;
    let max_bytes =
        aggtrade_websocket_staging_bytes(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES, true).unwrap();
    let budget = InflightBudget::with_limits(max_rows, max_bytes);
    let staging_reservation = budget
        .acquire(1, max_bytes, BUFFER_ACQUIRE_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(budget.available(), (0, 0));

    let (tx, mut rx) = mpsc::channel(1);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    emit_aggtrade_websocket_invalid(
        vec![b'x'; BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES],
        recv_time_utc_ms,
        BinanceAggTradeIngressCoordinate::Websocket { sequence: 1 },
        BinanceAggTradeInvalidReason::InvalidJson,
        "max_capture",
        &sink,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        &BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url: BINANCE_AGGTRADE_WS.to_owned(),
            frame_inactivity_timeout_secs: 30,
            recovery_wait_override: None,
        },
        "test-source-epoch",
        1,
        Instant::now(),
        staging_reservation,
    )
    .await
    .unwrap();

    let queued = rx.recv().await.unwrap();
    assert_eq!(queued.event.source_event_type, "invalid_input");
    let payload: Value = serde_json::from_str(&queued.event.raw_json).unwrap();
    assert_eq!(payload["payload_length_bytes"], 524_288);
    assert_eq!(payload["payload_capture_status"], "complete");
    assert_ne!(budget.available(), (max_rows, max_bytes));
    drop(queued);
    assert_eq!(budget.available(), (max_rows, max_bytes));
}

#[tokio::test]
async fn binance_server_shutdown_reconnects_before_forwarding_business_data() {
    assert_binance_reconnects_after(Some(Message::Text(
        json!({"e": "serverShutdown", "E": 1_783_392_404_500_i64})
            .to_string()
            .into(),
    )))
    .await;
}

#[tokio::test]
async fn binance_combined_server_shutdown_reconnects_before_business_data() {
    assert_binance_reconnects_after(Some(Message::Text(
        json!({
            "stream": "!serverShutdown",
            "data": {"e": "serverShutdown", "E": 1_783_392_404_500_i64}
        })
        .to_string()
        .into(),
    )))
    .await;
}

#[tokio::test]
async fn binance_server_close_reconnects_for_24_hour_connection_rotation() {
    assert_binance_reconnects_after(Some(Message::Close(None))).await;
}

#[tokio::test]
async fn binance_eof_reconnects_before_forwarding_business_data() {
    assert_binance_reconnects_after(None).await;
}

async fn assert_binance_reconnects_after(first_message: Option<Message>) {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = accept_async(first).await.unwrap();
        match first_message {
            Some(first_message) => first.send(first_message).await.unwrap(),
            None => drop(first),
        }

        let (second, _) = listener.accept().await.unwrap();
        let mut second = accept_async(second).await.unwrap();
        second.send(test_aggtrade_message()).await.unwrap();
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(8).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 10,
            recovery_wait_override: None,
        },
        sink,
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(3)),
        Instant::now(),
        control_rx,
    ));

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("aggTrade should arrive after reconnect")
        .expect("source queue should stay open")
        .into_legacy_event();

    assert_eq!(event.source_event_type, "aggTrade");
    assert_eq!(event.source_ts.as_deref(), Some("1783392404599"));
    assert_eq!(
        serde_json::from_str::<Value>(&event.raw_json).unwrap()["event_type"],
        "aggTrade"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&event.raw_json).unwrap()["connection_generation"],
        2,
        "the business row after reconnect must identify the second socket generation"
    );
    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_handshake_failure_recovers_without_allocating_a_connection_generation() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (failed_handshake, _) = listener.accept().await.unwrap();
        drop(failed_handshake);
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(4).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 5,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(5)),
        Instant::now(),
        control_rx,
    ));

    let event = timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("the source must recover after the failed handshake")
        .expect("source queue must remain open")
        .into_legacy_event();
    let envelope: Value = serde_json::from_str(&event.raw_json).unwrap();
    assert_eq!(envelope["connection_generation"], 1);
    assert!(gaps.lock().unwrap().all_reasons().is_empty());

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_frame_inactivity_reconnects_with_stable_epoch_and_no_gap() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = accept_async(first).await.unwrap();
        first.send(test_aggtrade_message_for_id(100)).await.unwrap();
        let (second, _) = timeout(Duration::from_secs(4), listener.accept())
            .await
            .expect("full-frame inactivity must create a replacement connection")
            .unwrap();
        let mut second = accept_async(second).await.unwrap();
        second
            .send(test_aggtrade_message_for_id(101))
            .await
            .unwrap();
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(4).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 1,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(6)),
        Instant::now(),
        control_rx,
    ));

    let mut envelopes = Vec::new();
    while envelopes.len() < 2 {
        let event = timeout(Duration::from_secs(4), rx.recv())
            .await
            .expect("both socket generations must deliver their trade")
            .expect("source queue must remain open")
            .into_legacy_event();
        envelopes.push(serde_json::from_str::<Value>(&event.raw_json).unwrap());
    }
    assert_eq!(envelopes[0]["connection_generation"], 1);
    assert_eq!(envelopes[1]["connection_generation"], 2);
    assert_eq!(
        envelopes[0]["source_sequence_epoch_id"],
        envelopes[1]["source_sequence_epoch_id"]
    );
    assert!(gaps.lock().unwrap().all_reasons().is_empty());

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_protocol_error_reconnects_in_process_without_gap() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = accept_async(first).await.unwrap();
        first.get_mut().write_all(&[0x83, 0x00]).await.unwrap();
        first.get_mut().flush().await.unwrap();
        let (second, _) = timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("a WebSocket protocol error must create a replacement connection")
            .unwrap();
        let mut second = accept_async(second).await.unwrap();
        second
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(4).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 5,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(5)),
        Instant::now(),
        control_rx,
    ));

    let event = timeout(Duration::from_secs(4), rx.recv())
        .await
        .expect("the source must recover after a WebSocket protocol error")
        .expect("source queue must remain open")
        .into_legacy_event();
    let envelope: Value = serde_json::from_str(&event.raw_json).unwrap();
    assert_eq!(envelope["connection_generation"], 2);
    assert!(gaps.lock().unwrap().all_reasons().is_empty());

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_one_hundred_reconnects_keep_one_source_loop_and_reach_generation_101() {
    if delegate_resource_test_to_isolated_process(
        "live_runtime::tests::binance_aggtrade_source::binance_one_hundred_reconnects_keep_one_source_loop_and_reach_generation_101",
    ) {
        return;
    }
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (final_release_tx, final_release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut fd_counts = Vec::with_capacity(101);
        for _ in 0..100 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            fd_counts.push(fs::read_dir("/proc/self/fd").unwrap().count());
            websocket.send(Message::Close(None)).await.unwrap();
        }
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        fd_counts.push(fs::read_dir("/proc/self/fd").unwrap().count());
        websocket
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
        let _ = final_release_rx.await;
        fd_counts
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(4).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 30,
            recovery_wait_override: Some(Duration::from_millis(1)),
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(10)),
        Instant::now(),
        control_rx,
    ));

    let event = timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("the same source loop must survive one hundred reconnects")
        .expect("source queue must remain open")
        .into_legacy_event();
    let envelope: Value = serde_json::from_str(&event.raw_json).unwrap();
    assert_eq!(envelope["connection_generation"], 101);
    assert_eq!(
        stats
            .drain_all_operational()
            .values()
            .find_map(|sources| sources.get("binance_aggtrade"))
            .unwrap()
            .reconnect_events,
        100
    );
    assert!(gaps.lock().unwrap().all_reasons().is_empty());
    assert!(!source.is_finished());

    let _ = final_release_tx.send(());
    source.abort();
    let _ = source.await;
    let fd_counts = server.await.unwrap();
    assert_eq!(fd_counts.len(), 101);
    let first = fd_counts[0];
    let last = *fd_counts.last().unwrap();
    let minimum = *fd_counts.iter().min().unwrap();
    let maximum = *fd_counts.iter().max().unwrap();
    assert!(
        last <= first + 2 && maximum <= minimum + 4,
        "connection FDs must remain bounded across reconnects: first={first}, last={last}, min={minimum}, max={maximum}"
    );
}

#[tokio::test]
async fn binance_ping_keeps_socket_alive_during_business_silence() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for _ in 0..25 {
            websocket
                .send(Message::Ping(Vec::<u8>::new().into()))
                .await
                .unwrap();
            sleep(Duration::from_millis(50)).await;
        }
        websocket.send(test_aggtrade_message()).await.unwrap();
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "business silence with valid Ping frames must not create a second connection"
        );
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(8).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 1,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(4)),
        Instant::now(),
        control_rx,
    ));

    let event = timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("aggTrade should arrive on the original Ping-kept socket")
        .expect("source queue should stay open")
        .into_legacy_event();
    assert_eq!(event.source_event_type, "aggTrade");
    assert_eq!(
        serde_json::from_str::<Value>(&event.raw_json).unwrap()["connection_generation"],
        1,
        "Ping-kept business silence must stay on the original socket"
    );

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_aggtrade_id_jump_is_forwarded_immediately_without_reconnect_or_gap() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for id in [100, 103] {
            websocket
                .send(test_aggtrade_message_for_id(id))
                .await
                .unwrap();
        }
        websocket
            .send(Message::Text("{not-json".into()))
            .await
            .unwrap();
        websocket
            .send(test_aggtrade_message_for_id(104))
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "an aggregate-trade ID jump must not create a second connection"
        );
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(8).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 5,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(5)),
        Instant::now(),
        control_rx,
    ));

    let mut events = Vec::new();
    while events.len() < 4 {
        let inflight = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("all jumped-ID rows must be forwarded immediately")
            .expect("source queue must remain open");
        if inflight.control.is_none() {
            events.push(inflight.into_legacy_event());
        }
    }
    let envelopes = events
        .iter()
        .map(|event| serde_json::from_str::<Value>(&event.raw_json).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["aggTrade", "aggTrade", "invalid_input", "aggTrade"]
    );
    assert_eq!(
        envelopes
            .iter()
            .filter(|value| value["record_kind"] == "trade")
            .map(|value| value["aggregate_trade_id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![100, 103, 104]
    );
    assert_eq!(
        envelopes
            .iter()
            .map(|value| value["websocket_ingress_sequence"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(
        envelopes
            .iter()
            .all(|value| value["connection_generation"] == 1)
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.collector_admission.as_ref().unwrap().sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(
        gaps.lock().unwrap().all_reasons().is_empty(),
        "aggregate-trade ID jumps are neutral observations, not confirmed gaps"
    );

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_aggtrade_payload_conflict_preserves_both_facts_and_records_conflict() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
        let mut conflicting = test_ws_aggtrade_value(100);
        conflicting["p"] = Value::String("99999.99".to_owned());
        websocket
            .send(Message::Text(conflicting.to_string().into()))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(4).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 5,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(4)),
        Instant::now(),
        control_rx,
    ));

    let mut envelopes = Vec::new();
    while envelopes.len() < 2 {
        let event = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("both conflicting payload facts must be forwarded")
            .expect("source queue must remain open")
            .into_legacy_event();
        envelopes.push(serde_json::from_str::<Value>(&event.raw_json).unwrap());
    }
    assert!(envelopes.iter().all(|value| {
        value["aggregate_trade_id"] == 100 && value["connection_generation"] == 1
    }));
    assert_eq!(
        envelopes
            .iter()
            .map(|value| value["websocket_ingress_sequence"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_ne!(envelopes[0]["price"], envelopes[1]["price"]);
    assert!(
        gaps.lock()
            .unwrap()
            .all_reasons()
            .iter()
            .any(|reason| reason == "binance_aggtrade_payload_conflict:id=100")
    );

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_aggtrade_invalid_text_frames_are_preserved_and_same_socket_continues() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let mut wrong_symbol = test_ws_aggtrade_value(901);
    wrong_symbol["s"] = Value::String("ETHUSDT".to_owned());
    let wrapped_payload = test_ws_aggtrade_value(902);
    let invalid_frames = vec![
        "{not-json".to_owned(),
        "{not-json".to_owned(),
        json!({"s": "BTCUSDT", "a": 1_u64}).to_string(),
        json!({"e": "depthUpdate", "s": "BTCUSDT"}).to_string(),
        wrong_symbol.to_string(),
        json!({"data": wrapped_payload}).to_string(),
        json!({"stream": 7, "data": test_ws_aggtrade_value(903)}).to_string(),
        json!({
            "stream": "ethusdt@aggTrade",
            "data": test_ws_aggtrade_value(904)
        })
        .to_string(),
    ];
    let expected_retained_rows = invalid_frames.len() + 2;
    let server_frames = invalid_frames.clone();
    let (frames_sent_tx, frames_sent_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
        for frame in server_frames {
            websocket.send(Message::Text(frame.into())).await.unwrap();
            sleep(Duration::from_millis(150)).await;
        }
        websocket
            .send(test_aggtrade_message_for_id(100))
            .await
            .unwrap();
        websocket
            .send(test_aggtrade_message_for_id(101))
            .await
            .unwrap();
        frames_sent_tx.send(()).unwrap();
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "invalid application frames must rearm inactivity on the same socket"
        );
    });
    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(16).unwrap();
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 1,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(8)),
        Instant::now(),
        control_rx,
    ));

    frames_sent_rx.await.unwrap();
    timeout(Duration::from_secs(1), async {
        while stats.aggtrade_admission_sequence.load(Ordering::Relaxed)
            < u64::try_from(expected_retained_rows).unwrap()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source must transfer all retained rows without a writer acknowledgement");

    let mut events = Vec::new();
    while events.len() < expected_retained_rows {
        let inflight = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("invalid and following valid frames must reach the source queue")
            .expect("source queue must remain open");
        if inflight.control.is_none() {
            events.push(inflight.into_legacy_event());
        }
    }
    assert_eq!(events[0].source_event_type, "aggTrade");
    let first_trade: Value = serde_json::from_str(&events[0].raw_json).unwrap();
    assert_eq!(first_trade["websocket_ingress_sequence"], 1);
    assert_eq!(first_trade["connection_generation"], 1);
    assert_eq!(events[0].collector_admission.as_ref().unwrap().sequence, 1);
    for (index, expected_frame) in invalid_frames.iter().enumerate() {
        let event = &events[index + 1];
        assert_eq!(event.source_event_type, "invalid_input");
        assert_eq!(
            event.collector_admission.as_ref().unwrap().sequence,
            u64::try_from(index + 2).unwrap()
        );
        let envelope: Value = serde_json::from_str(&event.raw_json).unwrap();
        assert_eq!(envelope["connection_generation"], 1);
        assert_eq!(
            envelope["websocket_ingress_sequence"].as_u64().unwrap(),
            u64::try_from(index + 2).unwrap()
        );
        let evidence: Value =
            serde_json::from_str(envelope["source_native_payload_json"].as_str().unwrap()).unwrap();
        assert_eq!(evidence["payload_capture_status"], "complete");
        assert_eq!(
            evidence["payload_base64"],
            base64::engine::general_purpose::STANDARD.encode(expected_frame.as_bytes())
        );
    }
    let final_event = events.last().unwrap();
    assert_eq!(final_event.source_event_type, "aggTrade");
    let second_trade: Value = serde_json::from_str(&final_event.raw_json).unwrap();
    assert_eq!(second_trade["connection_generation"], 1);
    assert_eq!(
        second_trade["websocket_ingress_sequence"],
        u64::try_from(invalid_frames.len() + 3).unwrap()
    );
    assert_eq!(
        final_event.collector_admission.as_ref().unwrap().sequence,
        u64::try_from(expected_retained_rows).unwrap()
    );
    let operational = stats.drain_all_operational();
    let aggtrade = operational
        .values()
        .find_map(|sources| sources.get("binance_aggtrade"))
        .unwrap();
    assert_eq!(aggtrade.structural_dedupe_dropped_events, 1);
    assert!(
        !source.is_finished(),
        "invalid frames must not terminate the source"
    );
    let rows = events
        .iter()
        .map(binance_aggtrade_row_from_event)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let writer_dir = dataset_root_path.join("writer-recovery");
    fs::create_dir_all(&writer_dir).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &writer_dir,
        "binance_aggtrade_00",
        "binance_aggtrade_v6",
        "binance_aggtrade",
    )
    .unwrap();
    writer.write_binance_aggtrade_rows(&rows).unwrap();
    assert_eq!(
        writer.finalize().unwrap().row_count,
        u64::try_from(expected_retained_rows).unwrap(),
        "all rows accepted while the writer was paused must remain writable after recovery"
    );

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}

#[tokio::test]
async fn binance_aggtrade_binary_is_exact_and_oversize_is_bounded_before_reconnect() {
    let dataset_root = tempdir().unwrap();
    let dataset_root_path = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let binary_payload = vec![0_u8, 1, 2, 255];
    let expected_binary = binary_payload.clone();
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let mut first = accept_async(first).await.unwrap();
        first
            .send(Message::Binary(binary_payload.into()))
            .await
            .unwrap();
        first.send(test_aggtrade_message_for_id(100)).await.unwrap();
        first.send(Message::Close(None)).await.unwrap();

        let (second, _) = listener.accept().await.unwrap();
        let mut second = accept_async(second).await.unwrap();
        let oversize = vec![7_u8; BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES + 1];
        let _ = second.send(Message::Binary(oversize.into())).await;

        let (third, _) = listener.accept().await.unwrap();
        let mut third = accept_async(third).await.unwrap();
        third.send(test_aggtrade_message_for_id(101)).await.unwrap();
        sleep(Duration::from_millis(200)).await;
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(16).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (_control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_ws_source(
        restore_binance_aggtrade_recovery(
            &dataset_root_path,
            &utc_day_string_from_ms(now_ms()).unwrap(),
            &test_daily_manifest_identity(),
        )
        .unwrap(),
        BinanceWsSpec {
            source_kind: "binance_aggtrade".to_owned(),
            default_event_type: "aggTrade".to_owned(),
            url,
            frame_inactivity_timeout_secs: 5,
            recovery_wait_override: None,
        },
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(8)),
        Instant::now(),
        control_rx,
    ));

    let mut events = Vec::new();
    while events.len() < 4 {
        let inflight = timeout(Duration::from_secs(6), rx.recv())
            .await
            .expect("binary, oversize and following trades must reach ingress")
            .expect("source queue must remain open");
        if inflight.control.is_none() {
            events.push(inflight.into_legacy_event());
        }
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["invalid_input", "aggTrade", "invalid_input", "aggTrade"]
    );

    let binary_envelope: Value = serde_json::from_str(&events[0].raw_json).unwrap();
    let binary_evidence: Value = serde_json::from_str(
        binary_envelope["source_native_payload_json"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(binary_evidence["payload_capture_status"], "complete");
    assert_eq!(
        binary_evidence["payload_base64"],
        base64::engine::general_purpose::STANDARD.encode(expected_binary)
    );
    assert_eq!(binary_envelope["websocket_ingress_sequence"], 1);
    assert_eq!(binary_envelope["connection_generation"], 1);
    let first_trade: Value = serde_json::from_str(&events[1].raw_json).unwrap();
    assert_eq!(first_trade["connection_generation"], 1);

    let oversize_envelope: Value = serde_json::from_str(&events[2].raw_json).unwrap();
    let oversize_evidence: Value = serde_json::from_str(
        oversize_envelope["source_native_payload_json"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        oversize_evidence["payload_capture_status"],
        "oversize_not_captured"
    );
    assert!(oversize_evidence["payload_base64"].is_null());
    assert!(oversize_evidence["payload_sha256"].is_null());
    assert_eq!(
        oversize_evidence["capture_limit_bytes"],
        u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES).unwrap()
    );
    assert!(
        oversize_evidence["observed_bytes_at_least"]
            .as_u64()
            .unwrap()
            > u64::try_from(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES).unwrap()
    );
    assert_eq!(oversize_envelope["websocket_ingress_sequence"], 3);
    assert_eq!(oversize_envelope["connection_generation"], 2);
    let following_trade: Value = serde_json::from_str(&events[3].raw_json).unwrap();
    assert_eq!(following_trade["websocket_ingress_sequence"], 4);
    assert_eq!(following_trade["connection_generation"], 3);

    source.abort();
    let _ = source.await;
    server.await.unwrap();
}
