use super::*;

#[tokio::test]
async fn binance_depth_reconnect_is_transport_only_and_preserves_snapshot_trigger() {
    let (tx, _rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut recovery = SourceRecoverySupervisor::new("binance_depth_update", 1);
    let mut snapshot_trigger_reason = "bootstrap".to_owned();

    assert!(
        prepare_binance_depth_reconnect(
            &sink,
            &mut recovery,
            &mut snapshot_trigger_reason,
            "stream_closed".to_owned(),
            None,
        )
        .await
    );

    assert_eq!(recovery.inner_attempts, 1);
    assert_eq!(snapshot_trigger_reason, "reconnect:stream_closed");
    assert!(gaps.lock().unwrap().is_empty());
}

fn test_binance_depth_context(
    http_client: reqwest::Client,
    sink: SourceEventSink,
    websocket_endpoint: String,
    snapshot_endpoint: String,
    deadline: Option<Instant>,
    trigger_reason: &str,
) -> BinanceDepthUpdateContext {
    BinanceDepthUpdateContext {
        http_client,
        sink,
        collector_identity: CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        deadline,
        run_started: Instant::now(),
        spec: BinanceDepthSpec {
            websocket_endpoint,
            snapshot_endpoint,
            frame_inactivity_timeout: Duration::from_secs(30),
            recovery_wait_override: None,
        },
        source_sequence_epoch_id: "depth-source-epoch-test".to_owned(),
        connection_generation: 1,
        ingress_sequence: DepthIngressSequenceAllocator::default(),
        snapshot_not_before: DepthSnapshotNotBefore::default(),
        initial_snapshot_trigger_reason: trigger_reason.to_owned(),
        pending_limits: None,
    }
}

fn test_binance_depth_message(first_update_id: u64, final_update_id: u64) -> Message {
    Message::Text(
        json!({
            "e": "depthUpdate",
            "E": now_ms(),
            "s": "BTCUSDT",
            "U": first_update_id,
            "u": final_update_id,
            "b": [["1", "2"]],
            "a": [["3", "4"]]
        })
        .to_string()
        .into(),
    )
}

#[tokio::test]
async fn binance_depth_bootstrap_connect_failure_uses_retry_without_gap_or_lag() {
    let (tx, _rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut recovery = SourceRecoverySupervisor::new("binance_depth_update", 1);
    let mut snapshot_trigger_reason = "bootstrap".to_owned();

    assert!(
        handle_binance_depth_ws_connect_failure(
            &sink,
            &mut recovery,
            &mut snapshot_trigger_reason,
            "connect_timeout".to_owned(),
            None,
        )
        .await
    );

    assert_eq!(recovery.inner_attempts, 1);
    assert_eq!(snapshot_trigger_reason, "reconnect:connect_timeout");
    assert!(gaps.lock().unwrap().is_empty());
    assert!(stats.drain_all_operational().values().all(|sources| {
        sources
            .get("binance_depth_update")
            .is_none_or(|counter| counter.lag_events == 0)
    }));
}

#[tokio::test]
async fn binance_depth_disconnected_control_is_terminal_before_transport_retry() {
    let (tx, _rx) = mpsc::channel(1);
    let (control_tx, control_rx) = mpsc::channel(1);
    drop(control_tx);
    let error = run_binance_depth_source_with_spec_and_control(
        BinanceDepthSpec {
            websocket_endpoint: "ws://127.0.0.1:9".to_owned(),
            snapshot_endpoint: "http://127.0.0.1:9/api/v3/depth".to_owned(),
            frame_inactivity_timeout: Duration::from_secs(1),
            recovery_wait_override: Some(Duration::from_millis(1)),
        },
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Some(Instant::now() + Duration::from_secs(1)),
        Instant::now(),
        Some(control_rx),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("depth source control channel closed"),
        "{error}"
    );
}

#[tokio::test]
async fn binance_depth_snapshot_keeps_real_fetch_start_and_full_response_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        sleep(Duration::from_millis(30)).await;
        socket.write_all(body.as_bytes()).await.unwrap();
        now_ms()
    });
    let (tx, mut rx) = mpsc::channel(2);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        sink.clone(),
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    let record = match fetch_binance_depth_snapshot(&context, "bootstrap")
        .await
        .unwrap()
    {
        DepthSnapshotCycleOutcome::Available(record) => record,
        DepthSnapshotCycleOutcome::Unavailable { reason, .. } => {
            panic!("snapshot unexpectedly unavailable: {reason}")
        }
    };
    assert_eq!(record.snapshot_last_update_id, 100);
    send_event(&sink, Instant::now(), record.event)
        .await
        .unwrap();
    let event = rx.recv().await.unwrap().into_event();
    let raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    let fetch_start = raw["snapshot_fetch_start_utc"].as_i64().unwrap();
    assert!(fetch_start < event.recv_time_utc_ms);
    let row = binance_depth_snapshot_row_from_event(&event).unwrap();
    assert_eq!(row.snapshot_fetch_start_utc, fetch_start);
    assert_eq!(row.timestamp_received, event.recv_time_utc_ms);
    let body_completed_utc_ms = server.await.unwrap();
    assert!(event.recv_time_utc_ms >= body_completed_utc_ms);
}

#[tokio::test]
async fn binance_depth_runtime_bootstraps_bridges_resnapshots_and_preserves_order() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (resnapshot_ready_tx, resnapshot_ready_rx) = tokio::sync::oneshot::channel();

    let http_server = tokio::spawn(async move {
        for (index, last_update_id) in [100_u64, 104_u64].into_iter().enumerate() {
            let (mut socket, _) = http_listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let body = json!({
                "lastUpdateId": last_update_id,
                "bids": [["1", "2"]],
                "asks": [["3", "4"]]
            })
            .to_string();
            if index == 1 {
                sleep(Duration::from_millis(30)).await;
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            if index == 1 {
                resnapshot_ready_tx.send(()).unwrap();
                break;
            }
        }
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for (first, last) in [(101_u64, 102_u64), (103, 103), (100, 100), (105, 105)] {
            websocket
                .send(Message::Text(
                    json!({
                        "e": "depthUpdate",
                        "E": now_ms(),
                        "s": "BTCUSDT",
                        "U": first,
                        "u": last,
                        "b": [["1", "2"]],
                        "a": [["3", "4"]]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        resnapshot_ready_rx.await.unwrap();
        websocket
            .send(Message::Text(
                json!({
                    "e": "depthUpdate",
                    "E": now_ms(),
                    "s": "BTCUSDT",
                    "U": 105_u64,
                    "u": 106_u64,
                    "b": [["1", "2"]],
                    "a": [["3", "4"]]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    });

    let (tx, mut rx) = mpsc::channel(16);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    run_binance_depth_source_with_spec(
        BinanceDepthSpec {
            websocket_endpoint,
            snapshot_endpoint,
            frame_inactivity_timeout: Duration::from_secs(30),
            recovery_wait_override: None,
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
        Some(Instant::now() + Duration::from_millis(600)),
        Instant::now(),
    )
    .await
    .unwrap();

    let mut events = Vec::new();
    while let Ok(timed) = rx.try_recv() {
        events.push(timed.into_event());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "depth_snapshot",
            "depth_update",
            "depth_update",
            "depth_update",
            "depth_update",
            "depth_snapshot",
            "depth_update",
        ]
    );
    let rows = events
        .iter()
        .filter(|event| event.source_event_type == "depth_update")
        .map(|event| serde_json::from_str::<Value>(&event.raw_json).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows[0]["u"], 102);
    assert_eq!(rows[1]["u"], 103);
    assert_eq!(rows[2]["u"], 100);
    assert_eq!(
        rows[2]["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_STALE_DUPLICATE
    );
    assert_eq!(rows[3]["u"], 105);
    assert_eq!(
        rows[3]["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE
    );
    assert_eq!(rows[3]["snapshot_last_update_id"], 100);
    assert_eq!(rows[4]["u"], 106);
    let triggering_update_recv_time = events[4].recv_time_utc_ms;
    let resnapshot_recv_time = events[5].recv_time_utc_ms;
    assert!(
        triggering_update_recv_time < resnapshot_recv_time,
        "the triggering frame must retain its pre-resnapshot receive time"
    );
    assert_eq!(rows[4]["snapshot_last_update_id"], 104);
    assert_eq!(rows[4]["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| gap == "binance_depth_update_stitch_gap:U > local_u + 1")
    );
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|sources| {
        sources
            .get("binance_depth_update")
            .is_none_or(|counter| counter.structural_dedupe_dropped_events == 0)
    }));
    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_utc_day_fence_cancels_snapshot_and_neutral_releases_buffered_updates() {
    const UTC_DAY_TEST_MARGIN_MS: i64 = 5_000;

    let mut fixture_now_utc_ms = now_ms();
    let mut following_boundary_utc_ms = fixture_now_utc_ms
        .div_euclid(UTC_DAY_MS)
        .saturating_add(1)
        .saturating_mul(UTC_DAY_MS);
    let remaining_utc_day_ms = following_boundary_utc_ms.saturating_sub(fixture_now_utc_ms);
    if remaining_utc_day_ms <= UTC_DAY_TEST_MARGIN_MS {
        sleep(Duration::from_millis(
            u64::try_from(remaining_utc_day_ms.saturating_add(10)).unwrap(),
        ))
        .await;
        fixture_now_utc_ms = now_ms();
        following_boundary_utc_ms = fixture_now_utc_ms
            .div_euclid(UTC_DAY_MS)
            .saturating_add(1)
            .saturating_mul(UTC_DAY_MS);
    }
    assert!(following_boundary_utc_ms.saturating_sub(fixture_now_utc_ms) > UTC_DAY_TEST_MARGIN_MS);
    let boundary_utc_ms = fixture_now_utc_ms
        .div_euclid(UTC_DAY_MS)
        .saturating_mul(UTC_DAY_MS);
    let old_utc_day = utc_day_string_from_ms(boundary_utc_ms.saturating_sub(1)).unwrap();

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (request_started_tx, mut request_started_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let _ = socket.read(&mut request).await.unwrap();
        let _ = request_started_tx.send(());
        sleep(Duration::from_millis(30)).await;
        let body = json!({
            "lastUpdateId": 100_u64,
            "bids": [["1", "2"]],
            "asks": [["3", "4"]]
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(Message::Text(
                json!({
                    "e": "depthUpdate",
                    "E": now_ms(),
                    "s": "BTCUSDT",
                    "U": 101_u64,
                    "u": 101_u64,
                    "b": [["1", "2"]],
                    "a": [["3", "4"]]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        websocket
            .send(Message::Text(
                json!({
                    "e": "depthUpdate",
                    "E": now_ms(),
                    "s": "BTCUSDT",
                    "U": 102_u64,
                    "u": 102_u64,
                    "b": [["1", "2"]],
                    "a": [["3", "4"]]
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
    });
    let (tx, mut rx) = mpsc::channel(8);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate
        .activate_startup(old_utc_day.clone(), INITIAL_RAW_DAY_GENERATION)
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
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let (control_tx, mut control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let depth_source = run_binance_depth_update_ws_source_with_control(
        stream,
        test_binance_depth_context(
            reqwest::Client::new(),
            sink.clone(),
            websocket_endpoint,
            snapshot_endpoint,
            Some(Instant::now() + Duration::from_millis(100)),
            "utc_day_boundary",
        ),
        Some(&mut control_rx),
    );
    let source = async { depth_source.await.map(|_| ()) };
    tokio::pin!(source);
    tokio::select! {
        result = &mut source => panic!("depth source ended before resnapshot started: {result:?}"),
        result = &mut request_started_rx => result.unwrap(),
    }
    let close = prepare_rollover_for_producer(
        &raw_day_gate,
        "binance_depth",
        &old_utc_day,
        boundary_utc_ms,
    );
    let consumer_gate = Arc::clone(&raw_day_gate);
    let consumer_close = close.clone();
    let consumer = tokio::spawn(async move {
        // This time-shifts the real frame-vs-boundary race: new-day rows are
        // deferred until the same authoritative gate commits activation.
        let mut barrier = rx.recv().await.unwrap();
        let Some(IngressControl::DayBarrier { reply, .. }) = barrier.control.take() else {
            panic!("expected depth day barrier before deferred new-day rows");
        };
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        commit_test_manifest_activation(&consumer_gate, &consumer_close);
        consumer_gate
            .activate_after_rollover(&consumer_close)
            .await
            .unwrap();
        reply.send(Ok(DayBarrierRelease::Resume)).unwrap();
        let first_update = rx.recv().await.unwrap();
        let second_update = rx.recv().await.unwrap();
        (barrier, first_update, second_update)
    });
    assert!(
        fence_depth_and_send_day_barrier(
            source.as_mut(),
            &control_tx,
            &sink,
            Instant::now(),
            &raw_day_gate,
            &close,
        )
        .await
        .unwrap()
        .is_none()
    );

    timeout(Duration::from_secs(1), source.as_mut())
        .await
        .expect("depth source must emit the post-barrier event")
        .unwrap();
    let (barrier, first_update, second_update) = consumer.await.unwrap();
    let expected_permit = RawDayPermit {
        utc_day: close.next_utc_day.clone(),
        generation: close.next_generation,
    };
    assert!(
        [&first_update, &second_update]
            .into_iter()
            .all(|event| event.raw_day_permit.as_ref() == Some(&expected_permit))
    );
    let first_update = first_update.into_event();
    let second_update = second_update.into_event();
    assert_eq!(first_update.source_event_type, "depth_update");
    assert_eq!(second_update.source_event_type, "depth_update");
    let first_raw = serde_json::from_str::<Value>(&first_update.raw_json).unwrap();
    let second_raw = serde_json::from_str::<Value>(&second_update.raw_json).unwrap();
    assert!(
        first_raw["depth_ingress_sequence"].as_u64().unwrap()
            < second_raw["depth_ingress_sequence"].as_u64().unwrap()
    );
    assert_eq!(
        first_raw["source_sequence_epoch_id"],
        second_raw["source_sequence_epoch_id"]
    );
    assert_eq!(
        first_raw["connection_generation"],
        second_raw["connection_generation"]
    );
    let first_admission = first_update.collector_admission.as_ref().unwrap();
    let second_admission = second_update.collector_admission.as_ref().unwrap();
    assert!(first_admission.sequence < second_admission.sequence);
    assert!(first_admission.time_utc_ms > 0);
    assert!(second_admission.time_utc_ms > 0);
    assert_eq!(first_raw["u"], 101);
    assert_eq!(second_raw["u"], 102);
    assert!(first_raw["snapshot_last_update_id"].is_null());
    assert!(second_raw["snapshot_last_update_id"].is_null());
    assert_eq!(
        first_raw["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert_eq!(
        second_raw["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert!(barrier.control.is_none());
    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_schema_error_is_process_fatal() {
    for (terminal, expected_error) in [
        (
            Message::Text("{not-json".into()),
            "binance_depth_update_parse_error:parse_depth_update_json",
        ),
        (
            Message::Binary(vec![0_u8, 1_u8].into()),
            "binance_depth_update_binary_payload_integrity_fatal",
        ),
        (
            test_binance_depth_message(92, 91),
            "binance_depth_update_contract_error:u",
        ),
        (
            Message::Text(
                json!({
                    "e": "depthUpdate", "E": now_ms(), "s": "BTCUSDT",
                    "U": 91, "u": 91, "b": [], "a": []
                })
                .to_string()
                .into(),
            ),
            "binance_depth_update_contract_error:b_or_a",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(test_binance_depth_message(90, 90))
                .await
                .unwrap();
            websocket.send(terminal).await.unwrap();
            sleep(Duration::from_millis(100)).await;
        });
        let (tx, mut rx) = mpsc::channel(8);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, now_ms()).await;
        let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
        let error = run_binance_depth_update_ws_source_with_control(
            stream,
            test_binance_depth_context(
                reqwest::Client::new(),
                SourceEventSink {
                    tx,
                    gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                    source_queue_backpressure: stats,
                    inflight_budget: InflightBudget::new(),
                    enqueue_lock: Arc::new(AsyncMutex::new(())),
                },
                websocket_endpoint,
                "http://127.0.0.1:9/api/v3/depth".to_owned(),
                Some(Instant::now() + Duration::from_secs(1)),
                "bootstrap",
            ),
            None,
        )
        .await
        .expect_err("depth parser/schema errors must terminate the source");
        assert!(error.to_string().contains(expected_error), "{error}");
        let released = rx.recv().await.unwrap().into_event();
        let raw = serde_json::from_str::<Value>(&released.raw_json).unwrap();
        assert_eq!(raw["u"], 90);
        assert_eq!(
            raw["stitch_verdict"],
            BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
        );
        assert!(raw["snapshot_last_update_id"].is_null());
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected)
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn binance_depth_server_shutdown_is_a_transport_event_for_raw_and_combined_frames() {
    for frame in [
        json!({"e": "serverShutdown", "E": now_ms()}),
        json!({
            "stream": "!serverShutdown",
            "data": {"e": "serverShutdown", "E": now_ms()}
        }),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text(frame.to_string().into()))
                .await
                .unwrap();
            sleep(Duration::from_millis(50)).await;
        });
        let (tx, _rx) = mpsc::channel(2);
        let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
        let outcome = run_binance_depth_update_ws_source_with_control(
            stream,
            test_binance_depth_context(
                reqwest::Client::new(),
                SourceEventSink {
                    tx,
                    gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                    source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                    inflight_budget: InflightBudget::new(),
                    enqueue_lock: Arc::new(AsyncMutex::new(())),
                },
                websocket_endpoint,
                "http://127.0.0.1:9/api/v3/depth".to_owned(),
                Some(Instant::now() + Duration::from_secs(1)),
                "bootstrap",
            ),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            BinanceDepthUpdateExit::StreamEnded {
                ref reason,
                ..
            } if reason == "server_shutdown"
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn binance_depth_snapshot_retries_408_then_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for status in ["408 Request Timeout", "200 OK"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let body = if status == "200 OK" {
                r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#
            } else {
                r#"{}"#
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let (tx, _rx) = mpsc::channel(2);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "stitch_gap:test",
    );
    let result = fetch_binance_depth_snapshot(&context, "stitch_gap:test")
        .await
        .unwrap();
    let DepthSnapshotCycleOutcome::Available(record) = result else {
        panic!("snapshot must recover after 408")
    };
    assert_eq!(record.snapshot_last_update_id, 100);
    let raw = serde_json::from_str::<Value>(&record.event.raw_json).unwrap();
    assert_eq!(raw["depth_ingress_sequence"], 2);
    assert!(gaps.lock().unwrap().is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_snapshot_honors_retry_after_before_next_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = first.read(&mut request).await.unwrap();
        first
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let (early_retry, (mut second, _)) =
            match timeout(Duration::from_millis(800), listener.accept()).await {
                Ok(connection) => (true, connection.unwrap()),
                Err(_) => (false, listener.accept().await.unwrap()),
            };
        let _ = second.read(&mut request).await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        second.write_all(response.as_bytes()).await.unwrap();
        early_retry
    });
    let (tx, _rx) = mpsc::channel(2);
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    assert!(matches!(
        fetch_binance_depth_snapshot(&context, "bootstrap")
            .await
            .unwrap(),
        DepthSnapshotCycleOutcome::Available(_)
    ));
    assert!(
        !server.await.unwrap(),
        "Retry-After must be the minimum delay before the next request"
    );
}

#[tokio::test]
async fn binance_depth_retry_after_survives_snapshot_future_replacement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let (limited_tx, limited_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = first.read(&mut request).await.unwrap();
        first
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let _ = limited_tx.send(());
        let (early_retry, (mut second, _)) =
            match timeout(Duration::from_millis(800), listener.accept()).await {
                Ok(connection) => (true, connection.unwrap()),
                Err(_) => (false, listener.accept().await.unwrap()),
            };
        let _ = second.read(&mut request).await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        second.write_all(response.as_bytes()).await.unwrap();
        early_retry
    });
    let (tx, _rx) = mpsc::channel(2);
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    let first_cycle = tokio::spawn(start_binance_depth_snapshot_cycle(&context, "bootstrap"));
    limited_rx.await.unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            let gate_is_set = context
                .snapshot_not_before
                .deadline
                .lock()
                .unwrap()
                .is_some_and(|(started, duration)| started.elapsed() < duration);
            if gate_is_set {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Retry-After gate must be installed after the 429 response");
    first_cycle.abort();
    let join_error = match first_cycle.await {
        Err(error) => error,
        Ok(_) => panic!("cancelled snapshot future unexpectedly completed"),
    };
    assert!(join_error.is_cancelled());

    assert!(matches!(
        start_binance_depth_snapshot_cycle(&context, "utc_day_boundary")
            .await
            .unwrap(),
        DepthSnapshotCycleOutcome::Available(_)
    ));
    assert!(
        !server.await.unwrap(),
        "replacing a snapshot future must not clear the source-task Retry-After gate"
    );
}

#[tokio::test]
async fn binance_depth_snapshot_non_rate_limit_4xx_is_terminal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        timeout(Duration::from_millis(400), listener.accept())
            .await
            .is_ok()
    });
    let (tx, _rx) = mpsc::channel(1);
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    let error = match fetch_binance_depth_snapshot(&context, "bootstrap").await {
        Err(error) => error,
        Ok(_) => panic!("a deterministic request 4xx must be terminal"),
    };
    assert!(
        error
            .to_string()
            .contains("binance_depth_snapshot_integrity_fatal:http_status_terminal:404"),
        "{error}"
    );
    assert!(!server.await.unwrap(), "terminal 4xx must not be retried");
}

#[tokio::test]
async fn binance_depth_snapshot_timeout_exhaustion_is_availability() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            sleep(Duration::from_millis(50)).await;
        }
    });
    let (tx, _rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_millis(10))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    let result = fetch_binance_depth_snapshot(&context, "bootstrap")
        .await
        .unwrap();
    assert!(matches!(
        result,
        DepthSnapshotCycleOutcome::Unavailable { .. }
    ));
    assert!(gaps.lock().unwrap().is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_snapshot_success_with_invalid_schema_is_terminal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":"invalid","asks":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let (tx, _rx) = mpsc::channel(1);
    let context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );

    let error = match fetch_binance_depth_snapshot(&context, "bootstrap").await {
        Err(error) => error,
        Ok(_) => panic!("a successful HTTP response with an invalid schema must fail closed"),
    };
    assert!(
        error
            .to_string()
            .contains("binance_depth_snapshot_schema_fatal:schema:"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_invalid_snapshot_releases_valid_pending_update_before_terminal() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(90, 90))
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    });
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        sleep(Duration::from_millis(100)).await;
        let body = r#"{"lastUpdateId":100,"bids":[],"asks":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    let (tx, mut rx) = mpsc::channel(8);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let error = run_binance_depth_update_ws_source_with_control(
        stream,
        test_binance_depth_context(
            reqwest::Client::new(),
            SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: stats,
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            websocket_endpoint,
            snapshot_endpoint,
            Some(Instant::now() + Duration::from_secs(2)),
            "bootstrap",
        ),
        None,
    )
    .await
    .expect_err("an invalid successful snapshot response must remain terminal");
    assert!(
        error
            .to_string()
            .contains("binance_depth_snapshot_schema_fatal:schema:invalid_snapshot_contract"),
        "{error}"
    );
    let released = rx.recv().await.unwrap().into_event();
    let raw = serde_json::from_str::<Value>(&released.raw_json).unwrap();
    assert_eq!(raw["u"], 90);
    assert_eq!(
        raw["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected)
    ));
    http_server.await.unwrap();
    ws_server.abort();
    let _ = ws_server.await;
}

#[tokio::test]
async fn binance_depth_chunked_snapshot_body_is_bounded_before_json_parse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/v3/depth", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let chunk = vec![b'x'; 64 * 1024];
        let mut sent = 0_usize;
        while sent <= MAX_TYPED_ROW_BYTES {
            let remaining = MAX_TYPED_ROW_BYTES.saturating_add(1).saturating_sub(sent);
            let length = remaining.min(chunk.len());
            let header = format!("{length:X}\r\n");
            if socket.write_all(header.as_bytes()).await.is_err()
                || socket.write_all(&chunk[..length]).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                break;
            }
            sent = sent.saturating_add(length);
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    let (tx, _rx) = mpsc::channel(1);
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        endpoint,
        None,
        "bootstrap",
    );
    let error = match fetch_binance_depth_snapshot(&context, "bootstrap").await {
        Err(error) => error,
        Ok(_) => panic!("a chunked snapshot body above the frozen cap must fail closed"),
    };
    assert!(
        error
            .to_string()
            .contains("response_body_too_large:limit=1048576"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_snapshot_exhaustion_recovers_on_the_same_websocket_generation() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (snapshot_available_tx, snapshot_available_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let mut snapshot_available_tx = Some(snapshot_available_tx);
        for attempt in 0..4 {
            let (mut socket, _) = http_listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            if attempt == 0 {
                sleep(Duration::from_millis(50)).await;
            }
            let (status, body) = if attempt < 3 {
                ("429 Too Many Requests", "{}")
            } else {
                (
                    "200 OK",
                    r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#,
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            if attempt == 3 {
                let _ = snapshot_available_tx.take().unwrap().send(());
            }
        }
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(90, 90))
            .await
            .unwrap();
        snapshot_available_rx.await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        let _ = websocket.send(Message::Close(None)).await;
    });

    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let mut context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        sink,
        websocket_endpoint,
        snapshot_endpoint,
        Some(Instant::now() + Duration::from_secs(4)),
        "bootstrap",
    );
    context.spec.frame_inactivity_timeout = Duration::from_secs(3);
    let outcome = run_binance_depth_update_ws_source_with_control(stream, context, None)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason.starts_with("ws_closed:")
    ));

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event.into_event());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["depth_update", "depth_snapshot", "depth_update"]
    );
    let first = serde_json::from_str::<Value>(&events[0].raw_json).unwrap();
    let snapshot = serde_json::from_str::<Value>(&events[1].raw_json).unwrap();
    let second = serde_json::from_str::<Value>(&events[2].raw_json).unwrap();
    assert_eq!(
        first["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert!(first["snapshot_last_update_id"].is_null());
    assert_eq!(second["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert_eq!(second["snapshot_last_update_id"], 100);
    assert!(
        first["depth_ingress_sequence"].as_u64().unwrap()
            < snapshot["depth_ingress_sequence"].as_u64().unwrap()
    );
    assert!(
        events[0].collector_admission.as_ref().unwrap().sequence
            < events[1].collector_admission.as_ref().unwrap().sequence
    );
    assert!(events.iter().all(|event| {
        serde_json::from_str::<Value>(&event.raw_json).unwrap()["connection_generation"] == 1
    }));
    assert!(gaps.lock().unwrap().is_empty());
    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_snapshot_admits_before_two_earlier_ingress_updates() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (updates_sent_tx, updates_sent_rx) = oneshot::channel();
    let (snapshot_sent_tx, snapshot_sent_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        updates_sent_rx.await.unwrap();
        sleep(Duration::from_millis(50)).await;
        socket.write_all(body.as_bytes()).await.unwrap();
        let _ = snapshot_sent_tx.send(());
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        websocket
            .send(test_binance_depth_message(102, 102))
            .await
            .unwrap();
        let _ = updates_sent_tx.send(());
        snapshot_sent_rx.await.unwrap();
        sleep(Duration::from_millis(50)).await;
        let _ = websocket.send(Message::Close(None)).await;
    });

    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        snapshot_endpoint,
        Some(Instant::now() + Duration::from_secs(2)),
        "bootstrap",
    );
    let outcome = run_binance_depth_update_ws_source_with_control(stream, context, None)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason.starts_with("ws_closed:")
    ));

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event.into_event());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["depth_snapshot", "depth_update", "depth_update"]
    );
    let snapshot = serde_json::from_str::<Value>(&events[0].raw_json).unwrap();
    let first = serde_json::from_str::<Value>(&events[1].raw_json).unwrap();
    let second = serde_json::from_str::<Value>(&events[2].raw_json).unwrap();
    assert!(events[1].recv_time_utc_ms < events[0].recv_time_utc_ms);
    assert!(events[2].recv_time_utc_ms < events[0].recv_time_utc_ms);
    assert!(
        first["depth_ingress_sequence"].as_u64().unwrap()
            < second["depth_ingress_sequence"].as_u64().unwrap()
    );
    assert!(
        second["depth_ingress_sequence"].as_u64().unwrap()
            < snapshot["depth_ingress_sequence"].as_u64().unwrap()
    );
    assert!(events.windows(2).all(|pair| {
        pair[0].collector_admission.as_ref().unwrap().sequence
            < pair[1].collector_admission.as_ref().unwrap().sequence
    }));
    assert_eq!(first["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert_eq!(second["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert_eq!(
        first["source_sequence_epoch_id"],
        snapshot["source_sequence_epoch_id"]
    );
    assert_eq!(
        second["connection_generation"],
        snapshot["connection_generation"]
    );
    assert!(gaps.lock().unwrap().is_empty());
    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_hour_fence_preserves_snapshot_and_ready_continuity() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (snapshot_release_tx, snapshot_release_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        snapshot_release_rx.await.unwrap();
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        timeout(Duration::from_millis(500), http_listener.accept())
            .await
            .is_ok()
    });
    let (first_update_sent_tx, first_update_sent_rx) = oneshot::channel();
    let (second_update_tx, second_update_rx) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        let _ = first_update_sent_tx.send(());
        second_update_rx.await.unwrap();
        websocket
            .send(test_binance_depth_message(102, 102))
            .await
            .unwrap();
        sleep(Duration::from_millis(30)).await;
        let _ = websocket.send(Message::Close(None)).await;
    });

    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let mut context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        snapshot_endpoint,
        Some(Instant::now() + Duration::from_secs(2)),
        "bootstrap",
    );
    context.spec.frame_inactivity_timeout = Duration::from_millis(80);
    let observed_ingress = context.ingress_sequence.clone();
    let (control_tx, mut control_rx) = mpsc::channel(2);
    let source = tokio::spawn(async move {
        run_binance_depth_update_ws_source_with_control(stream, context, Some(&mut control_rx))
            .await
    });

    first_update_sent_rx.await.unwrap();
    timeout(Duration::from_secs(1), async {
        while observed_ingress.last.load(Ordering::Relaxed) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (reply_tx, reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::HourFinalize,
            stop_after_fence: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    sleep(Duration::from_millis(120)).await;
    resume_tx.send(()).unwrap();
    snapshot_release_tx.send(()).unwrap();

    let snapshot = rx.recv().await.unwrap().into_event();
    let first = rx.recv().await.unwrap().into_event();
    assert_eq!(snapshot.source_event_type, "depth_snapshot");
    assert_eq!(first.source_event_type, "depth_update");
    let first_raw = serde_json::from_str::<Value>(&first.raw_json).unwrap();
    assert_eq!(first_raw["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);

    let (reply_tx, reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::HourFinalize,
            stop_after_fence: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    resume_tx.send(()).unwrap();
    second_update_tx.send(()).unwrap();

    let second = rx.recv().await.unwrap().into_event();
    let second_raw = serde_json::from_str::<Value>(&second.raw_json).unwrap();
    assert_eq!(second_raw["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert_eq!(second_raw["snapshot_last_update_id"], 100);
    assert_eq!(second_raw["local_u_before"], 101);
    let outcome = source.await.unwrap().unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason.starts_with("ws_closed:")
    ));
    assert!(
        !http_server.await.unwrap(),
        "hour-finalize must not create a replacement snapshot request"
    );
    ws_server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
}

#[tokio::test]
async fn binance_depth_dropped_hour_resume_releases_pending_before_terminal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(90, 90))
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    });
    let (tx, mut rx) = mpsc::channel(4);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        "http://127.0.0.1:9/api/v3/depth".to_owned(),
        Some(Instant::now() + Duration::from_secs(2)),
        "bootstrap",
    );
    let observed_ingress = context.ingress_sequence.clone();
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let source = tokio::spawn(async move {
        run_binance_depth_update_ws_source_with_control(stream, context, Some(&mut control_rx))
            .await
    });
    timeout(Duration::from_secs(1), async {
        while observed_ingress.last.load(Ordering::Relaxed) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (reply_tx, reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::HourFinalize,
            stop_after_fence: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    drop(resume_tx);
    let error = source.await.unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("depth barrier owner dropped source resume signal"),
        "{error}"
    );
    let released = rx.recv().await.unwrap().into_event();
    let raw = serde_json::from_str::<Value>(&released.raw_json).unwrap();
    assert_eq!(raw["u"], 90);
    assert_eq!(
        raw["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn binance_depth_operator_cutoff_discards_unadmitted_pending_and_stops_cleanly() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (server_release_tx, server_release_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 18446744073709551615\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        let _ = server_release_rx.await;
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    });

    let (tx, mut rx) = mpsc::channel(4);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        snapshot_endpoint,
        Some(Instant::now() + Duration::from_secs(2)),
        "bootstrap",
    );
    let observed_ingress = context.ingress_sequence.clone();
    let snapshot_wait = context.snapshot_not_before.clone();
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let source = tokio::spawn(async move {
        run_binance_depth_update_ws_source_with_control(stream, context, Some(&mut control_rx))
            .await
    });

    timeout(Duration::from_secs(1), async {
        while observed_ingress.last.load(Ordering::Relaxed) < 2
            || snapshot_wait.deadline.lock().unwrap().is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::task::yield_now().await;
    gate.prepare_runtime_exit_with_cause(now_ms(), RuntimeCloseCause::OperatorStop)
        .unwrap();
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::ResetContinuity,
            stop_after_fence: true,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    assert!(matches!(
        source.await.unwrap().unwrap(),
        BinanceDepthUpdateExit::Stopped
    ));
    assert!(rx.try_recv().is_err());
    assert_eq!(stats.depth_admission_sequence.load(Ordering::Relaxed), 0);
    assert!(gaps.lock().unwrap().is_empty());

    let _ = server_release_tx.send(());
    http_server.await.unwrap();
    ws_server.abort();
    let _ = ws_server.await;
}

#[tokio::test]
async fn binance_depth_cutoff_does_not_apply_unadmitted_ready_update() {
    let observed_utc_ms = now_ms();
    let (tx, mut rx) = mpsc::channel(4);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, observed_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        "ws://127.0.0.1:9".to_owned(),
        "http://127.0.0.1:9/api/v3/depth".to_owned(),
        None,
        "bootstrap",
    );
    let mut continuity = DepthContinuityState::Ready {
        snapshot_last_update_id: 100,
        local_u: Some(101),
        snapshot_utc_day: utc_day_string_from_ms(observed_utc_ms).unwrap(),
    };
    let event = RawEvent {
        source_kind: "binance_depth_update".to_owned(),
        source_event_type: "depth_update".to_owned(),
        source_identifier: "BTCUSDT".to_owned(),
        source_ts: Some(observed_utc_ms.to_string()),
        recv_time_utc_ms: observed_utc_ms,
        collector_host_id: "collector-test".to_owned(),
        collector_region: "local-test".to_owned(),
        source_endpoint: "ws://127.0.0.1:9".to_owned(),
        raw_json: json!({
            "e": "depthUpdate",
            "E": observed_utc_ms,
            "s": "BTCUSDT",
            "U": 103,
            "u": 103,
            "b": [["1", "2"]],
            "a": [["3", "4"]]
        })
        .to_string(),
        collector_admission: None,
    };
    gate.prepare_runtime_exit_with_cause(observed_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap();

    let assessment = admit_depth_update(&context, event, 1, (Some(100), Some(101)))
        .await
        .unwrap();
    if let Some(assessment) = assessment {
        let _ = apply_depth_assessment(&mut continuity, &assessment).unwrap();
        panic!("an update after the controlled cutoff must not be applied");
    }
    let DepthContinuityState::Ready { local_u, .. } = continuity else {
        panic!("the unadmitted update must not invalidate ready continuity");
    };
    assert_eq!(local_u, Some(101));
    assert!(rx.try_recv().is_err());
    assert_eq!(stats.depth_admission_sequence.load(Ordering::Relaxed), 0);
    assert!(gaps.lock().unwrap().is_empty());
}

#[tokio::test]
async fn binance_depth_run_deadline_discards_unadmitted_pending_without_gap() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (server_release_tx, server_release_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let _ = server_release_rx.await;
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    });

    let (tx, mut rx) = mpsc::channel(4);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let observed_utc_ms = now_ms();
    let gate = install_active_raw_day_gate(&stats, observed_utc_ms).await;
    let deadline = Instant::now() + Duration::from_millis(100);
    gate.install_runtime_deadline(Some(RuntimeDeadlineCandidate {
        deadline,
        observed_utc_ms: observed_utc_ms + 100,
    }))
    .unwrap();
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        snapshot_endpoint,
        Some(deadline),
        "bootstrap",
    );
    let observed_ingress = context.ingress_sequence.clone();
    let outcome = tokio::spawn(async move {
        run_binance_depth_update_ws_source_with_control(stream, context, None).await
    });

    timeout(Duration::from_secs(1), async {
        while observed_ingress.last.load(Ordering::Relaxed) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        outcome.await.unwrap().unwrap(),
        BinanceDepthUpdateExit::DeadlineReached
    ));
    assert!(rx.try_recv().is_err());
    assert_eq!(stats.depth_admission_sequence.load(Ordering::Relaxed), 0);
    assert!(gaps.lock().unwrap().is_empty());

    let _ = server_release_tx.send(());
    http_server.await.unwrap();
    ws_server.abort();
    let _ = ws_server.await;
}

#[tokio::test]
async fn binance_depth_eof_releases_pending_update_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        websocket
            .send(test_binance_depth_message(90, 90))
            .await
            .unwrap();
    });
    let (tx, mut rx) = mpsc::channel(4);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let outcome = run_binance_depth_update_ws_source_with_control(
        stream,
        test_binance_depth_context(
            reqwest::Client::new(),
            SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: stats,
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            websocket_endpoint,
            "http://127.0.0.1:9/api/v3/depth".to_owned(),
            Some(Instant::now() + Duration::from_secs(2)),
            "bootstrap",
        ),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { .. }
    ));
    let released = rx.recv().await.unwrap().into_event();
    let raw = serde_json::from_str::<Value>(&released.raw_json).unwrap();
    assert_eq!(raw["u"], 90);
    assert_eq!(
        raw["stitch_verdict"],
        BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
    );
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_pending_cap_neutral_releases_without_reconnecting_the_socket() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (snapshot_sent_tx, snapshot_sent_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let (mut socket, _) = http_listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        sleep(Duration::from_millis(200)).await;
        let body = r#"{"lastUpdateId":100,"bids":[["1","2"]],"asks":[["3","4"]]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = snapshot_sent_tx.send(());
        sleep(Duration::from_millis(30)).await;
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for update_id in [90_u64, 91, 92] {
            websocket
                .send(test_binance_depth_message(update_id, update_id))
                .await
                .unwrap();
        }
        snapshot_sent_rx.await.unwrap();
        websocket
            .send(test_binance_depth_message(101, 101))
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        let _ = websocket.send(Message::Close(None)).await;
    });

    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let mut context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        snapshot_endpoint,
        Some(Instant::now() + Duration::from_secs(3)),
        "bootstrap",
    );
    context.pending_limits = Some((2, 1024 * 1024));
    context.spec.frame_inactivity_timeout = Duration::from_secs(2);
    let outcome = run_binance_depth_update_ws_source_with_control(stream, context, None)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason.starts_with("ws_closed:")
    ));

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event.into_event());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "depth_update",
            "depth_update",
            "depth_update",
            "depth_snapshot",
            "depth_update",
        ]
    );
    let raw = events
        .iter()
        .map(|event| serde_json::from_str::<Value>(&event.raw_json).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        raw[..3]
            .iter()
            .map(|value| value["u"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![90, 91, 92]
    );
    assert!(raw[..3].iter().all(|value| {
        value["stitch_verdict"] == BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
            && value["snapshot_last_update_id"].is_null()
    }));
    assert_eq!(raw[4]["stitch_verdict"], BINANCE_DEPTH_VERDICT_APPLIED);
    assert!(raw.iter().all(|value| value["connection_generation"] == 1));
    assert!(raw[..3].windows(2).all(|pair| {
        pair[0]["depth_ingress_sequence"].as_u64().unwrap()
            < pair[1]["depth_ingress_sequence"].as_u64().unwrap()
    }));
    assert!(
        raw[4]["depth_ingress_sequence"].as_u64().unwrap()
            < raw[3]["depth_ingress_sequence"].as_u64().unwrap(),
        "the update observed while the snapshot response is pending must retain its earlier ingress coordinate"
    );
    assert!(events.windows(2).all(|pair| {
        pair[0].collector_admission.as_ref().unwrap().sequence
            < pair[1].collector_admission.as_ref().unwrap().sequence
    }));
    assert!(gaps.lock().unwrap().is_empty());
    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_ping_frames_keep_a_business_silent_socket_alive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        for marker in 0_u8..5 {
            websocket
                .send(Message::Ping(vec![marker].into()))
                .await
                .unwrap();
            let reply = timeout(Duration::from_millis(200), websocket.next())
                .await
                .expect("collector must answer Ping before the inactivity deadline")
                .expect("collector must keep the socket open")
                .unwrap();
            assert!(matches!(reply, Message::Pong(payload) if payload.as_ref() == [marker]));
            sleep(Duration::from_millis(40)).await;
        }
        let _ = websocket.send(Message::Close(None)).await;
    });
    let (tx, _rx) = mpsc::channel(1);
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let mut context = test_binance_depth_context(
        reqwest::Client::new(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        "http://127.0.0.1:9/api/v3/depth".to_owned(),
        Some(Instant::now() + Duration::from_secs(2)),
        "bootstrap",
    );
    context.spec.frame_inactivity_timeout = Duration::from_millis(80);

    let outcome = run_binance_depth_update_ws_source_with_control(stream, context, None)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason.starts_with("ws_closed:")
    ));
    let BinanceDepthUpdateExit::StreamEnded { saw_transport, .. } = outcome else {
        unreachable!()
    };
    assert!(saw_transport);
    let mut recovery = SourceRecoverySupervisor::new("binance_depth_update", 7);
    let _ = recovery.next_wait();
    let _ = recovery.next_wait();
    recovery.record_business_recovery();
    assert_eq!(recovery.next_wait().attempt, 1);
    server.await.unwrap();
}

#[tokio::test]
async fn binance_depth_snapshot_retries_do_not_reset_full_frame_inactivity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _websocket = accept_async(socket).await.unwrap();
        sleep(Duration::from_secs(2)).await;
    });
    let (tx, _rx) = mpsc::channel(1);
    let (stream, _) = connect_async(&websocket_endpoint).await.unwrap();
    let mut context = test_binance_depth_context(
        reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap(),
        SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        websocket_endpoint,
        "http://127.0.0.1:9/api/v3/depth".to_owned(),
        Some(Instant::now() + Duration::from_secs(3)),
        "bootstrap",
    );
    context.spec.frame_inactivity_timeout = Duration::from_millis(1_100);
    let started = Instant::now();
    let outcome = run_binance_depth_update_ws_source_with_control(stream, context, None)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        BinanceDepthUpdateExit::StreamEnded { ref reason, .. }
            if reason == "full_frame_inactivity"
    ));
    assert!(
        started.elapsed() < Duration::from_millis(1_600),
        "snapshot retry activity must not restart the persistent frame timer"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn binance_depth_one_hundred_reconnects_reach_generation_101_with_bounded_fds() {
    if delegate_resource_test_to_isolated_process(
        "training_runtime::tests::binance_depth_source::binance_depth_one_hundred_reconnects_reach_generation_101_with_bounded_fds",
    ) {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (final_release_tx, final_release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut fd_counts = Vec::with_capacity(101);
        for _ in 0..100 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(socket).await.unwrap();
            fd_counts.push(fs::read_dir("/proc/self/fd").unwrap().count());
            websocket
                .send(test_binance_depth_message(1, 1))
                .await
                .unwrap();
            websocket.send(Message::Close(None)).await.unwrap();
        }
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        fd_counts.push(fs::read_dir("/proc/self/fd").unwrap().count());
        websocket
            .send(test_binance_depth_message(1, 1))
            .await
            .unwrap();
        let _ = final_release_rx.await;
        fd_counts
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(128).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_depth_source_with_spec_and_control(
        BinanceDepthSpec {
            websocket_endpoint,
            snapshot_endpoint: "http://127.0.0.1:9/api/v3/depth".to_owned(),
            frame_inactivity_timeout: Duration::from_secs(30),
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
        Some(control_rx),
    ));

    let mut previous_ingress = 0_u64;
    for expected_generation in 1_u64..=101 {
        let event = timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("the depth source must survive one hundred reconnects")
            .expect("source queue must remain open")
            .into_event();
        let raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
        assert_eq!(raw["connection_generation"], expected_generation);
        assert_eq!(
            raw["stitch_verdict"],
            BINANCE_DEPTH_VERDICT_NOT_APPLIED_SNAPSHOT_UNAVAILABLE
        );
        let ingress = raw["depth_ingress_sequence"].as_u64().unwrap();
        assert!(ingress > previous_ingress);
        previous_ingress = ingress;
    }
    assert_eq!(
        stats
            .drain_all_operational()
            .values()
            .find_map(|sources| sources.get("binance_depth_update"))
            .unwrap()
            .reconnect_events,
        100
    );
    assert!(gaps.lock().unwrap().is_empty());
    assert!(!source.is_finished());

    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::ResetContinuity,
            stop_after_fence: true,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(2), reply_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), source)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = final_release_tx.send(());
    let fd_counts = server.await.unwrap();
    assert_eq!(fd_counts.len(), 101);
    let first = fd_counts[0];
    let last = *fd_counts.last().unwrap();
    let minimum = *fd_counts.iter().min().unwrap();
    let maximum = *fd_counts.iter().max().unwrap();
    assert!(
        last <= first + 2 && maximum <= minimum + 4,
        "depth connection FDs must remain bounded: first={first}, last={last}, min={minimum}, max={maximum}"
    );
}

#[tokio::test]
async fn binance_depth_one_hundred_resnapshots_keep_one_owner_and_bounded_fds() {
    if delegate_resource_test_to_isolated_process(
        "training_runtime::tests::binance_depth_source::binance_depth_one_hundred_resnapshots_keep_one_owner_and_bounded_fds",
    ) {
        return;
    }
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
    let snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        http_listener.local_addr().unwrap()
    );
    let (first_snapshot_tx, first_snapshot_rx) = oneshot::channel();
    let http_server = tokio::spawn(async move {
        let mut first_snapshot_tx = Some(first_snapshot_tx);
        let mut fd_counts = Vec::with_capacity(101);
        for snapshot_index in 0_u64..=100 {
            let (mut socket, _) = http_listener.accept().await.unwrap();
            fd_counts.push(fs::read_dir("/proc/self/fd").unwrap().count());
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let last_update_id = 100 + 3 * snapshot_index;
            let body = json!({
                "lastUpdateId": last_update_id,
                "bids": [["1", "2"]],
                "asks": [["3", "4"]]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            if snapshot_index == 0 {
                let _ = first_snapshot_tx.take().unwrap().send(());
            }
        }
        fd_counts
    });
    let (release_ws_tx, release_ws_rx) = oneshot::channel();
    let ws_server = tokio::spawn(async move {
        let (socket, _) = ws_listener.accept().await.unwrap();
        let mut websocket = accept_async(socket).await.unwrap();
        first_snapshot_rx.await.unwrap();
        for cycle in 0_u64..100 {
            let anchor = 100 + 3 * cycle;
            websocket
                .send(test_binance_depth_message(anchor + 1, anchor + 1))
                .await
                .unwrap();
            websocket
                .send(test_binance_depth_message(anchor + 3, anchor + 3))
                .await
                .unwrap();
        }
        let _ = release_ws_rx.await;
    });

    let (tx, mut rx) = bounded_event_queue::<SourceIngressPayload>(512).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (control_tx, control_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let source = tokio::spawn(run_binance_depth_source_with_spec_and_control(
        BinanceDepthSpec {
            websocket_endpoint,
            snapshot_endpoint,
            frame_inactivity_timeout: Duration::from_secs(30),
            recovery_wait_override: Some(Duration::from_millis(1)),
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
        Some(Instant::now() + Duration::from_secs(10)),
        Instant::now(),
        Some(control_rx),
    ));

    let mut snapshot_count = 0_u64;
    let mut applied_count = 0_u64;
    let mut hole_count = 0_u64;
    let mut ingress = BTreeSet::new();
    let mut previous_admission = 0_u64;
    for _ in 0..301 {
        let event = timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("one hundred resnapshot cycles must make progress")
            .expect("source queue must remain open")
            .into_event();
        let raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
        assert_eq!(raw["connection_generation"], 1);
        assert!(ingress.insert(raw["depth_ingress_sequence"].as_u64().unwrap()));
        let admission = event.collector_admission.unwrap().sequence;
        assert!(admission > previous_admission);
        previous_admission = admission;
        match event.source_event_type.as_str() {
            "depth_snapshot" => snapshot_count += 1,
            "depth_update" if raw["stitch_verdict"] == BINANCE_DEPTH_VERDICT_APPLIED => {
                applied_count += 1;
            }
            "depth_update"
                if raw["stitch_verdict"] == BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE =>
            {
                hole_count += 1;
            }
            other => panic!("unexpected resnapshot evidence: {other} {raw}"),
        }
    }
    assert_eq!(snapshot_count, 101);
    assert_eq!(applied_count, 100);
    assert_eq!(hole_count, 100);
    assert_eq!(ingress.len(), 301);
    assert!(!source.is_finished());

    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::ResetContinuity,
            stop_after_fence: true,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    timeout(Duration::from_secs(2), source)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = release_ws_tx.send(());
    ws_server.await.unwrap();
    let fd_counts = http_server.await.unwrap();
    assert_eq!(fd_counts.len(), 101);
    let minimum = *fd_counts.iter().min().unwrap();
    let maximum = *fd_counts.iter().max().unwrap();
    assert!(
        maximum <= minimum + 4,
        "snapshot HTTP sockets/FDs must remain bounded: min={minimum}, max={maximum}"
    );
    assert!(!gaps.lock().unwrap().is_empty());
}

#[test]
fn binance_depth_ingress_and_admission_sequences_fail_closed_on_overflow() {
    let ingress = DepthIngressSequenceAllocator {
        last: Arc::new(AtomicU64::new(u64::MAX)),
    };
    assert_eq!(
        ingress.allocate().unwrap_err(),
        "binance_depth_ingress_sequence_overflow"
    );

    let stats = SourceQueueBackpressureStats::default();
    stats
        .depth_admission_sequence
        .store(u64::MAX, Ordering::Relaxed);
    assert!(
        stats
            .reserve_admission_sequences("binance_depth", 1)
            .unwrap_err()
            .to_string()
            .contains("depth admission sequence overflow")
    );
}

#[test]
fn binance_depth_pending_buffer_enforces_byte_cap_independently_of_row_cap() {
    let pending = DepthPendingBuffer::with_limits(100, 10);
    assert!(!pending.would_exceed(10));
    assert!(pending.would_exceed(11));
    assert_eq!(
        depth_snapshot_retry_delay(u32::MAX, 7),
        depth_snapshot_retry_delay(32, 7)
    );
}

#[test]
fn binance_depth_retry_after_preserves_legal_seconds() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        reqwest::header::HeaderValue::from_static("18446744073709551615"),
    );
    assert_eq!(
        binance_snapshot_retry_after(&headers, reqwest::StatusCode::TOO_MANY_REQUESTS),
        Some(Duration::from_secs(u64::MAX))
    );
    headers.insert(
        reqwest::header::RETRY_AFTER,
        reqwest::header::HeaderValue::from_static("not-seconds"),
    );
    assert_eq!(
        binance_snapshot_retry_after(&headers, reqwest::StatusCode::TOO_MANY_REQUESTS),
        None
    );
}
