use super::*;

#[tokio::test]
async fn rtds_source_survives_startup_unavailable_then_applies_one_complete_desired() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("ws://{address}");
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let mut spec = RtdsSourceSpec::production();
    spec.endpoint = endpoint;
    spec.ws_config.reconnect.initial_backoff = Duration::from_millis(5);
    spec.ws_config.reconnect.max_backoff = Duration::from_millis(10);
    spec.ws_config.reconnect.backoff_multiplier = 1.0;
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(60);
    let started = Instant::now();
    let source = tokio::spawn(run_rtds_source_with_spec(
        spec,
        dataset_root,
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
        Some(Instant::now() + Duration::from_millis(500)),
        started,
    ));

    sleep(Duration::from_millis(100)).await;
    assert!(
        !source.is_finished(),
        "startup transport unavailability must not exhaust a retry or timeout budget"
    );

    let listener = TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let request = loop {
            let message = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            if text == "PING" {
                continue;
            }
            break serde_json::from_str::<Value>(&text).unwrap();
        };
        assert_eq!(request["action"], "subscribe");
        assert_eq!(request["subscriptions"].as_array().unwrap().len(), 1);
        assert_eq!(
            request["subscriptions"][0]["topic"],
            "crypto_prices_chainlink"
        );
        assert_eq!(request["subscriptions"][0]["type"], "*");
        assert_eq!(
            request["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );
        assert!(
            timeout(Duration::from_millis(40), async {
                loop {
                    let message = socket.next().await.unwrap().unwrap();
                    if let Message::Text(text) = message
                        && text != "PING"
                        && serde_json::from_str::<Value>(&text).is_ok()
                    {
                        break;
                    }
                }
            })
            .await
            .is_err(),
            "the first successful generation may receive only one complete desired replay"
        );
        let timestamp = now_ms();
        socket
            .send(Message::Text(
                json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": timestamp,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": timestamp,
                        "value": "65000.10"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    let event = timeout(Duration::from_millis(300), rx.recv())
        .await
        .expect("RTDS must recover after startup unavailability")
        .expect("source ingress remains open");
    assert_eq!(
        rtds_row_from_event(&event.event).unwrap().freshness_status,
        "fresh"
    );
    timeout(Duration::from_secs(1), source)
        .await
        .expect("run deadline must stop the recovered source")
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|by_source| {
        by_source
            .get("polymarket_rtds_chainlink_btc_usd")
            .is_none_or(|counter| counter.lag_events == 0 && counter.reconnect_events == 0)
    }));
}

#[tokio::test]
async fn rtds_source_retries_after_a_stalled_websocket_handshake() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let server = tokio::spawn(async move {
        let (mut stalled_handshake, _) = listener.accept().await.unwrap();
        let stalled_closed = tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            timeout(Duration::from_millis(300), async {
                loop {
                    match stalled_handshake.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            })
            .await
            .expect("timed-out handshake socket must be released");
        });

        let (stream, _) = timeout(Duration::from_millis(300), listener.accept())
            .await
            .expect("stalled WebSocket handshake must be retried")
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let request = loop {
            let message = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            if text == "PING" {
                continue;
            }
            break serde_json::from_str::<Value>(&text).unwrap();
        };
        assert_eq!(request["action"], "subscribe");
        assert_eq!(request["subscriptions"].as_array().unwrap().len(), 1);
        assert_eq!(
            request["subscriptions"][0]["topic"],
            "crypto_prices_chainlink"
        );
        assert_eq!(request["subscriptions"][0]["type"], "*");
        assert_eq!(
            request["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );
        assert!(
            timeout(Duration::from_millis(40), async {
                loop {
                    let message = socket.next().await.unwrap().unwrap();
                    if let Message::Text(text) = message
                        && text != "PING"
                        && serde_json::from_str::<Value>(&text).is_ok()
                    {
                        break;
                    }
                }
            })
            .await
            .is_err(),
            "successful generation may receive only one complete desired replay"
        );
        let timestamp = now_ms();
        socket
            .send(Message::Text(
                json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": timestamp,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": timestamp,
                        "value": "65000.10"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        stalled_closed.await.unwrap();
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "one stalled attempt must not create duplicate successful generations"
        );
    });

    let mut spec = RtdsSourceSpec::production();
    spec.endpoint = format!("ws://{address}");
    spec.ws_config.connect_timeout = Some(Duration::from_millis(40));
    spec.ws_config.reconnect.initial_backoff = Duration::from_millis(5);
    spec.ws_config.reconnect.max_backoff = Duration::from_millis(10);
    spec.ws_config.reconnect.backoff_multiplier = 1.0;
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(60);
    let source = tokio::spawn(run_rtds_source_with_spec(
        spec,
        dataset_root,
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
        Some(Instant::now() + Duration::from_millis(350)),
        Instant::now(),
    ));

    let event = timeout(Duration::from_millis(300), rx.recv())
        .await
        .expect("RTDS must recover after the stalled handshake")
        .expect("source ingress remains open");
    assert_eq!(
        rtds_row_from_event(&event.event).unwrap().freshness_status,
        "fresh"
    );
    timeout(Duration::from_secs(1), source)
        .await
        .expect("run deadline must stop the recovered source")
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|by_source| {
        by_source
            .get("polymarket_rtds_chainlink_btc_usd")
            .is_none_or(|counter| counter.lag_events == 0 && counter.reconnect_events == 0)
    }));
}

#[tokio::test]
async fn rtds_source_recovers_after_close_and_failures_without_gap_or_replay_duplication() {
    const FAILED_ATTEMPTS: usize = 5;

    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (failed_attempts_tx, failed_attempts_rx) = oneshot::channel();
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let initial = loop {
            let message = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            if text == "PING" {
                continue;
            }
            break serde_json::from_str::<Value>(&text).unwrap();
        };
        socket.send(Message::Close(None)).await.unwrap();
        drop(socket);

        for _ in 0..FAILED_ATTEMPTS {
            let (stream, _) = timeout(Duration::from_millis(300), listener.accept())
                .await
                .expect("natural reconnect attempt")
                .unwrap();
            drop(stream);
        }
        failed_attempts_tx.send(()).unwrap();

        let (stream, _) = timeout(Duration::from_millis(300), listener.accept())
            .await
            .expect("stable RTDS generation after failed attempts")
            .unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let replay = loop {
            let message = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            if text == "PING" {
                continue;
            }
            break serde_json::from_str::<Value>(&text).unwrap();
        };
        assert_eq!(initial, replay);
        assert_eq!(replay["action"], "subscribe");
        assert_eq!(replay["subscriptions"].as_array().unwrap().len(), 1);
        assert_eq!(
            replay["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );
        assert!(
            timeout(Duration::from_millis(40), async {
                loop {
                    let message = socket.next().await.unwrap().unwrap();
                    if let Message::Text(text) = message
                        && text != "PING"
                        && serde_json::from_str::<Value>(&text).is_ok()
                    {
                        break;
                    }
                }
            })
            .await
            .is_err(),
            "a successful generation may receive only one latest desired replay"
        );
        let timestamp = now_ms();
        socket
            .send(Message::Text(
                json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": timestamp,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": timestamp,
                        "value": "65000.10"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    let mut spec = RtdsSourceSpec::production();
    spec.endpoint = format!("ws://{address}");
    spec.ws_config.connect_timeout = Some(Duration::from_millis(50));
    spec.ws_config.reconnect.initial_backoff = Duration::from_millis(5);
    spec.ws_config.reconnect.max_backoff = Duration::from_millis(10);
    spec.ws_config.reconnect.backoff_multiplier = 1.0;
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(60);
    let source = tokio::spawn(run_rtds_source_with_spec(
        spec,
        dataset_root,
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
        Some(Instant::now() + Duration::from_millis(500)),
        Instant::now(),
    ));

    failed_attempts_rx.await.unwrap();
    assert!(
        !source.is_finished(),
        "failed reconnect attempts must not exhaust the RTDS source"
    );
    let event = timeout(Duration::from_millis(300), rx.recv())
        .await
        .expect("RTDS must recover after Close and failed attempts")
        .expect("source ingress remains open");
    assert_eq!(
        rtds_row_from_event(&event.event).unwrap().freshness_status,
        "fresh"
    );
    assert!(!source.is_finished());
    timeout(Duration::from_secs(1), source)
        .await
        .expect("run deadline must stop the recovered source")
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    let counter = counters
        .values()
        .find_map(|by_source| by_source.get("polymarket_rtds_chainlink_btc_usd"))
        .expect("natural RTDS generation advance counter");
    assert_eq!(counter.lag_events, 0);
    assert_eq!(counter.reconnect_events, 1);
}

#[tokio::test]
async fn rtds_source_keeps_one_socket_without_required_text_pong_and_consumes_stale_then_fresh() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut ping_count = 0_u64;
        let request = loop {
            let message = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            if text == "PING" {
                ping_count += 1;
                continue;
            }
            break serde_json::from_str::<Value>(&text).unwrap();
        };
        assert_eq!(request["action"], "subscribe");
        assert_eq!(request["subscriptions"].as_array().unwrap().len(), 1);
        assert_eq!(
            request["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );

        let silence = sleep(Duration::from_millis(200));
        tokio::pin!(silence);
        loop {
            tokio::select! {
                () = &mut silence => break,
                message = socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) if text == "PING" => {
                            ping_count += 1;
                        }
                        Some(Ok(Message::Text(text))) if serde_json::from_str::<Value>(&text).is_ok() => {
                            panic!("business silence must not send a second RTDS subscription: {text}");
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                            panic!("business silence must not close the healthy transport");
                        }
                        _ => {}
                    }
                }
            }
        }

        let timestamp = now_ms();
        for (payload_timestamp, value) in [
            (timestamp.saturating_sub(31_000), "64000.00"),
            (timestamp, "65000.00"),
        ] {
            socket
                .send(Message::Text(
                    json!({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": timestamp,
                        "payload": {
                            "symbol": "btc/usd",
                            "timestamp": payload_timestamp,
                            "value": value
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }

        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(text)) if text == "PING" => {
                    ping_count += 1;
                }
                Ok(Message::Text(text)) if serde_json::from_str::<Value>(&text).is_ok() => {
                    panic!("a healthy generation may receive only one complete desired replay")
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        assert!(
            ping_count >= 7,
            "RTDS must keep sending PING while text PONG is absent"
        );
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "business silence must not create a second connection generation"
        );
    });

    let mut spec = RtdsSourceSpec::production();
    spec.endpoint = format!("ws://{address}");
    spec.ws_config.reconnect.initial_backoff = Duration::from_millis(5);
    spec.ws_config.reconnect.max_backoff = Duration::from_millis(10);
    spec.ws_config.reconnect.backoff_multiplier = 1.0;
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(60);
    let source = tokio::spawn(run_rtds_source_with_spec(
        spec,
        dataset_root,
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
        None,
        Instant::now(),
    ));

    sleep(Duration::from_millis(170)).await;
    assert!(
        !source.is_finished(),
        "business silence must not finish the RTDS source"
    );
    let mut statuses = Vec::new();
    while statuses.len() < 2 {
        let event = timeout(Duration::from_millis(300), rx.recv())
            .await
            .expect("same socket must deliver stale and fresh rows")
            .expect("source ingress remains open");
        statuses.push(rtds_row_from_event(&event.event).unwrap().freshness_status);
    }
    assert_eq!(statuses, ["stale", "fresh"]);
    assert!(!source.is_finished());
    source.abort();
    let join_error = timeout(Duration::from_secs(1), source)
        .await
        .expect("aborting the run-forever source must release its runtime")
        .expect_err("the run-forever source must not return before explicit cancellation");
    assert!(join_error.is_cancelled());
    server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|by_source| {
        by_source
            .get("polymarket_rtds_chainlink_btc_usd")
            .is_none_or(|counter| counter.lag_events == 0 && counter.reconnect_events == 0)
    }));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn rtds_business_timer_uses_late_anchor_and_preserves_cancelled_backoff_level() {
    let timing = RtdsBusinessTiming {
        steady_silence_threshold: Duration::from_secs(300),
        first_fresh_observation_window: Duration::from_secs(60),
        outer_backoff_initial_ms: 1_000,
        outer_backoff_max_ms: 300_000,
    };
    let mut recovery = RtdsBusinessRecovery::new(timing, 7, 0);

    recovery.observe_state(
        ConnectionState::Connected {
            since: Instant::now(),
        },
        0,
        tokio::time::Instant::now(),
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    recovery.observe_applied(0, tokio::time::Instant::now());
    assert_eq!(
        recovery.next_deadline(),
        None,
        "cold start has no business watchdog"
    );

    recovery.observe_fresh(0, tokio::time::Instant::now());
    tokio::time::advance(Duration::from_secs(300)).await;
    assert!(recovery.replacement_due(tokio::time::Instant::now()));
    assert_eq!(recovery.commit_replacement(), ("steady_silence", 1));

    recovery.start_runtime(0);
    recovery.observe_applied(0, tokio::time::Instant::now());
    tokio::time::advance(Duration::from_secs(10)).await;
    recovery.observe_state(
        ConnectionState::Connected {
            since: Instant::now(),
        },
        0,
        tokio::time::Instant::now(),
    );
    let first_fresh_deadline = recovery.next_deadline().unwrap();
    assert_eq!(
        first_fresh_deadline.saturating_duration_since(tokio::time::Instant::now()),
        Duration::from_secs(60),
        "first-fresh window starts at the later Connected/applied observation"
    );

    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!recovery.replacement_due(tokio::time::Instant::now()));
    let first_backoff = recovery
        .next_deadline()
        .unwrap()
        .saturating_duration_since(tokio::time::Instant::now());
    recovery.observe_state(
        ConnectionState::Reconnecting { attempt: 1 },
        0,
        tokio::time::Instant::now(),
    );
    assert_eq!(
        recovery.next_deadline(),
        None,
        "transport recovery cancels the current outer wait"
    );

    recovery.observe_state(
        ConnectionState::Connected {
            since: Instant::now(),
        },
        1,
        tokio::time::Instant::now(),
    );
    recovery.observe_applied(1, tokio::time::Instant::now());
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!recovery.replacement_due(tokio::time::Instant::now()));
    assert_eq!(
        recovery
            .next_deadline()
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now()),
        first_backoff,
        "a cancelled wait does not consume the backoff level"
    );

    tokio::time::advance(first_backoff).await;
    assert!(recovery.replacement_due(tokio::time::Instant::now()));
    assert_eq!(
        recovery.commit_replacement(),
        ("steady_silence", 2),
        "an actual backoff replacement advances the episode attempt"
    );
    recovery.start_runtime(2);
    recovery.observe_state(
        ConnectionState::Connected {
            since: Instant::now(),
        },
        2,
        tokio::time::Instant::now(),
    );
    recovery.observe_applied(2, tokio::time::Instant::now());
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!recovery.replacement_due(tokio::time::Instant::now()));
    let second_backoff = recovery
        .next_deadline()
        .unwrap()
        .saturating_duration_since(tokio::time::Instant::now());
    assert!(second_backoff > first_backoff);

    recovery.observe_fresh(2, tokio::time::Instant::now());
    assert_eq!(
        recovery.next_deadline().unwrap(),
        tokio::time::Instant::now() + Duration::from_secs(300),
        "fresh ends the episode and restores the steady deadline"
    );

    recovery.observe_state(
        ConnectionState::Reconnecting { attempt: 1 },
        2,
        tokio::time::Instant::now(),
    );
    recovery.observe_state(
        ConnectionState::Connected {
            since: Instant::now(),
        },
        3,
        tokio::time::Instant::now(),
    );
    recovery.observe_applied(3, tokio::time::Instant::now());
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!recovery.replacement_due(tokio::time::Instant::now()));
    let backoff = recovery
        .next_deadline()
        .unwrap()
        .saturating_duration_since(tokio::time::Instant::now());
    assert_eq!(backoff, first_backoff, "fresh resets the outer level");
    tokio::time::advance(backoff).await;
    assert!(recovery.replacement_due(tokio::time::Instant::now()));
    assert_eq!(
        recovery.commit_replacement(),
        ("first_fresh_window_timeout", 1)
    );
    assert_eq!(
        retry_delay(
            "polymarket_rtds_chainlink_btc_usd",
            u32::MAX,
            timing.outer_backoff_initial_ms,
            timing.outer_backoff_max_ms,
            7,
        ),
        Duration::from_secs(300),
        "the shared retry helper enforces the frozen per-wait cap"
    );
}

#[tokio::test]
async fn rtds_business_silence_replaces_two_runtimes_then_fresh_resets_the_episode() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for runtime_epoch in 0..3_u64 {
            let (stream, _) = timeout(Duration::from_secs(2), listener.accept())
                .await
                .expect("next RTDS runtime connection")
                .unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let request = loop {
                let message = socket.next().await.unwrap().unwrap();
                let Message::Text(text) = message else {
                    continue;
                };
                if text == "PING" {
                    continue;
                }
                break serde_json::from_str::<Value>(&text).unwrap();
            };
            assert_eq!(request["action"], "subscribe");
            assert_eq!(request["subscriptions"].as_array().unwrap().len(), 1);
            assert_eq!(
                request["subscriptions"][0]["filters"],
                r#"{"symbol":"btc/usd"}"#
            );

            if runtime_epoch != 1 {
                let timestamp = now_ms();
                socket
                    .send(Message::Text(
                        json!({
                            "topic": "crypto_prices_chainlink",
                            "type": "update",
                            "timestamp": timestamp,
                            "payload": {
                                "symbol": "btc/usd",
                                "timestamp": timestamp,
                                "value": format!("65000.{runtime_epoch}")
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
            }

            let mut ignored_messages = tokio::time::interval(Duration::from_millis(5));
            loop {
                tokio::select! {
                    message = socket.next() => match message {
                        Some(Ok(Message::Text(text))) if text != "PING" => {
                            panic!("unexpected second RTDS control message: {text}")
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    },
                    _ = ignored_messages.tick(), if runtime_epoch == 0 => {
                        if socket
                            .send(Message::Text(
                                json!({
                                    "topic": "ignored",
                                    "type": "ignored",
                                    "timestamp": now_ms(),
                                    "payload": {}
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        }
    });

    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let mut spec = RtdsSourceSpec::production();
    spec.endpoint = format!("ws://{address}");
    spec.ws_config.reconnect.initial_backoff = Duration::from_millis(5);
    spec.ws_config.reconnect.max_backoff = Duration::from_millis(10);
    spec.ws_config.reconnect.backoff_multiplier = 1.0;
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(60);
    spec.business_timing = RtdsBusinessTiming {
        steady_silence_threshold: Duration::from_millis(80),
        first_fresh_observation_window: Duration::from_millis(30),
        outer_backoff_initial_ms: 2,
        outer_backoff_max_ms: 4,
    };
    let source = tokio::spawn(run_rtds_source_with_spec(
        spec,
        dataset_root,
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
        None,
        Instant::now(),
    ));

    timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("runtime 0 fresh row")
        .expect("source ingress remains open");
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("runtime 2 fresh row")
        .expect("source ingress remains open");
    source.abort();
    assert!(
        timeout(Duration::from_secs(1), source)
            .await
            .expect("source cancellation")
            .expect_err("run-forever source requires cancellation")
            .is_cancelled()
    );
    server.await.unwrap();
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|by_source| {
        by_source
            .get("polymarket_rtds_chainlink_btc_usd")
            .is_none_or(|counter| counter.lag_events == 0 && counter.reconnect_events == 0)
    }));
}

#[test]
fn rtds_freshness_classification_keeps_exact_boundaries_and_invalid_reasons() {
    const BASE_MS: i64 = 1_783_392_400_000;
    let message = |topic: &str, message_type: &str, symbol: &str, payload_timestamp: i64| {
        serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
            "topic": topic,
            "type": message_type,
            "timestamp": BASE_MS,
            "payload": {
                "symbol": symbol,
                "timestamp": payload_timestamp,
                "value": "65000.10"
            }
        }))
        .unwrap()
    };
    let status = |message| {
        classify_rtds_message(&message, BASE_MS)
            .unwrap()
            .expect("price-shaped RTDS payload")
            .freshness_status
    };

    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "btc/usd",
            BASE_MS + 5_000
        )),
        "fresh"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "btc/usd",
            BASE_MS - 30_000
        )),
        "fresh"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "btc/usd",
            BASE_MS + 5_001
        )),
        "stale"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "btc/usd",
            BASE_MS - 30_001
        )),
        "stale"
    );
    assert_eq!(
        status(message("wrong_topic", "update", "btc/usd", BASE_MS)),
        "invalid_topic"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "snapshot",
            "btc/usd",
            BASE_MS
        )),
        "invalid_type"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "eth/usd",
            BASE_MS
        )),
        "invalid_symbol"
    );
    assert_eq!(
        status(message("crypto_prices_chainlink", "update", "btc/usd", 0)),
        "invalid_timestamp"
    );
    assert_eq!(
        status(message(
            "crypto_prices_chainlink",
            "update",
            "btc/usd",
            BASE_MS / 1_000
        )),
        "invalid_timestamp"
    );
    let seconds_message =
        serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": BASE_MS / 1_000,
            "payload": {
                "symbol": "btc/usd",
                "timestamp": BASE_MS,
                "value": "65000.10"
            }
        }))
        .unwrap();
    assert_eq!(status(seconds_message), "invalid_timestamp");
}

#[tokio::test]
async fn rtds_only_new_fresh_matching_payload_returns_healthy() {
    let (tx, mut rx) = mpsc::channel(16);
    let base = now_ms();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, base).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut dedupe = TimedFingerprintWindow::with_limits(60 * 60 * 1000, 100);
    let message =
        |topic: &str, message_type: &str, symbol: &str, payload_timestamp: i64, value: &str| {
            serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
                "topic": topic,
                "type": message_type,
                "timestamp": base,
                "payload": {
                    "symbol": symbol,
                    "timestamp": payload_timestamp,
                    "value": value
                }
            }))
            .unwrap()
        };
    let run_started = Instant::now();
    for (input, expected_healthy) in [
        (
            message("crypto_prices_chainlink", "snapshot", "btc/usd", base, "1"),
            false,
        ),
        (message("wrong", "update", "btc/usd", base, "2"), false),
        (
            message("crypto_prices_chainlink", "update", "eth/usd", base, "3"),
            false,
        ),
        (
            message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                base - 31_000,
                "4",
            ),
            false,
        ),
        (
            message(
                "crypto_prices_chainlink",
                "update",
                "btc/usd",
                base + 6_000,
                "5",
            ),
            false,
        ),
        (
            message("crypto_prices_chainlink", "update", "btc/usd", 0, "6"),
            false,
        ),
        (
            message("crypto_prices_chainlink", "update", "btc/usd", base, "7"),
            true,
        ),
    ] {
        assert_eq!(
            emit_rtds_message(&sink, &identity, run_started, &mut dedupe, input,)
                .await
                .unwrap(),
            expected_healthy
        );
    }
    let duplicate = message("crypto_prices_chainlink", "update", "btc/usd", base, "7");
    assert!(
        !emit_rtds_message(&sink, &identity, run_started, &mut dedupe, duplicate,)
            .await
            .unwrap()
    );

    let mut statuses = Vec::new();
    while let Ok(timed) = rx.try_recv() {
        let row = rtds_row_from_event(&timed.event).unwrap();
        statuses.push(row.freshness_status);
    }
    assert_eq!(
        statuses,
        vec![
            "invalid_type",
            "invalid_topic",
            "invalid_symbol",
            "stale",
            "stale",
            "invalid_timestamp",
            "fresh"
        ]
    );
}

#[tokio::test]
async fn rtds_initial_snapshot_shape_is_ignored_after_failed_price_decode() {
    let (tx, mut rx) = mpsc::channel(4);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let base = now_ms();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, base).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut dedupe = TimedFingerprintWindow::with_limits(60 * 60 * 1000, 100);
    let snapshot = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
        "topic": "crypto_prices",
        "type": "subscribe",
        "timestamp": base,
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {"timestamp": base - 1_000, "value": 65_000.10},
                {"timestamp": base, "value": 65_001.20}
            ]
        }
    }))
    .unwrap();

    assert!(
        !emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, snapshot,)
            .await
            .unwrap()
    );
    assert!(rx.try_recv().is_err());
    assert!(gaps.lock().unwrap().is_empty());

    let update = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": base,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": base,
            "value": "65001.20"
        }
    }))
    .unwrap();
    assert!(
        emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, update,)
            .await
            .unwrap()
    );
    assert!(rx.recv().await.is_some());
}

#[tokio::test]
async fn rtds_expected_topic_payload_schema_error_is_process_fatal() {
    let (tx, mut rx) = mpsc::channel(4);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let base = now_ms();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, base).await;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut dedupe = TimedFingerprintWindow::with_limits(60 * 60 * 1000, 100);
    let valid = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": base,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": base,
            "value": "65000.10"
        }
    }))
    .unwrap();
    assert!(
        emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, valid)
            .await
            .unwrap()
    );
    assert!(rx.recv().await.is_some());

    let malformed = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": base,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": base
        }
    }))
    .unwrap();
    let error = emit_rtds_message(&sink, &identity, Instant::now(), &mut dedupe, malformed)
        .await
        .expect_err("subscribed RTDS payload schema errors must terminate the source");
    assert!(error.to_string().contains("RTDS Chainlink payload invalid"));
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| gap.starts_with("rtds_unparseable_chainlink_payload:"))
    );
}

#[test]
fn rtds_dedupe_restores_first_receive_anchored_payload() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = now_ms();
    let message = serde_json::from_value::<polymarket_client_sdk_v2::rtds::RtdsMessage>(json!({
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": 1_783_392_404_599_i64,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": 1_783_392_404_500_i64,
            "value": "65000.10"
        }
    }))
    .unwrap();
    let canonical = rtds_source_native_payload_json(&message).unwrap();
    let event = RawEvent {
        source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
        source_event_type: "update".to_owned(),
        source_identifier: "btc/usd".to_owned(),
        source_ts: Some("1783392404500".to_owned()),
        recv_time_utc_ms,
        collector_host_id: "collector-a".to_owned(),
        collector_region: "test".to_owned(),
        source_endpoint: RTDS_WS_ENDPOINT.to_owned(),
        raw_json: json!({
            "topic": message.topic,
            "type": message.msg_type,
            "message_timestamp": message.timestamp,
            "payload_timestamp": 1_783_392_404_500_i64,
            "symbol": "btc/usd",
            "value": "65000.10",
            "age_ms": recv_time_utc_ms - 1_783_392_404_500_i64,
            "freshness_status": "stale",
            "subscription_raw_json": rtds_subscription_evidence_json(),
            "payload": message.payload,
            "source_native_payload_json": canonical,
        })
        .to_string(),
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
    };
    let row = rtds_row_from_event(&event).unwrap();
    let canonical = row.source_native_payload_json.clone();
    let day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
    let source_dir = dataset_root
        .join("training")
        .join("typed_raw")
        .join(format!("utc_day={day}"))
        .join("source_kind=polymarket_rtds_chainlink_btc_usd");
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        &source_dir,
        "process-a",
        "polymarket_rtds_chainlink_btc_usd_v4",
        "polymarket_rtds_chainlink_btc_usd",
    )
    .unwrap();
    writer.write_rtds_chainlink_btc_usd_rows(&[row]).unwrap();
    writer.finalize().unwrap();

    let mut process_b =
        restore_rtds_dedupe(&dataset_root, recv_time_utc_ms.saturating_add(1)).unwrap();
    assert_eq!(
        process_b
            .observe(
                canonical.as_bytes(),
                recv_time_utc_ms.saturating_add(59_999)
            )
            .unwrap(),
        FingerprintDecision::Duplicate
    );
    assert_eq!(
        process_b
            .observe(
                canonical.as_bytes(),
                recv_time_utc_ms.saturating_add(60 * 60 * 1000)
            )
            .unwrap(),
        FingerprintDecision::Unique,
        "the restored first receive time, not payload time, controls expiry"
    );
}
