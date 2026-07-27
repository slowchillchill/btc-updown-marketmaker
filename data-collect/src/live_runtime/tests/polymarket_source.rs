use super::*;

fn polymarket_metadata_v5_prefixes_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketMarketMetadataV3Row>> {
    polymarket_metadata_v5_rows_from_event(event)?
        .into_iter()
        .map(|row| {
            let validation = row.validate();
            if !validation.is_complete() {
                return Err(invalid_data(format!(
                    "metadata v5 fixture validation failed:{}",
                    validation.violations.join(",")
                ))
                .into());
            }
            let mut prefix = row.into_compatibility_prefix();
            prefix.schema_version = POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION.to_owned();
            Ok(prefix)
        })
        .collect()
}

fn polymarket_v3_message(payload: &SourceIngressPayload) -> Option<&PolymarketOrderbookV3Message> {
    match payload {
        SourceIngressPayload::PolymarketOrderbookV3(inflight) => Some(&inflight.message),
        SourceIngressPayload::LegacyRaw(_) => None,
    }
}

fn polymarket_metadata_event(payload: &SourceIngressPayload) -> Option<&RawEvent> {
    match payload {
        SourceIngressPayload::LegacyRaw(inflight)
            if inflight.event.source_kind == "polymarket_market_metadata" =>
        {
            Some(&inflight.event)
        }
        SourceIngressPayload::LegacyRaw(_) | SourceIngressPayload::PolymarketOrderbookV3(_) => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DeferredIngressBatchFact {
    source_kinds: Vec<String>,
    recv_times_utc_ms: Vec<i64>,
    has_collector_admission: Vec<bool>,
    accepted_defer: bool,
    typed_polymarket_v3: bool,
}

fn deferred_ingress_facts(
    stats: &SourceQueueBackpressureStats,
    producer: &str,
) -> Vec<DeferredIngressBatchFact> {
    stats
        .deferred_ingress_batches
        .lock()
        .expect("deferred ingress mutex poisoned")
        .get(producer)
        .into_iter()
        .flatten()
        .map(|batch| match batch {
            DeferredIngressBatch::LegacyRaw(batch) => DeferredIngressBatchFact {
                source_kinds: batch
                    .events
                    .iter()
                    .map(|event| event.event.source_kind.clone())
                    .collect(),
                recv_times_utc_ms: batch
                    .events
                    .iter()
                    .map(|event| event.event.recv_time_utc_ms)
                    .collect(),
                has_collector_admission: batch
                    .events
                    .iter()
                    .map(|event| event.event.collector_admission.is_some())
                    .collect(),
                accepted_defer: matches!(
                    batch.accepted_ticket,
                    Some(RawDayAcceptedTicket::Defer(_))
                ),
                typed_polymarket_v3: false,
            },
            DeferredIngressBatch::PolymarketOrderbookV3(event) => DeferredIngressBatchFact {
                source_kinds: vec!["polymarket_market".to_owned()],
                recv_times_utc_ms: vec![event.message.envelope.timestamp_received],
                has_collector_admission: vec![true],
                accepted_defer: matches!(event.accepted_ticket, RawDayAcceptedTicket::Defer(_)),
                typed_polymarket_v3: true,
            },
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds() {
    if delegate_resource_test_to_isolated_process(
        "live_runtime::tests::polymarket_source::gamma_outer_supervisor_survives_one_hundred_outer_cycles_with_bounded_fds",
    ) {
        return;
    }

    let counters = Arc::new(GammaRecoveryLoopbackCounters::default());
    let failed_attempts = u64::from(SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS.saturating_add(1)) * 100;
    let (gamma_endpoint, gamma_server) =
        gamma_recovery_loopback(failed_attempts, Arc::clone(&counters)).await;
    let (clob_endpoint, clob_server) = silent_clob_loopback(Arc::clone(&counters)).await;
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (control_tx, control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let mut spec = PolymarketSourceSpec::production();
    spec.gamma_endpoint = gamma_endpoint;
    spec.market_ws_endpoint = clob_endpoint;
    spec.discovery_wait = Duration::ZERO;
    spec.discovery_retry_delay = Duration::from_millis(1);
    spec.recovery_wait_override = Some(Duration::from_millis(1));
    spec.ws_config.connect_timeout = Some(Duration::from_millis(500));
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(100);
    let source = tokio::spawn(run_polymarket_source_with_spec(
        spec,
        dataset_root,
        SourceEventSink {
            tx: event_tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        CollectorIdentity {
            host_id: "collector-gamma-100".to_owned(),
            region: "local-test".to_owned(),
        },
        None,
        Instant::now(),
        control_rx,
        PolymarketRestartRecovery {
            pending: Vec::new(),
        },
    ));

    timeout(Duration::from_secs(5), async {
        loop {
            if counters.gamma_successes.load(Ordering::Relaxed) >= 2
                && counters.clob_subscriptions.load(Ordering::Relaxed) >= 1
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Gamma must recover and start one bounded next-market discovery");
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: true,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(2), reply_rx)
        .await
        .expect("Gamma source stop acknowledgement must be bounded")
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), source)
        .await
        .expect("Gamma source must converge after its coordinated stop")
        .unwrap()
        .unwrap();

    assert_eq!(
        counters.gamma_attempts.load(Ordering::Relaxed),
        failed_attempts + 2
    );
    assert_eq!(counters.gamma_successes.load(Ordering::Relaxed), 2);
    let metadata = event_rx.recv().await.unwrap();
    assert_eq!(metadata.event.source_kind, "polymarket_market_metadata");
    assert!(event_rx.try_recv().is_err());
    assert!(
        gaps.lock().unwrap().is_empty(),
        "Gamma availability recovery must not create confirmed gaps"
    );
    let operational = stats.drain_all_operational();
    let reconnects = operational
        .values()
        .filter_map(|by_source| by_source.get("polymarket_market"))
        .map(|counter| counter.reconnect_events)
        .sum::<u64>();
    assert_eq!(reconnects, failed_attempts);
    let fd_samples = counters.gamma_fd_samples.lock().unwrap().clone();
    assert_eq!(
        fd_samples.len(),
        usize::try_from(failed_attempts + 2).unwrap()
    );
    let minimum = *fd_samples.iter().min().unwrap();
    let maximum = *fd_samples.iter().max().unwrap();
    assert!(
        fd_samples.last().unwrap() <= &(fd_samples[0] + 2),
        "Gamma recovery descriptors did not return to their initial plateau: {fd_samples:?}"
    );
    assert!(
        maximum.saturating_sub(minimum) <= 8,
        "Gamma recovery leaked file descriptors: {fd_samples:?}"
    );
    gamma_server.abort();
    clob_server.abort();
    let _ = gamma_server.await;
    let _ = clob_server.await;
}

#[tokio::test]
async fn polymarket_source_spec_uses_private_gamma_endpoint_and_millisecond_deadline() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let read = socket.read(&mut request).await.unwrap();
        assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /markets"));
        let body = "[]";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let (tx, _rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (_control_tx, control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let mut spec = PolymarketSourceSpec::production();
    spec.gamma_endpoint = gamma_endpoint;
    spec.market_ws_endpoint = "ws://127.0.0.1:9/ws/market".to_owned();
    spec.discovery_wait = Duration::ZERO;
    spec.discovery_retry_delay = Duration::from_millis(1);
    let started = Instant::now();

    timeout(
        Duration::from_secs(1),
        run_polymarket_source_with_spec(
            spec,
            dataset_root,
            SourceEventSink {
                tx,
                gaps: Arc::clone(&gaps),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Some(now_ms() + 50),
            started,
            control_rx,
            PolymarketRestartRecovery {
                pending: Vec::new(),
            },
        ),
    )
    .await
    .expect("private discovery timing must bound the production source loop")
    .unwrap();
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(
        gaps.lock().unwrap().is_empty(),
        "valid empty discovery and subsequent network unavailability are not confirmed gaps"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn polymarket_source_recovers_through_gamma_network_rate_limit_and_empty_response() {
    let until_boundary_ms = 300_000_i64 - now_ms().rem_euclid(300_000);
    if until_boundary_ms <= 500 {
        sleep(Duration::from_millis(
            u64::try_from(until_boundary_ms + 10).unwrap(),
        ))
        .await;
    }
    let windows = discovery_windows(now_ms());
    let expected_slug = windows.current.slug.clone();
    let valid_body = gamma_market_response_for_window(&windows.current);

    let gamma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", gamma_listener.local_addr().unwrap());
    let gamma_server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for step in 0..4 {
            let (mut socket, _) = gamma_listener.accept().await.unwrap();
            let mut request = [0_u8; 4_096];
            let read = socket.read(&mut request).await.unwrap();
            requests.push(String::from_utf8_lossy(&request[..read]).to_string());
            if step == 0 {
                continue;
            }
            let (status, body) = match step {
                1 => ("429 Too Many Requests", r#"{"retry":"later"}"#.to_owned()),
                2 => ("200 OK", "[]".to_owned()),
                3 => ("200 OK", valid_body.clone()),
                _ => unreachable!(),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });

    let clob_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let clob_endpoint = format!("ws://{}", clob_listener.local_addr().unwrap());
    let clob_server = tokio::spawn(async move {
        let (stream, _) = clob_listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut subscription = None;
        while let Some(frame) = socket.next().await {
            let Ok(frame) = frame else {
                break;
            };
            match frame {
                Message::Text(text) if text == "PING" => {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                }
                Message::Text(text) => {
                    if let Ok(request) = serde_json::from_str::<Value>(&text)
                        && request.get("operation").is_some()
                    {
                        subscription = Some(request);
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        subscription
    });

    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let (_control_tx, control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let mut spec = PolymarketSourceSpec::production();
    spec.gamma_endpoint = gamma_endpoint;
    spec.market_ws_endpoint = clob_endpoint;
    spec.discovery_wait = Duration::from_secs(1);
    spec.discovery_retry_delay = Duration::from_millis(10);
    spec.ws_config.heartbeat_interval = Duration::from_millis(20);
    spec.ws_config.heartbeat_timeout = Duration::from_millis(100);

    timeout(
        Duration::from_secs(2),
        run_polymarket_source_with_spec(
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
            Some(now_ms() + 600),
            Instant::now(),
            control_rx,
            PolymarketRestartRecovery {
                pending: Vec::new(),
            },
        ),
    )
    .await
    .expect("Gamma availability recovery and the explicit source deadline must be bounded")
    .unwrap();

    let requests = gamma_server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| request.contains(&expected_slug))
    );
    let subscription = timeout(Duration::from_secs(1), clob_server)
        .await
        .expect("CLOB owner must close after the explicit source deadline")
        .unwrap()
        .expect("the recovered market must be subscribed once");
    assert_eq!(subscription["operation"], "subscribe");
    assert_eq!(subscription["assets_ids"], json!(["101", "102"]));

    let mut metadata = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if event.event.source_kind == "polymarket_market_metadata" {
            metadata.push(event.into_legacy_event());
        }
    }
    assert_eq!(metadata.len(), 1, "unavailable attempts must not emit rows");
    assert_eq!(metadata[0].source_identifier, expected_slug);
    assert!(gaps.lock().unwrap().is_empty());
    assert!(
        !stats
            .active_recovery_sources
            .lock()
            .unwrap()
            .contains("polymarket_market"),
        "a successful subscription install clears recovery without waiting for business data"
    );
}

#[tokio::test]
async fn gamma_rate_limit_is_availability_but_malformed_json_is_integrity() {
    for (status, body, expect_integrity) in [
        ("429 Too Many Requests", r#"{"retry":"later"}"#, false),
        ("200 OK", "{not-json", true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2_048];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let error = fetch_exact_slug_candidates(
            &GammaClient::new(&endpoint).unwrap(),
            "btc-updown-5m-1783392300",
        )
        .await
        .expect_err("the fixture must not produce a candidate batch");
        assert_eq!(
            matches!(error, PolymarketDiscoveryError::Integrity(_)),
            expect_integrity,
            "unexpected Gamma classification: {error}"
        );
        server.await.unwrap();
    }
}

#[test]
fn gamma_candidate_selection_separates_missing_from_contradictory_identity() {
    assert!(matches!(
        classify_discovery_selection_error(DiscoverySelectionError::MissingCurrentCandidate {
            expected_slug: "current".to_owned(),
        }),
        PolymarketDiscoveryError::Unavailable(_)
    ));
    for error in [
        DiscoverySelectionError::MultipleCurrentCandidates {
            expected_slug: "current".to_owned(),
            count: 2,
        },
        DiscoverySelectionError::InvalidCurrent {
            expected_slug: "current".to_owned(),
            candidate_slug: Some("current".to_owned()),
            reason: crate::market_discovery::CandidateRejectReason::InvalidConditionId,
        },
        DiscoverySelectionError::PairRelationInvalid,
    ] {
        assert!(matches!(
            classify_discovery_selection_error(error),
            PolymarketDiscoveryError::Integrity(_)
        ));
    }
}

#[test]
fn gamma_late_current_or_next_response_cannot_be_installed_after_window_advance() {
    let boundary_ms = 1_800_000_000_i64 * 1_000;
    let before_boundary_ms = boundary_ms - 1;
    let windows = discovery_windows(before_boundary_ms);
    let next_batch = GammaCandidateBatch {
        requested_slug: windows.next.slug,
        candidates: Vec::new(),
    };
    let current_batch = GammaCandidateBatch {
        requested_slug: windows.current.slug.clone(),
        candidates: Vec::new(),
    };
    let mut discovery = test_btc_discovery();
    discovery.current.slug = windows.current.slug;

    for (applied_at_ms, expected_current) in [
        (boundary_ms - 1, true),
        (boundary_ms, false),
        (boundary_ms + 1, false),
    ] {
        assert_eq!(
            gamma_batch_is_current_next(&next_batch, applied_at_ms),
            expected_current,
            "next response installation must be decided only by applied_at={applied_at_ms}"
        );
        assert_eq!(
            gamma_batch_is_current(&current_batch, applied_at_ms),
            expected_current,
            "current response installation must be decided only by applied_at={applied_at_ms}"
        );
        assert_eq!(
            gamma_discovery_is_current(&discovery, applied_at_ms),
            expected_current,
            "discovery installation must be decided only by applied_at={applied_at_ms}"
        );
    }
}

#[test]
fn late_polymarket_identity_eligibility_uses_causal_times_but_only_applied_time_for_window() {
    let market_start = 1_800_000_000_i64 * 1_000;
    let market_end = market_start + 300_000;
    for (name, received, verified, applied, expected) in [
        (
            "received_before_t_verified_after_t_applied_inside",
            market_start - 1,
            market_start + 1,
            market_start + 2,
            true,
        ),
        (
            "received_and_verified_before_e_applied_e_minus_one",
            market_end - 3,
            market_end - 2,
            market_end - 1,
            true,
        ),
        (
            "received_and_verified_before_e_applied_at_e",
            market_end - 2,
            market_end - 1,
            market_end,
            false,
        ),
        (
            "received_and_verified_before_e_applied_after_e",
            market_end - 2,
            market_end - 1,
            market_end + 1,
            false,
        ),
    ] {
        assert_eq!(
            late_polymarket_identity_can_activate(
                received,
                verified,
                applied,
                market_start,
                market_end,
            ),
            expected,
            "{name}"
        );
    }
}

#[tokio::test]
async fn initial_expired_gamma_discovery_retries_without_mutation_gap_or_false_recovery() {
    let mut stale = test_btc_discovery();
    stale.current.slug = discovery_windows(now_ms().saturating_sub(600_000))
        .current
        .slug;
    assert!(!gamma_discovery_is_current(&stale, now_ms()));

    let global_budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        "ws://127.0.0.1:9".to_owned(),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime.abort_pump_and_wait().await;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.set_recovery_active("polymarket_market", true);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(stale.clone());
    let original_slug = context.current.as_ref().unwrap().slug.clone();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let _ = recovery.next_wait();
    let _ = recovery.next_wait();
    let mut resolution_poll_tasks = JoinSet::new();
    let (_control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);

    let error = collect_polymarket_single_receiver(
        Some(stale),
        &mut context,
        &mut resolution_poll_tasks,
        &sink,
        &identity,
        None,
        Instant::now(),
        &mut runtime,
        &GammaClient::new("http://127.0.0.1:9").unwrap(),
        &mut recovery,
        &mut control_rx,
        Duration::from_millis(10),
        Duration::from_millis(1),
        production_polymarket_timeline_now,
    )
    .await
    .expect_err("an expired initial discovery must return to the availability loop");

    assert!(market_source_error_is_retryable(error.as_ref()));
    assert_eq!(context.current.as_ref().unwrap().slug, original_slug);
    assert_eq!(recovery.next_wait().attempt, 3);
    assert!(
        stats
            .active_recovery_sources
            .lock()
            .unwrap()
            .contains("polymarket_market")
    );
    assert!(gaps.lock().unwrap().is_empty());
    assert!(rx.try_recv().is_err());
}

#[test]
fn market_supervisor_retries_only_transport_exhaustion() {
    assert!(market_source_error_is_retryable(
        &SdkStreamError::TerminalDisconnected
    ));
    assert!(market_source_error_is_retryable(
        &SdkStreamError::Connection("network".to_owned())
    ));
    assert!(!market_source_error_is_retryable(&SdkStreamError::Lagged(
        1
    )));
    assert!(!market_source_error_is_retryable(&SdkStreamError::Parser(
        "schema".to_owned()
    )));
    assert!(!market_source_error_is_retryable(
        &SdkStreamError::SubscriptionActor("internal".to_owned())
    ));
    assert!(market_source_error_is_retryable(
        &PolymarketDiscoveryError::Unavailable("stale window".to_owned())
    ));
    assert!(!market_source_error_is_retryable(
        &PolymarketDiscoveryError::Integrity("contradictory identity".to_owned())
    ));
}

#[test]
fn polymarket_context_switch_preserves_pending_ownership() {
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let pending_identity = PendingPolymarketIdentity::from_discovered(&discovery.current)
        .expect("test discovery has fixed identity evidence");
    let pending_slug = pending_identity.slug.clone();
    context.pending.insert(
        pending_slug.clone(),
        PendingPolymarketResolution::new(pending_identity, None),
    );

    let mut next = discovery.current.clone();
    next.id = "market-next".to_owned();
    next.slug = "btc-updown-5m-1783392600".to_owned();
    next.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    next.up_asset_id = "201".to_owned();
    next.down_asset_id = "202".to_owned();
    next.start_ts_s = 1_783_392_600;
    next.end_ts_s = 1_783_392_900;
    next.candidate_start_ts_s = next.start_ts_s;
    next.candidate_end_ts_s = next.end_ts_s;
    let mut with_next = discovery;
    with_next.next = Some(next.clone());
    context.replace_discovery(with_next);

    assert!(context.pending.contains_key(&pending_slug));

    let rollover = BtcUpdownDiscovery {
        current: next,
        next: None,
        relation: None,
    };
    context.replace_discovery(rollover);
    assert!(context.pending.contains_key(&pending_slug));
}

fn string_with_spare_capacity(value: &str, spare: usize) -> String {
    let mut owned = String::with_capacity(value.len().saturating_add(spare));
    owned.push_str(value);
    assert!(owned.capacity() > owned.len());
    owned
}

fn expected_accepted_polymarket_metadata_dynamic_bytes(
    accepted: &AcceptedPolymarketMetadataRow,
) -> usize {
    [
        accepted.row.collector_host_id.capacity(),
        accepted.row.collector_region.capacity(),
        accepted.row.schema_version.capacity(),
        accepted.row.slug.capacity(),
        accepted.row.condition_id.capacity(),
        accepted.row.clob_market_id.capacity(),
        accepted.row.asset_ids.capacity(),
        accepted
            .row
            .source_endpoint
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .source_evidence_payload_json
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .source_evidence_raw_sha256
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .closed_time_raw
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .uma_resolution_status
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .outcomes_raw
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .outcome_prices_raw
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .ws_platform_timestamp_raw
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .winning_asset_id
            .as_ref()
            .map_or(0, String::capacity),
        accepted
            .row
            .winning_outcome
            .as_ref()
            .map_or(0, String::capacity),
        accepted.raw_day_ticket.dynamic_owned_bytes(),
        accepted
            .ws_claimant_source_sequence_epoch_id
            .as_ref()
            .map_or(0, String::capacity),
    ]
    .into_iter()
    .sum()
}

#[test]
fn polymarket_pending_owned_bytes_counts_each_allocation_once() {
    let discovery = test_btc_discovery();
    let mut identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    identity.slug = string_with_spare_capacity("pending-slug", 31);
    identity.clob_market_id = string_with_spare_capacity("clob", 29);
    identity.condition_id = string_with_spare_capacity("condition", 27);
    identity.up_asset_id = string_with_spare_capacity("up", 25);
    identity.down_asset_id = string_with_spare_capacity("down", 23);
    identity.discovery_source_raw_sha256 = string_with_spare_capacity("digest", 21);

    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut row = polymarket_metadata_snapshot_v3_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &collector_identity,
    )
    .unwrap();
    row.collector_host_id = string_with_spare_capacity("host", 19);
    row.collector_region = string_with_spare_capacity("region", 18);
    row.schema_version = string_with_spare_capacity("schema", 17);
    row.slug = string_with_spare_capacity("row-slug", 16);
    row.condition_id = string_with_spare_capacity("row-condition", 15);
    row.clob_market_id = string_with_spare_capacity("row-clob", 14);
    row.asset_ids = string_with_spare_capacity("[\"up\",\"down\"]", 13);
    row.source_endpoint = Some(string_with_spare_capacity("endpoint", 12));
    row.source_evidence_payload_json = Some(string_with_spare_capacity("{\"closed\":true}", 11));
    row.source_evidence_raw_sha256 = Some(string_with_spare_capacity("source-digest", 10));
    row.closed_time_raw = Some(string_with_spare_capacity("closed-time", 9));
    row.uma_resolution_status = Some(string_with_spare_capacity("resolved", 8));
    row.outcomes_raw = Some(string_with_spare_capacity("[\"Up\",\"Down\"]", 7));
    row.outcome_prices_raw = Some(string_with_spare_capacity("[\"1\",\"0\"]", 6));
    row.ws_platform_timestamp_raw = Some(string_with_spare_capacity("timestamp", 5));
    row.winning_asset_id = Some(string_with_spare_capacity("winner", 4));
    row.winning_outcome = Some(string_with_spare_capacity("Up", 3));
    let accepted = AcceptedPolymarketMetadataRow {
        row,
        raw_day_ticket: RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
            utc_day: string_with_spare_capacity("2026-07-26", 9),
            old_generation: 1,
            expected_generation: 2,
        }),
        ws_claimant_source_sequence_epoch_id: Some(string_with_spare_capacity("claimant", 8)),
        ws_claimant_collector_admission_sequence: Some(7),
    };

    let mut pending = PendingPolymarketResolution::new(identity, None);
    pending.buffered_attempt_rows = Vec::with_capacity(4);
    pending.buffered_attempt_rows.push(accepted);
    pending.exhaustion_ticket = Some(RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
        utc_day: string_with_spare_capacity("2026-07-27", 7),
        old_generation: 2,
        expected_generation: 3,
    }));
    pending.recovery_missed_ordinals = VecDeque::with_capacity(9);
    pending.recovery_missed_ordinals.extend([2, 3]);

    let identity_dynamic_bytes = [
        pending.identity.slug.capacity(),
        pending.identity.clob_market_id.capacity(),
        pending.identity.condition_id.capacity(),
        pending.identity.up_asset_id.capacity(),
        pending.identity.down_asset_id.capacity(),
        pending.identity.discovery_source_raw_sha256.capacity(),
    ]
    .into_iter()
    .sum::<usize>();
    let expected_without_claim = std::mem::size_of::<PendingPolymarketResolution>()
        + identity_dynamic_bytes
        + pending.buffered_attempt_rows.capacity()
            * std::mem::size_of::<AcceptedPolymarketMetadataRow>()
        + pending
            .buffered_attempt_rows
            .iter()
            .map(expected_accepted_polymarket_metadata_dynamic_bytes)
            .sum::<usize>()
        + pending
            .exhaustion_ticket
            .as_ref()
            .unwrap()
            .dynamic_owned_bytes()
        + pending.recovery_missed_ordinals.capacity() * std::mem::size_of::<u8>();
    let slug = pending.identity.slug.clone();
    let pending_key = string_with_spare_capacity(&slug, 37);
    let pending_key_bytes = std::mem::size_of::<String>() + pending_key.capacity();
    let mut context = PolymarketProcessingContext::new(discovery);
    context.pending.insert(pending_key, pending);
    assert_eq!(
        context.pending_owned_bytes(),
        expected_without_claim + pending_key_bytes,
        "the map key must be counted while the inline identity fixed size is counted once"
    );

    let gamma_claim = context.pending[&slug].buffered_attempt_rows[0].clone();
    let expected_gamma_claim = std::mem::size_of::<AcceptedPolymarketMetadataRow>()
        + expected_accepted_polymarket_metadata_dynamic_bytes(&gamma_claim);
    context.pending.get_mut(&slug).unwrap().claim =
        Some(PolymarketTerminalClaim::GammaBySlug(Box::new(gamma_claim)));
    assert_eq!(
        context.pending_owned_bytes(),
        expected_without_claim + pending_key_bytes + expected_gamma_claim,
        "the Gamma Box must include its pointee fixed size and dynamic allocations"
    );

    let ws_evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: string_with_spare_capacity("source-epoch", 12),
        collector_admission_sequence: 8,
        source_received_at_utc_ms: 1_783_392_180_050,
        validation_completed_at_utc_ms: 1_783_392_180_051,
        source_payload_json: string_with_spare_capacity("{\"event_type\":\"market_resolved\"}", 11),
        source_payload_sha256: string_with_spare_capacity("payload-digest", 10),
        platform_timestamp_raw: string_with_spare_capacity("1783392180050", 9),
        platform_timestamp_utc_ms: 1_783_392_180_050,
        winning_asset_id: string_with_spare_capacity("winning-asset", 8),
        winning_outcome: string_with_spare_capacity("Up", 7),
        raw_day_ticket: RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
            utc_day: string_with_spare_capacity("2026-07-28", 6),
            old_generation: 3,
            expected_generation: 4,
        }),
    };
    let expected_ws_dynamic = [
        ws_evidence.source_sequence_epoch_id.capacity(),
        ws_evidence.source_payload_json.capacity(),
        ws_evidence.source_payload_sha256.capacity(),
        ws_evidence.platform_timestamp_raw.capacity(),
        ws_evidence.winning_asset_id.capacity(),
        ws_evidence.winning_outcome.capacity(),
        ws_evidence.raw_day_ticket.dynamic_owned_bytes(),
    ]
    .into_iter()
    .sum::<usize>();
    let expected_ws_claim =
        std::mem::size_of::<PolymarketWsTerminalEvidence>() + expected_ws_dynamic;
    context.pending.get_mut(&slug).unwrap().claim = Some(
        PolymarketTerminalClaim::MarketResolvedWs(Box::new(ws_evidence)),
    );
    assert_eq!(
        context.pending_owned_bytes(),
        expected_without_claim + pending_key_bytes + expected_ws_claim,
        "the WS Box must include its pointee fixed size and dynamic allocations"
    );

    let authority = PolymarketMetadataV3ReferenceRecord::from(
        &context.pending[&slug].buffered_attempt_rows[0].row,
    );
    let authority_dynamic = [
        authority.metadata_row_kind.capacity(),
        authority.slug.capacity(),
        authority.condition_id.capacity(),
        authority.clob_market_id.capacity(),
        authority.asset_ids.capacity(),
        authority
            .source_evidence_origin
            .as_ref()
            .map_or(0, String::capacity),
        authority
            .source_evidence_raw_sha256
            .as_ref()
            .map_or(0, String::capacity),
    ]
    .into_iter()
    .sum::<usize>();
    context.source_authorities = Vec::with_capacity(4);
    context.source_authorities.push(authority);
    let authority_backing = context.source_authorities.capacity()
        * std::mem::size_of::<PolymarketMetadataV3ReferenceRecord>();
    assert_eq!(
        context.pending_owned_bytes(),
        expected_without_claim
            + pending_key_bytes
            + expected_ws_claim
            + authority_backing
            + authority_dynamic,
        "source-authority Vec spare capacity and initialized dynamic allocations must be counted"
    );
}

#[test]
fn polymarket_pending_union_and_terminal_claim_stay_within_frozen_bounds() {
    let mut current = test_btc_discovery().current;
    let mut next = current.clone();
    next.id = "market-next".to_owned();
    next.slug = "btc-updown-5m-1783392600".to_owned();
    next.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    next.up_asset_id = "201".to_owned();
    next.down_asset_id = "202".to_owned();
    next.start_ts_s += 300;
    next.end_ts_s += 300;
    next.candidate_start_ts_s += 300;
    next.candidate_end_ts_s += 300;
    refresh_test_discovery_evidence(&mut next, 1_783_392_180_020);

    let mut pending_one = current.clone();
    pending_one.id = "market-pending-one".to_owned();
    pending_one.slug = "btc-updown-5m-1783392000".to_owned();
    pending_one.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000004".to_owned();
    pending_one.up_asset_id = "301".to_owned();
    pending_one.down_asset_id = "302".to_owned();
    pending_one.start_ts_s -= 300;
    pending_one.end_ts_s -= 300;
    pending_one.candidate_start_ts_s -= 300;
    pending_one.candidate_end_ts_s -= 300;
    refresh_test_discovery_evidence(&mut pending_one, 1_783_392_180_030);

    let mut pending_two = pending_one.clone();
    pending_two.id = "market-pending-two".to_owned();
    pending_two.slug = "btc-updown-5m-1783391700".to_owned();
    pending_two.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000005".to_owned();
    pending_two.up_asset_id = "401".to_owned();
    pending_two.down_asset_id = "402".to_owned();
    pending_two.start_ts_s -= 300;
    pending_two.end_ts_s -= 300;
    pending_two.candidate_start_ts_s -= 300;
    pending_two.candidate_end_ts_s -= 300;
    refresh_test_discovery_evidence(&mut pending_two, 1_783_392_180_040);

    refresh_test_discovery_evidence(&mut current, 1_783_392_180_010);
    let mut context = PolymarketProcessingContext::new(BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(next.clone()),
        relation: None,
    });
    for market in [&pending_one, &pending_two] {
        let identity = PendingPolymarketIdentity::from_discovered(market).unwrap();
        context.pending.insert(
            identity.slug.clone(),
            PendingPolymarketResolution::new(identity, None),
        );
    }
    context.rebuild_asset_to_slug();
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let authorities = [
        (&current, MarketRuntimeRole::Current),
        (&next, MarketRuntimeRole::Next),
        (&pending_one, MarketRuntimeRole::Current),
        (&pending_two, MarketRuntimeRole::Current),
    ]
    .into_iter()
    .map(|(market, role)| {
        let row = polymarket_metadata_snapshot_v3_row(market, role, &collector_identity).unwrap();
        PolymarketMetadataV3ReferenceRecord::from(&row)
    })
    .collect::<Vec<_>>();
    for authority in authorities.iter().cloned() {
        context.register_source_authority(authority).unwrap();
    }

    assert_eq!(context.desired_assets().unwrap().len(), 8);
    assert_eq!(context.pending.len(), 2);
    assert_eq!(context.source_authorities.len(), 4);
    assert!(
        context
            .register_source_authority(authorities[0].clone())
            .unwrap_err()
            .to_string()
            .contains("source authority bound exceeded")
    );
    assert!(context.pending_owned_bytes() < POLYMARKET_PENDING_STATE_MAX_BYTES);
    context.ensure_pending_bounds().unwrap();

    let mut third = PendingPolymarketIdentity::from_discovered(&pending_two).unwrap();
    third.slug = "btc-updown-5m-1783391400".to_owned();
    let third_slug = third.slug.clone();
    context.pending.insert(
        third_slug.clone(),
        PendingPolymarketResolution::new(third, None),
    );
    assert!(
        context
            .ensure_pending_bounds()
            .unwrap_err()
            .to_string()
            .contains("pending market bound exceeded")
    );

    context.pending.remove(&third_slug);
    let oversized_payload = "x".repeat(POLYMARKET_PENDING_STATE_MAX_BYTES);
    context.early_ws_claims.insert(
        pending_one.slug.clone(),
        PolymarketWsTerminalEvidence {
            source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
            collector_admission_sequence: 1,
            source_received_at_utc_ms: 1_783_392_180_050,
            validation_completed_at_utc_ms: 1_783_392_180_051,
            source_payload_sha256: "digest".to_owned(),
            source_payload_json: oversized_payload,
            platform_timestamp_raw: "1783392180050".to_owned(),
            platform_timestamp_utc_ms: 1_783_392_180_050,
            winning_asset_id: pending_one.up_asset_id.clone(),
            winning_outcome: "Up".to_owned(),
            raw_day_ticket: accepted_raw_day_ticket_for_sync_test(1_783_392_180_050),
        },
    );
    assert!(
        context
            .ensure_pending_bounds()
            .unwrap_err()
            .to_string()
            .contains("pending state byte bound exceeded"),
        "early WS claims must participate in the same sub-1MiB bound"
    );
}

#[tokio::test]
async fn polymarket_recovered_pending_admits_missed_evidence_before_future_slots() {
    let market = test_btc_discovery().current;
    let recovered = recovered_pending_for_test(&market, vec![1], vec![2], 3);
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered],
    })
    .unwrap()
    .unwrap();
    assert!(context.current.is_none());
    assert!(context.next.is_none());
    assert_eq!(context.desired_assets().unwrap().len(), 2);

    let observed_at_utc_ms = context.pending[&market.slug].slot_utc_ms(2) + 1;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, observed_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    admit_recovered_polymarket_missed_slots(
        observed_at_utc_ms,
        &mut context,
        &SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        &CollectorIdentity {
            host_id: "collector-restart".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
    )
    .await
    .unwrap();

    let event = rx.recv().await.unwrap().into_legacy_event();
    let rows = polymarket_metadata_v5_prefixes_from_event(&event).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].resolution_poll_attempt_ordinal, Some(2));
    assert_eq!(
        rows[0].resolution_poll_result,
        Some(ResolutionPollResult::MissedDueToDowntime)
    );
    assert!(rows[0].metadata_fetch_start_utc.is_none());
    assert!(rows[0].source_evidence_received_at_utc.is_none());
    assert!(rows[0].source_evidence_payload_json.is_none());
    assert!(rows[0].terminal_predicate_passed.is_none());
    let pending = &context.pending[&market.slug];
    assert_eq!(pending.attempts_recorded, 2);
    assert_eq!(pending.missed_slot_count, 1);
    assert_eq!(pending.next_attempt_ordinal, 3);
    assert!(pending.recovery_missed_ordinals.is_empty());
}

#[test]
fn polymarket_recovered_pending_reclassifies_elapsed_slots_before_probe() {
    let market = test_btc_discovery().current;
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            2,
        )],
    })
    .unwrap()
    .unwrap();
    let observed_at_utc_ms = context.pending[&market.slug].slot_utc_ms(3);
    context
        .reclassify_recovered_pending(observed_at_utc_ms)
        .unwrap();
    let pending = &context.pending[&market.slug];
    assert_eq!(
        pending
            .recovery_missed_ordinals
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(pending.next_attempt_ordinal, 4);
    assert!(!pending.recovery_reclassification_pending);

    let mut expired = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            6,
        )],
    })
    .unwrap()
    .unwrap();
    let final_slot_utc_ms = expired.pending[&market.slug].slot_utc_ms(6);
    expired
        .reclassify_recovered_pending(final_slot_utc_ms)
        .unwrap();
    assert!(expired.pending.is_empty());
    assert!(expired.desired_assets().unwrap().is_empty());
}

#[tokio::test]
async fn polymarket_recovered_finalize_allows_fresh_v3_delivery_after_install() {
    let discovery = test_btc_discovery();
    let market = discovery.current.clone();
    let fresh_market = next_market_for_test(&market);
    let fresh_discovery = BtcUpdownDiscovery {
        current: fresh_market.clone(),
        next: None,
        relation: None,
    };
    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let terminal_received_at_utc_ms = identity.market_end_utc_ms + 1;
    let actor_now_utc_ms = terminal_received_at_utc_ms + 1;
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    context.actor_now_utc_ms = actor_now_utc_ms;
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    {
        let pending = context.pending.get_mut(&identity.slug).unwrap();
        pending.recovery_reclassification_pending = false;
        pending.claim = Some(PolymarketTerminalClaim::MarketResolvedWs(Box::new(
            PolymarketWsTerminalEvidence {
                source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
                collector_admission_sequence: 1,
                source_received_at_utc_ms: terminal_received_at_utc_ms,
                validation_completed_at_utc_ms: terminal_received_at_utc_ms,
                source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                source_payload_json,
                platform_timestamp_raw: terminal_received_at_utc_ms.to_string(),
                platform_timestamp_utc_ms: terminal_received_at_utc_ms,
                winning_asset_id: identity.up_asset_id.clone(),
                winning_outcome: "Up".to_owned(),
                raw_day_ticket: accepted_raw_day_ticket_for_test(terminal_received_at_utc_ms).await,
            },
        )));
    }

    let book = serde_json::from_value::<WsMessage>(json!({
        "event_type": "book",
        "asset_id": fresh_market.up_asset_id,
        "market": fresh_market.condition_id,
        "timestamp": actor_now_utc_ms.to_string(),
        "bids": [],
        "asks": [],
        "hash": "recovered-book"
    }))
    .unwrap();

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, actor_now_utc_ms).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };
    finalize_polymarket_outer_ready_pending(
        &timeline,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
    )
    .await
    .unwrap();
    assert!(context.pending.is_empty());
    assert!(context.current.is_none());

    let counters = Arc::new(GammaRecoveryLoopbackCounters::default());
    let (clob_endpoint, clob_server) = silent_clob_loopback(counters).await;
    let mut runtime = PolymarketDrainRuntime::new(
        clob_endpoint,
        bounded_ws_config(),
        InflightBudget::new(),
        active_raw_day_gate_for_test(actor_now_utc_ms).await,
    )
    .unwrap();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let mut resolution_poll_tasks = JoinSet::new();
    install_polymarket_context(
        fresh_discovery,
        false,
        &mut runtime,
        &mut context,
        &mut resolution_poll_tasks,
        &sink,
        &collector_identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .unwrap();
    while event_rx.try_recv().is_ok() {}

    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    process_polymarket_budgeted_message(
        budgeted_polymarket_message_at_for_test(
            book,
            actor_now_utc_ms + 1,
            &global_budget,
            &decoded_budget,
        )
        .await,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .unwrap();
    let payload = event_rx
        .try_recv()
        .expect("fresh post-install delivery must use current v3 ingress");
    let message = polymarket_v3_message(&payload).expect("delivery must be Polymarket v3");
    assert_eq!(
        message.envelope.slug.as_deref(),
        Some(fresh_market.slug.as_str())
    );
    assert!(matches!(message.event, PolymarketOrderbookV3Event::Book(_)));
    assert!(event_rx.try_recv().is_err());
    let duplicate_drops = stats
        .drain_all_operational()
        .values()
        .filter_map(|sources| sources.get("polymarket_market"))
        .map(|counter| counter.structural_dedupe_dropped_events)
        .sum::<u64>();
    assert_eq!(duplicate_drops, 0);
    runtime.abort_pump_and_wait().await;
    clob_server.abort();
}

#[tokio::test]
async fn polymarket_hour_budget_and_two_task_ownership_are_hard_bounded() {
    assert_eq!(12_u32 * u32::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT), 72);

    let first = test_btc_discovery().current;
    let mut second = first.clone();
    second.id = "market-recovered-two".to_owned();
    second.slug = "btc-updown-5m-1783391700".to_owned();
    second.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000005".to_owned();
    second.up_asset_id = "401".to_owned();
    second.down_asset_id = "402".to_owned();
    second.start_ts_s -= 300;
    second.end_ts_s -= 300;
    second.candidate_start_ts_s -= 300;
    second.candidate_end_ts_s -= 300;
    refresh_test_discovery_evidence(&mut second, first.metadata_received_utc_ms.unwrap() - 1);
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![
            recovered_pending_for_test(&first, Vec::new(), Vec::new(), 1),
            recovered_pending_for_test(&second, Vec::new(), Vec::new(), 1),
        ],
    })
    .unwrap()
    .unwrap();
    let mut tasks = JoinSet::<GammaResolutionPollCompletion>::new();
    for (ordinal, pending) in context.pending.values_mut().enumerate() {
        let handle = tasks.spawn(std::future::pending());
        pending.in_flight_ordinal = Some(u8::try_from(ordinal + 1).unwrap());
        pending.poll_abort_handle = Some(handle);
    }
    ensure_polymarket_resolution_task_bound(&tasks, &context).unwrap();

    let extra = tasks.spawn(std::future::pending());
    assert!(
        ensure_polymarket_resolution_task_bound(&tasks, &context)
            .unwrap_err()
            .to_string()
            .contains("tasks=3")
    );
    extra.abort();
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[test]
fn polymarket_many_recovered_lifecycles_release_actor_owned_state() {
    let market = test_btc_discovery().current;
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    context.pending.clear();
    context.rebuild_asset_to_slug();

    for index in 0..100_u32 {
        let mut identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
        identity.slug = format!("{}-{index}", market.slug);
        identity.clob_market_id = format!("{}-{index}", market.id);
        identity.condition_id = format!("{}-{index}", market.condition_id);
        identity.up_asset_id = format!("{}{}", index + 10, 1);
        identity.down_asset_id = format!("{}{}", index + 10, 2);
        let slug = identity.slug.clone();
        context.pending.insert(
            slug.clone(),
            PendingPolymarketResolution::new(identity, None),
        );
        context.rebuild_asset_to_slug();
        context.ensure_pending_bounds().unwrap();
        assert_eq!(context.pending.len(), 1);
        assert_eq!(context.desired_assets().unwrap().len(), 2);

        context.pending.remove(&slug);
        context.rebuild_asset_to_slug();
        assert!(context.pending.is_empty());
        assert!(context.desired_assets().unwrap().is_empty());
        assert_eq!(context.pending_owned_bytes(), 0);
    }
}

#[tokio::test]
async fn polymarket_boundary_rejects_missing_source_authority_before_state_or_admission() {
    let discovery = test_btc_discovery();
    let expected_current = discovery.current.clone();
    let boundary_utc_ms = expected_current.end_ts_s * 1_000;
    let mut context = PolymarketProcessingContext::new(discovery);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, boundary_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let error = advance_polymarket_boundaries(
        boundary_utc_ms,
        &mut context,
        &SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("polymarket_metadata_source_reference_integrity")
    );
    assert_eq!(context.current.as_ref(), Some(&expected_current));
    assert!(context.pending.is_empty());
    assert!(context.early_ws_claims.is_empty());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn polymarket_deferred_source_row_does_not_create_pre_admission_authority() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let _close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_raw_day_gate(gate).unwrap();
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let discovery = test_btc_discovery();
    let mut row = polymarket_metadata_snapshot_v3_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();
    row.timestamp_received = boundary_utc_ms;
    row.source_evidence_received_at_utc = Some(boundary_utc_ms);
    row.row_evidence_validation_completed_at_utc = Some(boundary_utc_ms);
    let error = send_polymarket_metadata_v3_source_authority_row(&sink, Instant::now(), row)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("utc_deferred"));

    let context = PolymarketProcessingContext::new(discovery);
    assert!(context.source_authorities.is_empty());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        deferred_ingress_facts(&stats, "polymarket"),
        vec![DeferredIngressBatchFact {
            source_kinds: vec!["polymarket_market_metadata".to_owned()],
            recv_times_utc_ms: vec![boundary_utc_ms],
            has_collector_admission: vec![false],
            accepted_defer: false,
            typed_polymarket_v3: false,
        }]
    );
}

#[tokio::test]
async fn polymarket_late_candidate_row_becomes_the_e_transition_source_authority() {
    let discovery = test_btc_discovery();
    let market = discovery.current.clone();
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let applied_at_utc_ms = market.start_ts_s * 1_000 + 1;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, applied_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let activation =
        polymarket_candidate_to_active_v3_row(&market, applied_at_utc_ms, &collector_identity)
            .unwrap();
    let authority =
        send_polymarket_metadata_v3_source_authority_row(&sink, Instant::now(), activation)
            .await
            .unwrap();
    let mut context = PolymarketProcessingContext::new(discovery);
    context.register_source_authority(authority).unwrap();

    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let e_transition = polymarket_market_state_transition_v3_row(
        &identity,
        MarketRuntimeRoleTransition::CurrentToResolutionPending,
        market.end_ts_s * 1_000,
        &collector_identity,
    )
    .unwrap();
    context
        .validate_local_transition_authority(&e_transition)
        .unwrap();
    assert_eq!(context.source_authorities.len(), 1);
    let event = rx.recv().await.unwrap().into_legacy_event();
    assert_eq!(
        event.source_event_type,
        PolymarketMetadataRowKind::MarketStateTransition.as_str()
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_market_resolved_at_e_moves_early_claim_into_pending_once() {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let boundary_utc_ms = current.end_ts_s * 1_000;
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    context.actor_now_utc_ms = boundary_utc_ms + 1;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, boundary_utc_ms).await;
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let (tx, mut rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let authority = send_polymarket_metadata_event(
        &sink,
        Instant::now(),
        &current,
        "current",
        &collector_identity,
    )
    .await
    .unwrap();
    context.register_source_authority(authority).unwrap();
    let snapshot = rx.recv().await.unwrap().into_legacy_event();
    assert_eq!(
        snapshot.source_event_type,
        PolymarketMetadataRowKind::MetadataSnapshot.as_str()
    );

    let resolved_json = json!({
        "id": current.id,
        "market": current.condition_id,
        "slug": current.slug,
        "assets_ids": [current.up_asset_id, current.down_asset_id],
        "outcomes": ["Up", "Down"],
        "winning_asset_id": "101",
        "winning_outcome": "Up",
        "timestamp": boundary_utc_ms.to_string(),
        "event_type": "market_resolved"
    });
    let message = || serde_json::from_value::<WsMessage>(resolved_json.clone()).unwrap();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let run_started = Instant::now();
    for _ in 0..2 {
        process_polymarket_budgeted_message(
            budgeted_polymarket_message_at_for_test(
                message(),
                boundary_utc_ms,
                &global_budget,
                &decoded_budget,
            )
            .await,
            &mut context,
            &sink,
            &collector_identity,
            run_started,
            &mut recovery,
        )
        .await
        .unwrap();
        let typed = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
        assert!(matches!(
            typed.message.event,
            PolymarketOrderbookV3Event::MarketResolved(_)
        ));
    }
    assert_eq!(context.early_ws_claims.len(), 1);
    assert!(context.pending.is_empty());
    let first_digest = context.early_ws_claims[&current.slug]
        .source_payload_sha256
        .clone();

    assert!(
        advance_polymarket_boundaries(
            boundary_utc_ms,
            &mut context,
            &sink,
            &collector_identity,
            run_started,
        )
        .await
        .unwrap()
    );
    assert!(context.early_ws_claims.is_empty());
    assert!(context.current.is_none());
    let pending = &context.pending[&current.slug];
    assert!(pending.requires_fence());
    let Some(PolymarketTerminalClaim::MarketResolvedWs(evidence)) = &pending.claim else {
        panic!("E-boundary early claim must become the unique pending WS owner");
    };
    assert_eq!(evidence.source_payload_sha256, first_digest);
    let desired_assets = context.desired_assets().unwrap();
    assert!(desired_assets.contains(&U256::from_str(&current.up_asset_id).unwrap()));
    assert!(desired_assets.contains(&U256::from_str(&current.down_asset_id).unwrap()));
    let transition = rx.recv().await.unwrap().into_legacy_event();
    assert_eq!(
        transition.source_event_type,
        PolymarketMetadataRowKind::MarketStateTransition.as_str()
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_qualified_market_resolved_before_e_is_not_lost() {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let boundary_utc_ms = current.end_ts_s * 1_000;
    let received_at_utc_ms = boundary_utc_ms - 1;
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    context.actor_now_utc_ms = received_at_utc_ms;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, received_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let event = match serde_json::from_value::<WsMessage>(json!({
        "id": current.id,
        "market": current.condition_id,
        "slug": current.slug,
        "assets_ids": [current.up_asset_id, current.down_asset_id],
        "outcomes": ["Up", "Down"],
        "winning_asset_id": "101",
        "winning_outcome": "Up",
        "timestamp": received_at_utc_ms.to_string(),
        "event_type": "market_resolved"
    }))
    .unwrap()
    {
        WsMessage::MarketResolved(event) => event,
        _ => panic!("expected market_resolved message"),
    };

    process_polymarket_market_resolved(
        &event,
        received_at_utc_ms,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
        accepted_raw_day_ticket_for_test(received_at_utc_ms).await,
    )
    .await
    .unwrap();

    let typed = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    assert!(matches!(
        typed.message.event,
        PolymarketOrderbookV3Event::MarketResolved(_)
    ));
    assert!(context.pending.is_empty());
    let evidence = &context.early_ws_claims[&current.slug];
    assert_eq!(evidence.source_received_at_utc_ms, received_at_utc_ms);
    assert_eq!(evidence.validation_completed_at_utc_ms, received_at_utc_ms);
    assert!(rx.try_recv().is_err());
}

#[test]
fn polymarket_v3_resolved_target_admission_covers_current_next_and_recovery_pending() {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let next = next_market_for_test(&current);
    let current_context = PolymarketProcessingContext::new(BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(next.clone()),
        relation: None,
    });

    for market in [&current, &next] {
        let WsMessage::MarketResolved(event) =
            market_resolved_message_for_test(market, market.end_ts_s * 1_000)
        else {
            panic!("fixture must be market_resolved");
        };
        let projection = target_admitted_market_resolved_v3_projection(&event, &current_context)
            .unwrap()
            .expect("current and next identities must be target-admitted");
        assert_eq!(projection.slug.as_deref(), Some(market.slug.as_str()));
        let PolymarketOrderbookV3Event::MarketResolved(resolved) = projection.event else {
            panic!("target admission must preserve the market_resolved variant");
        };
        assert_eq!(resolved.clob_market_id, market.id);
        assert_eq!(resolved.winning_asset_id, market.up_asset_id);
    }

    let WsMessage::MarketResolved(mut case_variant) =
        market_resolved_message_for_test(&current, current.end_ts_s * 1_000)
    else {
        panic!("fixture must be market_resolved");
    };
    case_variant.outcomes = vec!["UP".to_owned(), "DOWN".to_owned()];
    case_variant.winning_outcome = Some(" up ".to_owned());
    let case_projection =
        target_admitted_market_resolved_v3_projection(&case_variant, &current_context)
            .unwrap()
            .expect("source admission must use the shared case-insensitive outcome contract");
    let case_message = case_projection
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: current.end_ts_s * 1_000,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            source_sequence_epoch_id: "source-epoch-case".to_owned(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: current.end_ts_s * 1_000 + 1,
        })
        .unwrap();
    let case_batch = crate::polymarket_orderbook_v3::polymarket_orderbook_v3_messages_to_batch(
        std::slice::from_ref(&case_message),
    )
    .unwrap();
    assert_eq!(
        crate::polymarket_orderbook_v3::polymarket_orderbook_v3_messages_from_batch(&case_batch)
            .unwrap(),
        [case_message]
    );

    let pending_context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &current,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    let WsMessage::MarketResolved(pending_event) =
        market_resolved_message_for_test(&current, current.end_ts_s * 1_000)
    else {
        panic!("fixture must be market_resolved");
    };
    assert!(
        target_admitted_market_resolved_v3_projection(&pending_event, &pending_context)
            .unwrap()
            .is_some()
    );

    let unmatched_context = PolymarketProcessingContext::new(discovery);
    let WsMessage::MarketResolved(unmatched_event) =
        market_resolved_message_for_test(&next, next.end_ts_s * 1_000)
    else {
        panic!("fixture must be market_resolved");
    };
    assert!(
        target_admitted_market_resolved_v3_projection(&unmatched_event, &unmatched_context)
            .unwrap()
            .is_none()
    );

    let mut contradictory = pending_event;
    contradictory.id = "wrong-market".to_owned();
    let error = target_admitted_market_resolved_v3_projection(&contradictory, &unmatched_context)
        .expect_err("candidate routing must not bypass fixed-identity validation");
    assert!(error.to_string().contains("market id mismatch"));
    assert!(unmatched_context.pending.is_empty());
    assert!(unmatched_context.early_ws_claims.is_empty());
}

fn polymarket_v3_resolved_projection_for_admission(
    recv_time_utc_ms: i64,
) -> PolymarketOrderbookV3Projection {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let context = PolymarketProcessingContext::new(discovery);
    let WsMessage::MarketResolved(event) =
        market_resolved_message_for_test(&current, recv_time_utc_ms)
    else {
        panic!("fixture must be market_resolved");
    };
    target_admitted_market_resolved_v3_projection(&event, &context)
        .unwrap()
        .expect("fixture must pass complete target admission")
}

#[tokio::test]
async fn polymarket_v3_admission_receipt_matches_the_queued_canonical_message() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let ticket = accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(2, 2 * MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-admission".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let projection = polymarket_v3_resolved_projection_for_admission(recv_time_utc_ms);
    let validation_completed_utc_ms = now_ms();

    let receipt = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || Ok(projection),
    )
    .await
    .unwrap();
    let call_returned_utc_ms = now_ms();
    assert!(recv_time_utc_ms <= receipt.collector_admission_time_utc_ms);
    assert!(validation_completed_utc_ms <= receipt.collector_admission_time_utc_ms);
    assert!(receipt.collector_admission_time_utc_ms <= call_returned_utc_ms);
    let inflight = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    assert_eq!(
        receipt.collector_admission_sequence,
        inflight.message.envelope.collector_admission_sequence
    );
    assert_eq!(
        receipt.collector_admission_time_utc_ms,
        inflight.message.envelope.collector_admission_time_utc_ms
    );
    assert_eq!(
        inflight.message.envelope.source_sequence_epoch_id,
        source_epoch
    );
    assert_eq!(
        inflight.raw_day_permit,
        match ticket {
            RawDayAcceptedTicket::Permit(permit) => permit,
            RawDayAcceptedTicket::Defer(_) => panic!("fixture must have an active permit"),
        }
    );
    assert_eq!(
        budget.available(),
        (baseline.0 - 1, baseline.1 - inflight.message.owned_bytes())
    );
    assert_eq!(
        stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        1
    );
    drop(inflight);
    assert_eq!(budget.available(), baseline);
}

#[tokio::test]
async fn polymarket_v3_identical_platform_deliveries_keep_fifo_and_distinct_keys() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let ticket = accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(3, 3 * MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-duplicates".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let projection = polymarket_v3_resolved_projection_for_admission(recv_time_utc_ms);

    let first_receipt = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        {
            let projection = projection.clone();
            move || Ok(projection)
        },
    )
    .await
    .unwrap();
    let second_receipt = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || Ok(projection),
    )
    .await
    .unwrap();
    let first_inflight = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    let second_inflight = rx.recv().await.unwrap().into_polymarket_orderbook_v3();

    assert_eq!(first_receipt.source_sequence_epoch_id, source_epoch);
    assert_eq!(second_receipt.source_sequence_epoch_id, source_epoch);
    assert_eq!(first_receipt.collector_admission_sequence, 1);
    assert_eq!(second_receipt.collector_admission_sequence, 2);
    assert_eq!(
        first_receipt.collector_admission_time_utc_ms,
        first_inflight
            .message
            .envelope
            .collector_admission_time_utc_ms
    );
    assert_eq!(
        second_receipt.collector_admission_time_utc_ms,
        second_inflight
            .message
            .envelope
            .collector_admission_time_utc_ms
    );
    assert!(first_receipt.market_resolved.is_some());
    assert_eq!(
        first_receipt.market_resolved,
        second_receipt.market_resolved
    );
    assert_eq!(first_inflight.message.event, second_inflight.message.event);
    assert_eq!(
        first_inflight.message.envelope.source_sequence_epoch_id,
        second_inflight.message.envelope.source_sequence_epoch_id
    );
    assert_eq!(
        first_inflight.message.envelope.collector_admission_sequence,
        1
    );
    assert_eq!(
        second_inflight
            .message
            .envelope
            .collector_admission_sequence,
        2
    );
    drop((first_inflight, second_inflight));
    assert_eq!(budget.available(), baseline);
}

#[tokio::test]
async fn polymarket_v3_defer_stages_source_key_while_closed_queue_does_not_allocate() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_raw_day_gate(Arc::clone(&gate)).unwrap();
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-defer".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let projection_built = Arc::new(AtomicBool::new(false));
    let ticket = RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
        utc_day: close.next_utc_day,
        old_generation: close.generation,
        expected_generation: close.next_generation,
    });

    let receipt = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        boundary_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        {
            let projection_built = Arc::clone(&projection_built);
            move || {
                projection_built.store(true, Ordering::Relaxed);
                Ok(polymarket_v3_resolved_projection_for_admission(
                    boundary_utc_ms,
                ))
            }
        },
    )
    .await
    .unwrap();
    assert!(projection_built.load(Ordering::Relaxed));
    assert_eq!(receipt.source_sequence_epoch_id, source_epoch);
    assert_eq!(receipt.collector_admission_sequence, 1);
    assert_eq!(
        receipt
            .market_resolved
            .as_ref()
            .map(|resolved| resolved.winning_asset_id.as_str()),
        Some("101")
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(budget.available().0, baseline.0 - 1);
    assert!(budget.available().1 < baseline.1);
    assert_eq!(
        stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        deferred_ingress_facts(&stats, "polymarket"),
        vec![DeferredIngressBatchFact {
            source_kinds: vec!["polymarket_market".to_owned()],
            recv_times_utc_ms: vec![boundary_utc_ms],
            has_collector_admission: vec![true],
            accepted_defer: true,
            typed_polymarket_v3: true,
        }]
    );

    let active_stats = Arc::new(SourceQueueBackpressureStats::default());
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let active_gate = install_active_raw_day_gate(&active_stats, recv_time_utc_ms).await;
    let active_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&active_gate, recv_time_utc_ms).await;
    let active_budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let active_baseline = active_budget.available();
    let (closed_tx, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let closed_sink = SourceEventSink {
        tx: closed_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&active_stats),
        inflight_budget: active_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let error = enqueue_polymarket_orderbook_v3_with_ticket(
        &closed_sink,
        Instant::now(),
        recv_time_utc_ms,
        &active_ticket,
        &identity,
        &source_epoch,
        move || {
            Ok(polymarket_v3_resolved_projection_for_admission(
                recv_time_utc_ms,
            ))
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("bounded_queue_send_closed"));
    assert_eq!(
        active_stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(active_budget.available(), active_baseline);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn polymarket_v3_budget_and_queue_timeouts_do_not_allocate_new_source_keys() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let identity = CollectorIdentity {
        host_id: "collector-v3-timeout".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();

    let budget_stats = Arc::new(SourceQueueBackpressureStats::default());
    let budget_gate = install_active_raw_day_gate(&budget_stats, recv_time_utc_ms).await;
    let budget_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&budget_gate, recv_time_utc_ms).await;
    let exhausted_budget = InflightBudget::with_limits(0, MAX_TYPED_ROW_BYTES);
    let exhausted_baseline = exhausted_budget.available();
    let (budget_tx, mut budget_rx) = mpsc::channel(1);
    let budget_sink = SourceEventSink {
        tx: budget_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&budget_stats),
        inflight_budget: exhausted_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let projection_built = Arc::new(AtomicBool::new(false));
    let error = enqueue_polymarket_orderbook_v3_with_ticket(
        &budget_sink,
        Instant::now(),
        recv_time_utc_ms,
        &budget_ticket,
        &identity,
        &source_epoch,
        {
            let projection_built = Arc::clone(&projection_built);
            move || {
                projection_built.store(true, Ordering::Relaxed);
                Ok(polymarket_v3_resolved_projection_for_admission(
                    recv_time_utc_ms,
                ))
            }
        },
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("global_inflight_row_acquire_timeout")
    );
    assert!(
        projection_built.load(Ordering::Relaxed),
        "projection is constructed under a byte-only staging reservation before row acquisition"
    );
    assert!(budget_rx.try_recv().is_err());
    assert_eq!(exhausted_budget.available(), exhausted_baseline);
    assert_eq!(
        budget_stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        0
    );

    let queue_stats = Arc::new(SourceQueueBackpressureStats::default());
    let queue_gate = install_active_raw_day_gate(&queue_stats, recv_time_utc_ms).await;
    let queue_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&queue_gate, recv_time_utc_ms).await;
    let queue_budget = InflightBudget::with_limits(2, 2 * MAX_TYPED_ROW_BYTES);
    let queue_baseline = queue_budget.available();
    let (queue_tx, mut queue_rx) = mpsc::channel(1);
    let queue_sink = SourceEventSink {
        tx: queue_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&queue_stats),
        inflight_budget: queue_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let first = enqueue_polymarket_orderbook_v3_with_ticket(
        &queue_sink,
        Instant::now(),
        recv_time_utc_ms,
        &queue_ticket,
        &identity,
        &source_epoch,
        move || {
            Ok(polymarket_v3_resolved_projection_for_admission(
                recv_time_utc_ms,
            ))
        },
    )
    .await
    .unwrap();
    assert_eq!(first.collector_admission_sequence, 1);
    let error = enqueue_polymarket_orderbook_v3_with_ticket(
        &queue_sink,
        Instant::now(),
        recv_time_utc_ms,
        &queue_ticket,
        &identity,
        &source_epoch,
        move || {
            Ok(polymarket_v3_resolved_projection_for_admission(
                recv_time_utc_ms,
            ))
        },
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("bounded_queue_backpressure_timeout")
    );
    assert_eq!(
        queue_stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        1
    );
    let first_inflight = queue_rx
        .recv()
        .await
        .unwrap()
        .into_polymarket_orderbook_v3();
    drop(first_inflight);
    assert_eq!(queue_budget.available(), queue_baseline);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn polymarket_v3_staging_and_row_waits_share_one_budget_deadline() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let ticket = accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let held_bytes = budget
        .try_acquire_available(0, MAX_TYPED_ROW_BYTES)
        .unwrap();
    let held_row = budget.try_acquire_available(1, 0).unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-shared-deadline".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let release_bytes = tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        drop(held_bytes);
    });
    let started = TokioInstant::now();

    let error = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || {
            Ok(polymarket_v3_resolved_projection_for_admission(
                recv_time_utc_ms,
            ))
        },
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("global_inflight_row_acquire_timeout")
    );
    assert_eq!(
        TokioInstant::now().duration_since(started),
        BUFFER_ACQUIRE_TIMEOUT
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
    release_bytes.await.unwrap();
    drop(held_row);
    assert_eq!(budget.available(), baseline);
}

#[tokio::test]
async fn polymarket_v3_exact_limit_is_admitted_and_oversize_fails_closed() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let ticket = accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-limit".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let mut exact_projection =
        polymarket_v3_resolved_projection_for_admission(recv_time_utc_ms).clone();
    match &mut exact_projection.event {
        PolymarketOrderbookV3Event::MarketResolved(resolved) => {
            resolved.question = Some(String::new());
        }
        _ => panic!("fixture must be market_resolved"),
    }
    let base = exact_projection
        .clone()
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: recv_time_utc_ms,
            collector_host_id: identity.host_id.clone(),
            collector_region: identity.region.clone(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            source_sequence_epoch_id: source_epoch.clone(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: recv_time_utc_ms.saturating_add(1),
        })
        .unwrap()
        .owned_bytes();
    assert!(base < MAX_TYPED_ROW_BYTES);
    match &mut exact_projection.event {
        PolymarketOrderbookV3Event::MarketResolved(resolved) => {
            resolved.question = Some("x".repeat(MAX_TYPED_ROW_BYTES - base));
        }
        _ => panic!("fixture must be market_resolved"),
    }
    let exact_preview = exact_projection
        .clone()
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: recv_time_utc_ms,
            collector_host_id: identity.host_id.clone(),
            collector_region: identity.region.clone(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            source_sequence_epoch_id: source_epoch.clone(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: recv_time_utc_ms.saturating_add(1),
        })
        .unwrap();
    assert_eq!(exact_preview.owned_bytes(), MAX_TYPED_ROW_BYTES);

    enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        {
            let exact_projection = exact_projection.clone();
            move || Ok(exact_projection)
        },
    )
    .await
    .unwrap();
    let exact_inflight = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    assert_eq!(exact_inflight.message.owned_bytes(), MAX_TYPED_ROW_BYTES);
    assert_eq!(budget.available(), (baseline.0 - 1, 0));
    drop(exact_inflight);
    assert_eq!(budget.available(), baseline);

    let PolymarketOrderbookV3Event::MarketResolved(oversize_resolved) = &mut exact_projection.event
    else {
        panic!("fixture must be market_resolved");
    };
    oversize_resolved
        .question
        .as_mut()
        .expect("question padding exists")
        .push('x');
    let error = enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || Ok(exact_projection),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("max_typed_row_bytes_exceeded"));
    assert!(rx.try_recv().is_err());
    assert_eq!(budget.available(), baseline);
    assert_eq!(
        stats
            .polymarket_market_admission_sequence
            .load(Ordering::Relaxed),
        1,
        "oversize must fail before queue ownership and source-key allocation"
    );
}

fn polymarket_v3_test_string(value: &str, capacity: usize) -> String {
    let mut output = String::with_capacity(capacity.max(value.len()));
    output.push_str(value);
    output
}

fn expected_nested_polymarket_v3_owned_bytes(message: &PolymarketOrderbookV3Message) -> usize {
    let envelope = &message.envelope;
    let common = std::mem::size_of::<PolymarketOrderbookV3Message>()
        + envelope.market.capacity()
        + envelope.slug.as_ref().map_or(0, String::capacity)
        + envelope.collector_host_id.capacity()
        + envelope.collector_region.capacity()
        + envelope.source_endpoint.capacity()
        + envelope.schema_version.capacity()
        + envelope.source_sequence_epoch_id.capacity();
    common
        + match &message.event {
            PolymarketOrderbookV3Event::Book(book) => {
                book.asset_id.capacity()
                    + book.bids.capacity()
                        * std::mem::size_of::<crate::polymarket_orderbook_v3::PolymarketPriceLevelV3>(
                        )
                    + book
                        .bids
                        .iter()
                        .map(|level| level.price.capacity() + level.size.capacity())
                        .sum::<usize>()
                    + book.asks.capacity()
                        * std::mem::size_of::<crate::polymarket_orderbook_v3::PolymarketPriceLevelV3>(
                        )
                    + book
                        .asks
                        .iter()
                        .map(|level| level.price.capacity() + level.size.capacity())
                        .sum::<usize>()
                    + book.last_trade_price.as_ref().map_or(0, String::capacity)
            }
            PolymarketOrderbookV3Event::PriceChange(price_change) => {
                price_change.price_changes.capacity()
                    * std::mem::size_of::<
                        crate::polymarket_orderbook_v3::PolymarketPriceChangeEntryV3,
                    >()
                    + price_change
                        .price_changes
                        .iter()
                        .map(|change| {
                            change.asset_id.capacity()
                                + change.price.capacity()
                                + change.size.as_ref().map_or(0, String::capacity)
                                + change.side.capacity()
                                + change.best_bid.as_ref().map_or(0, String::capacity)
                                + change.best_ask.as_ref().map_or(0, String::capacity)
                        })
                        .sum::<usize>()
            }
            _ => panic!("oracle only covers nested Book and PriceChange fixtures"),
        }
}

#[tokio::test]
async fn polymarket_v3_nested_capacity_oracle_matches_queue_budget_and_releases() {
    use crate::polymarket_orderbook_v3::{
        PolymarketBookV3, PolymarketPriceChangeEntryV3, PolymarketPriceChangeV3,
        PolymarketPriceLevelV3,
    };

    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let ticket = accepted_raw_day_ticket_from_gate_for_test(&gate, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-nested-capacity".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let condition_id = "0x0000000000000000000000000000000000000000000000000000000000000002";

    let mut bids = Vec::with_capacity(5);
    bids.push(PolymarketPriceLevelV3 {
        price: polymarket_v3_test_string("0.50", 17),
        size: polymarket_v3_test_string("12.0", 19),
    });
    let mut asks = Vec::with_capacity(4);
    asks.push(PolymarketPriceLevelV3 {
        price: polymarket_v3_test_string("0.52", 23),
        size: polymarket_v3_test_string("8.0", 29),
    });
    let book_projection = PolymarketOrderbookV3Projection {
        timestamp: Some(recv_time_utc_ms),
        market: polymarket_v3_test_string(condition_id, 96),
        slug: Some(polymarket_v3_test_string("btc-updown-5m-capacity-book", 61)),
        event: PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
            asset_id: polymarket_v3_test_string("101", 31),
            bids,
            asks,
            last_trade_price: Some(polymarket_v3_test_string("0.51", 37)),
        }),
    };
    enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || Ok(book_projection),
    )
    .await
    .unwrap();
    let book = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    let PolymarketOrderbookV3Event::Book(book_event) = &book.message.event else {
        panic!("fixture must remain Book");
    };
    assert!(book_event.bids.capacity() > book_event.bids.len());
    assert!(book_event.asks.capacity() > book_event.asks.len());
    let expected_book_bytes = expected_nested_polymarket_v3_owned_bytes(&book.message);
    assert_eq!(book.message.owned_bytes(), expected_book_bytes);
    assert_eq!(
        budget.available(),
        (baseline.0 - 1, baseline.1 - expected_book_bytes)
    );
    drop(book);
    assert_eq!(budget.available(), baseline);

    let mut changes = Vec::with_capacity(6);
    changes.push(PolymarketPriceChangeEntryV3 {
        asset_id: polymarket_v3_test_string("101", 13),
        price: polymarket_v3_test_string("0.50", 17),
        size: Some(polymarket_v3_test_string("4.0", 19)),
        side: polymarket_v3_test_string("BUY", 23),
        best_bid: Some(polymarket_v3_test_string("0.49", 29)),
        best_ask: Some(polymarket_v3_test_string("0.51", 31)),
    });
    changes.push(PolymarketPriceChangeEntryV3 {
        asset_id: polymarket_v3_test_string("102", 37),
        price: polymarket_v3_test_string("0.48", 41),
        size: None,
        side: polymarket_v3_test_string("SELL", 43),
        best_bid: None,
        best_ask: Some(polymarket_v3_test_string("0.49", 47)),
    });
    let price_change_projection = PolymarketOrderbookV3Projection {
        timestamp: Some(recv_time_utc_ms),
        market: polymarket_v3_test_string(condition_id, 101),
        slug: Some(polymarket_v3_test_string(
            "btc-updown-5m-capacity-change",
            67,
        )),
        event: PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
            price_changes: changes,
        }),
    };
    enqueue_polymarket_orderbook_v3_with_ticket(
        &sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        &identity,
        &source_epoch,
        move || Ok(price_change_projection),
    )
    .await
    .unwrap();
    let price_change = rx.recv().await.unwrap().into_polymarket_orderbook_v3();
    let PolymarketOrderbookV3Event::PriceChange(price_change_event) = &price_change.message.event
    else {
        panic!("fixture must remain PriceChange");
    };
    assert!(price_change_event.price_changes.capacity() > price_change_event.price_changes.len());
    let expected_price_change_bytes =
        expected_nested_polymarket_v3_owned_bytes(&price_change.message);
    assert_eq!(
        price_change.message.owned_bytes(),
        expected_price_change_bytes
    );
    assert_eq!(
        budget.available(),
        (baseline.0 - 1, baseline.1 - expected_price_change_bytes)
    );
    drop(price_change);
    assert_eq!(budget.available(), baseline);
}

async fn polymarket_v3_ingress_for_test(
    sink: &SourceEventSink,
    rx: &mut mpsc::Receiver<SourceIngressPayload>,
    gate: &Arc<RawDayGate>,
    collector_identity: &CollectorIdentity,
    source_epoch: &str,
    recv_time_utc_ms: i64,
) -> SourceIngressPayload {
    let ticket = accepted_raw_day_ticket_from_gate_for_test(gate, recv_time_utc_ms).await;
    enqueue_polymarket_orderbook_v3_with_ticket(
        sink,
        Instant::now(),
        recv_time_utc_ms,
        &ticket,
        collector_identity,
        source_epoch,
        move || {
            Ok(polymarket_v3_resolved_projection_for_admission(
                recv_time_utc_ms,
            ))
        },
    )
    .await
    .unwrap();
    rx.recv().await.expect("v3 ingress must be queued")
}

fn start_cold_polymarket_v3_writer_for_test(
    dataset_root: &Path,
    provenance: &RuntimeProvenance,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &Arc<SourceQueueBackpressureStats>,
    raw_day_gate: Option<Arc<RawDayGate>>,
) -> (
    BTreeMap<String, SourceWriterHandle>,
    std::sync::mpsc::Receiver<std::result::Result<SourceWriterOutput, String>>,
) {
    let (command_tx, command_rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let worker_queue_rows = Arc::clone(&queue_rows);
    let accepting_commands = Arc::new(Mutex::new(true));
    let worker_accepting_commands = Arc::clone(&accepting_commands);
    let (output_tx, output_rx) = std::sync::mpsc::sync_channel(1);
    let (_report_tx, report_rx) = std::sync::mpsc::sync_channel(1);
    let worker_root = dataset_root.to_path_buf();
    let worker_provenance = provenance.clone();
    let worker_manifest_tx = manifest_tx.clone();
    let worker_stats = Arc::clone(runtime_stats);
    let thread = std::thread::spawn(move || {
        let result = source_writer_worker_with_startup(
            worker_root,
            worker_provenance,
            "polymarket_market".to_owned(),
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
            command_rx,
            worker_queue_rows,
            worker_accepting_commands,
            worker_stats,
            worker_manifest_tx,
            None,
            WriterEpochCell::default(),
            raw_day_gate,
        )
        .map_err(|error| error.to_string());
        let _ = output_tx.send(result);
    });
    (
        BTreeMap::from([(
            "polymarket_market".to_owned(),
            SourceWriterHandle {
                schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
                tx: command_tx,
                queue_rows,
                accepting_commands,
                result_rx: report_rx,
                completed_result: None,
                thread: Some(thread),
            },
        )]),
        output_rx,
    )
}

#[tokio::test]
async fn legacy_v2_writer_rejects_polymarket_v3_ingress_without_mutating_batches() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-legacy-v2-guard".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let ingress = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        recv_time_utc_ms,
    )
    .await;
    assert_ne!(budget.available(), baseline);

    let (writer_tx, _writer_rx) = std::sync::mpsc::sync_channel(1);
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let mut writers = BTreeMap::from([(
        "polymarket_market".to_owned(),
        SourceWriterHandle {
            schema_version: "polymarket_orderbook_v2".to_owned(),
            tx: writer_tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: None,
        },
    )]);
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let (manifest_tx, _manifest_rx) = mpsc::channel(1);
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    let error = process_ingress_event(
        ingress,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Polymarket orderbook v3 ingress reached a non-v3 writer"),
        "{error}"
    );
    assert_eq!(received_rows, 0);
    assert!(coverage.is_empty());
    assert!(batches.polymarket_orderbook_v3.is_empty());
    assert!(batches.legacy.values().all(Vec::is_empty));
    assert_eq!(budget.available(), baseline);
}

#[tokio::test]
async fn cold_v3_ingress_uses_one_writer_and_actual_manifest_schema() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-cold-writer".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let ingress = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        recv_time_utc_ms,
    )
    .await;
    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let provenance = test_runtime_provenance("polymarket-v3-cold-writer");
    let (mut writers, output_rx) = start_cold_polymarket_v3_writer_for_test(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &stats,
        None,
    );
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    process_ingress_event(
        ingress,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    assert_eq!(received_rows, 1);
    assert_eq!(batches.polymarket_orderbook_v3.len(), 1);
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();
    assert!(batches.polymarket_orderbook_v3.is_empty());

    let mut writer = writers.remove("polymarket_market").unwrap();
    drop(writer.tx);
    let output = output_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.thread.take().unwrap().join().unwrap();
    assert_eq!(output.stats.events_written, 1);
    assert_eq!(output.stats.typed_rows_written, 1);
    assert_eq!(output.finalized_segments.len(), 1);
    assert_eq!(budget.available(), baseline);

    let ManifestCommand::Finalized(delta) = manifest_rx.recv().await.unwrap() else {
        panic!("cold v3 writer must emit one finalized manifest delta");
    };
    assert_eq!(delta.source_kind, "polymarket_market");
    assert_eq!(delta.schema_version, POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION);
    assert_eq!(delta.rows, 1);
    assert_eq!(delta.sha256, output.finalized_segments[0].sha256);
    assert!(manifest_rx.try_recv().is_err());

    let reader =
        SerializedFileReader::new(File::open(&output.finalized_segments[0].parquet_path).unwrap())
            .unwrap();
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    assert_eq!(
        metadata
            .iter()
            .find(|entry| entry.key == "sort_order")
            .and_then(|entry| entry.value.as_deref()),
        Some("collector_admission_order")
    );
    let embedded = metadata
        .iter()
        .find(|entry| entry.key == DAILY_FILE_RECOVERY_METADATA_KEY)
        .and_then(|entry| entry.value.as_deref())
        .map(|value| serde_json::from_str::<DailyFileRecoveryMetadata>(value).unwrap())
        .unwrap();
    assert_eq!(
        embedded.schema_version,
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
    );
    assert_eq!(
        embedded.facts["sort_order"],
        Value::String("collector_admission_order".to_owned())
    );
}

#[tokio::test]
async fn v3_writer_disconnect_releases_command_reservations_and_records_drop() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let utc_day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-writer-disconnect".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let ingress = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        recv_time_utc_ms,
    )
    .await;

    let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(1);
    drop(writer_rx);
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let mut writers = BTreeMap::from([(
        "polymarket_market".to_owned(),
        SourceWriterHandle {
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
            tx: writer_tx,
            queue_rows: Arc::clone(&queue_rows),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: None,
        },
    )]);
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let (manifest_tx, _manifest_rx) = mpsc::channel(1);
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    process_ingress_event(
        ingress,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    assert_eq!(batches.polymarket_orderbook_v3.len(), 1);
    assert_ne!(budget.available(), baseline);

    let error = flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .expect_err("a disconnected v3 writer must stop the runtime");

    assert!(error.to_string().contains("writer_queue_disconnected"));
    assert!(batches.polymarket_orderbook_v3.is_empty());
    assert_eq!(queue_rows.load(Ordering::Relaxed), 0);
    assert_eq!(tracker.finish(0).dropped_rows, 1);
    assert_eq!(
        stats.drain_operational_day(&utc_day)["polymarket_market"].dropped_rows,
        1
    );
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| gap == "writer_queue_disconnected:polymarket_market")
    );
    assert_eq!(budget.available(), baseline);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn v3_writer_backpressure_timeout_releases_command_reservations_and_records_drop() {
    let recv_time_utc_ms = now_ms().saturating_sub(10);
    let utc_day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(1, MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(1);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-writer-timeout".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let ingress = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        recv_time_utc_ms,
    )
    .await;
    let mut batch = vec![ingress.into_polymarket_orderbook_v3()];
    assert_ne!(budget.available(), baseline);

    let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(1);
    writer_tx
        .send(WriterCommand::FinalizeHour {
            utc_hour: "held-control-command".to_owned(),
        })
        .unwrap();
    let (_result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let queue_rows = Arc::new(AtomicU64::new(0));
    let mut writers = BTreeMap::from([(
        "polymarket_market".to_owned(),
        SourceWriterHandle {
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
            tx: writer_tx,
            queue_rows: Arc::clone(&queue_rows),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: None,
        },
    )]);
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");

    let error = flush_polymarket_orderbook_v3_batch(
        &mut writers,
        &mut batch,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .expect_err("a persistently full v3 writer queue must time out");

    assert!(
        error
            .to_string()
            .contains("writer_queue_backpressure_timeout")
    );
    assert!(batch.is_empty());
    assert_eq!(queue_rows.load(Ordering::Relaxed), 0);
    let summary = tracker.finish(0);
    assert_eq!(summary.backpressure_timeout_count, 1);
    assert_eq!(summary.dropped_rows, 1);
    assert_eq!(
        stats.drain_operational_day(&utc_day)["polymarket_market"].dropped_rows,
        1
    );
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|gap| gap == "writer_queue_backpressure_timeout:polymarket_market")
    );
    assert_eq!(budget.available(), baseline);
    drop(writer_rx);
}

#[tokio::test]
async fn v3_sequence_state_survives_live_segment_rollover() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let day_start_utc_ms = now_ms()
        .div_euclid(UTC_DAY_MS)
        .saturating_sub(1)
        .saturating_mul(UTC_DAY_MS);
    let first_recv_time_utc_ms = day_start_utc_ms + UTC_HOUR_MS;
    let second_recv_time_utc_ms = first_recv_time_utc_ms + UTC_HOUR_MS;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, first_recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(2, 2 * MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(2);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-live-segment-sequence".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let first = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        first_recv_time_utc_ms,
    )
    .await;
    let mut duplicate = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        second_recv_time_utc_ms,
    )
    .await;
    let SourceIngressPayload::PolymarketOrderbookV3(duplicate_message) = &mut duplicate else {
        panic!("fixture must be Polymarket v3");
    };
    duplicate_message
        .message
        .envelope
        .collector_admission_sequence = 1;

    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let provenance = test_runtime_provenance("polymarket-v3-live-segment-sequence");
    let (mut writers, output_rx) = start_cold_polymarket_v3_writer_for_test(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &stats,
        None,
    );
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    process_ingress_event(
        first,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();
    let first_hour = HourlyPartitionPolicy::new(&dataset_root)
        .unwrap()
        .partition("polymarket_market", first_recv_time_utc_ms, None)
        .unwrap()
        .utc_hour;
    send_writer_hour_finalize(&writers, "polymarket_market", &first_hour)
        .await
        .unwrap();

    process_ingress_event(
        duplicate,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();

    let mut writer = writers.remove("polymarket_market").unwrap();
    drop(writer.tx);
    let error = output_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    writer.thread.take().unwrap().join().unwrap();
    assert!(error.contains("not strictly increasing within epoch"));
    assert_eq!(budget.available(), baseline);
    let ManifestCommand::Finalized(first_delta) = manifest_rx.recv().await.unwrap() else {
        panic!("first hour must finalize before duplicate rejection");
    };
    assert_eq!(first_delta.rows, 1);
    assert_eq!(
        first_delta.schema_version,
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
    );
    assert!(manifest_rx.try_recv().is_err());
}

#[tokio::test]
async fn v3_sequence_state_survives_utc_day_rollover() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = now_ms()
        .div_euclid(UTC_DAY_MS)
        .saturating_sub(1)
        .saturating_mul(UTC_DAY_MS);
    let first_recv_time_utc_ms = boundary_utc_ms.saturating_sub(1_000);
    let second_recv_time_utc_ms = boundary_utc_ms.saturating_add(1_000);
    let old_utc_day = utc_day_string_from_ms(first_recv_time_utc_ms).unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, first_recv_time_utc_ms).await;
    let budget = InflightBudget::with_limits(2, 2 * MAX_TYPED_ROW_BYTES);
    let baseline = budget.available();
    let (ingress_tx, mut ingress_rx) = mpsc::channel(2);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let sink = SourceEventSink {
        tx: ingress_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-v3-day-sequence".to_owned(),
        region: "local-test".to_owned(),
    };
    let source_epoch = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let first = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        first_recv_time_utc_ms,
    )
    .await;

    let (manifest_tx, mut manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let provenance = test_runtime_provenance("polymarket-v3-day-sequence");
    let (mut writers, output_rx) = start_cold_polymarket_v3_writer_for_test(
        &dataset_root,
        &provenance,
        &manifest_tx,
        &stats,
        Some(Arc::clone(&gate)),
    );
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut average_event_bytes = 0;
    let mut tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    process_ingress_event(
        first,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();

    let close = gate
        .close_for_rollover(&old_utc_day, boundary_utc_ms)
        .await
        .unwrap();
    writers["polymarket_market"]
        .tx
        .send(WriterCommand::FinalizeDay {
            close: close.clone(),
        })
        .unwrap();
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();

    let mut duplicate = polymarket_v3_ingress_for_test(
        &sink,
        &mut ingress_rx,
        &gate,
        &identity,
        &source_epoch,
        second_recv_time_utc_ms,
    )
    .await;
    let SourceIngressPayload::PolymarketOrderbookV3(duplicate_message) = &mut duplicate else {
        panic!("fixture must be Polymarket v3");
    };
    duplicate_message
        .message
        .envelope
        .collector_admission_sequence = 1;
    process_ingress_event(
        duplicate,
        &mut writers,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &mut average_event_bytes,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
        &manifest_tx,
        &mut coordinator,
    )
    .await
    .unwrap();
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &stats,
        &mut tracker,
        Instant::now(),
    )
    .await
    .unwrap();

    let mut writer = writers.remove("polymarket_market").unwrap();
    drop(writer.tx);
    let error = output_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    writer.thread.take().unwrap().join().unwrap();
    assert!(error.contains("not strictly increasing within epoch"));
    assert_eq!(budget.available(), baseline);

    let ManifestCommand::Finalized(first_delta) = manifest_rx.recv().await.unwrap() else {
        panic!("old UTC day must finalize before duplicate rejection");
    };
    assert_eq!(first_delta.rows, 1);
    assert_eq!(
        first_delta.schema_version,
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
    );
    assert!(matches!(
        manifest_rx.recv().await.unwrap(),
        ManifestCommand::WriterDayFinalized { .. }
    ));
    assert!(manifest_rx.try_recv().is_err());
}

#[test]
fn polymarket_source_epoch_is_nonempty_and_changes_per_source_owner() {
    let identity = CollectorIdentity {
        host_id: "collector-v3-epoch".to_owned(),
        region: "local-test".to_owned(),
    };
    let first = polymarket_source_sequence_epoch_id(&identity).unwrap();
    let second = polymarket_source_sequence_epoch_id(&identity).unwrap();
    assert!(!first.is_empty());
    assert!(!second.is_empty());
    assert_ne!(first, second);
}

#[tokio::test]
async fn polymarket_current_v3_missing_resolved_winner_fails_before_row_claim_or_state() {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let received_at_utc_ms = current.end_ts_s * 1_000 - 1;
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    context.actor_now_utc_ms = received_at_utc_ms;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, received_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let event = match serde_json::from_value::<WsMessage>(json!({
        "id": current.id,
        "market": current.condition_id,
        "slug": current.slug,
        "assets_ids": [current.up_asset_id, current.down_asset_id],
        "outcomes": ["Up", "Down"],
        "winning_asset_id": null,
        "winning_outcome": "Up",
        "timestamp": received_at_utc_ms.to_string(),
        "event_type": "market_resolved"
    }))
    .unwrap()
    {
        WsMessage::MarketResolved(event) => event,
        _ => panic!("expected market_resolved message"),
    };

    let error = process_polymarket_market_resolved(
        &event,
        received_at_utc_ms,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
        accepted_raw_day_ticket_for_test(received_at_utc_ms).await,
    )
    .await
    .expect_err("missing target winner must fail before current-v3 persistence");

    assert!(
        error
            .to_string()
            .contains("Polymarket market_resolved winning asset missing"),
        "{error}"
    );
    assert!(rx.try_recv().is_err());
    assert!(context.early_ws_claims.is_empty());
    assert!(context.pending.is_empty());
    assert_eq!(
        context.current.as_ref().map(|market| market.slug.as_str()),
        Some(current.slug.as_str())
    );
}

#[tokio::test]
async fn polymarket_ws_claim_waits_for_completed_gamma_before_fence() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let slug = identity.slug.clone();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let sdk_market = serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
        &discovery.current,
    ))
    .unwrap();
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: scheduled_at_utc_ms,
        validation_completed_at_utc_ms: scheduled_at_utc_ms + 1,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: scheduled_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: scheduled_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_test(scheduled_at_utc_ms).await,
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    let mut pending = PendingPolymarketResolution::new(identity, Some(evidence));
    pending.in_flight_ordinal = Some(1);

    let (release_tx, release_rx) = oneshot::channel();
    let completion_slug = slug.clone();
    let mut tasks = JoinSet::new();
    let handle = tasks.spawn(async move {
        release_rx.await.unwrap();
        GammaResolutionPollCompletion {
            slug: completion_slug,
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms: scheduled_at_utc_ms + 1,
            result: Ok(sdk_market),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_for_test(scheduled_at_utc_ms + 1).await,
            ),
        }
    });
    pending.poll_abort_handle = Some(handle);
    context.pending.insert(slug.clone(), pending);
    assert!(!context.pending[&slug].requires_fence());

    release_tx.send(()).unwrap();
    while !context.pending[&slug]
        .poll_abort_handle
        .as_ref()
        .unwrap()
        .is_finished()
    {
        tokio::task::yield_now().await;
    }
    context.pending[&slug]
        .poll_abort_handle
        .as_ref()
        .unwrap()
        .abort();
    assert!(
        !context.pending[&slug].requires_fence(),
        "a queued completion must be classified before pending teardown"
    );

    let completion = tasks.join_next().await.unwrap().unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, completion.completed_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    record_gamma_resolution_completion(
        completion,
        &|| scheduled_at_utc_ms + 2,
        &mut context,
        &SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
    )
    .await
    .unwrap();
    let pending = &context.pending[&slug];
    assert!(pending.in_flight_ordinal.is_none());
    assert!(pending.poll_abort_handle.is_none());
    assert!(pending.requires_fence());
    assert_eq!(pending.buffered_attempt_rows.len(), 1);
    let post = &pending.buffered_attempt_rows[0];
    assert_eq!(
        post.resolution_poll_result,
        Some(ResolutionPollResult::PostTerminalClaimObservation)
    );
    assert_eq!(
        post.terminal_claim_owner,
        Some(TerminalClaimOwner::MarketResolvedWs)
    );
    assert!(post.resolution_evidence_source.is_none());
    assert!(post.market_lifecycle_transition.is_none());
    assert!(rx.try_recv().is_err(), "post evidence stays actor-buffered");
}

#[tokio::test]
async fn polymarket_ws_claim_preserves_late_gamma_classification_matrix() {
    #[derive(Clone, Copy, Debug)]
    enum Case {
        Terminal,
        Nonterminal,
        SdkDecode,
        AttemptBinding,
        IdentityShape,
        ClosedTime,
        Transport,
    }

    for case in [
        Case::Terminal,
        Case::Nonterminal,
        Case::SdkDecode,
        Case::AttemptBinding,
        Case::IdentityShape,
        Case::ClosedTime,
        Case::Transport,
    ] {
        let discovery = test_btc_discovery();
        let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
        let slug = identity.slug.clone();
        let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
        let completed_at_utc_ms = scheduled_at_utc_ms + 1;
        let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
        let evidence = PolymarketWsTerminalEvidence {
            source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
            collector_admission_sequence: 1,
            source_received_at_utc_ms: scheduled_at_utc_ms,
            validation_completed_at_utc_ms: completed_at_utc_ms,
            source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            source_payload_json,
            platform_timestamp_raw: scheduled_at_utc_ms.to_string(),
            platform_timestamp_utc_ms: scheduled_at_utc_ms,
            winning_asset_id: identity.up_asset_id.clone(),
            winning_outcome: "Up".to_owned(),
            raw_day_ticket: accepted_raw_day_ticket_for_test(scheduled_at_utc_ms).await,
        };
        let mut pending = PendingPolymarketResolution::new(identity.clone(), Some(evidence));
        pending.in_flight_ordinal = Some(if matches!(case, Case::AttemptBinding) {
            2
        } else {
            1
        });
        let result = match case {
            Case::Terminal => Ok(gamma_terminal_market_for_discovered(&discovery.current)),
            Case::Nonterminal | Case::AttemptBinding => Ok(
                serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
                    &discovery.current,
                ))
                .unwrap(),
            ),
            Case::SdkDecode => Err(GammaResolutionPollFailure::SdkDecodeOrSchema),
            Case::IdentityShape => {
                let mut value = gamma_market_value_for_discovered(&discovery.current);
                value["slug"] = json!("btc-updown-5m-unexpected");
                Ok(serde_json::from_value::<SdkGammaMarket>(value).unwrap())
            }
            Case::ClosedTime => {
                let mut value =
                    serde_json::to_value(gamma_terminal_market_for_discovered(&discovery.current))
                        .unwrap();
                value["closedTime"] = json!("not-a-timestamp");
                Ok(serde_json::from_value::<SdkGammaMarket>(value).unwrap())
            }
            Case::Transport => Err(GammaResolutionPollFailure::Transport {
                class: ResolutionTransportErrorClass::Timeout,
                http_status: None,
            }),
        };
        let mut context = PolymarketProcessingContext::new(discovery);
        context.pending.insert(slug.clone(), pending);
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, completed_at_utc_ms).await;
        let (tx, mut rx) = mpsc::channel(2);
        record_gamma_resolution_completion(
            GammaResolutionPollCompletion {
                slug: slug.clone(),
                ordinal: 1,
                scheduled_at_utc_ms,
                fetch_start_utc_ms: scheduled_at_utc_ms,
                completed_at_utc_ms,
                result,
                raw_day_ticket: RawDayTicketAcceptance::Accepted(
                    accepted_raw_day_ticket_for_test(completed_at_utc_ms).await,
                ),
            },
            &|| completed_at_utc_ms + 1,
            &mut context,
            &SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: stats,
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
        )
        .await
        .unwrap();

        let pending = &context.pending[&slug];
        assert!(matches!(
            pending.claim,
            Some(PolymarketTerminalClaim::MarketResolvedWs(_))
        ));
        assert_eq!(pending.attempts_recorded, 1);
        assert!(pending.in_flight_ordinal.is_none());
        assert_eq!(pending.buffered_attempt_rows.len(), 1);
        assert!(
            rx.try_recv().is_err(),
            "{case:?} must remain actor-buffered"
        );
        let row = &pending.buffered_attempt_rows[0];
        match case {
            Case::Terminal => {
                assert_eq!(
                    row.resolution_poll_result,
                    Some(ResolutionPollResult::PostTerminalClaimObservation)
                );
                assert_eq!(row.terminal_predicate_passed, Some(true));
                assert!(row.terminal_predicate_failure_reason.is_none());
                assert_eq!(
                    row.terminal_claim_owner,
                    Some(TerminalClaimOwner::MarketResolvedWs)
                );
                assert!(row.resolution_evidence_source.is_none());
                assert!(!pending.fatal_integrity_error);
            }
            Case::Nonterminal => {
                assert_eq!(
                    row.resolution_poll_result,
                    Some(ResolutionPollResult::PostTerminalClaimObservation)
                );
                assert_eq!(row.terminal_predicate_passed, Some(false));
                assert_eq!(
                    row.terminal_predicate_failure_reason,
                    Some(TerminalPredicateFailureReason::ClosedFalse)
                );
                assert_eq!(
                    row.terminal_claim_owner,
                    Some(TerminalClaimOwner::MarketResolvedWs)
                );
                assert!(!pending.fatal_integrity_error);
            }
            Case::SdkDecode => {
                assert_eq!(
                    row.resolution_integrity_error_class,
                    Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema)
                );
                assert!(row.source_evidence_payload_json.is_none());
                assert_eq!(
                    row.response_fields_unavailable_reason,
                    Some(ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError)
                );
                assert!(pending.fatal_integrity_error);
            }
            Case::AttemptBinding => {
                assert_eq!(
                    row.resolution_integrity_error_class,
                    Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant)
                );
                assert!(row.source_evidence_payload_json.is_some());
                assert!(pending.fatal_integrity_error);
            }
            Case::IdentityShape => {
                assert_eq!(
                    row.resolution_integrity_error_class,
                    Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction)
                );
                assert_eq!(
                    row.terminal_predicate_failure_reason,
                    Some(TerminalPredicateFailureReason::SlugMismatch)
                );
                assert!(pending.fatal_integrity_error);
            }
            Case::ClosedTime => {
                assert_eq!(
                    row.resolution_integrity_error_class,
                    Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid)
                );
                assert_eq!(
                    row.terminal_predicate_failure_reason,
                    Some(TerminalPredicateFailureReason::ClosedTimeParseError)
                );
                assert_eq!(row.closed_time_check_passed, Some(false));
                assert_eq!(
                    row.closed_time_check_failure_reason,
                    Some(ClosedTimeCheckFailureReason::ParseError)
                );
                assert!(pending.fatal_integrity_error);
            }
            Case::Transport => {
                assert_eq!(
                    row.resolution_poll_result,
                    Some(ResolutionPollResult::TransportUnavailable)
                );
                assert_eq!(
                    row.resolution_transport_error_class,
                    Some(ResolutionTransportErrorClass::Timeout)
                );
                assert!(row.terminal_claim_owner.is_none());
                assert!(!pending.fatal_integrity_error);
            }
        }
        if !matches!(case, Case::Terminal | Case::Nonterminal) {
            assert_ne!(
                row.resolution_poll_result,
                Some(ResolutionPollResult::PostTerminalClaimObservation)
            );
        }
    }
}

#[tokio::test]
async fn polymarket_ws_claim_reaps_cancelled_gamma_before_fence() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let slug = identity.slug.clone();
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: identity.market_end_utc_ms,
        validation_completed_at_utc_ms: identity.market_end_utc_ms + 1,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: identity.market_end_utc_ms.to_string(),
        platform_timestamp_utc_ms: identity.market_end_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_test(identity.market_end_utc_ms).await,
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    let mut pending = PendingPolymarketResolution::new(identity, Some(evidence));
    pending.in_flight_ordinal = Some(1);

    let mut tasks = JoinSet::<GammaResolutionPollCompletion>::new();
    let handle = tasks.spawn(std::future::pending::<GammaResolutionPollCompletion>());
    pending.poll_abort_handle = Some(handle);
    context.pending.insert(slug.clone(), pending);
    context.pending[&slug]
        .poll_abort_handle
        .as_ref()
        .unwrap()
        .abort();

    let join_error = match tasks.join_next().await.unwrap() {
        Ok(_) => panic!("aborted Gamma task unexpectedly completed"),
        Err(error) => error,
    };
    assert!(join_error.is_cancelled());
    record_cancelled_gamma_resolution_poll(join_error.id(), &mut context).unwrap();

    let pending = &context.pending[&slug];
    assert!(pending.in_flight_ordinal.is_none());
    assert!(pending.poll_abort_handle.is_none());
    assert!(pending.requires_fence());
}

#[tokio::test]
async fn polymarket_next_discovery_cancelled_at_e_is_not_fatal() {
    let discovery = test_btc_discovery();
    let market_end_utc_ms = discovery.current.end_ts_s * 1_000;
    let timeline_utc_ms = Arc::new(Mutex::new(market_end_utc_ms - 1));

    let gamma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", gamma_listener.local_addr().unwrap());
    let (gamma_request_tx, gamma_request_rx) = oneshot::channel();
    let gamma_server = tokio::spawn(async move {
        let (mut socket, _) = gamma_listener.accept().await.unwrap();
        let mut request = [0_u8; 4_096];
        let read = socket.read(&mut request).await.unwrap();
        assert!(read > 0);
        gamma_request_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });

    let counters = Arc::new(GammaRecoveryLoopbackCounters::default());
    let (clob_endpoint, clob_server) = silent_clob_loopback(counters).await;
    let budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        clob_endpoint,
        bounded_ws_config(),
        budget.clone(),
        active_raw_day_gate_for_test(market_end_utc_ms - 1).await,
    )
    .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, market_end_utc_ms - 1).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let timeline_for_collector = Arc::clone(&timeline_utc_ms);
    let collector = tokio::spawn(async move {
        let mut context = PolymarketProcessingContext::new(discovery.clone());
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let mut resolution_poll_tasks = JoinSet::new();
        collect_polymarket_single_receiver(
            Some(discovery),
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            None,
            Instant::now(),
            &mut runtime,
            &GammaClient::new(&gamma_endpoint).unwrap(),
            &mut recovery,
            &mut control_rx,
            Duration::ZERO,
            Duration::from_millis(1),
            move || PolymarketTimelineNow {
                utc_ms: *timeline_for_collector.lock().unwrap(),
                monotonic: TokioInstant::now(),
            },
        )
        .await
    });

    timeout(Duration::from_secs(2), gamma_request_rx)
        .await
        .expect("next-market discovery request must start before E")
        .unwrap();
    *timeline_utc_ms.lock().unwrap() = market_end_utc_ms;
    sleep(Duration::from_millis(350)).await;
    assert!(
        !collector.is_finished(),
        "actor-owned next discovery cancellation must not terminate the source"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: true,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap().unwrap();
    collector.await.unwrap().unwrap();

    let metadata = std::iter::from_fn(|| event_rx.try_recv().ok())
        .filter(|event| event.event.source_kind == "polymarket_market_metadata")
        .map(|event| {
            polymarket_metadata_v5_prefixes_from_event(&event.event)
                .unwrap()
                .remove(0)
        })
        .collect::<Vec<_>>();
    assert!(metadata.iter().any(|row| {
        row.market_runtime_role_transition
            == Some(MarketRuntimeRoleTransition::CurrentToResolutionPending)
    }));
    assert!(gaps.lock().unwrap().is_empty());
    gamma_server.abort();
    clob_server.abort();
}

#[tokio::test]
async fn polymarket_day_fence_discards_ready_and_inflight_discovery_results() {
    let mut next_discovery_tasks = JoinSet::new();
    next_discovery_tasks.spawn(async { "old-day-ready" });
    tokio::task::yield_now().await;
    let mut rollover_discovery_tasks = JoinSet::new();
    rollover_discovery_tasks.spawn(std::future::pending::<&'static str>());

    abort_polymarket_discovery_tasks(&mut next_discovery_tasks, &mut rollover_discovery_tasks)
        .await;

    assert!(next_discovery_tasks.is_empty());
    assert!(rollover_discovery_tasks.is_empty());
    assert!(next_discovery_tasks.try_join_next().is_none());
    assert!(rollover_discovery_tasks.try_join_next().is_none());
}

#[tokio::test]
async fn polymarket_terminal_at_initial_or_retry_stops_later_slots() {
    for (ordinal, retry_index) in [(1_u8, 0_u8), (2, 1), (4, 3), (6, 5)] {
        let discovery = test_btc_discovery();
        let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
        let slug = identity.slug.clone();
        let scheduled_at_utc_ms = identity.market_end_utc_ms
            + POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS
            + i64::from(retry_index) * POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS;
        let terminal_market = gamma_terminal_market_for_discovered(&discovery.current);
        let mut context = PolymarketProcessingContext::new(discovery);
        let mut pending = PendingPolymarketResolution::new(identity, None);
        pending.next_attempt_ordinal = ordinal;
        pending.in_flight_ordinal = Some(ordinal);
        assert_eq!(pending.slot_utc_ms(ordinal), scheduled_at_utc_ms);
        context.pending.insert(slug.clone(), pending);

        let (tx, mut rx) = mpsc::channel(1);
        record_gamma_resolution_completion(
            GammaResolutionPollCompletion {
                slug: slug.clone(),
                ordinal,
                scheduled_at_utc_ms,
                fetch_start_utc_ms: scheduled_at_utc_ms,
                completed_at_utc_ms: scheduled_at_utc_ms + 1,
                result: Ok(terminal_market),
                raw_day_ticket: RawDayTicketAcceptance::Accepted(
                    accepted_raw_day_ticket_for_test(scheduled_at_utc_ms + 1).await,
                ),
            },
            &|| scheduled_at_utc_ms + 2,
            &mut context,
            &SourceEventSink {
                tx,
                gaps: Arc::new(Mutex::new(SourceGapStore::default())),
                source_queue_backpressure: Arc::new(SourceQueueBackpressureStats::default()),
                inflight_budget: InflightBudget::new(),
                enqueue_lock: Arc::new(AsyncMutex::new(())),
            },
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
        )
        .await
        .unwrap();

        let pending = &context.pending[&slug];
        assert_eq!(pending.attempts_recorded, 1);
        assert_eq!(pending.next_attempt_ordinal, ordinal + 1);
        assert!(!pending.exhaustion_requested);
        assert!(pending.requires_fence());
        let Some(PolymarketTerminalClaim::GammaBySlug(row)) = pending.claim.as_ref() else {
            panic!("terminal attempt did not claim Gamma ownership")
        };
        assert_eq!(row.resolution_poll_attempt_ordinal, Some(ordinal));
        assert_eq!(row.resolution_poll_scheduled_utc, Some(scheduled_at_utc_ms));
        assert_eq!(
            row.resolution_followup_status,
            Some(ResolutionFollowupStatus::TerminalGammaFallback)
        );
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn polymarket_mixed_and_zero_valid_sequences_exhaust_once_at_six() {
    for valid_ordinals in [vec![1_u8, 3, 6], Vec::new()] {
        let discovery = test_btc_discovery();
        let market = discovery.current.clone();
        let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
        let slug = identity.slug.clone();
        let first_slot_utc_ms =
            identity.market_end_utc_ms + POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS;
        let mut context = PolymarketProcessingContext::new(discovery);
        context.pending.insert(
            slug.clone(),
            PendingPolymarketResolution::new(identity, None),
        );
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, first_slot_utc_ms).await;
        let (tx, mut rx) = mpsc::channel(8);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };

        for ordinal in 1..=POLYMARKET_RESOLUTION_ATTEMPT_COUNT {
            let scheduled_at_utc_ms = context.pending[&slug].slot_utc_ms(ordinal);
            context.pending.get_mut(&slug).unwrap().in_flight_ordinal = Some(ordinal);
            let result = if valid_ordinals.contains(&ordinal) {
                Ok(
                    serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
                        &market,
                    ))
                    .unwrap(),
                )
            } else {
                Err(GammaResolutionPollFailure::Transport {
                    class: ResolutionTransportErrorClass::Timeout,
                    http_status: None,
                })
            };
            record_gamma_resolution_completion(
                GammaResolutionPollCompletion {
                    slug: slug.clone(),
                    ordinal,
                    scheduled_at_utc_ms,
                    fetch_start_utc_ms: scheduled_at_utc_ms,
                    completed_at_utc_ms: scheduled_at_utc_ms + 1,
                    result,
                    raw_day_ticket: RawDayTicketAcceptance::Accepted(
                        accepted_raw_day_ticket_for_test(scheduled_at_utc_ms + 1).await,
                    ),
                },
                &|| scheduled_at_utc_ms + 2,
                &mut context,
                &sink,
                &collector_identity,
                Instant::now(),
            )
            .await
            .unwrap();
            assert_eq!(
                context.pending[&slug].exhaustion_requested,
                ordinal == POLYMARKET_RESOLUTION_ATTEMPT_COUNT
            );
        }

        let pending = &context.pending[&slug];
        assert_eq!(pending.attempts_recorded, 6);
        assert_eq!(pending.next_attempt_ordinal, 7);
        assert_eq!(
            pending.valid_response_count,
            u32::try_from(valid_ordinals.len()).unwrap()
        );
        assert_eq!(
            pending.transport_unavailable_count,
            6_u32.saturating_sub(pending.valid_response_count)
        );
        assert_eq!(
            pending.last_valid_metadata_received_at_utc_ms.is_some(),
            !valid_ordinals.is_empty()
        );
        assert!(pending.claim.is_none());
        assert!(pending.requires_fence());

        let summary = polymarket_resolution_followup_summary_v3_row(
            pending,
            pending.budget_horizon_utc_ms() + 1,
            &collector_identity,
        )
        .unwrap();
        assert_eq!(summary.resolution_poll_attempts_recorded, Some(6));
        assert_eq!(
            summary.resolution_followup_status,
            Some(if valid_ordinals.is_empty() {
                ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse
            } else {
                ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal
            })
        );

        let rows = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|event| {
                polymarket_metadata_v5_prefixes_from_event(&event.event)
                    .unwrap()
                    .remove(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 6);
        assert_eq!(
            rows.iter()
                .filter(|row| {
                    row.resolution_poll_result == Some(ResolutionPollResult::ValidNonterminal)
                })
                .count(),
            valid_ordinals.len()
        );
        assert!(rows.iter().all(|row| row.validate().is_complete()));
    }
}

#[tokio::test]
async fn polymarket_terminal_bundle_capacity_rejection_admits_zero_rows() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let completed_at_utc_ms = scheduled_at_utc_ms + 1;
    let sdk_market = serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
        &discovery.current,
    ))
    .unwrap();
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: scheduled_at_utc_ms,
        validation_completed_at_utc_ms: completed_at_utc_ms,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: scheduled_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: scheduled_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_test(scheduled_at_utc_ms).await,
    };
    let mut pending = PendingPolymarketResolution::new(identity.clone(), Some(evidence.clone()));
    pending.in_flight_ordinal = Some(1);
    let post = polymarket_resolution_attempt_v3_row(
        &pending,
        &GammaResolutionPollCompletion {
            slug: identity.slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms,
            result: Ok(sdk_market),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_for_test(completed_at_utc_ms).await,
            ),
        },
        &|| completed_at_utc_ms + 1,
        true,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();
    let terminal = polymarket_resolution_terminal_ws_v3_row(
        &pending,
        &evidence,
        completed_at_utc_ms + 2,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, scheduled_at_utc_ms).await;
    let budget = InflightBudget::new();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let error = send_polymarket_metadata_v3_rows_atomically_with_tickets(
        &sink,
        Instant::now(),
        vec![
            AcceptedPolymarketMetadataRow {
                row: terminal,
                raw_day_ticket: accepted_raw_day_ticket_for_test(scheduled_at_utc_ms).await,
                ws_claimant_source_sequence_epoch_id: Some(
                    evidence.source_sequence_epoch_id.clone(),
                ),
                ws_claimant_collector_admission_sequence: Some(
                    evidence.collector_admission_sequence,
                ),
            },
            AcceptedPolymarketMetadataRow {
                row: post,
                raw_day_ticket: accepted_raw_day_ticket_for_test(scheduled_at_utc_ms).await,
                ws_claimant_source_sequence_epoch_id: None,
                ws_claimant_collector_admission_sequence: None,
            },
        ],
    )
    .await
    .unwrap_err();

    assert!(
        error.to_string().contains("exceeds ingress capacity"),
        "{error}"
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn polymarket_ws_terminal_old_day_precedes_deferred_new_day_post() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let boundary_utc_ms = scheduled_at_utc_ms
        .div_euclid(UTC_DAY_MS)
        .saturating_add(1)
        .saturating_mul(UTC_DAY_MS);
    let terminal_received_at_utc_ms = boundary_utc_ms - 1;
    let post_received_at_utc_ms = boundary_utc_ms + 1;
    let sdk_market = serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
        &discovery.current,
    ))
    .unwrap();
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: terminal_received_at_utc_ms,
        validation_completed_at_utc_ms: terminal_received_at_utc_ms,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: terminal_received_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: terminal_received_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_test(terminal_received_at_utc_ms).await,
    };
    let mut pending = PendingPolymarketResolution::new(identity.clone(), Some(evidence.clone()));
    pending.in_flight_ordinal = Some(1);
    let post = polymarket_resolution_attempt_v3_row(
        &pending,
        &GammaResolutionPollCompletion {
            slug: identity.slug,
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms: post_received_at_utc_ms,
            result: Ok(sdk_market),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_for_test(post_received_at_utc_ms).await,
            ),
        },
        &|| post_received_at_utc_ms,
        true,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();
    let terminal = polymarket_resolution_terminal_ws_v3_row(
        &pending,
        &evidence,
        post_received_at_utc_ms,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, terminal_received_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    let terminal_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&gate, terminal_received_at_utc_ms).await;
    let post_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&gate, post_received_at_utc_ms).await;
    send_polymarket_terminal_prefix_ordered(
        &sink,
        Instant::now(),
        vec![
            AcceptedPolymarketMetadataRow {
                row: terminal,
                raw_day_ticket: terminal_ticket,
                ws_claimant_source_sequence_epoch_id: Some(
                    "test-polymarket-source-epoch".to_owned(),
                ),
                ws_claimant_collector_admission_sequence: Some(1),
            },
            AcceptedPolymarketMetadataRow {
                row: post,
                raw_day_ticket: post_ticket,
                ws_claimant_source_sequence_epoch_id: None,
                ws_claimant_collector_admission_sequence: None,
            },
        ],
    )
    .await
    .unwrap();

    let terminal_event = rx.recv().await.unwrap();
    assert_eq!(
        terminal_event.event.recv_time_utc_ms,
        terminal_received_at_utc_ms
    );
    assert_eq!(
        terminal_event
            .event
            .collector_admission
            .as_ref()
            .unwrap()
            .sequence,
        1
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        deferred_ingress_facts(&stats, "polymarket"),
        vec![DeferredIngressBatchFact {
            source_kinds: vec!["polymarket_market_metadata".to_owned()],
            recv_times_utc_ms: vec![post_received_at_utc_ms],
            has_collector_admission: vec![false],
            accepted_defer: true,
            typed_polymarket_v3: false,
        }]
    );

    let close = gate
        .close_for_rollover(
            &utc_day_string_from_ms(terminal_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .await
        .unwrap();
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    flush_deferred_raw_events(&sink, "polymarket", &close)
        .await
        .unwrap();
    let post_event = rx.recv().await.unwrap();
    assert_eq!(post_event.event.recv_time_utc_ms, post_received_at_utc_ms);
    assert_eq!(
        post_event
            .event
            .collector_admission
            .as_ref()
            .unwrap()
            .sequence,
        2
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_day_fence_orders_ws_old_before_gamma_new() {
    let boundary_utc_ms: i64 = 1_783_468_800_000;
    let mut market = test_btc_discovery().current;
    market.end_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
    market.start_ts_s = market.end_ts_s.saturating_sub(300);
    market.candidate_start_ts_s = market.start_ts_s;
    market.candidate_end_ts_s = market.end_ts_s;
    market.slug = format!("btc-updown-5m-{}", market.start_ts_s);
    let discovery_received_at_utc_ms = market.start_ts_s.saturating_mul(1_000).saturating_sub(1);
    refresh_test_discovery_evidence(&mut market, discovery_received_at_utc_ms);
    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let scheduled_at_utc_ms = identity
        .market_end_utc_ms
        .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    assert!(scheduled_at_utc_ms < boundary_utc_ms);
    let ws_received_at_utc_ms = boundary_utc_ms - 1;
    let gamma_received_at_utc_ms = boundary_utc_ms + 1;
    let actor_now_utc_ms = boundary_utc_ms + 2;
    let mut pending = PendingPolymarketResolution::new(identity.clone(), None);
    pending.in_flight_ordinal = Some(1);
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    context.pending.insert(identity.slug.clone(), pending);
    context.actor_now_utc_ms = actor_now_utc_ms;

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
    gate.prepare_rollover(
        &utc_day_string_from_ms(ws_received_at_utc_ms).unwrap(),
        boundary_utc_ms,
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let items = vec![
        PolymarketBoundaryDrainItem::Gamma(Box::new(GammaResolutionPollCompletion {
            slug: identity.slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms: gamma_received_at_utc_ms,
            result: Ok(serde_json::from_value::<SdkGammaMarket>(
                gamma_market_value_for_discovered(&market),
            )
            .unwrap()),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_from_gate_for_test(&gate, gamma_received_at_utc_ms).await,
            ),
        })),
        PolymarketBoundaryDrainItem::Market(Box::new(
            budgeted_polymarket_message_at_for_test(
                market_resolved_message_for_test(&market, ws_received_at_utc_ms),
                ws_received_at_utc_ms,
                &global_budget,
                &decoded_budget,
            )
            .await,
        )),
    ];
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };
    process_polymarket_boundary_drain_items(
        items,
        Some(boundary_utc_ms),
        &timeline,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .unwrap();
    finalize_polymarket_outer_ready_pending(
        &timeline,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
    )
    .await
    .unwrap();

    assert!(context.pending.is_empty());
    let admitted = [rx.recv().await.unwrap(), rx.recv().await.unwrap()];
    assert!(polymarket_v3_message(&admitted[0]).is_some());
    let terminal = polymarket_metadata_v5_prefixes_from_event(
        polymarket_metadata_event(&admitted[1]).expect("second row must be metadata v5"),
    )
    .unwrap()
    .remove(0);
    assert_eq!(
        terminal.metadata_row_kind,
        PolymarketMetadataRowKind::ResolutionTerminalWs
    );
    assert_eq!(
        deferred_ingress_facts(&stats, "polymarket"),
        vec![DeferredIngressBatchFact {
            source_kinds: vec!["polymarket_market_metadata".to_owned()],
            recv_times_utc_ms: vec![gamma_received_at_utc_ms],
            has_collector_admission: vec![false],
            accepted_defer: true,
            typed_polymarket_v3: false,
        }]
    );
    let close = gate
        .close_for_rollover(
            &utc_day_string_from_ms(ws_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .await
        .unwrap();
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    flush_deferred_raw_events(&sink, "polymarket", &close)
        .await
        .unwrap();
    let post = polymarket_metadata_v5_prefixes_from_event(&rx.recv().await.unwrap().event)
        .unwrap()
        .remove(0);
    assert_eq!(
        post.resolution_poll_result,
        Some(ResolutionPollResult::PostTerminalClaimObservation)
    );
    assert_eq!(post.timestamp_received, gamma_received_at_utc_ms);
}

#[tokio::test]
async fn polymarket_day_fence_orders_gamma_old_before_ws_new() {
    for gamma_terminal in [false, true] {
        let boundary_utc_ms: i64 = 1_783_468_800_000;
        let mut market = test_btc_discovery().current;
        market.end_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
        market.start_ts_s = market.end_ts_s.saturating_sub(300);
        market.candidate_start_ts_s = market.start_ts_s;
        market.candidate_end_ts_s = market.end_ts_s;
        market.slug = format!("btc-updown-5m-{}", market.start_ts_s);
        let discovery_received_at_utc_ms =
            market.start_ts_s.saturating_mul(1_000).saturating_sub(1);
        refresh_test_discovery_evidence(&mut market, discovery_received_at_utc_ms);
        let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
        let scheduled_at_utc_ms = identity
            .market_end_utc_ms
            .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
        assert!(scheduled_at_utc_ms < boundary_utc_ms);
        let gamma_received_at_utc_ms = boundary_utc_ms - 1;
        let ws_received_at_utc_ms = boundary_utc_ms + 1;
        let actor_now_utc_ms = boundary_utc_ms + 2;
        let mut pending = PendingPolymarketResolution::new(identity.clone(), None);
        pending.in_flight_ordinal = Some(1);
        let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
            pending: vec![recovered_pending_for_test(
                &market,
                Vec::new(),
                Vec::new(),
                1,
            )],
        })
        .unwrap()
        .unwrap();
        context.pending.insert(identity.slug.clone(), pending);
        context.actor_now_utc_ms = actor_now_utc_ms;

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let gate = install_active_raw_day_gate(&stats, gamma_received_at_utc_ms).await;
        gate.prepare_rollover(
            &utc_day_string_from_ms(gamma_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let global_budget = InflightBudget::new();
        let decoded_budget = InflightBudget::new();
        let gamma_market = if gamma_terminal {
            gamma_terminal_market_for_discovered(&market)
        } else {
            serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(&market))
                .unwrap()
        };
        let mut ws_message = budgeted_polymarket_message_at_for_test(
            market_resolved_message_for_test(&market, ws_received_at_utc_ms),
            ws_received_at_utc_ms,
            &global_budget,
            &decoded_budget,
        )
        .await;
        ws_message.raw_day_ticket =
            accepted_raw_day_ticket_from_gate_for_test(&gate, ws_received_at_utc_ms).await;
        let items = vec![
            PolymarketBoundaryDrainItem::Market(Box::new(ws_message)),
            PolymarketBoundaryDrainItem::Gamma(Box::new(GammaResolutionPollCompletion {
                slug: identity.slug.clone(),
                ordinal: 1,
                scheduled_at_utc_ms,
                fetch_start_utc_ms: scheduled_at_utc_ms,
                completed_at_utc_ms: gamma_received_at_utc_ms,
                result: Ok(gamma_market),
                raw_day_ticket: RawDayTicketAcceptance::Accepted(
                    accepted_raw_day_ticket_from_gate_for_test(&gate, gamma_received_at_utc_ms)
                        .await,
                ),
            })),
        ];
        let timeline = || PolymarketTimelineNow {
            utc_ms: actor_now_utc_ms,
            monotonic: TokioInstant::now(),
        };
        process_polymarket_boundary_drain_items(
            items,
            Some(boundary_utc_ms),
            &timeline,
            &mut context,
            &sink,
            &collector_identity,
            Instant::now(),
            &mut recovery,
        )
        .await
        .unwrap();
        finalize_polymarket_outer_ready_pending(
            &timeline,
            &mut context,
            &sink,
            &collector_identity,
            Instant::now(),
        )
        .await
        .unwrap();

        assert!(context.pending.is_empty());
        let old_day_row =
            polymarket_metadata_v5_prefixes_from_event(&rx.recv().await.unwrap().event)
                .unwrap()
                .remove(0);
        if gamma_terminal {
            assert_eq!(
                old_day_row.resolution_followup_status,
                Some(ResolutionFollowupStatus::TerminalGammaFallback)
            );
            assert_eq!(
                old_day_row.terminal_claim_owner,
                Some(TerminalClaimOwner::GammaBySlug)
            );
        } else {
            assert_eq!(
                old_day_row.resolution_poll_result,
                Some(ResolutionPollResult::ValidNonterminal)
            );
        }
        let deferred = deferred_ingress_facts(&stats, "polymarket");
        assert!(deferred.iter().any(|batch| {
            batch.typed_polymarket_v3
                && batch.source_kinds == ["polymarket_market"]
                && batch.recv_times_utc_ms == [ws_received_at_utc_ms]
                && batch.has_collector_admission == [true]
                && batch.accepted_defer
        }));
        let close = gate
            .close_for_rollover(
                &utc_day_string_from_ms(gamma_received_at_utc_ms).unwrap(),
                boundary_utc_ms,
            )
            .await
            .unwrap();
        commit_test_manifest_activation(&gate, &close);
        gate.activate_after_rollover(&close).await.unwrap();
        flush_deferred_raw_events(&sink, "polymarket", &close)
            .await
            .unwrap();
        let new_day_events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(
            new_day_events
                .iter()
                .filter_map(polymarket_v3_message)
                .any(|message| {
                    message.envelope.timestamp_received == ws_received_at_utc_ms
                        && matches!(message.event, PolymarketOrderbookV3Event::MarketResolved(_))
                }),
            "the admitted new-day typed v3 row must survive rollover"
        );
        if !gamma_terminal {
            assert!(
                new_day_events
                    .iter()
                    .filter_map(polymarket_metadata_event)
                    .any(|event| {
                        polymarket_metadata_v5_prefixes_from_event(event).is_ok_and(|rows| {
                            rows[0].metadata_row_kind
                                == PolymarketMetadataRowKind::ResolutionTerminalWs
                        })
                    })
            );
        }
    }
}

#[tokio::test]
async fn polymarket_hour_fence_keeps_gamma_claim_and_rejects_post_horizon_ws() {
    let discovery = test_btc_discovery();
    let market = discovery.current.clone();
    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let scheduled_at_utc_ms = identity
        .market_end_utc_ms
        .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    let hour_boundary_utc_ms = scheduled_at_utc_ms
        .div_euclid(UTC_HOUR_MS)
        .saturating_add(1)
        .saturating_mul(UTC_HOUR_MS);
    let gamma_completed_at_utc_ms = hour_boundary_utc_ms - 1;
    let ws_received_at_utc_ms = hour_boundary_utc_ms;
    let actor_now_utc_ms = hour_boundary_utc_ms + 1;

    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    context.actor_now_utc_ms = actor_now_utc_ms;
    let mut resolution_poll_tasks = JoinSet::new();
    let completion = GammaResolutionPollCompletion {
        slug: identity.slug.clone(),
        ordinal: 1,
        scheduled_at_utc_ms,
        fetch_start_utc_ms: scheduled_at_utc_ms,
        completed_at_utc_ms: gamma_completed_at_utc_ms,
        result: Ok(gamma_terminal_market_for_discovered(&market)),
        raw_day_ticket: RawDayTicketAcceptance::Accepted(
            accepted_raw_day_ticket_for_test(gamma_completed_at_utc_ms).await,
        ),
    };
    let handle = resolution_poll_tasks.spawn(async move { completion });
    {
        let pending = context.pending.get_mut(&identity.slug).unwrap();
        pending.recovery_reclassification_pending = false;
        pending.in_flight_ordinal = Some(1);
        pending.poll_abort_handle = Some(handle);
    }
    tokio::task::yield_now().await;

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, gamma_completed_at_utc_ms).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let ws_message = budgeted_polymarket_message_at_for_test(
        market_resolved_message_for_test(&market, ws_received_at_utc_ms),
        ws_received_at_utc_ms,
        &global_budget,
        &decoded_budget,
    )
    .await;
    let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };

    arbitrate_polymarket_fence_with_ready_gamma(
        &mut runtime,
        &mut resolution_poll_tasks,
        &timeline,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .unwrap();
    finalize_polymarket_outer_ready_pending(
        &timeline,
        &mut context,
        &sink,
        &collector_identity,
        Instant::now(),
    )
    .await
    .unwrap();

    assert!(context.pending.is_empty());
    assert!(resolution_poll_tasks.is_empty());
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(!events.iter().any(|event| {
        polymarket_v3_message(event)
            .is_some_and(|message| message.envelope.timestamp_received == ws_received_at_utc_ms)
    }));
    let metadata = events
        .iter()
        .filter_map(polymarket_metadata_event)
        .flat_map(|event| polymarket_metadata_v5_prefixes_from_event(event).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0].terminal_claim_owner,
        Some(TerminalClaimOwner::GammaBySlug)
    );
    assert_eq!(
        metadata[0].resolution_followup_status,
        Some(ResolutionFollowupStatus::TerminalGammaFallback)
    );
    assert_eq!(metadata[0].timestamp_received, gamma_completed_at_utc_ms);
    assert!(metadata.iter().all(|row| row.resolution_poll_result
        != Some(ResolutionPollResult::PostTerminalClaimObservation)));
}

#[tokio::test]
async fn polymarket_connected_stop_deadline_and_control_close_quiesce_ws_and_abort_network() {
    for clean_exit in ["stop", "deadline", "control_close"] {
        let discovery = test_btc_discovery();
        let market = discovery.current.clone();
        let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
        let ws_received_at_utc_ms = identity.market_end_utc_ms + 1;
        let actor_now_utc_ms = ws_received_at_utc_ms + 1;
        let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
            pending: vec![recovered_pending_for_test(
                &market,
                Vec::new(),
                Vec::new(),
                1,
            )],
        })
        .unwrap()
        .unwrap();
        context.actor_now_utc_ms = actor_now_utc_ms;
        let mut resolution_poll_tasks = JoinSet::new();
        let handle =
            resolution_poll_tasks.spawn(std::future::pending::<GammaResolutionPollCompletion>());
        {
            let pending = context.pending.get_mut(&identity.slug).unwrap();
            pending.recovery_reclassification_pending = false;
            pending.in_flight_ordinal = Some(1);
            pending.poll_abort_handle = Some(handle);
        }

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
        let raw_day_gate = stats.raw_day_gate().unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let global_budget = InflightBudget::new();
        let decoded_budget = InflightBudget::new();
        let mut ws_message = budgeted_polymarket_message_at_for_test(
            market_resolved_message_for_test(&market, ws_received_at_utc_ms),
            ws_received_at_utc_ms,
            &global_budget,
            &decoded_budget,
        )
        .await;
        ws_message.raw_day_ticket =
            accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
        let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
        if clean_exit != "control_close" {
            let cause = if clean_exit == "stop" {
                RuntimeCloseCause::OperatorStop
            } else {
                RuntimeCloseCause::RunDeadline
            };
            assert!(matches!(
                raw_day_gate
                    .prepare_runtime_exit_with_cause(actor_now_utc_ms, cause)
                    .unwrap(),
                PreparedRuntimeExit::SameDay(_)
            ));
        }
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let timeline = || PolymarketTimelineNow {
            utc_ms: actor_now_utc_ms,
            monotonic: TokioInstant::now(),
        };
        let quiesce_started = Instant::now();

        if clean_exit == "stop" {
            let (reply_tx, reply_rx) = oneshot::channel();
            assert!(
                handle_polymarket_source_control(
                    PolymarketSourceControl::Fence {
                        stop_after_fence: true,
                        day_boundary_utc_ms: None,
                        resume_after_barrier: None,
                        reply: reply_tx,
                    },
                    &mut runtime,
                    &mut context,
                    &mut resolution_poll_tasks,
                    &timeline,
                    &sink,
                    &collector_identity,
                    Instant::now(),
                    &mut recovery,
                )
                .await
                .unwrap()
            );
            reply_rx.await.unwrap().unwrap();
        } else {
            quiesce_connected_polymarket_source(
                &mut runtime,
                &mut resolution_poll_tasks,
                &timeline,
                &mut context,
                &sink,
                &collector_identity,
                Instant::now(),
                &mut recovery,
            )
            .await
            .unwrap();
        }

        assert!(
            quiesce_started.elapsed() < Duration::from_secs(1),
            "{clean_exit} must abort an unfinished network poll instead of waiting its request timeout"
        );
        assert!(resolution_poll_tasks.is_empty());
        assert!(context.pending.is_empty());
        assert!(runtime.pump.is_none());
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(events.len(), 2, "{clean_exit}");
        assert!(events.iter().any(|event| {
            polymarket_v3_message(event)
                .is_some_and(|message| message.envelope.timestamp_received == ws_received_at_utc_ms)
        }));
        let terminal = events
            .iter()
            .find_map(polymarket_metadata_event)
            .and_then(|event| {
                polymarket_metadata_v5_prefixes_from_event(event)
                    .ok()
                    .and_then(|mut rows| rows.pop())
            })
            .unwrap();
        assert_eq!(
            terminal.metadata_row_kind,
            PolymarketMetadataRowKind::ResolutionTerminalWs
        );
        assert_eq!(
            terminal.terminal_claim_owner,
            Some(TerminalClaimOwner::MarketResolvedWs)
        );
    }
}

#[tokio::test]
async fn polymarket_stop_materializes_accepted_current_ws_after_e_before_reply() {
    for clean_exit in ["stop", "deadline", "control_close"] {
        let discovery = test_btc_discovery();
        let market = discovery.current.clone();
        let market_end_utc_ms = market.end_ts_s.saturating_mul(1_000);
        let ws_received_at_utc_ms = market_end_utc_ms.saturating_add(1);
        let actor_now_utc_ms = ws_received_at_utc_ms.saturating_add(1);
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let mut context = PolymarketProcessingContext::new(discovery.clone());
        context
            .register_source_authority(PolymarketMetadataV3ReferenceRecord::from(
                &polymarket_metadata_snapshot_v3_row(
                    &market,
                    MarketRuntimeRole::Current,
                    &collector_identity,
                )
                .unwrap(),
            ))
            .unwrap();
        context.actor_now_utc_ms = actor_now_utc_ms;
        context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let raw_day_gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let global_budget = InflightBudget::new();
        let decoded_budget = InflightBudget::new();
        let mut ws_message = budgeted_polymarket_message_at_for_test(
            market_resolved_message_for_test(&market, ws_received_at_utc_ms),
            ws_received_at_utc_ms,
            &global_budget,
            &decoded_budget,
        )
        .await;
        ws_message.raw_day_ticket =
            accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
        let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
        if clean_exit != "control_close" {
            let cause = if clean_exit == "stop" {
                RuntimeCloseCause::OperatorStop
            } else {
                RuntimeCloseCause::RunDeadline
            };
            assert!(matches!(
                raw_day_gate
                    .prepare_runtime_exit_with_cause(actor_now_utc_ms, cause)
                    .unwrap(),
                PreparedRuntimeExit::SameDay(_)
            ));
        }

        let mut resolution_poll_tasks = JoinSet::new();
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let timeline = || PolymarketTimelineNow {
            utc_ms: actor_now_utc_ms,
            monotonic: TokioInstant::now(),
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        assert!(
            handle_polymarket_source_control(
                PolymarketSourceControl::Fence {
                    stop_after_fence: true,
                    day_boundary_utc_ms: None,
                    resume_after_barrier: None,
                    reply: reply_tx,
                },
                &mut runtime,
                &mut context,
                &mut resolution_poll_tasks,
                &timeline,
                &sink,
                &collector_identity,
                Instant::now(),
                &mut recovery,
            )
            .await
            .unwrap()
        );
        reply_rx.await.unwrap().unwrap();

        assert!(context.current.is_none(), "{clean_exit}");
        assert!(context.pending.is_empty(), "{clean_exit}");
        assert!(context.early_ws_claims.is_empty(), "{clean_exit}");
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(events.len(), 3, "{clean_exit}");
        assert!(polymarket_v3_message(&events[0]).is_some());
        let metadata = events
            .iter()
            .skip(1)
            .flat_map(|event| polymarket_metadata_v5_prefixes_from_event(&event.event).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(metadata.len(), 2, "{clean_exit}");
        assert_eq!(
            metadata[0].market_runtime_role_transition,
            Some(MarketRuntimeRoleTransition::CurrentToResolutionPending),
            "{clean_exit}"
        );
        assert_eq!(
            metadata[1].metadata_row_kind,
            PolymarketMetadataRowKind::ResolutionTerminalWs,
            "{clean_exit}"
        );
    }
}

#[tokio::test]
async fn polymarket_clean_stop_rejects_current_and_next_pre_e_claims() {
    for claim_role in ["current", "next"] {
        let mut discovery = test_btc_discovery();
        discovery.next = Some(next_market_for_test(&discovery.current));
        let market = if claim_role == "current" {
            discovery.current.clone()
        } else {
            discovery.next.clone().unwrap()
        };
        let current_end_utc_ms = discovery.current.end_ts_s.saturating_mul(1_000);
        let ws_received_at_utc_ms = current_end_utc_ms.saturating_sub(2);
        let actor_now_utc_ms = current_end_utc_ms.saturating_sub(1);
        let mut context = PolymarketProcessingContext::new(discovery.clone());
        context.actor_now_utc_ms = actor_now_utc_ms;
        context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let raw_day_gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let global_budget = InflightBudget::new();
        let decoded_budget = InflightBudget::new();
        let mut ws_message = budgeted_polymarket_message_at_for_test(
            market_resolved_message_for_test(&market, ws_received_at_utc_ms),
            ws_received_at_utc_ms,
            &global_budget,
            &decoded_budget,
        )
        .await;
        ws_message.raw_day_ticket =
            accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
        let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
        let mut resolution_poll_tasks = JoinSet::new();
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let timeline = || PolymarketTimelineNow {
            utc_ms: actor_now_utc_ms,
            monotonic: TokioInstant::now(),
        };
        let (reply_tx, reply_rx) = oneshot::channel();

        let error = handle_polymarket_source_control(
            PolymarketSourceControl::Fence {
                stop_after_fence: true,
                day_boundary_utc_ms: None,
                resume_after_barrier: None,
                reply: reply_tx,
            },
            &mut runtime,
            &mut context,
            &mut resolution_poll_tasks,
            &timeline,
            &sink,
            &collector_identity,
            Instant::now(),
            &mut recovery,
        )
        .await
        .expect_err("pre-E accepted WS claim must fail a clean stop");
        let reply_error = reply_rx
            .await
            .unwrap()
            .expect_err("pre-E accepted WS claim must reject the stop reply");
        assert_eq!(reply_error, error.to_string(), "{claim_role}");

        assert_eq!(
            context
                .current
                .as_ref()
                .map(|current| current.slug.as_str()),
            Some(discovery.current.slug.as_str()),
            "{claim_role}"
        );
        assert_eq!(
            context.next.as_ref().map(|next| next.slug.as_str()),
            discovery.next.as_ref().map(|next| next.slug.as_str()),
            "{claim_role}"
        );
        assert!(context.pending.is_empty(), "{claim_role}");
        assert_eq!(context.early_ws_claims.len(), 1, "{claim_role}");
        assert!(
            context.early_ws_claims.contains_key(&market.slug),
            "{claim_role}"
        );
        let admitted = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(admitted.len(), 1, "{claim_role}");
        assert!(
            polymarket_v3_message(&admitted[0]).is_some(),
            "{claim_role}"
        );
    }
}

#[tokio::test]
async fn polymarket_day_fence_rejects_next_pre_e_claim_before_reply() {
    let boundary_utc_ms: i64 = 1_783_468_800_000;
    let mut current = test_btc_discovery().current;
    current.start_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
    current.end_ts_s = boundary_utc_ms.div_euclid(1_000);
    current.candidate_start_ts_s = current.start_ts_s;
    current.candidate_end_ts_s = current.end_ts_s;
    current.slug = format!("btc-updown-5m-{}", current.start_ts_s);
    let received_at_utc_ms = current.start_ts_s.saturating_mul(1_000).saturating_sub(1);
    refresh_test_discovery_evidence(&mut current, received_at_utc_ms);
    let discovery = BtcUpdownDiscovery {
        next: Some(next_market_for_test(&current)),
        current,
        relation: None,
    };
    let next = discovery.next.clone().unwrap();
    let ws_received_at_utc_ms = boundary_utc_ms.saturating_sub(1);
    let actor_now_utc_ms = boundary_utc_ms.saturating_add(1);
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    context.actor_now_utc_ms = actor_now_utc_ms;
    context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
    raw_day_gate
        .prepare_rollover(
            &utc_day_string_from_ms(ws_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .unwrap();
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let mut ws_message = budgeted_polymarket_message_at_for_test(
        market_resolved_message_for_test(&next, ws_received_at_utc_ms),
        ws_received_at_utc_ms,
        &global_budget,
        &decoded_budget,
    )
    .await;
    ws_message.raw_day_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
    let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
    let mut resolution_poll_tasks = JoinSet::new();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };
    let (resume_tx, resume_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();

    let (result, reply) = timeout(Duration::from_secs(1), async {
        tokio::join!(
            handle_polymarket_source_control(
                PolymarketSourceControl::Fence {
                    stop_after_fence: false,
                    day_boundary_utc_ms: Some(boundary_utc_ms),
                    resume_after_barrier: Some(resume_rx),
                    reply: reply_tx,
                },
                &mut runtime,
                &mut context,
                &mut resolution_poll_tasks,
                &timeline,
                &sink,
                &collector_identity,
                Instant::now(),
                &mut recovery,
            ),
            async {
                let reply = reply_rx.await.unwrap();
                drop(resume_tx);
                reply
            }
        )
    })
    .await
    .expect("next pre-E day fence must fail closed without waiting for resume");
    let error = result.expect_err("next pre-E day fence must fail");
    assert_eq!(
        reply.expect_err("day fence reply must fail"),
        error.to_string()
    );
    assert_eq!(
        context
            .current
            .as_ref()
            .map(|current| current.slug.as_str()),
        Some(discovery.current.slug.as_str())
    );
    assert_eq!(
        context.next.as_ref().map(|market| market.slug.as_str()),
        Some(next.slug.as_str())
    );
    assert!(context.pending.is_empty());
    assert_eq!(context.early_ws_claims.len(), 1);
    assert!(context.early_ws_claims.contains_key(&next.slug));
    let admitted = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(admitted.len(), 1);
    assert!(polymarket_v3_message(&admitted[0]).is_some());
}

#[tokio::test]
async fn polymarket_2355_stop_defers_raw_e_transition_and_terminal_without_deadlock() {
    let boundary_utc_ms: i64 = 1_783_468_800_000;
    let mut market = test_btc_discovery().current;
    market.start_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
    market.end_ts_s = boundary_utc_ms.div_euclid(1_000);
    market.candidate_start_ts_s = market.start_ts_s;
    market.candidate_end_ts_s = market.end_ts_s;
    market.slug = format!("btc-updown-5m-{}", market.start_ts_s);
    let discovery_received_at_utc_ms = market.start_ts_s.saturating_mul(1_000).saturating_sub(1);
    refresh_test_discovery_evidence(&mut market, discovery_received_at_utc_ms);
    let discovery = BtcUpdownDiscovery {
        current: market.clone(),
        next: None,
        relation: None,
    };
    let ws_received_at_utc_ms = boundary_utc_ms.saturating_add(1);
    let actor_now_utc_ms = ws_received_at_utc_ms.saturating_add(1);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    context
        .register_source_authority(PolymarketMetadataV3ReferenceRecord::from(
            &polymarket_metadata_snapshot_v3_row(
                &market,
                MarketRuntimeRole::Current,
                &collector_identity,
            )
            .unwrap(),
        ))
        .unwrap();
    context.actor_now_utc_ms = actor_now_utc_ms;
    context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, boundary_utc_ms - 1).await;
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let mut ws_message = budgeted_polymarket_message_at_for_test(
        market_resolved_message_for_test(&market, ws_received_at_utc_ms),
        ws_received_at_utc_ms,
        &global_budget,
        &decoded_budget,
    )
    .await;
    ws_message.raw_day_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
    assert!(matches!(
        &ws_message.raw_day_ticket,
        RawDayAcceptedTicket::Defer(_)
    ));
    let PreparedRuntimeExit::Rollover(exit) = raw_day_gate
        .prepare_runtime_exit_with_cause(actor_now_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap()
    else {
        panic!("23:55 stop must prepare a rollover exit");
    };
    let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut resolution_poll_tasks = JoinSet::new();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };
    let (reply_tx, reply_rx) = oneshot::channel();

    assert!(
        timeout(
            Duration::from_secs(1),
            handle_polymarket_source_control(
                PolymarketSourceControl::Fence {
                    stop_after_fence: true,
                    day_boundary_utc_ms: None,
                    resume_after_barrier: None,
                    reply: reply_tx,
                },
                &mut runtime,
                &mut context,
                &mut resolution_poll_tasks,
                &timeline,
                &sink,
                &collector_identity,
                Instant::now(),
                &mut recovery,
            ),
        )
        .await
        .expect("23:55 accepted stop drain must be bounded")
        .unwrap()
    );
    reply_rx.await.unwrap().unwrap();
    assert!(context.current.is_none());
    assert!(context.pending.is_empty());
    assert!(context.early_ws_claims.is_empty());
    assert!(event_rx.try_recv().is_err());
    let deferred = deferred_ingress_facts(&stats, "polymarket");
    assert_eq!(deferred.len(), 3);
    assert!(deferred.iter().all(|batch| batch.accepted_defer));
    assert!(deferred[0].typed_polymarket_v3);
    assert!(
        deferred
            .iter()
            .skip(1)
            .all(|batch| !batch.typed_polymarket_v3)
    );

    for producer in LOGICAL_PRODUCERS {
        raw_day_gate
            .register_source_fence(producer, &exit.close)
            .unwrap();
    }
    let close = timeout(
        Duration::from_secs(1),
        raw_day_gate.seal_prepared_rollover(&exit.close),
    )
    .await
    .expect("detached accepted tickets must not deadlock the day seal")
    .unwrap();
    flush_accepted_deferred_raw_events_for_stop(&sink, "polymarket", &close)
        .await
        .unwrap();

    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert!(polymarket_v3_message(&events[0]).is_some());
    let metadata = events
        .iter()
        .skip(1)
        .flat_map(|event| polymarket_metadata_v5_prefixes_from_event(&event.event).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(metadata.len(), 2);
    assert_eq!(
        metadata[0].market_runtime_role_transition,
        Some(MarketRuntimeRoleTransition::CurrentToResolutionPending)
    );
    assert_eq!(
        metadata[1].metadata_row_kind,
        PolymarketMetadataRowKind::ResolutionTerminalWs
    );
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_2355_day_fence_finalizes_e_minus_one_ws_before_reply() {
    let boundary_utc_ms: i64 = 1_783_468_800_000;
    let mut current = test_btc_discovery().current;
    current.start_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
    current.end_ts_s = boundary_utc_ms.div_euclid(1_000);
    current.candidate_start_ts_s = current.start_ts_s;
    current.candidate_end_ts_s = current.end_ts_s;
    current.slug = format!("btc-updown-5m-{}", current.start_ts_s);
    let received_at_utc_ms = current.start_ts_s.saturating_mul(1_000).saturating_sub(1);
    refresh_test_discovery_evidence(&mut current, received_at_utc_ms);
    let next = next_market_for_test(&current);
    let discovery = BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(next.clone()),
        relation: None,
    };
    let ws_received_at_utc_ms = boundary_utc_ms.saturating_sub(1);
    let actor_now_utc_ms = boundary_utc_ms.saturating_add(1);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    for (market, role) in [
        (&current, MarketRuntimeRole::Current),
        (&next, MarketRuntimeRole::Next),
    ] {
        context
            .register_source_authority(PolymarketMetadataV3ReferenceRecord::from(
                &polymarket_metadata_snapshot_v3_row(market, role, &collector_identity).unwrap(),
            ))
            .unwrap();
    }
    context.actor_now_utc_ms = actor_now_utc_ms;
    context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
    let close = raw_day_gate
        .prepare_rollover(
            &utc_day_string_from_ms(ws_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .unwrap();
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let mut ws_message = budgeted_polymarket_message_at_for_test(
        market_resolved_message_for_test(&current, ws_received_at_utc_ms),
        ws_received_at_utc_ms,
        &global_budget,
        &decoded_budget,
    )
    .await;
    ws_message.raw_day_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
    assert!(matches!(
        ws_message.raw_day_ticket,
        RawDayAcceptedTicket::Permit(_)
    ));
    let mut runtime = polymarket_runtime_with_fence_messages_for_test(vec![ws_message]).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut resolution_poll_tasks = JoinSet::new();
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let timeline = || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    };
    let (resume_tx, resume_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();

    let (result, ()) = timeout(Duration::from_secs(1), async {
        tokio::join!(
            handle_polymarket_source_control(
                PolymarketSourceControl::Fence {
                    stop_after_fence: false,
                    day_boundary_utc_ms: Some(boundary_utc_ms),
                    resume_after_barrier: Some(resume_rx),
                    reply: reply_tx,
                },
                &mut runtime,
                &mut context,
                &mut resolution_poll_tasks,
                &timeline,
                &sink,
                &collector_identity,
                Instant::now(),
                &mut recovery,
            ),
            async {
                reply_rx
                    .await
                    .unwrap()
                    .expect("23:55 day fence must reply after terminal finalization");
                let before_activation =
                    std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
                assert_eq!(before_activation.len(), 2);
                assert!(polymarket_v3_message(&before_activation[0]).is_some());
                let terminal =
                    polymarket_metadata_v5_prefixes_from_event(&before_activation[1].event)
                        .unwrap()
                        .remove(0);
                assert_eq!(
                    terminal.metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionTerminalWs
                );
                let deferred = deferred_ingress_facts(&stats, "polymarket");
                assert_eq!(deferred.len(), 2);

                for producer in LOGICAL_PRODUCERS {
                    raw_day_gate
                        .register_source_fence(producer, &close)
                        .unwrap();
                }
                let sealed = raw_day_gate.seal_prepared_rollover(&close).await.unwrap();
                commit_test_manifest_activation(&raw_day_gate, &sealed);
                raw_day_gate.activate_after_rollover(&sealed).await.unwrap();
                flush_deferred_raw_events(&sink, "polymarket", &sealed)
                    .await
                    .unwrap();
                let after_activation =
                    std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
                assert_eq!(after_activation.len(), 2);
                let transitions = after_activation
                    .iter()
                    .map(|event| {
                        polymarket_metadata_v5_prefixes_from_event(&event.event)
                            .unwrap()
                            .remove(0)
                            .market_runtime_role_transition
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    transitions,
                    vec![
                        MarketRuntimeRoleTransition::CurrentToResolutionPending,
                        MarketRuntimeRoleTransition::NextToCurrent,
                    ]
                );
                drop(resume_tx);
            }
        )
    })
    .await
    .expect("23:55 day fence fixture must be bounded");
    let error = result.expect_err("dropped test resume must stop the paused handler");
    assert!(
        error
            .to_string()
            .contains("Polymarket day-barrier resume dropped"),
        "{error}"
    );
    assert_eq!(
        context.current.as_ref().map(|market| market.slug.as_str()),
        Some(next.slug.as_str())
    );
    assert!(context.next.is_none());
    assert!(context.pending.is_empty());
    assert!(context.early_ws_claims.is_empty());
}

#[tokio::test]
async fn polymarket_exhausted_summary_uses_applied_day_ticket_across_rollover_cutoffs() {
    async fn run_case(controlled_cause: Option<RuntimeCloseCause>) {
        let boundary_utc_ms: i64 = 1_783_468_800_000;
        let budget_horizon_delay_ms = POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS
            + i64::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT.saturating_sub(1))
                * POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS;
        let market_end_utc_ms = boundary_utc_ms
            .saturating_sub(budget_horizon_delay_ms)
            .saturating_sub(1_000);
        let completion_utc_ms = boundary_utc_ms.saturating_sub(1);
        let applied_at_utc_ms = boundary_utc_ms.saturating_add(1);
        let mut market = test_btc_discovery().current;
        market.start_ts_s = market_end_utc_ms.div_euclid(1_000).saturating_sub(300);
        market.end_ts_s = market_end_utc_ms.div_euclid(1_000);
        market.candidate_start_ts_s = market.start_ts_s;
        market.candidate_end_ts_s = market.end_ts_s;
        market.slug = format!("btc-updown-5m-{}", market.start_ts_s);
        let discovery_received_at_utc_ms =
            market.start_ts_s.saturating_mul(1_000).saturating_sub(1);
        refresh_test_discovery_evidence(&mut market, discovery_received_at_utc_ms);
        let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let raw_day_gate = install_active_raw_day_gate(&stats, completion_utc_ms).await;
        let completion_ticket =
            accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, completion_utc_ms).await;
        let close = if let Some(cause) = controlled_cause.clone() {
            let PreparedRuntimeExit::Rollover(exit) = raw_day_gate
                .prepare_runtime_exit_with_cause(applied_at_utc_ms, cause)
                .unwrap()
            else {
                panic!("cross-day controlled summary must prepare rollover exit");
            };
            exit.close
        } else {
            let old_day = utc_day_string_from_ms(completion_utc_ms).unwrap();
            prepare_rollover_for_producer(&raw_day_gate, "polymarket", &old_day, boundary_utc_ms)
        };

        let mut pending = PendingPolymarketResolution::new(identity.clone(), None);
        pending.attempts_recorded = u32::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT);
        pending.next_attempt_ordinal = POLYMARKET_RESOLUTION_ATTEMPT_COUNT.saturating_add(1);
        pending.transport_unavailable_count = u32::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT);
        pending.exhaustion_requested = true;
        pending.exhaustion_ticket = Some(completion_ticket);
        let mut context = PolymarketProcessingContext::new(test_btc_discovery());
        context.pending.insert(identity.slug.clone(), pending);
        context.rebuild_asset_to_slug();

        let (event_tx, mut event_rx) = mpsc::channel(2);
        let sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };

        finalize_polymarket_ready_pending_while_paused(
            &|| PolymarketTimelineNow {
                utc_ms: applied_at_utc_ms,
                monotonic: TokioInstant::now(),
            },
            &mut context,
            &sink,
            &collector_identity,
            Instant::now(),
        )
        .await
        .unwrap();

        assert!(context.pending.is_empty());
        assert!(event_rx.try_recv().is_err());
        assert_eq!(
            deferred_ingress_facts(&stats, "polymarket"),
            vec![DeferredIngressBatchFact {
                source_kinds: vec!["polymarket_market_metadata".to_owned()],
                recv_times_utc_ms: vec![applied_at_utc_ms],
                has_collector_admission: vec![false],
                accepted_defer: true,
                typed_polymarket_v3: false,
            }]
        );

        for producer in LOGICAL_PRODUCERS {
            raw_day_gate
                .register_source_fence(producer, &close)
                .unwrap();
        }
        let close = raw_day_gate.seal_prepared_rollover(&close).await.unwrap();
        if controlled_cause.is_some() {
            flush_accepted_deferred_raw_events_for_stop(&sink, "polymarket", &close)
                .await
                .unwrap();
        } else {
            flush_deferred_raw_events(&sink, "polymarket", &close)
                .await
                .unwrap();
        }
        let summary = event_rx.recv().await.unwrap();
        let row = polymarket_metadata_v5_prefixes_from_event(&summary.event)
            .unwrap()
            .remove(0);
        assert_eq!(
            row.metadata_row_kind,
            PolymarketMetadataRowKind::ResolutionFollowupSummary
        );
        assert_eq!(row.timestamp_received, applied_at_utc_ms);
        assert!(event_rx.try_recv().is_err());
    }

    run_case(None).await;
    run_case(Some(RuntimeCloseCause::OperatorStop)).await;
    run_case(Some(RuntimeCloseCause::RunDeadline)).await;
}

#[tokio::test]
async fn polymarket_outer_stop_harvests_ready_gamma_before_reply() {
    let discovery = test_btc_discovery();
    let market = discovery.current.clone();
    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let scheduled_at_utc_ms = identity
        .market_end_utc_ms
        .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    let completed_at_utc_ms = scheduled_at_utc_ms + 1;
    let actor_now_utc_ms = completed_at_utc_ms + 1;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, completed_at_utc_ms).await;
    let completion_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, completed_at_utc_ms).await;
    let mut context = PolymarketProcessingContext::from_recovery(PolymarketRestartRecovery {
        pending: vec![recovered_pending_for_test(
            &market,
            Vec::new(),
            Vec::new(),
            1,
        )],
    })
    .unwrap()
    .unwrap();
    context.actor_now_utc_ms = actor_now_utc_ms;
    let mut resolution_poll_tasks = JoinSet::new();
    let completion = GammaResolutionPollCompletion {
        slug: identity.slug.clone(),
        ordinal: 1,
        scheduled_at_utc_ms,
        fetch_start_utc_ms: scheduled_at_utc_ms,
        completed_at_utc_ms,
        result: Ok(gamma_terminal_market_for_discovered(&market)),
        raw_day_ticket: RawDayTicketAcceptance::Accepted(completion_ticket),
    };
    let handle = resolution_poll_tasks.spawn(async move { completion });
    {
        let pending = context.pending.get_mut(&identity.slug).unwrap();
        pending.recovery_reclassification_pending = false;
        pending.in_flight_ordinal = Some(1);
        pending.poll_abort_handle = Some(handle);
    }
    tokio::task::yield_now().await;

    let (event_tx, mut event_rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    recovery.set_wait_override(Some(Duration::from_secs(60)));
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: true,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let timeline = Arc::new(move || PolymarketTimelineNow {
        utc_ms: actor_now_utc_ms,
        monotonic: TokioInstant::now(),
    });
    assert!(matches!(
        raw_day_gate
            .prepare_runtime_exit_with_cause(actor_now_utc_ms, RuntimeCloseCause::OperatorStop,)
            .unwrap(),
        PreparedRuntimeExit::SameDay(_)
    ));

    assert!(
        !wait_for_polymarket_outer_recovery(
            &mut recovery,
            &sink,
            &GammaClient::new("http://127.0.0.1:9").unwrap(),
            None,
            &mut control_rx,
            &mut resolution_poll_tasks,
            &mut context,
            &collector_identity,
            Instant::now(),
            timeline,
        )
        .await
        .unwrap()
    );
    reply_rx.await.unwrap().unwrap();
    assert!(resolution_poll_tasks.is_empty());
    assert!(context.pending.is_empty());
    let event = timeout(Duration::from_secs(1), event_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let row = polymarket_metadata_v5_prefixes_from_event(&event.event)
        .unwrap()
        .remove(0);
    assert_eq!(
        row.terminal_claim_owner,
        Some(TerminalClaimOwner::GammaBySlug)
    );
    assert_eq!(
        row.resolution_followup_status,
        Some(ResolutionFollowupStatus::TerminalGammaFallback)
    );
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_post_cutoff_gamma_clears_owner_without_row_counter_claim_or_exhaustion() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let slug = identity.slug.clone();
    let scheduled_at_utc_ms = identity
        .market_end_utc_ms
        .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    let completed_at_utc_ms = scheduled_at_utc_ms + 1;
    let mut pending = PendingPolymarketResolution::new(identity, None);
    pending.in_flight_ordinal = Some(1);
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    context.pending.insert(slug.clone(), pending);

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, completed_at_utc_ms).await;
    assert!(matches!(
        raw_day_gate
            .prepare_runtime_exit_with_cause(completed_at_utc_ms, RuntimeCloseCause::OperatorStop,)
            .unwrap(),
        PreparedRuntimeExit::SameDay(_)
    ));
    let raw_day_ticket = match raw_day_gate
        .accept_unadmitted_event_ticket("polymarket", completed_at_utc_ms)
        .await
        .unwrap()
    {
        None => RawDayTicketAcceptance::Cutoff,
        Some(ticket) => panic!("post-cutoff Gamma unexpectedly received ticket:{ticket:?}"),
    };
    let (event_tx, mut event_rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    record_gamma_resolution_completion(
        GammaResolutionPollCompletion {
            slug: slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms,
            result: Ok(gamma_terminal_market_for_discovered(&discovery.current)),
            raw_day_ticket,
        },
        &|| completed_at_utc_ms + 1,
        &mut context,
        &sink,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
    )
    .await
    .unwrap();

    let pending = &context.pending[&slug];
    assert!(pending.in_flight_ordinal.is_none());
    assert!(pending.poll_abort_handle.is_none());
    assert_eq!(pending.attempts_recorded, 0);
    assert_eq!(pending.next_attempt_ordinal, 1);
    assert_eq!(pending.valid_response_count, 0);
    assert_eq!(pending.transport_unavailable_count, 0);
    assert!(pending.claim.is_none());
    assert!(!pending.exhaustion_requested);
    assert!(pending.exhaustion_ticket.is_none());
    assert!(event_rx.try_recv().is_err());
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn polymarket_outer_recovery_day_fence_drains_completed_poll_before_reply() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let boundary_utc_ms = scheduled_at_utc_ms
        .div_euclid(UTC_DAY_MS)
        .saturating_add(1)
        .saturating_mul(UTC_DAY_MS);
    let terminal_received_at_utc_ms = boundary_utc_ms - 1;
    let post_received_at_utc_ms = boundary_utc_ms + 1;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, terminal_received_at_utc_ms).await;
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: terminal_received_at_utc_ms,
        validation_completed_at_utc_ms: terminal_received_at_utc_ms,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: terminal_received_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: terminal_received_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_from_gate_for_test(
            &raw_day_gate,
            terminal_received_at_utc_ms,
        )
        .await,
    };
    let completion =
        GammaResolutionPollCompletion {
            slug: identity.slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms: post_received_at_utc_ms,
            result: Ok(serde_json::from_value::<SdkGammaMarket>(
                gamma_market_value_for_discovered(&discovery.current),
            )
            .unwrap()),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, post_received_at_utc_ms)
                    .await,
            ),
        };
    let mut resolution_poll_tasks = JoinSet::new();
    let handle = resolution_poll_tasks.spawn(async move { completion });
    tokio::task::yield_now().await;

    let mut context = PolymarketProcessingContext::new(discovery.clone());
    context.current = None;
    context.next = None;
    let mut pending = PendingPolymarketResolution::new(identity.clone(), Some(evidence));
    pending.in_flight_ordinal = Some(1);
    pending.poll_abort_handle = Some(handle);
    context.pending.insert(identity.slug.clone(), pending);
    context.rebuild_asset_to_slug();

    let (event_tx, mut event_rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let (resume_tx, resume_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: false,
            day_boundary_utc_ms: Some(boundary_utc_ms),
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .unwrap();
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    recovery.set_wait_override(Some(Duration::from_secs(60)));
    let gamma = GammaClient::new("http://127.0.0.1:9").unwrap();
    let waiter = tokio::spawn(async move {
        let result = wait_for_polymarket_outer_recovery(
            &mut recovery,
            &sink,
            &gamma,
            None,
            &mut control_rx,
            &mut resolution_poll_tasks,
            &mut context,
            &collector_identity,
            Instant::now(),
            Arc::new(move || PolymarketTimelineNow {
                utc_ms: post_received_at_utc_ms + 1,
                monotonic: TokioInstant::now(),
            }),
        )
        .await
        .unwrap();
        (result, context.pending.is_empty())
    });

    timeout(Duration::from_secs(2), reply_rx)
        .await
        .expect("outer recovery fence reply must be bounded")
        .unwrap()
        .unwrap();
    let terminal_event = event_rx.recv().await.unwrap();
    assert_eq!(
        terminal_event.event.recv_time_utc_ms,
        terminal_received_at_utc_ms
    );
    assert_eq!(
        deferred_ingress_facts(&stats, "polymarket"),
        vec![DeferredIngressBatchFact {
            source_kinds: vec!["polymarket_market_metadata".to_owned()],
            recv_times_utc_ms: vec![post_received_at_utc_ms],
            has_collector_admission: vec![false],
            accepted_defer: true,
            typed_polymarket_v3: false,
        }]
    );
    assert!(!waiter.is_finished());
    resume_tx.send(()).unwrap();
    assert_eq!(waiter.await.unwrap(), (true, true));
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_outer_recovery_day_fence_advances_after_pre_e_select_sample() {
    let boundary_utc_ms: i64 = 1_783_468_800_000;
    let mut current = test_btc_discovery().current;
    current.start_ts_s = boundary_utc_ms.div_euclid(1_000).saturating_sub(300);
    current.end_ts_s = boundary_utc_ms.div_euclid(1_000);
    current.candidate_start_ts_s = current.start_ts_s;
    current.candidate_end_ts_s = current.end_ts_s;
    current.slug = format!("btc-updown-5m-{}", current.start_ts_s);
    let received_at_utc_ms = current.start_ts_s.saturating_mul(1_000).saturating_sub(1);
    refresh_test_discovery_evidence(&mut current, received_at_utc_ms);
    let next = next_market_for_test(&current);
    let discovery = BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(next.clone()),
        relation: None,
    };
    let ws_received_at_utc_ms = boundary_utc_ms.saturating_sub(1);
    let actor_now_utc_ms = boundary_utc_ms.saturating_add(1);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    for (market, role) in [
        (&current, MarketRuntimeRole::Current),
        (&next, MarketRuntimeRole::Next),
    ] {
        context
            .register_source_authority(PolymarketMetadataV3ReferenceRecord::from(
                &polymarket_metadata_snapshot_v3_row(market, role, &collector_identity).unwrap(),
            ))
            .unwrap();
    }
    context.actor_now_utc_ms = actor_now_utc_ms;
    context.actor_utc_now = Some(Arc::new(move || actor_now_utc_ms));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let raw_day_gate = install_active_raw_day_gate(&stats, ws_received_at_utc_ms).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::new();
    let mut ws_message = budgeted_polymarket_message_at_for_test(
        market_resolved_message_for_test(&current, ws_received_at_utc_ms),
        ws_received_at_utc_ms,
        &global_budget,
        &decoded_budget,
    )
    .await;
    ws_message.raw_day_ticket =
        accepted_raw_day_ticket_from_gate_for_test(&raw_day_gate, ws_received_at_utc_ms).await;
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    recovery.set_wait_override(Some(Duration::from_secs(60)));
    let run_started = Instant::now();
    process_polymarket_budgeted_message(
        ws_message,
        &mut context,
        &sink,
        &collector_identity,
        run_started,
        &mut recovery,
    )
    .await
    .unwrap();
    assert!(context.early_ws_claims.contains_key(&current.slug));
    raw_day_gate
        .prepare_rollover(
            &utc_day_string_from_ms(ws_received_at_utc_ms).unwrap(),
            boundary_utc_ms,
        )
        .unwrap();

    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let (resume_tx, resume_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: false,
            day_boundary_utc_ms: Some(boundary_utc_ms),
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .unwrap();
    let mut resolution_poll_tasks = JoinSet::new();
    let gamma = GammaClient::new("http://127.0.0.1:9").unwrap();
    let clock_calls = Arc::new(AtomicU64::new(0));
    let timeline_calls = Arc::clone(&clock_calls);
    let timeline_now = Arc::new(move || {
        let call = timeline_calls.fetch_add(1, Ordering::Relaxed);
        PolymarketTimelineNow {
            utc_ms: if call < 3 {
                boundary_utc_ms.saturating_sub(1)
            } else {
                boundary_utc_ms.saturating_add(1)
            },
            monotonic: TokioInstant::now(),
        }
    });

    let (result, ()) = timeout(Duration::from_secs(1), async {
        tokio::join!(
            wait_for_polymarket_outer_recovery(
                &mut recovery,
                &sink,
                &gamma,
                None,
                &mut control_rx,
                &mut resolution_poll_tasks,
                &mut context,
                &collector_identity,
                run_started,
                timeline_now,
            ),
            async {
                reply_rx
                    .await
                    .unwrap()
                    .expect("outer recovery must finalize the E-1 WS before reply");
                let admitted = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
                assert_eq!(admitted.len(), 2);
                assert!(polymarket_v3_message(&admitted[0]).is_some());
                let terminal = polymarket_metadata_v5_prefixes_from_event(&admitted[1].event)
                    .unwrap()
                    .remove(0);
                assert_eq!(
                    terminal.metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionTerminalWs
                );
                let deferred = deferred_ingress_facts(&stats, "polymarket");
                assert_eq!(deferred.len(), 2);
                resume_tx.send(()).unwrap();
            }
        )
    })
    .await
    .expect("outer recovery day-fence race fixture must be bounded");
    assert!(result.unwrap());
    assert!(clock_calls.load(Ordering::Relaxed) >= 4);
    assert_eq!(
        context.current.as_ref().map(|market| market.slug.as_str()),
        Some(next.slug.as_str())
    );
    assert!(context.next.is_none());
    assert!(context.pending.is_empty());
    assert!(context.early_ws_claims.is_empty());
}

#[tokio::test]
async fn polymarket_outer_recovery_drives_due_slot_without_rebuilding_context() {
    let discovery = test_btc_discovery();
    let market = discovery.current.clone();
    let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
    let slug = identity.slug.clone();
    let slot_utc_ms = identity
        .market_end_utc_ms
        .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    let mut context = PolymarketProcessingContext::new(discovery);
    context.current = None;
    context.next = None;
    context.pending.insert(
        slug.clone(),
        PendingPolymarketResolution::new(identity, None),
    );
    context.rebuild_asset_to_slug();
    let mut resolution_poll_tasks = JoinSet::new();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let response = gamma_market_value_for_discovered(&market).to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_loopback_http_request(&mut socket).await.unwrap();
        write_loopback_http_response(&mut socket, "200 OK", &response).await;
        request
    });

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, slot_utc_ms + 1).await;
    let (event_tx, mut event_rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (_control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    recovery.set_wait_override(Some(Duration::from_millis(20)));
    let recovered = wait_for_polymarket_outer_recovery(
        &mut recovery,
        &sink,
        &GammaClient::new(&gamma_endpoint).unwrap(),
        None,
        &mut control_rx,
        &mut resolution_poll_tasks,
        &mut context,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
        Arc::new(move || PolymarketTimelineNow {
            utc_ms: slot_utc_ms + 1,
            monotonic: TokioInstant::now(),
        }),
    )
    .await
    .unwrap();

    assert!(recovered);
    assert!(resolution_poll_tasks.is_empty());
    let pending = &context.pending[&slug];
    assert_eq!(pending.attempts_recorded, 1);
    assert_eq!(pending.next_attempt_ordinal, 2);
    assert_eq!(pending.valid_response_count, 1);
    assert!(pending.in_flight_ordinal.is_none());
    assert!(pending.poll_abort_handle.is_none());
    let row = polymarket_metadata_v5_prefixes_from_event(&event_rx.recv().await.unwrap().event)
        .unwrap()
        .remove(0);
    assert_eq!(row.resolution_poll_attempt_ordinal, Some(1));
    assert_eq!(
        row.resolution_poll_result,
        Some(ResolutionPollResult::ValidNonterminal)
    );
    let request = server.await.unwrap();
    assert!(request.contains(&format!("/markets/slug/{slug}")));
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_outer_recovery_advances_current_across_e_before_retry() {
    let mut discovery = test_btc_discovery();
    discovery.next = None;
    discovery.relation = None;
    let market = discovery.current.clone();
    let market_end_utc_ms = market.end_ts_s.saturating_mul(1_000);
    let slot_utc_ms = market_end_utc_ms.saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let authority = PolymarketMetadataV3ReferenceRecord::from(
        &polymarket_metadata_snapshot_v3_row(
            &market,
            MarketRuntimeRole::Current,
            &collector_identity,
        )
        .unwrap(),
    );
    context.register_source_authority(authority).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let response = gamma_market_value_for_discovered(&market).to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_loopback_http_request(&mut socket).await.unwrap();
        write_loopback_http_response(&mut socket, "200 OK", &response).await;
        request
    });

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, slot_utc_ms + 1).await;
    let (event_tx, mut event_rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let (_control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    recovery.set_wait_override(Some(Duration::from_millis(100)));
    let mut resolution_poll_tasks = JoinSet::new();
    let clock_utc_ms = Arc::new(Mutex::new(market_end_utc_ms - 1));
    let updater_clock = Arc::clone(&clock_utc_ms);
    let updater = tokio::spawn(async move {
        sleep(Duration::from_millis(2)).await;
        *updater_clock.lock().unwrap() = slot_utc_ms + 1;
    });
    let timeline_clock = Arc::clone(&clock_utc_ms);
    let recovered = wait_for_polymarket_outer_recovery(
        &mut recovery,
        &sink,
        &GammaClient::new(&gamma_endpoint).unwrap(),
        None,
        &mut control_rx,
        &mut resolution_poll_tasks,
        &mut context,
        &collector_identity,
        Instant::now(),
        Arc::new(move || PolymarketTimelineNow {
            utc_ms: *timeline_clock.lock().unwrap(),
            monotonic: TokioInstant::now(),
        }),
    )
    .await
    .unwrap();
    updater.await.unwrap();

    assert!(recovered);
    assert!(context.current.is_none());
    let pending = context.pending.get(&market.slug).unwrap();
    assert_eq!(pending.attempts_recorded, 1);
    assert_eq!(pending.next_attempt_ordinal, 2);
    let rows = std::iter::from_fn(|| event_rx.try_recv().ok())
        .map(|event| {
            polymarket_metadata_v5_prefixes_from_event(&event.event)
                .unwrap()
                .remove(0)
        })
        .collect::<Vec<_>>();
    assert!(rows.iter().any(|row| {
        row.market_runtime_role_transition
            == Some(MarketRuntimeRoleTransition::CurrentToResolutionPending)
    }));
    assert!(rows.iter().any(|row| {
        row.resolution_poll_attempt_ordinal == Some(1)
            && row.resolution_poll_result == Some(ResolutionPollResult::ValidNonterminal)
    }));
    assert!(
        server
            .await
            .unwrap()
            .contains(&format!("/markets/slug/{}", market.slug))
    );
}

#[tokio::test]
async fn polymarket_outer_recovery_finalizes_terminal_and_exhausted_without_clob() {
    for terminal in [true, false] {
        let market = test_btc_discovery().current;
        let identity = PendingPolymarketIdentity::from_discovered(&market).unwrap();
        let ordinal = if terminal {
            1
        } else {
            POLYMARKET_RESOLUTION_ATTEMPT_COUNT
        };
        let slot_utc_ms = identity
            .market_end_utc_ms
            .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS)
            .saturating_add(
                i64::from(ordinal.saturating_sub(1))
                    .saturating_mul(POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS),
            );
        let mut pending = PendingPolymarketResolution::new(identity.clone(), None);
        pending.next_attempt_ordinal = ordinal;
        if !terminal {
            pending.attempts_recorded = u32::from(ordinal - 1);
            pending.valid_response_count = u32::from(ordinal - 1);
            pending.last_valid_metadata_received_at_utc_ms = Some(slot_utc_ms - 1);
        }
        let mut context = PolymarketProcessingContext::new(test_btc_discovery());
        context.current = None;
        context.next = None;
        context.pending.insert(identity.slug.clone(), pending);
        context.rebuild_asset_to_slug();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gamma_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let response = if terminal {
            serde_json::to_value(gamma_terminal_market_for_discovered(&market)).unwrap()
        } else {
            gamma_market_value_for_discovered(&market)
        }
        .to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_loopback_http_request(&mut socket).await.unwrap();
            write_loopback_http_response(&mut socket, "200 OK", &response).await;
        });

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, slot_utc_ms + 10).await;
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let sink = SourceEventSink {
            tx: event_tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let (_control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        recovery.set_wait_override(Some(Duration::from_millis(20)));
        let mut resolution_poll_tasks = JoinSet::new();
        let clock = Arc::new(Mutex::new(slot_utc_ms + 1));
        let timeline_clock = Arc::clone(&clock);
        wait_for_polymarket_outer_recovery(
            &mut recovery,
            &sink,
            &GammaClient::new(&gamma_endpoint).unwrap(),
            None,
            &mut control_rx,
            &mut resolution_poll_tasks,
            &mut context,
            &CollectorIdentity {
                host_id: "collector-test".to_owned(),
                region: "local-test".to_owned(),
            },
            Instant::now(),
            Arc::new(move || {
                let mut clock = timeline_clock.lock().unwrap();
                let utc_ms = *clock;
                *clock = (*clock).saturating_add(1);
                PolymarketTimelineNow {
                    utc_ms,
                    monotonic: TokioInstant::now(),
                }
            }),
        )
        .await
        .unwrap();
        server.await.unwrap();

        assert!(!context.pending.contains_key(&identity.slug));
        let rows = std::iter::from_fn(|| event_rx.try_recv().ok())
            .map(|event| {
                polymarket_metadata_v5_prefixes_from_event(&event.event)
                    .unwrap()
                    .remove(0)
            })
            .collect::<Vec<_>>();
        if terminal {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].resolution_followup_status,
                Some(ResolutionFollowupStatus::TerminalGammaFallback)
            );
        } else {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].resolution_poll_result,
                Some(ResolutionPollResult::ValidNonterminal)
            );
            assert_eq!(
                rows[1].metadata_row_kind,
                PolymarketMetadataRowKind::ResolutionFollowupSummary
            );
        }
    }
}

#[tokio::test]
async fn polymarket_terminal_fence_finalizes_three_reverse_races_once() {
    for mode in [
        "ws_first_post",
        "gamma_first_ws",
        "exhausted_valid_ws",
        "exhausted_timeout_ws",
        "exhausted_valid_no_ws",
        "exhausted_zero_valid_no_ws",
    ] {
        let received_at_utc_ms: i64 = 1_783_392_404_599;
        let current_start_ts_s = received_at_utc_ms.div_euclid(300_000) * 300;
        let mut current = test_btc_discovery().current;
        current.id = "market-current".to_owned();
        current.slug = format!("btc-updown-5m-{current_start_ts_s}");
        current.condition_id =
            "0x0000000000000000000000000000000000000000000000000000000000000002".to_owned();
        current.up_asset_id = "201".to_owned();
        current.down_asset_id = "202".to_owned();
        current.start_ts_s = current_start_ts_s;
        current.end_ts_s = current_start_ts_s + 300;
        current.candidate_start_ts_s = current.start_ts_s;
        current.candidate_end_ts_s = current.end_ts_s;
        refresh_test_discovery_evidence(&mut current, received_at_utc_ms - 20);

        let mut old = current.clone();
        old.id = "market-old".to_owned();
        old.slug = format!("btc-updown-5m-{}", current_start_ts_s - 300);
        old.condition_id =
            "0x0000000000000000000000000000000000000000000000000000000000000001".to_owned();
        old.up_asset_id = "101".to_owned();
        old.down_asset_id = "102".to_owned();
        old.start_ts_s -= 300;
        old.end_ts_s -= 300;
        old.candidate_start_ts_s -= 300;
        old.candidate_end_ts_s -= 300;
        let old_identity_received_at_utc_ms =
            old.end_ts_s.saturating_mul(1_000).saturating_sub(30_001);
        refresh_test_discovery_evidence(&mut old, old_identity_received_at_utc_ms);

        let discovery = BtcUpdownDiscovery {
            current,
            next: None,
            relation: None,
        };
        let fixed = PendingPolymarketIdentity::from_discovered(&old).unwrap();
        let slug = fixed.slug.clone();
        let collector_identity = CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        };
        let scheduled_at_utc_ms = fixed.market_end_utc_ms + 3 * 60_000;
        let completed_at_utc_ms = scheduled_at_utc_ms + 1;
        let transition_at_utc_ms = if mode.starts_with("exhausted_") {
            fixed.market_end_utc_ms + 8 * 60_000 + 1
        } else {
            completed_at_utc_ms + 2
        };
        let nonterminal_market =
            serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(&old))
                .unwrap();
        let terminal_market = gamma_terminal_market_for_discovered(&old);
        let completion_ticket = accepted_raw_day_ticket_for_test(completed_at_utc_ms).await;
        let mut pending = PendingPolymarketResolution::new(fixed.clone(), None);
        pending.in_flight_ordinal = Some(1);
        let completion = |market| GammaResolutionPollCompletion {
            slug: slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms,
            result: Ok(market),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(completion_ticket.clone()),
        };
        match mode {
            "ws_first_post" => {}
            "gamma_first_ws" => {
                let terminal = polymarket_resolution_attempt_v3_row(
                    &pending,
                    &completion(terminal_market),
                    &|| completed_at_utc_ms + 1,
                    false,
                    &collector_identity,
                )
                .unwrap();
                assert_eq!(
                    terminal.resolution_poll_result,
                    Some(ResolutionPollResult::Terminal)
                );
                pending.claim = Some(PolymarketTerminalClaim::GammaBySlug(Box::new(
                    AcceptedPolymarketMetadataRow {
                        row: terminal,
                        raw_day_ticket: completion_ticket.clone(),
                        ws_claimant_source_sequence_epoch_id: None,
                        ws_claimant_collector_admission_sequence: None,
                    },
                )));
            }
            "exhausted_valid_ws" => {
                pending.attempts_recorded = 6;
                pending.valid_response_count = 6;
                pending.last_valid_metadata_received_at_utc_ms = Some(completed_at_utc_ms);
                pending.exhaustion_requested = true;
            }
            "exhausted_timeout_ws" => {
                pending.attempts_recorded = 6;
                pending.transport_unavailable_count = 6;
                pending.exhaustion_requested = true;
            }
            "exhausted_valid_no_ws" => {
                pending.attempts_recorded = 6;
                pending.valid_response_count = 3;
                pending.transport_unavailable_count = 3;
                pending.last_valid_metadata_received_at_utc_ms = Some(completed_at_utc_ms);
                pending.exhaustion_requested = true;
            }
            "exhausted_zero_valid_no_ws" => {
                pending.attempts_recorded = 6;
                pending.transport_unavailable_count = 6;
                pending.exhaustion_requested = true;
            }
            _ => unreachable!(),
        }
        if mode.starts_with("exhausted_") {
            pending.exhaustion_ticket = Some(completion_ticket.clone());
        }
        if mode != "ws_first_post" {
            pending.in_flight_ordinal = None;
        }

        let mut context = PolymarketProcessingContext::new(discovery.clone());
        context.pending.insert(slug.clone(), pending);
        context.rebuild_asset_to_slug();
        context.actor_now_utc_ms = received_at_utc_ms;
        context.actor_utc_now = Some(Arc::new(move || received_at_utc_ms + 1));

        let resolved_json = json!({
            "id": fixed.clob_market_id,
            "market": fixed.condition_id,
            "slug": fixed.slug,
            "assets_ids": [fixed.up_asset_id, fixed.down_asset_id],
            "outcomes": ["Up", "Down"],
            "winning_asset_id": "101",
            "winning_outcome": "Up",
            "timestamp": received_at_utc_ms.to_string(),
            "event_type": "market_resolved"
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (initial_subscription_tx, initial_subscription_rx) = oneshot::channel();
        let (send_ws_tx, send_ws_rx) = oneshot::channel::<Option<String>>();
        let (requests_tx, requests_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let mut requests = Vec::new();
            while requests.is_empty() {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                } else if let Ok(request) = serde_json::from_str::<Value>(&text)
                    && request.get("operation").is_some()
                {
                    requests.push(request);
                }
            }
            initial_subscription_tx.send(()).unwrap();
            if let Some(message) = send_ws_rx.await.unwrap() {
                socket.send(Message::Text(message.into())).await.unwrap();
            }
            while requests.len() < 2 {
                let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                    continue;
                };
                if text == "PING" {
                    socket.send(Message::Text("PONG".into())).await.unwrap();
                } else if let Ok(request) = serde_json::from_str::<Value>(&text)
                    && request.get("operation").is_some()
                {
                    requests.push(request);
                }
            }
            requests_tx.send(requests).unwrap();
            sleep(Duration::from_secs(10)).await;
        });

        let global_budget = InflightBudget::new();
        let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
            format!("ws://{address}"),
            bounded_ws_config(),
            global_budget.clone(),
            active_raw_day_gate_for_test(received_at_utc_ms).await,
            Arc::new(move || received_at_utc_ms),
        )
        .unwrap();
        runtime
            .sdk
            .replace_and_wait(context.desired_assets().unwrap(), Duration::from_secs(2))
            .await
            .unwrap();
        initial_subscription_rx.await.unwrap();

        let stats = Arc::new(SourceQueueBackpressureStats::default());
        install_active_raw_day_gate(&stats, received_at_utc_ms).await;
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let (tx, mut rx) = mpsc::channel(16);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: global_budget,
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
        let run_started = Instant::now();

        if mode == "ws_first_post" {
            let WsMessage::MarketResolved(event) =
                serde_json::from_value::<WsMessage>(resolved_json).unwrap()
            else {
                unreachable!()
            };
            process_polymarket_market_resolved(
                &event,
                received_at_utc_ms,
                &mut context,
                &sink,
                &collector_identity,
                run_started,
                accepted_raw_day_ticket_for_test(received_at_utc_ms).await,
            )
            .await
            .unwrap();
            record_gamma_resolution_completion(
                completion(nonterminal_market),
                &|| completed_at_utc_ms + 1,
                &mut context,
                &sink,
                &collector_identity,
                run_started,
            )
            .await
            .unwrap();
            send_ws_tx.send(None).unwrap();
        } else if mode.ends_with("_no_ws") {
            send_ws_tx.send(None).unwrap();
        } else {
            send_ws_tx.send(Some(resolved_json.to_string())).unwrap();
            timeout(Duration::from_secs(2), async {
                while runtime.events.is_empty() {
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }

        let timeline_calls = AtomicU64::new(0);
        let mut resolution_poll_tasks = JoinSet::new();
        finalize_polymarket_pending_after_fence(
            &|| {
                let call = timeline_calls.fetch_add(1, Ordering::Relaxed);
                PolymarketTimelineNow {
                    utc_ms: transition_at_utc_ms + i64::from(call > 0),
                    monotonic: TokioInstant::now(),
                }
            },
            &mut runtime,
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &collector_identity,
            run_started,
            &mut recovery,
        )
        .await
        .unwrap();
        assert!(!context.pending.contains_key(&slug));

        let requests = requests_rx.await.unwrap();
        assert_eq!(requests[0]["operation"], "subscribe");
        assert_eq!(
            requests[0]["assets_ids"],
            json!(["101", "102", "201", "202"])
        );
        assert_eq!(requests[1]["operation"], "unsubscribe");
        assert_eq!(requests[1]["assets_ids"], json!(["101", "102"]));

        let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    polymarket_v3_message(event).is_some_and(|message| {
                        matches!(message.event, PolymarketOrderbookV3Event::MarketResolved(_))
                    })
                })
                .count(),
            usize::from(!mode.ends_with("_no_ws"))
        );
        let metadata = events
            .iter()
            .filter_map(polymarket_metadata_event)
            .map(|event| {
                polymarket_metadata_v5_prefixes_from_event(event)
                    .unwrap()
                    .remove(0)
            })
            .collect::<Vec<_>>();
        for row in &metadata {
            if let (Some(source_received_at_utc_ms), Some(validation_completed_at_utc_ms)) = (
                row.source_evidence_received_at_utc,
                row.row_evidence_validation_completed_at_utc,
            ) {
                assert!(source_received_at_utc_ms <= validation_completed_at_utc_ms);
                assert!(validation_completed_at_utc_ms <= row.collector_admission_time_utc_ms);
            }
        }
        match mode {
            "ws_first_post" => {
                assert_eq!(metadata.len(), 2);
                assert_eq!(
                    metadata[0].metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionTerminalWs
                );
                assert_eq!(
                    metadata[1].resolution_poll_result,
                    Some(ResolutionPollResult::PostTerminalClaimObservation)
                );
                assert_eq!(
                    metadata[0].collector_admission_sequence.saturating_add(1),
                    metadata[1].collector_admission_sequence
                );
                assert_eq!(
                    metadata[0].source_evidence_received_at_utc,
                    Some(received_at_utc_ms)
                );
                assert_eq!(
                    metadata[0].row_evidence_validation_completed_at_utc,
                    Some(received_at_utc_ms + 1)
                );
                assert_eq!(
                    metadata[1].source_evidence_received_at_utc,
                    Some(completed_at_utc_ms)
                );
                assert_eq!(
                    metadata[1].row_evidence_validation_completed_at_utc,
                    Some(completed_at_utc_ms + 1)
                );
            }
            "gamma_first_ws" => {
                assert_eq!(metadata.len(), 1);
                assert_eq!(
                    metadata[0].resolution_poll_result,
                    Some(ResolutionPollResult::Terminal)
                );
                assert_eq!(
                    metadata[0].terminal_claim_owner,
                    Some(TerminalClaimOwner::GammaBySlug)
                );
            }
            "exhausted_valid_ws" | "exhausted_timeout_ws" => {
                assert_eq!(metadata.len(), 1);
                assert_eq!(
                    metadata[0].metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionTerminalWs
                );
                assert!(metadata.iter().all(|row| {
                    row.metadata_row_kind != PolymarketMetadataRowKind::ResolutionFollowupSummary
                }));
            }
            "exhausted_valid_no_ws" => {
                assert_eq!(metadata.len(), 1);
                assert_eq!(
                    metadata[0].metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionFollowupSummary
                );
                assert_eq!(
                    metadata[0].resolution_followup_status,
                    Some(ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal)
                );
                assert_eq!(metadata[0].resolution_poll_attempts_recorded, Some(6));
                assert_eq!(metadata[0].resolution_valid_response_count, Some(3));
                assert_eq!(metadata[0].resolution_transport_unavailable_count, Some(3));
                assert_eq!(
                    metadata[0].runtime_role_applied_at_utc,
                    Some(transition_at_utc_ms)
                );
            }
            "exhausted_zero_valid_no_ws" => {
                assert_eq!(metadata.len(), 1);
                assert_eq!(
                    metadata[0].metadata_row_kind,
                    PolymarketMetadataRowKind::ResolutionFollowupSummary
                );
                assert_eq!(
                    metadata[0].resolution_followup_status,
                    Some(ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse)
                );
                assert_eq!(metadata[0].resolution_poll_attempts_recorded, Some(6));
                assert_eq!(metadata[0].resolution_valid_response_count, Some(0));
                assert_eq!(metadata[0].resolution_transport_unavailable_count, Some(6));
                assert!(metadata[0].last_valid_metadata_received_utc.is_none());
                assert_eq!(
                    metadata[0].runtime_role_applied_at_utc,
                    Some(transition_at_utc_ms)
                );
            }
            _ => unreachable!(),
        }
        assert!(gaps.lock().unwrap().is_empty());
        runtime.abort_pump_and_wait().await;
        server.abort();
    }
}

#[tokio::test]
async fn polymarket_two_ready_pending_sample_applied_time_per_transition() {
    let current = test_btc_discovery().current;
    let mut pending_one = current.clone();
    pending_one.id = "market-pending-one".to_owned();
    pending_one.slug = format!("btc-updown-5m-{}", pending_one.start_ts_s - 300);
    pending_one.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    pending_one.up_asset_id = "201".to_owned();
    pending_one.down_asset_id = "202".to_owned();
    pending_one.start_ts_s -= 300;
    pending_one.end_ts_s -= 300;
    pending_one.candidate_start_ts_s -= 300;
    pending_one.candidate_end_ts_s -= 300;
    let pending_one_received_at_utc_ms = pending_one.start_ts_s.saturating_mul(1_000) + 1;
    refresh_test_discovery_evidence(&mut pending_one, pending_one_received_at_utc_ms);
    let mut pending_two = pending_one.clone();
    pending_two.id = "market-pending-two".to_owned();
    pending_two.slug = format!("btc-updown-5m-{}", pending_two.start_ts_s - 300);
    pending_two.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000004".to_owned();
    pending_two.up_asset_id = "301".to_owned();
    pending_two.down_asset_id = "302".to_owned();
    pending_two.start_ts_s -= 300;
    pending_two.end_ts_s -= 300;
    pending_two.candidate_start_ts_s -= 300;
    pending_two.candidate_end_ts_s -= 300;
    let pending_two_received_at_utc_ms = pending_two.start_ts_s.saturating_mul(1_000) + 1;
    refresh_test_discovery_evidence(&mut pending_two, pending_two_received_at_utc_ms);

    let discovery = BtcUpdownDiscovery {
        current,
        next: None,
        relation: None,
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    for market in [&pending_one, &pending_two] {
        let identity = PendingPolymarketIdentity::from_discovered(market).unwrap();
        let mut pending = PendingPolymarketResolution::new(identity, None);
        pending.attempts_recorded = u32::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT);
        pending.next_attempt_ordinal = POLYMARKET_RESOLUTION_ATTEMPT_COUNT + 1;
        pending.transport_unavailable_count = u32::from(POLYMARKET_RESOLUTION_ATTEMPT_COUNT);
        pending.exhaustion_requested = true;
        context.pending.insert(market.slug.clone(), pending);
    }
    context.rebuild_asset_to_slug();
    let first_applied_at_utc_ms = context
        .pending
        .values()
        .map(PendingPolymarketResolution::budget_horizon_utc_ms)
        .max()
        .unwrap()
        + 1;
    let exhaustion_ticket = accepted_raw_day_ticket_for_test(first_applied_at_utc_ms).await;
    for pending in context.pending.values_mut() {
        pending.exhaustion_ticket = Some(exhaustion_ticket.clone());
    }
    let second_applied_at_utc_ms = first_applied_at_utc_ms + 1_000;
    let resumed_at_utc_ms = second_applied_at_utc_ms + 1;
    let post_horizon_ws = json!({
        "id": pending_one.id,
        "market": pending_one.condition_id,
        "slug": pending_one.slug,
        "assets_ids": [pending_one.up_asset_id, pending_one.down_asset_id],
        "outcomes": ["Up", "Down"],
        "winning_asset_id": "201",
        "winning_outcome": "Up",
        "timestamp": second_applied_at_utc_ms.to_string(),
        "event_type": "market_resolved"
    })
    .to_string();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests_tx, requests_rx) = oneshot::channel();
    let (post_horizon_tx, post_horizon_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut requests = Vec::new();
        while requests.is_empty() {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
            } else if let Ok(request) = serde_json::from_str::<Value>(&text)
                && request.get("operation").is_some()
            {
                requests.push(request);
            }
        }
        post_horizon_rx.await.unwrap();
        socket
            .send(Message::Text(post_horizon_ws.into()))
            .await
            .unwrap();
        while requests.len() < 2 {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
            } else if let Ok(request) = serde_json::from_str::<Value>(&text)
                && request.get("operation").is_some()
            {
                requests.push(request);
            }
        }
        requests_tx.send(requests).unwrap();
        sleep(Duration::from_secs(10)).await;
    });

    let timeline_utc_ms = Arc::new(Mutex::new(first_applied_at_utc_ms));
    let timeline_for_runtime = Arc::clone(&timeline_utc_ms);
    let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
        format!("ws://{address}"),
        bounded_ws_config(),
        InflightBudget::new(),
        active_raw_day_gate_for_test(first_applied_at_utc_ms).await,
        Arc::new(move || *timeline_for_runtime.lock().unwrap()),
    )
    .unwrap();
    runtime
        .sdk
        .replace_and_wait(context.desired_assets().unwrap(), Duration::from_secs(2))
        .await
        .unwrap();

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, first_applied_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    sink.tx
        .try_send(SourceIngressPayload::LegacyRaw(InflightRawEvent::for_test(
            test_raw_event("binance_aggtrade", first_applied_at_utc_ms),
        )))
        .unwrap();
    let timeline_for_finalize = Arc::clone(&timeline_utc_ms);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let finalize = tokio::spawn(async move {
        let mut resolution_poll_tasks = JoinSet::new();
        finalize_polymarket_pending_after_fence(
            &move || PolymarketTimelineNow {
                utc_ms: *timeline_for_finalize.lock().unwrap(),
                monotonic: TokioInstant::now(),
            },
            &mut runtime,
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &collector_identity,
            Instant::now(),
            &mut recovery,
        )
        .await
        .unwrap();
        let pending_is_empty = context.pending.is_empty();
        match runtime.recv(Duration::from_millis(100)).await {
            Ok(Some(PolymarketDrainEvent::Message(message))) => {
                process_polymarket_budgeted_message(
                    message,
                    &mut context,
                    &sink,
                    &collector_identity,
                    Instant::now(),
                    &mut recovery,
                )
                .await
                .unwrap();
            }
            Ok(None) => {}
            Ok(Some(PolymarketDrainEvent::Fence { .. })) => {
                panic!("unexpected fence after exhausted role-exit")
            }
            Err(error) => panic!("unexpected post-horizon drain terminal:{error:?}"),
        }
        runtime.abort_pump_and_wait().await;
        pending_is_empty
    });

    timeout(Duration::from_secs(2), async {
        while stats.wait_count.load(Ordering::Relaxed) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("first pending summary must reach bounded queue backpressure");
    *timeline_utc_ms.lock().unwrap() = second_applied_at_utc_ms;
    assert_eq!(
        rx.recv().await.unwrap().event.source_kind,
        "binance_aggtrade"
    );
    let first = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
        .into_legacy_event();
    let second = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
        .into_legacy_event();
    assert!(!finalize.is_finished());
    post_horizon_tx.send(()).unwrap();
    sleep(Duration::from_millis(20)).await;
    assert!(
        rx.try_recv().is_err(),
        "post-marker WS must remain behind the exhausted clock barrier"
    );
    assert!(!finalize.is_finished());
    *timeline_utc_ms.lock().unwrap() = resumed_at_utc_ms;
    assert!(finalize.await.unwrap());
    assert_eq!(stats.wait_count.load(Ordering::Relaxed), 2);

    let rows = [first, second].map(|event| {
        polymarket_metadata_v5_prefixes_from_event(&event)
            .unwrap()
            .remove(0)
    });
    assert_eq!(
        rows[0].runtime_role_applied_at_utc,
        Some(first_applied_at_utc_ms)
    );
    assert_eq!(rows[0].timestamp_received, first_applied_at_utc_ms);
    assert_eq!(
        rows[1].runtime_role_applied_at_utc,
        Some(second_applied_at_utc_ms)
    );
    assert_eq!(rows[1].timestamp_received, second_applied_at_utc_ms);
    assert!(
        rx.try_recv().is_err(),
        "an exhausted lifecycle must not admit a post-horizon source row"
    );

    let requests = requests_rx.await.unwrap();
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(requests[1]["operation"], "unsubscribe");
    assert_eq!(
        requests[1]["assets_ids"],
        json!(["201", "202", "301", "302"])
    );
    server.abort();
}

#[tokio::test]
async fn polymarket_decoded_attempt_binding_error_keeps_v3_evidence_then_fails_closed() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let slug = identity.slug.clone();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let sdk_market = serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
        &discovery.current,
    ))
    .unwrap();
    let mut context = PolymarketProcessingContext::new(discovery);
    let mut pending = PendingPolymarketResolution::new(identity, None);
    pending.in_flight_ordinal = Some(2);
    context.pending.insert(slug.clone(), pending);

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, scheduled_at_utc_ms + 1).await;
    let (tx, mut rx) = mpsc::channel(2);
    let error = record_gamma_resolution_completion(
        GammaResolutionPollCompletion {
            slug,
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms: scheduled_at_utc_ms + 1,
            result: Ok(sdk_market),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_for_test(scheduled_at_utc_ms + 1).await,
            ),
        },
        &|| scheduled_at_utc_ms + 2,
        &mut context,
        &SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        },
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("resolution integrity failure"));

    let event = rx.recv().await.unwrap().into_legacy_event();
    let rows = polymarket_metadata_v5_prefixes_from_event(&event).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.resolution_poll_result,
        Some(ResolutionPollResult::IntegrityError)
    );
    assert_eq!(
        row.resolution_integrity_error_class,
        Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant)
    );
    assert!(row.source_evidence_payload_json.is_some());
    assert!(row.source_evidence_raw_sha256.is_some());
    assert!(row.terminal_predicate_passed.is_none());
    assert!(row.terminal_predicate_failure_reason.is_none());
    let validation = row.validate();
    assert!(
        validation.is_complete(),
        "binding evidence row must satisfy v3 matrix:{:?}",
        validation.violations
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_rollover_rejects_prefetched_identity_mutation_before_fence_or_install() {
    let current = test_btc_discovery().current;
    let mut prefetched = current.clone();
    prefetched.id = "market-prefetched".to_owned();
    prefetched.slug = "btc-updown-5m-1783392600".to_owned();
    prefetched.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    prefetched.up_asset_id = "201".to_owned();
    prefetched.down_asset_id = "202".to_owned();
    prefetched.start_ts_s += 300;
    prefetched.end_ts_s += 300;
    prefetched.candidate_start_ts_s += 300;
    prefetched.candidate_end_ts_s += 300;
    let discovery = BtcUpdownDiscovery {
        current,
        next: Some(prefetched.clone()),
        relation: None,
    };
    let mut mutated = prefetched;
    mutated.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000004".to_owned();
    let replacement = BtcUpdownDiscovery {
        current: mutated,
        next: None,
        relation: None,
    };
    let mut skipped_window = replacement.clone();
    skipped_window.current.slug = "btc-updown-5m-1783392900".to_owned();
    assert!(validate_polymarket_context_switch(&discovery, &skipped_window).is_ok());
    let promoted_without_mutation = BtcUpdownDiscovery {
        current: discovery.next.clone().unwrap(),
        next: None,
        relation: None,
    };
    assert!(
        validate_polymarket_discovery_identity_continuity(&discovery, &promoted_without_mutation,)
            .is_ok()
    );
    let prefetched_fixed_identity_verified_at_utc_ms = promoted_without_mutation
        .current
        .fixed_identity_verified_at_utc_ms;
    let mut refreshed_promotion = promoted_without_mutation.clone();
    refresh_test_discovery_evidence(
        &mut refreshed_promotion.current,
        prefetched_fixed_identity_verified_at_utc_ms + 100,
    );
    let refreshed_validation_completed_at_utc_ms = refreshed_promotion
        .current
        .metadata_validation_completed_at_utc_ms;
    preserve_polymarket_fixed_identity_authority(&discovery, &mut refreshed_promotion).unwrap();
    assert_eq!(
        refreshed_promotion
            .current
            .fixed_identity_verified_at_utc_ms,
        prefetched_fixed_identity_verified_at_utc_ms
    );
    assert_eq!(
        refreshed_promotion
            .current
            .metadata_validation_completed_at_utc_ms,
        refreshed_validation_completed_at_utc_ms,
        "fresh payload validation remains row-local while fixed identity keeps its first authority"
    );
    let mut identity_mutations = Vec::new();
    for field in ["id", "condition", "up", "down"] {
        let mut mutation = discovery.clone();
        match field {
            "id" => mutation.current.id = "market-mutated".to_owned(),
            "condition" => {
                mutation.current.condition_id =
                    "0x0000000000000000000000000000000000000000000000000000000000000009".to_owned()
            }
            "up" => mutation.current.up_asset_id = "901".to_owned(),
            "down" => mutation.current.down_asset_id = "902".to_owned(),
            _ => unreachable!(),
        }
        identity_mutations.push(mutation);
    }
    assert!(identity_mutations.iter().all(|mutation| {
        validate_polymarket_discovery_identity_continuity(&discovery, mutation).is_err()
    }));
    let skipped_transport_window = BtcUpdownDiscovery {
        current: skipped_window.current.clone(),
        next: None,
        relation: None,
    };
    assert!(
        validate_polymarket_discovery_identity_continuity(&discovery, &skipped_transport_window,)
            .is_ok()
    );

    let global_budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        "ws://127.0.0.1:9".to_owned(),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime.abort_pump_and_wait().await;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: global_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let mut resolution_poll_tasks = JoinSet::new();

    let error = switch_polymarket_context(
        replacement,
        &production_polymarket_timeline_now,
        true,
        &mut runtime,
        &mut context,
        &mut resolution_poll_tasks,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .expect_err("a prefetched identity cannot mutate while being promoted");

    assert!(
        error
            .to_string()
            .contains("identity changed before promotion")
    );
    assert_eq!(context.discovery_snapshot().unwrap(), discovery);
    assert!(
        gaps.lock()
            .unwrap()
            .iter()
            .any(|reason| { reason.starts_with("polymarket_rollover_identity_integrity:") })
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn polymarket_context_switch_rechecks_applied_time_after_delayed_fence() {
    let boundary_ms = 1_800_000_000_i64 * 1_000;
    let window = discovery_windows(boundary_ms - 1).current;
    let mut discovery = test_btc_discovery();
    discovery.current.slug = window.slug;
    discovery.current.start_ts_s = window.target_ts_s;
    discovery.current.end_ts_s = window.end_ts_s;
    discovery.current.candidate_start_ts_s = window.target_ts_s;
    discovery.current.candidate_end_ts_s = window.end_ts_s;

    let global_budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        "ws://127.0.0.1:9".to_owned(),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(boundary_ms - 1).await,
    )
    .unwrap();
    runtime.abort_pump_and_wait().await;
    let (event_tx, event_rx) = mpsc::channel(1);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (fence_requested_tx, fence_requested_rx) = oneshot::channel();
    let (release_fence_tx, release_fence_rx) = oneshot::channel();
    runtime.events = event_rx;
    runtime.controls = control_tx;
    runtime.pump = Some(tokio::spawn(async move {
        let Some(PolymarketPumpControl::Fence {
            id,
            stop_after_fence,
            ..
        }) = control_rx.recv().await
        else {
            return;
        };
        fence_requested_tx.send(()).unwrap();
        release_fence_rx.await.unwrap();
        event_tx
            .send(PolymarketPumpItem::Fence {
                id,
                stop_after_fence,
            })
            .await
            .unwrap();
    }));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(4);
    let sink = SourceEventSink {
        tx,
        gaps,
        source_queue_backpressure: stats,
        inflight_budget: global_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let mut resolution_poll_tasks = JoinSet::new();
    let applied_utc_ms = Arc::new(Mutex::new(boundary_ms - 1));
    let timeline_calls = Arc::new(AtomicU64::new(0));
    let timeline = {
        let applied_utc_ms = Arc::clone(&applied_utc_ms);
        let timeline_calls = Arc::clone(&timeline_calls);
        move || {
            timeline_calls.fetch_add(1, Ordering::SeqCst);
            PolymarketTimelineNow {
                utc_ms: *applied_utc_ms.lock().unwrap(),
                monotonic: TokioInstant::now(),
            }
        }
    };

    let switch = switch_polymarket_context(
        discovery.clone(),
        &timeline,
        false,
        &mut runtime,
        &mut context,
        &mut resolution_poll_tasks,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    );
    let cross_boundary = async {
        fence_requested_rx.await.unwrap();
        assert_eq!(timeline_calls.load(Ordering::SeqCst), 0);
        *applied_utc_ms.lock().unwrap() = boundary_ms;
        release_fence_tx.send(()).unwrap();
    };
    let (installed, ()) = tokio::join!(switch, cross_boundary);

    assert_eq!(installed.unwrap(), PolymarketContextInstallOutcome::Expired);
    assert_eq!(timeline_calls.load(Ordering::SeqCst), 1);
    assert_eq!(context.discovery_snapshot().unwrap(), discovery);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn clob_current_and_prefetched_assets_survive_three_legacy_silence_cycles_on_one_socket() {
    const LEGACY_EQUIVALENT_CYCLE: Duration = Duration::from_millis(250);
    const LEGACY_CYCLES: u32 = 3;

    let until_boundary_ms = 300_000_i64 - now_ms().rem_euclid(300_000);
    if until_boundary_ms <= 2_000 {
        sleep(Duration::from_millis(
            u64::try_from(until_boundary_ms + 10).unwrap(),
        ))
        .await;
    }
    let windows = discovery_windows(now_ms());
    let mut current = test_btc_discovery().current;
    current.slug = windows.current.slug;
    current.start_ts_s = windows.current.target_ts_s;
    current.end_ts_s = windows.current.end_ts_s;
    current.candidate_start_ts_s = current.start_ts_s;
    current.candidate_end_ts_s = current.end_ts_s;
    let fixture_received_at_utc_ms = now_ms().saturating_sub(2);
    refresh_test_discovery_evidence(&mut current, fixture_received_at_utc_ms);
    let mut prefetched = current.clone();
    prefetched.id = "market-prefetched".to_owned();
    prefetched.slug = windows.next.slug;
    prefetched.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    prefetched.up_asset_id = "201".to_owned();
    prefetched.down_asset_id = "202".to_owned();
    prefetched.start_ts_s = windows.next.target_ts_s;
    prefetched.end_ts_s = windows.next.end_ts_s;
    prefetched.candidate_start_ts_s = prefetched.start_ts_s;
    prefetched.candidate_end_ts_s = prefetched.end_ts_s;
    refresh_test_discovery_evidence(&mut prefetched, fixture_received_at_utc_ms);
    let discovery = BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(prefetched.clone()),
        relation: None,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let current_condition = current.condition_id.clone();
    let prefetched_condition = prefetched.condition_id.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut accepts = 1_u32;
        let mut socket = accept_async(stream).await.unwrap();
        let mut socket_open = true;
        let mut requests = Vec::<Value>::new();
        let mut current_sent = false;
        let mut prefetched_sent = false;
        let started = Instant::now();
        let run_for = LEGACY_EQUIVALENT_CYCLE * LEGACY_CYCLES + Duration::from_millis(300);

        while started.elapsed() < run_for {
            if !current_sent && started.elapsed() >= Duration::from_millis(100) {
                current_sent = true;
                if socket
                    .send(Message::Text(
                        json!({
                            "event_type": "book",
                            "asset_id": "101",
                            "market": current_condition,
                            "timestamp": "100",
                            "bids": [],
                            "asks": []
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .is_err()
                {
                    socket_open = false;
                }
            }
            if !prefetched_sent && started.elapsed() >= LEGACY_EQUIVALENT_CYCLE * LEGACY_CYCLES {
                prefetched_sent = true;
                if socket
                    .send(Message::Text(
                        json!({
                            "event_type": "book",
                            "asset_id": "201",
                            "market": prefetched_condition,
                            "timestamp": "800",
                            "bids": [],
                            "asks": []
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .is_err()
                {
                    socket_open = false;
                }
            }

            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    accepts = accepts.saturating_add(1);
                    drop(stream);
                }
                frame = socket.next(), if socket_open => {
                    match frame {
                        Some(Ok(Message::Text(text))) if text == "PING"
                            && socket.send(Message::Text("PONG".into())).await.is_err() =>
                        {
                            socket_open = false;
                        }
                        Some(Ok(Message::Text(text))) if text == "PING" => {}
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(request) = serde_json::from_str::<Value>(&text)
                                && request.get("operation").is_some()
                            {
                                requests.push(request);
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                            socket_open = false;
                        }
                        _ => {}
                    }
                }
                () = sleep(Duration::from_millis(5)) => {}
            }
        }
        (accepts, requests)
    });

    let global_budget = InflightBudget::new();
    let mut config = bounded_ws_config();
    config.heartbeat_interval = Duration::from_millis(20);
    config.heartbeat_timeout = Duration::from_millis(100);
    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        config,
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, now_ms()).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(16);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let _ = recovery.next_wait();
    let _ = recovery.next_wait();
    let mut resolution_poll_tasks = JoinSet::new();
    let (_control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let run_started = Instant::now();

    timeout(
        Duration::from_secs(2),
        collect_polymarket_single_receiver(
            Some(discovery),
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &identity,
            Some(now_ms() + 1_000),
            run_started,
            &mut runtime,
            &GammaClient::new("http://127.0.0.1:9").unwrap(),
            &mut recovery,
            &mut control_rx,
            Duration::from_millis(10),
            Duration::from_millis(1),
            production_polymarket_timeline_now,
        ),
    )
    .await
    .expect("the silent CLOB source must stop only at its explicit deadline")
    .unwrap();
    drop(runtime);

    let (accepts, requests) = server.await.unwrap();
    assert_eq!(accepts, 1, "business silence must not change generation");
    assert_eq!(requests.len(), 1, "business silence must not resubscribe");
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(
        requests[0]["assets_ids"],
        json!(["101", "102", "201", "202"])
    );

    let mut market_rows = BTreeMap::new();
    while let Ok(payload) = rx.try_recv() {
        if let SourceIngressPayload::PolymarketOrderbookV3(inflight) = payload {
            let PolymarketOrderbookV3Event::Book(book) = &inflight.message.event else {
                panic!("silence fixture must emit one typed Book row per SDK message");
            };
            market_rows.insert(
                format!(
                    "{}:{}",
                    inflight
                        .message
                        .envelope
                        .slug
                        .as_deref()
                        .expect("target-admitted Book must carry its market slug"),
                    book.asset_id
                ),
                inflight.message.envelope.timestamp_received,
            );
        }
    }
    assert_eq!(
        market_rows.keys().cloned().collect::<Vec<_>>(),
        [
            format!("{}:101", current.slug),
            format!("{}:201", prefetched.slug),
        ]
    );
    assert!(
        market_rows[&format!("{}:101", current.slug)]
            < market_rows[&format!("{}:201", prefetched.slug)],
        "current and prefetched last-qualifying receive times remain independently traceable"
    );
    assert!(gaps.lock().unwrap().is_empty());
    let counters = stats.drain_all_operational();
    assert!(counters.values().all(|by_source| {
        by_source
            .get("polymarket_market")
            .is_none_or(|counter| counter.lag_events == 0 && counter.reconnect_events == 0)
    }));
    assert_eq!(recovery.next_wait().attempt, 1);
    assert_eq!(global_budget.available(), InflightBudget::new().available());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn polymarket_resolution_timeline_is_absolute_green() {
    assert_eq!(
        typed_schema_version("polymarket_market_metadata"),
        POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION
    );
    let real_windows = discovery_windows(1_783_392_404_599);
    let boundary_ms = real_windows.current.target_ts_s * 1_000;

    let mut market_b = test_btc_discovery().current;
    market_b.id = "market-b".to_owned();
    market_b.slug = real_windows.current.slug.clone();
    market_b.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000002".to_owned();
    market_b.up_asset_id = "201".to_owned();
    market_b.down_asset_id = "202".to_owned();
    market_b.start_ts_s = real_windows.current.target_ts_s;
    market_b.end_ts_s = real_windows.current.end_ts_s;
    market_b.candidate_start_ts_s = market_b.start_ts_s;
    market_b.candidate_end_ts_s = market_b.end_ts_s;

    let mut market_a = market_b.clone();
    market_a.id = "market-a".to_owned();
    market_a.slug = format!("btc-updown-5m-{}", market_b.start_ts_s - 300);
    market_a.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000001".to_owned();
    market_a.up_asset_id = "101".to_owned();
    market_a.down_asset_id = "102".to_owned();
    market_a.start_ts_s -= 300;
    market_a.end_ts_s -= 300;
    market_a.candidate_start_ts_s -= 300;
    market_a.candidate_end_ts_s -= 300;

    let mut market_c = market_b.clone();
    market_c.id = "market-c".to_owned();
    market_c.slug = real_windows.next.slug.clone();
    market_c.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    market_c.up_asset_id = "301".to_owned();
    market_c.down_asset_id = "302".to_owned();
    market_c.start_ts_s = real_windows.next.target_ts_s;
    market_c.end_ts_s = real_windows.next.end_ts_s;
    market_c.candidate_start_ts_s = market_c.start_ts_s;
    market_c.candidate_end_ts_s = market_c.end_ts_s;

    let fake_start_ms = boundary_ms - 60_000;
    refresh_test_discovery_evidence(&mut market_a, fake_start_ms - 1_000);
    refresh_test_discovery_evidence(&mut market_b, fake_start_ms - 900);
    refresh_test_discovery_evidence(&mut market_c, fake_start_ms - 800);
    let initial = BtcUpdownDiscovery {
        current: market_a.clone(),
        next: Some(market_b.clone()),
        relation: None,
    };
    let timeline_anchor = TokioInstant::now();
    let gamma_trace = Arc::new(Mutex::new(Vec::<(i64, String)>::new()));
    let clob_trace = Arc::new(Mutex::new(Vec::<(i64, Value)>::new()));
    let clob_timeline_utc_ms = Arc::new(Mutex::new(fake_start_ms));

    let gamma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gamma_endpoint = format!("http://{}", gamma_listener.local_addr().unwrap());
    let gamma_trace_server = Arc::clone(&gamma_trace);
    let exact_a_path = format!("/markets/slug/{}", market_a.slug);
    let market_a_body = gamma_market_value_for_discovered(&market_a).to_string();
    let exact_b_path = format!("/markets/slug/{}", market_b.slug);
    let market_b_value = gamma_market_value_for_discovered(&market_b);
    let market_b_exact_body = market_b_value.to_string();
    let market_b_body = json!([market_b_value]).to_string();
    let market_c_body = json!([gamma_market_value_for_discovered(&market_c)]).to_string();
    let market_b_slug = market_b.slug.clone();
    let market_c_slug = market_c.slug.clone();
    let gamma_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = gamma_listener.accept().await {
            let Some(request) = read_loopback_http_request(&mut socket).await else {
                continue;
            };
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            gamma_trace_server.lock().unwrap().push((
                paused_timeline_utc_ms(timeline_anchor, fake_start_ms),
                request_line.clone(),
            ));
            let (status, body) = if request_line.contains(&exact_a_path) {
                ("200 OK", market_a_body.as_str())
            } else if request_line.contains(&exact_b_path) {
                ("200 OK", market_b_exact_body.as_str())
            } else if request_line.contains(&market_b_slug) {
                ("200 OK", market_b_body.as_str())
            } else if request_line.contains(&market_c_slug) {
                ("200 OK", market_c_body.as_str())
            } else {
                ("503 Service Unavailable", "[]")
            };
            write_loopback_http_response(&mut socket, status, body).await;
        }
    });

    let clob_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    clob_listener.set_nonblocking(true).unwrap();
    let clob_endpoint = format!("ws://{}", clob_listener.local_addr().unwrap());
    let clob_trace_server = Arc::clone(&clob_trace);
    let clob_timeline_utc_ms_server = Arc::clone(&clob_timeline_utc_ms);
    let (initial_subscription_tx, mut initial_subscription_rx) = oneshot::channel();
    let (rollover_subscription_tx, mut rollover_subscription_rx) = oneshot::channel();
    let (old_assets_unsubscribed_tx, mut old_assets_unsubscribed_rx) = oneshot::channel();
    let (clob_shutdown_tx, clob_shutdown_rx) = oneshot::channel();
    // Keep loopback wire I/O on a real-time runtime so paused test time controls only
    // collector scheduling, not whether the server gets a chance to consume SDK writes.
    let clob_server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let clob_listener = TcpListener::from_std(clob_listener).unwrap();
            let (stream, _) = clob_listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let mut initial_subscription_tx = Some(initial_subscription_tx);
            let mut rollover_subscription_tx = Some(rollover_subscription_tx);
            let mut old_assets_unsubscribed_tx = Some(old_assets_unsubscribed_tx);
            let mut unsubscribed_old_assets = BTreeSet::new();
            let mut clob_shutdown_rx = clob_shutdown_rx;
            loop {
                let frame = tokio::select! {
                    biased;
                    frame = socket.next() => frame,
                    _ = &mut clob_shutdown_rx => return,
                };
                let Some(frame) = frame else {
                    return;
                };
                match frame {
                    Ok(Message::Text(text))
                        if text == "PING"
                            && socket.send(Message::Text("PONG".into())).await.is_err() =>
                    {
                        return;
                    }
                    Ok(Message::Text(text)) => {
                        let Ok(request) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        if request.get("operation").is_none() {
                            continue;
                        }
                        clob_trace_server.lock().unwrap().push((
                            *clob_timeline_utc_ms_server.lock().unwrap(),
                            request.clone(),
                        ));
                        if request["operation"] == "subscribe" {
                            if let Some(ready) = initial_subscription_tx.take() {
                                let _ = ready.send(());
                            }
                            if request["assets_ids"].as_array().is_some_and(|asset_ids| {
                                ["301", "302"].iter().all(|expected| {
                                    asset_ids.iter().any(|asset_id| asset_id == *expected)
                                })
                            }) && let Some(ready) = rollover_subscription_tx.take()
                            {
                                let _ = ready.send(());
                            }
                        }
                        if request["operation"] == "unsubscribe"
                            && let Some(asset_ids) = request["assets_ids"].as_array()
                        {
                            unsubscribed_old_assets.extend(
                                asset_ids
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .filter(|asset_id| matches!(*asset_id, "101" | "102"))
                                    .map(str::to_owned),
                            );
                            if unsubscribed_old_assets.len() == 2
                                && let Some(ready) = old_assets_unsubscribed_tx.take()
                            {
                                let _ = ready.send(());
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => return,
                    _ => {}
                }
            }
        });
    });

    let keep_time_paused = Arc::new(AtomicBool::new(true));
    let keep_time_paused_task = Arc::clone(&keep_time_paused);
    let time_guard = tokio::spawn(async move {
        while keep_time_paused_task.load(Ordering::Relaxed) {
            // A zero-duration timer keeps automatic time advance disabled while still
            // letting Tokio service the SDK socket; yield_now alone can starve the driver.
            tokio::time::sleep(Duration::ZERO).await;
        }
    });

    let global_budget = InflightBudget::new();
    let mut ws_config = bounded_ws_config();
    ws_config.heartbeat_interval = Duration::from_secs(3_600);
    ws_config.heartbeat_timeout = Duration::from_secs(7_200);
    let runtime = PolymarketDrainRuntime::new(
        clob_endpoint,
        ws_config,
        global_budget.clone(),
        active_raw_day_gate_for_test(fake_start_ms).await,
    )
    .unwrap();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, fake_start_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: global_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "timeline-fixture".to_owned(),
        region: "loopback".to_owned(),
    };
    let gamma = GammaClient::new(&gamma_endpoint).unwrap();
    let (control_tx, mut control_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let collector = tokio::spawn(async move {
        let mut context = PolymarketProcessingContext::new(initial.clone());
        let mut runtime = runtime;
        let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 17);
        recovery.set_wait_override(Some(Duration::from_secs(60)));
        let mut resolution_poll_tasks = JoinSet::new();
        collect_polymarket_single_receiver(
            Some(initial),
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &identity,
            None,
            Instant::now(),
            &mut runtime,
            &gamma,
            &mut recovery,
            &mut control_rx,
            Duration::ZERO,
            Duration::from_secs(1),
            move || PolymarketTimelineNow {
                utc_ms: paused_timeline_utc_ms(timeline_anchor, fake_start_ms),
                monotonic: TokioInstant::now(),
            },
        )
        .await
    });

    let initial_subscription_deadline = Instant::now() + Duration::from_secs(5);
    let initial_subscription_ready = loop {
        match initial_subscription_rx.try_recv() {
            Ok(()) => break true,
            Err(oneshot::error::TryRecvError::Empty)
                if Instant::now() < initial_subscription_deadline =>
            {
                tokio::task::yield_now().await;
            }
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed) => {
                break false;
            }
        }
    };
    assert!(
        initial_subscription_ready,
        "real CLOB loopback must observe the initial subscription"
    );

    let mut pre_budget_clob_trace = None;
    for target_ms in [
        boundary_ms,
        boundary_ms + 5_000,
        boundary_ms + 3 * 60_000,
        boundary_ms + 4 * 60_000,
        boundary_ms + 5 * 60_000,
        boundary_ms + 6 * 60_000,
        boundary_ms + 7 * 60_000,
        boundary_ms + 8 * 60_000,
    ] {
        *clob_timeline_utc_ms.lock().unwrap() = target_ms;
        advance_paused_timeline_to(timeline_anchor, fake_start_ms, target_ms).await;
        for _ in 0..512 {
            tokio::task::yield_now().await;
        }
        if target_ms == boundary_ms + 5_000 {
            let rollover_deadline = Instant::now() + Duration::from_secs(5);
            let rollover_subscription_ready = loop {
                match rollover_subscription_rx.try_recv() {
                    Ok(()) => break true,
                    Err(oneshot::error::TryRecvError::Empty)
                        if Instant::now() < rollover_deadline
                            && paused_timeline_utc_ms(timeline_anchor, fake_start_ms)
                                < boundary_ms + 3 * 60_000 =>
                    {
                        *clob_timeline_utc_ms.lock().unwrap() =
                            paused_timeline_utc_ms(timeline_anchor, fake_start_ms) + 250;
                        tokio::time::advance(Duration::from_millis(250)).await;
                        for _ in 0..16 {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(
                        oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed,
                    ) => break false,
                }
            };
            assert!(
                rollover_subscription_ready,
                "real CLOB loopback must observe market C subscription before the E+3m slot:collector_finished={}:gaps={:?}:gamma={:?}:clob={:?}",
                collector.is_finished(),
                gaps.lock().unwrap().all,
                gamma_trace.lock().unwrap(),
                clob_trace.lock().unwrap(),
            );
            let (reply_tx, reply_rx) = oneshot::channel();
            control_tx
                .send(PolymarketSourceControl::Fence {
                    stop_after_fence: false,
                    day_boundary_utc_ms: None,
                    resume_after_barrier: None,
                    reply: reply_tx,
                })
                .await
                .unwrap();
            *clob_timeline_utc_ms.lock().unwrap() =
                paused_timeline_utc_ms(timeline_anchor, fake_start_ms) + 250;
            tokio::time::advance(Duration::from_millis(250)).await;
            for _ in 0..512 {
                tokio::task::yield_now().await;
            }
            reply_rx.await.unwrap().unwrap();
            wait_for_cross_thread_clob_trace_quiet(&clob_trace).await;
            pre_budget_clob_trace = Some(clob_trace.lock().unwrap().clone());
        }
    }

    let (budget_reply_tx, mut budget_reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: false,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            reply: budget_reply_tx,
        })
        .await
        .unwrap();
    *clob_timeline_utc_ms.lock().unwrap() =
        paused_timeline_utc_ms(timeline_anchor, fake_start_ms) + 250;
    tokio::time::advance(Duration::from_millis(250)).await;
    for _ in 0..512 {
        tokio::task::yield_now().await;
    }
    let budget_reply_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match budget_reply_rx.try_recv() {
            Ok(result) => {
                result.unwrap();
                break;
            }
            Err(oneshot::error::TryRecvError::Empty) if Instant::now() < budget_reply_deadline => {
                *clob_timeline_utc_ms.lock().unwrap() =
                    paused_timeline_utc_ms(timeline_anchor, fake_start_ms) + 1;
                tokio::time::advance(Duration::from_millis(1)).await;
                tokio::task::yield_now().await;
            }
            Err(error) => {
                panic!(
                    "collector closed before budget fence:reply={error}:finished={}:gaps={:?}:gamma={:?}:clob={:?}",
                    collector.is_finished(),
                    gaps.lock().unwrap().all,
                    gamma_trace.lock().unwrap(),
                    clob_trace.lock().unwrap(),
                );
            }
        }
    }

    // This wall-clock bound only prevents a hung test. It does not advance fake UTC or
    // impose a teardown deadline after E+8; readiness remains part of the final evidence.
    let teardown_wall_deadline = Instant::now() + Duration::from_secs(5);
    let normal_old_asset_teardown_ready = loop {
        match old_assets_unsubscribed_rx.try_recv() {
            Ok(()) => break true,
            Err(oneshot::error::TryRecvError::Empty) if Instant::now() < teardown_wall_deadline => {
                tokio::time::sleep(Duration::ZERO).await;
            }
            Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed) => {
                break false;
            }
        }
    };
    wait_for_cross_thread_clob_trace_quiet(&clob_trace).await;
    let post_budget_pre_stop_clob_trace = clob_trace.lock().unwrap().clone();

    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(PolymarketSourceControl::Fence {
            stop_after_fence: true,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    *clob_timeline_utc_ms.lock().unwrap() =
        paused_timeline_utc_ms(timeline_anchor, fake_start_ms) + 250;
    tokio::time::advance(Duration::from_millis(250)).await;
    for _ in 0..512 {
        tokio::task::yield_now().await;
    }
    reply_rx.await.unwrap().unwrap();
    let collector_result = collector.await.unwrap();

    keep_time_paused.store(false, Ordering::Relaxed);
    time_guard.await.unwrap();
    gamma_server.abort();
    let _ = clob_shutdown_tx.send(());
    clob_server.join().unwrap();

    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    let metadata = events
        .iter()
        .filter_map(polymarket_metadata_event)
        .filter_map(|event| {
            serde_json::from_str::<Value>(&event.raw_json)
                .ok()
                .map(|raw| (event, raw))
        })
        .collect::<Vec<_>>();
    for (event, _) in &metadata {
        let rows = polymarket_metadata_v5_prefixes_from_event(event).unwrap();
        assert_eq!(rows.len(), 1);
        let validation = rows[0].validate();
        assert!(
            validation.is_complete(),
            "production v3 timeline row must satisfy the frozen matrix:{:?}:raw={}",
            validation.violations,
            event.raw_json,
        );
    }
    let prefetched_snapshot_before_t_minus_30 = metadata.iter().any(|(_, raw)| {
        raw["slug"] == market_b.slug
            && raw["metadata_row_kind"] == "metadata_snapshot"
            && raw["market_runtime_role"] == "next"
            && optional_i64(raw, "source_evidence_received_at_utc")
                .is_some_and(|received| received <= boundary_ms - 30_000)
    });
    let promotion_slots = metadata
        .iter()
        .filter(|(_, raw)| {
            raw["slug"] == market_b.slug
                && raw["market_lifecycle_transition"] == "prefetched_to_active"
                && raw["market_runtime_role_transition"] == "next_to_current"
        })
        .filter_map(|(_, raw)| optional_i64(raw, "lifecycle_scheduled_at_utc"))
        .collect::<Vec<_>>();
    let pending_slots = metadata
        .iter()
        .filter(|(_, raw)| {
            raw["slug"] == market_a.slug
                && raw["market_runtime_role_transition"] == "current_to_resolution_pending"
        })
        .filter_map(|(_, raw)| optional_i64(raw, "runtime_role_scheduled_at_utc"))
        .collect::<Vec<_>>();
    let attempt_rows = metadata
        .iter()
        .filter(|(_, raw)| {
            raw["slug"] == market_a.slug && raw["metadata_row_kind"] == "resolution_poll_attempt"
        })
        .filter_map(|(_, raw)| {
            Some((
                raw["resolution_poll_attempt_ordinal"].as_u64()?,
                optional_i64(raw, "resolution_poll_scheduled_utc")?,
            ))
        })
        .collect::<Vec<_>>();
    let attempt_results = metadata
        .iter()
        .filter(|(_, raw)| {
            raw["slug"] == market_a.slug && raw["metadata_row_kind"] == "resolution_poll_attempt"
        })
        .filter_map(|(_, raw)| raw["resolution_poll_result"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let exhausted_summaries = metadata
        .iter()
        .filter(|(_, raw)| {
            raw["slug"] == market_a.slug
                && raw["metadata_row_kind"] == "resolution_followup_summary"
        })
        .map(|(_, raw)| {
            json!({
                "status": raw["resolution_followup_status"],
                "attempts": raw["resolution_poll_attempts_recorded"],
                "valid": raw["resolution_valid_response_count"],
                "transport": raw["resolution_transport_unavailable_count"],
                "missed": raw["resolution_missed_slot_count"],
                "lifecycle": raw["market_lifecycle_state"],
                "role": raw["market_runtime_role"],
            })
        })
        .collect::<Vec<_>>();
    let legacy_sentinel_slots = metadata
        .iter()
        .filter(|(_, raw)| raw["market_lifecycle_state"] == "unresolved_window_end_observation")
        .filter_map(|(event, _)| event.source_ts.as_deref()?.parse::<i64>().ok())
        .collect::<Vec<_>>();
    let gamma_resolution_requests = gamma_trace
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, request)| request.contains(&format!("/markets/slug/{}", market_a.slug)))
        .map(|(at_ms, _)| *at_ms)
        .collect::<Vec<_>>();
    let clob_trace = clob_trace.lock().unwrap();
    let prefetched_before_t_minus_30 = clob_trace.iter().any(|(at_ms, request)| {
        *at_ms <= boundary_ms - 30_000
            && request["operation"] == "subscribe"
            && request["assets_ids"] == json!(["101", "102", "201", "202"])
    });
    drop(clob_trace);
    let unsubscribed_old_assets = |trace: &[(i64, Value)]| {
        trace
            .iter()
            .filter(|(_, request)| request["operation"] == "unsubscribe")
            .flat_map(|(_, request)| {
                request["assets_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|asset_id| matches!(*asset_id, "101" | "102"))
                    .map(str::to_owned)
            })
            .collect::<BTreeSet<_>>()
    };
    let old_assets_unsubscribed_before_budget = unsubscribed_old_assets(
        pre_budget_clob_trace
            .as_deref()
            .expect("pre-budget trace must be captured after market C is installed"),
    )
    .into_iter()
    .collect::<Vec<_>>();
    let old_assets_unsubscribed_after_budget_before_stop =
        unsubscribed_old_assets(&post_budget_pre_stop_clob_trace)
            .into_iter()
            .collect::<Vec<_>>();
    let old_assets_unsubscribed_before_horizon = post_budget_pre_stop_clob_trace
        .iter()
        .filter(|(_, request)| request["operation"] == "unsubscribe")
        .filter(|(_, request)| {
            request["assets_ids"].as_array().is_some_and(|asset_ids| {
                asset_ids.iter().any(|asset_id| {
                    asset_id
                        .as_str()
                        .is_some_and(|asset_id| matches!(asset_id, "101" | "102"))
                })
            })
        })
        .any(|(at_ms, _)| *at_ms < boundary_ms + 8 * 60_000);
    let expected_attempts = (1_u64..=6)
        .map(|ordinal| {
            (
                ordinal,
                boundary_ms + i64::try_from(ordinal + 2).unwrap() * 60_000,
            )
        })
        .collect::<Vec<_>>();
    let observed = json!({
        "collector_ok": collector_result.is_ok(),
        "prefetched_before_t_minus_30": prefetched_before_t_minus_30,
        "prefetched_snapshot_before_t_minus_30": prefetched_snapshot_before_t_minus_30,
        "promotion_slots": promotion_slots,
        "pending_slots": pending_slots,
        "attempt_rows": attempt_rows,
        "attempt_results": attempt_results,
        "exhausted_summaries": exhausted_summaries,
        "gamma_resolution_requests": gamma_resolution_requests,
        "old_assets_unsubscribed_before_budget": old_assets_unsubscribed_before_budget,
        "old_assets_unsubscribed_before_horizon": old_assets_unsubscribed_before_horizon,
        "old_assets_unsubscribed_before_stop": normal_old_asset_teardown_ready,
        "old_assets_unsubscribed_after_budget_before_stop": old_assets_unsubscribed_after_budget_before_stop,
        "legacy_sentinel_slots": legacy_sentinel_slots,
        "gaps": gaps.lock().unwrap().all.len(),
    });
    let expected = json!({
        "collector_ok": true,
        "prefetched_before_t_minus_30": true,
        "prefetched_snapshot_before_t_minus_30": true,
        "promotion_slots": [boundary_ms],
        "pending_slots": [boundary_ms],
        "attempt_rows": expected_attempts,
        "attempt_results": vec!["valid_nonterminal"; 6],
        "exhausted_summaries": [{
            "status": "budget_exhausted_after_valid_nonterminal",
            "attempts": 6,
            "valid": 6,
            "transport": 0,
            "missed": 0,
            "lifecycle": "active",
            "role": "none",
        }],
        "gamma_resolution_requests": expected_attempts.iter().map(|(_, slot)| *slot).collect::<Vec<_>>(),
        "old_assets_unsubscribed_before_budget": [],
        "old_assets_unsubscribed_before_horizon": false,
        "old_assets_unsubscribed_before_stop": true,
        "old_assets_unsubscribed_after_budget_before_stop": ["101", "102"],
        "legacy_sentinel_slots": [],
        "gaps": 0,
    });
    assert_eq!(
        observed, expected,
        "the approved absolute Polymarket lifecycle/follow-up timeline must remain green"
    );
}

#[tokio::test]
async fn polymarket_rollover_loopback_keeps_retained_lifecycle_and_filters_retired_tail() {
    let mut market_a = test_btc_discovery().current;
    let fixture_received_at_utc_ms: i64 = 1_783_392_404_599;
    let fixture_start_ts_s = fixture_received_at_utc_ms.div_euclid(300_000) * 300;
    market_a.id = "market-a".to_owned();
    market_a.start_ts_s = fixture_start_ts_s;
    market_a.end_ts_s = fixture_start_ts_s + 300;
    market_a.candidate_start_ts_s = fixture_start_ts_s;
    market_a.candidate_end_ts_s = fixture_start_ts_s + 300;
    market_a.slug = format!("btc-updown-5m-{}", market_a.start_ts_s);
    market_a.up_asset_id = "101".to_owned();
    market_a.down_asset_id = "102".to_owned();
    refresh_test_discovery_evidence(&mut market_a, fixture_received_at_utc_ms);
    let mut market_b = market_a.clone();
    market_b.id = "market-b".to_owned();
    market_b.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    market_b.up_asset_id = "201".to_owned();
    market_b.down_asset_id = "202".to_owned();
    market_b.start_ts_s += 300;
    market_b.end_ts_s += 300;
    market_b.candidate_start_ts_s += 300;
    market_b.candidate_end_ts_s += 300;
    market_b.slug = format!("btc-updown-5m-{}", market_b.start_ts_s);
    refresh_test_discovery_evidence(&mut market_b, fixture_received_at_utc_ms);
    let mut market_c = market_b.clone();
    market_c.id = "market-c".to_owned();
    market_c.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000004".to_owned();
    market_c.up_asset_id = "301".to_owned();
    market_c.down_asset_id = "302".to_owned();
    market_c.start_ts_s += 300;
    market_c.end_ts_s += 300;
    market_c.candidate_start_ts_s += 300;
    market_c.candidate_end_ts_s += 300;
    market_c.slug = format!("btc-updown-5m-{}", market_c.start_ts_s);
    refresh_test_discovery_evidence(&mut market_c, fixture_received_at_utc_ms);
    let initial = BtcUpdownDiscovery {
        current: market_a.clone(),
        next: Some(market_b.clone()),
        relation: None,
    };
    let rollover = BtcUpdownDiscovery {
        current: market_b.clone(),
        next: Some(market_c.clone()),
        relation: None,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (send_initial_tx, send_initial_rx) = oneshot::channel();
    let (initial_sent_tx, initial_sent_rx) = oneshot::channel();
    let (switch_requests_tx, switch_requests_rx) = oneshot::channel();
    let (send_rollover_tx, send_rollover_rx) = oneshot::channel();
    let (rollover_sent_tx, rollover_sent_rx) = oneshot::channel();
    let condition_a = market_a.condition_id.clone();
    let condition_b = market_b.condition_id.clone();
    let condition_c = market_c.condition_id.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut requests = Vec::<Value>::new();
        while requests.is_empty() {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if let Ok(request) = serde_json::from_str::<Value>(&text)
                && request.get("operation").is_some()
            {
                requests.push(request);
            }
        }
        send_initial_rx.await.unwrap();
        let initial_b = json!({
            "event_type": "book",
            "asset_id": "201",
            "market": condition_b,
            "timestamp": "200",
            "bids": [],
            "asks": []
        });
        for message in [
            json!({
                "event_type": "book",
                "asset_id": "101",
                "market": condition_a,
                "timestamp": "100",
                "bids": [],
                "asks": []
            }),
            initial_b.clone(),
        ] {
            socket
                .send(Message::Text(message.to_string().into()))
                .await
                .unwrap();
        }
        initial_sent_tx.send(()).unwrap();

        while requests.len() < 2 {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if let Ok(request) = serde_json::from_str::<Value>(&text)
                && request.get("operation").is_some()
            {
                requests.push(request);
                if requests.len() == 2 {
                    socket
                        .send(Message::Text(
                            json!({
                                "event_type": "book",
                                "asset_id": "301",
                                "market": condition_c,
                                "timestamp": "250",
                                "bids": [],
                                "asks": []
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
            }
        }
        switch_requests_tx.send(requests).unwrap();
        send_rollover_rx.await.unwrap();
        for message in [
            json!({
                "event_type": "book",
                "asset_id": "101",
                "market": condition_a,
                "timestamp": "101",
                "bids": [],
                "asks": []
            }),
            initial_b,
            json!({
                "event_type": "book",
                "asset_id": "201",
                "market": condition_b,
                "timestamp": "201",
                "bids": [],
                "asks": []
            }),
            json!({
                "event_type": "book",
                "asset_id": "301",
                "market": condition_c,
                "timestamp": "300",
                "bids": [],
                "asks": []
            }),
        ] {
            socket
                .send(Message::Text(message.to_string().into()))
                .await
                .unwrap();
        }
        rollover_sent_tx.send(()).unwrap();
        sleep(Duration::from_secs(10)).await;
    });

    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let recv_clock = Arc::new(Mutex::new(fixture_received_at_utc_ms));
    let runtime_recv_clock = Arc::clone(&recv_clock);
    let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(fixture_received_at_utc_ms).await,
        Arc::new(move || *runtime_recv_clock.lock().unwrap()),
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, fixture_received_at_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(16);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut context = PolymarketProcessingContext::new(initial.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let mut resolution_poll_tasks = JoinSet::new();
    let run_started = Instant::now();
    timeout(
        Duration::from_secs(5),
        install_polymarket_context(
            initial,
            true,
            &mut runtime,
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &identity,
            run_started,
            &mut recovery,
        ),
    )
    .await
    .expect("initial subscription must be bounded")
    .unwrap();

    send_initial_tx.send(()).unwrap();
    initial_sent_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 2 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("A and B must enter the decoded FIFO before the marker fence");
    fence_polymarket_processor(
        &mut runtime,
        false,
        &mut context,
        &sink,
        &identity,
        run_started,
        &mut recovery,
    )
    .await
    .unwrap();

    assert!(
        advance_polymarket_boundaries(
            market_b.start_ts_s * 1_000,
            &mut context,
            &sink,
            &identity,
            run_started,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        context.current.as_ref().map(|market| &market.slug),
        Some(&market_b.slug)
    );
    assert!(context.pending.contains_key(&market_a.slug));

    let installed = timeout(
        Duration::from_secs(5),
        install_polymarket_context_if_current(
            rollover,
            market_b.start_ts_s * 1_000,
            false,
            &mut runtime,
            &mut context,
            &mut resolution_poll_tasks,
            &sink,
            &identity,
            run_started,
            &mut recovery,
        ),
    )
    .await
    .expect("rollover subscription must be bounded")
    .unwrap();
    assert_eq!(
        installed,
        PolymarketContextInstallOutcome::Applied {
            applied_at_utc_ms: market_b.start_ts_s * 1_000,
        }
    );

    let requests = switch_requests_rx.await.unwrap();
    assert_eq!(requests[0]["operation"], "subscribe");
    assert_eq!(
        requests[0]["assets_ids"],
        json!(["101", "102", "201", "202"])
    );
    assert_eq!(requests[1]["operation"], "subscribe");
    assert_eq!(requests[1]["assets_ids"], json!(["301", "302"]));
    assert_eq!(requests.len(), 2);

    *recv_clock.lock().unwrap() = market_b.start_ts_s * 1_000 + 1;
    send_rollover_tx.send(()).unwrap();
    rollover_sent_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 4 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("retired tail and rollover events must enter the decoded FIFO");
    fence_polymarket_processor(
        &mut runtime,
        true,
        &mut context,
        &sink,
        &identity,
        run_started,
        &mut recovery,
    )
    .await
    .unwrap();

    let mut market_rows = Vec::new();
    while let Ok(payload) = rx.try_recv() {
        if let Some(message) = polymarket_v3_message(&payload) {
            let PolymarketOrderbookV3Event::Book(book) = &message.event else {
                panic!("rollover fixture must remain one typed Book row per SDK message");
            };
            market_rows.push((
                message
                    .envelope
                    .timestamp
                    .expect("Book fixture must carry its platform timestamp")
                    .to_string(),
                format!(
                    "{}:{}",
                    message
                        .envelope
                        .slug
                        .as_deref()
                        .expect("target-admitted Book must carry its slug"),
                    book.asset_id
                ),
            ));
        }
    }
    market_rows.sort();
    assert_eq!(
        market_rows,
        vec![
            ("100".to_owned(), format!("{}:101", market_a.slug)),
            ("200".to_owned(), format!("{}:201", market_b.slug)),
            ("200".to_owned(), format!("{}:201", market_b.slug)),
            ("201".to_owned(), format!("{}:201", market_b.slug)),
            ("250".to_owned(), format!("{}:301", market_c.slug)),
            ("300".to_owned(), format!("{}:301", market_c.slug)),
        ],
        "old A tail must be filtered while every retained B/C SDK delivery remains one v3 row"
    );
    assert!(runtime.pump.is_none());
    assert_eq!(stats.wait_count.load(Ordering::Relaxed), 0);
    assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 0);
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    let counters = stats.drain_all_operational();
    assert_eq!(
        counters
            .values()
            .flat_map(|by_source| by_source.values())
            .map(|counter| counter.structural_dedupe_dropped_events)
            .sum::<u64>(),
        0
    );
    assert!(gaps.lock().unwrap().is_empty());
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_fast_drain_absorbs_five_thousand_message_burst_in_fifo_order() {
    let (address, server) = spawn_polymarket_book_burst_server(
        5_000,
        "1",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
    )
    .await;

    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
        .await
        .unwrap();

    for expected_timestamp in 0..5_000_i64 {
        let event = timeout(Duration::from_secs(5), runtime.recv(Duration::from_secs(1)))
            .await
            .expect("fast-drain event wait")
            .expect("fast-drain terminal")
            .expect("fast-drain message");
        let PolymarketDrainEvent::Message(message) = event else {
            panic!("fence cannot precede the requested burst");
        };
        let WsMessage::Book(book) = message.message else {
            panic!("expected book event");
        };
        assert_eq!(book.timestamp, Some(expected_timestamp));
    }

    let request = runtime.request_fence(true).await.unwrap();
    let PolymarketFenceRequest::Queued(expected_fence_id) = request else {
        panic!("fast-drain pump must still be active");
    };
    loop {
        match runtime.recv(Duration::from_secs(1)).await.unwrap() {
            Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            }) => {
                assert_eq!(id, expected_fence_id);
                assert!(stop_after_fence);
                break;
            }
            Some(PolymarketDrainEvent::Message(_)) | None => {}
        }
    }
    runtime.wait_pump_stopped().await.unwrap();
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_fast_drain_fence_separates_before_and_after_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (before_sent_tx, before_sent_rx) = oneshot::channel();
    let (send_after_tx, send_after_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| value["operation"] == "subscribe")
            {
                break;
            }
        }
        for timestamp in [1_i64, 2] {
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "book",
                        "asset_id": "1",
                        "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "timestamp": timestamp.to_string(),
                        "bids": [],
                        "asks": []
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        before_sent_tx.send(()).unwrap();
        send_after_rx.await.unwrap();
        socket
            .send(Message::Text(
                json!({
                    "event_type": "book",
                    "asset_id": "1",
                    "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "timestamp": "3",
                    "bids": [],
                    "asks": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_secs(10)).await;
    });

    let global_budget = InflightBudget::new();
    let baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
        .await
        .unwrap();
    before_sent_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 2 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let PolymarketFenceRequest::Queued(fence_id) = runtime.request_fence(false).await.unwrap()
    else {
        panic!("pump must be active");
    };
    let mut before = Vec::new();
    loop {
        match runtime.recv(Duration::from_secs(1)).await.unwrap() {
            Some(PolymarketDrainEvent::Message(message)) => {
                let WsMessage::Book(book) = message.message else {
                    panic!("expected book event");
                };
                before.push(book.timestamp);
            }
            Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            }) => {
                assert_eq!(id, fence_id);
                assert!(!stop_after_fence);
                break;
            }
            None => {}
        }
    }
    assert_eq!(before, vec![Some(1), Some(2)]);

    send_after_tx.send(()).unwrap();
    let after = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(PolymarketDrainEvent::Message(message)) =
                runtime.recv(Duration::from_millis(100)).await.unwrap()
            {
                return message;
            }
        }
    })
    .await
    .unwrap();
    let WsMessage::Book(book) = &after.message else {
        panic!("expected book event");
    };
    assert_eq!(book.timestamp, Some(3));
    drop(after);
    let request = runtime.request_fence(true).await.unwrap();
    let PolymarketFenceRequest::Queued(stop_id) = request else {
        panic!("pump must be active before stop");
    };
    loop {
        if let Some(PolymarketDrainEvent::Fence {
            id,
            stop_after_fence,
        }) = runtime.recv(Duration::from_secs(1)).await.unwrap()
        {
            assert_eq!(id, stop_id);
            assert!(stop_after_fence);
            break;
        }
    }
    runtime.wait_pump_stopped().await.unwrap();
    assert_eq!(global_budget.available(), baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_paused_fence_holds_post_marker_messages_until_role_exit_release() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (before_sent_tx, before_sent_rx) = oneshot::channel();
    let (send_after_tx, send_after_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| value["operation"] == "subscribe")
            {
                break;
            }
        }
        socket
            .send(Message::Text(
                json!({
                    "event_type": "book",
                    "asset_id": "1",
                    "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "timestamp": "1",
                    "bids": [],
                    "asks": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        before_sent_tx.send(()).unwrap();
        send_after_rx.await.unwrap();
        socket
            .send(Message::Text(
                json!({
                    "event_type": "book",
                    "asset_id": "1",
                    "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "timestamp": "2",
                    "bids": [],
                    "asks": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_secs(10)).await;
    });

    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        bounded_ws_config(),
        InflightBudget::new(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
        .await
        .unwrap();
    before_sent_rx.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.is_empty() {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let PolymarketFenceRequest::Paused { id, resume } =
        runtime.request_paused_fence().await.unwrap()
    else {
        panic!("pump must accept the paused fence");
    };
    let mut before = Vec::new();
    loop {
        match runtime.recv(Duration::from_secs(1)).await.unwrap() {
            Some(PolymarketDrainEvent::Message(message)) => {
                let WsMessage::Book(book) = message.message else {
                    panic!("expected book event")
                };
                before.push(book.timestamp);
            }
            Some(PolymarketDrainEvent::Fence {
                id: observed,
                stop_after_fence,
            }) => {
                assert_eq!(observed, id);
                assert!(!stop_after_fence);
                break;
            }
            None => {}
        }
    }
    assert_eq!(before, vec![Some(1)]);

    send_after_tx.send(()).unwrap();
    assert!(
        runtime
            .recv(Duration::from_millis(100))
            .await
            .unwrap()
            .is_none(),
        "post-marker messages must not cross the role-exit linearization barrier"
    );
    resume.send(()).unwrap();
    let after = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(PolymarketDrainEvent::Message(message)) =
                runtime.recv(Duration::from_millis(100)).await.unwrap()
            {
                return message;
            }
        }
    })
    .await
    .unwrap();
    let WsMessage::Book(book) = after.message else {
        panic!("expected book event")
    };
    assert_eq!(book.timestamp, Some(2));
    runtime.abort_pump_and_wait().await;
    server.abort();
}

#[tokio::test]
async fn polymarket_fast_drain_fence_is_bounded_during_a_continuous_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stream_started_tx, stream_started_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| value["operation"] == "subscribe")
            {
                break;
            }
        }
        let mut stream_started_tx = Some(stream_started_tx);
        for timestamp in 0..2_000_i64 {
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "book",
                        "asset_id": "1",
                        "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "timestamp": timestamp.to_string(),
                        "bids": [],
                        "asks": []
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            if timestamp == 9 {
                stream_started_tx.take().unwrap().send(()).unwrap();
            }
            if timestamp % 10 == 9 {
                sleep(Duration::from_millis(1)).await;
            }
        }
        sleep(Duration::from_secs(10)).await;
    });

    let global_budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget,
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
        .await
        .unwrap();
    stream_started_rx.await.unwrap();
    let PolymarketFenceRequest::Queued(fence_id) = runtime.request_fence(false).await.unwrap()
    else {
        panic!("pump must be active");
    };

    let (before, after) = timeout(Duration::from_secs(5), async {
        let mut before = Vec::new();
        let mut after = Vec::new();
        let mut fence_seen = false;
        while before.len() + after.len() < 2_000 {
            match runtime.recv(Duration::from_millis(100)).await.unwrap() {
                Some(PolymarketDrainEvent::Message(message)) => {
                    let WsMessage::Book(book) = message.message else {
                        panic!("expected book event");
                    };
                    if fence_seen {
                        after.push(book.timestamp);
                    } else {
                        before.push(book.timestamp);
                    }
                }
                Some(PolymarketDrainEvent::Fence {
                    id,
                    stop_after_fence,
                }) => {
                    assert_eq!(id, fence_id);
                    assert!(!stop_after_fence);
                    assert!(!fence_seen);
                    fence_seen = true;
                }
                None => {}
            }
        }
        assert!(fence_seen);
        (before, after)
    })
    .await
    .expect("continuous stream fence must appear within a bounded time");
    assert!(!before.is_empty());
    assert!(!after.is_empty());
    assert_eq!(
        before.iter().chain(&after).copied().collect::<Vec<_>>(),
        (0..2_000_i64).map(Some).collect::<Vec<_>>()
    );

    let PolymarketFenceRequest::Queued(stop_id) = runtime.request_fence(true).await.unwrap() else {
        panic!("pump must be active before stop");
    };
    loop {
        if let Some(PolymarketDrainEvent::Fence {
            id,
            stop_after_fence,
        }) = runtime.recv(Duration::from_secs(1)).await.unwrap()
        {
            assert_eq!(id, stop_id);
            assert!(stop_after_fence);
            break;
        }
    }
    runtime.wait_pump_stopped().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn polymarket_fence_cannot_hide_a_terminal_already_present_at_its_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            if text == "PING" {
                socket.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            if serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| value["operation"] == "subscribe")
            {
                break;
            }
        }
        for timestamp in [1_i64, 2] {
            socket
                .send(Message::Text(
                    json!({
                        "event_type": "book",
                        "asset_id": "1",
                        "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "timestamp": timestamp.to_string(),
                        "bids": [],
                        "asks": []
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        socket
            .send(Message::Text(
                r#"{"event_type":"book","asset_id":"1""#.into(),
            ))
            .await
            .unwrap();
        sleep(Duration::from_secs(10)).await;
    });

    let mut sdk = MarketSdkRuntime::new(format!("ws://{address}"), bounded_ws_config())
        .expect("market runtime");
    sdk.replace_and_wait(BTreeSet::from([U256::from(1)]), Duration::from_secs(2))
        .await
        .unwrap();
    let mut receiver = sdk.take_message_receiver().unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if receiver.pending_len() == 2
                && matches!(
                    receiver.terminal_snapshot(),
                    Some(SdkStreamError::Parser(_))
                )
            {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("valid messages and parser terminal must both be ready before the fence");

    let global_budget = InflightBudget::new();
    let decoded_budget = InflightBudget::with_limits(8, MAX_TYPED_ROW_BYTES * 8);
    let global_baseline = global_budget.available();
    let decoded_baseline = decoded_budget.available();
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let (control_tx, control_rx) = mpsc::channel(1);
    control_tx
        .send(PolymarketPumpControl::Fence {
            id: 1,
            stop_after_fence: true,
            resume_after_fence: None,
        })
        .await
        .unwrap();
    let pump = tokio::spawn(run_polymarket_fast_drain(
        receiver,
        event_tx,
        control_rx,
        global_budget.clone(),
        decoded_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
        Arc::new(now_ms),
    ));

    for expected_timestamp in [1_i64, 2] {
        let Some(PolymarketPumpItem::Message(message)) = event_rx.recv().await else {
            panic!("accepted messages must precede the parser terminal");
        };
        let WsMessage::Book(book) = message.message else {
            panic!("expected book event");
        };
        assert_eq!(book.timestamp, Some(expected_timestamp));
    }
    assert!(matches!(
        event_rx.recv().await,
        Some(PolymarketPumpItem::Terminal(PolymarketDrainTerminal::Sdk(
            SdkStreamError::Parser(_)
        )))
    ));
    assert!(
        event_rx.recv().await.is_none(),
        "a successful fence must not follow the terminal"
    );
    timeout(Duration::from_secs(1), pump)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_five_thousand_message_burst_traverses_the_composed_ingress_path() {
    let (address, server) = spawn_polymarket_book_burst_server(
        5_001,
        "101",
        "0x0000000000000000000000000000000000000000000000000000000000000002",
    )
    .await;
    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(101)]), Duration::from_secs(2))
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if runtime.events.len() >= 5_001 {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all native messages enter the decoded FIFO without SDK lag");
    sleep(Duration::from_millis(25)).await;
    let dispatch_started_utc_ms = now_ms();

    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let (manifest_tx, _manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&runtime_stats, dispatch_started_utc_ms).await;
    let mut writers = start_writer_workers(
        &dataset_root,
        &test_runtime_provenance("polymarket-composed"),
        &manifest_tx,
        &runtime_stats,
    )
    .unwrap();
    let mut batches = PendingIngressBatches::for_live_sources();
    let mut coverage = BTreeMap::<String, CoverageStats>::new();
    let mut received_rows = 0_u64;
    let mut average_event_bytes = 0_u64;
    let mut writer_queue_tracker = QueueDurationTracker::new("writer_chunk_queue");
    let mut rollover_coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(6_000);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&runtime_stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let mut current = test_btc_discovery().current;
    let current_start_ts_s = dispatch_started_utc_ms.div_euclid(300_000) * 300;
    current.slug = format!("btc-updown-5m-{current_start_ts_s}");
    current.start_ts_s = current_start_ts_s;
    current.end_ts_s = current_start_ts_s + 300;
    current.candidate_start_ts_s = current.start_ts_s;
    current.candidate_end_ts_s = current.end_ts_s;
    refresh_test_discovery_evidence(&mut current, dispatch_started_utc_ms - 1);
    let discovery = BtcUpdownDiscovery {
        current,
        next: None,
        relation: None,
    };
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let run_started = Instant::now();

    let Some(PolymarketDrainEvent::Message(zero_timestamp)) = runtime
        .recv(Duration::from_secs(1))
        .await
        .expect("decoded zero-timestamp fixture")
    else {
        panic!("expected the initial zero-timestamp Book fixture");
    };
    let BudgetedPolymarketMessage {
        message,
        raw_day_ticket,
        _global_reservation,
        _decoded_reservation,
        ..
    } = zero_timestamp;
    let WsMessage::Book(zero_timestamp_book) = message else {
        panic!("expected an initial Book fixture");
    };
    assert_eq!(zero_timestamp_book.timestamp, Some(0));
    drop((
        zero_timestamp_book,
        raw_day_ticket,
        _global_reservation,
        _decoded_reservation,
    ));

    for _ in 0..5_000 {
        let Some(PolymarketDrainEvent::Message(message)) = runtime
            .recv(Duration::from_secs(1))
            .await
            .expect("decoded terminal")
        else {
            panic!("expected decoded market message");
        };
        process_polymarket_budgeted_message(
            message,
            &mut context,
            &sink,
            &identity,
            run_started,
            &mut recovery,
        )
        .await
        .unwrap();
    }
    fence_polymarket_processor(
        &mut runtime,
        true,
        &mut context,
        &sink,
        &identity,
        run_started,
        &mut recovery,
    )
    .await
    .unwrap();

    let mut previous_recv_time = i64::MIN;
    for expected_timestamp in 1..=5_000_i64 {
        let inflight_event = rx.recv().await.unwrap();
        let SourceIngressPayload::PolymarketOrderbookV3(typed) = &inflight_event else {
            panic!("activated Polymarket source must enqueue v3 typed ingress");
        };
        assert_eq!(typed.message.envelope.timestamp, Some(expected_timestamp));
        assert!(typed.message.envelope.timestamp_received <= dispatch_started_utc_ms);
        assert!(typed.message.envelope.timestamp_received >= previous_recv_time);
        previous_recv_time = typed.message.envelope.timestamp_received;
        let PolymarketOrderbookV3Event::Book(book) = &typed.message.event else {
            panic!("one SDK Book message must remain one v3 Book row");
        };
        assert_eq!(book.asset_id, "101");
        process_ingress_event(
            inflight_event,
            &mut writers,
            &mut batches,
            &mut coverage,
            &mut received_rows,
            &mut average_event_bytes,
            &gaps,
            &runtime_stats,
            &mut writer_queue_tracker,
            run_started,
            &manifest_tx,
            &mut rollover_coordinator,
        )
        .await
        .unwrap();
    }
    assert!(rx.try_recv().is_err());
    flush_all_batches(
        &mut writers,
        &mut batches,
        &gaps,
        &runtime_stats,
        &mut writer_queue_tracker,
        run_started,
    )
    .await
    .unwrap();
    let writer_outcome = stop_writer_workers_with_budget(writers, Duration::from_secs(30)).await;
    assert!(writer_outcome.all_terminal);
    assert!(
        writer_outcome.errors.is_empty(),
        "{:?}",
        writer_outcome.errors
    );
    let polymarket_output = writer_outcome
        .outputs
        .iter()
        .find(|output| output.source_kind == "polymarket_market")
        .unwrap();
    assert_eq!(polymarket_output.stats.events_received, 5_000);
    assert_eq!(polymarket_output.stats.events_written, 5_000);
    assert_eq!(polymarket_output.stats.typed_rows_written, 5_000);
    assert_eq!(polymarket_output.stats.events_dropped, 0);
    assert_eq!(polymarket_output.finalized_segments.len(), 1);
    let segment = &polymarket_output.finalized_segments[0];
    assert_eq!(segment.row_count, 5_000);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&segment.parquet_path).unwrap())
            .unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 5_000);
    assert!(gaps.lock().unwrap().is_empty());
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_processing_error_drains_later_messages_and_preserves_first_error() {
    let recv_time_utc_ms = 1_783_392_400_000;
    let (address, server) = spawn_polymarket_book_burst_server(
        3,
        "101",
        "0x0000000000000000000000000000000000000000000000000000000000000002",
    )
    .await;
    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(recv_time_utc_ms).await,
        Arc::new(move || recv_time_utc_ms),
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(101)]), Duration::from_secs(2))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 3 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all three messages enter the decoded FIFO");

    let Some(PolymarketDrainEvent::Message(first)) = runtime
        .recv(Duration::from_secs(1))
        .await
        .expect("decoded terminal")
    else {
        panic!("expected first decoded message");
    };
    let gate_recv_time_utc_ms = first.recv_time_utc_ms;
    drop(first);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, gate_recv_time_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let first_error_text =
        "operator_intervention_required:max_typed_row_bytes_exceeded:polymarket_market";

    let error = stop_polymarket_after_processing_error(
        invalid_data(first_error_text).into(),
        &mut runtime,
        &mut context,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .expect_err("the original processing error must remain primary");
    assert_eq!(error.to_string(), first_error_text);
    for expected_timestamp in [1, 2] {
        let payload = rx.recv().await.expect("later message must be dispatched");
        let message = polymarket_v3_message(&payload).expect("later message must use v3 ingress");
        assert_eq!(message.envelope.timestamp, Some(expected_timestamp));
    }
    assert!(rx.try_recv().is_err());
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    assert!(runtime.pump.is_none());
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_closed_ingress_abandons_exact_later_rows_and_joins_pump() {
    let recv_time_utc_ms = 1_783_392_400_000;
    let (address, server) = spawn_polymarket_book_burst_server(
        4,
        "101",
        "0x0000000000000000000000000000000000000000000000000000000000000002",
    )
    .await;
    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(recv_time_utc_ms).await,
        Arc::new(move || recv_time_utc_ms),
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(101)]), Duration::from_secs(2))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 4 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all four messages enter the decoded FIFO");

    let Some(PolymarketDrainEvent::Message(zero_timestamp)) = runtime
        .recv(Duration::from_secs(1))
        .await
        .expect("decoded zero-timestamp fixture")
    else {
        panic!("expected the initial zero-timestamp fixture");
    };
    drop(zero_timestamp);

    let Some(PolymarketDrainEvent::Message(first)) = runtime
        .recv(Duration::from_secs(1))
        .await
        .expect("decoded terminal")
    else {
        panic!("expected first decoded message");
    };
    let gate_recv_time_utc_ms = first.recv_time_utc_ms;
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, gate_recv_time_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, rx) = mpsc::channel(8);
    drop(rx);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);
    let run_started = Instant::now();
    let first_error = process_polymarket_budgeted_message(
        first,
        &mut context,
        &sink,
        &identity,
        run_started,
        &mut recovery,
    )
    .await
    .expect_err("closed ingress must reject the first message");

    let returned = stop_polymarket_after_processing_error(
        first_error,
        &mut runtime,
        &mut context,
        &sink,
        &identity,
        run_started,
        &mut recovery,
    )
    .await
    .expect_err("the first ingress error must remain primary");
    assert!(returned.to_string().contains("bounded_queue_send_closed"));
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 1);
    assert!(gaps.lock().unwrap().iter().any(|reason| {
        reason.contains("polymarket_accepted_decoded_messages_discarded:messages=2:first_recv")
            && !reason.contains(":rows=")
    }));
    assert!(runtime.pump.is_none());
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_abandoned_messages_are_reported_per_day_without_invented_row_counts() {
    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new(
        "ws://127.0.0.1:9".to_owned(),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime.abort_pump_and_wait().await;
    let decoded_baseline = runtime.decoded_budget.available();
    let message = || {
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "101",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1",
            "bids": [],
            "asks": []
        }))
        .unwrap()
    };
    let first_recv_time_utc_ms = UTC_DAY_MS - 1;
    let second_recv_time_utc_ms = UTC_DAY_MS;
    let first_day = utc_day_string_from_ms(first_recv_time_utc_ms).unwrap();
    let second_day = utc_day_string_from_ms(second_recv_time_utc_ms).unwrap();
    assert_ne!(first_day, second_day);
    let mut first =
        budgeted_polymarket_message_for_test(message(), &global_budget, &runtime.decoded_budget)
            .await;
    first.recv_time_utc_ms = first_recv_time_utc_ms;
    let mut second =
        budgeted_polymarket_message_for_test(message(), &global_budget, &runtime.decoded_budget)
            .await;
    second.recv_time_utc_ms = second_recv_time_utc_ms;
    let (event_tx, event_rx) = mpsc::channel(3);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    runtime.events = event_rx;
    runtime.controls = control_tx;
    runtime.pump = Some(tokio::spawn(async move {
        let Some(PolymarketPumpControl::Fence {
            id,
            stop_after_fence,
            ..
        }) = control_rx.recv().await
        else {
            return;
        };
        assert!(stop_after_fence);
        event_tx
            .send(PolymarketPumpItem::Message(first))
            .await
            .unwrap();
        event_tx
            .send(PolymarketPumpItem::Message(second))
            .await
            .unwrap();
        event_tx
            .send(PolymarketPumpItem::Fence {
                id,
                stop_after_fence,
            })
            .await
            .unwrap();
    }));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);

    stop_polymarket_after_processing_error(
        io::Error::new(io::ErrorKind::BrokenPipe, "closed ingress").into(),
        &mut runtime,
        &mut context,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .expect_err("the original closed-ingress error remains primary");

    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    assert!(stats.drain_operational_day(&first_day).is_empty());
    assert!(stats.drain_operational_day(&second_day).is_empty());
    let recorded = gaps.lock().unwrap();
    assert!(recorded.reasons_for_day(&first_day).iter().any(|reason| {
        reason.contains("messages=1")
            && reason.contains(&format!("first_recv_time_utc_ms={first_recv_time_utc_ms}"))
            && !reason.contains(":rows=")
    }));
    assert!(recorded.reasons_for_day(&second_day).iter().any(|reason| {
        reason.contains("messages=1")
            && reason.contains(&format!("first_recv_time_utc_ms={second_recv_time_utc_ms}"))
            && !reason.contains(":rows=")
    }));
    drop(recorded);
    assert!(runtime.pump.is_none());
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
}

#[tokio::test]
async fn polymarket_retryable_cleanup_terminal_does_not_create_a_false_gap() {
    let global_budget = InflightBudget::new();
    let mut runtime = PolymarketDrainRuntime::new(
        "ws://127.0.0.1:9".to_owned(),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(now_ms()).await,
    )
    .unwrap();
    runtime.abort_pump_and_wait().await;
    let (event_tx, event_rx) = mpsc::channel(1);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    runtime.events = event_rx;
    runtime.controls = control_tx;
    runtime.pump = Some(tokio::spawn(async move {
        let Some(PolymarketPumpControl::Fence {
            stop_after_fence, ..
        }) = control_rx.recv().await
        else {
            return;
        };
        assert!(stop_after_fence);
        event_tx
            .send(PolymarketPumpItem::Terminal(PolymarketDrainTerminal::Sdk(
                SdkStreamError::Connection("cleanup transport".to_owned()),
            )))
            .await
            .unwrap();
    }));

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, _rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: stats,
        inflight_budget: global_budget,
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);

    let error = stop_polymarket_after_processing_error(
        SdkStreamError::Connection("primary transport".to_owned()).into(),
        &mut runtime,
        &mut context,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .expect_err("the retryable primary error must be returned for outer recovery");

    assert!(error.to_string().contains("primary transport"));
    assert!(market_source_error_is_retryable(error.as_ref()));
    assert!(gaps.lock().unwrap().is_empty());
    assert!(runtime.pump.is_none());
}

#[tokio::test]
async fn polymarket_cleanup_preserves_retryable_primary_after_later_v3_delivery() {
    let recv_time_utc_ms = 1_783_392_400_000;
    let (address, server) = spawn_polymarket_book_burst_server(
        2,
        "101",
        "0x0000000000000000000000000000000000000000000000000000000000000002",
    )
    .await;
    let global_budget = InflightBudget::new();
    let global_baseline = global_budget.available();
    let mut runtime = PolymarketDrainRuntime::new_with_recv_clock(
        format!("ws://{address}"),
        bounded_ws_config(),
        global_budget.clone(),
        active_raw_day_gate_for_test(recv_time_utc_ms).await,
        Arc::new(move || recv_time_utc_ms),
    )
    .unwrap();
    let decoded_baseline = runtime.decoded_budget.available();
    runtime
        .sdk
        .replace_and_wait(BTreeSet::from([U256::from(101)]), Duration::from_secs(2))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while runtime.events.len() < 2 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("both decoded messages must be queued before cleanup");
    let Some(PolymarketDrainEvent::Message(zero_timestamp)) = runtime
        .recv(Duration::from_secs(1))
        .await
        .expect("decoded zero-timestamp fixture")
    else {
        panic!("expected the initial zero-timestamp fixture");
    };
    drop(zero_timestamp);

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, recv_time_utc_ms).await;
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, mut rx) = mpsc::channel(8);
    let sink = SourceEventSink {
        tx,
        gaps,
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: global_budget.clone(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let mut context = PolymarketProcessingContext::new(discovery.clone());
    let mut recovery = SourceRecoverySupervisor::new("polymarket_market", 7);

    let error = stop_polymarket_after_processing_error(
        SdkStreamError::Connection("retryable primary".to_owned()).into(),
        &mut runtime,
        &mut context,
        &sink,
        &identity,
        Instant::now(),
        &mut recovery,
    )
    .await
    .expect_err("cleanup must return the retryable primary after admitting the later v3 row");
    assert!(error.to_string().contains("retryable primary"));
    let payload = rx
        .try_recv()
        .expect("the later positive-timestamp message must be admitted");
    let message = polymarket_v3_message(&payload).expect("cleanup delivery must use v3 ingress");
    assert_eq!(message.envelope.timestamp, Some(1));
    assert!(rx.try_recv().is_err());
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    assert!(runtime.pump.is_none());
    drop(payload);
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(runtime.decoded_budget.available(), decoded_baseline);
    server.abort();
}

#[tokio::test]
async fn polymarket_decoded_capacity_terminal_follows_already_accepted_messages() {
    let message = || {
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "101",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1",
            "bids": [],
            "asks": []
        }))
        .unwrap()
    };
    let owned_bytes = owned_polymarket_message_bytes(&message());
    let global_budget = InflightBudget::with_limits(2, owned_bytes.saturating_mul(2));
    let decoded_budget = InflightBudget::with_limits(1, owned_bytes);
    let global_baseline = global_budget.available();
    let decoded_baseline = decoded_budget.available();
    let (tx, mut rx) = mpsc::channel(3);
    let raw_day_gate = active_raw_day_gate_for_test(now_ms()).await;

    assert!(
        enqueue_polymarket_decoded_message(
            message(),
            &tx,
            &global_budget,
            &decoded_budget,
            &raw_day_gate,
            now_ms(),
        )
        .await
    );
    assert!(
        !enqueue_polymarket_decoded_message(
            message(),
            &tx,
            &global_budget,
            &decoded_budget,
            &raw_day_gate,
            now_ms(),
        )
        .await
    );

    let Some(PolymarketPumpItem::Message(first)) = rx.recv().await else {
        panic!("accepted message must precede capacity terminal");
    };
    drop(first);
    let Some(PolymarketPumpItem::Terminal(PolymarketDrainTerminal::Capacity { reason, .. })) =
        rx.recv().await
    else {
        panic!("capacity terminal must follow accepted messages");
    };
    assert!(reason.contains("polymarket_decoded_budget_failed"));
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(decoded_budget.available(), decoded_baseline);
}

#[tokio::test]
async fn polymarket_global_capacity_terminal_follows_already_accepted_messages() {
    let message = || {
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "101",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1",
            "bids": [],
            "asks": []
        }))
        .unwrap()
    };
    let owned_bytes = owned_polymarket_message_bytes(&message());
    let global_budget = InflightBudget::with_limits(1, owned_bytes);
    let decoded_budget = InflightBudget::with_limits(2, owned_bytes.saturating_mul(2));
    let global_baseline = global_budget.available();
    let decoded_baseline = decoded_budget.available();
    let (tx, mut rx) = mpsc::channel(3);
    let raw_day_gate = active_raw_day_gate_for_test(now_ms()).await;

    assert!(
        enqueue_polymarket_decoded_message(
            message(),
            &tx,
            &global_budget,
            &decoded_budget,
            &raw_day_gate,
            now_ms(),
        )
        .await
    );
    assert!(
        !enqueue_polymarket_decoded_message(
            message(),
            &tx,
            &global_budget,
            &decoded_budget,
            &raw_day_gate,
            now_ms(),
        )
        .await
    );

    let Some(PolymarketPumpItem::Message(first)) = rx.recv().await else {
        panic!("accepted message must precede global-capacity terminal");
    };
    drop(first);
    let Some(PolymarketPumpItem::Terminal(PolymarketDrainTerminal::Capacity { reason, .. })) =
        rx.recv().await
    else {
        panic!("global-capacity terminal must follow accepted messages");
    };
    assert!(reason.contains("polymarket_decoded_global_budget_failed"));
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(decoded_budget.available(), decoded_baseline);
}

#[tokio::test]
async fn polymarket_queue_full_terminal_does_not_block_after_consumer_resumes() {
    let message = || {
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "101",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1",
            "bids": [],
            "asks": []
        }))
        .unwrap()
    };
    let owned_bytes = owned_polymarket_message_bytes(&message());
    let global_budget = InflightBudget::with_limits(2, owned_bytes.saturating_mul(2));
    let decoded_budget = InflightBudget::with_limits(2, owned_bytes.saturating_mul(2));
    let global_baseline = global_budget.available();
    let decoded_baseline = decoded_budget.available();
    let (tx, mut rx) = mpsc::channel(1);
    let raw_day_gate = active_raw_day_gate_for_test(now_ms()).await;

    assert!(
        enqueue_polymarket_decoded_message(
            message(),
            &tx,
            &global_budget,
            &decoded_budget,
            &raw_day_gate,
            now_ms(),
        )
        .await
    );
    let blocked_tx = tx.clone();
    let blocked_global = global_budget.clone();
    let blocked_decoded = decoded_budget.clone();
    let blocked_raw_day_gate = Arc::clone(&raw_day_gate);
    let blocked = tokio::spawn(async move {
        enqueue_polymarket_decoded_message(
            message(),
            &blocked_tx,
            &blocked_global,
            &blocked_decoded,
            &blocked_raw_day_gate,
            now_ms(),
        )
        .await
    });
    sleep(BUFFER_ACQUIRE_TIMEOUT + Duration::from_millis(50)).await;

    let Some(PolymarketPumpItem::Message(first)) = rx.recv().await else {
        panic!("accepted message must precede queue-full terminal");
    };
    drop(first);
    assert!(
        !timeout(Duration::from_secs(1), blocked)
            .await
            .expect("terminal enqueue must unblock when the consumer resumes")
            .unwrap()
    );
    let Some(PolymarketPumpItem::Terminal(PolymarketDrainTerminal::Capacity { reason, .. })) =
        rx.recv().await
    else {
        panic!("queue-full terminal must follow accepted messages");
    };
    assert_eq!(reason, "polymarket_decoded_queue_backpressure_timeout");
    assert_eq!(global_budget.available(), global_baseline);
    assert_eq!(decoded_budget.available(), decoded_baseline);
}

#[test]
fn polymarket_terminals_distinguish_transport_availability_from_integrity_gaps() {
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let (tx, _rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::clone(&gaps),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let recv_time_utc_ms = now_ms();
    let utc_day = utc_day_string_from_ms(recv_time_utc_ms).unwrap();

    let capacity = polymarket_drain_terminal_error(
        PolymarketDrainTerminal::Capacity {
            reason: "polymarket_decoded_global_budget_failed:test".to_owned(),
            recv_time_utc_ms,
            error_kind: io::ErrorKind::TimedOut,
        },
        &sink,
    );
    assert!(!market_source_error_is_retryable(capacity.as_ref()));
    assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 1);
    assert_eq!(stats.dropped_rows.load(Ordering::Relaxed), 0);
    let capacity_counters = stats.drain_operational_day(&utc_day);
    assert!(capacity_counters.is_empty());

    let oversized = polymarket_drain_terminal_error(
        PolymarketDrainTerminal::Capacity {
            reason: "operator_intervention_required:Polymarket decoded message exceeds limit"
                .to_owned(),
            recv_time_utc_ms,
            error_kind: io::ErrorKind::InvalidData,
        },
        &sink,
    );
    assert_eq!(
        oversized.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(stats.timeout_count.load(Ordering::Relaxed), 1);

    let gaps_before_transport = gaps.lock().unwrap().len();
    let transport = polymarket_drain_terminal_error(
        PolymarketDrainTerminal::Sdk(SdkStreamError::Connection(
            "test transport failure".to_owned(),
        )),
        &sink,
    );
    assert!(market_source_error_is_retryable(transport.as_ref()));
    assert_eq!(gaps.lock().unwrap().len(), gaps_before_transport);

    let lagged = polymarket_drain_terminal_error(
        PolymarketDrainTerminal::Sdk(SdkStreamError::Lagged(7)),
        &sink,
    );
    assert!(!market_source_error_is_retryable(lagged.as_ref()));
    let lag_counters = stats.drain_operational_day(&utc_day);
    assert_eq!(lag_counters["polymarket_market"].lag_events, 7);
    let reasons = gaps.lock().unwrap().all_reasons();
    assert!(reasons.iter().any(|reason| {
        reason.contains("polymarket_decoded_global_budget_failed:test")
            && reason.contains("decoded_messages_dropped=1")
            && reason.contains("normalized_rows=unknown")
    }));
    assert!(
        reasons
            .iter()
            .any(|reason| reason.contains("polymarket_sdk_stream_fatal:receiver lagged by 7"))
    );
}

#[tokio::test]
async fn gamma_candidate_keeps_real_fetch_start_and_full_response_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let _ = socket.read(&mut request).await.unwrap();
        sleep(Duration::from_millis(30)).await;
        let body = r#"[{"id":"market-test"}]"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let batch = fetch_exact_slug_candidates(
        &GammaClient::new(&endpoint).unwrap(),
        "btc-updown-5m-1783392300",
    )
    .await
    .unwrap();
    assert_eq!(batch.candidates.len(), 1);
    let fetch_start = batch.candidates[0].metadata_fetch_start_utc_ms.unwrap();
    let timestamp_received = batch.candidates[0].metadata_received_utc_ms.unwrap();
    assert!(fetch_start < timestamp_received);
    server.await.unwrap();
}

#[tokio::test]
async fn polymarket_discovery_samples_validation_after_candidate_selection() {
    let query_utc_ms = 1_783_392_450_000;
    let window = discovery_windows(query_utc_ms).current;
    let body = gamma_market_response_for_window(&window);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4_096];
        let _ = socket.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let clock_calls = AtomicU64::new(0);
    let discovery = discover_markets_with_wait_clock(
        &GammaClient::new(&endpoint).unwrap(),
        Duration::ZERO,
        Duration::ZERO,
        || query_utc_ms + i64::try_from(clock_calls.fetch_add(1, Ordering::SeqCst)).unwrap(),
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert_eq!(
        discovery.current.metadata_fetch_start_utc_ms,
        Some(query_utc_ms + 1)
    );
    assert_eq!(
        discovery.current.metadata_received_utc_ms,
        Some(query_utc_ms + 2)
    );
    assert_eq!(
        discovery.current.metadata_validation_completed_at_utc_ms,
        query_utc_ms + 4
    );
    assert_eq!(
        discovery.current.fixed_identity_verified_at_utc_ms,
        query_utc_ms + 4
    );
}

#[test]
fn polymarket_metadata_admission_uses_real_time_and_rejects_future_causal_fields() {
    let discovery = test_btc_discovery();
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let base_row = polymarket_metadata_snapshot_v3_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &collector_identity,
    )
    .unwrap();

    for field in [
        "recv_time_utc_ms",
        "timestamp_received",
        "fixed_identity_verified_at_utc",
        "source_evidence_received_at_utc",
        "row_evidence_validation_completed_at_utc",
        "metadata_fetch_start_utc",
        "lifecycle_applied_at_utc",
        "runtime_role_applied_at_utc",
    ] {
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let (tx, _rx) = mpsc::channel(1);
        let sink = SourceEventSink {
            tx,
            gaps: Arc::new(Mutex::new(SourceGapStore::default())),
            source_queue_backpressure: Arc::clone(&stats),
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut event = polymarket_metadata_v5_raw_event(
            PolymarketMarketMetadataV5Row::from_v3_prefix(base_row.clone(), None, None),
        )
        .unwrap();
        let future = now_ms() + 60_000;
        if field == "recv_time_utc_ms" {
            event.recv_time_utc_ms = future;
        } else {
            let mut raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
            raw[field] = json!(future);
            event.raw_json = raw.to_string();
        }

        let error = stamp_source_admission(&mut event, &sink).unwrap_err();
        assert!(error.to_string().contains(field), "{field}: {error}");
        assert!(event.collector_admission.is_none());
        assert_eq!(
            stats
                .polymarket_metadata_admission_sequence
                .load(Ordering::Relaxed),
            0
        );
    }

    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let (tx, _rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut event = polymarket_metadata_v5_raw_event(
        PolymarketMarketMetadataV5Row::from_v3_prefix(base_row, None, None),
    )
    .unwrap();
    let mut raw = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    raw["closed_time_utc"] = json!(now_ms() + 60_000);
    event.raw_json = raw.to_string();
    let before = now_ms();
    stamp_source_admission(&mut event, &sink).unwrap();
    let after = now_ms();
    let admission = event.collector_admission.unwrap();
    assert!((before..=after).contains(&admission.time_utc_ms));
    assert_eq!(admission.sequence, 1);
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn polymarket_terminal_post_batch_rejects_future_post_without_consuming_sequence() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let scheduled_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let completed_at_utc_ms = scheduled_at_utc_ms + 1;
    let sdk_market = serde_json::from_value::<SdkGammaMarket>(gamma_market_value_for_discovered(
        &discovery.current,
    ))
    .unwrap();
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms: scheduled_at_utc_ms,
        validation_completed_at_utc_ms: completed_at_utc_ms,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: scheduled_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: scheduled_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_sync_test(scheduled_at_utc_ms),
    };
    let mut pending = PendingPolymarketResolution::new(identity.clone(), Some(evidence.clone()));
    pending.in_flight_ordinal = Some(1);
    let post = polymarket_resolution_attempt_v3_row(
        &pending,
        &GammaResolutionPollCompletion {
            slug: identity.slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms,
            fetch_start_utc_ms: scheduled_at_utc_ms,
            completed_at_utc_ms,
            result: Ok(sdk_market),
            raw_day_ticket: RawDayTicketAcceptance::Failed("unused by pure row fixture".to_owned()),
        },
        &|| completed_at_utc_ms + 1,
        true,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();
    let terminal = polymarket_resolution_terminal_ws_v3_row(
        &pending,
        &evidence,
        completed_at_utc_ms + 2,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();

    let terminal_event = InflightRawEvent::for_test(
        polymarket_metadata_v5_raw_event(PolymarketMarketMetadataV5Row::from_v3_prefix(
            terminal,
            Some(evidence.source_sequence_epoch_id.clone()),
            Some(evidence.collector_admission_sequence),
        ))
        .unwrap(),
    );
    let mut post_event = InflightRawEvent::for_test(
        polymarket_metadata_v5_raw_event(PolymarketMarketMetadataV5Row::from_v3_prefix(
            post, None, None,
        ))
        .unwrap(),
    );
    let future = now_ms() + 60_000;
    let mut post_raw = serde_json::from_str::<Value>(&post_event.event.raw_json).unwrap();
    post_raw["timestamp_received"] = json!(future);
    post_event.event.raw_json = post_raw.to_string();
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    let (tx, _rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };

    let mut batch = [terminal_event, post_event];
    let error = stamp_source_admissions_atomically(&mut batch, &sink).unwrap_err();
    assert!(error.to_string().contains("timestamp_received"));
    assert!(
        batch
            .iter()
            .all(|event| event.event.collector_admission.is_none())
    );
    assert_eq!(
        stats
            .polymarket_metadata_admission_sequence
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn polymarket_lifecycle_identity_contradictions_are_terminal() {
    let discovery = test_btc_discovery();
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let current_condition = discovery.current.condition_id.as_str();
    let current_slug = discovery.current.slug.as_str();
    let unrelated_condition = "0x0000000000000000000000000000000000000000000000000000000000000009";
    let new_market = |condition: &str, slug: &str, assets: Vec<&str>, outcomes: Vec<&str>| {
        serde_json::from_value::<WsMessage>(json!({
            "id": "market-current",
            "question": "Bitcoin Up or Down",
            "market": condition,
            "slug": slug,
            "description": "Description",
            "assets_ids": assets,
            "outcomes": outcomes,
            "timestamp": "1783392404603",
            "event_type": "new_market"
        }))
        .unwrap()
    };
    let resolved = |clob_market_id: &str,
                    condition: &str,
                    slug: Option<&str>,
                    assets: Vec<&str>,
                    outcomes: Vec<&str>,
                    winning_asset: &str,
                    winning_outcome: &str| {
        let WsMessage::MarketResolved(event) = serde_json::from_value::<WsMessage>(json!({
            "id": clob_market_id,
            "market": condition,
            "slug": slug,
            "assets_ids": assets,
            "outcomes": outcomes,
            "winning_asset_id": winning_asset,
            "winning_outcome": winning_outcome,
            "timestamp": "1783392404604",
            "event_type": "market_resolved"
        }))
        .unwrap() else {
            panic!("expected market_resolved message")
        };
        event
    };
    let assert_error = |message: WsMessage, expected: &str| {
        let error = match target_admitted_polymarket_message(&message, &discovery, &asset_to_slug) {
            Err(error) => error,
            Ok(_) => panic!("a target identity contradiction must be terminal"),
        };
        assert!(
            error.to_string().contains(expected),
            "unexpected identity error: {error}"
        );
    };
    let pending_identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let assert_resolved_error = |event: MarketResolved, expected: &str| {
        let error = match validate_polymarket_market_resolved_identity(&pending_identity, &event) {
            Err(error) => error,
            Ok(()) => panic!("a market_resolved identity contradiction must be terminal"),
        };
        assert!(
            error.to_string().contains(expected),
            "unexpected market_resolved identity error: {error}"
        );
    };

    for message in [
        new_market(
            current_condition,
            current_slug,
            vec!["999", "1000"],
            vec!["Up", "Down"],
        ),
        new_market(
            current_condition,
            current_slug,
            vec!["101", "102", "999"],
            vec!["Up", "Down", "Other"],
        ),
        new_market(
            current_condition,
            current_slug,
            vec!["101", "102", "102"],
            vec!["Up", "Down", "Down"],
        ),
    ] {
        assert_error(message, "contradictory asset mapping");
    }
    assert_error(
        new_market(
            unrelated_condition,
            current_slug,
            vec!["101", "102"],
            vec!["Up", "Down"],
        ),
        "unexpected condition_id",
    );
    assert_error(
        new_market(
            current_condition,
            "btc-updown-5m-wrong",
            vec!["101", "102"],
            vec!["Up", "Down"],
        ),
        "unexpected slug",
    );
    assert_error(
        new_market(
            current_condition,
            current_slug,
            vec!["101", "102"],
            vec!["Down", "Up"],
        ),
        "contradictory outcome mapping",
    );
    assert_error(
        serde_json::from_value::<WsMessage>(json!({
            "id": "market-wrong",
            "question": "Bitcoin Up or Down",
            "market": current_condition,
            "slug": current_slug,
            "description": "Description",
            "assets_ids": ["101", "102"],
            "outcomes": ["Up", "Down"],
            "timestamp": "1783392404603",
            "event_type": "new_market"
        }))
        .unwrap(),
        "unexpected market id",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            current_condition,
            None,
            vec!["101", "102", "999"],
            vec!["Up", "Down", "Other"],
            "101",
            "Up",
        ),
        "asset mapping mismatch",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            current_condition,
            None,
            vec!["101", "102"],
            vec!["Up", "Down"],
            "999",
            "Up",
        ),
        "winner outside fixed assets",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            current_condition,
            None,
            vec!["101", "102"],
            vec!["Up", "Down"],
            "101",
            "Down",
        ),
        "winning outcome mismatch",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            unrelated_condition,
            None,
            vec!["101", "102"],
            Vec::new(),
            "101",
            "Up",
        ),
        "condition id mismatch",
    );
    assert_resolved_error(
        resolved(
            "market-wrong",
            current_condition,
            None,
            vec!["101", "102"],
            Vec::new(),
            "101",
            "Up",
        ),
        "market id mismatch",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            current_condition,
            Some("btc-updown-5m-wrong"),
            vec!["101", "102"],
            Vec::new(),
            "101",
            "Up",
        ),
        "slug mismatch",
    );
    assert_resolved_error(
        resolved(
            "market-current",
            current_condition,
            None,
            vec!["101", "102"],
            vec!["Down", "Up"],
            "101",
            "Up",
        ),
        "outcome mapping mismatch",
    );
    validate_polymarket_market_resolved_identity(
        &pending_identity,
        &resolved(
            "market-current",
            current_condition,
            None,
            vec!["101", "102"],
            Vec::new(),
            "101",
            "Up",
        ),
    )
    .unwrap();
}

#[tokio::test]
async fn polymarket_invalid_market_resolved_fails_before_v3_admission() {
    let discovery = test_btc_discovery();
    let current = discovery.current.clone();
    let received_at_utc_ms = now_ms();
    let event = match serde_json::from_value::<WsMessage>(json!({
        "id": "market-wrong",
        "market": current.condition_id,
        "slug": current.slug,
        "assets_ids": [current.up_asset_id, current.down_asset_id],
        "outcomes": ["Up", "Down"],
        "winning_asset_id": "101",
        "winning_outcome": "Up",
        "timestamp": received_at_utc_ms.to_string(),
        "event_type": "market_resolved"
    }))
    .unwrap()
    {
        WsMessage::MarketResolved(event) => event,
        _ => panic!("expected market_resolved message"),
    };
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    install_active_raw_day_gate(&stats, received_at_utc_ms).await;
    let (tx, mut rx) = mpsc::channel(2);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let mut context = PolymarketProcessingContext::new(discovery);
    context.actor_now_utc_ms = received_at_utc_ms;
    let error = process_polymarket_market_resolved(
        &event,
        received_at_utc_ms,
        &mut context,
        &sink,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
        Instant::now(),
        accepted_raw_day_ticket_for_test(received_at_utc_ms).await,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("market id mismatch"));
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected)
    ));
}
