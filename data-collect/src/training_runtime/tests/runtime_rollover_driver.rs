use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundaryExitPollOrder {
    ExitFirst,
    CommitFirst,
}

struct BoundaryExitPollOrderResult {
    exit: PendingExit,
    old_manifest: Value,
    old_digest: String,
    next_manifest: Value,
}

async fn run_boundary_exit_poll_order(
    dataset_root: &Path,
    order: BoundaryExitPollOrder,
) -> BoundaryExitPollOrderResult {
    fs::create_dir_all(dataset_root).unwrap();
    let dataset_root = dataset_root.canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let expected_close = test_rollover_close(boundary_utc_ms);
    let (owner, old_day_dir) = test_manifest_owner(&dataset_root, &expected_close.utc_day);
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    let (activation_reply_tx, activation_reply_rx) = oneshot::channel();
    manifest_tx
        .send(ManifestCommand::Activate {
            utc_day: expected_close.utc_day.clone(),
            started_utc_ms: boundary_utc_ms - 10_000,
            generation: expected_close.generation,
            raw_day_gate: Arc::clone(&raw_day_gate),
            deadline: None,
            reply: activation_reply_tx,
            ack_mode: ManifestActivationAckMode::Send,
        })
        .await
        .unwrap();
    activation_reply_rx.await.unwrap().unwrap();
    assert_eq!(
        raw_day_gate
            .activate_or_stop_startup_after_manifest(
                expected_close.utc_day.clone(),
                expected_close.generation,
                None,
                StartupReleaseHandles {
                    writers: Vec::new(),
                    producers: Vec::new(),
                },
            )
            .await
            .unwrap(),
        StartupActivationOutcome::Released
    );

    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    runtime_stats
        .install_raw_day_gate(Arc::clone(&raw_day_gate))
        .unwrap();
    let provenance = test_artifact_identity();
    let mut writer_handles =
        start_writer_workers(&dataset_root, &provenance, &manifest_tx, &runtime_stats).unwrap();
    let mut batches = PendingIngressBatches::for_training_source_kinds();
    let run_started = Instant::now();
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    let close = raw_day_gate
        .prepare_rollover(&expected_close.utc_day, boundary_utc_ms)
        .unwrap();
    assert_eq!(close, expected_close);
    for producer in LOGICAL_PRODUCERS {
        raw_day_gate
            .register_source_fence(producer, &close)
            .unwrap();
    }
    assert_eq!(
        raw_day_gate.seal_prepared_rollover(&close).await.unwrap(),
        close
    );

    let (token_seen_tx, mut token_seen_rx) = mpsc::channel(LOGICAL_PRODUCERS.len());
    let mut handles = Vec::new();
    for (producer, source_kind) in [
        ("polymarket", "polymarket_market"),
        ("polymarket_rtds", "polymarket_rtds_chainlink_btc_usd"),
        ("binance_depth", "binance_depth_update"),
        ("binance_aggtrade", "binance_aggtrade"),
    ] {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_ingress_control(
            IngressControl::DayBarrier {
                producer: producer.to_owned(),
                close: close.clone(),
                reply: reply_tx,
            },
            &mut writer_handles,
            &mut batches,
            &source_gaps,
            &runtime_stats,
            &manifest_tx,
            &mut coordinator,
        )
        .await
        .unwrap();
        let task_close = close.clone();
        let task_token_seen_tx = token_seen_tx.clone();
        handles.push(SourceTaskHandle::spawn(
            source_kind,
            async move {
                let release = reply_rx.await.unwrap().unwrap();
                let stop = match release {
                    DayBarrierRelease::StopWithClose(actual) => {
                        assert_eq!(order, BoundaryExitPollOrder::ExitFirst);
                        actual
                    }
                    DayBarrierRelease::PrepareResume(mut token) => {
                        assert_eq!(order, BoundaryExitPollOrder::CommitFirst);
                        assert_eq!(token.producer, producer);
                        assert_eq!(token.close, task_close);
                        task_token_seen_tx.send(producer.to_owned()).await.unwrap();
                        loop {
                            let phase = token.phase_rx.borrow().clone();
                            match phase {
                                ProducerReleasePhase::Stop(actual) => break actual,
                                ProducerReleasePhase::Held => {
                                    token.phase_rx.changed().await.unwrap();
                                }
                                ProducerReleasePhase::FlushDeferred
                                | ProducerReleasePhase::ResumeSources => {
                                    panic!(
                                        "boundary exit must stop before producer release: producer={producer} phase={phase:?}"
                                    );
                                }
                            }
                        }
                    }
                    DayBarrierRelease::Resume => {
                        panic!("boundary exit must not resume producer={producer}");
                    }
                };
                assert_eq!(
                    stop,
                    RuntimeCloseIdentity::from_rollover_old_day(&task_close)
                );
                Err(CoordinatedSourceStop {
                    producer: producer.to_owned(),
                    close: stop,
                }
                .into())
            },
            SourceTaskObservationCell::default(),
            Arc::clone(&raw_day_gate),
        ));
    }
    drop(token_seen_tx);

    let ready = timeout(Duration::from_secs(5), rollover_ready_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.close, close);
    if order == BoundaryExitPollOrder::ExitFirst {
        let PreparedRuntimeExit::Rollover(exit) = raw_day_gate
            .prepare_runtime_exit_with_cause(boundary_utc_ms, RuntimeCloseCause::RunDeadline)
            .unwrap()
        else {
            panic!("boundary exit must retain rollover ownership");
        };
        assert_eq!(exit.close, close);
        let manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
        assert_eq!(manifest_state(&manifest).unwrap(), "open");
    }

    let (runtime_exit_tx, mut runtime_exit_rx) = mpsc::channel(1);
    let commit_first_trigger = if order == BoundaryExitPollOrder::CommitFirst {
        let trigger_day_dir = old_day_dir.clone();
        Some(tokio::spawn(async move {
            let mut seen = BTreeSet::new();
            for _ in LOGICAL_PRODUCERS {
                seen.insert(
                    timeout(Duration::from_secs(5), token_seen_rx.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            assert_eq!(
                seen,
                LOGICAL_PRODUCERS
                    .iter()
                    .map(|producer| (*producer).to_owned())
                    .collect()
            );
            let committed = load_day_manifest(&trigger_day_dir).unwrap().unwrap();
            assert_eq!(manifest_state(&committed).unwrap(), "closed");
            runtime_exit_tx
                .send(RuntimeExitRequest {
                    observed_utc_ms: boundary_utc_ms,
                    cause: RuntimeCloseCause::RunDeadline,
                })
                .await
                .unwrap();
        }))
    } else {
        None
    };
    let (_event_tx, mut event_rx) =
        bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY).unwrap();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let mut release_stop_issued = false;
    let drive_outcome = drive_normal_rollover(
        ready,
        &manifest_tx,
        &raw_day_gate,
        &mut runtime_exit_rx,
        &mut event_rx,
        &mut writer_handles,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &source_gaps,
        &runtime_stats,
        &mut coordinator,
        &mut release_stop_issued,
        None,
    )
    .await
    .unwrap();
    assert_eq!(drive_outcome, RolloverCommitOutcome::PendingExit);
    if let Some(trigger) = commit_first_trigger {
        trigger.await.unwrap();
    }
    let manifest_after_drive = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(
        manifest_state(&manifest_after_drive).unwrap(),
        if order == BoundaryExitPollOrder::ExitFirst {
            "open"
        } else {
            "closed"
        }
    );
    let PreparedRuntimeExit::Rollover(exit) = raw_day_gate.prepared_runtime_exit().unwrap() else {
        panic!("boundary exit must remain pending until reports are complete");
    };

    let mut completed_terminations = Vec::new();
    let mut completed_writer_outcome = None;
    let fallback_sinks = BTreeMap::new();
    let mut runtime_failure = None;
    let completed = complete_pending_exit_rollover(
        exit,
        &mut handles,
        &mut completed_terminations,
        &fallback_sinks,
        &mut event_rx,
        &mut rollover_ready_rx,
        &mut writer_handles,
        &mut completed_writer_outcome,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &source_gaps,
        &runtime_stats,
        run_started,
        &manifest_tx,
        &mut coordinator,
        &raw_day_gate,
        &mut runtime_failure,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!completed.unclean);
    assert!(runtime_failure.is_none(), "{runtime_failure:?}");
    assert!(source_gaps.lock().unwrap().is_empty());
    assert_eq!(completed_terminations.len(), LOGICAL_PRODUCERS.len());
    assert!(completed_terminations.iter().all(|termination| {
        termination.kind == SourceTaskTerminationKind::CoordinatedAbort
            && termination.error.is_none()
    }));
    let writer_outcome = completed_writer_outcome.unwrap();
    assert_eq!(writer_outcome.outputs.len(), training_source_kinds().len());
    assert!(
        writer_outcome.errors.is_empty(),
        "{:?}",
        writer_outcome.errors
    );
    assert!(writer_outcome.all_terminal, "{:?}", writer_outcome.errors);
    assert!(
        writer_outcome.all_close_proven,
        "{:?}",
        writer_outcome.errors
    );

    let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&old_manifest).unwrap(), "closed");
    assert_eq!(old_manifest["closure_reason"], "utc_day_barrier");
    assert_eq!(
        old_manifest["collection_day_coverage"]["state"],
        "ended_known"
    );
    assert_eq!(
        old_manifest["collection_day_coverage"]["ended_utc_ms"],
        boundary_utc_ms
    );
    let old_digest = manifest_file_digest(&old_day_dir);
    let next_day_dir =
        typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let next_manifest = load_day_manifest(&next_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&next_manifest).unwrap(), "open");
    assert_eq!(
        next_manifest["collection_day_coverage"]["state"],
        "not_started"
    );
    assert_eq!(
        next_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    owner_handle.abort();
    let _ = owner_handle.await;
    BoundaryExitPollOrderResult {
        exit: completed,
        old_manifest,
        old_digest,
        next_manifest,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reliable_boundary_close_has_one_manifest_digest_for_both_poll_orders() {
    let tmp = tempdir().unwrap();
    let exit_first = run_boundary_exit_poll_order(
        &tmp.path().join("exit-first"),
        BoundaryExitPollOrder::ExitFirst,
    )
    .await;
    let commit_first = run_boundary_exit_poll_order(
        &tmp.path().join("commit-first"),
        BoundaryExitPollOrder::CommitFirst,
    )
    .await;

    assert_eq!(exit_first.exit, commit_first.exit);
    assert_eq!(exit_first.old_manifest, commit_first.old_manifest);
    assert_eq!(exit_first.old_digest, commit_first.old_digest);
    assert_eq!(exit_first.next_manifest, commit_first.next_manifest);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterRendezvousPollOrder {
    CommonWriterPollFirst,
    ReadyReceiveFirst,
}

struct WriterRendezvousPollOrderResult {
    old_manifest: Value,
    old_digest: String,
    next_manifest: Value,
}

async fn run_writer_rendezvous_poll_order(
    dataset_root: &Path,
    order: WriterRendezvousPollOrder,
) -> WriterRendezvousPollOrderResult {
    fs::create_dir_all(dataset_root).unwrap();
    let dataset_root = dataset_root.canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(&close.utc_day, boundary_utc_ms - 10_000, close.generation)
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate
        .activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    assert_eq!(
        raw_day_gate
            .close_for_rollover(&close.utc_day, close.boundary_utc_ms)
            .await
            .unwrap(),
        close
    );

    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let mut producer_replies = Vec::new();
    for producer in LOGICAL_PRODUCERS {
        let (reply_tx, reply_rx) = oneshot::channel();
        coordinator
            .register_producer_barrier((*producer).to_owned(), close.clone(), reply_tx)
            .unwrap();
        producer_replies.push(reply_rx);
    }
    for producer in &LOGICAL_PRODUCERS[..LOGICAL_PRODUCERS.len() - 1] {
        manifest_tx
            .send(ManifestCommand::ProducerBarrier {
                close: close.clone(),
                producer: (*producer).to_owned(),
                gaps: Vec::new(),
                operational_counters: BTreeMap::new(),
            })
            .await
            .unwrap();
    }
    for source_kind in training_source_kinds() {
        manifest_tx
            .send(ManifestCommand::WriterDayFinalized {
                close: close.clone(),
                source_kind: (*source_kind).to_owned(),
                raw_day_gate: Arc::clone(&raw_day_gate),
            })
            .await
            .unwrap();
    }
    let final_producer = LOGICAL_PRODUCERS.last().unwrap();
    manifest_tx
        .send(ManifestCommand::ProducerBarrier {
            close: close.clone(),
            producer: (*final_producer).to_owned(),
            gaps: Vec::new(),
            operational_counters: BTreeMap::new(),
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(1), async {
        while raw_day_gate.writer_rendezvous_epoch().is_none() || rollover_ready_rx.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner must publish the complete rendezvous before ready delivery");
    let expected_post_fence_epoch = RuntimeEpoch {
        utc_day: close.next_utc_day.clone(),
        generation: close.next_generation,
    };
    assert_eq!(
        raw_day_gate.writer_rendezvous_epoch(),
        Some(expected_post_fence_epoch.clone())
    );

    let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(1);
    drop(writer_rx);
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    drop(result_tx);
    let mut writer_handles = BTreeMap::from([(
        "binance_aggtrade".to_owned(),
        SourceWriterHandle {
            schema_version: TypedParquetSegmentWriter::current_schema_version("binance_aggtrade")
                .unwrap()
                .to_owned(),
            tx: writer_tx,
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: Some(std::thread::spawn(|| {})),
        },
    )]);
    if order == WriterRendezvousPollOrder::CommonWriterPollFirst {
        let completion = poll_writer_completion(&mut writer_handles, &raw_day_gate, None)
            .expect("the common writer poll must observe the disconnected report channel");
        assert!(completion.worker_epoch.is_none());
        assert_eq!(completion.rendezvous_epoch, Some(expected_post_fence_epoch));
        let PreparedRuntimeExit::Rollover(exit) = raw_day_gate.prepared_runtime_exit().unwrap()
        else {
            panic!("writer terminal at the boundary must prepare a rollover exit");
        };
        assert!(!exit.unclean);
        assert_eq!(rollover_ready_rx.len(), 1);
    }
    let ready = rollover_ready_rx.recv().await.unwrap();

    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    runtime_stats
        .install_raw_day_gate(Arc::clone(&raw_day_gate))
        .unwrap();
    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let mut batches = PendingIngressBatches::for_training_source_kinds();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;

    let (_runtime_exit_tx, mut runtime_exit_rx) = mpsc::channel(1);
    let (_event_tx, mut event_rx) =
        bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY).unwrap();
    let mut release_stop_issued = false;
    assert_eq!(
        drive_normal_rollover(
            ready,
            &manifest_tx,
            &raw_day_gate,
            &mut runtime_exit_rx,
            &mut event_rx,
            &mut writer_handles,
            &mut batches,
            &mut coverage,
            &mut received_rows,
            &source_gaps,
            &runtime_stats,
            &mut coordinator,
            &mut release_stop_issued,
            None,
        )
        .await
        .unwrap(),
        RolloverCommitOutcome::PendingExit
    );
    assert!(!release_stop_issued);
    for reply in producer_replies {
        let DayBarrierRelease::StopWithClose(actual) = reply.await.unwrap().unwrap() else {
            panic!("post-rendezvous writer terminal must never resume a producer");
        };
        assert_eq!(actual, RuntimeCloseIdentity::from_rollover_old_day(&close));
    }
    let PreparedRuntimeExit::Rollover(exit) = raw_day_gate.prepared_runtime_exit().unwrap() else {
        panic!("writer terminal must remain a pending rollover exit");
    };
    assert!(!exit.unclean);
    assert_eq!(
        coordinator
            .finalize_pending_exit_after_reports(&exit, &manifest_tx)
            .await
            .unwrap(),
        DurableMutationAck::Confirmed
    );
    raw_day_gate.clear_writer_rendezvous(&close).unwrap();
    let writer_outcome =
        stop_writer_workers_with_budget(writer_handles, Duration::from_secs(1)).await;
    assert!(writer_outcome.all_terminal, "{:?}", writer_outcome.errors);

    let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(
        old_manifest["collection_day_coverage"]["state"],
        "ended_known"
    );
    assert_eq!(
        old_manifest["collection_day_coverage"]["ended_utc_ms"],
        boundary_utc_ms
    );
    let old_digest = manifest_file_digest(&old_day_dir);
    let next_day_dir =
        typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let next_manifest = load_day_manifest(&next_day_dir).unwrap().unwrap();
    assert_eq!(
        next_manifest["collection_day_coverage"]["state"],
        "not_started"
    );
    assert_eq!(
        next_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    drop(manifest_tx);
    owner_handle.await.unwrap().unwrap();
    WriterRendezvousPollOrderResult {
        old_manifest,
        old_digest,
        next_manifest,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_terminal_after_owner_rendezvous_has_one_digest_for_both_poll_orders() {
    let tmp = tempdir().unwrap();
    let writer_first = run_writer_rendezvous_poll_order(
        &tmp.path().join("writer-first"),
        WriterRendezvousPollOrder::CommonWriterPollFirst,
    )
    .await;
    let ready_first = run_writer_rendezvous_poll_order(
        &tmp.path().join("ready-first"),
        WriterRendezvousPollOrder::ReadyReceiveFirst,
    )
    .await;

    assert_eq!(writer_first.old_manifest, ready_first.old_manifest);
    assert_eq!(writer_first.old_digest, ready_first.old_digest);
    assert_eq!(writer_first.next_manifest, ready_first.next_manifest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_exit_synthetic_barriers_receive_exact_stop_and_finalize_cleanly() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(&close.utc_day, boundary_utc_ms - 1_000, close.generation)
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));

    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate
        .activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    let PreparedRuntimeExit::Rollover(exit) = raw_day_gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms, RuntimeCloseCause::RunDeadline)
        .unwrap()
    else {
        panic!("a boundary deadline must create PendingExit");
    };
    assert_eq!(exit.close, close);

    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    runtime_stats
        .install_raw_day_gate(Arc::clone(&raw_day_gate))
        .unwrap();
    let (event_tx, mut event_rx) =
        bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY).unwrap();
    let event_sink = SourceEventSink {
        tx: event_tx,
        gaps: Arc::clone(&source_gaps),
        source_queue_backpressure: Arc::clone(&runtime_stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let fallback_sinks = BTreeMap::from([
        ("polymarket", event_sink.fork_logical_producer()),
        ("polymarket_rtds", event_sink.fork_logical_producer()),
        ("binance_depth", event_sink.fork_logical_producer()),
        ("binance_aggtrade", event_sink.fork_logical_producer()),
    ]);
    drop(event_sink);

    let source_close = RuntimeCloseIdentity::from_rollover_old_day(&close);
    let mut handles = Vec::new();
    for source_kind in [
        "polymarket_market",
        "polymarket_rtds_chainlink_btc_usd",
        "binance_depth_update",
        "binance_aggtrade",
    ] {
        let producer = logical_producer_for_source_task(source_kind)
            .unwrap()
            .to_owned();
        let stop_close = source_close.clone();
        handles.push(SourceTaskHandle::spawn(
            source_kind,
            async move {
                Err(CoordinatedSourceStop {
                    producer,
                    close: stop_close,
                }
                .into())
            },
            SourceTaskObservationCell::default(),
            Arc::clone(&raw_day_gate),
        ));
    }

    let provenance = test_artifact_identity();
    let mut writer_handles =
        start_writer_workers(&dataset_root, &provenance, &manifest_tx, &runtime_stats).unwrap();
    let mut completed_terminations = Vec::new();
    let mut completed_writer_outcome = None;
    let mut batches = PendingIngressBatches::for_training_source_kinds();
    let mut coverage = BTreeMap::new();
    let mut received_rows = 0;
    let run_started = Instant::now();
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let mut runtime_failure = None;

    let completed = complete_pending_exit_rollover(
        exit,
        &mut handles,
        &mut completed_terminations,
        &fallback_sinks,
        &mut event_rx,
        &mut rollover_ready_rx,
        &mut writer_handles,
        &mut completed_writer_outcome,
        &mut batches,
        &mut coverage,
        &mut received_rows,
        &source_gaps,
        &runtime_stats,
        run_started,
        &manifest_tx,
        &mut coordinator,
        &raw_day_gate,
        &mut runtime_failure,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!completed.unclean);
    assert!(runtime_failure.is_none(), "{runtime_failure:?}");
    assert!(source_gaps.lock().unwrap().is_empty());
    assert_eq!(completed_terminations.len(), LOGICAL_PRODUCERS.len());
    assert!(completed_terminations.iter().all(|termination| {
        termination.kind == SourceTaskTerminationKind::CoordinatedAbort
            && termination.error.is_none()
    }));
    let writer_outcome = completed_writer_outcome.unwrap();
    assert!(writer_outcome.all_terminal, "{:?}", writer_outcome.errors);
    assert!(
        writer_outcome.all_close_proven,
        "{:?}",
        writer_outcome.errors
    );

    let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(
        old_manifest["collection_day_coverage"]["state"],
        "ended_known"
    );
    let next_day_dir =
        typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let next_manifest = load_day_manifest(&next_day_dir).unwrap().unwrap();
    assert_eq!(
        next_manifest["collection_day_coverage"]["state"],
        "not_started"
    );
    assert_eq!(
        next_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    owner_handle.abort();
    let _ = owner_handle.await;
}

#[test]
fn precompleted_rollover_writer_failure_is_recorded_once_after_source_primary() {
    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    let primary = "source_task_error:polymarket_market:receiver lagged by 1364 messages".to_owned();
    record_gap(&source_gaps, primary.clone());
    let mut runtime_failure = None;
    promote_runtime_primary(&mut runtime_failure, primary.clone());
    let outcome = WriterStopOutcome {
        outputs: Vec::new(),
        errors: vec!["writer returned a reliable terminal error".to_owned()],
        all_terminal: true,
        all_close_proven: true,
    };

    record_writer_stop_failure(&outcome, &source_gaps, &mut runtime_failure);
    record_writer_stop_failure(&outcome, &source_gaps, &mut runtime_failure);
    let before_root_unpack = runtime_failure.clone();
    let (outputs, all_terminal, all_close_proven) = take_writer_stop_outcome(outcome);

    assert!(outputs.is_empty());
    assert!(all_terminal);
    assert!(all_close_proven);
    assert_eq!(runtime_failure, before_root_unpack);
    assert_eq!(
        runtime_failure.as_deref(),
        Some(
            "source_task_error:polymarket_market:receiver lagged by 1364 messages; \
             shutdown_secondary:writer_finalize_failed:writer returned a reliable terminal error"
        )
    );
    assert_eq!(
        runtime_failure
            .as_deref()
            .unwrap()
            .matches("shutdown_secondary:")
            .count(),
        1
    );
    assert_eq!(
        source_gaps
            .lock()
            .expect("source gap mutex poisoned")
            .all_reasons(),
        vec![
            primary,
            "writer_finalize_failed:writer returned a reliable terminal error".to_owned()
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_activation_exit_waits_for_all_flushes_then_stops_exact_next_generation() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, _) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(&close.utc_day, boundary_utc_ms - 1_000, close.generation)
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (rollover_ready_tx, mut rollover_ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, rollover_ready_tx));
    let raw_day_gate = Arc::new(RawDayGate::new());
    raw_day_gate
        .activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    assert_eq!(
        raw_day_gate
            .close_for_rollover(&close.utc_day, close.boundary_utc_ms)
            .await
            .unwrap(),
        close
    );

    let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    record_gap_for_day(&source_gaps, &close.utc_day, "old_day_gap".to_owned());
    record_gap_for_day(&source_gaps, &close.next_utc_day, "next_day_gap".to_owned());
    let runtime_stats = Arc::new(SourceQueueBackpressureStats::default());
    runtime_stats
        .install_raw_day_gate(Arc::clone(&raw_day_gate))
        .unwrap();
    let provenance = test_artifact_identity();
    let mut writer_handles =
        start_writer_workers(&dataset_root, &provenance, &manifest_tx, &runtime_stats).unwrap();
    let mut batches = PendingIngressBatches::for_training_source_kinds();

    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);

    let (flush_seen_tx, mut flush_seen_rx) = mpsc::channel(LOGICAL_PRODUCERS.len());
    let (allow_flush_tx, allow_flush_rx) = watch::channel(false);
    let (final_phase_tx, mut final_phase_rx) = mpsc::channel(LOGICAL_PRODUCERS.len());
    let mut producer_tasks = Vec::new();
    for producer in LOGICAL_PRODUCERS {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_ingress_control(
            IngressControl::DayBarrier {
                producer: (*producer).to_owned(),
                close: close.clone(),
                reply: reply_tx,
            },
            &mut writer_handles,
            &mut batches,
            &source_gaps,
            &runtime_stats,
            &manifest_tx,
            &mut coordinator,
        )
        .await
        .unwrap();
        let producer = (*producer).to_owned();
        let flush_seen_tx = flush_seen_tx.clone();
        let mut allow_flush_rx = allow_flush_rx.clone();
        let final_phase_tx = final_phase_tx.clone();
        producer_tasks.push(tokio::spawn(async move {
            let DayBarrierRelease::PrepareResume(mut token) = reply_rx.await.unwrap().unwrap()
            else {
                panic!("normal rollover must issue a staged release token");
            };
            token
                .ack_tx
                .send(ProducerReleaseAck {
                    producer: token.producer.clone(),
                    close: RuntimeCloseIdentity::from_rollover_old_day(&token.close),
                    kind: ProducerReleaseAckKind::Armed,
                    error: None,
                })
                .await
                .unwrap();
            loop {
                let phase = token.phase_rx.borrow().clone();
                if phase == ProducerReleasePhase::FlushDeferred {
                    break;
                }
                token.phase_rx.changed().await.unwrap();
            }
            flush_seen_tx.send(()).await.unwrap();
            while !*allow_flush_rx.borrow() {
                allow_flush_rx.changed().await.unwrap();
            }
            token
                .ack_tx
                .send(ProducerReleaseAck {
                    producer: token.producer.clone(),
                    close: RuntimeCloseIdentity::from_rollover_old_day(&token.close),
                    kind: ProducerReleaseAckKind::Flushed,
                    error: None,
                })
                .await
                .unwrap();
            loop {
                let phase = token.phase_rx.borrow().clone();
                if matches!(
                    phase,
                    ProducerReleasePhase::Stop(_) | ProducerReleasePhase::ResumeSources
                ) {
                    final_phase_tx.send((producer, phase)).await.unwrap();
                    break;
                }
                token.phase_rx.changed().await.unwrap();
            }
        }));
    }
    drop(flush_seen_tx);
    drop(final_phase_tx);
    assert_eq!(
        source_gaps.lock().unwrap().reasons_for_day(&close.utc_day),
        vec!["old_day_gap"]
    );
    let ready = timeout(Duration::from_secs(2), rollover_ready_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (runtime_exit_tx, mut runtime_exit_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) =
        bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY).unwrap();
    let drive_manifest_tx = manifest_tx.clone();
    let drive_gate = Arc::clone(&raw_day_gate);
    let drive_gaps = Arc::clone(&source_gaps);
    let drive_stats = Arc::clone(&runtime_stats);
    let (drive_done_tx, mut drive_done_rx) = oneshot::channel();
    let drive = async move {
        let mut coverage = BTreeMap::new();
        let mut received_rows = 0;
        let mut release_stop_issued = false;
        let outcome = drive_normal_rollover(
            ready,
            &drive_manifest_tx,
            &drive_gate,
            &mut runtime_exit_rx,
            &mut event_rx,
            &mut writer_handles,
            &mut batches,
            &mut coverage,
            &mut received_rows,
            &drive_gaps,
            &drive_stats,
            &mut coordinator,
            &mut release_stop_issued,
            None,
        )
        .await;
        let _ = drive_done_tx.send(());
        (outcome, release_stop_issued, writer_handles)
    };
    let expected_stop_day = close.next_utc_day.clone();
    let expected_stop_generation = close.next_generation;
    let orchestrator = async move {
        for _ in LOGICAL_PRODUCERS {
            timeout(Duration::from_secs(2), flush_seen_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        runtime_exit_tx
            .send(RuntimeExitRequest {
                observed_utc_ms: boundary_utc_ms + 1,
                cause: RuntimeCloseCause::OperatorStop,
            })
            .await
            .unwrap();
        sleep(Duration::from_millis(25)).await;
        assert!(matches!(
            drive_done_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        allow_flush_tx.send_replace(true);

        let mut stopped = BTreeSet::new();
        let mut observed_stop = None;
        for _ in LOGICAL_PRODUCERS {
            let (producer, phase) = timeout(Duration::from_secs(2), final_phase_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let ProducerReleasePhase::Stop(actual) = phase else {
                panic!("post-activation exit must never resume a producer");
            };
            assert_eq!(actual.utc_day, expected_stop_day);
            assert_eq!(actual.generation, expected_stop_generation);
            if let Some(expected) = observed_stop.as_ref() {
                assert_eq!(&actual, expected);
            } else {
                observed_stop = Some(actual);
            }
            stopped.insert(producer);
        }
        assert_eq!(
            stopped,
            LOGICAL_PRODUCERS
                .iter()
                .map(|producer| (*producer).to_owned())
                .collect()
        );
        for task in producer_tasks {
            task.await.unwrap();
        }
        drop(event_tx);
        observed_stop.unwrap()
    };

    let ((outcome, release_stop_issued, writer_handles), observed_stop) =
        tokio::join!(drive, orchestrator);
    assert_eq!(outcome.unwrap(), RolloverCommitOutcome::ActivatedStopped);
    assert!(release_stop_issued);
    let PreparedRuntimeExit::SameDay(prepared_close) =
        raw_day_gate.prepared_runtime_exit().unwrap()
    else {
        panic!("post-activation exit must belong to the new generation");
    };
    assert_eq!(prepared_close.identity, observed_stop);
    assert!(matches!(
        raw_day_gate.current_state(),
        RawDayGateState::Closing(ref actual) if actual == &close
    ));
    let gaps = source_gaps.lock().unwrap();
    assert!(gaps.reasons_for_day(&close.utc_day).is_empty());
    assert_eq!(
        gaps.reasons_for_day(&close.next_utc_day),
        vec!["next_day_gap"]
    );
    assert_eq!(gaps.all_reasons(), vec!["old_day_gap", "next_day_gap"]);
    drop(gaps);
    let writer_outcome = shutdown_writer_workers_with_budget(
        writer_handles,
        prepared_close.identity.clone(),
        Duration::from_secs(2),
    )
    .await;
    assert!(writer_outcome.all_terminal, "{:?}", writer_outcome.errors);
    assert!(
        writer_outcome.all_close_proven,
        "{:?}",
        writer_outcome.errors
    );

    let new_day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let new_manifest = load_day_manifest(&new_day_dir).unwrap().unwrap();
    assert_eq!(new_manifest["collection_day_coverage"]["state"], "active");
    assert_eq!(
        new_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    manifest_tx
        .send(ManifestCommand::Shutdown {
            close: prepared_close.identity,
            disposition: ManifestShutdownDisposition::EndedKnown {
                ended_utc_ms: prepared_close.ended_utc_ms,
            },
            gaps: Vec::new(),
            operational_counters: BTreeMap::new(),
            reply: shutdown_tx,
        })
        .await
        .unwrap();
    shutdown_rx.await.unwrap().unwrap();
    owner_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn old_close_dropped_reply_retains_day_gaps_and_owner_material() {
    let tmp = tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let close = test_rollover_close(1_783_468_800_000);
    let (mut owner, day_dir) = test_manifest_owner(&root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            close.boundary_utc_ms - 1_000,
            close.generation,
        )
        .unwrap();
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    gate.close_for_rollover(&close.utc_day, close.boundary_utc_ms)
        .await
        .unwrap();
    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let mut receivers = Vec::new();
    for producer in LOGICAL_PRODUCERS {
        let (tx, rx) = oneshot::channel();
        receivers.push(rx);
        coordinator
            .register_producer_barrier((*producer).to_owned(), close.clone(), tx)
            .unwrap();
        owner
            .record_producer_barrier(
                close.clone(),
                (*producer).to_owned(),
                &["retained_gap".to_owned()],
                &BTreeMap::new(),
            )
            .unwrap();
    }
    let mut ready = None;
    for source in training_source_kinds() {
        ready = owner
            .record_writer_ack(close.clone(), (*source).to_owned(), Arc::clone(&gate))
            .unwrap();
    }
    gate.publish_writer_rendezvous(&close).unwrap();
    let (manifest_tx, mut manifest_rx) = mpsc::channel(1);
    let owner_task = tokio::spawn(async move {
        let ManifestCommand::CommitRolloverOldDay {
            close,
            mode,
            normal_gate,
            deadline,
            reply,
        } = manifest_rx.recv().await.unwrap()
        else {
            panic!("old-day commit")
        };
        normal_gate
            .unwrap()
            .commit_old_day_if_no_exit(&close, deadline, || {
                owner.commit_rollover_old_day(&close, mode)
            })
            .unwrap();
        drop(reply); // The durable mutation completed, but its acknowledgment was lost.
        owner
    });
    let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
    record_gap_for_day(&gaps, &close.utc_day, "retained_gap".to_owned());
    let (_exit_tx, mut exit_rx) = mpsc::channel(1);
    let (_event_tx, mut event_rx) = mpsc::channel(1);
    let error = drive_normal_rollover(
        ready.unwrap(),
        &manifest_tx,
        &gate,
        &mut exit_rx,
        &mut event_rx,
        &mut BTreeMap::new(),
        &mut PendingIngressBatches::default(),
        &mut BTreeMap::new(),
        &mut 0,
        &gaps,
        &SourceQueueBackpressureStats::default(),
        &mut coordinator,
        &mut false,
        None,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("reply dropped"), "{error}");
    assert_eq!(
        coordinator.cutpoint(),
        Some(RolloverCutpoint::OutcomeUncertain)
    );
    assert_eq!(
        gaps.lock().unwrap().reasons_for_day(&close.utc_day),
        vec!["retained_gap"]
    );
    assert_eq!(owner_task.await.unwrap().retained_day_counts(), (1, 1));
    assert_eq!(
        manifest_state(&load_day_manifest(&day_dir).unwrap().unwrap()).unwrap(),
        "closed"
    );
}
