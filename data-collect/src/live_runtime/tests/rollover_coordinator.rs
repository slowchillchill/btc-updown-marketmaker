use super::*;

fn test_release_controller(
    close: RawDayClose,
) -> (
    ProducerReleaseController,
    watch::Receiver<ProducerReleasePhase>,
) {
    let (phase_tx, phase_rx) = watch::channel(ProducerReleasePhase::Held);
    let (_ack_tx, ack_rx) = mpsc::channel(LOGICAL_PRODUCERS.len() * 2);
    (
        ProducerReleaseController {
            close,
            phase: ProducerReleasePhase::Held,
            phase_tx,
            ack_rx,
            armed: BTreeSet::new(),
            flushed: BTreeSet::new(),
        },
        phase_rx,
    )
}

#[test]
fn producer_release_ack_requires_exact_identity_phase_and_uniqueness() {
    let close = test_rollover_close(1_783_468_800_000);
    let expected = RuntimeCloseIdentity::from_rollover_old_day(&close);
    let (mut release, _phase_rx) = test_release_controller(close.clone());

    let wrong_identity = ProducerReleaseAck {
        producer: LOGICAL_PRODUCERS[0].to_owned(),
        close: RuntimeCloseIdentity {
            close_id: expected.close_id.saturating_add(1),
            ..expected.clone()
        },
        kind: ProducerReleaseAckKind::Armed,
        error: None,
    };
    assert!(
        release
            .accept_ack(wrong_identity)
            .unwrap_err()
            .to_string()
            .contains("identity mismatch")
    );
    assert!(release.armed.is_empty());

    let unknown_producer = ProducerReleaseAck {
        producer: "unknown_producer".to_owned(),
        close: expected.clone(),
        kind: ProducerReleaseAckKind::Armed,
        error: None,
    };
    assert!(
        release
            .accept_ack(unknown_producer)
            .unwrap_err()
            .to_string()
            .contains("identity mismatch")
    );
    assert!(release.armed.is_empty());

    let wrong_phase = ProducerReleaseAck {
        producer: LOGICAL_PRODUCERS[0].to_owned(),
        close: expected.clone(),
        kind: ProducerReleaseAckKind::Flushed,
        error: None,
    };
    assert!(
        release
            .accept_ack(wrong_phase)
            .unwrap_err()
            .to_string()
            .contains("invalid phase")
    );
    assert!(release.flushed.is_empty());

    let armed = || ProducerReleaseAck {
        producer: LOGICAL_PRODUCERS[0].to_owned(),
        close: expected.clone(),
        kind: ProducerReleaseAckKind::Armed,
        error: None,
    };
    release.accept_ack(armed()).unwrap();
    assert!(
        release
            .accept_ack(armed())
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );

    release.set_phase(ProducerReleasePhase::FlushDeferred);
    let flushed = || ProducerReleaseAck {
        producer: LOGICAL_PRODUCERS[0].to_owned(),
        close: expected.clone(),
        kind: ProducerReleaseAckKind::Flushed,
        error: None,
    };
    release.accept_ack(flushed()).unwrap();
    assert!(
        release
            .accept_ack(flushed())
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );

    let unarmed_flush = ProducerReleaseAck {
        producer: LOGICAL_PRODUCERS[1].to_owned(),
        close: expected,
        kind: ProducerReleaseAckKind::Flushed,
        error: None,
    };
    assert!(
        release
            .accept_ack(unarmed_flush)
            .unwrap_err()
            .to_string()
            .contains("invalid phase")
    );
}

#[test]
fn controller_drop_preserves_an_explicit_next_generation_stop_identity() {
    let close = test_rollover_close(1_783_468_800_000);
    let next_generation = RuntimeCloseIdentity::from_rollover_next_day(&close);
    let (mut release, phase_rx) = test_release_controller(close);

    release.stop_with_close(next_generation.clone());
    drop(release);

    assert_eq!(
        phase_rx.borrow().clone(),
        ProducerReleasePhase::Stop(next_generation)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_rollover_with_dropped_ack_does_not_reopen_raw_gate() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            boundary_utc_ms - 1_000,
            INITIAL_RAW_DAY_GENERATION,
        )
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (ready_tx, mut ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, ready_tx));
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    let gate_close = gate
        .close_for_rollover(&close.utc_day, close.boundary_utc_ms)
        .await
        .unwrap();
    assert_eq!(gate_close, close);

    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Drop);
    let mut producer_receivers = Vec::new();
    for producer in LOGICAL_PRODUCERS {
        let (reply_tx, reply_rx) = oneshot::channel();
        coordinator
            .register_producer_barrier((*producer).to_owned(), close.clone(), reply_tx)
            .unwrap();
        producer_receivers.push(reply_rx);
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
    for source_kind in LIVE_SOURCES {
        manifest_tx
            .send(ManifestCommand::WriterDayFinalized {
                close: close.clone(),
                source_kind: (*source_kind).to_owned(),
                raw_day_gate: Arc::clone(&gate),
            })
            .await
            .unwrap();
    }
    let ready = timeout(Duration::from_secs(1), ready_rx.recv())
        .await
        .unwrap()
        .unwrap();
    coordinator.latch_ready(ready).unwrap();
    assert_eq!(
        coordinator
            .commit_old_day_once(
                ManifestRolloverCommitMode::Normal,
                &manifest_tx,
                Some(Arc::clone(&gate)),
                None,
            )
            .await
            .unwrap(),
        OldDayCommitAck::Confirmed
    );
    let mut release = coordinator.prepare_release_controller().unwrap();
    let mut phase_receivers = Vec::new();
    for receiver in producer_receivers {
        let DayBarrierRelease::PrepareResume(token) = receiver.await.unwrap().unwrap() else {
            panic!("producer must receive a two-stage release token");
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
        phase_receivers.push(token.phase_rx);
    }
    while !release.all_armed() {
        let ack = release.ack_rx.recv().await.unwrap();
        release.accept_ack(ack).unwrap();
    }
    coordinator.mark_tokens_armed().unwrap();
    let ActivationMutationAck::OutcomeUncertain(error) = coordinator
        .activate_new_day_once(&manifest_tx, Arc::clone(&gate), None)
        .await
        .unwrap()
    else {
        panic!("dropped activation ack must be outcome-uncertain");
    };
    assert!(error.contains("activation reply dropped"));
    release.stop();
    assert!(matches!(gate.current_state(), RawDayGateState::Closing(_)));
    for receiver in phase_receivers {
        assert_eq!(
            receiver.borrow().clone(),
            ProducerReleasePhase::Stop(RuntimeCloseIdentity::from_rollover_old_day(&close))
        );
    }

    let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(manifest_state(&old_manifest).unwrap(), "closed");
    assert_eq!(
        old_manifest["collection_day_coverage"]["state"],
        "ended_known"
    );
    let new_day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let new_manifest = load_day_manifest(&new_day_dir).unwrap().unwrap();
    assert_eq!(new_manifest["collection_day_coverage"]["state"], "active");
    assert_eq!(
        new_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    owner_handle.abort();
    let _ = owner_handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activation_prepared_exit_wins_without_new_day_activation_or_resume() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().canonicalize().unwrap();
    let boundary_utc_ms = 1_783_468_800_000;
    let close = test_rollover_close(boundary_utc_ms);
    let (mut owner, old_day_dir) = test_manifest_owner(&dataset_root, &close.utc_day);
    owner
        .activate_initial(
            &close.utc_day,
            boundary_utc_ms - 1_000,
            INITIAL_RAW_DAY_GENERATION,
        )
        .unwrap();
    let (manifest_tx, manifest_rx) = mpsc::channel(MANIFEST_CHANNEL_CAPACITY);
    let (ready_tx, mut ready_rx) = mpsc::channel(1);
    let owner_handle = tokio::spawn(run_manifest_owner(owner, manifest_rx, ready_tx));
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(close.utc_day.clone(), close.generation)
        .await
        .unwrap();
    assert_eq!(
        gate.close_for_rollover(&close.utc_day, close.boundary_utc_ms)
            .await
            .unwrap(),
        close
    );

    let mut coordinator = RolloverCoordinatorState::new(ManifestActivationAckMode::Send);
    let mut producer_receivers = Vec::new();
    for producer in LOGICAL_PRODUCERS {
        let (reply_tx, reply_rx) = oneshot::channel();
        coordinator
            .register_producer_barrier((*producer).to_owned(), close.clone(), reply_tx)
            .unwrap();
        producer_receivers.push(reply_rx);
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
    for source_kind in LIVE_SOURCES {
        manifest_tx
            .send(ManifestCommand::WriterDayFinalized {
                close: close.clone(),
                source_kind: (*source_kind).to_owned(),
                raw_day_gate: Arc::clone(&gate),
            })
            .await
            .unwrap();
    }
    coordinator
        .latch_ready(
            timeout(Duration::from_secs(1), ready_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        coordinator
            .commit_old_day_once(
                ManifestRolloverCommitMode::Normal,
                &manifest_tx,
                Some(Arc::clone(&gate)),
                None,
            )
            .await
            .unwrap(),
        OldDayCommitAck::Confirmed
    );
    let mut release = coordinator.prepare_release_controller().unwrap();
    let mut phase_receivers = Vec::new();
    for receiver in producer_receivers {
        let DayBarrierRelease::PrepareResume(token) = receiver.await.unwrap().unwrap() else {
            panic!("producer must receive a two-stage release token");
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
        phase_receivers.push(token.phase_rx);
    }
    while !release.all_armed() {
        let ack = release.ack_rx.recv().await.unwrap();
        release.accept_ack(ack).unwrap();
    }
    coordinator.mark_tokens_armed().unwrap();

    let PreparedRuntimeExit::Rollover(exit) = gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms + 1, RuntimeCloseCause::OperatorStop)
        .unwrap()
    else {
        panic!("an exit in the closed-old/new-unactivated window must remain PendingExit");
    };
    assert_eq!(
        coordinator
            .activate_new_day_once(&manifest_tx, Arc::clone(&gate), None)
            .await
            .unwrap(),
        ActivationMutationAck::PreparedExitWon
    );
    release.stop();
    for receiver in phase_receivers {
        assert_eq!(
            receiver.borrow().clone(),
            ProducerReleasePhase::Stop(RuntimeCloseIdentity::from_rollover_old_day(&close))
        );
    }
    assert_eq!(
        coordinator
            .finalize_pending_exit_after_reports(&exit, &manifest_tx)
            .await
            .unwrap(),
        DurableMutationAck::Confirmed
    );

    let old_manifest = load_day_manifest(&old_day_dir).unwrap().unwrap();
    assert_eq!(
        old_manifest["collection_day_coverage"]["state"],
        "ended_known"
    );
    let new_day_dir = typed_raw_root(&dataset_root).join(format!("utc_day={}", close.next_utc_day));
    let new_manifest = load_day_manifest(&new_day_dir).unwrap().unwrap();
    assert_eq!(
        new_manifest["collection_day_coverage"]["state"],
        "not_started"
    );
    assert_eq!(
        new_manifest["collection_day_coverage"]["reactivated_within_day"],
        false
    );

    owner_handle.abort();
    let _ = owner_handle.await;
}
