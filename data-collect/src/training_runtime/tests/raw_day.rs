use super::*;

#[tokio::test]
async fn runtime_exit_candidate_assigns_b_minus_one_b_and_b_plus_one_causally() {
    let boundary_utc_ms = 1_783_468_800_000;
    let old_day = "2026-07-07";

    let before_gate = RawDayGate::new();
    before_gate
        .activate_startup(old_day.to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let before = before_gate
        .prepare_runtime_exit(boundary_utc_ms - 1)
        .unwrap();
    let PreparedRuntimeExit::SameDay(before) = before else {
        panic!("B-1ms must remain a same-day close");
    };
    assert_eq!(before.ended_utc_ms, boundary_utc_ms - 1);
    assert!(
        before_gate
            .prepare_rollover(old_day, boundary_utc_ms)
            .unwrap_err()
            .to_string()
            .contains("prepared same-day close")
    );

    let boundary_gate = RawDayGate::new();
    boundary_gate
        .activate_startup(old_day.to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let at_boundary = boundary_gate.prepare_runtime_exit(boundary_utc_ms).unwrap();
    let PreparedRuntimeExit::Rollover(at_boundary) = at_boundary else {
        panic!("B must create PendingExit");
    };
    assert_eq!(at_boundary.observed_utc_ms, boundary_utc_ms);
    assert_eq!(at_boundary.close.boundary_utc_ms, boundary_utc_ms);
    let later = boundary_gate
        .prepare_runtime_exit(boundary_utc_ms + 1)
        .unwrap();
    assert_eq!(later, PreparedRuntimeExit::Rollover(at_boundary.clone()));

    let identity = RuntimeCloseIdentity::from_rollover_old_day(&at_boundary.close);
    let marked = boundary_gate.mark_runtime_exit_unclean(&identity).unwrap();
    let PreparedRuntimeExit::Rollover(marked) = marked else {
        panic!("unclean must preserve PendingExit identity");
    };
    assert!(marked.unclean);
    assert_eq!(marked.observed_utc_ms, boundary_utc_ms);
}

#[tokio::test]
async fn installed_deadline_wins_over_later_source_return_in_both_poll_orders() {
    let boundary_utc_ms = 1_783_468_800_000;
    let deadline_utc_ms = boundary_utc_ms - 1_000;
    let reached = RuntimeDeadlineCandidate {
        deadline: Instant::now() - Duration::from_millis(1),
        observed_utc_ms: deadline_utc_ms,
    };

    let source_first = RawDayGate::new();
    source_first
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    source_first
        .install_runtime_deadline(Some(reached))
        .unwrap();
    source_first.observe_source_terminal_now(
        RuntimeCloseCause::SourceInnerReturn {
            source_kind: "binance_aggtrade",
        },
        None,
        None,
    );

    let notifier_first = RawDayGate::new();
    notifier_first
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    notifier_first
        .install_runtime_deadline(Some(reached))
        .unwrap();
    notifier_first
        .prepare_runtime_exit_with_cause(deadline_utc_ms, RuntimeCloseCause::RunDeadline)
        .unwrap();
    notifier_first.observe_source_terminal_now(
        RuntimeCloseCause::SourceInnerReturn {
            source_kind: "binance_aggtrade",
        },
        None,
        None,
    );

    let source_first_close = source_first.prepared_runtime_exit().unwrap();
    let notifier_first_close = notifier_first.prepared_runtime_exit().unwrap();
    assert_eq!(source_first_close, notifier_first_close);
    let PreparedRuntimeExit::SameDay(close) = source_first_close else {
        panic!("an in-day deadline must prepare a same-day close");
    };
    assert_eq!(close.ended_utc_ms, deadline_utc_ms);
    assert_eq!(close.cause, RuntimeCloseCause::RunDeadline);

    let boundary_source_first = RawDayGate::new();
    boundary_source_first
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    boundary_source_first
        .install_runtime_deadline(Some(RuntimeDeadlineCandidate {
            observed_utc_ms: boundary_utc_ms,
            ..reached
        }))
        .unwrap();
    boundary_source_first.observe_source_terminal_now(
        RuntimeCloseCause::SourceInnerReturn {
            source_kind: "binance_aggtrade",
        },
        None,
        None,
    );

    let boundary_notifier_first = RawDayGate::new();
    boundary_notifier_first
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    boundary_notifier_first
        .prepare_runtime_exit_with_cause(boundary_utc_ms, RuntimeCloseCause::RunDeadline)
        .unwrap();
    assert_eq!(
        boundary_source_first.prepared_runtime_exit(),
        boundary_notifier_first.prepared_runtime_exit()
    );
}

#[tokio::test]
async fn delayed_deadline_is_materialized_before_every_activation_or_release_cutpoint() {
    let boundary_utc_ms = 1_783_468_800_000;
    let reached = RuntimeDeadlineCandidate {
        deadline: Instant::now() - Duration::from_millis(1),
        observed_utc_ms: boundary_utc_ms,
    };

    let startup_gate = RawDayGate::new();
    let startup_mutations = AtomicU64::new(0);
    let startup_error = startup_gate
        .commit_startup_manifest_activation(
            "2026-07-08",
            INITIAL_RAW_DAY_GENERATION,
            Some(reached),
            || {
                startup_mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap_err();
    assert!(startup_error.to_string().contains("deadline"));
    assert_eq!(startup_mutations.load(Ordering::SeqCst), 0);

    let old_commit_gate = Arc::new(RawDayGate::new());
    old_commit_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let old_close = old_commit_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let old_mutations = AtomicU64::new(0);
    assert!(
        !old_commit_gate
            .commit_old_day_if_no_exit(&old_close, Some(reached), || {
                old_mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(old_mutations.load(Ordering::SeqCst), 0);
    assert!(matches!(
        old_commit_gate.prepared_runtime_exit(),
        Some(PreparedRuntimeExit::Rollover(_))
    ));

    let activation_gate = Arc::new(RawDayGate::new());
    activation_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let activation_close = activation_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let activation_mutations = AtomicU64::new(0);
    assert!(
        !activation_gate
            .commit_manifest_activation(&activation_close, Some(reached), || {
                activation_mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(activation_mutations.load(Ordering::SeqCst), 0);
    assert!(matches!(
        activation_gate.prepared_runtime_exit(),
        Some(PreparedRuntimeExit::Rollover(_))
    ));

    let release_gate = Arc::new(RawDayGate::new());
    release_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let release_close = release_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    assert!(
        release_gate
            .commit_manifest_activation(&release_close, None, || Ok(()))
            .unwrap()
    );
    release_gate
        .activate_after_rollover(&release_close)
        .await
        .unwrap();
    let release_mutations = AtomicU64::new(0);
    let release_deadline = RuntimeDeadlineCandidate {
        observed_utc_ms: boundary_utc_ms + 1,
        ..reached
    };
    assert!(
        release_gate
            .commit_release_if_no_exit(&release_close, Some(release_deadline), || {
                release_mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .is_err()
    );
    assert_eq!(release_mutations.load(Ordering::SeqCst), 0);
    assert!(matches!(
        release_gate.prepared_runtime_exit(),
        Some(PreparedRuntimeExit::SameDay(_))
    ));

    let startup_release_gate = RawDayGate::new();
    let startup_now = now_ms();
    startup_release_gate
        .activate_startup(
            utc_day_string_from_ms(startup_now).unwrap(),
            INITIAL_RAW_DAY_GENERATION,
        )
        .await
        .unwrap();
    startup_release_gate
        .prepare_runtime_exit_with_cause(startup_now, RuntimeCloseCause::OperatorStop)
        .unwrap();
    let startup_releases = AtomicU64::new(0);
    assert!(
        startup_release_gate
            .commit_startup_release_if_no_exit(None, || {
                startup_releases.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .is_err()
    );
    assert_eq!(startup_releases.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transition_lock_linearizes_manifest_commit_against_runtime_exit() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let commit_gate = Arc::clone(&gate);
    let commit_close = close.clone();
    let commit = std::thread::spawn(move || {
        commit_gate.commit_manifest_activation(&commit_close, None, || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let exit_gate = Arc::clone(&gate);
    let exit = std::thread::spawn(move || {
        exit_gate
            .prepare_runtime_exit_with_cause(boundary_utc_ms + 1, RuntimeCloseCause::OperatorStop)
    });
    std::thread::sleep(Duration::from_millis(25));
    assert!(!exit.is_finished());
    release_tx.send(()).unwrap();
    assert!(commit.join().unwrap().unwrap());
    let PreparedRuntimeExit::SameDay(after_activation) = exit.join().unwrap().unwrap() else {
        panic!("activation-first exit must belong to the next generation");
    };
    assert_eq!(after_activation.identity.utc_day, close.next_utc_day);
    assert_eq!(after_activation.identity.generation, close.next_generation);

    let exit_first_gate = Arc::new(RawDayGate::new());
    exit_first_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let exit_first_close = exit_first_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    exit_first_gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap();
    let mutations = AtomicU64::new(0);
    assert!(
        !exit_first_gate
            .commit_manifest_activation(&exit_first_close, None, || {
                mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(mutations.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transition_lock_linearizes_old_day_commit_against_runtime_exit() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let close = gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let commit_gate = Arc::clone(&gate);
    let commit_close = close.clone();
    let commit = std::thread::spawn(move || {
        commit_gate.commit_old_day_if_no_exit(&commit_close, None, || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let exit_gate = Arc::clone(&gate);
    let exit = std::thread::spawn(move || {
        exit_gate
            .prepare_runtime_exit_with_cause(boundary_utc_ms + 1, RuntimeCloseCause::OperatorStop)
    });
    std::thread::sleep(Duration::from_millis(25));
    assert!(!exit.is_finished());
    release_tx.send(()).unwrap();
    assert!(commit.join().unwrap().unwrap());
    let PreparedRuntimeExit::Rollover(exit) = exit.join().unwrap().unwrap() else {
        panic!("a post-commit exit must remain owned by the closed old generation");
    };
    assert_eq!(exit.close, close);

    let exit_first_gate = Arc::new(RawDayGate::new());
    exit_first_gate
        .activate_startup("2026-07-07".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let exit_first_close = exit_first_gate
        .close_for_rollover("2026-07-07", boundary_utc_ms)
        .await
        .unwrap();
    exit_first_gate
        .prepare_runtime_exit_with_cause(boundary_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap();
    let mutations = AtomicU64::new(0);
    assert!(
        !exit_first_gate
            .commit_old_day_if_no_exit(&exit_first_close, None, || {
                mutations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(mutations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn same_day_candidate_closes_new_raw_admission_and_drains_prior_permits() {
    let observed_utc_ms = 1_783_468_799_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(observed_utc_ms).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();

    let admitted = match gate
        .begin_event_enqueue("binance_aggtrade", observed_utc_ms - 1)
        .await
        .unwrap()
    {
        RawDayEnqueueDecision::Permit(permit) => permit,
        RawDayEnqueueDecision::Defer(_) => panic!("same-day event must be admitted"),
    };
    let PreparedRuntimeExit::SameDay(close) = gate.prepare_runtime_exit(observed_utc_ms).unwrap()
    else {
        panic!("same-day return must prepare a same-day close");
    };
    assert!(matches!(
        gate.current_state(),
        RawDayGateState::ClosingSameDay(ref actual) if actual == &close
    ));
    let rejected = gate
        .begin_event_enqueue("binance_aggtrade", observed_utc_ms + 1)
        .await;
    assert!(matches!(
        rejected,
        Err(ref error) if error.to_string().contains("after same-day intake close")
    ));

    let sealing_gate = Arc::clone(&gate);
    let identity = close.identity.clone();
    let sealing =
        tokio::spawn(async move { sealing_gate.seal_same_day_close(&identity).await.unwrap() });
    tokio::task::yield_now().await;
    assert!(
        !sealing.is_finished(),
        "seal must wait for an enqueue admitted before the close candidate"
    );
    drop(admitted);
    assert_eq!(sealing.await.unwrap(), close);
}

#[tokio::test]
async fn accepted_raw_day_ticket_is_detached_and_reacquires_only_a_short_seal_guard() {
    let observed_utc_ms = 1_783_468_799_000;
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(observed_utc_ms).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();

    let ticket = gate
        .accept_unadmitted_event_ticket("polymarket", observed_utc_ms - 1)
        .await
        .unwrap()
        .unwrap();
    let cloned_ticket = ticket.clone();
    let PreparedRuntimeExit::SameDay(close) = gate
        .prepare_runtime_exit_with_cause(observed_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap()
    else {
        panic!("same-day ticket fixture must prepare a same-day close");
    };

    let enqueue = gate
        .begin_accepted_ticket_enqueue("polymarket", observed_utc_ms - 1, &cloned_ticket)
        .await
        .unwrap();
    assert!(matches!(enqueue, RawDayEnqueueDecision::Permit(_)));
    drop(enqueue);

    let sealed = timeout(
        Duration::from_secs(1),
        gate.seal_same_day_close(&close.identity),
    )
    .await
    .expect("a live detached ticket must not retain the enqueue fence")
    .unwrap();
    assert_eq!(sealed, close);
    assert_eq!(ticket, cloned_ticket);
}

#[tokio::test]
async fn controlled_runtime_cutoff_discards_only_unadmitted_events() {
    let observed_utc_ms = 1_783_468_799_000;
    for cause in [
        RuntimeCloseCause::OperatorStop,
        RuntimeCloseCause::RunDeadline,
    ] {
        let gate = Arc::new(RawDayGate::new());
        gate.activate_startup(
            utc_day_string_from_ms(observed_utc_ms).unwrap(),
            INITIAL_RAW_DAY_GENERATION,
        )
        .await
        .unwrap();
        let admitted = gate
            .begin_event_enqueue("binance_depth", observed_utc_ms - 1)
            .await
            .unwrap();
        let PreparedRuntimeExit::SameDay(_) = gate
            .prepare_runtime_exit_with_cause(observed_utc_ms, cause)
            .unwrap()
        else {
            panic!("controlled cutoff must remain in the active UTC day");
        };

        assert!(
            gate.begin_unadmitted_event_enqueue("binance_depth", observed_utc_ms + 1)
                .await
                .unwrap()
                .is_none(),
            "a new event after the controlled cutoff must not become admitted"
        );
        assert!(matches!(admitted, RawDayEnqueueDecision::Permit(_)));
    }

    let boundary_utc_ms = 1_783_468_800_000;
    for cause in [
        RuntimeCloseCause::OperatorStop,
        RuntimeCloseCause::RunDeadline,
    ] {
        let gate = Arc::new(RawDayGate::new());
        let old_utc_day = utc_day_string_from_ms(boundary_utc_ms - 1).unwrap();
        gate.activate_startup(old_utc_day, INITIAL_RAW_DAY_GENERATION)
            .await
            .unwrap();
        let admitted = gate
            .begin_event_enqueue("binance_depth", boundary_utc_ms - 1)
            .await
            .unwrap();
        assert!(matches!(
            gate.prepare_runtime_exit_with_cause(boundary_utc_ms + 1, cause)
                .unwrap(),
            PreparedRuntimeExit::Rollover(_)
        ));

        for recv_time_utc_ms in [boundary_utc_ms - 1, boundary_utc_ms] {
            assert!(
                gate.begin_unadmitted_event_enqueue("binance_depth", recv_time_utc_ms)
                    .await
                    .unwrap()
                    .is_none(),
                "a new old-day or next-day event after a rollover cutoff must not become admitted"
            );
        }
        assert!(matches!(admitted, RawDayEnqueueDecision::Permit(_)));
    }

    let gate = Arc::new(RawDayGate::new());
    let old_utc_day = utc_day_string_from_ms(boundary_utc_ms - 1).unwrap();
    gate.activate_startup(old_utc_day.clone(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    gate.prepare_rollover(&old_utc_day, boundary_utc_ms)
        .unwrap();
    assert!(matches!(
        gate.begin_unadmitted_event_enqueue("binance_depth", boundary_utc_ms)
            .await
            .unwrap(),
        Some(RawDayEnqueueDecision::Defer(_))
    ));

    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(observed_utc_ms).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    gate.install_runtime_deadline(Some(RuntimeDeadlineCandidate {
        deadline: Instant::now(),
        observed_utc_ms,
    }))
    .unwrap();
    assert!(
        gate.begin_unadmitted_event_enqueue("binance_depth", observed_utc_ms)
            .await
            .unwrap()
            .is_none(),
        "an elapsed installed deadline must close optional admission before the watcher runs"
    );

    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(observed_utc_ms).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    gate.prepare_runtime_exit_with_cause(
        observed_utc_ms,
        RuntimeCloseCause::CoordinatorFailure {
            stage: "controlled_cutoff_negative_control",
        },
    )
    .unwrap();
    gate.install_runtime_deadline(Some(RuntimeDeadlineCandidate {
        deadline: Instant::now(),
        observed_utc_ms,
    }))
    .unwrap();
    assert!(
        gate.begin_unadmitted_event_enqueue("binance_depth", observed_utc_ms + 1)
            .await
            .is_err(),
        "integrity exits must remain terminal"
    );
}

#[tokio::test]
async fn derived_polymarket_ticket_prefers_ordinary_and_bounds_controlled_fallback() {
    let same_day_utc_ms: i64 = 1_783_468_799_000;
    for cause in [
        RuntimeCloseCause::OperatorStop,
        RuntimeCloseCause::RunDeadline,
    ] {
        let gate = active_raw_day_gate_for_test(same_day_utc_ms).await;
        let parent = accepted_raw_day_ticket_from_gate_for_test(&gate, same_day_utc_ms - 1).await;
        assert!(matches!(
            gate.prepare_runtime_exit_with_cause(same_day_utc_ms, cause)
                .unwrap(),
            PreparedRuntimeExit::SameDay(_)
        ));
        let derived = gate
            .accept_derived_polymarket_event_ticket(&parent, same_day_utc_ms + 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(derived, parent);
    }

    let boundary_utc_ms: i64 = 1_783_468_800_000;
    for cause in [
        RuntimeCloseCause::OperatorStop,
        RuntimeCloseCause::RunDeadline,
    ] {
        let gate = active_raw_day_gate_for_test(boundary_utc_ms - 1).await;
        let old_day_parent =
            accepted_raw_day_ticket_from_gate_for_test(&gate, boundary_utc_ms - 1).await;
        let new_day_parent =
            accepted_raw_day_ticket_from_gate_for_test(&gate, boundary_utc_ms + 1).await;
        let PreparedRuntimeExit::Rollover(exit) = gate
            .prepare_runtime_exit_with_cause(boundary_utc_ms + 2, cause)
            .unwrap()
        else {
            panic!("cross-day controlled cutoff must prepare a rollover exit");
        };
        for parent in [old_day_parent, new_day_parent] {
            let derived = gate
                .accept_derived_polymarket_event_ticket(&parent, boundary_utc_ms + 3)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                derived,
                RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
                    utc_day: exit.close.next_utc_day.clone(),
                    old_generation: exit.close.generation,
                    expected_generation: exit.close.next_generation,
                })
            );
        }
    }

    let gate = active_raw_day_gate_for_test(boundary_utc_ms - 1).await;
    let old_day_parent =
        accepted_raw_day_ticket_from_gate_for_test(&gate, boundary_utc_ms - 1).await;
    let old_day = utc_day_string_from_ms(boundary_utc_ms - 1).unwrap();
    let close = prepare_rollover_for_producer(&gate, "polymarket", &old_day, boundary_utc_ms);
    gate.register_source_fence("polymarket", &close).unwrap();
    let close = gate.seal_prepared_rollover(&close).await.unwrap();
    commit_test_manifest_activation(&gate, &close);
    gate.activate_after_rollover(&close).await.unwrap();
    let ordinary = gate
        .accept_derived_polymarket_event_ticket(&old_day_parent, boundary_utc_ms + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ordinary,
        RawDayAcceptedTicket::Permit(RawDayPermit {
            utc_day: close.next_utc_day.clone(),
            generation: close.next_generation,
        })
    );

    let gate = active_raw_day_gate_for_test(same_day_utc_ms).await;
    let parent = accepted_raw_day_ticket_from_gate_for_test(&gate, same_day_utc_ms - 1).await;
    gate.prepare_runtime_exit_with_cause(same_day_utc_ms, RuntimeCloseCause::OperatorStop)
        .unwrap();
    let RawDayAcceptedTicket::Permit(valid_parent) = parent else {
        panic!("same-day parent must be a permit");
    };
    for invalid_parent in [
        RawDayAcceptedTicket::Permit(RawDayPermit {
            utc_day: valid_parent.utc_day.clone(),
            generation: valid_parent.generation.saturating_add(1),
        }),
        RawDayAcceptedTicket::Permit(RawDayPermit {
            utc_day: utc_day_string_from_ms(same_day_utc_ms + UTC_DAY_MS).unwrap(),
            generation: valid_parent.generation,
        }),
    ] {
        assert!(
            gate.accept_derived_polymarket_event_ticket(&invalid_parent, same_day_utc_ms + 1,)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        gate.accept_derived_polymarket_event_ticket(
            &RawDayAcceptedTicket::Permit(valid_parent),
            same_day_utc_ms + UTC_DAY_MS + 1,
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn manifest_activation_cutover_rejects_a_preexisting_pending_exit() {
    let boundary_utc_ms = 1_783_468_800_000;
    let gate = RawDayGate::new();
    gate.activate_startup(
        utc_day_string_from_ms(boundary_utc_ms - 1).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    let PreparedRuntimeExit::Rollover(exit) = gate.prepare_runtime_exit(boundary_utc_ms).unwrap()
    else {
        panic!("boundary-time exit must prepare PendingExit");
    };
    for producer in LOGICAL_PRODUCERS {
        gate.register_source_fence(producer, &exit.close).unwrap();
    }
    gate.seal_prepared_rollover(&exit.close).await.unwrap();

    assert!(
        !gate
            .commit_manifest_activation(&exit.close, None, || Ok(()))
            .unwrap()
    );
    let activation_error = gate.activate_after_rollover(&exit.close).await.unwrap_err();
    assert!(
        activation_error
            .to_string()
            .contains("matching committed manifest activation")
    );
    assert!(matches!(
        gate.current_state(),
        RawDayGateState::Closing(ref actual) if actual == &exit.close
    ));
    assert_eq!(
        gate.observation_epoch(),
        Some(RuntimeEpoch {
            utc_day: exit.close.utc_day,
            generation: exit.close.generation,
        })
    );
}
