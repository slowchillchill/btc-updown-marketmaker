use super::*;

#[tokio::test]
async fn completed_source_task_is_detected_during_runtime() {
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup("2026-07-17".to_owned(), INITIAL_RAW_DAY_GENERATION)
        .await
        .unwrap();
    let observation = SourceTaskObservationCell::default();
    let handle = SourceTaskHandle::spawn(
        "binance_aggtrade",
        observe_inner_source_return(
            "binance_aggtrade",
            async { Ok::<(), Box<dyn Error + Send + Sync>>(()) },
            observation.clone(),
            Arc::clone(&gate),
        ),
        observation,
        gate,
    );
    timeout(Duration::from_secs(1), async {
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let handles = vec![handle];

    assert_eq!(finished_source_task_index(&handles), Some(0));
    assert_eq!(
        source_task_ended_unexpectedly("binance_aggtrade"),
        "source_task_ended_unexpectedly:binance_aggtrade"
    );
}

#[tokio::test]
async fn exact_lag_report_remains_primary_before_writer_shutdown_secondary() {
    for lag_count in [1_364, 781] {
        let gate = current_day_active_gate().await;
        let observation = SourceTaskObservationCell::default();
        let handle = SourceTaskHandle::spawn(
            "polymarket_market",
            observe_inner_source_return(
                "polymarket_market",
                async move {
                    Err::<(), Box<dyn Error + Send + Sync>>(
                        SdkStreamError::Lagged(lag_count).into(),
                    )
                },
                observation.clone(),
                Arc::clone(&gate),
            ),
            observation,
            Arc::clone(&gate),
        );
        timeout(Duration::from_secs(1), async {
            while !handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut handles = vec![handle];
        let index = finished_source_task_index(&handles).unwrap();
        let Some(PreparedRuntimeExit::SameDay(close)) = gate.prepared_runtime_exit() else {
            panic!("source lag must prepare the authoritative same-day close");
        };
        assert_eq!(close.cause.source_task_kind(), Some("polymarket_market"));
        let termination = handles
            .swap_remove(index)
            .await_termination(&close.identity)
            .await;
        assert_eq!(
            termination.error.as_deref(),
            Some(format!("receiver lagged by {lag_count} messages").as_str())
        );
        let primary = assess_source_shutdown(&termination, None, &close.identity)
            .error
            .expect("the exact source report must classify the primary failure");
        let expected_primary =
            format!("source_task_error:polymarket_market:receiver lagged by {lag_count} messages");
        assert_eq!(primary, expected_primary);

        let source_gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        record_gap(&source_gaps, primary.clone());
        let mut runtime_failure = None;
        promote_runtime_primary(&mut runtime_failure, primary);
        let writer_error = format!("writer shutdown timeout after lag {lag_count}");
        writer_outputs_or_record_failure(
            WriterStopOutcome {
                outputs: Vec::new(),
                errors: vec![writer_error.clone()],
                all_terminal: false,
                all_close_proven: false,
            },
            &source_gaps,
            &mut runtime_failure,
        );

        promote_runtime_primary(&mut runtime_failure, expected_primary.clone());
        assert_eq!(
            runtime_failure,
            Some(format!(
                "{expected_primary}; shutdown_secondary:writer_finalize_failed:{writer_error}"
            ))
        );
        assert_eq!(
            source_gaps
                .lock()
                .expect("source gap mutex poisoned")
                .all_reasons(),
            vec![
                expected_primary,
                format!("writer_finalize_failed:{writer_error}")
            ]
        );
    }
}

pub(super) async fn current_day_active_gate() -> Arc<RawDayGate> {
    let gate = Arc::new(RawDayGate::new());
    gate.activate_startup(
        utc_day_string_from_ms(now_ms()).unwrap(),
        INITIAL_RAW_DAY_GENERATION,
    )
    .await
    .unwrap();
    gate
}

fn test_runtime_close(gate: &RawDayGate) -> RuntimeCloseIdentity {
    let PreparedRuntimeExit::SameDay(close) = gate.prepare_runtime_exit(now_ms()).unwrap() else {
        panic!("test close must remain in the current UTC day");
    };
    close.identity
}

#[tokio::test]
async fn source_abort_requires_one_exact_task_generation_close_permit() {
    let gate = current_day_active_gate().await;
    let close = test_runtime_close(&gate);

    let observation = SourceTaskObservationCell::default();
    let mut exact = SourceTaskHandle::spawn(
        "polymarket_rtds_chainlink_btc_usd",
        std::future::pending::<Result<()>>(),
        observation,
        Arc::clone(&gate),
    );
    exact.abort_with_permit(&close).unwrap();
    assert!(exact.abort_with_permit(&close).is_err());
    let exact = exact.await_termination(&close).await;
    assert_eq!(exact.kind, SourceTaskTerminationKind::CoordinatedAbort);
    assert!(exact.observed_utc_ms.is_some());
    assert_eq!(
        exact.epoch,
        Some(RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        })
    );

    let observation = SourceTaskObservationCell::default();
    let missing = SourceTaskHandle::spawn(
        "polymarket_rtds_chainlink_btc_usd",
        std::future::pending::<Result<()>>(),
        observation,
        Arc::clone(&gate),
    );
    missing.abort_without_permit();
    let missing = missing.await_termination(&close).await;
    assert_eq!(missing.kind, SourceTaskTerminationKind::Unclean);
    let Some(PreparedRuntimeExit::SameDay(prepared)) = gate.prepared_runtime_exit() else {
        panic!("missing abort permit must retain the same-day close");
    };
    assert!(prepared.unclean);

    let observation = SourceTaskObservationCell::default();
    let mut wrong = SourceTaskHandle::spawn(
        "polymarket_rtds_chainlink_btc_usd",
        std::future::pending::<Result<()>>(),
        observation,
        Arc::clone(&gate),
    );
    wrong.abort_permit = Some(SourceAbortPermit {
        source_kind: wrong.source_kind,
        close: RuntimeCloseIdentity {
            close_id: close.close_id.saturating_add(1),
            ..close.clone()
        },
        observed_utc_ms: now_ms(),
        epoch: RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        },
    });
    wrong.join.abort();
    let wrong = wrong.await_termination(&close).await;
    assert_eq!(wrong.kind, SourceTaskTerminationKind::Unclean);
}

#[tokio::test]
async fn matching_abort_permit_never_masks_a_source_task_panic() {
    let gate = current_day_active_gate().await;
    let close = test_runtime_close(&gate);
    let (panic_tx, panic_rx) = oneshot::channel::<()>();
    let observation = SourceTaskObservationCell::default();
    let observed_gate = Arc::clone(&gate);
    let mut handle = SourceTaskHandle::spawn(
        "polymarket_rtds_chainlink_btc_usd",
        async move {
            let _ = panic_rx.await;
            panic!("source panic after close permit registration");
        },
        observation,
        gate,
    );
    handle.abort_permit = Some(SourceAbortPermit {
        source_kind: handle.source_kind,
        close: close.clone(),
        observed_utc_ms: now_ms(),
        epoch: RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        },
    });
    panic_tx.send(()).unwrap();
    let termination = handle.await_termination(&close).await;
    assert_eq!(termination.kind, SourceTaskTerminationKind::Unclean);
    assert!(termination.observed_utc_ms.is_some());
    assert_eq!(
        termination.epoch,
        Some(RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        })
    );
    let Some(PreparedRuntimeExit::SameDay(prepared)) = observed_gate.prepared_runtime_exit() else {
        panic!("panic must retain the same-day close");
    };
    assert!(prepared.unclean);
    assert!(
        termination
            .error
            .as_deref()
            .is_some_and(|error| error.contains("task") || error.contains("panic"))
    );
}

#[tokio::test]
async fn source_report_keeps_inner_return_time_before_late_wrapper_detection() {
    let gate = current_day_active_gate().await;
    let observation = SourceTaskObservationCell::default();
    let inner = observe_inner_source_return(
        "binance_aggtrade",
        async { Ok(()) },
        observation.clone(),
        Arc::clone(&gate),
    );
    let handle = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async move {
            inner.await?;
            sleep(Duration::from_millis(200)).await;
            Ok(())
        },
        observation,
        Arc::clone(&gate),
    );
    let close = test_runtime_close(&gate);
    let detected_utc_ms = now_ms();
    let termination = handle.await_termination(&close).await;
    assert_eq!(termination.kind, SourceTaskTerminationKind::ReliableReturn);
    let observed_utc_ms = termination.observed_utc_ms.unwrap();
    assert!(detected_utc_ms <= observed_utc_ms.saturating_add(25));
    assert!(now_ms().saturating_sub(observed_utc_ms) >= 150);
}

#[tokio::test]
async fn source_termination_distinguishes_inner_wrapper_and_exact_stop_proofs() {
    let inner_gate = current_day_active_gate().await;
    let inner_observation = SourceTaskObservationCell::default();
    let inner = SourceTaskHandle::spawn(
        "binance_aggtrade",
        observe_inner_source_return(
            "binance_aggtrade",
            async {
                Err::<(), Box<dyn Error + Send + Sync>>(
                    io::Error::other("inner source failed").into(),
                )
            },
            inner_observation.clone(),
            Arc::clone(&inner_gate),
        ),
        inner_observation,
        Arc::clone(&inner_gate),
    );
    timeout(Duration::from_secs(1), async {
        while !inner.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let Some(PreparedRuntimeExit::SameDay(inner_close)) = inner_gate.prepared_runtime_exit() else {
        panic!("inner return must prepare a same-day close");
    };
    let inner = inner.await_termination(&inner_close.identity).await;
    assert_eq!(inner.kind, SourceTaskTerminationKind::ReliableReturn);
    assert!(
        inner
            .error
            .as_deref()
            .is_some_and(|error| error.contains("inner source failed"))
    );

    let wrapper_gate = current_day_active_gate().await;
    let wrapper_observation = SourceTaskObservationCell::default();
    let inner_observation = wrapper_observation.clone();
    let observed_gate = Arc::clone(&wrapper_gate);
    let wrapper = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async move {
            observe_inner_source_return(
                "binance_aggtrade",
                async { Ok(()) },
                inner_observation,
                observed_gate,
            )
            .await?;
            Err(io::Error::other("wrapper failed after inner return").into())
        },
        wrapper_observation,
        Arc::clone(&wrapper_gate),
    );
    timeout(Duration::from_secs(1), async {
        while !wrapper.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let Some(PreparedRuntimeExit::SameDay(wrapper_close)) = wrapper_gate.prepared_runtime_exit()
    else {
        panic!("inner return before wrapper failure must prepare a same-day close");
    };
    let wrapper = wrapper.await_termination(&wrapper_close.identity).await;
    assert_eq!(wrapper.kind, SourceTaskTerminationKind::Unclean);
    let Some(PreparedRuntimeExit::SameDay(prepared)) = wrapper_gate.prepared_runtime_exit() else {
        panic!("wrapper failure must retain the same-day close");
    };
    assert!(prepared.unclean);
    assert!(
        wrapper
            .error
            .as_deref()
            .is_some_and(|error| error.contains("wrapper failed after inner return"))
    );

    let exact_gate = current_day_active_gate().await;
    let exact_close = test_runtime_close(&exact_gate);
    let exact_stop = CoordinatedSourceStop {
        producer: "binance_aggtrade".to_owned(),
        close: exact_close.clone(),
    };
    let exact = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async move { Err(exact_stop.into()) },
        SourceTaskObservationCell::default(),
        exact_gate,
    )
    .await_termination(&exact_close)
    .await;
    assert_eq!(exact.kind, SourceTaskTerminationKind::CoordinatedAbort);
    assert!(exact.error.is_none());

    let wrong_gate = current_day_active_gate().await;
    let wrong_close = test_runtime_close(&wrong_gate);
    let wrong_stop = CoordinatedSourceStop {
        producer: "binance_depth".to_owned(),
        close: wrong_close.clone(),
    };
    let observed_wrong_gate = Arc::clone(&wrong_gate);
    let wrong = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async move { Err(wrong_stop.into()) },
        SourceTaskObservationCell::default(),
        wrong_gate,
    )
    .await_termination(&wrong_close)
    .await;
    assert_eq!(wrong.kind, SourceTaskTerminationKind::Unclean);
    let Some(PreparedRuntimeExit::SameDay(prepared)) = observed_wrong_gate.prepared_runtime_exit()
    else {
        panic!("wrong stop proof must retain the same-day close");
    };
    assert!(prepared.unclean);

    let wrong_identity_gate = current_day_active_gate().await;
    let expected_close = test_runtime_close(&wrong_identity_gate);
    let wrong_identity_stop = CoordinatedSourceStop {
        producer: "binance_aggtrade".to_owned(),
        close: RuntimeCloseIdentity {
            close_id: expected_close.close_id.saturating_add(1),
            ..expected_close.clone()
        },
    };
    let observed_wrong_identity_gate = Arc::clone(&wrong_identity_gate);
    let wrong_identity = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async move { Err(wrong_identity_stop.into()) },
        SourceTaskObservationCell::default(),
        wrong_identity_gate,
    )
    .await_termination(&expected_close)
    .await;
    assert_eq!(wrong_identity.kind, SourceTaskTerminationKind::Unclean);
    let Some(PreparedRuntimeExit::SameDay(prepared)) =
        observed_wrong_identity_gate.prepared_runtime_exit()
    else {
        panic!("wrong stop identity must retain the same-day close");
    };
    assert!(prepared.unclean);
}

#[tokio::test]
async fn bare_wrapper_return_is_known_but_wrapper_error_is_immediately_unclean() {
    let clean_gate = current_day_active_gate().await;
    let clean = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async { Ok(()) },
        SourceTaskObservationCell::default(),
        Arc::clone(&clean_gate),
    );
    let clean_close = loop {
        if let Some(PreparedRuntimeExit::SameDay(close)) = clean_gate.prepared_runtime_exit() {
            break close;
        }
        tokio::task::yield_now().await;
    };
    let clean = clean.await_termination(&clean_close.identity).await;
    assert_eq!(clean.kind, SourceTaskTerminationKind::ReliableReturn);
    assert!(!clean_close.unclean);

    let failed_gate = current_day_active_gate().await;
    let failed = SourceTaskHandle::spawn(
        "binance_aggtrade",
        async { Err(io::Error::other("wrapper terminal failure").into()) },
        SourceTaskObservationCell::default(),
        Arc::clone(&failed_gate),
    );
    let failed_close = loop {
        if let Some(PreparedRuntimeExit::SameDay(close)) = failed_gate.prepared_runtime_exit() {
            break close;
        }
        tokio::task::yield_now().await;
    };
    let failed = failed.await_termination(&failed_close.identity).await;
    assert_eq!(failed.kind, SourceTaskTerminationKind::Unclean);
    let Some(PreparedRuntimeExit::SameDay(failed_close)) = failed_gate.prepared_runtime_exit()
    else {
        panic!("failed wrapper must retain the same-day close");
    };
    assert!(failed_close.unclean);
}

#[test]
fn source_stop_fact_is_assessed_only_after_the_final_task_proof() {
    let close = RuntimeCloseIdentity {
        utc_day: "2026-07-17".to_owned(),
        generation: 7,
        close_id: 19,
    };
    let reliable = SourceTaskTermination {
        source_kind: "binance_aggtrade",
        observed_utc_ms: Some(1_784_236_800_123),
        epoch: Some(RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        }),
        kind: SourceTaskTerminationKind::ReliableReturn,
        error: None,
    };

    let ack_first = assess_source_shutdown(&reliable, Some(&SourceControlStopFact::Acked), &close);
    let failed_reply_first = assess_source_shutdown(
        &reliable,
        Some(&SourceControlStopFact::Failed(
            "control reply raced with reliable return".to_owned(),
        )),
        &close,
    );
    assert!(!ack_first.unclean);
    assert!(!failed_reply_first.unclean);
    assert!(ack_first.error.is_none());
    assert!(failed_reply_first.error.is_none());

    let unclean = SourceTaskTermination {
        kind: SourceTaskTerminationKind::Unclean,
        error: Some("source panicked".to_owned()),
        ..reliable
    };
    let assessment = assess_source_shutdown(&unclean, Some(&SourceControlStopFact::Acked), &close);
    assert!(assessment.unclean);
    assert!(
        assessment
            .error
            .as_deref()
            .is_some_and(|error| error.contains("source panicked"))
    );
}
