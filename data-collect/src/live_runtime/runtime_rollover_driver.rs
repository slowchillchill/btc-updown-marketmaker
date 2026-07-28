use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RolloverCommitOutcome {
    Activated,
    PendingExit,
    ActivatedStopped,
}

fn prepare_coordinator_failure_exit(
    raw_day_gate: &RawDayGate,
    stage: &'static str,
) -> Result<PreparedRuntimeExit> {
    raw_day_gate
        .prepare_runtime_exit_with_cause(now_ms(), RuntimeCloseCause::CoordinatorFailure { stage })
}

pub(super) fn freeze_coordinator_failure_message(
    raw_day_gate: &RawDayGate,
    stage: &'static str,
    message: String,
) -> String {
    match prepare_coordinator_failure_exit(raw_day_gate, stage) {
        Ok(_) => message,
        Err(error) => format!("{message}; runtime_close_prepare_failed:{error}"),
    }
}

fn prepare_post_activation_unclean_exit(
    raw_day_gate: &RawDayGate,
    stage: &'static str,
) -> Result<PreparedSameDayClose> {
    let cause = RuntimeCloseCause::CoordinatorFailure { stage };
    let prepared = raw_day_gate.prepare_runtime_exit_with_cause(now_ms(), cause.clone())?;
    let PreparedRuntimeExit::SameDay(close) = prepared else {
        return Err(invalid_data(format!(
            "post-activation failure was assigned to the old generation: stage={stage} prepared={prepared:?}"
        ))
        .into());
    };
    match raw_day_gate.mark_runtime_exit_unclean_with_cause(&close.identity, cause)? {
        PreparedRuntimeExit::SameDay(close) => Ok(close),
        PreparedRuntimeExit::Rollover(exit) => Err(invalid_data(format!(
            "post-activation unclean mark changed generation ownership: stage={stage} exit={exit:?}"
        ))
        .into()),
    }
}

fn stop_post_activation_release_unclean(
    raw_day_gate: &RawDayGate,
    rollover_coordinator: &mut RolloverCoordinatorState,
    release: &mut ProducerReleaseController,
    release_stop_issued: &mut bool,
    stage: &'static str,
) -> Result<PreparedSameDayClose> {
    match prepare_post_activation_unclean_exit(raw_day_gate, stage) {
        Ok(close) => {
            *release_stop_issued = true;
            release.stop_with_close(close.identity.clone());
            if let Err(error) = rollover_coordinator.abandon_release_after_activation() {
                let message = format!(
                    "post-activation release abandonment failed after exact stop: stage={stage} error={error}"
                );
                rollover_coordinator.mark_outcome_uncertain(message.clone());
                return Err(invalid_data(message).into());
            }
            Ok(close)
        }
        Err(error) => {
            *release_stop_issued = true;
            release.stop();
            let message = format!(
                "post-activation close ownership is uncertain; old-generation stop issued: stage={stage} error={error}"
            );
            rollover_coordinator.mark_outcome_uncertain(message.clone());
            Err(invalid_data(message).into())
        }
    }
}

fn stop_post_activation_release_known(
    rollover_coordinator: &mut RolloverCoordinatorState,
    release: &mut ProducerReleaseController,
    release_stop_issued: &mut bool,
    close: &PreparedSameDayClose,
) -> Result<()> {
    *release_stop_issued = true;
    release.stop_with_close(close.identity.clone());
    if let Err(error) = rollover_coordinator.abandon_release_after_activation() {
        let message = format!(
            "post-activation release abandonment failed after known exact stop: error={error}"
        );
        rollover_coordinator.mark_outcome_uncertain(message.clone());
        return Err(invalid_data(message).into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_normal_rollover(
    ready: ManifestRolloverReady,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    raw_day_gate: &Arc<RawDayGate>,
    runtime_exit_rx: &mut mpsc::Receiver<RuntimeExitRequest>,
    event_rx: &mut mpsc::Receiver<SourceIngressPayload>,
    writer_handles: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut PendingIngressBatches,
    coverage: &mut BTreeMap<String, CoverageStats>,
    received_rows: &mut u64,
    average_event_bytes: &mut u64,
    source_gaps: &SharedSourceGaps,
    source_queue_backpressure: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    rollover_coordinator: &mut RolloverCoordinatorState,
    release_stop_issued: &mut bool,
    runtime_deadline: Option<RuntimeDeadlineCandidate>,
) -> Result<RolloverCommitOutcome> {
    let close = ready.close.clone();
    let post_fence_epoch = RuntimeEpoch {
        utc_day: close.next_utc_day.clone(),
        generation: close.next_generation,
    };
    rollover_coordinator.latch_ready(ready)?;
    if let Some(completion) =
        poll_writer_completion(writer_handles, raw_day_gate, Some(&post_fence_epoch))
    {
        record_gap(source_gaps, completion.message);
        if raw_day_gate.prepared_runtime_exit().is_none() {
            let _ = prepare_coordinator_failure_exit(
                raw_day_gate,
                "post_fence_writer_terminal_before_old_commit",
            )?;
        }
        rollover_coordinator.issue_stop_with_close()?;
        return Ok(RolloverCommitOutcome::PendingExit);
    }
    if raw_day_gate.prepared_runtime_exit().is_some() {
        rollover_coordinator.issue_stop_with_close()?;
        return Ok(RolloverCommitOutcome::PendingExit);
    }

    match rollover_coordinator
        .commit_old_day_once(
            ManifestRolloverCommitMode::Normal,
            manifest_tx,
            Some(Arc::clone(raw_day_gate)),
            runtime_deadline,
        )
        .await?
    {
        OldDayCommitAck::Confirmed => {}
        OldDayCommitAck::PreparedExitWon => {
            rollover_coordinator.issue_stop_with_close()?;
            return Ok(RolloverCommitOutcome::PendingExit);
        }
        OldDayCommitAck::OutcomeUncertain(error) => {
            rollover_coordinator.stop_replies_preserving_cutpoint();
            return Err(invalid_data(error).into());
        }
    }
    if let Some(completion) =
        poll_writer_completion(writer_handles, raw_day_gate, Some(&post_fence_epoch))
    {
        record_gap(source_gaps, completion.message);
        if raw_day_gate.prepared_runtime_exit().is_none() {
            let _ = prepare_coordinator_failure_exit(
                raw_day_gate,
                "post_fence_writer_terminal_after_old_commit",
            )?;
        }
        rollover_coordinator.issue_stop_with_close()?;
        return Ok(RolloverCommitOutcome::PendingExit);
    }
    if raw_day_gate.prepared_runtime_exit().is_some() {
        rollover_coordinator.issue_stop_with_close()?;
        return Ok(RolloverCommitOutcome::PendingExit);
    }

    let mut release = match rollover_coordinator.prepare_release_controller() {
        Ok(release) => release,
        Err(error) => {
            let _ =
                prepare_coordinator_failure_exit(raw_day_gate, "producer_release_token_delivery")?;
            return Err(error);
        }
    };
    let armed_deadline = Instant::now() + STARTUP_GATE_TIMEOUT;
    while !release.all_armed() {
        if let Some(completion) =
            poll_writer_completion(writer_handles, raw_day_gate, Some(&post_fence_epoch))
        {
            record_gap(source_gaps, completion.message);
            if raw_day_gate.prepared_runtime_exit().is_none() {
                let _ = prepare_coordinator_failure_exit(
                    raw_day_gate,
                    "post_fence_writer_terminal_before_release_armed",
                )?;
            }
            release.stop();
            rollover_coordinator.mark_release_stopped_after_old_commit()?;
            return Ok(RolloverCommitOutcome::PendingExit);
        }
        let remaining = armed_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            release.stop();
            rollover_coordinator.mark_release_stopped_after_old_commit()?;
            let _ =
                prepare_coordinator_failure_exit(raw_day_gate, "producer_release_armed_timeout")?;
            return Err(invalid_data("producer release armed-ack timeout").into());
        }
        tokio::select! {
            biased;
            request = runtime_exit_rx.recv() => {
                let Some(request) = request else {
                    release.stop();
                    rollover_coordinator.mark_release_stopped_after_old_commit()?;
                    let _ = prepare_coordinator_failure_exit(
                        raw_day_gate,
                        "runtime_exit_channel_closed_before_release_armed",
                    )?;
                    return Err(invalid_data("runtime-exit channel closed before release armed").into());
                };
                if let Err(error) = apply_runtime_exit_request(raw_day_gate, &request) {
                    release.stop();
                    if let Err(mark_error) =
                        rollover_coordinator.mark_release_stopped_after_old_commit()
                    {
                        return Err(invalid_data(format!(
                            "runtime exit preparation failed before release and stop cutpoint could not be recorded: prepare={error} cutpoint={mark_error}"
                        ))
                        .into());
                    }
                    let _ = prepare_coordinator_failure_exit(
                        raw_day_gate,
                        "runtime_exit_prepare_before_release_armed",
                    );
                    return Err(error);
                }
                release.stop();
                rollover_coordinator.mark_release_stopped_after_old_commit()?;
                return Ok(RolloverCommitOutcome::PendingExit);
            }
            ack = release.ack_rx.recv() => {
                let Some(ack) = ack else {
                    release.stop();
                    rollover_coordinator.mark_release_stopped_after_old_commit()?;
                    let _ = prepare_coordinator_failure_exit(
                        raw_day_gate,
                        "producer_release_armed_ack_channel_closed",
                    )?;
                    return Err(invalid_data("producer release ack channel closed before all armed").into());
                };
                if let Err(error) = release.accept_ack(ack) {
                    release.stop();
                    rollover_coordinator.mark_release_stopped_after_old_commit()?;
                    let _ = prepare_coordinator_failure_exit(
                        raw_day_gate,
                        "producer_release_armed_ack_invalid",
                    )?;
                    return Err(error);
                }
            }
            () = sleep(remaining.min(Duration::from_millis(25))) => {
                if raw_day_gate.prepared_runtime_exit().is_some() {
                    release.stop();
                    rollover_coordinator.mark_release_stopped_after_old_commit()?;
                    return Ok(RolloverCommitOutcome::PendingExit);
                }
            }
        }
    }
    if let Some(completion) =
        poll_writer_completion(writer_handles, raw_day_gate, Some(&post_fence_epoch))
    {
        record_gap(source_gaps, completion.message);
        if raw_day_gate.prepared_runtime_exit().is_none() {
            let _ = prepare_coordinator_failure_exit(
                raw_day_gate,
                "post_fence_writer_terminal_before_new_day_activation",
            )?;
        }
        release.stop();
        rollover_coordinator.mark_release_stopped_after_old_commit()?;
        return Ok(RolloverCommitOutcome::PendingExit);
    }
    if let Err(error) = rollover_coordinator.mark_tokens_armed() {
        release.stop();
        rollover_coordinator.mark_release_stopped_after_old_commit()?;
        let _ = prepare_coordinator_failure_exit(raw_day_gate, "producer_release_arm_commit")?;
        return Err(error);
    }

    let activation = match rollover_coordinator
        .activate_new_day_once(manifest_tx, Arc::clone(raw_day_gate), runtime_deadline)
        .await
    {
        Ok(activation) => activation,
        Err(error) => {
            release.stop();
            rollover_coordinator.mark_release_stopped_after_old_commit()?;
            let _ = prepare_coordinator_failure_exit(raw_day_gate, "new_day_activation_send")?;
            return Err(error);
        }
    };
    match activation {
        ActivationMutationAck::Confirmed => {}
        ActivationMutationAck::PreparedExitWon => {
            *release_stop_issued = true;
            release.stop();
            return Ok(RolloverCommitOutcome::PendingExit);
        }
        ActivationMutationAck::OutcomeUncertain(error) => {
            *release_stop_issued = true;
            release.stop();
            return Err(invalid_data(error).into());
        }
    }
    let mut post_activation_exit = if let Some(prepared) = raw_day_gate.prepared_runtime_exit() {
        let PreparedRuntimeExit::SameDay(close) = prepared else {
            release.stop();
            let _ = rollover_coordinator.mark_outcome_uncertain(
                "post-activation exit retained old-generation ownership".to_owned(),
            );
            return Err(
                invalid_data("post-activation exit retained old-generation ownership").into(),
            );
        };
        Some(close)
    } else {
        None
    };

    if let Err(error) = rollover_coordinator.mark_deferred_flushing() {
        stop_post_activation_release_unclean(
            raw_day_gate,
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            "deferred_flush_cutpoint",
        )?;
        return Err(error);
    }
    release.set_phase(ProducerReleasePhase::FlushDeferred);
    let flush_deadline = Instant::now() + STARTUP_GATE_TIMEOUT;
    while !release.all_flushed() {
        let remaining = flush_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            stop_post_activation_release_unclean(
                raw_day_gate,
                rollover_coordinator,
                &mut release,
                release_stop_issued,
                "producer_deferred_flush_timeout",
            )?;
            return Err(invalid_data("producer deferred-flush ack timeout").into());
        }
        tokio::select! {
            biased;
            request = runtime_exit_rx.recv(), if post_activation_exit.is_none() => {
                let Some(request) = request else {
                    stop_post_activation_release_unclean(
                        raw_day_gate,
                        rollover_coordinator,
                        &mut release,
                        release_stop_issued,
                        "runtime_exit_channel_closed_during_deferred_flush",
                    )?;
                    return Err(invalid_data("runtime-exit channel closed during deferred flush").into());
                };
                let prepared = match apply_runtime_exit_request(raw_day_gate, &request) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        stop_post_activation_release_unclean(
                            raw_day_gate,
                            rollover_coordinator,
                            &mut release,
                            release_stop_issued,
                            "runtime_exit_prepare_during_deferred_flush",
                        )?;
                        return Err(error);
                    }
                };
                let PreparedRuntimeExit::SameDay(close) = prepared else {
                    *release_stop_issued = true;
                    release.stop();
                    let _ = rollover_coordinator.mark_outcome_uncertain(
                        "runtime exit during deferred flush retained old-generation ownership".to_owned(),
                    );
                    return Err(invalid_data(
                        "runtime exit during deferred flush retained old-generation ownership",
                    ).into());
                };
                post_activation_exit = Some(close);
            }
            ack = release.ack_rx.recv() => {
                let Some(ack) = ack else {
                    stop_post_activation_release_unclean(
                        raw_day_gate,
                        rollover_coordinator,
                        &mut release,
                        release_stop_issued,
                        "producer_deferred_flush_ack_channel_closed",
                    )?;
                    return Err(invalid_data("producer release ack channel closed before all flushed").into());
                };
                if let Err(error) = release.accept_ack(ack) {
                    stop_post_activation_release_unclean(
                        raw_day_gate,
                        rollover_coordinator,
                        &mut release,
                        release_stop_issued,
                        "producer_deferred_flush_failed",
                    )?;
                    return Err(error);
                }
            }
            received = event_rx.recv() => {
                let Some(inflight_event) = received else {
                    stop_post_activation_release_unclean(
                        raw_day_gate,
                        rollover_coordinator,
                        &mut release,
                        release_stop_issued,
                        "source_event_queue_closed_during_deferred_flush",
                    )?;
                    return Err(invalid_data("source event queue closed during deferred flush").into());
                };
                if let Err(error) = process_ingress_event(
                    inflight_event,
                    writer_handles,
                    batches,
                    coverage,
                    received_rows,
                    average_event_bytes,
                    source_gaps,
                    source_queue_backpressure,
                    writer_queue_tracker,
                    run_started,
                    manifest_tx,
                    rollover_coordinator,
                ).await {
                    stop_post_activation_release_unclean(
                        raw_day_gate,
                        rollover_coordinator,
                        &mut release,
                        release_stop_issued,
                        "deferred_ingress_processing",
                    )?;
                    return Err(error);
                }
            }
            () = sleep(remaining.min(Duration::from_millis(25))) => {
                if post_activation_exit.is_none()
                    && let Some(prepared) = raw_day_gate.prepared_runtime_exit()
                {
                    let PreparedRuntimeExit::SameDay(close) = prepared else {
                        *release_stop_issued = true;
                        release.stop();
                        let _ = rollover_coordinator.mark_outcome_uncertain(
                            "prepared exit during deferred flush retained old-generation ownership".to_owned(),
                        );
                        return Err(invalid_data(
                            "prepared exit during deferred flush retained old-generation ownership",
                        ).into());
                    };
                    post_activation_exit = Some(close);
                }
            }
        }
    }

    while let Ok(inflight_event) = event_rx.try_recv() {
        if let Err(error) = process_ingress_event(
            inflight_event,
            writer_handles,
            batches,
            coverage,
            received_rows,
            average_event_bytes,
            source_gaps,
            source_queue_backpressure,
            writer_queue_tracker,
            run_started,
            manifest_tx,
            rollover_coordinator,
        )
        .await
        {
            stop_post_activation_release_unclean(
                raw_day_gate,
                rollover_coordinator,
                &mut release,
                release_stop_issued,
                "post_activation_ingress_drain",
            )?;
            return Err(error);
        }
    }
    if let Err(error) = flush_all_batches(
        writer_handles,
        batches,
        source_gaps,
        source_queue_backpressure,
        writer_queue_tracker,
        run_started,
    )
    .await
    {
        stop_post_activation_release_unclean(
            raw_day_gate,
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            "post_activation_writer_flush",
        )?;
        return Err(error);
    }
    let prepared_after_flush = post_activation_exit
        .map(PreparedRuntimeExit::SameDay)
        .or_else(|| raw_day_gate.prepared_runtime_exit());
    if let Some(prepared) = prepared_after_flush {
        let PreparedRuntimeExit::SameDay(close) = prepared else {
            *release_stop_issued = true;
            release.stop();
            let _ = rollover_coordinator.mark_outcome_uncertain(
                "prepared exit before raw activation retained old-generation ownership".to_owned(),
            );
            return Err(invalid_data(
                "prepared exit before raw activation retained old-generation ownership",
            )
            .into());
        };
        stop_post_activation_release_known(
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            &close,
        )?;
        return Ok(RolloverCommitOutcome::ActivatedStopped);
    }
    if let Err(error) = raw_day_gate.activate_after_rollover(&close).await {
        if let Some(PreparedRuntimeExit::SameDay(close)) = raw_day_gate.prepared_runtime_exit() {
            stop_post_activation_release_known(
                rollover_coordinator,
                &mut release,
                release_stop_issued,
                &close,
            )?;
            return Ok(RolloverCommitOutcome::ActivatedStopped);
        }
        stop_post_activation_release_unclean(
            raw_day_gate,
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            "raw_day_activation",
        )?;
        return Err(error);
    }
    if let Err(error) = rollover_coordinator.mark_raw_activated() {
        stop_post_activation_release_unclean(
            raw_day_gate,
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            "raw_day_activation_cutpoint",
        )?;
        return Err(error);
    }
    if let Err(error) = raw_day_gate.commit_release_if_no_exit(&close, runtime_deadline, || {
        rollover_coordinator.finish_release(&mut release)
    }) {
        if let Some(PreparedRuntimeExit::SameDay(close)) = raw_day_gate.prepared_runtime_exit() {
            stop_post_activation_release_known(
                rollover_coordinator,
                &mut release,
                release_stop_issued,
                &close,
            )?;
            return Ok(RolloverCommitOutcome::ActivatedStopped);
        }
        stop_post_activation_release_unclean(
            raw_day_gate,
            rollover_coordinator,
            &mut release,
            release_stop_issued,
            "producer_release_completion",
        )?;
        return Err(error);
    }
    Ok(RolloverCommitOutcome::Activated)
}

async fn enqueue_synthetic_day_barrier(
    sink: &SourceEventSink,
    run_started: Instant,
    producer: &str,
    close: RawDayClose,
) -> Result<oneshot::Receiver<std::result::Result<DayBarrierRelease, String>>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    send_ingress_control(
        sink,
        run_started,
        IngressControl::DayBarrier {
            producer: producer.to_owned(),
            close,
            reply: reply_tx,
        },
        "terminated_producer_day_barrier",
    )
    .await?;
    Ok(reply_rx)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn complete_pending_exit_rollover(
    mut exit: PendingExit,
    handles: &mut Vec<SourceTaskHandle>,
    completed_terminations: &mut Vec<SourceTaskTermination>,
    fallback_sinks: &BTreeMap<&'static str, SourceEventSink>,
    event_rx: &mut mpsc::Receiver<SourceIngressPayload>,
    rollover_ready_rx: &mut mpsc::Receiver<ManifestRolloverReady>,
    writer_handles: &mut BTreeMap<String, SourceWriterHandle>,
    completed_writer_outcome: &mut Option<WriterStopOutcome>,
    batches: &mut PendingIngressBatches,
    coverage: &mut BTreeMap<String, CoverageStats>,
    received_rows: &mut u64,
    average_event_bytes: &mut u64,
    source_gaps: &SharedSourceGaps,
    source_queue_backpressure: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    rollover_coordinator: &mut RolloverCoordinatorState,
    raw_day_gate: &RawDayGate,
    runtime_failure: &mut Option<String>,
    deadline: Instant,
) -> Result<PendingExit> {
    let close_identity = RuntimeCloseIdentity::from_rollover_old_day(&exit.close);
    let mut synthetic_producers = BTreeSet::<&'static str>::new();
    let mut synthetic_sent = BTreeSet::<&'static str>::new();
    let mut synthetic_release_receivers = BTreeMap::<
        &'static str,
        oneshot::Receiver<std::result::Result<DayBarrierRelease, String>>,
    >::new();
    if matches!(
        rollover_coordinator.cutpoint(),
        Some(RolloverCutpoint::ReadyLatched | RolloverCutpoint::OldCommitted)
    ) && let Err(error) = rollover_coordinator.issue_stop_with_close()
    {
        let message = format!("pending_exit_stop_dispatch_failed:{error}");
        record_gap(source_gaps, message.clone());
        append_runtime_failure(runtime_failure, message);
    }
    loop {
        let mut completed_synthetic_releases = Vec::new();
        for (producer, receiver) in &mut synthetic_release_receivers {
            match receiver.try_recv() {
                Ok(Ok(DayBarrierRelease::StopWithClose(actual))) if actual == close_identity => {
                    completed_synthetic_releases.push(*producer);
                }
                Ok(Ok(DayBarrierRelease::StopWithClose(actual))) => {
                    return Err(invalid_data(format!(
                        "synthetic producer stop identity mismatch: producer={producer} expected={close_identity:?} actual={actual:?}"
                    ))
                    .into());
                }
                Ok(Ok(other)) => {
                    return Err(invalid_data(format!(
                        "synthetic producer received an invalid release: producer={producer} release={other:?}"
                    ))
                    .into());
                }
                Ok(Err(error)) => {
                    return Err(invalid_data(format!(
                        "synthetic producer stop failed: producer={producer} error={error}"
                    ))
                    .into());
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(invalid_data(format!(
                        "synthetic producer stop reply closed: producer={producer}"
                    ))
                    .into());
                }
            }
        }
        for producer in completed_synthetic_releases {
            synthetic_release_receivers.remove(producer);
        }
        let ready_latched = rollover_coordinator.ready().is_ok();
        let mut index = 0;
        while index < handles.len() {
            if !handles[index].is_finished() {
                index += 1;
                continue;
            }
            let handle = handles.swap_remove(index);
            let termination = handle.await_termination(&close_identity).await;
            let producer = logical_producer_for_source_task(termination.source_kind)?;
            let producer_fenced = raw_day_gate.source_fence_registered(producer, &exit.close);
            if !producer_fenced && termination.kind == SourceTaskTerminationKind::Unclean {
                exit.unclean = true;
                exit.unclean_cause
                    .get_or_insert(RuntimeCloseCause::CoordinatorFailure {
                        stage: "pre_fence_source_termination",
                    });
                rollover_coordinator.mark_pre_fence_unclean();
                let _ = raw_day_gate.mark_runtime_exit_unclean_with_cause(
                    &close_identity,
                    RuntimeCloseCause::CoordinatorFailure {
                        stage: "pre_fence_source_termination",
                    },
                )?;
            }
            if let Some(error) = termination.error.as_ref() {
                let prefix = if termination.kind == SourceTaskTerminationKind::Unclean {
                    "source_task_unclean"
                } else {
                    "source_task_error"
                };
                let message = format!("{prefix}:{}:{error}", termination.source_kind);
                record_gap(source_gaps, message.clone());
                if exit.cause.source_task_kind() == Some(termination.source_kind) {
                    promote_runtime_primary(runtime_failure, message);
                } else {
                    append_runtime_failure(runtime_failure, message);
                }
            }
            if !ready_latched && !producer_fenced {
                match raw_day_gate.current_state() {
                    RawDayGateState::Active(_) => {
                        raw_day_gate.register_source_fence(producer, &exit.close)?;
                    }
                    RawDayGateState::Closing(ref close) if close == &exit.close => {
                        return Err(invalid_data(format!(
                            "terminated producer was not fenced before raw-day close: producer={producer} close={close:?}"
                        ))
                        .into());
                    }
                    ref state => {
                        return Err(invalid_data(format!(
                            "terminated producer found incompatible raw-day state: producer={producer} state={state:?} close={:?}",
                            exit.close
                        ))
                        .into());
                    }
                }
            }
            synthetic_producers.insert(producer);
            completed_terminations.push(termination);
        }

        let post_fence_epoch = ready_latched.then(|| RuntimeEpoch {
            utc_day: exit.close.next_utc_day.clone(),
            generation: exit.close.next_generation,
        });
        if let Some(completion) =
            poll_writer_completion(writer_handles, raw_day_gate, post_fence_epoch.as_ref())
        {
            record_gap(source_gaps, completion.message.clone());
            append_runtime_failure(runtime_failure, completion.message);
            let worker_fenced = completion
                .worker_epoch
                .or(completion.rendezvous_epoch)
                .is_some_and(|epoch| {
                    epoch.utc_day == exit.close.next_utc_day
                        && epoch.generation == exit.close.next_generation
                });
            let reliable_old_epoch = completion.termination_kind
                == WriterTerminationKind::ReliableReturn
                && completion.lifecycle_epoch.is_some_and(|epoch| {
                    epoch.utc_day == exit.close.utc_day && epoch.generation == exit.close.generation
                });
            if !ready_latched && !reliable_old_epoch && !worker_fenced {
                rollover_coordinator.mark_pre_fence_unclean();
                exit.unclean = true;
            }
        }

        if raw_day_gate.all_source_fences_registered(&exit.close)
            && matches!(raw_day_gate.current_state(), RawDayGateState::Active(_))
        {
            raw_day_gate.seal_prepared_rollover(&exit.close).await?;
        }
        while let Ok(inflight_event) = event_rx.try_recv() {
            process_ingress_event(
                inflight_event,
                writer_handles,
                batches,
                coverage,
                received_rows,
                average_event_bytes,
                source_gaps,
                source_queue_backpressure,
                writer_queue_tracker,
                run_started,
                manifest_tx,
                rollover_coordinator,
            )
            .await?;
        }
        if !ready_latched
            && matches!(
                raw_day_gate.current_state(),
                RawDayGateState::Closing(ref close) if close == &exit.close
            )
        {
            for producer in synthetic_producers.iter().copied() {
                if synthetic_sent.contains(producer)
                    || rollover_coordinator.has_producer_barrier(producer, &exit.close)
                {
                    continue;
                }
                let sink = fallback_sinks.get(producer).ok_or_else(|| {
                    invalid_data(format!(
                        "missing fallback sink for terminated producer: {producer}"
                    ))
                })?;
                let reply =
                    enqueue_synthetic_day_barrier(sink, run_started, producer, exit.close.clone())
                        .await?;
                synthetic_release_receivers.insert(producer, reply);
                synthetic_sent.insert(producer);
            }
        }

        if rollover_coordinator.ready().is_ok()
            && handles.is_empty()
            && synthetic_release_receivers.is_empty()
        {
            while let Ok(inflight_event) = event_rx.try_recv() {
                process_ingress_event(
                    inflight_event,
                    writer_handles,
                    batches,
                    coverage,
                    received_rows,
                    average_event_bytes,
                    source_gaps,
                    source_queue_backpressure,
                    writer_queue_tracker,
                    run_started,
                    manifest_tx,
                    rollover_coordinator,
                )
                .await?;
            }
            flush_all_batches(
                writer_handles,
                batches,
                source_gaps,
                source_queue_backpressure,
                writer_queue_tracker,
                run_started,
            )
            .await?;
            let writer_outcome = shutdown_writer_workers_with_budget(
                std::mem::take(writer_handles),
                RuntimeCloseIdentity::from_rollover_next_day(&exit.close),
                deadline.saturating_duration_since(Instant::now()),
            )
            .await;
            record_writer_stop_failure(&writer_outcome, source_gaps, runtime_failure);
            *completed_writer_outcome = Some(writer_outcome);
            let refreshed = match raw_day_gate.prepared_runtime_exit() {
                Some(PreparedRuntimeExit::Rollover(refreshed)) => refreshed,
                other => {
                    return Err(invalid_data(format!(
                        "pending-exit rollover lost authoritative gate state before finalization: {other:?}"
                    ))
                    .into());
                }
            };
            exit.unclean |= refreshed.unclean || rollover_coordinator.pre_fence_unclean();
            if exit.unclean_cause.is_none() {
                exit.unclean_cause = refreshed.unclean_cause;
            }
            match rollover_coordinator
                .finalize_pending_exit_after_reports(&exit, manifest_tx)
                .await?
            {
                DurableMutationAck::Confirmed => {
                    raw_day_gate.clear_writer_rendezvous(&exit.close)?;
                    return Ok(exit);
                }
                DurableMutationAck::OutcomeUncertain(error) => {
                    return Err(invalid_data(error).into());
                }
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pending-exit rollover rendezvous timeout",
            )
            .into());
        }
        tokio::select! {
            ready = rollover_ready_rx.recv(), if rollover_coordinator.ready().is_err() => {
                let ready = ready.ok_or_else(|| {
                    invalid_data("rollover-ready channel closed during pending exit")
                })?;
                rollover_coordinator.latch_ready(ready)?;
                if let Err(error) = rollover_coordinator.issue_stop_with_close() {
                    let message = format!("pending_exit_stop_dispatch_failed:{error}");
                    record_gap(source_gaps, message.clone());
                    append_runtime_failure(runtime_failure, message);
                }
            }
            received = event_rx.recv() => {
                let inflight_event = received.ok_or_else(|| {
                    invalid_data("source event queue closed during pending-exit rollover")
                })?;
                process_ingress_event(
                    inflight_event,
                    writer_handles,
                    batches,
                    coverage,
                    received_rows,
                    average_event_bytes,
                    source_gaps,
                    source_queue_backpressure,
                    writer_queue_tracker,
                    run_started,
                    manifest_tx,
                    rollover_coordinator,
                )
                .await?;
            }
            () = sleep(remaining.min(Duration::from_millis(100))) => {}
        }
    }
}
