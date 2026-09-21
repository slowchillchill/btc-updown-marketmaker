use super::*;

#[derive(Debug)]
pub(super) enum DayBarrierRelease {
    #[cfg(test)]
    Resume,
    PrepareResume(ProducerReleaseToken),
    StopWithClose(RuntimeCloseIdentity),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ProducerReleasePhase {
    Held,
    FlushDeferred,
    ResumeSources,
    Stop(RuntimeCloseIdentity),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProducerReleaseAckKind {
    Armed,
    Flushed,
}

#[derive(Debug)]
pub(super) struct ProducerReleaseAck {
    pub(super) producer: String,
    pub(super) close: RuntimeCloseIdentity,
    pub(super) kind: ProducerReleaseAckKind,
    pub(super) error: Option<String>,
}

#[derive(Debug)]
pub(super) struct ProducerReleaseToken {
    pub(super) producer: String,
    pub(super) close: RawDayClose,
    pub(super) phase_rx: watch::Receiver<ProducerReleasePhase>,
    pub(super) ack_tx: mpsc::Sender<ProducerReleaseAck>,
}

pub(super) enum RawDaySealOutcome {
    Sealed(RawDayClose),
    SourceEnded(Result<()>),
}

async fn complete_source_return_at_pending_rollover(
    sink: &SourceEventSink,
    run_started: Instant,
    producer: &'static str,
    source_result: Result<()>,
) -> Result<()> {
    let Some(gate) = sink.source_queue_backpressure.raw_day_gate() else {
        return source_result;
    };
    let Some(PreparedRuntimeExit::Rollover(exit)) = gate.prepared_runtime_exit() else {
        return source_result;
    };
    if producer == "polymarket_rtds" {
        gate.wait_for_other_source_fences(producer, &exit.close)
            .await?;
    }
    gate.register_source_fence(producer, &exit.close)?;
    let close = gate.seal_prepared_rollover(&exit.close).await?;
    let release = send_day_barrier(sink, run_started, producer, close.clone()).await?;
    match apply_day_barrier_release(sink, producer, &close, release).await {
        Ok(()) => Err(invalid_data(format!(
            "source returned during rollover but coordinator resumed producer: {producer}"
        ))
        .into()),
        Err(error) if is_exact_coordinated_source_stop(error.as_ref(), producer, &close) => {
            source_result
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn seal_raw_day_while_source_runs<F>(
    mut source: Pin<&mut F>,
    raw_day_gate: &RawDayGate,
    close: &RawDayClose,
) -> Result<RawDaySealOutcome>
where
    F: Future<Output = Result<()>>,
{
    let seal = raw_day_gate.seal_prepared_rollover(close);
    tokio::pin!(seal);
    tokio::select! {
        biased;
        result = &mut seal => result.map(RawDaySealOutcome::Sealed),
        result = source.as_mut() => Ok(RawDaySealOutcome::SourceEnded(result)),
    }
}

pub(super) async fn run_logical_producer_with_day_barriers<F>(
    producer: &'static str,
    sink: SourceEventSink,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for producer barrier"))?;
            let raw_day_gate = raw_day_gate_for_rollover(&sink)?;
            let close = raw_day_gate.prepare_rollover(&utc_day, next_boundary_ms)?;
            raw_day_gate
                .wait_for_other_source_fences(producer, &close)
                .await?;
            raw_day_gate.register_source_fence(producer, &close)?;
            let close = if source_started {
                match seal_raw_day_while_source_runs(source.as_mut(), &raw_day_gate, &close).await?
                {
                    RawDaySealOutcome::Sealed(close) => close,
                    RawDaySealOutcome::SourceEnded(result) => return result,
                }
            } else {
                raw_day_gate.seal_prepared_rollover(&close).await?
            };
            let release = send_day_barrier(&sink, run_started, producer, close.clone()).await?;
            apply_day_barrier_release(&sink, producer, &close, release).await?;
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => {
                return complete_source_return_at_pending_rollover(
                    &sink,
                    run_started,
                    producer,
                    result,
                ).await;
            },
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

pub(super) fn raw_day_gate_for_rollover(sink: &SourceEventSink) -> Result<Arc<RawDayGate>> {
    sink.source_queue_backpressure
        .raw_day_gate()
        .ok_or_else(|| invalid_data("raw-day gate missing from producer rollover wrapper").into())
}

async fn wait_for_producer_release_phase(
    phase_rx: &mut watch::Receiver<ProducerReleasePhase>,
) -> Result<ProducerReleasePhase> {
    loop {
        let phase = phase_rx.borrow_and_update().clone();
        if phase != ProducerReleasePhase::Held {
            return Ok(phase);
        }
        phase_rx.changed().await.map_err(|_| {
            invalid_data("producer release phase owner dropped before a terminal phase")
        })?;
    }
}

pub(super) async fn apply_day_barrier_release(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
    release: DayBarrierRelease,
) -> Result<()> {
    match release {
        #[cfg(test)]
        DayBarrierRelease::Resume => {
            flush_deferred_raw_events(sink, producer, close).await?;
            Ok(())
        }
        DayBarrierRelease::StopWithClose(actual) => {
            let expected = RuntimeCloseIdentity::from_rollover_old_day(close);
            if actual != expected {
                return Err(invalid_data(format!(
                    "producer Stop close identity mismatch: producer={producer} expected={expected:?} actual={actual:?}"
                ))
                .into());
            }
            flush_accepted_deferred_raw_events_for_stop(sink, producer, close).await?;
            Err(CoordinatedSourceStop {
                producer: producer.to_owned(),
                close: actual,
            }
            .into())
        }
        DayBarrierRelease::PrepareResume(mut token) => {
            if token.producer != producer || token.close != *close {
                return Err(invalid_data(format!(
                    "producer release token identity mismatch: expected_producer={producer} expected_close={close:?} actual_producer={} actual_close={:?}",
                    token.producer, token.close
                ))
                .into());
            }
            let close_identity = RuntimeCloseIdentity::from_rollover_old_day(close);
            token
                .ack_tx
                .send(ProducerReleaseAck {
                    producer: producer.to_owned(),
                    close: close_identity.clone(),
                    kind: ProducerReleaseAckKind::Armed,
                    error: None,
                })
                .await
                .map_err(|_| invalid_data("producer release armed-ack channel closed"))?;
            match wait_for_producer_release_phase(&mut token.phase_rx).await? {
                ProducerReleasePhase::Stop(actual) => {
                    validate_release_stop_identity(close, &actual)?;
                    flush_accepted_deferred_raw_events_for_stop(sink, producer, close).await?;
                    return Err(CoordinatedSourceStop {
                        producer: producer.to_owned(),
                        close: actual,
                    }
                    .into());
                }
                ProducerReleasePhase::FlushDeferred => {}
                ProducerReleasePhase::ResumeSources => {
                    return Err(invalid_data(format!(
                        "producer release skipped deferred flush: producer={producer} close={close:?}"
                    ))
                    .into());
                }
                ProducerReleasePhase::Held => unreachable!("phase waiter excludes Held"),
            }

            let flush_error = flush_deferred_raw_events(sink, producer, close)
                .await
                .err()
                .map(|error| error.to_string());
            token
                .ack_tx
                .send(ProducerReleaseAck {
                    producer: producer.to_owned(),
                    close: close_identity,
                    kind: ProducerReleaseAckKind::Flushed,
                    error: flush_error.clone(),
                })
                .await
                .map_err(|_| invalid_data("producer deferred-flush ack channel closed"))?;
            if let Some(error) = flush_error {
                return Err(invalid_data(format!(
                    "producer deferred flush failed: producer={producer} error={error}"
                ))
                .into());
            }
            loop {
                let phase = token.phase_rx.borrow_and_update().clone();
                match phase {
                    ProducerReleasePhase::ResumeSources => return Ok(()),
                    ProducerReleasePhase::Stop(actual) => {
                        validate_release_stop_identity(close, &actual)?;
                        flush_accepted_deferred_raw_events_for_stop(sink, producer, close).await?;
                        return Err(CoordinatedSourceStop {
                            producer: producer.to_owned(),
                            close: actual,
                        }
                        .into());
                    }
                    ProducerReleasePhase::Held | ProducerReleasePhase::FlushDeferred => {
                        token.phase_rx.changed().await.map_err(|_| {
                            invalid_data(
                                "producer release phase owner dropped before resume or stop",
                            )
                        })?;
                    }
                }
            }
        }
    }
}

fn validate_release_stop_identity(
    close: &RawDayClose,
    actual: &RuntimeCloseIdentity,
) -> Result<()> {
    let old = RuntimeCloseIdentity::from_rollover_old_day(close);
    let belongs_to_committed_next_generation = actual.utc_day == close.next_utc_day
        && actual.generation == close.next_generation
        && actual.close_id != 0;
    if *actual == old || belongs_to_committed_next_generation {
        Ok(())
    } else {
        Err(invalid_data(format!(
            "producer release stop identity mismatch: rollover={close:?} actual={actual:?}"
        ))
        .into())
    }
}

fn is_exact_coordinated_source_stop(
    error: &(dyn Error + Send + Sync + 'static),
    producer: &str,
    close: &RawDayClose,
) -> bool {
    error
        .downcast_ref::<CoordinatedSourceStop>()
        .is_some_and(|stop| {
            stop.producer == producer
                && stop.close == RuntimeCloseIdentity::from_rollover_old_day(close)
        })
}

pub(super) fn coordinated_stop_matches_prepared_source_terminal(
    error: &(dyn Error + Send + Sync + 'static),
    producer: &str,
    source_kind: &str,
    raw_day_gate: &RawDayGate,
) -> bool {
    let Some(stop) = error.downcast_ref::<CoordinatedSourceStop>() else {
        return false;
    };
    let Some(prepared) = raw_day_gate.prepared_runtime_exit() else {
        return false;
    };
    let cause = match &prepared {
        PreparedRuntimeExit::SameDay(close) => &close.cause,
        PreparedRuntimeExit::Rollover(exit) => &exit.cause,
    };
    stop.producer == producer
        && stop.close == RawDayGate::runtime_exit_identity(&prepared)
        && cause.source_task_kind() == Some(source_kind)
}

async fn wait_for_source_fence_reply_or_source<F>(
    mut source: Pin<&mut F>,
    reply_rx: &mut oneshot::Receiver<std::result::Result<(), String>>,
    failure_label: &str,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    tokio::select! {
        biased;
        reply = reply_rx => {
            match reply {
                Ok(Ok(())) => Ok(None),
                Ok(Err(error)) => Err(invalid_data(format!(
                    "{failure_label} failed before day barrier:{error}"
                )).into()),
                Err(_) => Err(invalid_data(format!(
                    "{failure_label} dropped day-barrier reply"
                )).into()),
            }
        },
        result = source.as_mut() => Ok(Some(result)),
    }
}

pub(super) async fn run_polymarket_logical_producer_with_day_barriers<F>(
    sink: SourceEventSink,
    control_tx: mpsc::Sender<PolymarketSourceControl>,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for Polymarket barrier"))?;
            let raw_day_gate = raw_day_gate_for_rollover(&sink)?;
            let close = raw_day_gate.prepare_rollover(&utc_day, next_boundary_ms)?;
            if source_started {
                if let Some(source_result) = fence_polymarket_and_send_day_barrier(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &raw_day_gate,
                    &close,
                )
                .await?
                {
                    return complete_source_return_at_pending_rollover(
                        &sink,
                        run_started,
                        "polymarket",
                        source_result,
                    )
                    .await;
                }
            } else {
                raw_day_gate.register_source_fence("polymarket", &close)?;
                let close = raw_day_gate.seal_prepared_rollover(&close).await?;
                let release =
                    send_day_barrier(&sink, run_started, "polymarket", close.clone()).await?;
                apply_day_barrier_release(&sink, "polymarket", &close, release).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => {
                return complete_source_return_at_pending_rollover(
                    &sink,
                    run_started,
                    "polymarket",
                    result,
                ).await;
            },
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

pub(super) async fn fence_polymarket_and_send_day_barrier<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<PolymarketSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    raw_day_gate: &RawDayGate,
    close: &RawDayClose,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let resume_tx = {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let (sender, resume_rx) = oneshot::channel();
        control_tx
            .send(PolymarketSourceControl::Fence {
                stop_after_fence: false,
                day_boundary_utc_ms: Some(close.boundary_utc_ms),
                resume_after_barrier: Some(resume_rx),
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                invalid_data("Polymarket source control channel closed before day barrier")
            })?;
        let source_result = wait_for_source_fence_reply_or_source(
            source.as_mut(),
            &mut reply_rx,
            "Polymarket source fence",
        )
        .await?;
        if let Some(result) = source_result {
            return Ok(Some(result));
        }
        sender
    };
    raw_day_gate.register_source_fence("polymarket", close)?;
    let close = match seal_raw_day_while_source_runs(source.as_mut(), raw_day_gate, close).await? {
        RawDaySealOutcome::Sealed(close) => close,
        RawDaySealOutcome::SourceEnded(result) => return Ok(Some(result)),
    };
    let release = send_day_barrier(sink, run_started, "polymarket", close.clone()).await?;
    match apply_day_barrier_release(sink, "polymarket", &close, release).await {
        Ok(()) => {
            let _ = resume_tx.send(());
            Ok(None)
        }
        Err(error)
            if coordinated_stop_matches_prepared_source_terminal(
                error.as_ref(),
                "polymarket",
                "polymarket_market",
                raw_day_gate,
            ) =>
        {
            let _ = resume_tx.send(());
            let source_result = source.as_mut().await;
            if let Err(flush_error) =
                flush_accepted_deferred_raw_events_for_stop(sink, "polymarket", &close).await
            {
                record_gap(
                    &sink.gaps,
                    format!("polymarket_post_terminal_stop_flush_failed:{flush_error}"),
                );
                if source_result.is_ok() {
                    return Err(flush_error);
                }
            }
            match source_result {
                Ok(()) => Err(error),
                Err(source_error) => Err(source_error),
            }
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn run_aggtrade_logical_producer_with_day_barriers<F>(
    sink: SourceEventSink,
    control_tx: mpsc::Sender<AggTradeSourceControl>,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for aggTrade barrier"))?;
            let raw_day_gate = raw_day_gate_for_rollover(&sink)?;
            let close = raw_day_gate.prepare_rollover(&utc_day, next_boundary_ms)?;
            if source_started {
                if let Some(source_result) = flush_aggtrade_and_send_day_barrier(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &raw_day_gate,
                    &close,
                )
                .await?
                {
                    return complete_source_return_at_pending_rollover(
                        &sink,
                        run_started,
                        "binance_aggtrade",
                        source_result,
                    )
                    .await;
                }
            } else {
                raw_day_gate.register_source_fence("binance_aggtrade", &close)?;
                let close = raw_day_gate.seal_prepared_rollover(&close).await?;
                let release =
                    send_day_barrier(&sink, run_started, "binance_aggtrade", close.clone()).await?;
                apply_day_barrier_release(&sink, "binance_aggtrade", &close, release).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        let wait_ms = next_boundary_ms.saturating_sub(now_ms()).max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => {
                return complete_source_return_at_pending_rollover(
                    &sink,
                    run_started,
                    "binance_aggtrade",
                    result,
                ).await;
            },
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

pub(super) async fn run_depth_logical_producer_with_time_barriers<F>(
    sink: SourceEventSink,
    control_tx: mpsc::Sender<DepthSourceControl>,
    run_started: Instant,
    initial_next_boundary_ms: i64,
    initial_next_hour_boundary_ms: i64,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut next_boundary_ms = initial_next_boundary_ms;
    let mut next_hour_boundary_ms = initial_next_hour_boundary_ms;
    let mut source_started = false;
    tokio::pin!(source);
    loop {
        if now_ms() >= next_boundary_ms {
            let utc_day = utc_day_string_from_ms(next_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC day unavailable for depth barrier"))?;
            let raw_day_gate = raw_day_gate_for_rollover(&sink)?;
            let close = raw_day_gate.prepare_rollover(&utc_day, next_boundary_ms)?;
            if source_started {
                if let Some(source_result) = fence_depth_and_send_day_barrier(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &raw_day_gate,
                    &close,
                )
                .await?
                {
                    return complete_source_return_at_pending_rollover(
                        &sink,
                        run_started,
                        "binance_depth",
                        source_result,
                    )
                    .await;
                }
            } else {
                raw_day_gate.register_source_fence("binance_depth", &close)?;
                let close = raw_day_gate.seal_prepared_rollover(&close).await?;
                let release =
                    send_day_barrier(&sink, run_started, "binance_depth", close.clone()).await?;
                apply_day_barrier_release(&sink, "binance_depth", &close, release).await?;
            }
            next_boundary_ms = next_boundary_ms.saturating_add(UTC_DAY_MS);
            continue;
        }
        if now_ms() >= next_hour_boundary_ms {
            let utc_hour = utc_hour_string_from_ms(next_hour_boundary_ms.saturating_sub(1))
                .ok_or_else(|| invalid_data("UTC hour unavailable for depth barrier"))?;
            if source_started {
                if let Some(source_result) = fence_depth_and_finalize_snapshot_hour(
                    source.as_mut(),
                    &control_tx,
                    &sink,
                    run_started,
                    &utc_hour,
                )
                .await?
                {
                    return source_result;
                }
            } else {
                send_source_hour_finalize(&sink, run_started, "binance_depth_snapshot", utc_hour)
                    .await?;
            }
            next_hour_boundary_ms = next_hour_boundary_ms.saturating_add(UTC_HOUR_MS);
            continue;
        }
        let wait_ms = next_boundary_ms
            .min(next_hour_boundary_ms)
            .saturating_sub(now_ms())
            .max(0) as u64;
        source_started = true;
        tokio::select! {
            result = &mut source => {
                return complete_source_return_at_pending_rollover(
                    &sink,
                    run_started,
                    "binance_depth",
                    result,
                ).await;
            },
            () = sleep(Duration::from_millis(wait_ms)) => {}
        }
    }
}

async fn fence_depth_and_finalize_snapshot_hour<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<DepthSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    utc_hour: &str,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let (reply_tx, mut reply_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    control_tx
        .send(DepthSourceControl::Fence {
            kind: DepthFenceKind::HourFinalize,
            stop_after_fence: false,
            resume_after_barrier: Some(resume_rx),
            reply: reply_tx,
        })
        .await
        .map_err(|_| invalid_data("depth source control channel closed before hour barrier"))?;
    tokio::select! {
        result = source.as_mut() => Ok(Some(result)),
        reply = &mut reply_rx => {
            match reply {
                Ok(Ok(())) => {
                    let barrier_result = send_source_hour_finalize(
                        sink,
                        run_started,
                        "binance_depth_snapshot",
                        utc_hour.to_owned(),
                    )
                    .await;
                    let _ = resume_tx.send(());
                    barrier_result?;
                    Ok(None)
                }
                Ok(Err(error)) => Err(invalid_data(format!(
                    "depth source fence failed before hour barrier:{error}"
                )).into()),
                Err(_) => Err(invalid_data(
                    "depth source dropped hour-barrier fence reply",
                ).into()),
            }
        }
    }
}

pub(super) async fn fence_depth_and_send_day_barrier<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<DepthSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    raw_day_gate: &RawDayGate,
    close: &RawDayClose,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let resume_tx = {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let (sender, resume_rx) = oneshot::channel();
        control_tx
            .send(DepthSourceControl::Fence {
                kind: DepthFenceKind::ResetContinuity,
                stop_after_fence: false,
                resume_after_barrier: Some(resume_rx),
                reply: reply_tx,
            })
            .await
            .map_err(|_| invalid_data("depth source control channel closed before day barrier"))?;
        let source_result = wait_for_source_fence_reply_or_source(
            source.as_mut(),
            &mut reply_rx,
            "depth source fence",
        )
        .await?;
        if let Some(result) = source_result {
            return Ok(Some(result));
        }
        sender
    };
    raw_day_gate.register_source_fence("binance_depth", close)?;
    let close = match seal_raw_day_while_source_runs(source.as_mut(), raw_day_gate, close).await? {
        RawDaySealOutcome::Sealed(close) => close,
        RawDaySealOutcome::SourceEnded(result) => return Ok(Some(result)),
    };
    let release = send_day_barrier(sink, run_started, "binance_depth", close.clone()).await?;
    apply_day_barrier_release(sink, "binance_depth", &close, release).await?;
    let _ = resume_tx.send(());
    Ok(None)
}

async fn flush_aggtrade_and_send_day_barrier<F>(
    mut source: Pin<&mut F>,
    control_tx: &mpsc::Sender<AggTradeSourceControl>,
    sink: &SourceEventSink,
    run_started: Instant,
    raw_day_gate: &RawDayGate,
    close: &RawDayClose,
) -> Result<Option<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    let resume_tx = {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let (sender, resume_rx) = oneshot::channel();
        control_tx
            .send(AggTradeSourceControl::Fence {
                stop_after_fence: false,
                resume_after_barrier: Some(resume_rx),
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                invalid_data("aggTrade source control channel closed before day barrier")
            })?;
        let source_result = wait_for_source_fence_reply_or_source(
            source.as_mut(),
            &mut reply_rx,
            "aggTrade source fence",
        )
        .await?;
        if let Some(result) = source_result {
            return Ok(Some(result));
        }
        sender
    };
    raw_day_gate.register_source_fence("binance_aggtrade", close)?;
    let close = match seal_raw_day_while_source_runs(source.as_mut(), raw_day_gate, close).await? {
        RawDaySealOutcome::Sealed(close) => close,
        RawDaySealOutcome::SourceEnded(result) => return Ok(Some(result)),
    };
    let release = send_day_barrier(sink, run_started, "binance_aggtrade", close.clone()).await?;
    apply_day_barrier_release(sink, "binance_aggtrade", &close, release).await?;
    let _ = resume_tx.send(());
    Ok(None)
}
