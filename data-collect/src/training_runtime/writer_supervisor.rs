use super::*;

pub(super) const DEFAULT_WRITER_CHANNEL_CAPACITY: usize = 8;

pub(super) fn record_writer_stop_failure(
    outcome: &WriterStopOutcome,
    source_gaps: &SharedSourceGaps,
    runtime_failure: &mut Option<String>,
) {
    if outcome.errors.is_empty() {
        return;
    }
    let message = format!("writer_finalize_failed:{}", outcome.errors.join(" | "));
    record_gap(source_gaps, message.clone());
    append_runtime_failure(runtime_failure, message);
}

pub(super) fn writer_outputs_or_record_failure(
    outcome: WriterStopOutcome,
    source_gaps: &SharedSourceGaps,
    runtime_failure: &mut Option<String>,
) -> (Vec<SourceWriterOutput>, bool, bool) {
    record_writer_stop_failure(&outcome, source_gaps, runtime_failure);
    take_writer_stop_outcome(outcome)
}

pub(super) fn take_writer_stop_outcome(
    outcome: WriterStopOutcome,
) -> (Vec<SourceWriterOutput>, bool, bool) {
    (
        outcome.outputs,
        outcome.all_terminal,
        outcome.all_close_proven,
    )
}

pub(super) struct WriterStartupGate {
    participant: StartupParticipant,
    ready_tx: mpsc::Sender<StartupParticipant>,
    release_rx: oneshot::Receiver<StartupReleasePhase>,
}

#[cfg(test)]
pub(super) fn start_writer_workers(
    dataset_root: &Path,
    provenance: &TrainingArtifactIdentity,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &Arc<SourceQueueBackpressureStats>,
) -> Result<BTreeMap<String, SourceWriterHandle>> {
    let raw_day_gate = runtime_stats.raw_day_gate();
    let (handles, releases) = start_writer_workers_inner(
        dataset_root,
        provenance,
        manifest_tx,
        runtime_stats,
        None,
        raw_day_gate,
        None,
        |_| Ok(()),
    )?;
    debug_assert!(releases.is_empty());
    Ok(handles)
}

pub(super) fn start_writer_workers_gated(
    dataset_root: &Path,
    provenance: &TrainingArtifactIdentity,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &Arc<SourceQueueBackpressureStats>,
    ready_tx: &mpsc::Sender<StartupParticipant>,
    raw_day_gate: Arc<RawDayGate>,
    dataset_lease: Option<Arc<File>>,
) -> Result<(
    BTreeMap<String, SourceWriterHandle>,
    Vec<StartupReleaseSender>,
)> {
    start_writer_workers_inner(
        dataset_root,
        provenance,
        manifest_tx,
        runtime_stats,
        Some(ready_tx),
        Some(raw_day_gate),
        dataset_lease,
        |_| Ok(()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn start_writer_workers_inner<F>(
    dataset_root: &Path,
    provenance: &TrainingArtifactIdentity,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &Arc<SourceQueueBackpressureStats>,
    ready_tx: Option<&mpsc::Sender<StartupParticipant>>,
    raw_day_gate: Option<Arc<RawDayGate>>,
    dataset_lease: Option<Arc<File>>,
    mut before_spawn: F,
) -> Result<(
    BTreeMap<String, SourceWriterHandle>,
    Vec<StartupReleaseSender>,
)>
where
    F: FnMut(usize) -> io::Result<()>,
{
    let mut handles = BTreeMap::new();
    let mut releases = Vec::new();
    for (index, source_kind) in training_source_kinds().iter().enumerate() {
        if let Err(error) = before_spawn(index) {
            converge_partial_writer_spawn(handles, releases, STARTUP_GATE_TIMEOUT);
            return Err(error.into());
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
        let queue_rows = Arc::new(AtomicU64::new(0));
        let queue_rows_for_worker = Arc::clone(&queue_rows);
        let accepting_commands = Arc::new(Mutex::new(true));
        let accepting_commands_for_worker = Arc::clone(&accepting_commands);
        let runtime_stats = Arc::clone(runtime_stats);
        let dataset_root = dataset_root.to_path_buf();
        let provenance = provenance.clone();
        let manifest_tx = manifest_tx.clone();
        let source_kind_owned = (*source_kind).to_owned();
        let report_source_kind = source_kind_owned.clone();
        let writer_schema_version =
            TypedParquetSegmentWriter::current_schema_version(source_kind)?.to_owned();
        let worker_schema_version = writer_schema_version.clone();
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let writer_epoch = WriterEpochCell::default();
        let worker_epoch = writer_epoch.clone();
        let lifecycle_gate = raw_day_gate.clone();
        let writer_rendezvous_gate = raw_day_gate.clone();
        let writer_dataset_lease = dataset_lease.clone();
        let startup_gate = ready_tx.map(|ready_tx| {
            let participant = StartupParticipant::writer(source_kind);
            let (release_tx, release_rx) = oneshot::channel();
            releases.push(StartupReleaseSender {
                participant,
                sender: release_tx,
            });
            WriterStartupGate {
                participant,
                ready_tx: ready_tx.clone(),
                release_rx,
            }
        });
        let thread = match std::thread::Builder::new()
            .name(format!("data-collect-writer-{source_kind_owned}"))
            .spawn(move || {
                let _dataset_lease = writer_dataset_lease;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    source_writer_worker_with_startup(
                        dataset_root,
                        provenance,
                        source_kind_owned,
                        worker_schema_version,
                        rx,
                        queue_rows_for_worker,
                        accepting_commands_for_worker,
                        runtime_stats,
                        manifest_tx,
                        startup_gate,
                        worker_epoch.clone(),
                        writer_rendezvous_gate,
                    )
                }));
                let outcome = match result {
                    Ok(result) => WriterTaskOutcome::Returned(result),
                    Err(payload) => WriterTaskOutcome::Panicked(
                        payload
                            .downcast_ref::<&str>()
                            .map(|message| (*message).to_owned())
                            .or_else(|| payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "writer thread panicked".to_owned()),
                    ),
                };
                let lifecycle_close = match &outcome {
                    WriterTaskOutcome::Returned(Ok(output)) => output.coordinated_close.clone(),
                    WriterTaskOutcome::Returned(Err(_))
                    | WriterTaskOutcome::Panicked(_)
                    | WriterTaskOutcome::MissingReport(_) => None,
                };
                let worker_epoch_snapshot = worker_epoch.snapshot();
                let lifecycle = if lifecycle_close.is_none() {
                    lifecycle_gate.as_ref().map(|gate| {
                        let cause = match &outcome {
                            WriterTaskOutcome::Panicked(_) => RuntimeCloseCause::WriterPanic {
                                source_kind: report_source_kind.clone(),
                            },
                            _ => RuntimeCloseCause::WriterReturn {
                                source_kind: report_source_kind.clone(),
                            },
                        };
                        let unclean_cause = matches!(&outcome, WriterTaskOutcome::Panicked(_))
                            .then_some(cause.clone());
                        gate.observe_source_terminal_now(
                            cause,
                            unclean_cause,
                            worker_epoch_snapshot.as_ref(),
                        )
                    })
                } else {
                    None
                };
                let first_observed_utc_ms = lifecycle
                    .as_ref()
                    .map_or_else(now_ms, |lifecycle| lifecycle.observed_utc_ms);
                let lifecycle_epoch = lifecycle
                    .as_ref()
                    .and_then(|lifecycle| lifecycle.epoch.clone())
                    .or_else(|| {
                        lifecycle_close.as_ref().map(|close| RuntimeEpoch {
                            utc_day: close.utc_day.clone(),
                            generation: close.generation,
                        })
                    });
                let termination_kind = if lifecycle_close.is_some() {
                    WriterTerminationKind::CoordinatedClose
                } else {
                    lifecycle.as_ref().map_or_else(
                        || match &outcome {
                            WriterTaskOutcome::Panicked(_)
                            | WriterTaskOutcome::MissingReport(_) => WriterTerminationKind::Unclean,
                            WriterTaskOutcome::Returned(_) => WriterTerminationKind::ReliableReturn,
                        },
                        |lifecycle| match lifecycle.termination_kind {
                            SourceTaskTerminationKind::ReliableReturn => {
                                WriterTerminationKind::ReliableReturn
                            }
                            SourceTaskTerminationKind::CoordinatedAbort
                            | SourceTaskTerminationKind::Unclean => WriterTerminationKind::Unclean,
                        },
                    )
                };
                let lifecycle_record_error =
                    lifecycle.and_then(|lifecycle| lifecycle.lifecycle_record_error);
                let _ = result_tx.send(WriterTaskReport {
                    source_kind: report_source_kind,
                    first_observed_utc_ms,
                    worker_epoch: worker_epoch_snapshot,
                    lifecycle_epoch,
                    termination_kind,
                    lifecycle_close,
                    lifecycle_record_error,
                    outcome,
                });
            }) {
            Ok(thread) => thread,
            Err(error) => {
                converge_partial_writer_spawn(handles, releases, STARTUP_GATE_TIMEOUT);
                return Err(
                    io::Error::other(format!("spawn writer thread failed: {error}")).into(),
                );
            }
        };
        handles.insert(
            (*source_kind).to_owned(),
            SourceWriterHandle {
                schema_version: writer_schema_version,
                tx,
                queue_rows,
                accepting_commands,
                result_rx,
                completed_result: None,
                thread: Some(thread),
            },
        );
    }
    Ok((handles, releases))
}

fn converge_partial_writer_spawn(
    handles: BTreeMap<String, SourceWriterHandle>,
    releases: Vec<StartupReleaseSender>,
    budget: Duration,
) {
    drop(releases);
    let deadline = Instant::now() + budget;
    for (_, writer) in handles {
        let SourceWriterHandle {
            schema_version: _,
            tx,
            queue_rows: _,
            accepting_commands: _,
            result_rx,
            completed_result: _,
            thread,
        } = writer;
        drop(tx);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            let _ = result_rx.recv_timeout(remaining);
        }
        let Some(thread) = thread else {
            continue;
        };
        while !thread.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        if thread.is_finished() {
            let _ = thread.join();
        }
    }
}

pub(super) struct WriterCompletionNotice {
    pub(super) message: String,
    pub(super) worker_epoch: Option<RuntimeEpoch>,
    pub(super) rendezvous_epoch: Option<RuntimeEpoch>,
    pub(super) lifecycle_epoch: Option<RuntimeEpoch>,
    pub(super) termination_kind: WriterTerminationKind,
}

pub(super) fn poll_writer_completion(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    raw_day_gate: &RawDayGate,
    post_fence_epoch: Option<&RuntimeEpoch>,
) -> Option<WriterCompletionNotice> {
    for (source_kind, writer) in writers {
        if writer.completed_result.is_none() {
            match writer.result_rx.try_recv() {
                Ok(result) => writer.completed_result = Some(result),
                Err(std::sync::mpsc::TryRecvError::Empty) => continue,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    let message = format!("writer_result_channel_disconnected:{source_kind}");
                    let cause = RuntimeCloseCause::WriterReportDisconnected {
                        source_kind: source_kind.clone(),
                    };
                    let rendezvous_epoch = post_fence_epoch
                        .cloned()
                        .or_else(|| raw_day_gate.writer_rendezvous_epoch());
                    let lifecycle = raw_day_gate.observe_source_terminal_now(
                        cause.clone(),
                        Some(cause),
                        rendezvous_epoch.as_ref(),
                    );
                    let lifecycle_epoch = lifecycle.epoch.clone();
                    writer.completed_result = Some(WriterTaskReport {
                        source_kind: source_kind.clone(),
                        first_observed_utc_ms: lifecycle.observed_utc_ms,
                        worker_epoch: None,
                        lifecycle_epoch: lifecycle.epoch,
                        termination_kind: WriterTerminationKind::Unclean,
                        lifecycle_close: None,
                        lifecycle_record_error: lifecycle.lifecycle_record_error,
                        outcome: WriterTaskOutcome::MissingReport(message.clone()),
                    });
                    return Some(WriterCompletionNotice {
                        message,
                        worker_epoch: None,
                        rendezvous_epoch,
                        lifecycle_epoch,
                        termination_kind: WriterTerminationKind::Unclean,
                    });
                }
            }
        }
        if let Some(report) = writer.completed_result.as_ref() {
            let message = match &report.outcome {
                WriterTaskOutcome::Returned(Ok(_)) => {
                    format!("writer_ended_unexpectedly:{source_kind}")
                }
                WriterTaskOutcome::Returned(Err(error)) => {
                    format!("writer_failed:{source_kind}:{error}")
                }
                WriterTaskOutcome::Panicked(error) => {
                    format!("writer_panicked:{source_kind}:{error}")
                }
                WriterTaskOutcome::MissingReport(error) => error.clone(),
            };
            return Some(WriterCompletionNotice {
                message,
                worker_epoch: report.worker_epoch.clone(),
                rendezvous_epoch: raw_day_gate.writer_rendezvous_epoch(),
                lifecycle_epoch: report.lifecycle_epoch.clone(),
                termination_kind: report.termination_kind,
            });
        }
    }
    None
}

#[cfg(test)]
pub(super) fn start_writer_workers_with_hung_result(
    dataset_root: &Path,
    provenance: &TrainingArtifactIdentity,
    dataset_lease: Option<Arc<File>>,
) -> Result<BTreeMap<String, SourceWriterHandle>> {
    let _ = (dataset_root, provenance);
    let mut handles = BTreeMap::new();
    let (tx, _command_rx) = std::sync::mpsc::sync_channel(DEFAULT_WRITER_CHANNEL_CAPACITY);
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("data-collect-writer-hung".to_owned())
        .spawn(move || {
            let _dataset_lease = dataset_lease;
            let _result_tx = result_tx;
            std::thread::sleep(Duration::from_secs(60));
        })
        .map_err(|err| io::Error::other(format!("spawn hung writer thread failed: {err}")))?;
    handles.insert(
        "hung_writer".to_owned(),
        SourceWriterHandle {
            schema_version: "unknown_typed_layout".to_owned(),
            tx,
            queue_rows: Arc::new(AtomicU64::new(0)),
            accepting_commands: Arc::new(Mutex::new(true)),
            result_rx,
            completed_result: None,
            thread: Some(thread),
        },
    );
    Ok(handles)
}

pub(super) async fn flush_source_batch(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    source_kind: &str,
    batch: &mut Vec<InflightRawEvent>,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = batch.len() as u64;
    let events = std::mem::take(batch);
    let drop_facts = events
        .iter()
        .map(|event| {
            (
                event.event.source_kind.clone(),
                event.event.recv_time_utc_ms,
            )
        })
        .collect::<Vec<_>>();
    send_writer_batch(
        writers,
        source_kind,
        WriterCommand::Events(events),
        rows,
        &drop_facts,
        gaps,
        runtime_stats,
        writer_queue_tracker,
        run_started,
    )
    .await
}

pub(super) async fn flush_polymarket_orderbook_v3_batch(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batch: &mut Vec<InflightPolymarketOrderbookV3>,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = batch.len() as u64;
    let events = std::mem::take(batch);
    let drop_facts = events
        .iter()
        .map(|event| {
            (
                "polymarket_market".to_owned(),
                event.message.envelope.timestamp_received,
            )
        })
        .collect::<Vec<_>>();
    send_writer_batch(
        writers,
        "polymarket_market",
        WriterCommand::PolymarketOrderbookV3(events),
        rows,
        &drop_facts,
        gaps,
        runtime_stats,
        writer_queue_tracker,
        run_started,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_writer_batch(
    writers: &BTreeMap<String, SourceWriterHandle>,
    source_kind: &str,
    mut command: WriterCommand,
    rows: u64,
    drop_facts: &[(String, i64)],
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    let Some(writer) = writers.get(source_kind) else {
        writer_queue_tracker.record_drop(rows);
        record_writer_drop_facts(runtime_stats, drop_facts);
        let gap = format!("writer_missing_for_source:{source_kind}");
        record_gap(gaps, gap.clone());
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
    };
    writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
    match try_send_writer_command(writer, command) {
        Ok(()) => {
            writer_queue_tracker.observe(
                monotonic_ms(run_started),
                writers
                    .values()
                    .map(|handle| handle.queue_rows.load(Ordering::Relaxed))
                    .sum::<u64>(),
                0,
            );
            return Ok(());
        }
        Err(std::sync::mpsc::TrySendError::Full(returned)) => {
            writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
            writer_queue_tracker.record_backpressure_wait();
            command = returned;
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
            writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, drop_facts);
            let gap = format!("writer_queue_disconnected:{source_kind}");
            record_gap(gaps, gap.clone());
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
        }
    }
    match timeout(BUFFER_ACQUIRE_TIMEOUT, async {
        loop {
            writer.queue_rows.fetch_add(rows, Ordering::Relaxed);
            match try_send_writer_command(writer, command) {
                Ok(()) => return None,
                Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                    writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
                    command = returned;
                    sleep(Duration::from_millis(50)).await;
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    writer.queue_rows.fetch_sub(rows, Ordering::Relaxed);
                    return Some(format!("writer_queue_disconnected:{source_kind}"));
                }
            }
        }
    })
    .await
    {
        Ok(Some(gap)) => {
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, drop_facts);
            record_gap(gaps, gap.clone());
            Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into())
        }
        Ok(None) => Ok(()),
        Err(_) => {
            writer_queue_tracker.record_backpressure_timeout();
            writer_queue_tracker.record_drop(rows);
            record_writer_drop_facts(runtime_stats, drop_facts);
            let gap = format!("writer_queue_backpressure_timeout:{source_kind}");
            record_gap(gaps, gap.clone());
            Err(io::Error::new(io::ErrorKind::TimedOut, gap).into())
        }
    }
}

fn try_send_writer_command(
    writer: &SourceWriterHandle,
    command: WriterCommand,
) -> std::result::Result<(), std::sync::mpsc::TrySendError<WriterCommand>> {
    let accepting = writer
        .accepting_commands
        .lock()
        .expect("writer accepting-command mutex poisoned");
    if !*accepting {
        return Err(std::sync::mpsc::TrySendError::Disconnected(command));
    }
    writer.tx.try_send(command)
}

fn record_writer_drop_facts(
    runtime_stats: &SourceQueueBackpressureStats,
    drop_facts: &[(String, i64)],
) {
    runtime_stats.record_drop(drop_facts.len() as u64);
    for (source_kind, recv_time_utc_ms) in drop_facts {
        runtime_stats.record_operational_drop(source_kind, *recv_time_utc_ms, 1);
    }
}

pub(super) async fn send_writer_day_finalize(
    writers: &BTreeMap<String, SourceWriterHandle>,
    producer: &str,
    close: &RawDayClose,
) -> Result<()> {
    for source_kind in producer_writer_sources(producer)? {
        let writer = writers.get(source_kind).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("writer missing for day barrier: {source_kind}"),
            )
        })?;
        let mut command = WriterCommand::FinalizeDay {
            close: close.clone(),
        };
        timeout(BUFFER_ACQUIRE_TIMEOUT, async {
            loop {
                match try_send_writer_command(writer, command) {
                    Ok(()) => return Ok(()),
                    Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                        command = returned;
                        sleep(Duration::from_millis(50)).await;
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            format!("writer queue disconnected during day barrier: {source_kind}"),
                        ));
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("writer day barrier timeout: {source_kind}"),
            )
        })??;
    }
    Ok(())
}

pub(super) async fn send_writer_hour_finalize(
    writers: &BTreeMap<String, SourceWriterHandle>,
    source_kind: &str,
    utc_hour: &str,
) -> Result<()> {
    let writer = writers.get(source_kind).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("writer missing for hour finalize: {source_kind}"),
        )
    })?;
    let mut command = WriterCommand::FinalizeHour {
        utc_hour: utc_hour.to_owned(),
    };
    timeout(BUFFER_ACQUIRE_TIMEOUT, async {
        loop {
            match try_send_writer_command(writer, command) {
                Ok(()) => return Ok(()),
                Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                    command = returned;
                    sleep(Duration::from_millis(50)).await;
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("writer queue disconnected during hour finalize: {source_kind}"),
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("writer hour finalize timeout: {source_kind}"),
        )
    })??;
    Ok(())
}

pub(super) async fn stop_writer_workers_with_budget(
    writers: BTreeMap<String, SourceWriterHandle>,
    budget: Duration,
) -> WriterStopOutcome {
    stop_writer_workers_with_optional_close(writers, None, budget).await
}

pub(super) async fn shutdown_writer_workers_with_budget(
    writers: BTreeMap<String, SourceWriterHandle>,
    close: RuntimeCloseIdentity,
    budget: Duration,
) -> WriterStopOutcome {
    stop_writer_workers_with_optional_close(writers, Some(close), budget).await
}

async fn send_writer_shutdown(
    source_kind: &str,
    writer: &SourceWriterHandle,
    close: &RuntimeCloseIdentity,
    deadline: Instant,
) -> std::result::Result<(), String> {
    let mut command = WriterCommand::Shutdown {
        close: close.clone(),
    };
    loop {
        command = {
            let mut accepting = writer
                .accepting_commands
                .lock()
                .expect("writer accepting-command mutex poisoned");
            if !*accepting {
                return Err(format!(
                    "{source_kind} writer stopped accepting before coordinated shutdown"
                ));
            }
            match writer.tx.try_send(command) {
                Ok(()) => {
                    *accepting = false;
                    return Ok(());
                }
                Err(std::sync::mpsc::TrySendError::Full(returned)) => returned,
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    return Err(format!(
                        "{source_kind} writer queue disconnected before coordinated shutdown"
                    ));
                }
            }
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "{source_kind} writer coordinated-shutdown command timeout"
            ));
        }
        sleep(remaining.min(Duration::from_millis(25))).await;
    }
}

fn collect_writer_report(
    source_kind: &str,
    report: WriterTaskReport,
    expected_close: Option<&RuntimeCloseIdentity>,
    outputs: &mut Vec<SourceWriterOutput>,
    errors: &mut Vec<String>,
    proven: &mut BTreeSet<String>,
) {
    let report_identity_matches = report.source_kind == source_kind;
    if !report_identity_matches {
        errors.push(format!(
            "writer result identity mismatch: expected={source_kind} actual={}",
            report.source_kind
        ));
    }
    if let Some(error) = report.lifecycle_record_error.as_ref() {
        errors.push(format!(
            "{source_kind} writer lifecycle recording failed at {}: {error}",
            report.first_observed_utc_ms
        ));
    }
    let lifecycle_epoch_matches = expected_close.is_none_or(|expected| {
        report.lifecycle_epoch.as_ref()
            == Some(&RuntimeEpoch {
                utc_day: expected.utc_day.clone(),
                generation: expected.generation,
            })
    });
    let reliable_return_proves_close = report_identity_matches
        && report.termination_kind == WriterTerminationKind::ReliableReturn
        && lifecycle_epoch_matches
        && report.lifecycle_record_error.is_none()
        && matches!(&report.outcome, WriterTaskOutcome::Returned(_));
    match report.outcome {
        WriterTaskOutcome::Returned(Ok(output)) => {
            let identity_matches = output.source_kind == source_kind && report_identity_matches;
            let coordinated_close_matches = expected_close.is_none_or(|expected| {
                output.coordinated_close.as_ref() == Some(expected)
                    && report.lifecycle_close.as_ref() == Some(expected)
                    && report.worker_epoch.as_ref()
                        == Some(&RuntimeEpoch {
                            utc_day: expected.utc_day.clone(),
                            generation: expected.generation,
                        })
                    && lifecycle_epoch_matches
                    && report.lifecycle_record_error.is_none()
            });
            let close_proven = match report.termination_kind {
                WriterTerminationKind::CoordinatedClose => coordinated_close_matches,
                WriterTerminationKind::ReliableReturn => reliable_return_proves_close,
                WriterTerminationKind::Unclean => false,
            };
            if !identity_matches {
                errors.push(format!(
                    "writer output identity mismatch: expected={source_kind} actual={}",
                    output.source_kind
                ));
            }
            if let Some(expected) = expected_close
                && !close_proven
            {
                errors.push(format!(
                    "{source_kind} writer terminal proof mismatch: expected={expected:?} kind={:?} output={:?} report={:?} worker_epoch={:?} lifecycle_epoch={:?}",
                    report.termination_kind,
                    output.coordinated_close,
                    report.lifecycle_close,
                    report.worker_epoch,
                    report.lifecycle_epoch,
                ));
            }
            if identity_matches && close_proven && expected_close.is_some() {
                proven.insert(source_kind.to_owned());
            }
            outputs.push(output);
        }
        WriterTaskOutcome::Returned(Err(error)) => {
            if expected_close.is_some() && reliable_return_proves_close {
                proven.insert(source_kind.to_owned());
            } else if let Some(expected) = expected_close {
                errors.push(format!(
                    "{source_kind} writer reliable-return proof mismatch: expected={expected:?} kind={:?} lifecycle_epoch={:?}",
                    report.termination_kind, report.lifecycle_epoch
                ));
            }
            errors.push(format!(
                "{source_kind} writer failed at {}: {error}",
                report.first_observed_utc_ms
            ));
        }
        WriterTaskOutcome::Panicked(error) => {
            errors.push(format!(
                "{source_kind} writer panicked at {}: {error}",
                report.first_observed_utc_ms
            ));
        }
        WriterTaskOutcome::MissingReport(error) => {
            errors.push(format!(
                "{source_kind} writer report missing at {}: {error}",
                report.first_observed_utc_ms
            ));
        }
    }
}

async fn stop_writer_workers_with_optional_close(
    mut writers: BTreeMap<String, SourceWriterHandle>,
    expected_close: Option<RuntimeCloseIdentity>,
    budget: Duration,
) -> WriterStopOutcome {
    let deadline = Instant::now() + budget;
    let mut errors = Vec::new();
    let required = training_source_kinds()
        .iter()
        .map(|source| (*source).to_owned())
        .collect::<BTreeSet<_>>();
    let actual = writers.keys().cloned().collect::<BTreeSet<_>>();
    let mandatory_keys_exact = expected_close.is_none() || actual == required;
    if expected_close.is_some() && !mandatory_keys_exact {
        let missing = required.difference(&actual).cloned().collect::<Vec<_>>();
        let extra = actual.difference(&required).cloned().collect::<Vec<_>>();
        errors.push(format!(
            "writer mandatory key mismatch: missing={missing:?} extra={extra:?}"
        ));
    }
    if let Some(close) = expected_close.as_ref() {
        for (source_kind, writer) in &writers {
            if writer.completed_result.is_some() {
                continue;
            }
            if let Err(error) = send_writer_shutdown(source_kind, writer, close, deadline).await {
                errors.push(error);
            }
        }
    }

    let mut receivers = Vec::new();
    let mut threads = Vec::new();
    let mut outputs = Vec::new();
    let mut proven = BTreeSet::new();
    for (source_kind, writer) in std::mem::take(&mut writers) {
        let SourceWriterHandle {
            schema_version: _,
            tx,
            queue_rows: _,
            accepting_commands: _,
            result_rx,
            completed_result,
            thread,
        } = writer;
        drop(tx);
        threads.push((source_kind.clone(), thread));
        match completed_result {
            Some(report) => collect_writer_report(
                &source_kind,
                report,
                expected_close.as_ref(),
                &mut outputs,
                &mut errors,
                &mut proven,
            ),
            None => receivers.push((source_kind, result_rx)),
        }
    }
    for (source_kind, result_rx) in receivers {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            match result_rx.try_recv() {
                Ok(report) => collect_writer_report(
                    &source_kind,
                    report,
                    expected_close.as_ref(),
                    &mut outputs,
                    &mut errors,
                    &mut proven,
                ),
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    errors.push(format!(
                        "writer finalize timeout before {source_kind} completed"
                    ));
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    errors.push(format!("{source_kind} writer result channel disconnected"));
                }
            }
            continue;
        }
        match result_rx.recv_timeout(remaining) {
            Ok(report) => collect_writer_report(
                &source_kind,
                report,
                expected_close.as_ref(),
                &mut outputs,
                &mut errors,
                &mut proven,
            ),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                errors.push(format!("writer finalize timeout for {source_kind}"));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                errors.push(format!("{source_kind} writer result channel disconnected"));
            }
        }
    }
    let mut all_terminal = true;
    for (source_kind, thread) in threads {
        let Some(thread) = thread else {
            all_terminal = false;
            proven.remove(&source_kind);
            errors.push(format!("{source_kind} writer thread owner missing"));
            continue;
        };
        while !thread.is_finished() && Instant::now() < deadline {
            sleep(Duration::from_millis(1)).await;
        }
        if !thread.is_finished() {
            all_terminal = false;
            proven.remove(&source_kind);
            errors.push(format!("writer thread join timeout for {source_kind}"));
            continue;
        }
        if thread.join().is_err() {
            proven.remove(&source_kind);
            errors.push(format!("writer thread join panicked for {source_kind}"));
        }
    }
    let all_close_proven = expected_close.is_none()
        || (mandatory_keys_exact && proven == required && actual == required);
    WriterStopOutcome {
        outputs,
        errors,
        all_terminal,
        all_close_proven,
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn source_writer_worker(
    dataset_root: PathBuf,
    provenance: TrainingArtifactIdentity,
    source_kind: String,
    rx: std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    accepting_commands: Arc<Mutex<bool>>,
    runtime_stats: Arc<SourceQueueBackpressureStats>,
    manifest_tx: mpsc::Sender<ManifestCommand>,
) -> Result<SourceWriterOutput> {
    let writer_schema_version =
        TypedParquetSegmentWriter::current_schema_version(&source_kind)?.to_owned();
    source_writer_worker_with_startup(
        dataset_root,
        provenance,
        source_kind,
        writer_schema_version,
        rx,
        queue_rows,
        accepting_commands,
        runtime_stats,
        manifest_tx,
        None,
        WriterEpochCell::default(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn source_writer_worker_with_startup(
    dataset_root: PathBuf,
    provenance: TrainingArtifactIdentity,
    source_kind: String,
    writer_schema_version: String,
    rx: std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: Arc<AtomicU64>,
    accepting_commands: Arc<Mutex<bool>>,
    runtime_stats: Arc<SourceQueueBackpressureStats>,
    manifest_tx: mpsc::Sender<ManifestCommand>,
    startup_gate: Option<WriterStartupGate>,
    writer_epoch: WriterEpochCell,
    raw_day_gate: Option<Arc<RawDayGate>>,
) -> Result<SourceWriterOutput> {
    let mut active = None::<ActiveSegment>;
    let mut polymarket_v3_sequence_state = (source_kind == "polymarket_market"
        && writer_schema_version == POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION)
        .then(PolymarketOrderbookV3WriteSequenceState::default);
    let mut finalized_segments = Vec::new();
    let mut stats = SourceWriterStatsBuilder::new(source_kind.clone());
    let partition_policy = HourlyPartitionPolicy::new(&dataset_root)?;
    let mut expected_raw_epoch = None::<RawDayPermit>;
    let mut coordinated_close = None::<RuntimeCloseIdentity>;
    let run_after_startup = if let Some(startup_gate) = startup_gate {
        startup_gate
            .ready_tx
            .blocking_send(startup_gate.participant)
            .map_err(|_| {
                invalid_data(format!(
                    "startup ready channel closed before writer ready: {}",
                    startup_gate.participant.name
                ))
            })?;
        match startup_gate.release_rx.blocking_recv().map_err(|_| {
            invalid_data(format!(
                "startup activation gate closed before writer release: {}",
                startup_gate.participant.name
            ))
        })? {
            StartupReleasePhase::Run => true,
            StartupReleasePhase::Stop(close) => {
                writer_epoch.set_close(&close);
                coordinated_close = Some(close);
                false
            }
        }
    } else {
        true
    };
    if run_after_startup {
        while let Ok(command) = rx.recv() {
            match command {
                WriterCommand::Events(events) => {
                    queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                    if writer_schema_version
                        != TypedParquetSegmentWriter::current_schema_version(&source_kind)?
                    {
                        let drop_facts = events
                            .iter()
                            .map(|event| {
                                (
                                    event.event.source_kind.clone(),
                                    event.event.recv_time_utc_ms,
                                )
                            })
                            .collect::<Vec<_>>();
                        record_writer_drop_facts(&runtime_stats, &drop_facts);
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(invalid_data(format!(
                            "raw event writer command does not match frozen writer schema:source={source_kind}:schema={writer_schema_version}"
                        ))
                        .into());
                    }
                    let mut events = events.into_iter();
                    while let Some(inflight_event) = events.next() {
                        let event = &inflight_event.event;
                        let process_result = (|| -> Result<()> {
                            if let Some(permit) = inflight_event.raw_day_permit.as_ref() {
                                let event_day = utc_day_string_from_ms(event.recv_time_utc_ms)
                                    .ok_or_else(|| {
                                        invalid_data("UTC day unavailable in source writer")
                                    })?;
                                if event_day != permit.utc_day {
                                    return Err(invalid_data(format!(
                                    "writer raw-day permit mismatch: source={source_kind} event_day={event_day} permit={permit:?}"
                                ))
                                .into());
                                }
                                match expected_raw_epoch.as_ref() {
                                    Some(expected) if expected != permit => {
                                        return Err(invalid_data(format!(
                                        "writer raw-day generation mismatch: source={source_kind} expected={expected:?} actual={permit:?}"
                                    ))
                                    .into());
                                    }
                                    None => {
                                        expected_raw_epoch = Some(permit.clone());
                                        writer_epoch.set(permit);
                                    }
                                    Some(_) => {}
                                }
                            }
                            let current_partition = partition_policy.partition(
                                &source_kind,
                                event.recv_time_utc_ms,
                                None,
                            )?;
                            if let Some(reason) = active.as_ref().and_then(|segment| {
                                training_segment_rollover_reason(
                                    segment,
                                    &current_partition.utc_hour,
                                )
                            }) {
                                let segment = active.take().expect("active segment exists");
                                finalized_segments.push(finalize_segment_with_drop_accounting(
                                    segment,
                                    reason,
                                    &provenance,
                                    &manifest_tx,
                                    &runtime_stats,
                                )?);
                            }
                            if active.is_none() {
                                let (naming, selected_hour_part) = next_available_hourly_partition(
                                    &partition_policy,
                                    &source_kind,
                                    event.recv_time_utc_ms,
                                    None,
                                )?;
                                active = Some(ActiveSegment::new(
                                    naming,
                                    &source_kind,
                                    selected_hour_part,
                                )?);
                            }
                            let segment = active.as_mut().expect("active segment exists");
                            let write_started = Instant::now();
                            let written_rows = segment.writer.write_event(event)?;
                            let write_events_us = write_started.elapsed().as_micros() as u64;
                            segment.observe(event, written_rows);
                            stats.record_written_event(
                                event.recv_time_utc_ms,
                                written_rows,
                                write_events_us,
                            );
                            Ok(())
                        })();
                        if let Err(error) = process_result {
                            if let Some(segment) = active.as_ref() {
                                record_unfinalized_segment_drop(&runtime_stats, segment);
                            }
                            record_writer_drop_facts(
                                &runtime_stats,
                                &std::iter::once(inflight_event)
                                    .chain(events)
                                    .map(|event| {
                                        (event.event.source_kind, event.event.recv_time_utc_ms)
                                    })
                                    .collect::<Vec<_>>(),
                            );
                            stop_accepting_and_record_queued_drops(
                                &accepting_commands,
                                &rx,
                                &queue_rows,
                                &runtime_stats,
                            );
                            return Err(error);
                        }
                    }
                }
                WriterCommand::PolymarketOrderbookV3(events) => {
                    queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                    if source_kind != "polymarket_market"
                        || writer_schema_version != POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
                    {
                        let drop_facts = events
                            .iter()
                            .map(|event| {
                                (
                                    "polymarket_market".to_owned(),
                                    event.message.envelope.timestamp_received,
                                )
                            })
                            .collect::<Vec<_>>();
                        record_writer_drop_facts(&runtime_stats, &drop_facts);
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(invalid_data(format!(
                            "Polymarket v3 writer command does not match frozen writer identity:source={source_kind}:schema={writer_schema_version}"
                        ))
                        .into());
                    }
                    let sequence_state = polymarket_v3_sequence_state
                        .as_mut()
                        .expect("validated Polymarket v3 writer owns sequence state");
                    let mut events = events.into_iter();
                    while let Some(inflight_event) = events.next() {
                        let message = &inflight_event.message;
                        let recv_time_utc_ms = message.envelope.timestamp_received;
                        let process_result = (|| -> Result<()> {
                            let event_day =
                                utc_day_string_from_ms(recv_time_utc_ms).ok_or_else(|| {
                                    invalid_data(
                                        "UTC day unavailable in Polymarket v3 source writer",
                                    )
                                })?;
                            if event_day != inflight_event.raw_day_permit.utc_day {
                                return Err(invalid_data(format!(
                                    "writer Polymarket v3 raw-day permit mismatch:source={source_kind}:event_day={event_day}:permit={:?}",
                                    inflight_event.raw_day_permit
                                ))
                                .into());
                            }
                            match expected_raw_epoch.as_ref() {
                                Some(expected) if expected != &inflight_event.raw_day_permit => {
                                    return Err(invalid_data(format!(
                                        "writer Polymarket v3 raw-day generation mismatch:source={source_kind}:expected={expected:?}:actual={:?}",
                                        inflight_event.raw_day_permit
                                    ))
                                    .into());
                                }
                                None => {
                                    expected_raw_epoch =
                                        Some(inflight_event.raw_day_permit.clone());
                                    writer_epoch.set(&inflight_event.raw_day_permit);
                                }
                                Some(_) => {}
                            }
                            let current_partition =
                                partition_policy.partition(&source_kind, recv_time_utc_ms, None)?;
                            if let Some(reason) = active.as_ref().and_then(|segment| {
                                training_segment_rollover_reason(
                                    segment,
                                    &current_partition.utc_hour,
                                )
                            }) {
                                let segment = active.take().expect("active segment exists");
                                finalized_segments.push(finalize_segment_with_drop_accounting(
                                    segment,
                                    reason,
                                    &provenance,
                                    &manifest_tx,
                                    &runtime_stats,
                                )?);
                            }
                            if active.is_none() {
                                let (naming, selected_hour_part) = next_available_hourly_partition(
                                    &partition_policy,
                                    &source_kind,
                                    recv_time_utc_ms,
                                    None,
                                )?;
                                active = Some(ActiveSegment::new(
                                    naming,
                                    &source_kind,
                                    selected_hour_part,
                                )?);
                            }
                            let segment = active.as_mut().expect("active segment exists");
                            if segment.writer.schema_version() != writer_schema_version {
                                return Err(invalid_data(format!(
                                    "active segment schema drift:source={source_kind}:expected={writer_schema_version}:actual={}",
                                    segment.writer.schema_version()
                                ))
                                .into());
                            }
                            let write_started = Instant::now();
                            let written_rows = segment
                                .writer
                                .write_polymarket_orderbook_v3(message, sequence_state)?;
                            let write_events_us = write_started.elapsed().as_micros() as u64;
                            segment.observe_polymarket_orderbook_v3(message, written_rows);
                            stats.record_written_event(
                                recv_time_utc_ms,
                                written_rows,
                                write_events_us,
                            );
                            Ok(())
                        })();
                        if let Err(error) = process_result {
                            if let Some(segment) = active.as_ref() {
                                record_unfinalized_segment_drop(&runtime_stats, segment);
                            }
                            record_writer_drop_facts(
                                &runtime_stats,
                                &std::iter::once(inflight_event)
                                    .chain(events)
                                    .map(|event| {
                                        (
                                            "polymarket_market".to_owned(),
                                            event.message.envelope.timestamp_received,
                                        )
                                    })
                                    .collect::<Vec<_>>(),
                            );
                            stop_accepting_and_record_queued_drops(
                                &accepting_commands,
                                &rx,
                                &queue_rows,
                                &runtime_stats,
                            );
                            return Err(error);
                        }
                    }
                }
                WriterCommand::FinalizeHour { utc_hour } => {
                    if active
                        .as_ref()
                        .is_some_and(|segment| segment.utc_hour < utc_hour)
                    {
                        let segment = active.as_ref().expect("active segment exists");
                        let error = invalid_data(format!(
                            "writer hour barrier skipped UTC hour: source={source_kind} active={} barrier={utc_hour}",
                            segment.utc_hour
                        ));
                        record_unfinalized_segment_drop(&runtime_stats, segment);
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    if active
                        .as_ref()
                        .is_some_and(|segment| segment.utc_hour == utc_hour)
                    {
                        let segment = active.take().expect("active segment exists");
                        let finalized = finalize_segment_with_drop_accounting(
                            segment,
                            "utc_hour_boundary",
                            &provenance,
                            &manifest_tx,
                            &runtime_stats,
                        );
                        match finalized {
                            Ok(finalized) => finalized_segments.push(finalized),
                            Err(error) => {
                                stop_accepting_and_record_queued_drops(
                                    &accepting_commands,
                                    &rx,
                                    &queue_rows,
                                    &runtime_stats,
                                );
                                return Err(error);
                            }
                        }
                    }
                }
                WriterCommand::FinalizeDay { close } => {
                    let utc_day = &close.utc_day;
                    if let Some(expected) = expected_raw_epoch.as_ref()
                        && (expected.utc_day != close.utc_day
                            || expected.generation != close.generation)
                    {
                        let error = invalid_data(format!(
                            "writer finalize identity mismatch: source={source_kind} expected={expected:?} close={close:?}"
                        ));
                        if let Some(segment) = active.as_ref() {
                            record_unfinalized_segment_drop(&runtime_stats, segment);
                        }
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    if active
                        .as_ref()
                        .is_some_and(|segment| segment.utc_day.as_str() <= utc_day.as_str())
                    {
                        if active
                            .as_ref()
                            .is_some_and(|segment| segment.utc_day != *utc_day)
                        {
                            let error = invalid_data(format!(
                                "writer day barrier skipped UTC day: source={source_kind} active={} barrier={utc_day}",
                                active.as_ref().expect("active segment exists").utc_day
                            ));
                            record_unfinalized_segment_drop(
                                &runtime_stats,
                                active.as_ref().expect("active segment exists"),
                            );
                            stop_accepting_and_record_queued_drops(
                                &accepting_commands,
                                &rx,
                                &queue_rows,
                                &runtime_stats,
                            );
                            return Err(error.into());
                        }
                        let segment = active.take().expect("active segment exists");
                        let finalized = finalize_segment_with_drop_accounting(
                            segment,
                            "utc_day_barrier",
                            &provenance,
                            &manifest_tx,
                            &runtime_stats,
                        );
                        match finalized {
                            Ok(finalized) => finalized_segments.push(finalized),
                            Err(error) => {
                                stop_accepting_and_record_queued_drops(
                                    &accepting_commands,
                                    &rx,
                                    &queue_rows,
                                    &runtime_stats,
                                );
                                return Err(error);
                            }
                        }
                    }
                    if let Err(error) = manifest_tx
                        .blocking_send(ManifestCommand::WriterDayFinalized {
                            close: close.clone(),
                            source_kind: source_kind.clone(),
                            raw_day_gate: raw_day_gate.clone().ok_or_else(|| {
                                invalid_data(
                                    "writer day finalize requires the production raw-day gate",
                                )
                            })?,
                        })
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "daily manifest owner channel closed during writer day finalize",
                            )
                        })
                    {
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    expected_raw_epoch = Some(RawDayPermit {
                        utc_day: close.next_utc_day,
                        generation: close.next_generation,
                    });
                    writer_epoch.set(
                        expected_raw_epoch
                            .as_ref()
                            .expect("writer next epoch was assigned"),
                    );
                }
                WriterCommand::Shutdown { close } => {
                    if let Some(expected) = expected_raw_epoch.as_ref()
                        && (expected.utc_day != close.utc_day
                            || expected.generation != close.generation)
                    {
                        let error = invalid_data(format!(
                            "writer shutdown identity mismatch: source={source_kind} expected={expected:?} close={close:?}"
                        ));
                        if let Some(segment) = active.as_ref() {
                            record_unfinalized_segment_drop(&runtime_stats, segment);
                        }
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    if active
                        .as_ref()
                        .is_some_and(|segment| segment.utc_day != close.utc_day)
                    {
                        let error = invalid_data(format!(
                            "writer shutdown active-day mismatch: source={source_kind} active={} close={}",
                            active.as_ref().expect("active segment exists").utc_day,
                            close.utc_day
                        ));
                        record_unfinalized_segment_drop(
                            &runtime_stats,
                            active.as_ref().expect("active segment exists"),
                        );
                        stop_accepting_and_record_queued_drops(
                            &accepting_commands,
                            &rx,
                            &queue_rows,
                            &runtime_stats,
                        );
                        return Err(error.into());
                    }
                    *accepting_commands
                        .lock()
                        .expect("writer accepting-command mutex poisoned") = false;
                    if expected_raw_epoch.is_none() {
                        writer_epoch.set_close(&close);
                    }
                    coordinated_close = Some(close);
                    break;
                }
            }
        }
    }
    if let Some(segment) = active.take() {
        finalized_segments.push(finalize_segment_with_drop_accounting(
            segment,
            "shutdown_finalize",
            &provenance,
            &manifest_tx,
            &runtime_stats,
        )?);
    }
    let stats = stats.finish(&finalized_segments);
    Ok(SourceWriterOutput {
        source_kind,
        finalized_segments,
        stats,
        coordinated_close,
        observed_utc_ms: now_ms(),
    })
}

fn finalize_segment_with_drop_accounting(
    segment: ActiveSegment,
    rollover_reason: &str,
    provenance: &TrainingArtifactIdentity,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    runtime_stats: &SourceQueueBackpressureStats,
) -> Result<TrainingFinalizedSegment> {
    let final_path = segment.writer.final_path().to_path_buf();
    let source_kind = segment.source_kind.clone();
    let recv_time_utc_ms = segment.segment_start_recv_time_utc_ms;
    let rows = segment.rows;
    match finalize_segment(segment, rollover_reason, provenance, manifest_tx) {
        Ok(finalized) => Ok(finalized),
        Err(error) => {
            if rows > 0 && !final_path.exists() {
                runtime_stats.record_drop(rows);
                runtime_stats.record_operational_drop(&source_kind, recv_time_utc_ms, rows);
            }
            Err(error)
        }
    }
}

fn record_unfinalized_segment_drop(
    runtime_stats: &SourceQueueBackpressureStats,
    segment: &ActiveSegment,
) {
    if segment.rows == 0 || segment.writer.final_path().exists() {
        return;
    }
    runtime_stats.record_drop(segment.rows);
    runtime_stats.record_operational_drop(
        &segment.source_kind,
        segment.segment_start_recv_time_utc_ms,
        segment.rows,
    );
}

fn stop_accepting_and_record_queued_drops(
    accepting_commands: &Mutex<bool>,
    rx: &std::sync::mpsc::Receiver<WriterCommand>,
    queue_rows: &AtomicU64,
    runtime_stats: &SourceQueueBackpressureStats,
) {
    let mut accepting = accepting_commands
        .lock()
        .expect("writer accepting-command mutex poisoned");
    *accepting = false;
    let mut drop_facts = Vec::new();
    while let Ok(command) = rx.try_recv() {
        match command {
            WriterCommand::Events(events) => {
                queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                drop_facts.extend(
                    events
                        .into_iter()
                        .map(|event| (event.event.source_kind, event.event.recv_time_utc_ms)),
                );
            }
            WriterCommand::PolymarketOrderbookV3(events) => {
                queue_rows.fetch_sub(events.len() as u64, Ordering::Relaxed);
                drop_facts.extend(events.into_iter().map(|event| {
                    (
                        "polymarket_market".to_owned(),
                        event.message.envelope.timestamp_received,
                    )
                }));
            }
            WriterCommand::FinalizeHour { .. }
            | WriterCommand::FinalizeDay { .. }
            | WriterCommand::Shutdown { .. } => {}
        }
    }
    drop(accepting);
    record_writer_drop_facts(runtime_stats, &drop_facts);
}

pub(super) fn finalize_segment(
    mut segment: ActiveSegment,
    rollover_reason: &str,
    provenance: &TrainingArtifactIdentity,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
) -> Result<TrainingFinalizedSegment> {
    let source_kind = segment.source_kind.clone();
    let schema_version = segment.writer.schema_version().to_owned();
    let manifest_context = SegmentManifestContext::from_segment(&segment);
    let facts = segment_daily_facts(
        &manifest_context,
        &schema_version,
        rollover_reason,
        provenance,
    );
    let recovery_metadata = DailyFileRecoveryMetadata {
        source_kind: source_kind.clone(),
        utc_hour: manifest_context.utc_hour.clone(),
        part: manifest_context.hour_part,
        rows: manifest_context.rows,
        schema_version,
        first_recv_time_utc_ms: manifest_context.segment_start_recv_time_utc_ms,
        last_recv_time_utc_ms: manifest_context.last_recv_time_utc_ms,
        source_timestamp_start: manifest_context.source_ts_start.clone(),
        source_timestamp_end: manifest_context.source_ts_end.clone(),
        event_type_counts: manifest_context.event_type_counts.clone(),
        facts,
    };
    segment.writer.append_key_value_metadata(
        DAILY_FILE_RECOVERY_METADATA_KEY,
        serde_json::to_string(&recovery_metadata)?,
    )?;
    let published = segment.writer.finalize()?;
    if published.row_count != recovery_metadata.rows
        || published.schema_version != recovery_metadata.schema_version
    {
        return Err(invalid_data(format!(
            "finalized Parquet recovery metadata mismatch: source={source_kind} rows={} metadata_rows={} schema={} metadata_schema={}",
            published.row_count,
            recovery_metadata.rows,
            published.schema_version,
            recovery_metadata.schema_version,
        ))
        .into());
    }
    let bytes = fs::metadata(&published.final_path)?.len();
    let delta = DailyFinalizedFileDelta {
        utc_day: manifest_context.utc_day.clone(),
        final_path: published.final_path.clone(),
        bytes,
        sha256: published.sha256.clone(),
        recovery_metadata,
    };
    manifest_tx
        .blocking_send(ManifestCommand::Finalized(delta))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "daily manifest owner channel closed during finalize",
            )
        })?;
    Ok(TrainingFinalizedSegment {
        source_kind,
        parquet_path: published.final_path,
        row_count: published.row_count,
        sha256: published.sha256,
    })
}

pub(super) fn next_available_hourly_partition(
    partition_policy: &HourlyPartitionPolicy,
    source_kind: &str,
    recv_time_utc_ms: i64,
    preferred_part: Option<u32>,
) -> Result<(HourlyPartition, u32)> {
    let mut part = preferred_part.unwrap_or_default();
    for _ in 0..10_000 {
        let partition_part = if part == 0 { None } else { Some(part) };
        let partition =
            partition_policy.partition(source_kind, recv_time_utc_ms, partition_part)?;
        if !partition.running_path.exists() && !partition.final_path.exists() {
            return Ok((partition, part));
        }
        part = part.saturating_add(1);
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("no available hourly part for source {source_kind}"),
    )
    .into())
}

#[derive(Clone, Debug)]
pub(super) struct SegmentManifestContext {
    pub(super) source_kind: String,
    utc_day: String,
    pub(super) utc_hour: String,
    hour_start_utc_ms: i64,
    hour_end_utc_ms: i64,
    pub(super) hour_part: u32,
    pub(super) segment_start_recv_time_utc_ms: i64,
    pub(super) last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    event_type_counts: BTreeMap<String, u64>,
    backfill_rows: u64,
    book_rows_with_bids_asks: u64,
    covered_market_slugs: BTreeSet<String>,
    covered_condition_ids: BTreeSet<String>,
    covered_asset_ids: BTreeSet<String>,
    depth_snapshot_last_update_id: Option<u64>,
    depth_stitch_applied_rows: u64,
    depth_stitch_gap_rows: u64,
    depth_resnapshot_count: u64,
    depth_local_u_min: Option<u64>,
    depth_local_u_max: Option<u64>,
    depth_first_update_bridged_snapshot: bool,
    depth_source_gaps: BTreeSet<String>,
    pub(super) metadata_complete_rows: u64,
    pub(super) metadata_unavailable_rows: u64,
}

impl SegmentManifestContext {
    pub(super) fn from_segment(segment: &ActiveSegment) -> Self {
        Self {
            source_kind: segment.source_kind.clone(),
            utc_day: segment.utc_day.clone(),
            utc_hour: segment.utc_hour.clone(),
            hour_start_utc_ms: segment.hour_start_utc_ms,
            hour_end_utc_ms: segment.hour_end_utc_ms,
            hour_part: segment.hour_part,
            segment_start_recv_time_utc_ms: segment.segment_start_recv_time_utc_ms,
            last_recv_time_utc_ms: segment.last_recv_time_utc_ms,
            source_ts_start: segment.source_ts_start.clone(),
            source_ts_end: segment.source_ts_end.clone(),
            rows: segment.rows,
            event_type_counts: segment.event_type_counts.clone(),
            backfill_rows: segment.backfill_rows,
            book_rows_with_bids_asks: segment.book_rows_with_bids_asks,
            covered_market_slugs: segment.covered_market_slugs.clone(),
            covered_condition_ids: segment.covered_condition_ids.clone(),
            covered_asset_ids: segment.covered_asset_ids.clone(),
            depth_snapshot_last_update_id: segment.depth_snapshot_last_update_id,
            depth_stitch_applied_rows: segment.depth_stitch_applied_rows,
            depth_stitch_gap_rows: segment.depth_stitch_gap_rows,
            depth_resnapshot_count: segment.depth_resnapshot_count,
            depth_local_u_min: segment.depth_local_u_min,
            depth_local_u_max: segment.depth_local_u_max,
            depth_first_update_bridged_snapshot: segment.depth_first_update_bridged_snapshot,
            depth_source_gaps: segment.depth_source_gaps.clone(),
            metadata_complete_rows: segment.metadata_complete_rows,
            metadata_unavailable_rows: segment.metadata_unavailable_rows,
        }
    }
}

pub(super) fn segment_daily_facts(
    segment: &SegmentManifestContext,
    schema_version: &str,
    rollover_reason: &str,
    provenance: &TrainingArtifactIdentity,
) -> Value {
    let sort_order = typed_sort_order(schema_version, &segment.source_kind);
    json!({
        "rollover_reason": rollover_reason,
        "hour_window_utc_ms": {
            "start": segment.hour_start_utc_ms,
            "end": segment.hour_end_utc_ms,
        },
        "layout_family": layout_family_for_source(&segment.source_kind),
        "sort_order": sort_order,
        "compression": "zstd_level_3",
        "backfill_rows": segment.backfill_rows,
        "book_rows_with_bids_asks": segment.book_rows_with_bids_asks,
        "covered_market_slugs": segment.covered_market_slugs,
        "covered_condition_ids": segment.covered_condition_ids,
        "covered_asset_ids": segment.covered_asset_ids,
        "binance_depth_stitch": {
            "snapshot_last_update_id": segment.depth_snapshot_last_update_id,
            "stitch_applied_rows": segment.depth_stitch_applied_rows,
            "gap_rows": segment.depth_stitch_gap_rows,
            "resnapshot_count": segment.depth_resnapshot_count,
            "local_u_min": segment.depth_local_u_min,
            "local_u_max": segment.depth_local_u_max,
            "first_update_bridged_snapshot": segment.depth_first_update_bridged_snapshot,
        },
        "source_gaps": segment.depth_source_gaps,
        "metadata_complete_rows": segment.metadata_complete_rows,
        "metadata_unavailable_rows": segment.metadata_unavailable_rows,
        "code_version": provenance.git_commit,
        "collector_binary_sha256": provenance.collector_binary_sha256,
        "source_config_digest": provenance.source_config_digest,
        "collector_host_id": provenance.collector_host_id,
        "collector_region": provenance.collector_region,
    })
}

fn layout_family_for_source(source_kind: &str) -> &'static str {
    match source_kind {
        "polymarket_market" => "pmxt_inspired_polymarket_orderbook",
        "polymarket_market_metadata" => "pmxt_inspired_polymarket_market_metadata",
        _ => "source_specific_hourly_typed_raw",
    }
}

pub(super) struct SourceWriterHandle {
    pub(super) schema_version: String,
    pub(super) tx: std::sync::mpsc::SyncSender<WriterCommand>,
    pub(super) queue_rows: Arc<AtomicU64>,
    pub(super) accepting_commands: Arc<Mutex<bool>>,
    pub(super) result_rx: std::sync::mpsc::Receiver<WriterTaskReport>,
    pub(super) completed_result: Option<WriterTaskReport>,
    pub(super) thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone, Default)]
pub(super) struct WriterEpochCell {
    epoch: Arc<Mutex<Option<RuntimeEpoch>>>,
}

impl WriterEpochCell {
    fn set(&self, permit: &RawDayPermit) {
        *self.epoch.lock().expect("writer epoch mutex poisoned") = Some(RuntimeEpoch {
            utc_day: permit.utc_day.clone(),
            generation: permit.generation,
        });
    }

    fn set_close(&self, close: &RuntimeCloseIdentity) {
        *self.epoch.lock().expect("writer epoch mutex poisoned") = Some(RuntimeEpoch {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
        });
    }

    fn snapshot(&self) -> Option<RuntimeEpoch> {
        self.epoch
            .lock()
            .expect("writer epoch mutex poisoned")
            .clone()
    }
}

#[allow(clippy::large_enum_variant)]
pub(super) enum WriterTaskOutcome {
    Returned(Result<SourceWriterOutput>),
    Panicked(String),
    MissingReport(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WriterTerminationKind {
    CoordinatedClose,
    ReliableReturn,
    Unclean,
}

pub(super) struct WriterTaskReport {
    pub(super) source_kind: String,
    pub(super) first_observed_utc_ms: i64,
    pub(super) worker_epoch: Option<RuntimeEpoch>,
    pub(super) lifecycle_epoch: Option<RuntimeEpoch>,
    pub(super) termination_kind: WriterTerminationKind,
    pub(super) lifecycle_close: Option<RuntimeCloseIdentity>,
    pub(super) lifecycle_record_error: Option<String>,
    pub(super) outcome: WriterTaskOutcome,
}

pub(super) enum WriterCommand {
    Events(Vec<InflightRawEvent>),
    PolymarketOrderbookV3(Vec<InflightPolymarketOrderbookV3>),
    FinalizeHour { utc_hour: String },
    FinalizeDay { close: RawDayClose },
    Shutdown { close: RuntimeCloseIdentity },
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct SourceWriterOutput {
    pub(super) source_kind: String,
    pub(super) finalized_segments: Vec<TrainingFinalizedSegment>,
    pub(super) stats: SourceWriterStats,
    pub(super) coordinated_close: Option<RuntimeCloseIdentity>,
    pub(super) observed_utc_ms: i64,
}

pub(super) struct WriterStopOutcome {
    pub(super) outputs: Vec<SourceWriterOutput>,
    pub(super) errors: Vec<String>,
    pub(super) all_terminal: bool,
    pub(super) all_close_proven: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct SourceWriterStats {
    pub(super) source_kind: String,
    pub(super) events_received: u64,
    pub(super) events_written: u64,
    pub(super) events_dropped: u64,
    pub(super) typed_rows_written: u64,
    pub(super) typed_rows_dropped: u64,
    pub(super) finalized_segments: u64,
    pub(super) finalized_segment_rows: u64,
    pub(super) first_recv_time_utc_ms: Option<i64>,
    pub(super) last_recv_time_utc_ms: Option<i64>,
    pub(super) write_events_us: LatencyStats,
}

impl SourceWriterStats {
    pub(super) fn empty(source_kind: String) -> Self {
        Self {
            source_kind,
            events_received: 0,
            events_written: 0,
            events_dropped: 0,
            typed_rows_written: 0,
            typed_rows_dropped: 0,
            finalized_segments: 0,
            finalized_segment_rows: 0,
            first_recv_time_utc_ms: None,
            last_recv_time_utc_ms: None,
            write_events_us: LatencyStats::from_values(&[]),
        }
    }
}

struct SourceWriterStatsBuilder {
    stats: SourceWriterStats,
    write_events_us: LatencyAccumulator,
}

impl SourceWriterStatsBuilder {
    fn new(source_kind: String) -> Self {
        Self {
            stats: SourceWriterStats::empty(source_kind),
            write_events_us: LatencyAccumulator::default(),
        }
    }

    fn record_written_event(&mut self, recv_time_utc_ms: i64, typed_rows: u64, write_us: u64) {
        self.stats.events_received = self.stats.events_received.saturating_add(1);
        self.stats.events_written = self.stats.events_written.saturating_add(1);
        self.stats.typed_rows_written = self.stats.typed_rows_written.saturating_add(typed_rows);
        self.stats.first_recv_time_utc_ms = Some(
            self.stats
                .first_recv_time_utc_ms
                .map_or(recv_time_utc_ms, |current| current.min(recv_time_utc_ms)),
        );
        self.stats.last_recv_time_utc_ms = Some(
            self.stats
                .last_recv_time_utc_ms
                .map_or(recv_time_utc_ms, |current| current.max(recv_time_utc_ms)),
        );
        self.write_events_us.observe(write_us);
    }

    fn finish(mut self, finalized_segments: &[TrainingFinalizedSegment]) -> SourceWriterStats {
        self.stats.finalized_segments = finalized_segments.len() as u64;
        self.stats.finalized_segment_rows = finalized_segments
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        self.stats.write_events_us = self.write_events_us.finish();
        self.stats
    }
}

struct LatencyAccumulator {
    count: u64,
    min: Option<u64>,
    max: u64,
    buckets: [u64; 65],
}

impl Default for LatencyAccumulator {
    fn default() -> Self {
        Self {
            count: 0,
            min: None,
            max: 0,
            buckets: [0; 65],
        }
    }
}

impl LatencyAccumulator {
    fn observe(&mut self, value: u64) {
        self.count = self.count.saturating_add(1);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = self.max.max(value);
        self.buckets[latency_bucket(value)] = self.buckets[latency_bucket(value)].saturating_add(1);
    }

    fn finish(&self) -> LatencyStats {
        if self.count == 0 {
            return LatencyStats::from_values(&[]);
        }
        LatencyStats {
            count: self.count,
            min: self.min.unwrap_or(0),
            p50: self.percentile(50),
            p95: self.percentile(95),
            p99: self.percentile(99),
            max: self.max,
        }
    }

    fn percentile(&self, percentile: u64) -> u64 {
        let target_rank = self.count.saturating_mul(percentile).saturating_add(99) / 100;
        let target_rank = target_rank.max(1);
        let mut cumulative = 0_u64;
        for (bucket, count) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            if cumulative >= target_rank {
                return latency_bucket_upper_bound(bucket);
            }
        }
        self.max
    }
}

fn latency_bucket(value: u64) -> usize {
    if value == 0 {
        0
    } else {
        (64 - value.leading_zeros()) as usize
    }
}

fn latency_bucket_upper_bound(bucket: usize) -> u64 {
    match bucket {
        0 => 0,
        64.. => u64::MAX,
        _ => (1_u64 << bucket) - 1,
    }
}

pub(super) struct ActiveSegment {
    source_kind: String,
    pub(super) writer: TrainingSegmentWriter,
    utc_day: String,
    utc_hour: String,
    hour_start_utc_ms: i64,
    hour_end_utc_ms: i64,
    hour_part: u32,
    segment_start_recv_time_utc_ms: i64,
    last_recv_time_utc_ms: i64,
    source_ts_start: Option<String>,
    source_ts_end: Option<String>,
    rows: u64,
    pub(super) bytes: u64,
    event_type_counts: BTreeMap<String, u64>,
    backfill_rows: u64,
    book_rows_with_bids_asks: u64,
    covered_market_slugs: BTreeSet<String>,
    covered_condition_ids: BTreeSet<String>,
    covered_asset_ids: BTreeSet<String>,
    depth_snapshot_last_update_id: Option<u64>,
    depth_stitch_applied_rows: u64,
    depth_stitch_gap_rows: u64,
    depth_resnapshot_count: u64,
    depth_local_u_min: Option<u64>,
    depth_local_u_max: Option<u64>,
    depth_first_update_bridged_snapshot: bool,
    depth_source_gaps: BTreeSet<String>,
    metadata_complete_rows: u64,
    metadata_unavailable_rows: u64,
}

impl ActiveSegment {
    pub(super) fn new(naming: HourlyPartition, source_kind: &str, hour_part: u32) -> Result<Self> {
        let writer = TrainingSegmentWriter::create_for_current_source(&naming, source_kind)?;
        Ok(Self {
            source_kind: source_kind.to_owned(),
            utc_day: naming.utc_day.clone(),
            utc_hour: naming.utc_hour.clone(),
            hour_start_utc_ms: naming.hour_start_utc_ms,
            hour_end_utc_ms: naming.hour_end_utc_ms,
            writer,
            hour_part,
            segment_start_recv_time_utc_ms: 0,
            last_recv_time_utc_ms: 0,
            source_ts_start: None,
            source_ts_end: None,
            rows: 0,
            bytes: 0,
            event_type_counts: BTreeMap::new(),
            backfill_rows: 0,
            book_rows_with_bids_asks: 0,
            covered_market_slugs: BTreeSet::new(),
            covered_condition_ids: BTreeSet::new(),
            covered_asset_ids: BTreeSet::new(),
            depth_snapshot_last_update_id: None,
            depth_stitch_applied_rows: 0,
            depth_stitch_gap_rows: 0,
            depth_resnapshot_count: 0,
            depth_local_u_min: None,
            depth_local_u_max: None,
            depth_first_update_bridged_snapshot: false,
            depth_source_gaps: BTreeSet::new(),
            metadata_complete_rows: 0,
            metadata_unavailable_rows: 0,
        })
    }

    pub(super) fn observe(&mut self, event: &RawEvent, typed_rows: u64) {
        if self.rows == 0 {
            self.segment_start_recv_time_utc_ms = event.recv_time_utc_ms;
            self.last_recv_time_utc_ms = event.recv_time_utc_ms;
        } else {
            self.segment_start_recv_time_utc_ms = self
                .segment_start_recv_time_utc_ms
                .min(event.recv_time_utc_ms);
            self.last_recv_time_utc_ms = self.last_recv_time_utc_ms.max(event.recv_time_utc_ms);
        }
        if let Some(source_ts) = event.source_ts.clone() {
            if self.source_ts_start.is_none() {
                self.source_ts_start = Some(source_ts.clone());
            }
            self.source_ts_end = Some(source_ts);
        }
        self.rows = self.rows.saturating_add(typed_rows);
        self.bytes = self.bytes.saturating_add(estimated_event_bytes(event));
        *self
            .event_type_counts
            .entry(event.source_event_type.clone())
            .or_default() += typed_rows;
        self.observe_raw_json_evidence(event, typed_rows);
    }

    fn observe_raw_json_evidence(&mut self, event: &RawEvent, typed_rows: u64) {
        let Ok(value) = serde_json::from_str::<Value>(&event.raw_json) else {
            return;
        };
        if event.source_kind == "binance_aggtrade"
            && value
                .get("record_kind")
                .and_then(Value::as_str)
                .is_none_or(|kind| kind == "trade")
            && value
                .get("ingest_source")
                .and_then(Value::as_str)
                .is_some_and(|source| matches!(source, "rest_backfill" | "websocket_rest_merged"))
        {
            self.backfill_rows = self.backfill_rows.saturating_add(typed_rows);
        }
        if event.source_event_type == "depth_snapshot" {
            self.depth_snapshot_last_update_id = value
                .get("lastUpdateId")
                .and_then(Value::as_u64)
                .or(self.depth_snapshot_last_update_id);
        }
        if event.source_event_type == "depth_update" {
            self.depth_snapshot_last_update_id = value
                .get("snapshot_last_update_id")
                .and_then(Value::as_u64)
                .or(self.depth_snapshot_last_update_id);
            let stitch_verdict = value.get("stitch_verdict").and_then(Value::as_str);
            if stitch_verdict == Some(BINANCE_DEPTH_VERDICT_APPLIED)
                && let Some(applied_u) = binance_ws_payload(&value).get("u").and_then(Value::as_u64)
            {
                self.depth_stitch_applied_rows =
                    self.depth_stitch_applied_rows.saturating_add(typed_rows);
                self.depth_local_u_min = Some(
                    self.depth_local_u_min
                        .map_or(applied_u, |current| current.min(applied_u)),
                );
                self.depth_local_u_max = Some(
                    self.depth_local_u_max
                        .map_or(applied_u, |current| current.max(applied_u)),
                );
            }
            if stitch_verdict == Some(BINANCE_DEPTH_VERDICT_APPLIED)
                && value
                    .get("stitch_evidence")
                    .and_then(Value::as_str)
                    .is_some_and(|evidence| evidence == "U <= lastUpdateId + 1 <= u")
            {
                self.depth_first_update_bridged_snapshot = true;
            }
            if stitch_verdict == Some(BINANCE_DEPTH_VERDICT_NOT_APPLIED_CONFIRMED_HOLE) {
                self.depth_resnapshot_count = self.depth_resnapshot_count.saturating_add(1);
                self.depth_stitch_gap_rows = self.depth_stitch_gap_rows.saturating_add(typed_rows);
                if let Some(reason) = value.get("stitch_gap_reason").and_then(Value::as_str) {
                    self.depth_source_gaps
                        .insert(format!("binance_depth_update_stitch_gap:{}", reason));
                }
            }
        }
        if event.source_event_type == "book"
            && value.get("bids").and_then(Value::as_array).is_some()
            && value.get("asks").and_then(Value::as_array).is_some()
        {
            self.book_rows_with_bids_asks =
                self.book_rows_with_bids_asks.saturating_add(typed_rows);
        }
        if event.source_kind == "polymarket_market_metadata" {
            if metadata_unavailable_reason(&value).is_some() {
                self.metadata_unavailable_rows =
                    self.metadata_unavailable_rows.saturating_add(typed_rows);
            } else {
                self.metadata_complete_rows =
                    self.metadata_complete_rows.saturating_add(typed_rows);
            }
        }
        if let Some(slug) = value
            .get("slug")
            .and_then(Value::as_str)
            .filter(|slug| !slug.trim().is_empty())
        {
            self.covered_market_slugs.insert(slug.to_owned());
        }
        if let Some(market) = value
            .get("market")
            .and_then(Value::as_str)
            .filter(|market| !market.trim().is_empty())
        {
            self.covered_condition_ids.insert(market.to_owned());
        }
        if let Some(asset_id) = value
            .get("asset_id")
            .and_then(Value::as_str)
            .filter(|asset_id| !asset_id.trim().is_empty())
        {
            self.covered_asset_ids.insert(asset_id.to_owned());
        }
        for field in ["asset_ids", "resolved_event_asset_ids"] {
            if let Some(asset_ids) = value.get(field).and_then(Value::as_array) {
                for asset_id in asset_ids.iter().filter_map(Value::as_str) {
                    if !asset_id.trim().is_empty() {
                        self.covered_asset_ids.insert(asset_id.to_owned());
                    }
                }
            }
        }
        if let Some(changes) = value.get("price_changes").and_then(Value::as_array) {
            for change in changes {
                if let Some(asset_id) = change
                    .get("asset_id")
                    .and_then(Value::as_str)
                    .filter(|asset_id| !asset_id.trim().is_empty())
                {
                    self.covered_asset_ids.insert(asset_id.to_owned());
                }
            }
        }
    }

    fn observe_polymarket_orderbook_v3(
        &mut self,
        message: &PolymarketOrderbookV3Message,
        typed_rows: u64,
    ) {
        let recv_time_utc_ms = message.envelope.timestamp_received;
        if self.rows == 0 {
            self.segment_start_recv_time_utc_ms = recv_time_utc_ms;
            self.last_recv_time_utc_ms = recv_time_utc_ms;
        } else {
            self.segment_start_recv_time_utc_ms =
                self.segment_start_recv_time_utc_ms.min(recv_time_utc_ms);
            self.last_recv_time_utc_ms = self.last_recv_time_utc_ms.max(recv_time_utc_ms);
        }
        if let Some(source_ts) = message.envelope.timestamp.map(|value| value.to_string()) {
            if self.source_ts_start.is_none() {
                self.source_ts_start = Some(source_ts.clone());
            }
            self.source_ts_end = Some(source_ts);
        }
        self.rows = self.rows.saturating_add(typed_rows);
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(message.owned_bytes()).unwrap_or(u64::MAX));
        *self
            .event_type_counts
            .entry(message.event_type().to_owned())
            .or_default() += typed_rows;
        if let Some(slug) = message
            .envelope
            .slug
            .as_deref()
            .filter(|slug| !slug.trim().is_empty())
        {
            self.covered_market_slugs.insert(slug.to_owned());
        }
        self.covered_condition_ids
            .insert(message.envelope.market.clone());
        match &message.event {
            PolymarketOrderbookV3Event::Book(book) => {
                self.book_rows_with_bids_asks =
                    self.book_rows_with_bids_asks.saturating_add(typed_rows);
                self.covered_asset_ids.insert(book.asset_id.clone());
            }
            PolymarketOrderbookV3Event::PriceChange(price_change) => {
                self.covered_asset_ids.extend(
                    price_change
                        .price_changes
                        .iter()
                        .map(|change| change.asset_id.clone()),
                );
            }
            PolymarketOrderbookV3Event::TickSizeChange(tick) => {
                self.covered_asset_ids.insert(tick.asset_id.clone());
            }
            PolymarketOrderbookV3Event::LastTradePrice(trade) => {
                self.covered_asset_ids.insert(trade.asset_id.clone());
            }
            PolymarketOrderbookV3Event::BestBidAsk(best) => {
                self.covered_asset_ids.insert(best.asset_id.clone());
            }
            PolymarketOrderbookV3Event::NewMarket(market) => {
                self.covered_asset_ids
                    .extend(market.asset_ids.iter().cloned());
            }
            PolymarketOrderbookV3Event::MarketResolved(market) => {
                self.covered_asset_ids
                    .extend(market.asset_ids.iter().cloned());
            }
        }
    }
}

pub(super) enum TrainingSegmentWriter {
    Typed(TypedParquetSegmentWriter),
}

impl TrainingSegmentWriter {
    fn create_for_current_source(naming: &HourlyPartition, source_kind: &str) -> Result<Self> {
        Ok(Self::Typed(
            TypedParquetSegmentWriter::create_for_current_source(
                &naming.partition_path,
                &naming.stem,
                source_kind,
            )?,
        ))
    }

    pub(super) fn write_event(&mut self, event: &RawEvent) -> Result<u64> {
        match self {
            Self::Typed(writer) => write_typed_event(writer, event),
        }
    }

    fn write_polymarket_orderbook_v3(
        &mut self,
        message: &PolymarketOrderbookV3Message,
        sequence_state: &mut PolymarketOrderbookV3WriteSequenceState,
    ) -> Result<u64> {
        match self {
            Self::Typed(writer) => {
                writer.write_polymarket_orderbook_v3_messages(
                    std::slice::from_ref(message),
                    sequence_state,
                )?;
                Ok(1)
            }
        }
    }

    fn schema_version(&self) -> &str {
        match self {
            Self::Typed(writer) => writer.schema_version(),
        }
    }

    fn append_key_value_metadata(&mut self, key: &str, value: String) -> Result<()> {
        match self {
            Self::Typed(writer) => writer.append_key_value_metadata(key, value),
        }
    }

    fn final_path(&self) -> &Path {
        match self {
            Self::Typed(writer) => writer.final_path(),
        }
    }

    fn finalize(self) -> Result<PublishedSegment> {
        match self {
            Self::Typed(writer) => writer.finalize(),
        }
    }
}

fn write_typed_event(writer: &mut TypedParquetSegmentWriter, event: &RawEvent) -> Result<u64> {
    let logical_bytes = u64::try_from(owned_event_bytes(event))
        .map_err(|_| invalid_data("typed event logical size does not fit in u64"))?;
    match event.source_kind.as_str() {
        "polymarket_market" => {
            Err(invalid_data("raw Polymarket event reached the activated v3 writer").into())
        }
        "polymarket_market_metadata" => {
            write_polymarket_metadata_event(writer, event, logical_bytes)
        }
        "binance_depth_snapshot" => {
            let row = binance_depth_snapshot_row_from_event(event)?;
            writer.write_binance_depth_snapshot_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "binance_depth_update" => {
            let row = binance_depth_update_row_from_event(event)?;
            writer.write_binance_depth_update_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "binance_aggtrade" => {
            let row = binance_aggtrade_row_from_event(event)?;
            writer.write_binance_aggtrade_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "polymarket_rtds_chainlink_btc_usd" => {
            let row = rtds_row_from_event(event)?;
            writer.write_rtds_chainlink_btc_usd_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        "polymarket_rtds_chainlink_twap" => {
            let row = rtds_twap_row_from_event(event)?;
            writer.write_rtds_chainlink_twap_rows_with_logical_bytes(&[row], logical_bytes)?;
            Ok(1)
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported training typed source kind: {other}"),
        )
        .into()),
    }
}

pub(super) fn binance_depth_snapshot_row_from_event(
    event: &RawEvent,
) -> Result<BinanceDepthSnapshotHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let snapshot_fetch_start_utc = required_i64(&value, "snapshot_fetch_start_utc")?;
    let raw = parse_depth_snapshot_raw(
        event_envelope(event, SourceKind::BinanceDepthSnapshot),
        &event.raw_json,
        snapshot_fetch_start_utc,
        event.recv_time_utc_ms,
        event.source_endpoint.clone(),
    )
    .map_err(invalid_data)?;
    BinanceDepthSnapshotHourlyRow::from_raw_with_ordering(
        &raw,
        binance_depth_ordering_from_event(event, &value)?,
    )
    .map_err(|error| invalid_data(error).into())
}

fn binance_depth_update_row_from_event(event: &RawEvent) -> Result<BinanceDepthUpdateHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let raw = parse_depth_update_raw(
        event_envelope(event, SourceKind::BinanceDepthUpdate),
        &event.raw_json,
    )
    .map_err(invalid_data)?;
    let stitch_verdict = required_value_string(&value, "stitch_verdict")?;
    let verdict = DepthUpdateVerdict::try_from(stitch_verdict.as_str()).map_err(invalid_data)?;
    let assessment = DepthUpdateAssessment {
        snapshot_last_update_id: value.get("snapshot_last_update_id").and_then(Value::as_u64),
        local_u_before: value.get("local_u_before").and_then(Value::as_u64),
        verdict,
        local_u_after: value.get("local_u_after").and_then(Value::as_u64),
        stitch_evidence: optional_string(&value, "stitch_evidence"),
        gap_reason: optional_string(&value, "stitch_gap_reason"),
    };
    assessment.validate_contract().map_err(invalid_data)?;
    BinanceDepthUpdateHourlyRow::from_assessment(
        &raw,
        &assessment,
        binance_depth_ordering_from_event(event, &value)?,
    )
    .map_err(|error| invalid_data(error).into())
}

fn binance_depth_ordering_from_event(
    event: &RawEvent,
    value: &Value,
) -> Result<BinanceDepthOrdering> {
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("Binance depth event reached writer without collector admission")
    })?;
    let ordering = BinanceDepthOrdering {
        source_sequence_epoch_id: required_value_string(value, "source_sequence_epoch_id")?,
        connection_generation: value
            .get("connection_generation")
            .and_then(Value::as_u64)
            .filter(|generation| *generation > 0)
            .ok_or_else(|| invalid_data("Binance depth connection_generation missing or zero"))?,
        depth_ingress_sequence: value
            .get("depth_ingress_sequence")
            .and_then(Value::as_u64)
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| invalid_data("Binance depth ingress sequence missing or zero"))?,
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
    };
    ordering.validate_contract().map_err(invalid_data)?;
    Ok(ordering)
}

pub(super) fn binance_aggtrade_row_from_event(
    event: &RawEvent,
) -> Result<BinanceAggTradeHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let record_kind = required_value_string(&value, "record_kind")?;
    let timestamp_received = required_i64(&value, "timestamp_received")?;
    if timestamp_received != event.recv_time_utc_ms {
        return Err(invalid_data("aggTrade raw/event receive time mismatch").into());
    }
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("binance aggTrade event reached writer without collector admission")
    })?;
    let ingest_source = required_value_string(&value, "ingest_source")?;
    let rest_backfill_timestamp_received = optional_i64(&value, "rest_backfill_timestamp_received");
    let row = BinanceAggTradeHourlyRow {
        record_kind,
        timestamp_received,
        rest_backfill_timestamp_received,
        ingest_source,
        event_type: optional_string(&value, "event_type"),
        event_time: optional_i64(&value, "event_time"),
        trade_time: optional_i64(&value, "trade_time"),
        symbol: optional_string(&value, "symbol"),
        aggregate_trade_id: value.get("aggregate_trade_id").and_then(Value::as_u64),
        price: optional_string(&value, "price"),
        quantity: optional_string(&value, "quantity"),
        first_trade_id: value.get("first_trade_id").and_then(Value::as_u64),
        last_trade_id: value.get("last_trade_id").and_then(Value::as_u64),
        buyer_is_maker: optional_bool(&value, "buyer_is_maker"),
        stream: optional_string(&value, "stream"),
        ws_ignore: optional_bool(&value, "ws_ignore"),
        rest_was_best_price_match: optional_bool(&value, "rest_was_best_price_match"),
        source_native_payload_json: required_value_string(&value, "source_native_payload_json")?,
        source_sequence_epoch_id: required_value_string(&value, "source_sequence_epoch_id")?,
        connection_generation: value
            .get("connection_generation")
            .and_then(Value::as_u64)
            .filter(|generation| *generation != 0)
            .ok_or_else(|| invalid_data("aggTrade connection_generation missing or zero"))?,
        websocket_ingress_sequence: value
            .get("websocket_ingress_sequence")
            .and_then(Value::as_u64),
        rest_response_ingress_sequence: value
            .get("rest_response_ingress_sequence")
            .and_then(Value::as_u64),
        rest_response_item_index: value
            .get("rest_response_item_index")
            .and_then(Value::as_u64),
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
        invalid_reason_code: optional_string(&value, "invalid_reason_code"),
        invalid_reason_detail: optional_string(&value, "invalid_reason_detail"),
        payload_encoding: optional_string(&value, "payload_encoding"),
        payload_length_bytes: value.get("payload_length_bytes").and_then(Value::as_u64),
        payload_sha256: optional_string(&value, "payload_sha256"),
        payload_capture_status: optional_string(&value, "payload_capture_status"),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
    };
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "Binance aggTrade row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

pub(super) fn rtds_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    let row = rtds_common_row_from_event(event)?;
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "RTDS row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn rtds_common_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let raw = RtdsChainlinkBtcUsdRaw {
        envelope: event_envelope(event, SourceKind::PolymarketRtdsChainlinkBtcUsd),
        message_timestamp_ms: required_i64(&value, "message_timestamp")?,
        payload_timestamp_ms: required_i64(&value, "payload_timestamp")?,
        symbol: required_value_string(&value, "symbol")?,
        value: required_value_string(&value, "value")?,
        source_topic: required_value_string(&value, "topic")?,
        message_type: required_value_string(&value, "type")?,
        age_ms: optional_i64(&value, "age_ms"),
        freshness_status: required_value_string(&value, "freshness_status")?,
        subscription_raw_json: value
            .get("subscription_raw_json")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(rtds_subscription_evidence_json),
        ack_or_first_message_evidence: Some("first_chainlink_price_message".to_owned()),
        close_or_error_evidence: None,
    };
    let mut row = RtdsChainlinkBtcUsdHourlyRow::from_raw(&raw);
    row.source_native_payload_json = required_value_string(&value, "source_native_payload_json")?;
    row.schema_version = "polymarket_rtds_chainlink_btc_usd_v4".to_owned();
    Ok(row)
}

pub(super) fn rtds_twap_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkTwapHourlyRow> {
    let value: Value = serde_json::from_str(&event.raw_json)?;
    let payload = value
        .get("payload")
        .ok_or_else(|| invalid_data("TWAP payload missing"))?;
    let admission = event
        .collector_admission
        .as_ref()
        .ok_or_else(|| invalid_data("TWAP collector admission missing"))?;
    let mut common = rtds_common_row_from_event(event)?;
    common.schema_version = RTDS_CHAINLINK_TWAP_SCHEMA_VERSION.to_owned();
    let row = RtdsChainlinkTwapHourlyRow {
        common,
        full_accuracy_value: required_value_string(payload, "full_accuracy_value")?,
        window_s: payload
            .get("window_s")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid_data("TWAP window_s missing"))?,
        source_sequence_epoch_id: required_value_string(&value, "source_sequence_epoch_id")?,
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
    };
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "RTDS TWAP row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn required_value_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn optional_bool(value: &Value, field: &str) -> Option<bool> {
    value.get(field).and_then(Value::as_bool)
}

fn metadata_unavailable_reason(value: &Value) -> Option<String> {
    match value.get("metadata_unavailable_reason") {
        Some(Value::String(reason)) if !reason.trim().is_empty() => Some(reason.clone()),
        Some(Value::Array(reasons)) if !reasons.is_empty() => {
            let reasons = reasons
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()
                .ok()?;
            Some(reasons.join(";"))
        }
        Some(other) if !other.is_null() => Some(other.to_string()),
        _ => None,
    }
}

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    optional_i64(value, field).ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

pub(super) fn optional_i64(value: &Value, field: &str) -> Option<i64> {
    value.get(field).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn json_scalar_to_string(value: &Value) -> Result<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(invalid_data("expected string or number JSON scalar").into()),
    }
}

pub(super) fn training_segment_rollover_reason(
    segment: &ActiveSegment,
    current_utc_hour: &str,
) -> Option<&'static str> {
    if segment.utc_hour != current_utc_hour {
        return Some("utc_hour_boundary");
    }
    None
}
