use super::*;

pub(super) const STARTUP_READY_CHANNEL_CAPACITY: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StartupParticipantKind {
    Producer,
    Writer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StartupParticipant {
    kind: StartupParticipantKind,
    pub(super) name: &'static str,
}

impl StartupParticipant {
    pub(super) fn producer(name: &'static str) -> Self {
        Self {
            kind: StartupParticipantKind::Producer,
            name,
        }
    }

    pub(super) fn writer(name: &'static str) -> Self {
        Self {
            kind: StartupParticipantKind::Writer,
            name,
        }
    }
}

pub(super) struct StartupReleaseSender {
    pub(super) participant: StartupParticipant,
    pub(super) sender: oneshot::Sender<StartupReleasePhase>,
}

pub(super) struct StartupReleaseHandles {
    pub(super) writers: Vec<StartupReleaseSender>,
    pub(super) producers: Vec<StartupReleaseSender>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StartupReleasePhase {
    Run,
    Stop(RuntimeCloseIdentity),
}

impl StartupReleaseHandles {
    fn participants(&self) -> BTreeSet<StartupParticipant> {
        self.writers
            .iter()
            .chain(&self.producers)
            .map(|release| release.participant)
            .collect()
    }

    pub(super) fn release(self) -> Result<()> {
        self.send_phases(|_| StartupReleasePhase::Run, |_| StartupReleasePhase::Run)
    }

    pub(super) fn stop_for_exit(self, exit: &PreparedRuntimeExit) -> Result<()> {
        let source_close = RawDayGate::runtime_exit_identity(exit);
        self.send_phases(
            |_| StartupReleasePhase::Run,
            |_| StartupReleasePhase::Stop(source_close.clone()),
        )
    }

    fn send_phases<FW, FP>(self, writer_phase: FW, producer_phase: FP) -> Result<()>
    where
        FW: Fn(&StartupParticipant) -> StartupReleasePhase,
        FP: Fn(&StartupParticipant) -> StartupReleasePhase,
    {
        let mut failures = Vec::new();
        for release in self.writers {
            if release
                .sender
                .send(writer_phase(&release.participant))
                .is_err()
            {
                failures.push(release.participant);
            }
        }
        for release in self.producers {
            if release
                .sender
                .send(producer_phase(&release.participant))
                .is_err()
            {
                failures.push(release.participant);
            }
        }
        if !failures.is_empty() {
            return Err(invalid_data(format!(
                "startup participants dropped before release: {failures:?}"
            ))
            .into());
        }
        Ok(())
    }
}

pub(super) async fn run_producer_after_startup_gate<F>(
    participant: StartupParticipant,
    ready_tx: mpsc::Sender<StartupParticipant>,
    release_rx: oneshot::Receiver<StartupReleasePhase>,
    source: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    ready_tx.send(participant).await.map_err(|_| {
        invalid_data(format!(
            "startup ready channel closed before producer ready: {}",
            participant.name
        ))
    })?;
    let phase = release_rx.await.map_err(|_| {
        invalid_data(format!(
            "startup activation gate closed before producer release: {}",
            participant.name
        ))
    })?;
    match phase {
        StartupReleasePhase::Run => source.await,
        StartupReleasePhase::Stop(close) => Err(CoordinatedSourceStop {
            producer: participant.name.to_owned(),
            close,
        }
        .into()),
    }
}

pub(super) fn expected_startup_participants() -> BTreeSet<StartupParticipant> {
    LOGICAL_PRODUCERS
        .iter()
        .map(|producer| StartupParticipant::producer(producer))
        .chain(
            LIVE_SOURCES
                .iter()
                .map(|source_kind| StartupParticipant::writer(source_kind)),
        )
        .collect()
}

pub(super) async fn collect_startup_ready(
    ready_rx: &mut mpsc::Receiver<StartupParticipant>,
    expected: &BTreeSet<StartupParticipant>,
) -> Result<()> {
    let mut pending = expected.clone();
    while !pending.is_empty() {
        let participant = ready_rx.recv().await.ok_or_else(|| {
            invalid_data(format!(
                "startup ready channel closed with {} mandatory participants pending",
                pending.len()
            ))
        })?;
        if !expected.contains(&participant) {
            return Err(invalid_data(format!(
                "unexpected startup participant ready: kind={:?} name={}",
                participant.kind, participant.name
            ))
            .into());
        }
        if !pending.remove(&participant) {
            return Err(invalid_data(format!(
                "duplicate startup participant ready: kind={:?} name={}",
                participant.kind, participant.name
            ))
            .into());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn activate_and_release_startup<F>(
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    raw_day_gate: Arc<RawDayGate>,
    mut ready_rx: mpsc::Receiver<StartupParticipant>,
    releases: StartupReleaseHandles,
    utc_day: String,
    ack_mode: ManifestActivationAckMode,
    runtime_deadline: Option<RuntimeDeadlineCandidate>,
    activation_clock: F,
) -> Result<StartupActivationOutcome>
where
    F: FnOnce() -> i64,
{
    let expected = expected_startup_participants();
    if expected.len() != STARTUP_READY_CHANNEL_CAPACITY {
        return Err(invalid_data(format!(
            "startup topology count mismatch: expected={} channel_capacity={STARTUP_READY_CHANNEL_CAPACITY}",
            expected.len()
        ))
        .into());
    }
    let release_participants = releases.participants();
    let release_count = releases
        .writers
        .len()
        .saturating_add(releases.producers.len());
    if release_count != expected.len() || release_participants != expected {
        return Err(invalid_data(format!(
            "startup release topology mismatch: expected={expected:?} actual={release_participants:?} actual_count={release_count}"
        ))
        .into());
    }
    timeout(
        STARTUP_GATE_TIMEOUT,
        collect_startup_ready(&mut ready_rx, &expected),
    )
    .await
    .map_err(|_| {
        invalid_data(format!(
            "startup ready timeout after {}ms",
            STARTUP_GATE_TIMEOUT.as_millis()
        ))
    })??;

    let started_utc_ms = activation_clock();
    let activation_day = utc_day_string_from_ms(started_utc_ms)
        .ok_or_else(|| invalid_data("UTC day unavailable for collection activation"))?;
    if activation_day != utc_day {
        return Err(invalid_data(format!(
            "UTC day advanced before collection activation: recovered={utc_day} activation={activation_day}"
        ))
        .into());
    }

    let (reply_tx, reply_rx) = oneshot::channel();
    timeout(
        STARTUP_GATE_TIMEOUT,
        manifest_tx.send(ManifestCommand::Activate {
            utc_day,
            started_utc_ms,
            generation: INITIAL_RAW_DAY_GENERATION,
            raw_day_gate: Arc::clone(&raw_day_gate),
            deadline: runtime_deadline,
            reply: reply_tx,
            ack_mode,
        }),
    )
    .await
    .map_err(|_| invalid_data("daily manifest activation command timeout"))?
    .map_err(|_| invalid_data("daily manifest owner channel closed before activation"))?;

    match timeout(STARTUP_GATE_TIMEOUT, reply_rx).await {
        Ok(Ok(Ok(()))) => {
            raw_day_gate
                .activate_or_stop_startup_after_manifest(
                    activation_day,
                    INITIAL_RAW_DAY_GENERATION,
                    runtime_deadline,
                    releases,
                )
                .await
        }
        Ok(Ok(Err(error))) => {
            Err(invalid_data(format!("daily manifest activation failed: {error}")).into())
        }
        Ok(Err(_)) => {
            Err(invalid_data("daily manifest activation reply dropped after command").into())
        }
        Err(_) => Err(invalid_data("daily manifest activation reply timeout").into()),
    }
}
