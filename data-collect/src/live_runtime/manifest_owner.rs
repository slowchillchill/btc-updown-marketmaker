use super::*;

pub(super) const MANIFEST_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ManifestShutdownDisposition {
    EndedKnown { ended_utc_ms: i64 },
    UncleanEndUnknown,
}

pub(super) enum ManifestCommand {
    Activate {
        utc_day: String,
        started_utc_ms: i64,
        generation: u64,
        raw_day_gate: Arc<RawDayGate>,
        deadline: Option<RuntimeDeadlineCandidate>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
        ack_mode: ManifestActivationAckMode,
    },
    Finalized(DailyFinalizedFileDelta),
    ProducerBarrier {
        close: RawDayClose,
        producer: String,
        gaps: Vec<String>,
        operational_counters: BTreeMap<String, SourceOperationalCounterDelta>,
    },
    WriterDayFinalized {
        close: RawDayClose,
        source_kind: String,
        raw_day_gate: Arc<RawDayGate>,
    },
    CommitRolloverOldDay {
        close: RawDayClose,
        mode: ManifestRolloverCommitMode,
        normal_gate: Option<Arc<RawDayGate>>,
        deadline: Option<RuntimeDeadlineCandidate>,
        reply: oneshot::Sender<std::result::Result<ManifestOldDayCommitReply, String>>,
    },
    ActivateRolloverNewDay {
        close: RawDayClose,
        raw_day_gate: Arc<RawDayGate>,
        deadline: Option<RuntimeDeadlineCandidate>,
        reply: oneshot::Sender<std::result::Result<ManifestActivationReply, String>>,
        ack_mode: ManifestActivationAckMode,
    },
    PrepareRolloverExitDay {
        close: RawDayClose,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    FinishUncleanRollover {
        close: RawDayClose,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    Shutdown {
        close: RuntimeCloseIdentity,
        disposition: ManifestShutdownDisposition,
        gaps: Vec<String>,
        operational_counters: BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

pub(super) async fn send_manifest_command_once(
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    command: ManifestCommand,
    stage: &'static str,
) -> Result<()> {
    match timeout(STARTUP_GATE_TIMEOUT, manifest_tx.send(command)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => {
            Err(invalid_data(format!("manifest owner channel closed before {stage}")).into())
        }
        Err(_) => Err(invalid_data(format!("manifest command send timeout before {stage}")).into()),
    }
}

#[derive(Clone, Copy)]
pub(super) enum ManifestActivationAckMode {
    Send,
    #[cfg(test)]
    Drop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ManifestActivationReply {
    Committed,
    PreparedExitWon,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ManifestRolloverCommitMode {
    Normal,
    PendingExitKnown { ended_utc_ms: i64 },
    UncleanOldDay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ManifestOldDayCommitReply {
    Committed,
    PreparedExitWon,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ManifestRolloverReady {
    pub(super) close: RawDayClose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ManifestActiveEpoch {
    utc_day: String,
    generation: u64,
}

pub(super) struct ManifestRolloverRendezvous {
    close: RawDayClose,
    producer_barriers: BTreeSet<String>,
    writer_finalize_acks: BTreeSet<String>,
    ready_sent: bool,
    old_day_committed: bool,
    old_day_mode: Option<ManifestRolloverCommitMode>,
    raw_day_gate: Option<Arc<RawDayGate>>,
}

pub(super) struct ManifestOwnerState {
    dataset_root: PathBuf,
    identity: DailyManifestIdentity,
    manifests: BTreeMap<String, Value>,
    activation_attempts: BTreeSet<String>,
    pub(super) active_epoch: Option<ManifestActiveEpoch>,
    pub(super) rollover: Option<ManifestRolloverRendezvous>,
}

impl ManifestOwnerState {
    pub(super) fn new(dataset_root: PathBuf, identity: DailyManifestIdentity) -> Self {
        Self {
            dataset_root,
            identity,
            manifests: BTreeMap::new(),
            activation_attempts: BTreeSet::new(),
            active_epoch: None,
            rollover: None,
        }
    }

    pub(super) fn insert_reconciled_manifest(
        &mut self,
        utc_day: String,
        mut manifest: Value,
    ) -> Result<()> {
        add_manifest_operational_counters(
            &self.day_dir(&utc_day),
            &mut manifest,
            &required_source_counter_defaults(),
        )?;
        self.manifests.insert(utc_day, manifest);
        Ok(())
    }

    fn day_dir(&self, utc_day: &str) -> PathBuf {
        typed_raw_root(&self.dataset_root).join(format!("utc_day={utc_day}"))
    }

    pub(super) fn ensure_open(&mut self, utc_day: &str) -> Result<()> {
        if self.manifests.contains_key(utc_day) {
            return Ok(());
        }
        let mut manifest =
            reconcile_open_day_manifest(&self.day_dir(utc_day), utc_day, &self.identity)?;
        add_manifest_operational_counters(
            &self.day_dir(utc_day),
            &mut manifest,
            &required_source_counter_defaults(),
        )?;
        self.manifests.insert(utc_day.to_owned(), manifest);
        Ok(())
    }

    fn activate_coverage(&mut self, utc_day: &str, started_utc_ms: i64) -> Result<()> {
        if !self.activation_attempts.insert(utc_day.to_owned()) {
            return Err(invalid_data(format!(
                "duplicate collection-day activation attempt: {utc_day}"
            ))
            .into());
        }
        self.ensure_open(utc_day)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        activate_collection_day_coverage(&day_dir, manifest, started_utc_ms)?;
        Ok(())
    }

    pub(super) fn activate_initial(
        &mut self,
        utc_day: &str,
        started_utc_ms: i64,
        generation: u64,
    ) -> Result<()> {
        if self.active_epoch.is_some() || self.rollover.is_some() {
            return Err(invalid_data("initial manifest activation found an active epoch").into());
        }
        if generation != INITIAL_RAW_DAY_GENERATION {
            return Err(invalid_data(format!(
                "initial manifest activation generation mismatch: expected={INITIAL_RAW_DAY_GENERATION} actual={generation}"
            ))
            .into());
        }
        self.activate_coverage(utc_day, started_utc_ms)?;
        self.active_epoch = Some(ManifestActiveEpoch {
            utc_day: utc_day.to_owned(),
            generation,
        });
        Ok(())
    }

    pub(super) fn apply_finalized(&mut self, delta: DailyFinalizedFileDelta) -> Result<()> {
        self.ensure_open(&delta.utc_day)?;
        let day_dir = self.day_dir(&delta.utc_day);
        let manifest = self
            .manifests
            .get_mut(&delta.utc_day)
            .expect("manifest was ensured");
        merge_finalized_file_delta(&day_dir, manifest, &delta)?;
        Ok(())
    }

    pub(super) fn record_gaps(&mut self, utc_day: &str, gaps: &[String]) -> Result<()> {
        self.ensure_open(utc_day)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        record_manifest_gaps(&day_dir, manifest, gaps)?;
        Ok(())
    }

    pub(super) fn record_operational_counters(
        &mut self,
        utc_day: &str,
        counters: &BTreeMap<String, SourceOperationalCounterDelta>,
    ) -> Result<()> {
        if counters.is_empty() {
            return Ok(());
        }
        self.ensure_open(utc_day)?;
        let day_dir = self.day_dir(utc_day);
        let manifest = self
            .manifests
            .get_mut(utc_day)
            .expect("manifest was ensured");
        add_manifest_operational_counters(&day_dir, manifest, counters)?;
        Ok(())
    }

    pub(super) fn record_producer_barrier(
        &mut self,
        close: RawDayClose,
        producer: String,
        gaps: &[String],
        operational_counters: &BTreeMap<String, SourceOperationalCounterDelta>,
    ) -> Result<Option<ManifestRolloverReady>> {
        if !LOGICAL_PRODUCERS.contains(&producer.as_str()) {
            return Err(
                invalid_data(format!("unknown logical producer barrier: {producer}")).into(),
            );
        }
        self.ensure_rollover_identity(&close)?;
        {
            let rollover = self.rollover.as_ref().expect("rollover was ensured");
            if rollover.ready_sent {
                return Err(invalid_data("producer barrier arrived after rollover ready").into());
            }
            if rollover.producer_barriers.contains(&producer) {
                return Err(invalid_data(format!(
                    "duplicate logical producer barrier: producer={producer} generation={} close_id={}",
                    close.generation, close.close_id
                ))
                .into());
            }
        }
        self.record_operational_counters(&close.utc_day, operational_counters)?;
        self.record_gaps(&close.utc_day, gaps)?;
        self.rollover
            .as_mut()
            .expect("rollover was ensured")
            .producer_barriers
            .insert(producer);
        self.persist_rollover_barriers()?;
        self.maybe_mark_rollover_ready()
    }

    pub(super) fn record_writer_ack(
        &mut self,
        close: RawDayClose,
        source_kind: String,
    ) -> Result<Option<ManifestRolloverReady>> {
        if !LIVE_SOURCES.contains(&source_kind.as_str()) {
            return Err(invalid_data(format!("unknown writer finalize ack: {source_kind}")).into());
        }
        self.ensure_rollover_identity(&close)?;
        let rollover = self.rollover.as_mut().expect("rollover was ensured");
        if rollover.ready_sent {
            return Err(invalid_data("writer finalize ack arrived after rollover ready").into());
        }
        if !rollover.writer_finalize_acks.insert(source_kind.clone()) {
            return Err(invalid_data(format!(
                "duplicate writer finalize ack: source={source_kind} generation={} close_id={}",
                close.generation, close.close_id
            ))
            .into());
        }
        self.persist_rollover_barriers()?;
        self.maybe_mark_rollover_ready()
    }

    fn ensure_rollover_identity(&mut self, close: &RawDayClose) -> Result<()> {
        let active = self
            .active_epoch
            .as_ref()
            .ok_or_else(|| invalid_data("rollover barrier arrived without an active epoch"))?;
        if active.utc_day != close.utc_day || active.generation != close.generation {
            return Err(invalid_data(format!(
                "rollover barrier active epoch mismatch: active={active:?} close={close:?}"
            ))
            .into());
        }
        if close.close_id == 0
            || close.next_generation
                != close
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("rollover generation overflow"))?
        {
            return Err(invalid_data(format!(
                "invalid rollover generation or close-id: {close:?}"
            ))
            .into());
        }
        let derived_old = utc_day_string_from_ms(close.boundary_utc_ms.saturating_sub(1))
            .ok_or_else(|| invalid_data("UTC day unavailable before manifest rollover"))?;
        let derived_new = utc_day_string_from_ms(close.boundary_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable at manifest rollover"))?;
        if derived_old != close.utc_day || derived_new != close.next_utc_day {
            return Err(invalid_data(format!(
                "rollover boundary/day mismatch: close={close:?} derived_old={derived_old} derived_new={derived_new}"
            ))
            .into());
        }
        match self.rollover.as_ref() {
            Some(rollover) if rollover.close != *close => {
                return Err(invalid_data(format!(
                    "rollover tuple mismatch: expected={:?} actual={close:?}",
                    rollover.close
                ))
                .into());
            }
            Some(_) => {}
            None => {
                self.rollover = Some(ManifestRolloverRendezvous {
                    close: close.clone(),
                    producer_barriers: BTreeSet::new(),
                    writer_finalize_acks: BTreeSet::new(),
                    ready_sent: false,
                    old_day_committed: false,
                    old_day_mode: None,
                    raw_day_gate: None,
                });
            }
        }
        Ok(())
    }

    fn persist_rollover_barriers(&mut self) -> Result<()> {
        let rollover = self.rollover.as_ref().expect("rollover was ensured");
        let utc_day = rollover.close.utc_day.clone();
        let producers = rollover.producer_barriers.clone();
        let writers = rollover.writer_finalize_acks.clone();
        let day_dir = self.day_dir(&utc_day);
        let manifest = self
            .manifests
            .get_mut(&utc_day)
            .expect("manifest was ensured");
        record_day_barrier_state(&day_dir, manifest, &producers, &writers)?;
        Ok(())
    }

    fn maybe_mark_rollover_ready(&mut self) -> Result<Option<ManifestRolloverReady>> {
        let rollover = self.rollover.as_mut().expect("rollover was ensured");
        if rollover.producer_barriers.len() != LOGICAL_PRODUCERS.len()
            || rollover.writer_finalize_acks.len() != LIVE_SOURCES.len()
        {
            return Ok(None);
        }
        if rollover.ready_sent {
            return Err(invalid_data("rollover rendezvous became ready more than once").into());
        }
        rollover.ready_sent = true;
        Ok(Some(ManifestRolloverReady {
            close: rollover.close.clone(),
        }))
    }

    fn register_rollover_gate(
        &mut self,
        close: &RawDayClose,
        raw_day_gate: Arc<RawDayGate>,
    ) -> Result<()> {
        self.ensure_rollover_identity(close)?;
        let rollover = self.rollover.as_mut().expect("rollover was ensured");
        match rollover.raw_day_gate.as_ref() {
            Some(existing) if Arc::ptr_eq(existing, &raw_day_gate) => Ok(()),
            Some(_) => Err(invalid_data(format!(
                "writer finalize commands disagreed on the raw-day gate: close={close:?}"
            ))
            .into()),
            None => {
                rollover.raw_day_gate = Some(raw_day_gate);
                Ok(())
            }
        }
    }

    fn publish_rollover_ready(&self, ready: &ManifestRolloverReady) -> Result<()> {
        let rollover = self
            .rollover
            .as_ref()
            .ok_or_else(|| invalid_data("rollover ready has no owner rendezvous"))?;
        if rollover.close != ready.close
            || !rollover.ready_sent
            || rollover.producer_barriers.len() != LOGICAL_PRODUCERS.len()
            || rollover.writer_finalize_acks.len() != LIVE_SOURCES.len()
        {
            return Err(invalid_data(format!(
                "rollover ready witness lacks the complete owner rendezvous: ready={ready:?}"
            ))
            .into());
        }
        rollover
            .raw_day_gate
            .as_ref()
            .ok_or_else(|| invalid_data("complete rollover rendezvous has no raw-day gate"))?
            .publish_writer_rendezvous(&ready.close)
    }

    pub(super) fn commit_rollover_old_day(
        &mut self,
        close: &RawDayClose,
        mode: ManifestRolloverCommitMode,
    ) -> Result<()> {
        let rollover = self
            .rollover
            .as_ref()
            .ok_or_else(|| invalid_data("rollover commit arrived without rendezvous"))?;
        if rollover.close != *close
            || !rollover.ready_sent
            || rollover.old_day_committed
            || rollover.producer_barriers.len() != LOGICAL_PRODUCERS.len()
            || rollover.writer_finalize_acks.len() != LIVE_SOURCES.len()
        {
            return Err(invalid_data(format!(
                "rollover commit identity/readiness mismatch: requested={close:?}"
            ))
            .into());
        }

        let old_day_dir = self.day_dir(&close.utc_day);
        let old_manifest = self
            .manifests
            .get_mut(&close.utc_day)
            .expect("active rollover manifest exists");
        match &mode {
            ManifestRolloverCommitMode::Normal => {
                close_day_manifest_with_collection_end(
                    &old_day_dir,
                    old_manifest,
                    "utc_day_barrier",
                    close.boundary_utc_ms,
                )?;
            }
            ManifestRolloverCommitMode::PendingExitKnown { ended_utc_ms } => {
                close_day_manifest_with_collection_end(
                    &old_day_dir,
                    old_manifest,
                    "utc_day_barrier",
                    *ended_utc_ms,
                )?;
            }
            ManifestRolloverCommitMode::UncleanOldDay => {
                close_day_manifest_with_unclean_end(
                    &old_day_dir,
                    old_manifest,
                    "unclean_rollover",
                    &["collection_day_unclean_end_unknown".to_owned()],
                )?;
            }
        }
        self.active_epoch = None;
        let rollover = self
            .rollover
            .as_mut()
            .expect("rollover exists during old-day commit");
        rollover.old_day_committed = true;
        rollover.old_day_mode = Some(mode);
        Ok(())
    }

    pub(super) fn activate_rollover_new_day(&mut self, close: &RawDayClose) -> Result<()> {
        let rollover = self
            .rollover
            .as_ref()
            .ok_or_else(|| invalid_data("new-day activation arrived without rollover"))?;
        if rollover.close != *close
            || !rollover.ready_sent
            || !rollover.old_day_committed
            || rollover.old_day_mode.as_ref() != Some(&ManifestRolloverCommitMode::Normal)
        {
            return Err(invalid_data(format!(
                "new-day activation identity/cutpoint mismatch: requested={close:?}"
            ))
            .into());
        }
        if self.active_epoch.is_some() {
            return Err(invalid_data("new-day activation found an active manifest epoch").into());
        }
        self.ensure_open(&close.next_utc_day)?;
        self.activate_coverage(&close.next_utc_day, close.boundary_utc_ms)?;
        self.active_epoch = Some(ManifestActiveEpoch {
            utc_day: close.next_utc_day.clone(),
            generation: close.next_generation,
        });
        self.rollover = None;
        Ok(())
    }

    pub(super) fn prepare_rollover_exit_day(&mut self, close: &RawDayClose) -> Result<()> {
        let rollover = self
            .rollover
            .as_ref()
            .ok_or_else(|| invalid_data("exit-day preparation arrived without rollover"))?;
        if rollover.close != *close
            || !rollover.ready_sent
            || !rollover.old_day_committed
            || !matches!(
                rollover.old_day_mode.as_ref(),
                Some(ManifestRolloverCommitMode::Normal)
                    | Some(ManifestRolloverCommitMode::PendingExitKnown { .. })
            )
        {
            return Err(invalid_data(format!(
                "exit-day preparation identity/cutpoint mismatch: requested={close:?}"
            ))
            .into());
        }
        if self.active_epoch.is_some() {
            return Err(invalid_data("exit-day preparation found an active manifest epoch").into());
        }
        self.ensure_open(&close.next_utc_day)?;
        self.rollover = None;
        Ok(())
    }

    pub(super) fn finish_unclean_rollover(&mut self, close: &RawDayClose) -> Result<()> {
        let rollover = self
            .rollover
            .as_ref()
            .ok_or_else(|| invalid_data("unclean rollover finish arrived without rollover"))?;
        if rollover.close != *close
            || !rollover.ready_sent
            || !rollover.old_day_committed
            || rollover.old_day_mode.as_ref() != Some(&ManifestRolloverCommitMode::UncleanOldDay)
        {
            return Err(invalid_data(format!(
                "unclean rollover finish identity/cutpoint mismatch: requested={close:?}"
            ))
            .into());
        }
        if self.active_epoch.is_some() {
            return Err(
                invalid_data("unclean rollover finish found an active manifest epoch").into(),
            );
        }
        self.rollover = None;
        Ok(())
    }

    pub(super) fn shutdown(
        &mut self,
        close: &RuntimeCloseIdentity,
        disposition: &ManifestShutdownDisposition,
        gaps: &[String],
        operational_counters: &BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>,
    ) -> Result<()> {
        let active = self
            .active_epoch
            .as_ref()
            .ok_or_else(|| invalid_data("manifest shutdown arrived without an active epoch"))?;
        if active.utc_day != close.utc_day || active.generation != close.generation {
            return Err(invalid_data(format!(
                "manifest shutdown active epoch mismatch: active={active:?} close={close:?}"
            ))
            .into());
        }
        if close.close_id == 0 {
            return Err(invalid_data("manifest shutdown close-id must be nonzero").into());
        }
        if self.rollover.is_some() {
            return Err(invalid_data("manifest shutdown arrived during an open rollover").into());
        }
        let manifest = self
            .manifests
            .get(&close.utc_day)
            .ok_or_else(|| invalid_data("manifest shutdown active day is missing"))?;
        if manifest_state(manifest)? != "open" {
            return Err(invalid_data("manifest shutdown requires an open daily manifest").into());
        }
        let coverage = manifest_collection_day_coverage(manifest)?
            .ok_or_else(|| invalid_data("manifest shutdown coverage is missing"))?;
        if coverage.state != CollectionDayCoverageState::Active {
            return Err(invalid_data(format!(
                "manifest shutdown requires active collection coverage: actual={:?}",
                coverage.state
            ))
            .into());
        }
        if let ManifestShutdownDisposition::EndedKnown { ended_utc_ms } = disposition {
            let last_started_utc_ms = coverage
                .last_started_utc_ms
                .expect("validated active coverage has a last start");
            let ended_day = utc_day_string_from_ms(*ended_utc_ms)
                .ok_or_else(|| invalid_data("UTC day unavailable for manifest shutdown end"))?;
            let end_belongs_to_active_day = ended_day == close.utc_day
                || (ended_utc_ms.rem_euclid(UTC_DAY_MS) == 0
                    && utc_day_string_from_ms(ended_utc_ms.saturating_sub(1)).as_deref()
                        == Some(close.utc_day.as_str()));
            if *ended_utc_ms < last_started_utc_ms || !end_belongs_to_active_day {
                return Err(invalid_data(format!(
                    "manifest shutdown end is outside active coverage: close={close:?} last_started_utc_ms={last_started_utc_ms} ended_utc_ms={ended_utc_ms}"
                ))
                .into());
            }
        }
        for (counter_day, counters) in operational_counters {
            self.record_operational_counters(counter_day, counters)?;
        }
        self.record_gaps(&close.utc_day, gaps)?;
        let day_dir = self.day_dir(&close.utc_day);
        let manifest = self
            .manifests
            .get_mut(&close.utc_day)
            .ok_or_else(|| invalid_data("manifest shutdown active day is missing"))?;
        match disposition {
            ManifestShutdownDisposition::EndedKnown { ended_utc_ms } => {
                mark_collection_day_coverage_ended_known(&day_dir, manifest, *ended_utc_ms)?;
            }
            ManifestShutdownDisposition::UncleanEndUnknown => {
                mark_collection_day_coverage_unclean(&day_dir, manifest)?;
            }
        }
        self.active_epoch = None;
        Ok(())
    }
}

pub(super) fn required_source_counter_defaults() -> BTreeMap<String, SourceOperationalCounterDelta>
{
    LIVE_SOURCES
        .iter()
        .map(|source_kind| {
            (
                (*source_kind).to_owned(),
                SourceOperationalCounterDelta::default(),
            )
        })
        .collect()
}

pub(super) async fn run_manifest_owner(
    mut owner: ManifestOwnerState,
    mut commands: mpsc::Receiver<ManifestCommand>,
    rollover_ready_tx: mpsc::Sender<ManifestRolloverReady>,
) -> Result<()> {
    while let Some(command) = commands.recv().await {
        match command {
            ManifestCommand::Activate {
                utc_day,
                started_utc_ms,
                generation,
                raw_day_gate,
                deadline,
                reply,
                ack_mode,
            } => {
                let result = raw_day_gate.commit_startup_manifest_activation(
                    &utc_day,
                    generation,
                    deadline,
                    || owner.activate_initial(&utc_day, started_utc_ms, generation),
                );
                let reply_result = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                match ack_mode {
                    ManifestActivationAckMode::Send => {
                        let _ = reply.send(reply_result);
                    }
                    #[cfg(test)]
                    ManifestActivationAckMode::Drop => drop(reply),
                }
                result?;
            }
            ManifestCommand::Finalized(delta) => owner.apply_finalized(delta)?,
            ManifestCommand::ProducerBarrier {
                close,
                producer,
                gaps,
                operational_counters,
            } => {
                if let Some(ready) = owner.record_producer_barrier(
                    close.clone(),
                    producer,
                    &gaps,
                    &operational_counters,
                )? {
                    owner.publish_rollover_ready(&ready)?;
                    rollover_ready_tx.send(ready).await.map_err(|_| {
                        invalid_data("rollover-ready channel closed before owner rendezvous")
                    })?;
                }
            }
            ManifestCommand::WriterDayFinalized {
                close,
                source_kind,
                raw_day_gate,
            } => {
                owner.register_rollover_gate(&close, Arc::clone(&raw_day_gate))?;
                let ready = owner.record_writer_ack(close.clone(), source_kind)?;
                if let Some(ready) = ready {
                    owner.publish_rollover_ready(&ready)?;
                    rollover_ready_tx.send(ready).await.map_err(|_| {
                        invalid_data("rollover-ready channel closed before owner rendezvous")
                    })?;
                }
            }
            ManifestCommand::CommitRolloverOldDay {
                close,
                mode,
                normal_gate,
                deadline,
                reply,
            } => {
                let result = match (&mode, normal_gate) {
                    (ManifestRolloverCommitMode::Normal, Some(gate)) => gate
                        .commit_old_day_if_no_exit(&close, deadline, || {
                            owner.commit_rollover_old_day(&close, mode)
                        })
                        .map(|committed| {
                            if committed {
                                ManifestOldDayCommitReply::Committed
                            } else {
                                ManifestOldDayCommitReply::PreparedExitWon
                            }
                        }),
                    (ManifestRolloverCommitMode::Normal, None) => Err(invalid_data(
                        "normal old-day commit requires the authoritative raw-day gate",
                    )
                    .into()),
                    (_, Some(_)) => Err(invalid_data(
                        "non-normal old-day commit must not carry a normal-commit gate",
                    )
                    .into()),
                    (_, None) => owner
                        .commit_rollover_old_day(&close, mode)
                        .map(|_| ManifestOldDayCommitReply::Committed),
                };
                let reply_result = result.as_ref().copied().map_err(ToString::to_string);
                let _ = reply.send(reply_result);
                if result.is_err() {
                    result?;
                }
            }
            ManifestCommand::ActivateRolloverNewDay {
                close,
                raw_day_gate,
                deadline,
                reply,
                ack_mode,
            } => {
                let result = raw_day_gate.commit_manifest_activation(&close, deadline, || {
                    owner.activate_rollover_new_day(&close)
                });
                let prepared_exit_won = matches!(result, Ok(false));
                let reply_result = result
                    .as_ref()
                    .map(|committed| {
                        if *committed {
                            ManifestActivationReply::Committed
                        } else {
                            ManifestActivationReply::PreparedExitWon
                        }
                    })
                    .map_err(ToString::to_string);
                if result.is_err()
                    || prepared_exit_won
                    || matches!(ack_mode, ManifestActivationAckMode::Send)
                {
                    let _ = reply.send(reply_result);
                } else {
                    drop(reply);
                }
                if result.is_err() {
                    result?;
                }
            }
            ManifestCommand::PrepareRolloverExitDay { close, reply } => {
                let result = owner.prepare_rollover_exit_day(&close);
                let reply_result = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                let _ = reply.send(reply_result);
                result?;
                return Ok(());
            }
            ManifestCommand::FinishUncleanRollover { close, reply } => {
                let result = owner.finish_unclean_rollover(&close);
                let reply_result = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                let _ = reply.send(reply_result);
                result?;
                return Ok(());
            }
            ManifestCommand::Shutdown {
                close,
                disposition,
                gaps,
                operational_counters,
                reply,
            } => {
                let result = owner
                    .shutdown(&close, &disposition, &gaps, &operational_counters)
                    .map_err(|error| error.to_string());
                let should_stop = result.is_ok();
                let _ = reply.send(result);
                if should_stop {
                    return Ok(());
                }
                return Err(io::Error::other("daily manifest owner shutdown failed").into());
            }
        }
    }
    Err(io::Error::other("daily manifest command channel closed without shutdown").into())
}
