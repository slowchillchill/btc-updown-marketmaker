use super::*;

struct CoordinatorRollover {
    close: RawDayClose,
    cutpoint: RolloverCutpoint,
    ready: Option<ManifestRolloverReady>,
    pre_fence_unclean: bool,
    producer_replies:
        BTreeMap<String, oneshot::Sender<std::result::Result<DayBarrierRelease, String>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RolloverCutpoint {
    Collecting,
    ReadyLatched,
    StopIssued,
    StopIssuedAfterOldCommit,
    OldCommitCommandSent,
    OldCommitted,
    ResumeTokensDelivered,
    ResumeTokensArmed,
    ActivationCommandSent,
    ActivationAcked,
    DeferredFlushing,
    RawActivated,
    Released,
    Terminal,
    OutcomeUncertain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DurableMutationAck {
    Confirmed,
    OutcomeUncertain(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OldDayCommitAck {
    Confirmed,
    PreparedExitWon,
    OutcomeUncertain(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ActivationMutationAck {
    Confirmed,
    PreparedExitWon,
    OutcomeUncertain(String),
}

pub(super) struct ProducerReleaseController {
    pub(super) close: RawDayClose,
    pub(super) phase: ProducerReleasePhase,
    pub(super) phase_tx: watch::Sender<ProducerReleasePhase>,
    pub(super) ack_rx: mpsc::Receiver<ProducerReleaseAck>,
    pub(super) armed: BTreeSet<String>,
    pub(super) flushed: BTreeSet<String>,
}

impl ProducerReleaseController {
    pub(super) fn set_phase(&mut self, phase: ProducerReleasePhase) {
        self.phase = phase.clone();
        self.phase_tx.send_replace(phase);
    }

    pub(super) fn stop(&mut self) {
        self.stop_with_close(RuntimeCloseIdentity::from_rollover_old_day(&self.close));
    }

    pub(super) fn stop_with_close(&mut self, close: RuntimeCloseIdentity) {
        self.set_phase(ProducerReleasePhase::Stop(close));
    }

    pub(super) fn accept_ack(&mut self, ack: ProducerReleaseAck) -> Result<()> {
        let expected_close = RuntimeCloseIdentity::from_rollover_old_day(&self.close);
        if !LOGICAL_PRODUCERS.contains(&ack.producer.as_str()) || ack.close != expected_close {
            return Err(invalid_data(format!(
                "producer release ack identity mismatch: ack={ack:?} expected_close={expected_close:?}"
            ))
            .into());
        }
        if let Some(error) = ack.error {
            return Err(invalid_data(format!(
                "producer release ack reported failure: producer={} kind={:?} error={error}",
                ack.producer, ack.kind
            ))
            .into());
        }
        let set = match ack.kind {
            ProducerReleaseAckKind::Armed if self.phase == ProducerReleasePhase::Held => {
                &mut self.armed
            }
            ProducerReleaseAckKind::Flushed
                if self.phase == ProducerReleasePhase::FlushDeferred
                    && self.armed.contains(&ack.producer) =>
            {
                &mut self.flushed
            }
            _ => {
                return Err(invalid_data(format!(
                    "producer release ack arrived at invalid phase: producer={} kind={:?} phase={:?}",
                    ack.producer, ack.kind, self.phase
                ))
                .into());
            }
        };
        if !set.insert(ack.producer.clone()) {
            return Err(invalid_data(format!(
                "duplicate producer release ack: producer={} kind={:?}",
                ack.producer, ack.kind
            ))
            .into());
        }
        Ok(())
    }

    pub(super) fn all_armed(&self) -> bool {
        self.armed.len() == LOGICAL_PRODUCERS.len()
    }

    pub(super) fn all_flushed(&self) -> bool {
        self.flushed.len() == LOGICAL_PRODUCERS.len()
    }
}

impl Drop for ProducerReleaseController {
    fn drop(&mut self) {
        if !matches!(
            self.phase,
            ProducerReleasePhase::ResumeSources | ProducerReleasePhase::Stop(_)
        ) {
            self.phase_tx.send_replace(ProducerReleasePhase::Stop(
                RuntimeCloseIdentity::from_rollover_old_day(&self.close),
            ));
        }
    }
}

pub(super) struct RolloverCoordinatorState {
    pending: Option<CoordinatorRollover>,
    activation_ack_mode: ManifestActivationAckMode,
}

impl RolloverCoordinatorState {
    pub(super) fn new(activation_ack_mode: ManifestActivationAckMode) -> Self {
        Self {
            pending: None,
            activation_ack_mode,
        }
    }

    pub(super) fn register_producer_barrier(
        &mut self,
        producer: String,
        close: RawDayClose,
        reply: oneshot::Sender<std::result::Result<DayBarrierRelease, String>>,
    ) -> Result<()> {
        if !LOGICAL_PRODUCERS.contains(&producer.as_str()) {
            return Err(invalid_data(format!(
                "coordinator received unknown producer barrier: {producer}"
            ))
            .into());
        }
        let pending = self.pending.get_or_insert_with(|| CoordinatorRollover {
            close: close.clone(),
            cutpoint: RolloverCutpoint::Collecting,
            ready: None,
            pre_fence_unclean: false,
            producer_replies: BTreeMap::new(),
        });
        if pending.cutpoint != RolloverCutpoint::Collecting {
            return Err(invalid_data(format!(
                "producer barrier arrived after rollover collecting cutpoint: producer={producer} cutpoint={:?}",
                pending.cutpoint
            ))
            .into());
        }
        if pending.close != close {
            return Err(invalid_data(format!(
                "coordinator rollover tuple mismatch: expected={:?} actual={close:?}",
                pending.close
            ))
            .into());
        }
        if pending
            .producer_replies
            .insert(producer.clone(), reply)
            .is_some()
        {
            return Err(invalid_data(format!(
                "coordinator received duplicate producer barrier reply: {producer}"
            ))
            .into());
        }
        Ok(())
    }

    pub(super) fn has_producer_barrier(&self, producer: &str, close: &RawDayClose) -> bool {
        self.pending.as_ref().is_some_and(|pending| {
            pending.close == *close && pending.producer_replies.contains_key(producer)
        })
    }

    pub(super) fn latch_ready(&mut self, ready: ManifestRolloverReady) -> Result<()> {
        let pending = self.pending.as_mut().ok_or_else(|| {
            invalid_data("manifest owner reported rollover ready without coordinator barriers")
        })?;
        if pending.close != ready.close || pending.producer_replies.len() != LOGICAL_PRODUCERS.len()
        {
            return Err(invalid_data(format!(
                "coordinator rollover readiness mismatch: ready={:?} pending_close={:?} producer_count={}",
                ready.close,
                pending.close,
                pending.producer_replies.len()
            ))
            .into());
        }
        if pending.cutpoint != RolloverCutpoint::Collecting || pending.ready.is_some() {
            return Err(invalid_data(format!(
                "coordinator received duplicate or late rollover ready: cutpoint={:?} ready={ready:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.ready = Some(ready);
        pending.cutpoint = RolloverCutpoint::ReadyLatched;
        Ok(())
    }

    pub(super) fn ready(&self) -> Result<&ManifestRolloverReady> {
        self.pending
            .as_ref()
            .and_then(|pending| pending.ready.as_ref())
            .ok_or_else(|| invalid_data("coordinator rollover ready is not latched").into())
    }

    pub(super) fn cutpoint(&self) -> Option<RolloverCutpoint> {
        self.pending.as_ref().map(|pending| pending.cutpoint)
    }

    pub(super) fn mark_pre_fence_unclean(&mut self) {
        if let Some(pending) = self.pending.as_mut()
            && matches!(
                pending.cutpoint,
                RolloverCutpoint::Collecting | RolloverCutpoint::ReadyLatched
            )
        {
            pending.pre_fence_unclean = true;
        }
    }

    pub(super) fn pre_fence_unclean(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.pre_fence_unclean)
    }

    pub(super) async fn commit_old_day_once(
        &mut self,
        mode: ManifestRolloverCommitMode,
        manifest_tx: &mpsc::Sender<ManifestCommand>,
        normal_gate: Option<Arc<RawDayGate>>,
        deadline: Option<RuntimeDeadlineCandidate>,
    ) -> Result<OldDayCommitAck> {
        let ready = self.ready()?.clone();
        let cutpoint = self
            .cutpoint()
            .ok_or_else(|| invalid_data("old-day commit found no pending rollover"))?;
        if !matches!(
            cutpoint,
            RolloverCutpoint::ReadyLatched | RolloverCutpoint::StopIssued
        ) {
            return Err(
                invalid_data(format!("old-day commit at invalid cutpoint: {cutpoint:?}")).into(),
            );
        }
        let (old_reply_tx, old_reply_rx) = oneshot::channel();
        if let Err(error) = send_manifest_command_once(
            manifest_tx,
            ManifestCommand::CommitRolloverOldDay {
                close: ready.close.clone(),
                mode,
                normal_gate,
                deadline,
                reply: old_reply_tx,
            },
            "old-day rollover commit",
        )
        .await
        {
            let error = format!("old-day rollover commit was not enqueued: {error}");
            self.mark_outcome_uncertain(error.clone());
            return Ok(OldDayCommitAck::OutcomeUncertain(error));
        }
        self.pending
            .as_mut()
            .expect("pending rollover exists during old-day commit")
            .cutpoint = RolloverCutpoint::OldCommitCommandSent;
        match timeout(STARTUP_GATE_TIMEOUT, old_reply_rx).await {
            Ok(Ok(Ok(ManifestOldDayCommitReply::Committed))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists after old-day commit")
                    .cutpoint = RolloverCutpoint::OldCommitted;
                Ok(OldDayCommitAck::Confirmed)
            }
            Ok(Ok(Ok(ManifestOldDayCommitReply::PreparedExitWon))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists after prepared exit won")
                    .cutpoint = RolloverCutpoint::ReadyLatched;
                Ok(OldDayCommitAck::PreparedExitWon)
            }
            Ok(Ok(Err(error))) => {
                let error =
                    format!("old-day rollover commit returned error after command: {error}");
                self.mark_outcome_uncertain(error.clone());
                Ok(OldDayCommitAck::OutcomeUncertain(error))
            }
            Ok(Err(_)) => {
                let error = "old-day rollover commit reply dropped after command".to_owned();
                self.mark_outcome_uncertain(error.clone());
                Ok(OldDayCommitAck::OutcomeUncertain(error))
            }
            Err(_) => {
                let error = "old-day rollover commit reply timeout after command".to_owned();
                self.mark_outcome_uncertain(error.clone());
                Ok(OldDayCommitAck::OutcomeUncertain(error))
            }
        }
    }

    pub(super) fn mark_outcome_uncertain(&mut self, error: String) -> DurableMutationAck {
        if let Some(pending) = self.pending.as_mut() {
            pending.cutpoint = RolloverCutpoint::OutcomeUncertain;
        }
        DurableMutationAck::OutcomeUncertain(error)
    }

    async fn prepare_exit_day_once(
        &mut self,
        close: &RawDayClose,
        manifest_tx: &mpsc::Sender<ManifestCommand>,
    ) -> Result<DurableMutationAck> {
        if !matches!(
            self.cutpoint(),
            Some(RolloverCutpoint::OldCommitted | RolloverCutpoint::StopIssuedAfterOldCommit)
        ) {
            return Err(
                invalid_data("exit-day preparation requires a confirmed old-day commit").into(),
            );
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        send_manifest_command_once(
            manifest_tx,
            ManifestCommand::PrepareRolloverExitDay {
                close: close.clone(),
                reply: reply_tx,
            },
            "exit-day preparation",
        )
        .await?;
        match timeout(STARTUP_GATE_TIMEOUT, reply_rx).await {
            Ok(Ok(Ok(()))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists during exit-day preparation")
                    .cutpoint = RolloverCutpoint::Terminal;
                Ok(DurableMutationAck::Confirmed)
            }
            Ok(Ok(Err(error))) => Ok(self.mark_outcome_uncertain(format!(
                "rollover exit-day preparation returned error after command: {error}"
            ))),
            Ok(Err(_)) => Ok(self.mark_outcome_uncertain(
                "rollover exit-day preparation reply dropped after command".to_owned(),
            )),
            Err(_) => Ok(self.mark_outcome_uncertain(
                "rollover exit-day preparation reply timeout after command".to_owned(),
            )),
        }
    }

    async fn finish_unclean_rollover_once(
        &mut self,
        close: &RawDayClose,
        manifest_tx: &mpsc::Sender<ManifestCommand>,
    ) -> Result<DurableMutationAck> {
        if self.cutpoint() != Some(RolloverCutpoint::OldCommitted) {
            return Err(invalid_data(
                "unclean rollover finish requires a confirmed old-day commit",
            )
            .into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        send_manifest_command_once(
            manifest_tx,
            ManifestCommand::FinishUncleanRollover {
                close: close.clone(),
                reply: reply_tx,
            },
            "unclean rollover finish",
        )
        .await?;
        match timeout(STARTUP_GATE_TIMEOUT, reply_rx).await {
            Ok(Ok(Ok(()))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists during unclean finish")
                    .cutpoint = RolloverCutpoint::Terminal;
                Ok(DurableMutationAck::Confirmed)
            }
            Ok(Ok(Err(error))) => Ok(self.mark_outcome_uncertain(format!(
                "unclean rollover finish returned error after command: {error}"
            ))),
            Ok(Err(_)) => Ok(self.mark_outcome_uncertain(
                "unclean rollover finish reply dropped after command".to_owned(),
            )),
            Err(_) => Ok(self.mark_outcome_uncertain(
                "unclean rollover finish reply timeout after command".to_owned(),
            )),
        }
    }

    pub(super) fn issue_stop_with_close(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("producer stop found no pending rollover"))?;
        if !matches!(
            pending.cutpoint,
            RolloverCutpoint::ReadyLatched | RolloverCutpoint::OldCommitted
        ) {
            return Err(invalid_data(format!(
                "producer stop at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        let expected = RuntimeCloseIdentity::from_rollover_old_day(&pending.close);
        let replies = std::mem::take(&mut pending.producer_replies);
        pending.cutpoint = if pending.cutpoint == RolloverCutpoint::OldCommitted {
            RolloverCutpoint::StopIssuedAfterOldCommit
        } else {
            RolloverCutpoint::StopIssued
        };
        let mut failures = Vec::new();
        for (producer, reply) in replies {
            if reply
                .send(Ok(DayBarrierRelease::StopWithClose(expected.clone())))
                .is_err()
            {
                failures.push(producer);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(invalid_data(format!(
                "producer stop reply receivers closed: {failures:?}"
            ))
            .into())
        }
    }

    pub(super) fn stop_replies_preserving_cutpoint(&mut self) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        let expected = RuntimeCloseIdentity::from_rollover_old_day(&pending.close);
        for (_, reply) in std::mem::take(&mut pending.producer_replies) {
            let _ = reply.send(Ok(DayBarrierRelease::StopWithClose(expected.clone())));
        }
    }

    pub(super) async fn finalize_pending_exit_after_reports(
        &mut self,
        exit: &PendingExit,
        manifest_tx: &mpsc::Sender<ManifestCommand>,
    ) -> Result<DurableMutationAck> {
        let ready = self.ready()?.clone();
        if ready.close != exit.close {
            return Err(invalid_data(format!(
                "pending-exit rollover identity mismatch: ready={:?} exit={exit:?}",
                ready.close
            ))
            .into());
        }
        if !matches!(
            self.cutpoint(),
            Some(
                RolloverCutpoint::StopIssued
                    | RolloverCutpoint::StopIssuedAfterOldCommit
                    | RolloverCutpoint::OldCommitted
            )
        ) {
            return Err(invalid_data(format!(
                "pending-exit finalization requires stop proof cutpoint: {:?}",
                self.cutpoint()
            ))
            .into());
        }
        let old_already_committed = matches!(
            self.cutpoint(),
            Some(RolloverCutpoint::OldCommitted | RolloverCutpoint::StopIssuedAfterOldCommit)
        );
        let unclean = exit.unclean || self.pre_fence_unclean();
        let mode = if unclean {
            ManifestRolloverCommitMode::UncleanOldDay
        } else {
            ManifestRolloverCommitMode::PendingExitKnown {
                ended_utc_ms: exit.close.boundary_utc_ms,
            }
        };
        if !old_already_committed {
            let commit = self
                .commit_old_day_once(mode, manifest_tx, None, None)
                .await?;
            match commit {
                OldDayCommitAck::Confirmed => {}
                OldDayCommitAck::OutcomeUncertain(error) => {
                    return Ok(DurableMutationAck::OutcomeUncertain(error));
                }
                OldDayCommitAck::PreparedExitWon => {
                    return Err(invalid_data(
                        "pending-exit old-day commit was skipped by another prepared exit",
                    )
                    .into());
                }
            }
        } else if self.pre_fence_unclean() {
            return Err(invalid_data(
                "pre-fence unclean evidence arrived after the old-day commit",
            )
            .into());
        }
        let finish = if unclean && !old_already_committed {
            self.finish_unclean_rollover_once(&ready.close, manifest_tx)
                .await?
        } else {
            self.prepare_exit_day_once(&ready.close, manifest_tx)
                .await?
        };
        if matches!(finish, DurableMutationAck::Confirmed) {
            self.pending.take();
        }
        Ok(finish)
    }

    pub(super) fn prepare_release_controller(&mut self) -> Result<ProducerReleaseController> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("release preparation found no pending rollover"))?;
        if pending.cutpoint != RolloverCutpoint::OldCommitted
            || pending.producer_replies.len() != LOGICAL_PRODUCERS.len()
        {
            return Err(invalid_data(format!(
                "release preparation cutpoint/reply mismatch: cutpoint={:?} replies={}",
                pending.cutpoint,
                pending.producer_replies.len()
            ))
            .into());
        }
        let (phase_tx, phase_rx) = watch::channel(ProducerReleasePhase::Held);
        let (ack_tx, ack_rx) = mpsc::channel(LOGICAL_PRODUCERS.len() * 2);
        let replies = std::mem::take(&mut pending.producer_replies);
        let mut controller = ProducerReleaseController {
            close: pending.close.clone(),
            phase: ProducerReleasePhase::Held,
            phase_tx,
            ack_rx,
            armed: BTreeSet::new(),
            flushed: BTreeSet::new(),
        };
        pending.cutpoint = RolloverCutpoint::ResumeTokensDelivered;
        let mut failures = Vec::new();
        for (producer, reply) in replies {
            let token = ProducerReleaseToken {
                producer: producer.clone(),
                close: pending.close.clone(),
                phase_rx: phase_rx.clone(),
                ack_tx: ack_tx.clone(),
            };
            if reply
                .send(Ok(DayBarrierRelease::PrepareResume(token)))
                .is_err()
            {
                failures.push(producer);
            }
        }
        drop(ack_tx);
        if failures.is_empty() {
            Ok(controller)
        } else {
            controller.stop();
            pending.cutpoint = RolloverCutpoint::StopIssuedAfterOldCommit;
            Err(invalid_data(format!(
                "producer release token receivers closed: {failures:?}"
            ))
            .into())
        }
    }

    pub(super) fn mark_tokens_armed(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("armed release found no pending rollover"))?;
        if pending.cutpoint != RolloverCutpoint::ResumeTokensDelivered {
            return Err(invalid_data(format!(
                "release armed at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.cutpoint = RolloverCutpoint::ResumeTokensArmed;
        Ok(())
    }

    pub(super) fn mark_release_stopped_after_old_commit(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("release stop found no pending rollover"))?;
        if !matches!(
            pending.cutpoint,
            RolloverCutpoint::ResumeTokensDelivered | RolloverCutpoint::ResumeTokensArmed
        ) {
            return Err(invalid_data(format!(
                "release stop at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.cutpoint = RolloverCutpoint::StopIssuedAfterOldCommit;
        Ok(())
    }

    pub(super) async fn activate_new_day_once(
        &mut self,
        manifest_tx: &mpsc::Sender<ManifestCommand>,
        raw_day_gate: Arc<RawDayGate>,
        deadline: Option<RuntimeDeadlineCandidate>,
    ) -> Result<ActivationMutationAck> {
        if self.cutpoint() != Some(RolloverCutpoint::ResumeTokensArmed) {
            return Err(
                invalid_data("new-day activation requires all release tokens armed").into(),
            );
        }
        let close = self.ready()?.close.clone();
        let (activation_reply_tx, activation_reply_rx) = oneshot::channel();
        send_manifest_command_once(
            manifest_tx,
            ManifestCommand::ActivateRolloverNewDay {
                close,
                raw_day_gate,
                deadline,
                reply: activation_reply_tx,
                ack_mode: self.activation_ack_mode,
            },
            "new-day activation",
        )
        .await?;
        self.pending
            .as_mut()
            .expect("pending rollover exists during activation")
            .cutpoint = RolloverCutpoint::ActivationCommandSent;
        match timeout(STARTUP_GATE_TIMEOUT, activation_reply_rx).await {
            Ok(Ok(Ok(ManifestActivationReply::Committed))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists after activation")
                    .cutpoint = RolloverCutpoint::ActivationAcked;
                Ok(ActivationMutationAck::Confirmed)
            }
            Ok(Ok(Ok(ManifestActivationReply::PreparedExitWon))) => {
                self.pending
                    .as_mut()
                    .expect("pending rollover exists after prepared exit won activation")
                    .cutpoint = RolloverCutpoint::StopIssuedAfterOldCommit;
                Ok(ActivationMutationAck::PreparedExitWon)
            }
            Ok(Ok(Err(error))) => {
                let error = format!("new-day activation returned error after command: {error}");
                self.mark_outcome_uncertain(error.clone());
                Ok(ActivationMutationAck::OutcomeUncertain(error))
            }
            Ok(Err(_)) => {
                let error = "new-day activation reply dropped after command".to_owned();
                self.mark_outcome_uncertain(error.clone());
                Ok(ActivationMutationAck::OutcomeUncertain(error))
            }
            Err(_) => {
                let error = "new-day activation reply timeout after command".to_owned();
                self.mark_outcome_uncertain(error.clone());
                Ok(ActivationMutationAck::OutcomeUncertain(error))
            }
        }
    }

    pub(super) fn mark_deferred_flushing(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("deferred flush found no pending rollover"))?;
        if pending.cutpoint != RolloverCutpoint::ActivationAcked {
            return Err(invalid_data(format!(
                "deferred flush at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.cutpoint = RolloverCutpoint::DeferredFlushing;
        Ok(())
    }

    pub(super) fn mark_raw_activated(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("raw activation found no pending rollover"))?;
        if pending.cutpoint != RolloverCutpoint::DeferredFlushing {
            return Err(invalid_data(format!(
                "raw activation at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.cutpoint = RolloverCutpoint::RawActivated;
        Ok(())
    }

    pub(super) fn finish_release(
        &mut self,
        controller: &mut ProducerReleaseController,
    ) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("release completion found no pending rollover"))?;
        if pending.cutpoint != RolloverCutpoint::RawActivated || !controller.all_flushed() {
            return Err(invalid_data(format!(
                "release completion proof mismatch: cutpoint={:?} flushed={:?}",
                pending.cutpoint, controller.flushed
            ))
            .into());
        }
        controller.set_phase(ProducerReleasePhase::ResumeSources);
        pending.cutpoint = RolloverCutpoint::Released;
        self.pending.take();
        Ok(())
    }

    pub(super) fn abandon_release_after_activation(&mut self) -> Result<()> {
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| invalid_data("activation release abandonment found no rollover"))?;
        if !matches!(
            pending.cutpoint,
            RolloverCutpoint::ActivationAcked
                | RolloverCutpoint::DeferredFlushing
                | RolloverCutpoint::RawActivated
        ) {
            return Err(invalid_data(format!(
                "activation release abandonment at invalid cutpoint: {:?}",
                pending.cutpoint
            ))
            .into());
        }
        pending.cutpoint = RolloverCutpoint::Terminal;
        self.pending.take();
        Ok(())
    }
}
