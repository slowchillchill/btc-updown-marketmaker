use super::*;

pub(super) const INITIAL_RAW_DAY_GENERATION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RawDayPermit {
    pub(super) utc_day: String,
    pub(super) generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RawDayClose {
    pub(super) utc_day: String,
    pub(super) next_utc_day: String,
    pub(super) generation: u64,
    pub(super) next_generation: u64,
    pub(super) close_id: u64,
    pub(super) boundary_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct RuntimeCloseIdentity {
    pub(super) utc_day: String,
    pub(super) generation: u64,
    pub(super) close_id: u64,
}

impl RuntimeCloseIdentity {
    pub(super) fn from_rollover_old_day(close: &RawDayClose) -> Self {
        Self {
            utc_day: close.utc_day.clone(),
            generation: close.generation,
            close_id: close.close_id,
        }
    }

    pub(super) fn from_rollover_next_day(close: &RawDayClose) -> Self {
        Self {
            utc_day: close.next_utc_day.clone(),
            generation: close.next_generation,
            close_id: close.close_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RuntimeEpoch {
    pub(super) utc_day: String,
    pub(super) generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RuntimeCloseCause {
    OperatorStop,
    RunDeadline,
    CapacityLimit,
    SourceInnerReturn { source_kind: &'static str },
    SourceWrapperReturn { source_kind: &'static str },
    SourceWrapperFailure { source_kind: &'static str },
    SourcePanic { source_kind: &'static str },
    SourceAbortWithoutPermit { source_kind: &'static str },
    WriterReturn { source_kind: String },
    WriterPanic { source_kind: String },
    WriterReportDisconnected { source_kind: String },
    CoordinatorFailure { stage: &'static str },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RuntimeExitRequest {
    pub(super) observed_utc_ms: i64,
    pub(super) cause: RuntimeCloseCause,
}

pub(super) enum SourceWrapperTerminalClass {
    ExistingInnerReturn,
    CleanReturn,
    CoordinatedStop(RuntimeCloseIdentity),
    Unclean(RuntimeCloseCause),
}

pub(super) struct SourceWrapperLifecycleObservation {
    pub(super) observed_utc_ms: i64,
    pub(super) epoch: Option<RuntimeEpoch>,
    pub(super) lifecycle_record_error: Option<String>,
    pub(super) replace_existing: bool,
    pub(super) termination_kind: SourceTaskTerminationKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreparedSameDayClose {
    pub(super) identity: RuntimeCloseIdentity,
    pub(super) ended_utc_ms: i64,
    pub(super) cause: RuntimeCloseCause,
    pub(super) unclean: bool,
    pub(super) unclean_cause: Option<RuntimeCloseCause>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingExit {
    pub(super) close: RawDayClose,
    pub(super) observed_utc_ms: i64,
    pub(super) cause: RuntimeCloseCause,
    pub(super) unclean: bool,
    pub(super) unclean_cause: Option<RuntimeCloseCause>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PreparedRuntimeExit {
    SameDay(PreparedSameDayClose),
    Rollover(PendingExit),
}

pub(super) fn apply_runtime_exit_request(
    raw_day_gate: &RawDayGate,
    request: &RuntimeExitRequest,
) -> Result<PreparedRuntimeExit> {
    raw_day_gate.prepare_runtime_exit_with_cause(request.observed_utc_ms, request.cause.clone())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RawDayGateState {
    Inactive,
    Active(RawDayPermit),
    Closing(RawDayClose),
    ClosingSameDay(PreparedSameDayClose),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StartupRawActivation {
    Active(RawDayPermit),
    Stopped(PreparedRuntimeExit),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StartupActivationOutcome {
    Released,
    CommittedStopped(PreparedRuntimeExit),
}

pub(super) struct RawDayGate {
    state_word: AtomicU64,
    next_close_id: AtomicU64,
    state: Mutex<RawDayGateState>,
    prepared_close: Mutex<Option<RawDayClose>>,
    prepared_same_day_close: Mutex<Option<PreparedSameDayClose>>,
    pending_exit: Mutex<Option<PendingExit>>,
    pre_activation_exit: Mutex<Option<RuntimeExitRequest>>,
    pub(super) startup_activation_committed: Mutex<Option<RawDayPermit>>,
    pub(super) activation_committed: Mutex<Option<RawDayClose>>,
    runtime_deadline: Mutex<Option<RuntimeDeadlineCandidate>>,
    rollover_transition: Mutex<()>,
    enqueue_fence: Arc<AsyncRwLock<()>>,
    source_fences: Mutex<BTreeMap<String, (u64, u64)>>,
    writer_rendezvous: Mutex<Option<RawDayClose>>,
    state_changes: watch::Sender<u64>,
}

pub(super) struct RawDayEnqueuePermit {
    pub(super) permit: RawDayPermit,
    _fence: OwnedRwLockReadGuard<()>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RawDayDeferredEpoch {
    pub(super) utc_day: String,
    pub(super) old_generation: u64,
    pub(super) expected_generation: u64,
}

pub(super) enum RawDayEnqueueDecision {
    Permit(RawDayEnqueuePermit),
    Defer(RawDayDeferredEpoch),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RawDayAcceptedTicket {
    Permit(RawDayPermit),
    Defer(RawDayDeferredEpoch),
}

impl RawDayAcceptedTicket {
    fn from_enqueue_decision(decision: RawDayEnqueueDecision) -> Self {
        match decision {
            RawDayEnqueueDecision::Permit(enqueue) => Self::Permit(enqueue.permit.clone()),
            RawDayEnqueueDecision::Defer(epoch) => Self::Defer(epoch),
        }
    }

    fn utc_day(&self) -> &str {
        match self {
            Self::Permit(permit) => &permit.utc_day,
            Self::Defer(epoch) => &epoch.utc_day,
        }
    }

    pub(super) fn dynamic_owned_bytes(&self) -> usize {
        match self {
            Self::Permit(permit) => permit.utc_day.capacity(),
            Self::Defer(epoch) => epoch.utc_day.capacity(),
        }
    }
}

impl RawDayGate {
    pub(super) fn new() -> Self {
        let (state_changes, _) = watch::channel(0);
        Self {
            state_word: AtomicU64::new(0),
            next_close_id: AtomicU64::new(1),
            state: Mutex::new(RawDayGateState::Inactive),
            prepared_close: Mutex::new(None),
            prepared_same_day_close: Mutex::new(None),
            pending_exit: Mutex::new(None),
            pre_activation_exit: Mutex::new(None),
            startup_activation_committed: Mutex::new(None),
            activation_committed: Mutex::new(None),
            runtime_deadline: Mutex::new(None),
            rollover_transition: Mutex::new(()),
            enqueue_fence: Arc::new(AsyncRwLock::new(())),
            source_fences: Mutex::new(BTreeMap::new()),
            writer_rendezvous: Mutex::new(None),
            state_changes,
        }
    }

    fn encoded_open_generation(generation: u64) -> Result<u64> {
        generation
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| invalid_data("raw-day generation encoding overflow").into())
    }

    fn encoded_closed_generation(generation: u64) -> Result<u64> {
        generation
            .checked_mul(2)
            .ok_or_else(|| invalid_data("raw-day generation encoding overflow").into())
    }

    fn next_close_id(&self) -> Result<u64> {
        self.next_close_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| invalid_data("raw-day close-id overflow").into())
    }

    fn notify_change(&self) {
        self.state_changes
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub(super) fn install_runtime_deadline(
        &self,
        candidate: Option<RuntimeDeadlineCandidate>,
    ) -> Result<()> {
        let mut installed = self
            .runtime_deadline
            .lock()
            .expect("raw-day runtime-deadline mutex poisoned");
        if installed.is_some() {
            return Err(invalid_data("raw-day runtime deadline already installed").into());
        }
        *installed = candidate;
        Ok(())
    }

    fn materialize_deadline_locked(
        &self,
        candidate: Option<RuntimeDeadlineCandidate>,
    ) -> Result<bool> {
        let installed = *self
            .runtime_deadline
            .lock()
            .expect("raw-day runtime-deadline mutex poisoned");
        let Some(candidate) = candidate
            .or(installed)
            .filter(|candidate| candidate.is_reached())
        else {
            return Ok(false);
        };
        match self.prepare_runtime_exit_with_cause_locked(
            candidate.observed_utc_ms,
            RuntimeCloseCause::RunDeadline,
        ) {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(self.current_state(), RawDayGateState::Inactive)
                    && self
                        .pre_activation_exit
                        .lock()
                        .expect("pre-activation exit mutex poisoned")
                        .is_some() =>
            {
                let _ = error;
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(super) async fn activate_startup(
        &self,
        utc_day: String,
        generation: u64,
    ) -> Result<RawDayPermit> {
        self.commit_startup_manifest_activation(&utc_day, generation, None, || Ok(()))?;
        match self
            .activate_startup_after_manifest(utc_day, generation, None)
            .await?
        {
            StartupRawActivation::Active(permit) => Ok(permit),
            StartupRawActivation::Stopped(exit) => Err(invalid_data(format!(
                "test startup activation was stopped after manifest commit: {exit:?}"
            ))
            .into()),
        }
    }

    pub(super) fn commit_startup_manifest_activation<F>(
        &self,
        utc_day: &str,
        generation: u64,
        deadline: Option<RuntimeDeadlineCandidate>,
        commit: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self.materialize_deadline_locked(deadline)? {
            return Err(invalid_data(
                "startup manifest activation rejected because the run deadline was reached",
            )
            .into());
        }
        if let Some(exit) = self
            .pre_activation_exit
            .lock()
            .expect("pre-activation exit mutex poisoned")
            .as_ref()
        {
            return Err(invalid_data(format!(
                "startup manifest activation rejected by a pre-activation exit: {exit:?}"
            ))
            .into());
        }
        if !matches!(self.current_state(), RawDayGateState::Inactive) {
            return Err(invalid_data(
                "startup manifest activation requires an inactive raw-day gate",
            )
            .into());
        }
        let mut committed = self
            .startup_activation_committed
            .lock()
            .expect("startup activation-commit mutex poisoned");
        if committed.is_some() {
            return Err(invalid_data("duplicate startup manifest activation commit").into());
        }
        commit()?;
        *committed = Some(RawDayPermit {
            utc_day: utc_day.to_owned(),
            generation,
        });
        drop(committed);
        self.notify_change();
        Ok(())
    }

    #[cfg(test)]
    async fn activate_startup_after_manifest(
        &self,
        utc_day: String,
        generation: u64,
        deadline: Option<RuntimeDeadlineCandidate>,
    ) -> Result<StartupRawActivation> {
        let _fence = Arc::clone(&self.enqueue_fence).write_owned().await;
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        self.activate_startup_after_manifest_locked(&utc_day, generation, deadline)
    }

    pub(super) async fn activate_or_stop_startup_after_manifest(
        &self,
        utc_day: String,
        generation: u64,
        deadline: Option<RuntimeDeadlineCandidate>,
        releases: StartupReleaseHandles,
    ) -> Result<StartupActivationOutcome> {
        let _fence = Arc::clone(&self.enqueue_fence).write_owned().await;
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        match self.activate_startup_after_manifest_locked(&utc_day, generation, deadline)? {
            StartupRawActivation::Active(_) => {
                releases.release()?;
                Ok(StartupActivationOutcome::Released)
            }
            StartupRawActivation::Stopped(exit) => {
                releases.stop_for_exit(&exit)?;
                Ok(StartupActivationOutcome::CommittedStopped(exit))
            }
        }
    }

    fn activate_startup_after_manifest_locked(
        &self,
        utc_day: &str,
        generation: u64,
        deadline: Option<RuntimeDeadlineCandidate>,
    ) -> Result<StartupRawActivation> {
        let _ = self.materialize_deadline_locked(deadline)?;
        let pre_activation_exit = self
            .pre_activation_exit
            .lock()
            .expect("pre-activation exit mutex poisoned")
            .take();
        let expected = RawDayPermit {
            utc_day: utc_day.to_owned(),
            generation,
        };
        if self
            .startup_activation_committed
            .lock()
            .expect("startup activation-commit mutex poisoned")
            .as_ref()
            != Some(&expected)
        {
            return Err(invalid_data(
                "raw-day startup activation requires the matching manifest commit",
            )
            .into());
        }
        let mut state = self.state.lock().expect("raw-day gate mutex poisoned");
        if !matches!(*state, RawDayGateState::Inactive) {
            return Err(
                invalid_data("raw-day startup activation requires an inactive gate").into(),
            );
        }
        let encoded = Self::encoded_open_generation(generation)?;
        self.state_word
            .compare_exchange(0, encoded, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid_data("raw-day startup activation CAS failed"))?;
        let permit = expected;
        *state = RawDayGateState::Active(permit.clone());
        *self
            .startup_activation_committed
            .lock()
            .expect("startup activation-commit mutex poisoned") = None;
        drop(state);
        self.notify_change();
        if let Some(exit) = pre_activation_exit {
            return self
                .prepare_runtime_exit_with_cause_locked_inner(exit.observed_utc_ms, exit.cause)
                .map(StartupRawActivation::Stopped);
        }
        Ok(StartupRawActivation::Active(permit))
    }

    fn prepare_same_day_close_with_cause(
        &self,
        ended_utc_ms: i64,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedSameDayClose> {
        let ended_day = utc_day_string_from_ms(ended_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for same-day close"))?;
        let mut state = self.state.lock().expect("raw-day gate mutex poisoned");
        let RawDayGateState::Active(active) = &*state else {
            return match &*state {
                RawDayGateState::ClosingSameDay(existing) => Ok(existing.clone()),
                other => Err(invalid_data(format!(
                    "same-day close requires an active epoch: state={other:?} ended_utc_ms={ended_utc_ms}"
                ))
                .into()),
            };
        };
        if active.utc_day != ended_day {
            return Err(invalid_data(format!(
                "same-day close crossed UTC day: active={} ended_day={ended_day} ended_utc_ms={ended_utc_ms}",
                active.utc_day
            ))
            .into());
        }
        if self
            .prepared_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned")
            .is_some()
        {
            return Err(invalid_data("same-day close conflicts with prepared rollover").into());
        }
        let mut prepared = self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned");
        if let Some(existing) = prepared.as_ref() {
            return Ok(existing.clone());
        }
        let close = PreparedSameDayClose {
            identity: RuntimeCloseIdentity {
                utc_day: active.utc_day.clone(),
                generation: active.generation,
                close_id: self.next_close_id()?,
            },
            ended_utc_ms,
            cause,
            unclean: false,
            unclean_cause: None,
        };
        let open = Self::encoded_open_generation(active.generation)?;
        let closed = Self::encoded_closed_generation(active.generation)?;
        self.state_word
            .compare_exchange(open, closed, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid_data("same-day close preparation CAS failed"))?;
        *state = RawDayGateState::ClosingSameDay(close.clone());
        *prepared = Some(close.clone());
        drop(prepared);
        drop(state);
        self.notify_change();
        Ok(close)
    }

    pub(super) fn prepare_runtime_exit(&self, observed_utc_ms: i64) -> Result<PreparedRuntimeExit> {
        self.prepare_runtime_exit_with_cause(
            observed_utc_ms,
            RuntimeCloseCause::CoordinatorFailure {
                stage: "runtime_exit",
            },
        )
    }

    pub(super) fn prepare_runtime_exit_with_cause(
        &self,
        observed_utc_ms: i64,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedRuntimeExit> {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        self.prepare_runtime_exit_with_cause_locked(observed_utc_ms, cause)
    }

    fn prepare_runtime_exit_with_cause_locked(
        &self,
        observed_utc_ms: i64,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedRuntimeExit> {
        if !matches!(cause, RuntimeCloseCause::RunDeadline)
            && let Some(deadline) = *self
                .runtime_deadline
                .lock()
                .expect("raw-day runtime-deadline mutex poisoned")
            && deadline.is_reached()
        {
            return self.prepare_runtime_exit_with_cause_locked_inner(
                deadline.observed_utc_ms,
                RuntimeCloseCause::RunDeadline,
            );
        }
        self.prepare_runtime_exit_with_cause_locked_inner(observed_utc_ms, cause)
    }

    fn prepare_runtime_exit_with_cause_locked_inner(
        &self,
        observed_utc_ms: i64,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedRuntimeExit> {
        if let Some(close) = self
            .prepared_same_day_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned")
            .clone()
        {
            return Ok(PreparedRuntimeExit::SameDay(close));
        }
        if let Some(exit) = self
            .pending_exit
            .lock()
            .expect("raw-day pending-exit mutex poisoned")
            .clone()
        {
            return Ok(PreparedRuntimeExit::Rollover(exit));
        }
        if let Some(close) = self
            .activation_committed
            .lock()
            .expect("raw-day activation-commit mutex poisoned")
            .clone()
        {
            let observed_day = utc_day_string_from_ms(observed_utc_ms)
                .ok_or_else(|| invalid_data("UTC day unavailable for post-activation exit"))?;
            if observed_day != close.next_utc_day || observed_utc_ms < close.boundary_utc_ms {
                return Err(invalid_data(format!(
                    "post-activation exit day/time mismatch: observed_day={observed_day} observed_utc_ms={observed_utc_ms} close={close:?}"
                ))
                .into());
            }
            let mut prepared = self
                .prepared_same_day_close
                .lock()
                .expect("same-day prepared-close mutex poisoned");
            if let Some(existing) = prepared.as_ref() {
                return Ok(PreparedRuntimeExit::SameDay(existing.clone()));
            }
            let candidate = PreparedSameDayClose {
                identity: RuntimeCloseIdentity {
                    utc_day: close.next_utc_day,
                    generation: close.next_generation,
                    close_id: self.next_close_id()?,
                },
                ended_utc_ms: observed_utc_ms,
                cause,
                unclean: false,
                unclean_cause: None,
            };
            *prepared = Some(candidate.clone());
            drop(prepared);
            self.notify_change();
            return Ok(PreparedRuntimeExit::SameDay(candidate));
        }

        let observed_day = utc_day_string_from_ms(observed_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for runtime exit"))?;
        match self.current_state() {
            RawDayGateState::Active(active) if observed_day == active.utc_day => self
                .prepare_same_day_close_with_cause(observed_utc_ms, cause)
                .map(PreparedRuntimeExit::SameDay),
            RawDayGateState::Active(active) if observed_day > active.utc_day => {
                let boundary_utc_ms = observed_utc_ms
                    .div_euclid(UTC_DAY_MS)
                    .saturating_mul(UTC_DAY_MS);
                let close = self.prepare_rollover(&active.utc_day, boundary_utc_ms)?;
                let exit = PendingExit {
                    close,
                    observed_utc_ms,
                    cause,
                    unclean: false,
                    unclean_cause: None,
                };
                let mut pending = self
                    .pending_exit
                    .lock()
                    .expect("raw-day pending-exit mutex poisoned");
                if let Some(existing) = pending.as_ref() {
                    return Ok(PreparedRuntimeExit::Rollover(existing.clone()));
                }
                *pending = Some(exit.clone());
                drop(pending);
                self.notify_change();
                Ok(PreparedRuntimeExit::Rollover(exit))
            }
            RawDayGateState::Active(active) => Err(invalid_data(format!(
                "runtime exit time regressed before active UTC day: active={} observed_day={observed_day} observed_utc_ms={observed_utc_ms}",
                active.utc_day
            ))
            .into()),
            RawDayGateState::Closing(close) => {
                if observed_utc_ms < close.boundary_utc_ms {
                    return Err(invalid_data(format!(
                        "runtime exit observed before an already completed boundary fence: observed_utc_ms={observed_utc_ms} close={close:?}"
                    ))
                    .into());
                }
                let exit = PendingExit {
                    close,
                    observed_utc_ms,
                    cause,
                    unclean: false,
                    unclean_cause: None,
                };
                let mut pending = self
                    .pending_exit
                    .lock()
                    .expect("raw-day pending-exit mutex poisoned");
                if let Some(existing) = pending.as_ref() {
                    return Ok(PreparedRuntimeExit::Rollover(existing.clone()));
                }
                *pending = Some(exit.clone());
                drop(pending);
                self.notify_change();
                Ok(PreparedRuntimeExit::Rollover(exit))
            }
            RawDayGateState::ClosingSameDay(close) => {
                Ok(PreparedRuntimeExit::SameDay(close))
            }
            RawDayGateState::Inactive => {
                let mut pending = self
                    .pre_activation_exit
                    .lock()
                    .expect("pre-activation exit mutex poisoned");
                if pending.is_none() {
                    *pending = Some(RuntimeExitRequest {
                        observed_utc_ms,
                        cause,
                    });
                    drop(pending);
                    self.notify_change();
                }
                Err(invalid_data("runtime exit reached an inactive raw-day gate").into())
            }
        }
    }

    pub(super) fn runtime_exit_identity(prepared: &PreparedRuntimeExit) -> RuntimeCloseIdentity {
        match prepared {
            PreparedRuntimeExit::SameDay(close) => close.identity.clone(),
            PreparedRuntimeExit::Rollover(exit) => {
                RuntimeCloseIdentity::from_rollover_old_day(&exit.close)
            }
        }
    }

    fn runtime_exit_epoch(prepared: &PreparedRuntimeExit) -> RuntimeEpoch {
        let identity = Self::runtime_exit_identity(prepared);
        RuntimeEpoch {
            utc_day: identity.utc_day,
            generation: identity.generation,
        }
    }

    pub(super) fn observe_source_terminal_now(
        &self,
        cause: RuntimeCloseCause,
        unclean_cause: Option<RuntimeCloseCause>,
        post_fence_epoch: Option<&RuntimeEpoch>,
    ) -> SourceWrapperLifecycleObservation {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        let observed_utc_ms = now_ms();
        let prepared = match self.prepare_runtime_exit_with_cause_locked(observed_utc_ms, cause) {
            Ok(prepared) => prepared,
            Err(error) => {
                return SourceWrapperLifecycleObservation {
                    observed_utc_ms,
                    epoch: None,
                    lifecycle_record_error: Some(error.to_string()),
                    replace_existing: unclean_cause.is_some(),
                    termination_kind: SourceTaskTerminationKind::Unclean,
                };
            }
        };
        let epoch = Some(Self::runtime_exit_epoch(&prepared));
        let Some(unclean_cause) = unclean_cause else {
            return SourceWrapperLifecycleObservation {
                observed_utc_ms,
                epoch,
                lifecycle_record_error: None,
                replace_existing: false,
                termination_kind: SourceTaskTerminationKind::ReliableReturn,
            };
        };
        let lifecycle_record_error =
            if self.unclean_affects_prepared_epoch(&prepared, &unclean_cause, post_fence_epoch) {
                self.mark_runtime_exit_unclean_with_cause(
                    &Self::runtime_exit_identity(&prepared),
                    unclean_cause,
                )
                .err()
                .map(|error| error.to_string())
            } else {
                None
            };
        SourceWrapperLifecycleObservation {
            observed_utc_ms,
            epoch,
            lifecycle_record_error,
            replace_existing: true,
            termination_kind: SourceTaskTerminationKind::Unclean,
        }
    }

    pub(super) fn observe_source_wrapper_terminal_now<F>(
        &self,
        source_kind: &'static str,
        classify: F,
    ) -> SourceWrapperLifecycleObservation
    where
        F: FnOnce() -> SourceWrapperTerminalClass,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        let observed_utc_ms = now_ms();
        let class = classify();
        if matches!(&class, SourceWrapperTerminalClass::ExistingInnerReturn) {
            return SourceWrapperLifecycleObservation {
                observed_utc_ms,
                epoch: None,
                lifecycle_record_error: None,
                replace_existing: false,
                termination_kind: SourceTaskTerminationKind::ReliableReturn,
            };
        }

        let (candidate_cause, unclean_cause, replace_existing) = match class {
            SourceWrapperTerminalClass::ExistingInnerReturn => unreachable!(),
            SourceWrapperTerminalClass::CleanReturn => (
                RuntimeCloseCause::SourceWrapperReturn { source_kind },
                None,
                false,
            ),
            SourceWrapperTerminalClass::Unclean(cause) => (cause.clone(), Some(cause), true),
            SourceWrapperTerminalClass::CoordinatedStop(expected) => {
                if let Some(prepared) = self.prepared_runtime_exit()
                    && Self::runtime_exit_identity(&prepared) == expected
                {
                    return SourceWrapperLifecycleObservation {
                        observed_utc_ms,
                        epoch: Some(Self::runtime_exit_epoch(&prepared)),
                        lifecycle_record_error: None,
                        replace_existing: false,
                        termination_kind: SourceTaskTerminationKind::CoordinatedAbort,
                    };
                }
                let cause = RuntimeCloseCause::SourceWrapperFailure { source_kind };
                (cause.clone(), Some(cause), true)
            }
        };
        let prepared =
            match self.prepare_runtime_exit_with_cause_locked(observed_utc_ms, candidate_cause) {
                Ok(prepared) => prepared,
                Err(error) => {
                    return SourceWrapperLifecycleObservation {
                        observed_utc_ms,
                        epoch: None,
                        lifecycle_record_error: Some(error.to_string()),
                        replace_existing,
                        termination_kind: SourceTaskTerminationKind::Unclean,
                    };
                }
            };
        let epoch = Some(Self::runtime_exit_epoch(&prepared));
        let lifecycle_record_error = unclean_cause.and_then(|cause| {
            if self.unclean_affects_prepared_epoch(&prepared, &cause, None) {
                self.mark_runtime_exit_unclean_with_cause(
                    &Self::runtime_exit_identity(&prepared),
                    cause,
                )
                .err()
                .map(|error| error.to_string())
            } else {
                None
            }
        });
        SourceWrapperLifecycleObservation {
            observed_utc_ms,
            epoch,
            lifecycle_record_error,
            replace_existing,
            termination_kind: if replace_existing {
                SourceTaskTerminationKind::Unclean
            } else {
                SourceTaskTerminationKind::ReliableReturn
            },
        }
    }

    fn unclean_affects_prepared_epoch(
        &self,
        prepared: &PreparedRuntimeExit,
        cause: &RuntimeCloseCause,
        post_fence_epoch: Option<&RuntimeEpoch>,
    ) -> bool {
        let PreparedRuntimeExit::Rollover(exit) = prepared else {
            return true;
        };
        if post_fence_epoch.is_some_and(|epoch| {
            epoch.utc_day == exit.close.next_utc_day
                && epoch.generation == exit.close.next_generation
        }) {
            return false;
        }
        let source_kind = match cause {
            RuntimeCloseCause::SourceInnerReturn { source_kind }
            | RuntimeCloseCause::SourceWrapperReturn { source_kind }
            | RuntimeCloseCause::SourceWrapperFailure { source_kind }
            | RuntimeCloseCause::SourcePanic { source_kind }
            | RuntimeCloseCause::SourceAbortWithoutPermit { source_kind } => Some(*source_kind),
            _ => None,
        };
        let Some(producer) =
            source_kind.and_then(|source_kind| logical_producer_for_source_task(source_kind).ok())
        else {
            return true;
        };
        !self.source_fence_registered(producer, &exit.close)
    }

    pub(super) fn register_source_abort_permit_now(
        &self,
        close: &RuntimeCloseIdentity,
    ) -> Result<(i64, RuntimeEpoch)> {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        let observed_utc_ms = now_ms();
        let authoritative_close = self
            .prepared_runtime_exit()
            .map(|prepared| Self::runtime_exit_identity(&prepared));
        if authoritative_close.as_ref() != Some(close) {
            return Err(invalid_data(format!(
                "source abort permit does not match the authoritative gate close: requested={close:?} authoritative={authoritative_close:?}"
            ))
            .into());
        }
        Ok((
            observed_utc_ms,
            RuntimeEpoch {
                utc_day: close.utc_day.clone(),
                generation: close.generation,
            },
        ))
    }

    pub(super) fn prepared_runtime_exit(&self) -> Option<PreparedRuntimeExit> {
        self.prepared_same_day_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned")
            .clone()
            .map(PreparedRuntimeExit::SameDay)
            .or_else(|| {
                self.pending_exit
                    .lock()
                    .expect("raw-day pending-exit mutex poisoned")
                    .clone()
                    .map(PreparedRuntimeExit::Rollover)
            })
    }

    pub(super) fn unadmitted_runtime_cutoff_is_active(&self) -> bool {
        if let Some(exit) = self.prepared_runtime_exit() {
            return match exit {
                PreparedRuntimeExit::SameDay(close) => matches!(
                    close.cause,
                    RuntimeCloseCause::OperatorStop | RuntimeCloseCause::RunDeadline
                ),
                PreparedRuntimeExit::Rollover(exit) => matches!(
                    exit.cause,
                    RuntimeCloseCause::OperatorStop | RuntimeCloseCause::RunDeadline
                ),
            };
        }
        self.runtime_deadline
            .lock()
            .expect("raw-day runtime-deadline mutex poisoned")
            .is_some_and(RuntimeDeadlineCandidate::is_reached)
    }

    pub(super) fn mark_runtime_exit_unclean(
        &self,
        identity: &RuntimeCloseIdentity,
    ) -> Result<PreparedRuntimeExit> {
        self.mark_runtime_exit_unclean_with_cause(
            identity,
            RuntimeCloseCause::CoordinatorFailure {
                stage: "runtime_unclean",
            },
        )
    }

    pub(super) fn mark_runtime_exit_unclean_with_cause(
        &self,
        identity: &RuntimeCloseIdentity,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedRuntimeExit> {
        if self
            .prepared_same_day_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned")
            .is_some()
            || matches!(self.current_state(), RawDayGateState::ClosingSameDay(_))
        {
            return self
                .mark_same_day_close_unclean_with_cause(identity, cause)
                .map(PreparedRuntimeExit::SameDay);
        }
        let mut pending = self
            .pending_exit
            .lock()
            .expect("raw-day pending-exit mutex poisoned");
        let exit = pending
            .as_mut()
            .ok_or_else(|| invalid_data("runtime unclean mark found no prepared exit"))?;
        if exit.close.utc_day != identity.utc_day
            || exit.close.generation != identity.generation
            || exit.close.close_id != identity.close_id
        {
            return Err(invalid_data(format!(
                "pending-exit unclean identity mismatch: exit={exit:?} requested={identity:?}"
            ))
            .into());
        }
        exit.unclean = true;
        if exit.unclean_cause.is_none() {
            exit.unclean_cause = Some(cause);
        }
        Ok(PreparedRuntimeExit::Rollover(exit.clone()))
    }

    fn mark_same_day_close_unclean_with_cause(
        &self,
        identity: &RuntimeCloseIdentity,
        cause: RuntimeCloseCause,
    ) -> Result<PreparedSameDayClose> {
        let mut state = self.state.lock().expect("raw-day gate mutex poisoned");
        let mut prepared = self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned");
        let Some(prepared_close) = prepared.as_mut() else {
            return Err(invalid_data(format!(
                "same-day unclean close has no prepared identity: state={state:?} identity={identity:?}"
            ))
            .into());
        };
        if prepared_close.identity != *identity {
            return Err(invalid_data(format!(
                "same-day unclean identity mismatch: prepared={prepared_close:?} state={state:?} actual={identity:?}"
            ))
            .into());
        }
        prepared_close.unclean = true;
        if prepared_close.unclean_cause.is_none() {
            prepared_close.unclean_cause = Some(cause.clone());
        }
        match &mut *state {
            RawDayGateState::ClosingSameDay(state_close) => {
                if state_close.identity != *identity {
                    return Err(invalid_data(format!(
                        "same-day unclean state identity mismatch: state={state_close:?} actual={identity:?}"
                    ))
                    .into());
                }
                state_close.unclean = true;
                if state_close.unclean_cause.is_none() {
                    state_close.unclean_cause = Some(cause);
                }
            }
            RawDayGateState::Closing(close)
                if self
                    .activation_committed
                    .lock()
                    .expect("raw-day activation-commit mutex poisoned")
                    .as_ref()
                    == Some(close) => {}
            _ => {
                return Err(invalid_data(format!(
                    "same-day unclean close found incompatible state: state={state:?} identity={identity:?}"
                ))
                .into());
            }
        }
        Ok(prepared_close.clone())
    }

    pub(super) async fn seal_same_day_close(
        &self,
        identity: &RuntimeCloseIdentity,
    ) -> Result<PreparedSameDayClose> {
        let _fence = Arc::clone(&self.enqueue_fence).write_owned().await;
        let state = self.state.lock().expect("raw-day gate mutex poisoned");
        if let RawDayGateState::ClosingSameDay(existing) = &*state {
            if existing.identity == *identity {
                return Ok(existing.clone());
            }
            return Err(invalid_data(format!(
                "conflicting same-day close: existing={existing:?} requested={identity:?}"
            ))
            .into());
        }
        if let RawDayGateState::Closing(close) = &*state
            && self
                .activation_committed
                .lock()
                .expect("raw-day activation-commit mutex poisoned")
                .as_ref()
                == Some(close)
            && let Some(existing) = self
                .prepared_same_day_close
                .lock()
                .expect("same-day prepared-close mutex poisoned")
                .as_ref()
            && existing.identity == *identity
        {
            return Ok(existing.clone());
        }
        Err(invalid_data(format!(
            "same-day close seal requires a prepared closing gate: state={state:?}"
        ))
        .into())
    }

    pub(super) fn prepare_rollover(
        &self,
        utc_day: &str,
        boundary_utc_ms: i64,
    ) -> Result<RawDayClose> {
        if let Some(existing) = self
            .prepared_same_day_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned")
            .as_ref()
        {
            return Err(invalid_data(format!(
                "raw-day rollover conflicts with prepared same-day close: existing={existing:?} requested_day={utc_day}"
            ))
            .into());
        }
        let expected_old_day = utc_day_string_from_ms(boundary_utc_ms.saturating_sub(1))
            .ok_or_else(|| invalid_data("UTC day unavailable before raw-day boundary"))?;
        let next_utc_day = utc_day_string_from_ms(boundary_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable at raw-day boundary"))?;
        if expected_old_day != utc_day || next_utc_day == utc_day {
            return Err(invalid_data(format!(
                "invalid raw-day rollover boundary: old={utc_day} boundary={boundary_utc_ms} derived_old={expected_old_day} derived_new={next_utc_day}"
            ))
            .into());
        }

        let state = self.state.lock().expect("raw-day gate mutex poisoned");
        let mut prepared = self
            .prepared_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned");
        let close = match &*state {
            RawDayGateState::Active(permit) => {
                if permit.utc_day != utc_day {
                    return Err(invalid_data(format!(
                        "raw-day rollover active-day mismatch: active={} requested={utc_day}",
                        permit.utc_day
                    ))
                    .into());
                }
                match prepared.as_ref() {
                    Some(existing)
                        if existing.utc_day == utc_day
                            && existing.boundary_utc_ms == boundary_utc_ms =>
                    {
                        existing.clone()
                    }
                    Some(existing) => {
                        return Err(invalid_data(format!(
                            "conflicting prepared raw-day rollover: existing={existing:?} requested_day={utc_day} requested_boundary={boundary_utc_ms}"
                        ))
                        .into());
                    }
                    None => RawDayClose {
                        utc_day: utc_day.to_owned(),
                        next_utc_day,
                        generation: permit.generation,
                        next_generation: permit
                            .generation
                            .checked_add(1)
                            .ok_or_else(|| invalid_data("raw-day generation overflow"))?,
                        close_id: self.next_close_id()?,
                        boundary_utc_ms,
                    },
                }
            }
            RawDayGateState::Closing(existing)
                if existing.utc_day == utc_day && existing.boundary_utc_ms == boundary_utc_ms =>
            {
                return Ok(existing.clone());
            }
            RawDayGateState::Closing(existing) => {
                return Err(invalid_data(format!(
                    "conflicting raw-day rollover close: existing={existing:?} requested_day={utc_day} requested_boundary={boundary_utc_ms}"
                ))
                .into());
            }
            RawDayGateState::ClosingSameDay(existing) => {
                return Err(invalid_data(format!(
                    "raw-day rollover conflicts with same-day close: existing={existing:?} requested_day={utc_day}"
                ))
                .into());
            }
            RawDayGateState::Inactive => {
                return Err(invalid_data("raw-day rollover attempted before activation").into());
            }
        };
        if prepared.is_none() {
            *prepared = Some(close.clone());
        }
        drop(prepared);
        drop(state);
        self.notify_change();
        Ok(close)
    }

    pub(super) fn register_source_fence(&self, producer: &str, close: &RawDayClose) -> Result<()> {
        if !LOGICAL_PRODUCERS.contains(&producer) {
            return Err(
                invalid_data(format!("unknown logical producer source fence: {producer}")).into(),
            );
        }
        let state = self.state.lock().expect("raw-day gate mutex poisoned");
        match &*state {
            RawDayGateState::Active(active)
                if active.utc_day == close.utc_day && active.generation == close.generation => {}
            RawDayGateState::Closing(existing) if existing == close => {
                return Err(invalid_data(format!(
                    "source fence arrived after raw-day intake closed: producer={producer} close={close:?}"
                ))
                .into());
            }
            other => {
                return Err(invalid_data(format!(
                    "source fence requires matching active epoch: producer={producer} state={other:?} close={close:?}"
                ))
                .into());
            }
        }
        let prepared = self
            .prepared_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned");
        if prepared.as_ref() != Some(close) {
            return Err(invalid_data(format!(
                "source fence prepared-close mismatch: producer={producer} prepared={prepared:?} close={close:?}"
            ))
            .into());
        }
        let mut fences = self
            .source_fences
            .lock()
            .expect("raw-day source-fence mutex poisoned");
        let identity = (close.generation, close.close_id);
        match fences.get(producer) {
            Some(existing) if *existing == identity => {
                return Ok(());
            }
            Some(existing) => {
                return Err(invalid_data(format!(
                    "conflicting source fence: producer={producer} existing={existing:?} requested={identity:?}"
                ))
                .into());
            }
            None => {
                fences.insert(producer.to_owned(), identity);
            }
        }
        drop(fences);
        drop(prepared);
        drop(state);
        self.notify_change();
        Ok(())
    }

    pub(super) fn all_source_fences_registered(&self, close: &RawDayClose) -> bool {
        let identity = (close.generation, close.close_id);
        let fences = self
            .source_fences
            .lock()
            .expect("raw-day source-fence mutex poisoned");
        fences.len() == LOGICAL_PRODUCERS.len()
            && LOGICAL_PRODUCERS
                .iter()
                .all(|producer| fences.get(*producer) == Some(&identity))
    }

    pub(super) fn source_fence_registered(&self, producer: &str, close: &RawDayClose) -> bool {
        self.source_fences
            .lock()
            .expect("raw-day source-fence mutex poisoned")
            .get(producer)
            == Some(&(close.generation, close.close_id))
    }

    pub(super) fn publish_writer_rendezvous(&self, close: &RawDayClose) -> Result<()> {
        let state = self.state.lock().expect("raw-day gate mutex poisoned");
        match &*state {
            RawDayGateState::Active(active)
                if active.utc_day == close.utc_day && active.generation == close.generation => {}
            RawDayGateState::Closing(existing) if existing == close => {}
            other => {
                return Err(invalid_data(format!(
                    "writer rendezvous requires the exact old-day close: state={other:?} close={close:?}"
                ))
                .into());
            }
        }
        let mut witness = self
            .writer_rendezvous
            .lock()
            .expect("raw-day writer-rendezvous mutex poisoned");
        match witness.as_ref() {
            Some(existing) if existing == close => return Ok(()),
            Some(existing) => {
                return Err(invalid_data(format!(
                    "conflicting writer rendezvous: existing={existing:?} requested={close:?}"
                ))
                .into());
            }
            None => *witness = Some(close.clone()),
        }
        drop(witness);
        drop(state);
        self.notify_change();
        Ok(())
    }

    pub(super) fn writer_rendezvous_epoch(&self) -> Option<RuntimeEpoch> {
        let close = self
            .writer_rendezvous
            .lock()
            .expect("raw-day writer-rendezvous mutex poisoned")
            .clone()?;
        let matches = match &*self.state.lock().expect("raw-day gate mutex poisoned") {
            RawDayGateState::Active(active) => {
                active.utc_day == close.utc_day && active.generation == close.generation
            }
            RawDayGateState::Closing(existing) => existing == &close,
            RawDayGateState::ClosingSameDay(_) | RawDayGateState::Inactive => false,
        };
        matches.then_some(RuntimeEpoch {
            utc_day: close.next_utc_day,
            generation: close.next_generation,
        })
    }

    pub(super) fn clear_writer_rendezvous(&self, close: &RawDayClose) -> Result<()> {
        let mut witness = self
            .writer_rendezvous
            .lock()
            .expect("raw-day writer-rendezvous mutex poisoned");
        match witness.as_ref() {
            Some(existing) if existing == close => *witness = None,
            Some(existing) => {
                return Err(invalid_data(format!(
                    "writer rendezvous clear identity mismatch: existing={existing:?} requested={close:?}"
                ))
                .into());
            }
            None => {}
        }
        Ok(())
    }

    fn all_other_source_fences_registered(
        &self,
        excluded_producer: &str,
        close: &RawDayClose,
    ) -> bool {
        let identity = (close.generation, close.close_id);
        let fences = self
            .source_fences
            .lock()
            .expect("raw-day source-fence mutex poisoned");
        LOGICAL_PRODUCERS
            .iter()
            .filter(|producer| **producer != excluded_producer)
            .all(|producer| fences.get(*producer) == Some(&identity))
    }

    pub(super) async fn wait_for_other_source_fences(
        &self,
        excluded_producer: &str,
        close: &RawDayClose,
    ) -> Result<()> {
        if !LOGICAL_PRODUCERS.contains(&excluded_producer) {
            return Err(invalid_data(format!(
                "unknown logical producer excluded from source-fence wait: {excluded_producer}"
            ))
            .into());
        }
        let mut changes = self.state_changes.subscribe();
        loop {
            match self.current_state() {
                RawDayGateState::Active(active)
                    if active.utc_day == close.utc_day && active.generation == close.generation =>
                {
                    if self.all_other_source_fences_registered(excluded_producer, close) {
                        return Ok(());
                    }
                }
                other => {
                    return Err(invalid_data(format!(
                        "raw-day partial source-fence wait found conflicting state: excluded={excluded_producer} state={other:?} close={close:?}"
                    ))
                    .into());
                }
            }
            changes.changed().await.map_err(|_| {
                invalid_data(
                    "raw-day gate change channel closed while partial source fences waited",
                )
            })?;
        }
    }

    async fn wait_for_all_source_fences(&self, close: &RawDayClose) -> Result<()> {
        let mut changes = self.state_changes.subscribe();
        loop {
            match self.current_state() {
                RawDayGateState::Active(active)
                    if active.utc_day == close.utc_day && active.generation == close.generation =>
                {
                    let prepared = self
                        .prepared_close
                        .lock()
                        .expect("raw-day prepared-close mutex poisoned")
                        .clone();
                    if prepared.as_ref() != Some(close) {
                        return Err(invalid_data(format!(
                            "raw-day source-fence wait lost prepared close: prepared={prepared:?} close={close:?}"
                        ))
                        .into());
                    }
                    if self.all_source_fences_registered(close) {
                        return Ok(());
                    }
                }
                RawDayGateState::Closing(existing) if existing == *close => return Ok(()),
                other => {
                    return Err(invalid_data(format!(
                        "raw-day source-fence wait found conflicting state: state={other:?} close={close:?}"
                    ))
                    .into());
                }
            }
            changes.changed().await.map_err(|_| {
                invalid_data("raw-day gate change channel closed while source fences waited")
            })?;
        }
    }

    pub(super) async fn seal_prepared_rollover(&self, close: &RawDayClose) -> Result<RawDayClose> {
        self.wait_for_all_source_fences(close).await?;
        let _fence = Arc::clone(&self.enqueue_fence).write_owned().await;
        let mut state = self.state.lock().expect("raw-day gate mutex poisoned");
        if let RawDayGateState::Closing(existing) = &*state {
            if existing == close {
                return Ok(existing.clone());
            }
            return Err(invalid_data(format!(
                "conflicting raw-day rollover close: existing={existing:?} requested={close:?}"
            ))
            .into());
        }
        let RawDayGateState::Active(active) = &*state else {
            return Err(invalid_data("raw-day rollover seal requires an active gate").into());
        };
        if active.utc_day != close.utc_day || active.generation != close.generation {
            return Err(invalid_data(format!(
                "raw-day rollover seal active epoch mismatch: active={active:?} close={close:?}"
            ))
            .into());
        }
        let mut prepared = self
            .prepared_close
            .lock()
            .expect("raw-day prepared-close mutex poisoned");
        if prepared.as_ref() != Some(close) || !self.all_source_fences_registered(close) {
            return Err(invalid_data(format!(
                "raw-day rollover seal missing prepared close or source fences: prepared={prepared:?} close={close:?}"
            ))
            .into());
        }
        let open = Self::encoded_open_generation(close.generation)?;
        let closed = Self::encoded_closed_generation(close.generation)?;
        self.state_word
            .compare_exchange(open, closed, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid_data("raw-day rollover close CAS failed"))?;
        *state = RawDayGateState::Closing(close.clone());
        *prepared = None;
        drop(prepared);
        drop(state);
        self.notify_change();
        Ok(close.clone())
    }

    #[cfg(test)]
    pub(super) async fn close_for_rollover(
        &self,
        utc_day: &str,
        boundary_utc_ms: i64,
    ) -> Result<RawDayClose> {
        let close = self.prepare_rollover(utc_day, boundary_utc_ms)?;
        for producer in LOGICAL_PRODUCERS {
            self.register_source_fence(producer, &close)?;
        }
        self.seal_prepared_rollover(&close).await
    }

    pub(super) async fn activate_after_rollover(
        &self,
        close: &RawDayClose,
    ) -> Result<RawDayPermit> {
        let _fence = Arc::clone(&self.enqueue_fence).write_owned().await;
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self
            .activation_committed
            .lock()
            .expect("raw-day activation-commit mutex poisoned")
            .as_ref()
            != Some(close)
        {
            return Err(invalid_data(format!(
                "raw-day activation requires the matching committed manifest activation: close={close:?}"
            ))
            .into());
        }
        let mut state = self.state.lock().expect("raw-day gate mutex poisoned");
        if self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned")
            .is_some()
        {
            return Err(invalid_data(
                "raw-day activation rejected because the committed new day is already closing",
            )
            .into());
        }
        match &*state {
            RawDayGateState::Closing(existing) if existing == close => {}
            RawDayGateState::Closing(existing) => {
                return Err(invalid_data(format!(
                    "raw-day rollover activation identity mismatch: expected={existing:?} actual={close:?}"
                ))
                .into());
            }
            RawDayGateState::Active(active)
                if active.utc_day == close.next_utc_day
                    && active.generation == close.next_generation =>
            {
                return Ok(active.clone());
            }
            RawDayGateState::Active(active) => {
                return Err(invalid_data(format!(
                    "raw-day rollover activation encountered unexpected active epoch: {active:?}"
                ))
                .into());
            }
            RawDayGateState::ClosingSameDay(existing) => {
                return Err(invalid_data(format!(
                    "raw-day rollover activation conflicts with same-day close: {existing:?}"
                ))
                .into());
            }
            RawDayGateState::Inactive => {
                return Err(invalid_data("raw-day rollover activation found inactive gate").into());
            }
        }
        let closed = Self::encoded_closed_generation(close.generation)?;
        let open = Self::encoded_open_generation(close.next_generation)?;
        self.state_word
            .compare_exchange(closed, open, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| invalid_data("raw-day rollover activation CAS failed"))?;
        let permit = RawDayPermit {
            utc_day: close.next_utc_day.clone(),
            generation: close.next_generation,
        };
        *state = RawDayGateState::Active(permit.clone());
        self.source_fences
            .lock()
            .expect("raw-day source-fence mutex poisoned")
            .clear();
        self.clear_writer_rendezvous(close)?;
        *self
            .activation_committed
            .lock()
            .expect("raw-day activation-commit mutex poisoned") = None;
        drop(state);
        self.notify_change();
        Ok(permit)
    }

    pub(super) fn commit_release_if_no_exit<F>(
        &self,
        close: &RawDayClose,
        deadline: Option<RuntimeDeadlineCandidate>,
        release: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self.materialize_deadline_locked(deadline)? {
            return Err(invalid_data(format!(
                "producer release rejected because the run deadline was reached: close={close:?}"
            ))
            .into());
        }
        if let Some(prepared) = self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned")
            .as_ref()
        {
            return Err(invalid_data(format!(
                "producer release rejected by an existing same-day close: prepared={prepared:?} rollover={close:?}"
            ))
            .into());
        }
        if let Some(exit) = self
            .pending_exit
            .lock()
            .expect("raw-day pending-exit mutex poisoned")
            .as_ref()
        {
            return Err(invalid_data(format!(
                "producer release rejected by an existing pending exit: exit={exit:?} rollover={close:?}"
            ))
            .into());
        }
        match self.current_state() {
            RawDayGateState::Active(active)
                if active.utc_day == close.next_utc_day
                    && active.generation == close.next_generation => {}
            state => {
                return Err(invalid_data(format!(
                    "producer release requires the active next generation: state={state:?} close={close:?}"
                ))
                .into());
            }
        }
        release()
    }

    #[cfg(test)]
    pub(super) fn commit_startup_release_if_no_exit<F>(
        &self,
        deadline: Option<RuntimeDeadlineCandidate>,
        release: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self.materialize_deadline_locked(deadline)? {
            return Err(invalid_data(
                "startup release rejected because the run deadline was reached",
            )
            .into());
        }
        if let Some(exit) = self.prepared_runtime_exit() {
            return Err(invalid_data(format!(
                "startup release rejected by an existing runtime exit: {exit:?}"
            ))
            .into());
        }
        if !matches!(self.current_state(), RawDayGateState::Active(_)) {
            return Err(
                invalid_data("startup release requires an active raw-day generation").into(),
            );
        }
        release()
    }

    pub(super) fn current_state(&self) -> RawDayGateState {
        self.state
            .lock()
            .expect("raw-day gate mutex poisoned")
            .clone()
    }

    #[cfg(test)]
    pub(super) fn observation_epoch(&self) -> Option<RuntimeEpoch> {
        if let Some(close) = self
            .activation_committed
            .lock()
            .expect("raw-day activation-commit mutex poisoned")
            .as_ref()
        {
            return Some(RuntimeEpoch {
                utc_day: close.next_utc_day.clone(),
                generation: close.next_generation,
            });
        }
        match self.current_state() {
            RawDayGateState::Inactive => None,
            RawDayGateState::Active(permit) => Some(RuntimeEpoch {
                utc_day: permit.utc_day,
                generation: permit.generation,
            }),
            RawDayGateState::Closing(close) => Some(RuntimeEpoch {
                utc_day: close.utc_day,
                generation: close.generation,
            }),
            RawDayGateState::ClosingSameDay(close) => Some(RuntimeEpoch {
                utc_day: close.identity.utc_day,
                generation: close.identity.generation,
            }),
        }
    }

    pub(super) fn commit_manifest_activation<F>(
        &self,
        close: &RawDayClose,
        deadline: Option<RuntimeDeadlineCandidate>,
        commit: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> Result<()>,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self.materialize_deadline_locked(deadline)? {
            return Ok(false);
        }
        if let Some(exit) = self
            .pending_exit
            .lock()
            .expect("raw-day pending-exit mutex poisoned")
            .as_ref()
        {
            let _ = exit;
            return Ok(false);
        }
        if let Some(prepared_close) = self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned")
            .as_ref()
        {
            let _ = prepared_close;
            return Ok(false);
        }
        match self.current_state() {
            RawDayGateState::Closing(actual) if actual == *close => {}
            state => {
                return Err(invalid_data(format!(
                    "manifest activation commit found incompatible raw gate: state={state:?} close={close:?}"
                ))
                .into());
            }
        }
        let mut committed = self
            .activation_committed
            .lock()
            .expect("raw-day activation-commit mutex poisoned");
        if let Some(existing) = committed.as_ref() {
            return Err(invalid_data(format!(
                "duplicate or conflicting manifest activation commit: existing={existing:?} actual={close:?}"
            ))
            .into());
        }
        commit()?;
        *committed = Some(close.clone());
        drop(committed);
        self.notify_change();
        Ok(true)
    }

    pub(super) fn commit_old_day_if_no_exit<F>(
        &self,
        close: &RawDayClose,
        deadline: Option<RuntimeDeadlineCandidate>,
        commit: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> Result<()>,
    {
        let _transition = self
            .rollover_transition
            .lock()
            .expect("raw-day rollover-transition mutex poisoned");
        if self.materialize_deadline_locked(deadline)? {
            return Ok(false);
        }
        if self
            .prepared_same_day_close
            .lock()
            .expect("same-day prepared-close mutex poisoned")
            .is_some()
            || self
                .pending_exit
                .lock()
                .expect("raw-day pending-exit mutex poisoned")
                .is_some()
        {
            return Ok(false);
        }
        match self.current_state() {
            RawDayGateState::Closing(actual) if actual == *close => {}
            state => {
                return Err(invalid_data(format!(
                    "old-day normal commit found incompatible raw gate: state={state:?} close={close:?}"
                ))
                .into());
            }
        }
        commit()?;
        Ok(true)
    }

    pub(super) async fn begin_close_bound_deferred_flush(
        self: &Arc<Self>,
        producer: &str,
        close: &RawDayClose,
        recv_time_utc_ms: i64,
    ) -> Result<Option<RawDayEnqueuePermit>> {
        let utc_day = utc_day_string_from_ms(recv_time_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for deferred raw-event permit"))?;
        let fence = Arc::clone(&self.enqueue_fence).read_owned().await;
        if self.unadmitted_runtime_cutoff_is_active() {
            drop(fence);
            return Ok(None);
        }
        let state = self.current_state();
        let RawDayGateState::Closing(actual) = state else {
            return Err(invalid_data(format!(
                "close-bound deferred flush requires a closing raw-day gate: producer={producer} state={state:?} close={close:?}"
            ))
            .into());
        };
        if actual != *close || utc_day != close.next_utc_day {
            return Err(invalid_data(format!(
                "close-bound deferred flush identity mismatch: producer={producer} event_day={utc_day} actual={actual:?} expected={close:?}"
            ))
            .into());
        }
        Ok(Some(RawDayEnqueuePermit {
            permit: RawDayPermit {
                utc_day,
                generation: close.next_generation,
            },
            _fence: fence,
        }))
    }

    pub(super) async fn begin_accepted_close_bound_deferred_flush(
        self: &Arc<Self>,
        producer: &str,
        close: &RawDayClose,
        recv_time_utc_ms: i64,
        ticket: &RawDayAcceptedTicket,
    ) -> Result<RawDayEnqueuePermit> {
        if producer != "polymarket" {
            return Err(invalid_data(format!(
                "accepted raw-day ticket is scoped to polymarket: producer={producer}"
            ))
            .into());
        }
        let RawDayAcceptedTicket::Defer(epoch) = ticket else {
            return Err(invalid_data(format!(
                "close-bound accepted deferred flush requires a deferred ticket: producer={producer} ticket={ticket:?}"
            ))
            .into());
        };
        let utc_day = utc_day_string_from_ms(recv_time_utc_ms).ok_or_else(|| {
            invalid_data("UTC day unavailable for accepted deferred raw-event permit")
        })?;
        let fence = Arc::clone(&self.enqueue_fence).read_owned().await;
        let state = self.current_state();
        let RawDayGateState::Closing(actual) = state else {
            return Err(invalid_data(format!(
                "close-bound accepted deferred flush requires a closing raw-day gate: producer={producer} state={state:?} close={close:?}"
            ))
            .into());
        };
        if actual != *close
            || utc_day != close.next_utc_day
            || epoch.utc_day != close.next_utc_day
            || epoch.old_generation != close.generation
            || epoch.expected_generation != close.next_generation
        {
            return Err(invalid_data(format!(
                "close-bound accepted deferred flush identity mismatch: producer={producer} event_day={utc_day} ticket={ticket:?} actual={actual:?} expected={close:?}"
            ))
            .into());
        }
        Ok(RawDayEnqueuePermit {
            permit: RawDayPermit {
                utc_day,
                generation: close.next_generation,
            },
            _fence: fence,
        })
    }

    #[cfg(test)]
    pub(super) async fn begin_event_enqueue(
        self: &Arc<Self>,
        producer: &str,
        recv_time_utc_ms: i64,
    ) -> Result<RawDayEnqueueDecision> {
        self.begin_event_enqueue_with_cutoff(producer, recv_time_utc_ms, false)
            .await?
            .ok_or_else(|| {
                invalid_data("strict raw enqueue unexpectedly accepted a runtime cutoff").into()
            })
    }

    pub(super) async fn begin_unadmitted_event_enqueue(
        self: &Arc<Self>,
        producer: &str,
        recv_time_utc_ms: i64,
    ) -> Result<Option<RawDayEnqueueDecision>> {
        self.begin_event_enqueue_with_cutoff(producer, recv_time_utc_ms, true)
            .await
    }

    pub(super) async fn accept_unadmitted_event_ticket(
        self: &Arc<Self>,
        producer: &str,
        recv_time_utc_ms: i64,
    ) -> Result<Option<RawDayAcceptedTicket>> {
        if producer != "polymarket" {
            return Err(invalid_data(format!(
                "accepted raw-day ticket is scoped to polymarket: producer={producer}"
            ))
            .into());
        }
        Ok(self
            .begin_unadmitted_event_enqueue(producer, recv_time_utc_ms)
            .await?
            .map(RawDayAcceptedTicket::from_enqueue_decision))
    }

    pub(super) async fn accept_derived_polymarket_event_ticket(
        self: &Arc<Self>,
        parent: &RawDayAcceptedTicket,
        recv_time_utc_ms: i64,
    ) -> Result<RawDayAcceptedTicket> {
        let event_day = utc_day_string_from_ms(recv_time_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for derived Polymarket event"))?;
        let ordinary = self
            .accept_unadmitted_event_ticket("polymarket", recv_time_utc_ms)
            .await?;
        if let Some(ticket) = ordinary {
            return Ok(ticket);
        }
        let state = self.current_state();
        let parent_matches_state = match (parent, &state) {
            (RawDayAcceptedTicket::Permit(parent), RawDayGateState::Active(active)) => {
                parent == active
            }
            (RawDayAcceptedTicket::Permit(parent), RawDayGateState::Closing(close)) => {
                parent.utc_day == close.utc_day && parent.generation == close.generation
            }
            (RawDayAcceptedTicket::Permit(parent), RawDayGateState::ClosingSameDay(close)) => {
                parent.utc_day == close.identity.utc_day
                    && parent.generation == close.identity.generation
            }
            (RawDayAcceptedTicket::Defer(parent), RawDayGateState::Active(active))
                if parent.utc_day == event_day =>
            {
                (parent.utc_day == active.utc_day
                    && parent.expected_generation == active.generation)
                    || (parent.old_generation == active.generation
                        && parent.expected_generation == parent.old_generation.saturating_add(1)
                        && utc_day_string_from_ms(
                            recv_time_utc_ms
                                .div_euclid(UTC_DAY_MS)
                                .saturating_mul(UTC_DAY_MS)
                                .saturating_sub(1),
                        )
                        .as_deref()
                            == Some(active.utc_day.as_str()))
            }
            (RawDayAcceptedTicket::Defer(parent), RawDayGateState::Closing(close)) => {
                parent.utc_day == event_day
                    && parent.utc_day == close.next_utc_day
                    && parent.old_generation == close.generation
                    && parent.expected_generation == close.next_generation
            }
            (RawDayAcceptedTicket::Defer(parent), RawDayGateState::ClosingSameDay(close)) => {
                parent.utc_day == event_day
                    && parent.utc_day == close.identity.utc_day
                    && parent.expected_generation == close.identity.generation
            }
            _ => false,
        };
        if !parent_matches_state {
            return Err(invalid_data(format!(
                "accepted parent ticket cannot authorize derived Polymarket event: event_day={event_day} parent={parent:?} state={state:?}"
            ))
            .into());
        }

        match self.prepared_runtime_exit() {
            Some(PreparedRuntimeExit::SameDay(close))
                if matches!(
                    close.cause,
                    RuntimeCloseCause::OperatorStop | RuntimeCloseCause::RunDeadline
                ) && event_day == close.identity.utc_day =>
            {
                Ok(parent.clone())
            }
            Some(PreparedRuntimeExit::Rollover(exit))
                if matches!(
                    exit.cause,
                    RuntimeCloseCause::OperatorStop | RuntimeCloseCause::RunDeadline
                ) && event_day == exit.close.next_utc_day
                    && match parent {
                        RawDayAcceptedTicket::Permit(parent) => {
                            parent.utc_day == exit.close.utc_day
                                && parent.generation == exit.close.generation
                        }
                        RawDayAcceptedTicket::Defer(parent) => {
                            parent.utc_day == exit.close.next_utc_day
                                && parent.old_generation == exit.close.generation
                                && parent.expected_generation == exit.close.next_generation
                        }
                    } =>
            {
                Ok(RawDayAcceptedTicket::Defer(RawDayDeferredEpoch {
                    utc_day: exit.close.next_utc_day,
                    old_generation: exit.close.generation,
                    expected_generation: exit.close.next_generation,
                }))
            }
            exit => Err(invalid_data(format!(
                "controlled cutoff cannot authorize derived Polymarket event: event_day={event_day} parent={parent:?} state={state:?} exit={exit:?}"
            ))
            .into()),
        }
    }

    pub(super) async fn begin_accepted_ticket_enqueue(
        self: &Arc<Self>,
        producer: &str,
        recv_time_utc_ms: i64,
        ticket: &RawDayAcceptedTicket,
    ) -> Result<RawDayEnqueueDecision> {
        if producer != "polymarket" {
            return Err(invalid_data(format!(
                "accepted raw-day ticket is scoped to polymarket: producer={producer}"
            ))
            .into());
        }
        let event_day = utc_day_string_from_ms(recv_time_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for accepted raw-event ticket"))?;
        if event_day != ticket.utc_day() {
            return Err(invalid_data(format!(
                "accepted raw-event ticket UTC day mismatch: producer={producer} event={event_day} ticket={ticket:?}"
            ))
            .into());
        }
        let fence = Arc::clone(&self.enqueue_fence).read_owned().await;
        let state = self.current_state();
        match (ticket, &state) {
            (RawDayAcceptedTicket::Permit(ticket), RawDayGateState::Active(active))
                if ticket == active =>
            {
                Ok(RawDayEnqueueDecision::Permit(RawDayEnqueuePermit {
                    permit: ticket.clone(),
                    _fence: fence,
                }))
            }
            (RawDayAcceptedTicket::Permit(ticket), RawDayGateState::ClosingSameDay(close))
                if close.identity.utc_day == ticket.utc_day
                    && close.identity.generation == ticket.generation =>
            {
                Ok(RawDayEnqueueDecision::Permit(RawDayEnqueuePermit {
                    permit: ticket.clone(),
                    _fence: fence,
                }))
            }
            (RawDayAcceptedTicket::Defer(epoch), RawDayGateState::Active(active))
                if active.utc_day == epoch.utc_day
                    && active.generation == epoch.expected_generation =>
            {
                Ok(RawDayEnqueueDecision::Permit(RawDayEnqueuePermit {
                    permit: active.clone(),
                    _fence: fence,
                }))
            }
            (RawDayAcceptedTicket::Defer(epoch), RawDayGateState::ClosingSameDay(close))
                if close.identity.utc_day == epoch.utc_day
                    && close.identity.generation == epoch.expected_generation =>
            {
                Ok(RawDayEnqueueDecision::Permit(RawDayEnqueuePermit {
                    permit: RawDayPermit {
                        utc_day: close.identity.utc_day.clone(),
                        generation: close.identity.generation,
                    },
                    _fence: fence,
                }))
            }
            (RawDayAcceptedTicket::Defer(epoch), RawDayGateState::Active(active))
                if active.generation == epoch.old_generation
                    && epoch.expected_generation == epoch.old_generation.saturating_add(1)
                    && utc_day_string_from_ms(
                        recv_time_utc_ms
                            .div_euclid(UTC_DAY_MS)
                            .saturating_mul(UTC_DAY_MS)
                            .saturating_sub(1),
                    )
                    .as_deref()
                        == Some(active.utc_day.as_str()) =>
            {
                drop(fence);
                Ok(RawDayEnqueueDecision::Defer(epoch.clone()))
            }
            (RawDayAcceptedTicket::Defer(epoch), RawDayGateState::Closing(close))
                if close.utc_day != epoch.utc_day
                    && close.next_utc_day == epoch.utc_day
                    && close.generation == epoch.old_generation
                    && close.next_generation == epoch.expected_generation =>
            {
                drop(fence);
                Ok(RawDayEnqueueDecision::Defer(epoch.clone()))
            }
            _ => {
                drop(fence);
                Err(invalid_data(format!(
                    "accepted raw-event ticket no longer matches raw-day gate: producer={producer} event={event_day} ticket={ticket:?} state={state:?}"
                ))
                .into())
            }
        }
    }

    async fn begin_event_enqueue_with_cutoff(
        self: &Arc<Self>,
        producer: &str,
        recv_time_utc_ms: i64,
        allow_controlled_cutoff: bool,
    ) -> Result<Option<RawDayEnqueueDecision>> {
        let utc_day = utc_day_string_from_ms(recv_time_utc_ms)
            .ok_or_else(|| invalid_data("UTC day unavailable for raw event permit"))?;
        let fence = Arc::clone(&self.enqueue_fence).read_owned().await;
        if allow_controlled_cutoff && self.unadmitted_runtime_cutoff_is_active() {
            drop(fence);
            return Ok(None);
        }
        match self.current_state() {
            RawDayGateState::Active(permit) if permit.utc_day == utc_day => {
                Ok(Some(RawDayEnqueueDecision::Permit(RawDayEnqueuePermit {
                        permit,
                        _fence: fence,
                    })))
            }
            RawDayGateState::Active(permit) if utc_day > permit.utc_day => {
                let event_day_start = recv_time_utc_ms
                    .div_euclid(UTC_DAY_MS)
                    .saturating_mul(UTC_DAY_MS);
                let preceding_day = utc_day_string_from_ms(event_day_start.saturating_sub(1))
                    .ok_or_else(|| invalid_data("UTC day unavailable before deferred raw event"))?;
                if preceding_day != permit.utc_day {
                    return Err(invalid_data(format!(
                        "raw event skipped beyond the immediately following UTC day: producer={producer} event={utc_day} active={} generation={}",
                        permit.utc_day, permit.generation
                    ))
                    .into());
                }
                let expected_generation = permit
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("raw-day generation overflow while deferring"))?;
                drop(fence);
                Ok(Some(RawDayEnqueueDecision::Defer(RawDayDeferredEpoch {
                    utc_day,
                    old_generation: permit.generation,
                    expected_generation,
                })))
            }
            RawDayGateState::Closing(close) if utc_day == close.next_utc_day => {
                drop(fence);
                Ok(Some(RawDayEnqueueDecision::Defer(RawDayDeferredEpoch {
                    utc_day,
                    old_generation: close.generation,
                    expected_generation: close.next_generation,
                })))
            }
            RawDayGateState::Inactive => {
                Err(invalid_data("raw event reached inactive raw-day gate").into())
            }
            RawDayGateState::Active(permit) => Err(invalid_data(format!(
                "raw event UTC day regressed: producer={producer} event={utc_day} active={} generation={}",
                permit.utc_day, permit.generation
            ))
            .into()),
            RawDayGateState::Closing(close) => Err(invalid_data(format!(
                "raw event does not belong to next UTC day after intake close: producer={producer} event={utc_day} close={close:?}"
            ))
            .into()),
            RawDayGateState::ClosingSameDay(close)
                if allow_controlled_cutoff
                    && matches!(
                        close.cause,
                        RuntimeCloseCause::OperatorStop | RuntimeCloseCause::RunDeadline
                    ) =>
            {
                drop(fence);
                Ok(None)
            }
            RawDayGateState::ClosingSameDay(close) => {
                Err(invalid_data(format!(
                    "raw event arrived after same-day intake close: producer={producer} event={utc_day} close={close:?}"
                ))
                .into())
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RuntimeDeadlineCandidate {
    pub(super) deadline: Instant,
    pub(super) observed_utc_ms: i64,
}

impl RuntimeDeadlineCandidate {
    fn is_reached(self) -> bool {
        Instant::now() >= self.deadline
    }
}
