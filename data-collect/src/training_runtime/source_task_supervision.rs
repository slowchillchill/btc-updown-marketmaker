use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SourceTaskObservation {
    observed_utc_ms: i64,
    epoch: Option<RuntimeEpoch>,
    inner_source_returned: bool,
    inner_result_error: Option<String>,
    lifecycle_record_error: Option<String>,
}

#[derive(Debug)]
struct InnerSourceReturnError {
    source_kind: &'static str,
    message: String,
}

impl std::fmt::Display for InnerSourceReturnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "inner source returned error: source={} error={}",
            self.source_kind, self.message
        )
    }
}

impl Error for InnerSourceReturnError {}

#[derive(Debug)]
pub(super) struct CoordinatedSourceStop {
    pub(super) producer: String,
    pub(super) close: RuntimeCloseIdentity,
}

impl std::fmt::Display for CoordinatedSourceStop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "coordinated source stop: producer={} close={:?}",
            self.producer, self.close
        )
    }
}

impl Error for CoordinatedSourceStop {}

#[derive(Clone, Default)]
pub(super) struct SourceTaskObservationCell {
    observation: Arc<Mutex<Option<SourceTaskObservation>>>,
}

impl SourceTaskObservationCell {
    fn record_inner_return(
        &self,
        observed_utc_ms: i64,
        epoch: Option<RuntimeEpoch>,
        inner_result_error: Option<String>,
        lifecycle_record_error: Option<String>,
    ) {
        let mut observation = self
            .observation
            .lock()
            .expect("source-task observation mutex poisoned");
        if observation.is_none() {
            *observation = Some(SourceTaskObservation {
                observed_utc_ms,
                epoch,
                inner_source_returned: true,
                inner_result_error,
                lifecycle_record_error,
            });
        }
    }

    fn record_wrapper_return(
        &self,
        observed_utc_ms: i64,
        epoch: Option<RuntimeEpoch>,
        lifecycle_record_error: Option<String>,
        replace_existing: bool,
    ) -> SourceTaskObservation {
        let mut observation = self
            .observation
            .lock()
            .expect("source-task observation mutex poisoned");
        if let Some(existing) = observation.as_mut() {
            if replace_existing {
                *existing = SourceTaskObservation {
                    observed_utc_ms,
                    epoch,
                    inner_source_returned: false,
                    inner_result_error: None,
                    lifecycle_record_error,
                };
                return existing.clone();
            }
            if existing.lifecycle_record_error.is_none() {
                existing.lifecycle_record_error = lifecycle_record_error;
            }
            return existing.clone();
        }
        observation
            .insert(SourceTaskObservation {
                observed_utc_ms,
                epoch,
                inner_source_returned: false,
                inner_result_error: None,
                lifecycle_record_error,
            })
            .clone()
    }

    fn snapshot(&self) -> Option<SourceTaskObservation> {
        self.observation
            .lock()
            .expect("source-task observation mutex poisoned")
            .clone()
    }
}

pub(super) async fn observe_inner_source_return<F>(
    source_kind: &'static str,
    source: F,
    observation: SourceTaskObservationCell,
    gate: Arc<RawDayGate>,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let result = source.await;
    let lifecycle = gate.observe_source_terminal_now(
        RuntimeCloseCause::SourceInnerReturn { source_kind },
        None,
        None,
    );
    let inner_result_error = result.as_ref().err().map(ToString::to_string);
    observation.record_inner_return(
        lifecycle.observed_utc_ms,
        lifecycle.epoch,
        inner_result_error.clone(),
        lifecycle.lifecycle_record_error,
    );
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(InnerSourceReturnError {
            source_kind,
            message: error.to_string(),
        }
        .into()),
    }
}

pub(super) struct SourceTaskReport {
    source_kind: &'static str,
    observation: SourceTaskObservation,
    termination_kind: SourceTaskTerminationKind,
    result: Result<()>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SourceAbortPermit {
    pub(super) source_kind: &'static str,
    pub(super) close: RuntimeCloseIdentity,
    pub(super) observed_utc_ms: i64,
    pub(super) epoch: RuntimeEpoch,
}

pub(super) struct SourceTaskHandle {
    pub(super) source_kind: &'static str,
    pub(super) join: tokio::task::JoinHandle<SourceTaskReport>,
    observation: SourceTaskObservationCell,
    gate: Arc<RawDayGate>,
    pub(super) abort_permit: Option<SourceAbortPermit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceTaskTerminationKind {
    ReliableReturn,
    CoordinatedAbort,
    Unclean,
}

pub(super) struct SourceTaskTermination {
    pub(super) source_kind: &'static str,
    pub(super) observed_utc_ms: Option<i64>,
    pub(super) epoch: Option<RuntimeEpoch>,
    pub(super) kind: SourceTaskTerminationKind,
    pub(super) error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SourceControlStopFact {
    Acked,
    Failed(String),
}

pub(super) struct SourceShutdownAssessment {
    pub(super) unclean: bool,
    pub(super) error: Option<String>,
}

pub(super) fn assess_source_shutdown(
    termination: &SourceTaskTermination,
    control_stop: Option<&SourceControlStopFact>,
    close: &RuntimeCloseIdentity,
) -> SourceShutdownAssessment {
    let lifecycle_proven = termination.observed_utc_ms.is_some()
        && termination.epoch.as_ref()
            == Some(&RuntimeEpoch {
                utc_day: close.utc_day.clone(),
                generation: close.generation,
            });
    match termination.kind {
        SourceTaskTerminationKind::ReliableReturn if lifecycle_proven => SourceShutdownAssessment {
            unclean: false,
            error: termination
                .error
                .as_ref()
                .map(|error| format!("source_task_error:{}:{error}", termination.source_kind)),
        },
        SourceTaskTerminationKind::CoordinatedAbort if lifecycle_proven => {
            SourceShutdownAssessment {
                unclean: false,
                error: termination.error.as_ref().map(|error| {
                    format!(
                        "source_task_coordinated_stop_error:{}:{error}",
                        termination.source_kind
                    )
                }),
            }
        }
        SourceTaskTerminationKind::ReliableReturn
        | SourceTaskTerminationKind::CoordinatedAbort
        | SourceTaskTerminationKind::Unclean => {
            let termination_detail = termination
                .error
                .as_deref()
                .unwrap_or("missing reliable lifecycle proof");
            let control_detail = match control_stop {
                Some(SourceControlStopFact::Failed(error)) => {
                    format!("; control_stop_failed:{error}")
                }
                Some(SourceControlStopFact::Acked) | None => String::new(),
            };
            SourceShutdownAssessment {
                unclean: true,
                error: Some(format!(
                    "source_task_unclean:{}:{termination_detail}{control_detail}",
                    termination.source_kind
                )),
            }
        }
    }
}

pub(super) async fn await_source_control_stop_fact(
    reply: oneshot::Receiver<std::result::Result<(), String>>,
    deadline: Instant,
    operation: &'static str,
) -> SourceControlStopFact {
    match timeout(deadline.saturating_duration_since(Instant::now()), reply).await {
        Ok(Ok(Ok(()))) => SourceControlStopFact::Acked,
        Ok(Ok(Err(error))) => SourceControlStopFact::Failed(format!("{operation}_failed:{error}")),
        Ok(Err(_)) => SourceControlStopFact::Failed(format!("{operation}_reply_dropped")),
        Err(_) => SourceControlStopFact::Failed(format!("{operation}_reply_timeout")),
    }
}

impl SourceTaskHandle {
    #[cfg(test)]
    pub(super) fn spawn<F>(
        source_kind: &'static str,
        wrapper: F,
        observation: SourceTaskObservationCell,
        gate: Arc<RawDayGate>,
    ) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        Self::spawn_inner(source_kind, wrapper, observation, gate, None)
    }

    pub(super) fn spawn_with_dataset_lease<F>(
        source_kind: &'static str,
        wrapper: F,
        observation: SourceTaskObservationCell,
        gate: Arc<RawDayGate>,
        dataset_lease: Arc<File>,
    ) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        Self::spawn_inner(source_kind, wrapper, observation, gate, Some(dataset_lease))
    }

    fn spawn_inner<F>(
        source_kind: &'static str,
        wrapper: F,
        observation: SourceTaskObservationCell,
        gate: Arc<RawDayGate>,
        dataset_lease: Option<Arc<File>>,
    ) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let task_observation = observation.clone();
        let task_gate = Arc::clone(&gate);
        let join = tokio::spawn(async move {
            let _dataset_lease = dataset_lease;
            match catch_source_return(wrapper).await {
                Ok(result) => {
                    let existing = task_observation.snapshot();
                    let lifecycle =
                        task_gate.observe_source_wrapper_terminal_now(source_kind, || {
                            let consistent_inner = existing.as_ref().is_some_and(|observed| {
                                observed.inner_source_returned
                                    && observed.lifecycle_record_error.is_none()
                                    && match result.as_ref() {
                                        Ok(()) => observed.inner_result_error.is_none(),
                                        Err(error) => error
                                            .downcast_ref::<InnerSourceReturnError>()
                                            .is_some_and(|inner| {
                                                inner.source_kind == source_kind
                                                    && observed.inner_result_error.is_some()
                                            }),
                                    }
                            });
                            if consistent_inner {
                                return SourceWrapperTerminalClass::ExistingInnerReturn;
                            }
                            match result.as_ref() {
                                Ok(()) if existing.is_none() => {
                                    SourceWrapperTerminalClass::CleanReturn
                                }
                                Err(error) if existing.is_none() => error
                                    .downcast_ref::<CoordinatedSourceStop>()
                                    .filter(|stop| {
                                        logical_producer_for_source_task(source_kind).ok()
                                            == Some(stop.producer.as_str())
                                    })
                                    .map_or_else(
                                        || {
                                            SourceWrapperTerminalClass::Unclean(
                                                RuntimeCloseCause::SourceWrapperFailure {
                                                    source_kind,
                                                },
                                            )
                                        },
                                        |stop| {
                                            SourceWrapperTerminalClass::CoordinatedStop(
                                                stop.close.clone(),
                                            )
                                        },
                                    ),
                                _ => SourceWrapperTerminalClass::Unclean(
                                    RuntimeCloseCause::SourceWrapperFailure { source_kind },
                                ),
                            }
                        });
                    let observation = task_observation.record_wrapper_return(
                        lifecycle.observed_utc_ms,
                        lifecycle.epoch,
                        lifecycle.lifecycle_record_error,
                        lifecycle.replace_existing,
                    );
                    SourceTaskReport {
                        source_kind,
                        observation,
                        termination_kind: lifecycle.termination_kind,
                        result,
                    }
                }
                Err(payload) => {
                    let lifecycle =
                        task_gate.observe_source_wrapper_terminal_now(source_kind, || {
                            SourceWrapperTerminalClass::Unclean(RuntimeCloseCause::SourcePanic {
                                source_kind,
                            })
                        });
                    task_observation.record_wrapper_return(
                        lifecycle.observed_utc_ms,
                        lifecycle.epoch,
                        lifecycle.lifecycle_record_error,
                        lifecycle.replace_existing,
                    );
                    std::panic::resume_unwind(payload)
                }
            }
        });
        Self {
            source_kind,
            join,
            observation,
            gate,
            abort_permit: None,
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    pub(super) fn abort_without_permit(&self) {
        let lifecycle = self.gate.observe_source_terminal_now(
            RuntimeCloseCause::SourceAbortWithoutPermit {
                source_kind: self.source_kind,
            },
            Some(RuntimeCloseCause::SourceAbortWithoutPermit {
                source_kind: self.source_kind,
            }),
            None,
        );
        self.observation.record_wrapper_return(
            lifecycle.observed_utc_ms,
            lifecycle.epoch,
            lifecycle.lifecycle_record_error,
            true,
        );
        self.join.abort();
    }

    pub(super) fn abort_with_permit(&mut self, close: &RuntimeCloseIdentity) -> Result<()> {
        if self.abort_permit.is_some() {
            return Err(invalid_data(format!(
                "source abort permit already registered: source={} close={close:?}",
                self.source_kind
            ))
            .into());
        }
        let (observed_utc_ms, epoch) = self.gate.register_source_abort_permit_now(close)?;
        self.abort_permit = Some(SourceAbortPermit {
            source_kind: self.source_kind,
            close: close.clone(),
            observed_utc_ms,
            epoch,
        });
        self.join.abort();
        Ok(())
    }

    pub(super) async fn await_termination(
        self,
        close: &RuntimeCloseIdentity,
    ) -> SourceTaskTermination {
        let SourceTaskHandle {
            source_kind,
            join,
            observation,
            gate: _,
            abort_permit,
        } = self;
        match join.await {
            Ok(report) => {
                let identity_matches = report.source_kind == source_kind;
                let coordinated_stop_matches = report.result.as_ref().err().is_some_and(|error| {
                    error
                        .downcast_ref::<CoordinatedSourceStop>()
                        .is_some_and(|stop| {
                            logical_producer_for_source_task(source_kind).ok()
                                == Some(stop.producer.as_str())
                                && stop.close == *close
                        })
                });
                let kind = if !identity_matches
                    || (report.termination_kind == SourceTaskTerminationKind::CoordinatedAbort
                        && !coordinated_stop_matches)
                {
                    SourceTaskTerminationKind::Unclean
                } else {
                    report.termination_kind
                };
                let lifecycle_error = report.observation.lifecycle_record_error.clone();
                SourceTaskTermination {
                    source_kind,
                    observed_utc_ms: Some(report.observation.observed_utc_ms),
                    epoch: report.observation.epoch,
                    kind,
                    error: match (report.result, lifecycle_error, kind) {
                        (_, Some(error), _) => Some(error),
                        (_, None, SourceTaskTerminationKind::CoordinatedAbort) => None,
                        (_, None, SourceTaskTerminationKind::ReliableReturn) => {
                            report.observation.inner_result_error
                        }
                        (Ok(()), None, _) if !identity_matches => Some(format!(
                            "source task report identity mismatch: expected={source_kind} actual={}",
                            report.source_kind
                        )),
                        (Ok(()), None, _) => Some(format!(
                            "source wrapper ended without a reliable terminal proof: {source_kind}"
                        )),
                        (Err(error), None, _) => Some(error.to_string()),
                    },
                }
            }
            Err(error) if error.is_panic() => {
                let observed = observation.snapshot();
                SourceTaskTermination {
                    source_kind,
                    observed_utc_ms: observed.as_ref().map(|observed| observed.observed_utc_ms),
                    epoch: observed
                        .as_ref()
                        .and_then(|observed| observed.epoch.clone()),
                    kind: SourceTaskTerminationKind::Unclean,
                    error: Some(
                        observed
                            .and_then(|observed| observed.lifecycle_record_error)
                            .map_or_else(
                                || error.to_string(),
                                |record_error| {
                                    format!("{error}; lifecycle_record_error:{record_error}")
                                },
                            ),
                    ),
                }
            }
            Err(error) if error.is_cancelled() => {
                let permit_observation = abort_permit
                    .as_ref()
                    .filter(|permit| permit.source_kind == source_kind && permit.close == *close);
                let permit_matches = permit_observation.is_some();
                let inner = observation.snapshot();
                let reliable_inner = permit_matches
                    && inner.as_ref().is_some_and(|inner| {
                        inner.inner_source_returned && inner.lifecycle_record_error.is_none()
                    });
                SourceTaskTermination {
                    source_kind,
                    observed_utc_ms: inner
                        .as_ref()
                        .map(|inner| inner.observed_utc_ms)
                        .or_else(|| permit_observation.map(|permit| permit.observed_utc_ms)),
                    epoch: inner
                        .as_ref()
                        .and_then(|inner| inner.epoch.clone())
                        .or_else(|| permit_observation.map(|permit| permit.epoch.clone())),
                    kind: if reliable_inner {
                        SourceTaskTerminationKind::ReliableReturn
                    } else if permit_matches {
                        SourceTaskTerminationKind::CoordinatedAbort
                    } else {
                        SourceTaskTerminationKind::Unclean
                    },
                    error: if reliable_inner {
                        inner.and_then(|inner| {
                            inner.lifecycle_record_error.or(inner.inner_result_error)
                        })
                    } else {
                        (!permit_matches).then(|| error.to_string())
                    },
                }
            }
            Err(error) => SourceTaskTermination {
                source_kind,
                observed_utc_ms: None,
                epoch: None,
                kind: SourceTaskTerminationKind::Unclean,
                error: Some(error.to_string()),
            },
        }
    }
}

pub(super) fn finished_source_task_index(handles: &[SourceTaskHandle]) -> Option<usize> {
    handles.iter().position(SourceTaskHandle::is_finished)
}

pub(super) fn source_task_ended_unexpectedly(source_kind: &str) -> String {
    format!("source_task_ended_unexpectedly:{source_kind}")
}

fn catch_source_return<F>(source: F) -> impl Future<Output = std::thread::Result<Result<()>>>
where
    F: Future<Output = Result<()>>,
{
    // 组合来源 Future 放在堆上，避免新增监督层放大默认工作线程的 poll 栈帧。
    std::panic::AssertUnwindSafe(Box::pin(source)).catch_unwind()
}

/// 共用来源监督的返回/异常边界；热会话不构造训练日 epoch 或关闭凭据。
pub(super) async fn observe_hot_source_return<F>(
    source_kind: &'static str,
    source: F,
    gate: Arc<HotSessionGate>,
) -> SourceTaskTermination
where
    F: Future<Output = Result<()>>,
{
    let result = catch_source_return(source).await;
    let observed_utc_ms = now_ms();
    let (kind, error) = match result {
        Ok(Ok(())) if gate.cutoff() => (SourceTaskTerminationKind::ReliableReturn, None),
        Ok(Ok(())) => (
            SourceTaskTerminationKind::Unclean,
            Some(format!(
                "source returned before hot session cutoff:{source_kind}"
            )),
        ),
        Ok(Err(error)) => (
            SourceTaskTerminationKind::ReliableReturn,
            Some(error.to_string()),
        ),
        Err(payload) => {
            let reason = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|value| (*value).to_owned())
                })
                .unwrap_or_else(|| "non-string panic".to_owned());
            (
                SourceTaskTerminationKind::Unclean,
                Some(format!("source panic:{source_kind}:{reason}")),
            )
        }
    };
    if let Some(error) = &error {
        gate.stop(RuntimeCloseCause::SourceInnerReturn { source_kind });
        gate.fail(format!("{source_kind}:{error}"));
    }
    SourceTaskTermination {
        source_kind,
        observed_utc_ms: Some(observed_utc_ms),
        epoch: None,
        kind,
        error,
    }
}
