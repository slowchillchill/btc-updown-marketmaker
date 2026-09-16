use super::*;

/// 来源只选择训练日或当前热会话；排序、额度及准入印章仍由原队列负责。
#[derive(Clone)]
pub(super) enum SourceAdmissionGate {
    Training(Arc<RawDayGate>),
    Hot(Arc<HotSessionGate>),
}

impl From<Arc<RawDayGate>> for SourceAdmissionGate {
    fn from(gate: Arc<RawDayGate>) -> Self {
        Self::Training(gate)
    }
}

pub(super) struct HotSessionGate {
    deadline: Option<Instant>,
    stopped: Mutex<Option<RuntimeCloseCause>>,
    failure: Mutex<Option<String>>,
    changes: watch::Sender<u64>,
    #[cfg(test)]
    pub(super) comparison_root: Option<PathBuf>,
    #[cfg(test)]
    pub(super) send_measurements: Mutex<Option<Vec<(usize, usize, Option<usize>)>>>,
}

impl HotSessionGate {
    pub(super) fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            stopped: Mutex::new(None),
            failure: Mutex::new(None),
            changes: watch::channel(0).0,
            #[cfg(test)]
            comparison_root: None,
            #[cfg(test)]
            send_measurements: Mutex::new(None),
        }
    }

    pub(super) fn stop(&self, cause: RuntimeCloseCause) {
        let mut stopped = self.stopped.lock().expect("hot session mutex poisoned");
        if stopped.is_none() {
            *stopped = Some(cause);
            self.changes
                .send_modify(|value| *value = value.wrapping_add(1));
        }
    }

    pub(super) fn cutoff(&self) -> bool {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.stop(RuntimeCloseCause::RunDeadline);
        }
        self.stopped
            .lock()
            .expect("hot session mutex poisoned")
            .is_some()
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(super) fn fail(&self, reason: String) {
        let mut failure = self.failure.lock().expect("hot failure mutex poisoned");
        if failure.is_none() {
            *failure = Some(reason);
            self.changes
                .send_modify(|value| *value = value.wrapping_add(1));
        }
    }

    pub(super) fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .expect("hot failure mutex poisoned")
            .clone()
    }
}

pub(super) struct SourceEnqueuePermit {
    pub(super) raw_day_permit: Option<RawDayPermit>,
    _training: Option<RawDayEnqueuePermit>,
}

pub(super) enum SourceEnqueueDecision {
    Permit(SourceEnqueuePermit),
    Defer(RawDayDeferredEpoch),
}

impl From<RawDayEnqueueDecision> for SourceEnqueueDecision {
    fn from(value: RawDayEnqueueDecision) -> Self {
        match value {
            RawDayEnqueueDecision::Permit(permit) => Self::Permit(SourceEnqueuePermit {
                raw_day_permit: Some(permit.permit.clone()),
                _training: Some(permit),
            }),
            RawDayEnqueueDecision::Defer(epoch) => Self::Defer(epoch),
        }
    }
}

impl SourceAdmissionGate {
    pub(super) fn training(&self) -> Option<Arc<RawDayGate>> {
        match self {
            Self::Training(gate) => Some(Arc::clone(gate)),
            Self::Hot(_) => None,
        }
    }

    pub(super) fn unadmitted_runtime_cutoff_is_active(&self) -> bool {
        match self {
            Self::Training(gate) => gate.unadmitted_runtime_cutoff_is_active(),
            Self::Hot(gate) => gate.cutoff(),
        }
    }

    pub(super) async fn begin_unadmitted_event_enqueue(
        &self,
        producer: &str,
        received: i64,
    ) -> Result<Option<SourceEnqueueDecision>> {
        match self {
            Self::Training(gate) => Ok(gate
                .begin_unadmitted_event_enqueue(producer, received)
                .await?
                .map(Into::into)),
            Self::Hot(gate) => Ok((!gate.cutoff()).then_some(SourceEnqueueDecision::Permit(
                SourceEnqueuePermit {
                    raw_day_permit: None,
                    _training: None,
                },
            ))),
        }
    }

    pub(super) async fn accept_unadmitted_event_ticket(
        &self,
        producer: &str,
        received: i64,
    ) -> Result<Option<RawDayAcceptedTicket>> {
        match self {
            Self::Training(gate) => {
                gate.accept_unadmitted_event_ticket(producer, received)
                    .await
            }
            Self::Hot(gate) => Ok((!gate.cutoff()).then_some(RawDayAcceptedTicket::HotSession)),
        }
    }

    /// 热请求在发起时受理；完成时沿父票据保留真实接收事实。
    pub(super) fn hot_request_ticket(&self) -> Option<RawDayAcceptedTicket> {
        match self {
            Self::Hot(gate) if !gate.cutoff() => Some(RawDayAcceptedTicket::HotSession),
            _ => None,
        }
    }

    pub(super) async fn accept_derived_polymarket_event_ticket(
        &self,
        parent: &RawDayAcceptedTicket,
        received: i64,
    ) -> Result<RawDayAcceptedTicket> {
        match self {
            Self::Training(gate) => {
                gate.accept_derived_polymarket_event_ticket(parent, received)
                    .await
            }
            Self::Hot(_) => Ok(parent.clone()),
        }
    }

    pub(super) async fn begin_accepted_ticket_enqueue(
        &self,
        producer: &str,
        received: i64,
        ticket: &RawDayAcceptedTicket,
    ) -> Result<SourceEnqueueDecision> {
        match self {
            Self::Training(gate) => Ok(gate
                .begin_accepted_ticket_enqueue(producer, received, ticket)
                .await?
                .into()),
            Self::Hot(_) => Ok(SourceEnqueueDecision::Permit(SourceEnqueuePermit {
                raw_day_permit: None,
                _training: None,
            })),
        }
    }

    pub(super) async fn wait_for_hot_cutoff(&self) {
        match self {
            Self::Training(_) => std::future::pending::<()>().await,
            Self::Hot(gate) => {
                let mut changes = gate.subscribe();
                while !gate.cutoff() {
                    if changes.changed().await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    pub(super) async fn accept_response_ticket(
        &self,
        parent: Option<&RawDayAcceptedTicket>,
        received: i64,
    ) -> Result<Option<RawDayAcceptedTicket>> {
        match parent {
            Some(parent) => self
                .accept_derived_polymarket_event_ticket(parent, received)
                .await
                .map(Some),
            None => {
                self.accept_unadmitted_event_ticket("polymarket", received)
                    .await
            }
        }
    }

    pub(super) fn observe_source_failure(&self, cause: RuntimeCloseCause) {
        match self {
            Self::Training(gate) => {
                gate.observe_source_terminal_now(cause, None, None);
            }
            Self::Hot(gate) => gate.stop(cause),
        }
    }
}
